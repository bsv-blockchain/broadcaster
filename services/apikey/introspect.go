package apikey

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/store"
)

const (
	introspectMaxBody   = 64 << 10
	introspectWarnEvery = 10 * time.Second

	// After a failed call the provider is left alone for a while: every
	// uncached key's lookup would otherwise go straight back to a provider
	// that is down, throttling us (WoC rate-limits per source IP, so one
	// replica's retries would throttle every replica behind the same egress)
	// or rejecting our token. Cached keys keep working throughout via the
	// resolver's stale policy.
	backoffTransient   = 2 * time.Second  // timeout, connection error, 5xx
	backoffThrottled   = 5 * time.Second  // 429 without Retry-After
	backoffCredentials = 30 * time.Second // 401/403: needs a config fix
	backoffMax         = time.Minute      // cap on a provider's Retry-After
)

// Results for metrics.APIKeyIntrospectTotal.
const (
	introspectValid        = "valid"
	introspectInvalid      = "invalid"
	introspectUnauthorized = "unauthorized"
	introspectError        = "error"
	introspectBackoff      = "backoff" // not called: backing off after a failure
)

// errIntrospectionBackoff is returned without calling the provider while
// it is backing off after a failure.
var errIntrospectionBackoff = errors.New("backing off after a failed call")

// ErrIntrospectionCredentials means the provider rejected the service token
// (401/403). It is a configuration error, not an unknown key: the resolver
// treats it like any other lookup failure (503, or a stale cached answer),
// never as "key not found", so a bad token cannot 401 every customer.
var ErrIntrospectionCredentials = errors.New("introspection provider rejected the service token")

// Introspector is a Lookup backed by an external provider's introspection
// endpoint (auth.introspection), for keys the provider issues under its own
// prefix. The protocol:
//
//	POST <url>
//	Authorization: Bearer <service token>
//	{"key_hash": "<hex sha256 of the key>"}
//
// 200 {"valid": true, "key_id", "tier", "limit_id"?, "expires_at"?} is a
// usable key; 200 {"valid": false} is an unknown, revoked or out-of-scope
// key; any other status is a failure to verify. The key itself never
// leaves the broadcaster. Answers are cached by the Resolver, not here.
//
// After a failed call (other than a malformed answer) the Introspector
// fails fast without calling the provider until a backoff passes:
// Retry-After when the provider sends one (capped at backoffMax), else a
// default by failure kind.
type Introspector struct {
	name   string
	url    string
	token  string
	client *http.Client
	logger *zap.Logger

	now func() time.Time

	mu        sync.Mutex
	openUntil time.Time // no calls before this
	warnedAt  time.Time
}

// NewIntrospector builds an Introspector for provider name. timeout bounds
// each call; keep it under the resolver's lookup timeout.
func NewIntrospector(name, url, token string, timeout time.Duration, logger *zap.Logger) *Introspector {
	return &Introspector{
		name: name, url: url, token: token,
		client: &http.Client{
			Timeout: timeout,
			// Never follow a redirect: it would resend the service token
			// to wherever the provider points.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		logger: logger.Named("introspect").With(zap.String("provider", name)),
		now:    time.Now,
	}
}

type introspectRequest struct {
	KeyHash string `json:"key_hash"`
}

type introspectResponse struct {
	Valid     bool       `json:"valid"`
	KeyID     string     `json:"key_id"`
	LimitID   string     `json:"limit_id"`
	Tier      string     `json:"tier"`
	ExpiresAt *time.Time `json:"expires_at"`
}

// GetAPIKeyByHash implements Lookup.
func (in *Introspector) GetAPIKeyByHash(ctx context.Context, hash string) (*store.APIKey, error) {
	in.mu.Lock()
	until := in.openUntil
	in.mu.Unlock()
	if in.now().Before(until) {
		metrics.APIKeyIntrospectTotal.WithLabelValues(in.name, introspectBackoff).Inc()
		return nil, fmt.Errorf("introspect %s: %w until %s", in.name, errIntrospectionBackoff, until.UTC().Format(time.RFC3339))
	}
	k, result, backoff, err := in.introspect(ctx, hash)
	metrics.APIKeyIntrospectTotal.WithLabelValues(in.name, result).Inc()
	if backoff > 0 {
		in.mu.Lock()
		in.openUntil = in.now().Add(backoff)
		in.mu.Unlock()
	}
	return k, err
}

// introspect makes one call. backoff, when positive, is how long to leave
// the provider alone after this failure.
func (in *Introspector) introspect(ctx context.Context, hash string) (k *store.APIKey, result string, backoff time.Duration, err error) {
	body, err := json.Marshal(introspectRequest{KeyHash: hash})
	if err != nil {
		return nil, introspectError, 0, fmt.Errorf("introspect %s: encode: %w", in.name, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, in.url, bytes.NewReader(body))
	if err != nil {
		return nil, introspectError, 0, fmt.Errorf("introspect %s: %w", in.name, err)
	}
	req.Header.Set("Authorization", "Bearer "+in.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := in.client.Do(req)
	if err != nil {
		return nil, introspectError, backoffTransient, fmt.Errorf("introspect %s: %w", in.name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, introspectMaxBody))
	if err != nil {
		return nil, introspectError, backoffTransient, fmt.Errorf("introspect %s: read body: %w", in.name, err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		in.throttledError("introspection provider rejected the service token; check auth.introspection token",
			zap.Int("http_status", resp.StatusCode))
		return nil, introspectUnauthorized, backoffCredentials, fmt.Errorf("%w: %s answered %d", ErrIntrospectionCredentials, in.name, resp.StatusCode)
	case http.StatusTooManyRequests:
		backoff = retryAfter(resp.Header.Get("Retry-After"), backoffThrottled)
		in.throttledError("introspection provider is rate limiting us; backing off",
			zap.Int("http_status", resp.StatusCode), zap.Duration("backoff", backoff))
		return nil, introspectError, backoff, fmt.Errorf("introspect %s: throttled (429)", in.name)
	default:
		if resp.StatusCode >= http.StatusInternalServerError {
			backoff = retryAfter(resp.Header.Get("Retry-After"), backoffTransient)
		}
		return nil, introspectError, backoff, fmt.Errorf("introspect %s: unexpected status %d", in.name, resp.StatusCode)
	}

	var r introspectResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, introspectError, 0, fmt.Errorf("introspect %s: decode: %w", in.name, err)
	}
	if !r.Valid {
		return nil, introspectInvalid, 0, store.ErrAPIKeyNotFound
	}
	if r.KeyID == "" {
		return nil, introspectError, 0, fmt.Errorf("introspect %s: valid answer without key_id", in.name)
	}
	k = &store.APIKey{
		ID:      r.KeyID,
		Hash:    hash,
		Tier:    r.Tier,
		Status:  store.APIKeyActive,
		LimitID: r.LimitID,
	}
	if r.ExpiresAt != nil {
		t := r.ExpiresAt.UTC()
		k.ExpiresAt = &t
	}
	return k, introspectValid, 0, nil
}

// retryAfter parses a Retry-After header given in seconds, capped at
// backoffMax; def when absent or unparseable. (The HTTP-date form is not
// used by our providers.)
func retryAfter(h string, def time.Duration) time.Duration {
	secs, err := strconv.Atoi(h)
	if err != nil || secs <= 0 {
		return def
	}
	return min(time.Duration(secs)*time.Second, backoffMax)
}

// throttledError logs at Error at most once per introspectWarnEvery: a bad
// service token or a throttling provider fails every lookup until it clears.
func (in *Introspector) throttledError(msg string, fields ...zap.Field) {
	in.mu.Lock()
	now := in.now()
	if now.Sub(in.warnedAt) < introspectWarnEvery {
		in.mu.Unlock()
		return
	}
	in.warnedAt = now
	in.mu.Unlock()
	in.logger.Error(msg, fields...)
}
