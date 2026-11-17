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
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/store"
)

const (
	introspectMaxBody   = 64 << 10
	introspectWarnEvery = 10 * time.Second
	// introspectMaxConcurrent caps in-flight calls to one provider, so a
	// burst of distinct uncached keys (a cold cache after a deploy) queues
	// here instead of arriving at the provider all at once. It is enforced
	// with a semaphore around the call, not by the transport alone: over
	// HTTP/2 one connection multiplexes many concurrent requests.
	introspectMaxConcurrent = 16

	// After a failed call the provider is left alone for a while: every
	// uncached key's lookup would otherwise go straight back to a provider
	// that is down, throttling us, or rejecting our token. Providers that
	// rate-limit per source IP would otherwise throttle every replica behind
	// the same egress. Cached keys keep working throughout via the
	// resolver's stale policy.
	backoffTransient     = 2 * time.Second  // timeout, connection error, 5xx, undecodable answer
	backoffThrottled     = 5 * time.Second  // 429 without Retry-After
	backoffMisconfigured = 30 * time.Second // 401/403 or another unexpected status: needs a config fix
	backoffMax           = time.Minute      // cap on a provider's Retry-After

	// introspectRetryWait is how long a lookup waits before its single
	// retry after a throttled (429) or failed (5xx, transport) call, and the
	// longest backoff a lookup waits out rather than failing. A provider
	// that throttles briefly (WoC answered two of 22 calls with 429 on
	// 2026-11-17) then costs the caller about a second instead of a 503.
	introspectRetryWait = time.Second
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

// IntrospectorConfig configures one provider.
type IntrospectorConfig struct {
	// Name identifies the provider in logs and metrics, and namespaces the
	// key and limit ids it returns.
	Name  string
	URL   string
	Token string
	// Timeout bounds each call; keep it under the resolver's lookup timeout.
	Timeout time.Duration
	// Tiers maps the provider's tier names (lowercase) to local
	// ratelimit.tiers names. An answer whose tier is not mapped gets
	// config.TierFree, so a provider can only reach tiers the operator
	// mapped.
	Tiers map[string]string
}

// Introspector is a Lookup backed by a delegated-auth provider's
// introspection endpoint (auth.introspection), for keys the provider issues
// under its own prefix. The protocol:
//
//	POST <url>
//	Authorization: Bearer <service token>
//	{"key_hash": "<hex sha256 of the key>"}
//
// 200 {"valid": true, "key_id", "tier"?, "limit_id"?, "expires_at"?} is a
// usable key (expires_at is RFC 3339, or null); 200 {"valid": false} is an
// unknown, revoked or out-of-scope key; any other status is a failure to
// verify. The key itself never leaves the broadcaster. Answers are cached
// by the Resolver, not here.
//
// The returned key's ID and LimitID are namespaced "<provider>:<id>", so
// two providers (or a provider and a local key) can never share an
// allowance by returning the same id.
//
// After a failed call the Introspector fails fast without calling the
// provider until a backoff passes: Retry-After when the provider sends one
// (capped at backoffMax), else a default by failure kind. A backoff only
// ever extends: a later failure with a shorter one does not cut short a
// longer one already in force.
//
// A throttled or failed call that the provider did not ask us to stay away
// from for longer than introspectRetryWait is retried once, after
// introspectRetryWait, before the lookup fails. Meanwhile a short backoff is
// in force, which other lookups wait out instead of failing; a long one
// (a second failure, a long Retry-After, a misconfiguration) still fails
// them fast, so an outage never queues requests.
type Introspector struct {
	name   string
	url    string
	token  string
	tiers  map[string]string
	client *http.Client
	logger *zap.Logger
	sem    chan struct{} // introspectMaxConcurrent slots

	now   func() time.Time
	sleep func(context.Context, time.Duration) error // waits for a backoff; a field so tests can fake it

	mu        sync.Mutex
	openUntil time.Time            // no calls before this
	warnedAt  map[string]time.Time // per log message
}

// NewIntrospector builds an Introspector.
func NewIntrospector(cfg IntrospectorConfig, logger *zap.Logger) *Introspector {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = introspectMaxConcurrent
	transport.MaxIdleConnsPerHost = introspectMaxConcurrent
	tiers := make(map[string]string, len(cfg.Tiers))
	for k, v := range cfg.Tiers {
		tiers[strings.ToLower(k)] = v
	}
	return &Introspector{
		name: cfg.Name, url: cfg.URL, token: cfg.Token, tiers: tiers,
		client: &http.Client{
			Timeout:   cfg.Timeout,
			Transport: transport,
			// Never follow a redirect: it would resend the service token
			// to wherever the provider points.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		logger:   logger.Named("introspect").With(zap.String("provider", cfg.Name)),
		sem:      make(chan struct{}, introspectMaxConcurrent),
		now:      time.Now,
		sleep:    sleepCtx,
		warnedAt: map[string]time.Time{},
	}
}

type introspectRequest struct {
	KeyHash string `json:"key_hash"`
}

type introspectResponse struct {
	Valid     bool    `json:"valid"`
	KeyID     string  `json:"key_id"`
	LimitID   string  `json:"limit_id"`
	Tier      string  `json:"tier"`
	ExpiresAt *string `json:"expires_at"`
}

// GetAPIKeyByHash implements Lookup.
func (in *Introspector) GetAPIKeyByHash(ctx context.Context, hash string) (*store.APIKey, error) {
	for attempt := 0; ; attempt++ {
		k, retry, err := in.call(ctx, hash, attempt == 0)
		if !retry {
			return k, err
		}
	}
}

// call makes one guarded call. When mayRetry and the failure is one worth a
// single retry (see introspect), it starts only a short backoff and reports
// retry; the caller's next call waits that backoff out.
func (in *Introspector) call(ctx context.Context, hash string, mayRetry bool) (k *store.APIKey, retry bool, err error) {
	if err = in.awaitBackoff(ctx); err != nil {
		return nil, false, err
	}
	select {
	case in.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, false, fmt.Errorf("introspect %s: waiting for a free call slot: %w", in.name, ctx.Err())
	}
	defer func() { <-in.sem }()
	// A call that failed while this one queued may have started a backoff.
	if err = in.backingOff(); err != nil {
		return nil, false, err
	}
	k, result, backoff, retryable, err := in.introspect(ctx, hash)
	metrics.APIKeyIntrospectTotal.WithLabelValues(in.name, result).Inc()
	if backoff > 0 {
		if mayRetry && retryable {
			in.extendBackoff(introspectRetryWait)
			return nil, true, err
		}
		in.extendBackoff(backoff)
	}
	return k, false, err
}

// awaitBackoff waits out a backoff of at most introspectRetryWait and fails
// fast, as backingOff does, on a longer one.
func (in *Introspector) awaitBackoff(ctx context.Context) error {
	in.mu.Lock()
	until := in.openUntil
	in.mu.Unlock()
	wait := until.Sub(in.now())
	if wait <= 0 || wait > introspectRetryWait {
		return in.backingOff()
	}
	if err := in.sleep(ctx, wait); err != nil {
		return fmt.Errorf("introspect %s: waiting out a backoff: %w", in.name, err)
	}
	return nil
}

// backingOff returns errIntrospectionBackoff, and counts it, while a
// backoff is in force.
func (in *Introspector) backingOff() error {
	in.mu.Lock()
	until := in.openUntil
	in.mu.Unlock()
	if !in.now().Before(until) {
		return nil
	}
	metrics.APIKeyIntrospectTotal.WithLabelValues(in.name, introspectBackoff).Inc()
	return fmt.Errorf("introspect %s: %w until %s", in.name, errIntrospectionBackoff, until.UTC().Format(time.RFC3339))
}

// sleepCtx waits d or until ctx ends.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// extendBackoff stops calls for d from now, unless a longer backoff is
// already in force: a concurrent call failing later with a short backoff
// (a timeout) must not cut short a provider's Retry-After.
func (in *Introspector) extendBackoff(d time.Duration) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if t := in.now().Add(d); t.After(in.openUntil) {
		in.openUntil = t
	}
}

// introspect makes one call. backoff, when positive, is how long to leave
// the provider alone after this failure. retryable marks a failure worth one
// retry after introspectRetryWait: a throttle (429), a provider error (5xx)
// or a transport failure, when the provider did not ask for a longer wait.
// A rejected token, an unexpected status or an undecodable answer will not
// change in a second, so it is not.
func (in *Introspector) introspect(ctx context.Context, hash string) (k *store.APIKey, result string, backoff time.Duration, retryable bool, err error) {
	body, err := json.Marshal(introspectRequest{KeyHash: hash})
	if err != nil {
		return nil, introspectError, 0, false, fmt.Errorf("introspect %s: encode: %w", in.name, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, in.url, bytes.NewReader(body))
	if err != nil {
		return nil, introspectError, 0, false, fmt.Errorf("introspect %s: %w", in.name, err)
	}
	req.Header.Set("Authorization", "Bearer "+in.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := in.client.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			in.throttledLog("introspection call failed", zap.Error(err))
		}
		return nil, introspectError, backoffTransient, ctx.Err() == nil, fmt.Errorf("introspect %s: %w", in.name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, introspectMaxBody))
	if err != nil {
		return nil, introspectError, backoffTransient, false, fmt.Errorf("introspect %s: read body: %w", in.name, err)
	}

	switch code := resp.StatusCode; {
	case code == http.StatusOK:
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		in.throttledLog("introspection provider rejected the service token; check auth.introspection token",
			zap.Int("http_status", code))
		return nil, introspectUnauthorized, backoffMisconfigured, false, fmt.Errorf("%w: %s answered %d", ErrIntrospectionCredentials, in.name, code)
	case code == http.StatusTooManyRequests:
		backoff = retryAfter(resp.Header.Get("Retry-After"), backoffThrottled, in.now())
		in.throttledLog("introspection provider is rate limiting us; backing off",
			zap.Int("http_status", code), zap.Duration("backoff", backoff))
		return nil, introspectError, backoff, shortWait(resp.Header.Get("Retry-After"), in.now()), fmt.Errorf("introspect %s: throttled (429)", in.name)
	case code >= http.StatusInternalServerError:
		backoff = retryAfter(resp.Header.Get("Retry-After"), backoffTransient, in.now())
		in.throttledLog("introspection provider failed; backing off",
			zap.Int("http_status", code), zap.Duration("backoff", backoff))
		return nil, introspectError, backoff, shortWait(resp.Header.Get("Retry-After"), in.now()), fmt.Errorf("introspect %s: status %d", in.name, code)
	default:
		// A redirect, 404, 405, 400...: the request will not succeed for any
		// key until the configuration changes.
		in.throttledLog("introspection provider answered an unexpected status; check auth.introspection url",
			zap.Int("http_status", code))
		return nil, introspectError, backoffMisconfigured, false, fmt.Errorf("introspect %s: unexpected status %d", in.name, code)
	}

	var r introspectResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		in.throttledLog("introspection provider sent an undecodable answer", zap.Error(err))
		return nil, introspectError, backoffTransient, false, fmt.Errorf("introspect %s: decode: %w", in.name, err)
	}
	if !r.Valid {
		return nil, introspectInvalid, 0, false, store.ErrAPIKeyNotFound
	}
	// A malformed record fails only its own key: backing off the provider
	// for it would 503 every other uncached key while one client retries.
	if r.KeyID == "" {
		in.throttledLog("introspection provider sent a valid answer without key_id")
		return nil, introspectError, 0, false, fmt.Errorf("introspect %s: valid answer without key_id", in.name)
	}
	k = &store.APIKey{
		ID:            in.namespaced(r.KeyID),
		Provider:      in.name,
		ProviderKeyID: r.KeyID,
		Hash:          hash,
		Tier:          in.localTier(r.Tier),
		Status:        store.APIKeyActive,
	}
	if r.LimitID != "" {
		k.LimitID = in.namespaced(r.LimitID)
	}
	if r.ExpiresAt != nil && *r.ExpiresAt != "" {
		t, err := time.Parse(time.RFC3339Nano, *r.ExpiresAt)
		if err != nil {
			in.throttledLog("introspection provider sent an expires_at that is not RFC 3339", zap.Error(err))
			return nil, introspectError, 0, false, fmt.Errorf("introspect %s: expires_at: %w", in.name, err)
		}
		t = t.UTC()
		k.ExpiresAt = &t
	}
	return k, introspectValid, 0, false, nil
}

// namespaced prefixes a provider-issued id with the provider name.
func (in *Introspector) namespaced(id string) string { return in.name + ":" + id }

// localTier maps the provider's tier name to a local one, falling back to
// config.TierFree for a tier the operator has not mapped.
func (in *Introspector) localTier(providerTier string) string {
	if local, ok := in.tiers[strings.ToLower(providerTier)]; ok {
		return local
	}
	in.throttledLog("introspection provider returned an unmapped tier; applying free",
		zap.String("provider_tier", providerTier))
	return config.TierFree
}

// retryAfter parses a Retry-After header (delay-seconds or an HTTP-date),
// capped at backoffMax; def when absent, unparseable or not in the future.
// shortWait reports whether a Retry-After header (absent, or at most
// introspectRetryWait) allows the single early retry.
func shortWait(h string, now time.Time) bool {
	return h == "" || retryAfter(h, introspectRetryWait+time.Nanosecond, now) <= introspectRetryWait
}

func retryAfter(h string, def time.Duration, now time.Time) time.Duration {
	if h == "" {
		return def
	}
	var d time.Duration
	if secs, err := strconv.ParseInt(h, 10, 64); err == nil {
		if secs <= 0 {
			return def
		}
		if secs >= int64(backoffMax/time.Second) {
			return backoffMax
		}
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(h); err == nil {
		d = t.Sub(now)
	}
	if d <= 0 {
		return def
	}
	return min(d, backoffMax)
}

// throttledLog logs msg at Error at most once per introspectWarnEvery,
// independently per message, so one noisy condition (a throttling provider)
// cannot hide another (a rejected token).
func (in *Introspector) throttledLog(msg string, fields ...zap.Field) {
	in.mu.Lock()
	now := in.now()
	if last, ok := in.warnedAt[msg]; ok && now.Sub(last) < introspectWarnEvery {
		in.mu.Unlock()
		return
	}
	in.warnedAt[msg] = now
	in.mu.Unlock()
	in.logger.Error(msg, fields...)
}
