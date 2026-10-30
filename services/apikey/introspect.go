package apikey

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/store"
)

const (
	introspectMaxBody   = 64 << 10
	introspectWarnEvery = 10 * time.Second
)

// Results for metrics.APIKeyIntrospectTotal.
const (
	introspectValid        = "valid"
	introspectInvalid      = "invalid"
	introspectUnauthorized = "unauthorized"
	introspectError        = "error"
)

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
type Introspector struct {
	name   string
	url    string
	token  string
	client *http.Client
	logger *zap.Logger

	warnMu   sync.Mutex
	warnedAt time.Time
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
	k, result, err := in.introspect(ctx, hash)
	metrics.APIKeyIntrospectTotal.WithLabelValues(in.name, result).Inc()
	return k, err
}

func (in *Introspector) introspect(ctx context.Context, hash string) (*store.APIKey, string, error) {
	body, err := json.Marshal(introspectRequest{KeyHash: hash})
	if err != nil {
		return nil, introspectError, fmt.Errorf("introspect %s: encode: %w", in.name, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, in.url, bytes.NewReader(body))
	if err != nil {
		return nil, introspectError, fmt.Errorf("introspect %s: %w", in.name, err)
	}
	req.Header.Set("Authorization", "Bearer "+in.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := in.client.Do(req)
	if err != nil {
		return nil, introspectError, fmt.Errorf("introspect %s: %w", in.name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, introspectMaxBody))
	if err != nil {
		return nil, introspectError, fmt.Errorf("introspect %s: read body: %w", in.name, err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		in.throttledError("introspection provider rejected the service token; check auth.introspection token",
			zap.Int("http_status", resp.StatusCode))
		return nil, introspectUnauthorized, fmt.Errorf("%w: %s answered %d", ErrIntrospectionCredentials, in.name, resp.StatusCode)
	default:
		return nil, introspectError, fmt.Errorf("introspect %s: unexpected status %d", in.name, resp.StatusCode)
	}

	var r introspectResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, introspectError, fmt.Errorf("introspect %s: decode: %w", in.name, err)
	}
	if !r.Valid {
		return nil, introspectInvalid, store.ErrAPIKeyNotFound
	}
	if r.KeyID == "" {
		return nil, introspectError, fmt.Errorf("introspect %s: valid answer without key_id", in.name)
	}
	k := &store.APIKey{
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
	return k, introspectValid, nil
}

// throttledError logs at Error at most once per introspectWarnEvery: a bad
// service token fails every lookup until someone fixes the config.
func (in *Introspector) throttledError(msg string, fields ...zap.Field) {
	in.warnMu.Lock()
	now := time.Now()
	if now.Sub(in.warnedAt) < introspectWarnEvery {
		in.warnMu.Unlock()
		return
	}
	in.warnedAt = now
	in.warnMu.Unlock()
	in.logger.Error(msg, fields...)
}
