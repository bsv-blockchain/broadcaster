package apikey

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/store"
)

const (
	testServiceToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	testProvider     = "acme"
	testPrefix       = "acme_"
	testLocalTier    = "pro"
)

// fakeProvider answers introspection calls with status and body, after
// checking the request carries the service token and a well-formed hash.
func fakeProvider(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+testServiceToken {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		var req introspectRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.KeyHash) != 64 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func introspectorFor(srv *httptest.Server, token string) *Introspector {
	return NewIntrospector(IntrospectorConfig{
		Name: testProvider, URL: srv.URL, Token: token, Timeout: time.Second,
		Tiers: map[string]string{"Gold": testLocalTier},
	}, zap.NewNop())
}

func TestIntrospectorValidKey(t *testing.T) {
	srv := fakeProvider(t, http.StatusOK, `{"valid":true,"key_id":"k-123","limit_id":"plan-9","tier":"GOLD",
		"unknown_field":["ignored"],"expires_at":"2027-01-31T00:00:00+02:00"}`)
	hash := Hash(testPrefix + "anything")

	k, err := introspectorFor(srv, testServiceToken).GetAPIKeyByHash(context.Background(), hash)
	require.NoError(t, err)
	require.Equal(t, "acme:k-123", k.ID, "ids are namespaced by provider")
	require.Equal(t, "acme:plan-9", k.LimitID)
	require.Equal(t, "acme:plan-9", k.QuotaID())
	require.Equal(t, hash, k.Hash)
	require.Equal(t, testLocalTier, k.Tier, "provider tiers map case-insensitively to local tiers")
	require.Equal(t, store.APIKeyActive, k.Status)
	require.NotNil(t, k.ExpiresAt)
	require.Equal(t, time.Date(2027, 1, 30, 22, 0, 0, 0, time.UTC), *k.ExpiresAt)
	require.Equal(t, time.UTC, k.ExpiresAt.Location())
}

func TestIntrospectorOptionalFields(t *testing.T) {
	for name, body := range map[string]string{
		"null expiry":  `{"valid":true,"key_id":"k","tier":"gold","expires_at":null}`,
		"empty expiry": `{"valid":true,"key_id":"k","tier":"gold","expires_at":""}`,
		"no expiry":    `{"valid":true,"key_id":"k","tier":"gold"}`,
	} {
		t.Run(name, func(t *testing.T) {
			k, err := introspectorFor(fakeProvider(t, http.StatusOK, body), testServiceToken).GetAPIKeyByHash(context.Background(), Hash("x"))
			require.NoError(t, err)
			require.Nil(t, k.ExpiresAt)
			require.Empty(t, k.LimitID)
			require.Equal(t, "acme:k", k.QuotaID(), "without a limit_id the key is its own allowance")
		})
	}
}

// TestIntrospectorUnmappedTierIsFree pins that a provider can only grant
// tiers the operator mapped: anything else, including a name that happens to
// match a local tier, is free.
func TestIntrospectorUnmappedTierIsFree(t *testing.T) {
	for _, tier := range []string{testLocalTier, "enterprise", "", "platinum"} {
		srv := fakeProvider(t, http.StatusOK, `{"valid":true,"key_id":"k","tier":"`+tier+`"}`)
		k, err := introspectorFor(srv, testServiceToken).GetAPIKeyByHash(context.Background(), Hash("x"))
		require.NoError(t, err)
		require.Equal(t, config.TierFree, k.Tier, "provider tier %q", tier)
	}
}

func TestIntrospectorFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		status   int
		body     string
		token    string
		notFound bool
		creds    bool
	}{
		"invalid key":              {http.StatusOK, `{"valid":false}`, testServiceToken, true, false},
		"wrong service token":      {http.StatusOK, `{"valid":true}`, "wrong", false, true},
		"forbidden":                {http.StatusForbidden, `{}`, testServiceToken, false, true},
		"server error":             {http.StatusServiceUnavailable, `{}`, testServiceToken, false, false},
		"valid without key_id":     {http.StatusOK, `{"valid":true,"tier":"gold"}`, testServiceToken, false, false},
		"undecodable":              {http.StatusOK, `not json`, testServiceToken, false, false},
		"expiry not RFC 3339":      {http.StatusOK, `{"valid":true,"key_id":"k","expires_at":"2027-01-31T00:00:00"}`, testServiceToken, false, false},
		"redirect is not followed": {http.StatusFound, ``, testServiceToken, false, false},
		"not found":                {http.StatusNotFound, ``, testServiceToken, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			srv := fakeProvider(t, tc.status, tc.body)
			k, err := introspectorFor(srv, tc.token).GetAPIKeyByHash(context.Background(), Hash("x"))
			require.Error(t, err)
			require.Nil(t, k)
			require.Equal(t, tc.notFound, errors.Is(err, store.ErrAPIKeyNotFound), err)
			require.Equal(t, tc.creds, errors.Is(err, ErrIntrospectionCredentials), err)
		})
	}
}

// TestIntrospectorBacksOffAfterFailures pins that a failing provider is not
// called again until its backoff passes, so an outage or a bad token cannot
// turn every uncached request into an outbound call.
func TestIntrospectorBacksOffAfterFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		status     int
		retryAfter string
		token      string
		body       string
		backoff    time.Duration
	}{
		"429 with Retry-After":        {http.StatusTooManyRequests, "7", testServiceToken, ``, 7 * time.Second},
		"429 Retry-After capped":      {http.StatusTooManyRequests, "3600", testServiceToken, ``, backoffMax},
		"429 Retry-After overflow":    {http.StatusTooManyRequests, "9999999999", testServiceToken, ``, backoffMax},
		"429 Retry-After as a date":   {http.StatusTooManyRequests, "Fri, 30 Oct 2026 12:00:09 GMT", testServiceToken, ``, 9 * time.Second},
		"429 Retry-After in the past": {http.StatusTooManyRequests, "Fri, 30 Oct 2026 11:00:00 GMT", testServiceToken, ``, backoffThrottled},
		"429 without header":          {http.StatusTooManyRequests, "", testServiceToken, ``, backoffThrottled},
		"503":                         {http.StatusServiceUnavailable, "", testServiceToken, ``, backoffTransient},
		"rejected token":              {http.StatusOK, "", "wrong", ``, backoffMisconfigured},
		"404 (wrong url)":             {http.StatusNotFound, "", testServiceToken, ``, backoffMisconfigured},
		"undecodable answer":          {http.StatusOK, "", testServiceToken, `nope`, backoffTransient},
		"invalid key":                 {http.StatusOK, "", testServiceToken, `{"valid":false}`, 0},
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("Authorization") != "Bearer "+testServiceToken {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)
			in := introspectorFor(srv, tc.token)
			clk := &fakeClock{t: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
			in.now = clk.now

			_, err := in.GetAPIKeyByHash(context.Background(), Hash("x"))
			require.Error(t, err)
			require.EqualValues(t, 1, calls.Load())

			_, err = in.GetAPIKeyByHash(context.Background(), Hash("y"))
			require.Error(t, err)
			if tc.backoff == 0 {
				require.EqualValues(t, 2, calls.Load(), "no backoff: the next key is looked up")
				return
			}
			require.ErrorIs(t, err, errIntrospectionBackoff)
			require.NotErrorIs(t, err, store.ErrAPIKeyNotFound, "backing off is unavailable, never unknown")
			require.EqualValues(t, 1, calls.Load(), "backing off: the provider is not called")

			clk.advance(tc.backoff - time.Millisecond)
			_, err = in.GetAPIKeyByHash(context.Background(), Hash("y"))
			require.ErrorIs(t, err, errIntrospectionBackoff)
			clk.advance(time.Millisecond)
			_, _ = in.GetAPIKeyByHash(context.Background(), Hash("y"))
			require.EqualValues(t, 2, calls.Load(), "called again once the backoff has passed")
		})
	}
}

// TestIntrospectorBackoffOnlyExtends pins that a later failure with a short
// backoff (a concurrent call timing out) does not cut short a longer one
// already in force (a provider's Retry-After).
func TestIntrospectorBackoffOnlyExtends(t *testing.T) {
	in := NewIntrospector(IntrospectorConfig{Name: testProvider, URL: "http://127.0.0.1:1", Timeout: time.Second}, zap.NewNop())
	clk := &fakeClock{t: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	in.now = clk.now

	in.extendBackoff(backoffMax)
	in.extendBackoff(backoffTransient)
	require.Equal(t, clk.now().Add(backoffMax), in.openUntil)

	clk.advance(backoffMax)
	in.extendBackoff(backoffTransient)
	require.Equal(t, clk.now().Add(backoffTransient), in.openUntil, "an expired backoff is replaced")
}

// TestResolverRoutesByPrefix pins that keys go to the lookup their prefix
// names, that an unrouted prefix never reaches any lookup, and that a
// provider failure is "unavailable", never "unauthorized".
func TestResolverRoutesByPrefix(t *testing.T) {
	localRaw, err := Generate()
	require.NoError(t, err)
	remoteRaw := generateWithPrefix(t, testPrefix)

	expired := time.Date(2026, 10, 21, 11, 0, 0, 0, time.UTC)
	local := &fakeLookup{keys: map[string]*store.APIKey{Hash(localRaw): {ID: "key_l", Tier: "free", Status: store.APIKeyActive}}}
	remote := &fakeLookup{keys: map[string]*store.APIKey{Hash(remoteRaw): {ID: "acme:1", LimitID: "acme:plan", Tier: testLocalTier, Status: store.APIKeyActive}}}
	clk := &fakeClock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	r := NewResolver(local, 30*time.Second, 10*time.Minute, WithRoutes(Route{Prefix: testPrefix, Lookup: remote}))
	r.now = clk.now
	ctx := context.Background()

	k, err := r.Resolve(ctx, localRaw)
	require.NoError(t, err)
	require.Equal(t, "key_l", k.ID)
	k, err = r.Resolve(ctx, remoteRaw)
	require.NoError(t, err)
	require.Equal(t, "acme:1", k.ID)
	require.Equal(t, "acme:plan", k.QuotaID())
	require.Equal(t, 1, local.n())
	require.Equal(t, 1, remote.n())

	_, err = r.Resolve(ctx, generateWithPrefix(t, "other_"))
	require.ErrorIs(t, err, ErrUnauthorized, "a prefix no route claims")
	badSum := []byte(remoteRaw)
	if badSum[len(badSum)-1] == 'a' {
		badSum[len(badSum)-1] = 'b'
	} else {
		badSum[len(badSum)-1] = 'a'
	}
	_, err = r.Resolve(ctx, string(badSum))
	require.ErrorIs(t, err, ErrUnauthorized, "a bad checksum under a provider prefix")
	require.Equal(t, 1, local.n())
	require.Equal(t, 1, remote.n(), "malformed and unrouted keys never reach a lookup")

	// expires_at passing must take effect even while the cached answer is
	// still fresh.
	remote.set(func(f *fakeLookup) { f.keys[Hash(remoteRaw)].ExpiresAt = &expired })
	clk.advance(31 * time.Second)
	_, err = r.Resolve(ctx, remoteRaw)
	require.ErrorIs(t, err, ErrUnauthorized)

	// A rejected service token on an uncached key is unavailable (503), not
	// a 401 that would be cached as "no such key".
	other := generateWithPrefix(t, testPrefix)
	remote.set(func(f *fakeLookup) { f.err = ErrIntrospectionCredentials })
	_, err = r.Resolve(ctx, other)
	require.ErrorIs(t, err, ErrUnavailable)
	remote.set(func(f *fakeLookup) {
		f.err = nil
		f.keys[Hash(other)] = &store.APIKey{ID: "acme:2", Tier: testLocalTier, Status: store.APIKeyActive}
	})
	k, err = r.Resolve(ctx, other)
	require.NoError(t, err, "a failed introspection is never negatively cached")
	require.Equal(t, "acme:2", k.ID)
}

func generateWithPrefix(t *testing.T, prefix string) string {
	t.Helper()
	raw, err := Generate()
	require.NoError(t, err)
	return prefix + raw[len(Prefix):]
}
