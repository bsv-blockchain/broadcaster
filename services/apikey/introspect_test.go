package apikey

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/store"
)

const testServiceToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

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
	return NewIntrospector("woc", srv.URL, token, time.Second, zap.NewNop())
}

func TestIntrospectorValidKey(t *testing.T) {
	srv := fakeProvider(t, http.StatusOK, `{"valid":true,"key_id":"c1566bac","limit_id":"lim_1","tier":"standard",
		"scopes":["broadcaster"],"rate_limit_per_sec":20,"rate_limit_per_day":100000,"expires_at":"2027-01-31T00:00:00+02:00"}`)
	hash := Hash("bsv_anything")

	k, err := introspectorFor(srv, testServiceToken).GetAPIKeyByHash(context.Background(), hash)
	require.NoError(t, err)
	require.Equal(t, "c1566bac", k.ID)
	require.Equal(t, hash, k.Hash)
	require.Equal(t, "standard", k.Tier)
	require.Equal(t, "lim_1", k.LimitID)
	require.Equal(t, "lim_1", k.QuotaID())
	require.Equal(t, store.APIKeyActive, k.Status)
	require.NotNil(t, k.ExpiresAt)
	require.Equal(t, time.Date(2027, 1, 30, 22, 0, 0, 0, time.UTC), *k.ExpiresAt)
	require.Equal(t, time.UTC, k.ExpiresAt.Location())
}

func TestIntrospectorNoExpiry(t *testing.T) {
	srv := fakeProvider(t, http.StatusOK, `{"valid":true,"key_id":"k","tier":"standard","expires_at":null}`)
	k, err := introspectorFor(srv, testServiceToken).GetAPIKeyByHash(context.Background(), Hash("x"))
	require.NoError(t, err)
	require.Nil(t, k.ExpiresAt)
	require.Equal(t, "k", k.QuotaID(), "without a limit_id the key is its own allowance")
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
		"valid without key_id":     {http.StatusOK, `{"valid":true,"tier":"standard"}`, testServiceToken, false, false},
		"undecodable":              {http.StatusOK, `not json`, testServiceToken, false, false},
		"redirect is not followed": {http.StatusFound, ``, testServiceToken, false, false},
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

// TestResolverRoutesByPrefix pins that keys go to the lookup their prefix
// names, that an unrouted prefix never reaches any lookup, and that a
// provider failure is "unavailable", never "unauthorized".
func TestResolverRoutesByPrefix(t *testing.T) {
	localRaw, err := Generate()
	require.NoError(t, err)
	remoteRaw := generateWithPrefix(t, "bsv_")

	expired := time.Date(2026, 10, 21, 11, 0, 0, 0, time.UTC)
	local := &fakeLookup{keys: map[string]*store.APIKey{Hash(localRaw): {ID: "key_l", Tier: "free", Status: store.APIKeyActive}}}
	remote := &fakeLookup{keys: map[string]*store.APIKey{Hash(remoteRaw): {ID: "woc_1", LimitID: "lim_1", Tier: "standard", Status: store.APIKeyActive}}}
	clk := &fakeClock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	r := NewResolver(local, 30*time.Second, 10*time.Minute, WithRoutes(Route{Prefix: "bsv_", Lookup: remote}))
	r.now = clk.now
	ctx := context.Background()

	k, err := r.Resolve(ctx, localRaw)
	require.NoError(t, err)
	require.Equal(t, "key_l", k.ID)
	k, err = r.Resolve(ctx, remoteRaw)
	require.NoError(t, err)
	require.Equal(t, "woc_1", k.ID)
	require.Equal(t, "lim_1", k.QuotaID())
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
	other := generateWithPrefix(t, "bsv_")
	remote.set(func(f *fakeLookup) { f.err = ErrIntrospectionCredentials })
	_, err = r.Resolve(ctx, other)
	require.ErrorIs(t, err, ErrUnavailable)
	remote.set(func(f *fakeLookup) {
		f.err = nil
		f.keys[Hash(other)] = &store.APIKey{ID: "woc_2", Tier: "standard", Status: store.APIKeyActive}
	})
	k, err = r.Resolve(ctx, other)
	require.NoError(t, err, "a failed introspection is never negatively cached")
	require.Equal(t, "woc_2", k.ID)
}

func generateWithPrefix(t *testing.T, prefix string) string {
	t.Helper()
	raw, err := Generate()
	require.NoError(t, err)
	return prefix + raw[len(Prefix):]
}
