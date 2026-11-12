package usage

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/services/apikey"
)

const (
	testSource   = "acme"
	testEndpoint = "v1/tx"
)

func TestClassify(t *testing.T) {
	for status, want := range map[int]string{
		200: Class2xx, 202: Class2xx, 304: Class2xx,
		400: Class4xx, 401: Class4xx, 413: Class4xx, 499: Class4xx,
		429: Class429,
		500: Class5xx, 503: Class5xx,
	} {
		require.Equal(t, want, Classify(status), "status %d", status)
	}
}

func TestValidEndpoint(t *testing.T) {
	for _, ok := range []string{testEndpoint, "v1/txs", "v1/tx/status", "v1/blocks/status", "policy"} {
		require.True(t, ValidEndpoint(ok), ok)
	}
	for _, bad := range []string{
		"", "/v1/tx", "v1/tx/", "v1/tx/:txid", "v1/a/b/c", "V1/tx", "v1/tx?x=1",
		"v1/" + string(make([]byte, 70)),
	} {
		require.False(t, ValidEndpoint(bad), bad)
	}
}

func TestDayOfIsUTC(t *testing.T) {
	ny := time.FixedZone("NY", -5*3600)
	require.Equal(t, "2026-11-13", DayOf(time.Date(2026, 11, 12, 21, 0, 0, 0, ny)))
}

func TestAggregatorAddDrain(t *testing.T) {
	a := NewAggregator(10)
	b1 := Bucket{Source: testSource, KeyID: "k1", Endpoint: testEndpoint, Class: Class2xx, Day: "2026-11-12"}
	b2 := b1
	b2.Class = Class429

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() { defer wg.Done(); a.Add(b1, 10) }()
	}
	wg.Wait()
	a.Add(b2, 0)

	got := map[Bucket]Totals{}
	for _, e := range a.Drain() {
		got[e.Bucket] = e.Totals
	}
	require.Equal(t, map[Bucket]Totals{b1: {Count: 100, Units: 1000}, b2: {Count: 1, Units: 0}}, got)
	require.Empty(t, a.Drain(), "drain starts afresh")
}

func TestAggregatorOverflow(t *testing.T) {
	a := NewAggregator(1)
	b1 := Bucket{Source: testSource, KeyID: "k1", Endpoint: testEndpoint, Class: Class2xx, Day: "d"}
	b2 := Bucket{Source: testSource, KeyID: "k2", Endpoint: testEndpoint, Class: Class2xx, Day: "d"}
	a.Add(b1, 1)
	a.Add(b2, 1) // a new bucket past the limit is dropped
	a.Add(b1, 1) // an existing one still counts
	select {
	case <-a.Full():
	default:
		t.Fatal("a full aggregator must signal an early flush")
	}
	entries := a.Drain()
	require.Len(t, entries, 1)
	require.Equal(t, Totals{Count: 2, Units: 2}, entries[0].Totals)
}

// TestRecorder pins what is counted: only keys of a provider with a sink,
// under that provider's own key id; units are what the request was charged
// (base, plus submit cost per batch transaction, 0 for a 429); the day is
// when the request finished.
func TestRecorder(t *testing.T) {
	gin.SetMode(gin.TestMode)
	provider := apikey.Principal{KeyID: "acme:k1", Tier: "pro", Provider: testSource, ProviderKeyID: "k1"}
	for _, tc := range []struct {
		name    string
		p       *apikey.Principal
		status  int
		txs     int
		counted bool
		class   string
		units   int64
	}{
		{"provider key 2xx", &provider, 200, 0, true, Class2xx, 1},
		{"provider key batch", &provider, 202, 3, true, Class2xx, 1 + 3*10},
		{"provider key limited", &provider, 429, 0, true, Class429, 0},
		{"provider key client error", &provider, 400, 0, true, Class4xx, 1},
		{"local key", &apikey.Principal{KeyID: "key_1", Tier: "pro"}, 200, 0, false, "", 0},
		{"provider without a sink", &apikey.Principal{KeyID: "other:k", Provider: "other", ProviderKeyID: "k"}, 200, 0, false, "", 0},
		{"anonymous", nil, 200, 0, false, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agg := NewAggregator(100)
			rec := NewRecorder(agg, []string{testSource}, 10)
			finished := time.Date(2026, 11, 12, 23, 59, 59, 0, time.UTC)
			rec.now = func() time.Time { return finished }
			e := gin.New()
			e.GET("/x", rec.Middleware("v1/thing", 1), func(c *gin.Context) {
				if tc.p != nil {
					apikey.WithPrincipal(c, *tc.p)
				} else {
					apikey.WithPrincipal(c, apikey.Principal{Tier: config.TierAnonymous})
				}
				if tc.txs > 0 {
					SetTxCount(c, tc.txs)
				}
				c.Status(tc.status)
			})
			w := httptest.NewRecorder()
			e.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))

			entries := agg.Drain()
			if !tc.counted {
				require.Empty(t, entries)
				return
			}
			require.Equal(t, []Entry{{
				Bucket: Bucket{Source: testSource, KeyID: "k1", Endpoint: "v1/thing", Class: tc.class, Day: "2026-11-12"},
				Totals: Totals{Count: 1, Units: tc.units},
			}}, entries)
		})
	}
}
