package chaintracks_server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/block"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/gin-gonic/gin"
)

// fakeChaintracks implements the chaintracks.Chaintracks interface with
// canned responses and a tip channel the test drives directly. Just
// enough to exercise the Gin adapter — we don't pretend to run a real
// P2P subscription.
type fakeChaintracks struct {
	network string
	height  uint32
	tip     *chaintracks.BlockHeader
	headers map[uint32]*chaintracks.BlockHeader
	// heightErr fails GetHeaderByHeight at those heights with the given
	// error — a lookup failure, as opposed to a header that does not exist.
	heightErr map[uint32]error
	tipCh     chan *chaintracks.BlockHeader
	reorgCh   chan *chaintracks.ReorgEvent
}

func newFakeChaintracks() *fakeChaintracks {
	return &fakeChaintracks{
		network: "mainnet",
		height:  100,
		headers: make(map[uint32]*chaintracks.BlockHeader),
		tipCh:   make(chan *chaintracks.BlockHeader, 4),
		reorgCh: make(chan *chaintracks.ReorgEvent, 4),
	}
}

func (f *fakeChaintracks) IsValidRootForHeight(_ context.Context, _ *chainhash.Hash, _ uint32) (bool, error) {
	return true, nil
}
func (f *fakeChaintracks) CurrentHeight(context.Context) (uint32, error) { return f.height, nil }
func (f *fakeChaintracks) GetNetwork(context.Context) (string, error)    { return f.network, nil }
func (f *fakeChaintracks) GetHeight(context.Context) uint32              { return f.height }
func (f *fakeChaintracks) GetTip(context.Context) *chaintracks.BlockHeader {
	return f.tip
}

func (f *fakeChaintracks) GetHeaderByHeight(_ context.Context, h uint32) (*chaintracks.BlockHeader, error) {
	if err, ok := f.heightErr[h]; ok {
		return nil, err
	}
	if hdr, ok := f.headers[h]; ok {
		return hdr, nil
	}
	return nil, chaintracks.ErrHeaderNotFound
}

func (f *fakeChaintracks) GetHeaderByHash(context.Context, *chainhash.Hash) (*chaintracks.BlockHeader, error) {
	return nil, chaintracks.ErrHeaderNotFound
}

func (f *fakeChaintracks) GetHeaders(context.Context, uint32, uint32) ([]*chaintracks.BlockHeader, error) {
	return nil, nil
}

func (f *fakeChaintracks) Subscribe(context.Context) <-chan *chaintracks.BlockHeader {
	return f.tipCh
}
func (f *fakeChaintracks) Unsubscribe(<-chan *chaintracks.BlockHeader) {}
func (f *fakeChaintracks) SubscribeReorg(context.Context) <-chan *chaintracks.ReorgEvent {
	return f.reorgCh
}
func (f *fakeChaintracks) UnsubscribeReorg(<-chan *chaintracks.ReorgEvent) {}

func fixtureTip() *chaintracks.BlockHeader {
	var prev, merkle chainhash.Hash
	for i := range prev {
		prev[i] = byte(i)
		merkle[i] = byte(255 - i)
	}
	h := &block.Header{
		Version:    1,
		PrevHash:   prev,
		MerkleRoot: merkle,
		Timestamp:  1234567890,
		Bits:       0x1d00ffff,
		Nonce:      42,
	}
	return &chaintracks.BlockHeader{
		Header: h,
		Height: 100,
		Hash:   h.Hash(),
	}
}

func setupChaintracksRouter(t *testing.T, ct chaintracks.Chaintracks) (*gin.Engine, *Routes) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routes := NewRoutes(ctx, ct)
	routes.Register(r.Group("/chaintracks/v2"))
	routes.RegisterLegacy(r.Group("/chaintracks/v1"))
	return r, routes
}

func TestChaintracksRoutes_MountsRoutes(t *testing.T) {
	f := newFakeChaintracks()
	r, _ := setupChaintracksRouter(t, f)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/chaintracks/v2/network", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("network: got %d, want 200", w.Code)
	}
	var netResp struct {
		Network string `json:"network"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &netResp); err != nil {
		t.Fatalf("decode network: %v", err)
	}
	if netResp.Network != "mainnet" {
		t.Errorf("network=%q, want mainnet", netResp.Network)
	}

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/chaintracks/v2/height", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("height: got %d, want 200", w.Code)
	}
	var heightResp struct {
		Height uint32 `json:"height"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &heightResp); err != nil {
		t.Fatalf("decode height: %v", err)
	}
	if heightResp.Height != 100 {
		t.Errorf("height=%d, want 100", heightResp.Height)
	}
}

func TestChaintracksRoutes_BinaryHeader_Returns80Bytes(t *testing.T) {
	f := newFakeChaintracks()
	tip := fixtureTip()
	f.tip = tip
	f.headers[tip.Height] = tip
	r, _ := setupChaintracksRouter(t, f)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/chaintracks/v2/tip.bin", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("tip.bin status=%d, want 200", w.Code)
	}
	if n := w.Body.Len(); n != 80 {
		t.Errorf("tip.bin body length=%d, want 80", n)
	}
	if got := w.Header().Get("X-Block-Height"); got != "100" {
		t.Errorf("X-Block-Height=%q, want \"100\"", got)
	}
	if got := w.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type=%q, want application/octet-stream", got)
	}
}

func TestChaintracksRoutes_TipStream_ForwardsUpdates(t *testing.T) {
	f := newFakeChaintracks()
	f.tip = fixtureTip()

	gin.SetMode(gin.TestMode)
	r := gin.New()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routes := NewRoutes(ctx, f)
	routes.Register(r.Group("/chaintracks/v2"))
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	client := &http.Client{Timeout: 3 * time.Second}
	reqCtx, reqCancel := context.WithCancel(context.Background())
	t.Cleanup(reqCancel)
	req, _ := http.NewRequestWithContext(reqCtx, "GET", srv.URL+"/chaintracks/v2/tip/stream", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status=%d, want 200", resp.StatusCode)
	}

	buf := make([]byte, 512)
	n, err := resp.Body.Read(buf)
	if err != nil && n == 0 {
		t.Fatalf("read initial tip: err=%v n=%d", err, n)
	}
	if n < len("data: ") || string(buf[:6]) != "data: " {
		t.Errorf("expected SSE 'data: ' prefix, got %q", string(buf[:min(n, 32)]))
	}

	nextTip := fixtureTip()
	nextTip.Height = 101
	f.tipCh <- nextTip

	time.Sleep(100 * time.Millisecond)
	n2, _ := resp.Body.Read(buf)
	if n2 == 0 {
		t.Fatal("no second tip frame received")
	}

	reqCancel()
}

// TestChaintracksRoutes_HeaderByHeight_404OnlyWhenNotFound: remote readers
// (bump-builder's reconciler in microservice mode) read a 404 as "no header
// at this height" — the end of an upward walk — so only a header that does
// not exist may answer 404. Any other lookup failure must be a 5xx the
// reader retries, not absence it acts on.
func TestChaintracksRoutes_HeaderByHeight_404OnlyWhenNotFound(t *testing.T) {
	f := newFakeChaintracks()
	f.heightErr = map[uint32]error{7: errors.New("storage unavailable")}
	r, _ := setupChaintracksRouter(t, f)

	for _, tc := range []struct {
		path string
		want int
	}{
		{"/chaintracks/v2/header/height/6", http.StatusNotFound},
		{"/chaintracks/v2/header/height/7", http.StatusInternalServerError},
		{"/chaintracks/v2/header/height/6.bin", http.StatusNotFound},
		{"/chaintracks/v2/header/height/7.bin", http.StatusInternalServerError},
		{"/chaintracks/v1/findHeaderHexForHeight?height=6", http.StatusNotFound},
		{"/chaintracks/v1/findHeaderHexForHeight?height=7", http.StatusInternalServerError},
	} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tc.path, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != tc.want {
			t.Errorf("%s: status=%d, want %d", tc.path, w.Code, tc.want)
		}
	}
}
