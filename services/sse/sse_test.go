package sse

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

const tokA = "tok-A"

// fakePublisher is a test-only events.Publisher. It captures published
// updates and lets tests synthesize subscriber-side delivery.
type fakePublisher struct {
	mu          sync.Mutex
	published   []*models.TransactionStatus
	subscribers []chan *models.TransactionStatus
}

func (p *fakePublisher) Publish(_ context.Context, status *models.TransactionStatus) error {
	p.mu.Lock()
	subs := append([]chan *models.TransactionStatus(nil), p.subscribers...)
	p.published = append(p.published, status)
	p.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- status:
		default:
		}
	}
	return nil
}

func (p *fakePublisher) PublishBulk(ctx context.Context, template *models.TransactionStatus) error {
	return p.Publish(ctx, template)
}

func (p *fakePublisher) Subscribe(_ context.Context, _ string) (<-chan *models.TransactionStatus, error) {
	ch := make(chan *models.TransactionStatus, 64)
	p.mu.Lock()
	p.subscribers = append(p.subscribers, ch)
	p.mu.Unlock()
	return ch, nil
}

func (p *fakePublisher) Close() error { return nil }

// sseStoreStub is a minimum-surface fake. The SSE service only touches
// GetSubmissionsByToken, GetStatus, TokensForTxIDs and IterateStatusesByToken
// on the Store; embed the interface so every other method panics if
// accidentally called.
type sseStoreStub struct {
	store.Store

	mu          sync.RWMutex
	subsByToken map[string][]*models.Submission
	statusByTx  map[string]*models.TransactionStatus

	// iterErr, when set, is returned by IterateStatusesByToken instead of
	// streaming — models a backend refusing a replay it cannot bound.
	iterErr error

	// tokenCalls counts TokensForTxIDs round-trips and tokenTxIDs the txids
	// they carried — the fan-out's store cost, which is what
	// TestFanOutResolvesTokensOncePerEvent asserts on.
	tokenCalls int
	tokenTxIDs int
}

func (s *sseStoreStub) GetSubmissionsByToken(_ context.Context, token string) ([]*models.Submission, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.subsByToken[token], nil
}

func (s *sseStoreStub) GetStatus(_ context.Context, txid string) (*models.TransactionStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.statusByTx[txid], nil
}

// EnrichMerklePath is a no-op for this stub: these tests don't exercise the
// compound-BUMP extraction (the dedicated pebble-backed e2e tests do). It must
// still exist so fan-out/catchup enrichment doesn't dereference the nil
// embedded store.Store.
func (s *sseStoreStub) EnrichMerklePath(_ context.Context, _ *models.TransactionStatus) {}

// IterateStatusesByToken mirrors the real backends: distinct txids under
// the token, current status projection, since/only filters, ascending
// timestamp order.
func (s *sseStoreStub) IterateStatusesByToken(_ context.Context, token string, since time.Time, onlyStatuses []models.Status, fn func(*models.TransactionStatus) error) error {
	if s.iterErr != nil {
		return s.iterErr
	}
	s.mu.RLock()
	seen := make(map[string]bool)
	var statuses []*models.TransactionStatus
	for _, sub := range s.subsByToken[token] {
		if seen[sub.TxID] {
			continue
		}
		seen[sub.TxID] = true
		st := s.statusByTx[sub.TxID]
		if st == nil {
			continue
		}
		if !since.IsZero() && !st.Timestamp.After(since) {
			continue
		}
		if len(onlyStatuses) > 0 && !slices.Contains(onlyStatuses, st.Status) {
			continue
		}
		statuses = append(statuses, st)
	}
	s.mu.RUnlock()
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].Timestamp.Before(statuses[j].Timestamp) })
	for _, st := range statuses {
		if err := fn(st); err != nil {
			return err
		}
	}
	return nil
}

// TokensForTxIDs mirrors the real backends' membership resolution and records
// what fan-out asked of the store.
func (s *sseStoreStub) TokensForTxIDs(_ context.Context, txids []string) (map[string][]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokenCalls++
	s.tokenTxIDs += len(txids)

	want := make(map[string]struct{}, len(txids))
	for _, txid := range txids {
		want[txid] = struct{}{}
	}
	out := make(map[string][]string)
	for token, subs := range s.subsByToken {
		for _, sub := range subs {
			if _, ok := want[sub.TxID]; !ok {
				continue
			}
			if !slices.Contains(out[sub.TxID], token) {
				out[sub.TxID] = append(out[sub.TxID], token)
			}
		}
	}
	return out, nil
}

// storeCounters snapshots the fan-out's store usage.
func (s *sseStoreStub) storeCounters() (calls, txids int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tokenCalls, s.tokenTxIDs
}

func (s *sseStoreStub) setStatus(txid string, status *models.TransactionStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusByTx[txid] = status
}

// setupSSEService wires a Service backed by a fakePublisher and the
// supplied store stub. Returns the service, a gin router with /events
// registered, the publisher, and a cancel that releases the manager
// goroutine.
func setupSSEService(t *testing.T, st *sseStoreStub) (*Service, *gin.Engine, *fakePublisher, context.CancelFunc) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	pub := &fakePublisher{}
	svc := &Service{
		cfg:       &config.Config{SSE: config.SSEConfig{Enabled: true}},
		logger:    zap.NewNop(),
		store:     st,
		publisher: pub,
	}
	ctx, cancel := context.WithCancel(t.Context())
	mgr, err := newManager(ctx, pub, st, zap.NewNop())
	if err != nil {
		cancel()
		t.Fatalf("newManager: %v", err)
	}
	svc.manager = mgr
	router := gin.New()
	router.GET("/events", svc.handleEvents)
	return svc, router, pub, cancel
}

func TestSSEFrameFormat(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{
			tokA: {{TxID: "abc", CallbackToken: tokA}},
		},
	}
	_, router, pub, cancel := setupSSEService(t, st)
	defer cancel()

	srv := httptest.NewServer(router)
	defer srv.Close()

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/events?callbackToken="+tokA, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", got)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = pub.Publish(t.Context(), &models.TransactionStatus{
			TxID:      "abc",
			Status:    models.StatusMined,
			Timestamp: time.Unix(0, 1700000000000000000).UTC(),
		})
	}()

	frame, err := readNextSSEFrame(resp.Body, 2*time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}

	wantPrefix := "id: 1700000000000000000\nevent: status\ndata: "
	if !strings.HasPrefix(frame, wantPrefix) {
		t.Errorf("frame prefix mismatch:\n got %q\nwant %q...", frame, wantPrefix)
	}
	dataLine := strings.TrimPrefix(frame, wantPrefix)
	dataLine = strings.TrimSuffix(dataLine, "\n\n")
	var payload map[string]string
	if err := json.Unmarshal([]byte(dataLine), &payload); err != nil {
		t.Fatalf("unmarshal data: %v (raw=%q)", err, dataLine)
	}
	if payload["txid"] != "abc" || payload["txStatus"] != string(models.StatusMined) {
		t.Errorf("payload = %+v", payload)
	}
	if _, err := time.Parse(time.RFC3339, payload["timestamp"]); err != nil {
		t.Errorf("timestamp not RFC3339: %q", payload["timestamp"])
	}
}

func TestSSETokenFilter(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{
			tokA: {{TxID: "match", CallbackToken: tokA}},
		},
	}
	_, router, pub, cancel := setupSSEService(t, st)
	defer cancel()

	srv := httptest.NewServer(router)
	defer srv.Close()

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/events?callbackToken="+tokA, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = pub.Publish(t.Context(), &models.TransactionStatus{TxID: "other", Status: models.StatusMined, Timestamp: time.Now()})
		_ = pub.Publish(t.Context(), &models.TransactionStatus{TxID: "match", Status: models.StatusMined, Timestamp: time.Now()})
	}()

	frame, err := readNextSSEFrame(resp.Body, 2*time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if !strings.Contains(frame, `"txid":"match"`) {
		t.Errorf("expected match-tx frame, got %q", frame)
	}
}

func TestSSECatchup(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	older := now.Add(-time.Hour)
	newer := now

	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{
			tokA: {
				{TxID: "old", CallbackToken: tokA},
				{TxID: "new", CallbackToken: tokA},
			},
		},
		statusByTx: map[string]*models.TransactionStatus{
			"old": {TxID: "old", Status: models.StatusMined, Timestamp: older},
			"new": {TxID: "new", Status: models.StatusMined, Timestamp: newer},
		},
	}
	_, router, _, cancel := setupSSEService(t, st)
	defer cancel()

	srv := httptest.NewServer(router)
	defer srv.Close()

	since := older.Add(time.Minute).UnixNano() // strictly between older and newer
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/events?callbackToken="+tokA, nil)
	req.Header.Set("Last-Event-ID", fmt.Sprintf("%d", since))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	frame, err := readNextSSEFrame(resp.Body, 2*time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if !strings.Contains(frame, `"txid":"new"`) {
		t.Errorf("expected catchup of newer tx, got %q", frame)
	}
	if strings.Contains(frame, `"txid":"old"`) {
		t.Errorf("older tx should not have replayed: %q", frame)
	}
}

// TestSSECatchupFreshConnectNonTerminalOnly verifies the OOM fix: a
// connect WITHOUT Last-Event-ID replays only non-terminal statuses.
// Terminal history (MINED/REJECTED/...) is queryable via the REST API
// and replaying it wholesale is what used to OOM the pod.
func TestSSECatchupFreshConnectNonTerminalOnly(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{
			tokA: {
				{TxID: "mined", CallbackToken: tokA},
				{TxID: "rejected", CallbackToken: tokA},
				{TxID: "pending", CallbackToken: tokA},
			},
		},
		statusByTx: map[string]*models.TransactionStatus{
			"mined":    {TxID: "mined", Status: models.StatusMined, Timestamp: now.Add(-2 * time.Minute)},
			"rejected": {TxID: "rejected", Status: models.StatusRejected, Timestamp: now.Add(-time.Minute)},
			"pending":  {TxID: "pending", Status: models.StatusSeenOnNetwork, Timestamp: now},
		},
	}
	_, router, _, cancel := setupSSEService(t, st)
	defer cancel()

	srv := httptest.NewServer(router)
	defer srv.Close()

	// Fresh connect: no Last-Event-ID header.
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/events?callbackToken="+tokA, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	frame, err := readNextSSEFrame(resp.Body, 2*time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if !strings.Contains(frame, `"txid":"pending"`) {
		t.Errorf("expected non-terminal tx in catchup, got %q", frame)
	}
	if strings.Contains(frame, `"txid":"mined"`) || strings.Contains(frame, `"txid":"rejected"`) {
		t.Errorf("terminal statuses must not replay on fresh connect: %q", frame)
	}
}

// TestSSECatchupReconnectIncludesTerminal verifies the Last-Event-ID
// contract survives the OOM fix: a reconnect with a since watermark
// still replays terminal transitions that happened during the
// disconnect window (that's how a client learns its tx mined).
func TestSSECatchupReconnectIncludesTerminal(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{
			tokA: {{TxID: "minedRecently", CallbackToken: tokA}},
		},
		statusByTx: map[string]*models.TransactionStatus{
			"minedRecently": {TxID: "minedRecently", Status: models.StatusMined, Timestamp: now},
		},
	}
	_, router, _, cancel := setupSSEService(t, st)
	defer cancel()

	srv := httptest.NewServer(router)
	defer srv.Close()

	since := now.Add(-time.Minute).UnixNano()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/events?callbackToken="+tokA, nil)
	req.Header.Set("Last-Event-ID", fmt.Sprintf("%d", since))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	frame, err := readNextSSEFrame(resp.Body, 2*time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if !strings.Contains(frame, `"txid":"minedRecently"`) {
		t.Errorf("expected terminal tx in reconnect catchup, got %q", frame)
	}
}

// nopFlusher satisfies the sseWriter flusher contract without a live
// HTTP connection so the cap test doesn't stream 10k frames through
// httptest.
type nopFlusher struct{ http.ResponseWriter }

func (nopFlusher) Flush() {}

// TestSSECatchupFrameCap verifies the backstop: catchup stops at
// catchupMaxFrames instead of replaying an unbounded history.
func TestSSECatchupFrameCap(t *testing.T) {
	n := catchupMaxFrames + 500
	subs := make([]*models.Submission, 0, n)
	statuses := make(map[string]*models.TransactionStatus, n)
	base := time.Now().UTC().Add(-time.Duration(n) * time.Second)
	for i := 0; i < n; i++ {
		txid := fmt.Sprintf("tx-%06d", i)
		subs = append(subs, &models.Submission{TxID: txid, CallbackToken: "tok-big"})
		statuses[txid] = &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusSeenOnNetwork,
			Timestamp: base.Add(time.Duration(i) * time.Second),
		}
	}
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{"tok-big": subs},
		statusByTx:  statuses,
	}
	svc := &Service{
		cfg:    &config.Config{SSE: config.SSEConfig{Enabled: true}},
		logger: zap.NewNop(),
		store:  st,
	}

	rec := httptest.NewRecorder()
	w := &sseWriter{w: rec, f: nopFlusher{rec}}
	res := svc.sendCatchup(t.Context(), w, "tok-big", time.Time{}, nil, false, catchupMaxFrames)

	frames := strings.Count(rec.Body.String(), "\nevent: status\n")
	if frames != catchupMaxFrames {
		t.Errorf("catchup frames = %d, want cap %d", frames, catchupMaxFrames)
	}
	if !res.capped || res.frames != catchupMaxFrames {
		t.Errorf("result = %+v, want capped at %d frames", res, catchupMaxFrames)
	}
}

func TestSSENoPublisher(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &Service{
		cfg:    &config.Config{SSE: config.SSEConfig{Enabled: true}},
		logger: zap.NewNop(),
		store:  &sseStoreStub{},
		// publisher and manager intentionally nil
	}
	router := gin.New()
	router.GET("/events", svc.handleEvents)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/events", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSSEFanOutConcurrentUnregister(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	_, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	ctx, mgrCancel := context.WithCancel(t.Context())
	defer mgrCancel()
	mgr, err := newManager(ctx, &fakePublisher{}, st, zap.NewNop())
	if err != nil {
		t.Fatalf("newManager: %v", err)
	}

	const N = 200
	clients := make([]*Client, N)
	for i := 0; i < N; i++ {
		c := mgr.NewClient("")
		mgr.Register(c)
		clients[i] = c
	}

	startGoroutines := runtime.NumGoroutine()

	status := &models.TransactionStatus{
		TxID:      "race-tx",
		Status:    models.StatusMined,
		Timestamp: time.Unix(0, 1700000000000000000).UTC(),
	}

	const iterations = 50
	for it := 0; it < iterations; it++ {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < N; i += 2 {
				mgr.Unregister(clients[i].ID)
			}
		}()
		go func() {
			defer wg.Done()
			mgr.fanOut(ctx, status)
		}()
		wg.Wait()

		for i := 0; i < N; i += 2 {
			c := mgr.NewClient("")
			mgr.Register(c)
			clients[i] = c
		}
	}

	mgrCancel()
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= startGoroutines+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("goroutines start=%d end=%d (fudge tolerated)", startGoroutines, runtime.NumGoroutine())
}

func TestSSEFanOutSlowClientDrops(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	_, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	ctx, mgrCancel := context.WithCancel(t.Context())
	defer mgrCancel()
	mgr, err := newManager(ctx, &fakePublisher{}, st, zap.NewNop())
	if err != nil {
		t.Fatalf("newManager: %v", err)
	}

	slow := mgr.NewClient("")
	mgr.Register(slow)
	defer mgr.Unregister(slow.ID)

	for i := 0; i < cap(slow.Ch); i++ {
		slow.Ch <- &models.TransactionStatus{TxID: "filler", Timestamp: time.Now()}
	}

	before := testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("slow_client"))

	const drops = 5
	done := make(chan struct{})
	go func() {
		for i := 0; i < drops; i++ {
			mgr.fanOut(ctx, &models.TransactionStatus{
				TxID:      "drop",
				Status:    models.StatusMined,
				Timestamp: time.Now(),
			})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fanOut blocked on slow client (should have dropped)")
	}

	after := testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("slow_client"))
	if got := after - before; got != drops {
		t.Errorf("slow_client drops = %v, want %d", got, drops)
	}
}

func TestSSEFanOutClientGoneDrops(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	_, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	ctx, mgrCancel := context.WithCancel(t.Context())
	defer mgrCancel()
	mgr, err := newManager(ctx, &fakePublisher{}, st, zap.NewNop())
	if err != nil {
		t.Fatalf("newManager: %v", err)
	}

	gone := mgr.NewClient("")
	mgr.Register(gone)
	gone.Cancel()

	before := testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("client_gone"))

	mgr.fanOut(ctx, &models.TransactionStatus{
		TxID:      "gone",
		Status:    models.StatusMined,
		Timestamp: time.Now(),
	})

	after := testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("client_gone"))
	if got := after - before; got < 1 {
		t.Errorf("client_gone drops = %v, want >= 1", got)
	}
}

// TestSSEMidstreamCatchupDeliversDroppedEvents is the end-to-end contract of
// the mid-stream self-healing path: a token-scoped client whose channel is
// overflowed by fanOut still receives EVERY status (at-least-once) on the
// SAME connection, served from the store, and the catchup-round counter
// increments.
func TestSSEMidstreamCatchupDeliversDroppedEvents(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	const nTx = 5
	subs := make([]*models.Submission, 0, nTx)
	statuses := make([]*models.TransactionStatus, 0, nTx)
	statusByTx := make(map[string]*models.TransactionStatus, nTx)
	for i := 0; i < nTx; i++ {
		txid := fmt.Sprintf("drop-%d", i)
		st := &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusMined,
			Timestamp: now.Add(time.Duration(i) * time.Millisecond),
		}
		subs = append(subs, &models.Submission{TxID: txid, CallbackToken: tokA})
		statuses = append(statuses, st)
		statusByTx[txid] = st
	}
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{tokA: subs},
		statusByTx:  statusByTx,
	}
	svc, router, _, cancel := setupSSEService(t, st)
	defer cancel()
	mgr := svc.manager
	// A one-slot buffer makes fanOut overflow immediately once the writer
	// goroutine lags a single frame behind.
	mgr.clientBufferSize = 1

	srv := httptest.NewServer(router)
	defer srv.Close()

	catchupsBefore := testutil.ToFloat64(metrics.SSEMidstreamCatchupsTotal)
	dropsBefore := testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("slow_client"))

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/events?callbackToken="+tokA, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Wait for the connection's client to register.
	deadline := time.Now().Add(2 * time.Second)
	for {
		mgr.mu.RLock()
		n := len(mgr.clients)
		mgr.mu.RUnlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("client never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Fan the statuses out until at least one is dropped on the full channel.
	deadline = time.Now().Add(2 * time.Second)
	for i := 0; testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("slow_client")) == dropsBefore; i++ {
		if time.Now().After(deadline) {
			t.Fatal("fanOut never dropped despite 1-slot buffer")
		}
		mgr.fanOut(t.Context(), statuses[i%nTx])
	}

	// Nudge the stream while collecting: each delivered frame triggers a
	// drain cycle whose post-flush check runs the mid-stream catchup (the
	// 15s keepalive would eventually do it too; nudging keeps the test fast).
	nudgeDone := make(chan struct{})
	defer close(nudgeDone)
	go func() {
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-nudgeDone:
				return
			case <-tick.C:
				mgr.fanOut(context.Background(), statuses[nTx-1])
			}
		}
	}()

	want := make(map[string]bool, nTx)
	for _, s := range statuses {
		want[s.TxID] = true
	}
	if err := readSSEUntilTxIDs(resp.Body, want, 10*time.Second); err != nil {
		t.Fatalf("stream did not deliver every status: %v", err)
	}
	// The dropped flag is armed, so the writer's next drain cycle (the nudger
	// keeps producing them) MUST run a catchup round — but it may not have
	// happened yet when the reader finishes (every status can also arrive
	// live because the drop-forcing loop cycles them). Poll.
	deadline = time.Now().Add(5 * time.Second)
	for testutil.ToFloat64(metrics.SSEMidstreamCatchupsTotal)-catchupsBefore < 1 {
		if time.Now().After(deadline) {
			t.Fatal("bsv_broadcaster_sse_midstream_catchups_total never incremented after a drop")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSSEMidstreamCatchupTokenlessRaisesGapNotCatchup verifies what a firehose
// (tokenless) client gets on a drop: NO store-backed catchup round, because the
// catchup source is token-scoped and no bounded query reconstructs an
// unfiltered stream — but a gap frame, so the loss is declared rather than
// silent. Historically this path produced only a counter and a log line.
func TestSSEMidstreamCatchupTokenlessRaisesGapNotCatchup(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	svc, _, _, cancel := setupSSEService(t, st)
	defer cancel()
	mgr := svc.manager

	client := mgr.NewClient("")
	mgr.Register(client)
	defer mgr.Unregister(client.ID)

	for i := 0; i < cap(client.Ch); i++ {
		client.Ch <- &models.TransactionStatus{TxID: "filler", Timestamp: time.Now()}
	}
	dropsBefore := testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("slow_client"))
	mgr.fanOut(t.Context(), &models.TransactionStatus{TxID: "lost", Status: models.StatusMined, Timestamp: time.Now()})
	if got := testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("slow_client")) - dropsBefore; got != 1 {
		t.Fatalf("slow_client drops = %v, want 1", got)
	}
	if !client.dropped.Load() {
		t.Fatal("dropped flag not set by fanOut drop arm")
	}

	catchupsBefore := testutil.ToFloat64(metrics.SSEMidstreamCatchupsTotal)
	rec := httptest.NewRecorder()
	w := &sseWriter{w: rec, f: nopFlusher{rec}}
	if ok := svc.midstreamCatchup(t.Context(), w, client); !ok {
		t.Fatal("midstreamCatchup reported a dead connection")
	}
	if strings.Contains(rec.Body.String(), "event: status") {
		t.Errorf("tokenless client received catchup status frames: %q", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"reason":"`+gapReasonFirehoseDrop+`"`) {
		t.Errorf("tokenless drop was not declared to the client: %q", rec.Body.String())
	}
	if got := testutil.ToFloat64(metrics.SSEMidstreamCatchupsTotal) - catchupsBefore; got != 0 {
		t.Errorf("catchup rounds ran for tokenless client: %v", got)
	}
}

// TestSSEFanOutDropLogAggregation verifies the drop Warn is aggregated and
// rate-limited per client instead of logged once per dropped event.
func TestSSEFanOutDropLogAggregation(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	core, logs := observer.New(zap.WarnLevel)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	mgr, err := newManager(ctx, &fakePublisher{}, st, zap.New(core))
	if err != nil {
		t.Fatalf("newManager: %v", err)
	}

	client := mgr.NewClient("")
	mgr.Register(client)
	defer mgr.Unregister(client.ID)
	for i := 0; i < cap(client.Ch); i++ {
		client.Ch <- &models.TransactionStatus{TxID: "filler", Timestamp: time.Now()}
	}

	const drops = 100
	for i := 0; i < drops; i++ {
		mgr.fanOut(ctx, &models.TransactionStatus{TxID: "burst", Status: models.StatusMined, Timestamp: time.Now()})
	}

	warns := logs.FilterMessage("sse fan-out could not enqueue events: client send buffer full; will catch up mid-stream").Len()
	// 100 back-to-back drops land inside one dropLogInterval on any sane
	// machine, so one aggregate line is expected; tolerate a second in case
	// the loop straddles an interval boundary. The old code logged all 100.
	if warns < 1 || warns > 2 {
		t.Errorf("aggregate drop warns = %d, want 1 (2 tolerated), not one per %d drops", warns, drops)
	}
}

// TestSSEMidstreamCatchupCappedRoundsProgress verifies the multi-round loop:
// a replay window larger than one round's frame budget is delivered across
// capped rounds whose `since` watermark strictly advances.
func TestSSEMidstreamCatchupCappedRoundsProgress(t *testing.T) {
	n := catchupMaxFrames + 500
	subs := make([]*models.Submission, 0, n)
	statusByTx := make(map[string]*models.TransactionStatus, n)
	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < n; i++ {
		txid := fmt.Sprintf("prog-%06d", i)
		subs = append(subs, &models.Submission{TxID: txid, CallbackToken: "tok-prog"})
		statusByTx[txid] = &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusMined,
			Timestamp: base.Add(time.Duration(i) * time.Millisecond),
		}
	}
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{"tok-prog": subs},
		statusByTx:  statusByTx,
	}
	svc, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	client := svc.manager.NewClient("tok-prog")
	// Simulate fanOut having dropped events starting at the first status.
	client.droppedFloor.Store(base.UnixNano())
	client.dropped.Store(true)

	catchupsBefore := testutil.ToFloat64(metrics.SSEMidstreamCatchupsTotal)
	framesBefore := testutil.ToFloat64(metrics.SSEMidstreamCatchupFramesTotal)
	rec := httptest.NewRecorder()
	w := &sseWriter{w: rec, f: nopFlusher{rec}}
	if ok := svc.midstreamCatchup(t.Context(), w, client); !ok {
		t.Fatal("midstreamCatchup reported a dead connection")
	}

	if frames := strings.Count(rec.Body.String(), "\nevent: status\n"); frames != n {
		t.Errorf("frames written = %d, want %d (every status, across rounds)", frames, n)
	}
	// Rounds are bounded by the client's send-buffer budget, not by
	// catchupMaxFrames, so the window pages across ceil(n/budget) rounds.
	budget := midstreamRoundFrames(client)
	wantRounds := float64((n + budget - 1) / budget)
	if got := testutil.ToFloat64(metrics.SSEMidstreamCatchupsTotal) - catchupsBefore; got != wantRounds {
		t.Errorf("catchup rounds = %v, want %v (capped rounds of %d frames + resumed round)", got, wantRounds, budget)
	}
	if got := testutil.ToFloat64(metrics.SSEMidstreamCatchupFramesTotal) - framesBefore; got != float64(n) {
		t.Errorf("replayed frames metric delta = %v, want %d", got, n)
	}
	wantLast := statusByTx[fmt.Sprintf("prog-%06d", n-1)].Timestamp.UnixNano()
	if got := client.lastDelivered.Load(); got != wantLast {
		t.Errorf("lastDelivered = %d, want %d (watermark advanced to the final frame)", got, wantLast)
	}
}

// TestSSEMidstreamCatchupSameTimestampGlut verifies the soft frame cap: when
// an equal-timestamp run (one SetMinedByTxIDs batch) straddles the cap, the
// round finishes the tie instead of stranding the remainder — a timestamp
// cursor cannot resume mid-tie because the store filter is strictly-after.
func TestSSEMidstreamCatchupSameTimestampGlut(t *testing.T) {
	n := catchupMaxFrames + 500
	subs := make([]*models.Submission, 0, n)
	statusByTx := make(map[string]*models.TransactionStatus, n)
	ts := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < n; i++ {
		txid := fmt.Sprintf("glut-%06d", i)
		subs = append(subs, &models.Submission{TxID: txid, CallbackToken: "tok-glut"})
		statusByTx[txid] = &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusMined,
			Timestamp: ts, // the whole set shares ONE timestamp
		}
	}
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{"tok-glut": subs},
		statusByTx:  statusByTx,
	}
	svc, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	client := svc.manager.NewClient("tok-glut")
	client.droppedFloor.Store(ts.UnixNano())
	client.dropped.Store(true)

	catchupsBefore := testutil.ToFloat64(metrics.SSEMidstreamCatchupsTotal)
	rec := httptest.NewRecorder()
	w := &sseWriter{w: rec, f: nopFlusher{rec}}
	if ok := svc.midstreamCatchup(t.Context(), w, client); !ok {
		t.Fatal("midstreamCatchup reported a dead connection")
	}

	if frames := strings.Count(rec.Body.String(), "\nevent: status\n"); frames != n {
		t.Errorf("frames written = %d, want %d (tie drained past the cap)", frames, n)
	}
	if got := testutil.ToFloat64(metrics.SSEMidstreamCatchupsTotal) - catchupsBefore; got != 1 {
		t.Errorf("catchup rounds = %v, want 1 (single round drains the tie)", got)
	}
}

// txidsFromFrames extracts the txid of every `event: status` frame in an SSE
// body, in wire order.
func txidsFromFrames(t *testing.T, body string) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var payload struct {
			TxID string `json:"txid"`
		}
		raw := strings.TrimPrefix(line, "data: ")
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			continue // gap/lag frames carry a different shape
		}
		if payload.TxID != "" {
			out = append(out, payload.TxID)
		}
	}
	return out
}

// TestSSEConnectCatchupSameTimestampGlutIsResumable is the regression for the
// silent-loss bug on the CONNECT-TIME catchup path.
//
// transactions.timestamp_at is TIMESTAMPTZ (microsecond) and the store filter
// is strictly-after, while SetMinedByTxIDs stamps a whole block's rows with one
// timestamp_at. The mid-stream path already drains an equal-timestamp run that
// straddles the frame cap; the connect-time path did not, so it stopped
// mid-tie, reported lastTS as the tied timestamp, and every remaining row
// sharing that microsecond became unreachable forever — the client resumed
// strictly after it and was told nothing.
//
// The assertion is an identity, not a frame count: replay, resume from the id
// the client would have kept, replay again, and require that the union covers
// every row.
func TestSSEConnectCatchupSameTimestampGlutIsResumable(t *testing.T) {
	n := catchupMaxFrames + 500
	subs := make([]*models.Submission, 0, n)
	statusByTx := make(map[string]*models.TransactionStatus, n)
	ts := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	for i := 0; i < n; i++ {
		txid := fmt.Sprintf("tie-%06d", i)
		subs = append(subs, &models.Submission{TxID: txid, CallbackToken: "tok-tie"})
		statusByTx[txid] = &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusMined,
			Timestamp: ts, // one SetMinedByTxIDs batch: the whole block shares ONE timestamp
		}
	}
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{"tok-tie": subs},
		statusByTx:  statusByTx,
	}
	svc, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	client := svc.manager.NewClient("tok-tie")
	// A Last-Event-ID reconnect: `since` is non-zero, so terminal statuses
	// replay and the strictly-after filter admits the whole tie.
	since := ts.Add(-time.Second)

	seen := make(map[string]bool, n)
	rec := httptest.NewRecorder()
	res := svc.sendCatchup(t.Context(), &sseWriter{w: rec, f: nopFlusher{rec}}, "tok-tie", since, client, false, catchupMaxFrames)
	if res.err != nil {
		t.Fatalf("connect catchup: %v", res.err)
	}
	for _, txid := range txidsFromFrames(t, rec.Body.String()) {
		seen[txid] = true
	}

	// Resume exactly as the client would: Last-Event-ID is the last id it was
	// handed, and the store filter is strictly-after it.
	rec2 := httptest.NewRecorder()
	res2 := svc.sendCatchup(t.Context(), &sseWriter{w: rec2, f: nopFlusher{rec2}}, "tok-tie", time.Unix(0, res.lastTS), client, false, catchupMaxFrames)
	if res2.err != nil {
		t.Fatalf("resumed catchup: %v", res2.err)
	}
	for _, txid := range txidsFromFrames(t, rec2.Body.String()) {
		seen[txid] = true
	}

	var missing []string
	for txid := range statusByTx {
		if !seen[txid] {
			missing = append(missing, txid)
		}
	}
	if len(missing) != 0 {
		sort.Strings(missing)
		show := missing
		if len(show) > 5 {
			show = show[:5]
		}
		t.Errorf("%d of %d rows are unreachable after resuming from the delivered cursor (e.g. %v); "+
			"a same-timestamp run straddling the frame cap must be drained, not stranded", len(missing), n, show)
	}
}

// TestSSEConnectCatchupTruncationEmitsGap asserts that a connect-time replay
// which genuinely runs out of frame budget tells the client so. Truncation is
// not the bug — SILENT truncation is; before the gap frame this path logged a
// server-side Warn and the client streamed on believing it was whole.
func TestSSEConnectCatchupTruncationEmitsGap(t *testing.T) {
	n := catchupMaxFrames + 500
	subs := make([]*models.Submission, 0, n)
	statusByTx := make(map[string]*models.TransactionStatus, n)
	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < n; i++ {
		txid := fmt.Sprintf("trunc-%06d", i)
		subs = append(subs, &models.Submission{TxID: txid, CallbackToken: "tok-trunc"})
		statusByTx[txid] = &models.TransactionStatus{
			TxID:   txid,
			Status: models.StatusMined,
			// DISTINCT timestamps, so the cap bites at a real boundary rather
			// than being drained as a tie.
			Timestamp: base.Add(time.Duration(i) * time.Millisecond),
		}
	}
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{"tok-trunc": subs},
		statusByTx:  statusByTx,
	}
	svc, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	client := svc.manager.NewClient("tok-trunc")
	gapsBefore := testutil.ToFloat64(metrics.SSEGapEventsTotal.WithLabelValues(gapReasonCatchupTruncated))

	rec := httptest.NewRecorder()
	res := svc.sendCatchup(t.Context(), &sseWriter{w: rec, f: nopFlusher{rec}}, "tok-trunc", base.Add(-time.Second), client, false, catchupMaxFrames)
	if !res.capped {
		t.Fatalf("catchup did not cap: frames=%d", res.frames)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "\nevent: gap\n") && !strings.HasPrefix(body, "event: gap\n") {
		t.Error("truncated catchup wrote no gap frame; the client cannot know it lost history")
	}
	if !strings.Contains(body, `"reason":"`+gapReasonCatchupTruncated+`"`) {
		t.Errorf("gap frame missing reason %q", gapReasonCatchupTruncated)
	}
	if !strings.Contains(body, `"action":"reconcile"`) {
		t.Error("gap frame missing the reconcile action the client contract depends on")
	}
	if got := testutil.ToFloat64(metrics.SSEGapEventsTotal.WithLabelValues(gapReasonCatchupTruncated)) - gapsBefore; got != 1 {
		t.Errorf("gap metric delta = %v, want 1", got)
	}
}

// TestSSEFirehoseDropEmitsGap covers the tokenless client, which has no
// token-scoped replay available and previously got a counter and a log line and
// nothing else — its stream degraded silently and permanently.
func TestSSEFirehoseDropEmitsGap(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	svc, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	client := svc.manager.NewClient("") // firehose
	dropTS := time.Now().UTC().Add(-time.Minute).UnixNano()
	client.droppedFloor.Store(dropTS)
	client.dropped.Store(true)

	gapsBefore := testutil.ToFloat64(metrics.SSEGapEventsTotal.WithLabelValues(gapReasonFirehoseDrop))
	rec := httptest.NewRecorder()
	w := &sseWriter{w: rec, f: nopFlusher{rec}}
	if ok := svc.midstreamCatchup(t.Context(), w, client); !ok {
		t.Fatal("midstreamCatchup reported a dead connection")
	}

	body := rec.Body.String()
	if !strings.Contains(body, `"reason":"`+gapReasonFirehoseDrop+`"`) {
		t.Errorf("firehose drop wrote no gap frame; body = %q", body)
	}
	if !strings.Contains(body, fmt.Sprintf(`"from":"%d"`, dropTS)) {
		t.Errorf("gap frame missing the drop floor as its cursor; body = %q", body)
	}
	if got := testutil.ToFloat64(metrics.SSEGapEventsTotal.WithLabelValues(gapReasonFirehoseDrop)) - gapsBefore; got != 1 {
		t.Errorf("gap metric delta = %v, want 1", got)
	}
	if client.dropped.Load() {
		t.Error("drop flag not consumed: the gap would be re-emitted on every keepalive tick")
	}
}

// TestSSEReadyFailsWhileDrainingButLivenessDoesNot pins the probe split. They
// were the same handler, so there was no way to say "stop sending me traffic"
// without also saying "kill me" — and a liveness failure mid-drain is exactly
// the opposite of draining.
func TestSSEReadyFailsWhileDrainingButLivenessDoesNot(t *testing.T) {
	svc := &Service{cfg: &config.Config{}, logger: zap.NewNop()}

	call := func(h gin.HandlerFunc) int {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		h(c)
		return rec.Code
	}

	if got := call(svc.handleReady); got != http.StatusOK {
		t.Errorf("ready before draining = %d, want 200", got)
	}
	svc.draining.Store(true)
	if got := call(svc.handleReady); got != http.StatusServiceUnavailable {
		t.Errorf("ready while draining = %d, want 503 (pod stays in Service endpoints)", got)
	}
	if got := call(svc.handleHealth); got != http.StatusOK {
		t.Errorf("liveness while draining = %d, want 200 (a failure here kills the pod mid-drain)", got)
	}
}

// TestSSEDrainReleasesClientsWithStaggeredRetry covers the graceful drain: a
// client is released with a clean shutdown frame carrying its OWN reconnect
// delay, instead of having its socket severed when the process exits.
func TestSSEDrainReleasesClientsWithStaggeredRetry(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	svc, router, _, cancel := setupSSEService(t, st)
	defer cancel()

	server := httptest.NewServer(router)
	defer server.Close()

	resp, err := http.Get(server.URL + "/events") //nolint:noctx // test client
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Wait for the handler to register before draining.
	deadline := time.Now().Add(5 * time.Second)
	for svc.manager.clientCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("client never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if released := svc.manager.Drain(30 * time.Second); released != 1 {
		t.Fatalf("Drain released %d clients, want 1", released)
	}

	// The stream must END (clean EOF), not hang, and the last thing on it must
	// be the shutdown frame with a retry directive.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read drained stream: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "event: shutdown") {
		t.Errorf("drained stream carried no shutdown frame: %q", text)
	}
	var retryMS int
	if _, err := fmt.Sscanf(text[strings.Index(text, "retry: "):], "retry: %d", &retryMS); err != nil {
		t.Fatalf("no retry directive in %q: %v", text, err)
	}
	if retryMS <= 0 || retryMS > 30_000 {
		t.Errorf("retry hint = %dms, want within (0, 30000]", retryMS)
	}
}

// TestSSEMidstreamCatchupRefusedRaisesGapAndKeepsStream covers a backend that
// declines a replay it cannot bound (store.ErrReplayUnavailable, returned by
// the Pebble/Aerospike scan budget for a token too large to serve without an
// index over it).
//
// The distinction under test: a refused replay is a bounded, DECLARED
// degradation, not a dead connection. The client is told to reconcile over
// REST and its live stream keeps running — dropping the connection instead
// would send it straight back to reconnect and request the same impossible
// replay again.
func TestSSEMidstreamCatchupRefusedRaisesGapAndKeepsStream(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{tokA: {{TxID: "aaaa", CallbackToken: tokA}}},
		statusByTx:  map[string]*models.TransactionStatus{},
		iterErr:     fmt.Errorf("wrapped: %w", store.ErrReplayUnavailable),
	}
	svc, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	client := svc.manager.NewClient(tokA)
	client.droppedFloor.Store(time.Now().Add(-time.Minute).UnixNano())
	client.dropped.Store(true)

	gapsBefore := testutil.ToFloat64(metrics.SSEGapEventsTotal.WithLabelValues(gapReasonReplayUnavailable))
	rec := httptest.NewRecorder()
	w := &sseWriter{w: rec, f: nopFlusher{rec}}

	if ok := svc.midstreamCatchup(t.Context(), w, client); !ok {
		t.Error("a refused replay killed the connection; the live stream is still serviceable")
	}
	if !strings.Contains(rec.Body.String(), `"reason":"`+gapReasonReplayUnavailable+`"`) {
		t.Errorf("refused replay raised no gap; body = %q", rec.Body.String())
	}
	if got := testutil.ToFloat64(metrics.SSEGapEventsTotal.WithLabelValues(gapReasonReplayUnavailable)) - gapsBefore; got != 1 {
		t.Errorf("gap metric delta = %v, want 1", got)
	}
}

// TestSSEDrainAlwaysAnnouncesShutdown pins that the shutdown event does not
// depend on reconnect hints being enabled.
//
// The two signals carry different information: `event: shutdown` says the
// release was DELIBERATE — letting a client treat a rolling restart as routine
// rather than as an error — while `retry:` says when to come back. Suppressing
// the former because the latter is disabled would drop the more important of
// the two.
func TestSSEDrainAlwaysAnnouncesShutdown(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	svc, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	for _, tc := range []struct {
		name      string
		retryMax  time.Duration
		wantRetry bool
	}{
		{"hints enabled", 30 * time.Second, true},
		{"hints disabled", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := svc.manager.NewClient("")
			if tc.retryMax > 0 {
				client.drainRetryMS.Store(tc.retryMax.Milliseconds())
			}
			rec := httptest.NewRecorder()
			writeShutdown(&sseWriter{w: rec, f: nopFlusher{rec}}, client)

			body := rec.Body.String()
			if !strings.Contains(body, "event: shutdown") {
				t.Errorf("no shutdown frame; the client cannot tell a deliberate release from a broken connection. body = %q", body)
			}
			if got := strings.Contains(body, "retry: "); got != tc.wantRetry {
				t.Errorf("retry directive present = %v, want %v; body = %q", got, tc.wantRetry, body)
			}
		})
	}
}

// readSSEUntilTxIDs consumes SSE frames from r until every txid in want has
// been seen at least once (duplicates allowed) or the timeout elapses.
func readSSEUntilTxIDs(r io.Reader, want map[string]bool, timeout time.Duration) error {
	var mu sync.Mutex
	seen := make(map[string]bool, len(want))
	done := make(chan error, 1)
	go func() {
		br := bufio.NewReader(r)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				done <- fmt.Errorf("read: %w", err)
				return
			}
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var payload struct {
				TxID string `json:"txid"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(line), "data: ")), &payload); err != nil {
				continue
			}
			mu.Lock()
			if want[payload.TxID] {
				seen[payload.TxID] = true
			}
			complete := len(seen) == len(want)
			mu.Unlock()
			if complete {
				done <- nil
				return
			}
		}
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		mu.Lock()
		defer mu.Unlock()
		missing := make([]string, 0, len(want))
		for txid := range want {
			if !seen[txid] {
				missing = append(missing, txid)
			}
		}
		return fmt.Errorf("timeout; missing txids: %v", missing)
	}
}

func readNextSSEFrame(r io.Reader, timeout time.Duration) (string, error) {
	type result struct {
		s   string
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		br := bufio.NewReader(r)
		var buf bytes.Buffer
		for {
			line, err := br.ReadBytes('\n')
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			buf.Write(line)
			if len(line) > 0 && line[0] == ':' {
				buf.Reset()
				continue
			}
			if len(line) == 1 && line[0] == '\n' {
				if buf.Len() > 1 {
					resultCh <- result{s: buf.String()}
					return
				}
				buf.Reset()
			}
		}
	}()

	select {
	case r := <-resultCh:
		return r.s, r.err
	case <-time.After(timeout):
		return "", fmt.Errorf("timeout waiting for SSE frame")
	}
}
