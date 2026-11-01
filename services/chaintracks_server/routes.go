package chaintracks_server

// Gin adapter for go-chaintracks HTTP routes.
//
// Upstream reference: github.com/bsv-blockchain/go-chaintracks/routes/fiber@v1.1.5.
// The URL surface, response bodies, and headers are intended to match that
// package 1:1 so clients written against the original arcade's Fiber-backed
// routes work unchanged against this Gin-backed one.
//
// Only the transport layer is ours; all data access goes through the
// chaintracks.Chaintracks interface so the library keeps sole ownership of
// header storage, P2P subscriptions, and reorg detection.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/gin-gonic/gin"
)

// Routes wraps a chaintracks.Chaintracks instance with HTTP
// plumbing: one broadcaster goroutine per SSE stream, fanning updates out to
// all connected clients. The broadcaster lifetimes are tied to the ctx passed
// to NewRoutes (which is the api-server's ctx), so a service
// shutdown cleanly tears down subscriptions.
type Routes struct {
	cm chaintracks.Chaintracks

	// SSE fan-out registries. clientID is a monotonic counter assigned on
	// connect; the value is a thread-safe sseWriter that serializes writes
	// from the broadcaster goroutine.
	nextClientID atomic.Int64

	tipMu      sync.RWMutex
	tipClients map[int64]*sseWriter

	reorgMu      sync.RWMutex
	reorgClients map[int64]*sseWriter
}

// sseWriter serializes writes to a single http.ResponseWriter. Gin does not
// promise concurrent-safe writes, and our broadcaster + keepalive goroutines
// both write to the same writer — a mutex keeps framing intact.
//
// closed is set when the owning handler returns. The broadcaster may have
// already snapshotted this writer before the handler removed itself from the
// client map, so close() must serialize against any in-flight write to keep
// the broadcaster from touching the response after the HTTP server starts
// finalizing it.
type sseWriter struct {
	mu     sync.Mutex
	w      http.ResponseWriter
	f      http.Flusher
	closed bool
}

func (s *sseWriter) write(payload string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return http.ErrBodyNotAllowed
	}
	if _, err := fmt.Fprint(s.w, payload); err != nil {
		return err
	}
	s.f.Flush()
	return nil
}

func (s *sseWriter) close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

// NewRoutes subscribes to tip and reorg channels and starts
// broadcaster goroutines. Both exit when ctx is canceled.
func NewRoutes(ctx context.Context, cm chaintracks.Chaintracks) *Routes {
	r := &Routes{
		cm:           cm,
		tipClients:   make(map[int64]*sseWriter),
		reorgClients: make(map[int64]*sseWriter),
	}

	tipCh := cm.Subscribe(ctx)
	go r.runBroadcaster(ctx, tipCh, r.broadcastTip)

	reorgCh := cm.SubscribeReorg(ctx)
	go r.runReorgBroadcaster(ctx, reorgCh)

	return r
}

func (r *Routes) runBroadcaster(ctx context.Context, ch <-chan *chaintracks.BlockHeader, fan func(*chaintracks.BlockHeader)) {
	for {
		select {
		case <-ctx.Done():
			return
		case h, ok := <-ch:
			if !ok {
				return
			}
			if h != nil {
				fan(h)
			}
		}
	}
}

func (r *Routes) runReorgBroadcaster(ctx context.Context, ch <-chan *chaintracks.ReorgEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if ev != nil {
				r.broadcastReorg(ev)
			}
		}
	}
}

func (r *Routes) broadcastTip(tip *chaintracks.BlockHeader) {
	data, err := json.Marshal(tip)
	if err != nil {
		return
	}
	payload := "data: " + string(data) + "\n\n"

	r.tipMu.RLock()
	clients := make([]*sseWriter, 0, len(r.tipClients))
	ids := make([]int64, 0, len(r.tipClients))
	for id, w := range r.tipClients {
		clients = append(clients, w)
		ids = append(ids, id)
	}
	r.tipMu.RUnlock()

	var failed []int64
	for i, w := range clients {
		if err := w.write(payload); err != nil {
			failed = append(failed, ids[i])
		}
	}
	if len(failed) > 0 {
		r.tipMu.Lock()
		for _, id := range failed {
			delete(r.tipClients, id)
		}
		r.tipMu.Unlock()
	}
}

func (r *Routes) broadcastReorg(ev *chaintracks.ReorgEvent) {
	data, err := json.Marshal(ev)
	if err != nil {
		return
	}
	payload := "data: " + string(data) + "\n\n"

	r.reorgMu.RLock()
	clients := make([]*sseWriter, 0, len(r.reorgClients))
	ids := make([]int64, 0, len(r.reorgClients))
	for id, w := range r.reorgClients {
		clients = append(clients, w)
		ids = append(ids, id)
	}
	r.reorgMu.RUnlock()

	var failed []int64
	for i, w := range clients {
		if err := w.write(payload); err != nil {
			failed = append(failed, ids[i])
		}
	}
	if len(failed) > 0 {
		r.reorgMu.Lock()
		for _, id := range failed {
			delete(r.reorgClients, id)
		}
		r.reorgMu.Unlock()
	}
}

// --- v2 routes ---

// Register mounts the v2 route surface on the given router group. Call on a
// group rooted at /chaintracks/v2.
//
// JSON and binary header endpoints share a route pattern: the handler inspects
// the param's ".bin" suffix to branch, because Gin's radix router does not
// allow literal suffixes on named params (`:height.bin` would conflict with
// `:height`). Upstream Fiber doesn't have this restriction, but the dispatch
// difference is invisible to clients.
func (r *Routes) Register(router *gin.RouterGroup) {
	r.registerCommon(router)
	router.GET("/tip/stream", r.handleTipStream)
	router.GET("/reorg/stream", r.handleReorgStream)
}

// RegisterLegacy mounts the v1 RPC-style routes matching the original
// chaintracks-server API PLUS the JSON/binary surface (so clients targeting
// the shorter v1 prefix get the whole API, matching upstream behavior).
func (r *Routes) RegisterLegacy(router *gin.RouterGroup) {
	router.GET("/getChain", r.handleLegacyGetChain)
	router.GET("/getPresentHeight", r.handleLegacyGetPresentHeight)
	router.GET("/findChainTipHashHex", r.handleLegacyFindChainTipHashHex)
	router.GET("/findChainTipHeaderHex", r.handleLegacyFindChainTipHeaderHex)
	router.GET("/findHeaderHexForHeight", r.handleLegacyFindHeaderHexForHeight)
	router.GET("/findHeaderHexForBlockHash", r.handleLegacyFindHeaderHexForBlockHash)
	router.GET("/getHeaders", r.handleLegacyGetHeaders)
	r.registerCommon(router)
}

// registerCommon wires the JSON + binary surface shared by v1 and v2.
func (r *Routes) registerCommon(router *gin.RouterGroup) {
	router.GET("/network", r.handleGetNetwork)
	router.GET("/height", r.handleGetHeight)
	router.GET("/tip", r.handleGetTip)
	router.GET("/tip.bin", r.handleGetTipBinary)
	router.GET("/headers", r.handleGetHeaders)
	router.GET("/headers.bin", r.handleGetHeadersBinary)
	// Dispatch JSON vs binary inside the handler on the param's ".bin" suffix.
	router.GET("/header/height/:height", r.handleGetHeaderByHeightDispatch)
	router.GET("/header/hash/:hash", r.handleGetHeaderByHashDispatch)
}

func (r *Routes) handleGetHeaderByHeightDispatch(c *gin.Context) {
	if strings.HasSuffix(c.Param("height"), ".bin") {
		r.handleGetHeaderByHeightBinary(c)
		return
	}
	r.handleGetHeaderByHeight(c)
}

func (r *Routes) handleGetHeaderByHashDispatch(c *gin.Context) {
	if strings.HasSuffix(c.Param("hash"), ".bin") {
		r.handleGetHeaderByHashBinary(c)
		return
	}
	r.handleGetHeaderByHash(c)
}

// --- JSON handlers ---

func (r *Routes) handleGetNetwork(c *gin.Context) {
	network, err := r.cm.GetNetwork(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"network": network})
}

func (r *Routes) handleGetHeight(c *gin.Context) {
	c.Header("Cache-Control", "public, max-age=60")
	c.JSON(http.StatusOK, gin.H{"height": r.cm.GetHeight(c.Request.Context())})
}

func (r *Routes) handleGetTip(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	tip := r.cm.GetTip(c.Request.Context())
	if tip == nil {
		c.JSON(http.StatusNotFound, gin.H{jsonKeyError: "Chain tip not found"})
		return
	}
	c.Header("X-Block-Height", strconv.FormatUint(uint64(tip.Height), 10))
	c.JSON(http.StatusOK, tip)
}

func (r *Routes) handleGetHeaderByHeight(c *gin.Context) {
	heightStr := strings.TrimSuffix(c.Param("height"), ".bin")
	height, err := strconv.ParseUint(heightStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "Invalid height parameter"})
		return
	}

	ctx := c.Request.Context()
	r.setHeightCache(ctx, c, uint32(height))

	header, err := r.cm.GetHeaderByHeight(ctx, uint32(height))
	if err != nil {
		c.JSON(headerLookupStatus(err), gin.H{jsonKeyError: headerLookupMessage(err)})
		return
	}
	c.JSON(http.StatusOK, header)
}

func (r *Routes) handleGetHeaderByHash(c *gin.Context) {
	hashStr := strings.TrimSuffix(c.Param("hash"), ".bin")
	hash, err := chainhash.NewHashFromHex(hashStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "Invalid hash parameter"})
		return
	}

	ctx := c.Request.Context()
	header, err := r.cm.GetHeaderByHash(ctx, hash)
	if err != nil {
		c.JSON(headerLookupStatus(err), gin.H{jsonKeyError: headerLookupMessage(err)})
		return
	}
	r.setHeightCache(ctx, c, header.Height)
	c.JSON(http.StatusOK, header)
}

func (r *Routes) handleGetHeaders(c *gin.Context) {
	heightStr := c.Query("height")
	countStr := c.Query("count")
	if heightStr == "" || countStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "Missing height or count parameter"})
		return
	}
	height, err := strconv.ParseUint(heightStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "Invalid height parameter"})
		return
	}
	count, err := strconv.ParseUint(countStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "Invalid count parameter"})
		return
	}

	ctx := c.Request.Context()
	r.setHeightCache(ctx, c, uint32(height))

	var data []byte
	for i := uint32(0); i < uint32(count); i++ {
		h, err := r.cm.GetHeaderByHeight(ctx, uint32(height)+i)
		if err != nil {
			break
		}
		data = append(data, h.Bytes()...)
	}
	c.Data(http.StatusOK, "application/octet-stream", data)
}

// --- Binary handlers ---

func (r *Routes) handleGetTipBinary(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	tip := r.cm.GetTip(c.Request.Context())
	if tip == nil {
		c.JSON(http.StatusNotFound, gin.H{jsonKeyError: "Chain tip not found"})
		return
	}
	c.Header("X-Block-Height", strconv.FormatUint(uint64(tip.Height), 10))
	c.Data(http.StatusOK, "application/octet-stream", tip.Bytes())
}

func (r *Routes) handleGetHeaderByHeightBinary(c *gin.Context) {
	heightStr := strings.TrimSuffix(c.Param("height"), ".bin")
	height, err := strconv.ParseUint(heightStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "Invalid height parameter"})
		return
	}

	ctx := c.Request.Context()
	r.setHeightCache(ctx, c, uint32(height))

	header, err := r.cm.GetHeaderByHeight(ctx, uint32(height))
	if err != nil {
		c.JSON(headerLookupStatus(err), gin.H{jsonKeyError: headerLookupMessage(err)})
		return
	}
	c.Header("X-Block-Height", strconv.FormatUint(uint64(header.Height), 10))
	c.Data(http.StatusOK, "application/octet-stream", header.Bytes())
}

func (r *Routes) handleGetHeaderByHashBinary(c *gin.Context) {
	hashStr := strings.TrimSuffix(c.Param("hash"), ".bin")
	hash, err := chainhash.NewHashFromHex(hashStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "Invalid hash parameter"})
		return
	}
	ctx := c.Request.Context()
	header, err := r.cm.GetHeaderByHash(ctx, hash)
	if err != nil {
		c.JSON(headerLookupStatus(err), gin.H{jsonKeyError: headerLookupMessage(err)})
		return
	}
	r.setHeightCache(ctx, c, header.Height)
	c.Header("X-Block-Height", strconv.FormatUint(uint64(header.Height), 10))
	c.Data(http.StatusOK, "application/octet-stream", header.Bytes())
}

func (r *Routes) handleGetHeadersBinary(c *gin.Context) {
	heightStr := c.Query("height")
	countStr := c.Query("count")
	if heightStr == "" || countStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "Missing height or count parameter"})
		return
	}
	height, err := strconv.ParseUint(heightStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "Invalid height parameter"})
		return
	}
	count, err := strconv.ParseUint(countStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "Invalid count parameter"})
		return
	}

	ctx := c.Request.Context()
	r.setHeightCache(ctx, c, uint32(height))

	var data []byte
	var headerCount uint32
	for i := uint32(0); i < uint32(count); i++ {
		h, err := r.cm.GetHeaderByHeight(ctx, uint32(height)+i)
		if err != nil {
			break
		}
		data = append(data, h.Bytes()...)
		headerCount++
	}
	c.Header("X-Start-Height", heightStr)
	c.Header("X-Header-Count", strconv.FormatUint(uint64(headerCount), 10))
	c.Data(http.StatusOK, "application/octet-stream", data)
}

// setHeightCache marks heights older than ~100 blocks as immutable for an hour
// so CDNs can cache them; recent heights are marked no-cache because they may
// still reorg. Matches the upstream Fiber handler behavior.
func (r *Routes) setHeightCache(ctx context.Context, c *gin.Context, height uint32) {
	tip := r.cm.GetHeight(ctx)
	if tip > 100 && height < tip-100 {
		c.Header("Cache-Control", "public, max-age=3600")
	} else {
		c.Header("Cache-Control", "no-cache")
	}
}

// --- SSE handlers ---

func (r *Routes) handleTipStream(c *gin.Context) {
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "streaming unsupported"})
		return
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Writer.WriteHeader(http.StatusOK)

	writer := &sseWriter{w: c.Writer, f: flusher}
	id := r.nextClientID.Add(1)

	r.tipMu.Lock()
	r.tipClients[id] = writer
	r.tipMu.Unlock()
	defer func() {
		r.tipMu.Lock()
		delete(r.tipClients, id)
		r.tipMu.Unlock()
		writer.close()
	}()

	ctx := c.Request.Context()

	// Deliver the current tip immediately so reconnecting clients get state
	// without waiting for the next block.
	if tip := r.cm.GetTip(ctx); tip != nil {
		if data, err := json.Marshal(tip); err == nil {
			if err := writer.write("data: " + string(data) + "\n\n"); err != nil {
				return
			}
		}
	}

	r.runKeepalive(ctx, writer)
}

func (r *Routes) handleReorgStream(c *gin.Context) {
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "streaming unsupported"})
		return
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Writer.WriteHeader(http.StatusOK)

	writer := &sseWriter{w: c.Writer, f: flusher}
	id := r.nextClientID.Add(1)

	r.reorgMu.Lock()
	r.reorgClients[id] = writer
	r.reorgMu.Unlock()
	defer func() {
		r.reorgMu.Lock()
		delete(r.reorgClients, id)
		r.reorgMu.Unlock()
		writer.close()
	}()

	r.runKeepalive(c.Request.Context(), writer)
}

// runKeepalive blocks on ctx.Done() and periodically sends SSE comments so
// intermediaries don't time the connection out. Exits as soon as the client
// disconnects (ctx.Done closes) or a keepalive write fails.
func (r *Routes) runKeepalive(ctx context.Context, writer *sseWriter) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := writer.write(": keepalive\n\n"); err != nil {
				return
			}
		}
	}
}

// --- Legacy v1 RPC-style handlers ---

type legacyResponse struct {
	Status      string      `json:"status"`
	Value       interface{} `json:"value,omitempty"`
	Code        string      `json:"code,omitempty"`
	Description string      `json:"description,omitempty"`
}

func legacySuccess(value interface{}) legacyResponse {
	return legacyResponse{Status: "success", Value: value}
}

func legacyError(code, description string) legacyResponse {
	return legacyResponse{Status: jsonKeyError, Code: code, Description: description}
}

func (r *Routes) handleLegacyGetChain(c *gin.Context) {
	network, err := r.cm.GetNetwork(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, legacyError("ERR_INTERNAL", err.Error()))
		return
	}
	c.JSON(http.StatusOK, legacySuccess(network))
}

func (r *Routes) handleLegacyGetPresentHeight(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	c.JSON(http.StatusOK, legacySuccess(r.cm.GetHeight(c.Request.Context())))
}

func (r *Routes) handleLegacyFindChainTipHashHex(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	tip := r.cm.GetTip(c.Request.Context())
	if tip == nil {
		c.JSON(http.StatusNotFound, legacyError("ERR_NO_TIP", "Chain tip not found"))
		return
	}
	c.JSON(http.StatusOK, legacySuccess(tip.Hash.String()))
}

func (r *Routes) handleLegacyFindChainTipHeaderHex(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")
	tip := r.cm.GetTip(c.Request.Context())
	if tip == nil {
		c.JSON(http.StatusNotFound, legacyError("ERR_NO_TIP", "Chain tip not found"))
		return
	}
	c.JSON(http.StatusOK, legacySuccess(tip))
}

func (r *Routes) handleLegacyFindHeaderHexForHeight(c *gin.Context) {
	heightStr := c.Query("height")
	if heightStr == "" {
		c.JSON(http.StatusBadRequest, legacyError("ERR_INVALID_PARAMS", "Missing height parameter"))
		return
	}
	height, err := strconv.ParseUint(heightStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, legacyError("ERR_INVALID_PARAMS", "Invalid height parameter"))
		return
	}
	ctx := c.Request.Context()
	r.setHeightCache(ctx, c, uint32(height))

	header, err := r.cm.GetHeaderByHeight(ctx, uint32(height))
	if err != nil {
		if !errors.Is(err, chaintracks.ErrHeaderNotFound) {
			c.JSON(http.StatusInternalServerError, legacyError("ERR_INTERNAL", "Header lookup failed at height "+heightStr))
			return
		}
		c.JSON(http.StatusNotFound, legacyError("ERR_NOT_FOUND", "Header not found at height "+heightStr))
		return
	}
	c.JSON(http.StatusOK, legacySuccess(header))
}

func (r *Routes) handleLegacyFindHeaderHexForBlockHash(c *gin.Context) {
	hashStr := c.Query("hash")
	if hashStr == "" {
		c.JSON(http.StatusBadRequest, legacyError("ERR_INVALID_PARAMS", "Missing hash parameter"))
		return
	}
	hash, err := chainhash.NewHashFromHex(hashStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, legacyError("ERR_INVALID_PARAMS", "Invalid hash parameter"))
		return
	}
	ctx := c.Request.Context()
	header, err := r.cm.GetHeaderByHash(ctx, hash)
	if err != nil {
		if !errors.Is(err, chaintracks.ErrHeaderNotFound) {
			c.JSON(http.StatusInternalServerError, legacyError("ERR_INTERNAL", "Header lookup failed for hash "+hashStr))
			return
		}
		c.JSON(http.StatusNotFound, legacyError("ERR_NOT_FOUND", "Header not found for hash "+hashStr))
		return
	}
	r.setHeightCache(ctx, c, header.Height)
	c.JSON(http.StatusOK, legacySuccess(header))
}

func (r *Routes) handleLegacyGetHeaders(c *gin.Context) {
	heightStr := c.Query("height")
	countStr := c.Query("count")
	if heightStr == "" || countStr == "" {
		c.JSON(http.StatusBadRequest, legacyError("ERR_INVALID_PARAMS", "Missing height or count parameter"))
		return
	}
	height, err := strconv.ParseUint(heightStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, legacyError("ERR_INVALID_PARAMS", "Invalid height parameter"))
		return
	}
	count, err := strconv.ParseUint(countStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, legacyError("ERR_INVALID_PARAMS", "Invalid count parameter"))
		return
	}
	ctx := c.Request.Context()
	r.setHeightCache(ctx, c, uint32(height))

	var data []byte
	for i := uint32(0); i < uint32(count); i++ {
		h, err := r.cm.GetHeaderByHeight(ctx, uint32(height)+i)
		if err != nil {
			break
		}
		data = append(data, h.Bytes()...)
	}
	c.JSON(http.StatusOK, legacySuccess(fmt.Sprintf("%x", data)))
}

// headerLookupStatus answers 404 only for a header that positively does not
// exist (chaintracks.ErrHeaderNotFound) and 500 for any other lookup failure.
// Remote readers (bump-builder's reconciler and anchor guard in microservice
// mode) treat a 404 as absence — the ordinary end of an upward walk — so a
// real failure reported as 404 would make them act on a missing header
// instead of retrying.
func headerLookupStatus(err error) int {
	if errors.Is(err, chaintracks.ErrHeaderNotFound) {
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}

func headerLookupMessage(err error) string {
	if errors.Is(err, chaintracks.ErrHeaderNotFound) {
		return "Header not found"
	}
	return "Header lookup failed"
}
