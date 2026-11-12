package api_server

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
	"errors"
	"html/template"
	"io"
	"maps"
	"net/http"
	"strings"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/callbackurl"
	"github.com/bsv-blockchain/broadcaster/config"
	arcerrors "github.com/bsv-blockchain/broadcaster/errors"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/logfields"
	"github.com/bsv-blockchain/broadcaster/merkleservice"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/services/ratelimit"
	"github.com/bsv-blockchain/broadcaster/services/usage"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/telemetry"
	"github.com/bsv-blockchain/broadcaster/teranode"
	"github.com/bsv-blockchain/broadcaster/version"
)

// collectInputTXIDs returns the parent txids referenced by every input of
// tx. Empty for coinbase. Intake keys a submission like its in-flight
// parent's family from these; the propagation envelope no longer carries
// them (#329), so the consumer re-derives parents from the stored bytes.
func collectInputTXIDs(tx *sdkTx.Transaction) []string {
	if tx == nil || len(tx.Inputs) == 0 {
		return nil
	}
	out := make([]string, 0, len(tx.Inputs))
	for _, in := range tx.Inputs {
		if in == nil || in.SourceTXID == nil {
			continue
		}
		out = append(out, in.SourceTXID.String())
	}
	return out
}

// shouldRepublishExisting reports store rows whose POST /tx retry must
// re-enter Kafka. REJECTED resubmits re-run broadcast (existing contract).
// RECEIVED resubmits cover the insert-then-Kafka-fail hole: GetOrInsertStatus
// already wrote RECEIVED, Send failed, the client retries, and the old
// duplicate branch echoed 202 without publishing — the tx never left BSV Broadcaster
// until the one-hour RECEIVED reaper. Seen/accepted/mined rows stay echoed.
func shouldRepublishExisting(status models.Status) bool {
	return status == models.StatusRejected || status == models.StatusReceived
}

const jsonKeyError = "error"

// restoreRawTxs writes each txid's submitted bytes onto its existing row
// before the row is republished. The propagation message carries the txid
// only (#329), so the consumer publishes whatever the row holds: a REJECTED
// row may have lost raw_tx (ClearRetryState) or hold the rejected bytes, and
// a stranded RECEIVED row may hold bytes an older intake stored untrimmed.
// The retry's parsed bytes replace them all, as the pre-#329 message did.
// Callers answer a failure with 503; nothing is published yet.
func (s *Server) restoreRawTxs(ctx context.Context, raws map[string][]byte) error {
	for txid, raw := range raws {
		if err := s.store.SetRawTx(ctx, txid, raw); err != nil {
			s.logger.Error("restoring raw tx before republish", logfields.TxID(txid), zap.Error(err))
			return err
		}
	}
	return nil
}

// shedBatch answers a /txs batch that failed on the store after part of it
// was inserted. Those rows stay RECEIVED with their raw tx, and the
// client's retry republishes them, but as CAS losers: only the inserter
// publishes RECEIVED (#391), so it goes out here or never. Nothing reached
// Kafka, so it cannot trail a verdict.
func (s *Server) shedBatch(c *gin.Context, inserted []string, receivedAt time.Time, opts submitOptions) {
	ctx := c.Request.Context()
	for _, txid := range inserted {
		s.recordSubmission(ctx, txid, opts)
	}
	s.publishReceived(ctx, inserted, bulkReceivedAt(receivedAt, len(inserted)), opts)
	respondStoreUnavailable(c)
}

// respondStoreUnavailable answers a submission whose store write failed or
// left the row's state unknown. 503 + Retry-After like Kafka backpressure:
// nothing was published, and a retry's CAS inserts or finds the row.
func respondStoreUnavailable(c *gin.Context) {
	c.Header("Retry-After", "1")
	c.JSON(http.StatusServiceUnavailable, gin.H{jsonKeyError: "store unavailable, retry shortly"})
}

// submitOptions captures the callback subscription preferences a client
// expressed via the X-CallbackUrl / X-CallbackToken / X-FullStatusUpdates
// request headers. Empty when the client did not subscribe.
type submitOptions struct {
	CallbackURL       string
	CallbackToken     string
	FullStatusUpdates bool
	// ExpiresAt is the retention deadline for the rows this request writes
	// (Server.retentionExpiry); nil keeps them forever.
	ExpiresAt *time.Time
}

// extractSubmitOptions reads the callback-subscription headers off a submit
// request. Mirrors the old BSV Broadcaster's header contract so existing clients (and
// the SSE catchup token filter) keep working unchanged.
func extractSubmitOptions(c *gin.Context) submitOptions {
	return submitOptions{
		CallbackURL:       c.GetHeader("X-CallbackUrl"),
		CallbackToken:     c.GetHeader("X-CallbackToken"),
		FullStatusUpdates: c.GetHeader("X-FullStatusUpdates") == "true",
	}
}

// validateCallbackURL applies the SSRF guard to the X-CallbackUrl header
// before the request is allowed to register a subscription. Empty URLs
// pass through — token-only subscriptions don't trigger an outbound dial,
// so there's no SSRF surface to protect. The shared callbackurl predicate
// is the same one the webhook delivery client uses at dial time, so a host
// that survives this check still gets re-validated at connection time
// (catches DNS rebinding).
//
// Returns a 400 to the client on failure and reports false; callers should
// abort processing in that case. The unsafe URL is logged at debug (not
// the value itself, just the host) so operators can correlate refusals
// without leaking attacker-controlled strings into structured logs.
func (s *Server) validateCallbackURL(c *gin.Context, url string) bool {
	if url == "" {
		return true
	}
	if err := callbackurl.ValidateURL(url, s.cfg.Callback.AllowPrivateIPs); err != nil {
		s.logger.Warn(
			"rejecting submit due to unsafe callback url",
			zap.String("client_ip", c.ClientIP()),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "invalid callback url: " + err.Error()})
		return false
	}
	return true
}

// hasSubscription reports whether the request asked for callback / SSE
// notifications. Either a URL or a token is enough — token-only clients use
// the SSE endpoint with no webhook delivery, URL-only clients get webhooks
// without SSE filtering.
func (o submitOptions) hasSubscription() bool {
	return o.CallbackURL != "" || o.CallbackToken != ""
}

// recordSubmission queues a callback registration for txid onto the async
// recorder. Best-effort: a full queue drops with a warn+metric — the contract
// matches the prior synchronous version (InsertSubmission failures were
// already logged and non-fatal). Moving the Pebble write off the HTTP
// handler removes a per-request DB write from POST tail latency.
func (s *Server) recordSubmission(_ context.Context, txid string, opts submitOptions) {
	if !opts.hasSubscription() {
		return
	}
	id, err := newSubmissionID()
	if err != nil {
		s.logger.Warn("could not generate submission id", zap.Error(err))
		return
	}
	sub := &models.Submission{
		SubmissionID:      id,
		TxID:              txid,
		CallbackURL:       opts.CallbackURL,
		CallbackToken:     opts.CallbackToken,
		FullStatusUpdates: opts.FullStatusUpdates,
		CreatedAt:         time.Now(),
		ExpiresAt:         opts.ExpiresAt,
	}
	select {
	case s.submissionCh <- submissionRecord{sub: sub}:
	default:
		metrics.APISubmissionRecorderDropTotal.Inc()
		s.logger.Warn(
			"submission recorder queue full; dropping (best-effort)",
			logfields.TxID(txid),
		)
	}
}

// newSubmissionID returns a 16-byte random hex identifier. Globally unique
// per call without coordinating across pods.
func newSubmissionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

//go:embed docs.html
var docsTemplate string

var docsTmpl = template.Must(template.New("docs").Funcs(template.FuncMap{
	"lower": strings.ToLower,
}).Parse(docsTemplate))

func (s *Server) handleDocs(c *gin.Context) {
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Status(http.StatusOK)
	if err := RenderDocs(c.Writer); err != nil {
		s.logger.Error("failed to render docs", zap.Error(err))
	}
}

// RenderDocs writes the rendered API docs HTML to w. Exported so the
// docs-preview dev command (cmd/docs-preview) can spin up the same page
// without standing up the full API server with its Kafka / store / etc
// dependencies.
func RenderDocs(w io.Writer) error {
	return docsTmpl.Execute(w, docsPage{Routes: routeDocs})
}

// healthResponse is the schema of GET /health. The leading "healthy" bool and
// "version" string are the ARC health contract (github.com/bitcoin-sv/arc) — ARC
// clients deserialise against it and gate transaction submission on
// healthy == true, so a missing field makes them reject an otherwise-healthy
// node (issue #208). The top-level "status":"ok" and "datahub_urls" are retained
// for backwards compatibility with existing health checkers. As a liveness
// probe, "healthy" is always true here: if the process answers, it is alive —
// per-endpoint Datahub health stays in datahub_urls[].healthy. Chaintracks moved
// out of api-server in the microservice decomposition — its health is now
// reported by the standalone chaintracks pod's /health endpoint.
//
// blockHeight is BSV Broadcaster's own processed active-tip height. It is a freshness
// signal, not a liveness gate: datahub_urls[].healthy is reachability-only,
// so this instance can look green while its chain view is stale — clients
// comparing blockHeight against the real network tip can detect that and
// route around it (issue #254). Omitted (0) when no store is wired or the
// height has never been readable.
type healthResponse struct {
	Healthy     bool            `json:"healthy"`
	Version     string          `json:"version"`
	Status      string          `json:"status"`
	BlockHeight uint64          `json:"blockHeight,omitempty"`
	DatahubURLs []datahubStatus `json:"datahub_urls"`
	// MerkleEndpoints is the per-endpoint breaker view of the configured
	// merkle-services (url, healthy, consecutive failures, open since).
	// Omitted when the Merkle integration is disabled.
	MerkleEndpoints []merkleservice.EndpointStatus `json:"merkle_endpoints,omitempty"`
}

// datahubStatus is one entry of health.datahub_urls: the circuit-breaker view
// of an endpoint, plus the transaction policy that node advertised when it
// registered the URL.
//
// teranode.EndpointStatus is embedded rather than copied so url/source/healthy
// keep their exact position and spelling in the JSON — existing health checkers
// and ARC clients parse this, and policy is purely additive.
//
// Policy answers "what will this specific endpoint accept", which
// GET /policy cannot: that reports the single network-wide policy BSV Broadcaster
// enforces, so when a broadcast is refused on policy grounds this is what
// identifies the strict node. Absent for an endpoint no node_status has
// announced — a statically configured URL, typically — or one whose node
// predates fee_policy.
type datahubStatus struct {
	teranode.EndpointStatus

	Policy *endpointPolicyView `json:"policy,omitempty"`
}

// endpointPolicyView renders an advertised policy in the same vocabulary as
// GET /policy (models.Policy), minus standardFormatSupported, which is an ARC
// transport flag rather than something a peer advertises. Operators reading
// both endpoints see one set of names.
type endpointPolicyView struct {
	MiningFee               models.FeeAmount `json:"miningFee"`
	MaxTxSizePolicy         uint64           `json:"maxtxsizepolicy"`
	MaxScriptSizePolicy     uint64           `json:"maxscriptsizepolicy"`
	MaxTxSigopsCountsPolicy uint64           `json:"maxtxsigopscountspolicy"`
}

func endpointPolicyViewFrom(p *store.EndpointPolicy) *endpointPolicyView {
	if p == nil {
		return nil
	}
	return &endpointPolicyView{
		MiningFee:               models.FeeAmount{Satoshis: p.MiningFeeSatoshis, Bytes: p.MiningFeeBytes},
		MaxTxSizePolicy:         p.MaxTxSizePolicy,
		MaxScriptSizePolicy:     p.MaxScriptSizePolicy,
		MaxTxSigopsCountsPolicy: p.MaxTxSigopsCountsPolicy,
	}
}

// healthTipTTL bounds how often /health re-reads the store's active tip
// height. Probes arrive every few seconds per replica (kubelet, LBs, ARC
// clients); one MAX-scan per TTL keeps the store cost flat regardless of
// probe fan-in while staying far fresher than block cadence.
const healthTipTTL = 5 * time.Second

// activeTipHeight returns BSV Broadcaster's processed active-tip height for /health,
// cached for healthTipTTL. Never fails the probe: on a store error it
// serves the last known height (a frozen value reads as stale to clients —
// which is exactly the signal — and the next probe retries the read).
func (s *Server) activeTipHeight(ctx context.Context) uint64 {
	if s.store == nil {
		return 0
	}
	s.tipMu.Lock()
	defer s.tipMu.Unlock()
	if time.Since(s.tipFetchedAt) < healthTipTTL {
		return s.tipHeight
	}
	h, err := s.store.GetActiveTipBlockHeight(ctx)
	if err != nil {
		s.logger.Debug("health tip-height read failed", zap.Error(err))
		return s.tipHeight
	}
	s.tipHeight = h
	s.tipFetchedAt = time.Now()
	metrics.ChainTipHeight.Set(float64(h))
	return h
}

// endpointPolicies returns the advertised policy for each registered datahub
// URL, keyed by normalized URL, cached for healthTipTTL on the same terms as
// activeTipHeight: /health is probed every few seconds per replica and this is
// a full-registry read. Never fails the probe — on a store error it serves the
// last known map and the next probe retries.
func (s *Server) endpointPolicies(ctx context.Context) map[string]*store.EndpointPolicy {
	if s.store == nil {
		return nil
	}
	s.policyMu.Lock()
	defer s.policyMu.Unlock()
	if time.Since(s.policyFetchedAt) < healthTipTTL {
		return s.policyByURL
	}
	eps, err := s.store.ListDatahubEndpoints(ctx, s.cfg.Network)
	if err != nil {
		s.logger.Debug("health endpoint-policy read failed", zap.Error(err))
		return s.policyByURL
	}
	byURL := make(map[string]*store.EndpointPolicy, len(eps))
	for _, ep := range eps {
		if ep.Policy != nil {
			byURL[normalizeEndpointURL(ep.URL)] = ep.Policy
		}
	}
	s.policyByURL = byURL
	s.policyFetchedAt = time.Now()
	return s.policyByURL
}

// normalizeEndpointURL keys a URL the same way on both sides of the policy
// join. It has to exist because the two sides are normalized differently
// upstream: teranode.Client trims a trailing slash off every endpoint it
// registers, while the store holds discovered URLs already normalized by
// validateURL but statically configured ones exactly as they appear in config.
// A seed URL written with a trailing slash therefore keys as "https://x/" in
// the store and "https://x" in the client, and that endpoint's policy would
// silently vanish from /health.
//
// Applied to both sides rather than just the store so the two cannot diverge
// on anything else either — trimming one side only would reintroduce the same
// class of miss for leading whitespace.
func normalizeEndpointURL(u string) string {
	return strings.TrimSuffix(strings.TrimSpace(u), "/")
}

func (s *Server) handleHealth(c *gin.Context) {
	resp := healthResponse{
		Healthy:     true,
		Version:     version.Version,
		Status:      "ok",
		BlockHeight: s.activeTipHeight(c.Request.Context()),
		DatahubURLs: []datahubStatus{},
	}
	if hr, ok := s.merkleClient.(merkleservice.HealthReporter); ok {
		resp.MerkleEndpoints = hr.EndpointStatuses()
	}
	if s.teranode != nil {
		statuses := s.teranode.GetEndpointStatuses()
		policies := s.endpointPolicies(c.Request.Context())
		resp.DatahubURLs = make([]datahubStatus, 0, len(statuses))
		for _, st := range statuses {
			resp.DatahubURLs = append(resp.DatahubURLs, datahubStatus{
				EndpointStatus: st,
				Policy:         endpointPolicyViewFrom(policies[normalizeEndpointURL(st.URL)]),
			})
		}
	}
	c.JSON(http.StatusOK, resp)
}

func (s *Server) handleReady(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// callbackMaxBodyBytes returns the configured upper bound on the request
// body for POST /api/v1/merkle-service/callback, falling back to the
// package default when the operator leaves the knob unset or sets a
// non-positive value. Centralized so the handler and tests stay in sync.
func (s *Server) callbackMaxBodyBytes() int64 {
	if n := s.cfg.Callback.MaxBodyBytes; n > 0 {
		return n
	}
	return config.DefaultCallbackMaxBodyBytes
}

// handleCallback processes inbound callbacks from Merkle Service.
// Uses CallbackMessage format with Type field.
//
// Bearer-token authentication is mandatory. config.validate refuses to start
// the binary when MerkleService is configured without a CallbackToken (finding
// F-018 / issue #76), so reaching this handler with an empty configured token
// means a misconfigured deployment outside the supported envelope. We still
// fail closed here as a defense-in-depth measure: an empty/missing bearer or
// any mismatch is rejected with 401 before any callback processing runs.
//
// The request body is wrapped in http.MaxBytesReader before JSON-binding so
// a malicious or malfunctioning peer can't exhaust memory by streaming an
// unbounded JSON body (notably the embedded STUMP blob). Oversize bodies
// produce a 413 Payload Too Large; the cap is configurable via
// Callback.MaxBodyBytes (default 16 MiB). See F-019 / issue #77.
func (s *Server) handleCallback(c *gin.Context) {
	// Bearer token validation — always enforced, never skipped on empty
	// configured token. subtle.ConstantTimeCompare removes the timing side
	// channel that a plain == on the secret would expose.
	auth := c.GetHeader("Authorization")
	const bearerPrefix = "Bearer "
	configured := []byte(s.cfg.CallbackToken)
	var presented []byte
	if strings.HasPrefix(auth, bearerPrefix) {
		presented = []byte(auth[len(bearerPrefix):])
	}
	if len(configured) == 0 || subtle.ConstantTimeCompare(configured, presented) != 1 {
		c.JSON(http.StatusUnauthorized, gin.H{jsonKeyError: "unauthorized"})
		return
	}

	// Cap the inbound body BEFORE JSON-binding. http.MaxBytesReader returns a
	// *http.MaxBytesError once the limit is exceeded, which we map to 413.
	// Other decode errors (malformed JSON, type mismatches) keep their
	// existing 400 mapping — the contract for legitimate malformed input
	// hasn't changed.
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, s.callbackMaxBodyBytes())

	var msg models.CallbackMessage
	if err := c.ShouldBindJSON(&msg); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{jsonKeyError: "request body too large"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "invalid request body"})
		return
	}

	logger := telemetry.LoggerWith(c.Request.Context(), s.logger.With(
		zap.String("type", string(msg.Type)),
		logfields.TxID(msg.TxID),
		logfields.TxIDs(msg.TxIDs),
		logfields.BlockHash(msg.BlockHash),
	))

	switch msg.Type {
	case models.CallbackSeenOnNetwork:
		if err := s.handleSeenOnNetwork(c, msg, logger); err != nil {
			// 500, not 4xx: the callback itself was valid. Merkle retries
			// non-2xx and treats 2xx as delivered, so a store failure must
			// not look like success. Matches the STUMP storage-error path.
			c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to store seen status"})
			return
		}
		c.Status(http.StatusOK)
	case models.CallbackSeenMultipleNodes:
		if err := s.handleSeenMultipleNodes(c, msg, logger); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to store seen status"})
			return
		}
		c.Status(http.StatusOK)
	case models.CallbackStump:
		s.handleStump(c, msg, logger)
	case models.CallbackBlockProcessed:
		s.handleBlockProcessed(c, msg, logger)
	default:
		// Unknown types stay 200 on purpose. Merkle retries non-2xx, so a
		// 5xx would retry forever a message this build will never apply,
		// and a 4xx can permanently reject a delivery. The type is logged;
		// no lifecycle row is written. A later build that understands the
		// type still receives new callbacks of that type.
		logger.Warn("unknown callback type")
		c.Status(http.StatusOK)
	}
}

// handleSeenOnNetwork applies a SEEN_ON_NETWORK callback from merkle-service
// to every txid in the message. Unknown txids (i.e. callbacks for txs we
// never recorded) are dropped with a Warn — never created as phantom rows.
// See F-033 / issue #91; the store layer enforces this by returning
// store.ErrNotFound from UpdateStatus when the row is absent.
//
// Batch path: one BatchUpdateStatusReturning call (parallel per-shard
// writes, with previous rows returned for transition-age observation) plus
// one PublishBulk event covering every successful txid. The previous
// per-tx loop did N synchronous store calls and N kafka.Send calls; under
// 100 TPS with merkle-service batching ~50 txids per callback that was
// ~50× the work per callback. See plan: RECEIVED → SEEN_ON_NETWORK
// Latency.
func (s *Server) handleSeenOnNetwork(c *gin.Context, msg models.CallbackMessage, logger *zap.Logger) error {
	return s.applySeenCallback(c, msg, logger, models.StatusSeenOnNetwork, "SEEN_ON_NETWORK")
}

// handleSeenMultipleNodes applies a SEEN_MULTIPLE_NODES callback. Same
// unknown-txid handling as handleSeenOnNetwork — the store rejects updates
// to absent rows (F-033 / #91) and we log + continue rather than creating
// phantom rows.
func (s *Server) handleSeenMultipleNodes(c *gin.Context, msg models.CallbackMessage, logger *zap.Logger) error {
	return s.applySeenCallback(c, msg, logger, models.StatusSeenMultipleNodes, "SEEN_MULTIPLE_NODES")
}

// applySeenCallback is the shared body of the two "seen" callback paths.
// targetStatus is the status to transition each known txid to;
// metricLabel is the type label used on the callback metrics.
//
// A non-nil error means the batch store write failed. The caller must
// answer the HTTP request with a retriable 5xx so Merkle delivers the
// callback again. Rows that did land before the error are still published;
// a retry is idempotent because the status lattice skips a tx that is
// already at targetStatus or past it.
func (s *Server) applySeenCallback(c *gin.Context, msg models.CallbackMessage, logger *zap.Logger, targetStatus models.Status, metricLabel string) error {
	start := time.Now()
	outcome := "success"
	defer func() {
		metrics.CallbackHandlerDuration.WithLabelValues(metricLabel, outcome).Observe(time.Since(start).Seconds())
	}()

	txids := msg.ResolveSeenTxIDs()
	metrics.CallbackBatchSize.WithLabelValues(metricLabel).Observe(float64(len(txids)))
	if len(txids) == 0 {
		return nil
	}

	ctx := c.Request.Context()
	now := time.Now()

	// Tracker prefilter: drop txids whose in-memory tracked status already
	// eclipses targetStatus. At 100 TPS sustained ~91% of SEEN_ON_NETWORK
	// callback txids end up lattice-skipped at the store (merkle-service
	// re-fires after the tx is already past SEEN_ON_NETWORK). The tracker
	// is updated under the same RWMutex the rest of BSV Broadcaster uses and is
	// always ≤ the store's status, so a prefilter hit is provably safe —
	// the store-side lattice remains authoritative for anything the
	// tracker doesn't know about. keptTxIDs maps the post-filter index
	// back to the original txid so per-row metric labels stay accurate.
	statuses := make([]*models.TransactionStatus, 0, len(txids))
	keptTxIDs := make([]string, 0, len(txids))
	for _, txid := range txids {
		if s.txTracker != nil {
			if prevStatus, ok := s.txTracker.GetStatus(txid); ok {
				if prevStatus == targetStatus || !targetStatus.CanTransitionFrom(prevStatus) {
					metrics.CallbackStaleTotal.WithLabelValues(metricLabel, string(prevStatus)).Inc()
					continue
				}
			}
		}
		statuses = append(statuses, &models.TransactionStatus{
			TxID:      txid,
			Status:    targetStatus,
			Timestamp: now,
		})
		keptTxIDs = append(keptTxIDs, txid)
	}
	if len(statuses) == 0 {
		return nil
	}

	updates, err := s.store.BatchUpdateStatusReturning(ctx, statuses)
	if err != nil {
		outcome = "error"
		logger.Error(
			"batch update seen status failed",
			zap.String("type", metricLabel),
			zap.Int("batch_size", len(txids)),
			zap.Error(err),
		)
		// Rows that landed before the error are still published below.
		// The error is returned so the HTTP handler answers 500 and Merkle
		// retries. A retry is safe: the lattice no-ops txids already at
		// targetStatus. outcome stays "error" even if some results are
		// empty, so the duration histogram does not report a partial success.
	}

	successful := make([]string, 0, len(statuses))
	for i, upd := range updates {
		if upd.Applied() {
			// Observe transition age — the headline metric the user asked for.
			// Use the previous row's Timestamp (last-update wall-clock) as the
			// anchor; for the RECEIVED→SEEN_ON_NETWORK case it equals the
			// time the validator marked the tx RECEIVED, which is exactly
			// the latency we want to optimize against.
			if !upd.Prev.Timestamp.IsZero() {
				metrics.StatusTransitionAge.
					WithLabelValues(string(upd.Prev.Status), string(targetStatus)).
					Observe(time.Since(upd.Prev.Timestamp).Seconds())
			}
			// An applied idempotent re-assert: the row was already at
			// targetStatus, so there is no transition to fan out. A non-nil
			// Prev already implies the lattice allowed the move.
			if upd.Prev.Status == targetStatus {
				metrics.CallbackStaleTotal.WithLabelValues(metricLabel, string(upd.Prev.Status)).Inc()
				continue
			}
			successful = append(successful, keptTxIDs[i])
			if s.txTracker != nil {
				s.txTracker.UpdateStatus(keptTxIDs[i], targetStatus)
			}
			continue
		}
		if upd.Current != nil {
			// Known row, transition not applied. Do not publish, do not
			// move txTracker, and do not count the txid as unknown.
			metrics.CallbackStaleTotal.WithLabelValues(metricLabel, string(upd.Current.Status)).Inc()
			continue
		}
		// Genuinely absent, or a per-row failure inside a batch error.
		// Failed rows are indistinguishable from unknown txids. Do not
		// count them as unknown: the HTTP handler returns 500 and Merkle
		// retries.
		if outcome == "error" {
			continue
		}
		outcome = "partial"
		metrics.CallbackUnknownTxIDTotal.WithLabelValues(metricLabel).Inc()
		logger.Warn("dropping callback for unknown txid",
			zap.String("type", metricLabel),
			logfields.TxID(keptTxIDs[i]))
	}

	// Full-coverage SEEN line(s): this callback path is driven by
	// merkle-service (not the client submit request), so it's off the
	// synchronous hot path — emit every seen txid, chunked and never capped,
	// matching MINED, so a txid search finds the SEEN transition.
	logfields.ForEachTxIDChunk(successful, func(chunk []string, chunkIdx, totalChunks int) {
		logger.Info(
			"transactions seen",
			logfields.Status(string(targetStatus)),
			logfields.TxIDCount(len(successful)),
			logfields.TxIDs(chunk),
			zap.Int("chunk_index", chunkIdx),
			zap.Int("chunk_total", totalChunks),
		)
	})

	// Single PublishBulk for the whole batch — drops the per-txid Kafka send
	// down to one event regardless of N. Subscribers (SSE, webhook) unfan in
	// their own handler, where they pay the per-tx cost without saturating
	// the bounded work queue.
	if len(successful) > 0 && s.publisher != nil {
		template := &models.TransactionStatus{
			Status:    targetStatus,
			Timestamp: now,
			TxIDs:     successful,
		}
		if pubErr := s.publisher.PublishBulk(ctx, template); pubErr != nil {
			logger.Warn(
				"failed to publish bulk seen-status",
				zap.String("type", metricLabel),
				zap.Int("count", len(successful)),
				zap.Error(pubErr),
			)
		}
	}
	return err
}

func (s *Server) handleStump(c *gin.Context, msg models.CallbackMessage, logger *zap.Logger) {
	if msg.BlockHash == "" || len(msg.Stump) == 0 {
		logger.Warn("incomplete STUMP callback")
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "blockHash and stump are required"})
		return
	}

	// Refuse a STUMP that does not parse. Rows are content-addressed, so a
	// bad copy is never overwritten by a good resend; it would sit beside
	// the good one and fail every build and redelivery of the block until
	// the Kafka message dead-letters. Rejecting it here surfaces the problem
	// at the sending merkle-service instead.
	if mp, err := sdkTx.NewMerklePathFromBinary(msg.Stump); err != nil || len(mp.Path) == 0 {
		logger.Warn("malformed STUMP callback",
			logfields.BlockHash(msg.BlockHash),
			zap.Int("subtree_index", msg.SubtreeIndex),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "stump is not a valid BRC-74 merkle path"})
		return
	}

	// Store STUMP keyed by (blockHash, subtreeIndex, contentHash). Synchronous
	// write so that a 200 to merkle-service is a durability guarantee —
	// merkle-service only fires BLOCK_PROCESSED after all STUMPs succeed, so
	// the bump builder can then rely on finding them all. Content addressing
	// makes the same STUMP from a second merkle-service a no-op and keeps a
	// divergent one (a service that missed registrations) as its own row.
	stump := models.NewStump(msg.BlockHash, msg.SubtreeIndex, msg.Stump)
	if err := s.store.InsertStump(c.Request.Context(), stump); err != nil {
		logger.Error("failed to store STUMP", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to store stump"})
		return
	}

	logger.Info(
		"stump stored",
		logfields.BlockHash(msg.BlockHash),
		zap.Int("subtree_index", msg.SubtreeIndex),
	)
	c.Status(http.StatusOK)
}

func (s *Server) handleBlockProcessed(c *gin.Context, msg models.CallbackMessage, logger *zap.Logger) {
	if msg.BlockHash == "" {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "blockHash is required"})
		return
	}
	// Do NOT stamp processed_at here. processed_at is the watchdog's
	// recovery signal — a block_processing row with header_seen_at set (by the
	// chaintracks UpsertBlockHeaderSeen path, which is authoritative for height)
	// but processed_at NULL is what ListStaleBlockProcessingStatus picks up to
	// re-drive via /reprocess. Stamping it the instant BLOCK_PROCESSED arrives
	// (as this handler used to) blinds the watchdog to the silent partial-STUMP
	// loss: bump-builder may receive an incomplete STUMP set and build a BUMP
	// that drops the missing subtree's txs, yet processed_at would already be
	// set so the block could never be recovered. Ownership of the stamp now
	// belongs to bump-builder, which sets it only once the block is genuinely
	// finalized (all expected STUMPs present and the BUMP built/validated).
	//
	// Recovery of a deferred (incomplete) block relies on the watchdog finding a
	// block_processing row with processed_at NULL — which requires a real-height
	// header_seen_at row from the chaintracks UpsertBlockHeaderSeen path (the
	// watchdog's stale query filters on both header_seen_at and block_height, so
	// the height-0 placeholder this handler used to write was never recoverable
	// anyway). That chaintracks dependency is pre-existing and unchanged here;
	// see services/watchdog.
	if err := s.producer.Send(c.Request.Context(), kafka.TopicBlockProcessed, msg.BlockHash, msg); err != nil {
		logger.Error("failed to publish block_processed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to enqueue"})
		return
	}
	logger.Info("block_processed enqueued", logfields.BlockHash(msg.BlockHash))
	c.Status(http.StatusOK)
}

// callbackDelivery is the wire shape of one subscription's webhook delivery
// state on GET /tx?callbackToken=… — the self-diagnosis surface for receivers
// whose edge is rejecting BSV Broadcaster's POSTs (issue #249: a WAF 403'd 24k
// callbacks while the receiver's own logs showed nothing). attempts /
// lastAttemptAt / lastResult are lifetime bookkeeping stamped on every POST;
// retry-scheduling state (nextRetryAt) is per-transition and cleared when the
// next transition is claimed. Deliberately excludes CallbackToken.
type callbackDelivery struct {
	CallbackURL         string     `json:"callbackUrl,omitempty"`
	LastDeliveredStatus string     `json:"lastDeliveredStatus,omitempty"`
	Attempts            int        `json:"attempts"`
	LastAttemptAt       *time.Time `json:"lastAttemptAt,omitempty"`
	LastResult          string     `json:"lastResult,omitempty"`
	NextRetryAt         *time.Time `json:"nextRetryAt,omitempty"`
}

// txStatusWithCallbacks embeds the plain status so the base response shape is
// byte-identical to today's, plus the token-scoped callbacks array.
type txStatusWithCallbacks struct {
	*models.TransactionStatus

	Callbacks []callbackDelivery `json:"callbacks,omitempty"`
}

// handleGetTransaction retrieves a transaction status by TXID. With an
// optional ?callbackToken=… the response additionally carries the webhook
// delivery state of the submissions registered under THAT token (capability
// model — same scoping the SSE stream uses). Without the token, or when no
// submission matches, the response is unchanged: a txid can carry
// subscriptions from multiple unrelated clients, and their callback URLs and
// delivery health are not each other's business.
func (s *Server) handleGetTransaction(c *gin.Context) {
	txid := c.Param("txid")
	if txid == "" {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "txid is required"})
		return
	}

	status, err := s.store.GetStatus(c.Request.Context(), txid)
	if err != nil {
		s.logger.Error("failed to get status", logfields.TxID(txid), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "internal error"})
		return
	}

	if status == nil {
		c.JSON(http.StatusNotFound, gin.H{jsonKeyError: "transaction not found"})
		return
	}

	if callbacks := s.callbackDeliveries(c.Request.Context(), txid, c.Query("callbackToken")); len(callbacks) > 0 {
		c.JSON(http.StatusOK, txStatusWithCallbacks{TransactionStatus: status, Callbacks: callbacks})
		return
	}
	c.JSON(http.StatusOK, status)
}

// callbackDeliveries returns the delivery state of the txid's submissions
// registered under token. Empty token or no match returns nil (never an
// error to the client — the callbacks view is best-effort diagnostics and
// must not break the status read).
func (s *Server) callbackDeliveries(ctx context.Context, txid, token string) []callbackDelivery {
	if token == "" || s.store == nil {
		return nil
	}
	subs, err := s.store.GetSubmissionsByTxID(ctx, txid)
	if err != nil {
		s.logger.Warn("submission lookup for callback state failed", logfields.TxID(txid), zap.Error(err))
		return nil
	}
	var out []callbackDelivery
	for _, sub := range subs {
		if sub.CallbackToken != token {
			continue
		}
		out = append(out, callbackDelivery{
			CallbackURL:         sub.CallbackURL,
			LastDeliveredStatus: string(sub.LastDeliveredStatus),
			Attempts:            sub.Attempts,
			LastAttemptAt:       sub.LastAttemptAt,
			LastResult:          sub.LastResult,
			NextRetryAt:         sub.NextRetryAt,
		})
	}
	return out
}

// Per-request body size caps for the submit endpoints. A BSV transaction can
// legally be quite large, but at the API boundary we want a hard upper bound
// so a single client can't exhaust memory with a crafted body. Sized for a
// generous single transaction and a generous batch.
const (
	maxSingleTxBytes = 32 << 20  // 32 MiB per single-tx submit
	maxBatchBytes    = 256 << 20 // 256 MiB per batch submit
)

// submitResponse is the ARC-compatible body for POST /tx. status mirrors the
// HTTP code (202 Accepted) as an integer — ARC clients deserialise status as an
// integer and abort on a string, so a fully-accepted tx looked like a failure
// (issue #209). txStatus carries the lifecycle state and txid lets the client
// track the submission, matching the GET /tx read shape (models.TransactionStatus).
type submitResponse struct {
	TxID     string        `json:"txid"`
	Status   int           `json:"status"`
	TxStatus models.Status `json:"txStatus"`
}

// handleSubmitTransaction accepts transactions for validation and propagation.
// Supports application/octet-stream, text/plain (hex), and JSON.
func (s *Server) handleSubmitTransaction(c *gin.Context) {
	// SSRF guard: reject before reading the body so a hostile client can't
	// exhaust ingress bandwidth alongside a banned callback host. The same
	// predicate runs again at dial time on the webhook delivery client to
	// catch DNS rebinding.
	opts := extractSubmitOptions(c)
	opts.ExpiresAt = s.retentionExpiry(c)
	if !s.validateCallbackURL(c, opts.CallbackURL) {
		return
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxSingleTxBytes)

	var rawTx []byte

	contentType := c.ContentType()
	switch {
	case strings.Contains(contentType, "octet-stream"):
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "failed to read body"})
			return
		}
		rawTx = body
	case strings.Contains(contentType, "text/plain"):
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "failed to read body"})
			return
		}
		decoded, err := hex.DecodeString(strings.TrimSpace(string(body)))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "invalid hex"})
			return
		}
		rawTx = decoded
	default:
		// JSON format
		var req struct {
			RawTx string `json:"rawTx"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "invalid request"})
			return
		}
		decoded, err := hex.DecodeString(req.RawTx)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "invalid hex in rawTx"})
			return
		}
		rawTx = decoded
	}

	if len(rawTx) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "empty transaction"})
		return
	}

	// Parse the wire bytes so we can derive the canonical txid from the
	// transaction structure. Hashing the wire bytes directly is wrong when
	// clients submit Extended Format (which includes per-input source
	// metadata) — the resulting hash matches the EF blob, not the canonical
	// Bitcoin txid the validator emits later, so SSE/webhook callbacks
	// (keyed by submissions.txid) silently fail to match.
	parsedTx, used, err := sdkTx.NewTransactionFromStream(rawTx)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "failed to parse transaction"})
		return
	}
	// Keep only the tx, as /txs does: the bytes are stored and later
	// concatenated into a Teranode /txs stream, where anything trailing
	// would be read as the start of the next tx.
	rawTx = rawTx[:used]

	// Publish to transaction topic for validation. The partition key is the
	// txid, or — when the submission spends an output of a transaction still
	// in flight through this broadcaster — that parent's key, so the child
	// lands on the dispatcher holding its parent (partitionKeyForSubmit). A
	// re-submission (retry, user double-post) keeps its row's key and lands
	// on the same consumer.
	//
	// Raw tx bytes travel as []byte in the JSON payload (encoded as base64 by
	// encoding/json) so the validator and propagator never hex-decode the body
	// and re-encode it downstream.
	txid := parsedTx.TxID().String()

	// Synchronous policy, script, and fee validation. Merkle proofs on
	// BEEF parents are trusted (BSV Broadcaster doesn't own a chaintracker on the
	// intake path); script execution against per-input source data still
	// runs, which catches malformed unlocking scripts, bad signatures,
	// and underpaid txs. Failures are a verdict about the transaction
	// bytes, so they write a terminal REJECTED row and return 400 —
	// durable and immediate.
	if s.validator != nil {
		if err := s.validator.ValidateTransaction(c.Request.Context(), parsedTx, false); err != nil {
			s.rejectAtIntake(c.Request.Context(), txid, err.Error(), arcStatusCodeOf(err), opts)
			respondSubmitError(c, txid, err)
			return
		}
	}

	// nLockTime/BIP113 finality gate. The embedded validator above does not
	// check finality (teranode enforces it a layer above the TxValidator it
	// wraps), so without this a non-final tx is broadcast and bounced by the
	// network with an opaque generic error (issue #245). Runs after the
	// validator so its fail-open behavior can never mask a validation error.
	//
	// Unlike a validator failure, a non-final verdict reflects BSV Broadcaster's
	// CURRENT CHAIN VIEW (which can trail the network's real tip), not the
	// transaction bytes — a condition, not a verdict. So it is deliberately
	// NOT persisted: no REJECTED row, no submission record, no status event
	// (issue #254). The client gets the actionable 400 below; GET /tx keeps
	// returning its prior state — 404 ("no verdict") for a fresh tx, so
	// resubmission policies keyed on no-verdict work unchanged — and a
	// resubmit evaluated against a stale tip can never clobber an existing
	// in-flight row.
	if fErr := s.finality.Check(c.Request.Context(), parsedTx); fErr != nil {
		fErr = arcerrors.NewArcError(fErr, arcerrors.StatusNotFinal)
		metrics.APIFinalityRejectionsTotal.WithLabelValues("/tx").Inc()
		s.logger.Info(
			"transaction not final at intake; no verdict persisted",
			logfields.TxID(txid),
			zap.String("reason", fErr.Error()),
			logfields.Stage(logfields.StageIntake),
		)
		respondSubmitError(c, txid, fErr)
		return
	}

	// Dedup CAS via GetOrInsertStatus. Two submitters racing on the
	// same txid both attempt the insert; the loser sees inserted=false
	// and returns 202 idempotently without re-publishing.
	inputTXIDs := collectInputTXIDs(parsedTx)
	partitionKey, ownSeen := s.partitionKeyForSubmit(c.Request.Context(), txid, inputTXIDs)
	submitResult := "new"
	// inserted reports whether THIS request's CAS created the row; only then
	// does it publish RECEIVED (see below). Without a store there is no CAS
	// to coordinate with, so every submission counts as the first.
	inserted := s.store == nil
	// restore: the CAS found a row this request must publish again, so it
	// gets this request's bytes first (see restoreRawTxs).
	restore := false
	receivedAt := time.Now()
	if s.store != nil {
		row := &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusReceived,
			Timestamp: receivedAt,
			// Carry the raw bytes on the status row so the propagation
			// reaper can rebroadcast txs that are stuck in non-terminal
			// states without re-fetching from Kafka or the API caller.
			RawTx: rawTx,
			// Persist the key so a child submitted later can follow it.
			PartitionKey: partitionKey,
			ExpiresAt:    opts.ExpiresAt,
		}
		var existing *models.TransactionStatus
		var dedupErr error
		existing, inserted, dedupErr = s.store.GetOrInsertStatus(c.Request.Context(), row)
		switch {
		case dedupErr != nil || (!inserted && existing == nil):
			// The propagation message carries the txid only (#329), so a
			// row that may not have landed cannot be published: the
			// consumer would have no raw tx to load. Shed it like
			// backpressure; the retry's CAS inserts or finds the row.
			s.logger.Error("dedup CAS failed", logfields.TxID(txid), zap.Error(dedupErr))
			respondStoreUnavailable(c)
			return
		case s.extendOnResubmit(c.Request.Context(), txid, existing, inserted, opts.ExpiresAt) != nil:
			// A resubmit whose row could not be extended to this caller's
			// retention: nothing published yet, the retry repeats the extend.
			respondStoreUnavailable(c)
			return
		case !inserted && shouldRepublishExisting(existing.Status):
			restore = true
			// REJECTED resubmit: re-run broadcast. RECEIVED resubmit: the
			// first attempt wrote the row then failed to Kafka-publish
			// (503/500). Fall through so this retry actually queues.
			if existing.Status == models.StatusRejected {
				submitResult = "retry_rejected"
			} else {
				submitResult = "retry_received"
			}
			// Republish under the key the row was stored with: a concurrent
			// identical submission may have won the CAS with a different key.
			// A row seen by the pre-CAS lookup already supplied its key.
			partitionKey = s.storedPartitionKey(c.Request.Context(), txid, partitionKey, ownSeen)
		case !inserted && existing != nil:
			// Idempotent re-submit: row already exists at a non-REJECTED
			// status. Register the txid with the in-process TxTracker so
			// bump-builder's tracked-only filtering recognizes it after a
			// process restart, then echo back the existing state without
			// re-publishing.
			if s.txTracker != nil {
				s.txTracker.Add(txid, existing.Status)
			}
			metrics.APITxsSubmittedTotal.WithLabelValues("/tx", "duplicate").Inc()
			s.recordSubmission(c.Request.Context(), txid, opts)
			c.JSON(http.StatusAccepted, submitResponse{
				TxID:     txid,
				Status:   http.StatusAccepted,
				TxStatus: existing.Status,
			})
			return
		}
	}

	// A row this request did not create is republished with this
	// request's bytes (see restoreRawTxs).
	if restore {
		if err := s.restoreRawTxs(c.Request.Context(), map[string][]byte{txid: rawTx}); err != nil {
			respondStoreUnavailable(c)
			return
		}
	}

	metrics.APITxsSubmittedTotal.WithLabelValues("/tx", submitResult).Inc()

	// Register the tx with the in-process TxTracker so the bump-builder
	// recognizes it when its block is processed.
	if s.txTracker != nil {
		s.txTracker.Add(txid, models.StatusReceived)
	}

	// Record the callback subscription BEFORE publishing to Kafka so
	// any status events fired on this txid can find a matching row.
	s.recordSubmission(c.Request.Context(), txid, opts)

	// RECEIVED goes out before this request's propagation message, and only
	// from the request whose CAS created the row: any other attempt — a
	// REJECTED resubmit, a retry of a stranded RECEIVED row, an attempt whose
	// CAS errored — can run after an earlier attempt's verdict. Neither rule
	// orders RECEIVED against a verdict produced by ANOTHER request's
	// propagation (this request can pause between its CAS and this publish),
	// and nothing here can across replicas. What keeps the stream true is the
	// SSE writer: it skips a RECEIVED once its stream has carried a later
	// status for the txid (sse.Client.admit). Webhooks never post RECEIVED.
	// These rules just keep the topic from carrying RECEIVED events that would
	// nearly always be skipped.
	if inserted {
		s.publishReceived(c.Request.Context(), []string{txid}, receivedAt, opts)
	}

	msg := propagationMessage(txid)
	if err := s.producer.Send(c.Request.Context(), kafka.TopicPropagation, partitionKey, msg); err != nil {
		if errors.Is(err, kafka.ErrBrokerBackpressure) {
			// Backpressure → shed load to the client. A RECEIVED row may
			// already exist; shouldRepublishExisting makes the 503 retry
			// re-publish instead of echoing 202 with no Kafka message.
			s.logger.Warn("submit rejected: kafka backpressure", logfields.TxID(txid))
			c.Header("Retry-After", "1")
			c.JSON(http.StatusServiceUnavailable, gin.H{jsonKeyError: "service overloaded, retry shortly"})
			return
		}
		s.logger.Error("failed to publish transaction", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to submit"})
		return
	}

	telemetry.LoggerWith(c.Request.Context(), s.logger).Info(
		"transaction received",
		logfields.TxID(txid),
		logfields.Status(string(models.StatusReceived)),
	)

	c.JSON(http.StatusAccepted, submitResponse{
		TxID:     txid,
		Status:   http.StatusAccepted,
		TxStatus: models.StatusReceived,
	})
}

// publishReceived fans the RECEIVED status onto the events publisher once the
// RECEIVED row is durable and before the propagation message is queued, so a
// subscriber that opened its stream before submitting sees the lifecycle from
// its first state rather than from ACCEPTED_BY_NETWORK (hosted-service
// report, issue 6d). The request's callback token rides along as a hint
// because the submission row is written by the asynchronous recorder and may
// not exist yet when the SSE pod resolves the txid's subscribers. Callers pass
// only txids whose row this request's CAS inserted. The event can still trail
// a verdict another request's attempt produced; the SSE writer skips it then
// (see handleSubmitTransaction). Best-effort: the row is already durable, and
// connect-time catchup covers a lost event. A failed Kafka send after this
// leaves the row at RECEIVED, which the client's retry re-queues without a
// second RECEIVED, so the frame stays true.
//
// receivedAt is the inserted rows' timestamp (the earliest, for a batch), so
// the frame's SSE id sits in the store's timestamp domain that a reconnect's
// Last-Event-ID catchup resumes from, rather than a later publish-time clock
// reading. A batch goes out in chunks of maxTxIDsPerReceivedEvent.
func (s *Server) publishReceived(ctx context.Context, txids []string, receivedAt time.Time, opts submitOptions) {
	if s.publisher == nil || len(txids) == 0 {
		return
	}
	if len(txids) == 1 {
		st := &models.TransactionStatus{
			TxID:              txids[0],
			Status:            models.StatusReceived,
			Timestamp:         receivedAt,
			CallbackTokenHint: opts.CallbackToken,
		}
		if err := s.publisher.Publish(ctx, st); err != nil {
			s.logger.Warn("RECEIVED publish failed", zap.Int("count", 1), zap.Error(err))
		}
		return
	}
	for start := 0; start < len(txids); start += maxTxIDsPerReceivedEvent {
		chunk := txids[start:min(start+maxTxIDsPerReceivedEvent, len(txids))]
		st := &models.TransactionStatus{
			TxIDs:             chunk,
			Status:            models.StatusReceived,
			Timestamp:         receivedAt,
			CallbackTokenHint: opts.CallbackToken,
		}
		if err := s.publisher.PublishBulk(ctx, st); err != nil {
			s.logger.Warn("RECEIVED publish failed", zap.Int("count", len(chunk)), zap.Error(err))
		}
	}
}

// bulkReceivedAt is the timestamp a POST /txs RECEIVED event carries. Its SSE
// id is a reconnect's Last-Event-ID, and catchup replays rows strictly after
// it, so it must sit below every row of the batch at the precision the store
// keeps (milliseconds on Aerospike and MongoDB): the earliest row's
// millisecond, minus 1ns. A row replayed twice is harmless; one skipped never
// gets its RECEIVED frame. A single-tx event keeps the row's own timestamp.
func bulkReceivedAt(earliest time.Time, n int) time.Time {
	if n < 2 {
		return earliest
	}
	return earliest.Truncate(time.Millisecond).Add(-time.Nanosecond)
}

// maxTxIDsPerReceivedEvent caps the txids in one bulk RECEIVED event so a
// large POST /txs stays under the Kafka producer's 1 MiB default
// MaxMessageBytes (~67 bytes of JSON per txid). Mirrors bump_builder's
// maxTxIDsPerBulkEvent.
const maxTxIDsPerReceivedEvent = 5000

// arcStatusCodeOf extracts the numeric ARC status code (460-476 taxonomy,
// errors/errors.go) from an error chain, or 0 when none is attached.
func arcStatusCodeOf(err error) int {
	if arcErr := arcerrors.GetArcError(err); arcErr != nil {
		return int(arcErr.StatusCode)
	}
	return 0
}

// respondSubmitError writes the 400 for an intake validation or finality
// failure. The legacy "error"/"reason" fields are unchanged so existing
// clients keep parsing; "txid" and — when the error carries an ARC status
// code — the ARC error taxonomy ("status", "title", "detail", "type") are
// added alongside, so ARC-compatible clients can branch on the code (and
// judge retryability, e.g. 476 non-final) without matching reason text.
func respondSubmitError(c *gin.Context, txid string, err error) {
	body := gin.H{
		jsonKeyError: "transaction failed validation",
		"txid":       txid,
		"reason":     err.Error(),
	}
	if arcErr := arcerrors.GetArcError(err); arcErr != nil {
		f := arcErr.ToErrorFields()
		body["status"] = f.Status
		body["title"] = f.Title
		body["detail"] = f.Detail
		body["type"] = f.Type
	}
	c.JSON(http.StatusBadRequest, body)
}

// rejectAtIntake is the terminal-rejection counterpart to the intake
// success sequence, for VALIDATOR failures only — verdicts about the
// transaction bytes. (Finality failures are conditions of BSV Broadcaster's chain
// view and deliberately persist nothing; see the finality gate in
// handleSubmitTransaction.) It persists a REJECTED row, records the
// submission so SSE/webhook can resolve the callback URL+token on
// delivery, then publishes the REJECTED status to TopicStatusUpdate so
// live subscribers see the terminal outcome. Every step is best-effort —
// the client has already received its 400 by the time this runs, so a
// store or publish failure does not change the HTTP outcome.
//
// statusCode is the numeric ARC code (0 when unclassified) persisted and
// published alongside the reason so consumers get a machine-readable
// cause, not just prose.
//
// Order matches the success path: persist row, then queue submission,
// then publish. Queuing the submission before the publish minimizes
// the window in which an SSE/webhook subscriber receives the event
// without a matching submission row (the recorder pool is async, so
// the window can't be eliminated entirely without making the row
// write synchronous — a trade-off the success path also accepts).
func (s *Server) rejectAtIntake(ctx context.Context, txid, reason string, statusCode int, opts submitOptions) {
	s.logger.Info(
		"transaction rejected",
		logfields.TxID(txid),
		zap.String("reason", reason),
		logfields.Stage(logfields.StageIntake),
	)
	wrote := s.persistRejectedAtIntake(ctx, txid, reason, statusCode, opts.ExpiresAt)
	s.recordSubmission(ctx, txid, opts)
	if !wrote || s.publisher == nil {
		return
	}
	status := &models.TransactionStatus{
		TxID:       txid,
		Status:     models.StatusRejected,
		StatusCode: statusCode,
		ExtraInfo:  reason,
		Timestamp:  time.Now(),
		// Same race as publishReceived: the submission row may not exist yet.
		CallbackTokenHint: opts.CallbackToken,
	}
	if err := s.publisher.Publish(ctx, status); err != nil {
		s.logger.Warn(
			"intake rejection publish failed",
			logfields.TxID(txid),
			zap.Error(err),
		)
	}
}

// persistRejectedAtIntake writes a terminal REJECTED row for a tx that
// failed validation at the intake handler. Best-effort: a write
// failure is logged but doesn't change the client response (the 400
// has already told them the tx was rejected). When the store is nil
// (test setups using struct-literal construction), this is a no-op and
// reports wrote=true so the REJECTED event still publishes.
//
// wrote is false when an existing row already forbids a REJECTED
// transition (SEEN_*/MINED/IMMUTABLE — issue #251). Callers must not
// publish a REJECTED status event in that case.
func (s *Server) persistRejectedAtIntake(ctx context.Context, txid, reason string, statusCode int, expiresAt *time.Time) (wrote bool) {
	if s.store == nil {
		return true
	}
	row := &models.TransactionStatus{
		TxID:       txid,
		Status:     models.StatusRejected,
		StatusCode: statusCode,
		ExtraInfo:  reason,
		Timestamp:  time.Now(),
		ExpiresAt:  expiresAt,
	}
	existing, inserted, err := s.store.GetOrInsertStatus(ctx, row)
	// The client already has its 400; a failed extension here is logged and
	// counted (extendOnResubmit), and the row keeps its earlier retention.
	if err == nil {
		_ = s.extendOnResubmit(ctx, txid, existing, inserted, expiresAt)
	}
	if err != nil {
		s.logger.Warn(
			"intake rejection persist failed",
			logfields.TxID(txid),
			zap.Error(err),
		)
		return true
	}
	if !inserted {
		if existing != nil && !models.StatusRejected.CanTransitionFrom(existing.Status) {
			s.logger.Info(
				"intake rejection ignored; existing status forbids REJECTED",
				logfields.TxID(txid),
				zap.String("existing_status", string(existing.Status)),
			)
			return false
		}
		if err := s.store.UpdateStatus(ctx, row); err != nil {
			s.logger.Warn(
				"intake rejection status update failed",
				logfields.TxID(txid),
				zap.Error(err),
			)
		}
	}
	return true
}

// handleSubmitTransactions accepts a batch of concatenated raw transactions.
// Mirrors handleSubmitTransaction's intake pipeline (parse → validate →
// dedup CAS → publish to TopicPropagation) for each tx in the batch. The
// publish is a single SendBatch so all txs in one HTTP request hit the
// in-memory broker as one fan-out. Each tx carries its input_txids so
// the propagation dispatcher can detect parent-child relationships and
// hold children until their parents have terminalized.
func (s *Server) handleSubmitTransactions(c *gin.Context) {
	if !strings.Contains(c.ContentType(), "octet-stream") {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "Content-Type must be application/octet-stream"})
		return
	}

	// SSRF guard: reject early so a hostile client posting a 256 MiB batch
	// with a banned callback host doesn't get to consume any ingress.
	opts := extractSubmitOptions(c)
	opts.ExpiresAt = s.retentionExpiry(c)
	if !s.validateCallbackURL(c, opts.CallbackURL) {
		return
	}

	// maxBatch caps the body itself (when the caller's tier bounds the
	// batch) on top of the flat maxBatchBytes ceiling, and -- in the parse
	// loop below -- stops parsing as soon as the cap is exceeded, instead
	// of reading, parsing and holding the whole oversize body in memory
	// before CheckBatch gets a chance to reject it.
	// Compared by division, not maxBatch*maxSingleTxBytes, which an
	// unbounded tier's (unvalidated) max_batch could overflow.
	bodyLimit := int64(maxBatchBytes)
	maxBatch := ratelimit.MaxBatch(c)
	tierCapped := maxBatch > 0 && int64(maxBatch) < bodyLimit/maxSingleTxBytes
	if tierCapped {
		bodyLimit = int64(maxBatch) * maxSingleTxBytes
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, bodyLimit)

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		// http.MaxBytesReader surfaces a *http.MaxBytesError once bodyLimit
		// is exceeded; map it to 413, the same way handleCallback does,
		// rather than the generic 400 every other read failure gets.
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			ratelimit.RejectBatchBytesTooLarge(c, tierCapped)
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "failed to read body"})
		return
	}
	if len(body) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "empty body"})
		return
	}

	// Phase 1: parse the whole stream. Use the canonical TxID() (not a
	// hash of the wire bytes) so Extended Format submissions key the
	// same as the canonical txid the propagator broadcasts.
	var parsed []parsedItem
	offset := 0
	for offset < len(body) {
		tx, bytesUsed, parseErr := sdkTx.NewTransactionFromStream(body[offset:])
		if parseErr != nil {
			s.logger.Error(
				"failed to parse transaction in batch",
				zap.Int("offset", offset),
				zap.Int("parsed", len(parsed)),
				zap.Error(parseErr),
			)
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "failed to parse transaction", "parsed": len(parsed)})
			return
		}
		if bytesUsed == 0 {
			break
		}
		parsed = append(parsed, parsedItem{
			tx:   tx,
			raw:  body[offset : offset+bytesUsed],
			txid: tx.TxID().String(),
		})
		offset += bytesUsed
		if maxBatch > 0 && len(parsed) > maxBatch {
			// Abort parsing the instant the batch cap is exceeded -- no
			// need to parse the rest of a body that's already rejected --
			// and reuse CheckBatch's own response so the body and the
			// batch_too_large metric stay identical to the full-batch
			// rejection below.
			ratelimit.CheckBatch(c, len(parsed))
			return
		}
	}
	if len(parsed) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "no transactions parsed"})
		return
	}

	// Tier batch cap, then the per-transaction quota charge, both before
	// validation: a rejected batch costs no validation CPU and publishes
	// nothing. No-ops when rate limiting is off. Batches at or under
	// maxBatch reach here; CheckBatch is otherwise a no-op repeat of the
	// cutoff above.
	if !ratelimit.CheckBatch(c, len(parsed)) || !ratelimit.ChargeTxs(c, len(parsed)) {
		return
	}
	usage.SetTxCount(c, len(parsed))

	// Phase 2: synchronous policy + script + fee validation per tx
	// (merkle proofs on BEEF parents are trusted; see
	// handleSubmitTransaction for the rationale). Any failure aborts
	// the whole batch with 400 — matches the single-tx contract. The
	// failed tx gets a durable REJECTED row before the response so
	// SSE/webhook subscribers can resolve the outcome.
	if s.validator != nil {
		ctx := c.Request.Context()
		for _, p := range parsed {
			if vErr := s.validator.ValidateTransaction(ctx, p.tx, false); vErr != nil {
				s.rejectAtIntake(ctx, p.txid, vErr.Error(), arcStatusCodeOf(vErr), opts)
				respondSubmitError(c, p.txid, vErr)
				return
			}
		}
	}

	// nLockTime/BIP113 finality gate per tx; see handleSubmitTransaction for
	// the rationale — including why a non-final verdict is NOT persisted
	// (condition of BSV Broadcaster's chain view, not a verdict about the tx; issue
	// #254). Aborts the whole batch naming the offending txid — matches the
	// validation contract above.
	for _, p := range parsed {
		if fErr := s.finality.Check(c.Request.Context(), p.tx); fErr != nil {
			fErr = arcerrors.NewArcError(fErr, arcerrors.StatusNotFinal)
			metrics.APIFinalityRejectionsTotal.WithLabelValues("/txs").Inc()
			s.logger.Info(
				"transaction not final at intake; no verdict persisted",
				logfields.TxID(p.txid),
				zap.String("reason", fErr.Error()),
				logfields.Stage(logfields.StageIntake),
			)
			respondSubmitError(c, p.txid, fErr)
			return
		}
	}

	// Partition keys, computed over the FULL parsed batch (see
	// familyKeysBySubmittedTxid) and before the CAS so every row persists the
	// key its message will carry. A family keeps a member's stored key (a
	// retry rejoins its earlier messages), else joins the partition of an
	// in-flight parent outside this batch (inheritStoredPartitionKeys).
	keyByTxid, inputsByTxid := familyKeysBySubmittedTxid(parsed)
	famByTxid := maps.Clone(keyByTxid) // family identity; inheritance may merge routing keys
	s.inheritStoredPartitionKeys(c.Request.Context(), keyByTxid, inputsByTxid)

	// Phase 3: dedup CAS per tx. Duplicates are skipped from the
	// publish but counted in the response so the caller can reconcile.
	// A dedup error fails the batch with 503 — matches single-tx.
	toPublish := make([]parsedItem, 0, len(parsed))
	// republished collects CAS losers re-entering toPublish, with the status
	// that selected them; their stored keys win over this request's choice
	// (adoptStoredFamilyKeys).
	republished := make(map[string]models.Status)
	// receivedTxIDs is the subset of toPublish whose row this request's CAS
	// inserted: only the first attempt publishes RECEIVED, since any other
	// attempt can run after an earlier attempt's verdict was published (see
	// handleSubmitTransaction).
	receivedTxIDs := make([]string, 0, len(parsed))
	duplicates := 0
	newSubmits, retryRejected := 0, 0
	var receivedAt time.Time // the first inserted row's timestamp
	if s.store != nil {
		ctx := c.Request.Context()
		for _, p := range parsed {
			row := &models.TransactionStatus{
				TxID:      p.txid,
				Status:    models.StatusReceived,
				Timestamp: time.Now(),
				// Carry the raw bytes on the row so the propagation
				// reaper can rebroadcast stuck txs without re-fetching.
				RawTx:        p.raw,
				PartitionKey: keyByTxid[p.txid],
				ExpiresAt:    opts.ExpiresAt,
			}
			existing, inserted, dedupErr := s.store.GetOrInsertStatus(ctx, row)
			switch {
			case dedupErr != nil || (!inserted && existing == nil):
				// Row state unknown: a txid-only message (#329) cannot be
				// published for it. Fail the whole batch before anything
				// reaches Kafka; rows already inserted are RECEIVED, which
				// the retry republishes (shouldRepublishExisting).
				s.logger.Error("dedup CAS failed", logfields.TxID(p.txid), zap.Error(dedupErr))
				s.shedBatch(c, receivedTxIDs, receivedAt, opts)
				return
			case s.extendOnResubmit(ctx, p.txid, existing, inserted, opts.ExpiresAt) != nil:
				// Nothing reached Kafka yet; the retry repeats the extend.
				s.shedBatch(c, receivedTxIDs, receivedAt, opts)
				return
			case !inserted && shouldRepublishExisting(existing.Status):
				// REJECTED resubmit and stranded RECEIVED (Kafka send failed
				// after insert) both re-enter toPublish. Mirrors
				// handleSubmitTransaction, raw_tx restore included (below).
				// Seen/accepted rows stay duplicates.
				if existing.Status == models.StatusRejected {
					retryRejected++
				}
				toPublish = append(toPublish, p)
				republished[p.txid] = existing.Status
			case !inserted:
				duplicates++
				// Idempotent re-submit at a non-REJECTED status: register
				// the txid with the in-process TxTracker using the
				// persisted status so bump-builder's tracked-only filtering
				// recognizes it. Mirrors the single-submit dedup branch.
				if s.txTracker != nil {
					s.txTracker.Add(p.txid, existing.Status)
				}
				s.recordSubmission(ctx, p.txid, opts)
			default:
				newSubmits++
				toPublish = append(toPublish, p)
				// Each row keeps its own timestamp, so a large batch is not one
				// equal-timestamp run for catchup to drain; the event is
				// stamped just below the earliest (bulkReceivedAt), so a
				// Last-Event-ID reconnect mid-burst replays the rest instead
				// of skipping them.
				receivedAt = cmp.Or(receivedAt, row.Timestamp)
				receivedTxIDs = append(receivedTxIDs, p.txid)
			}
		}
	} else {
		toPublish = parsed
		newSubmits = len(parsed)
		receivedAt = time.Now()
		for _, p := range parsed {
			receivedTxIDs = append(receivedTxIDs, p.txid)
		}
	}
	// Put this request's bytes on every republished row (restoreRawTxs), as
	// handleSubmitTransaction does.
	raws := make(map[string][]byte, len(republished))
	for _, p := range toPublish {
		if _, ok := republished[p.txid]; ok {
			raws[p.txid] = p.raw
		}
	}
	if len(raws) > 0 {
		if err := s.restoreRawTxs(c.Request.Context(), raws); err != nil {
			s.shedBatch(c, receivedTxIDs, receivedAt, opts)
			return
		}
	}
	metrics.APITxsSubmittedTotal.WithLabelValues("/txs", "new").Add(float64(newSubmits))
	metrics.APITxsSubmittedTotal.WithLabelValues("/txs", "duplicate").Add(float64(duplicates))
	metrics.APITxsSubmittedTotal.WithLabelValues("/txs", "retry_rejected").Add(float64(retryRejected))

	// Register every accepted tx with the in-process TxTracker so the
	// bump-builder's filterTrackedTxids recognizes them when their block
	// is processed. Without this, tracked-only fan-out drops every MINED
	// transition and txs stay stuck at SEEN_ON_NETWORK forever.
	if s.txTracker != nil {
		for _, p := range toPublish {
			s.txTracker.Add(p.txid, models.StatusReceived)
		}
	}

	// Record subscriptions BEFORE publishing so any status events that
	// fire on these txids find a matching row.
	ctx := c.Request.Context()
	for _, p := range toPublish {
		s.recordSubmission(ctx, p.txid, opts)
	}

	// Phase 4: build propagation envelopes and publish as one batch.
	// Envelopes carry the txid only (#329); the consumer loads the raw tx
	// from the row written above and derives its parents for dep-aware
	// admission. Messages are keyed by dependency family (#295): txs of this
	// submission connected through in-batch spends share a key, so on a
	// multi-partition topic a whole family lands on one partition and
	// per-partition order still delivers parents before children.
	if len(toPublish) > 0 {
		s.adoptStoredFamilyKeys(ctx, keyByTxid, famByTxid, republished)
		// Before SendBatch, for the ordering reason in handleSubmitTransaction.
		s.publishReceived(ctx, receivedTxIDs, bulkReceivedAt(receivedAt, len(receivedTxIDs)), opts)

		msgs := propagationMessages(toPublish, keyByTxid)
		if err := s.producer.SendBatch(ctx, kafka.TopicPropagation, msgs); err != nil {
			if errors.Is(err, kafka.ErrBrokerBackpressure) {
				s.logger.Warn("batch submit rejected: kafka backpressure", zap.Int("count", len(msgs)))
				c.Header("Retry-After", "1")
				c.JSON(http.StatusServiceUnavailable, gin.H{jsonKeyError: "service overloaded, retry shortly"})
				return
			}
			s.logger.Error("failed to publish transaction batch", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to submit"})
			return
		}

		publishedTxIDs := make([]string, len(toPublish))
		for i, p := range toPublish {
			publishedTxIDs[i] = p.txid
		}
		// Bounded (TxIDBatch: true count + capped list), NOT chunked: this
		// runs synchronously before the HTTP response and /txs accepts up to
		// 256 MiB, so unbounded sequential chunk writes would land in the
		// client's request latency. Full per-txid coverage arrives shortly
		// after on the async "transactions accepted by network" line; a tx
		// rejected at intake is covered by the existing REJECTED line.
		receivedFields := append([]zap.Field{logfields.Status(string(models.StatusReceived))}, logfields.TxIDBatch(publishedTxIDs)...)
		telemetry.LoggerWith(ctx, s.logger).Info("transactions received", receivedFields...)
	}

	c.JSON(http.StatusAccepted, gin.H{
		"submitted":  len(toPublish),
		"duplicates": duplicates,
		"total":      len(parsed),
	})
}
