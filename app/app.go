// Package app builds and wires the shared dependencies BSV Broadcaster's services
// need (kafka broker + producer, store, teranode client, merkle-service
// client, events publisher, validator) and turns them into a slice of
// runnable services. cmd/bsv-broadcaster/main.go is a thin wrapper that loads
// config, calls Bootstrap, then BuildServices, and supervises the lifecycle.
//
// Splitting this out of cmd/bsv-broadcaster lets the e2e test harness reuse the same
// boot path without invoking the binary or duplicating wiring.
package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	chaintrackslib "github.com/bsv-blockchain/go-chaintracks/chaintracks"
	chaintracksconfig "github.com/bsv-blockchain/go-chaintracks/config"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/events"
	"github.com/bsv-blockchain/broadcaster/finality"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/merkleservice"
	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/services"
	"github.com/bsv-blockchain/broadcaster/services/api_server"
	"github.com/bsv-blockchain/broadcaster/services/bump_builder"
	"github.com/bsv-blockchain/broadcaster/services/chaintracks_server"
	"github.com/bsv-blockchain/broadcaster/services/p2p_client"
	"github.com/bsv-blockchain/broadcaster/services/propagation"
	"github.com/bsv-blockchain/broadcaster/services/sse"
	"github.com/bsv-blockchain/broadcaster/services/watchdog"
	"github.com/bsv-blockchain/broadcaster/services/webhook"
	"github.com/bsv-blockchain/broadcaster/ssrfguard"
	"github.com/bsv-blockchain/broadcaster/store"
	storefactory "github.com/bsv-blockchain/broadcaster/store/factory"
	"github.com/bsv-blockchain/broadcaster/store/mongodb"
	"github.com/bsv-blockchain/broadcaster/teranode"
	"github.com/bsv-blockchain/broadcaster/validator"
)

// Deps is the set of process-wide dependencies BSV Broadcaster's services share.
// Bootstrap returns one of these fully wired; BuildServices consumes it.
type Deps struct {
	Cfg            *config.Config
	Logger         *zap.Logger
	Broker         kafka.Broker
	Producer       *kafka.Producer
	Publisher      events.Publisher
	Store          store.Store
	Leaser         store.Leaser
	TxTracker      *store.TxTracker
	TeranodeClient *teranode.Client
	MerkleClient   merkleservice.Service // nil when no merkle_service endpoint is configured
	Validator      *validator.Validator
	// Chaintracks is the shared in-process header tracker. nil when
	// chaintracks_server is disabled (regtest, or explicit opt-out) — services
	// that consume it (chaintracks_server, bump-builder canonical-root
	// validation) must nil-guard.
	Chaintracks chaintrackslib.Chaintracks
	// FinalityChecker runs the api-server's nLockTime/BIP113 intake gate.
	// nil (no chain source, or validator.disable_finality_check) disables
	// the gate; the checker itself is nil-receiver-safe.
	FinalityChecker *finality.Checker
	// AuthStore holds API keys and quota counters when auth.store.mongodb is
	// configured; nil means they live in Store. Only api-server reads it.
	AuthStore api_server.AuthStore
}

// Bootstrap creates every shared dependency the services rely on, in the
// order they need to be created. The returned cleanup func closes them in
// reverse order. teranodeClient.Start is wired to ctx, so its background
// probes terminate when ctx is canceled.
func Bootstrap(ctx context.Context, cfg *config.Config, logger *zap.Logger) (*Deps, func(), error) {
	logger.Info(
		"starting bsv-broadcaster",
		zap.String("mode", cfg.Mode),
		zap.String("kafka_backend", cfg.Kafka.Backend),
		zap.String("kafka_topic_prefix", cfg.Kafka.TopicPrefix),
		zap.String("store_backend", cfg.Store.Backend),
	)

	// Must precede every topic lookup below and in the services: topic names
	// are fixed for the life of the process once the broker exists.
	kafka.SetTopicPrefix(cfg.Kafka.TopicPrefix)

	// The memory backend provisions its own topics, so it takes the
	// propagation partition count directly; the sarama backend ignores the
	// map (real topics are broker-provisioned and checked below).
	broker, err := kafka.NewBroker(cfg.Kafka, map[string]int{
		kafka.TopicPropagation: cfg.Propagation.Partitions,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("creating kafka broker: %w", err)
	}
	producer := kafka.NewProducer(broker)

	// TopicPropagation must exist with at least propagation.partitions
	// partitions (fail-closed on a missing topic — auto-creation would let
	// the broker default decide the key→partition mapping). Since #295 the
	// topic may have any partition count: the api server keys messages by
	// dependency family, so per-partition order still delivers a parent's
	// admit to its dispatcher before its children's, and cross-partition
	// stragglers are absorbed by the missing-parent safety net instead of
	// being terminally REJECTED.
	//
	// `>=` rather than `==` because a wider topic is safe to run against —
	// not because the extra partitions are inert. Nothing passes this value
	// to a partitioner, so the producer hashes over the topic's ACTUAL
	// partition count: every partition carries real traffic and gets its own
	// dispatcher. This knob is a fail-closed assertion, not a placement
	// control; lowering it does not narrow the producer's hash space.
	if pErr := kafka.CheckMinPartitions(broker, kafka.TopicPropagation, cfg.Propagation.Partitions, logger); pErr != nil {
		_ = producer.Close()
		return nil, nil, fmt.Errorf("kafka partition check: %w", pErr)
	}
	// MinPartitions is a soft hint for horizontally-scaled topics. There
	// are currently no other hot-path topics that need it (TopicTransaction
	// was retired with tx_validator) but the knob is retained so a future
	// fan-out topic can opt into the check without re-introducing config.
	//
	// Deliberately still passed a nil topic list, which makes CheckPartitions
	// a no-op. Handing it the real fan-out topics turns a check that has been
	// dead since TopicTransaction was retired into a live boot-blocking one:
	// any deployment carrying a leftover kafka.min_partitions greater than
	// its actual bsv-broadcaster.tx_status / bsv-broadcaster.block_processed width would
	// crash-loop on upgrade, for a reason unrelated to multi-partition
	// propagation. Enabling it is its own change, with its own release note.
	if cfg.Kafka.MinPartitions > 1 {
		if pErr := kafka.CheckPartitions(broker, nil, cfg.Kafka.MinPartitions, logger); pErr != nil {
			_ = producer.Close()
			return nil, nil, fmt.Errorf("kafka partition check: %w", pErr)
		}
	}

	st, leaser, err := storefactory.New(ctx, cfg, logger)
	if err != nil {
		_ = producer.Close()
		return nil, nil, fmt.Errorf("creating store: %w", err)
	}
	if idxErr := st.EnsureIndexes(ctx); idxErr != nil {
		_ = st.Close()
		_ = producer.Close()
		return nil, nil, fmt.Errorf("ensuring store indexes: %w", idxErr)
	}

	// auth.store.mongodb moves API keys and quota out of the main store so
	// several deployments (one per network) share one key set. Opened right
	// after the main store so a misconfigured auth database fails boot before
	// the slow work below (tx tracker hydration, chaintracks).
	var authStore closableAuthStore
	if cfg.Auth.Store.Configured() && modeNeedsAuthStore(cfg.Mode) {
		as, asErr := openAuthStore(ctx, cfg.Auth.Store.Mongo, logger)
		if asErr != nil {
			_ = st.Close()
			_ = producer.Close()
			return nil, nil, asErr
		}
		authStore = as
	}
	closeAuthStore := func() {
		if authStore != nil {
			_ = authStore.Close()
		}
	}

	// Align store batch-helper concurrency with config. Zero falls back to
	// runtime.NumCPU which matches validator parallelism — keeps DB write
	// fanout from being the bottleneck.
	store.SetBatchConcurrency(cfg.Store.BatchConcurrency)

	txTracker := store.NewTxTracker()

	discoveredTTL := time.Duration(cfg.Propagation.EndpointHealth.DiscoveredTTLMs) * time.Millisecond
	teranodeClient := teranode.NewClient(cfg.DatahubURLs, cfg.Teranode.AuthToken, teranode.HealthConfig{
		FailureThreshold:          cfg.Propagation.EndpointHealth.FailureThreshold,
		BroadcastFailureThreshold: cfg.Propagation.EndpointHealth.BroadcastFailureThreshold,
		ProbeInterval:             time.Duration(cfg.Propagation.EndpointHealth.ProbeIntervalMs) * time.Millisecond,
		ProbeTimeout:              time.Duration(cfg.Propagation.EndpointHealth.ProbeTimeoutMs) * time.Millisecond,
		MinHealthyEndpoints:       cfg.Propagation.EndpointHealth.MinHealthyEndpoints,
		RefreshInterval:           time.Duration(cfg.Propagation.EndpointHealth.RefreshIntervalMs) * time.Millisecond,
		Source:                    newEndpointSource(st, cfg.Network, cfg.P2P.DatahubDiscovery, cfg.P2P.AllowPrivateURLs, discoveredTTL, logger),
		Logger:                    logger,
	})

	// Seed the registry with statically configured URLs so a freshly started
	// pod (especially mode=p2p-client) always sees them via the same discovery
	// path. The upsert is idempotent and refreshes LastSeen on every restart,
	// so this also works as a heartbeat for the operator-defined seed list.
	if len(cfg.DatahubURLs) > 0 {
		seedCtx, seedCancel := context.WithTimeout(ctx, 5*time.Second)
		for _, url := range cfg.DatahubURLs {
			if seedErr := st.UpsertDatahubEndpoint(seedCtx, store.DatahubEndpoint{
				URL:      url,
				Network:  cfg.Network,
				Source:   store.DatahubEndpointSourceConfigured,
				LastSeen: time.Now(),
			}); seedErr != nil {
				logger.Warn(
					"failed to seed configured datahub url",
					zap.String("url", url),
					zap.Error(seedErr),
				)
			}
		}
		seedCancel()
	}

	// The interface stays nil when the integration is disabled; a typed-nil
	// *Pool assigned here would make every consumer's nil guard pass and
	// dereference it.
	var merkleClient merkleservice.Service
	if cfg.MerkleService.Enabled() {
		endpoints := cfg.MerkleService.Endpoints()
		// One Pool for every endpoint, single or many: each /watch and
		// /reprocess fans out to all of them, a tx counts as registered once
		// any endpoint accepts it, and a per-endpoint breaker keeps a dead
		// merkle-service from adding its timeout to every batch. Start runs
		// the /health probe that closes breakers again.
		pool := merkleservice.NewPool(endpoints, cfg.MerkleService.AuthToken, 0)
		pool.SetLogger(logger.Named("merkle-client"))
		pool.Start(ctx)
		merkleClient = pool
		logger.Info("merkle-service endpoints configured", zap.Strings("endpoints", endpoints))
		if cfg.MerkleService.AuthToken == "" {
			// Not fatal: many merkle-service deployments don't enforce auth, and
			// the standalone/no-auth profile is legitimate. But if the target
			// DOES enforce it (see merkle-service#204), /watch registration and
			// /reprocess recovery will 401 at runtime — the watchdog now flags
			// this loudly (issue #269) rather than misreporting it as a
			// consensus problem. Warn at startup so the cause is obvious.
			logger.Warn(
				"merkle_service endpoints are set but merkle_service.auth_token is empty; " +
					"if a target merkle-service enforces auth, tx registration (/watch) " +
					"and block-processed recovery (/reprocess) will fail with 401 at runtime — " +
					"set merkle_service.auth_token to authenticate",
			)
		}
	}

	// Build the GoBDK transaction validator only for modes that actually
	// validate submissions (api-server). NewValidatorForNetwork constructs
	// two cgo/BDK script-verification engines; standing them up in modes that
	// never call Validator.ValidateTransaction (sse, watchdog, propagation,
	// p2p-client, bump-builder, chaintracks) is pure per-pod memory overhead.
	// Keep in sync with BuildServices / modeNeedsValidator.
	var txVal *validator.Validator
	if modeNeedsValidator(cfg.Mode) {
		v, verr := validator.NewValidatorForNetwork(cfg.Network, validatorPolicyFromConfig(cfg))
		if verr != nil {
			closeAuthStore()
			_ = st.Close()
			_ = producer.Close()
			return nil, nil, fmt.Errorf("creating validator: %w", verr)
		}
		txVal = v
	}

	publisher := events.NewKafkaPublisher(producer, logger, cfg.Events.SubscriberBuffer)

	teranodeClient.Start(ctx)

	// Construct chaintracks once at process startup so every in-process
	// consumer (chaintracks_server, bump-builder canonical-root validation)
	// shares one P2P subscription and header cache. Skipped when:
	//   - cfg.ChaintracksServer.Enabled is false (operator-wide off switch;
	//     regtest force-disables it via config.validate), or
	//   - the configured mode never dereferences deps.Chaintracks. Without
	//     this gate every microservice pod (api-server, sse, propagation, …)
	//     would spin up an embedded ChainManager and poll the upstream node
	//     even though nothing in that pod reads from it. In microservice
	//     deployments the chaintracks pod runs embedded and bump-builder
	//     points chaintracks.mode=remote at it; everything else skips here.
	var chainTracks chaintrackslib.Chaintracks
	if cfg.ChaintracksServer.Enabled && modeNeedsChaintracks(cfg.Mode) {
		ct, ctErr := initChaintracks(ctx, cfg, logger)
		if ctErr != nil {
			closeAuthStore()
			_ = st.Close()
			_ = producer.Close()
			return nil, nil, fmt.Errorf("chaintracks init: %w", ctErr)
		}
		chainTracks = ct
	}

	// One chain source per process, shared by the finality gate and TxTracker
	// hydration below.
	reader := chainReader(ctx, cfg, chainTracks, logger)

	finalityChecker := buildFinalityChecker(cfg, reader, st, logger)

	// Hydrate the TxTracker from the store BEFORE handing it to api-server.
	// Gated to modes that actually consume it: only api-server reads
	// deps.TxTracker (see BuildServices). Without hydration a restart leaves the
	// tracker empty and in-flight transitions can be dropped until re-observed.
	// Running it in modes that never read the tracker (sse, watchdog,
	// propagation, p2p-client, bump-builder, chaintracks) is what OOMKilled
	// every 512Mi arcade-v2 component after the v0.9.0 rollout.
	// Keep in sync with BuildServices / modeNeedsTxTracker.
	if modeNeedsTxTracker(cfg.Mode) {
		hydrateHeight, heightKnown, heightSource := hydrationHeight(ctx, reader, st, logger)
		scan := store.NewTrackerScan(hydrateHeight, heightKnown)

		logger.Info(
			"tx tracker hydration height resolved",
			zap.Uint64("height", hydrateHeight),
			zap.Bool("height_known", heightKnown),
			zap.String("height_source", heightSource),
			zap.Uint64("prune_mined_below", scan.PruneMinedBelow),
			zap.String("chaintracks_mode", string(cfg.Chaintracks.Mode)),
			zap.Bool("chain_reader_configured", reader != nil),
		)
		if !heightKnown {
			logger.Warn(
				"tx tracker hydrating without a chain height: MINED pruning is DISABLED and every " +
					"MINED row will be loaded (the v0.13.0 current_height=0 condition). Check " +
					"chaintracks.mode/chaintracks.url reachability from this pod, and that " +
					"block_processing has status='active' rows.",
			)
		}

		hydrateStart := time.Now()
		stats, hydrateErr := txTracker.LoadFromStore(ctx, st, scan)
		// `loaded` and `current_height` keep their names — dashboards key on them.
		fields := []zap.Field{
			zap.Int("loaded", stats.Loaded),
			zap.Int("scanned", stats.Scanned),
			zap.Int("skipped", stats.Skipped),
			zap.Uint64("current_height", hydrateHeight),
			zap.String("height_source", heightSource),
			zap.Uint64("prune_mined_below", scan.PruneMinedBelow),
			zap.Duration("elapsed", time.Since(hydrateStart)),
		}
		if hydrateErr != nil {
			logger.Warn("tx tracker hydration partial", append(fields, zap.Error(hydrateErr))...)
		} else {
			logger.Info("tx tracker hydrated", fields...)
		}
	}

	deps := &Deps{
		Cfg:             cfg,
		Logger:          logger,
		Broker:          broker,
		Producer:        producer,
		Publisher:       publisher,
		Store:           st,
		Leaser:          leaser,
		TxTracker:       txTracker,
		TeranodeClient:  teranodeClient,
		MerkleClient:    merkleClient,
		Validator:       txVal,
		Chaintracks:     chainTracks,
		FinalityChecker: finalityChecker,
	}
	// Nil (keys and quota in the main store) unless auth.store was opened.
	if authStore != nil {
		deps.AuthStore = authStore
	}

	cleanup := func() {
		_ = publisher.Close()
		teranodeClient.Close()
		closeAuthStore()
		_ = st.Close()
		_ = producer.Close()
	}
	return deps, cleanup, nil
}

// validatorPolicyFromConfig builds the validator.Policy from cfg.Validator.
// The size/sigop fields carry their (defaulted) config values through so the
// GET /policy endpoint and intake validation agree on the advertised limits;
// a zero there falls back to the validator's canonical teranode default.
//
// Fee handling preserves the original semantics: AcceptZeroFee returns an
// explicit pointer-to-zero MinFeePerKB so NewValidator preserves it (its
// nil-check is on the pointer, not the value) and ValidateTransaction feeds
// Satoshis=0 to spv.Verify, which accepts any non-negative fee — including
// fee=0. A zero MinFeePerKB with AcceptZeroFee=false leaves the pointer nil so
// the validator's DefaultMinFeePerKB applies.
func validatorPolicyFromConfig(cfg *config.Config) *validator.Policy {
	p := &validator.Policy{
		MaxTxSizePolicy:         int(cfg.Validator.MaxTxSizePolicy),     //nolint:gosec // policy size in bytes, bounded far below int max
		MaxScriptSizePolicy:     int(cfg.Validator.MaxScriptSizePolicy), //nolint:gosec // policy size in bytes, bounded far below int max
		MaxTxSigopsCountsPolicy: cfg.Validator.MaxTxSigopsCountsPolicy,
	}
	switch {
	case cfg.Validator.AcceptZeroFee:
		zero := uint64(0)
		p.MinFeePerKB = &zero
	case cfg.Validator.MinFeePerKB != 0:
		minFee := cfg.Validator.MinFeePerKB
		p.MinFeePerKB = &minFee
	}
	return p
}

// modeNeedsChaintracks reports whether the configured service mode constructs
// at least one service that reads from deps.Chaintracks. Other modes
// (api-server, sse, propagation, p2p-client, watchdog) never dereference it,
// so initializing it would spin up an unused embedded ChainManager + upstream
// header poller in every pod — which is exactly the wrong thing for
// microservice deployments where the dedicated chaintracks pod owns the
// header store and bump-builder reads via chaintracks.mode=remote.
//
// Keep this list in sync with BuildServices: any new mode that wires
// deps.Chaintracks into a service must be added here.
func modeNeedsChaintracks(mode string) bool {
	switch mode {
	case "all", "chaintracks", "bump-builder":
		return true
	default:
		return false
	}
}

// modeNeedsFinality reports whether the configured mode constructs a service
// that consumes Deps.FinalityChecker — only api-server does (the intake
// finality gate). Deliberately NOT part of modeNeedsChaintracks: the gate
// uses the remote chaintracks client in microservice deployments precisely
// so api-server pods never spin up an embedded ChainManager.
func modeNeedsFinality(mode string) bool {
	return mode == "all" || mode == "api-server"
}

// buildFinalityChecker wires the chain sources for the api-server's
// nLockTime/BIP113 finality gate (issue #245). In mode=all the embedded
// chaintracks is reused; in microservice deployments the api-server pod runs
// with chaintracks disabled (see modeNeedsChaintracks), so when the operator
// points chaintracks.mode=remote at the chaintracks pod the cheap HTTP
// client is constructed here — no P2P, no storage, no ChainManager. The
// store always backs a height-only fallback (adopted from PR #181): without
// any chaintracks source, height-based locktimes are still gated while
// timestamp locktimes fail open (they need MTP). Teranode stays the
// authority in every degraded mode.
// The reader is built by the caller (chainReader) and shared with TxTracker
// hydration, so a pod holds exactly one chaintracks client and one degradation
// story.
func buildFinalityChecker(cfg *config.Config, reader finality.ChainReader, st store.Store, logger *zap.Logger) *finality.Checker {
	if cfg.Validator.DisableFinalityCheck || !modeNeedsFinality(cfg.Mode) {
		return nil
	}

	if reader == nil {
		logger.Info("finality pre-check degraded to height-only: no MTP source (enable chaintracks_server or set chaintracks.mode=remote with a URL)")
	}

	return finality.NewChecker(reader, logger,
		finality.WithHeightFallback(st),
		finality.WithUnavailableHook(metrics.APIFinalityPrecheckUnavailableTotal.Inc))
}

// chainReader picks the process-wide chain source: the shared embedded
// chaintracks when it exists, else a remote go-chaintracks client when
// configured, else nil.
//
// Two consumers share it — the finality gate and TxTracker hydration — so it
// is deliberately NOT gated on cfg.Validator.DisableFinalityCheck. Letting a
// validator flag suppress hydration's height source is exactly the kind of
// action-at-a-distance that produced issue #276.
//
// In remote mode Initialize is just an HTTP client construction: no goroutines,
// no I/O, and nothing to close.
func chainReader(ctx context.Context, cfg *config.Config, chainTracks chaintrackslib.Chaintracks, logger *zap.Logger) finality.ChainReader {
	if chainTracks != nil {
		return chainTracks
	}
	if cfg.Chaintracks.Mode != chaintracksconfig.ModeRemote || cfg.Chaintracks.URL == "" {
		return nil
	}
	remote, err := cfg.Chaintracks.Initialize(ctx, "bsv-broadcaster-chain-reader", nil)
	if err != nil {
		logger.Warn(
			"remote chaintracks init failed: finality pre-check degrades to height-only and "+
				"tx tracker hydration loses its primary chain-height source",
			zap.Error(err),
		)
		return nil
	}
	return remote
}

// hydrationHeightTimeout bounds each chain-height probe at startup. The remote
// go-chaintracks client is built on a zero-value http.Client, which has NO
// timeout of its own, so without this a wedged chaintracks pod would stall
// boot indefinitely rather than degrading to the store fallback.
const hydrationHeightTimeout = 5 * time.Second

// hydrationHeight resolves the chain tip used to prune deeply-confirmed MINED
// rows during hydration, returning whether the height is actually KNOWN.
//
// The (value, known) pair is the whole point. go-chaintracks' GetHeight returns
// uint32 and collapses every failure into 0, indistinguishable from genesis —
// so api-server pods silently hydrated with height 0, the deeply-confirmed skip
// never fired, and all 4.48M MINED rows loaded on every boot (issue #276).
// GetTip returns a nil-able header, which is the only accessor that can express
// "unavailable", so this uses GetTip and never GetHeight.
//
// Sources in order:
//  1. the chain reader (embedded chaintracks, or the remote client) — the true
//     tip;
//  2. the store's active tip, written to block_processing by the chaintracks
//     pod and shared through the same database, so an api-server pod that runs
//     no chaintracks of its own still gets a real height. Already the finality
//     gate's height-only fallback (finality.WithHeightFallback).
//
// Both are lower bounds on the true tip, and both fail safe: a height that is
// too low under-prunes (a bigger map, today's behavior), which costs memory but
// never correctness — the tracker is only a prefilter, and the store lattice
// stays authoritative for anything it doesn't know.
func hydrationHeight(
	ctx context.Context, reader finality.ChainReader, st store.Store, logger *zap.Logger,
) (height uint64, known bool, source string) {
	if reader != nil {
		tipCtx, cancel := context.WithTimeout(ctx, hydrationHeightTimeout)
		tip := reader.GetTip(tipCtx)
		cancel()
		if tip != nil {
			return uint64(tip.Height), true, "chaintracks"
		}
		logger.Warn("chain height degraded: chaintracks tip unavailable, falling back to the store active tip")
	}

	storeCtx, cancel := context.WithTimeout(ctx, hydrationHeightTimeout)
	activeTip, err := st.GetActiveTipBlockHeight(storeCtx)
	cancel()
	switch {
	case err != nil:
		logger.Warn("chain height unknown: store active tip read failed", zap.Error(err))
		return 0, false, "none"
	case activeTip == 0:
		logger.Warn("chain height unknown: no active blocks recorded (store active tip is 0)")
		return 0, false, "none"
	default:
		return activeTip, true, "store"
	}
}

// modeNeedsTxTracker reports whether the configured mode constructs a service
// that reads deps.TxTracker. Only api-server does (see BuildServices). Other
// modes must not hydrate it: TxTracker.LoadFromStore streams the entire
// transactions history into an in-memory map, a cost that scales with tx volume
// and OOMKilled the 512Mi arcade-v2 components (sse, watchdog, p2p-client,
// bump-builder) after the v0.9.0 rollout.
//
// Keep this list in sync with BuildServices: any new mode that wires
// deps.TxTracker into a service must be added here.
func modeNeedsTxTracker(mode string) bool {
	switch mode {
	case "all", "api-server":
		return true
	default:
		return false
	}
}

// modeNeedsAuthStore reports whether the configured mode constructs the
// api-server, the only service that reads API keys and quota (auth
// middleware, rate limiter, admin key endpoints). Other modes never open the
// auth database. Keep in sync with BuildServices.
func modeNeedsAuthStore(mode string) bool {
	switch mode {
	case "all", "api-server":
		return true
	default:
		return false
	}
}

// closableAuthStore is the dedicated auth store as Bootstrap holds it: what
// the api-server needs plus Close for the cleanup func.
type closableAuthStore interface {
	api_server.AuthStore
	Close() error
}

// openAuthStore connects to the dedicated auth database and provisions only
// the api_keys / api_quota indexes, so no transaction collections appear in
// it. The outer ceiling mirrors the main store factory's: server selection
// is bounded by connect_timeout_ms, this guards a stuck TLS handshake or SRV
// lookup. Neither the log line nor the errors carry the uri (credentials).
//
// It touches only <database>.api_keys and <database>.api_quota, so it runs
// under a role scoped to those two collections (see docs/api-keys.md).
func openAuthStore(ctx context.Context, cfg config.Mongo, logger *zap.Logger) (closableAuthStore, error) {
	octx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	as, err := mongodb.NewAuthStore(octx, cfg, logger.Named("auth-store"))
	if err != nil {
		return nil, fmt.Errorf("creating auth store (auth.store.mongodb): %w", err)
	}
	if err := as.EnsureIndexes(ctx); err != nil {
		_ = as.Close() //nolint:contextcheck // Close bounds the disconnect with its own timeout so it outlives an expiring boot ctx, like st.Close() above.
		return nil, fmt.Errorf("ensuring auth store indexes (auth.store.mongodb): %w", err)
	}
	logger.Info("api keys and quota served from dedicated auth store",
		zap.String("auth_store_database", cfg.Database))
	return as, nil
}

// modeNeedsValidator reports whether the configured mode constructs a service
// that calls deps.Validator.ValidateTransaction. Only api-server does (see
// BuildServices). NewValidatorForNetwork builds two cgo/BDK script-verification
// engines, so constructing it in other modes is pure per-pod memory overhead.
//
// Keep this list in sync with BuildServices: any new mode that wires
// deps.Validator into a service must be added here.
func modeNeedsValidator(mode string) bool {
	switch mode {
	case "all", "api-server":
		return true
	default:
		return false
	}
}

// resolveChaintracksBootstrapPeers picks the bootstrap peer list passed
// to chaintracks' embedded p2p client. Precedence, highest first:
//  1. chaintracks.p2p.msgbus.bootstrappeers — explicit chaintracks-only
//     override. The upstream msgbus.Config field has no mapstructure
//     tag, so YAML must use the camelCase-flattened key
//     ("bootstrappeers", not "bootstrap_peers"); viper matches case-
//     insensitively but does not translate snake_case.
//  2. p2p.bootstrap_peers — the well-documented BSV Broadcaster key, shared
//     with services/p2p_client. Configuring it once at the p2p block
//     lets both the p2p-client and chaintracks pick it up, which is
//     what almost every private-network deployment wants.
//  3. The network-derived default from ResolveP2PNetwork — mainnet/
//     testnet/teratestnet DNS bootstrap, nil for regtest.
func resolveChaintracksBootstrapPeers(cfg *config.Config) []string {
	if len(cfg.Chaintracks.P2P.MsgBus.BootstrapPeers) > 0 {
		return cfg.Chaintracks.P2P.MsgBus.BootstrapPeers
	}
	if len(cfg.P2P.BootstrapPeers) > 0 {
		return cfg.P2P.BootstrapPeers
	}
	_, defaultBootstrap := config.ResolveP2PNetwork(cfg.Network)
	return defaultBootstrap
}

// initChaintracks brings up the embedded go-chaintracks instance shared
// across the process. Caller gates the enabled-ness check; this function
// always tries to construct and returns an error on failure.
//
// The construction logic mirrors what previously lived in
// chaintracks_server.Service.initChaintracks. Moving it here lets bump-
// builder use the same instance without depending on a service's
// initialization timing or a duplicate P2P subscription.
func initChaintracks(ctx context.Context, cfg *config.Config, logger *zap.Logger) (chaintrackslib.Chaintracks, error) {
	// Default chaintracks storage to <storage_path>/chaintracks/ so
	// operators only need to set a single storage root. Tilde expansion
	// happens in config.Load.
	if cfg.Chaintracks.StoragePath == "" {
		root := cfg.StoragePath
		if root == "" {
			root = "."
		}
		if err := os.MkdirAll(root, 0o750); err != nil {
			return nil, fmt.Errorf("creating storage directory %s: %w", root, err)
		}
		cfg.Chaintracks.StoragePath = path.Join(root, "chaintracks")
	}

	// Thread the top-level network into chaintracks' embedded p2p config.
	// Without this go-chaintracks falls back to "main" silently. Chaintracks
	// needs the upstream-strict spelling ("main"/"test"/"teratestnet").
	cfg.Chaintracks.P2P.Network = config.ResolveChaintracksNetwork(cfg.Network)
	cfg.Chaintracks.P2P.MsgBus.BootstrapPeers = resolveChaintracksBootstrapPeers(cfg)

	ct, err := cfg.Chaintracks.Initialize(ctx, "bsv-broadcaster", nil)
	if err != nil {
		return nil, fmt.Errorf("chaintracks initialize: %w", err)
	}

	network, _ := ct.GetNetwork(ctx)
	logger.Info(
		"chaintracks initialized",
		zap.String("storage_path", cfg.Chaintracks.StoragePath),
		zap.String("network", network),
	)
	return ct, nil
}

// BuildServices returns the services that should run for the configured mode.
// Each service's lifetime is tied to the ctx passed to its Start method by
// the caller — the supervisor in cmd/bsv-broadcaster or the test harness.
func BuildServices(d *Deps) []services.Service {
	cfg := d.Cfg
	var svcs []services.Service

	shouldRun := func(name string) bool {
		return cfg.Mode == "all" || cfg.Mode == name
	}

	if shouldRun("api-server") {
		apiSrv := api_server.New(cfg, d.Logger, d.Producer, d.Publisher, d.Store, d.TxTracker, d.TeranodeClient, d.MerkleClient, d.Validator, d.FinalityChecker)
		if d.AuthStore != nil {
			apiSrv.SetAuthStore(d.AuthStore)
		}
		svcs = append(svcs, apiSrv)
	}
	if shouldRun("bump-builder") {
		// chainHeader is nil when chaintracks is disabled — bump-builder
		// nil-guards and falls back to subtree-count-only validation, the
		// anchor guard fails open, and the reconciler below is skipped.
		var chainHeader bump_builder.ChainHeaderReader
		if d.Chaintracks != nil {
			chainHeader = chaintracksHeaderReader{ct: d.Chaintracks}
		}
		svcs = append(svcs, bump_builder.New(cfg, d.Logger, d.Producer, d.Publisher, d.Store, d.TeranodeClient, chainHeader))
		// The anchor reconciler heals txs anchored to orphaned blocks
		// (issue #279). Hosted in the bump-builder mode because it shares
		// the MINED-publish machinery, the store, and the chaintracks
		// header source; lease-gated for multi-replica deployments.
		if rec := bump_builder.NewReconciler(cfg, d.Logger, d.Store, d.Publisher, chainHeader, d.MerkleClient, d.Leaser); rec != nil {
			svcs = append(svcs, rec)
		}
	}
	if shouldRun("watchdog") && cfg.Watchdog.Enabled {
		if wd := watchdog.NewService(cfg, d.Logger, d.Store, d.Leaser, d.MerkleClient); wd != nil {
			svcs = append(svcs, wd)
		} else {
			d.Logger.Info("watchdog skipped: merkle_service endpoints or leaser not configured")
		}
	}
	if shouldRun("sse") {
		if ssvc := sse.New(cfg, d.Logger, d.Publisher, d.Store); ssvc != nil {
			svcs = append(svcs, ssvc)
		} else {
			d.Logger.Info("sse skipped: sse.enabled=false or publisher not configured")
		}
	}
	if shouldRun("chaintracks") {
		if ct := chaintracks_server.New(cfg, d.Logger, d.Store, d.Chaintracks); ct != nil {
			svcs = append(svcs, ct)
		} else {
			d.Logger.Info("chaintracks skipped: chaintracks_server.enabled=false (regtest force-disables this)")
		}
	}
	if shouldRun("propagation") {
		svcs = append(svcs, propagation.New(cfg, d.Logger, d.Producer, d.Publisher, d.Store, d.Leaser, d.TeranodeClient, d.MerkleClient))
	}
	if shouldRun("api-server") || shouldRun("webhook") {
		svcs = append(svcs, webhook.New(cfg.Webhook, cfg.Callback, d.Logger, d.Publisher, d.Store, d.Leaser))
	}
	// p2p_client is intentionally NOT bundled with propagation. Running a
	// libp2p host inside the propagation pod duplicates the peer discovery
	// the dedicated p2p-client deployment already performs and is a
	// significant memory cost that OOMKilled propagation in arcade-v2 at
	// 512Mi. Propagation now reads discovered URLs from the shared store
	// via teranode.Client's refresh loop (see teranode/client.go:refreshLoop).
	// In --mode all the shouldRun gate still matches via the cfg.Mode=="all"
	// branch, so single-process development is unchanged.
	if shouldRun("p2p-client") {
		svcs = append(svcs, p2p_client.New(cfg, d.Logger, d.Producer, d.Store))
	}

	return svcs
}

// chaintracksHeaderReader adapts go-chaintracks's Chaintracks interface to
// the narrower ChainHeaderReader contract bump-builder needs. The library
// already exposes GetHeaderByHash with the same signature, so this is a
// trivial passthrough; the wrapper exists only to keep BSV Broadcaster packages from
// importing chaintracks types into their public APIs.
type chaintracksHeaderReader struct {
	ct chaintrackslib.Chaintracks
}

func (a chaintracksHeaderReader) GetHeaderByHash(ctx context.Context, hash *chainhash.Hash) (*chaintrackslib.BlockHeader, error) {
	return a.ct.GetHeaderByHash(ctx, hash)
}

// GetHeaderByHeight maps a remote chaintracks 404 to ErrHeaderNotFound, the
// absence sentinel ChainHeaderReader promises. The embedded ChainManager
// already returns the sentinel above its tip; the go-chaintracks HTTP client
// (microservice mode) reports the same answer as "server request failed:
// status 404", which the reconciler's neighborhood walk read as a failed
// lookup and retried every tick until the tip outgrew the walk (mainnet
// 973522, 2026-10-31). Any other failed request stays a failure.
func (a chaintracksHeaderReader) GetHeaderByHeight(ctx context.Context, height uint32) (*chaintrackslib.BlockHeader, error) {
	h, err := a.ct.GetHeaderByHeight(ctx, height)
	if isRemoteHeaderNotFound(err) {
		return nil, fmt.Errorf("%w: %w", chaintrackslib.ErrHeaderNotFound, err)
	}
	return h, err
}

// isRemoteHeaderNotFound recognizes the go-chaintracks client's 404, which it
// only exposes as ErrServerRequestFailed wrapped with "status <code>".
func isRemoteHeaderNotFound(err error) bool {
	return errors.Is(err, chaintrackslib.ErrServerRequestFailed) &&
		strings.HasSuffix(err.Error(), fmt.Sprintf("status %d", http.StatusNotFound))
}

// endpointSourceValidationTTL is how long a discovered URL that passed
// validation skips re-validation (including its DNS round-trip) on
// subsequent refresh ticks. Success-only: rejected URLs are re-checked
// every tick so a row whose DNS recovers is admitted within one interval.
const endpointSourceValidationTTL = 5 * time.Minute

// endpointSourceValidationCacheMax bounds the validation cache. Discovered
// registry rows originate from untrusted p2p announcements; the cap keeps a
// registry flooded with unique URLs from growing process memory without
// bound. Far above the size of any real datahub fleet.
const endpointSourceValidationCacheMax = 512

// endpointSourceLookupTimeout bounds each DNS lookup during refresh so a
// dead resolver cannot wedge the refresh tick (the synchronous first
// refresh in teranode.Client.Start is separately capped, and lookups
// derive from the tick's ctx).
const endpointSourceLookupTimeout = 2 * time.Second

// datahubLister is the narrow store contract endpointSource needs;
// extracted so tests can fake the registry without a real store.
type datahubLister interface {
	ListDatahubEndpoints(ctx context.Context, network string) ([]store.DatahubEndpoint, error)
}

// endpointSource adapts the store's DatahubEndpoint registry to
// teranode.EndpointSource. network scopes the listing to the configured
// Bitcoin network so a store shared across pods (or reused after a network
// change) never replays peers from a different network. When
// includeDiscovered is false (operator disabled p2p.datahub_discovery),
// rows persisted as source=discovered by prior runs are filtered out — the
// toggle means "ignore discovered URLs" end-to-end, not just "stop
// discovering new ones."
//
// Discovered rows are additionally re-validated with ssrfguard on every
// listing (behind a success-only TTL cache): the p2p_client validates at
// announcement intake, but rows persisted before that guard existed — or by
// older builds — would otherwise flow straight into every pod's endpoint
// set forever, since the registry has no eviction. Operator-configured rows
// are exempt: static datahub_urls may legitimately point at
// cluster-internal names.
type endpointSource struct {
	st                datahubLister
	network           string
	includeDiscovered bool
	allowPrivate      bool

	// lookupIP is the DNS seam; nil is never stored — newEndpointSource
	// installs the bounded default.
	lookupIP func(ctx context.Context, host string) ([]net.IP, error)
	// validated caches URLs that passed validation. Bounded — see
	// endpointSourceValidationCacheMax.
	validated *ssrfguard.SuccessCache
	// warned tracks URLs already logged at WARN so each rejected row logs
	// loudly once per process and quietly (DEBUG) on the ~30s re-checks.
	// Pruned against every listing, so it is bounded by the registry's
	// current row count rather than by everything ever seen.
	warnedMu sync.Mutex
	warned   map[string]struct{}
	logger   *zap.Logger
	// discoveredTTL ages out discovered rows not re-announced within it; see
	// config.EndpointHealthConfig.DiscoveredTTLMs. now is the clock seam.
	discoveredTTL time.Duration
	now           func() time.Time
}

func newEndpointSource(st datahubLister, network string, includeDiscovered, allowPrivate bool, discoveredTTL time.Duration, logger *zap.Logger) *endpointSource {
	if discoveredTTL <= 0 {
		discoveredTTL = time.Duration(config.DefaultEndpointHealthDiscoveredTTLMs) * time.Millisecond
	}
	return &endpointSource{
		st:                st,
		network:           network,
		includeDiscovered: includeDiscovered,
		allowPrivate:      allowPrivate,
		validated:         ssrfguard.NewSuccessCache(endpointSourceValidationTTL, endpointSourceValidationCacheMax),
		warned:            map[string]struct{}{},
		logger:            logger,
		discoveredTTL:     discoveredTTL,
		now:               time.Now,
		lookupIP: func(ctx context.Context, host string) ([]net.IP, error) {
			lctx, cancel := context.WithTimeout(ctx, endpointSourceLookupTimeout)
			defer cancel()
			return net.DefaultResolver.LookupIP(lctx, "ip", host)
		},
	}
}

func (a *endpointSource) ListEndpointURLs(ctx context.Context) ([]string, error) {
	eps, err := a.st.ListDatahubEndpoints(ctx, a.network)
	if err != nil {
		return nil, err
	}
	cutoff := a.now().Add(-a.discoveredTTL)
	current := make(map[string]struct{}, len(eps))
	out := make([]string, 0, len(eps))
	for _, ep := range eps {
		// A discovered row not re-announced within the TTL: the peer stopped
		// advertising it. It is skipped before being counted as current, so
		// its warn-dampening entry is pruned and a bad URL that is announced
		// again WARNs once more. Rows with no LastSeen predate its tracking
		// and are kept.
		if ep.Source == store.DatahubEndpointSourceDiscovered && !ep.LastSeen.IsZero() && ep.LastSeen.Before(cutoff) {
			continue
		}
		current[ep.URL] = struct{}{}
		if ep.Source == store.DatahubEndpointSourceDiscovered {
			if !a.includeDiscovered {
				continue
			}
			if !a.validDiscoveredURL(ctx, ep.URL) {
				continue
			}
		}
		out = append(out, ep.URL)
	}
	a.pruneWarned(current)
	return out, nil
}

// pruneWarned drops warn-dampening entries for URLs no longer present in
// the registry listing, keeping the map bounded by the registry's current
// size. A row that later reappears simply WARNs once more.
func (a *endpointSource) pruneWarned(current map[string]struct{}) {
	a.warnedMu.Lock()
	defer a.warnedMu.Unlock()
	for url := range a.warned {
		if _, ok := current[url]; !ok {
			delete(a.warned, url)
		}
	}
}

// validDiscoveredURL runs ssrfguard validation behind the success-only TTL
// cache and handles rejection metering/log dampening.
func (a *endpointSource) validDiscoveredURL(ctx context.Context, url string) bool {
	if _, ok := a.validated.Get(url); ok {
		return true
	}

	err := ssrfguard.ValidateURL(url, a.allowPrivate, func(host string) ([]net.IP, error) {
		return a.lookupIP(ctx, host)
	})
	if err != nil {
		outcome := "invalid"
		if errors.Is(err, ssrfguard.ErrBlockedAddress) {
			outcome = "blocked"
		}
		metrics.TeranodeEndpointRefreshRejectedTotal.WithLabelValues(outcome).Inc()
		a.logRejection(url, err)
		return false
	}

	a.validated.Put(url, "")
	a.warnedMu.Lock()
	delete(a.warned, url) // if it later goes bad again, WARN once more
	a.warnedMu.Unlock()
	return true
}

// logRejection logs a rejected registry row at WARN the first time a URL is
// seen and DEBUG thereafter. A bad row is re-listed every refresh tick in
// every pod; an unconditional WARN would drip permanently from a registry
// row nobody has pruned yet.
func (a *endpointSource) logRejection(url string, err error) {
	a.warnedMu.Lock()
	_, already := a.warned[url]
	if !already {
		a.warned[url] = struct{}{}
	}
	a.warnedMu.Unlock()
	if already {
		a.logger.Debug("skipping invalid discovered datahub url", zap.String("url", url), zap.Error(err))
		return
	}
	a.logger.Warn("skipping invalid discovered datahub url", zap.String("url", url), zap.Error(err))
}
