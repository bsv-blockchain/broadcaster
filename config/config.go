package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	chaintracksconfig "github.com/bsv-blockchain/go-chaintracks/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// ExpandHome rewrites a leading "~" or "~/" to the current user's home dir.
// Other paths (absolute, relative, or empty) are returned unchanged. Centralized
// so every config consumer sees real filesystem paths instead of literal tildes —
// libraries like cockroachdb/pebble don't expand "~" themselves and will happily
// create a directory named "~" in cwd.
func ExpandHome(path string) (string, error) {
	if path == "" || !strings.HasPrefix(path, "~") {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, strings.TrimPrefix(path, "~")), nil
}

// Canonical network names accepted at the top-level `network` config key.
// These are the only values operators ever type; internal mapping to the
// go-teranode-p2p-client network identifiers lives in ResolveP2PNetwork.
const (
	NetworkMainnet     = "mainnet"
	NetworkTestnet     = "testnet"
	NetworkTeratestnet = "teratestnet"
	NetworkRegtest     = "regtest"
	// NetworkTstn is the Teranode scaling test network. It has its own
	// genesis (go-chaincfg TeraScalingTestNetParams) and p2p topic prefix
	// (".../tstn-*"), so it can't be run as regtest.
	NetworkTstn = "tstn"
)

// knownNetworks gates validate() and ResolveP2PNetwork.
var knownNetworks = map[string]struct{}{
	NetworkMainnet:     {},
	NetworkTestnet:     {},
	NetworkTeratestnet: {},
	NetworkRegtest:     {},
	NetworkTstn:        {},
}

// ResolveP2PNetwork maps a canonical BSV Broadcaster network name to the values that
// go-teranode-p2p-client expects: the network identifier used to build pubsub
// topic names, and the bootstrap peer list to hand to libp2p.
//
// The upstream library's getDefaultBootstrapPeers returns bootstraps for the
// keys "main"/"test"/"stn" — its "stn" entry points at
// teratestnet.bootstrap.teranode.bsvb.tech while TopicName("stn", …) builds
// ".../stn-node_status". That mismatch is why configuring network=stn fails to
// see any teratestnet data hub URLs: the bootstrap DNS is right but the topic
// name is wrong. We sidestep it by handing the library the canonical topic
// network ("mainnet"/"testnet"/"teratestnet") and injecting the bootstrap peer
// list ourselves — so topic and peers always agree.
//
// Regtest and tstn deliberately return a nil bootstrap list: neither has a
// canonical bootstrap DNS, so operators must supply p2p.bootstrap_peers
// themselves. validate() enforces this when datahub_discovery is enabled.
func ResolveP2PNetwork(network string) (topicNetwork string, bootstrapPeers []string) {
	switch network {
	case NetworkTestnet:
		return NetworkTestnet, []string{"/dnsaddr/testnet.bootstrap.teranode.bsvb.tech"}
	case NetworkTeratestnet:
		return NetworkTeratestnet, []string{"/dnsaddr/teratestnet.bootstrap.teranode.bsvb.tech"}
	case NetworkRegtest:
		return NetworkRegtest, nil
	case NetworkTstn:
		return NetworkTstn, nil
	case NetworkMainnet, "":
		fallthrough
	default:
		return NetworkMainnet, []string{"/dnsaddr/mainnet.bootstrap.teranode.bsvb.tech"}
	}
}

// ResolveChaintracksNetwork maps a canonical BSV Broadcaster network name to the value
// go-chaintracks accepts at config.P2P.Network. Its chainmanager.getGenesisHeader
// resolves the name through go-chaincfg (accepting "main"/"test"/"teratest" as
// aliases), so we translate at the boundary instead of leaking upstream naming
// into the BSV Broadcaster config surface.
func ResolveChaintracksNetwork(network string) string {
	switch network {
	case NetworkTestnet:
		return "test"
	case NetworkTeratestnet:
		return NetworkTeratestnet
	case NetworkRegtest:
		return NetworkRegtest
	case NetworkTstn:
		return NetworkTstn
	case NetworkMainnet, "":
		fallthrough
	default:
		return "main"
	}
}

type Config struct {
	Mode          string `mapstructure:"mode"`
	LogLevel      string `mapstructure:"log_level"`
	CallbackURL   string `mapstructure:"callback_url"`
	CallbackToken string `mapstructure:"callback_token"`
	StoragePath   string `mapstructure:"storage_path"`
	// Network selects the Bitcoin network everything downstream participates in.
	// Canonical values: "mainnet", "testnet", "teratestnet". Propagated to the
	// libp2p peer-discovery client and to the embedded chaintracks instance so
	// they agree on pubsub topic and bootstrap peers.
	Network       string              `mapstructure:"network"`
	APIServer     API                 `mapstructure:"api"`
	Kafka         Kafka               `mapstructure:"kafka"`
	Store         Store               `mapstructure:"store"`
	DatahubURLs   []string            `mapstructure:"datahub_urls"`
	Teranode      TeranodeConfig      `mapstructure:"teranode"`
	MerkleService MerkleServiceConfig `mapstructure:"merkle_service"`
	P2P           P2PConfig           `mapstructure:"p2p"`
	Health        HealthConfig        `mapstructure:"health"`
	Telemetry     TelemetryConfig     `mapstructure:"telemetry"`
	Propagation   PropagationConfig   `mapstructure:"propagation"`
	BumpBuilder   BumpBuilderConfig   `mapstructure:"bump_builder"`
	Watchdog      WatchdogConfig      `mapstructure:"watchdog"`
	SSE           SSEConfig           `mapstructure:"sse"`
	Webhook       WebhookConfig       `mapstructure:"webhook"`
	Callback      CallbackConfig      `mapstructure:"callback"`
	Events        EventsConfig        `mapstructure:"events"`
	Validator     ValidatorConfig     `mapstructure:"validator"`
	Auth          AuthConfig          `mapstructure:"auth"`
	Admin         AdminConfig         `mapstructure:"admin"`
	RateLimit     RateLimitConfig     `mapstructure:"ratelimit"`
	// ChaintracksServer gates whether the embedded go-chaintracks HTTP API
	// runs alongside api-server. Default is on so the refactor is a drop-in
	// replacement for the original single-binary BSV Broadcaster.
	ChaintracksServer ChaintracksServerConfig `mapstructure:"chaintracks_server"`
	// Chaintracks is the upstream go-chaintracks config; defaults delegated
	// to the library's own SetDefaults so new fields flow through automatically.
	Chaintracks chaintracksconfig.Config `mapstructure:"chaintracks"`

	// Deprecations lists human-readable notices about legacy (pre-rebrand)
	// configuration Load honored, e.g. ARCADE_* environment variables or a
	// config file under /etc/arcade. The caller logs them once a logger
	// exists; they never affect behavior.
	Deprecations []string `mapstructure:"-"`
}

// ChaintracksServerConfig toggles the chaintracks HTTP surface. Separate
// from Chaintracks itself so the instance can be disabled without wiping
// the library's config block. Chaintracks runs as a standalone BSV Broadcaster
// service in production (mode=chaintracks) or as an in-process goroutine
// alongside other services under mode=all. The pod owns its own HTTP
// port — set Port to a value distinct from api.port when chaintracks
// runs in the same process (mode=all).
type ChaintracksServerConfig struct {
	Enabled bool `mapstructure:"enabled"`
	// Host the chaintracks HTTP listener binds to. Default 0.0.0.0.
	Host string `mapstructure:"host"`
	// Port the chaintracks HTTP listener binds to. Default 8083. Must
	// differ from api.port and sse.port when the standalone service
	// runs in the same process (mode=all).
	Port int `mapstructure:"port"`
	// TieScanDepth is how many heights below the tip the block-status
	// tracker re-verifies on each tip update, in both directions: any
	// 'active' block_processing row in the window whose hash is not the
	// active-chain block at its height is marked orphaned, and any
	// 'orphaned' row whose hash IS the active-chain block at its height is
	// reset to active. The first is the detection edge for same-height
	// competition losers, which never produce a chaintracks ReorgEvent — an
	// equal-chainwork alternate never becomes tip (issue #279); the second
	// heals the flip-flop where that loser wins the next block and the
	// ReorgEvent names only the orphans and the new tip (issue #339).
	// Default 20; 0 disables the scan.
	TieScanDepth int `mapstructure:"tie_scan_depth"`
	// TieScanMinIntervalMs debounces the scan across tip bursts (regtest
	// mining, catch-up sync). Default 5000.
	TieScanMinIntervalMs int `mapstructure:"tie_scan_min_interval_ms"`
}

type API struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
	// TrustedPlatform names the proxy whose client-IP header the API trusts.
	// "" (default) trusts no forwarding header, so c.ClientIP() is the TCP
	// peer. "cloudflare" reads CF-Connecting-IP; set it only when every
	// request reaches the pod through Cloudflare (e.g. a Cloudflare Tunnel),
	// because the anonymous rate-limit tier is keyed by this address.
	TrustedPlatform string `mapstructure:"trusted_platform"`
	// TrustedProxies lists the reverse proxies (CIDRs, or bare IPs) allowed
	// to set X-Forwarded-For; gin's ClientIP() only reads that header from a
	// request whose immediate TCP peer matches one of these. Empty (the
	// default) trusts none, so the header is never honored regardless of
	// TrustedPlatform.
	TrustedProxies []string `mapstructure:"trusted_proxies"`
}

// TrustedPlatformCloudflare is the api.trusted_platform value for
// deployments fronted by Cloudflare.
const TrustedPlatformCloudflare = "cloudflare"

// Rate-limit tier names with fixed meaning.
const (
	// TierFree must be configured: it is the fallback for a key whose tier
	// is no longer in config.
	TierFree = "free"
	// TierAnonymous is reserved for unkeyed callers (ratelimit.anonymous).
	TierAnonymous = "anonymous"
)

// AuthConfig gates API-key identification on the public API.
type AuthConfig struct {
	Enabled bool `mapstructure:"enabled"`
	// Store optionally moves API keys and rate-limit quota counters out of
	// the main store into a dedicated MongoDB database, so several BSV Broadcaster
	// deployments (one per network, each with its own store) can share one
	// key set and one quota window per key. Unset (uri empty) keeps keys and
	// quota in the main store. See docs/api-keys.md.
	Store AuthStoreConfig `mapstructure:"store"`
}

// AuthStoreConfig is auth.store. Only MongoDB is supported.
type AuthStoreConfig struct {
	Mongo Mongo `mapstructure:"mongodb"`
}

// Configured reports whether a dedicated auth store is set; when false the
// api-server reads keys and quota from the main store.
func (a AuthStoreConfig) Configured() bool { return a.Mongo.URI != "" }

// AdminConfig is the admin listener: API-key management and block reprocess.
// It must never be routed through the public ingress.
type AdminConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Host    string `mapstructure:"host"`
	Port    int    `mapstructure:"port"`
	Token   string `mapstructure:"token"`
}

// RateLimitConfig is the tiered quota policy. See docs/api-keys.md.
type RateLimitConfig struct {
	Enabled bool `mapstructure:"enabled"`
	// LeaseFraction is the share of a tier's per-minute limit a replica
	// leases from the shared counter at once.
	LeaseFraction float64 `mapstructure:"lease_fraction"`
	// ShrinkThreshold: once a window's shared usage reaches this share of
	// the limit, replicas lease only the exact cost of each request.
	ShrinkThreshold float64 `mapstructure:"shrink_threshold"`
	// KeyCacheTTLMs is how long a resolved key is trusted without a store
	// read: the worst-case delay before a revocation or tier change applies.
	KeyCacheTTLMs int `mapstructure:"key_cache_ttl_ms"`
	// KeyCacheStaleMaxMs is how long a cached key keeps authenticating while
	// the store is unreachable.
	KeyCacheStaleMaxMs int `mapstructure:"key_cache_stale_max_ms"`
	// ExpectedReplicas splits each tier's max_in_flight across api-server
	// replicas (per-replica cap = ceil(max_in_flight / expected_replicas)).
	ExpectedReplicas int                   `mapstructure:"expected_replicas"`
	Costs            RateLimitCosts        `mapstructure:"costs"`
	Anonymous        TierConfig            `mapstructure:"anonymous"`
	Tiers            map[string]TierConfig `mapstructure:"tiers"`
}

// RateLimitCosts are the unit prices charged against a tier's quota.
type RateLimitCosts struct {
	// SubmitTx is charged per transaction: POST /tx is one, POST /txs is n.
	SubmitTx int64 `mapstructure:"submit_tx"`
	// Read is charged per read request and as the base cost of POST /txs.
	Read int64 `mapstructure:"read"`
}

// TierConfig is one tier's limits. Zero means "no limit" for each field.
type TierConfig struct {
	UnitsPerMinute int64 `mapstructure:"units_per_minute"`
	MaxInFlight    int   `mapstructure:"max_in_flight"`
	MaxBatch       int   `mapstructure:"max_batch"`
}

// Kafka configures the message broker. Backend picks between a real Sarama
// client (production, multi-node) and an in-process memory broker (single-
// binary standalone mode). Brokers is required only when Backend=sarama.
type Kafka struct {
	Backend       string   `mapstructure:"backend"` // "sarama" (default) or "memory"
	Brokers       []string `mapstructure:"brokers"`
	ConsumerGroup string   `mapstructure:"consumer_group"`
	MaxRetries    int      `mapstructure:"max_retries"`
	BufferSize    int      `mapstructure:"buffer_size"`
	// MinPartitions is a soft minimum-partition hint for horizontally-scaled
	// topics; BSV Broadcaster fails fast at startup when an existing topic has fewer.
	// TopicPropagation has its own dedicated knob (propagation.partitions)
	// with a fail-closed startup check — see PropagationConfig.Partitions.
	// Leave at 0 or 1 in standalone/single-replica deployments.
	MinPartitions int `mapstructure:"min_partitions"`
	// TopicPrefix namespaces every topic as "<prefix>.<name>" (default
	// "bsv-broadcaster"). Deployments that predate the BSV Broadcaster
	// rebrand set it to "arcade" to keep consuming their existing topics.
	TopicPrefix string `mapstructure:"topic_prefix"`
	// SendTimeoutMs bounds how long the in-process memory broker waits for a
	// slot in a full mailbox before returning ErrBrokerBackpressure to the
	// producer. Only consulted by the memory backend; sarama has its own
	// producer-side timeouts. Default 2000ms — long enough to absorb brief
	// flush gaps, short enough that HTTP handlers don't hang.
	SendTimeoutMs int `mapstructure:"send_timeout_ms"`
}

// Store picks the persistence backend. Backend dispatches construction in
// storefactory.New; sub-blocks are read only when their backend is selected
// so operators don't need to fill in unused sections.
type Store struct {
	Backend string `mapstructure:"backend"` // "aerospike" (default), "pebble", "postgres", or "mongodb"
	// BatchConcurrency tunes the parallel-loop helpers (BatchGetOrInsertStatus,
	// BatchUpdateStatus) used by backends without a native batch path
	// (Aerospike, Pebble). Default 0 → runtime.NumCPU(). Raise to match
	// validator parallelism when DB writes are the bottleneck; lower to
	// reduce DB pool pressure under contention.
	BatchConcurrency int      `mapstructure:"batch_concurrency"`
	Aerospike        Aero     `mapstructure:"aerospike"`
	Pebble           Pebble   `mapstructure:"pebble"`
	Postgres         Postgres `mapstructure:"postgres"`
	Mongo            Mongo    `mapstructure:"mongodb"`
}

type Aero struct {
	Hosts           []string `mapstructure:"hosts"`
	Namespace       string   `mapstructure:"namespace"`
	BatchSize       int      `mapstructure:"batch_size"`
	PoolSize        int      `mapstructure:"pool_size"`
	QueryTimeoutMs  int      `mapstructure:"query_timeout_ms"`
	OpTimeoutMs     int      `mapstructure:"op_timeout_ms"`
	SocketTimeoutMs int      `mapstructure:"socket_timeout_ms"`
	// IdleTimeoutSec sets ClientPolicy.IdleTimeout — client-side reap of idle
	// pooled connections. Required when the Aerospike server runs with
	// proto-fd-idle-ms=0. Set a few seconds below the server value when
	// nonzero. Default 55.
	IdleTimeoutSec int `mapstructure:"idle_timeout_sec"`
	// MaxErrorRate is the threshold for the per-node circuit breaker — once
	// a node returns this many errors within an ErrorRateWindow tend cycles,
	// requests fast-fail with MAX_ERROR_RATE instead of timing out and
	// holding pool slots. Default 5; the Aerospike default of 100 is too
	// lenient for a multi-tenant cluster.
	MaxErrorRate int `mapstructure:"max_error_rate"`
	// ErrorRateWindow is the number of cluster-tend iterations the breaker
	// observes before resetting. Default 1 (~1 second).
	ErrorRateWindow int `mapstructure:"error_rate_window"`
	// BumpChunkSizeBytes is the target chunk size used by InsertBUMP when
	// splitting a compound BUMP across multiple Aerospike records. A single
	// Aerospike record cannot exceed the namespace's write-block-size (default
	// 1 MiB, hard ceiling 8 MiB), so very large BUMPs from scaling networks
	// must be chunked. Default 768 KiB stays safely under the 1 MiB default
	// write-block-size; operators who raise write-block-size to 8 MiB can
	// raise this to ~6 MiB to reduce chunk fan-out for huge BUMPs.
	BumpChunkSizeBytes int `mapstructure:"bump_chunk_size_bytes"`
}

// Postgres configures the Postgres-backed store. Embedded=true spins up
// fergusstrange/embedded-postgres on Start, extracting the bundled Postgres
// binary into EmbeddedCacheDir on first run — so the binary is a one-time
// download, not per-start. DSN is used verbatim when Embedded=false; when
// Embedded=true it's constructed from EmbeddedPort/EmbeddedUser/EmbeddedPass
// and the standalone DataDir.
type Postgres struct {
	DSN              string `mapstructure:"dsn"`
	Embedded         bool   `mapstructure:"embedded"`
	EmbeddedPort     uint32 `mapstructure:"embedded_port"`
	EmbeddedUser     string `mapstructure:"embedded_user"`
	EmbeddedPassword string `mapstructure:"embedded_password"`
	EmbeddedDatabase string `mapstructure:"embedded_database"`
	EmbeddedDataDir  string `mapstructure:"embedded_data_dir"`
	EmbeddedCacheDir string `mapstructure:"embedded_cache_dir"`
	MaxConns         int32  `mapstructure:"max_conns"`
	// SchemaApplyTimeoutMs bounds the slow path of EnsureIndexes (schema apply
	// on a fresh DB or after a schema change). The checksum fast path makes
	// normal restarts skip DDL entirely, so this can be generous. 0 = default
	// (5 minutes). See issue #278.
	SchemaApplyTimeoutMs int `mapstructure:"schema_apply_timeout_ms"`
}

// Pebble configures the embedded Pebble KV backend. Path is the data directory
// (Pebble takes an exclusive file lock, so single-process only). MemTableSizeMB
// and L0CompactionThreshold are the two knobs operators usually want to tune;
// the rest of Pebble's defaults are fine for BSV Broadcaster's workload. SyncWrites trades
// durability of the last handful of writes for a ~10x throughput improvement —
// acceptable for standalone since the reaper will re-send unacked transactions.
type Pebble struct {
	Path                  string `mapstructure:"path"`
	MemTableSizeMB        int    `mapstructure:"memtable_size_mb"`
	L0CompactionThreshold int    `mapstructure:"l0_compaction_threshold"`
	SyncWrites            bool   `mapstructure:"sync_writes"`
}

// Mongo configures the MongoDB-backed store. URI is a standard connection
// string (mongodb:// or mongodb+srv://) and may carry replica-set, auth and
// TLS options; Database is the database BSV Broadcaster writes its collections and
// GridFS buckets into. The backend never uses multi-document transactions, so
// a standalone mongod is sufficient — a replica set is only needed for HA.
//
// OpTimeoutMs bounds single-document reads and writes (including counts);
// QueryTimeoutMs bounds the queries whose result is capped by a limit or a
// page — lists, pages, bulk-write chunks, the blob sweep. Anything whose size
// scales with the data — the iterators, GetTxIDsByBlockHash, the census
// aggregate — runs under the caller's context only, because a fixed deadline
// there does not fail one call, it fails the same call on every retry.
// IndexTimeoutMs bounds EnsureIndexes at boot (createIndexes on a populated,
// restored collection is the one boot step that scales with data). BatchSize
// caps the documents per bulk-write chunk and per $in on the write paths;
// the txid $in on the read paths is a fixed 1000.
//
// MinPoolSize is the idle connection floor the driver keeps open to EVERY
// replica-set member (not just the primary), per process, for the life of the
// process — so its cost is pods × members × floor on each mongod. Keep it
// small; MaxConnIdleMs reaps the connections a burst opened above it.
// MaxPoolSize, MinPoolSize and MaxConnIdleMs each fall back to the URI's
// maxPoolSize / minPoolSize / maxIdleTimeMS when set to 0, then to the
// backend's own default.
type Mongo struct {
	URI              string `mapstructure:"uri"`
	Database         string `mapstructure:"database"`
	ConnectTimeoutMs int    `mapstructure:"connect_timeout_ms"`
	OpTimeoutMs      int    `mapstructure:"op_timeout_ms"`
	QueryTimeoutMs   int    `mapstructure:"query_timeout_ms"`
	IndexTimeoutMs   int    `mapstructure:"index_timeout_ms"`
	MaxPoolSize      int    `mapstructure:"max_pool_size"`
	MinPoolSize      int    `mapstructure:"min_pool_size"`
	MaxConnIdleMs    int    `mapstructure:"max_conn_idle_ms"`
	BatchSize        int    `mapstructure:"batch_size"`
}

type TeranodeConfig struct {
	AuthToken string `mapstructure:"auth_token"`
}

// MerkleServiceConfig selects the merkle-service endpoints BSV Broadcaster registers
// transactions with. URL is the original single-endpoint form; URLs is the
// multi-endpoint form. Both feed Endpoints(), so an existing config with only
// `url` keeps working and `urls` may be added alongside it.
//
// With several endpoints every tx is registered with all of them and every
// endpoint delivers callbacks; BSV Broadcaster treats a callback that carries nothing
// new as a no-op. A tx counts as registered once at least one endpoint
// accepted it, so a single unreachable merkle-service no longer blocks
// broadcast (F-024 still holds: zero acceptances means no broadcast).
type MerkleServiceConfig struct {
	URL       string   `mapstructure:"url"`
	URLs      []string `mapstructure:"urls"`
	AuthToken string   `mapstructure:"auth_token"`
}

// Endpoints returns every configured merkle-service base URL, in config
// order (URL first, then URLs), trimmed, without a trailing slash, and with
// duplicates removed. Empty entries are dropped here; validate rejects them so
// a typo in `urls` fails loudly at startup instead of silently shrinking the
// redundancy set.
func (c MerkleServiceConfig) Endpoints() []string {
	raw := make([]string, 0, 1+len(c.URLs))
	raw = append(raw, c.URL)
	raw = append(raw, c.URLs...)
	seen := make(map[string]struct{}, len(raw))
	out := make([]string, 0, len(raw))
	for _, u := range raw {
		u = strings.TrimSuffix(strings.TrimSpace(u), "/")
		if u == "" {
			continue
		}
		if _, dup := seen[u]; dup {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, u)
	}
	return out
}

// Enabled reports whether the Merkle integration is configured at all. It is
// the single toggle every consumer checks: no endpoints means no /watch
// registration, no /reprocess recovery, and no watchdog.
func (c MerkleServiceConfig) Enabled() bool {
	return len(c.Endpoints()) > 0
}

// P2PConfig controls the libp2p-based peer discovery service. Seeds is the
// legacy block-announcement seed list; DatahubDiscovery and its siblings gate
// the node_status subscription that auto-populates the propagation endpoint
// list. When DatahubDiscovery is false, no libp2p client is started and the
// rest of the P2P fields are ignored.
//
// The network is not set here — it comes from the top-level `network` config
// value so the p2p client and embedded chaintracks stay in sync. Operators only
// need BootstrapPeers here to override the canonical defaults resolved from
// ResolveP2PNetwork (useful for private networks or bootstrap migrations).
type P2PConfig struct {
	Seeds            []string `mapstructure:"seeds"`
	DatahubDiscovery bool     `mapstructure:"datahub_discovery"`
	ListenPort       int      `mapstructure:"listen_port"`
	BootstrapPeers   []string `mapstructure:"bootstrap_peers"`
	DHTMode          string   `mapstructure:"dht_mode"`
	StoragePath      string   `mapstructure:"storage_path"`
	EnableMDNS       bool     `mapstructure:"enable_mdns"`
	AllowPrivateURLs bool     `mapstructure:"allow_private_urls"`
}

type HealthConfig struct {
	Port int `mapstructure:"port"`
	// PprofEnabled mounts net/http/pprof handlers under /debug/pprof on the
	// health server when true. Off by default: pprof's CPU and trace
	// endpoints are expensive to capture and the heap profile carries
	// information an operator may not want exposed continuously. Toggle on
	// in the ConfigMap when capturing a profile, then toggle back off.
	PprofEnabled bool `mapstructure:"pprof_enabled"`
}

// TelemetryConfig configures OpenTelemetry export to an external OTLP
// collector (see docs/plans/otel-integration.md). Disabled by default:
// with Enabled=false, telemetry.Init installs no-op providers and opens
// no network connections, so runtime behaviour matches the pre-OTEL build
// exactly. When Enabled, Endpoint may be left empty to fall back to the
// standard OTEL_EXPORTER_OTLP_ENDPOINT / OTEL_EXPORTER_OTLP_*_ENDPOINT
// environment variables (resolved inside telemetry.Init, not here).
type TelemetryConfig struct {
	Enabled bool `mapstructure:"enabled"`
	// Endpoint is the host:port of the OTLP collector. May be left empty
	// when Enabled=true — the exporter then falls back to the standard
	// OTEL_EXPORTER_OTLP_ENDPOINT / OTEL_EXPORTER_OTLP_{TRACES,METRICS}_ENDPOINT
	// env vars.
	Endpoint string `mapstructure:"endpoint"`
	// Protocol selects the OTLP transport: "grpc" (default) or "http".
	Protocol string `mapstructure:"protocol"`
	// Insecure skips TLS when dialing the collector (e.g. an in-cluster
	// collector reachable without TLS).
	Insecure bool `mapstructure:"insecure"`
	// ServiceName sets the resource's service.name attribute. Default "bsv-broadcaster".
	ServiceName string `mapstructure:"service_name"`
	// Namespace sets the resource's service.namespace attribute (e.g. the
	// downstream solution's name). Omitted from the resource when empty.
	Namespace string `mapstructure:"namespace"`
	// Traces enables the OTLP trace pipeline. Default true.
	Traces bool `mapstructure:"traces"`
	// Metrics enables the OTLP metric pipeline (Prometheus bridge). Default true.
	Metrics bool `mapstructure:"metrics"`
	// Logs, when true, only logs a warning that OTLP log export is not
	// implemented; logs continue to ship via stdout JSON. Default false.
	Logs bool `mapstructure:"logs"`
	// SampleRatio is the ratio (0.0-1.0) fed to a ParentBased(TraceIDRatioBased)
	// sampler. Default 1.0 (sample everything).
	SampleRatio float64 `mapstructure:"sample_ratio"`
	// ExportTimeoutMs bounds Shutdown's ForceFlush+Shutdown of every
	// provider. Default 10000 (10s).
	ExportTimeoutMs int `mapstructure:"export_timeout_ms"`
}

type PropagationConfig struct {
	// Partitions is the minimum partition count BSV Broadcaster requires on
	// bsv-broadcaster.propagation at startup (fail-closed: missing topic or fewer
	// partitions aborts boot; more is fine — extra partitions only cost
	// idle dispatchers). Set it to at least the propagation replica count
	// when scaling horizontally, and widen the topic BEFORE rolling the
	// config (a pod configured above the broker's actual count crash-loops
	// by design).
	//
	// Ordering model (#295, replacing the #151 exact-1 rule): the api
	// server keys every propagation message by its dependency family —
	// all txs of one submission connected through in-batch spends share a
	// partition key — so per-partition Kafka order still delivers parents
	// to a dispatcher before their children. A cross-submission child that
	// still lands on a different partition (submitted before its parent, or
	// its parent already accepted) is absorbed by the missing-parent safety
	// net (TX_MISSING_PARENT is a retryable condition, never a terminal
	// verdict on its own). Default 1.
	//
	// Since the hosted-service report (issue 2) a single-tx POST /tx also
	// keys like an in-flight parent it spends: the api-server looks the
	// parents up in the store (LookupPartitionKeys) and reuses the key a
	// parent still held by a dispatcher (RECEIVED/SENT_TO_NETWORK) was
	// published with, so a chain submitted one request at a time still lands
	// on one partition. A resubmission
	// (single or batch) reuses the key its row was stored with. Bounded by
	// ParentKeyLookupMaxInputs.
	Partitions int `mapstructure:"partitions"`
	// ParentKeyLookupMaxInputs bounds how many of a single POST /tx
	// submission's inputs the api-server consults in the store to key the
	// child like an in-flight parent (and how many external parents per tx a
	// POST /txs family consults). A transaction with more inputs only consults
	// the first N. 0 disables every partition-key read, the pre-fix
	// behavior: a single-tx submission keys by its own txid and a batch by
	// its families, and a resubmission no longer reuses its stored key.
	// Default 8.
	ParentKeyLookupMaxInputs int `mapstructure:"parent_key_lookup_max_inputs"`
	MerkleConcurrency        int `mapstructure:"merkle_concurrency"`
	// RetryMaxAttempts bounds how many times the propagator will push a
	// transaction back through the in-memory requeue loop when a broadcast
	// produced no per-tx network verdict (no healthy peer, body-less 5xx,
	// or a conflict-family 409 whose lines could not be attributed to any
	// submitted tx). Merkle /watch failures do not consume this budget:
	// registration retries are a separate failure domain, and /watch must
	// succeed before broadcast (F-024) without counting as a propagation
	// attempt. After the network budget is spent the tx is parked at
	// PENDING_RETRY, which releases the dispatcher's in-flight entry so the
	// Kafka commit watermark can advance; the reaper then owns the durable
	// retry.
	//
	// The bound is not a nicety. On 2026-08-11 (dev-ovh-1, BSV Broadcaster v0.11.5) a
	// 21-tx batch of stale transactions drew a permanent HTTP 409 from
	// Teranode — 21 "UTXO_SPENT (70): <outpoint>:<vout> utxo already spent by
	// tx <spender>[n]" lines, all keyed by the spent outpoint rather than by
	// any submitted txid. With no attributable verdict every tx fell to the
	// requeue arm and looped forever: ~2,410 409s in two minutes, 42 of 43
	// batches requeued, 4,679 transactions pinned in the dispatcher's
	// inFlight map and therefore 337,247 messages of Kafka lag frozen on the
	// single-partition bsv-broadcaster.propagation topic while the leader burned 4.2
	// cores. Nothing new could propagate until operators seeked the consumer
	// group past the backlog by hand.
	//
	// Defaults to 5 (matching the shipped viper default). Non-positive values
	// fall back to the default rather than disabling the bound — an unbounded
	// requeue is the failure mode this exists to prevent.
	RetryMaxAttempts int `mapstructure:"retry_max_attempts"`
	// RetryBackoffMs is the flat wait (milliseconds) before a requeued tx
	// lands back on the dispatcher for another broadcast attempt. Flat by
	// design, not exponential: the budget above bounds total attempts, so
	// a predictable wall-clock window (attempts × backoff ≈ 10s at the
	// defaults) is what matters; the reaper's cadence dominates anything
	// slower. Non-positive falls back to 2000ms. NOTE: this knob was
	// declared but unread before #295 — deployments that explicitly set
	// it (e.g. to the old default 500) start feeling their configured
	// value on upgrade.
	RetryBackoffMs int `mapstructure:"retry_backoff_ms"`
	// PendingRetryBackoffMs is the base delay before a PENDING_RETRY row's
	// first reaper attempt. Each subsequent attempt doubles it, capped at
	// PendingRetryMaxBackoffMs, with ±20% jitter so a batch parked together
	// doesn't re-broadcast in lockstep.
	//
	// Before #299 there was no per-row schedule at all: eligibility was
	// "parked more than a hardcoded 5 minutes ago", so every parked tx —
	// including a child whose parent lands a second later — waited the full
	// 5 minutes, and a row that kept failing was retried every tick forever.
	// Defaults to 5000ms; non-positive falls back to the default.
	PendingRetryBackoffMs int `mapstructure:"pending_retry_backoff_ms"`
	// PendingRetryMaxBackoffMs caps the exponential above. Defaults to
	// 300000ms (5 minutes) — the old flat eligibility age, so the slowest
	// this ever retries matches the previous behavior while early attempts
	// are far faster.
	PendingRetryMaxBackoffMs int `mapstructure:"pending_retry_max_backoff_ms"`
	// PendingRetryMaxAttempts bounds how many times the reaper rebroadcasts
	// a PENDING_RETRY row before giving up and writing REJECTED.
	//
	// Without a bound a tx whose parent never arrives — the single most
	// common client mistake, spending an outpoint that does not exist — is
	// retried forever. Before #299 such rows were not retried forever so
	// much as silently abandoned: their timestamp_at never moved, so at
	// staleScanLookback (24h) they fell out of the reaper's scan window and
	// stayed PENDING_RETRY with no terminal status, no log and no metric.
	//
	// The default 288 is 24h at the 5-minute backoff cap, so give-up happens
	// about when abandonment used to — except it is now an observable
	// verdict the submitter can read. Non-positive falls back to the default.
	PendingRetryMaxAttempts int `mapstructure:"pending_retry_max_attempts"`
	ReaperIntervalMs        int `mapstructure:"reaper_interval_ms"`
	ReaperBatchSize         int `mapstructure:"reaper_batch_size"`
	// ReaperRebroadcastBatch caps how many stuck transactions the reaper's
	// durable-rebroadcast backstop pushes back through the register+broadcast
	// pipeline per tick (reapOnce collects at most this many stale RECEIVED /
	// ACCEPTED_BY_NETWORK / SEEN rows). During the 2026-08-10 requeue-spiral
	// incident ~178k txs were stranded at RECEIVED with the reaper as their
	// only drain path; the then-hardcoded 200-per-tick cap at the default 30s
	// interval drained ~6.7 tx/s (~7.4h to clear), forcing operators to crank
	// reaper_interval_ms down instead. Raise this to drain a large backlog
	// faster without touching the tick interval. Defaults to 200 (the
	// historical hardcoded cap) when unset or non-positive.
	ReaperRebroadcastBatch int `mapstructure:"reaper_rebroadcast_batch"`
	// LeaseTTLMs bounds how long the reaper lease remains valid without a
	// renewal. Set to at least 2–3× reaper_interval_ms so a missed tick
	// doesn't trigger a false-positive failover. Defaults to 3× interval.
	LeaseTTLMs int `mapstructure:"lease_ttl_ms"`
	// TeranodeMaxBatchSize caps the number of transactions per POST /txs call.
	// Teranode's propagation server allows at most maxTransactionsPerRequest
	// (1024) per request, and since v0.15.0 checks the count with ">=" BEFORE
	// reading each transaction — so a body holding exactly 1024 is refused
	// with a bare 400 "Invalid request body: too many transactions" after
	// every one of them was already read and dispatched. The effective rule
	// is therefore FEWER than 1024. Splitting into chunks keeps the batch
	// endpoint in play even under Kafka backlog (an oversize batch used to
	// cascade into a 1k+ per-tx fallback storm); a chunk the peer still
	// refuses is narrowed by halving rather than requeued blind.
	//
	// The cap is inclusive on BSV Broadcaster's side; the safety margin lives in the
	// default (DefaultTeranodeMaxBatchSize = 1000), not in the comparison.
	// Non-positive falls back to the default.
	TeranodeMaxBatchSize int `mapstructure:"teranode_max_batch_size"`
	// TeranodeMaxBatchBytes caps the sum of raw transaction bytes per POST
	// /txs call. The body is the plain concatenation of each tx's bytes, so
	// this is exactly the request Content-Length. Teranode reads at most
	// maxDataPerRequest (32 MiB) per request, checked with ">=" before each
	// read like the count above, so the effective rule is strictly UNDER
	// 32 MiB; exceeding it draws a bare 400 "Invalid request body: too much
	// data" after the peer already processed everything it read (a proxy or
	// Echo body limit answers 413 instead). Neither response carries a
	// per-tx verdict.
	//
	// A single transaction larger than this cap is still sent, alone in its
	// own chunk — the cap decides where chunks end, never whether a tx is
	// broadcast; Teranode stays the oracle for per-tx size policy. Resident
	// request memory per pod is bounded by roughly this value ×
	// len(endpoints) × max_parallel_chunks × max_concurrent_batches.
	// Inclusive on BSV Broadcaster's side; the margin is in the default
	// (DefaultTeranodeMaxBatchBytes = 16 MiB). Non-positive falls back to
	// the default.
	TeranodeMaxBatchBytes int                  `mapstructure:"teranode_max_batch_bytes"`
	EndpointHealth        EndpointHealthConfig `mapstructure:"endpoint_health"`
	// RegisterReplayOnStart re-registers every non-terminal tx in the store
	// with merkle-service /watch at startup. This compensates for the lack
	// of durability of /watch entries on the merkle-service side: when
	// merkle-service loses its postgres state (data wipe, migration,
	// recreation), BSV Broadcaster's previously-submitted txs are no longer being
	// watched and no STUMP callbacks will ever fire for them. Without
	// replay, the only recovery path is resubmitting every in-flight tx
	// manually.
	//
	// Defaults to true. Set to false if you operate against a guaranteed-
	// durable merkle-service deployment, or if the cost of one POST /watch
	// per in-flight tx at boot is unacceptable for your fleet size.
	RegisterReplayOnStart *bool `mapstructure:"register_replay_on_start"`
	// RegisterReplayLookbackHours bounds how far back IterateStatusesSince
	// scans when replaying. Older txs are very likely terminal already
	// (MINED/IMMUTABLE), and a non-terminal tx older than this window has
	// almost certainly stalled — re-registering it on every boot won't
	// unstick it. Defaults to 24 (one day), tightened from the original
	// 7 days after issue #145.
	RegisterReplayLookbackHours int `mapstructure:"register_replay_lookback_hours"`
	// MerkleReplaySkipRecentMinutes lets the startup replay skip txs whose
	// MerkleRegisteredAt is within this window. /watch is INSERT ... ON
	// CONFLICT DO NOTHING on the merkle-service side and does not refresh
	// expires_at, so re-registering a tx merkle-service already knows about
	// is wasted work. Default -1 (auto): 30 (matches merkle-service
	// postMineTTLSec) with a single merkle-service, 0 with several, because
	// the stamp is set once any one of them accepted the tx and the pool's
	// queue of registrations owed to a lagging endpoint is in memory, so a
	// restart must re-send them. Set to 0 to disable the skip and
	// re-register every non-terminal tx — useful for forcing a full re-sync
	// after a known merkle-service wipe. Issue #145.
	MerkleReplaySkipRecentMinutes int `mapstructure:"merkle_replay_skip_recent_minutes"`
	// MerkleReplayRPS caps the average requests-per-second the startup
	// replay issues against merkle-service. Implemented as an inter-batch
	// sleep proportional to batch size, so the actual rate hovers around
	// the configured RPS rather than burst-then-stall. 0 disables
	// throttling. Default 50 — a 24h replay over a 1.85M-row store would
	// otherwise pin merkle-service at its postgres write ceiling for hours.
	// Issue #145.
	MerkleReplayRPS int `mapstructure:"merkle_replay_rps"`
	// MaxPending caps the dispatcher's pending-batch slice — the
	// broadcast-bound queue that accumulates between flushes. When
	// the slice reaches this size, the dispatcher stops accepting
	// new admits from Kafka; handleMessage's channel send blocks,
	// the Kafka consumer goroutine pauses pulls, and backpressure
	// flows back to the broker. Held waiters do NOT count (they're
	// in a separate map). Defaults to 50000 — large enough to
	// absorb a multi-minute downstream stall at 50 TPS without
	// blocking the consumer, small enough to bound memory.
	MaxPending int `mapstructure:"max_pending"`
	// MaxConcurrentBatches caps how many flushed batches run their
	// register+broadcast pipeline concurrently. With concurrency=1 (the
	// historical default), batch N+1 cannot begin merkle /watch until
	// batch N's broadcast completes — meaning sustained-100-TPS traffic
	// pays ~half-a-pipeline-cycle of queue wait per tx on top of the
	// pipeline work itself. Bumping to ≥2 lets the register and broadcast
	// stages overlap across adjacent batches. F-024 is preserved per-batch
	// (each goroutine registers before it broadcasts) and per-row lattice
	// guards prevent out-of-order status transitions from regressing state.
	// Defaults to 4 — enough to fully overlap pipeline stages at typical
	// pipeline times without flooding merkle-service or teranode.
	MaxConcurrentBatches int `mapstructure:"max_concurrent_batches"`
	// BroadcastWorkers sizes the persistent goroutine pool that runs every
	// per-endpoint POST /txs HTTP call. Peak in-flight jobs is
	// MaxConcurrentBatches × MaxParallelChunks × len(healthy endpoints).
	// Under-sized workers serialize the pool and eat the parallelism gain
	// from smaller chunks — at 8 concurrent batches × 4 chunks × 8 endpoints
	// peak load = 256 jobs, the default. Lower it on small fleets to bound
	// goroutine count.
	BroadcastWorkers int `mapstructure:"broadcast_workers"`
	// MaxParallelChunks caps how many chunk broadcasts within a single
	// flushBatch can run concurrently. Each chunk already fans out across
	// every healthy endpoint, so effective in-flight is MaxParallelChunks ×
	// len(endpoints) per batch. Bigger values let a single oversized flush
	// spread chunk work across the broadcast pool instead of serializing.
	// Default 4.
	MaxParallelChunks int `mapstructure:"max_parallel_chunks"`
}

// EndpointHealthConfig tunes the per-endpoint circuit-breaker in teranode.Client.
// FailureThreshold is the number of consecutive failures before an endpoint is
// marked unhealthy and excluded from broadcasts. ProbeIntervalMs is how often
// the recovery probe runs against the unhealthy set; ProbeTimeoutMs bounds each
// probe request. MinHealthyEndpoints is an advisory warning threshold — when
// the healthy count drops below it a single WARN is logged, but broadcasts are
// never blocked by this value.
//
// RefreshIntervalMs governs how often each pod's teranode.Client polls the
// shared store for newly registered datahub URLs (the cross-pod discovery
// path). Smaller values mean a fresh pod converges faster; larger values
// reduce store load. Zero or negative values fall back to the documented
// defaults at client construction time.
type EndpointHealthConfig struct {
	FailureThreshold int `mapstructure:"failure_threshold"`
	// BroadcastFailureThreshold is the slow-track breaker: how many
	// consecutive non-2xx broadcast responses sideline an endpoint. Zero
	// falls back to the teranode-client default (10). Set lower for
	// stricter pruning, higher to tolerate flappy peers.
	BroadcastFailureThreshold int `mapstructure:"broadcast_failure_threshold"`
	ProbeIntervalMs           int `mapstructure:"probe_interval_ms"`
	ProbeTimeoutMs            int `mapstructure:"probe_timeout_ms"`
	MinHealthyEndpoints       int `mapstructure:"min_healthy_endpoints"`
	RefreshIntervalMs         int `mapstructure:"refresh_interval_ms"`
	// DiscoveredTTLMs ages out peer-discovered datahub URLs: a discovered
	// registry row whose LastSeen is older than this is no longer handed to
	// the client, so a URL no peer announces any more stops being probed and
	// re-admitted on restart. Configured URLs are never aged out. Keep it well
	// above the node_status announcement interval (~10s). Zero or negative
	// falls back to DefaultEndpointHealthDiscoveredTTLMs.
	DiscoveredTTLMs int `mapstructure:"discovered_ttl_ms"`
}

// BumpBuilderConfig controls the BUMP construction workflow. GraceWindowMs is the
// delay applied after receiving BLOCK_PROCESSED before reading STUMPs from the store,
// giving merkle-service retries time to land for any STUMPs that initially got a 5xx.
type BumpBuilderConfig struct {
	GraceWindowMs int `mapstructure:"grace_window_ms"`
	// HeaderWaitMs bounds how long the DataHub-fetch canonical-root
	// validator waits for chaintracks to learn a freshly-mined block's
	// header before giving up and skipping the cross-check. bump-builder is
	// driven by the BLOCK_PROCESSED Kafka message while chaintracks ingests
	// headers over an independent teranode P2P subscription; for a block
	// mined seconds earlier the Kafka path usually wins that race, and
	// without a wait the validator silently skipped its only independent
	// canonicality check. <= 0 disables the wait (single lookup, legacy
	// behavior).
	HeaderWaitMs int `mapstructure:"header_wait_ms"`
	// DataHubMaxBlockBytes caps a single /block/<hash> response body fetched
	// from a DataHub endpoint, in bytes. The DataHub serves block metadata
	// (header + subtree-hash list + coinbase tx + coinbase BUMP), so the
	// default of 1 GiB is two-plus orders of magnitude over a realistic
	// Teranode payload while still bounding memory against a hostile or
	// malfunctioning endpoint. A value <= 0 selects bump.DefaultMaxBlockBytes.
	// Mitigates F-007 (DataHub block fetch reads unbounded response bodies
	// into memory).
	DataHubMaxBlockBytes int64 `mapstructure:"datahub_max_block_bytes"`
	// AnchorGuardEnabled gates the write-time anchor guard (issue #279):
	// before marking a block's txs MINED, bump-builder checks that the
	// block is chaintracks' active-chain block at its height and refuses
	// when the height is provably held by a same-height competitor. Fail-
	// open on any uncertainty. Default true (viper); false restores the
	// legacy last-writer-wins behavior as an instant rollback lever.
	AnchorGuardEnabled bool `mapstructure:"anchor_guard_enabled"`
	// StumpRetentionMinutes is how long STUMP rows are kept after the
	// block's BUMP was built before the janitor deletes them. STUMPs are no
	// longer pruned right after a build: with several merkle-services each
	// one delivers its own STUMP set, and a later BLOCK_PROCESSED must be
	// able to re-read the full set to decide whether it adds leaves the
	// stored BUMP lacks. <= 0 selects the default (60).
	StumpRetentionMinutes int `mapstructure:"stump_retention_minutes"`
	// Reconciler tunes the anchor reconciler that heals txs anchored to
	// orphaned blocks (issue #279).
	Reconciler ReconcilerConfig `mapstructure:"reconciler"`
}

// ReconcilerConfig tunes the anchor reconciler — the bump-builder-hosted
// loop that consumes orphaned block_processing rows and re-anchors (or
// reverts) the transactions still pointing at them. See
// services/bump_builder/reconciler.go for the algorithm.
type ReconcilerConfig struct {
	// Enabled gates the whole loop. Default true (viper); false is the
	// rollback lever — orphaned rows then queue up (reconciled_at stays
	// NULL) and are healed when re-enabled.
	Enabled bool `mapstructure:"enabled"`
	// IntervalMs is the tick cadence for consuming the orphaned-block
	// queue. Default 30000.
	IntervalMs int `mapstructure:"interval_ms"`
	// BatchSize caps txids per SetMinedByTxIDs call / bulk status event
	// during re-anchor. Default 5000 (matches maxTxIDsPerBulkEvent).
	BatchSize int `mapstructure:"batch_size"`
	// BlocksPerTick bounds how many orphaned blocks one tick reconciles.
	// Default 4.
	BlocksPerTick int `mapstructure:"blocks_per_tick"`
	// NeighborhoodDepth is how many heights ABOVE an orphan's height are
	// searched for the canonical block containing its txs (a deeper reorg
	// re-bins txs into later blocks). Default 6.
	NeighborhoodDepth int `mapstructure:"neighborhood_depth"`
	// MaxDeferAttempts bounds how many ticks an orphan waits for the
	// canonical block's BUMP (each defer fires a merkle-service /reprocess
	// when wired) before falling back to revert-all — safe, because
	// SEEN_ON_NETWORK → MINED is lattice-legal and a late canonical
	// redelivery re-mines via the short-circuit path. Default 10.
	MaxDeferAttempts int `mapstructure:"max_defer_attempts"`
	// RewatchConcurrency is how many merkle-service /watch calls run at once
	// when the reconciler re-registers the txs anchored to an orphan. 0 falls
	// back to propagation.merkle_concurrency (the same merkle-service), then
	// to the client default — set it here in microservice mode, where the
	// propagation settings are only on the propagation deployment.
	RewatchConcurrency int `mapstructure:"rewatch_concurrency"`
	// StartupFullScan runs one pass over the block_processing table after
	// startup, orphan-marking any 'active' row whose height's canonical
	// header differs — the deep backstop that also heals incidents that
	// predate this code (like the height-764 block on the scaling cluster).
	// Bounded by FullScanDepth (or the explicit FullScan{Min,Max}Height
	// target). Default true.
	StartupFullScan bool `mapstructure:"startup_full_scan"`
	// FullScanDepth bounds the STARTUP full-scan to heights within this many
	// blocks of the active tip (store.GetActiveTipBlockHeight): the sweep
	// only considers rows with height >= tip-FullScanDepth. This stops the
	// backstop from becoming a whole-history rewrite on a long chain — the
	// original scan paged the ENTIRE table oldest-first and re-orphaned
	// hundreds of historical competition losers, starving the actual recent
	// incident (issue #282). Default 144 (~1 day on BSV; matches the
	// watchdog's recency window). <= 0 selects the default; a chain shorter
	// than the horizon (tip <= depth) scans unbounded, which is cheap. For
	// deep recovery of an old incident, use the explicit target below.
	FullScanDepth int `mapstructure:"full_scan_depth"`
	// FullScanMinHeight / FullScanMaxHeight, when EITHER is > 0, point the
	// startup scan at an explicit height range [min, max] (max 0 = up to the
	// tip), OVERRIDING the FullScanDepth horizon — operator-targeted incident
	// recovery. E.g. min=760 max=770 heals just the height-764 neighborhood
	// from its already-stored canonical BUMP without touching the rest of the
	// table. min=1 restores the legacy whole-history sweep as an opt-in.
	// Both default 0 (use the depth horizon).
	FullScanMinHeight uint64 `mapstructure:"full_scan_min_height"`
	FullScanMaxHeight uint64 `mapstructure:"full_scan_max_height"`
	// RevertWhenUnreconcilable controls the fallback when a block's canonical
	// BUMP is unavailable and the defer cap is reached. Default false: PARK
	// the block (issue #282) — leave its txs MINED against the orphan and
	// stamp reconciled_at so it drops out of the queue (no revert, no
	// infinite retry). A later stored/rebuilt canonical BUMP re-anchors them
	// through the normal mine path; un-mining a tx that is almost certainly
	// in the not-yet-fetched canonical block is a downgrade, not a repair.
	// true restores the pre-#282 revert-all-to-SEEN_ON_NETWORK fallback as an
	// opt-in.
	RevertWhenUnreconcilable bool `mapstructure:"revert_when_unreconcilable"`
	// FullScanChaintracksReadyTimeoutMs bounds how long Start waits, before the
	// FIRST startup full-scan attempt, for the chain-header source to sync up
	// to the store's active tip. The bump-builder pod runs an EMBEDDED
	// chaintracks with ephemeral storage, so every deploy wipes its headers and
	// it resyncs from genesis (height 0); a scan fired before it catches up
	// sees GetHeaderByHeight return nil for every height, fails open on every
	// row, and heals nothing — silently breaking reorg recovery in exactly the
	// window it is needed (right after a deploy). The wait uses bounded backoff
	// and respects context cancellation. On timeout the scan is NOT abandoned:
	// it re-runs on a later tick once chaintracks is ready (self-healing across
	// the resync), so a slow catch-up is never fatal. Default 120000 (2 min);
	// <= 0 selects the default.
	FullScanChaintracksReadyTimeoutMs int `mapstructure:"full_scan_chaintracks_ready_timeout_ms"`
}

// WatchdogConfig tunes the stale-block recovery watchdog. Defaults are
// applied in setDefaults; zero values for everything except Enabled fall
// back to those defaults. The watchdog requires at least one merkle_service
// endpoint (no /reprocess target otherwise) and runs as a standalone BSV Broadcaster
// service (mode=watchdog) or alongside other services when mode=all. At
// most one replica fires per tick — coordination via the
// `block-processing-watchdog` lease.
type WatchdogConfig struct {
	// Enabled gates the watchdog goroutine. Defaults to true; operators
	// can set false to disable temporarily without removing the wiring.
	Enabled bool `mapstructure:"enabled"`
	// IntervalMs is the period between watchdog ticks. Default 30s — matches
	// the propagation reaper cadence.
	IntervalMs int `mapstructure:"interval_ms"`
	// StaleThresholdMs is how long a block_processing row can sit with
	// header_seen_at set but processed_at NULL before the watchdog
	// considers it stale. Default 2 min: comfortably longer than a healthy
	// merkle-service round-trip but short enough that recovery happens
	// before downstream consumers notice the gap.
	StaleThresholdMs int `mapstructure:"stale_threshold_ms"`
	// RecencyDepth bounds the candidate set to blocks within N of the
	// active tip. Default 144 (~1 day on BSV). Prevents a chaintracks
	// catch-up after a long BSV Broadcaster outage from flooding merkle-service
	// with thousands of historical /reprocess calls.
	RecencyDepth int `mapstructure:"recency_depth"`
	// BatchSize caps how many stale rows one tick acts on. Default 100.
	BatchSize int `mapstructure:"batch_size"`
	// MaxConcurrent caps in-flight /reprocess HTTP calls per tick.
	// Default 4 — keeps merkle-service load bounded.
	MaxConcurrent int `mapstructure:"max_concurrent"`
	// LeaseTTLMs is the lease lifetime for `block-processing-watchdog`.
	// 0 keeps the 3×interval default applied at construction time.
	LeaseTTLMs int `mapstructure:"lease_ttl_ms"`
	// InitialBackoffMs is the first transient-failure backoff. Default 1 min.
	InitialBackoffMs int `mapstructure:"initial_backoff_ms"`
	// MaxBackoffMs caps transient-failure backoff growth. Default 30 min.
	MaxBackoffMs int `mapstructure:"max_backoff_ms"`
	// TerminalBackoffMs is the backoff applied on a 4xx response — the
	// block likely isn't on the consensus chain, so retrying soon would
	// produce the same disagreement. Default 4 h.
	TerminalBackoffMs int `mapstructure:"terminal_backoff_ms"`
	// MaxReprocessAttempts caps total /reprocess dispatches per block; once a
	// block is re-driven this many times it is parked (reprocessing stops) so a
	// permanently un-finalizable block can't flood bsv-broadcaster.block_processed and
	// starve the bump-builder consumer. Default 10; <= 0 disables the cap.
	MaxReprocessAttempts int `mapstructure:"max_reprocess_attempts"`
	// MaxStaleAgeMs parks a block whose header_seen_at is older than this,
	// regardless of attempt count — a restart-proof backstop (the in-memory
	// attempt counter resets on redeploy). Default 6 h; <= 0 disables it.
	MaxStaleAgeMs int `mapstructure:"max_stale_age_ms"`
}

// SSEConfig governs the standalone SSE (server-sent events) service.
// SSE runs as its own pod in production (mode=sse) or as an in-process
// goroutine alongside other services under mode=all. Each pod that
// hosts SSE binds its own HTTP port — set Port to a value distinct from
// api.port when running mode=all so the two listeners don't collide.
//
// The fan-out path consumes from the shared events.Publisher (Kafka)
// and serves Last-Event-ID catchup from the shared store, so an SSE pod
// only needs read access to the store and a Kafka consumer connection.
type SSEConfig struct {
	// Enabled gates the /events endpoint and the subscriber goroutine.
	// Default true: extracted from api-server in the microservice
	// decomposition, kept on by default so the in-process bundle
	// (mode=all) keeps working without operator action.
	Enabled bool `mapstructure:"enabled"`
	// Host the SSE listener binds to. Default 0.0.0.0 — clients hit
	// this through a K8s Service that fronts the SSE Deployment.
	Host string `mapstructure:"host"`
	// Port the SSE listener binds to. Default 8082. Must differ from
	// api.port when SSE runs in the same process (mode=all).
	Port int `mapstructure:"port"`
	// ClientBufferSize is the per-connection SSE send-channel capacity.
	// The manager unfans one Kafka message into a burst of per-tx events
	// (bulk-unfan coalesces ~50 txids per message), so a single burst of
	// ~50 must fit with headroom; sizing this too small silently drops
	// events for slow or bursty consumers (the channel send is
	// non-blocking, so overflow is lost, not backpressured). Default
	// DefaultSSEClientBuffer. A value <= 0 selects the default.
	ClientBufferSize int `mapstructure:"client_buffer_size"`
	// CatchupOnDrop enables mid-stream store-backed catchup for token-scoped
	// clients whose send channel overflowed: instead of losing the dropped
	// events until the client reconnects with Last-Event-ID, the connection's
	// writer replays them from the store as soon as it drains its backlog.
	// Replay may duplicate frames the client already received (clients dedupe
	// by txid+status). Default true; false restores the previous drop-only
	// behavior. Tokenless (firehose) clients always keep drop-only behavior —
	// the store catchup source is token-scoped.
	CatchupOnDrop bool `mapstructure:"catchup_on_drop"`
	// DrainQuiesceMS is how long shutdown waits, after /ready starts failing,
	// before it begins closing client connections. It covers endpoint-removal
	// propagation: kube-proxy and the ingress learn the pod is unready
	// asynchronously, so cutting connections immediately means clients
	// reconnect straight back into a terminating pod. Default
	// DefaultSSEDrainQuiesceMS.
	DrainQuiesceMS int `mapstructure:"drain_quiesce_ms"`
	// DrainRetryMaxMS bounds the reconnect delay handed to clients on
	// shutdown. Each connection gets its own value drawn uniformly below this
	// and emitted as an SSE `retry:` directive on its shutdown frame, so a
	// rolling restart releases clients as a ramp rather than a synchronized
	// herd hitting a pod that is still cold. Default
	// DefaultSSEDrainRetryMaxMS; zero omits the hint and clients fall back to
	// their own backoff.
	DrainRetryMaxMS int `mapstructure:"drain_retry_max_ms"`
	// ShutdownTimeoutMS bounds the final http.Server.Shutdown. Connections are
	// already closing by then, so this only covers stragglers. Default
	// DefaultSSEShutdownTimeoutMS.
	ShutdownTimeoutMS int `mapstructure:"shutdown_timeout_ms"`
}

// WebhookConfig tunes the HTTP webhook delivery service. The service
// subscribes to status updates and POSTs them to each submission's
// CallbackURL; failures are retried with exponential backoff persisted via
// the store's UpdateDeliveryStatus.
type WebhookConfig struct {
	// MaxRetries caps how many times a failed POST is re-attempted before
	// the submission is given up on. Mirrors arc's default of 10.
	MaxRetries int `mapstructure:"max_retries"`
	// ExpirationMinutes bounds the total wall-clock lifetime of a webhook
	// delivery. Past this point the service stops retrying even if
	// MaxRetries hasn't been hit. Defaults to 24 hours.
	ExpirationMinutes int `mapstructure:"expiration_minutes"`
	// InitialBackoffMs is the first retry delay; subsequent retries double
	// it (capped). Defaults to 5s, matching arc.
	InitialBackoffMs int `mapstructure:"initial_backoff_ms"`
	// MaxBackoffMs caps how long backoff can grow between retries. Default
	// 5 minutes.
	MaxBackoffMs int `mapstructure:"max_backoff_ms"`
	// HTTPTimeoutMs caps how long a single POST attempt may run. Default
	// 10s — webhook receivers should ack fast or risk being timed out.
	HTTPTimeoutMs int `mapstructure:"http_timeout_ms"`
	// MaxConcurrentDeliveries bounds the worker pool that fans status
	// updates out to callback URLs. The service's channel reader hands
	// each status to the pool and immediately returns to draining the
	// upstream events.Publisher channel — without this decoupling, a
	// single slow callback target (synchronous http.Client.Do up to
	// HTTPTimeoutMs) would block the channel reader and cause the
	// publisher to drop subsequent events as the in-memory buffer
	// filled. Default 32: comfortably above expected concurrent slow
	// callbacks while staying well under the per-pod TCP/connection
	// budget. Increase only if pool saturation
	// (bsv_broadcaster_webhook_pool_saturated_total) shows non-trivial drop rate.
	MaxConcurrentDeliveries int `mapstructure:"max_concurrent_deliveries"`
	// ReaperIntervalMs controls how often the webhook retry sweep runs.
	// The sweep re-fires deliveries whose POST failed and whose
	// NextRetryAt has elapsed — without it, the CAS-at-claim-time advance
	// of LastDeliveredStatus would leave a single-attempt failure as a
	// permanent loss. Default 30s, matching propagation's reaper cadence.
	ReaperIntervalMs int `mapstructure:"reaper_interval_ms"`
	// LeaseTTLMs controls how long the webhook-reaper lease is valid.
	// Only the lease holder runs the sweep; the rest of the replicas
	// no-op. Default 3 × ReaperIntervalMs.
	LeaseTTLMs int `mapstructure:"lease_ttl_ms"`
}

// CallbackConfig governs the SSRF guard that protects api-server's
// X-CallbackUrl registration and the webhook delivery client's outbound
// dials. Both layers share the same knob so an operator who opts in
// for internal-network callbacks doesn't have to remember to flip a
// matching flag elsewhere. See finding F-017 / issue #75.
type CallbackConfig struct {
	// AllowPrivateIPs, when true, disables the SSRF guard. Default false:
	// X-CallbackUrl values whose host parses as a loopback / link-local /
	// metadata / RFC1918 IP are rejected at submit time, and the webhook
	// delivery http.Client refuses to dial those IPs at connect time.
	// Operators running purely against internal services (testing rigs,
	// k8s service DNS, intranet webhooks) can set this true.
	AllowPrivateIPs bool `mapstructure:"allow_private_ips"`
	// MaxBodyBytes caps the size of an inbound POST
	// /api/v1/merkle-service/callback body, in bytes. The callback receives
	// JSON with embedded STUMP payloads (subtree merkle paths) that can be
	// genuinely large for busy subtrees, but unbounded reads let a malicious
	// or malfunctioning peer exhaust memory. Default 16 MiB is well over a
	// realistic STUMP delivery (~hundreds of KiB even for the largest
	// subtrees observed in production) while still bounding worst-case
	// memory use per request. A value <= 0 selects DefaultCallbackMaxBodyBytes.
	// Mitigates F-019 (callback JSON bodies and STUMP payloads are unbounded).
	MaxBodyBytes int64 `mapstructure:"max_body_bytes"`
}

// DefaultCallbackMaxBodyBytes is the fallback value for
// CallbackConfig.MaxBodyBytes when an operator leaves it unset (or sets a
// non-positive value). 16 MiB is generous enough for the largest realistic
// STUMP payload while still bounding memory against a hostile peer; see
// F-019 for the threat model.
const DefaultCallbackMaxBodyBytes int64 = 16 << 20

// EventsConfig tunes the in-process events.Publisher. SubscriberBuffer is
// the channel capacity each Subscribe call mints — when a downstream
// consumer (SSE manager, webhook delivery) can't drain fast enough the
// publisher logs "subscriber channel full, dropping update" and drops.
// Raising the buffer absorbs larger bursts without dropping; the practical
// ceiling is ~65536 (beyond that you're masking a slow consumer rather
// than absorbing a spike). Each Subscribe gets its own buffer, so total
// memory scales with subscribers × SubscriberBuffer × sizeof(*TransactionStatus).
type EventsConfig struct {
	SubscriberBuffer int `mapstructure:"subscriber_buffer"`
}

// DefaultEventsSubscriberBuffer is the fallback channel capacity for each
// events.Publisher.Subscribe channel when SubscriberBuffer is unset or
// non-positive. 4096 is 16× the original 256 — enough headroom for typical
// status-update bursts without committing significant memory upfront.
const DefaultEventsSubscriberBuffer = 4096

// DefaultSSEClientBuffer is the fallback per-connection SSE send-channel
// capacity when SSEConfig.ClientBufferSize is unset or non-positive. The
// manager unfans one Kafka message into a burst of per-tx events, so the
// buffer must absorb many such bursts between writer drains; 8192 gives ample
// headroom over a single ~50-event bulk-unfan while staying a modest
// per-connection memory cost (channel of *TransactionStatus pointers).
const DefaultSSEClientBuffer = 8192

// SSE graceful-drain defaults.
//
// A long-lived streaming connection is never "idle", so http.Server.Shutdown
// can never complete on its own: it burns its whole timeout, the process then
// exits, and every stream is severed with a TCP RST — sending every client back
// simultaneously. These bound an explicit drain instead: fail readiness, wait
// for the endpoint to be withdrawn, then release clients with staggered
// reconnect hints.
//
// Their sum must fit inside the pod's terminationGracePeriodSeconds, or the
// kubelet SIGKILLs mid-drain and the RST is back.
const (
	// DefaultSSEDrainQuiesceMS covers endpoint-removal propagation through
	// kube-proxy and the ingress.
	DefaultSSEDrainQuiesceMS = 10_000
	// DefaultSSEDrainRetryMaxMS is the widest reconnect delay handed out during
	// a drain; each client draws its own beneath it.
	DefaultSSEDrainRetryMaxMS = 30_000
	// DefaultSSEShutdownTimeoutMS bounds the final server shutdown once clients
	// have been released.
	DefaultSSEShutdownTimeoutMS = 15_000
)

// ValidatorConfig controls intake-time transaction validation. It carries the
// operator-facing fee floor enforced against EF/BEEF submissions; the consensus
// and policy script rules come from teranode's BDK-backed validator (keyed by
// the top-level network), not from this config. Wired through to the validator
// at process startup.
type ValidatorConfig struct {
	// MinFeePerKB is the network's minimum acceptable fee rate in
	// satoshis per kilobyte. Transactions below this rate are rejected
	// at intake. Zero (the default) falls back to DefaultValidatorMinFeePerKB
	// at intake unless AcceptZeroFee is true (which forces a zero floor
	// regardless).
	//
	// Zero additionally signals "not operator-configured" to the GET /policy
	// endpoint (issue #212): the advertised miningFee then becomes the lowest
	// fee observed from peer node_status announcements, falling back to
	// DefaultValidatorMinFeePerKB when no peer has been heard. A non-zero value
	// (or AcceptZeroFee) is reported verbatim.
	//
	// An observed fee is floored at 1 sat/kB: peer announcements are
	// unauthenticated and the rule is a bare minimum, so a single peer
	// advertising 0 would otherwise disable fee enforcement entirely. Setting
	// AcceptZeroFee is the only way to reach a 0 floor.
	MinFeePerKB uint64 `mapstructure:"min_fee_per_kb"`

	// AcceptZeroFee, when true, pins the validator's fee floor to exactly
	// 0 sat/kB, so any transaction including fee=0 is accepted at intake.
	// Intended for testnet/regtest/teratestnet environments where the
	// upstream Teranode is also configured to accept zero-fee txs. Leave
	// false (default) on mainnet and any environment that should enforce
	// the production fee floor. Takes precedence over MinFeePerKB.
	AcceptZeroFee bool `mapstructure:"accept_zero_fee"`

	// DisableFinalityCheck turns off the api-server's nLockTime/BIP113
	// intake gate (issue #245). The gate is UX-only — teranode remains
	// authoritative and the check fails open when chain state is
	// unavailable — so this exists purely as an emergency escape hatch.
	DisableFinalityCheck bool `mapstructure:"disable_finality_check"`

	// The fields below back the ARC-compatible GET /policy endpoint
	// (issue #212). Each is the policy value BSV Broadcaster advertises and, for
	// the size/sigop fields, enforces.

	// MaxTxSizePolicy caps accepted transaction size in bytes.
	//
	// Zero (the default) signals "not operator-configured", the same sentinel
	// MinFeePerKB uses: the limit then tracks the largest size any peer
	// advertises in its node_status fee_policy, so BSV Broadcaster rejects a transaction
	// at intake only when no node on the network would accept it. Observation
	// can only widen the limit from DefaultValidatorMaxTxSizePolicy, never
	// narrow it, so a misconfigured peer cannot cause false rejections. Falls
	// back to DefaultValidatorMaxTxSizePolicy when no peer has been heard.
	// A non-zero value is enforced and reported, but is clamped to the BDK consensus maximum.
	MaxTxSizePolicy uint64 `mapstructure:"max_tx_size_policy"`

	// MaxScriptSizePolicy caps accepted script size in bytes. Zero (the
	// default) tracks the network exactly as MaxTxSizePolicy does, falling back
	// to DefaultValidatorMaxScriptSizePolicy.
	MaxScriptSizePolicy uint64 `mapstructure:"max_script_size_policy"`

	// MaxTxSigopsCountsPolicy caps sigops per transaction. 0 => unlimited,
	// the BSV post-Genesis default, and is reported verbatim in /policy.
	// Unlike the size limits this is not tracked from the network: 0 already
	// means "unlimited" here, so it cannot double as an "unset" sentinel.
	MaxTxSigopsCountsPolicy int64 `mapstructure:"max_tx_sigops_counts_policy"`

	// StandardFormatSupported is advertised verbatim in GET /policy. It must
	// stay false: intake converts every submission to extended format before
	// validation, and a standard (non-EF) transaction carries no input source
	// data, so the BDK validator cannot compute fees or run scripts for it.
	// Such a submission is rejected with ARC 460 (validator/convert.go); there
	// is no fallback that fetches parent outputs from the network, so clients
	// must submit EF (intake does not decode BEEF). The flag exists only so an operator who adds
	// such a fallback can advertise it without a code change. Default false.
	StandardFormatSupported bool `mapstructure:"standard_format_supported"`

	// ObservedFeeTTLMs bounds how long a policy observed from a peer's
	// node_status announcement stays eligible for the network computation. A
	// peer not re-heard within this window is dropped so a departed node can't
	// pin the fee low — or the size limits high — forever.
	// 0 => DefaultValidatorObservedFeeTTLMs.
	ObservedFeeTTLMs uint64 `mapstructure:"observed_fee_ttl_ms"`

	// ObservedFeeRefreshMs is how often the api-server recomputes the
	// network-derived policy from observed peer policies and pushes it into the
	// intake validator (issue #212). Applies to every dimension the operator
	// has NOT pinned; a pinned dimension is left alone.
	// 0 => DefaultValidatorObservedFeeRefreshMs.
	ObservedFeeRefreshMs uint64 `mapstructure:"observed_fee_refresh_ms"`
}

// DefaultValidatorMinFeePerKB is the production fee floor in satoshis
// per kilobyte. 100 reflects the current BSV miner policy minimum.
// Tests and ad-hoc deployments may set ValidatorConfig.MinFeePerKB to
// a lower value to accept older or non-standard txs.
const DefaultValidatorMinFeePerKB = 100

// Default /txs chunk caps for the propagation service (issue #271). Both sit
// under Teranode's hard per-request limits (services/propagation/Server.go:
// maxTransactionsPerRequest = 1024, maxDataPerRequest = 32 MiB), which the
// peer checks with ">=" before each read — so the effective upstream rules
// are strictly below those values. The propagator reads these same constants
// for its non-positive fallback, so the code and the shipped config can never
// disagree about what "unset" means.
const (
	// DefaultTeranodeMaxBatchSize is the fallback for
	// propagation.teranode_max_batch_size: 1000 txs per POST /txs.
	DefaultTeranodeMaxBatchSize = 1000
	// DefaultTeranodeMaxBatchBytes is the fallback for
	// propagation.teranode_max_batch_bytes: 16 MiB of raw tx bytes per POST
	// /txs, half of Teranode's 32 MiB ceiling and equal to the Kafka
	// producer message cap, so anything the pipeline can carry fits.
	DefaultTeranodeMaxBatchBytes = 16 * 1024 * 1024
)

// Default policy values for the GET /policy endpoint. They mirror teranode's
// canonical BSV policy defaults (validator.defaultPolicySettings) so BSV Broadcaster
// advertises the same limits the upstream node enforces when the operator
// leaves them unset.
const (
	// DefaultValidatorMaxTxSizePolicy is the max accepted tx size in bytes (10 MiB).
	DefaultValidatorMaxTxSizePolicy = 10485760
	// DefaultValidatorMaxScriptSizePolicy is the max accepted script size in bytes.
	DefaultValidatorMaxScriptSizePolicy = 500000
	// DefaultValidatorObservedFeeTTLMs is how long (15 min) a peer's observed
	// mining fee remains eligible for the network-minimum.
	DefaultValidatorObservedFeeTTLMs = 900000
	// DefaultValidatorObservedFeeRefreshMs is how often (30 s) the api-server
	// recomputes the network-minimum fee and updates the intake validator.
	DefaultValidatorObservedFeeRefreshMs = 30000
	// DefaultEndpointHealthDiscoveredTTLMs is how long (1h) a peer-discovered
	// datahub URL stays listed after its last node_status announcement.
	DefaultEndpointHealthDiscoveredTTLMs = 3600000
)

func BindFlags(cmd *cobra.Command) {
	cmd.Flags().String("mode", "all", "Service mode: all, api-server, bump-builder, propagation, p2p-client")
	cmd.Flags().String("config", "", "Path to config file")
	cmd.Flags().String("log-level", "info", "Log level: debug, info, warn, error")
	_ = viper.BindPFlag("mode", cmd.Flags().Lookup("mode"))
	_ = viper.BindPFlag("log_level", cmd.Flags().Lookup("log-level"))
}

func Load(cmd *cobra.Command) (*Config, error) {
	cfgFile, _ := cmd.Flags().GetString("config")
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("/etc/bsv-broadcaster")
		viper.AddConfigPath(legacyConfigDir)
	}

	legacyEnv := applyLegacyEnv()

	viper.SetEnvPrefix(EnvPrefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	setDefaults()

	if err := viper.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			return nil, fmt.Errorf("reading config: %w", err)
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	if err := expandPaths(&cfg); err != nil {
		return nil, err
	}

	if err := validate(&cfg); err != nil {
		return nil, err
	}

	if len(legacyEnv) > 0 {
		cfg.Deprecations = append(cfg.Deprecations, fmt.Sprintf(
			"environment variables with the legacy %s_ prefix are deprecated; rename them to %s_*: %s",
			LegacyEnvPrefix, EnvPrefix, strings.Join(legacyEnv, ", ")))
	}
	if used := viper.ConfigFileUsed(); strings.HasPrefix(used, legacyConfigDir+"/") {
		cfg.Deprecations = append(cfg.Deprecations, fmt.Sprintf(
			"config file %s is in the legacy directory %s; move it to /etc/bsv-broadcaster", used, legacyConfigDir))
	}

	return &cfg, nil
}

const (
	// EnvPrefix is the prefix for environment-variable overrides: config key
	// store.backend is read from BSV_BROADCASTER_STORE_BACKEND.
	EnvPrefix = "BSV_BROADCASTER"
	// LegacyEnvPrefix is the prefix used before the BSV Broadcaster rebrand.
	// Variables under it are still honored (see applyLegacyEnv).
	LegacyEnvPrefix = "ARCADE"

	// legacyConfigDir is the pre-rebrand config directory, searched after
	// /etc/bsv-broadcaster.
	legacyConfigDir = "/etc/arcade"
)

// applyLegacyEnv copies every ARCADE_* environment variable to its
// BSV_BROADCASTER_* equivalent so deployments configured before the rebrand
// keep working. An explicitly set BSV_BROADCASTER_* variable always wins. It
// returns the sorted legacy names it mapped.
func applyLegacyEnv() []string {
	var mapped []string
	for _, kv := range os.Environ() {
		name, value, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		suffix, ok := strings.CutPrefix(name, LegacyEnvPrefix+"_")
		if !ok || suffix == "" {
			continue
		}
		newName := EnvPrefix + "_" + suffix
		if _, set := os.LookupEnv(newName); set {
			continue
		}
		if err := os.Setenv(newName, value); err != nil {
			continue
		}
		mapped = append(mapped, name)
	}
	slices.Sort(mapped)
	return mapped
}

// expandPaths resolves "~/..." in every filesystem path the config exposes,
// so downstream consumers (pebble, postgres, chaintracks, p2p) never see a
// literal tilde. New path fields should be added here.
func expandPaths(cfg *Config) error {
	for _, p := range []*string{
		&cfg.StoragePath,
		&cfg.Store.Pebble.Path,
		&cfg.Store.Postgres.EmbeddedDataDir,
		&cfg.Store.Postgres.EmbeddedCacheDir,
		&cfg.P2P.StoragePath,
		&cfg.Chaintracks.StoragePath,
	} {
		expanded, err := ExpandHome(*p)
		if err != nil {
			return fmt.Errorf("expanding path %q: %w", *p, err)
		}
		*p = expanded
	}
	return nil
}

func setDefaults() {
	viper.SetDefault("mode", "all")
	viper.SetDefault("log_level", "info")
	viper.SetDefault("api.host", "0.0.0.0")
	viper.SetDefault("api.port", 8080)
	viper.SetDefault("api.trusted_platform", "")
	viper.SetDefault("api.trusted_proxies", []string{})
	// API keys, admin and rate limiting all ship off. Only the free tier
	// and the anonymous tier have defaults: viper deep-merges maps, so a
	// default "pro" tier could never be removed by an operator's config.
	viper.SetDefault("auth.enabled", false)
	// auth.store.mongodb ships unset (uri empty = keys and quota live in the
	// main store). The keys are declared anyway so AutomaticEnv honors
	// BSV_BROADCASTER_AUTH_STORE_MONGODB_* even when the config file omits the block;
	// zero timeouts/pool mean the mongodb backend's own defaults.
	viper.SetDefault("auth.store.mongodb.uri", "")
	viper.SetDefault("auth.store.mongodb.database", "")
	viper.SetDefault("auth.store.mongodb.connect_timeout_ms", 0)
	viper.SetDefault("auth.store.mongodb.op_timeout_ms", 0)
	viper.SetDefault("auth.store.mongodb.query_timeout_ms", 0)
	viper.SetDefault("auth.store.mongodb.index_timeout_ms", 0)
	viper.SetDefault("auth.store.mongodb.max_pool_size", 0)
	viper.SetDefault("auth.store.mongodb.min_pool_size", 0)
	viper.SetDefault("auth.store.mongodb.max_conn_idle_ms", 0)
	viper.SetDefault("admin.enabled", false)
	viper.SetDefault("admin.host", "127.0.0.1")
	viper.SetDefault("admin.port", 8090)
	viper.SetDefault("admin.token", "")
	viper.SetDefault("ratelimit.enabled", false)
	viper.SetDefault("ratelimit.lease_fraction", 0.10)
	viper.SetDefault("ratelimit.shrink_threshold", 0.80)
	viper.SetDefault("ratelimit.key_cache_ttl_ms", 30000)
	viper.SetDefault("ratelimit.key_cache_stale_max_ms", 600000)
	viper.SetDefault("ratelimit.expected_replicas", 1)
	viper.SetDefault("ratelimit.costs.submit_tx", 10)
	viper.SetDefault("ratelimit.costs.read", 1)
	viper.SetDefault("ratelimit.anonymous.units_per_minute", 50)
	viper.SetDefault("ratelimit.anonymous.max_in_flight", 5)
	viper.SetDefault("ratelimit.anonymous.max_batch", 1)
	viper.SetDefault("ratelimit.tiers.free.units_per_minute", 50)
	viper.SetDefault("ratelimit.tiers.free.max_in_flight", 5)
	viper.SetDefault("ratelimit.tiers.free.max_batch", 1)
	viper.SetDefault("kafka.backend", "sarama")
	// callback_* and merkle_service.* defaults exist so viper.AutomaticEnv
	// knows the keys: without a registered key a BSV_BROADCASTER_MERKLE_SERVICE_URLS
	// or BSV_BROADCASTER_CALLBACK_TOKEN override is silently ignored (see
	// TestPropagationBatchCapsBind for the same trap). Empty values keep the
	// integration disabled.
	viper.SetDefault("callback_url", "")
	viper.SetDefault("callback_token", "")
	viper.SetDefault("merkle_service.url", "")
	viper.SetDefault("merkle_service.urls", []string{})
	viper.SetDefault("merkle_service.auth_token", "")

	viper.SetDefault("kafka.brokers", []string{"localhost:9092"})
	viper.SetDefault("kafka.consumer_group", "bsv-broadcaster")
	viper.SetDefault("kafka.topic_prefix", "bsv-broadcaster")
	viper.SetDefault("kafka.max_retries", 5)
	viper.SetDefault("kafka.buffer_size", 10000)
	viper.SetDefault("kafka.send_timeout_ms", 2000)
	viper.SetDefault("store.backend", "aerospike")
	viper.SetDefault("store.aerospike.hosts", []string{"localhost:3000"})
	viper.SetDefault("store.aerospike.namespace", "bsv_broadcaster")
	viper.SetDefault("store.aerospike.batch_size", 500)
	viper.SetDefault("store.aerospike.pool_size", 256)
	viper.SetDefault("store.aerospike.query_timeout_ms", 8000)
	viper.SetDefault("store.aerospike.op_timeout_ms", 3000)
	viper.SetDefault("store.aerospike.socket_timeout_ms", 5000)
	viper.SetDefault("store.pebble.path", "~/.bsv-broadcaster/pebble")
	viper.SetDefault("store.pebble.memtable_size_mb", 64)
	viper.SetDefault("store.pebble.l0_compaction_threshold", 4)
	viper.SetDefault("store.pebble.sync_writes", false)
	viper.SetDefault("store.postgres.embedded", false)
	viper.SetDefault("store.postgres.embedded_port", 0)
	viper.SetDefault("store.postgres.embedded_user", "bsv_broadcaster")
	viper.SetDefault("store.postgres.embedded_password", "bsv_broadcaster")
	viper.SetDefault("store.postgres.embedded_database", "bsv_broadcaster")
	viper.SetDefault("store.postgres.embedded_data_dir", "~/.bsv-broadcaster/postgres")
	viper.SetDefault("store.postgres.embedded_cache_dir", "~/.bsv-broadcaster/postgres-cache")
	viper.SetDefault("store.postgres.max_conns", 16)
	viper.SetDefault("store.postgres.schema_apply_timeout_ms", 300000)
	viper.SetDefault("store.mongodb.uri", "mongodb://localhost:27017")
	viper.SetDefault("store.mongodb.database", "bsv_broadcaster")
	viper.SetDefault("store.mongodb.connect_timeout_ms", 10000)
	viper.SetDefault("store.mongodb.op_timeout_ms", 3000)
	viper.SetDefault("store.mongodb.query_timeout_ms", 8000)
	viper.SetDefault("store.mongodb.index_timeout_ms", 300000)
	viper.SetDefault("store.mongodb.max_pool_size", 64)
	viper.SetDefault("store.mongodb.min_pool_size", 2)
	viper.SetDefault("store.mongodb.max_conn_idle_ms", 300000)
	viper.SetDefault("store.mongodb.batch_size", 500)
	viper.SetDefault("health.port", 8081)

	// OTEL telemetry export: off by default. See TelemetryConfig doc comment
	// and docs/plans/otel-integration.md. Every field gets an explicit
	// SetDefault — including the zero-valued ones — because viper's
	// AutomaticEnv only recognizes BSV_BROADCASTER_TELEMETRY_* overrides for keys it
	// already knows about (same reasoning as the chaintracks.url default
	// below); without these, BSV_BROADCASTER_TELEMETRY_ENDPOINT/NAMESPACE/INSECURE
	// would silently be ignored.
	viper.SetDefault("telemetry.enabled", false)
	viper.SetDefault("telemetry.endpoint", "")
	viper.SetDefault("telemetry.protocol", "grpc")
	viper.SetDefault("telemetry.insecure", false)
	viper.SetDefault("telemetry.service_name", "bsv-broadcaster")
	viper.SetDefault("telemetry.namespace", "")
	viper.SetDefault("telemetry.traces", true)
	viper.SetDefault("telemetry.metrics", true)
	viper.SetDefault("telemetry.logs", false)
	viper.SetDefault("telemetry.sample_ratio", 1.0)
	viper.SetDefault("telemetry.export_timeout_ms", 10000)
	viper.SetDefault("propagation.partitions", 1)
	viper.SetDefault("propagation.parent_key_lookup_max_inputs", 8)
	viper.SetDefault("propagation.merkle_concurrency", 10)
	viper.SetDefault("propagation.retry_max_attempts", 5)
	viper.SetDefault("propagation.retry_backoff_ms", 2000)
	viper.SetDefault("propagation.pending_retry_backoff_ms", 5000)
	viper.SetDefault("propagation.pending_retry_max_backoff_ms", 300000)
	viper.SetDefault("propagation.pending_retry_max_attempts", 288)
	viper.SetDefault("propagation.reaper_interval_ms", 30000)
	viper.SetDefault("propagation.reaper_batch_size", 500)
	viper.SetDefault("propagation.reaper_rebroadcast_batch", 200)
	// 0 keeps New()'s 3×reaper_interval default, so changing reaper_interval
	// automatically moves the lease TTL unless the operator opts into a fixed value.
	viper.SetDefault("propagation.lease_ttl_ms", 0)
	viper.SetDefault("propagation.teranode_max_batch_size", DefaultTeranodeMaxBatchSize)
	viper.SetDefault("propagation.teranode_max_batch_bytes", DefaultTeranodeMaxBatchBytes)
	viper.SetDefault("propagation.max_concurrent_batches", 4)
	viper.SetDefault("propagation.broadcast_workers", 256)
	viper.SetDefault("propagation.max_parallel_chunks", 4)
	viper.SetDefault("propagation.endpoint_health.failure_threshold", 3)
	viper.SetDefault("propagation.endpoint_health.broadcast_failure_threshold", 10)
	viper.SetDefault("propagation.endpoint_health.probe_interval_ms", 30000)
	viper.SetDefault("propagation.endpoint_health.probe_timeout_ms", 2000)
	viper.SetDefault("propagation.endpoint_health.min_healthy_endpoints", 0)
	viper.SetDefault("propagation.endpoint_health.refresh_interval_ms", 30000)
	viper.SetDefault("propagation.endpoint_health.discovered_ttl_ms", DefaultEndpointHealthDiscoveredTTLMs)
	// Replay BSV Broadcaster's in-flight tx set to merkle-service /watch at startup.
	// Defaults to true: /watch is idempotent on merkle-service so the cost
	// of replaying covers the (real, observed) case where merkle-service
	// drops its registration state and BSV Broadcaster silently stops receiving
	// callbacks for everything in-flight.
	viper.SetDefault("propagation.register_replay_on_start", true)
	viper.SetDefault("propagation.register_replay_lookback_hours", 24)
	viper.SetDefault("propagation.merkle_replay_skip_recent_minutes", -1)
	viper.SetDefault("propagation.merkle_replay_rps", 50)

	viper.SetDefault("network", NetworkMainnet)

	viper.SetDefault("p2p.datahub_discovery", false)
	viper.SetDefault("p2p.listen_port", 9905)
	viper.SetDefault("p2p.dht_mode", "off")
	viper.SetDefault("p2p.enable_mdns", false)
	viper.SetDefault("p2p.allow_private_urls", false)

	viper.SetDefault("webhook.max_retries", 10)
	viper.SetDefault("webhook.expiration_minutes", 60*24)
	viper.SetDefault("webhook.initial_backoff_ms", 5000)
	viper.SetDefault("webhook.max_backoff_ms", 300000)
	viper.SetDefault("webhook.http_timeout_ms", 10000)
	viper.SetDefault("webhook.max_concurrent_deliveries", 32)

	// Callback SSRF guard defaults to enabled (allow_private_ips=false). See
	// finding F-017 / issue #75 — accepting any X-CallbackUrl turned BSV Broadcaster
	// into a blind SSRF primitive against internal services and cloud
	// metadata endpoints.
	viper.SetDefault("callback.allow_private_ips", false)
	// Inbound callback body cap. 16 MiB headroom over realistic STUMP payloads
	// while bounding memory against a hostile or malfunctioning peer (F-019).
	viper.SetDefault("callback.max_body_bytes", DefaultCallbackMaxBodyBytes)

	// Per-subscription channel capacity for events.Publisher. 4096 is 16× the
	// original 256 — enough to absorb status-update bursts without committing
	// significant memory upfront. Operators seeing "subscriber channel full,
	// dropping update" warnings can raise this up to ~65536.
	viper.SetDefault("events.subscriber_buffer", DefaultEventsSubscriberBuffer)

	viper.SetDefault("storage_path", "~/.bsv-broadcaster")
	viper.SetDefault("chaintracks_server.enabled", true)
	// Delegate chaintracks-library defaults (mode, network, bootstrap, p2p, …)
	// to the upstream SetDefaults so any new fields are picked up automatically.
	var ct chaintracksconfig.Config
	ct.SetDefaults(viper.GetViper(), "chaintracks")
	// go-chaintracks SetDefaults omits chaintracks.url, so viper.AutomaticEnv()
	// can't see BSV_BROADCASTER_CHAINTRACKS_URL — register the key explicitly here.
	viper.SetDefault("chaintracks.url", "")
	viper.SetDefault("bump_builder.grace_window_ms", 30000)
	// 10s covers the observed chaintracks P2P-ingestion lag for a
	// freshly-mined block (typically < 2s) with margin, while bounding how
	// long a DataHub fetch can stall when chaintracks genuinely never
	// learns the header (e.g. it is down).
	viper.SetDefault("bump_builder.header_wait_ms", 10000)
	// 1 GiB — DataHub /block/<hash> responses contain block metadata
	// (header + subtree hashes + coinbase tx + coinbase BUMP). 1 GiB is
	// two-plus orders of magnitude of headroom over a realistic payload
	// while still bounding memory against a hostile DataHub. See F-007.
	viper.SetDefault("bump_builder.datahub_max_block_bytes", int64(1*1024*1024*1024))
	viper.SetDefault("bump_builder.anchor_guard_enabled", true)
	viper.SetDefault("bump_builder.stump_retention_minutes", 60)
	viper.SetDefault("bump_builder.reconciler.enabled", true)
	viper.SetDefault("bump_builder.reconciler.interval_ms", 30000)
	viper.SetDefault("bump_builder.reconciler.batch_size", 5000)
	viper.SetDefault("bump_builder.reconciler.blocks_per_tick", 4)
	viper.SetDefault("bump_builder.reconciler.neighborhood_depth", 6)
	viper.SetDefault("bump_builder.reconciler.rewatch_concurrency", 0)
	viper.SetDefault("bump_builder.reconciler.max_defer_attempts", 10)
	viper.SetDefault("bump_builder.reconciler.startup_full_scan", true)
	viper.SetDefault("bump_builder.reconciler.full_scan_depth", 144)
	viper.SetDefault("bump_builder.reconciler.full_scan_min_height", 0)
	viper.SetDefault("bump_builder.reconciler.full_scan_max_height", 0)
	viper.SetDefault("bump_builder.reconciler.revert_when_unreconcilable", false)
	viper.SetDefault("bump_builder.reconciler.full_scan_chaintracks_ready_timeout_ms", 120000)
	// Block-processing watchdog (standalone BSV Broadcaster service — mode=watchdog
	// in production, in-process under mode=all): on by default. The runtime
	// nil-guards the merkle-service client; an unconfigured deployment
	// (no merkle_service.url/urls) skips the watchdog regardless of this flag.
	viper.SetDefault("watchdog.enabled", true)
	viper.SetDefault("watchdog.interval_ms", 30000)
	viper.SetDefault("watchdog.stale_threshold_ms", 120000)
	viper.SetDefault("watchdog.recency_depth", 144)
	viper.SetDefault("watchdog.batch_size", 100)
	viper.SetDefault("watchdog.max_concurrent", 4)
	// 0 keeps the 3×interval default chosen at watchdog construction time.
	viper.SetDefault("watchdog.lease_ttl_ms", 0)
	viper.SetDefault("watchdog.initial_backoff_ms", 60000)
	viper.SetDefault("watchdog.max_backoff_ms", 1800000)
	viper.SetDefault("watchdog.terminal_backoff_ms", 14400000)
	viper.SetDefault("watchdog.max_reprocess_attempts", 10)
	viper.SetDefault("watchdog.max_stale_age_ms", 21600000) // 6h

	// SSE standalone service (mode=sse, or in-process under mode=all):
	// enabled by default. Distinct port from api.port avoids the bind
	// collision in single-binary deployments.
	viper.SetDefault("sse.enabled", true)
	viper.SetDefault("sse.host", "0.0.0.0")
	viper.SetDefault("sse.port", 8082)
	viper.SetDefault("sse.drain_quiesce_ms", DefaultSSEDrainQuiesceMS)
	viper.SetDefault("sse.drain_retry_max_ms", DefaultSSEDrainRetryMaxMS)
	viper.SetDefault("sse.shutdown_timeout_ms", DefaultSSEShutdownTimeoutMS)
	viper.SetDefault("sse.client_buffer_size", DefaultSSEClientBuffer)
	viper.SetDefault("sse.catchup_on_drop", true)

	// chaintracks standalone service: enabled by default. The bind port
	// must differ from api.port and sse.port in single-binary deployments
	// (mode=all).
	viper.SetDefault("chaintracks_server.host", "0.0.0.0")
	viper.SetDefault("chaintracks_server.port", 8083)
	viper.SetDefault("chaintracks_server.tie_scan_depth", 20)
	viper.SetDefault("chaintracks_server.tie_scan_min_interval_ms", 5000)

	// Intake validator fee floor in satoshis per KB. Default 0 means "not
	// operator-configured": intake still enforces DefaultValidatorMinFeePerKB
	// (the validator's built-in floor), while GET /policy advertises the
	// network-observed minimum (issue #212). SetDefault is required even at 0
	// so AutomaticEnv honors the BSV_BROADCASTER_VALIDATOR_MIN_FEE_PER_KB override.
	viper.SetDefault("validator.min_fee_per_kb", 0)

	// ARC-compatible GET /policy fields (issue #212). The two size limits
	// default to 0 for the same reason min_fee_per_kb does: it marks them "not
	// operator-configured", so they track the most permissive limit the network
	// advertises, falling back to DefaultValidatorMax*Policy until a peer is
	// heard. Every key needs an explicit SetDefault for AutomaticEnv to honour
	// its BSV_BROADCASTER_VALIDATOR_* override (same reasoning as above).
	viper.SetDefault("validator.max_tx_size_policy", 0)
	viper.SetDefault("validator.max_script_size_policy", 0)
	viper.SetDefault("validator.max_tx_sigops_counts_policy", 0)
	viper.SetDefault("validator.standard_format_supported", false)
	viper.SetDefault("validator.observed_fee_ttl_ms", DefaultValidatorObservedFeeTTLMs)
	viper.SetDefault("validator.observed_fee_refresh_ms", DefaultValidatorObservedFeeRefreshMs)
}

// validateMerkleService checks the merkle_service block. Every entry of
// `url`/`urls` must be an absolute http(s) URL; a blank entry in `urls` is a
// misconfiguration rather than "disabled" (only an entirely empty block
// disables the integration). When at least one endpoint is set the top-level
// callback_token is mandatory — see the comment in validate.
func validateMerkleService(cfg *Config) error {
	for i, u := range cfg.MerkleService.URLs {
		if strings.TrimSpace(u) == "" {
			return fmt.Errorf("merkle_service.urls[%d] is empty; remove the entry or set a URL", i)
		}
	}
	// Every error below names the URL through redactURL: the URL may carry
	// credentials (user info, a token in the query, or a scheme-less
	// "user:pass@host" that parses as an opaque URL), and the startup error
	// is logged.
	for _, u := range cfg.MerkleService.Endpoints() {
		parsed, err := url.Parse(u)
		if err != nil {
			return fmt.Errorf("merkle_service url %s does not parse", redactURL(u))
		}
		// The URL is shown in /health, metric labels and startup logs, so
		// credentials in it would leak there; auth_token is the supported
		// way to authenticate.
		if parsed.User != nil {
			return fmt.Errorf("merkle_service url %s must not contain user info; use merkle_service.auth_token for authentication", redactURL(u))
		}
		if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("merkle_service url %s must be an absolute http(s) URL", redactURL(u))
		}
		// The client appends /watch, /reprocess and /health to the base URL
		// as a string, so a query or fragment would swallow the path.
		if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
			return fmt.Errorf("merkle_service url %s must not contain a query or fragment", redactURL(u))
		}
	}
	if cfg.MerkleService.Enabled() && cfg.CallbackToken == "" {
		return fmt.Errorf("callback_token is required when merkle_service.url/urls is set " +
			"(unauthenticated /api/v1/merkle-service/callback would accept forged callbacks; see issue #76)")
	}
	return nil
}

// redactURL renders a configured URL for an error message with everything
// that may be a secret replaced: user info, an opaque part (a scheme-less
// "user:pass@host:port" parses with its first segment as the scheme and the
// rest opaque), the query and the fragment. A URL that does not parse is not
// echoed at all.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "(unparseable)"
	}
	const redacted = "REDACTED"
	c := *u
	if c.User != nil {
		c.User = url.User(redacted)
	}
	if c.Opaque != "" {
		c.Opaque = redacted
	}
	if c.RawQuery != "" {
		c.RawQuery = redacted
	}
	if c.Fragment != "" {
		c.Fragment, c.RawFragment = redacted, ""
	}
	return strconv.Quote(c.String())
}

// validTopicPrefix is the Kafka topic-name alphabet.
var validTopicPrefix = regexp.MustCompile(`^[a-zA-Z0-9._-]*$`)

func validate(cfg *Config) error {
	switch cfg.Kafka.Backend {
	case "", "sarama":
		if len(cfg.Kafka.Brokers) == 0 {
			return fmt.Errorf("kafka.brokers is required when kafka.backend=sarama")
		}
	case "memory":
		// no external config required
	default:
		return fmt.Errorf("unknown kafka.backend %q (expected sarama or memory)", cfg.Kafka.Backend)
	}
	if !validTopicPrefix.MatchString(cfg.Kafka.TopicPrefix) {
		return fmt.Errorf("kafka.topic_prefix %q must contain only letters, digits, '.', '_' or '-'", cfg.Kafka.TopicPrefix)
	}
	switch cfg.Store.Backend {
	case "", "aerospike":
		if len(cfg.Store.Aerospike.Hosts) == 0 {
			return fmt.Errorf("store.aerospike.hosts is required when store.backend=aerospike")
		}
	case "pebble":
		if cfg.Store.Pebble.Path == "" {
			return fmt.Errorf("store.pebble.path is required when store.backend=pebble")
		}
	case "postgres":
		if !cfg.Store.Postgres.Embedded && cfg.Store.Postgres.DSN == "" {
			return fmt.Errorf("store.postgres.dsn is required when store.backend=postgres and postgres.embedded=false")
		}
		if cfg.Store.Postgres.SchemaApplyTimeoutMs < 0 {
			return fmt.Errorf("store.postgres.schema_apply_timeout_ms must be >= 0 (0 = default)")
		}
	case "mongodb":
		if cfg.Store.Mongo.URI == "" {
			return fmt.Errorf("store.mongodb.uri is required when store.backend=mongodb")
		}
		if cfg.Store.Mongo.Database == "" {
			return fmt.Errorf("store.mongodb.database is required when store.backend=mongodb")
		}
		for name, v := range map[string]int{
			"connect_timeout_ms": cfg.Store.Mongo.ConnectTimeoutMs,
			"op_timeout_ms":      cfg.Store.Mongo.OpTimeoutMs,
			"query_timeout_ms":   cfg.Store.Mongo.QueryTimeoutMs,
			"index_timeout_ms":   cfg.Store.Mongo.IndexTimeoutMs,
			"max_pool_size":      cfg.Store.Mongo.MaxPoolSize,
			"min_pool_size":      cfg.Store.Mongo.MinPoolSize,
			"max_conn_idle_ms":   cfg.Store.Mongo.MaxConnIdleMs,
		} {
			if v < 0 {
				return fmt.Errorf("store.mongodb.%s must be >= 0 (0 = default)", name)
			}
		}
		// A single UpdateMany carries one $in of batch_size txids, and unlike
		// BulkWrite it is not split by the driver: past ~200k ids it exceeds
		// the 16 MB command limit. 10k is far below that and far above any
		// useful chunk.
		if b := cfg.Store.Mongo.BatchSize; b < 0 || b > 10000 {
			return fmt.Errorf("store.mongodb.batch_size must be between 0 (default) and 10000, got %d", b)
		}
	default:
		return fmt.Errorf("unknown store.backend %q (expected aerospike, pebble, postgres, or mongodb)", cfg.Store.Backend)
	}
	// merkle_service.url / urls are intentionally optional: no endpoints means
	// the Merkle integration is disabled. The runtime treats
	// MerkleServiceConfig.Enabled() as the toggle — app.Bootstrap only
	// constructs a merkleservice.Pool when at least one endpoint is set, and
	// propagation.Propagator nil-guards every dereference of the client. The
	// documented standalone and zero-dependency profiles
	// (config.example.standalone.yaml) ship with merkle_service.url: "" for
	// exactly this reason. See issue #59 / finding F-001.
	//
	// When the Merkle integration IS enabled, callback_token is
	// mandatory. The /api/v1/merkle-service/callback endpoint accepts forged
	// status updates for any txid in the system if it runs without bearer-token
	// auth, so we fail-closed here at config load rather than silently exposing
	// the unauthenticated receiver. See issue #76 / finding F-018.
	//
	// This same check now also gates the OUTBOUND /watch token forwarding:
	// merkleservice.Client.Register/RegisterBatch propagate cfg.CallbackToken
	// to merkle-service so it can attach `Authorization: Bearer <token>` on
	// callbacks. Without a configured token there's nothing to forward AND the
	// inbound receiver would 401 anyway — the same fail-closed posture covers
	// both ends, so a duplicate "outbound token required" check is unnecessary.
	if err := validateMerkleService(cfg); err != nil {
		return err
	}
	if cfg.Network == "" {
		cfg.Network = NetworkMainnet
	}
	if _, ok := knownNetworks[cfg.Network]; !ok {
		return fmt.Errorf("invalid network %q (expected %s, %s, %s, %s, or %s)",
			cfg.Network, NetworkMainnet, NetworkTestnet, NetworkTeratestnet, NetworkRegtest, NetworkTstn)
	}
	if cfg.Network == NetworkRegtest || cfg.Network == NetworkTstn {
		if cfg.P2P.DatahubDiscovery && len(cfg.P2P.BootstrapPeers) == 0 {
			return fmt.Errorf("p2p.bootstrap_peers is required when network=%s and p2p.datahub_discovery=true", cfg.Network)
		}
	}
	validModes := map[string]bool{
		"all": true, "api-server": true,
		"bump-builder": true,
		"propagation":  true,
		"p2p-client":   true,
		"chaintracks":  true,
		"sse":          true,
		"watchdog":     true,
	}
	if !validModes[cfg.Mode] {
		return fmt.Errorf("invalid mode %q", cfg.Mode)
	}
	if cfg.Watchdog.Enabled {
		if cfg.Watchdog.StaleThresholdMs <= 0 {
			return fmt.Errorf("watchdog.stale_threshold_ms must be > 0 when watchdog.enabled")
		}
		if cfg.Watchdog.RecencyDepth <= 0 {
			return fmt.Errorf("watchdog.recency_depth must be > 0 when watchdog.enabled")
		}
	}
	if cfg.Telemetry.Enabled {
		switch cfg.Telemetry.Protocol {
		case "grpc", "http":
		default:
			return fmt.Errorf("invalid telemetry.protocol %q (expected grpc or http)", cfg.Telemetry.Protocol)
		}
		// Both the gRPC and HTTP exporters' WithEndpoint dial the value
		// verbatim as host:port; a scheme prefix makes the dial fail silently
		// in the background exporter goroutine (for HTTP the scheme is instead
		// controlled by telemetry.insecure), so fail fast here for either
		// protocol. Only the OTEL_EXPORTER_OTLP_ENDPOINT env var understands a
		// scheme-prefixed URL.
		if strings.HasPrefix(cfg.Telemetry.Endpoint, "http://") || strings.HasPrefix(cfg.Telemetry.Endpoint, "https://") {
			return fmt.Errorf("telemetry.endpoint %q must be host:port without a scheme "+
				"(e.g. \"collector:4317\"); scheme-prefixed URLs are only understood by the OTEL_EXPORTER_OTLP_ENDPOINT env var",
				cfg.Telemetry.Endpoint)
		}
		// telemetry.endpoint is intentionally not required here: an empty
		// value falls back to the standard OTEL_EXPORTER_OTLP_ENDPOINT /
		// OTEL_EXPORTER_OTLP_{TRACES,METRICS}_ENDPOINT env vars, resolved
		// inside telemetry.Init (which errors if neither is set).
		if cfg.Telemetry.SampleRatio < 0 || cfg.Telemetry.SampleRatio > 1 {
			return fmt.Errorf("telemetry.sample_ratio must be between 0 and 1, got %v", cfg.Telemetry.SampleRatio)
		}
	}
	return validateAccessControl(cfg)
}

// validateAdminPortCollisions rejects an admin.port that collides with
// api.port (always, since the admin listener lives in services/api_server —
// see the plan's deviation 1) or, under mode=all (the default, so "" counts
// too), with any other in-process HTTP listener. Under mode=all every
// listener shares one binary, so a shared port means one of them fails to
// bind at startup.
func validateAdminPortCollisions(cfg *Config) error {
	modeAll := cfg.Mode == "" || cfg.Mode == "all"
	collisions := []struct {
		key     string
		port    int
		applies bool
	}{
		{"api.port", cfg.APIServer.Port, true},
		{"health.port", cfg.Health.Port, modeAll},
		{"sse.port", cfg.SSE.Port, modeAll && cfg.SSE.Enabled},
		{"chaintracks_server.port", cfg.ChaintracksServer.Port, modeAll && cfg.ChaintracksServer.Enabled},
	}
	for _, c := range collisions {
		if c.applies && cfg.Admin.Port == c.port {
			return fmt.Errorf("admin.port must differ from %s (%d): the admin listener must never share a port with another listener", c.key, c.port)
		}
	}
	return nil
}

// validateAuthStore checks auth.store.mongodb. Unset (no uri, no database)
// is valid and leaves keys and quota in the main store. Errors never echo
// the uri: it may carry credentials.
func validateAuthStore(cfg *Config) error {
	m := cfg.Auth.Store.Mongo
	if !cfg.Auth.Store.Configured() {
		if m.Database != "" {
			return fmt.Errorf("auth.store.mongodb.database is set but auth.store.mongodb.uri is empty")
		}
		return nil
	}
	if !cfg.Auth.Enabled {
		return fmt.Errorf("auth.store.mongodb.uri requires auth.enabled")
	}
	if m.Database == "" {
		return fmt.Errorf("auth.store.mongodb.database is required when auth.store.mongodb.uri is set")
	}
	for _, f := range []struct {
		name string
		v    int
	}{
		{"connect_timeout_ms", m.ConnectTimeoutMs},
		{"op_timeout_ms", m.OpTimeoutMs},
		{"query_timeout_ms", m.QueryTimeoutMs},
		{"index_timeout_ms", m.IndexTimeoutMs},
		{"max_pool_size", m.MaxPoolSize},
		{"min_pool_size", m.MinPoolSize},
		{"max_conn_idle_ms", m.MaxConnIdleMs},
	} {
		if f.v < 0 {
			return fmt.Errorf("auth.store.mongodb.%s must be >= 0 (0 = default)", f.name)
		}
	}
	return nil
}

// validateAccessControl checks the API-key, admin and rate-limit settings.
func validateAccessControl(cfg *Config) error {
	switch cfg.APIServer.TrustedPlatform {
	case "", TrustedPlatformCloudflare:
	default:
		return fmt.Errorf("api.trusted_platform must be \"\" or %q, got %q", TrustedPlatformCloudflare, cfg.APIServer.TrustedPlatform)
	}
	for _, p := range cfg.APIServer.TrustedProxies {
		if _, _, err := net.ParseCIDR(p); err != nil && net.ParseIP(p) == nil {
			return fmt.Errorf("api.trusted_proxies: %q is not a valid IP or CIDR", p)
		}
	}
	if cfg.Admin.Enabled {
		if cfg.Admin.Token == "" {
			return fmt.Errorf("admin.token is required when admin.enabled")
		}
		if cfg.Admin.Port <= 0 {
			return fmt.Errorf("admin.port must be > 0")
		}
		if err := validateAdminPortCollisions(cfg); err != nil {
			return err
		}
	}
	if err := validateAuthStore(cfg); err != nil {
		return err
	}
	rl := cfg.RateLimit
	if cfg.Auth.Enabled && (rl.KeyCacheTTLMs <= 0 || rl.KeyCacheStaleMaxMs < rl.KeyCacheTTLMs) {
		return fmt.Errorf("ratelimit.key_cache_ttl_ms (%d) must be > 0 and <= ratelimit.key_cache_stale_max_ms (%d)",
			rl.KeyCacheTTLMs, rl.KeyCacheStaleMaxMs)
	}
	if !rl.Enabled {
		return nil
	}
	if !cfg.Auth.Enabled {
		return fmt.Errorf("ratelimit.enabled requires auth.enabled")
	}
	if rl.LeaseFraction <= 0 || rl.LeaseFraction > 1 {
		return fmt.Errorf("ratelimit.lease_fraction must be in (0, 1], got %v", rl.LeaseFraction)
	}
	if rl.ShrinkThreshold <= 0 || rl.ShrinkThreshold > 1 {
		return fmt.Errorf("ratelimit.shrink_threshold must be in (0, 1], got %v", rl.ShrinkThreshold)
	}
	if rl.ExpectedReplicas < 1 {
		return fmt.Errorf("ratelimit.expected_replicas must be >= 1")
	}
	if rl.Costs.SubmitTx <= 0 || rl.Costs.Read <= 0 {
		return fmt.Errorf("ratelimit.costs.submit_tx and ratelimit.costs.read must be > 0")
	}
	if _, ok := rl.Tiers[TierFree]; !ok {
		return fmt.Errorf("ratelimit.tiers.free is required: it is the fallback for keys whose tier is not configured")
	}
	if _, ok := rl.Tiers[TierAnonymous]; ok {
		return fmt.Errorf("ratelimit.tiers.anonymous is reserved; configure unkeyed callers under ratelimit.anonymous")
	}
	check := func(name string, t TierConfig) error {
		if t.UnitsPerMinute < 0 || t.MaxInFlight < 0 || t.MaxBatch < 0 {
			return fmt.Errorf("ratelimit tier %q: units_per_minute, max_in_flight and max_batch must be >= 0", name)
		}
		return checkTierPayable(name, t, rl.Costs)
	}
	if err := check(TierAnonymous, rl.Anonymous); err != nil {
		return err
	}
	for name, t := range rl.Tiers {
		if !tierNameRe.MatchString(name) {
			return fmt.Errorf("ratelimit tier name %q is invalid: must match %s", name, tierNameRe.String())
		}
		if err := check(name, t); err != nil {
			return err
		}
	}
	return nil
}

// tierNameRe bounds configured tier names: they're embedded verbatim in the
// RateLimit-Policy/RateLimit sf-string headers and used as a metric label
// value, so anything outside this set risks breaking header syntax or
// exploding label cardinality.
var tierNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// checkTierPayable rejects a bounded tier whose configured batch (or even a
// single /tx, or a single read) could never fit in its own per-minute budget — a batch that
// would always 429 is a config bug, not a runtime condition. Unbounded
// tiers (units_per_minute == 0) have no budget to check against.
func checkTierPayable(name string, t TierConfig, costs RateLimitCosts) error {
	if t.UnitsPerMinute <= 0 {
		return nil
	}
	if costs.SubmitTx > t.UnitsPerMinute {
		return fmt.Errorf("ratelimit tier %q: costs.submit_tx (%d) exceeds units_per_minute (%d): a single /tx could never succeed",
			name, costs.SubmitTx, t.UnitsPerMinute)
	}
	// Reads (and /txs's base charge) cost costs.read whatever max_batch is.
	if costs.Read > t.UnitsPerMinute {
		return fmt.Errorf("ratelimit tier %q: costs.read (%d) exceeds units_per_minute (%d): a single read could never succeed",
			name, costs.Read, t.UnitsPerMinute)
	}
	// By division, not max_batch*submit_tx, which a huge max_batch could
	// overflow into passing. Both costs are > 0 (checked by the caller).
	if t.MaxBatch > 0 && int64(t.MaxBatch) > (t.UnitsPerMinute-costs.Read)/costs.SubmitTx {
		return fmt.Errorf("ratelimit tier %q: max_batch (%d) at costs.submit_tx (%d) plus costs.read (%d) exceeds units_per_minute (%d): the largest allowed batch could never succeed",
			name, t.MaxBatch, costs.SubmitTx, costs.Read, t.UnitsPerMinute)
	}
	return nil
}
