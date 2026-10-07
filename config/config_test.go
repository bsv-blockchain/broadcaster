package config

import (
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// baseValidConfig returns a Config populated with the minimum fields other
// validate() branches require so each test can focus on the network branch.
//
// Sets a non-empty CallbackToken because validate() now refuses to start
// when MerkleService.URL is set without a bearer token (issue #76 / F-018).
// Tests that need to exercise that branch override CallbackToken explicitly.
func baseValidConfig() *Config {
	cfg := &Config{}
	cfg.Mode = "all"
	cfg.Kafka.Backend = "memory"
	cfg.Store.Backend = "pebble"
	cfg.Store.Pebble.Path = "/tmp/bsv-broadcaster-test"
	cfg.Network = NetworkMainnet
	cfg.MerkleService.URL = "http://merkle.local"
	cfg.CallbackToken = "test-callback-token"
	return cfg
}

// merkle_service.url is optional: URL-presence is the runtime toggle for the
// Merkle integration. cmd/bsv-broadcaster/main.go only constructs a merkleservice.Client
// when the URL is set, and propagation nil-guards every dereference. The
// documented standalone profile ships with merkle_service.url: "" — validation
// must accept that, otherwise the standalone binary refuses to start (issue #59
// / finding F-001).
func TestValidate_AllowsEmptyMerkleServiceURL(t *testing.T) {
	cfg := baseValidConfig()
	cfg.MerkleService.URL = ""
	if err := validate(cfg); err != nil {
		t.Fatalf("empty merkle_service.url should be accepted, got: %v", err)
	}
}

// A populated merkle_service.url is the production path and must continue to
// validate cleanly so the "set the URL to enable Merkle" toggle stays symmetric
// with the empty-URL case above.
func TestValidate_AcceptsPopulatedMerkleServiceURL(t *testing.T) {
	cfg := baseValidConfig()
	cfg.MerkleService.URL = "http://merkle.local"
	if err := validate(cfg); err != nil {
		t.Fatalf("populated merkle_service.url should be accepted, got: %v", err)
	}
}

// Issue #76 / finding F-018: when the Merkle integration is wired up, the
// inbound /api/v1/merkle-service/callback endpoint MUST be authenticated. We
// fail fast at config load when MerkleService.URL is set without a
// callback_token rather than silently exposing an unauthenticated receiver
// that any unauthenticated caller could use to submit forged status updates.
func TestValidate_RequiresCallbackTokenWhenMerkleEnabled(t *testing.T) {
	cfg := baseValidConfig()
	cfg.MerkleService.URL = "http://merkle.local"
	cfg.CallbackToken = ""
	err := validate(cfg)
	if err == nil {
		t.Fatal("expected error when merkle_service.url is set without callback_token")
	}
	if !strings.Contains(err.Error(), "callback_token") {
		t.Errorf("error should mention callback_token, got: %v", err)
	}
}

// Standalone profiles ship with merkle_service.url: "" and frequently leave
// callback_token empty too — there is no Merkle Service issuing callbacks, so
// no token is required. The validation must pass cleanly so the standalone
// binary keeps booting (issue #59 fix from #104 must continue to hold).
func TestValidate_AllowsEmptyCallbackTokenInStandaloneMode(t *testing.T) {
	cfg := baseValidConfig()
	cfg.MerkleService.URL = ""
	cfg.CallbackToken = ""
	if err := validate(cfg); err != nil {
		t.Fatalf("standalone (no merkle, no callback_token) should validate, got: %v", err)
	}
}

// Each canonical network name must validate cleanly. The empty string is also
// accepted — validate() normalizes it to mainnet so CLI users can omit the key.
// Regtest validates without bootstrap_peers here because baseValidConfig leaves
// p2p.datahub_discovery=false; the bootstrap-peers requirement only triggers
// when discovery is on (covered by TestValidate_RegtestRequiresBootstrapPeers).
func TestValidate_AcceptsCanonicalNetworks(t *testing.T) {
	for _, net := range []string{"", NetworkMainnet, NetworkTestnet, NetworkTeratestnet, NetworkRegtest, NetworkTstn} {
		cfg := baseValidConfig()
		cfg.Network = net
		if err := validate(cfg); err != nil {
			t.Errorf("network=%q unexpected error: %v", net, err)
		}
	}
}

// Anything outside the canonical set is rejected — operators who need a
// private network should override p2p.bootstrap_peers on top of a canonical
// choice, not invent a new name.
func TestValidate_RejectsUnknownNetwork(t *testing.T) {
	for _, net := range []string{"main", "stn", "ttn", "bogus"} {
		cfg := baseValidConfig()
		cfg.Network = net
		err := validate(cfg)
		if err == nil {
			t.Fatalf("network=%q should be rejected", net)
		}
		if !strings.Contains(err.Error(), "invalid network") {
			t.Errorf("error should mention invalid network, got: %v", err)
		}
	}
}

// ResolveP2PNetwork is the bridge between BSV Broadcaster's canonical names and the
// values go-teranode-p2p-client actually accepts. Regressing any of these
// pairings silently breaks bootstrap or topic subscription.
func TestResolveP2PNetwork(t *testing.T) {
	cases := []struct {
		network         string
		wantTopic       string
		wantBootstrapIn string // empty => expect no default peers
	}{
		{NetworkMainnet, NetworkMainnet, "mainnet.bootstrap.teranode.bsvb.tech"},
		{NetworkTestnet, NetworkTestnet, "testnet.bootstrap.teranode.bsvb.tech"},
		{NetworkTeratestnet, NetworkTeratestnet, "teratestnet.bootstrap.teranode.bsvb.tech"},
		{"", NetworkMainnet, "mainnet.bootstrap.teranode.bsvb.tech"},
		{NetworkRegtest, NetworkRegtest, ""},
		{NetworkTstn, NetworkTstn, ""},
	}
	for _, tc := range cases {
		t.Run(tc.network, func(t *testing.T) {
			topic, boots := ResolveP2PNetwork(tc.network)
			if topic != tc.wantTopic {
				t.Errorf("topic: got %q, want %q", topic, tc.wantTopic)
			}
			if tc.wantBootstrapIn == "" {
				if len(boots) != 0 {
					t.Errorf("expected no default bootstrap peers for %q, got %v", tc.network, boots)
				}
				return
			}
			if len(boots) == 0 {
				t.Fatalf("expected at least one bootstrap peer for %q", tc.network)
			}
			if !strings.Contains(boots[0], tc.wantBootstrapIn) {
				t.Errorf("bootstrap: got %q, want substring %q", boots[0], tc.wantBootstrapIn)
			}
		})
	}
}

// Regtest has no canonical bootstrap DNS, so validate() requires the operator
// to supply p2p.bootstrap_peers when datahub_discovery is on. Catching this at
// config load is much friendlier than a libp2p host that boots into a vacuum.
// Tstn has no bootstrap DNS either, so the same requirement applies to it.
func TestValidate_RegtestRequiresBootstrapPeers(t *testing.T) {
	for _, net := range []string{NetworkRegtest, NetworkTstn} {
		t.Run(net+" missing peers errors", func(t *testing.T) {
			cfg := baseValidConfig()
			cfg.Network = net
			cfg.P2P.DatahubDiscovery = true
			err := validate(cfg)
			if err == nil {
				t.Fatalf("expected error when %s+discovery has no bootstrap_peers", net)
			}
			if !strings.Contains(err.Error(), "bootstrap_peers") {
				t.Errorf("error should mention bootstrap_peers, got: %v", err)
			}
		})
		t.Run(net+" peers populated passes", func(t *testing.T) {
			cfg := baseValidConfig()
			cfg.Network = net
			cfg.P2P.DatahubDiscovery = true
			cfg.P2P.BootstrapPeers = []string{"/ip4/127.0.0.1/tcp/9905/p2p/12D3KooWxyz"}
			if err := validate(cfg); err != nil {
				t.Errorf("unexpected error with bootstrap_peers set: %v", err)
			}
		})
	}
}

// go-chaintracks now sources the regtest genesis header from go-chaincfg, so
// validate() no longer force-disables chaintracks_server for regtest. The
// operator's chaintracks_server.enabled choice must be respected.
func TestValidate_RegtestPreservesChaintracksServerEnabled(t *testing.T) {
	cfg := baseValidConfig()
	cfg.Network = NetworkRegtest
	cfg.ChaintracksServer.Enabled = true
	if err := validate(cfg); err != nil {
		t.Fatalf("unexpected validate error: %v", err)
	}
	if !cfg.ChaintracksServer.Enabled {
		t.Error("expected chaintracks_server.enabled to remain true for regtest")
	}
}

// ResolveChaintracksNetwork translates to go-chaintracks's stricter accepted
// set. Regressing this is the bug that crashed prod with "unknown network:
// mainnet" — chainmanager.getGenesisHeader is an exact-match switch over
// "main"/"test"/"teratest"/"teratestnet"/"regtest".
func TestResolveChaintracksNetwork(t *testing.T) {
	cases := []struct {
		network string
		want    string
	}{
		{NetworkMainnet, "main"},
		{NetworkTestnet, "test"},
		{NetworkTeratestnet, NetworkTeratestnet},
		{NetworkRegtest, NetworkRegtest},
		{NetworkTstn, NetworkTstn},
		{"", "main"},
	}
	for _, tc := range cases {
		t.Run(tc.network, func(t *testing.T) {
			got := ResolveChaintracksNetwork(tc.network)
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDefaultCallbackMaxBodyBytes pins the default body cap for the inbound
// callback receiver. Set to 16 MiB — comfortably over a realistic STUMP
// delivery while bounding worst-case memory if a peer is malicious or
// malfunctioning. Mitigates F-019 / issue #77; bumping this value should be
// a deliberate, reviewed change.
func TestDefaultCallbackMaxBodyBytes(t *testing.T) {
	const want int64 = 16 * 1024 * 1024
	if DefaultCallbackMaxBodyBytes != want {
		t.Errorf("DefaultCallbackMaxBodyBytes = %d, want %d", DefaultCallbackMaxBodyBytes, want)
	}
}

// telemetryValidConfig extends baseValidConfig with an enabled, valid
// telemetry block so each telemetry-validation test can break exactly one
// rule at a time.
func telemetryValidConfig() *Config {
	cfg := baseValidConfig()
	cfg.Telemetry.Enabled = true
	cfg.Telemetry.Protocol = "grpc"
	cfg.Telemetry.SampleRatio = 1.0
	return cfg
}

// Telemetry validation only applies when telemetry.enabled=true: a disabled
// block may carry garbage values (typical of half-filled config templates)
// without blocking startup, because none of it is ever read.
func TestValidate_TelemetryDisabledSkipsChecks(t *testing.T) {
	cfg := baseValidConfig()
	cfg.Telemetry.Enabled = false
	cfg.Telemetry.Protocol = "carrier-pigeon"
	cfg.Telemetry.SampleRatio = 42
	cfg.Telemetry.Endpoint = "http://collector:4317"
	if err := validate(cfg); err != nil {
		t.Fatalf("disabled telemetry block must not be validated, got: %v", err)
	}
}

func TestValidate_TelemetryAcceptsValidConfig(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		t.Run(protocol, func(t *testing.T) {
			cfg := telemetryValidConfig()
			cfg.Telemetry.Protocol = protocol
			// Endpoint intentionally empty: the env fallback is resolved in
			// telemetry.Init, not validate().
			if err := validate(cfg); err != nil {
				t.Fatalf("valid telemetry config rejected: %v", err)
			}
		})
	}
}

func TestValidate_TelemetryRejectsBadProtocol(t *testing.T) {
	cfg := telemetryValidConfig()
	cfg.Telemetry.Protocol = "carrier-pigeon"
	err := validate(cfg)
	if err == nil {
		t.Fatal("expected error for telemetry.protocol=carrier-pigeon when enabled")
	}
	if !strings.Contains(err.Error(), "telemetry.protocol") {
		t.Errorf("error should name telemetry.protocol, got: %v", err)
	}
}

func TestValidate_TelemetryRejectsOutOfRangeSampleRatio(t *testing.T) {
	for _, ratio := range []float64{-0.1, 1.5} {
		cfg := telemetryValidConfig()
		cfg.Telemetry.SampleRatio = ratio
		err := validate(cfg)
		if err == nil {
			t.Fatalf("expected error for telemetry.sample_ratio=%v when enabled", ratio)
		}
		if !strings.Contains(err.Error(), "telemetry.sample_ratio") {
			t.Errorf("error should name telemetry.sample_ratio, got: %v", err)
		}
	}
}

// Both the gRPC and HTTP OTLP exporters' WithEndpoint dial telemetry.endpoint
// verbatim as host:port; a scheme prefix fails silently inside the
// background exporter goroutine for either protocol (for HTTP the scheme is
// instead controlled by telemetry.insecure), so validate() rejects a
// scheme-prefixed endpoint regardless of telemetry.protocol.
func TestValidate_TelemetryRejectsSchemedEndpoint(t *testing.T) {
	for _, protocol := range []string{"grpc", "http"} {
		for _, endpoint := range []string{"http://collector:4317", "https://collector:4317"} {
			t.Run(protocol+"/"+endpoint, func(t *testing.T) {
				cfg := telemetryValidConfig()
				cfg.Telemetry.Protocol = protocol
				cfg.Telemetry.Endpoint = endpoint
				err := validate(cfg)
				if err == nil {
					t.Fatalf("expected error for scheme-prefixed endpoint %q with protocol=%s", endpoint, protocol)
				}
				if !strings.Contains(err.Error(), "telemetry.endpoint") {
					t.Errorf("error should name telemetry.endpoint, got: %v", err)
				}
			})
		}

		cfg := telemetryValidConfig()
		cfg.Telemetry.Protocol = protocol
		cfg.Telemetry.Endpoint = "collector:4317"
		if err := validate(cfg); err != nil {
			t.Fatalf("plain host:port endpoint rejected for protocol=%s: %v", protocol, err)
		}
	}
}

// Issue #278: the schema-apply slow path (fresh DB or schema change) runs
// under store.postgres.schema_apply_timeout_ms. Zero means "use the built-in
// default" so an unset YAML block can't foot-gun boot; only negatives are
// config errors.
func TestValidate_RejectsNegativeSchemaApplyTimeout(t *testing.T) {
	cfg := baseValidConfig()
	cfg.Store.Backend = "postgres"
	cfg.Store.Postgres.Embedded = true
	cfg.Store.Postgres.SchemaApplyTimeoutMs = -1
	err := validate(cfg)
	if err == nil {
		t.Fatal("negative schema_apply_timeout_ms should be rejected")
	}
	if !strings.Contains(err.Error(), "schema_apply_timeout_ms") {
		t.Errorf("error should mention schema_apply_timeout_ms, got: %v", err)
	}
}

func TestValidate_AllowsZeroSchemaApplyTimeout(t *testing.T) {
	cfg := baseValidConfig()
	cfg.Store.Backend = "postgres"
	cfg.Store.Postgres.Embedded = true
	cfg.Store.Postgres.SchemaApplyTimeoutMs = 0
	if err := validate(cfg); err != nil {
		t.Fatalf("zero schema_apply_timeout_ms (= use default) should be accepted, got: %v", err)
	}
}

// TestSSEDrainKnobsBind pins the viper wiring for the SSE drain windows.
//
// The failure mode this guards is silent: a typo in a mapstructure tag or a
// SetDefault key leaves the field at its zero value, the service falls back to
// its compiled-in default, and nothing anywhere reports that the operator's
// setting was ignored. These particular knobs have to stay coordinated with
// the pod's terminationGracePeriodSeconds, so an ignored override is a drain
// that gets SIGKILLed halfway through.
func TestSSEDrainKnobsBind(t *testing.T) {
	cfg, err := Load(&cobra.Command{})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	for _, tc := range []struct {
		name string
		got  int
		want int
	}{
		{"drain_quiesce_ms", cfg.SSE.DrainQuiesceMS, DefaultSSEDrainQuiesceMS},
		{"drain_retry_max_ms", cfg.SSE.DrainRetryMaxMS, DefaultSSEDrainRetryMaxMS},
		{"shutdown_timeout_ms", cfg.SSE.ShutdownTimeoutMS, DefaultSSEShutdownTimeoutMS},
	} {
		if tc.got != tc.want {
			t.Errorf("sse.%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}

	t.Setenv("BSV_BROADCASTER_SSE_DRAIN_QUIESCE_MS", "2500")
	overridden, err := Load(&cobra.Command{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if overridden.SSE.DrainQuiesceMS != 2500 {
		t.Errorf("BSV_BROADCASTER_SSE_DRAIN_QUIESCE_MS override = %d, want 2500", overridden.SSE.DrainQuiesceMS)
	}
}

// TestPropagationBatchCapsBind pins the shipped /txs chunk caps (issue #271)
// and proves both keys have a SetDefault, without which the BSV_BROADCASTER_* env
// override is silently ignored by viper.AutomaticEnv.
func TestPropagationBatchCapsBind(t *testing.T) {
	if DefaultTeranodeMaxBatchSize != 1000 {
		t.Errorf("DefaultTeranodeMaxBatchSize = %d, want 1000 (Teranode rejects a chunk of >= 1024 txs)", DefaultTeranodeMaxBatchSize)
	}
	if DefaultTeranodeMaxBatchBytes != 16*1024*1024 {
		t.Errorf("DefaultTeranodeMaxBatchBytes = %d, want 16 MiB (half of Teranode's 32 MiB maxDataPerRequest)", DefaultTeranodeMaxBatchBytes)
	}

	cfg, err := Load(&cobra.Command{})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Propagation.TeranodeMaxBatchSize != DefaultTeranodeMaxBatchSize {
		t.Errorf("propagation.teranode_max_batch_size = %d, want %d", cfg.Propagation.TeranodeMaxBatchSize, DefaultTeranodeMaxBatchSize)
	}
	if cfg.Propagation.TeranodeMaxBatchBytes != DefaultTeranodeMaxBatchBytes {
		t.Errorf("propagation.teranode_max_batch_bytes = %d, want %d", cfg.Propagation.TeranodeMaxBatchBytes, DefaultTeranodeMaxBatchBytes)
	}

	t.Setenv("BSV_BROADCASTER_PROPAGATION_TERANODE_MAX_BATCH_SIZE", "250")
	t.Setenv("BSV_BROADCASTER_PROPAGATION_TERANODE_MAX_BATCH_BYTES", "4194304")
	overridden, err := Load(&cobra.Command{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if overridden.Propagation.TeranodeMaxBatchSize != 250 {
		t.Errorf("BSV_BROADCASTER_PROPAGATION_TERANODE_MAX_BATCH_SIZE override = %d, want 250", overridden.Propagation.TeranodeMaxBatchSize)
	}
	if overridden.Propagation.TeranodeMaxBatchBytes != 4194304 {
		t.Errorf("BSV_BROADCASTER_PROPAGATION_TERANODE_MAX_BATCH_BYTES override = %d, want 4194304", overridden.Propagation.TeranodeMaxBatchBytes)
	}
}

// The mongodb backend needs both a connection URI and a database name; the
// other fields have defaults. The default: error string enumerates every
// accepted backend, so a typo'd name tells the operator what is valid.
func TestValidate_MongoDBBackend(t *testing.T) {
	cfg := baseValidConfig()
	cfg.Store.Backend = "mongodb"
	cfg.Store.Mongo.URI = "mongodb://localhost:27017"
	cfg.Store.Mongo.Database = "bsv_broadcaster"
	if err := validate(cfg); err != nil {
		t.Fatalf("mongodb backend with uri+database should be accepted, got: %v", err)
	}

	cfg.Store.Mongo.URI = ""
	err := validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "store.mongodb.uri") {
		t.Fatalf("missing uri should be rejected naming store.mongodb.uri, got: %v", err)
	}

	cfg.Store.Mongo.URI = "mongodb://localhost:27017"
	cfg.Store.Mongo.Database = ""
	err = validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "store.mongodb.database") {
		t.Fatalf("missing database should be rejected naming store.mongodb.database, got: %v", err)
	}

	cfg.Store.Backend = "mongo"
	err = validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "mongodb") {
		t.Fatalf("unknown backend error should list mongodb as an accepted value, got: %v", err)
	}
}

// Issue #90: Aerospike batch loops advance by batch_size. Zero (or negative)
// never moves the index, so mined-status / BUMP / STUMP cleanup hangs. The
// viper default is DefaultAerospikeBatchSize; an explicit non-positive value
// must be rejected.
func TestValidate_AerospikeBatchSizeMustBePositive(t *testing.T) {
	cfg := baseValidConfig()
	cfg.Store.Backend = "aerospike"
	cfg.Store.Aerospike.Hosts = []string{"localhost:3000"}
	cfg.Store.Aerospike.BatchSize = DefaultAerospikeBatchSize
	if err := validate(cfg); err != nil {
		t.Fatalf("positive store.aerospike.batch_size should be accepted: %v", err)
	}
	for _, n := range []int{0, -1} {
		cfg.Store.Aerospike.BatchSize = n
		err := validate(cfg)
		if err == nil || !strings.Contains(err.Error(), "store.aerospike.batch_size") {
			t.Fatalf("batch_size %d must be rejected naming store.aerospike.batch_size, got: %v", n, err)
		}
	}

	viper.Reset()
	t.Cleanup(viper.Reset)
	setDefaults()
	if got := viper.GetInt("store.aerospike.batch_size"); got != DefaultAerospikeBatchSize {
		t.Fatalf("store.aerospike.batch_size default = %d, want DefaultAerospikeBatchSize %d", got, DefaultAerospikeBatchSize)
	}
}

// Every store.mongodb.* key must have a SetDefault so BSV_BROADCASTER_STORE_MONGODB_*
// env overrides are honored by viper's AutomaticEnv (see setDefaults).
func TestSetDefaults_MongoDBKeysBound(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	setDefaults()
	for _, key := range []string{
		"store.mongodb.uri", "store.mongodb.database", "store.mongodb.connect_timeout_ms",
		"store.mongodb.op_timeout_ms", "store.mongodb.query_timeout_ms",
		"store.mongodb.index_timeout_ms",
		"store.mongodb.max_pool_size", "store.mongodb.min_pool_size",
		"store.mongodb.max_conn_idle_ms", "store.mongodb.batch_size",
	} {
		if !viper.IsSet(key) {
			t.Errorf("%s has no default; BSV_BROADCASTER_%s would be ignored", key, strings.ToUpper(strings.ReplaceAll(key, ".", "_")))
		}
	}
}

// The mongodb block rejects values that would otherwise be substituted
// silently or overflow a server limit: negative timeouts became compiled
// defaults with no error, and a batch_size past ~200k puts one UpdateMany $in
// over the 16 MB command limit. index_timeout_ms exists so createIndexes on a
// restored, populated collection cannot sit past the liveness probe.
func TestValidate_MongoDBBounds(t *testing.T) {
	base := func() *Config {
		cfg := baseValidConfig()
		cfg.Store.Backend = "mongodb"
		cfg.Store.Mongo.URI = "mongodb://localhost:27017"
		cfg.Store.Mongo.Database = "bsv_broadcaster"
		return cfg
	}
	if err := validate(base()); err != nil {
		t.Fatalf("baseline must validate: %v", err)
	}
	cfg := base()
	cfg.Store.Mongo.OpTimeoutMs = -1
	if err := validate(cfg); err == nil || !strings.Contains(err.Error(), "op_timeout_ms") {
		t.Fatalf("negative op_timeout_ms must be rejected naming the key, got: %v", err)
	}
	cfg = base()
	cfg.Store.Mongo.IndexTimeoutMs = -5
	if err := validate(cfg); err == nil || !strings.Contains(err.Error(), "index_timeout_ms") {
		t.Fatalf("negative index_timeout_ms must be rejected, got: %v", err)
	}
	cfg = base()
	cfg.Store.Mongo.BatchSize = 10001
	if err := validate(cfg); err == nil || !strings.Contains(err.Error(), "batch_size") {
		t.Fatalf("batch_size over 10000 must be rejected, got: %v", err)
	}
	cfg = base()
	cfg.Store.Mongo.BatchSize = 10000
	if err := validate(cfg); err != nil {
		t.Fatalf("batch_size 10000 is the inclusive cap: %v", err)
	}

	viper.Reset()
	t.Cleanup(viper.Reset)
	setDefaults()
	if got := viper.GetInt("store.mongodb.index_timeout_ms"); got != 300000 {
		t.Fatalf("store.mongodb.index_timeout_ms default = %d, want 300000 (matches postgres schema_apply_timeout_ms)", got)
	}
}

// Multi-endpoint merkle_service: `urls` extends `url`, and Endpoints() is
// the single normalized view every consumer uses.
func TestMerkleServiceEndpoints_MergesURLAndURLs(t *testing.T) {
	cfg := MerkleServiceConfig{
		URL:  "http://merkle-a.local/",
		URLs: []string{" http://merkle-b.local ", "http://merkle-a.local", "http://merkle-b.local/"},
	}
	got := cfg.Endpoints()
	want := []string{"http://merkle-a.local", "http://merkle-b.local"}
	if len(got) != len(want) {
		t.Fatalf("Endpoints() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Endpoints()[%d] = %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
	if !cfg.Enabled() {
		t.Fatal("Enabled() must be true with endpoints")
	}
	if (MerkleServiceConfig{}).Enabled() {
		t.Fatal("Enabled() must be false with no endpoints")
	}
}

func TestValidate_MerkleServiceURLsOnlyRequiresCallbackToken(t *testing.T) {
	cfg := baseValidConfig()
	cfg.MerkleService.URL = ""
	cfg.MerkleService.URLs = []string{"http://merkle-a.local", "http://merkle-b.local"}
	if err := validate(cfg); err != nil {
		t.Fatalf("urls-only config should validate, got: %v", err)
	}
	cfg.CallbackToken = ""
	if err := validate(cfg); err == nil {
		t.Fatal("callback_token must be required when merkle_service.urls is set")
	}
}

func TestValidate_MerkleServiceRejectsEmptyAndMalformedEntries(t *testing.T) {
	cfg := baseValidConfig()
	cfg.MerkleService.URLs = []string{"http://merkle-b.local", "  "}
	if err := validate(cfg); err == nil {
		t.Fatal("a blank merkle_service.urls entry must be rejected")
	}

	for _, bad := range []string{
		"merkle.local", "ftp://merkle.local", "http://",
		// The client string-appends /watch, so these would lose the path.
		"http://merkle.local?tenant=a", "http://merkle.local?", "http://merkle.local#frag",
	} {
		cfg := baseValidConfig()
		cfg.MerkleService.URL = bad
		if err := validate(cfg); err == nil {
			t.Fatalf("merkle_service.url %q must be rejected", bad)
		}
	}
}

// BSV_BROADCASTER_MERKLE_SERVICE_URLS is the env form of the list; viper splits it
// on commas the same way it does BSV_BROADCASTER_KAFKA_BROKERS. This only works
// because setDefaults registers the key.
func TestMerkleServiceURLsBindFromEnv(t *testing.T) {
	t.Setenv("BSV_BROADCASTER_MERKLE_SERVICE_URLS", "http://merkle-a.local,http://merkle-b.local/")
	t.Setenv("BSV_BROADCASTER_CALLBACK_TOKEN", "tok")
	cfg, err := Load(&cobra.Command{})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got := cfg.MerkleService.Endpoints()
	if len(got) != 2 || got[0] != "http://merkle-a.local" || got[1] != "http://merkle-b.local" {
		t.Fatalf("Endpoints() from env = %v", got)
	}
}

// Credentials in a merkle_service URL would surface in /health, metric
// labels and startup logs; auth_token is the supported mechanism. The error
// must not echo the password either.
func TestValidate_MerkleServiceRejectsUserinfo(t *testing.T) {
	for _, bad := range []string{"https://user:s3cret@merkle.local", "http://user@merkle.local"} {
		cfg := baseValidConfig()
		cfg.MerkleService.URLs = []string{"http://merkle-a.local", bad}
		err := validate(cfg)
		if err == nil {
			t.Fatalf("merkle_service url %q must be rejected", bad)
		}
		if strings.Contains(err.Error(), "s3cret") {
			t.Fatalf("validation error leaks the password: %v", err)
		}
	}
}

// No merkle_service URL validation error echoes a secret: not for a URL that
// does not parse, a scheme-less user:pass@host (which parses as opaque), a
// token in the query, or a fragment.
func TestValidate_MerkleServiceErrorsRedactSecrets(t *testing.T) {
	for _, bad := range []string{
		"http://merkle.local/%zz-s3cret",     // does not parse
		"svc:s3cret@merkle.local:8080",       // scheme-less user info
		"https://merkle.local/?token=s3cret", // query
		"https://merkle.local/#s3cret",       // fragment
		"ftp://user:s3cret@merkle.local",     // user info with a bad scheme
	} {
		cfg := baseValidConfig()
		cfg.MerkleService.URLs = []string{bad}
		err := validate(cfg)
		if err == nil {
			t.Fatalf("merkle_service url %q must be rejected", bad)
		}
		if strings.Contains(err.Error(), "s3cret") {
			t.Fatalf("validation error for %q leaks the secret: %v", bad, err)
		}
	}
}

// Chaintracks seeds its header chain from the go-chaincfg genesis for the
// network name ResolveChaintracksNetwork hands it. Tstn must resolve to the
// scaling test network's own genesis — seeding it from regtest's means the
// chain's headers never connect and chaintracks never syncs.
func TestResolveChaintracksNetwork_TstnGenesis(t *testing.T) {
	params, err := chaincfg.GetChainParams(ResolveChaintracksNetwork(NetworkTstn))
	if err != nil {
		t.Fatalf("GetChainParams: %v", err)
	}
	const want = "000000005d221c0e023cb56b5682cf094f32cd959958b40bc931e5797cae706c"
	if got := params.GenesisHash.String(); got != want {
		t.Errorf("tstn genesis: got %s, want %s", got, want)
	}
}
