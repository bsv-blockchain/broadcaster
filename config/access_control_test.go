package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func validAccessControl() *Config {
	return &Config{
		APIServer: API{Port: 8080, TrustedPlatform: TrustedPlatformCloudflare},
		Auth:      AuthConfig{Enabled: true},
		Admin:     AdminConfig{Enabled: true, Port: 8090, Token: "secret"},
		RateLimit: RateLimitConfig{
			Enabled: true, LeaseFraction: 0.1, ShrinkThreshold: 0.8,
			KeyCacheTTLMs: 30000, KeyCacheStaleMaxMs: 600000, ExpectedReplicas: 3,
			Costs:     RateLimitCosts{SubmitTx: 10, Read: 1},
			Anonymous: TierConfig{UnitsPerMinute: 50, MaxInFlight: 5, MaxBatch: 1},
			Tiers: map[string]TierConfig{
				TierFree:     {UnitsPerMinute: 50, MaxInFlight: 5, MaxBatch: 1},
				"enterprise": {UnitsPerMinute: 0, MaxInFlight: 500, MaxBatch: 10000},
			},
		},
	}
}

const testProvider = "acme"

func testIntrospectionProvider() IntrospectionProvider {
	return IntrospectionProvider{
		KeyPrefix: "acme_",
		URL:       "https://auth.example.com/v1/introspect",
		Token:     "service-token",
		Tiers:     map[string]string{"gold": "enterprise"},
	}
}

func TestValidateAccessControl(t *testing.T) {
	t.Setenv("TEST_INTROSPECTION_TOKEN", "service-token")
	cases := []struct {
		name    string
		mutate  func(c *Config)
		wantErr string
	}{
		{"valid", func(*Config) {}, ""},
		{"all disabled", func(c *Config) { *c = Config{APIServer: API{Port: 8080}} }, ""},
		{"bad trusted platform", func(c *Config) { c.APIServer.TrustedPlatform = "akamai" }, "api.trusted_platform"},
		{"trusted_proxies entry is neither an IP nor a CIDR", func(c *Config) {
			c.APIServer.TrustedProxies = []string{"not-an-ip"}
		}, "api.trusted_proxies"},
		{"trusted_proxies accepts a CIDR", func(c *Config) {
			c.APIServer.TrustedProxies = []string{"10.0.0.0/8"}
		}, ""},
		{"trusted_proxies accepts a bare IP", func(c *Config) {
			c.APIServer.TrustedProxies = []string{"192.168.1.1"}
		}, ""},
		{"admin without token", func(c *Config) { c.Admin.Token = "" }, "admin.token"},
		{"admin on api port", func(c *Config) { c.Admin.Port = 8080 }, "admin.port"},
		{"admin collides with health port in mode=all", func(c *Config) {
			c.Mode = "all"
			c.Health.Port = 8090
		}, "health.port"},
		{"admin collides with health port when mode empty (defaults to all)", func(c *Config) {
			c.Health.Port = 8090
		}, "health.port"},
		{"admin collides with sse port when sse enabled", func(c *Config) {
			c.Mode = "all"
			c.SSE = SSEConfig{Enabled: true, Port: 8090}
		}, "sse.port"},
		{"admin not collides with sse port when sse disabled", func(c *Config) {
			c.Mode = "all"
			c.SSE = SSEConfig{Enabled: false, Port: 8090}
		}, ""},
		{"admin collides with chaintracks_server port when enabled", func(c *Config) {
			c.Mode = "all"
			c.ChaintracksServer = ChaintracksServerConfig{Enabled: true, Port: 8090}
		}, "chaintracks_server.port"},
		{"admin port collision check skipped outside mode=all", func(c *Config) {
			c.Mode = "api-server"
			c.Health.Port = 8090
		}, ""},
		{"auth store with uri and database", func(c *Config) {
			c.Auth.Store.Mongo = Mongo{URI: "mongodb://auth:27017", Database: "bsv_broadcaster_auth"}
		}, ""},
		{"auth store with explicit timeouts", func(c *Config) {
			c.Auth.Store.Mongo = Mongo{URI: "mongodb://auth:27017", Database: "bsv_broadcaster_auth", ConnectTimeoutMs: 5000, OpTimeoutMs: 2000, MaxPoolSize: 16}
		}, ""},
		{"auth store without database", func(c *Config) {
			c.Auth.Store.Mongo = Mongo{URI: "mongodb://auth:27017"}
		}, "auth.store.mongodb.database"},
		{"auth store database without uri", func(c *Config) {
			c.Auth.Store.Mongo = Mongo{Database: "bsv_broadcaster_auth"}
		}, "auth.store.mongodb.uri is empty"},
		{"auth store without auth.enabled", func(c *Config) {
			c.Auth.Enabled = false
			c.RateLimit.Enabled = false
			c.Auth.Store.Mongo = Mongo{URI: "mongodb://auth:27017", Database: "bsv_broadcaster_auth"}
		}, "auth.store.mongodb.uri requires auth.enabled"},
		{"auth store negative op timeout", func(c *Config) {
			c.Auth.Store.Mongo = Mongo{URI: "mongodb://auth:27017", Database: "bsv_broadcaster_auth", OpTimeoutMs: -1}
		}, "auth.store.mongodb.op_timeout_ms"},
		{"auth store negative pool size", func(c *Config) {
			c.Auth.Store.Mongo = Mongo{URI: "mongodb://auth:27017", Database: "bsv_broadcaster_auth", MaxPoolSize: -1}
		}, "auth.store.mongodb.max_pool_size"},
		{"introspection provider", func(c *Config) {
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: testIntrospectionProvider()}
		}, ""},
		{"introspection provider with token_env", func(c *Config) {
			p := testIntrospectionProvider()
			p.Token, p.TokenEnv = "", "TEST_INTROSPECTION_TOKEN"
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, ""},
		{"introspection token_env unset", func(c *Config) {
			p := testIntrospectionProvider()
			p.Token, p.TokenEnv = "", "TEST_INTROSPECTION_TOKEN_UNSET"
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, "TEST_INTROSPECTION_TOKEN_UNSET is empty or unset"},
		{"introspection token and token_env", func(c *Config) {
			p := testIntrospectionProvider()
			p.TokenEnv = "TEST_INTROSPECTION_TOKEN"
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, "not both"},
		{"introspection without token", func(c *Config) {
			p := testIntrospectionProvider()
			p.Token = ""
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, "token or token_env is required"},
		{"introspection without auth", func(c *Config) {
			c.Auth.Enabled, c.RateLimit.Enabled = false, false
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: testIntrospectionProvider()}
		}, "auth.introspection requires auth.enabled"},
		{"introspection prefix shadows local keys", func(c *Config) {
			p := testIntrospectionProvider()
			p.KeyPrefix = "bsvb_"
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, "overlaps"},
		{"introspection duplicate prefix", func(c *Config) {
			c.Auth.Introspection = map[string]IntrospectionProvider{"one": testIntrospectionProvider(), "two": testIntrospectionProvider()}
		}, "overlaps"},
		{"introspection prefix without underscore", func(c *Config) {
			p := testIntrospectionProvider()
			p.KeyPrefix = "bsv"
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, "key_prefix"},
		{"introspection relative url", func(c *Config) {
			p := testIntrospectionProvider()
			p.URL = "/v1/introspect"
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, "absolute https URL"},
		{"introspection timeout above max", func(c *Config) {
			p := testIntrospectionProvider()
			p.TimeoutMs = 5000
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, "timeout_ms"},
		{"introspection plain http to a remote host", func(c *Config) {
			p := testIntrospectionProvider()
			p.URL = "http://auth.example.com/v1/introspect"
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, "https"},
		{"introspection url with user info", func(c *Config) {
			p := testIntrospectionProvider()
			p.URL = "https://user:pass@auth.example.com/v1/introspect"
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, "must not contain user info"},
		{"introspection url with a query", func(c *Config) {
			p := testIntrospectionProvider()
			p.URL = "https://auth.example.com/v1/introspect?key=x"
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, "must not contain a query or fragment"},
		{"introspection plain http on loopback", func(c *Config) {
			p := testIntrospectionProvider()
			p.URL = "http://127.0.0.1:8099/introspect"
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, ""},
		{"introspection token with trailing newline is trimmed", func(c *Config) {
			p := testIntrospectionProvider()
			p.Token = "service-token\n"
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, ""},
		{"introspection token with inner whitespace", func(c *Config) {
			p := testIntrospectionProvider()
			p.Token = "service token"
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, "whitespace or control characters"},
		{"introspection tier maps to an unconfigured tier", func(c *Config) {
			p := testIntrospectionProvider()
			p.Tiers = map[string]string{"gold": "platinum"}
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, `"platinum" is not a configured ratelimit tier`},
		{"introspection without a tier map", func(c *Config) {
			p := testIntrospectionProvider()
			p.Tiers = nil
			c.Auth.Introspection = map[string]IntrospectionProvider{testProvider: p}
		}, ""},
		{"introspection bad provider name", func(c *Config) {
			c.Auth.Introspection = map[string]IntrospectionProvider{"Wo C": testIntrospectionProvider()}
		}, "provider name"},
		{"ratelimit without auth", func(c *Config) { c.Auth.Enabled = false }, "requires auth.enabled"},
		{"auth with zero cache ttl", func(c *Config) { c.RateLimit.KeyCacheTTLMs = 0 }, "key_cache_ttl_ms"},
		{"stale shorter than ttl", func(c *Config) { c.RateLimit.KeyCacheStaleMaxMs = 1 }, "key_cache_ttl_ms"},
		{"lease fraction zero", func(c *Config) { c.RateLimit.LeaseFraction = 0 }, "lease_fraction"},
		{"shrink above one", func(c *Config) { c.RateLimit.ShrinkThreshold = 1.5 }, "shrink_threshold"},
		{"no replicas", func(c *Config) { c.RateLimit.ExpectedReplicas = 0 }, "expected_replicas"},
		{"zero cost", func(c *Config) { c.RateLimit.Costs.Read = 0 }, "costs"},
		{"missing free tier", func(c *Config) { delete(c.RateLimit.Tiers, TierFree) }, "tiers.free"},
		{"reserved anonymous tier", func(c *Config) { c.RateLimit.Tiers[TierAnonymous] = TierConfig{} }, "reserved"},
		{"negative units", func(c *Config) { c.RateLimit.Tiers["enterprise"] = TierConfig{UnitsPerMinute: -1} }, ">= 0"},
		{"single tx costs more than the tier's whole budget", func(c *Config) {
			c.RateLimit.Tiers["enterprise"] = TierConfig{UnitsPerMinute: 5} // costs.submit_tx is 10
		}, "costs.submit_tx"},
		{"max_batch at the configured costs exceeds units_per_minute", func(c *Config) {
			c.RateLimit.Tiers["enterprise"] = TierConfig{UnitsPerMinute: 10, MaxBatch: 5} // 5*10+1=51 > 10
		}, "max_batch"},
		{"read costs more than the tier's whole budget with no max_batch", func(c *Config) {
			// Only the tier under test may be unpayable. At costs.read 100
			// the default anonymous and free tiers (50 units, max_batch 1)
			// would fail the batch check first, whose message also names
			// costs.read -- which is how this case used to pass with the
			// single-read guard removed. The expected error is the guard's
			// own, for a tier whose max_batch of 0 skips the batch check.
			c.RateLimit.Costs.Read = 100
			c.RateLimit.Anonymous = TierConfig{UnitsPerMinute: 1000, MaxInFlight: 5, MaxBatch: 1}
			c.RateLimit.Tiers[TierFree] = TierConfig{UnitsPerMinute: 1000, MaxInFlight: 5, MaxBatch: 1}
			c.RateLimit.Tiers["enterprise"] = TierConfig{UnitsPerMinute: 50}
		}, `tier "enterprise": costs.read (100) exceeds units_per_minute (50): a single read could never succeed`},
		{"read exactly payable with no max_batch", func(c *Config) {
			c.RateLimit.Costs.Read = 50
			c.RateLimit.Anonymous = TierConfig{UnitsPerMinute: 1000, MaxInFlight: 5, MaxBatch: 1}
			c.RateLimit.Tiers[TierFree] = TierConfig{UnitsPerMinute: 1000, MaxInFlight: 5, MaxBatch: 1}
			c.RateLimit.Tiers["enterprise"] = TierConfig{UnitsPerMinute: 50}
		}, ""},
		{"max_batch large enough to overflow the batch cost", func(c *Config) {
			c.RateLimit.Tiers["enterprise"] = TierConfig{UnitsPerMinute: 1000, MaxBatch: 1 << 62}
		}, "max_batch"},
		{"max_batch exactly payable", func(c *Config) {
			c.RateLimit.Tiers["enterprise"] = TierConfig{UnitsPerMinute: 51, MaxBatch: 5} // 5*10+1=51
		}, ""},
		{"anonymous max_batch exceeds units_per_minute", func(c *Config) {
			c.RateLimit.Anonymous = TierConfig{UnitsPerMinute: 10, MaxBatch: 5}
		}, "max_batch"},
		{"unbounded tier skips the payability check even with a huge max_batch", func(c *Config) {
			c.RateLimit.Tiers["enterprise"] = TierConfig{UnitsPerMinute: 0, MaxBatch: 1_000_000}
		}, ""},
		{"free tier at the default costs and max_batch is payable", func(c *Config) {
			c.RateLimit.Tiers[TierFree] = TierConfig{UnitsPerMinute: 50, MaxInFlight: 5, MaxBatch: 1} // 1*10+1=11 <= 50
		}, ""},
		{"invalid tier name with uppercase", func(c *Config) {
			c.RateLimit.Tiers["Enterprise"] = TierConfig{UnitsPerMinute: 0}
		}, "invalid"},
		{"invalid tier name starting with a dash", func(c *Config) {
			c.RateLimit.Tiers["-enterprise"] = TierConfig{UnitsPerMinute: 0}
		}, "invalid"},
		{"valid tier name with digits, underscore and dash", func(c *Config) {
			c.RateLimit.Tiers["tier-2_pro"] = TierConfig{UnitsPerMinute: 0}
		}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validAccessControl()
			tc.mutate(c)
			err := validateAccessControl(c)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

func TestAccessControlDefaults(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	cfg, err := Load(&cobra.Command{})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Auth.Enabled || cfg.RateLimit.Enabled || cfg.Admin.Enabled {
		t.Fatalf("auth/ratelimit/admin must default off, got %v/%v/%v", cfg.Auth.Enabled, cfg.RateLimit.Enabled, cfg.Admin.Enabled)
	}
	if cfg.APIServer.TrustedPlatform != "" {
		t.Errorf("api.trusted_platform default = %q, want empty", cfg.APIServer.TrustedPlatform)
	}
	if len(cfg.APIServer.TrustedProxies) != 0 {
		t.Errorf("api.trusted_proxies default = %v, want empty", cfg.APIServer.TrustedProxies)
	}
	free, ok := cfg.RateLimit.Tiers[TierFree]
	if !ok || free.UnitsPerMinute != 50 || free.MaxInFlight != 5 || free.MaxBatch != 1 {
		t.Errorf("free tier default = %+v (present=%v), want 50/5/1", free, ok)
	}
	if cfg.RateLimit.Costs != (RateLimitCosts{SubmitTx: 10, Read: 1}) {
		t.Errorf("costs default = %+v", cfg.RateLimit.Costs)
	}
	if cfg.Admin.Port != 8090 || cfg.RateLimit.KeyCacheTTLMs != 30000 || cfg.RateLimit.LeaseFraction != 0.10 {
		t.Errorf("defaults: admin.port=%d key_cache_ttl_ms=%d lease_fraction=%v",
			cfg.Admin.Port, cfg.RateLimit.KeyCacheTTLMs, cfg.RateLimit.LeaseFraction)
	}
	if cfg.Admin.Host != "127.0.0.1" {
		t.Errorf("admin.host default = %q, want 127.0.0.1 (kubectl port-forward works with it; in-cluster tooling opts in to 0.0.0.0)", cfg.Admin.Host)
	}

	t.Setenv("BSV_BROADCASTER_ADMIN_TOKEN", "from-env")
	overridden, err := Load(&cobra.Command{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if overridden.Admin.Token != "from-env" {
		t.Errorf("BSV_BROADCASTER_ADMIN_TOKEN override = %q", overridden.Admin.Token)
	}
}

// TestAuthStoreDefaultsAndEnv pins that auth.store.mongodb ships unset and
// that BSV_BROADCASTER_AUTH_STORE_MONGODB_* overrides work with no config file at
// all — AutomaticEnv only honors keys viper already knows about.
func TestAuthStoreDefaultsAndEnv(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	cfg, err := Load(&cobra.Command{})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Auth.Store.Configured() || cfg.Auth.Store.Mongo != (Mongo{}) {
		t.Fatalf("auth.store must default unset, got database=%q configured=%v",
			cfg.Auth.Store.Mongo.Database, cfg.Auth.Store.Configured())
	}

	viper.Reset()
	t.Setenv("BSV_BROADCASTER_AUTH_ENABLED", "true")
	t.Setenv("BSV_BROADCASTER_AUTH_STORE_MONGODB_URI", "mongodb://auth-env:27017")
	t.Setenv("BSV_BROADCASTER_AUTH_STORE_MONGODB_DATABASE", "bsv_broadcaster_auth")
	t.Setenv("BSV_BROADCASTER_AUTH_STORE_MONGODB_OP_TIMEOUT_MS", "1500")
	overridden, err := Load(&cobra.Command{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got := overridden.Auth.Store.Mongo
	if !overridden.Auth.Store.Configured() || got.URI != "mongodb://auth-env:27017" || got.Database != "bsv_broadcaster_auth" || got.OpTimeoutMs != 1500 {
		t.Errorf("env override = %+v", got)
	}
}

// TestIntrospectionLoadsFromFile runs the real Load path: providers come
// from the config file, the token from the variable token_env names.
func TestIntrospectionLoadsFromFile(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("TEST_PROVIDER_SERVICE_TOKEN", "from-env\n")

	cfgFile := filepath.Join(t.TempDir(), "config.yaml")
	yaml := `kafka:
  backend: memory
store:
  backend: pebble
auth:
  enabled: true
  introspection:
    acme:
      key_prefix: acme_
      url: https://auth.example.com/v1/introspect
      token_env: TEST_PROVIDER_SERVICE_TOKEN
      timeout_ms: 2000
      tiers:
        Gold: free
`
	if err := os.WriteFile(cfgFile, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := &cobra.Command{}
	BindFlags(cmd)
	if err := cmd.Flags().Set("config", cfgFile); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(cmd)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, ok := cfg.Auth.Introspection[testProvider]
	if !ok {
		t.Fatalf("auth.introspection.acme missing: %+v", cfg.Auth.Introspection)
	}
	if p.KeyPrefix != "acme_" || p.ServiceToken() != "from-env" || p.Timeout() != 2*time.Second {
		t.Errorf("acme provider = %+v (token %q, timeout %v)", p, p.ServiceToken(), p.Timeout())
	}
	// viper lowercases map keys; the introspector matches provider tiers
	// case-insensitively, so this is harmless.
	if p.Tiers["gold"] != "free" {
		t.Errorf("tiers = %v, want gold -> free", p.Tiers)
	}
	if (IntrospectionProvider{}).Timeout() != DefaultIntrospectionTimeoutMs*time.Millisecond {
		t.Error("zero timeout_ms must default")
	}
}
