//go:build e2e

package harness

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	dockerclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Network aliases — referenced by name from inside any container attached
// to the harness network. Outside the network, the host-mapped ports are
// what the test process uses (via accessor methods on Containers).
//
// Names follow docker-style snake-case-with-dashes since most CI runners
// invoke the test under docker; podman accepts the same.
const (
	netAliasPostgres = "bsv-broadcaster-e2e-postgres"
	netAliasRedpanda = "bsv-broadcaster-e2e-redpanda"
	netAliasMerkle   = "bsv-broadcaster-e2e-merkle"

	postgresUser = "bsv_broadcaster"
	postgresPass = "bsv_broadcaster"
	postgresDB   = "merkle"

	// inNetworkKafkaPort is the port redpanda listens on for the alias
	// listener — accessible from other containers on the shared network.
	// 29092 avoids collision with the module's built-in 9092 (host) and
	// 9093 (internal-rpc) listeners.
	inNetworkKafkaPort = 29092

	merkleHTTPPort = "8080/tcp"
	merkleP2PPort  = "9905/tcp"

	// defaultMerkleImage is the merkle-service container the smoke tests
	// boot. It is pinned to an immutable digest ON PURPOSE — the floating
	// `:latest` tag is republished out-of-band by the merkle-service repo
	// and has regressed the block round-trip in the past (dropped
	// BLOCK_PROCESSED callbacks → txs never reach MINED), turning every
	// BSV Broadcaster CI run red regardless of BSV Broadcaster's own changes.
	//
	// This digest is the merkle-service v0.2.5 release, which both (a) enriches
	// BLOCK_PROCESSED with merkleRoot + subtreeCount + subtreeHashes +
	// coinbaseBump (issue #195 producer side) and (b) fixes the round-trip
	// regressions the sarama->franz-go migration introduced: producers/consumers
	// now set AllowAutoTopicCreation (#145) and consumers create their topics
	// before joining the group (#147). v0.2.3/v0.2.4 had the enrichment but a
	// broken round-trip — produce failed with UNKNOWN_TOPIC_OR_PARTITION ("failed
	// to enqueue") and /reprocess block messages sat unconsumed. BSV Broadcaster consumes
	// the enrichment to build the compound BUMP without any datahub fetch — the
	// e2e TestSmoke_RealBlockMined_SingleSubtree asserts the datahub-free path.
	// Re-point this at a newer digest once upstream merkle-service is confirmed
	// compatible (track via the e2e suite), rather than `:latest`. Keep in sync
	// with the pre-pull digest in .github/workflows/e2e-smoke.yml.
	defaultMerkleImage = "ghcr.io/bsv-blockchain/merkle-service@sha256:ff4ae409df2680c54267c27887dc1744fb3b64e5ddc5636882185ce82b35ced3"
)

// Containers holds the running container set the harness manages and the
// shared network they all attach to. Close terminates everything in the
// reverse order it was started.
type Containers struct {
	Network  *testcontainers.DockerNetwork
	Postgres *postgres.PostgresContainer
	Redpanda *redpanda.Container
	Merkle   testcontainers.Container

	// MerkleHostURL is reachable from the test process (host-side).
	MerkleHostURL string
	// MerkleInternalURL is reachable from other containers (e.g. one peer
	// gossiping to merkle-service via libp2p — though that path uses
	// multiaddrs, not HTTP).
	MerkleInternalURL string

	// GatewayIP is the bridge-network gateway IP from a container's
	// perspective — i.e., the address that routes back to the host the
	// test process is running on. Used by LibP2PHost to advertise an
	// announce multiaddr containers can actually dial.
	//
	// On Docker, host.docker.internal:host-gateway resolves to this
	// gateway IP and works either way. On rootless podman + slirp4netns
	// the host-gateway IP is a synthetic link-local address that isn't
	// routable from the container, so we have to thread the real
	// gateway through explicitly.
	GatewayIP string

	// HostFromContainer is the hostname/IP containers on this network
	// should use to reach listeners bound on the TEST HOST (the harness
	// datahub, the libp2p host, BSV Broadcaster's callback receiver). Prefer this
	// over GatewayIP: on current rootless podman (pasta) the bridge
	// gateway no longer maps to the host at all — connections are
	// refused — while the podman-injected host.containers.internal alias
	// maps correctly. On Docker on Linux this is the GatewayIP recipe as
	// before; on Docker Desktop it is host.docker.internal.
	HostFromContainer string
}

// hostFromContainerAddr resolves the address containers use to reach
// host-bound listeners. Podman injects host.containers.internal into
// every container's /etc/hosts and maps it through pasta to the host;
// the bridge-network gateway IP stopped being host-mapped in podman ≥5.8
// (dials are refused), so the alias is the only reliable route. Docker
// on Linux keeps the gateway recipe (host.docker.internal:host-gateway
// resolves to it and routes to the host). Docker Desktop on macOS and
// Windows runs containers in a VM whose bridge gateway is the VM's, not
// the test host's (dials are refused there too); its built-in
// host.docker.internal is what routes to the host.
func hostFromContainerAddr(gatewayIP string) string {
	if isPodmanRuntime() {
		return "host.containers.internal"
	}
	if runtime.GOOS != "linux" {
		return "host.docker.internal"
	}
	if gatewayIP != "" {
		return gatewayIP
	}
	return "host.docker.internal"
}

// isPodmanRuntime sniffs whether the container runtime testcontainers
// resolves to is podman: an explicit podman DOCKER_HOST, or (when unset)
// the presence of the rootless podman socket.
func isPodmanRuntime() bool {
	if host := os.Getenv("DOCKER_HOST"); host != "" {
		return strings.Contains(host, "podman")
	}
	sock := fmt.Sprintf("/run/user/%d/podman/podman.sock", os.Getuid())
	if _, err := os.Stat(sock); err == nil {
		return true
	}
	return false
}

// MerkleStartOptions tells StartContainers how to wire merkle-service to
// the rest of the world. The harness lifts these out of its higher-level
// configuration so callers can swap libp2p bootstrap peers and the
// callback URL without re-running container setup.
type MerkleStartOptions struct {
	// Image is the merkle-service image to pull and run.
	Image string
	// P2PNetwork is the BSV network identifier — "regtest" for the smoke
	// test. go-p2p-message-bus namespaces pubsub topics by this string.
	P2PNetwork string
	// BootstrapPeers is the multiaddr (or comma-separated list) of peers
	// merkle-service should dial on startup. For the smoke test this is
	// the in-process libp2p host's advertised address.
	BootstrapPeers string
	// ExtraEnv is merged on top of the harness-managed env so tests can
	// add knobs (e.g. CALLBACK_TIMEOUT_SEC=2 for fast retries).
	ExtraEnv map[string]string
}

// StartContainers brings up Postgres, Redpanda, and merkle-service on a
// fresh user-defined bridge network. Caller is responsible for calling
// Close on the returned Containers (or wiring it to t.Cleanup).
//
// Most callers should prefer harness.New(), which sequences network
// creation, gateway discovery, and libp2p-host construction in the
// right order — StartContainers is the lower-level entry for tests
// that want to manage those pieces themselves.
//
// Wait strategies: Postgres uses the module's BasicWaitStrategies (log
// "ready to accept connections" twice + 5432/tcp listening), Redpanda
// uses the module default (port mappings), merkle-service waits for
// HTTP 200 OR 503 on /health. 503 (degraded) is accepted because
// merkle-service reports degraded until it has at least one libp2p
// peer — which only happens after this function returns.
func StartContainers(ctx context.Context, t *testing.T, opts MerkleStartOptions) (*Containers, error) {
	t.Helper()

	nw, gateway, err := newNetworkWithGateway(ctx)
	if err != nil {
		return nil, err
	}
	c, err := startContainersOnNetwork(ctx, t, opts, nw, gateway)
	if err != nil {
		// network ownership transfers to startContainersOnNetwork's
		// cleanup path on error; we don't double-remove.
		return nil, err
	}
	return c, nil
}

// newNetworkWithGateway creates the e2e bridge network and inspects it
// to find the gateway IP a container would route through to reach the
// host. Used by harness.New() to thread the gateway into the libp2p
// host before any container starts up.
func newNetworkWithGateway(ctx context.Context) (*testcontainers.DockerNetwork, string, error) {
	nw, err := network.New(
		ctx,
		network.WithDriver("bridge"),
		network.WithLabels(map[string]string{"bsv-broadcaster-e2e": "true"}),
	)
	if err != nil {
		return nil, "", fmt.Errorf("create network: %w", err)
	}
	gateway, err := networkGatewayIP(ctx, nw)
	if err != nil {
		// Best-effort: a missing gateway is logged by the caller and
		// the libp2p host falls back to host.docker.internal.
		gateway = ""
	}
	return nw, gateway, nil
}

// startContainersOnNetwork runs the per-container start sequence
// against an already-created network. Owns Containers ownership for
// teardown when one of the inner Run calls fails part-way through.
func startContainersOnNetwork(ctx context.Context, t *testing.T, opts MerkleStartOptions, nw *testcontainers.DockerNetwork, gateway string) (*Containers, error) {
	t.Helper()

	if opts.Image == "" {
		opts.Image = defaultMerkleImage
	}
	if opts.P2PNetwork == "" {
		opts.P2PNetwork = "regtest"
	}

	out := &Containers{Network: nw, GatewayIP: gateway, HostFromContainer: hostFromContainerAddr(gateway)}

	// Postgres ----------------------------------------------------------
	// BasicWaitStrategies() composes the log-and-port wait the postgres
	// module ships (log "ready to accept connections" twice + 5432/tcp
	// listening) on top of the default wait — without it, postgres.Run
	// returns before the server is actually accepting connections from
	// peer containers on the user-defined network. CI runners hit this
	// race because the log line fires before the inter-container TCP
	// listener is up; merkle-service then tries to dial Postgres and
	// fails with `connection refused` (issue surfaced in CI run
	// 25600805894).
	pg, err := postgres.Run(
		ctx,
		"postgres:17-alpine@sha256:f9cf3e1f0066f0eb0c48707d864f0ac5941e9ff454a072077d97a4d278d07d73",
		postgres.WithDatabase(postgresDB),
		postgres.WithUsername(postgresUser),
		postgres.WithPassword(postgresPass),
		postgres.BasicWaitStrategies(),
		network.WithNetwork([]string{netAliasPostgres}, nw),
	)
	if err != nil {
		out.closeOnError(ctx)
		return nil, fmt.Errorf("start postgres: %w", err)
	}
	out.Postgres = pg

	// Redpanda ----------------------------------------------------------
	// WithListener creates a second Kafka listener accessible from inside
	// the network at the alias:port we name here. The default listener
	// remains the host-mapped one, used by the Container.KafkaSeedBroker
	// helper for host-side test debug.
	rp, err := redpanda.Run(
		ctx,
		"docker.redpanda.com/redpandadata/redpanda:v26.1.9@sha256:8cb52078690728e2b3d1507fc84eda4c07d4d59bd8fa241e9b1cc7eb7de096c9",
		redpanda.WithAutoCreateTopics(),
		redpanda.WithListener(fmt.Sprintf("%s:%d", netAliasRedpanda, inNetworkKafkaPort)),
		network.WithNetwork([]string{netAliasRedpanda}, nw),
	)
	if err != nil {
		out.closeOnError(ctx)
		return nil, fmt.Errorf("start redpanda: %w", err)
	}
	out.Redpanda = rp

	// merkle-service ----------------------------------------------------
	// Env layering: harness-managed defaults first, caller's ExtraEnv
	// last so tests can override.
	env := map[string]string{
		"MODE":      "all-in-one",
		"LOG_LEVEL": "info",

		"STORE_BACKEND":    "sql",
		"STORE_SQL_DRIVER": "postgres",
		"STORE_SQL_DSN": fmt.Sprintf(
			"postgres://%s:%s@%s:5432/%s?sslmode=disable",
			postgresUser, postgresPass, netAliasPostgres, postgresDB,
		),

		"KAFKA_BROKERS": fmt.Sprintf("%s:%d", netAliasRedpanda, inNetworkKafkaPort),

		"P2P_NETWORK":         opts.P2PNetwork,
		"P2P_BOOTSTRAP_PEERS": opts.BootstrapPeers,
		"P2P_DHT_MODE":        "off", // private network — no public DHT crawl
		"P2P_ENABLE_NAT":      "false",
		"P2P_ENABLE_MDNS":     "false",

		// Tests run on RFC1918 / loopback by definition. The published
		// callback URL points back to host.docker.internal so we have to
		// disable the SSRF guard on both inbound HTTP-callback dispatch
		// and outbound datahub fetches.
		"CALLBACK_ALLOW_PRIVATE_IPS": "true",
		"DATAHUB_ALLOW_PRIVATE_IPS":  "true",

		// In-memory blob store avoids needing a writable mount for STUMP
		// data. Lost on container restart — fine for tests.
		"BLOB_STORE_URL": "memory:",

		// Tighten retry budget so tests fail fast rather than wait minutes.
		"CALLBACK_BACKOFF_BASE_SEC": "1",
		"CALLBACK_TIMEOUT_SEC":      "5",
	}
	for k, v := range opts.ExtraEnv {
		env[k] = v
	}

	merkleC, err := testcontainers.Run(
		ctx, opts.Image,
		testcontainers.WithEnv(env),
		// host-gateway lets the container reach the host (and our in-process
		// BSV Broadcaster callback receiver + datahub) at host.docker.internal.
		// Recent Linux Docker engines and Docker Desktop honor this; on
		// rootless podman it requires --network slirp4netns:allow_host_loopback=true
		// which the README documents.
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
		}),
		testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
			req.ExposedPorts = append(req.ExposedPorts, merkleHTTPPort, merkleP2PPort)
			return nil
		}),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/health").
				WithPort(merkleHTTPPort).
				WithStatusCodeMatcher(func(status int) bool {
					// 200 = healthy, 503 = degraded (peer count == 0). Both
					// mean the HTTP handler is up; we only treat unreachable
					// as failure.
					return status == 200 || status == 503
				}).
				WithStartupTimeout(90*time.Second),
		),
		network.WithNetwork([]string{netAliasMerkle}, nw),
	)
	if err != nil {
		out.closeOnError(ctx)
		return nil, fmt.Errorf("start merkle-service: %w", err)
	}
	out.Merkle = merkleC

	hostPort, err := merkleC.MappedPort(ctx, merkleHTTPPort)
	if err != nil {
		out.closeOnError(ctx)
		return nil, fmt.Errorf("merkle host port: %w", err)
	}
	host, err := merkleC.Host(ctx)
	if err != nil {
		out.closeOnError(ctx)
		return nil, fmt.Errorf("merkle host: %w", err)
	}
	out.MerkleHostURL = fmt.Sprintf("http://%s:%s", host, hostPort.Port())
	out.MerkleInternalURL = fmt.Sprintf("http://%s:8080", netAliasMerkle)

	return out, nil
}

// networkGatewayIP inspects the docker/podman network to find the
// gateway IP from a container's perspective — i.e., the address that
// routes back to the host the test process is running on. Returns an
// empty string + error when the network has no IPv4 IPAM entry (very
// unusual; bridge networks always have one in practice).
func networkGatewayIP(ctx context.Context, nw *testcontainers.DockerNetwork) (string, error) {
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return "", fmt.Errorf("docker client: %w", err)
	}
	defer func() { _ = cli.Close() }()
	res, err := cli.NetworkInspect(ctx, nw.ID, dockerclient.NetworkInspectOptions{})
	if err != nil {
		return "", fmt.Errorf("inspect network: %w", err)
	}
	for _, cfg := range res.Network.IPAM.Config {
		if cfg.Gateway.IsValid() && cfg.Gateway.Is4() {
			return cfg.Gateway.String(), nil
		}
	}
	return "", fmt.Errorf("no IPv4 gateway on network %s", nw.Name)
}

// PostgresDSNInternal returns the DSN merkle-service uses to reach
// Postgres from inside the container.
func (c *Containers) PostgresDSNInternal() string {
	return fmt.Sprintf("postgres://%s:%s@%s:5432/%s?sslmode=disable",
		postgresUser, postgresPass, netAliasPostgres, postgresDB)
}

// KafkaBrokerInternal returns the bootstrap broker merkle-service uses
// from inside the container network.
func (c *Containers) KafkaBrokerInternal() string {
	return fmt.Sprintf("%s:%d", netAliasRedpanda, inNetworkKafkaPort)
}

// KafkaBrokerHost returns the host-mapped broker address — useful for
// the test process when it wants to drive Kafka directly.
func (c *Containers) KafkaBrokerHost(ctx context.Context) (string, error) {
	return c.Redpanda.KafkaSeedBroker(ctx)
}

// MerkleLogsContain reads the merkle-service container's stdout/stderr
// since startup and reports whether it contains the substring. Used by
// peering assertions: go-p2p-message-bus logs `[CONNECTED] Topic peer
// <peer-id>` on every libp2p connection, so the harness can grep for
// its own peer ID to confirm merkle-service dialed back.
func (c *Containers) MerkleLogsContain(ctx context.Context, substr string) (bool, error) {
	if c == nil || c.Merkle == nil {
		return false, fmt.Errorf("merkle container not started")
	}
	r, err := c.Merkle.Logs(ctx)
	if err != nil {
		return false, fmt.Errorf("read merkle logs: %w", err)
	}
	defer func() { _ = r.Close() }()
	buf := make([]byte, 4096)
	var collected strings.Builder
	for {
		n, err := r.Read(buf)
		if n > 0 {
			collected.Write(buf[:n])
			if strings.Contains(collected.String(), substr) {
				return true, nil
			}
		}
		if err != nil {
			break
		}
	}
	return false, nil
}

// WaitForMerkleLogLine polls the merkle-service container's logs until
// substr appears, ctx is canceled, or timeout elapses. Returns nil on
// match, error otherwise. Useful for asserting events like libp2p peer
// connections and BLOCK_PROCESSED dispatches that the merkle-service
// binary logs at INFO.
func (c *Containers) WaitForMerkleLogLine(ctx context.Context, substr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := c.MerkleLogsContain(ctx, substr)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for merkle-service log line %q", substr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Close terminates every running container and removes the network. Safe
// to call multiple times; nil receivers are tolerated so a half-built
// Containers can be cleaned up after a partial-failure StartContainers.
func (c *Containers) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	var errs []string
	if c.Merkle != nil {
		if err := c.Merkle.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Sprintf("merkle: %v", err))
		}
	}
	if c.Redpanda != nil {
		if err := c.Redpanda.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Sprintf("redpanda: %v", err))
		}
	}
	if c.Postgres != nil {
		if err := c.Postgres.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Sprintf("postgres: %v", err))
		}
	}
	if c.Network != nil {
		if err := c.Network.Remove(ctx); err != nil {
			errs = append(errs, fmt.Sprintf("network: %v", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("teardown errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

// closeOnError is the partial-cleanup path for StartContainers.
func (c *Containers) closeOnError(ctx context.Context) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = c.Close(cleanupCtx)
	_ = ctx // referenced for clarity — we use a fresh context for cleanup so an already-cancelled ctx doesn't block us
}
