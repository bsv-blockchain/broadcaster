package postgres

import (
	"context"
	"fmt"
	"net"
	"os"

	embedded "github.com/fergusstrange/embedded-postgres/v2"

	"github.com/bsv-blockchain/broadcaster/config"
)

// startEmbedded spins up a local Postgres instance using the fergusstrange
// embedded-postgres bundle. The binary is extracted to EmbeddedCacheDir on
// first run (which is slow and gets logged once at the call site), and the
// database state lives under EmbeddedDataDir. The returned teardown must be
// called during shutdown — leaving the embedded process alive will leak the
// data-dir lock and block the next start.
//
// Returns the DSN the pgx pool should use, plus a stop func.
func startEmbedded(cfg config.Postgres) (dsn string, stop func() error, err error) {
	if err := os.MkdirAll(cfg.EmbeddedDataDir, 0o750); err != nil {
		return "", nil, fmt.Errorf("creating embedded postgres data dir: %w", err)
	}
	if err := os.MkdirAll(cfg.EmbeddedCacheDir, 0o750); err != nil {
		return "", nil, fmt.Errorf("creating embedded postgres cache dir: %w", err)
	}

	port := cfg.EmbeddedPort
	if port == 0 {
		p, lerr := pickFreePort()
		if lerr != nil {
			return "", nil, fmt.Errorf("picking free port: %w", lerr)
		}
		port = p
	}

	user := cfg.EmbeddedUser
	if user == "" {
		user = "bsv_broadcaster"
	}
	pass := cfg.EmbeddedPassword
	if pass == "" {
		pass = "bsv_broadcaster" //nolint:gosec // local embedded-postgres default, not a secret
	}
	db := cfg.EmbeddedDatabase
	if db == "" {
		db = "bsv_broadcaster"
	}

	ep := embedded.NewDatabase(
		embedded.DefaultConfig().
			Username(user).
			Password(pass).
			Database(db).
			Port(port).
			DataPath(cfg.EmbeddedDataDir).
			RuntimePath(cfg.EmbeddedCacheDir).
			CachePath(cfg.EmbeddedCacheDir),
	)
	if err := ep.Start(); err != nil {
		return "", nil, fmt.Errorf("start embedded postgres on port %d: %w", port, err)
	}

	dsn = fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable",
		user, pass, port, db)
	return dsn, ep.Stop, nil
}

// pickFreePort asks the OS for an ephemeral TCP port and returns it.
// Two concurrent calls could race for the same port, but for a single
// embedded-postgres startup this is fine — the TOCTOU window is tiny.
func pickFreePort() (uint32, error) {
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return uint32(l.Addr().(*net.TCPAddr).Port), nil //nolint:gosec // ephemeral local port fits in uint32 by definition
}
