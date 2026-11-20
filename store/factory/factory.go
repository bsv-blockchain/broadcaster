// Package factory constructs a store.Store + store.Leaser pair from config.
// It lives outside the store package so backend implementations can depend on
// store's interfaces without creating an import cycle with the dispatcher.
package factory

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/store"
	"github.com/bsv-blockchain/broadcaster/store/aerospike"
	"github.com/bsv-blockchain/broadcaster/store/mongodb"
	"github.com/bsv-blockchain/broadcaster/store/pebble"
	"github.com/bsv-blockchain/broadcaster/store/postgres"
)

// New constructs a Store and Leaser pair dispatching on cfg.Store.Backend.
// Both return values point to the same underlying backend — every supported
// backend implements both interfaces — so callers can pass the returned
// Leaser into services.propagation without a second factory.
//
// logger carries construction-time warnings a backend can only raise once it
// has parsed its own configuration — settings that are legal but cost a
// guarantee the backend otherwise relies on. It may be nil.
func New(ctx context.Context, cfg *config.Config, logger *zap.Logger) (store.Store, store.Leaser, error) {
	switch cfg.Store.Backend {
	case "", "aerospike":
		s, err := aerospike.New(ctx, cfg.Store.Aerospike)
		if err != nil {
			return nil, nil, err
		}
		return s, s, nil
	case "pebble":
		s, err := pebble.New(cfg.Store.Pebble)
		if err != nil {
			return nil, nil, err
		}
		return s, s, nil
	case "postgres":
		// Embedded Postgres extraction on first run can take tens of seconds;
		// give the connect phase a generous ceiling so the CLI fails fast on
		// real misconfigs rather than hanging forever.
		pgCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		s, err := postgres.New(pgCtx, cfg.Store.Postgres)
		if err != nil {
			return nil, nil, err
		}
		return s, s, nil
	case "mongodb":
		// Server selection against an unreachable or misconfigured deployment
		// is bounded by store.mongodb.connect_timeout_ms; the outer ceiling
		// keeps a pathological TLS handshake — or a mongodb+srv:// SRV/TXT
		// lookup against a black-holed resolver, which the driver runs with no
		// context of its own and New therefore waits on under this ctx — from
		// hanging the CLI.
		mongoCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		s, err := mongodb.New(mongoCtx, cfg.Store.Mongo, logger)
		if err != nil {
			return nil, nil, err
		}
		return s, s, nil
	default:
		return nil, nil, fmt.Errorf("unknown store.backend %q", cfg.Store.Backend)
	}
}
