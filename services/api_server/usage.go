package api_server

import (
	"context"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/services/usage"
)

// usageCallTimeout bounds one usage report call. Kept above a provider's
// own time to record a batch (10s for the first one), so a slow but
// successful call isn't dropped as uncertain.
const usageCallTimeout = 15 * time.Second

// initUsage builds the usage recorder and reporter when usage.enabled.
// Config validation guarantees at least one provider sets usage_url. Must
// run before the routes are registered: guarded installs the recorder.
func (s *Server) initUsage(ctx context.Context) {
	if !s.cfg.Usage.Enabled {
		return
	}
	providers := s.cfg.UsageProviders()
	agg := usage.NewAggregator(s.cfg.Usage.MaxBuckets)
	sinks := make([]usage.Sink, 0, len(providers))
	for _, name := range providers {
		p := s.cfg.Auth.Introspection[name]
		client := &http.Client{
			Timeout: usageCallTimeout,
			// The service token travels with every call: never follow a
			// redirect elsewhere.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
		sinks = append(sinks, usage.NewHTTPSink(name, p.UsageURL, p.ServiceToken(), client, s.cfg.Usage.MaxPendingEntries, s.logger))
		s.logger.Info("reporting api key usage to provider", zap.String("provider", name))
	}
	metrics.PreRegisterUsage(providers)
	s.usageRecorder = usage.NewRecorder(agg, providers, s.cfg.RateLimit.Costs.SubmitTx)
	s.usageReporter = usage.NewReporter(agg, sinks, time.Duration(s.cfg.Usage.FlushIntervalMs)*time.Millisecond, s.logger)
	s.usageReporter.Start(ctx)
}

// stopUsage reports what is left; call after the HTTP server has drained.
func (s *Server) stopUsage() {
	if s.usageReporter == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.cfg.Usage.ShutdownFlushTimeoutMs)*time.Millisecond)
	defer cancel()
	s.usageReporter.Close(ctx)
}
