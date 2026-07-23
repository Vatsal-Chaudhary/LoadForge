package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vatsalchaudhary/loadforge/aggregator/exposition"
	"github.com/vatsalchaudhary/loadforge/aggregator/store"
	"github.com/vatsalchaudhary/loadforge/aggregator/subscriber"
	"github.com/vatsalchaudhary/loadforge/aggregator/windower"
)

type config struct {
	natsURL       string
	postgresDSN   string
	redisAddr     string
	metricsAddr   string
	window        time.Duration
	grace         time.Duration
	lifecyclePoll time.Duration
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	if err := run(logger); err != nil {
		logger.Error("aggregator stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	applicationMetrics := exposition.New(registry)
	persistence, err := store.Open(ctx, store.Config{
		PostgresDSN: cfg.postgresDSN,
		RedisAddr:   cfg.redisAddr,
		Logger:      logger,
		Registerer:  registry,
	})
	if err != nil {
		return err
	}
	defer persistence.Close()

	window := windower.New(windower.Config{
		Window: cfg.window, Grace: cfg.grace, Logger: logger, Registerer: registry,
		OnClose: persistence.WriteSnapshots,
	})
	sub, err := subscriber.New(subscriber.Config{
		NATSURL: cfg.natsURL, Logger: logger, Registerer: registry,
		Sink: window, Observer: applicationMetrics,
	})
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	httpServer := &http.Server{
		Addr: cfg.metricsAddr, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("aggregator metrics server listening", "addr", cfg.metricsAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()
	go window.Run(ctx)
	go applicationMetrics.ExpireWorkers(ctx, 2*cfg.window)
	go persistence.PollTerminalRuns(ctx, cfg.lifecyclePoll, func(pollCtx context.Context, runID string) {
		window.EvictRun(pollCtx, runID)
		applicationMetrics.EvictRun(runID)
		logger.Info("evicted completed run metrics", "run_id", runID)
	})

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-serverErr:
		stop()
		runErr = err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := sub.Close(); err != nil {
		logger.Warn("NATS subscriber drain failed", "error", err)
	}
	window.Flush(shutdownCtx)
	return errors.Join(runErr, httpServer.Shutdown(shutdownCtx))
}

func loadConfig() (config, error) {
	window, err := envDuration("AGGREGATOR_WINDOW_SIZE", "WINDOW_SIZE", 10*time.Second)
	if err != nil {
		return config{}, err
	}
	grace, err := envDuration("AGGREGATOR_GRACE_PERIOD", "GRACE_PERIOD", time.Second)
	if err != nil {
		return config{}, err
	}
	poll, err := envDuration("AGGREGATOR_LIFECYCLE_POLL_INTERVAL", "", 5*time.Second)
	if err != nil {
		return config{}, err
	}
	if window <= 0 {
		return config{}, errors.New("window size must be positive")
	}
	if grace < 0 {
		return config{}, errors.New("grace period cannot be negative")
	}
	return config{
		natsURL: os.Getenv("NATS_URL"), postgresDSN: os.Getenv("POSTGRES_DSN"),
		redisAddr:   os.Getenv("REDIS_ADDR"),
		metricsAddr: envString("AGGREGATOR_METRICS_ADDR", ":9090"),
		window:      window, grace: grace, lifecyclePoll: poll,
	}, nil
}

func envDuration(primary, fallback string, defaultValue time.Duration) (time.Duration, error) {
	value := os.Getenv(primary)
	if value == "" && fallback != "" {
		value = os.Getenv(fallback)
	}
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", primary, err)
	}
	return parsed, nil
}

func envString(name, defaultValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defaultValue
}
