package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vatsalchaudhary/loadforge/apiserver/handlers"
	"github.com/vatsalchaudhary/loadforge/apiserver/middleware"
	"github.com/vatsalchaudhary/loadforge/apiserver/orchclient"
	"github.com/vatsalchaudhary/loadforge/apiserver/sse"
	"github.com/vatsalchaudhary/loadforge/apiserver/store"
)

type config struct {
	listenAddr       string
	postgresDSN      string
	redisAddr        string
	redisPassword    string
	orchestratorAddr string
	natsURL          string
	apiKeyPepper     string
	rateLimitRPS     float64
	rateLimitBurst   int
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("API server stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	startupCtx, cancel := context.WithTimeout(rootCtx, 10*time.Second)
	defer cancel()

	persistence, err := store.Open(startupCtx, cfg.postgresDSN, cfg.redisAddr, cfg.redisPassword, cfg.apiKeyPepper)
	if err != nil {
		return err
	}
	defer persistence.Close()
	orch, err := orchclient.Dial(startupCtx, cfg.orchestratorAddr)
	if err != nil {
		return err
	}
	defer orch.Close()
	nc, err := nats.Connect(cfg.natsURL,
		nats.Name("loadforge-apiserver"),
		nats.Timeout(3*time.Second),
		nats.NoReconnect(),
	)
	if err != nil {
		return fmt.Errorf("connect NATS: %w", err)
	}
	defer nc.Close()
	if err := nc.FlushTimeout(3 * time.Second); err != nil {
		return fmt.Errorf("check NATS: %w", err)
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	apiMetrics := middleware.NewMetrics(registry)
	streamer := sse.New(nc, persistence, time.Second)
	api := handlers.New(persistence, orch, streamer)
	limiter := middleware.NewRateLimiter(cfg.rateLimitRPS, cfg.rateLimitBurst)

	protected := middleware.Auth(persistence, limiter.Middleware(api.Routes()))

	root := http.NewServeMux()
	root.HandleFunc("GET /healthz", api.Healthz)
	root.HandleFunc("GET /readyz", api.Readyz)
	root.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	root.Handle("/", protected)
	var rootHandler http.Handler = root
	rootHandler = apiMetrics.Middleware(rootHandler)
	rootHandler = middleware.Logging(logger, rootHandler)

	server := &http.Server{
		Addr: cfg.listenAddr, Handler: rootHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      0, // SSE connections are intentionally long-lived.
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("API server listening", "addr", cfg.listenAddr)
		if serveErr := server.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
	}()

	var runErr error
	select {
	case <-rootCtx.Done():
	case runErr = <-errCh:
		stop()
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	return errors.Join(runErr, server.Shutdown(shutdownCtx))
}

func loadConfig() (config, error) {
	rps, err := envFloat("API_RATE_LIMIT_RPS", 10)
	if err != nil || rps <= 0 {
		return config{}, errors.New("API_RATE_LIMIT_RPS must be positive")
	}
	burst, err := envInt("API_RATE_LIMIT_BURST", 20)
	if err != nil || burst <= 0 {
		return config{}, errors.New("API_RATE_LIMIT_BURST must be positive")
	}
	cfg := config{
		listenAddr:       envString("APISERVER_LISTEN_ADDR", ":8080"),
		postgresDSN:      os.Getenv("POSTGRES_DSN"),
		redisAddr:        os.Getenv("REDIS_ADDR"),
		redisPassword:    os.Getenv("REDIS_PASSWORD"),
		orchestratorAddr: os.Getenv("ORCHESTRATOR_ADDR"),
		natsURL:          os.Getenv("NATS_URL"),
		apiKeyPepper:     os.Getenv("API_KEY_PEPPER"),
		rateLimitRPS:     rps,
		rateLimitBurst:   burst,
	}
	switch {
	case cfg.postgresDSN == "":
		return config{}, errors.New("POSTGRES_DSN is required")
	case cfg.redisAddr == "":
		return config{}, errors.New("REDIS_ADDR is required")
	case cfg.orchestratorAddr == "":
		return config{}, errors.New("ORCHESTRATOR_ADDR is required")
	case cfg.natsURL == "":
		return config{}, errors.New("NATS_URL is required")
	case cfg.apiKeyPepper == "":
		return config{}, errors.New("API_KEY_PEPPER is required")
	}
	return cfg, nil
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envFloat(name string, fallback float64) (float64, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseFloat(value, 64)
}

func envInt(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}
