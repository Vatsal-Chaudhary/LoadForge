//go:build integration

package aggregator_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/vatsalchaudhary/loadforge/aggregator/exposition"
	"github.com/vatsalchaudhary/loadforge/aggregator/store"
	"github.com/vatsalchaudhary/loadforge/aggregator/subscriber"
	"github.com/vatsalchaudhary/loadforge/aggregator/windower"
)

func TestMetricsPipeline(t *testing.T) {
	postgresDSN := os.Getenv("TEST_POSTGRES_DSN")
	redisAddr := os.Getenv("TEST_REDIS_ADDR")
	if postgresDSN == "" || redisAddr == "" {
		t.Skip("TEST_POSTGRES_DSN and TEST_REDIS_ADDR are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runID := fmt.Sprintf("aggregator-it-%d", time.Now().UnixNano())

	ns := startNATS(t)
	defer ns.Shutdown()
	registry := prometheus.NewRegistry()
	metrics := exposition.New(registry)
	persistence, err := store.Open(ctx, store.Config{
		PostgresDSN: postgresDSN, RedisAddr: redisAddr,
		Logger: logger, Registerer: registry,
	})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	defer persistence.Close()
	window := windower.New(windower.Config{
		Window: time.Second, Grace: 5 * time.Second,
		Logger: logger, Registerer: registry, OnClose: persistence.WriteSnapshots,
	})
	sub, err := subscriber.New(subscriber.Config{
		NATSURL: ns.ClientURL(), Logger: logger, Registerer: registry,
		Sink: window, Observer: metrics,
	})
	if err != nil {
		t.Fatalf("subscriber.New() error = %v", err)
	}
	defer sub.Close()

	db, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM metric_snapshots WHERE run_id = $1", runID)
		_ = redisClient.Del(context.Background(),
			"loadforge:run:"+runID+":rps",
			"loadforge:run:"+runID+":p95",
			"loadforge:run:"+runID+":p99",
			"loadforge:run:"+runID+":error_rate",
		).Err()
	})

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	batch := subscriber.MetricsBatch{
		RunID: runID, WorkerID: "worker-1", Timestamp: time.Now().UTC(),
		Samples: []subscriber.RequestSample{
			{Endpoint: "/items", Method: "GET", StatusCode: 200, LatencyMs: 10},
			{Endpoint: "/items", Method: "GET", StatusCode: 500, LatencyMs: 20, Error: "server error"},
		},
	}
	payload, _ := json.Marshal(batch)
	if err := nc.Publish("loadforge.metrics."+runID+".worker-1", payload); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		families, _ := registry.Gather()
		for _, family := range families {
			if family.GetName() == "loadforge_request_total" && len(family.Metric) > 0 {
				return true
			}
		}
		return false
	})

	window.Flush(ctx)
	var reqCount, errCount int64
	var rps, p95, errorRate float64
	var statusJSON []byte
	err = db.QueryRowContext(ctx, `
SELECT rps, p95_ms, error_rate, req_count, err_count, status_codes
FROM metric_snapshots WHERE run_id = $1`, runID).
		Scan(&rps, &p95, &errorRate, &reqCount, &errCount, &statusJSON)
	if err != nil {
		t.Fatalf("snapshot query error = %v", err)
	}
	if rps != 2 || reqCount != 2 || errCount != 1 || errorRate != 0.5 || p95 < 19.9 {
		t.Fatalf("snapshot = rps=%v p95=%v error_rate=%v req=%d err=%d", rps, p95, errorRate, reqCount, errCount)
	}
	var statuses map[string]int64
	if err := json.Unmarshal(statusJSON, &statuses); err != nil || statuses["200"] != 1 || statuses["500"] != 1 {
		t.Fatalf("status_codes = %s, error = %v", statusJSON, err)
	}
	if got, err := redisClient.Get(ctx, "loadforge:run:"+runID+":rps").Float64(); err != nil || got != 2 {
		t.Fatalf("Redis rps = %v, %v", got, err)
	}
	if got, err := redisClient.Get(ctx, "loadforge:run:"+runID+":p95").Float64(); err != nil || got < 19.9 {
		t.Fatalf("Redis p95 = %v, %v", got, err)
	}
	if got, err := redisClient.Get(ctx, "loadforge:run:"+runID+":p99").Float64(); err != nil || got < 19.9 {
		t.Fatalf("Redis p99 = %v, %v", got, err)
	}
	if got, err := redisClient.Get(ctx, "loadforge:run:"+runID+":error_rate").Float64(); err != nil || got != 0.5 {
		t.Fatalf("Redis error_rate = %v, %v", got, err)
	}
	if ttl := redisClient.TTL(ctx, "loadforge:run:"+runID+":rps").Val(); ttl <= 9*time.Minute {
		t.Fatalf("Redis TTL = %v, want about 10m", ttl)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(recorder, request)
	body := recorder.Body.String()
	if !strings.Contains(body, `loadforge_request_total{endpoint="/items",method="GET",run_id="`+runID+`",status_code="200"} 1`) {
		t.Fatalf("/metrics missing request counter:\n%s", body)
	}
	if !strings.Contains(body, `loadforge_request_duration_ms_bucket{endpoint="/items",run_id="`+runID+`",le="10"}`) {
		t.Fatalf("/metrics missing duration histogram bucket:\n%s", body)
	}
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func startNATS(t *testing.T) *natsserver.Server {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS server did not become ready")
	}
	return ns
}
