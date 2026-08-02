//go:build integration

package report

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/vatsalchaudhary/loadforge/aggregator/histogram"
)

func TestBuilderBuildsReportFromSeededPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is required")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ensureReportTestSchema(t, ctx, db)

	runID := uuid.NewString()
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM worker_events WHERE run_id = $1`, runID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM metric_snapshots WHERE run_id = $1`, runID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM test_runs WHERE id = $1`, runID)
	})

	plan := json.RawMessage(`{
		"name":"checkout",
		"version":"1",
		"target":{"base_url":"https://example.com"},
		"load_profile":{"type":"constant","initial_workers":2},
		"workers":{"virtual_users_per_worker":1},
		"scenarios":[{"name":"browse","weight":1,"steps":[{"name":"home","method":"GET","path":"/"}]}]
	}`)
	_, err = db.ExecContext(ctx, `
INSERT INTO test_runs(id, name, status, created_at, ended_at, test_plan, threshold_results)
VALUES ($1, 'checkout', 'DONE', $2, $3, $4, '{"passed":true}'::jsonb)`,
		runID, time.Now().UTC().Add(-time.Minute), time.Now().UTC(), plan)
	if err != nil {
		t.Fatal(err)
	}

	first := encodedRange(t, 1, 100)
	second := encodedRange(t, 1000, 1099)
	for _, row := range []struct {
		ts     time.Time
		p95    float64
		p99    float64
		hist   string
		errors int64
	}{
		{time.Now().UTC().Add(-20 * time.Second), 95, 99, first, 1},
		{time.Now().UTC().Add(-10 * time.Second), 1094, 1098, second, 0},
	} {
		_, err = db.ExecContext(ctx, `
INSERT INTO metric_snapshots(run_id, ts, endpoint, method, rps, p50_ms, p95_ms, p99_ms, error_rate, req_count, err_count, status_codes, latency_histogram)
VALUES ($1, $2, '/items', 'GET', 10, 50, $3, $4, 0, 100, $5, '{"200":99,"500":1}'::jsonb, $6)`,
			runID, row.ts, row.p95, row.p99, row.errors, row.hist)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = db.ExecContext(ctx, `
INSERT INTO worker_events(run_id, worker_id, event_type, message, created_at)
VALUES ($1, 'worker-1', 'UNHEALTHY', 'missed heartbeat', now())`, runID)
	if err != nil {
		t.Fatal(err)
	}

	report, err := (Builder{DB: db}).Build(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Overall.TotalRequests != 200 || report.Overall.TotalErrors != 1 {
		t.Fatalf("overall = %#v", report.Overall)
	}
	if report.Overall.P95LatencyMS < 1088 {
		t.Fatalf("overall p95 = %.3f, want merged histogram p95", report.Overall.P95LatencyMS)
	}
	if len(report.Endpoints) != 1 || report.Endpoints[0].Endpoint != "/items" || report.Endpoints[0].TotalRequests != 200 {
		t.Fatalf("endpoints = %#v", report.Endpoints)
	}
	if report.WorkerFleet.Count != 2 || report.WorkerFleet.UnhealthyCount != 1 {
		t.Fatalf("worker fleet = %#v", report.WorkerFleet)
	}
}

func encodedRange(t *testing.T, start, end int) string {
	t.Helper()
	h := histogram.New()
	for value := start; value <= end; value++ {
		h.RecordMilliseconds(float64(value))
	}
	encoded, err := h.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func ensureReportTestSchema(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS test_runs (
	id UUID PRIMARY KEY,
	name TEXT NOT NULL,
	status TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	started_at TIMESTAMPTZ,
	ended_at TIMESTAMPTZ,
	test_plan JSONB NOT NULL,
	threshold_results JSONB
);
CREATE TABLE IF NOT EXISTS metric_snapshots (
	id BIGSERIAL PRIMARY KEY,
	run_id TEXT NOT NULL,
	ts TIMESTAMPTZ NOT NULL,
	endpoint TEXT NOT NULL,
	method TEXT NOT NULL,
	rps DOUBLE PRECISION,
	p50_ms DOUBLE PRECISION,
	p95_ms DOUBLE PRECISION,
	p99_ms DOUBLE PRECISION,
	error_rate DOUBLE PRECISION,
	req_count BIGINT,
	err_count BIGINT,
	status_codes JSONB,
	latency_histogram TEXT
);
CREATE TABLE IF NOT EXISTS worker_events (
	id BIGSERIAL PRIMARY KEY,
	run_id TEXT NOT NULL,
	worker_id TEXT NOT NULL,
	event_type TEXT NOT NULL,
	message TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`)
	if err != nil {
		t.Fatal(err)
	}
}
