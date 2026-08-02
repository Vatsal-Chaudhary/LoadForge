package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/vatsalchaudhary/loadforge/apiserver/model"
	reportpkg "github.com/vatsalchaudhary/loadforge/report"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	db     *sql.DB
	redis  *redis.Client
	pepper string
}

func Open(ctx context.Context, postgresDSN, redisAddr, redisPassword, pepper string) (*Store, error) {
	if postgresDSN == "" {
		return nil, errors.New("POSTGRES_DSN is required")
	}
	if redisAddr == "" {
		return nil, errors.New("REDIS_ADDR is required")
	}
	if pepper == "" {
		return nil, errors.New("API_KEY_PEPPER is required")
	}
	db, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	rc := redis.NewClient(&redis.Options{Addr: redisAddr, Password: redisPassword})
	if err := rc.Ping(ctx).Err(); err != nil {
		db.Close()
		rc.Close()
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	s := &Store{db: db, redis: rc, pepper: pepper}
	if err := s.EnsureSchema(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS test_runs (
	id UUID PRIMARY KEY,
	name TEXT NOT NULL,
	status TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	started_at TIMESTAMPTZ,
	ended_at TIMESTAMPTZ,
	duration_seconds INT,
	test_plan JSONB NOT NULL,
	result_summary JSONB,
	threshold_results JSONB,
	worker_count INT,
	peak_rps DOUBLE PRECISION,
	total_requests BIGINT NOT NULL DEFAULT 0,
	total_errors BIGINT NOT NULL DEFAULT 0,
	error_rate DOUBLE PRECISION,
	p50_latency_ms DOUBLE PRECISION,
	p95_latency_ms DOUBLE PRECISION,
	p99_latency_ms DOUBLE PRECISION,
	degraded BOOLEAN NOT NULL DEFAULT false,
	created_by TEXT
);
CREATE INDEX IF NOT EXISTS idx_test_runs_status ON test_runs(status);
CREATE INDEX IF NOT EXISTS idx_test_runs_created_at ON test_runs(created_at DESC);
CREATE TABLE IF NOT EXISTS api_keys (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	name TEXT NOT NULL,
	token_hash BYTEA NOT NULL UNIQUE,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	revoked_at TIMESTAMPTZ
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
CREATE INDEX IF NOT EXISTS idx_metric_snapshots_run_ts
	ON metric_snapshots(run_id, ts);
ALTER TABLE metric_snapshots
	ADD COLUMN IF NOT EXISTS latency_histogram TEXT;
CREATE TABLE IF NOT EXISTS worker_events (
	id BIGSERIAL PRIMARY KEY,
	run_id TEXT NOT NULL,
	worker_id TEXT NOT NULL,
	event_type TEXT NOT NULL,
	message TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_worker_events_run_id_created_at
	ON worker_events(run_id, created_at);`)
	return err
}

func (s *Store) Close() error {
	return errors.Join(s.db.Close(), s.redis.Close())
}

func (s *Store) PingPostgres(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *Store) BuildReport(ctx context.Context, runID string) (reportpkg.Report, error) {
	return reportpkg.Builder{DB: s.db}.Build(ctx, runID)
}

// UUID tokens already carry high entropy, so SHA-256 plus a server-side pepper
// provides constant-time indexed lookup without persisting a reusable secret.
func TokenHash(token, pepper string) []byte {
	sum := sha256.Sum256([]byte(pepper + "\x00" + token))
	return sum[:]
}

func (s *Store) Authenticate(ctx context.Context, token string) (model.APIKey, error) {
	var key model.APIKey
	err := s.db.QueryRowContext(ctx, `
SELECT id::text, name FROM api_keys
WHERE token_hash = $1 AND revoked_at IS NULL`, TokenHash(token, s.pepper)).
		Scan(&key.ID, &key.Name)
	if errors.Is(err, sql.ErrNoRows) {
		return key, ErrNotFound
	}
	return key, err
}

func (s *Store) CreatePending(ctx context.Context, runID, name string, plan json.RawMessage, createdBy string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO test_runs(id, name, status, created_at, test_plan, created_by)
VALUES ($1, $2, 'PENDING', $3, $4, $5)`, runID, name, now, plan, createdBy)
	return err
}

func (s *Store) MarkFailed(ctx context.Context, runID string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE test_runs SET status = 'FAILED', ended_at = now() WHERE id = $1`, runID)
	return err
}

func (s *Store) GetRun(ctx context.Context, runID string) (model.Run, error) {
	var out model.Run
	var result []byte
	err := s.db.QueryRowContext(ctx, `
SELECT id::text, name, status, created_at, started_at, ended_at,
       COALESCE(worker_count, 0), COALESCE(total_requests, 0),
       COALESCE(total_errors, 0), COALESCE(p99_latency_ms, 0),
       result_summary, COALESCE(created_by, '')
FROM test_runs WHERE id = $1`, runID).Scan(
		&out.RunID, &out.Name, &out.Status, &out.CreatedAt, &out.StartedAt, &out.EndedAt,
		&out.ActiveWorkers, &out.TotalRequests, &out.TotalErrors, &out.P99MS,
		&result, &out.CreatedBy,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	out.ResultSummary = result
	rps, p95, rate := s.live(ctx, runID)
	out.Live = &model.LiveMetrics{RPS: rps, P95MS: p95, ErrorRate: rate}
	if workers, err := s.redis.Get(ctx, "loadforge:run:"+runID+":active_workers").Int64(); err == nil {
		out.ActiveWorkers = workers
	}
	return out, nil
}

func (s *Store) live(ctx context.Context, runID string) (float64, float64, float64) {
	prefix := "loadforge:run:" + runID
	values, err := s.redis.MGet(ctx, prefix+":rps", prefix+":p95", prefix+":error_rate").Result()
	if err != nil {
		return 0, 0, 0
	}
	parse := func(v any) float64 {
		if v == nil {
			return 0
		}
		n, _ := strconv.ParseFloat(fmt.Sprint(v), 64)
		return n
	}
	return parse(values[0]), parse(values[1]), parse(values[2])
}

func (s *Store) ListRuns(ctx context.Context, limit, offset int, status string) ([]model.Run, int64, error) {
	where := ""
	args := []any{}
	if status != "" {
		where = " WHERE status = $1"
		args = append(args, status)
	}
	var total int64
	if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM test_runs"+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, limit, offset)
	l := len(args)
	rows, err := s.db.QueryContext(ctx, `
SELECT id::text, name, status, created_at, started_at, ended_at,
       COALESCE(worker_count, 0), COALESCE(total_requests, 0),
       COALESCE(total_errors, 0), COALESCE(p99_latency_ms, 0),
       COALESCE(created_by, '')
FROM test_runs`+where+` ORDER BY created_at DESC LIMIT $`+strconv.Itoa(l-1)+` OFFSET $`+strconv.Itoa(l), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := make([]model.Run, 0)
	for rows.Next() {
		var item model.Run
		if err := rows.Scan(&item.RunID, &item.Name, &item.Status, &item.CreatedAt,
			&item.StartedAt, &item.EndedAt, &item.ActiveWorkers, &item.TotalRequests,
			&item.TotalErrors, &item.P99MS, &item.CreatedBy); err != nil {
			return nil, 0, err
		}
		out = append(out, item)
	}
	return out, total, rows.Err()
}
