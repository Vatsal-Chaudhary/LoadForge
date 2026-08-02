package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/vatsalchaudhary/loadforge/aggregator/windower"
)

const liveTTL = 10 * time.Minute

type Store struct {
	postgres *sql.DB
	redis    *redis.Client
	log      *slog.Logger
	latency  *prometheus.HistogramVec
	failures *prometheus.CounterVec
}

type Config struct {
	PostgresDSN string
	RedisAddr   string
	Logger      *slog.Logger
	Registerer  prometheus.Registerer
}

func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.PostgresDSN == "" {
		return nil, errors.New("POSTGRES_DSN is required")
	}
	if cfg.RedisAddr == "" {
		return nil, errors.New("REDIS_ADDR is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Registerer == nil {
		cfg.Registerer = prometheus.DefaultRegisterer
	}
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "loadforge_aggregator_store_write_duration_seconds",
		Help:    "Aggregator persistence operation latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"backend"})
	failures := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "loadforge_aggregator_store_write_failures_total",
		Help: "Failed aggregator persistence operations.",
	}, []string{"backend"})
	cfg.Registerer.MustRegister(latency, failures)

	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	client := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := client.Ping(ctx).Err(); err != nil {
		db.Close()
		client.Close()
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	s := &Store{
		postgres: db, redis: client, log: cfg.Logger,
		latency: latency, failures: failures,
	}
	if err := s.EnsureSchema(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) EnsureSchema(ctx context.Context) error {
	_, err := s.postgres.ExecContext(ctx, `
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
	ADD COLUMN IF NOT EXISTS latency_histogram TEXT;`)
	return err
}

func (s *Store) WriteSnapshots(ctx context.Context, snapshots []windower.Snapshot) error {
	if len(snapshots) == 0 {
		return nil
	}
	pgErr := s.writePostgres(ctx, snapshots)
	redisErr := s.writeRedis(ctx, snapshots)
	return errors.Join(pgErr, redisErr)
}

func (s *Store) writePostgres(ctx context.Context, snapshots []windower.Snapshot) error {
	started := time.Now()
	defer func() { s.latency.WithLabelValues("postgres").Observe(time.Since(started).Seconds()) }()

	const maxRowsPerInsert = 1000
	for start := 0; start < len(snapshots); start += maxRowsPerInsert {
		end := min(start+maxRowsPerInsert, len(snapshots))
		if err := s.insertPostgresBatch(ctx, snapshots[start:end]); err != nil {
			s.failures.WithLabelValues("postgres").Inc()
			s.log.Error("Postgres snapshot write failed", "error", err, "snapshots", len(snapshots))
			return err
		}
	}
	return nil
}

func (s *Store) insertPostgresBatch(ctx context.Context, snapshots []windower.Snapshot) error {
	const fields = 13
	args := make([]any, 0, len(snapshots)*fields)
	values := make([]string, 0, len(snapshots))
	for i, snapshot := range snapshots {
		statusCodes, err := json.Marshal(snapshot.StatusCodes)
		if err != nil {
			return err
		}
		base := i*fields + 1
		placeholders := make([]string, fields)
		for j := range placeholders {
			placeholders[j] = "$" + strconv.Itoa(base+j)
		}
		values = append(values, "("+strings.Join(placeholders, ",")+")")
		args = append(args,
			snapshot.Key.RunID, snapshot.Timestamp, snapshot.Key.Endpoint, snapshot.Key.Method,
			snapshot.RPS, snapshot.P50Ms, snapshot.P95Ms, snapshot.P99Ms,
			snapshot.ErrorRate, snapshot.ReqCount, snapshot.ErrCount, statusCodes,
			snapshot.LatencyHist,
		)
	}
	query := `INSERT INTO metric_snapshots
		(run_id, ts, endpoint, method, rps, p50_ms, p95_ms, p99_ms,
		 error_rate, req_count, err_count, status_codes, latency_histogram) VALUES ` + strings.Join(values, ",")
	_, err := s.postgres.ExecContext(ctx, query, args...)
	return err
}

func (s *Store) writeRedis(ctx context.Context, snapshots []windower.Snapshot) error {
	started := time.Now()
	defer func() { s.latency.WithLabelValues("redis").Observe(time.Since(started).Seconds()) }()

	latest := make(map[string]windower.Snapshot)
	for _, snapshot := range snapshots {
		current, ok := latest[snapshot.Key.RunID]
		if !ok || snapshot.Timestamp.After(current.Timestamp) {
			latest[snapshot.Key.RunID] = snapshot
		}
	}
	pipe := s.redis.Pipeline()
	for runID, snapshot := range latest {
		prefix := "loadforge:run:" + runID
		pipe.Set(ctx, prefix+":rps", snapshot.RunRPS, liveTTL)
		pipe.Set(ctx, prefix+":p95", snapshot.RunP95Ms, liveTTL)
		pipe.Set(ctx, prefix+":p99", snapshot.RunP99Ms, liveTTL)
		pipe.Set(ctx, prefix+":error_rate", snapshot.RunErrorRate, liveTTL)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		s.failures.WithLabelValues("redis").Inc()
		s.log.Error("Redis live counter write failed", "error", err, "runs", len(latest))
	}
	return err
}

// PollTerminalRuns reads persisted FSM transitions instead of requiring the
// orchestrator to publish a new lifecycle event. Polling adds bounded database
// latency, but keeps the aggregator decoupled from orchestrator event internals.
func (s *Store) PollTerminalRuns(ctx context.Context, interval time.Duration, evict func(context.Context, string)) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	watermark := time.Now().UTC()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			rows, err := s.postgres.QueryContext(ctx, `
SELECT DISTINCT run_id
FROM test_run_state_transitions
WHERE to_state IN ('DONE', 'FAILED', 'THRESHOLD_BREACHED') AND created_at > $1 AND created_at <= $2`,
				watermark, now.UTC())
			if err != nil {
				s.log.Warn("terminal run poll failed", "error", err)
				continue
			}
			for rows.Next() {
				var runID string
				if err := rows.Scan(&runID); err != nil {
					s.log.Warn("terminal run scan failed", "error", err)
					continue
				}
				evict(ctx, runID)
			}
			if err := rows.Err(); err != nil {
				s.log.Warn("terminal run rows failed", "error", err)
			}
			rows.Close()
			watermark = now.UTC()
		}
	}
}

func (s *Store) Close() error {
	return errors.Join(s.postgres.Close(), s.redis.Close())
}
