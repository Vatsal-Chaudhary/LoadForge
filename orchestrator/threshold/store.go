package threshold

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

type RedisReader struct {
	client redis.Cmdable
}

func NewRedisReader(client redis.Cmdable) *RedisReader {
	return &RedisReader{client: client}
}

func (r *RedisReader) ReadMetrics(ctx context.Context, runID string) (Metrics, error) {
	prefix := "loadforge:run:" + runID
	values, err := r.client.MGet(ctx, prefix+":rps", prefix+":p95", prefix+":error_rate", prefix+":p99").Result()
	if err != nil {
		return Metrics{}, err
	}
	return Metrics{
		RPS: parseFloat(values[0]), HasRPS: values[0] != nil,
		P95MS: parseFloat(values[1]), HasP95MS: values[1] != nil,
		ErrorRate: parseFloat(values[2]), HasErrorRate: values[2] != nil,
		P99MS: parseFloat(values[3]), HasP99MS: values[3] != nil,
	}, nil
}

type PostgresStore struct {
	db    *sql.DB
	redis *redis.Client
}

func OpenPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	store := &PostgresStore{db: db}
	if err := store.EnsureSchema(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func OpenRedis(ctx context.Context, addr string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	return client, nil
}

func (s *PostgresStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS threshold_events (
	id BIGSERIAL PRIMARY KEY,
	run_id TEXT NOT NULL,
	threshold_name TEXT NOT NULL,
	operator TEXT NOT NULL,
	limit_value DOUBLE PRECISION NOT NULL,
	actual_value DOUBLE PRECISION NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_threshold_events_run_id_created_at
	ON threshold_events(run_id, created_at);
ALTER TABLE test_runs
	ADD COLUMN IF NOT EXISTS threshold_results JSONB;`)
	return err
}

func (s *PostgresStore) RecordThresholdEvent(ctx context.Context, event Event) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO threshold_events(run_id, threshold_name, operator, limit_value, actual_value, created_at)
VALUES ($1, $2, $3, $4, $5, $6)`,
		event.RunID, event.Name, event.Operator, event.Limit, event.Value, event.CreatedAt)
	return err
}

func (s *PostgresStore) UpdateThresholdResults(ctx context.Context, runID string, results Results) error {
	payload, err := json.Marshal(results)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE test_runs SET threshold_results = $2 WHERE id::text = $1`, runID, payload)
	return err
}

func parseFloat(value any) float64 {
	if value == nil {
		return 0
	}
	n, _ := strconv.ParseFloat(fmt.Sprint(value), 64)
	return n
}
