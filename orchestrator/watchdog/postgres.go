package watchdog

import (
	"context"
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresEventStore struct {
	db *sql.DB
}

func OpenPostgresEventStore(ctx context.Context, dsn string) (*PostgresEventStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	store := &PostgresEventStore{db: db}
	if err := store.EnsureSchema(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *PostgresEventStore) Close() error {
	return s.db.Close()
}

func (s *PostgresEventStore) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
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

func (s *PostgresEventStore) WriteWorkerEvent(ctx context.Context, event Event) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO worker_events(run_id, worker_id, event_type, message, created_at)
VALUES ($1, $2, $3, $4, $5)`, event.RunID, event.WorkerID, event.Type, event.Message, event.CreatedAt)
	return err
}
