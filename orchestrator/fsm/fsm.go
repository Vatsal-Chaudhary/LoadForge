package fsm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
)

var transitionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "loadforge_orchestrator_fsm_transitions_total",
	Help: "Total persisted test run state transitions.",
}, []string{"from", "to"})

type Transition struct {
	RunID     string
	From      run.State
	To        run.State
	Reason    string
	CreatedAt time.Time
}

type Store interface {
	PersistTransition(ctx context.Context, t Transition) error
}

type InvalidTransitionError struct {
	From run.State
	To   run.State
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("invalid test run state transition: %s -> %s", e.From, e.To)
}

func IsInvalidTransition(err error) bool {
	var target *InvalidTransitionError
	return errors.As(err, &target)
}

type Machine struct {
	runID string
	state run.State
	store Store
	log   *slog.Logger
}

func New(runID string, initial run.State, store Store, logger *slog.Logger) *Machine {
	if initial == "" {
		initial = run.StatePending
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Machine{runID: runID, state: initial, store: store, log: logger}
}

func (m *Machine) State() run.State {
	return m.state
}

func (m *Machine) Transition(ctx context.Context, to run.State, reason string) error {
	from := m.state
	if !ValidTransition(from, to) {
		return &InvalidTransitionError{From: from, To: to}
	}
	t := Transition{RunID: m.runID, From: from, To: to, Reason: reason, CreatedAt: time.Now().UTC()}
	if err := m.store.PersistTransition(ctx, t); err != nil {
		return err
	}
	m.state = to
	transitionsTotal.WithLabelValues(string(from), string(to)).Inc()
	m.log.Info("test run state transition persisted", "run_id", m.runID, "from", from, "to", to, "reason", reason)
	return nil
}

func ValidTransition(from, to run.State) bool {
	return validTransitions[from][to]
}

var validTransitions = map[run.State]map[run.State]bool{
	run.StatePending:      {run.StateProvisioning: true, run.StateDraining: true, run.StateFailed: true},
	run.StateProvisioning: {run.StateRunning: true, run.StateDraining: true, run.StateFailed: true},
	run.StateRunning:      {run.StateScaling: true, run.StateDraining: true, run.StateFailed: true},
	run.StateScaling:      {run.StateRunning: true, run.StateDraining: true, run.StateFailed: true},
	run.StateDraining:     {run.StateDone: true, run.StateFailed: true},
	run.StateDone:         {},
	run.StateFailed:       {},
}

type PostgresStore struct {
	db *sql.DB
}

func OpenPostgres(ctx context.Context, dsn string) (*PostgresStore, error) {
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

func (s *PostgresStore) Close() error {
	return s.db.Close()
}

func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS test_run_state_transitions (
	id BIGSERIAL PRIMARY KEY,
	run_id TEXT NOT NULL,
	from_state TEXT NOT NULL,
	to_state TEXT NOT NULL,
	reason TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_test_run_state_transitions_run_id_created_at
	ON test_run_state_transitions(run_id, created_at);
CREATE TABLE IF NOT EXISTS worker_events (
	id BIGSERIAL PRIMARY KEY,
	run_id TEXT NOT NULL,
	worker_id TEXT NOT NULL,
	event_type TEXT NOT NULL,
	message TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`)
	return err
}

func (s *PostgresStore) PersistTransition(ctx context.Context, t Transition) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `
INSERT INTO test_run_state_transitions(run_id, from_state, to_state, reason, created_at)
VALUES ($1, $2, $3, $4, $5)`, t.RunID, t.From, t.To, t.Reason, t.CreatedAt); err != nil {
		return err
	}
	var testRunsTable sql.NullString
	if err = tx.QueryRowContext(ctx, `SELECT to_regclass('public.test_runs')::text`).Scan(&testRunsTable); err != nil {
		return err
	}
	if testRunsTable.Valid {
		_, err = tx.ExecContext(ctx, `
UPDATE test_runs
SET status = $2,
	started_at = CASE WHEN $2 = 'RUNNING' THEN COALESCE(started_at, $3) ELSE started_at END,
	ended_at = CASE WHEN $2 IN ('DONE', 'FAILED') THEN COALESCE(ended_at, $3) ELSE ended_at END
WHERE id::text = $1`, t.RunID, t.To, t.CreatedAt)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
