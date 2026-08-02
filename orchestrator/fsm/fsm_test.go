package fsm

import (
	"context"
	"errors"
	"testing"

	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
)

type memoryStore struct {
	transitions []Transition
	err         error
}

func (s *memoryStore) PersistTransition(ctx context.Context, t Transition) error {
	if s.err != nil {
		return s.err
	}
	s.transitions = append(s.transitions, t)
	return nil
}

func TestTransitionMatrix(t *testing.T) {
	states := []run.State{
		run.StatePending,
		run.StateProvisioning,
		run.StateRunning,
		run.StateScaling,
		run.StateThresholdBreached,
		run.StateDraining,
		run.StateDone,
		run.StateFailed,
	}

	for _, from := range states {
		for _, to := range states {
			t.Run(string(from)+"_to_"+string(to), func(t *testing.T) {
				store := &memoryStore{}
				m := New("run-1", from, store, nil)
				err := m.Transition(context.Background(), to, "test")
				if ValidTransition(from, to) {
					if err != nil {
						t.Fatalf("expected valid transition, got %v", err)
					}
					if got := m.State(); got != to {
						t.Fatalf("state = %s, want %s", got, to)
					}
					if len(store.transitions) != 1 {
						t.Fatalf("persisted transitions = %d, want 1", len(store.transitions))
					}
					return
				}
				if !IsInvalidTransition(err) {
					t.Fatalf("expected InvalidTransitionError, got %T %v", err, err)
				}
				if got := m.State(); got != from {
					t.Fatalf("state changed on invalid transition: %s -> %s", from, got)
				}
				if len(store.transitions) != 0 {
					t.Fatalf("invalid transition was persisted")
				}
			})
		}
	}
}

func TestTransitionWriteAheadPersistence(t *testing.T) {
	persistErr := errors.New("postgres unavailable")
	m := New("run-1", run.StatePending, &memoryStore{err: persistErr}, nil)
	err := m.Transition(context.Background(), run.StateProvisioning, "validated")
	if !errors.Is(err, persistErr) {
		t.Fatalf("error = %v, want %v", err, persistErr)
	}
	if got := m.State(); got != run.StatePending {
		t.Fatalf("state = %s, want %s", got, run.StatePending)
	}
}
