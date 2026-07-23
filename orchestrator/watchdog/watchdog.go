package watchdog

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
)

type Event struct {
	RunID     string
	WorkerID  string
	Type      string
	Message   string
	CreatedAt time.Time
}

type EventStore interface {
	WriteWorkerEvent(ctx context.Context, event Event) error
}

type Provisioner interface {
	CreateWorkers(ctx context.Context, testRun run.TestRun, count int) error
}

type Registry interface {
	HeartbeatReceived(ctx context.Context, workerID string, within time.Duration) (bool, error)
	MarkDead(ctx context.Context, runID, workerID string) error
}

type Manager struct {
	Registry    Registry
	Provisioner Provisioner
	Events      EventStore
	Interval    time.Duration
	Log         *slog.Logger

	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

func (m *Manager) Watch(ctx context.Context, testRun run.TestRun, workerID string) {
	m.mu.Lock()
	if m.cancels == nil {
		m.cancels = make(map[string]context.CancelFunc)
	}
	if oldCancel, ok := m.cancels[workerID]; ok {
		oldCancel()
	}
	watchCtx, cancel := context.WithCancel(ctx)
	m.cancels[workerID] = cancel
	m.mu.Unlock()

	go m.loop(watchCtx, testRun, workerID)
}

func (m *Manager) Remove(workerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cancel, ok := m.cancels[workerID]; ok {
		cancel()
		delete(m.cancels, workerID)
	}
}

func (m *Manager) Active() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.cancels)
}

func (m *Manager) loop(ctx context.Context, testRun run.TestRun, workerID string) {
	interval := m.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer m.Remove(workerID)

	missed := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := m.Registry.HeartbeatReceived(ctx, workerID, interval*2)
			if err != nil && m.Log != nil {
				m.Log.Warn("heartbeat lookup failed", "run_id", testRun.ID, "worker_id", workerID, "error", err)
			}
			if ok {
				missed = 0
				continue
			}
			missed++
			if missed >= 3 {
				m.handleUnhealthy(ctx, testRun, workerID)
				return
			}
		}
	}
}

func (m *Manager) handleUnhealthy(ctx context.Context, testRun run.TestRun, workerID string) {
	if m.Log != nil {
		m.Log.Warn("worker unhealthy", "run_id", testRun.ID, "worker_id", workerID)
	}
	_ = m.Registry.MarkDead(ctx, testRun.ID, workerID)
	if m.Events != nil {
		_ = m.Events.WriteWorkerEvent(ctx, Event{
			RunID:     testRun.ID,
			WorkerID:  workerID,
			Type:      "unhealthy",
			Message:   "missed heartbeat threshold exceeded",
			CreatedAt: time.Now().UTC(),
		})
	}
	if testRun.State == run.StateRunning && m.Provisioner != nil {
		_ = m.Provisioner.CreateWorkers(ctx, testRun, 1)
	}
}
