package executor

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/vatsalchaudhary/loadforge/worker/scenario"
)

var activeVUGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "loadforge_worker_active_virtual_users",
	Help: "Current active virtual users in this worker.",
})

type Pool struct {
	runner *scenario.Runner
	log    *slog.Logger

	rootCtx context.Context
	cancel  context.CancelFunc

	mu      sync.Mutex
	vus     map[int]context.CancelFunc
	nextID  int
	wg      sync.WaitGroup
	stopped bool

	active atomic.Int64
}

func NewPool(parent context.Context, runner *scenario.Runner, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(parent)
	return &Pool{
		runner:  runner,
		log:     logger,
		rootCtx: ctx,
		cancel:  cancel,
		vus:     make(map[int]context.CancelFunc),
	}
}

func (p *Pool) Scale(target int) error {
	if target < 0 {
		return errors.New("target VU count cannot be negative")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return errors.New("executor pool is stopped")
	}

	current := len(p.vus)
	switch {
	case target > current:
		for i := 0; i < target-current; i++ {
			p.startLocked()
		}
	case target < current:
		toStop := current - target
		for id, cancel := range p.vus {
			cancel()
			delete(p.vus, id)
			toStop--
			if toStop == 0 {
				break
			}
		}
	}
	return nil
}

func (p *Pool) Stop() {
	p.mu.Lock()
	if !p.stopped {
		p.stopped = true
		p.cancel()
		for id, cancel := range p.vus {
			cancel()
			delete(p.vus, id)
		}
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func (p *Pool) Active() int64 {
	return p.active.Load()
}

func (p *Pool) startLocked() {
	id := p.nextID
	p.nextID++
	vuCtx, cancel := context.WithCancel(p.rootCtx)
	p.vus[id] = cancel
	p.wg.Add(1)
	go p.runVU(vuCtx, id)
}

func (p *Pool) runVU(ctx context.Context, id int) {
	defer p.wg.Done()
	defer func() {
		if recovered := recover(); recovered != nil {
			p.log.Error("virtual user panic recovered", "vu_id", id, "panic", recovered, "stack", string(debug.Stack()))
		}
		p.active.Add(-1)
		activeVUGauge.Dec()
		p.log.Info("virtual user stopped", "vu_id", id)
	}()

	p.active.Add(1)
	activeVUGauge.Inc()
	p.log.Info("virtual user started", "vu_id", id)

	vars := make(map[string]string)
	for ctx.Err() == nil {
		if err := p.runner.RunOnce(ctx, vars); err != nil && !errors.Is(err, context.Canceled) {
			p.log.Warn("virtual user scenario iteration failed", "vu_id", id, "error", err)
		}
	}
}
