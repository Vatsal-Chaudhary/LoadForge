package scaler

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
)

var reconcileActionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "loadforge_orchestrator_scaler_reconcile_actions_total",
	Help: "Total scaler reconcile actions.",
}, []string{"action"})

type Profile interface {
	ComputeDesired(elapsed time.Duration) int
	RampDown() bool
}

type StepRampProfile struct {
	InitialWorkers int
	StepSize       int
	StepInterval   time.Duration
	MaxWorkers     int
	AllowRampDown  bool
}

func (p StepRampProfile) ComputeDesired(elapsed time.Duration) int {
	if p.StepInterval <= 0 {
		return clamp(p.InitialWorkers, p.MaxWorkers)
	}
	steps := int(elapsed / p.StepInterval)
	return clamp(p.InitialWorkers+(steps*p.StepSize), p.MaxWorkers)
}

func (p StepRampProfile) RampDown() bool {
	return p.AllowRampDown
}

func clamp(v, max int) int {
	if max > 0 && v > max {
		return max
	}
	if v < 0 {
		return 0
	}
	return v
}

type Provisioner interface {
	CreateWorkers(ctx context.Context, run run.TestRun, count int) error
	DrainAndRemoveWorkers(ctx context.Context, run run.TestRun, count int) error
}

type Registry interface {
	Count(ctx context.Context, runID string) (int, error)
}

type Scaler struct {
	Profile     Profile
	Provisioner Provisioner
	Registry    Registry
	Interval    time.Duration
	Log         *slog.Logger
}

func (s *Scaler) Reconcile(ctx context.Context, testRun run.TestRun, now time.Time) (int, int, error) {
	desired := s.Profile.ComputeDesired(now.Sub(testRun.StartedAt))
	actual, err := s.Registry.Count(ctx, testRun.ID)
	if err != nil {
		reconcileActionsTotal.WithLabelValues("registry_error").Inc()
		return desired, actual, err
	}
	switch {
	case desired > actual:
		toAdd := desired - actual
		reconcileActionsTotal.WithLabelValues("scale_up").Inc()
		if s.Log != nil {
			s.Log.Info("scaler creating workers", "run_id", testRun.ID, "desired", desired, "actual", actual, "count", toAdd)
		}
		return desired, actual, s.Provisioner.CreateWorkers(ctx, testRun, toAdd)
	case desired < actual && s.Profile.RampDown():
		toRemove := actual - desired
		reconcileActionsTotal.WithLabelValues("scale_down").Inc()
		if s.Log != nil {
			s.Log.Info("scaler draining workers", "run_id", testRun.ID, "desired", desired, "actual", actual, "count", toRemove)
		}
		return desired, actual, s.Provisioner.DrainAndRemoveWorkers(ctx, testRun, toRemove)
	default:
		reconcileActionsTotal.WithLabelValues("noop").Inc()
		return desired, actual, nil
	}
}

func (s *Scaler) Run(ctx context.Context, testRun run.TestRun) error {
	interval := s.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			if _, _, err := s.Reconcile(ctx, testRun, now); err != nil && s.Log != nil {
				s.Log.Warn("scaler reconcile failed", "run_id", testRun.ID, "error", err)
			}
		}
	}
}
