package scaler

import (
	"context"
	"testing"
	"time"

	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
)

func TestStepRampProfileComputeDesired(t *testing.T) {
	profile := StepRampProfile{InitialWorkers: 2, StepSize: 3, StepInterval: 10 * time.Second, MaxWorkers: 10}
	tests := []struct {
		elapsed time.Duration
		want    int
	}{
		{0, 2},
		{9 * time.Second, 2},
		{10 * time.Second, 5},
		{20 * time.Second, 8},
		{30 * time.Second, 10},
	}
	for _, tt := range tests {
		if got := profile.ComputeDesired(tt.elapsed); got != tt.want {
			t.Fatalf("ComputeDesired(%s) = %d, want %d", tt.elapsed, got, tt.want)
		}
	}
}

type fakeProvisioner struct {
	created int
	drained int
}

func (p *fakeProvisioner) CreateWorkers(ctx context.Context, run run.TestRun, count int) error {
	p.created += count
	return nil
}

func (p *fakeProvisioner) DrainAndRemoveWorkers(ctx context.Context, run run.TestRun, count int) error {
	p.drained += count
	return nil
}

type fakeRegistry struct{ count int }

func (r fakeRegistry) Count(ctx context.Context, runID string) (int, error) { return r.count, nil }

func TestReconcileScaleUpDownAndNoop(t *testing.T) {
	started := time.Unix(100, 0)
	testRun := run.TestRun{ID: "run-1", StartedAt: started}
	tests := []struct {
		name       string
		actual     int
		rampDown   bool
		wantCreate int
		wantDrain  int
	}{
		{name: "scale up", actual: 1, wantCreate: 4},
		{name: "scale down allowed", actual: 7, rampDown: true, wantDrain: 2},
		{name: "scale down disabled", actual: 7},
		{name: "noop", actual: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prov := &fakeProvisioner{}
			s := &Scaler{
				Profile:     StepRampProfile{InitialWorkers: 5, StepSize: 1, StepInterval: time.Minute, MaxWorkers: 5, AllowRampDown: tt.rampDown},
				Provisioner: prov,
				Registry:    fakeRegistry{count: tt.actual},
			}
			desired, actual, err := s.Reconcile(context.Background(), testRun, started)
			if err != nil {
				t.Fatalf("Reconcile returned error: %v", err)
			}
			if desired != 5 || actual != tt.actual {
				t.Fatalf("desired/actual = %d/%d, want 5/%d", desired, actual, tt.actual)
			}
			if prov.created != tt.wantCreate || prov.drained != tt.wantDrain {
				t.Fatalf("created/drained = %d/%d, want %d/%d", prov.created, prov.drained, tt.wantCreate, tt.wantDrain)
			}
		})
	}
}
