package watchdog

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
)

type testRegistry struct{ marked bool }

func (r *testRegistry) HeartbeatReceived(context.Context, string, time.Duration) (bool, error) {
	return false, nil
}
func (r *testRegistry) MarkDead(context.Context, string, string) error {
	r.marked = true
	return nil
}

type testProvisioner struct{ created int }

func (p *testProvisioner) CreateWorkers(context.Context, run.TestRun, int) error {
	p.created++
	return nil
}

func TestHandleUnhealthyEmitsMetricAndReplacesRunningWorker(t *testing.T) {
	registry := &testRegistry{}
	provisioner := &testProvisioner{}
	manager := &Manager{Registry: registry, Provisioner: provisioner}
	counter := workerEventsTotal.WithLabelValues("run-watchdog", "worker-watchdog", "UNHEALTHY")
	before := testutil.ToFloat64(counter)
	replaced := workerEventsTotal.WithLabelValues("run-watchdog", "worker-watchdog", "REPLACED")
	replacedBefore := testutil.ToFloat64(replaced)

	manager.handleUnhealthy(context.Background(), run.TestRun{ID: "run-watchdog", State: run.StateRunning}, "worker-watchdog")

	if got := testutil.ToFloat64(counter); got != before+1 {
		t.Fatalf("unhealthy event counter = %v, want %v", got, before+1)
	}
	if got := testutil.ToFloat64(replaced); got != replacedBefore+1 {
		t.Fatalf("replaced event counter = %v, want %v", got, replacedBefore+1)
	}
	if !registry.marked || provisioner.created != 1 {
		t.Fatalf("marked/created = %v/%d, want true/1", registry.marked, provisioner.created)
	}
}
