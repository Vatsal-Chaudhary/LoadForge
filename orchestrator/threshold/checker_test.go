package threshold

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
	"github.com/vatsalchaudhary/loadforge/pkg/testplan"
)

type fakeReader struct {
	metrics Metrics
	err     error
}

func (r fakeReader) ReadMetrics(context.Context, string) (Metrics, error) {
	return r.metrics, r.err
}

type fakeStore struct {
	events  []Event
	results Results
}

func (s *fakeStore) RecordThresholdEvent(_ context.Context, event Event) error {
	s.events = append(s.events, event)
	return nil
}

func (s *fakeStore) UpdateThresholdResults(_ context.Context, _ string, results Results) error {
	s.results = results
	return nil
}

func ptr(v float64) *float64 { return &v }

func TestEvaluateAllThresholdTypesPassAndFail(t *testing.T) {
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	thresholds := &testplan.Thresholds{
		P95LatencyMs:     ptr(100),
		ErrorRatePercent: ptr(2),
		MinRPS:           ptr(50),
		MaxP99LatencyMs:  ptr(250),
		OnBreach:         "report",
	}
	results, breaches := Evaluate(thresholds, Metrics{
		RPS: 49, HasRPS: true,
		P95MS: 101, HasP95MS: true,
		P99MS: 251, HasP99MS: true,
		ErrorRate: .021, HasErrorRate: true,
	}, now)
	if results.Passed {
		t.Fatal("results passed, want failure")
	}
	if len(breaches) != 4 {
		t.Fatalf("breaches = %d, want 4: %#v", len(breaches), breaches)
	}
	for _, name := range []string{"p95_latency_ms", "error_rate_percent", "min_rps", "max_p99_latency_ms"} {
		if results.Checks[name].Passed {
			t.Fatalf("%s passed, want breached", name)
		}
	}

	results, breaches = Evaluate(thresholds, Metrics{
		RPS: 50, HasRPS: true,
		P95MS: 100, HasP95MS: true,
		P99MS: 250, HasP99MS: true,
		ErrorRate: .02, HasErrorRate: true,
	}, now)
	if !results.Passed || len(breaches) != 0 {
		t.Fatalf("results=%#v breaches=%#v, want pass", results, breaches)
	}
}

func TestCheckerReportModeTransitionsWithoutStopping(t *testing.T) {
	store := &fakeStore{}
	state := run.StateRunning
	transitions := []run.State{}
	checker := New(Config{
		RunID:      "run-1",
		Thresholds: &testplan.Thresholds{P95LatencyMs: ptr(100), OnBreach: "report"},
		Reader:     fakeReader{metrics: Metrics{P95MS: 101, HasP95MS: true}},
		Store:      store,
		Logger:     slog.Default(),
		Now:        func() time.Time { return time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC) },
		GetState:   func() run.State { return state },
		SetState:   func(next run.State) { state = next },
		Transition: func(_ context.Context, next run.State, _ string) error {
			transitions = append(transitions, next)
			return nil
		},
		OnStop: func(context.Context) error {
			t.Fatal("OnStop called in report mode")
			return nil
		},
	})
	stop, err := checker.CheckOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !stop || state != run.StateThresholdBreached {
		t.Fatalf("stop=%v state=%s", stop, state)
	}
	if len(transitions) != 1 || transitions[0] != run.StateThresholdBreached {
		t.Fatalf("transitions=%v", transitions)
	}
	if len(store.events) != 1 || store.results.Passed {
		t.Fatalf("events=%#v results=%#v", store.events, store.results)
	}
}

func TestCheckerStopModeInvokesStopPolicy(t *testing.T) {
	state := run.StateRunning
	stopped := false
	checker := New(Config{
		RunID:      "run-1",
		Thresholds: &testplan.Thresholds{MinRPS: ptr(10), OnBreach: "stop"},
		Reader:     fakeReader{metrics: Metrics{RPS: 9, HasRPS: true}},
		GetState:   func() run.State { return state },
		SetState:   func(next run.State) { state = next },
		Transition: func(context.Context, run.State, string) error { return nil },
		OnStop: func(context.Context) error {
			stopped = true
			return nil
		},
	})
	stop, err := checker.CheckOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !stop || !stopped {
		t.Fatalf("stop=%v stopped=%v", stop, stopped)
	}
}

func TestCheckerRecordsMultipleSimultaneousBreaches(t *testing.T) {
	store := &fakeStore{}
	checker := New(Config{
		RunID: "run-1",
		Thresholds: &testplan.Thresholds{
			P95LatencyMs:    ptr(100),
			MaxP99LatencyMs: ptr(150),
			OnBreach:        "report",
		},
		Reader: fakeReader{metrics: Metrics{P95MS: 101, HasP95MS: true, P99MS: 151, HasP99MS: true}},
		Store:  store,
		GetState: func() run.State {
			return run.StateRunning
		},
		Transition: func(context.Context, run.State, string) error { return nil },
	})
	stop, err := checker.CheckOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !stop || len(store.events) != 2 {
		t.Fatalf("stop=%v events=%#v", stop, store.events)
	}
}
