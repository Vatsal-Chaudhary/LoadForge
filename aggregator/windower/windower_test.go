package windower

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestAlignedBoundariesAndGrace(t *testing.T) {
	base := time.Date(2026, 7, 23, 10, 0, 0, 0, time.UTC)
	now := base.Add(10*time.Second + 999*time.Millisecond)
	var got []Snapshot
	w := New(Config{
		Window:     10 * time.Second,
		Grace:      time.Second,
		Now:        func() time.Time { return now },
		Registerer: prometheus.NewRegistry(),
		OnClose: func(_ context.Context, snapshots []Snapshot) error {
			got = append(got, snapshots...)
			return nil
		},
	})
	key := Key{RunID: "run-1", Endpoint: "/items", Method: "GET"}

	if !w.Add(key, Sample{Timestamp: base, StatusCode: 200, LatencyMs: 10}) {
		t.Fatal("sample at window start was rejected")
	}
	if !w.Add(key, Sample{Timestamp: base.Add(10*time.Second - time.Nanosecond), StatusCode: 500, LatencyMs: 20, Failed: true}) {
		t.Fatal("sample before window end was rejected")
	}
	if !w.Add(key, Sample{Timestamp: base.Add(10 * time.Second), StatusCode: 201, LatencyMs: 30}) {
		t.Fatal("sample exactly on next window edge was rejected")
	}

	w.CloseDue(context.Background(), base.Add(11*time.Second))
	if len(got) != 1 {
		t.Fatalf("closed snapshots = %d, want 1", len(got))
	}
	if got[0].Timestamp != base.Add(10*time.Second) || got[0].ReqCount != 2 {
		t.Fatalf("first snapshot = %+v", got[0])
	}
	if got[0].RPS != 0.2 || got[0].ErrorRate != 0.5 {
		t.Fatalf("first snapshot rates = rps %v, error_rate %v", got[0].RPS, got[0].ErrorRate)
	}

	w.Flush(context.Background())
	if len(got) != 2 || got[1].Timestamp != base.Add(20*time.Second) || got[1].ReqCount != 1 {
		t.Fatalf("snapshots after flush = %+v", got)
	}
}

func TestLateSamplesInsideAndOutsideGrace(t *testing.T) {
	base := time.Date(2026, 7, 23, 10, 0, 0, 0, time.UTC)
	now := base.Add(10*time.Second + 500*time.Millisecond)
	var got []Snapshot
	w := New(Config{
		Window:     10 * time.Second,
		Grace:      time.Second,
		Now:        func() time.Time { return now },
		Registerer: prometheus.NewRegistry(),
		OnClose: func(_ context.Context, snapshots []Snapshot) error {
			got = append(got, snapshots...)
			return nil
		},
	})
	key := Key{RunID: "run-1", Endpoint: "/", Method: "GET"}
	sample := Sample{Timestamp: base.Add(9 * time.Second), StatusCode: 200, LatencyMs: 5}

	if !w.Add(key, sample) {
		t.Fatal("late sample within grace was rejected")
	}
	now = base.Add(11 * time.Second)
	if w.Add(key, sample) {
		t.Fatal("sample at grace deadline was accepted")
	}
	w.CloseDue(context.Background(), now)
	if len(got) != 1 || got[0].ReqCount != 1 {
		t.Fatalf("closed snapshots = %+v", got)
	}
}
