package render

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/vatsalchaudhary/loadforge/apiserver/model"
	"github.com/vatsalchaudhary/loadforge/internal/cliclient"
)

func TestMetricsAlignmentAndContent(t *testing.T) {
	var buf bytes.Buffer
	err := Metrics(&buf, cliclient.MetricsEvent{TS: time.Date(2026, 7, 23, 9, 0, 0, 0, time.Local), Workers: 3, RPS: 42.2, P50: 10, P95: 20, P99: 30, Errors: .05})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"Time", "Workers", "RPS", "p50", "09:00:00", "3", "42.2", "5.00%"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 || strings.Count(lines[0], "  ") < 3 || strings.Count(lines[1], "  ") < 3 {
		t.Fatalf("table not visibly aligned:\n%s", out)
	}
}

func TestRunsTableContent(t *testing.T) {
	start := time.Now().Add(-2 * time.Minute)
	var buf bytes.Buffer
	err := Runs(&buf, []model.Run{{RunID: "run-1", Status: "DONE", CreatedAt: start, EndedAt: ptr(start.Add(time.Minute)), TotalRequests: 100, TotalErrors: 2, P99MS: 99}})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"RUN_ID", "STATUS", "DURATION", "run-1", "DONE", "1m0s", "error_rate=2.00%"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func ptr[T any](v T) *T { return &v }
