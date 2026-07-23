package histogram

import (
	"math"
	"testing"
)

func TestPercentilesKnownDistribution(t *testing.T) {
	h := New()
	for value := 1; value <= 100; value++ {
		h.RecordMilliseconds(float64(value))
	}

	got := h.Percentiles()
	assertClose(t, "p50", got.P50, 50, 0.1)
	assertClose(t, "p95", got.P95, 95, 0.2)
	assertClose(t, "p99", got.P99, 99, 0.2)

	h.Reset()
	if got := h.Percentiles(); got != (Percentiles{}) {
		t.Fatalf("Percentiles() after Reset = %+v, want zero value", got)
	}
}

func assertClose(t *testing.T, name string, got, want, tolerance float64) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s = %f, want %f (+/-%f)", name, got, want, tolerance)
	}
}
