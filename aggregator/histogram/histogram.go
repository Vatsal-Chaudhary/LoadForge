package histogram

import (
	"math"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

const (
	minMicros = int64(1)
	maxMicros = int64(60 * 60 * 1_000_000)
)

type Percentiles struct {
	P50 float64
	P95 float64
	P99 float64
}

type Histogram struct {
	h *hdr.Histogram
}

func New() *Histogram {
	return &Histogram{h: hdr.New(minMicros, maxMicros, 3)}
}

func (h *Histogram) RecordMilliseconds(value float64) {
	micros := int64(math.Round(value * 1000))
	if micros < minMicros {
		micros = minMicros
	}
	if micros > maxMicros {
		micros = maxMicros
	}
	_ = h.h.RecordValue(micros)
}

func (h *Histogram) Percentiles() Percentiles {
	if h.h.TotalCount() == 0 {
		return Percentiles{}
	}
	return Percentiles{
		P50: float64(h.h.ValueAtQuantile(50)) / 1000,
		P95: float64(h.h.ValueAtQuantile(95)) / 1000,
		P99: float64(h.h.ValueAtQuantile(99)) / 1000,
	}
}

func (h *Histogram) Merge(other *Histogram) {
	if other != nil {
		h.h.Merge(other.h)
	}
}

func (h *Histogram) Reset() {
	h.h.Reset()
}
