package sse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vatsalchaudhary/loadforge/aggregator/subscriber"
	"github.com/vatsalchaudhary/loadforge/apiserver/model"
)

type RunReader interface {
	GetRun(context.Context, string) (model.Run, error)
}

type Streamer struct {
	nc       *nats.Conn
	runs     RunReader
	interval time.Duration
}

func New(nc *nats.Conn, runs RunReader, interval time.Duration) *Streamer {
	if interval <= 0 {
		interval = time.Second
	}
	return &Streamer{nc: nc, runs: runs, interval: interval}
}

type metricsEvent struct {
	TS      time.Time `json:"ts"`
	RPS     float64   `json:"rps"`
	P50     float64   `json:"p50"`
	P95     float64   `json:"p95"`
	P99     float64   `json:"p99"`
	Errors  float64   `json:"errors"`
	Workers int64     `json:"workers"`
}

type doneEvent struct {
	Status        string  `json:"status"`
	TotalRequests int64   `json:"total_requests"`
	TotalErrors   int64   `json:"total_errors"`
	P99MS         float64 `json:"p99_ms"`
}

type accumulator struct {
	mu        sync.Mutex
	latencies []float64
	requests  int64
	errors    int64
	workers   map[string]struct{}
}

func (a *accumulator) add(batch subscriber.MetricsBatch) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.workers[batch.WorkerID] = struct{}{}
	for _, sample := range batch.Samples {
		a.requests++
		a.latencies = append(a.latencies, sample.LatencyMs)
		if sample.Error != "" || sample.StatusCode >= 400 {
			a.errors++
		}
	}
}

func (a *accumulator) take(window time.Duration, fallbackWorkers int64) metricsEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	sort.Float64s(a.latencies)
	percentile := func(q float64) float64 {
		if len(a.latencies) == 0 {
			return 0
		}
		index := int(math.Ceil(float64(len(a.latencies))*q)) - 1
		index = max(0, min(index, len(a.latencies)-1))
		return a.latencies[index]
	}
	workers := int64(len(a.workers))
	if workers == 0 {
		workers = fallbackWorkers
	}
	out := metricsEvent{
		TS: time.Now().UTC(), RPS: float64(a.requests) / window.Seconds(),
		P50: percentile(.50), P95: percentile(.95), P99: percentile(.99),
		Workers: workers,
	}
	if a.requests > 0 {
		out.Errors = float64(a.errors) / float64(a.requests)
	}
	a.latencies = nil
	a.requests, a.errors = 0, 0
	a.workers = make(map[string]struct{})
	return out
}

func (s *Streamer) Serve(w http.ResponseWriter, r *http.Request, runID string) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return errors.New("streaming is unsupported")
	}
	if s.nc == nil || !s.nc.IsConnected() {
		return errors.New("NATS is unavailable")
	}
	run, err := s.runs.GetRun(r.Context(), runID)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	acc := &accumulator{workers: make(map[string]struct{})}
	sub, err := s.nc.Subscribe("loadforge.metrics."+runID+".*", func(msg *nats.Msg) {
		var batch subscriber.MetricsBatch
		if json.Unmarshal(msg.Data, &batch) == nil && batch.RunID == runID {
			acc.add(batch)
		}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	if err := s.nc.Flush(); err != nil {
		return err
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		if terminal(run.Status) {
			return writeEvent(w, flusher, "done", doneEvent{
				Status: run.Status, TotalRequests: run.TotalRequests,
				TotalErrors: run.TotalErrors, P99MS: run.P99MS,
			})
		}
		select {
		case <-r.Context().Done():
			return nil
		case <-ticker.C:
			run, err = s.runs.GetRun(r.Context(), runID)
			if err != nil {
				return err
			}
			if terminal(run.Status) {
				continue
			}
			if err := writeEvent(w, flusher, "metrics", acc.take(s.interval, run.ActiveWorkers)); err != nil {
				return err
			}
		}
	}
}

func terminal(status string) bool { return status == "DONE" || status == "FAILED" }

func writeEvent(w http.ResponseWriter, flusher http.Flusher, event string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, payload); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}
