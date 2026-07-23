package exposition

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/vatsalchaudhary/loadforge/aggregator/subscriber"
)

type Metrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	active   *prometheus.GaugeVec

	mu       sync.Mutex
	workers  map[string]map[string]struct{}
	lastSeen map[string]map[string]time.Time
}

func New(registerer prometheus.Registerer) *Metrics {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "loadforge_request_total",
			Help: "Total requests observed from LoadForge workers.",
		}, []string{"run_id", "endpoint", "method", "status_code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "loadforge_request_duration_ms",
			Help: "Request latency observed from LoadForge workers in milliseconds.",
			Buckets: []float64{
				1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000,
			},
		}, []string{"run_id", "endpoint"}),
		active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "loadforge_active_workers",
			Help: "Workers that have published metrics for a run.",
		}, []string{"run_id"}),
		workers:  make(map[string]map[string]struct{}),
		lastSeen: make(map[string]map[string]time.Time),
	}
	registerer.MustRegister(m.requests, m.duration, m.active)
	return m
}

func (m *Metrics) ObserveWorker(runID, workerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	workers := m.workers[runID]
	if workers == nil {
		workers = make(map[string]struct{})
		m.workers[runID] = workers
		m.lastSeen[runID] = make(map[string]time.Time)
	}
	workers[workerID] = struct{}{}
	m.lastSeen[runID][workerID] = time.Now()
	m.active.WithLabelValues(runID).Set(float64(len(workers)))
}

func (m *Metrics) ObserveSample(runID string, sample subscriber.RequestSample) {
	m.requests.WithLabelValues(runID, sample.Endpoint, sample.Method, strconv.Itoa(sample.StatusCode)).Inc()
	m.duration.WithLabelValues(runID, sample.Endpoint).Observe(sample.LatencyMs)
}

func (m *Metrics) EvictRun(runID string) {
	m.mu.Lock()
	delete(m.workers, runID)
	delete(m.lastSeen, runID)
	m.mu.Unlock()
	m.active.DeleteLabelValues(runID)
	m.requests.DeletePartialMatch(prometheus.Labels{"run_id": runID})
	m.duration.DeletePartialMatch(prometheus.Labels{"run_id": runID})
}

func (m *Metrics) ExpireWorkers(ctx context.Context, staleAfter time.Duration) {
	if staleAfter <= 0 {
		staleAfter = 20 * time.Second
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.mu.Lock()
			for runID, seen := range m.lastSeen {
				for workerID, last := range seen {
					if now.Sub(last) >= staleAfter {
						delete(seen, workerID)
						delete(m.workers[runID], workerID)
					}
				}
				m.active.WithLabelValues(runID).Set(float64(len(m.workers[runID])))
			}
			m.mu.Unlock()
		}
	}
}
