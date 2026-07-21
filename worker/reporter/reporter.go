package reporter

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	natsPublishFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "loadforge_worker_nats_publish_failures_total",
		Help: "Total failed NATS metric batch publishes.",
	})
	natsDroppedSamples = promauto.NewCounter(prometheus.CounterOpts{
		Name: "loadforge_worker_metric_samples_dropped_total",
		Help: "Total metric samples dropped because the worker reporter buffer was full.",
	})
)

type RequestSample struct {
	Endpoint   string  `json:"endpoint"`
	Method     string  `json:"method"`
	StatusCode int     `json:"status_code"`
	LatencyMs  float64 `json:"latency_ms"`
	BytesRecv  int64   `json:"bytes_recv"`
	Error      string  `json:"error,omitempty"`
}

type MetricsBatch struct {
	RunID     string          `json:"run_id"`
	WorkerID  string          `json:"worker_id"`
	Timestamp time.Time       `json:"ts"`
	Samples   []RequestSample `json:"samples"`
}

type Reporter struct {
	runID    string
	workerID string
	subject  string
	nc       *nats.Conn
	log      *slog.Logger
	interval time.Duration

	ch     chan RequestSample
	done   chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
	closed atomic.Bool
}

type Config struct {
	RunID          string
	WorkerID       string
	NATSURL        string
	FlushInterval  time.Duration
	BufferCapacity int
	Logger         *slog.Logger
}

func New(ctx context.Context, cfg Config) (*Reporter, error) {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 500 * time.Millisecond
	}
	if cfg.BufferCapacity <= 0 {
		cfg.BufferCapacity = 10000
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.NATSURL == "" {
		cfg.NATSURL = nats.DefaultURL
	}

	nc, err := nats.Connect(cfg.NATSURL,
		nats.Name("loadforge-worker-reporter"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(250*time.Millisecond),
		nats.Timeout(3*time.Second),
	)
	if err != nil {
		return nil, err
	}

	r := &Reporter{
		runID:    cfg.RunID,
		workerID: cfg.WorkerID,
		subject:  "loadforge.metrics." + cfg.RunID + "." + cfg.WorkerID,
		nc:       nc,
		log:      cfg.Logger,
		interval: cfg.FlushInterval,
		ch:       make(chan RequestSample, cfg.BufferCapacity),
		done:     make(chan struct{}),
	}
	r.wg.Add(1)
	go r.loop(ctx)
	return r, nil
}

func (r *Reporter) Record(sample RequestSample) {
	if r.closed.Load() {
		return
	}
	select {
	case r.ch <- sample:
		return
	default:
		select {
		case <-r.ch:
			natsDroppedSamples.Inc()
			r.log.Warn("metric sample dropped due to full reporter buffer", "run_id", r.runID, "worker_id", r.workerID)
		default:
		}
		select {
		case r.ch <- sample:
		default:
			natsDroppedSamples.Inc()
		}
	}
}

func (r *Reporter) Close(ctx context.Context) error {
	r.once.Do(func() {
		r.closed.Store(true)
		close(r.ch)
	})

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := r.nc.FlushWithContext(ctx); err != nil {
		return err
	}
	r.nc.Close()
	return nil
}

func (r *Reporter) loop(ctx context.Context) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	batch := make([]RequestSample, 0, 1024)
	for {
		select {
		case sample, ok := <-r.ch:
			if !ok {
				r.publish(batch)
				return
			}
			batch = append(batch, sample)
		case <-ticker.C:
			if len(batch) > 0 {
				r.publish(batch)
				batch = make([]RequestSample, 0, cap(batch))
			}
		case <-ctx.Done():
			for {
				select {
				case sample, ok := <-r.ch:
					if !ok {
						r.publish(batch)
						return
					}
					batch = append(batch, sample)
				default:
					r.publish(batch)
					return
				}
			}
		}
	}
}

func (r *Reporter) publish(samples []RequestSample) {
	if len(samples) == 0 {
		return
	}
	payload, err := json.Marshal(MetricsBatch{
		RunID:     r.runID,
		WorkerID:  r.workerID,
		Timestamp: time.Now().UTC(),
		Samples:   samples,
	})
	if err != nil {
		r.log.Error("failed to marshal metrics batch", "error", err)
		return
	}
	if err := r.nc.Publish(r.subject, payload); err != nil {
		natsPublishFailures.Inc()
		r.log.Warn("failed to publish metrics batch", "error", err, "subject", r.subject, "samples", len(samples))
	}
}

func (r *Reporter) Subject() string {
	return r.subject
}

func IsClosed(err error) bool {
	return errors.Is(err, nats.ErrConnectionClosed)
}
