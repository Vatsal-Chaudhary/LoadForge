package subscriber

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/vatsalchaudhary/loadforge/aggregator/windower"
)

const (
	Subject    = "loadforge.metrics.*.*"
	QueueGroup = "loadforge-aggregators"
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

type Sink interface {
	Add(windower.Key, windower.Sample) bool
}

type Observer interface {
	ObserveWorker(runID, workerID string)
	ObserveSample(runID string, sample RequestSample)
}

type Config struct {
	NATSURL    string
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	Sink       Sink
	Observer   Observer
}

type Subscriber struct {
	nc       *nats.Conn
	sub      *nats.Subscription
	log      *slog.Logger
	sink     Sink
	observer Observer
	decode   prometheus.Counter
	lag      prometheus.Gauge
}

func New(cfg Config) (*Subscriber, error) {
	if cfg.Sink == nil {
		return nil, errors.New("subscriber sink is required")
	}
	if cfg.NATSURL == "" {
		cfg.NATSURL = nats.DefaultURL
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Registerer == nil {
		cfg.Registerer = prometheus.DefaultRegisterer
	}
	decode := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "loadforge_aggregator_nats_decode_failures_total",
		Help: "Malformed or invalid NATS metrics batches rejected by the aggregator.",
	})
	lag := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "loadforge_aggregator_nats_lag_seconds",
		Help: "Age of the most recently consumed metrics batch.",
	})
	cfg.Registerer.MustRegister(decode, lag)

	nc, err := nats.Connect(cfg.NATSURL,
		nats.Name("loadforge-aggregator"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(250*time.Millisecond),
		nats.Timeout(3*time.Second),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			cfg.Logger.Warn("NATS asynchronous subscription error", "error", err)
		}),
	)
	if err != nil {
		return nil, err
	}
	s := &Subscriber{
		nc: nc, log: cfg.Logger, sink: cfg.Sink, observer: cfg.Observer,
		decode: decode, lag: lag,
	}
	sub, err := nc.QueueSubscribe(Subject, QueueGroup, s.handle)
	if err != nil {
		nc.Close()
		return nil, err
	}
	s.sub = sub
	if err := nc.Flush(); err != nil {
		nc.Close()
		return nil, err
	}
	return s, nil
}

func (s *Subscriber) handle(msg *nats.Msg) {
	var batch MetricsBatch
	if err := json.Unmarshal(msg.Data, &batch); err != nil {
		s.reject(msg, err)
		return
	}
	if err := validate(msg.Subject, batch); err != nil {
		s.reject(msg, err)
		return
	}
	lag := time.Since(batch.Timestamp).Seconds()
	if lag < 0 {
		lag = 0
	}
	s.lag.Set(lag)
	if s.observer != nil {
		s.observer.ObserveWorker(batch.RunID, batch.WorkerID)
	}
	for _, sample := range batch.Samples {
		accepted := s.sink.Add(windower.Key{
			RunID: batch.RunID, Endpoint: sample.Endpoint, Method: sample.Method,
		}, windower.Sample{
			Timestamp: batch.Timestamp, StatusCode: sample.StatusCode,
			LatencyMs: sample.LatencyMs,
			Failed:    sample.Error != "" || sample.StatusCode >= 400,
		})
		if accepted && s.observer != nil {
			s.observer.ObserveSample(batch.RunID, sample)
		}
	}
}

func (s *Subscriber) reject(msg *nats.Msg, err error) {
	s.decode.Inc()
	s.log.Warn("rejected metrics batch", "subject", msg.Subject, "error", err)
}

func (s *Subscriber) Close() error {
	if s.sub != nil {
		if err := s.sub.Drain(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
			return err
		}
	}
	if err := s.nc.Drain(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
		return err
	}
	return nil
}

func validate(subject string, batch MetricsBatch) error {
	parts := strings.Split(subject, ".")
	if len(parts) != 4 || parts[0] != "loadforge" || parts[1] != "metrics" {
		return fmt.Errorf("invalid metrics subject %q", subject)
	}
	if batch.RunID == "" || batch.WorkerID == "" || batch.Timestamp.IsZero() {
		return errors.New("run_id, worker_id, and ts are required")
	}
	if parts[2] != batch.RunID || parts[3] != batch.WorkerID {
		return errors.New("subject identifiers do not match payload")
	}
	for i, sample := range batch.Samples {
		if sample.Endpoint == "" || sample.Method == "" || sample.StatusCode < 0 || sample.LatencyMs < 0 {
			return fmt.Errorf("invalid sample at index %d", i)
		}
	}
	return nil
}
