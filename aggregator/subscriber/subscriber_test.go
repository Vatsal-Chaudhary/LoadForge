package subscriber

import (
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/vatsalchaudhary/loadforge/aggregator/windower"
)

type recordingSink struct {
	mu      sync.Mutex
	samples []windower.Sample
}

func (s *recordingSink) Add(_ windower.Key, sample windower.Sample) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples = append(s.samples, sample)
	return true
}

func TestSubscriberRejectsMalformedBatchAndContinues(t *testing.T) {
	ns := startNATS(t)
	defer ns.Shutdown()
	registry := prometheus.NewRegistry()
	sink := &recordingSink{}
	sub, err := New(Config{
		NATSURL: ns.ClientURL(), Sink: sink, Registerer: registry,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	if err := nc.Publish("loadforge.metrics.run-1.worker-1", []byte(`not-json`)); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(MetricsBatch{
		RunID: "run-1", WorkerID: "worker-1", Timestamp: time.Now().UTC(),
		Samples: []RequestSample{{Endpoint: "/", Method: "GET", StatusCode: 200, LatencyMs: 12}},
	})
	if err := nc.Publish("loadforge.metrics.run-1.worker-1", payload); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		sink.mu.Lock()
		count := len(sink.samples)
		sink.mu.Unlock()
		if count == 1 && metricValue(t, registry, "loadforge_aggregator_nats_decode_failures_total") == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("subscriber did not process valid message after malformed message")
}

func metricValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name && len(family.Metric) > 0 {
			return counterValue(family.Metric[0])
		}
	}
	return 0
}

func counterValue(metric *dto.Metric) float64 {
	if metric.Counter == nil {
		return 0
	}
	return metric.Counter.GetValue()
}

func startNATS(t *testing.T) *natsserver.Server {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS server did not become ready")
	}
	return ns
}
