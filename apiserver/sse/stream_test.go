package sse

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/vatsalchaudhary/loadforge/aggregator/subscriber"
	"github.com/vatsalchaudhary/loadforge/apiserver/model"
)

type sequenceReader struct {
	mu     sync.Mutex
	calls  int
	doneAt int
}

func (r *sequenceReader) GetRun(context.Context, string) (model.Run, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	status := "RUNNING"
	if r.doneAt > 0 && r.calls >= r.doneAt {
		status = "DONE"
	}
	return model.Run{
		RunID: "run-1", Status: status, ActiveWorkers: 2,
		TotalRequests: 10, TotalErrors: 1, P99MS: 30,
	}, nil
}

func TestEventsArriveMetricsThenDone(t *testing.T) {
	ns := startNATS(t)
	defer ns.Shutdown()
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	stream := New(nc, &sequenceReader{doneAt: 3}, 40*time.Millisecond)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := stream.Serve(w, r, "run-1"); err != nil {
			t.Errorf("Serve() error = %v", err)
		}
	}))
	defer server.Close()

	responseCh := make(chan *http.Response, 1)
	go func() {
		resp, _ := http.Get(server.URL)
		responseCh <- resp
	}()
	time.Sleep(15 * time.Millisecond)
	payload, _ := json.Marshal(subscriber.MetricsBatch{
		RunID: "run-1", WorkerID: "worker-1", Timestamp: time.Now(),
		Samples: []subscriber.RequestSample{
			{Endpoint: "/", Method: "GET", StatusCode: 200, LatencyMs: 10},
			{Endpoint: "/", Method: "GET", StatusCode: 500, LatencyMs: 30, Error: "failed"},
		},
	})
	if err := nc.Publish("loadforge.metrics.run-1.worker-1", payload); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	resp := <-responseCh
	if resp == nil {
		t.Fatal("HTTP request failed")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	metricsAt := strings.Index(text, "event: metrics")
	doneAt := strings.Index(text, "event: done")
	if metricsAt < 0 || doneAt <= metricsAt {
		t.Fatalf("events out of order:\n%s", text)
	}
	if !strings.Contains(text, `"rps":50`) || !strings.Contains(text, `"status":"DONE"`) {
		t.Fatalf("event payloads:\n%s", text)
	}
}

func TestClientCancelClosesStreamAndUnsubscribes(t *testing.T) {
	ns := startNATS(t)
	defer ns.Shutdown()
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	stream := New(nc, &sequenceReader{}, 20*time.Millisecond)
	handlerDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = stream.Serve(w, r, "run-1")
		close(handlerDone)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if scanner.Text() == "event: metrics" {
			break
		}
	}
	cancel()
	resp.Body.Close()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("stream handler did not exit after client cancellation")
	}
	deadline := time.Now().Add(time.Second)
	for nc.NumSubscriptions() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := nc.NumSubscriptions(); got != 0 {
		t.Fatalf("NATS subscriptions after cancel = %d, want 0", got)
	}
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
