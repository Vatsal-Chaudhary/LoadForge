//go:build integration

package executor_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/vatsalchaudhary/loadforge/pkg/testplan"
	lfclient "github.com/vatsalchaudhary/loadforge/worker/client"
	"github.com/vatsalchaudhary/loadforge/worker/executor"
	"github.com/vatsalchaudhary/loadforge/worker/reporter"
	"github.com/vatsalchaudhary/loadforge/worker/scenario"
)

func TestExecutorPublishesMetricsToNATS(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"token":"abc"}`))
		case "/use":
			if got, want := r.Header.Get("Authorization"), "Bearer abc"; got != want {
				http.Error(w, "missing token", http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte("ok"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer target.Close()

	ns := startNATSServer(t)
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	defer nc.Close()

	subject := "loadforge.metrics.run-it.worker-it"
	var (
		mu      sync.Mutex
		samples []reporter.RequestSample
	)
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var batch reporter.MetricsBatch
		if err := json.Unmarshal(msg.Data, &batch); err != nil {
			t.Errorf("json.Unmarshal() error = %v", err)
			return
		}
		mu.Lock()
		samples = append(samples, batch.Samples...)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rep, err := reporter.New(ctx, reporter.Config{
		RunID:          "run-it",
		WorkerID:       "worker-it",
		NATSURL:        ns.ClientURL(),
		FlushInterval:  100 * time.Millisecond,
		BufferCapacity: 128,
		Logger:         slog.New(slog.NewTextHandler(os.Stdout, nil)),
	})
	if err != nil {
		t.Fatalf("reporter.New() error = %v", err)
	}

	runner, err := scenario.NewRunner(scenario.Config{
		Plan: testplan.TestPlan{
			Target: testplan.Target{BaseURL: target.URL, Timeout: "2s"},
			Scenarios: []testplan.Scenario{{
				Name: "token flow",
				Steps: []testplan.Step{
					{
						Method: "GET",
						Path:   "/token",
						Extract: []testplan.Extraction{
							{Name: "token", From: "response_body", JSONPath: "$.token"},
						},
					},
					{
						Method:  "GET",
						Path:    "/use",
						Headers: map[string]string{"Authorization": "Bearer {{token}}"},
					},
				},
			}},
		},
		Client:   lfclient.NewHTTPClient(lfclient.Config{Timeout: 2 * time.Second}),
		Recorder: rep,
		Logger:   slog.New(slog.NewTextHandler(os.Stdout, nil)),
	})
	if err != nil {
		t.Fatalf("scenario.NewRunner() error = %v", err)
	}

	pool := executor.NewPool(ctx, runner, slog.New(slog.NewTextHandler(os.Stdout, nil)))
	if err := pool.Scale(2); err != nil {
		t.Fatalf("Scale() error = %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := len(samples)
		mu.Unlock()
		if got >= 4 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	pool.Stop()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer closeCancel()
	if err := rep.Close(closeCtx); err != nil {
		t.Fatalf("Reporter.Close() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(samples) < 4 {
		t.Fatalf("samples = %d, want at least 4", len(samples))
	}
	var sawUse bool
	for _, sample := range samples {
		if sample.Endpoint == target.URL+"/use" && sample.StatusCode == http.StatusOK && sample.Error == "" {
			sawUse = true
		}
	}
	if !sawUse {
		t.Fatalf("did not observe successful /use sample: %#v", samples)
	}
}

func startNATSServer(t *testing.T) *server.Server {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		Host:   "127.0.0.1",
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
	})
	if err != nil {
		t.Fatalf("server.NewServer() error = %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS server did not become ready")
	}
	return ns
}
