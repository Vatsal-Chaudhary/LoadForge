package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vatsalchaudhary/loadforge/apiserver/model"
	"github.com/vatsalchaudhary/loadforge/internal/cliclient"
)

const testPlan = `
name: checkout
version: "1"
target:
  base_url: https://example.com
  timeout: 10s
load_profile:
  type: step_ramp
  initial_workers: 1
  max_workers: 3
  step_size: 1
  step_interval: 10s
workers:
  virtual_users_per_worker: 2
scenarios:
  - name: browse
    weight: 1
    steps:
      - name: home
        method: GET
        path: /
`

func TestCommandsSuccessPaths(t *testing.T) {
	planFile := writePlan(t, testPlan)
	var listStatus string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/validate":
			apiWrite(t, w, http.StatusOK, cliclient.ValidationResponse{Valid: true})
		case r.Method == http.MethodPost && r.URL.Path == "/runs":
			apiWrite(t, w, http.StatusAccepted, cliclient.CreateRunResponse{RunID: "run-1", Status: "PENDING"})
		case r.Method == http.MethodGet && r.URL.Path == "/runs/run-1/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "event: metrics\ndata: {\"rps\":7,\"workers\":1,\"p50\":10,\"p95\":20,\"p99\":30}\n\n")
			fmt.Fprint(w, "event: done\ndata: {\"status\":\"DONE\",\"total_requests\":20,\"total_errors\":1,\"p99_ms\":30}\n\n")
		case r.Method == http.MethodGet && r.URL.Path == "/runs/run-1":
			apiWrite(t, w, http.StatusOK, model.Run{RunID: "run-1", Status: "RUNNING", ActiveWorkers: 1, Live: &model.LiveMetrics{RPS: 7, P95MS: 20, ErrorRate: .05}})
		case r.Method == http.MethodPost && r.URL.Path == "/runs/run-1/stop":
			apiWrite(t, w, http.StatusAccepted, cliclient.StopRunResponse{Status: "DRAINING"})
		case r.Method == http.MethodGet && r.URL.Path == "/runs":
			listStatus = r.URL.Query().Get("status")
			apiWrite(t, w, http.StatusOK, cliclient.ListRunsResponse{Runs: []model.Run{{RunID: "run-1", Status: "DONE", TotalRequests: 10}}, Total: 1})
		case r.Method == http.MethodGet && r.URL.Path == "/runs/run-1/report":
			w.Write([]byte(`{"run_id":"run-1"}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{"validate", []string{"--api-url", srv.URL, "validate", planFile}, "✓ valid"},
		{"dry-run", []string{"--api-url", srv.URL, "run", "--dry-run", planFile}, "Would start run"},
		{"run-watch", []string{"--api-url", srv.URL, "run", planFile}, "→ Report: loadforge report run-1"},
		{"status", []string{"--api-url", srv.URL, "status", "run-1"}, "RUNNING"},
		{"stop", []string{"--api-url", srv.URL, "stop", "run-1"}, "DRAINING"},
		{"list", []string{"--api-url", srv.URL, "list", "--status", "done"}, "run-1"},
		{"report-json", []string{"--api-url", srv.URL, "report", "--json", "run-1"}, `"run_id":"run-1"`},
		{"worker-logs", []string{"worker", "logs", "run-1"}, "not yet implemented"},
		{"dashboard", []string{"dashboard"}, "Milestone 10"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, _, err := runCommandForTest(t, tt.args...)
			if err != nil {
				t.Fatalf("err = %v, out=%s", err, out)
			}
			if !strings.Contains(out, tt.want) {
				t.Fatalf("missing %q in:\n%s", tt.want, out)
			}
		})
	}
	if listStatus != "DONE" {
		t.Fatalf("list status query = %q", listStatus)
	}
}

func TestCommandsErrorPaths(t *testing.T) {
	planFile := writePlan(t, strings.Replace(testPlan, "https://example.com", "http://10.0.0.1", 1))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/validate":
			apiWrite(t, w, http.StatusUnprocessableEntity, cliclient.ValidationResponse{Valid: false, Errors: []cliclient.ValidationError{{Field: "target.base_url", Message: "private/internal targets require allow_internal: true"}}})
		case r.URL.Path == "/runs/missing":
			apiWrite(t, w, http.StatusNotFound, model.ErrorBody{Error: model.APIError{Code: "not_found", Message: "run not found"}})
		case r.URL.Path == "/runs/missing/stop":
			apiWrite(t, w, http.StatusNotFound, model.ErrorBody{Error: model.APIError{Code: "not_found", Message: "run not found"}})
		case r.URL.Path == "/runs":
			apiWrite(t, w, http.StatusBadRequest, model.ErrorBody{Error: model.APIError{Code: "invalid_request", Message: "bad status"}})
		case r.URL.Path == "/runs/missing/report":
			apiWrite(t, w, http.StatusNotFound, model.ErrorBody{Error: model.APIError{Code: "not_found", Message: "run not found"}})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	tests := [][]string{
		{"--api-url", srv.URL, "validate", planFile},
		{"--api-url", srv.URL, "status", "missing"},
		{"--api-url", srv.URL, "stop", "missing"},
		{"--api-url", srv.URL, "list", "--status", "bad"},
		{"--api-url", srv.URL, "report", "--json", "missing"},
	}
	for _, args := range tests {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			if _, _, err := runCommandForTest(t, args...); err == nil {
				t.Fatalf("expected error")
			}
		})
	}
}

func TestRunCIExitsNonZeroOnThresholdBreached(t *testing.T) {
	planFile := writePlan(t, testPlan)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/runs":
			apiWrite(t, w, http.StatusAccepted, cliclient.CreateRunResponse{RunID: "run-1", Status: "PENDING"})
		case r.Method == http.MethodGet && r.URL.Path == "/runs/run-1":
			apiWrite(t, w, http.StatusOK, model.Run{RunID: "run-1", Status: "THRESHOLD_BREACHED"})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	out, _, err := runCommandForTest(t, "--api-url", srv.URL, "run", "--ci", planFile)
	if err == nil {
		t.Fatalf("expected CI error, out=%s", out)
	}
	if !strings.Contains(out, "THRESHOLD_BREACHED") {
		t.Fatalf("missing threshold status in output:\n%s", out)
	}
}

func TestConfigRoundTripAndMasking(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".loadforge")
	out, _, err := runCommandInDir(t, dir, "config", "set", "api_url", "http://api.example")
	if err != nil || !strings.Contains(out, "set api_url") {
		t.Fatalf("set api_url out=%q err=%v", out, err)
	}
	out, _, err = runCommandInDir(t, dir, "config", "set-key", "secret-token-1234")
	if err != nil || strings.Contains(out, "secret-token-1234") || !strings.Contains(out, "*************1234") {
		t.Fatalf("set-key out=%q err=%v", out, err)
	}
	out, _, err = runCommandInDir(t, dir, "config", "get")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "api_url: http://api.example") || !strings.Contains(out, "token: *************1234") || strings.Contains(out, "secret-token-1234") {
		t.Fatalf("config get leaked or missed value:\n%s", out)
	}
}

func runCommandForTest(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	return runCommandInDir(t, filepath.Join(t.TempDir(), ".loadforge"), args...)
}

func runCommandInDir(t *testing.T, dir string, args ...string) (string, string, error) {
	t.Helper()
	oldDir := configDirOverride
	configDirOverride = dir
	t.Cleanup(func() { configDirOverride = oldDir })
	var out, errOut bytes.Buffer
	root := NewRootCommand(&out, &errOut)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func writePlan(t *testing.T, body string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "plan.yaml")
	if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func apiWrite(t *testing.T, w http.ResponseWriter, status int, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}
