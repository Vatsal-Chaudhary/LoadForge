package cliclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vatsalchaudhary/loadforge/apiserver/model"
)

func TestClientEndpointsSuccess(t *testing.T) {
	var seenAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/runs":
			write(t, w, http.StatusAccepted, CreateRunResponse{RunID: "run-1", Status: "PENDING"})
		case r.Method == http.MethodGet && r.URL.Path == "/runs/run-1":
			write(t, w, http.StatusOK, model.Run{RunID: "run-1", Status: "RUNNING", Live: &model.LiveMetrics{RPS: 10}})
		case r.Method == http.MethodGet && r.URL.Path == "/runs":
			if r.URL.Query().Get("limit") != "20" || r.URL.Query().Get("status") != "RUNNING" {
				t.Fatalf("bad list query: %s", r.URL.RawQuery)
			}
			write(t, w, http.StatusOK, ListRunsResponse{Runs: []model.Run{{RunID: "run-1", Status: "RUNNING"}}, Total: 1})
		case r.Method == http.MethodPost && r.URL.Path == "/runs/run-1/stop":
			write(t, w, http.StatusAccepted, StopRunResponse{Status: "DRAINING"})
		case r.Method == http.MethodPost && r.URL.Path == "/validate":
			write(t, w, http.StatusOK, ValidationResponse{Valid: true})
		case r.Method == http.MethodGet && r.URL.Path == "/runs/run-1/report":
			w.Write([]byte(`{"ok":true}`))
		case r.Method == http.MethodGet && r.URL.Path == "/runs/run-1/report.html":
			w.Write([]byte(`<html>ok</html>`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()
	client, err := New(srv.URL, "tok")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateRun(context.Background(), CreateRunRequest{TestPlan: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if seenAuth != "Bearer tok" {
		t.Fatalf("auth = %q", seenAuth)
	}
	if run, err := client.GetRun(context.Background(), "run-1"); err != nil || run.Live.RPS != 10 {
		t.Fatalf("GetRun = %#v, %v", run, err)
	}
	if list, err := client.ListRuns(context.Background(), 20, "running"); err != nil || list.Total != 1 {
		t.Fatalf("ListRuns = %#v, %v", list, err)
	}
	if stop, err := client.StopRun(context.Background(), "run-1"); err != nil || stop.Status != "DRAINING" {
		t.Fatalf("StopRun = %#v, %v", stop, err)
	}
	if validation, err := client.Validate(context.Background(), CreateRunRequest{TestPlan: json.RawMessage(`{}`)}); err != nil || !validation.Valid {
		t.Fatalf("Validate = %#v, %v", validation, err)
	}
	if body, err := client.ReportJSON(context.Background(), "run-1"); err != nil || string(body) != `{"ok":true}` {
		t.Fatalf("ReportJSON = %s, %v", body, err)
	}
	if body, err := client.ReportHTML(context.Background(), "run-1"); err != nil || string(body) != `<html>ok</html>` {
		t.Fatalf("ReportHTML = %s, %v", body, err)
	}
}

func TestClientErrorsAndValidation422(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/validate" {
			write(t, w, http.StatusUnprocessableEntity, ValidationResponse{Valid: false, Errors: []ValidationError{{Field: "name", Message: "required"}}})
			return
		}
		write(t, w, http.StatusNotFound, model.ErrorBody{Error: model.APIError{Code: "not_found", Message: "run not found"}})
	}))
	defer srv.Close()
	client, err := New(srv.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	validation, err := client.Validate(context.Background(), CreateRunRequest{TestPlan: json.RawMessage(`{}`)})
	if err != nil || validation.Valid || validation.Errors[0].Field != "name" {
		t.Fatalf("Validate 422 = %#v, %v", validation, err)
	}
	_, err = client.GetRun(context.Background(), "missing")
	if !IsNotFound(err) || !strings.Contains(err.Error(), "run not found") {
		t.Fatalf("not found err = %v", err)
	}
}

func TestClientStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/runs/run-1/stream" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: metrics\ndata: {\"rps\":12,\"workers\":2}\n\n")
		fmt.Fprint(w, "event: done\ndata: {\"status\":\"DONE\",\"total_requests\":10}\n\n")
	}))
	defer srv.Close()
	client, err := New(srv.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan StreamEvent, 2)
	if err := client.Stream(context.Background(), "run-1", events); err != nil {
		t.Fatal(err)
	}
	close(events)
	first := <-events
	second := <-events
	if first.Type != "metrics" || first.Metrics.RPS != 12 || second.Type != "done" || second.Done.Status != "DONE" {
		t.Fatalf("events = %#v %#v", first, second)
	}
}

func write(t *testing.T, w http.ResponseWriter, status int, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}
