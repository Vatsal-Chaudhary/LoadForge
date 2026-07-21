package scenario

import (
	"context"
	"net/http"
	"testing"

	"github.com/vatsalchaudhary/loadforge/pkg/testplan"
	lfclient "github.com/vatsalchaudhary/loadforge/worker/client"
	"github.com/vatsalchaudhary/loadforge/worker/reporter"
)

type fakeHTTPClient struct {
	t        *testing.T
	requests []lfclient.Request
}

func (f *fakeHTTPClient) Do(_ context.Context, req lfclient.Request) (*lfclient.Response, error) {
	f.requests = append(f.requests, req)
	switch len(f.requests) {
	case 1:
		return &lfclient.Response{
			StatusCode: http.StatusOK,
			Headers:    http.Header{"X-Session": []string{"session-123"}},
			Body:       []byte(`{"auth":{"token":"token-abc"},"id":42}`),
			BytesRecv:  39,
		}, nil
	case 2:
		if got, want := lfclient.Substitute(req.Headers["Authorization"], req.Vars), "Bearer token-abc"; got != want {
			f.t.Fatalf("authorization header = %q, want %q", got, want)
		}
		if got, want := lfclient.Substitute(req.Headers["X-Session"], req.Vars), "session-123"; got != want {
			f.t.Fatalf("session header = %q, want %q", got, want)
		}
		return &lfclient.Response{StatusCode: http.StatusCreated, Body: []byte(`ok`), BytesRecv: 2}, nil
	default:
		f.t.Fatalf("unexpected request count %d", len(f.requests))
		return nil, nil
	}
}

type sampleRecorder struct {
	samples []reporter.RequestSample
}

func (r *sampleRecorder) Record(sample reporter.RequestSample) {
	r.samples = append(r.samples, sample)
}

func TestRunOnceExecutesStepsAndSubstitutesExtractions(t *testing.T) {
	httpClient := &fakeHTTPClient{t: t}
	rec := &sampleRecorder{}
	runner, err := NewRunner(Config{
		Plan: testplan.TestPlan{
			Target: testplan.Target{BaseURL: "https://example.test"},
			Scenarios: []testplan.Scenario{{
				Name: "auth flow",
				Steps: []testplan.Step{
					{
						Name:   "login",
						Method: "POST",
						Path:   "/login",
						Extract: []testplan.Extraction{
							{Name: "token", From: "response_body", JSONPath: "$.auth.token"},
							{Name: "session", From: "header", Header: "X-Session"},
						},
					},
					{
						Name:   "create",
						Method: "POST",
						Path:   "/items",
						Headers: map[string]string{
							"Authorization": "Bearer {{ token }}",
							"X-Session":     "{{session}}",
						},
						Body: `{"id":"{{token}}"}`,
					},
				},
			}},
		},
		Client:   httpClient,
		Recorder: rec,
	})
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	if err := runner.RunOnce(context.Background(), nil); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if got, want := len(httpClient.requests), 2; got != want {
		t.Fatalf("requests = %d, want %d", got, want)
	}
	if got, want := len(rec.samples), 2; got != want {
		t.Fatalf("samples = %d, want %d", got, want)
	}
	if rec.samples[0].Endpoint != "https://example.test/login" {
		t.Fatalf("first endpoint = %q", rec.samples[0].Endpoint)
	}
}

func TestApplyExtractions(t *testing.T) {
	tests := []struct {
		name  string
		rules []testplan.Extraction
		want  map[string]string
	}{
		{
			name:  "json string",
			rules: []testplan.Extraction{{Name: "token", From: "response_body", JSONPath: "$.auth.token"}},
			want:  map[string]string{"token": "abc"},
		},
		{
			name:  "json number",
			rules: []testplan.Extraction{{Name: "id", From: "body", JSONPath: "$.id"}},
			want:  map[string]string{"id": "42"},
		},
		{
			name:  "header",
			rules: []testplan.Extraction{{Name: "request_id", From: "header", Header: "X-Request-ID"}},
			want:  map[string]string{"request_id": "req-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := map[string]string{}
			headers := http.Header{}
			headers.Set("X-Request-ID", "req-1")
			err := ApplyExtractions(tt.rules, &lfclient.Response{
				Headers: headers,
				Body:    []byte(`{"auth":{"token":"abc"},"id":42}`),
			}, vars)
			if err != nil {
				t.Fatalf("ApplyExtractions() error = %v", err)
			}
			for k, want := range tt.want {
				if got := vars[k]; got != want {
					t.Fatalf("vars[%q] = %q, want %q", k, got, want)
				}
			}
		})
	}
}
