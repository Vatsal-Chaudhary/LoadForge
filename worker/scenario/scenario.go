package scenario

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/vatsalchaudhary/loadforge/pkg/testplan"
	lfclient "github.com/vatsalchaudhary/loadforge/worker/client"
	"github.com/vatsalchaudhary/loadforge/worker/reporter"
)

var requestsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "loadforge_worker_requests_in_flight",
	Help: "Current number of target requests in flight from this worker.",
})

type HTTPDoer interface {
	Do(ctx context.Context, req lfclient.Request) (*lfclient.Response, error)
}

type SampleRecorder interface {
	Record(sample reporter.RequestSample)
}

type Runner struct {
	baseURL string
	steps   []testplan.Step
	client  HTTPDoer
	rec     SampleRecorder
	log     *slog.Logger

	totalRequests atomic.Int64
	totalErrors   atomic.Int64
}

type Config struct {
	Plan     testplan.TestPlan
	Client   HTTPDoer
	Recorder SampleRecorder
	Logger   *slog.Logger
}

func NewRunner(cfg Config) (*Runner, error) {
	if cfg.Client == nil {
		return nil, errors.New("scenario client is required")
	}
	if cfg.Recorder == nil {
		return nil, errors.New("scenario recorder is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if len(cfg.Plan.Scenarios) == 0 {
		return nil, errors.New("test plan has no scenarios")
	}

	selected := cfg.Plan.Scenarios[0]
	if len(selected.Steps) == 0 {
		return nil, fmt.Errorf("scenario %q has no steps", selected.Name)
	}

	return &Runner{
		baseURL: cfg.Plan.Target.BaseURL,
		steps:   selected.Steps,
		client:  cfg.Client,
		rec:     cfg.Recorder,
		log:     cfg.Logger,
	}, nil
}

func (r *Runner) RunOnce(ctx context.Context, vars map[string]string) error {
	if vars == nil {
		vars = map[string]string{}
	}
	for _, step := range r.steps {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.runStep(step, vars); err != nil {
			r.log.Warn("scenario step failed", "step", step.Name, "error", err)
		}
		if step.ThinkTime != "" {
			thinkTime, err := time.ParseDuration(step.ThinkTime)
			if err != nil {
				r.log.Warn("invalid think time", "step", step.Name, "think_time", step.ThinkTime, "error", err)
				continue
			}
			timer := time.NewTimer(thinkTime)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return ctx.Err()
			}
		}
	}
	return nil
}

func (r *Runner) Stats() (totalRequests int64, totalErrors int64) {
	return r.totalRequests.Load(), r.totalErrors.Load()
}

func (r *Runner) runStep(step testplan.Step, vars map[string]string) error {
	endpoint := lfclient.JoinURL(r.baseURL, step.Path)
	req := lfclient.Request{
		Method:  step.Method,
		URL:     endpoint,
		Headers: step.Headers,
		Body:    step.Body,
		Vars:    vars,
	}

	requestsInFlight.Inc()
	start := time.Now()
	resp, err := r.client.Do(context.Background(), req)
	latency := time.Since(start)
	requestsInFlight.Dec()

	sample := reporter.RequestSample{
		Endpoint:  endpoint,
		Method:    strings.ToUpper(step.Method),
		LatencyMs: float64(latency.Microseconds()) / 1000.0,
	}
	if sample.Method == "" {
		sample.Method = "GET"
	}
	if resp != nil {
		sample.StatusCode = resp.StatusCode
		sample.BytesRecv = resp.BytesRecv
	}
	if err != nil {
		sample.Error = err.Error()
		r.totalErrors.Add(1)
	}
	r.totalRequests.Add(1)
	r.rec.Record(sample)

	if err != nil {
		return err
	}
	if err := ApplyExtractions(step.Extract, resp, vars); err != nil {
		return err
	}
	return nil
}

func ApplyExtractions(rules []testplan.Extraction, resp *lfclient.Response, vars map[string]string) error {
	if resp == nil || len(rules) == 0 {
		return nil
	}
	for _, rule := range rules {
		if rule.Name == "" {
			continue
		}
		switch strings.ToLower(rule.From) {
		case "header":
			if rule.Header == "" {
				return fmt.Errorf("extraction %q missing header", rule.Name)
			}
			vars[rule.Name] = resp.Headers.Get(rule.Header)
		case "response_body", "body", "":
			val, err := extractJSONPath(resp.Body, rule.JSONPath)
			if err != nil {
				return fmt.Errorf("extraction %q failed: %w", rule.Name, err)
			}
			vars[rule.Name] = val
		default:
			return fmt.Errorf("unsupported extraction source %q", rule.From)
		}
	}
	return nil
}

func extractJSONPath(body []byte, path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("jsonpath is required")
	}
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")
	if path == "" {
		return string(body), nil
	}

	var current any
	if err := json.Unmarshal(body, &current); err != nil {
		return "", err
	}

	for _, part := range strings.Split(path, ".") {
		obj, ok := current.(map[string]any)
		if !ok {
			return "", fmt.Errorf("path %q is not an object", part)
		}
		current, ok = obj[part]
		if !ok {
			return "", fmt.Errorf("path %q not found", part)
		}
	}

	switch v := current.(type) {
	case string:
		return v, nil
	case float64, bool, nil:
		return fmt.Sprint(v), nil
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	}
}
