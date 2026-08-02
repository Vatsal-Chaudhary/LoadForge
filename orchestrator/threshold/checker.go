package threshold

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/vatsalchaudhary/loadforge/orchestrator/run"
	"github.com/vatsalchaudhary/loadforge/pkg/testplan"
)

const DefaultInterval = 10 * time.Second

type Metrics struct {
	RPS       float64
	P95MS     float64
	P99MS     float64
	ErrorRate float64

	HasRPS       bool
	HasP95MS     bool
	HasP99MS     bool
	HasErrorRate bool
}

type MetricsReader interface {
	ReadMetrics(ctx context.Context, runID string) (Metrics, error)
}

type Store interface {
	RecordThresholdEvent(ctx context.Context, event Event) error
	UpdateThresholdResults(ctx context.Context, runID string, results Results) error
}

type Event struct {
	RunID     string    `json:"run_id"`
	Name      string    `json:"name"`
	Limit     float64   `json:"limit"`
	Value     float64   `json:"value"`
	Operator  string    `json:"operator"`
	CreatedAt time.Time `json:"created_at"`
}

type Result struct {
	Configured bool       `json:"configured"`
	Passed     bool       `json:"passed"`
	Limit      float64    `json:"limit,omitempty"`
	Value      float64    `json:"value,omitempty"`
	Operator   string     `json:"operator,omitempty"`
	BreachedAt *time.Time `json:"breached_at,omitempty"`
}

type Results struct {
	Passed    bool              `json:"passed"`
	OnBreach  string            `json:"on_breach,omitempty"`
	CheckedAt time.Time         `json:"checked_at"`
	Checks    map[string]Result `json:"checks"`
}

type Breach struct {
	Name     string
	Limit    float64
	Value    float64
	Operator string
}

type TransitionFunc func(context.Context, run.State, string) error

type Config struct {
	RunID      string
	Thresholds *testplan.Thresholds
	Reader     MetricsReader
	Store      Store
	Interval   time.Duration
	Logger     *slog.Logger
	Now        func() time.Time

	GetState   func() run.State
	SetState   func(run.State)
	Transition TransitionFunc
	OnStop     func(context.Context) error
}

type Checker struct {
	cfg Config
}

func New(cfg Config) *Checker {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Checker{cfg: cfg}
}

func (c *Checker) Run(ctx context.Context) {
	if c.cfg.Thresholds == nil || c.cfg.Reader == nil {
		return
	}
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stop, err := c.CheckOnce(ctx)
			if err != nil {
				c.cfg.Logger.Warn("threshold check failed", "run_id", c.cfg.RunID, "error", err)
			}
			if stop {
				return
			}
		}
	}
}

func (c *Checker) CheckOnce(ctx context.Context) (bool, error) {
	if c.cfg.Thresholds == nil || c.cfg.Reader == nil {
		return true, nil
	}
	if c.cfg.GetState != nil && !activeState(c.cfg.GetState()) {
		return true, nil
	}
	metrics, err := c.cfg.Reader.ReadMetrics(ctx, c.cfg.RunID)
	if err != nil {
		return false, err
	}
	now := c.cfg.Now().UTC()
	results, breaches := Evaluate(c.cfg.Thresholds, metrics, now)
	if c.cfg.Store != nil {
		if err := c.cfg.Store.UpdateThresholdResults(ctx, c.cfg.RunID, results); err != nil {
			return false, err
		}
	}
	if len(breaches) == 0 {
		return false, nil
	}
	for _, breach := range breaches {
		c.cfg.Logger.Warn("threshold breached",
			"run_id", c.cfg.RunID,
			"threshold", breach.Name,
			"operator", breach.Operator,
			"limit", breach.Limit,
			"value", breach.Value,
			"timestamp", now,
		)
		if c.cfg.Store != nil {
			if err := c.cfg.Store.RecordThresholdEvent(ctx, Event{
				RunID: c.cfg.RunID, Name: breach.Name, Limit: breach.Limit,
				Value: breach.Value, Operator: breach.Operator, CreatedAt: now,
			}); err != nil {
				return false, err
			}
		}
	}
	reason := breachReason(breaches)
	if c.cfg.Transition != nil {
		if err := c.cfg.Transition(ctx, run.StateThresholdBreached, reason); err != nil {
			return false, err
		}
	}
	if c.cfg.SetState != nil {
		c.cfg.SetState(run.StateThresholdBreached)
	}
	if onBreach(c.cfg.Thresholds) == "stop" {
		if c.cfg.OnStop != nil {
			if err := c.cfg.OnStop(ctx); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	return true, nil
}

func Evaluate(thresholds *testplan.Thresholds, metrics Metrics, now time.Time) (Results, []Breach) {
	results := Results{
		Passed:    true,
		OnBreach:  onBreach(thresholds),
		CheckedAt: now.UTC(),
		Checks:    make(map[string]Result),
	}
	var breaches []Breach
	add := func(name string, configured bool, limit, value float64, op string, breached bool) {
		result := Result{Configured: configured, Passed: true, Limit: limit, Value: value, Operator: op}
		if configured && breached {
			result.Passed = false
			breachedAt := now.UTC()
			result.BreachedAt = &breachedAt
			results.Passed = false
			breaches = append(breaches, Breach{Name: name, Limit: limit, Value: value, Operator: op})
		}
		results.Checks[name] = result
	}
	if thresholds.P95LatencyMs != nil {
		add("p95_latency_ms", true, *thresholds.P95LatencyMs, metrics.P95MS, "<=", metrics.HasP95MS && metrics.P95MS > *thresholds.P95LatencyMs)
	}
	if thresholds.ErrorRatePercent != nil {
		value := metrics.ErrorRate * 100
		add("error_rate_percent", true, *thresholds.ErrorRatePercent, value, "<=", metrics.HasErrorRate && value > *thresholds.ErrorRatePercent)
	}
	if thresholds.MinRPS != nil {
		add("min_rps", true, *thresholds.MinRPS, metrics.RPS, ">=", metrics.HasRPS && metrics.RPS < *thresholds.MinRPS)
	}
	if thresholds.MaxP99LatencyMs != nil {
		add("max_p99_latency_ms", true, *thresholds.MaxP99LatencyMs, metrics.P99MS, "<=", metrics.HasP99MS && metrics.P99MS > *thresholds.MaxP99LatencyMs)
	}
	return results, breaches
}

func activeState(state run.State) bool {
	return state == run.StateRunning || state == run.StateScaling
}

func onBreach(thresholds *testplan.Thresholds) string {
	if thresholds == nil || strings.EqualFold(thresholds.OnBreach, "") {
		return "report"
	}
	return strings.ToLower(thresholds.OnBreach)
}

func breachReason(breaches []Breach) string {
	parts := make([]string, 0, len(breaches))
	for _, breach := range breaches {
		parts = append(parts, fmt.Sprintf("%s %.3f %s %.3f", breach.Name, breach.Value, breach.Operator, breach.Limit))
	}
	return "threshold breached: " + strings.Join(parts, "; ")
}
