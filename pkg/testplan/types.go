package testplan

type TestPlan struct {
	Name        string      `yaml:"name" json:"name"`
	Version     string      `yaml:"version" json:"version"`
	Target      Target      `yaml:"target" json:"target"`
	LoadProfile LoadProfile `yaml:"load_profile" json:"load_profile"`
	Scenarios   []Scenario  `yaml:"scenarios" json:"scenarios"`
	Workers     WorkerSpec  `yaml:"workers" json:"workers"`
	Thresholds  *Thresholds `yaml:"thresholds,omitempty" json:"thresholds,omitempty"`
}

type Target struct {
	BaseURL       string `yaml:"base_url" json:"base_url"`
	TLSSkipVerify bool   `yaml:"tls_skip_verify" json:"tls_skip_verify"`
	Timeout       string `yaml:"timeout" json:"timeout"` // e.g., "30s"
	AllowInternal bool   `yaml:"allow_internal,omitempty" json:"allow_internal,omitempty"`
}

type LoadProfile struct {
	Type           string `yaml:"type" json:"type"` // constant|step_ramp|spike|soak|stress
	InitialWorkers int    `yaml:"initial_workers" json:"initial_workers"`
	MaxWorkers     int    `yaml:"max_workers" json:"max_workers"`
	StepSize       int    `yaml:"step_size" json:"step_size"`
	StepInterval   string `yaml:"step_interval" json:"step_interval"`
	HoldDuration   string `yaml:"hold_duration" json:"hold_duration"`
	RampDown       bool   `yaml:"ramp_down" json:"ramp_down"`
}

type Scenario struct {
	Name   string  `yaml:"name" json:"name"`
	Weight float64 `yaml:"weight" json:"weight"`
	Steps  []Step  `yaml:"steps" json:"steps"`
}

type Step struct {
	Name      string            `yaml:"name" json:"name"`
	Method    string            `yaml:"method" json:"method"`
	Path      string            `yaml:"path" json:"path"`
	Headers   map[string]string `yaml:"headers" json:"headers"`
	Body      string            `yaml:"body" json:"body"`
	Extract   []Extraction      `yaml:"extract" json:"extract"`
	ThinkTime string            `yaml:"think_time" json:"think_time"`
	Assert    []Assertion       `yaml:"assert" json:"assert"`
}

type Extraction struct {
	Name     string `yaml:"name" json:"name"`
	From     string `yaml:"from" json:"from"` // response_body | header
	JSONPath string `yaml:"jsonpath" json:"jsonpath"`
	Header   string `yaml:"header" json:"header"`
}

type Assertion struct {
	Field    string `yaml:"field" json:"field"`       // status_code | latency_ms | body_contains
	Operator string `yaml:"operator" json:"operator"` // eq | lt | gt | contains
	Value    string `yaml:"value" json:"value"`
}

type WorkerSpec struct {
	Resources             ResourceSpec `yaml:"resources" json:"resources"`
	VirtualUsersPerWorker int          `yaml:"virtual_users_per_worker" json:"virtual_users_per_worker"`
}

type ResourceSpec struct {
	CPU    string `yaml:"cpu" json:"cpu"`
	Memory string `yaml:"memory" json:"memory"`
}

type Thresholds struct {
	P95LatencyMs     *float64 `yaml:"p95_latency_ms" json:"p95_latency_ms"`
	ErrorRatePercent *float64 `yaml:"error_rate_percent" json:"error_rate_percent"`
	MinRPS           *float64 `yaml:"min_rps" json:"min_rps"`
	MaxP99LatencyMs  *float64 `yaml:"max_p99_latency_ms" json:"max_p99_latency_ms"`
	OnBreach         string   `yaml:"on_breach" json:"on_breach"` // stop | report
}
