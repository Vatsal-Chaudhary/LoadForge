package planner

import "testing"

const validPlan = `
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
  step_interval: 5s
workers:
  virtual_users_per_worker: 10
  resources:
    cpu: 200m
    memory: 256Mi
scenarios:
  - name: browse
    weight: 1
    steps:
      - name: home
        method: GET
        path: /
        think_time: 100ms
`

func TestParseYAMLValid(t *testing.T) {
	plan, err := ParseYAML([]byte(validPlan))
	if err != nil {
		t.Fatalf("ParseYAML returned error: %v", err)
	}
	if plan.LoadProfile.InitialWorkers != 1 {
		t.Fatalf("initial workers = %d, want 1", plan.LoadProfile.InitialWorkers)
	}
}

func TestParseYAMLValidationRejections(t *testing.T) {
	tests := []struct {
		name      string
		yaml      string
		wantField string
	}{
		{
			name: "unknown field",
			yaml: `
name: checkout
version: "1"
extra: true
target: {base_url: https://example.com}
load_profile: {type: step_ramp, initial_workers: 1, max_workers: 2, step_size: 1, step_interval: 1s}
workers: {virtual_users_per_worker: 1}
scenarios: [{name: s, weight: 1, steps: [{name: x, method: GET, path: /}]}]
`,
			wantField: "$",
		},
		{name: "missing required", yaml: `
version: "1"
target: {base_url: https://example.com}
load_profile: {type: step_ramp, initial_workers: 1, max_workers: 2, step_size: 1, step_interval: 1s}
workers: {virtual_users_per_worker: 1}
scenarios: [{name: s, weight: 1, steps: [{name: x, method: GET, path: /}]}]
`, wantField: "name"},
		{name: "malformed duration", yaml: `
name: checkout
version: "1"
target: {base_url: https://example.com, timeout: forever}
load_profile: {type: step_ramp, initial_workers: 1, max_workers: 2, step_size: 1, step_interval: 1s}
workers: {virtual_users_per_worker: 1}
scenarios: [{name: s, weight: 1, steps: [{name: x, method: GET, path: /}]}]
`, wantField: "target.timeout"},
		{name: "non positive worker count", yaml: `
name: checkout
version: "1"
target: {base_url: https://example.com}
load_profile: {type: step_ramp, initial_workers: 0, max_workers: 2, step_size: 1, step_interval: 1s}
workers: {virtual_users_per_worker: 1}
scenarios: [{name: s, weight: 1, steps: [{name: x, method: GET, path: /}]}]
`, wantField: "load_profile.initial_workers"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseYAML([]byte(tt.yaml))
			if err == nil {
				t.Fatal("expected validation error")
			}
			errs, ok := err.(ValidationErrors)
			if !ok {
				t.Fatalf("error type = %T, want ValidationErrors", err)
			}
			for _, item := range errs {
				if item.Field == tt.wantField {
					return
				}
			}
			t.Fatalf("fields = %v, want %s", errs, tt.wantField)
		})
	}
}
