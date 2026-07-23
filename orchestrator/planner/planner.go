package planner

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/vatsalchaudhary/loadforge/pkg/testplan"
	"gopkg.in/yaml.v3"
)

type ValidationError struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	parts := make([]string, 0, len(e))
	for _, item := range e {
		parts = append(parts, item.Field+": "+item.Reason)
	}
	return strings.Join(parts, "; ")
}

func ParseYAML(data []byte) (testplan.TestPlan, error) {
	var plan testplan.TestPlan
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&plan); err != nil {
		return plan, yamlError(err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return plan, ValidationErrors{{Field: "$", Reason: "document must contain exactly one YAML object"}}
	}
	if errs := validate(plan); len(errs) > 0 {
		return plan, errs
	}
	return plan, nil
}

func yamlError(err error) error {
	var typeErr *yaml.TypeError
	if errors.As(err, &typeErr) {
		out := make(ValidationErrors, 0, len(typeErr.Errors))
		for _, msg := range typeErr.Errors {
			out = append(out, ValidationError{Field: "$", Reason: msg})
		}
		return out
	}
	return ValidationErrors{{Field: "$", Reason: err.Error()}}
}

func validate(plan testplan.TestPlan) ValidationErrors {
	var errs ValidationErrors
	required := func(field, val string) {
		if strings.TrimSpace(val) == "" {
			errs = append(errs, ValidationError{Field: field, Reason: "required"})
		}
	}
	positive := func(field string, val int) {
		if val <= 0 {
			errs = append(errs, ValidationError{Field: field, Reason: "must be positive"})
		}
	}
	duration := func(field, val string, requiredField bool) {
		if strings.TrimSpace(val) == "" {
			if requiredField {
				errs = append(errs, ValidationError{Field: field, Reason: "required"})
			}
			return
		}
		if _, err := time.ParseDuration(val); err != nil {
			errs = append(errs, ValidationError{Field: field, Reason: "malformed duration"})
		}
	}

	required("name", plan.Name)
	required("version", plan.Version)
	required("target.base_url", plan.Target.BaseURL)
	duration("target.timeout", plan.Target.Timeout, false)
	required("load_profile.type", plan.LoadProfile.Type)
	switch plan.LoadProfile.Type {
	case "step_ramp":
		positive("load_profile.initial_workers", plan.LoadProfile.InitialWorkers)
		positive("load_profile.max_workers", plan.LoadProfile.MaxWorkers)
		positive("load_profile.step_size", plan.LoadProfile.StepSize)
		duration("load_profile.step_interval", plan.LoadProfile.StepInterval, true)
		if plan.LoadProfile.MaxWorkers < plan.LoadProfile.InitialWorkers {
			errs = append(errs, ValidationError{Field: "load_profile.max_workers", Reason: "must be greater than or equal to initial_workers"})
		}
	case "constant", "spike", "soak", "stress":
		positive("load_profile.initial_workers", plan.LoadProfile.InitialWorkers)
		if plan.LoadProfile.MaxWorkers != 0 {
			positive("load_profile.max_workers", plan.LoadProfile.MaxWorkers)
		}
	default:
		if strings.TrimSpace(plan.LoadProfile.Type) != "" {
			errs = append(errs, ValidationError{Field: "load_profile.type", Reason: fmt.Sprintf("unsupported profile %q", plan.LoadProfile.Type)})
		}
	}
	duration("load_profile.hold_duration", plan.LoadProfile.HoldDuration, false)
	positive("workers.virtual_users_per_worker", plan.Workers.VirtualUsersPerWorker)
	if len(plan.Scenarios) == 0 {
		errs = append(errs, ValidationError{Field: "scenarios", Reason: "at least one scenario is required"})
	}
	for i, sc := range plan.Scenarios {
		prefix := fmt.Sprintf("scenarios[%d]", i)
		required(prefix+".name", sc.Name)
		if sc.Weight <= 0 {
			errs = append(errs, ValidationError{Field: prefix + ".weight", Reason: "must be positive"})
		}
		if len(sc.Steps) == 0 {
			errs = append(errs, ValidationError{Field: prefix + ".steps", Reason: "at least one step is required"})
		}
		for j, step := range sc.Steps {
			stepPrefix := fmt.Sprintf("%s.steps[%d]", prefix, j)
			required(stepPrefix+".name", step.Name)
			required(stepPrefix+".method", step.Method)
			required(stepPrefix+".path", step.Path)
			duration(stepPrefix+".think_time", step.ThinkTime, false)
		}
	}
	return errs
}
