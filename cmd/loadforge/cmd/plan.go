package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/vatsalchaudhary/loadforge/orchestrator/planner"
	"github.com/vatsalchaudhary/loadforge/pkg/testplan"
)

func loadPlanJSON(file string) (testplan.TestPlan, json.RawMessage, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return testplan.TestPlan{}, nil, err
	}
	plan, err := planner.ParseYAML(data)
	if err != nil {
		return testplan.TestPlan{}, nil, err
	}
	raw, err := json.Marshal(plan)
	return plan, raw, err
}

type profilePoint struct {
	At      time.Duration `json:"at"`
	Workers int           `json:"workers"`
}

func profilePreview(plan testplan.TestPlan) ([]profilePoint, error) {
	profile := plan.LoadProfile
	switch profile.Type {
	case "step_ramp":
		step, err := time.ParseDuration(profile.StepInterval)
		if err != nil {
			return nil, err
		}
		if step <= 0 {
			return nil, fmt.Errorf("step_interval must be positive")
		}
		var out []profilePoint
		for elapsed, workers := time.Duration(0), profile.InitialWorkers; ; elapsed, workers = elapsed+step, workers+profile.StepSize {
			if profile.MaxWorkers > 0 && workers > profile.MaxWorkers {
				workers = profile.MaxWorkers
			}
			out = append(out, profilePoint{At: elapsed, Workers: workers})
			if profile.MaxWorkers == 0 || workers >= profile.MaxWorkers {
				break
			}
		}
		return out, nil
	case "constant", "spike", "soak", "stress":
		workers := profile.InitialWorkers
		if profile.Type == "spike" && profile.MaxWorkers > workers {
			return []profilePoint{{At: 0, Workers: workers}, {At: 0, Workers: profile.MaxWorkers}}, nil
		}
		return []profilePoint{{At: 0, Workers: workers}}, nil
	default:
		return nil, fmt.Errorf("unsupported profile %q", profile.Type)
	}
}
