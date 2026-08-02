package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/vatsalchaudhary/loadforge/cmd/loadforge/render"
	"github.com/vatsalchaudhary/loadforge/internal/cliclient"
)

func newRunCommand(cli *CLI) *cobra.Command {
	var ci bool
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "run <test-plan.yaml>",
		Short: "Start a load test run",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			plan, raw, err := loadPlanJSON(args[0])
			if err != nil {
				return err
			}
			client, err := cli.client()
			if err != nil {
				return err
			}
			req := cliclient.CreateRunRequest{TestPlan: raw, AllowInternal: plan.Target.AllowInternal}
			if dryRun {
				validation, err := client.Validate(ctx(), req)
				if err != nil {
					return err
				}
				if !validation.Valid {
					printValidationErrors(cli.errOut, validation.Errors)
					return failure("test plan is invalid")
				}
				points, err := profilePreview(plan)
				if err != nil {
					return err
				}
				fmt.Fprintf(cli.out, "✓ valid\nWould start run %q against %s\n", plan.Name, plan.Target.BaseURL)
				fmt.Fprintf(cli.out, "Load profile: %s\n", plan.LoadProfile.Type)
				for _, point := range points {
					fmt.Fprintf(cli.out, "  t+%s: %d workers\n", point.At, point.Workers)
				}
				return nil
			}
			created, err := client.CreateRun(ctx(), req)
			if err != nil {
				return err
			}
			fmt.Fprintf(cli.out, "Run: %s (%s)\n", created.RunID, created.Status)
			if ci {
				return waitCI(cli, client, created.RunID)
			}
			return watchRun(cli, client, created.RunID)
		},
	}
	cmd.Flags().BoolVar(&ci, "ci", false, "wait for completion and use CI-friendly exit codes")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate and preview the run without creating it")
	return cmd
}

func waitCI(cli *CLI, client *cliclient.Client, runID string) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		run, err := client.GetRun(ctx(), runID)
		if err != nil {
			return err
		}
		switch run.Status {
		case "DONE":
			fmt.Fprintf(cli.out, "DONE: %s\n", render.Summary(run))
			// TODO(Milestone 7): exit based on server-side threshold breach results.
			return nil
		case "FAILED":
			fmt.Fprintf(cli.out, "FAILED: %s\n", render.Summary(run))
			return ciFailed("run failed")
		case "THRESHOLD_BREACHED":
			fmt.Fprintf(cli.out, "THRESHOLD_BREACHED: %s\n", render.Summary(run))
			return ciFailed("threshold breached")
		}
		<-ticker.C
	}
}

func watchRun(cli *CLI, client *cliclient.Client, runID string) error {
	events := make(chan cliclient.StreamEvent, 16)
	errc := make(chan error, 1)
	go func() {
		errc <- client.Stream(ctx(), runID, events)
		close(events)
	}()
	renderEvery := 5 * time.Second
	nextRender := time.Now()
	var latest *cliclient.MetricsEvent
	for {
		select {
		case err := <-errc:
			if err != nil {
				return err
			}
			for event := range events {
				done, err := handleStreamEvent(cli, runID, event, &latest, &nextRender, renderEvery)
				if err != nil || done {
					return err
				}
			}
			return nil
		case event, ok := <-events:
			if !ok {
				return nil
			}
			done, err := handleStreamEvent(cli, runID, event, &latest, &nextRender, renderEvery)
			if err != nil || done {
				return err
			}
		}
	}
}

func handleStreamEvent(cli *CLI, runID string, event cliclient.StreamEvent, latest **cliclient.MetricsEvent, nextRender *time.Time, renderEvery time.Duration) (bool, error) {
	switch event.Type {
	case "metrics":
		metric := event.Metrics
		*latest = &metric
		if time.Now().After(*nextRender) {
			if err := render.Metrics(cli.out, **latest); err != nil {
				return false, err
			}
			*nextRender = time.Now().Add(renderEvery)
		}
	case "done":
		if *latest != nil {
			if err := render.Metrics(cli.out, **latest); err != nil {
				return false, err
			}
		}
		fmt.Fprintf(cli.out, "%s: requests=%d errors=%d p99=%.0fms\n", event.Done.Status, event.Done.TotalRequests, event.Done.TotalErrors, event.Done.P99MS)
		fmt.Fprintf(cli.out, "→ Report: loadforge report %s\n", runID)
		return true, nil
	}
	return false, nil
}
