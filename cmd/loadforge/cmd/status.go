package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vatsalchaudhary/loadforge/cmd/loadforge/render"
	"github.com/vatsalchaudhary/loadforge/internal/cliclient"
)

func newStatusCommand(cli *CLI) *cobra.Command {
	return &cobra.Command{
		Use:   "status <run-id>",
		Short: "Show run status",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			client, err := cli.client()
			if err != nil {
				return err
			}
			run, err := client.GetRun(ctx(), args[0])
			if cliclient.IsNotFound(err) {
				return notFound("run not found")
			}
			if err != nil {
				return err
			}
			if cli.output == "json" {
				return printJSON(cli.out, run)
			}
			fmt.Fprintf(cli.out, "%s: %s\n", run.RunID, run.Status)
			if run.Status == "RUNNING" && run.Live != nil {
				fmt.Fprintf(cli.out, "rps=%.1f p95=%.0fms error_rate=%.2f%% workers=%d\n", run.Live.RPS, run.Live.P95MS, run.Live.ErrorRate*100, run.ActiveWorkers)
				return nil
			}
			fmt.Fprintln(cli.out, render.Summary(run))
			return nil
		},
	}
}
