package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newWorkerCommand(cli *CLI) *cobra.Command {
	root := &cobra.Command{Use: "worker", Short: "Worker operations"}
	root.AddCommand(&cobra.Command{
		Use:   "logs <run-id>",
		Short: "Show worker logs",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, _ []string) error {
			fmt.Fprintln(cli.out, "worker logs are not yet implemented: worker log aggregation is outside Milestone 5 scope")
			return nil
		},
	})
	return root
}

func newDashboardCommand(cli *CLI) *cobra.Command {
	return &cobra.Command{
		Use:   "dashboard",
		Short: "Open the web dashboard",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			fmt.Fprintln(cli.out, "dashboard is not yet implemented: the web dashboard is Milestone 10 scope")
			return nil
		},
	}
}
