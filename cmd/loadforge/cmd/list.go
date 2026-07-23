package cmd

import (
	"github.com/spf13/cobra"
	"github.com/vatsalchaudhary/loadforge/cmd/loadforge/render"
)

func newListCommand(cli *CLI) *cobra.Command {
	var status string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List recent runs",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			client, err := cli.client()
			if err != nil {
				return err
			}
			response, err := client.ListRuns(ctx(), 20, status)
			if err != nil {
				return err
			}
			if cli.output == "json" {
				return printJSON(cli.out, response)
			}
			return render.Runs(cli.out, response.Runs)
		},
	}
	cmd.Flags().StringVar(&status, "status", "", "filter by run status")
	return cmd
}
