package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vatsalchaudhary/loadforge/internal/cliclient"
)

func newStopCommand(cli *CLI) *cobra.Command {
	return &cobra.Command{
		Use:   "stop <run-id>",
		Short: "Stop a running load test",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			client, err := cli.client()
			if err != nil {
				return err
			}
			response, err := client.StopRun(ctx(), args[0])
			if cliclient.IsNotFound(err) {
				return notFound("run not found")
			}
			if err != nil {
				return err
			}
			fmt.Fprintf(cli.out, "%s\n", response.Status)
			return nil
		},
	}
}
