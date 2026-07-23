package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vatsalchaudhary/loadforge/internal/cliclient"
)

func newValidateCommand(cli *CLI) *cobra.Command {
	return &cobra.Command{
		Use:   "validate <test-plan.yaml>",
		Short: "Validate a test plan",
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
			response, err := client.Validate(ctx(), cliclient.CreateRunRequest{TestPlan: raw, AllowInternal: plan.Target.AllowInternal})
			if err != nil {
				return err
			}
			if cli.output == "json" {
				return printJSON(cli.out, response)
			}
			if response.Valid {
				fmt.Fprintln(cli.out, "✓ valid")
				return nil
			}
			printValidationErrors(cli.out, response.Errors)
			return failure("test plan is invalid")
		},
	}
}

func printValidationErrors(w interface{ Write([]byte) (int, error) }, errors []cliclient.ValidationError) {
	fmt.Fprintln(w, "invalid test plan:")
	for _, item := range errors {
		fmt.Fprintf(w, "  - %s: %s\n", item.Field, item.Message)
	}
}
