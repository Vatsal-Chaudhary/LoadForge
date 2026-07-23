package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newConfigCommand(cli *CLI) *cobra.Command {
	root := &cobra.Command{Use: "config", Short: "Manage LoadForge CLI config"}
	root.AddCommand(&cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set config value",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			switch args[0] {
			case "api_url":
				cfg.APIURL = args[1]
			case "token":
				cfg.Token = args[1]
			default:
				return failure("supported keys: api_url, token")
			}
			if err := writeConfig(cfg); err != nil {
				return err
			}
			fmt.Fprintf(cli.out, "set %s\n", args[0])
			return nil
		},
	})
	root.AddCommand(&cobra.Command{
		Use:   "set-key <token>",
		Short: "Store API bearer token",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			cfg.Token = args[0]
			if err := writeConfig(cfg); err != nil {
				return err
			}
			fmt.Fprintf(cli.out, "set token %s\n", maskToken(args[0]))
			return nil
		},
	})
	root.AddCommand(&cobra.Command{
		Use:   "get",
		Short: "Print config",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			fmt.Fprintf(cli.out, "api_url: %s\n", cfg.APIURL)
			fmt.Fprintf(cli.out, "token: %s\n", maskToken(cfg.Token))
			return nil
		},
	})
	return root
}
