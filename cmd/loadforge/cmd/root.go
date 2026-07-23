package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/vatsalchaudhary/loadforge/internal/cliclient"
)

type CLI struct {
	out      io.Writer
	errOut   io.Writer
	apiURL   string
	token    string
	output   string
	openFile func(string) error
}

func Execute() {
	if err := NewRootCommand(os.Stdout, os.Stderr).Execute(); err != nil {
		fmt.Fprintln(os.Stderr, humanError(err))
		os.Exit(exitCode(err))
	}
}

func NewRootCommand(out, errOut io.Writer) *cobra.Command {
	cli := &CLI{out: out, errOut: errOut, openFile: openPath}
	root := &cobra.Command{
		Use:           "loadforge",
		Short:         "LoadForge CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&cli.apiURL, "api-url", "", "API server base URL")
	root.PersistentFlags().StringVar(&cli.token, "token", "", "Bearer token")
	root.PersistentFlags().StringVar(&cli.output, "output", "", "output format for supported commands: json")
	root.PersistentPreRunE = func(_ *cobra.Command, _ []string) error {
		if cli.output != "" && cli.output != "json" {
			return failure("--output supports only json")
		}
		cfg, err := loadConfig()
		if err != nil {
			return err
		}
		if cli.apiURL == "" {
			cli.apiURL = cfg.APIURL
		}
		if cli.token == "" {
			cli.token = cfg.Token
		}
		return nil
	}
	root.AddCommand(
		newRunCommand(cli),
		newStatusCommand(cli),
		newStopCommand(cli),
		newListCommand(cli),
		newValidateCommand(cli),
		newReportCommand(cli),
		newConfigCommand(cli),
		newWorkerCommand(cli),
		newDashboardCommand(cli),
	)
	return root
}

func (c *CLI) client() (*cliclient.Client, error) {
	return cliclient.New(c.apiURL, c.token)
}

func printJSON(w io.Writer, value any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func humanError(err error) string {
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return "error"
	}
	return "error: " + msg
}

type exitCoder interface{ ExitCode() int }

type commandError struct {
	message string
	code    int
}

func (e commandError) Error() string { return e.message }
func (e commandError) ExitCode() int { return e.code }
func failure(message string) error   { return commandError{message: message, code: 1} }
func notFound(message string) error  { return commandError{message: message, code: 1} }
func ciFailed(message string) error  { return commandError{message: message, code: 1} }
func exitCode(err error) int {
	if coder, ok := err.(exitCoder); ok {
		return coder.ExitCode()
	}
	return 1
}

func ctx() context.Context {
	return context.Background()
}
