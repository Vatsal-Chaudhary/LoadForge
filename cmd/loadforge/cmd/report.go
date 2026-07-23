package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/vatsalchaudhary/loadforge/internal/cliclient"
)

func newReportCommand(cli *CLI) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "report <run-id>",
		Short: "Open or print a final report",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			client, err := cli.client()
			if err != nil {
				return err
			}
			if asJSON {
				body, err := client.ReportJSON(ctx(), args[0])
				if cliclient.IsNotFound(err) {
					return notFound("run not found")
				}
				if err != nil {
					return err
				}
				_, err = cli.out.Write(append(body, '\n'))
				return err
			}
			body, err := client.ReportHTML(ctx(), args[0])
			if cliclient.IsNotFound(err) {
				return notFound("run not found")
			}
			if err != nil {
				return err
			}
			file, err := os.CreateTemp("", "loadforge-report-*.html")
			if err != nil {
				return err
			}
			name := file.Name()
			if _, err := file.Write(body); err != nil {
				file.Close()
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
			if err := cli.openFile(name); err != nil {
				return err
			}
			fmt.Fprintf(cli.out, "Opened report: %s\n", name)
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON report")
	return cmd
}

func openPath(file string) error {
	abs, err := filepath.Abs(file)
	if err != nil {
		return err
	}
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("open", abs).Start()
	case "windows":
		return exec.Command("cmd", "/c", "start", "", abs).Start()
	default:
		return exec.Command("xdg-open", abs).Start()
	}
}
