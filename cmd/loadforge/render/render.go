package render

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/vatsalchaudhary/loadforge/apiserver/model"
	"github.com/vatsalchaudhary/loadforge/internal/cliclient"
)

func Metrics(w io.Writer, metric cliclient.MetricsEvent) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "Time\tWorkers\tRPS\tp50\tp95\tp99\tErrors"); err != nil {
		return err
	}
	ts := metric.TS.Local().Format("15:04:05")
	if metric.TS.IsZero() {
		ts = "-"
	}
	if _, err := fmt.Fprintf(tw, "%s\t%d\t%.1f\t%.0fms\t%.0fms\t%.0fms\t%.2f%%\n",
		ts, metric.Workers, metric.RPS, metric.P50, metric.P95, metric.P99, metric.Errors*100); err != nil {
		return err
	}
	return tw.Flush()
}

func Runs(w io.Writer, runs []model.Run) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "RUN_ID\tSTATUS\tDURATION\tRESULT"); err != nil {
		return err
	}
	for _, run := range runs {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", run.RunID, run.Status, Duration(run), Summary(run)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func Duration(run model.Run) string {
	start := run.CreatedAt
	if run.StartedAt != nil {
		start = *run.StartedAt
	}
	if start.IsZero() {
		return "-"
	}
	end := time.Now()
	if run.EndedAt != nil {
		end = *run.EndedAt
	}
	if end.Before(start) {
		return "-"
	}
	return end.Sub(start).Round(time.Second).String()
}

func Summary(run model.Run) string {
	if run.Live != nil {
		return fmt.Sprintf("rps=%.1f error_rate=%.2f%%", run.Live.RPS, run.Live.ErrorRate*100)
	}
	if run.TotalRequests > 0 {
		errRate := float64(run.TotalErrors) / float64(run.TotalRequests) * 100
		return fmt.Sprintf("requests=%d error_rate=%.2f%% p99=%.0fms", run.TotalRequests, errRate, run.P99MS)
	}
	return "-"
}
