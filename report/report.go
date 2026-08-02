package report

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/vatsalchaudhary/loadforge/aggregator/histogram"
	"github.com/vatsalchaudhary/loadforge/pkg/testplan"
)

var ErrNotFound = errors.New("report data not found")

type Builder struct {
	DB *sql.DB
}

type Report struct {
	RunID            string            `json:"run_id"`
	Name             string            `json:"name"`
	Status           string            `json:"status"`
	CreatedAt        time.Time         `json:"created_at"`
	StartedAt        *time.Time        `json:"started_at,omitempty"`
	EndedAt          *time.Time        `json:"ended_at,omitempty"`
	TestPlan         testplan.TestPlan `json:"test_plan"`
	Overall          Stats             `json:"overall"`
	Endpoints        []EndpointStats   `json:"endpoints"`
	ThresholdResults json.RawMessage   `json:"threshold_results,omitempty"`
	WorkerFleet      WorkerFleet       `json:"worker_fleet"`
	Timeline         []TimelinePoint   `json:"timeline"`
}

type Stats struct {
	TotalRequests    int64            `json:"total_requests"`
	TotalErrors      int64            `json:"total_errors"`
	ErrorRatePercent float64          `json:"error_rate_percent"`
	RPS              float64          `json:"rps"`
	P50LatencyMS     float64          `json:"p50_latency_ms"`
	P95LatencyMS     float64          `json:"p95_latency_ms"`
	P99LatencyMS     float64          `json:"p99_latency_ms"`
	StatusCodes      map[string]int64 `json:"status_codes,omitempty"`
}

type EndpointStats struct {
	Endpoint string `json:"endpoint"`
	Method   string `json:"method"`
	Stats
}

type WorkerFleet struct {
	Count          int64         `json:"count"`
	Unhealthy      []WorkerEvent `json:"unhealthy,omitempty"`
	Replaced       []WorkerEvent `json:"replaced,omitempty"`
	UnhealthyCount int           `json:"unhealthy_count"`
	ReplacedCount  int           `json:"replaced_count"`
}

type WorkerEvent struct {
	WorkerID  string    `json:"worker_id"`
	Type      string    `json:"type"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

type TimelinePoint struct {
	TS           time.Time `json:"ts"`
	RPS          float64   `json:"rps"`
	P95LatencyMS float64   `json:"p95_latency_ms"`
	P99LatencyMS float64   `json:"p99_latency_ms"`
	WorkerCount  int64     `json:"worker_count"`
}

type snapshot struct {
	TS               time.Time
	Endpoint         string
	Method           string
	RPS              float64
	P50MS            float64
	P95MS            float64
	P99MS            float64
	ReqCount         int64
	ErrCount         int64
	StatusCodes      map[string]int64
	LatencyHistogram string
}

func (b Builder) Build(ctx context.Context, runID string) (Report, error) {
	if b.DB == nil {
		return Report{}, errors.New("report builder requires DB")
	}
	report, err := b.readRun(ctx, runID)
	if err != nil {
		return report, err
	}
	snapshots, err := b.readSnapshots(ctx, runID)
	if err != nil {
		return report, err
	}
	report.Overall, report.Endpoints, report.Timeline = aggregate(snapshots, workerCount(report))
	fleet, err := b.readWorkerFleet(ctx, runID, report)
	if err != nil {
		return report, err
	}
	report.WorkerFleet = fleet
	for i := range report.Timeline {
		report.Timeline[i].WorkerCount = fleet.Count
	}
	return report, nil
}

func (b Builder) readRun(ctx context.Context, runID string) (Report, error) {
	var out Report
	var planBytes []byte
	var thresholdResults sql.NullString
	err := b.DB.QueryRowContext(ctx, `
SELECT id::text, name, status, created_at, started_at, ended_at, test_plan, threshold_results
FROM test_runs WHERE id::text = $1`, runID).Scan(
		&out.RunID, &out.Name, &out.Status, &out.CreatedAt, &out.StartedAt, &out.EndedAt, &planBytes, &thresholdResults,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	if len(planBytes) > 0 {
		if err := json.Unmarshal(planBytes, &out.TestPlan); err != nil {
			return out, fmt.Errorf("decode test plan: %w", err)
		}
	}
	if thresholdResults.Valid && thresholdResults.String != "" {
		out.ThresholdResults = json.RawMessage(thresholdResults.String)
	}
	return out, nil
}

func (b Builder) readSnapshots(ctx context.Context, runID string) ([]snapshot, error) {
	rows, err := b.DB.QueryContext(ctx, `
SELECT ts, endpoint, method,
       COALESCE(rps, 0), COALESCE(p50_ms, 0), COALESCE(p95_ms, 0), COALESCE(p99_ms, 0),
       COALESCE(req_count, 0), COALESCE(err_count, 0), status_codes, COALESCE(latency_histogram, '')
FROM metric_snapshots
WHERE run_id = $1
ORDER BY ts, endpoint, method`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []snapshot
	for rows.Next() {
		var item snapshot
		var statusBytes sql.NullString
		if err := rows.Scan(&item.TS, &item.Endpoint, &item.Method, &item.RPS, &item.P50MS, &item.P95MS, &item.P99MS,
			&item.ReqCount, &item.ErrCount, &statusBytes, &item.LatencyHistogram); err != nil {
			return nil, err
		}
		if statusBytes.Valid && statusBytes.String != "" {
			_ = json.Unmarshal([]byte(statusBytes.String), &item.StatusCodes)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (b Builder) readWorkerFleet(ctx context.Context, runID string, report Report) (WorkerFleet, error) {
	fleet := WorkerFleet{Count: workerCount(report)}
	rows, err := b.DB.QueryContext(ctx, `
SELECT worker_id, event_type, message, created_at
FROM worker_events
WHERE run_id = $1 AND upper(event_type) IN ('UNHEALTHY', 'REPLACED')
ORDER BY created_at`, runID)
	if err != nil {
		return fleet, err
	}
	defer rows.Close()
	seen := make(map[string]struct{})
	for rows.Next() {
		var event WorkerEvent
		if err := rows.Scan(&event.WorkerID, &event.Type, &event.Message, &event.CreatedAt); err != nil {
			return fleet, err
		}
		seen[event.WorkerID] = struct{}{}
		switch strings.ToUpper(event.Type) {
		case "UNHEALTHY":
			fleet.Unhealthy = append(fleet.Unhealthy, event)
		case "REPLACED":
			fleet.Replaced = append(fleet.Replaced, event)
		}
	}
	if err := rows.Err(); err != nil {
		return fleet, err
	}
	if fleet.Count == 0 {
		fleet.Count = int64(len(seen))
	}
	fleet.UnhealthyCount = len(fleet.Unhealthy)
	fleet.ReplacedCount = len(fleet.Replaced)
	return fleet, nil
}

func aggregate(snapshots []snapshot, workers int64) (Stats, []EndpointStats, []TimelinePoint) {
	overallAgg := newAgg()
	endpointAggs := make(map[string]*agg)
	timelineAggs := make(map[time.Time]*agg)
	for _, snap := range snapshots {
		overallAgg.add(snap)
		key := snap.Method + "\x00" + snap.Endpoint
		if endpointAggs[key] == nil {
			endpointAggs[key] = newAgg()
		}
		endpointAggs[key].endpoint = snap.Endpoint
		endpointAggs[key].method = snap.Method
		endpointAggs[key].add(snap)
		if timelineAggs[snap.TS] == nil {
			timelineAggs[snap.TS] = newAgg()
		}
		timelineAggs[snap.TS].add(snap)
	}
	endpoints := make([]EndpointStats, 0, len(endpointAggs))
	for _, ag := range endpointAggs {
		endpoints = append(endpoints, EndpointStats{Endpoint: ag.endpoint, Method: ag.method, Stats: ag.statsAverageRPS()})
	}
	sort.Slice(endpoints, func(i, j int) bool {
		if endpoints[i].Endpoint == endpoints[j].Endpoint {
			return endpoints[i].Method < endpoints[j].Method
		}
		return endpoints[i].Endpoint < endpoints[j].Endpoint
	})
	times := make([]time.Time, 0, len(timelineAggs))
	for ts := range timelineAggs {
		times = append(times, ts)
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	timeline := make([]TimelinePoint, 0, len(times))
	for _, ts := range times {
		stats := timelineAggs[ts].statsSumRPS()
		timeline = append(timeline, TimelinePoint{
			TS: ts, RPS: stats.RPS, P95LatencyMS: stats.P95LatencyMS,
			P99LatencyMS: stats.P99LatencyMS, WorkerCount: workers,
		})
	}
	overall := overallAgg.statsAverageRPS()
	if len(timeline) > 0 {
		var totalRPS float64
		for _, point := range timeline {
			totalRPS += point.RPS
		}
		overall.RPS = totalRPS / float64(len(timeline))
	}
	return overall, endpoints, timeline
}

type agg struct {
	endpoint    string
	method      string
	req         int64
	errs        int64
	rps         float64
	rpsSamples  int64
	statusCodes map[string]int64
	hist        *histogram.Histogram
}

func newAgg() *agg {
	return &agg{statusCodes: make(map[string]int64), hist: histogram.New()}
}

func (a *agg) add(s snapshot) {
	a.req += s.ReqCount
	a.errs += s.ErrCount
	a.rps += s.RPS
	a.rpsSamples++
	for code, count := range s.StatusCodes {
		a.statusCodes[code] += count
	}
	if s.LatencyHistogram != "" {
		if h, err := histogram.Decode(s.LatencyHistogram); err == nil {
			a.hist.Merge(h)
			return
		}
	}
	// Compatibility fallback for pre-Milestone-7 rows. This is an approximation
	// only; new rows carry HDR histograms so run-wide percentiles are recomputed
	// by merging distributions rather than averaging p50/p95/p99 snapshots.
	approxCount := max(s.ReqCount, 1)
	for _, value := range []float64{s.P50MS, s.P95MS, s.P99MS} {
		for range max(approxCount/3, 1) {
			a.hist.RecordMilliseconds(value)
		}
	}
}

func (a *agg) statsAverageRPS() Stats {
	stats := a.baseStats()
	if a.rpsSamples > 0 {
		stats.RPS = a.rps / float64(a.rpsSamples)
	}
	return stats
}

func (a *agg) statsSumRPS() Stats {
	stats := a.baseStats()
	stats.RPS = a.rps
	return stats
}

func (a *agg) baseStats() Stats {
	percentiles := a.hist.Percentiles()
	stats := Stats{
		TotalRequests: a.req, TotalErrors: a.errs,
		P50LatencyMS: percentiles.P50, P95LatencyMS: percentiles.P95, P99LatencyMS: percentiles.P99,
		StatusCodes: a.statusCodes,
	}
	if a.req > 0 {
		stats.ErrorRatePercent = float64(a.errs) / float64(a.req) * 100
	}
	return stats
}

func workerCount(report Report) int64 {
	if report.TestPlan.LoadProfile.InitialWorkers > 0 {
		return int64(report.TestPlan.LoadProfile.InitialWorkers)
	}
	return 0
}

func JSON(r Report) ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

func HTML(r Report) ([]byte, error) {
	var buf bytes.Buffer
	if err := htmlTemplate.Execute(&buf, htmlView{
		Report:     r,
		RPSSVG:     sparkline(values(r.Timeline, func(p TimelinePoint) float64 { return p.RPS })),
		P95SVG:     sparkline(values(r.Timeline, func(p TimelinePoint) float64 { return p.P95LatencyMS })),
		WorkersSVG: sparkline(values(r.Timeline, func(p TimelinePoint) float64 { return float64(p.WorkerCount) })),
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type htmlView struct {
	Report     Report
	RPSSVG     template.HTML
	P95SVG     template.HTML
	WorkersSVG template.HTML
}

// Hand-rolled SVG keeps the API server binary lighter than embedding go-echarts
// and produces reports that work fully offline when downloaded.
func sparkline(points []float64) template.HTML {
	const width, height = 640, 120
	if len(points) == 0 {
		return template.HTML(`<svg viewBox="0 0 640 120" role="img" aria-label="No data"><text x="16" y="64">No data</text></svg>`)
	}
	minVal, maxVal := points[0], points[0]
	for _, p := range points {
		minVal = math.Min(minVal, p)
		maxVal = math.Max(maxVal, p)
	}
	scaleY := func(v float64) float64 {
		if maxVal == minVal {
			return height / 2
		}
		return height - 12 - ((v - minVal) / (maxVal - minVal) * (height - 24))
	}
	coords := make([]string, 0, len(points))
	for i, p := range points {
		x := 12.0
		if len(points) > 1 {
			x = 12 + float64(i)*(width-24)/float64(len(points)-1)
		}
		coords = append(coords, fmt.Sprintf("%.1f,%.1f", x, scaleY(p)))
	}
	return template.HTML(fmt.Sprintf(`<svg viewBox="0 0 %d %d" role="img"><polyline fill="none" stroke="#2563eb" stroke-width="2" points="%s"/></svg>`,
		width, height, strings.Join(coords, " ")))
}

func values(points []TimelinePoint, pick func(TimelinePoint) float64) []float64 {
	out := make([]float64, 0, len(points))
	for _, point := range points {
		out = append(out, pick(point))
	}
	return out
}

var htmlTemplate = template.Must(template.New("report").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>LoadForge Report - {{.Report.RunID}}</title>
<style>
body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:2rem;color:#111827}
section{margin:2rem 0}table{border-collapse:collapse;width:100%}th,td{border:1px solid #d1d5db;padding:.5rem;text-align:left}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(12rem,1fr));gap:1rem}.card{border:1px solid #d1d5db;border-radius:.5rem;padding:1rem}
svg{width:100%;height:120px;background:#f9fafb;border:1px solid #e5e7eb}
</style>
</head>
<body>
<h1>LoadForge Report</h1>
<section>
<h2>Test Plan Summary</h2>
<p><strong>{{.Report.Name}}</strong> — {{.Report.Status}} — run {{.Report.RunID}}</p>
<p>Target: {{.Report.TestPlan.Target.BaseURL}}</p>
</section>
<section>
<h2>Overall Stats</h2>
<div class="grid">
<div class="card">Requests<br>{{.Report.Overall.TotalRequests}}</div>
<div class="card">Errors<br>{{.Report.Overall.TotalErrors}}</div>
<div class="card">Error Rate<br>{{printf "%.2f" .Report.Overall.ErrorRatePercent}}%</div>
<div class="card">p95<br>{{printf "%.0f" .Report.Overall.P95LatencyMS}}ms</div>
<div class="card">p99<br>{{printf "%.0f" .Report.Overall.P99LatencyMS}}ms</div>
</div>
</section>
<section>
<h2>Timeline Charts</h2>
<h3>RPS</h3>{{.RPSSVG}}
<h3>p95 Latency</h3>{{.P95SVG}}
<h3>Worker Count</h3>{{.WorkersSVG}}
</section>
<section>
<h2>Per-Endpoint Breakdown</h2>
<table><thead><tr><th>Method</th><th>Endpoint</th><th>Requests</th><th>Errors</th><th>p50</th><th>p95</th><th>p99</th></tr></thead><tbody>
{{range .Report.Endpoints}}<tr><td>{{.Method}}</td><td>{{.Endpoint}}</td><td>{{.TotalRequests}}</td><td>{{.TotalErrors}}</td><td>{{printf "%.0f" .P50LatencyMS}}</td><td>{{printf "%.0f" .P95LatencyMS}}</td><td>{{printf "%.0f" .P99LatencyMS}}</td></tr>{{end}}
</tbody></table>
</section>
<section>
<h2>Threshold Results</h2>
{{if .Report.ThresholdResults}}<pre>{{printf "%s" .Report.ThresholdResults}}</pre>{{else}}<p>No thresholds configured.</p>{{end}}
</section>
<section>
<h2>Worker Fleet Summary</h2>
<p>Workers: {{.Report.WorkerFleet.Count}}. Unhealthy events: {{.Report.WorkerFleet.UnhealthyCount}}. Replaced events: {{.Report.WorkerFleet.ReplacedCount}}.</p>
</section>
</body>
</html>`))
