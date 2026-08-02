package report

import (
	"strings"
	"testing"
	"time"

	"github.com/vatsalchaudhary/loadforge/aggregator/histogram"
)

func encodedHistogram(t *testing.T, values ...float64) string {
	t.Helper()
	h := histogram.New()
	for _, value := range values {
		h.RecordMilliseconds(value)
	}
	encoded, err := h.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestAggregateMergesHistogramsInsteadOfAveragingPercentiles(t *testing.T) {
	base := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	first := make([]float64, 0, 100)
	second := make([]float64, 0, 100)
	for i := 1; i <= 100; i++ {
		first = append(first, float64(i))
		second = append(second, float64(999+i))
	}
	overall, endpoints, timeline := aggregate([]snapshot{
		{
			TS: base, Endpoint: "/items", Method: "GET", RPS: 10,
			P95MS: 95, P99MS: 99, ReqCount: 100, ErrCount: 1,
			StatusCodes: map[string]int64{"200": 99, "500": 1}, LatencyHistogram: encodedHistogram(t, first...),
		},
		{
			TS: base.Add(10 * time.Second), Endpoint: "/items", Method: "GET", RPS: 10,
			P95MS: 1094, P99MS: 1098, ReqCount: 100, ErrCount: 0,
			StatusCodes: map[string]int64{"200": 100}, LatencyHistogram: encodedHistogram(t, second...),
		},
	}, 2)
	if overall.TotalRequests != 200 || overall.TotalErrors != 1 {
		t.Fatalf("overall counts = %#v", overall)
	}
	if overall.P95LatencyMS < 1088 || overall.P95LatencyMS > 1091 {
		t.Fatalf("overall p95 = %.3f, want merged-distribution p95 around 1090", overall.P95LatencyMS)
	}
	if overall.StatusCodes["200"] != 199 || overall.StatusCodes["500"] != 1 {
		t.Fatalf("status codes = %#v", overall.StatusCodes)
	}
	if len(endpoints) != 1 || endpoints[0].TotalRequests != 200 || endpoints[0].P95LatencyMS < 1088 {
		t.Fatalf("endpoints = %#v", endpoints)
	}
	if len(timeline) != 2 || timeline[0].WorkerCount != 2 || timeline[1].WorkerCount != 2 {
		t.Fatalf("timeline = %#v", timeline)
	}
}

func TestHTMLReportStructureAndNoExternalResources(t *testing.T) {
	body, err := HTML(Report{
		RunID: "run-1", Name: "checkout", Status: "DONE",
		Overall:   Stats{TotalRequests: 10, P95LatencyMS: 20, P99LatencyMS: 30},
		Endpoints: []EndpointStats{{Endpoint: "/items", Method: "GET", Stats: Stats{TotalRequests: 10}}},
		Timeline:  []TimelinePoint{{TS: time.Now(), RPS: 1, P95LatencyMS: 20, WorkerCount: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	html := string(body)
	for _, want := range []string{"<!doctype html>", "Test Plan Summary", "Overall Stats", "Timeline Charts", "Per-Endpoint Breakdown", "Threshold Results", "Worker Fleet Summary", "<svg"} {
		if !strings.Contains(html, want) {
			t.Fatalf("HTML missing %q:\n%s", want, html)
		}
	}
	for _, forbidden := range []string{"<script", "src=\"http", "href=\"http", "cdn"} {
		if strings.Contains(strings.ToLower(html), forbidden) {
			t.Fatalf("HTML contains external resource marker %q:\n%s", forbidden, html)
		}
	}
}
