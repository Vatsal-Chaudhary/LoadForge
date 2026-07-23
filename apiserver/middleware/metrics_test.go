package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestMetricsExpositionIncludesCountsLatenciesAndErrors(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /failure", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "failed", http.StatusInternalServerError)
	})
	handler := metrics.Middleware(mux)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/failure", nil))

	rec := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, `loadforge_apiserver_http_requests_total{method="GET",route="GET /failure",status="500"} 1`) {
		t.Fatalf("request/error counter missing:\n%s", body)
	}
	if !strings.Contains(body, `loadforge_apiserver_http_request_duration_seconds_bucket{method="GET",route="GET /failure"`) {
		t.Fatalf("latency histogram missing:\n%s", body)
	}
}
