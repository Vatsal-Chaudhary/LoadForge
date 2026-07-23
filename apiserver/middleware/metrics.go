package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "loadforge_apiserver_http_requests_total",
			Help: "API server HTTP requests by method, route, and status.",
		}, []string{"method", "route", "status"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "loadforge_apiserver_http_request_duration_seconds",
			Help: "API server HTTP request duration.",
		}, []string{"method", "route"}),
	}
	reg.MustRegister(m.requests, m.latency)
	return m
}

func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		m.requests.WithLabelValues(r.Method, route, strconv.Itoa(sw.status)).Inc()
		m.latency.WithLabelValues(r.Method, route).Observe(time.Since(started).Seconds())
	})
}
