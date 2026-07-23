package model

import (
	"encoding/json"
	"time"
)

type Run struct {
	RunID         string          `json:"run_id"`
	Name          string          `json:"name,omitempty"`
	Status        string          `json:"status"`
	CreatedAt     time.Time       `json:"created_at"`
	StartedAt     *time.Time      `json:"started_at,omitempty"`
	EndedAt       *time.Time      `json:"ended_at,omitempty"`
	ActiveWorkers int64           `json:"active_workers,omitempty"`
	TotalRequests int64           `json:"total_requests,omitempty"`
	TotalErrors   int64           `json:"total_errors,omitempty"`
	P99MS         float64         `json:"p99_ms,omitempty"`
	ResultSummary json.RawMessage `json:"result_summary,omitempty"`
	CreatedBy     string          `json:"created_by,omitempty"`
	Live          *LiveMetrics    `json:"live,omitempty"`
}

type LiveMetrics struct {
	RPS       float64 `json:"rps"`
	P95MS     float64 `json:"p95_ms"`
	ErrorRate float64 `json:"error_rate"`
}

type APIKey struct {
	ID   string
	Name string
}

type ErrorBody struct {
	Error APIError `json:"error"`
}

type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
