package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vatsalchaudhary/loadforge/apiserver/middleware"
	"github.com/vatsalchaudhary/loadforge/apiserver/model"
	apistore "github.com/vatsalchaudhary/loadforge/apiserver/store"
	"github.com/vatsalchaudhary/loadforge/orchestrator/planner"
	"github.com/vatsalchaudhary/loadforge/pkg/testplan"
)

const maxRequestBytes = 64 << 20
const maxStepBodyBytes = 10 << 20

type Store interface {
	CreatePending(context.Context, string, string, json.RawMessage, string, time.Time) error
	MarkFailed(context.Context, string) error
	GetRun(context.Context, string) (model.Run, error)
	ListRuns(context.Context, int, int, string) ([]model.Run, int64, error)
	PingPostgres(context.Context) error
}

type Orchestrator interface {
	Submit(context.Context, string, json.RawMessage) (string, error)
	Stop(context.Context, string) (string, error)
	Ready(context.Context) error
}

type Streamer interface {
	Serve(http.ResponseWriter, *http.Request, string) error
}

type Handler struct {
	store Store
	orch  Orchestrator
	sse   Streamer
	now   func() time.Time
}

func New(store Store, orch Orchestrator, stream Streamer) *Handler {
	return &Handler{store: store, orch: orch, sse: stream, now: time.Now}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /runs", h.createRun)
	mux.HandleFunc("GET /runs", h.listRuns)
	mux.HandleFunc("GET /runs/{run_id}", h.getRun)
	mux.HandleFunc("POST /runs/{run_id}/stop", h.stopRun)
	mux.HandleFunc("GET /runs/{run_id}/stream", h.streamRun)
	mux.HandleFunc("POST /validate", h.validate)
	return mux
}

type planRequest struct {
	TestPlan      json.RawMessage   `json:"test_plan"`
	Variables     map[string]string `json:"variables,omitempty"`
	AllowInternal bool              `json:"allow_internal,omitempty"`
}

type validationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type validationResponse struct {
	Valid  bool              `json:"valid"`
	Errors []validationError `json:"errors,omitempty"`
}

func (h *Handler) createRun(w http.ResponseWriter, r *http.Request) {
	request, plan, resolved, validationErrs, err := decodeAndValidate(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if len(validationErrs) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, validationResponse{Valid: false, Errors: validationErrs})
		return
	}
	_ = request
	runID := uuid.NewString()
	createdAt := h.now().UTC()
	key, _ := middleware.APIKeyFromContext(r.Context())
	if err := h.store.CreatePending(r.Context(), runID, plan.Name, resolved, key.ID, createdAt); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", "failed to persist test run")
		return
	}
	if _, err := h.orch.Submit(r.Context(), runID, resolved); err != nil {
		_ = h.store.MarkFailed(r.Context(), runID)
		writeError(w, http.StatusBadGateway, "orchestrator_unavailable", "failed to submit test run")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"run_id": runID, "status": "PENDING", "created_at": createdAt,
	})
}

func (h *Handler) validate(w http.ResponseWriter, r *http.Request) {
	_, _, _, validationErrs, err := decodeAndValidate(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if len(validationErrs) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, validationResponse{Valid: false, Errors: validationErrs})
		return
	}
	writeJSON(w, http.StatusOK, validationResponse{Valid: true})
}

func decodeAndValidate(w http.ResponseWriter, r *http.Request) (planRequest, testplan.TestPlan, json.RawMessage, []validationError, error) {
	var request planRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&request); err != nil {
		return request, testplan.TestPlan{}, nil, nil, fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return request, testplan.TestPlan{}, nil, nil, errors.New("request body must contain one JSON object")
	}
	if len(request.TestPlan) == 0 || string(request.TestPlan) == "null" {
		return request, testplan.TestPlan{}, nil, []validationError{{Field: "test_plan", Message: "required"}}, nil
	}
	plan, resolved, err := planner.ParseJSON(request.TestPlan, request.Variables)
	if err != nil {
		return request, plan, resolved, plannerErrors(err), nil
	}
	if request.AllowInternal {
		plan.Target.AllowInternal = true
		resolved, _ = json.Marshal(plan)
	}
	errs := sanitize(plan)
	return request, plan, resolved, errs, nil
}

func plannerErrors(err error) []validationError {
	var items planner.ValidationErrors
	if !errors.As(err, &items) {
		return []validationError{{Field: "$", Message: err.Error()}}
	}
	out := make([]validationError, 0, len(items))
	for _, item := range items {
		out = append(out, validationError{Field: item.Field, Message: item.Reason})
	}
	return out
}

func sanitize(plan testplan.TestPlan) []validationError {
	var errs []validationError
	parsed, err := url.Parse(plan.Target.BaseURL)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		errs = append(errs, validationError{Field: "target.base_url", Message: "must be a valid absolute HTTP(S) URL"})
	} else if !plan.Target.AllowInternal && internalHost(parsed.Hostname()) {
		errs = append(errs, validationError{Field: "target.base_url", Message: "private/internal targets require allow_internal: true"})
	}
	for i, scenario := range plan.Scenarios {
		for j, step := range scenario.Steps {
			if len([]byte(step.Body)) > maxStepBodyBytes {
				errs = append(errs, validationError{
					Field:   fmt.Sprintf("scenarios[%d].steps[%d].body", i, j),
					Message: "must not exceed 10MB",
				})
			}
		}
	}
	return errs
}

func internalHost(host string) bool {
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && (ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast())
}

func (h *Handler) getRun(w http.ResponseWriter, r *http.Request) {
	runID, ok := validRunID(w, r)
	if !ok {
		return
	}
	run, err := h.store.GetRun(r.Context(), runID)
	if errors.Is(err, apistore.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "run not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", "failed to read test run")
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (h *Handler) stopRun(w http.ResponseWriter, r *http.Request) {
	runID, ok := validRunID(w, r)
	if !ok {
		return
	}
	run, err := h.store.GetRun(r.Context(), runID)
	if errors.Is(err, apistore.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "run not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", "failed to read test run")
		return
	}
	if run.Status == "DRAINING" || run.Status == "DONE" || run.Status == "FAILED" {
		writeJSON(w, http.StatusAccepted, map[string]string{"status": run.Status})
		return
	}
	status, err := h.orch.Stop(r.Context(), runID)
	if err != nil {
		writeError(w, http.StatusBadGateway, "orchestrator_unavailable", "failed to stop test run")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": status})
}

func (h *Handler) listRuns(w http.ResponseWriter, r *http.Request) {
	limit, err := queryInt(r, "limit", 20)
	if err != nil || limit < 1 || limit > 100 {
		writeError(w, http.StatusBadRequest, "invalid_request", "limit must be between 1 and 100")
		return
	}
	offset, err := queryInt(r, "offset", 0)
	if err != nil || offset < 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "offset must be non-negative")
		return
	}
	status := strings.ToUpper(r.URL.Query().Get("status"))
	if status != "" && !validStatus(status) {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid status filter")
		return
	}
	runs, total, err := h.store.ListRuns(r.Context(), limit, offset, status)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", "failed to list test runs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs, "total": total})
}

func (h *Handler) streamRun(w http.ResponseWriter, r *http.Request) {
	runID, ok := validRunID(w, r)
	if !ok {
		return
	}
	if err := h.sse.Serve(w, r, runID); err != nil {
		if errors.Is(err, apistore.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "run not found")
			return
		}
		// Once an SSE event has been flushed the status cannot be changed.
		if r.Context().Err() == nil && w.Header().Get("Content-Type") == "" {
			writeError(w, http.StatusServiceUnavailable, "stream_unavailable", "live metrics stream is unavailable")
			return
		}
	}
}

func (h *Handler) Healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	dbErr := h.store.PingPostgres(ctx)
	orchErr := h.orch.Ready(ctx)
	if dbErr != nil || orchErr != nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "required dependency is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func validRunID(w http.ResponseWriter, r *http.Request) (string, bool) {
	runID := r.PathValue("run_id")
	if _, err := uuid.Parse(runID); err != nil {
		writeError(w, http.StatusNotFound, "not_found", "run not found")
		return "", false
	}
	return runID, true
}

func queryInt(r *http.Request, name string, fallback int) (int, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}

func validStatus(status string) bool {
	switch status {
	case "PENDING", "PROVISIONING", "RUNNING", "SCALING", "DRAINING", "DONE", "FAILED", "THRESHOLD_BREACHED":
		return true
	default:
		return false
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, model.ErrorBody{Error: model.APIError{Code: code, Message: message}})
}
