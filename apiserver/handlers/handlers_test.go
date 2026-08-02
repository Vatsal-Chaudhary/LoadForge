package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vatsalchaudhary/loadforge/apiserver/middleware"
	"github.com/vatsalchaudhary/loadforge/apiserver/model"
	"github.com/vatsalchaudhary/loadforge/apiserver/store"
	reportpkg "github.com/vatsalchaudhary/loadforge/report"
)

const validPlanJSON = `{
  "name":"checkout","version":"1",
  "target":{"base_url":"https://example.com","timeout":"10s"},
  "load_profile":{"type":"constant","initial_workers":1},
  "workers":{"virtual_users_per_worker":1},
  "scenarios":[{"name":"browse","weight":1,"steps":[{"name":"home","method":"GET","path":"/"}]}]
}`

type fakeStore struct {
	mu         sync.Mutex
	runs       map[string]model.Run
	created    model.Run
	list       []model.Run
	total      int64
	authErr    error
	pingErr    error
	listLimit  int
	listOffset int
	listStatus string
	report     reportpkg.Report
	reportErr  error
}

func (s *fakeStore) Authenticate(context.Context, string) (model.APIKey, error) {
	if s.authErr != nil {
		return model.APIKey{}, s.authErr
	}
	return model.APIKey{ID: "key-1", Name: "test"}, nil
}
func (s *fakeStore) CreatePending(_ context.Context, id, name string, _ json.RawMessage, createdBy string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.created = model.Run{RunID: id, Name: name, Status: "PENDING", CreatedAt: at, CreatedBy: createdBy}
	if s.runs == nil {
		s.runs = make(map[string]model.Run)
	}
	s.runs[id] = s.created
	return nil
}
func (s *fakeStore) MarkFailed(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.runs[id]
	item.Status = "FAILED"
	s.runs[id] = item
	return nil
}
func (s *fakeStore) GetRun(_ context.Context, id string) (model.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.runs[id]
	if !ok {
		return model.Run{}, store.ErrNotFound
	}
	return item, nil
}
func (s *fakeStore) ListRuns(_ context.Context, limit, offset int, status string) ([]model.Run, int64, error) {
	s.listLimit, s.listOffset, s.listStatus = limit, offset, status
	return s.list, s.total, nil
}
func (s *fakeStore) PingPostgres(context.Context) error { return s.pingErr }
func (s *fakeStore) BuildReport(context.Context, string) (reportpkg.Report, error) {
	return s.report, s.reportErr
}

type fakeOrch struct {
	submitted int
	stopped   int
	submitErr error
	stopErr   error
	readyErr  error
}

func (o *fakeOrch) Submit(context.Context, string, json.RawMessage) (string, error) {
	o.submitted++
	return "PENDING", o.submitErr
}
func (o *fakeOrch) Stop(context.Context, string) (string, error) {
	o.stopped++
	return "DRAINING", o.stopErr
}
func (o *fakeOrch) Ready(context.Context) error { return o.readyErr }

type fakeStream struct{ called string }

func (s *fakeStream) Serve(_ http.ResponseWriter, _ *http.Request, id string) error {
	s.called = id
	return nil
}

func protectedHandler(s *fakeStore, o *fakeOrch, stream Streamer, limiter *middleware.RateLimiter) http.Handler {
	api := New(s, o, stream)
	var handler http.Handler = api.Routes()
	if limiter != nil {
		handler = limiter.Middleware(handler)
	}
	return middleware.Auth(s, handler)
}

func request(t *testing.T, handler http.Handler, method, target, body string, auth bool) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if auth {
		req.Header.Set("Authorization", "Bearer "+uuid.NewString())
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestCreateRunAndValidateUseSameErrors(t *testing.T) {
	for _, path := range []string{"/runs", "/validate"} {
		t.Run(path, func(t *testing.T) {
			s := &fakeStore{runs: make(map[string]model.Run)}
			o := &fakeOrch{}
			h := protectedHandler(s, o, &fakeStream{}, nil)
			body := `{"test_plan":{"name":"","version":"1","target":{"base_url":"https://example.com"},"load_profile":{"type":"constant","initial_workers":1},"workers":{"virtual_users_per_worker":1},"scenarios":[]}}`
			rec := request(t, h, http.MethodPost, path, body, true)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), `"field":"name"`) || !strings.Contains(rec.Body.String(), `"message":"required"`) {
				t.Fatalf("validation body = %s", rec.Body.String())
			}
		})
	}

	s := &fakeStore{runs: make(map[string]model.Run)}
	o := &fakeOrch{}
	h := protectedHandler(s, o, &fakeStream{}, nil)
	rec := request(t, h, http.MethodPost, "/runs", `{"test_plan":`+validPlanJSON+`}`, true)
	if rec.Code != http.StatusAccepted || o.submitted != 1 {
		t.Fatalf("status=%d submitted=%d body=%s", rec.Code, o.submitted, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(response["run_id"].(string)); err != nil || response["status"] != "PENDING" {
		t.Fatalf("response = %#v, parseErr=%v", response, err)
	}

	rec = request(t, h, http.MethodPost, "/validate", `{"test_plan":`+validPlanJSON+`}`, true)
	if rec.Code != http.StatusOK || rec.Body.String() != "{\"valid\":true}\n" {
		t.Fatalf("validate status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestValidationSanitizationAndVariables(t *testing.T) {
	s := &fakeStore{runs: make(map[string]model.Run)}
	h := protectedHandler(s, &fakeOrch{}, &fakeStream{}, nil)
	privatePlan := strings.Replace(validPlanJSON, "https://example.com", "http://10.0.0.1", 1)
	rec := request(t, h, http.MethodPost, "/validate", `{"test_plan":`+privatePlan+`}`, true)
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), "allow_internal") {
		t.Fatalf("private target: status=%d body=%s", rec.Code, rec.Body.String())
	}
	rec = request(t, h, http.MethodPost, "/validate", `{"test_plan":`+privatePlan+`,"allow_internal":true}`, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("allowed private target: status=%d body=%s", rec.Code, rec.Body.String())
	}
	variablePlan := strings.Replace(validPlanJSON, "https://example.com", "{{ .base_url }}", 1)
	rec = request(t, h, http.MethodPost, "/validate",
		`{"test_plan":`+variablePlan+`,"variables":{"base_url":"https://service.example"}}`, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("variables: status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestGetRunListStopStreamAndNotFound(t *testing.T) {
	id := uuid.NewString()
	run := model.Run{RunID: id, Name: "x", Status: "RUNNING", Live: &model.LiveMetrics{RPS: 4}}
	s := &fakeStore{runs: map[string]model.Run{id: run}, list: []model.Run{run}, total: 1}
	o := &fakeOrch{}
	stream := &fakeStream{}
	h := protectedHandler(s, o, stream, nil)

	rec := request(t, h, http.MethodGet, "/runs/"+id, "", true)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"rps":4`) {
		t.Fatalf("get status=%d body=%s", rec.Code, rec.Body.String())
	}
	rec = request(t, h, http.MethodGet, "/runs?limit=10&offset=2&status=running", "", true)
	if rec.Code != http.StatusOK || s.listLimit != 10 || s.listOffset != 2 || s.listStatus != "RUNNING" {
		t.Fatalf("list status=%d args=%d,%d,%s body=%s", rec.Code, s.listLimit, s.listOffset, s.listStatus, rec.Body.String())
	}
	rec = request(t, h, http.MethodPost, "/runs/"+id+"/stop", "", true)
	if rec.Code != http.StatusAccepted || o.stopped != 1 || !strings.Contains(rec.Body.String(), "DRAINING") {
		t.Fatalf("stop status=%d calls=%d body=%s", rec.Code, o.stopped, rec.Body.String())
	}
	s.runs[id] = model.Run{RunID: id, Status: "DONE"}
	rec = request(t, h, http.MethodPost, "/runs/"+id+"/stop", "", true)
	if rec.Code != http.StatusAccepted || o.stopped != 1 || !strings.Contains(rec.Body.String(), "DONE") {
		t.Fatalf("idempotent stop status=%d calls=%d body=%s", rec.Code, o.stopped, rec.Body.String())
	}
	rec = request(t, h, http.MethodGet, "/runs/"+id+"/stream", "", true)
	if rec.Code != http.StatusOK || stream.called != id {
		t.Fatalf("stream status=%d called=%s", rec.Code, stream.called)
	}
	rec = request(t, h, http.MethodGet, "/runs/"+uuid.NewString(), "", true)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing run status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestReportRoutesRequireTerminalRun(t *testing.T) {
	id := uuid.NewString()
	s := &fakeStore{
		runs: map[string]model.Run{id: {RunID: id, Name: "checkout", Status: "RUNNING"}},
		report: reportpkg.Report{
			RunID: id, Name: "checkout", Status: "DONE",
			Overall: reportpkg.Stats{TotalRequests: 10, P95LatencyMS: 20, P99LatencyMS: 30},
		},
	}
	h := protectedHandler(s, &fakeOrch{}, &fakeStream{}, nil)

	rec := request(t, h, http.MethodGet, "/runs/"+id+"/report", "", true)
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "terminal state") {
		t.Fatalf("mid-run report status=%d body=%s", rec.Code, rec.Body.String())
	}

	s.runs[id] = model.Run{RunID: id, Name: "checkout", Status: "DONE"}
	rec = request(t, h, http.MethodGet, "/runs/"+id+"/report", "", true)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" || !strings.Contains(rec.Body.String(), `"run_id": "`+id+`"`) {
		t.Fatalf("json report status=%d content-type=%q body=%s", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}

	rec = request(t, h, http.MethodGet, "/runs/"+id+"/report.html", "", true)
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") || !strings.Contains(rec.Body.String(), "LoadForge Report") {
		t.Fatalf("html report status=%d content-type=%q body=%s", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
}

func TestBadAuthRateLimitAndBadInputs(t *testing.T) {
	s := &fakeStore{runs: make(map[string]model.Run)}
	o := &fakeOrch{}
	h := protectedHandler(s, o, &fakeStream{}, middleware.NewRateLimiter(0.01, 1))
	rec := request(t, h, http.MethodGet, "/runs", "", false)
	if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), `"code":"unauthorized"`) {
		t.Fatalf("missing auth status=%d body=%s", rec.Code, rec.Body.String())
	}
	req := httptest.NewRequest(http.MethodGet, "/runs", nil)
	req.Header.Set("Authorization", "Bearer not-a-uuid")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid auth status=%d", rec.Code)
	}
	token := uuid.NewString()
	for i := 0; i < 2; i++ {
		req = httptest.NewRequest(http.MethodGet, "/runs", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("rate status=%d retry=%q body=%s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	h = protectedHandler(s, o, &fakeStream{}, nil)
	rec = request(t, h, http.MethodGet, "/runs?limit=1000", "", true)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad pagination status=%d", rec.Code)
	}
	rec = request(t, h, http.MethodGet, "/runs/not-a-uuid", "", true)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("bad run id status=%d", rec.Code)
	}
}

func TestHealthAndReadiness(t *testing.T) {
	s := &fakeStore{}
	o := &fakeOrch{}
	api := New(s, o, &fakeStream{})
	rec := httptest.NewRecorder()
	api.Healthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("health status=%d", rec.Code)
	}
	rec = httptest.NewRecorder()
	api.Readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("ready status=%d body=%s", rec.Code, rec.Body.String())
	}
	o.readyErr = errors.New("down")
	rec = httptest.NewRecorder()
	api.Readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("not ready status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCreateRunSubmissionFailureMarksFailed(t *testing.T) {
	s := &fakeStore{runs: make(map[string]model.Run)}
	o := &fakeOrch{submitErr: errors.New("down")}
	h := protectedHandler(s, o, &fakeStream{}, nil)
	rec := request(t, h, http.MethodPost, "/runs", `{"test_plan":`+validPlanJSON+`}`, true)
	if rec.Code != http.StatusBadGateway || s.runs[s.created.RunID].Status != "FAILED" {
		t.Fatalf("status=%d run=%+v body=%s", rec.Code, s.runs[s.created.RunID], rec.Body.String())
	}
}

func TestOversizedStepBodyRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a 10MB validation body")
	}
	var plan map[string]any
	if err := json.Unmarshal([]byte(validPlanJSON), &plan); err != nil {
		t.Fatal(err)
	}
	scenarios := plan["scenarios"].([]any)
	steps := scenarios[0].(map[string]any)["steps"].([]any)
	steps[0].(map[string]any)["body"] = strings.Repeat("x", maxStepBodyBytes+1)
	payload, _ := json.Marshal(map[string]any{"test_plan": plan})
	h := protectedHandler(&fakeStore{}, &fakeOrch{}, &fakeStream{}, nil)
	rec := request(t, h, http.MethodPost, "/validate", string(bytes.Clone(payload)), true)
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), "10MB") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
