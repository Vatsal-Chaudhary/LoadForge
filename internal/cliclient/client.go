package cliclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/vatsalchaudhary/loadforge/apiserver/model"
)

type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

type Option func(*Client)

func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) {
		if httpClient != nil {
			c.httpClient = httpClient
		}
	}
}

func New(baseURL, token string, opts ...Option) (*Client, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return nil, errors.New("api url is required")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid api url %q", baseURL)
	}
	c := &Client{baseURL: baseURL, token: strings.TrimSpace(token), httpClient: &http.Client{Timeout: 30 * time.Second}}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

type CreateRunRequest struct {
	TestPlan      json.RawMessage   `json:"test_plan"`
	Variables     map[string]string `json:"variables,omitempty"`
	AllowInternal bool              `json:"allow_internal,omitempty"`
}

type CreateRunResponse struct {
	RunID     string    `json:"run_id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type ValidationResponse struct {
	Valid  bool              `json:"valid"`
	Errors []ValidationError `json:"errors,omitempty"`
}

type ListRunsResponse struct {
	Runs  []model.Run `json:"runs"`
	Total int64       `json:"total"`
}

type StopRunResponse struct {
	Status string `json:"status"`
}

type MetricsEvent struct {
	TS      time.Time `json:"ts"`
	RPS     float64   `json:"rps"`
	P50     float64   `json:"p50"`
	P95     float64   `json:"p95"`
	P99     float64   `json:"p99"`
	Errors  float64   `json:"errors"`
	Workers int64     `json:"workers"`
}

type DoneEvent struct {
	Status        string  `json:"status"`
	TotalRequests int64   `json:"total_requests"`
	TotalErrors   int64   `json:"total_errors"`
	P99MS         float64 `json:"p99_ms"`
}

type StreamEvent struct {
	Type    string
	Metrics MetricsEvent
	Done    DoneEvent
}

type APIError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *APIError) Error() string {
	if e.Code == "" && e.Message == "" {
		return fmt.Sprintf("api request failed with HTTP %d", e.StatusCode)
	}
	if e.Code == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound
}

func (c *Client) CreateRun(ctx context.Context, req CreateRunRequest) (CreateRunResponse, error) {
	var out CreateRunResponse
	err := c.doJSON(ctx, http.MethodPost, "/runs", req, &out)
	return out, err
}

func (c *Client) GetRun(ctx context.Context, runID string) (model.Run, error) {
	var out model.Run
	err := c.doJSON(ctx, http.MethodGet, "/runs/"+url.PathEscape(runID), nil, &out)
	return out, err
}

func (c *Client) ListRuns(ctx context.Context, limit int, status string) (ListRunsResponse, error) {
	values := url.Values{"limit": {fmt.Sprintf("%d", limit)}}
	if strings.TrimSpace(status) != "" {
		values.Set("status", strings.ToUpper(strings.TrimSpace(status)))
	}
	var out ListRunsResponse
	err := c.doJSON(ctx, http.MethodGet, "/runs?"+values.Encode(), nil, &out)
	return out, err
}

func (c *Client) StopRun(ctx context.Context, runID string) (StopRunResponse, error) {
	var out StopRunResponse
	err := c.doJSON(ctx, http.MethodPost, "/runs/"+url.PathEscape(runID)+"/stop", nil, &out)
	return out, err
}

func (c *Client) Validate(ctx context.Context, req CreateRunRequest) (ValidationResponse, error) {
	var out ValidationResponse
	payload, err := encodeBody(req)
	if err != nil {
		return out, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url("/validate"), payload)
	if err != nil {
		return out, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	c.authorize(httpReq)
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnprocessableEntity {
		return out, json.NewDecoder(resp.Body).Decode(&out)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return out, decodeAPIError(resp)
	}
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

func (c *Client) ReportJSON(ctx context.Context, runID string) (json.RawMessage, error) {
	return c.doBytes(ctx, http.MethodGet, "/runs/"+url.PathEscape(runID)+"/report", nil)
}

func (c *Client) ReportHTML(ctx context.Context, runID string) ([]byte, error) {
	return c.doBytes(ctx, http.MethodGet, "/runs/"+url.PathEscape(runID)+"/report.html", nil)
}

func (c *Client) Stream(ctx context.Context, runID string, events chan<- StreamEvent) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url("/runs/"+url.PathEscape(runID)+"/stream"), nil)
	if err != nil {
		return err
	}
	c.authorize(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeAPIError(resp)
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var eventName string
	var data bytes.Buffer
	emit := func() error {
		if eventName == "" && data.Len() == 0 {
			return nil
		}
		ev := StreamEvent{Type: eventName}
		switch eventName {
		case "metrics":
			if err := json.Unmarshal(data.Bytes(), &ev.Metrics); err != nil {
				return err
			}
		case "done":
			if err := json.Unmarshal(data.Bytes(), &ev.Done); err != nil {
				return err
			}
		default:
			eventName = ""
			data.Reset()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case events <- ev:
		}
		eventName = ""
		data.Reset()
		return nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := emit(); err != nil {
				return err
			}
			continue
		}
		switch {
		case strings.HasPrefix(line, "event:"):
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return emit()
}

func (c *Client) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	payload, err := encodeBody(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.url(endpoint), payload)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	c.authorize(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeAPIError(resp)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) doBytes(ctx context.Context, method, endpoint string, body any) ([]byte, error) {
	payload, err := encodeBody(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.url(endpoint), payload)
	if err != nil {
		return nil, err
	}
	c.authorize(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, decodeAPIError(resp)
	}
	return io.ReadAll(resp.Body)
}

func encodeBody(body any) (io.Reader, error) {
	if body == nil {
		return nil, nil
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return nil, err
	}
	return &buf, nil
}

func (c *Client) url(endpoint string) string {
	parsed, _ := url.Parse(c.baseURL)
	if i := strings.Index(endpoint, "?"); i >= 0 {
		parsed.Path = path.Join(parsed.Path, endpoint[:i])
		parsed.RawQuery = endpoint[i+1:]
		return parsed.String()
	}
	if strings.HasPrefix(endpoint, "?") {
		parsed.RawQuery = endpoint[1:]
		return parsed.String()
	}
	parsed.Path = path.Join(parsed.Path, endpoint)
	if strings.HasSuffix(endpoint, "/") && !strings.HasSuffix(parsed.Path, "/") {
		parsed.Path += "/"
	}
	return parsed.String()
}

func (c *Client) authorize(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
}

func decodeAPIError(resp *http.Response) error {
	apiErr := &APIError{StatusCode: resp.StatusCode}
	var body model.ErrorBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err == nil {
		apiErr.Code = body.Error.Code
		apiErr.Message = body.Error.Message
	}
	if apiErr.Message == "" {
		apiErr.Message = resp.Status
	}
	return apiErr
}
