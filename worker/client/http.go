package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var variablePattern = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)

type Config struct {
	Timeout             time.Duration
	KeepAlive           time.Duration
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	MaxConnsPerHost     int
	TLSSkipVerify       bool
}

type Request struct {
	Method  string
	URL     string
	Headers map[string]string
	Body    string
	Vars    map[string]string
}

type Response struct {
	StatusCode int
	Body       []byte
	Headers    http.Header
	BytesRecv  int64
}

type HTTPClient struct {
	client *http.Client
}

func NewHTTPClient(cfg Config) *HTTPClient {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.KeepAlive <= 0 {
		cfg.KeepAlive = 30 * time.Second
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = 2048
	}
	if cfg.MaxIdleConnsPerHost <= 0 {
		cfg.MaxIdleConnsPerHost = 512
	}
	if cfg.MaxConnsPerHost <= 0 {
		cfg.MaxConnsPerHost = 0
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: cfg.KeepAlive,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: cfg.TLSSkipVerify}, //nolint:gosec
	}

	return &HTTPClient{client: &http.Client{
		Timeout:   cfg.Timeout,
		Transport: transport,
	}}
}

func (c *HTTPClient) Do(ctx context.Context, req Request) (*Response, error) {
	method := strings.ToUpper(strings.TrimSpace(req.Method))
	if method == "" {
		method = http.MethodGet
	}

	resolvedURL := Substitute(req.URL, req.Vars)
	resolvedBody := Substitute(req.Body, req.Vars)

	httpReq, err := http.NewRequestWithContext(ctx, method, resolvedURL, bytes.NewBufferString(resolvedBody))
	if err != nil {
		return nil, err
	}
	for k, v := range req.Headers {
		httpReq.Header.Set(k, Substitute(v, req.Vars))
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	out := &Response{
		StatusCode: resp.StatusCode,
		Body:       body,
		Headers:    resp.Header.Clone(),
		BytesRecv:  int64(len(body)),
	}
	if readErr != nil {
		return out, readErr
	}
	return out, nil
}

func Substitute(input string, vars map[string]string) string {
	if input == "" || len(vars) == 0 {
		return input
	}
	return variablePattern.ReplaceAllStringFunc(input, func(match string) string {
		parts := variablePattern.FindStringSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		if val, ok := vars[parts[1]]; ok {
			return val
		}
		return match
	})
}

func JoinURL(baseURL, path string) string {
	if path == "" {
		return baseURL
	}
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return strings.TrimRight(baseURL, "/") + "/" + strings.TrimLeft(path, "/")
	}
	ref, err := url.Parse(path)
	if err != nil {
		return strings.TrimRight(baseURL, "/") + "/" + strings.TrimLeft(path, "/")
	}
	return base.ResolveReference(ref).String()
}
