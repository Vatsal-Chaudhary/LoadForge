//go:build integration

package apiserver_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/vatsalchaudhary/loadforge/apiserver/handlers"
	"github.com/vatsalchaudhary/loadforge/apiserver/middleware"
	"github.com/vatsalchaudhary/loadforge/apiserver/store"
)

type integrationOrchestrator struct{}

func (integrationOrchestrator) Submit(context.Context, string, json.RawMessage) (string, error) {
	return "PENDING", nil
}
func (integrationOrchestrator) Stop(context.Context, string) (string, error) {
	return "DRAINING", nil
}
func (integrationOrchestrator) Ready(context.Context) error { return nil }

type integrationStream struct{}

func (integrationStream) Serve(http.ResponseWriter, *http.Request, string) error { return nil }

func TestPostgresRedisHandlerRoundTrip(t *testing.T) {
	postgresDSN := os.Getenv("TEST_POSTGRES_DSN")
	redisAddr := os.Getenv("TEST_REDIS_ADDR")
	if postgresDSN == "" || redisAddr == "" {
		t.Skip("TEST_POSTGRES_DSN and TEST_REDIS_ADDR are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const pepper = "integration-pepper"
	persistence, err := store.Open(ctx, postgresDSN, redisAddr, pepper)
	if err != nil {
		t.Fatal(err)
	}
	defer persistence.Close()
	db, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	token := uuid.NewString()
	keyID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO api_keys(id, name, token_hash) VALUES ($1, 'integration', $2)`,
		keyID, store.TokenHash(token, pepper)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM test_runs WHERE created_by = $1", keyID)
		_, _ = db.ExecContext(context.Background(), "DELETE FROM api_keys WHERE id = $1", keyID)
	})
	api := handlers.New(persistence, integrationOrchestrator{}, integrationStream{})
	handler := middleware.Auth(persistence, api.Routes())
	plan := `{"name":"it","version":"1","target":{"base_url":"https://example.com"},"load_profile":{"type":"constant","initial_workers":1},"workers":{"virtual_users_per_worker":1},"scenarios":[{"name":"s","weight":1,"steps":[{"name":"x","method":"GET","path":"/"}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/runs", strings.NewReader(`{"test_plan":`+plan+`}`))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create status=%d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	rc := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rc.Close()
	prefix := "loadforge:run:" + created.RunID
	if err := rc.MSet(ctx, prefix+":rps", "12.5", prefix+":p95", "42", prefix+":error_rate", ".01").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rc.Del(context.Background(), prefix+":rps", prefix+":p95", prefix+":error_rate").Err() })
	req = httptest.NewRequest(http.MethodGet, "/runs/"+created.RunID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"rps":12.5`) {
		t.Fatalf("get status=%d body=%s", rec.Code, rec.Body.String())
	}
}
