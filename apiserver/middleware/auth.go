package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/vatsalchaudhary/loadforge/apiserver/model"
)

type Authenticator interface {
	Authenticate(context.Context, string) (model.APIKey, error)
}

type contextKey string

const apiKeyContextKey contextKey = "api-key"

func APIKeyFromContext(ctx context.Context) (model.APIKey, bool) {
	key, ok := ctx.Value(apiKeyContextKey).(model.APIKey)
	return key, ok
}

func Auth(auth Authenticator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := r.Header.Get("Authorization")
		if !strings.HasPrefix(value, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "unauthorized", "a valid bearer token is required")
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
		if _, err := uuid.Parse(token); err != nil {
			writeError(w, http.StatusUnauthorized, "unauthorized", "a valid bearer token is required")
			return
		}
		key, err := auth.Authenticate(r.Context(), token)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "unauthorized", "a valid bearer token is required")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), apiKeyContextKey, key)))
	})
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(model.ErrorBody{Error: model.APIError{Code: code, Message: message}})
}
