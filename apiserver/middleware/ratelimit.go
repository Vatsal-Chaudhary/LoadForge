package middleware

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type RateLimiter struct {
	rate  rate.Limit
	burst int
	mu    sync.Mutex
	keys  map[string]*rate.Limiter
}

func NewRateLimiter(perSecond float64, burst int) *RateLimiter {
	if perSecond <= 0 {
		perSecond = 10
	}
	if burst <= 0 {
		burst = 20
	}
	return &RateLimiter{rate: rate.Limit(perSecond), burst: burst, keys: make(map[string]*rate.Limiter)}
}

func (l *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := APIKeyFromContext(r.Context())
		if !ok {
			writeError(w, http.StatusUnauthorized, "unauthorized", "a valid bearer token is required")
			return
		}
		l.mu.Lock()
		bucket := l.keys[key.ID]
		if bucket == nil {
			bucket = rate.NewLimiter(l.rate, l.burst)
			l.keys[key.ID] = bucket
		}
		allowed := bucket.Allow()
		l.mu.Unlock()
		if !allowed {
			retry := time.Duration(float64(time.Second) / float64(l.rate))
			seconds := max(1, int(retry.Round(time.Second)/time.Second))
			w.Header().Set("Retry-After", strconv.Itoa(seconds))
			writeError(w, http.StatusTooManyRequests, "rate_limit_exceeded", "API key rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}
