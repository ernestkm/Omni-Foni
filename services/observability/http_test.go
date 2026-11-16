package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddlewareRequestID(t *testing.T) {
	var got string
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = RequestIDFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/me", nil)
	req.Header.Set("x-request-id", "http-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got != "http-1" {
		t.Fatalf("context request id = %q, want http-1", got)
	}
	if rec.Header().Get("x-request-id") != "http-1" {
		t.Fatalf("response header = %q", rec.Header().Get("x-request-id"))
	}
}
