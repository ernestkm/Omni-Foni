package observability

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Middleware adds an HTTP server span and request-id propagation/echo.
func Middleware(next http.Handler) http.Handler {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			id = NewRequestID()
		}
		w.Header().Set(requestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(WithRequestID(r.Context(), id)))
	})
	return otelhttp.NewHandler(h, "http")
}
