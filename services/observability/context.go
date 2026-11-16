package observability

import (
	"context"

	"github.com/google/uuid"
)

type requestIDKey struct{}

// NewRequestID returns a random UUID used to correlate one logical request.
func NewRequestID() string { return uuid.NewString() }

// WithRequestID stores the request id in ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestIDFrom returns the request id in ctx, or "" when absent.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}
