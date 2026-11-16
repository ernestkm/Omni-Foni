package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

var sensitiveKeys = map[string]bool{
	"phone": true, "msisdn": true, "caller": true, "callee": true,
	"destination": true,
	"email":       true, "token": true, "password": true, "authorization": true,
	"secret": true,
}

// SetupLogging installs the JSON handler on stdout with correlation and redaction.
func SetupLogging(service string) {
	slog.SetDefault(slog.New(NewHandler(slog.NewJSONHandler(os.Stdout, nil), service)))
}

// NewHandler wraps inner so every record carries the service name and any
// trace/request correlation in ctx, with sensitive values masked.
func NewHandler(inner slog.Handler, service string) slog.Handler {
	return &handler{inner: inner, service: service}
}

type handler struct {
	inner   slog.Handler
	service string
}

func (h *handler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *handler) Handle(ctx context.Context, r slog.Record) error {
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(redactAttr(a))
		return true
	})
	out.AddAttrs(slog.String("service", h.service))
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		out.AddAttrs(slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()))
	}
	if id := RequestIDFrom(ctx); id != "" {
		out.AddAttrs(slog.String("request_id", id))
	}
	return h.inner.Handle(ctx, out)
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	red := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		red = append(red, redactAttr(a))
	}
	return &handler{inner: h.inner.WithAttrs(red), service: h.service}
}

func (h *handler) WithGroup(name string) slog.Handler {
	return &handler{inner: h.inner.WithGroup(name), service: h.service}
}

func redactAttr(a slog.Attr) slog.Attr {
	if sensitiveKeys[strings.ToLower(a.Key)] {
		return slog.String(a.Key, Mask(a.Value.String()))
	}
	if a.Value.Kind() == slog.KindGroup {
		attrs := a.Value.Group()
		out := make([]slog.Attr, 0, len(attrs))
		for _, ga := range attrs {
			out = append(out, redactAttr(ga))
		}
		any := make([]any, len(out))
		for i := range out {
			any[i] = out[i]
		}
		return slog.Group(a.Key, any...)
	}
	return a
}

// Mask keeps a short prefix and suffix so logs stay correlatable without
// exposing the value. Values of nine characters or fewer are fully masked.
func Mask(v string) string {
	if len(v) <= 9 {
		return "***"
	}
	return v[:5] + "***" + v[len(v)-4:]
}
