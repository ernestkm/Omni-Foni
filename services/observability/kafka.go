package observability

import (
	"context"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// InjectKafka writes the W3C traceparent into record headers.
func InjectKafka(ctx context.Context, r *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, kafkaCarrier{headers: &r.Headers})
}

// ExtractKafka returns a context carrying the record's producer trace.
func ExtractKafka(ctx context.Context, r *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, kafkaCarrier{headers: &r.Headers})
}

// ConsumerSpan starts a span parented by the record's producer trace.
func ConsumerSpan(ctx context.Context, name, callID string) (context.Context, trace.Span) {
	return otel.Tracer("omnifoni/kafka").Start(ctx, name,
		trace.WithAttributes(attribute.String("call_id", callID)))
}

type kafkaCarrier struct{ headers *[]kgo.RecordHeader }

func (c kafkaCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaCarrier) Set(key, value string) {
	*c.headers = append(*c.headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c kafkaCarrier) Keys() []string {
	out := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		out = append(out, h.Key)
	}
	return out
}
