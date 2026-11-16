package observability

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestServerRequestIDGenerates(t *testing.T) {
	var got string
	_, err := serverRequestID(context.Background(), nil, &grpc.UnaryServerInfo{},
		func(ctx context.Context, req any) (any, error) {
			got = RequestIDFrom(ctx)
			return nil, nil
		})
	if err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	if got == "" {
		t.Fatal("no request id in context")
	}
}

func TestServerRequestIDEchoesIncoming(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "in-1"))
	var got string
	_, _ = serverRequestID(ctx, nil, &grpc.UnaryServerInfo{},
		func(ctx context.Context, req any) (any, error) {
			got = RequestIDFrom(ctx)
			return nil, nil
		})
	if got != "in-1" {
		t.Fatalf("request id = %q, want in-1", got)
	}
}

func TestClientRequestIDPropagates(t *testing.T) {
	ctx := WithRequestID(context.Background(), "out-1")
	var got string
	err := clientRequestID(ctx, "/x/Y", nil, nil, nil,
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			md, _ := metadata.FromOutgoingContext(ctx)
			if v := md.Get("x-request-id"); len(v) > 0 {
				got = v[0]
			}
			return nil
		})
	if err != nil {
		t.Fatalf("invoker: %v", err)
	}
	if got != "out-1" {
		t.Fatalf("outgoing request id = %q, want out-1", got)
	}
}
