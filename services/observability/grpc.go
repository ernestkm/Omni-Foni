package observability

import (
	"context"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const requestIDHeader = "x-request-id"

// ServerOptions returns the otelgrpc stats handler plus request-id propagation.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(serverRequestID),
	}
}

// ClientOptions returns the otelgrpc stats handler plus request-id propagation.
func ClientOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(clientRequestID),
	}
}

func serverRequestID(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if id := incomingRequestID(ctx); id != "" {
		ctx = WithRequestID(ctx, id)
	} else {
		ctx = WithRequestID(ctx, NewRequestID())
	}
	return handler(ctx, req)
}

func clientRequestID(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if id := RequestIDFrom(ctx); id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, requestIDHeader, id)
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func incomingRequestID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if v := md.Get(requestIDHeader); len(v) > 0 {
		return v[0]
	}
	return ""
}
