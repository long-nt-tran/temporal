package validation

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type callContextKey struct{}

type activeCall struct {
	registry *Registry
	method   string
	request  proto.Message
}

func (r *Registry) Intercept(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if _, enabled := r.methods[info.FullMethod]; !enabled {
		return handler(ctx, request)
	}
	message, ok := request.(proto.Message)
	if !ok {
		return nil, serviceerror.NewInternal("RPC validation: request is not a protobuf message")
	}
	return ValidateCall(ctx, r, info.FullMethod, message, func(ctx context.Context, _ proto.Message) (any, error) { return handler(ctx, request) })
}
