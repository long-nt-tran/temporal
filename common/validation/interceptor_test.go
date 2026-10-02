package validation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestInterceptorAndBoundaryCheckResponseOnlyOnce(t *testing.T) {
	services, fixtures := responseFixture(t, true, true, nil)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	registry, err := NewRegistry(services, fixtures[0].Dynamic, log.NewNoopLogger(), metricsHandler)
	require.NoError(t, err)
	request := fixtures[0].Request.(*dynamicpb.Message)
	response := fixtures[0].Response.(*dynamicpb.Message)
	setString(request, "namespace", "ns")
	result, err := registry.Intercept(t.Context(), request, &grpc.UnaryServerInfo{FullMethod: fixtures[0].Method}, func(ctx context.Context, _ any) (any, error) {
		return ValidateCall(ctx, registry, fixtures[0].Method, request, func(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error) { return response, nil })
	})
	require.NoError(t, err)
	require.Same(t, response, result)
	require.Len(t, capture.SnapshotMetric("response_validation_failures"), 1)
}

func TestBoundaryDoesNotSkipAnotherRequest(t *testing.T) {
	services, fixtures := responseFixture(t, true, false, nil)
	registry, err := NewRegistry(services, fixtures[0].Dynamic, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	request := fixtures[0].Request.(*dynamicpb.Message)
	setString(request, "namespace", "ns")
	_, err = registry.Intercept(t.Context(), request, &grpc.UnaryServerInfo{FullMethod: fixtures[0].Method}, func(ctx context.Context, _ any) (any, error) {
		invalid := dynamicpb.NewMessage(request.Descriptor())
		return ValidateCall(ctx, registry, fixtures[0].Method, invalid, func(context.Context, *dynamicpb.Message) (any, error) {
			t.Fatal("invalid request reached handler")
			return nil, nil
		})
	})
	require.Error(t, err)
}
