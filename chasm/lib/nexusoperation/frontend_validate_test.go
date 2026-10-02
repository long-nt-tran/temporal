package nexusoperation

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/validation"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
)

func testFrontendRegistry(t *testing.T) *validation.Registry {
	t.Helper()
	registry, err := validation.NewRegistry(validation.APIServices(), testDynamicValidator(testDynamicConfig()), log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	return registry
}

func TestValidationErrorsIncludeFieldDetails(t *testing.T) {
	registry := testFrontendRegistry(t)
	const method = "/temporal.api.workflowservice.v1.WorkflowService/StartNexusOperationExecution"
	for _, dynamic := range []bool{false, true} {
		t.Run(map[bool]string{false: "static", true: "dynamic"}[dynamic], func(t *testing.T) {
			request := &workflowservice.StartNexusOperationExecutionRequest{Namespace: "ns", Endpoint: "endpoint", Service: "service", Operation: "operation", OperationId: "id"}
			field := "operation_id"
			if dynamic {
				request.Service = strings.Repeat("s", 1001)
				field = "service"
			} else {
				request.OperationId = ""
			}
			err := registry.Validate(method, request)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
			details := serviceerror.ToStatus(err).Details()
			require.Len(t, details, 1)
			badRequest, ok := details[0].(*errdetails.BadRequest)
			require.True(t, ok)
			require.NotEmpty(t, badRequest.FieldViolations)
			require.Equal(t, field, badRequest.FieldViolations[0].Field)
			require.NotEmpty(t, badRequest.FieldViolations[0].Reason)
		})
	}
}

func TestMissingDynamicConfigFailsStartup(t *testing.T) {
	config := testDynamicConfig()
	config.MaxServiceNameLength = nil
	_, err := validation.NewRegistry(validation.APIServices(), testDynamicValidator(config), log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.ErrorContains(t, err, "dynamic_namespace_max_service_name_length has no implementation")
}

func testDynamicConfig() *Config {
	return &Config{
		MaxIDLengthLimit:           func() int { return 1000 },
		MaxServiceNameLength:       func(string) int { return 1000 },
		MaxOperationNameLength:     func(string) int { return 1000 },
		MaxReasonLength:            func(string) int { return 1000 },
		PayloadSizeLimit:           func(string) int { return 1000 },
		MaxUserMetadataSummarySize: func(string) int { return 1000 },
		MaxUserMetadataDetailsSize: func(string) int { return 1000 },
	}
}

func TestValidateProto(t *testing.T) {
	registry := testFrontendRegistry(t)
	const method = "/temporal.api.workflowservice.v1.WorkflowService/StartNexusOperationExecution"

	t.Run("valid", func(t *testing.T) {
		req := &workflowservice.StartNexusOperationExecutionRequest{
			Namespace:   "ns",
			Endpoint:    "endpoint",
			Service:     "service",
			Operation:   "operation",
			OperationId: "id",
		}
		require.NoError(t, registry.Validate(method, req))
	})
	t.Run("invalid surfaces as InvalidArgument", func(t *testing.T) {
		req := &workflowservice.StartNexusOperationExecutionRequest{} // missing every required field
		err := registry.Validate(method, req)
		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArg)
	})
	t.Run("dynamic rule violation also surfaces as InvalidArgument", func(t *testing.T) {
		req := &workflowservice.StartNexusOperationExecutionRequest{
			Namespace:   "ns",
			Endpoint:    "endpoint",
			Service:     strings.Repeat("a", 1001), // exceeds the 1000-char stub limit
			Operation:   "operation",
			OperationId: "id",
		}
		err := registry.Validate(method, req)
		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArg)
	})
}
