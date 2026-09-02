package nexusoperation

import (
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
)

func testFrontendHandlerForValidation(t *testing.T) *frontendHandler {
	t.Helper()
	messages, err := validationMessages()
	require.NoError(t, err)
	protoValidator, err := protovalidate.New(
		protovalidate.WithMessages(messages...),
		protovalidate.WithDisableLazy(),
	)
	require.NoError(t, err)
	dynamicValidator := newDynamicValidator(testDynamicConfig())
	return &frontendHandler{
		protoValidator:   protoValidator,
		dynamicValidator: dynamicValidator,
	}
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
	h := testFrontendHandlerForValidation(t)

	t.Run("valid", func(t *testing.T) {
		req := &workflowservice.StartNexusOperationExecutionRequest{
			Namespace: "ns",
			Endpoint:  "endpoint",
			Service:   "service",
			Operation: "operation",
		}
		require.NoError(t, h.validateProto(req))
	})
	t.Run("invalid surfaces as InvalidArgument", func(t *testing.T) {
		req := &workflowservice.StartNexusOperationExecutionRequest{} // missing every required field
		err := h.validateProto(req)
		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArg)
	})
	t.Run("dynamic rule violation also surfaces as InvalidArgument", func(t *testing.T) {
		req := &workflowservice.StartNexusOperationExecutionRequest{
			Namespace: "ns",
			Endpoint:  "endpoint",
			Service:   strings.Repeat("a", 1001), // exceeds the 1000-char stub limit
			Operation: "operation",
		}
		err := h.validateProto(req)
		var invalidArg *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArg)
	})
}
