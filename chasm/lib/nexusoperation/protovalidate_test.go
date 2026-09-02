package nexusoperation

import (
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
)

func TestProtovalidate_StartNexusOperationExecutionRequest(t *testing.T) {
	valid := func() *workflowservice.StartNexusOperationExecutionRequest {
		return &workflowservice.StartNexusOperationExecutionRequest{
			Namespace: "ns",
			Endpoint:  "endpoint",
			Service:   "service",
			Operation: "operation",
		}
	}

	t.Run("valid", func(t *testing.T) {
		require.NoError(t, protovalidate.Validate(valid()))
	})
	t.Run("request_id is not required to be a UUID", func(t *testing.T) {
		req := valid()
		req.RequestId = "request-id"
		require.NoError(t, protovalidate.Validate(req))
	})
	t.Run("missing namespace", func(t *testing.T) {
		req := valid()
		req.Namespace = ""
		require.Error(t, protovalidate.Validate(req))
	})
	t.Run("missing endpoint", func(t *testing.T) {
		req := valid()
		req.Endpoint = ""
		require.Error(t, protovalidate.Validate(req))
	})
	t.Run("missing service", func(t *testing.T) {
		req := valid()
		req.Service = ""
		require.Error(t, protovalidate.Validate(req))
	})
	t.Run("missing operation", func(t *testing.T) {
		req := valid()
		req.Operation = ""
		require.Error(t, protovalidate.Validate(req))
	})
}

func TestProtovalidate_PollNexusOperationExecutionRequest(t *testing.T) {
	valid := func() *workflowservice.PollNexusOperationExecutionRequest {
		return &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   "ns",
			OperationId: "op",
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
		}
	}

	t.Run("valid", func(t *testing.T) {
		require.NoError(t, protovalidate.Validate(valid()))
	})
	t.Run("missing operation_id", func(t *testing.T) {
		req := valid()
		req.OperationId = ""
		require.Error(t, protovalidate.Validate(req))
	})
	t.Run("wait_stage unspecified is normalized later", func(t *testing.T) {
		req := valid()
		req.WaitStage = enumspb.NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED
		require.NoError(t, protovalidate.Validate(req))
	})
	t.Run("run_id is validated later", func(t *testing.T) {
		req := valid()
		req.RunId = "not-a-uuid"
		require.NoError(t, protovalidate.Validate(req))
	})
	t.Run("empty run_id is valid", func(t *testing.T) {
		req := valid()
		req.RunId = ""
		require.NoError(t, protovalidate.Validate(req))
	})
}

func TestProtovalidate_DeleteNexusOperationExecutionRequest(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   "ns",
			OperationId: "op",
		}
		require.NoError(t, protovalidate.Validate(req))
	})
	t.Run("missing operation_id", func(t *testing.T) {
		req := &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace: "ns",
		}
		require.Error(t, protovalidate.Validate(req))
	})
}
