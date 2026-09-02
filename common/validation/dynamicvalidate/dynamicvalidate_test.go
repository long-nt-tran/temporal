package dynamicvalidate_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/validation/dynamicvalidate"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func lenLimit(limit int, msg string) dynamicvalidate.Rule {
	return dynamicvalidate.GlobalStringRule(func(value string) error {
		if len(value) > limit {
			return fmt.Errorf("%s", msg)
		}
		return nil
	})
}

func nsLenLimit(limit int, msg string) dynamicvalidate.Rule {
	return dynamicvalidate.NamespaceStringRule(func(_ string, value string) error {
		if len(value) > limit {
			return fmt.Errorf("%s", msg)
		}
		return nil
	})
}

func testRunner(t *testing.T) *dynamicvalidate.Runner {
	t.Helper()
	return dynamicvalidate.New(dynamicvalidate.Registries{
		Rules: map[protoreflect.ExtensionType]dynamicvalidate.Rule{
			temporalvalidatepb.E_DynamicGlobalMaxIdLength:               lenLimit(10, "operation_id exceeds the max length limit"),
			temporalvalidatepb.E_DynamicNamespaceMaxServiceNameLength:   nsLenLimit(20, "service exceeds the namespace's max length limit"),
			temporalvalidatepb.E_DynamicNamespaceMaxOperationNameLength: nsLenLimit(20, "operation exceeds the namespace's max length limit"),
			temporalvalidatepb.E_DynamicNamespaceMaxReasonLength:        nsLenLimit(20, "reason exceeds the namespace's max length limit"),
			temporalvalidatepb.E_DynamicNamespaceMaxPayloadSize: dynamicvalidate.NamespaceMessageRule(&commonpb.Payload{}, func(_ string, payload *commonpb.Payload) error {
				if payload.Size() > 5 {
					return errors.New("input exceeds the namespace's payload size limit")
				}
				return nil
			}),
			temporalvalidatepb.E_DynamicNamespaceMaxUserMetadataSummarySize: dynamicvalidate.NamespaceMessageRule(&sdkpb.UserMetadata{}, func(_ string, metadata *sdkpb.UserMetadata) error {
				if metadata.GetSummary().Size() > 5 {
					return errors.New("user_metadata.summary exceeds the namespace's max size limit")
				}
				return nil
			}),
			temporalvalidatepb.E_DynamicNamespaceMaxUserMetadataDetailsSize: dynamicvalidate.NamespaceMessageRule(&sdkpb.UserMetadata{}, func(_ string, metadata *sdkpb.UserMetadata) error {
				if metadata.GetDetails().Size() > 5 {
					return errors.New("user_metadata.details exceeds the namespace's max size limit")
				}
				return nil
			}),
		},
	})
}

func TestCheckMessage_FieldLevel_GlobalLimit(t *testing.T) {
	r := testRunner(t)

	t.Run("within limit", func(t *testing.T) {
		req := &workflowservice.DeleteNexusOperationExecutionRequest{OperationId: "short"}
		require.NoError(t, r.CheckMessage(req))
	})
	t.Run("exceeds limit", func(t *testing.T) {
		req := &workflowservice.DeleteNexusOperationExecutionRequest{OperationId: "way-too-long-for-the-limit"}
		err := r.CheckMessage(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "operation_id exceeds the max length limit")
	})
	t.Run("optional on Start, empty is fine", func(t *testing.T) {
		req := &workflowservice.StartNexusOperationExecutionRequest{Namespace: "ns", Service: "svc", Operation: "op", Endpoint: "ep"}
		require.NoError(t, r.CheckMessage(req))
	})
	t.Run("optional on Start, still bounded if set", func(t *testing.T) {
		req := &workflowservice.StartNexusOperationExecutionRequest{OperationId: "way-too-long-for-the-limit"}
		err := r.CheckMessage(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "operation_id exceeds the max length limit")
	})
	t.Run("rule is reused by request_id", func(t *testing.T) {
		req := &workflowservice.StartNexusOperationExecutionRequest{RequestId: "way-too-long-for-the-limit"}
		require.Error(t, r.CheckMessage(req))
	})
	t.Run("rule is reused by identity", func(t *testing.T) {
		req := &workflowservice.StartNexusOperationExecutionRequest{Identity: "way-too-long-for-the-limit"}
		require.Error(t, r.CheckMessage(req))
	})
}

func TestCheckMessage_MessageLevel_NamespaceLimit(t *testing.T) {
	r := testRunner(t)

	valid := func() *workflowservice.StartNexusOperationExecutionRequest {
		return &workflowservice.StartNexusOperationExecutionRequest{
			Namespace: "ns", Endpoint: "ep", Service: "svc", Operation: "op",
		}
	}

	t.Run("valid, unset input/user_metadata", func(t *testing.T) {
		require.NoError(t, r.CheckMessage(valid()))
	})
	t.Run("service too long for namespace", func(t *testing.T) {
		req := valid()
		req.Service = "this-service-name-is-way-too-long-for-the-limit"
		err := r.CheckMessage(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "service exceeds the namespace's max length limit")
	})
	t.Run("operation too long for namespace", func(t *testing.T) {
		req := valid()
		req.Operation = "this-operation-name-is-way-too-long-for-the-limit"
		err := r.CheckMessage(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "operation exceeds the namespace's max length limit")
	})
	t.Run("payload too big for namespace", func(t *testing.T) {
		req := valid()
		req.Input = &commonpb.Payload{Data: []byte("this-is-more-than-five-bytes")}
		err := r.CheckMessage(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "input exceeds the namespace's payload size limit")
	})
	t.Run("payload limit includes metadata", func(t *testing.T) {
		req := valid()
		req.Input = &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}}
		require.Error(t, r.CheckMessage(req))
	})
	t.Run("user_metadata summary too big for namespace", func(t *testing.T) {
		req := valid()
		req.UserMetadata = &sdkpb.UserMetadata{Summary: &commonpb.Payload{Data: []byte("way-too-big")}}
		err := r.CheckMessage(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "user_metadata.summary exceeds the namespace's max size limit")
	})
	t.Run("user_metadata details too big for namespace", func(t *testing.T) {
		req := valid()
		req.UserMetadata = &sdkpb.UserMetadata{Details: &commonpb.Payload{Data: []byte("way-too-big")}}
		err := r.CheckMessage(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "user_metadata.details exceeds the namespace's max size limit")
	})
	t.Run("reason too long, on Terminate", func(t *testing.T) {
		req := &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace: "ns", OperationId: "op", Reason: "this-reason-is-way-too-long-for-the-limit",
		}
		err := r.CheckMessage(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "reason exceeds the namespace's max length limit")
	})
	t.Run("reason too long, on RequestCancel", func(t *testing.T) {
		req := &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace: "ns", OperationId: "op", Reason: "this-reason-is-way-too-long-for-the-limit",
		}
		err := r.CheckMessage(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "reason exceeds the namespace's max length limit")
	})
}

func TestCheckMessage_UnregisteredKey_IsABugNotAViolation(t *testing.T) {
	// A runner missing a key that a real annotated message actually
	// references should surface a plain error, not a *ValidationError.
	r := dynamicvalidate.New(dynamicvalidate.Registries{})

	err := r.CheckMessage(&workflowservice.DeleteNexusOperationExecutionRequest{OperationId: "x"})
	require.Error(t, err)
	var valErr *dynamicvalidate.ValidationError
	require.NotErrorAs(t, err, &valErr, "expected a plain error, not *ValidationError")
	require.Contains(t, err.Error(), "no implementation for temporalvalidate.v1.dynamic_global_max_id_length")
}

func TestPrecompile(t *testing.T) {
	r := testRunner(t)
	// Zero-value messages: rule pass/fail doesn't matter, only that every
	// key referenced actually resolves.
	err := r.Precompile(
		&workflowservice.DeleteNexusOperationExecutionRequest{},
		&workflowservice.DescribeNexusOperationExecutionRequest{},
		&workflowservice.PollNexusOperationExecutionRequest{},
		&workflowservice.RequestCancelNexusOperationExecutionRequest{},
		&workflowservice.TerminateNexusOperationExecutionRequest{},
		&workflowservice.StartNexusOperationExecutionRequest{},
	)
	require.NoError(t, err)
}

func TestPrecompile_CatchesUnknownKey(t *testing.T) {
	r := dynamicvalidate.New(dynamicvalidate.Registries{})
	err := r.Precompile(&workflowservice.DeleteNexusOperationExecutionRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "DeleteNexusOperationExecutionRequest")
}

func TestPrecompile_CatchesMissingImplementation(t *testing.T) {
	r := dynamicvalidate.New(dynamicvalidate.Registries{
		Rules: map[protoreflect.ExtensionType]dynamicvalidate.Rule{
			temporalvalidatepb.E_DynamicGlobalMaxIdLength: dynamicvalidate.GlobalStringRule(nil),
		},
	})
	err := r.Precompile(&workflowservice.DeleteNexusOperationExecutionRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "dynamic_global_max_id_length has no implementation")
}
