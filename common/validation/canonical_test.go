package validation

import (
	"context"
	"strings"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestCanonicalMetadataPreservesStaticRequestAndResponseValidation(t *testing.T) {
	for _, rule := range []struct {
		option protoreflect.ExtensionType
		field  string
	}{
		{temporalvalidatepb.E_Namespace, "namespace"},
		{temporalvalidatepb.E_OperationId, "operation_id"},
	} {
		t.Run(rule.field, func(t *testing.T) {
			metadata, ok := proto.GetExtension(rule.option.TypeDescriptor().Options(), temporalvalidatepb.E_CanonicalRule).(*temporalvalidatepb.CanonicalRule)
			require.True(t, ok)
			require.Equal(t, []string{rule.field}, metadata.GetFieldNames())
			services, fixtures := responseFixture(t, true, true, func(file *descriptorpb.FileDescriptorProto) {
				for _, message := range file.MessageType {
					message.Field = message.Field[:1]
					message.Field[0].Name = proto.String(rule.field)
					stringRules := &validate.StringRules{MaxLen: proto.Uint64(255)}
					proto.SetExtension(stringRules, rule.option, true)
					proto.SetExtension(message.Field[0].Options, validate.E_Field, &validate.FieldRules{Type: &validate.FieldRules_String_{String_: stringRules}})
				}
			})
			core, logs := observer.New(zap.WarnLevel)
			registry, err := NewRegistry(services, nil, log.NewZapLogger(zap.New(core)), metrics.NoopMetricsHandler)
			require.NoError(t, err)
			request := fixtures[0].Request.(*dynamicpb.Message)
			response := fixtures[0].Response.(*dynamicpb.Message)
			called := false
			handler := func(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error) {
				called = true
				return response, nil
			}
			_, err = ValidateCall(t.Context(), registry, fixtures[0].Method, request, handler)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
			require.False(t, called)
			setString(request, rule.field, "valid")
			result, err := ValidateCall(t.Context(), registry, fixtures[0].Method, request, handler)
			require.NoError(t, err)
			require.True(t, called)
			require.Same(t, response, result)
			require.Equal(t, 1, logs.FilterMessage("response validation failed").Len())
			setString(response, rule.field, "valid")
			_, err = ValidateCall(t.Context(), registry, fixtures[0].Method, request, handler)
			require.NoError(t, err)
			require.Equal(t, 1, logs.FilterMessage("response validation failed").Len())
			setString(request, rule.field, strings.Repeat("x", 256))
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(registry.Validate(fixtures[0].Method, request)).Code())
		})
	}
}
