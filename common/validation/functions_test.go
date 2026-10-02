package validation

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	"go.temporal.io/server/common/validation/dynamicvalidate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestFunctionsReceiveAbsentMessagesAndRootRequest(t *testing.T) {
	services, registrations := responseFixture(t, true, func(file *descriptorpb.FileDescriptorProto) {
		file.Dependency = append(file.Dependency, "temporal/api/common/v1/message.proto")
		options := &descriptorpb.FieldOptions{}
		proto.SetExtension(options, temporalvalidatepb.E_Payload, true)
		file.MessageType[0].Field = append(file.MessageType[0].Field, &descriptorpb.FieldDescriptorProto{
			Name: proto.String("payload"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
			TypeName: proto.String(".temporal.api.common.v1.Payload"), Options: options,
		})
	})
	request := registrations[0].Request
	calls := 0
	runner := dynamicvalidate.New(&testValidator{payload: func(ctx ValidationContext, value *commonpb.Payload) error {
		require.Same(t, request, ctx.Request)
		calls++
		if calls == 1 {
			require.Nil(t, value)
		} else {
			require.Equal(t, []byte("value"), value.Data)
		}
		return nil
	}})
	require.NoError(t, runner.Precompile(request))
	require.NoError(t, runner.CheckMessage(request))
	field := services[0].Methods().Get(0).Input().Fields().ByName("payload")
	payload := dynamicpb.NewMessage(field.Message())
	payload.Set(payload.Descriptor().Fields().ByName("data"), protoreflect.ValueOfBytes([]byte("value")))
	request.ProtoReflect().Set(field, protoreflect.ValueOfMessage(payload))
	require.NoError(t, runner.CheckMessage(request))
	require.Equal(t, 2, calls)
}

type symbolValidator struct {
	testValidator
	namespace func(ValidationContext, string) error
}

func (v *symbolValidator) ValidateNamespace(ctx ValidationContext, value string) error {
	return v.namespace(ctx, value)
}

func TestMultipleSymbolsUseTypedMethods(t *testing.T) {
	services, registrations := responseFixture(t, true, func(file *descriptorpb.FileDescriptorProto) {
		options := file.MessageType[0].Field[0].Options
		proto.SetExtension(options, temporalvalidatepb.E_Namespace, true)
		proto.SetExtension(options, temporalvalidatepb.E_IdLength, true)
		proto.SetExtension(options, temporalvalidatepb.E_CanonicalRuleIgnored, "explicit exception does not disable runtime checks")
	})
	request := registrations[0].Request
	setString(request, "namespace", "ns")
	var calls []string
	runner := dynamicvalidate.New(&symbolValidator{
		namespace: func(ctx ValidationContext, value string) error {
			require.Same(t, request, ctx.Request)
			require.Equal(t, "ns", value)
			calls = append(calls, "namespace")
			return nil
		},
		testValidator: testValidator{id: func(value string) error {
			require.Equal(t, "ns", value)
			calls = append(calls, "id_length")
			return nil
		}},
	})
	require.NotEmpty(t, services)
	require.NoError(t, runner.Precompile(request))
	require.NoError(t, runner.CheckMessage(request))
	require.NoError(t, runner.CheckMessage(request))
	require.Equal(t, []string{"id_length", "namespace", "id_length", "namespace"}, calls)
}

func TestFunctionsVisitMessageMapsAndKeepRootContext(t *testing.T) {
	_, registrations := responseFixture(t, true, func(file *descriptorpb.FileDescriptorProto) {
		child := &descriptorpb.DescriptorProto{Name: proto.String("Child"), Field: file.MessageType[1].Field}
		file.MessageType = append(file.MessageType, child)
		file.MessageType[1].NestedType = []*descriptorpb.DescriptorProto{{
			Name: proto.String("ItemsEntry"), Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("key"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
				{Name: proto.String("value"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".registrytest.Child")},
			},
		}}
		file.MessageType[1].Field = []*descriptorpb.FieldDescriptorProto{{
			Name: proto.String("items"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
			TypeName: proto.String(".registrytest.Response.ItemsEntry"), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
		}}
	})
	request := registrations[0].Request
	response := registrations[0].Response.ProtoReflect()
	field := response.Descriptor().Fields().ByName("items")
	for _, key := range []string{"b", "a"} {
		child := dynamicpb.NewMessage(field.MapValue().Message())
		setString(child, "reason", key)
		response.Mutable(field).Map().Set(protoreflect.ValueOfString(key).MapKey(), protoreflect.ValueOfMessage(child))
	}
	var values []string
	runner := dynamicvalidate.New(&testValidator{reason: func(ctx ValidationContext, value string) error {
		require.Same(t, request, ctx.Request)
		values = append(values, value)
		return errors.New("invalid")
	}})
	require.NoError(t, runner.Precompile(registrations[0].Response))
	err := runner.CheckResponse(registrations[0].Response, request)
	var violation *dynamicvalidate.ValidationError
	require.ErrorAs(t, err, &violation)
	require.Equal(t, []string{"a", "b"}, values)
	require.Equal(t, "items[\"a\"].reason", violation.Violations[0].FieldPath)
	require.Equal(t, "ValidateReasonLength", violation.Violations[0].RuleID)
}
