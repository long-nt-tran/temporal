package validation

import (
	"context"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/validation/dynamicvalidate"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func registryFixture(t *testing.T) ([]protoreflect.ServiceDescriptor, []methodRegistration) {
	t.Helper()
	enabled := &descriptorpb.MethodOptions{}
	proto.SetExtension(enabled, temporalvalidatepb.E_RequestValidation, &temporalvalidatepb.RequestValidation{Enabled: proto.Bool(true)})
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("registry_test.proto"), Package: proto.String("registrytest"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Request")}, {Name: proto.String("Other")}},
		Service: []*descriptorpb.ServiceDescriptorProto{
			{Name: proto.String("First"), Method: []*descriptorpb.MethodDescriptorProto{
				{Name: proto.String("Call"), InputType: proto.String(".registrytest.Request"), OutputType: proto.String(".registrytest.Request"), Options: enabled},
				{Name: proto.String("Legacy"), InputType: proto.String(".registrytest.Request"), OutputType: proto.String(".registrytest.Request")},
			}},
			{Name: proto.String("Second"), Method: []*descriptorpb.MethodDescriptorProto{
				{Name: proto.String("Call"), InputType: proto.String(".registrytest.Request"), OutputType: proto.String(".registrytest.Request"), Options: enabled},
			}},
		},
	}, protoregistry.GlobalFiles)
	require.NoError(t, err)
	runner := dynamicvalidate.New(dynamicvalidate.Registries{})
	return []protoreflect.ServiceDescriptor{file.Services().Get(0), file.Services().Get(1)}, []methodRegistration{
		{Method: "/registrytest.First/Call", Request: dynamicpb.NewMessage(file.Messages().Get(0)), Dynamic: runner},
		{Method: "/registrytest.Second/Call", Request: dynamicpb.NewMessage(file.Messages().Get(0)), Dynamic: runner},
	}
}

func TestRegistryDiscoversEnrolledMethodsAcrossServices(t *testing.T) {
	services, registrations := registryFixture(t)
	registry, err := NewRegistry(services, nil, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	for _, registration := range registrations {
		require.NoError(t, registry.Validate(registration.Method, registration.Request))
	}
	require.NoError(t, registry.Validate("/registrytest.First/Legacy", nil))
	_, err = NewRegistry(append(services, services[0]), nil, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.ErrorContains(t, err, "duplicate RPC")
}

func TestNewMethodRequiresSharedBoundaryRegeneration(t *testing.T) {
	services, _ := registryFixture(t)
	require.ErrorContains(t, checkBoundaryInventory(services, map[string]struct{}{}), "has no generated boundary")
	require.NoError(t, checkBoundaryInventory(APIServices(), generatedUnaryMethods))
}

func TestValidateCallStopsInvalidRequests(t *testing.T) {
	services, registrations := registryFixture(t)
	registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	called := false
	handler := func(_ context.Context, _ *dynamicpb.Message) (string, error) { called = true; return "ok", nil }
	var request *dynamicpb.Message
	_, err = ValidateCall(t.Context(), registry, registrations[0].Method, request, handler)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	require.False(t, called)
	result, err := ValidateCall(t.Context(), registry, registrations[0].Method, registrations[0].Request.(*dynamicpb.Message), handler)
	require.NoError(t, err)
	require.Equal(t, "ok", result)
	require.True(t, called)
	require.NoError(t, registry.Validate("/unknown/Call", registrations[0].Request))
	wrong := dynamicpb.NewMessage(services[0].ParentFile().Messages().ByName("Other"))
	require.Equal(t, codes.Internal, serviceerror.ToStatus(registry.Validate(registrations[0].Method, wrong)).Code())
}

func TestStaticRequestAndResponseNeedNoBindings(t *testing.T) {
	services, fixtures := responseFixture(t, true, true, func(file *descriptorpb.FileDescriptorProto) {
		file.MessageType[1].Field[1].Options = nil
	})
	registry, err := NewRegistry(services, nil, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	request := fixtures[0].Request.(*dynamicpb.Message)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(registry.Validate(fixtures[0].Method, request)).Code())
	setString(request, "namespace", "ns")
	require.NoError(t, registry.Validate(fixtures[0].Method, request))
	result, err := ValidateCall(t.Context(), registry, fixtures[0].Method, request, func(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error) {
		return fixtures[0].Response.(*dynamicpb.Message), nil
	})
	require.NoError(t, err)
	require.Same(t, fixtures[0].Response, result)
}

func TestInvalidStaticCELFailsStartup(t *testing.T) {
	services, registrations := registryFixture(t)
	file := protodesc.ToFileDescriptorProto(services[0].ParentFile())
	file.MessageType[0].Options = &descriptorpb.MessageOptions{}
	proto.SetExtension(file.MessageType[0].Options, validate.E_Message, &validate.MessageRules{Cel: []*validate.Rule{{Id: proto.String("invalid"), Message: proto.String("invalid schema"), Expression: proto.String("this.missing > 0")}}})
	descriptor, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	require.NoError(t, err)
	for i := range registrations {
		registrations[i].Request = dynamicpb.NewMessage(descriptor.Messages().Get(0))
		services[i] = descriptor.Services().Get(i)
	}
	_, err = NewRegistry(services, registrations[0].Dynamic, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.ErrorContains(t, err, "compile static rules")
}

func TestInvalidCELInAbsentChildFailsStartup(t *testing.T) {
	services, registrations := registryFixture(t)
	file := protodesc.ToFileDescriptorProto(services[0].ParentFile())
	child := file.MessageType[1]
	child.Options = &descriptorpb.MessageOptions{}
	proto.SetExtension(child.Options, validate.E_Message, &validate.MessageRules{Cel: []*validate.Rule{{Id: proto.String("invalid"), Message: proto.String("invalid schema"), Expression: proto.String("this.missing > 0")}}})
	file.MessageType[0].Field = []*descriptorpb.FieldDescriptorProto{{Name: proto.String("child"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".registrytest.Other")}}
	descriptor, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	require.NoError(t, err)
	for i := range registrations {
		registrations[i].Request = dynamicpb.NewMessage(descriptor.Messages().Get(0))
		services[i] = descriptor.Services().Get(i)
	}
	_, err = NewRegistry(services, registrations[0].Dynamic, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.ErrorContains(t, err, "compile static rules")
}
