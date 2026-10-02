package validation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/validation/dynamicvalidate"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func responseFixture(t *testing.T, requestEnabled, responseEnabled bool, mutate func(*descriptorpb.FileDescriptorProto)) ([]protoreflect.ServiceDescriptor, []methodRegistration) {
	t.Helper()
	services, _ := registryFixture(t)
	file := protodesc.ToFileDescriptorProto(services[0].ParentFile())
	file.Service = file.Service[:1]
	file.Service[0].Method = file.Service[0].Method[:1]
	method := file.Service[0].Method[0]
	method.Options = &descriptorpb.MethodOptions{}
	if requestEnabled {
		proto.SetExtension(method.Options, temporalvalidatepb.E_RequestValidation, &temporalvalidatepb.RequestValidation{Enabled: proto.Bool(true)})
	}
	if responseEnabled {
		proto.SetExtension(method.Options, temporalvalidatepb.E_ResponseValidation, &temporalvalidatepb.ResponseValidation{Enabled: proto.Bool(true)})
	}
	field := func(name string, number int32) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(number), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: &descriptorpb.FieldOptions{}}
	}
	namespace := field("namespace", 1)
	label := field("label", 1)
	for _, target := range []*descriptorpb.FieldDescriptorProto{namespace, label} {
		proto.SetExtension(target.Options, validate.E_Field, &validate.FieldRules{Type: &validate.FieldRules_String_{String_: &validate.StringRules{MinLen: proto.Uint64(1)}}})
	}
	reason := field("reason", 2)
	proto.SetExtension(reason.Options, temporalvalidatepb.E_DynamicNamespaceMaxReasonLength, true)
	file.MessageType[0].Field = []*descriptorpb.FieldDescriptorProto{namespace}
	file.MessageType[1].Name = proto.String("Response")
	file.MessageType[1].Field = []*descriptorpb.FieldDescriptorProto{label, reason}
	method.OutputType = proto.String(".registrytest.Response")
	if mutate != nil {
		mutate(file)
	}
	descriptor, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	require.NoError(t, err)
	runner := dynamicvalidate.New(dynamicvalidate.Registries{Rules: map[protoreflect.ExtensionType]dynamicvalidate.Rule{
		temporalvalidatepb.E_DynamicNamespaceMaxReasonLength: dynamicvalidate.NamespaceStringRule(func(_ string, value string) error {
			if len(value) > 5 {
				return fmt.Errorf("private value: %s", value)
			}
			return nil
		}),
	}})
	return []protoreflect.ServiceDescriptor{descriptor.Services().Get(0)}, []methodRegistration{{
		Method: "/registrytest.First/Call", Request: dynamicpb.NewMessage(descriptor.Messages().Get(0)), Response: dynamicpb.NewMessage(descriptor.Messages().Get(1)), Dynamic: runner,
	}}
}

func setString(message proto.Message, field, value string) {
	m := message.ProtoReflect()
	m.Set(m.Descriptor().Fields().ByName(protoreflect.Name(field)), protoreflect.ValueOfString(value))
}

func TestResponseWarningsPreserveSuccessfulResult(t *testing.T) {
	for _, scenario := range []string{"valid", "static", "dynamic", "both", "missing namespace", "nil response", "wrong response", "validator panic"} {
		t.Run(scenario, func(t *testing.T) {
			services, registrations := responseFixture(t, false, true, nil)
			request := registrations[0].Request.(*dynamicpb.Message)
			response := registrations[0].Response.(*dynamicpb.Message)
			setString(request, "namespace", "customer")
			setString(response, "label", "ok")
			if scenario == "static" || scenario == "both" {
				setString(response, "label", "")
			}
			if scenario == "dynamic" || scenario == "both" {
				setString(response, "reason", "secret-value")
			}
			if scenario == "missing namespace" {
				setString(request, "namespace", "")
			}
			if scenario == "nil response" {
				response = nil
			}
			if scenario == "wrong response" {
				response = request
			}
			if scenario == "validator panic" {
				registrations[0].Dynamic = dynamicvalidate.New(dynamicvalidate.Registries{Rules: map[protoreflect.ExtensionType]dynamicvalidate.Rule{
					temporalvalidatepb.E_DynamicNamespaceMaxReasonLength: dynamicvalidate.NamespaceStringRule(func(string, string) error { panic("secret-value") }),
				}})
			}
			core, logs := observer.New(zap.WarnLevel)
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			t.Cleanup(func() { handler.StopCapture(capture) })
			registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewZapLogger(zap.New(core)), handler)
			require.NoError(t, err)
			result, err := ValidateCall(t.Context(), registry, registrations[0].Method, request, func(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error) { return response, nil })
			require.NoError(t, err)
			require.Equal(t, response, result)
			if response != nil {
				require.Same(t, response, result)
			}
			if scenario == "valid" {
				require.Zero(t, logs.Len())
				require.Empty(t, capture.Snapshot())
				return
			}
			require.Equal(t, 1, logs.Len())
			entry := logs.All()[0]
			require.Equal(t, zap.WarnLevel, entry.Level)
			require.Equal(t, registrations[0].Method, entry.ContextMap()["rpc"])
			require.NotContains(t, fmt.Sprint(entry.ContextMap()), "secret-value")
			if scenario == "both" {
				require.EqualValues(t, 2, entry.ContextMap()["violation_count"])
			}
			recordings := capture.SnapshotMetric("response_validation_failures")
			require.Len(t, recordings, 1)
			require.EqualValues(t, 1, recordings[0].Value)
			require.Equal(t, registrations[0].Method, recordings[0].Tags["operation"])
		})
	}
}

func TestResponseNamespaceSnapshotAndConfigChanges(t *testing.T) {
	services, registrations := responseFixture(t, true, true, nil)
	limit := 2
	var namespaces []string
	registrations[0].Dynamic = dynamicvalidate.New(dynamicvalidate.Registries{Rules: map[protoreflect.ExtensionType]dynamicvalidate.Rule{
		temporalvalidatepb.E_DynamicNamespaceMaxReasonLength: dynamicvalidate.NamespaceStringRule(func(namespace, value string) error {
			namespaces = append(namespaces, namespace)
			if len(value) > limit {
				return errors.New("too long")
			}
			return nil
		}),
	}})
	core, logs := observer.New(zap.WarnLevel)
	registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewZapLogger(zap.New(core)), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	request := registrations[0].Request.(*dynamicpb.Message)
	response := registrations[0].Response.(*dynamicpb.Message)
	setString(response, "label", "ok")
	setString(response, "reason", "abcd")
	for range 2 {
		setString(request, "namespace", "original")
		result, err := ValidateCall(t.Context(), registry, registrations[0].Method, request, func(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error) {
			setString(request, "namespace", "changed-by-handler")
			return response, nil
		})
		require.NoError(t, err)
		require.Same(t, response, result)
		limit = 10
	}
	require.Equal(t, []string{"original", "original"}, namespaces)
	require.Equal(t, 1, logs.Len())
}

func TestRequestFailureAndHandlerErrorSkipResponseChecks(t *testing.T) {
	for _, requestEnabled := range []bool{false, true} {
		t.Run(fmt.Sprint(requestEnabled), func(t *testing.T) {
			services, registrations := responseFixture(t, requestEnabled, true, nil)
			core, logs := observer.New(zap.WarnLevel)
			registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewZapLogger(zap.New(core)), metrics.NoopMetricsHandler)
			require.NoError(t, err)
			called := false
			handlerErr := errors.New("handler error")
			response := registrations[0].Response.(*dynamicpb.Message)
			result, err := ValidateCall(t.Context(), registry, registrations[0].Method, registrations[0].Request.(*dynamicpb.Message), func(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error) {
				called = true
				return response, handlerErr
			})
			if requestEnabled {
				require.False(t, called)
				require.Nil(t, result)
				require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
			} else {
				require.True(t, called)
				require.Same(t, response, result)
				require.ErrorIs(t, err, handlerErr)
			}
			require.Zero(t, logs.Len())
		})
	}
}

func TestResponseStartupChecks(t *testing.T) {
	for _, scenario := range []string{"invalid CEL", "missing namespace", "repeated namespace", "response namespace cannot substitute", "missing implementation"} {
		t.Run(scenario, func(t *testing.T) {
			services, registrations := responseFixture(t, false, true, func(file *descriptorpb.FileDescriptorProto) {
				if scenario == "invalid CEL" {
					file.MessageType[1].Options = &descriptorpb.MessageOptions{}
					proto.SetExtension(file.MessageType[1].Options, validate.E_Message, &validate.MessageRules{Cel: []*validate.Rule{{Id: proto.String("broken"), Message: proto.String("broken schema"), Expression: proto.String("this.missing > 0")}}})
				}
				if scenario == "missing namespace" || scenario == "response namespace cannot substitute" {
					if scenario == "response namespace cannot substitute" {
						file.MessageType[1].Field = append(file.MessageType[1].Field, file.MessageType[0].Field[0])
						file.MessageType[1].Field[2].Number = proto.Int32(3)
					}
					file.MessageType[0].Field = nil
				}
				if scenario == "repeated namespace" {
					file.MessageType[0].Field[0].Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
				}
			})
			switch scenario {
			case "missing implementation":
				registrations[0].Dynamic = dynamicvalidate.New(dynamicvalidate.Registries{})
			default:
			}
			_, err := NewRegistry(services, registrations[0].Dynamic, log.NewNoopLogger(), metrics.NoopMetricsHandler)
			require.Error(t, err)
		})
	}
}

func TestResponseDiagnosticsAreBoundedAndRedacted(t *testing.T) {
	var diagnostics responseDiagnostics
	for range 20 {
		diagnostics.add(`named_items["private\"]key"].reason`, "rule")
	}
	require.Equal(t, 20, diagnostics.count)
	require.Len(t, diagnostics.fields, maxResponseDiagnostics)
	for _, field := range diagnostics.fields {
		require.Equal(t, "named_items[*].reason", field)
	}
	require.NotContains(t, strings.Join(diagnostics.fields, ","), "private")
}

func TestResponseWarningsAreThrottledButMetricsAreNot(t *testing.T) {
	services, registrations := responseFixture(t, false, true, nil)
	core, logs := observer.New(zap.WarnLevel)
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	t.Cleanup(func() { handler.StopCapture(capture) })
	registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewZapLogger(zap.New(core)), handler)
	require.NoError(t, err)
	for range 100 {
		registry.reportResponse(registry.methods[registrations[0].Method], registrations[0].Response, "ns")
	}
	require.Positive(t, logs.Len())
	require.Less(t, logs.Len(), 100)
	require.Len(t, capture.SnapshotMetric("response_validation_failures"), 100)
}

func TestGlobalResponseRulesDoNotNeedRequestNamespace(t *testing.T) {
	services, registrations := responseFixture(t, false, true, func(file *descriptorpb.FileDescriptorProto) {
		file.MessageType[0].Field = nil
		options := &descriptorpb.FieldOptions{}
		proto.SetExtension(options, temporalvalidatepb.E_DynamicGlobalMaxIdLength, true)
		file.MessageType[1].Field[1].Options = options
	})
	registrations[0].Dynamic = dynamicvalidate.New(dynamicvalidate.Registries{Rules: map[protoreflect.ExtensionType]dynamicvalidate.Rule{
		temporalvalidatepb.E_DynamicGlobalMaxIdLength: dynamicvalidate.GlobalStringRule(func(value string) error {
			if len(value) > 5 {
				return errors.New("too long")
			}
			return nil
		}),
	}})
	registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	setString(registrations[0].Response, "label", "ok")
	diagnostics := registry.checkResponse(registry.methods[registrations[0].Method], registrations[0].Response, "")
	require.Empty(t, diagnostics.kind)
	setString(registrations[0].Response, "reason", "too-long")
	diagnostics = registry.checkResponse(registry.methods[registrations[0].Method], registrations[0].Response, "")
	require.Equal(t, "violation", diagnostics.kind)
}

func TestUnenrolledResponseIsNotCompiledOrChecked(t *testing.T) {
	services, registrations := responseFixture(t, true, false, func(file *descriptorpb.FileDescriptorProto) {
		file.MessageType[1].Options = &descriptorpb.MessageOptions{}
		proto.SetExtension(file.MessageType[1].Options, validate.E_Message, &validate.MessageRules{Cel: []*validate.Rule{{Id: proto.String("broken"), Message: proto.String("broken schema"), Expression: proto.String("this.missing > 0")}}})
	})
	core, logs := observer.New(zap.WarnLevel)
	registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewZapLogger(zap.New(core)), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	request := registrations[0].Request.(*dynamicpb.Message)
	setString(request, "namespace", "ns")
	result, err := ValidateCall(t.Context(), registry, registrations[0].Method, request, func(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error) {
		return registrations[0].Response.(*dynamicpb.Message), nil
	})
	require.NoError(t, err)
	require.Same(t, registrations[0].Response, result)
	require.Zero(t, logs.Len())
}

func TestNestedResponseUsesRequestNamespace(t *testing.T) {
	services, registrations := responseFixture(t, false, true, func(file *descriptorpb.FileDescriptorProto) {
		child := &descriptorpb.DescriptorProto{Name: proto.String("Child"), Field: file.MessageType[1].Field}
		file.MessageType = append(file.MessageType, child)
		file.MessageType[1].Field = []*descriptorpb.FieldDescriptorProto{{
			Name: proto.String("items"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".registrytest.Child"), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
		}}
	})
	registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	response := registrations[0].Response.ProtoReflect()
	items := response.Descriptor().Fields().ByName("items")
	child := dynamicpb.NewMessage(items.Message())
	setString(child, "label", "ok")
	setString(child, "reason", "too-long")
	response.Mutable(items).List().Append(protoreflect.ValueOfMessage(child))
	diagnostics := registry.checkResponse(registry.methods[registrations[0].Method], registrations[0].Response, "ns")
	require.Equal(t, "violation", diagnostics.kind)
	require.Equal(t, []string{"items[*].reason"}, diagnostics.fields)
	registrations[0].Response = dynamicpb.NewMessage(response.Descriptor())
	require.NoError(t, registrations[0].Dynamic.PrecompileResponse(registrations[0].Response, registrations[0].Request))
}

func TestResponseValidatorCannotMutateReturnedMessage(t *testing.T) {
	called := false
	services, registrations := responseFixture(t, false, true, func(file *descriptorpb.FileDescriptorProto) {
		file.Dependency = append(file.Dependency, "temporal/api/common/v1/message.proto")
		field := file.MessageType[1].Field[1]
		field.Name = proto.String("payload")
		field.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
		field.TypeName = proto.String(".temporal.api.common.v1.Payload")
		field.Options = &descriptorpb.FieldOptions{}
		proto.SetExtension(field.Options, temporalvalidatepb.E_DynamicNamespaceMaxPayloadSize, true)
	})
	registrations[0].Dynamic = dynamicvalidate.New(dynamicvalidate.Registries{Rules: map[protoreflect.ExtensionType]dynamicvalidate.Rule{
		temporalvalidatepb.E_DynamicNamespaceMaxPayloadSize: dynamicvalidate.NamespaceMessageRule[proto.Message](&commonpb.Payload{}, func(_ string, payload proto.Message) error {
			called = true
			message := payload.ProtoReflect()
			message.Get(message.Descriptor().Fields().ByName("data")).Bytes()[0] = 'x'
			return errors.New("validator mutated its argument")
		}),
	}})
	registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	request := registrations[0].Request.(*dynamicpb.Message)
	setString(request, "namespace", "ns")
	response := registrations[0].Response.(*dynamicpb.Message)
	setString(response, "label", "ok")
	payload := &commonpb.Payload{Data: []byte("original")}
	response.Set(response.Descriptor().Fields().ByName("payload"), protoreflect.ValueOfMessage(payload.ProtoReflect()))
	result, err := ValidateCall(t.Context(), registry, registrations[0].Method, request, func(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error) { return response, nil })
	require.NoError(t, err)
	require.Same(t, response, result)
	require.True(t, called)
	require.Equal(t, []byte("original"), payload.Data)
}
