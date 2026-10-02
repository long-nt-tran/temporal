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
	apivalidation "go.temporal.io/api/temporalvalidate/validation"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/validation/dynamicvalidate"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func responseFixture(t *testing.T, enabled bool, mutate func(*descriptorpb.FileDescriptorProto)) ([]protoreflect.ServiceDescriptor, []methodRegistration) {
	t.Helper()
	services, _ := registryFixture(t)
	file := protodesc.ToFileDescriptorProto(services[0].ParentFile())
	file.Dependency = append(file.Dependency, "temporalvalidate/v1/annotations.proto", "temporalvalidate/v1/rules.proto")
	file.Service = file.Service[:1]
	file.Service[0].Method = file.Service[0].Method[:1]
	method := file.Service[0].Method[0]
	method.Options = &descriptorpb.MethodOptions{}
	if enabled {
		proto.SetExtension(method.Options, temporalvalidatepb.E_RpcValidation, &temporalvalidatepb.RPCValidation{Enabled: proto.Bool(true)})
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
	proto.SetExtension(reason.Options, temporalvalidatepb.E_ReasonLength, true)
	file.MessageType[0].Field = []*descriptorpb.FieldDescriptorProto{namespace}
	file.MessageType[1].Name = proto.String("Response")
	file.MessageType[1].Field = []*descriptorpb.FieldDescriptorProto{label, reason}
	method.OutputType = proto.String(".registrytest.Response")
	if mutate != nil {
		mutate(file)
	}
	descriptor, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	require.NoError(t, err)
	runner := dynamicvalidate.New(&testValidator{reason: func(ctx ValidationContext, value string) error {
		if testNamespace(ctx) == "" {
			return errors.New("request namespace is required")
		}
		if len(value) > 5 {
			return fmt.Errorf("private value: %s", value)
		}
		return nil
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
			services, registrations := responseFixture(t, true, func(file *descriptorpb.FileDescriptorProto) {
				if scenario == "missing namespace" {
					file.MessageType[0].Field[0].Options = nil
				}
			})
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
				registrations[0].Dynamic = dynamicvalidate.New(&testValidator{reason: func(ValidationContext, string) error { panic("secret-value") }})
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
	services, registrations := responseFixture(t, true, nil)
	limit := 2
	var namespaces []string
	registrations[0].Dynamic = dynamicvalidate.New(&testValidator{reason: func(ctx ValidationContext, value string) error {
		namespaces = append(namespaces, testNamespace(ctx))
		if len(value) > limit {
			return errors.New("too long")
		}
		return nil
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
	for _, invalidRequest := range []bool{false, true} {
		t.Run(fmt.Sprint(invalidRequest), func(t *testing.T) {
			services, registrations := responseFixture(t, true, nil)
			core, logs := observer.New(zap.WarnLevel)
			registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewZapLogger(zap.New(core)), metrics.NoopMetricsHandler)
			require.NoError(t, err)
			request := registrations[0].Request.(*dynamicpb.Message)
			if !invalidRequest {
				setString(request, "namespace", "ns")
			}
			called := false
			handlerErr := errors.New("handler error")
			response := registrations[0].Response.(*dynamicpb.Message)
			result, err := ValidateCall(t.Context(), registry, registrations[0].Method, request, func(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error) {
				called = true
				return response, handlerErr
			})
			if invalidRequest {
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
	for _, scenario := range []string{"invalid CEL", "missing implementation", "unknown function", "wrong type"} {
		t.Run(scenario, func(t *testing.T) {
			services, registrations := responseFixture(t, true, func(file *descriptorpb.FileDescriptorProto) {
				switch scenario {
				case "invalid CEL":
					file.MessageType[1].Options = &descriptorpb.MessageOptions{}
					proto.SetExtension(file.MessageType[1].Options, validate.E_Message, &validate.MessageRules{Cel: []*validate.Rule{{Id: proto.String("broken"), Message: proto.String("broken schema"), Expression: proto.String("this.missing > 0")}}})
				case "unknown function":
					file.Dependency = append(file.Dependency, "google/protobuf/descriptor.proto")
					symbol := &descriptorpb.FieldDescriptorProto{Name: proto.String("new_rule"), Number: proto.Int32(901100), Extendee: proto.String(".google.protobuf.FieldOptions"), Type: descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Options: &descriptorpb.FieldOptions{}}
					proto.SetExtension(symbol.Options, temporalvalidatepb.E_Rule, &temporalvalidatepb.Rule{Function: proto.String("ValidateNewFunction"), FieldType: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()})
					file.Extension = append(file.Extension, symbol)
					data := protowire.AppendTag(nil, 901100, protowire.VarintType)
					file.MessageType[1].Field[1].Options = &descriptorpb.FieldOptions{}
					file.MessageType[1].Field[1].Options.ProtoReflect().SetUnknown(protowire.AppendVarint(data, 1))
				case "wrong type":
					file.MessageType[1].Field[1].Type = descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum()
				default:
					require.Equal(t, "missing implementation", scenario)
				}
			})
			if scenario == "missing implementation" {
				registrations[0].Dynamic = dynamicvalidate.New(nil)
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
	services, registrations := responseFixture(t, true, nil)
	core, logs := observer.New(zap.WarnLevel)
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	t.Cleanup(func() { handler.StopCapture(capture) })
	registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewZapLogger(zap.New(core)), handler)
	require.NoError(t, err)
	for range 100 {
		registry.reportResponse(registry.methods[registrations[0].Method], registrations[0].Response, registrations[0].Request)
	}
	require.Positive(t, logs.Len())
	require.Less(t, logs.Len(), 100)
	require.Len(t, capture.SnapshotMetric("response_validation_failures"), 100)
}

func TestGlobalResponseRulesDoNotNeedRequestNamespace(t *testing.T) {
	services, registrations := responseFixture(t, true, func(file *descriptorpb.FileDescriptorProto) {
		file.MessageType[0].Field = nil
		options := &descriptorpb.FieldOptions{}
		proto.SetExtension(options, temporalvalidatepb.E_IdLength, true)
		file.MessageType[1].Field[1].Options = options
	})
	registrations[0].Dynamic = dynamicvalidate.New(&testValidator{id: func(value string) error {
		if len(value) > 5 {
			return errors.New("too long")
		}
		return nil
	}})
	registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	setString(registrations[0].Response, "label", "ok")
	diagnostics := registry.checkResponse(registry.methods[registrations[0].Method], registrations[0].Response, registrations[0].Request)
	require.Empty(t, diagnostics.kind)
	setString(registrations[0].Response, "reason", "too-long")
	diagnostics = registry.checkResponse(registry.methods[registrations[0].Method], registrations[0].Response, registrations[0].Request)
	require.Equal(t, "violation", diagnostics.kind)
}

func TestUnenrolledRPCIsNotCompiledOrChecked(t *testing.T) {
	services, registrations := responseFixture(t, false, func(file *descriptorpb.FileDescriptorProto) {
		for _, message := range file.MessageType {
			message.Options = &descriptorpb.MessageOptions{}
			proto.SetExtension(message.Options, validate.E_Message, &validate.MessageRules{Cel: []*validate.Rule{{Id: proto.String("broken"), Message: proto.String("broken schema"), Expression: proto.String("this.missing > 0")}}})
		}
	})
	core, logs := observer.New(zap.WarnLevel)
	registry, err := NewRegistry(services, registrations[0].Dynamic, log.NewZapLogger(zap.New(core)), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	request := registrations[0].Request.(*dynamicpb.Message)
	result, err := ValidateCall(t.Context(), registry, registrations[0].Method, request, func(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error) {
		return registrations[0].Response.(*dynamicpb.Message), nil
	})
	require.NoError(t, err)
	require.Same(t, registrations[0].Response, result)
	require.Zero(t, logs.Len())
}

func TestNestedResponseUsesRequestNamespace(t *testing.T) {
	services, registrations := responseFixture(t, true, func(file *descriptorpb.FileDescriptorProto) {
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
	diagnostics := registry.checkResponse(registry.methods[registrations[0].Method], registrations[0].Response, registrations[0].Request)
	require.Equal(t, "violation", diagnostics.kind)
	require.Equal(t, []string{"items[*].reason"}, diagnostics.fields)
	registrations[0].Response = dynamicpb.NewMessage(response.Descriptor())
	require.NoError(t, registrations[0].Dynamic.Precompile(registrations[0].Response))
}

func TestResponseValidatorCannotMutateReturnedMessage(t *testing.T) {
	called := false
	services, registrations := responseFixture(t, true, func(file *descriptorpb.FileDescriptorProto) {
		file.Dependency = append(file.Dependency, "temporal/api/common/v1/message.proto")
		field := file.MessageType[1].Field[1]
		field.Name = proto.String("payload")
		field.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
		field.TypeName = proto.String(".temporal.api.common.v1.Payload")
		field.Options = &descriptorpb.FieldOptions{}
		proto.SetExtension(field.Options, temporalvalidatepb.E_Payload, true)
	})
	registrations[0].Dynamic = dynamicvalidate.New(&testValidator{payload: func(_ ValidationContext, payload *commonpb.Payload) error {
		called = true
		payload.Data[0] = 'x'
		return errors.New("validator mutated its argument")
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

type testValidator struct {
	apivalidation.Validator[ValidationContext]
	reason  func(ValidationContext, string) error
	id      func(string) error
	payload func(ValidationContext, *commonpb.Payload) error
}

func (v *testValidator) ValidateReasonLength(ctx ValidationContext, value string) error {
	return v.reason(ctx, value)
}
func (v *testValidator) ValidateIDLength(_ ValidationContext, value string) error { return v.id(value) }
func (v *testValidator) ValidatePayload(ctx ValidationContext, value *commonpb.Payload) error {
	return v.payload(ctx, value)
}

func testNamespace(ctx ValidationContext) string {
	if ctx.Request == nil {
		return ""
	}
	message := ctx.Request.ProtoReflect()
	field := message.Descriptor().Fields().ByName("namespace")
	if field == nil {
		return ""
	}
	return message.Get(field).String()
}
