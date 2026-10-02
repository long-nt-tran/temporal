package validation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"buf.build/go/protovalidate"
	"go.temporal.io/api/serviceerror"
	apiregistry "go.temporal.io/api/temporalproto/registry"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/validation/dynamicvalidate"
	"go.uber.org/fx"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type methodRegistration struct {
	Method          string
	Request         proto.Message
	Response        proto.Message
	Dynamic         *dynamicvalidate.Runner
	requestEnabled  bool
	responseEnabled bool
	logger          log.Logger
}

type Registry struct {
	methods map[string]methodRegistration
	static  protovalidate.Validator
	metrics metrics.Handler
}

var Module = fx.Provide(newRegistry)

type registryParams struct {
	fx.In
	Rules   []dynamicvalidate.Binding `group:"validation-rules"`
	Logger  log.Logger
	Metrics metrics.Handler
}

func newRegistry(params registryParams) (*Registry, error) {
	services := APIServices()
	if err := checkBoundaryInventory(services, generatedUnaryMethods); err != nil {
		return nil, err
	}
	runner, err := dynamicvalidate.NewBindings(params.Rules)
	if err != nil {
		return nil, err
	}
	return NewRegistry(services, runner, params.Logger, params.Metrics)
}

func checkBoundaryInventory(services []protoreflect.ServiceDescriptor, generated map[string]struct{}) error {
	for _, service := range services {
		for i := range service.Methods().Len() {
			method := service.Methods().Get(i)
			if method.IsStreamingClient() || method.IsStreamingServer() {
				continue
			}
			name := "/" + string(service.FullName()) + "/" + string(method.Name())
			if _, ok := generated[name]; !ok {
				return fmt.Errorf("RPC validation: %s has no generated boundary; run go generate ./common/validation", name)
			}
		}
	}
	return nil
}

func APIServices() []protoreflect.ServiceDescriptor { return apiregistry.Services() }

// NewRegistry discovers enrolled RPCs from descriptors. A nil runner supports static-only schemas.
func NewRegistry(services []protoreflect.ServiceDescriptor, runner *dynamicvalidate.Runner, logger log.Logger, metricsHandler metrics.Handler) (*Registry, error) {
	if logger == nil || metricsHandler == nil {
		return nil, errors.New("RPC validation: logger and metrics handler are required")
	}
	if runner == nil {
		runner = dynamicvalidate.New(dynamicvalidate.Registries{})
	}
	if err := runner.Precompile(); err != nil {
		return nil, err
	}
	registry := &Registry{methods: map[string]methodRegistration{}, metrics: metricsHandler}
	var messages []proto.Message
	seen := map[string]bool{}
	for _, service := range services {
		for i := range service.Methods().Len() {
			method := service.Methods().Get(i)
			name := "/" + string(service.FullName()) + "/" + string(method.Name())
			if seen[name] {
				return nil, fmt.Errorf("RPC validation: duplicate RPC %s", name)
			}
			seen[name] = true
			methodMessages, err := registry.discoverMethod(name, method, runner, logger)
			if err != nil {
				return nil, err
			}
			messages = append(messages, methodMessages...)
		}
	}
	static, err := precompileStatic(messages)
	if err != nil {
		return nil, fmt.Errorf("RPC validation: compile static rules: %w", err)
	}
	registry.static = static
	return registry, nil
}

func (r *Registry) discoverMethod(name string, method protoreflect.MethodDescriptor, runner *dynamicvalidate.Runner, logger log.Logger) ([]proto.Message, error) {
	requestEnabled, responseEnabled, err := enrollment(name, method)
	if err != nil {
		return nil, err
	}
	if !requestEnabled && !responseEnabled {
		return nil, nil
	}
	if method.IsStreamingClient() || method.IsStreamingServer() {
		return nil, fmt.Errorf("RPC validation: %s only supports unary RPCs", name)
	}
	entry := methodRegistration{
		Method: name, Request: dynamicpb.NewMessage(method.Input()), Response: dynamicpb.NewMessage(method.Output()),
		Dynamic: runner, requestEnabled: requestEnabled, responseEnabled: responseEnabled,
	}
	var messages []proto.Message
	if requestEnabled {
		if err := runner.Precompile(entry.Request); err != nil {
			return nil, fmt.Errorf("RPC validation: %s request: %w", name, err)
		}
		messages = append(messages, entry.Request)
	}
	if responseEnabled {
		entry.logger = log.NewThrottledLogger(log.With(logger, tag.NewStringTag("rpc", name)), func() float64 { return 1 })
		if err := runner.PrecompileResponse(entry.Response, entry.Request); err != nil {
			return nil, fmt.Errorf("RPC validation: %s response: %w", name, err)
		}
		messages = append(messages, entry.Response)
	}
	r.methods[name] = entry
	return messages, nil
}

func enrollment(name string, method protoreflect.MethodDescriptor) (requestEnabled, responseEnabled bool, err error) {
	request, ok := proto.GetExtension(method.Options(), temporalvalidatepb.E_RequestValidation).(*temporalvalidatepb.RequestValidation)
	if !ok {
		return false, false, fmt.Errorf("RPC validation: %s has an invalid request enrollment annotation", name)
	}
	response, ok := proto.GetExtension(method.Options(), temporalvalidatepb.E_ResponseValidation).(*temporalvalidatepb.ResponseValidation)
	if !ok {
		return false, false, fmt.Errorf("RPC validation: %s has an invalid response enrollment annotation", name)
	}
	for _, side := range []struct {
		name             string
		present, enabled bool
		ignored          string
	}{
		{"request", proto.HasExtension(method.Options(), temporalvalidatepb.E_RequestValidation), request.GetEnabled(), request.GetIgnored()},
		{"response", proto.HasExtension(method.Options(), temporalvalidatepb.E_ResponseValidation), response.GetEnabled(), response.GetIgnored()},
	} {
		if side.present && (side.enabled && strings.TrimSpace(side.ignored) != "" || !side.enabled && strings.TrimSpace(side.ignored) == "") {
			return false, false, fmt.Errorf("RPC validation: %s %s must be enabled or have an exclusion reason, not both", name, side.name)
		}
	}
	return request.GetEnabled(), response.GetEnabled(), nil
}

func precompileStatic(messages []proto.Message) (protovalidate.Validator, error) {
	var descriptors []protoreflect.MessageDescriptor
	visited := map[protoreflect.FullName]bool{}
	var visit func(protoreflect.MessageDescriptor)
	visit = func(descriptor protoreflect.MessageDescriptor) {
		if visited[descriptor.FullName()] {
			return
		}
		visited[descriptor.FullName()] = true
		if !descriptor.IsMapEntry() {
			descriptors = append(descriptors, descriptor)
		}
		for i := range descriptor.Fields().Len() {
			if child := descriptor.Fields().Get(i).Message(); child != nil {
				visit(child)
			}
		}
	}
	for _, message := range messages {
		visit(message.ProtoReflect().Descriptor())
	}
	static, err := protovalidate.New(protovalidate.WithMessageDescriptors(descriptors...), protovalidate.WithDisableLazy())
	if err != nil {
		return nil, err
	}
	// New caches compilation errors. Probe each scope so a value error or an
	// absent child cannot hide a compilation error in another scope.
	for _, descriptor := range descriptors {
		message := dynamicpb.NewMessage(descriptor)
		probe := func(target protoreflect.Descriptor) error {
			err := static.Validate(message, protovalidate.WithFilter(protovalidate.FilterFunc(func(_ protoreflect.Message, scope protoreflect.Descriptor) bool {
				return scope == target
			})))
			var compilation *protovalidate.CompilationError
			if errors.As(err, &compilation) {
				return fmt.Errorf("%s: %w", target.FullName(), err)
			}
			return nil
		}
		if err := probe(descriptor); err != nil {
			return nil, err
		}
		for i := range descriptor.Fields().Len() {
			if err := probe(descriptor.Fields().Get(i)); err != nil {
				return nil, err
			}
		}
	}
	return static, nil
}

func (r *Registry) Validate(method string, message proto.Message) error {
	registration, ok := r.methods[method]
	if !ok {
		return nil
	}
	if !registration.requestEnabled {
		return nil
	}
	if nilMessage(message) || !message.ProtoReflect().IsValid() {
		return serviceerror.NewInvalidArgument("request is required")
	}
	if message.ProtoReflect().Descriptor().FullName() != registration.Request.ProtoReflect().Descriptor().FullName() {
		return serviceerror.NewInternalf("request validation: %s received the wrong request type", method)
	}
	if err := r.static.Validate(message); err != nil {
		var violation *protovalidate.ValidationError
		if !errors.As(err, &violation) {
			return serviceerror.NewInternalf("request validation: %s: %v", method, err)
		}
		details := &errdetails.BadRequest{}
		for _, item := range violation.Violations {
			details.FieldViolations = append(details.FieldViolations, &errdetails.BadRequest_FieldViolation{Field: protovalidate.FieldPathString(item.Proto.GetField()), Description: item.Proto.GetMessage(), Reason: item.Proto.GetRuleId()})
		}
		return invalidArgument(err.Error(), details)
	}
	if err := registration.Dynamic.CheckMessage(message); err != nil {
		var violation *dynamicvalidate.ValidationError
		if !errors.As(err, &violation) {
			return serviceerror.NewInternalf("request validation: %s: %v", method, err)
		}
		details := &errdetails.BadRequest{}
		for _, item := range violation.Violations {
			details.FieldViolations = append(details.FieldViolations, &errdetails.BadRequest_FieldViolation{Field: item.FieldPath, Description: item.Message, Reason: item.RuleID})
		}
		return invalidArgument(err.Error(), details)
	}
	return nil
}

func invalidArgument(message string, details *errdetails.BadRequest) error {
	st, err := status.New(codes.InvalidArgument, message).WithDetails(details)
	if err != nil {
		return serviceerror.NewInternalf("request validation: encode error details: %v", err)
	}
	return serviceerror.FromStatus(st)
}

// ValidateCall also supports direct calls that do not use a gRPC interceptor.
func ValidateCall[Request proto.Message, Response any](ctx context.Context, registry *Registry, method string, request Request, handler func(context.Context, Request) (Response, error)) (Response, error) {
	registration, enrolled := registry.methods[method]
	if !enrolled {
		return handler(ctx, request)
	}
	// The interceptor and generated wrapper can guard the same invocation.
	if active, ok := ctx.Value(callContextKey{}).(activeCall); ok && active.registry == registry && active.method == method && reflect.ValueOf(active.request) == reflect.ValueOf(request) {
		return handler(ctx, request)
	}
	if err := registry.Validate(method, request); err != nil {
		var zero Response
		return zero, err
	}
	namespace := ""
	if registration.responseEnabled {
		namespace = requestNamespace(request)
	}
	ctx = context.WithValue(ctx, callContextKey{}, activeCall{registry: registry, method: method, request: request})
	response, err := handler(ctx, request)
	if err == nil && registration.responseEnabled {
		registry.reportResponse(registration, response, namespace)
	}
	return response, err
}

func requestNamespace(request proto.Message) string {
	if nilMessage(request) || !request.ProtoReflect().IsValid() {
		return ""
	}
	message := request.ProtoReflect()
	field := message.Descriptor().Fields().ByName("namespace")
	if field == nil || field.Kind() != protoreflect.StringKind || field.IsList() || field.IsMap() {
		return ""
	}
	return message.Get(field).String()
}

func nilMessage(message proto.Message) bool {
	return message == nil || (reflect.ValueOf(message).Kind() == reflect.Pointer && reflect.ValueOf(message).IsNil())
}
