package frontend

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	"go.temporal.io/api/workflowservice/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/validation"
	"go.temporal.io/server/common/validation/dynamicvalidate"
	"go.uber.org/fx"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func TestAutomaticValidationFxStartup(t *testing.T) {
	observability := fx.Provide(func() log.Logger { return log.NewNoopLogger() }, func() metrics.Handler { return metrics.NoopMetricsHandler })
	construct := fx.Invoke(func(*validation.Registry) {})
	app := fx.New(fx.NopLogger, validation.Module, observability,
		fx.Provide(dynamicconfig.NewNoopCollection),
		fx.Provide(fx.Annotate(ValidationRulesProvider, fx.ResultTags(`group:"validation-rules,flatten"`))),
		fx.Provide(func() Handler { return &WorkflowHandler{} }, func() OperatorHandler { return &OperatorHandlerImpl{} }),
		fx.Decorate(validateWorkflowHandler, validateOperatorHandler),
		fx.Invoke(func(handler Handler, operator OperatorHandler) {
			_, err := handler.StartNexusOperationExecution(t.Context(), nil)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
			require.IsType(t, &validatedOperatorHandler{}, operator)
		}), construct)
	require.NoError(t, app.Err())
	missing := fx.New(fx.NopLogger, validation.Module, observability, construct)
	require.ErrorContains(t, missing.Err(), "no implementation for")
}

func TestEveryEnrolledWorkflowMethodGuardsDirectCalls(t *testing.T) {
	runner, err := dynamicvalidate.NewBindings(ValidationRulesProvider(dynamicconfig.NewNoopCollection()))
	require.NoError(t, err)
	registry, err := validation.NewRegistry(validation.APIServices(), runner, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	handler := validation.WrapWorkflowService(&WorkflowHandler{}, registry)
	service := workflowservice.File_temporal_api_workflowservice_v1_service_proto.Services().Get(0)
	for i := range service.Methods().Len() {
		method := service.Methods().Get(i)
		annotation, ok := proto.GetExtension(method.Options(), temporalvalidatepb.E_RequestValidation).(*temporalvalidatepb.RequestValidation)
		if !ok || !annotation.GetEnabled() {
			continue
		}
		t.Run(string(method.Name()), func(t *testing.T) {
			methodValue := reflect.ValueOf(handler).MethodByName(string(method.Name()))
			result := methodValue.Call([]reflect.Value{reflect.ValueOf(t.Context()), reflect.Zero(methodValue.Type().In(1))})
			callErr, ok := result[1].Interface().(error)
			require.True(t, ok)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(callErr).Code())
		})
	}
}

func TestSharedRuleReadsCurrentNamespaceConfig(t *testing.T) {
	client := dynamicconfig.NewMemoryClient()
	dc := dynamicconfig.NewCollection(client, log.NewNoopLogger())
	runner, err := dynamicvalidate.NewBindings(ValidationRulesProvider(dc))
	require.NoError(t, err)
	registry, err := validation.NewRegistry(validation.APIServices(), runner, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	request := &workflowservice.StartNexusOperationExecutionRequest{Namespace: "ns", OperationId: "id", Endpoint: "endpoint", Service: "service", Operation: "operation"}
	const method = "/temporal.api.workflowservice.v1.WorkflowService/StartNexusOperationExecution"
	require.NoError(t, registry.Validate(method, request))
	undo := client.OverrideSetting(chasmnexus.MaxServiceNameLength, []dynamicconfig.ConstrainedValue{{Constraints: dynamicconfig.Constraints{Namespace: "ns"}, Value: 2}})
	t.Cleanup(undo)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(registry.Validate(method, request)).Code())
	request.Namespace = "other"
	require.NoError(t, registry.Validate(method, request))
	undo()
	request.Namespace = "ns"
	require.NoError(t, registry.Validate(method, request))
}
