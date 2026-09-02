package nexusoperation

import (
	"context"
	"fmt"

	"buf.build/go/protovalidate"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/validation/dynamicvalidate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// FrontendHandler provides the frontend-facing API for standalone Nexus operations.
type FrontendHandler interface {
	StartNexusOperationExecution(context.Context, *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error)
	DescribeNexusOperationExecution(context.Context, *workflowservice.DescribeNexusOperationExecutionRequest) (*workflowservice.DescribeNexusOperationExecutionResponse, error)
	PollNexusOperationExecution(context.Context, *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error)
	ListNexusOperationExecutions(context.Context, *workflowservice.ListNexusOperationExecutionsRequest) (*workflowservice.ListNexusOperationExecutionsResponse, error)
	CountNexusOperationExecutions(context.Context, *workflowservice.CountNexusOperationExecutionsRequest) (*workflowservice.CountNexusOperationExecutionsResponse, error)
	RequestCancelNexusOperationExecution(context.Context, *workflowservice.RequestCancelNexusOperationExecutionRequest) (*workflowservice.RequestCancelNexusOperationExecutionResponse, error)
	TerminateNexusOperationExecution(context.Context, *workflowservice.TerminateNexusOperationExecutionRequest) (*workflowservice.TerminateNexusOperationExecutionResponse, error)
	DeleteNexusOperationExecution(context.Context, *workflowservice.DeleteNexusOperationExecutionRequest) (*workflowservice.DeleteNexusOperationExecutionResponse, error)
}

var ErrStandaloneNexusOperationDisabled = serviceerror.NewUnimplemented("Standalone Nexus operation is disabled")

type frontendHandler struct {
	client            nexusoperationpb.NexusOperationServiceClient
	config            *Config
	namespaceRegistry namespace.Registry
	endpointRegistry  commonnexus.EndpointRegistry
	validator         *validator
	protoValidator    protovalidate.Validator
	dynamicValidator  *dynamicvalidate.Runner
}

func newDynamicValidator(config *Config) *dynamicvalidate.Runner {
	lenLimit := func(limit func() int, msg string) dynamicvalidate.Rule {
		return dynamicvalidate.GlobalStringRule(func(value string) error {
			if got, limitValue := len(value), limit(); got > limitValue {
				return fmt.Errorf("%s (length %d, limit %d)", msg, got, limitValue)
			}
			return nil
		})
	}
	nsLenLimit := func(limit func(string) int, msg string) dynamicvalidate.Rule {
		return dynamicvalidate.NamespaceStringRule(func(namespace string, value string) error {
			if got, limitValue := len(value), limit(namespace); got > limitValue {
				return fmt.Errorf("%s (length %d, limit %d)", msg, got, limitValue)
			}
			return nil
		})
	}
	nsPayloadLimit := func(limit func(string) int, msg string) dynamicvalidate.Rule {
		return dynamicvalidate.NamespaceMessageRule(&commonpb.Payload{}, func(namespace string, payload *commonpb.Payload) error {
			if got, limitValue := payload.Size(), limit(namespace); got > limitValue {
				return fmt.Errorf("%s (size %d, limit %d)", msg, got, limitValue)
			}
			return nil
		})
	}

	return dynamicvalidate.New(dynamicvalidate.Registries{
		Rules: map[protoreflect.ExtensionType]dynamicvalidate.Rule{
			temporalvalidatepb.E_DynamicGlobalMaxIdLength:               lenLimit(config.MaxIDLengthLimit, "value exceeds the max ID length"),
			temporalvalidatepb.E_DynamicNamespaceMaxServiceNameLength:   nsLenLimit(config.MaxServiceNameLength, "service exceeds the namespace's max length"),
			temporalvalidatepb.E_DynamicNamespaceMaxOperationNameLength: nsLenLimit(config.MaxOperationNameLength, "operation exceeds the namespace's max length"),
			temporalvalidatepb.E_DynamicNamespaceMaxReasonLength:        nsLenLimit(config.MaxReasonLength, "reason exceeds the namespace's max length"),
			temporalvalidatepb.E_DynamicNamespaceMaxPayloadSize:         nsPayloadLimit(config.PayloadSizeLimit, "input exceeds the namespace's max payload size"),
			temporalvalidatepb.E_DynamicNamespaceMaxUserMetadataSummarySize: dynamicvalidate.NamespaceMessageRule(&sdkpb.UserMetadata{}, func(namespace string, metadata *sdkpb.UserMetadata) error {
				if got, limitValue := metadata.GetSummary().Size(), config.MaxUserMetadataSummarySize(namespace); got > limitValue {
					return fmt.Errorf("user_metadata.summary exceeds the namespace's max size limit (size %d, limit %d)", got, limitValue)
				}
				return nil
			}),
			temporalvalidatepb.E_DynamicNamespaceMaxUserMetadataDetailsSize: dynamicvalidate.NamespaceMessageRule(&sdkpb.UserMetadata{}, func(namespace string, metadata *sdkpb.UserMetadata) error {
				if got, limitValue := metadata.GetDetails().Size(), config.MaxUserMetadataDetailsSize(namespace); got > limitValue {
					return fmt.Errorf("user_metadata.details exceeds the namespace's max size limit (size %d, limit %d)", got, limitValue)
				}
				return nil
			}),
		},
	})
}

func NewFrontendHandler(
	client nexusoperationpb.NexusOperationServiceClient,
	config *Config,
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	endpointRegistry commonnexus.EndpointRegistry,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) (FrontendHandler, error) {
	messages, err := validationMessages()
	if err != nil {
		return nil, err
	}
	protoValidator, err := protovalidate.New(
		protovalidate.WithMessages(messages...),
		protovalidate.WithDisableLazy(),
	)
	if err != nil {
		return nil, err
	}
	dynamicValidator := newDynamicValidator(config)
	if err := dynamicValidator.Precompile(messages...); err != nil {
		return nil, err
	}
	return &frontendHandler{
		client:            client,
		config:            config,
		namespaceRegistry: namespaceRegistry,
		endpointRegistry:  endpointRegistry,
		validator:         newValidator(config, logger, saMapperProvider, saValidator),
		protoValidator:    protoValidator,
		dynamicValidator:  dynamicValidator,
	}, nil
}

func (h *frontendHandler) StartNexusOperationExecution(
	ctx context.Context,
	req *workflowservice.StartNexusOperationExecutionRequest,
) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	if err := h.validateProto(req); err != nil {
		return nil, err
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := h.validator.validateAndNormalizeStartRequest(req); err != nil {
		return nil, err
	}

	// Verify the endpoint exists before creating the operation.
	endpointEntry, err := h.endpointRegistry.GetByName(ctx, namespaceID, req.GetEndpoint())
	if err != nil {
		return nil, err
	}

	resp, err := h.client.StartNexusOperation(ctx, &nexusoperationpb.StartNexusOperationRequest{
		EndpointId:      endpointEntry.GetId(),
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

func (h *frontendHandler) DescribeNexusOperationExecution(
	ctx context.Context,
	req *workflowservice.DescribeNexusOperationExecutionRequest,
) (*workflowservice.DescribeNexusOperationExecutionResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	if err := h.validateProto(req); err != nil {
		return nil, err
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := h.validator.validateAndNormalizeDescribeRequest(req, namespaceID.String()); err != nil {
		return nil, err
	}

	resp, err := h.client.DescribeNexusOperation(ctx, &nexusoperationpb.DescribeNexusOperationRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

// PollNexusOperationExecution long-polls for a Nexus operation to reach a specific stage.
func (h *frontendHandler) PollNexusOperationExecution(
	ctx context.Context,
	req *workflowservice.PollNexusOperationExecutionRequest,
) (*workflowservice.PollNexusOperationExecutionResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	if err := h.validateProto(req); err != nil {
		return nil, err
	}

	if err := h.validator.validateAndNormalizePollRequest(req); err != nil {
		return nil, err
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	resp, err := h.client.PollNexusOperation(ctx, &nexusoperationpb.PollNexusOperationRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

func (h *frontendHandler) ListNexusOperationExecutions(
	ctx context.Context,
	req *workflowservice.ListNexusOperationExecutionsRequest,
) (*workflowservice.ListNexusOperationExecutionsResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	if err := h.validateProto(req); err != nil {
		return nil, err
	}

	pageSize := req.GetPageSize()
	maxPageSize := int32(h.config.VisibilityMaxPageSize(req.GetNamespace()))
	if pageSize <= 0 || pageSize > maxPageSize {
		pageSize = maxPageSize
	}

	resp, err := chasm.ListExecutions[*Operation, *emptypb.Empty](ctx, &chasm.ListExecutionsRequest{
		NamespaceName: req.GetNamespace(),
		PageSize:      int(pageSize),
		NextPageToken: req.GetNextPageToken(),
		Query:         req.GetQuery(),
	})
	if err != nil {
		return nil, err
	}

	operations := make([]*nexuspb.NexusOperationExecutionListInfo, 0, len(resp.Executions))
	for _, exec := range resp.Executions {
		endpoint, _ := chasm.SearchAttributeValue(exec.ChasmSearchAttributes, EndpointSearchAttribute)
		service, _ := chasm.SearchAttributeValue(exec.ChasmSearchAttributes, ServiceSearchAttribute)
		operation, _ := chasm.SearchAttributeValue(exec.ChasmSearchAttributes, OperationSearchAttribute)
		statusStr, _ := chasm.SearchAttributeValue(exec.ChasmSearchAttributes, StatusSearchAttribute)
		status, _ := enumspb.NexusOperationExecutionStatusFromString(statusStr)

		var closeTime *timestamppb.Timestamp
		var executionDuration *durationpb.Duration
		if !exec.CloseTime.IsZero() {
			closeTime = timestamppb.New(exec.CloseTime)
			if !exec.StartTime.IsZero() {
				executionDuration = durationpb.New(exec.CloseTime.Sub(exec.StartTime))
			}
		}

		operations = append(operations, &nexuspb.NexusOperationExecutionListInfo{
			OperationId:          exec.BusinessID,
			RunId:                exec.RunID,
			Endpoint:             endpoint,
			Service:              service,
			Operation:            operation,
			Status:               status,
			ScheduleTime:         timestamppb.New(exec.StartTime),
			CloseTime:            closeTime,
			ExecutionDuration:    executionDuration,
			StateTransitionCount: exec.StateTransitionCount,
			SearchAttributes:     &commonpb.SearchAttributes{IndexedFields: exec.CustomSearchAttributes},
		})
	}

	return &workflowservice.ListNexusOperationExecutionsResponse{
		Operations:    operations,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (h *frontendHandler) CountNexusOperationExecutions(
	ctx context.Context,
	req *workflowservice.CountNexusOperationExecutionsRequest,
) (*workflowservice.CountNexusOperationExecutionsResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	if err := h.validateProto(req); err != nil {
		return nil, err
	}

	resp, err := chasm.CountExecutions[*Operation](ctx, &chasm.CountExecutionsRequest{
		NamespaceName: req.GetNamespace(),
		Query:         req.GetQuery(),
	})
	if err != nil {
		return nil, err
	}

	groups := make([]*workflowservice.CountNexusOperationExecutionsResponse_AggregationGroup, 0, len(resp.Groups))
	for _, g := range resp.Groups {
		groups = append(groups, &workflowservice.CountNexusOperationExecutionsResponse_AggregationGroup{
			GroupValues: g.Values,
			Count:       g.Count,
		})
	}

	return &workflowservice.CountNexusOperationExecutionsResponse{
		Count:  resp.Count,
		Groups: groups,
	}, nil
}

func (h *frontendHandler) RequestCancelNexusOperationExecution(
	ctx context.Context,
	req *workflowservice.RequestCancelNexusOperationExecutionRequest,
) (*workflowservice.RequestCancelNexusOperationExecutionResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	if err := h.validateProto(req); err != nil {
		return nil, err
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := h.validator.validateAndNormalizeCancelRequest(req); err != nil {
		return nil, err
	}

	_, err = h.client.RequestCancelNexusOperation(ctx, &nexusoperationpb.RequestCancelNexusOperationRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.RequestCancelNexusOperationExecutionResponse{}, nil
}

func (h *frontendHandler) TerminateNexusOperationExecution(
	ctx context.Context,
	req *workflowservice.TerminateNexusOperationExecutionRequest,
) (*workflowservice.TerminateNexusOperationExecutionResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	if err := h.validateProto(req); err != nil {
		return nil, err
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := h.validator.validateAndNormalizeTerminateRequest(req); err != nil {
		return nil, err
	}

	_, err = h.client.TerminateNexusOperation(ctx, &nexusoperationpb.TerminateNexusOperationRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.TerminateNexusOperationExecutionResponse{}, nil
}

func (h *frontendHandler) DeleteNexusOperationExecution(
	ctx context.Context,
	req *workflowservice.DeleteNexusOperationExecutionRequest,
) (*workflowservice.DeleteNexusOperationExecutionResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	if err := h.validateProto(req); err != nil {
		return nil, err
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := h.validator.validateAndNormalizeDeleteRequest(req); err != nil {
		return nil, err
	}

	_, err = h.client.DeleteNexusOperation(ctx, &nexusoperationpb.DeleteNexusOperationRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.DeleteNexusOperationExecutionResponse{}, nil
}

// isStandaloneNexusOperationEnabled checks if standalone Nexus operations are enabled for the given namespace.
func (h *frontendHandler) isStandaloneNexusOperationEnabled(namespaceName string) bool {
	return h.config.EnableChasm(namespaceName) && h.config.Enabled(namespaceName)
}

func (h *frontendHandler) validateProto(msg proto.Message) error {
	if err := h.protoValidator.Validate(msg); err != nil {
		return serviceerror.NewInvalidArgument(err.Error())
	}
	if err := h.dynamicValidator.CheckMessage(msg); err != nil {
		return serviceerror.NewInvalidArgument(err.Error())
	}
	return nil
}
