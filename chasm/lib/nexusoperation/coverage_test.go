package nexusoperation

import (
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

//go:embed dynamic_rules.json
var dynamicRulesJSON []byte

func TestValidationEnrollment(t *testing.T) {
	messages, err := validationMessages()
	require.NoError(t, err)
	dynamicValidator := newDynamicValidator(testDynamicConfig())
	require.NoError(t, dynamicValidator.Precompile(messages...))
	var publishedDynamicRules []string
	require.NoError(t, json.Unmarshal(dynamicRulesJSON, &publishedDynamicRules))
	require.Equal(t, publishedDynamicRules, dynamicValidator.RegisteredRuleNames())

	var handlerRequests []string
	for _, message := range messages {
		handlerRequests = append(handlerRequests, string(message.ProtoReflect().Descriptor().FullName()))
	}
	var apiRequests []string
	service := workflowservice.File_temporal_api_workflowservice_v1_service_proto.Services().ByName("WorkflowService")
	methods := service.Methods()
	for i := range methods.Len() {
		method := methods.Get(i)
		options := method.Options().(*descriptorpb.MethodOptions)
		if !proto.HasExtension(options, temporalvalidatepb.E_RequestValidation) {
			continue
		}
		validation := proto.GetExtension(options, temporalvalidatepb.E_RequestValidation).(*temporalvalidatepb.RequestValidation)
		if validation.GetEnabled() {
			apiRequests = append(apiRequests, string(method.Input().FullName()))
		}
	}
	require.ElementsMatch(t, handlerRequests, apiRequests, "API request validation enrollment must match FrontendHandler")
}
