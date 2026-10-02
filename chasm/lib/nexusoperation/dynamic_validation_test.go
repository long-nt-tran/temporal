package nexusoperation

import (
	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	"go.temporal.io/server/common/validation/dynamicvalidate"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func testDynamicValidator(config *Config) *dynamicvalidate.Runner {
	return dynamicvalidate.New(dynamicvalidate.Registries{Rules: map[protoreflect.ExtensionType]dynamicvalidate.Rule{
		temporalvalidatepb.E_DynamicGlobalMaxIdLength:                   dynamicvalidate.GlobalByteLengthRule(config.MaxIDLengthLimit),
		temporalvalidatepb.E_DynamicNamespaceMaxServiceNameLength:       dynamicvalidate.NamespaceByteLengthRule(config.MaxServiceNameLength),
		temporalvalidatepb.E_DynamicNamespaceMaxOperationNameLength:     dynamicvalidate.NamespaceByteLengthRule(config.MaxOperationNameLength),
		temporalvalidatepb.E_DynamicNamespaceMaxReasonLength:            dynamicvalidate.NamespaceByteLengthRule(config.MaxReasonLength),
		temporalvalidatepb.E_DynamicNamespaceMaxPayloadSize:             dynamicvalidate.NamespaceMessageSizeRule(&commonpb.Payload{}, config.PayloadSizeLimit, func(value *commonpb.Payload) int { return value.Size() }),
		temporalvalidatepb.E_DynamicNamespaceMaxUserMetadataSummarySize: dynamicvalidate.NamespaceMessageSizeRule(&sdkpb.UserMetadata{}, config.MaxUserMetadataSummarySize, func(value *sdkpb.UserMetadata) int { return value.GetSummary().Size() }),
		temporalvalidatepb.E_DynamicNamespaceMaxUserMetadataDetailsSize: dynamicvalidate.NamespaceMessageSizeRule(&sdkpb.UserMetadata{}, config.MaxUserMetadataDetailsSize, func(value *sdkpb.UserMetadata) int { return value.GetDetails().Size() }),
	}})
}
