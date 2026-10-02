package frontend

import (
	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/validation/dynamicvalidate"
)

func ValidationRulesProvider(dc *dynamicconfig.Collection) []dynamicvalidate.Binding {
	return []dynamicvalidate.Binding{
		{Option: temporalvalidatepb.E_DynamicGlobalMaxIdLength, Rule: dynamicvalidate.GlobalByteLengthRule(dynamicconfig.MaxIDLengthLimit.Get(dc))},
		{Option: temporalvalidatepb.E_DynamicNamespaceMaxServiceNameLength, Rule: dynamicvalidate.NamespaceByteLengthRule(chasmnexus.MaxServiceNameLength.Get(dc))},
		{Option: temporalvalidatepb.E_DynamicNamespaceMaxOperationNameLength, Rule: dynamicvalidate.NamespaceByteLengthRule(chasmnexus.MaxOperationNameLength.Get(dc))},
		{Option: temporalvalidatepb.E_DynamicNamespaceMaxReasonLength, Rule: dynamicvalidate.NamespaceByteLengthRule(chasmnexus.MaxReasonLength.Get(dc))},
		{Option: temporalvalidatepb.E_DynamicNamespaceMaxPayloadSize, Rule: dynamicvalidate.NamespaceMessageSizeRule(&commonpb.Payload{}, dynamicconfig.BlobSizeLimitError.Get(dc), func(value *commonpb.Payload) int { return value.Size() })},
		{Option: temporalvalidatepb.E_DynamicNamespaceMaxUserMetadataSummarySize, Rule: dynamicvalidate.NamespaceMessageSizeRule(&sdkpb.UserMetadata{}, dynamicconfig.MaxUserMetadataSummarySize.Get(dc), func(value *sdkpb.UserMetadata) int { return value.GetSummary().Size() })},
		{Option: temporalvalidatepb.E_DynamicNamespaceMaxUserMetadataDetailsSize, Rule: dynamicvalidate.NamespaceMessageSizeRule(&sdkpb.UserMetadata{}, dynamicconfig.MaxUserMetadataDetailsSize.Get(dc), func(value *sdkpb.UserMetadata) int { return value.GetDetails().Size() })},
	}
}
