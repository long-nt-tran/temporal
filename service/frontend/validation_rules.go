package frontend

import (
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	apivalidation "go.temporal.io/api/temporalvalidate/validation"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/validation"
)

type Validator struct {
	maxIDLength            dynamicconfig.IntPropertyFn
	maxServiceNameLength   dynamicconfig.IntPropertyFnWithNamespaceFilter
	maxOperationNameLength dynamicconfig.IntPropertyFnWithNamespaceFilter
	maxReasonLength        dynamicconfig.IntPropertyFnWithNamespaceFilter
	maxPayloadSize         dynamicconfig.IntPropertyFnWithNamespaceFilter
	maxSummarySize         dynamicconfig.IntPropertyFnWithNamespaceFilter
	maxDetailsSize         dynamicconfig.IntPropertyFnWithNamespaceFilter
}

var _ apivalidation.Validator[validation.ValidationContext] = (*Validator)(nil)

func ValidationRulesProvider(dc *dynamicconfig.Collection) apivalidation.Validator[validation.ValidationContext] {
	return &Validator{
		maxIDLength:            dynamicconfig.MaxIDLengthLimit.Get(dc),
		maxServiceNameLength:   chasmnexus.MaxServiceNameLength.Get(dc),
		maxOperationNameLength: chasmnexus.MaxOperationNameLength.Get(dc),
		maxReasonLength:        chasmnexus.MaxReasonLength.Get(dc),
		maxPayloadSize:         dynamicconfig.BlobSizeLimitError.Get(dc),
		maxSummarySize:         dynamicconfig.MaxUserMetadataSummarySize.Get(dc),
		maxDetailsSize:         dynamicconfig.MaxUserMetadataDetailsSize.Get(dc),
	}
}

func (*Validator) ValidateNamespace(_ validation.ValidationContext, value string) error {
	if value == "" {
		return errors.New("namespace is required")
	}
	return nil
}

func (v *Validator) ValidateIDLength(_ validation.ValidationContext, value string) error {
	return checkValidationSize(len(value), v.maxIDLength())
}

func (v *Validator) ValidateServiceNameLength(ctx validation.ValidationContext, value string) error {
	return checkNamespaceValidationSize(ctx, len(value), v.maxServiceNameLength)
}

func (v *Validator) ValidateOperationNameLength(ctx validation.ValidationContext, value string) error {
	return checkNamespaceValidationSize(ctx, len(value), v.maxOperationNameLength)
}

func (v *Validator) ValidateReasonLength(ctx validation.ValidationContext, value string) error {
	return checkNamespaceValidationSize(ctx, len(value), v.maxReasonLength)
}

func (v *Validator) ValidatePayload(ctx validation.ValidationContext, value *commonpb.Payload) error {
	if value == nil {
		return nil
	}
	return checkNamespaceValidationSize(ctx, value.Size(), v.maxPayloadSize)
}

func (v *Validator) ValidateUserMetadata(ctx validation.ValidationContext, value *sdkpb.UserMetadata) error {
	if value == nil {
		return nil
	}
	if err := checkNamespaceValidationSize(ctx, value.GetSummary().Size(), v.maxSummarySize); err != nil {
		return err
	}
	return checkNamespaceValidationSize(ctx, value.GetDetails().Size(), v.maxDetailsSize)
}

func checkNamespaceValidationSize(ctx validation.ValidationContext, size int, limit dynamicconfig.IntPropertyFnWithNamespaceFilter) error {
	request, ok := ctx.Request.(interface{ GetNamespace() string })
	if !ok || request.GetNamespace() == "" {
		return errors.New("request namespace is required")
	}
	return checkValidationSize(size, limit(request.GetNamespace()))
}

func checkValidationSize(size, limit int) error {
	if size > limit {
		return fmt.Errorf("size %d exceeds configured limit %d", size, limit)
	}
	return nil
}
