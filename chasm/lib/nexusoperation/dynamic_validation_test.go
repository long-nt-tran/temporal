package nexusoperation

import (
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/server/common/validation/dynamicvalidate"
)

type testFunctionValidator struct{ config *Config }

func testDynamicValidator(config *Config) *dynamicvalidate.Runner {
	return dynamicvalidate.New(&testFunctionValidator{config: config})
}

func (*testFunctionValidator) ValidateNamespace(_ dynamicvalidate.ValidationContext, value string) error {
	if value == "" {
		return errors.New("namespace is required")
	}
	return nil
}
func (v *testFunctionValidator) ValidateIDLength(_ dynamicvalidate.ValidationContext, value string) error {
	return testSize(len(value), v.config.MaxIDLengthLimit())
}
func (v *testFunctionValidator) ValidateServiceNameLength(ctx dynamicvalidate.ValidationContext, value string) error {
	return testNamespaceSize(ctx, len(value), v.config.MaxServiceNameLength)
}
func (v *testFunctionValidator) ValidateOperationNameLength(ctx dynamicvalidate.ValidationContext, value string) error {
	return testNamespaceSize(ctx, len(value), v.config.MaxOperationNameLength)
}
func (v *testFunctionValidator) ValidateReasonLength(ctx dynamicvalidate.ValidationContext, value string) error {
	return testNamespaceSize(ctx, len(value), v.config.MaxReasonLength)
}
func (v *testFunctionValidator) ValidatePayload(ctx dynamicvalidate.ValidationContext, value *commonpb.Payload) error {
	if value == nil {
		return nil
	}
	return testNamespaceSize(ctx, value.Size(), v.config.PayloadSizeLimit)
}
func (v *testFunctionValidator) ValidateUserMetadata(ctx dynamicvalidate.ValidationContext, value *sdkpb.UserMetadata) error {
	if value == nil {
		return nil
	}
	if err := testNamespaceSize(ctx, value.GetSummary().Size(), v.config.MaxUserMetadataSummarySize); err != nil {
		return err
	}
	return testNamespaceSize(ctx, value.GetDetails().Size(), v.config.MaxUserMetadataDetailsSize)
}
func testNamespaceSize(ctx dynamicvalidate.ValidationContext, size int, limit func(string) int) error {
	request, ok := ctx.Request.(interface{ GetNamespace() string })
	if !ok || request.GetNamespace() == "" {
		return errors.New("request namespace is required")
	}
	return testSize(size, limit(request.GetNamespace()))
}
func testSize(size, limit int) error {
	if size > limit {
		return fmt.Errorf("size %d exceeds limit %d", size, limit)
	}
	return nil
}
