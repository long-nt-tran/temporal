package dynamicvalidate_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	"go.temporal.io/server/common/validation/dynamicvalidate"
)

func TestBindingsRejectInvalidImplementations(t *testing.T) {
	valid := dynamicvalidate.Binding{Option: temporalvalidatepb.E_DynamicGlobalMaxIdLength, Rule: dynamicvalidate.GlobalByteLengthRule(func() int { return 10 })}
	_, err := dynamicvalidate.NewBindings([]dynamicvalidate.Binding{valid, valid})
	require.ErrorContains(t, err, "duplicate implementation")
	_, err = dynamicvalidate.NewBindings([]dynamicvalidate.Binding{{}})
	require.ErrorContains(t, err, "has no option")
	_, err = dynamicvalidate.NewBindings([]dynamicvalidate.Binding{{Option: valid.Option, Rule: dynamicvalidate.GlobalByteLengthRule(nil)}})
	require.ErrorContains(t, err, "has no implementation")
	_, err = dynamicvalidate.NewBindings([]dynamicvalidate.Binding{valid})
	require.NoError(t, err)
	_, err = dynamicvalidate.NewBindings(nil)
	require.NoError(t, err)
}
