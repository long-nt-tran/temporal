package dynamicvalidate_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	temporalvalidatepb "go.temporal.io/api/temporalvalidate/v1"
	"go.temporal.io/server/common/validation/dynamicvalidate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func nestedDescriptor(t *testing.T, collectionRule bool) protoreflect.MessageDescriptor {
	t.Helper()
	options := &descriptorpb.FieldOptions{}
	proto.SetExtension(options, temporalvalidatepb.E_DynamicNamespaceMaxReasonLength, true)
	field := func(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type, typeName string, repeated bool) *descriptorpb.FieldDescriptorProto {
		label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
		if repeated {
			label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
		}
		result := &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(number), Type: &kind, Label: &label}
		if typeName != "" {
			result.TypeName = proto.String(typeName)
		}
		return result
	}
	reason := field("reason", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, "", collectionRule)
	reason.Options = options
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("nested_test.proto"), Package: proto.String("nestedtest"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Child"), Field: []*descriptorpb.FieldDescriptorProto{reason}},
			{Name: proto.String("Root"), Field: []*descriptorpb.FieldDescriptorProto{
				field("namespace", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, "", false),
				field("child", 2, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, ".nestedtest.Child", false),
				field("items", 3, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, ".nestedtest.Child", true),
				field("named", 4, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, ".nestedtest.Root.NamedEntry", true),
			}, NestedType: []*descriptorpb.DescriptorProto{{Name: proto.String("NamedEntry"), Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)}, Field: []*descriptorpb.FieldDescriptorProto{
				field("key", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, "", false),
				field("value", 2, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, ".nestedtest.Child", false),
			}}}},
		},
	}, protoregistry.GlobalFiles)
	require.NoError(t, err)
	return file.Messages().ByName("Root")
}

func nestedRunner() *dynamicvalidate.Runner {
	return dynamicvalidate.New(dynamicvalidate.Registries{Rules: map[protoreflect.ExtensionType]dynamicvalidate.Rule{
		temporalvalidatepb.E_DynamicNamespaceMaxReasonLength: dynamicvalidate.NamespaceStringRule(func(string, string) error { return nil }),
	}})
}

func TestNestedRulesUseRootNamespaceAndPaths(t *testing.T) {
	descriptor := nestedDescriptor(t, false)
	root := dynamicpb.NewMessage(descriptor)
	root.Set(descriptor.Fields().ByName("namespace"), protoreflect.ValueOfString("root-ns"))
	child := func() protoreflect.Value {
		message := dynamicpb.NewMessage(descriptor.Fields().ByName("child").Message())
		message.Set(message.Descriptor().Fields().ByName("reason"), protoreflect.ValueOfString("invalid"))
		return protoreflect.ValueOfMessage(message)
	}
	root.Set(descriptor.Fields().ByName("child"), child())
	root.Mutable(descriptor.Fields().ByName("items")).List().Append(child())
	named := root.Mutable(descriptor.Fields().ByName("named")).Map()
	named.Set(protoreflect.ValueOfString("z").MapKey(), child())
	named.Set(protoreflect.ValueOfString("a").MapKey(), child())
	var namespaces []string
	runner := dynamicvalidate.New(dynamicvalidate.Registries{Rules: map[protoreflect.ExtensionType]dynamicvalidate.Rule{
		temporalvalidatepb.E_DynamicNamespaceMaxReasonLength: dynamicvalidate.NamespaceStringRule(func(namespace, _ string) error {
			namespaces = append(namespaces, namespace)
			return errors.New("reason exceeds limit")
		}),
	}})
	require.NoError(t, runner.Precompile(root))
	var failure *dynamicvalidate.ValidationError
	require.ErrorAs(t, runner.CheckMessage(root), &failure)
	require.Equal(t, []string{"root-ns", "root-ns", "root-ns", "root-ns"}, namespaces)
	var paths []string
	for _, violation := range failure.Violations {
		paths = append(paths, violation.FieldPath)
	}
	require.Equal(t, []string{"child.reason", "items[0].reason", "named[\"a\"].reason", "named[\"z\"].reason"}, paths)
}

func TestNestedSchemaErrorsFailBeforeRequests(t *testing.T) {
	t.Run("unknown rule in absent child", func(t *testing.T) {
		root := dynamicpb.NewMessage(nestedDescriptor(t, false))
		runner := dynamicvalidate.New(dynamicvalidate.Registries{})
		require.ErrorContains(t, runner.Precompile(root), "no implementation")
	})
	t.Run("repeated dynamic field", func(t *testing.T) {
		root := dynamicpb.NewMessage(nestedDescriptor(t, true))
		runner := nestedRunner()
		require.ErrorContains(t, runner.Precompile(root), "requires a singular field")
		childField := root.Descriptor().Fields().ByName("child")
		root.Mutable(childField)
		require.NotPanics(t, func() {
			require.ErrorContains(t, runner.CheckMessage(root), "requires a singular field")
		})
	})
	t.Run("missing root namespace", func(t *testing.T) {
		root := nestedDescriptor(t, false)
		child := dynamicpb.NewMessage(root.Fields().ByName("child").Message())
		require.ErrorContains(t, nestedRunner().Precompile(child), "no string namespace")
	})
	t.Run("absent optional children", func(t *testing.T) {
		root := dynamicpb.NewMessage(nestedDescriptor(t, false))
		require.NoError(t, nestedRunner().Precompile(root))
		require.NoError(t, nestedRunner().CheckMessage(root))
	})
}
