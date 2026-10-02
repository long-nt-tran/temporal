package dynamicvalidate

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// Binding connects an API field option to one shared Server implementation.
type Binding struct {
	Option protoreflect.ExtensionType
	Rule   Rule
}

func NewBindings(bindings []Binding) (*Runner, error) {
	rules := make(map[protoreflect.ExtensionType]Rule, len(bindings))
	names := map[protoreflect.FullName]bool{}
	for _, binding := range bindings {
		if binding.Option == nil {
			return nil, errors.New("dynamicvalidate: binding has no option")
		}
		name := binding.Option.TypeDescriptor().FullName()
		if names[name] {
			return nil, fmt.Errorf("dynamicvalidate: duplicate implementation for %s", name)
		}
		names[name] = true
		rules[binding.Option] = binding.Rule
	}
	runner := New(Registries{Rules: rules})
	if err := runner.Precompile(); err != nil {
		return nil, err
	}
	return runner, nil
}
