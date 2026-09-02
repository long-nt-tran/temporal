// Package dynamicvalidate runs proto rules backed by dynamic config.
package dynamicvalidate

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	dynamicRulePrefix   = "temporalvalidate.v1.dynamic_"
	globalRulePrefix    = dynamicRulePrefix + "global_"
	namespaceRulePrefix = dynamicRulePrefix + "namespace_"
)

type ruleScope uint8

const (
	globalScope ruleScope = iota + 1
	namespaceScope
)

// Rule validates a field using dynamic config.
type Rule struct {
	scope    ruleScope
	accepts  func(protoreflect.FieldDescriptor) bool
	validate func(namespace string, value any) error
}

// GlobalStringRule creates a global rule for string fields.
func GlobalStringRule(validate func(string) error) Rule {
	if validate == nil {
		return Rule{scope: globalScope}
	}
	return Rule{
		scope: globalScope,
		accepts: func(field protoreflect.FieldDescriptor) bool {
			return field.Kind() == protoreflect.StringKind
		},
		validate: func(_ string, value any) error {
			return validate(value.(string))
		},
	}
}

// NamespaceStringRule creates a namespace rule for string fields.
func NamespaceStringRule(validate func(namespace string, value string) error) Rule {
	if validate == nil {
		return Rule{scope: namespaceScope}
	}
	return Rule{
		scope: namespaceScope,
		accepts: func(field protoreflect.FieldDescriptor) bool {
			return field.Kind() == protoreflect.StringKind
		},
		validate: func(namespace string, value any) error {
			return validate(namespace, value.(string))
		},
	}
}

// NamespaceMessageRule creates a namespace rule for one message type.
func NamespaceMessageRule[M proto.Message](message M, validate func(namespace string, value M) error) Rule {
	if validate == nil {
		return Rule{scope: namespaceScope}
	}
	messageName := message.ProtoReflect().Descriptor().FullName()
	return Rule{
		scope: namespaceScope,
		accepts: func(field protoreflect.FieldDescriptor) bool {
			return field.Kind() == protoreflect.MessageKind && field.Message().FullName() == messageName
		},
		validate: func(namespace string, value any) error {
			return validate(namespace, value.(M))
		},
	}
}

// Registries maps typed proto rule options to server implementations.
type Registries struct {
	Rules map[protoreflect.ExtensionType]Rule
}

type Runner struct {
	rules   map[protoreflect.FullName]Rule
	initErr error
}

func New(registries Registries) *Runner {
	runner := &Runner{rules: make(map[protoreflect.FullName]Rule, len(registries.Rules))}
	var errs []error
	for extension, rule := range registries.Rules {
		name := extension.TypeDescriptor().FullName()
		expectedPrefix := ""
		switch rule.scope {
		case globalScope:
			expectedPrefix = globalRulePrefix
		case namespaceScope:
			expectedPrefix = namespaceRulePrefix
		default:
			errs = append(errs, fmt.Errorf("dynamicvalidate: %s has the wrong rule scope", name))
			continue
		}
		if !strings.HasPrefix(string(name), expectedPrefix) {
			errs = append(errs, fmt.Errorf("dynamicvalidate: %s has the wrong rule scope", name))
			continue
		}
		if rule.accepts == nil || rule.validate == nil {
			errs = append(errs, fmt.Errorf("dynamicvalidate: %s has no implementation", name))
			continue
		}
		runner.rules[name] = rule
	}
	runner.initErr = errors.Join(errs...)
	return runner
}

// RegisteredRuleNames returns the typed options backed by implementations.
func (r *Runner) RegisteredRuleNames() []string {
	names := make([]string, 0, len(r.rules))
	for name := range r.rules {
		names = append(names, string(name))
	}
	slices.Sort(names)
	return names
}

type Violation struct {
	RuleID  string
	Message string
}

type ValidationError struct {
	Violations []Violation
}

func (e *ValidationError) Error() string {
	if len(e.Violations) == 1 {
		return e.Violations[0].Message
	}
	message := fmt.Sprintf("%d dynamic validation rules failed:", len(e.Violations))
	for _, violation := range e.Violations {
		message += fmt.Sprintf("\n\t%s: %s", violation.RuleID, violation.Message)
	}
	return message
}

// CheckMessage returns ValidationError for invalid input. Schema errors return
// a plain error.
func (r *Runner) CheckMessage(message proto.Message) error {
	if r.initErr != nil {
		return r.initErr
	}
	refMessage := message.ProtoReflect()
	var violations []Violation
	fields := refMessage.Descriptor().Fields()
	for i := range fields.Len() {
		fieldViolations, err := r.checkField(refMessage, fields.Get(i))
		if err != nil {
			return err
		}
		violations = append(violations, fieldViolations...)
	}
	if len(violations) == 0 {
		return nil
	}
	return &ValidationError{Violations: violations}
}

func (r *Runner) checkField(message protoreflect.Message, field protoreflect.FieldDescriptor) ([]Violation, error) {
	names, err := annotatedRules(field)
	if err != nil || len(names) == 0 {
		return nil, err
	}
	value := fieldValue(message, field)
	namespace := ""
	hasNamespace := false
	var violations []Violation
	for _, name := range names {
		rule, err := r.resolve(name, field)
		if err != nil {
			return nil, err
		}
		if rule.scope == namespaceScope && !hasNamespace {
			namespace, err = messageNamespace(message)
			if err != nil {
				return nil, err
			}
			hasNamespace = true
		}
		if err := rule.validate(namespace, value); err != nil {
			violations = append(violations, Violation{RuleID: string(name), Message: err.Error()})
		}
	}
	return violations, nil
}

// Precompile checks registration, field types, and namespace access.
func (r *Runner) Precompile(messages ...proto.Message) error {
	errs := []error{r.initErr}
	for _, message := range messages {
		refMessage := message.ProtoReflect()
		fields := refMessage.Descriptor().Fields()
		for i := range fields.Len() {
			field := fields.Get(i)
			names, err := annotatedRules(field)
			errs = append(errs, err)
			for _, name := range names {
				rule, err := r.resolve(name, field)
				errs = append(errs, err)
				if err == nil && rule.scope == namespaceScope {
					_, err = messageNamespace(refMessage)
					errs = append(errs, err)
				}
			}
		}
	}
	return errors.Join(errs...)
}

func (r *Runner) resolve(name protoreflect.FullName, field protoreflect.FieldDescriptor) (Rule, error) {
	rule, ok := r.rules[name]
	if !ok {
		return Rule{}, fmt.Errorf("dynamicvalidate: no implementation for %s on field %s", name, field.FullName())
	}
	if !rule.accepts(field) {
		return Rule{}, fmt.Errorf("dynamicvalidate: %s does not support field %s", name, field.FullName())
	}
	return rule, nil
}

func annotatedRules(field protoreflect.FieldDescriptor) ([]protoreflect.FullName, error) {
	var names []protoreflect.FullName
	var errs []error
	proto.RangeExtensions(field.Options(), func(extension protoreflect.ExtensionType, value any) bool {
		name := extension.TypeDescriptor().FullName()
		if !strings.HasPrefix(string(name), dynamicRulePrefix) {
			return true
		}
		enabled, ok := value.(bool)
		if !ok || !enabled {
			errs = append(errs, fmt.Errorf("dynamicvalidate: %s must be true when set on field %s", name, field.FullName()))
			return true
		}
		names = append(names, name)
		return true
	})
	slices.Sort(names)
	return names, errors.Join(errs...)
}

func fieldValue(message protoreflect.Message, field protoreflect.FieldDescriptor) any {
	value := message.Get(field)
	if field.Kind() == protoreflect.MessageKind && !field.IsMap() {
		return value.Message().Interface()
	}
	return value.Interface()
}

func messageNamespace(message protoreflect.Message) (string, error) {
	field := message.Descriptor().Fields().ByName("namespace")
	if field == nil || field.Kind() != protoreflect.StringKind {
		return "", fmt.Errorf("dynamicvalidate: message %s uses namespace rules but has no string namespace field", message.Descriptor().FullName())
	}
	return message.Get(field).String(), nil
}
