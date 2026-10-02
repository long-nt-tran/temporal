// Package dynamicvalidate runs proto rules backed by dynamic config.
package dynamicvalidate

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	dynamicRulePrefix   = "temporalvalidate.v1.dynamic_"
	globalRulePrefix    = dynamicRulePrefix + "global_"
	namespaceRulePrefix = dynamicRulePrefix + "namespace_"
)

var ErrResponseNamespaceMissing = errors.New("dynamicvalidate: response namespace context is missing")

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
	if validate == nil || nilMessage(message) {
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
		if extension == nil {
			errs = append(errs, errors.New("dynamicvalidate: nil rule option"))
			continue
		}
		name := extension.TypeDescriptor().FullName()
		if extension.TypeDescriptor().Kind() != protoreflect.BoolKind || extension.TypeDescriptor().ContainingMessage().FullName() != "google.protobuf.FieldOptions" {
			errs = append(errs, fmt.Errorf("dynamicvalidate: %s must be a boolean FieldOptions extension", name))
			continue
		}
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

type Violation struct {
	FieldPath string
	RuleID    string
	Message   string
}

type ValidationError struct {
	Violations []Violation
}

func (e *ValidationError) Error() string {
	if len(e.Violations) == 1 {
		return e.Violations[0].FieldPath + ": " + e.Violations[0].Message
	}
	message := fmt.Sprintf("%d dynamic validation rules failed:", len(e.Violations))
	for _, violation := range e.Violations {
		message += fmt.Sprintf("\n\t%s (%s): %s", violation.FieldPath, violation.RuleID, violation.Message)
	}
	return message
}

// CheckMessage returns ValidationError for invalid input. Schema errors return
// a plain error.
func (r *Runner) CheckMessage(message proto.Message) error {
	return r.check(message, func() (string, error) { return messageNamespace(message.ProtoReflect()) })
}

// CheckResponse uses a namespace captured from the request, not the response.
func (r *Runner) CheckResponse(message proto.Message, namespace string) error {
	return r.check(message, func() (string, error) {
		if namespace == "" {
			return "", ErrResponseNamespaceMissing
		}
		return namespace, nil
	})
}

func (r *Runner) check(message proto.Message, namespaceSource func() (string, error)) error {
	if r.initErr != nil {
		return r.initErr
	}
	if nilMessage(message) || !message.ProtoReflect().IsValid() {
		return errors.New("dynamicvalidate: message is required")
	}
	refMessage := message.ProtoReflect()
	violations, err := r.checkMessage(refMessage, namespaceSource, "", map[proto.Message]bool{})
	if err != nil {
		return err
	}
	if len(violations) == 0 {
		return nil
	}
	return &ValidationError{Violations: violations}
}

func (r *Runner) checkMessage(message protoreflect.Message, namespaceSource func() (string, error), path string, ancestors map[proto.Message]bool) ([]Violation, error) {
	if !message.IsValid() {
		return nil, nil
	}
	if ancestors[message.Interface()] {
		return nil, fmt.Errorf("dynamicvalidate: cyclic message at %s", path)
	}
	ancestors[message.Interface()] = true
	defer delete(ancestors, message.Interface())
	var violations []Violation
	fields := message.Descriptor().Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		fieldPath := string(field.Name())
		if path != "" {
			fieldPath = path + "." + fieldPath
		}
		fieldViolations, err := r.checkField(message, namespaceSource, field, fieldPath)
		if err != nil {
			return nil, err
		}
		violations = append(violations, fieldViolations...)
		if field.Kind() != protoreflect.MessageKind || !message.Has(field) {
			continue
		}
		visit := func(child protoreflect.Message, childPath string) error {
			childViolations, err := r.checkMessage(child, namespaceSource, childPath, ancestors)
			violations = append(violations, childViolations...)
			return err
		}
		if err := visitChildren(message.Get(field), field, fieldPath, visit); err != nil {
			return nil, err
		}
	}
	return violations, nil
}

func visitChildren(value protoreflect.Value, field protoreflect.FieldDescriptor, path string, visit func(protoreflect.Message, string) error) error {
	switch {
	case field.IsMap():
		if field.MapValue().Kind() != protoreflect.MessageKind {
			return nil
		}
		var keys []protoreflect.MapKey
		value.Map().Range(func(key protoreflect.MapKey, _ protoreflect.Value) bool { keys = append(keys, key); return true })
		slices.SortFunc(keys, func(a, b protoreflect.MapKey) int {
			return strings.Compare(fmt.Sprint(a.Interface()), fmt.Sprint(b.Interface()))
		})
		for _, key := range keys {
			keyPath := fmt.Sprint(key.Interface())
			if field.MapKey().Kind() == protoreflect.StringKind {
				keyPath = strconv.Quote(keyPath)
			}
			if err := visit(value.Map().Get(key).Message(), path+"["+keyPath+"]"); err != nil {
				return err
			}
		}
	case field.IsList():
		for j := range value.List().Len() {
			if err := visit(value.List().Get(j).Message(), fmt.Sprintf("%s[%d]", path, j)); err != nil {
				return err
			}
		}
	default:
		return visit(value.Message(), path)
	}
	return nil
}

func (r *Runner) checkField(message protoreflect.Message, namespaceSource func() (string, error), field protoreflect.FieldDescriptor, path string) ([]Violation, error) {
	names, err := annotatedRules(field)
	if err != nil || len(names) == 0 {
		return nil, err
	}
	namespace := ""
	hasNamespace := false
	var violations []Violation
	for _, name := range names {
		rule, err := r.resolve(name, field)
		if err != nil {
			return nil, err
		}
		if field.HasPresence() && !message.Has(field) {
			continue
		}
		if rule.scope == namespaceScope && !hasNamespace {
			namespace, err = namespaceSource()
			if err != nil {
				return nil, err
			}
			hasNamespace = true
		}
		if err := rule.validate(namespace, fieldValue(message, field)); err != nil {
			violations = append(violations, Violation{FieldPath: path, RuleID: string(name), Message: err.Error()})
		}
	}
	return violations, nil
}

// Precompile checks registration, field types, and namespace access.
func (r *Runner) Precompile(messages ...proto.Message) error {
	errs := []error{r.initErr}
	for _, message := range messages {
		errs = append(errs, r.precompile(message, message))
	}
	return errors.Join(errs...)
}

// PrecompileResponse checks response rules against the request namespace schema.
func (r *Runner) PrecompileResponse(response, request proto.Message) error {
	return errors.Join(r.initErr, r.precompile(response, request))
}

func (r *Runner) precompile(message, namespaceSource proto.Message) error {
	if nilMessage(message) {
		return errors.New("dynamicvalidate: message type is required")
	}
	var errs []error
	visited := map[protoreflect.FullName]bool{}
	var walk func(protoreflect.MessageDescriptor)
	walk = func(descriptor protoreflect.MessageDescriptor) {
		if visited[descriptor.FullName()] {
			return
		}
		visited[descriptor.FullName()] = true
		fields := descriptor.Fields()
		for i := range fields.Len() {
			field := fields.Get(i)
			names, err := annotatedRules(field)
			errs = append(errs, err)
			for _, name := range names {
				rule, err := r.resolve(name, field)
				errs = append(errs, err)
				if err == nil && rule.scope == namespaceScope {
					if nilMessage(namespaceSource) {
						errs = append(errs, errors.New("dynamicvalidate: request namespace source is required"))
					} else {
						_, err = messageNamespace(namespaceSource.ProtoReflect())
						errs = append(errs, err)
					}
				}
			}
			if field.Message() != nil {
				walk(field.Message())
			}
		}
	}
	walk(message.ProtoReflect().Descriptor())
	return errors.Join(errs...)
}

func (r *Runner) resolve(name protoreflect.FullName, field protoreflect.FieldDescriptor) (Rule, error) {
	if field.IsList() || field.IsMap() {
		return Rule{}, fmt.Errorf("dynamicvalidate: %s requires a singular field, got %s", name, field.FullName())
	}
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
	if field == nil || field.Kind() != protoreflect.StringKind || field.IsList() || field.IsMap() {
		return "", fmt.Errorf("dynamicvalidate: message %s uses namespace rules but has no string namespace field", message.Descriptor().FullName())
	}
	return message.Get(field).String(), nil
}

func nilMessage(message proto.Message) bool {
	return message == nil || (reflect.ValueOf(message).Kind() == reflect.Pointer && reflect.ValueOf(message).IsNil())
}
