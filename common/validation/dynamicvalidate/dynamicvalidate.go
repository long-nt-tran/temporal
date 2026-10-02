// Package dynamicvalidate calls typed functions selected by protobuf fields.
package dynamicvalidate

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	"go.temporal.io/api/temporalvalidate/rules"
	apivalidation "go.temporal.io/api/temporalvalidate/validation"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type ValidationContext struct {
	Request proto.Message
}

type Runner struct {
	validator apivalidation.Validator[ValidationContext]
	catalogs  sync.Map
	fields    sync.Map
}

type resolvedFunctions struct {
	names []string
	err   error
}

func New(validator apivalidation.Validator[ValidationContext]) *Runner {
	return &Runner{validator: validator}
}

type Violation struct {
	FieldPath string
	RuleID    string
	Message   string
}

type ValidationError struct{ Violations []Violation }

func (e *ValidationError) Error() string {
	var messages []string
	for _, violation := range e.Violations {
		messages = append(messages, violation.FieldPath+": "+violation.Message)
	}
	return strings.Join(messages, "; ")
}

// Precompile rejects stale generated signatures or a missing implementation.
func (r *Runner) Precompile(messages ...proto.Message) error {
	visited := map[protoreflect.FullName]bool{}
	var walk func(protoreflect.MessageDescriptor) error
	walk = func(message protoreflect.MessageDescriptor) error {
		if visited[message.FullName()] {
			return nil
		}
		visited[message.FullName()] = true
		for i := range message.Fields().Len() {
			field := message.Fields().Get(i)
			if _, err := r.functions(field); err != nil {
				return err
			}
			if field.Message() != nil {
				if err := walk(field.Message()); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, message := range messages {
		if nilMessage(message) {
			return errors.New("validation: message type is required")
		}
		if err := walk(message.ProtoReflect().Descriptor()); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) CheckMessage(message proto.Message) error {
	return r.CheckResponse(message, message)
}

func (r *Runner) CheckResponse(message, request proto.Message) error {
	if nilMessage(message) || !message.ProtoReflect().IsValid() {
		return errors.New("validation: message is required")
	}
	violations, err := r.checkMessage(message.ProtoReflect(), ValidationContext{Request: request}, "", map[proto.Message]bool{})
	if err != nil {
		return err
	}
	if len(violations) == 0 {
		return nil
	}
	return &ValidationError{Violations: violations}
}

func (r *Runner) checkMessage(message protoreflect.Message, ctx ValidationContext, path string, ancestors map[proto.Message]bool) ([]Violation, error) {
	if !message.IsValid() {
		return nil, nil
	}
	if ancestors[message.Interface()] {
		return nil, fmt.Errorf("validation: cyclic message at %s", path)
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
		names, err := r.functions(field)
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			if err := apivalidation.Invoke(r.validator, ctx, name, field, message.Get(field)); err != nil {
				violations = append(violations, Violation{FieldPath: fieldPath, RuleID: name, Message: err.Error()})
			}
		}
		if field.Kind() != protoreflect.MessageKind || !message.Has(field) {
			continue
		}
		visit := func(child protoreflect.Message, childPath string) error {
			childViolations, err := r.checkMessage(child, ctx, childPath, ancestors)
			violations = append(violations, childViolations...)
			return err
		}
		if err := visitChildren(message.Get(field), field, fieldPath, visit); err != nil {
			return nil, err
		}
	}
	return violations, nil
}

func (r *Runner) functions(field protoreflect.FieldDescriptor) ([]string, error) {
	if cached, ok := r.fields.Load(field); ok {
		result, ok := cached.(resolvedFunctions)
		if !ok {
			return nil, fmt.Errorf("validation: invalid function cache entry for %s", field.FullName())
		}
		return result.names, result.err
	}
	names, err := r.resolveFunctions(field)
	cached, _ := r.fields.LoadOrStore(field, resolvedFunctions{names: names, err: err})
	result, ok := cached.(resolvedFunctions)
	if !ok {
		return nil, fmt.Errorf("validation: invalid function cache entry for %s", field.FullName())
	}
	return result.names, result.err
}

func (r *Runner) resolveFunctions(field protoreflect.FieldDescriptor) ([]string, error) {
	file := field.ParentFile()
	cached, ok := r.catalogs.Load(file)
	if !ok {
		catalog, err := rules.New([]protoreflect.FileDescriptor{file})
		if err != nil {
			return nil, err
		}
		cached, _ = r.catalogs.LoadOrStore(file, catalog)
	}
	catalog, ok := cached.(*rules.Catalog)
	if !ok {
		return nil, fmt.Errorf("validation: invalid catalog cache entry for %s", file.Path())
	}
	selected, err := catalog.Lookup(field)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, rule := range selected {
		if r.validator == nil {
			return nil, fmt.Errorf("no implementation for %s", rule.Function)
		}
		if err := apivalidation.Check(rule.Function, field); err != nil {
			return nil, err
		}
		names = append(names, rule.Function)
	}
	return names, nil
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

func nilMessage(message proto.Message) bool {
	return message == nil || (reflect.ValueOf(message).Kind() == reflect.Pointer && reflect.ValueOf(message).IsNil())
}
