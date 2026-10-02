package validation

import (
	"errors"
	"regexp"

	"buf.build/go/protovalidate"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/validation/dynamicvalidate"
	"google.golang.org/protobuf/proto"
)

const maxResponseDiagnostics = 10

var collectionIndex = regexp.MustCompile(`\[(?:"(?:\\.|[^"\\])*"|[^\]]*)\]`)

type responseDiagnostics struct {
	kind          string
	cause         string
	count         int
	fields, rules []string
}

func (d *responseDiagnostics) add(field, rule string) {
	d.count++
	if len(d.fields) < maxResponseDiagnostics {
		d.fields = append(d.fields, collectionIndex.ReplaceAllString(field, "[*]"))
		d.rules = append(d.rules, rule)
	}
}

func (r *Registry) reportResponse(registration methodRegistration, response any, namespace string) {
	diagnostics := r.checkResponse(registration, response, namespace)
	if diagnostics.kind == "" {
		return
	}
	metrics.ResponseValidationFailures.With(r.metrics).Record(1,
		metrics.OperationTag(registration.Method), metrics.StringTag("validation_failure_type", diagnostics.kind))
	registration.logger.Warn("response validation failed",
		tag.NewStringTag("failure_type", diagnostics.kind),
		tag.NewStringTag("cause", diagnostics.cause),
		tag.NewInt("violation_count", diagnostics.count),
		tag.NewStringsTag("field_paths", diagnostics.fields),
		tag.NewStringsTag("rule_ids", diagnostics.rules))
}

func (r *Registry) checkResponse(registration methodRegistration, response any, namespace string) (diagnostics responseDiagnostics) {
	// A validator bug must not replace an already successful handler result.
	defer func() {
		if recover() != nil {
			diagnostics.kind = "implementation"
			diagnostics.cause = "validator_panic"
		}
	}()
	message, ok := response.(proto.Message)
	if !ok || nilMessage(message) || !message.ProtoReflect().IsValid() {
		diagnostics.kind = "implementation"
		diagnostics.cause = "missing_or_invalid_response"
		return diagnostics
	}
	if message.ProtoReflect().Descriptor().FullName() != registration.Response.ProtoReflect().Descriptor().FullName() {
		diagnostics.kind = "implementation"
		diagnostics.cause = "wrong_response_type"
		return diagnostics
	}
	// Dynamic message callbacks receive mutable protobuf pointers.
	message = proto.Clone(message)
	if err := r.static.Validate(message); err != nil {
		var violation *protovalidate.ValidationError
		if errors.As(err, &violation) {
			diagnostics.kind = "violation"
			for _, item := range violation.Violations {
				diagnostics.add(protovalidate.FieldPathString(item.Proto.GetField()), item.Proto.GetRuleId())
			}
		} else {
			diagnostics.kind = "implementation"
			diagnostics.cause = "static_validator"
		}
	}
	if err := registration.Dynamic.CheckResponse(message, namespace); err != nil {
		var violation *dynamicvalidate.ValidationError
		if errors.As(err, &violation) {
			if diagnostics.kind == "" {
				diagnostics.kind = "violation"
			}
			for _, item := range violation.Violations {
				diagnostics.add(item.FieldPath, item.RuleID)
			}
		} else if errors.Is(err, dynamicvalidate.ErrResponseNamespaceMissing) {
			if diagnostics.kind != "implementation" {
				diagnostics.kind = "namespace_context"
				diagnostics.cause = "missing_request_namespace"
			}
		} else {
			diagnostics.kind = "implementation"
			diagnostics.cause = "dynamic_validator"
		}
	}
	return diagnostics
}
