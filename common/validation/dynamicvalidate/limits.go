package dynamicvalidate

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

func GlobalByteLengthRule(limit func() int) Rule {
	if limit == nil {
		return GlobalStringRule(nil)
	}
	return GlobalStringRule(func(value string) error { return checkSize(len(value), limit()) })
}

func NamespaceByteLengthRule(limit func(string) int) Rule {
	if limit == nil {
		return NamespaceStringRule(nil)
	}
	return NamespaceStringRule(func(namespace, value string) error { return checkSize(len(value), limit(namespace)) })
}

func NamespaceMessageSizeRule[M proto.Message](message M, limit func(string) int, size func(M) int) Rule {
	if limit == nil || size == nil {
		return NamespaceMessageRule(message, nil)
	}
	return NamespaceMessageRule(message, func(namespace string, value M) error { return checkSize(size(value), limit(namespace)) })
}

func checkSize(size, limit int) error {
	if size > limit {
		return fmt.Errorf("size %d exceeds limit %d", size, limit)
	}
	return nil
}
