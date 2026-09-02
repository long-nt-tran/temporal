package nexusoperation

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
)

func validationMessages() ([]proto.Message, error) {
	handlerType := reflect.TypeOf((*FrontendHandler)(nil)).Elem()
	messages := make([]proto.Message, 0, handlerType.NumMethod())
	protoMessageType := reflect.TypeFor[proto.Message]()

	for i := range handlerType.NumMethod() {
		method := handlerType.Method(i)
		if method.Type.NumIn() != 2 {
			return nil, fmt.Errorf("%s must accept context and a request", method.Name)
		}
		requestType := method.Type.In(1)
		if requestType.Kind() != reflect.Pointer || !requestType.Implements(protoMessageType) {
			return nil, fmt.Errorf("%s request %s is not a protobuf message", method.Name, requestType)
		}
		messages = append(messages, reflect.New(requestType.Elem()).Interface().(proto.Message))
	}
	return messages, nil
}
