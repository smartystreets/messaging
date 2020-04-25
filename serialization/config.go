package serialization

import (
	"reflect"
)

type configuration struct {
	MessageTypes map[reflect.Type]string
	ReadTypes    map[string]reflect.Type

	Deserializers map[string]Deserializer
	Serializer    Serializer
}
