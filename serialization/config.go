package serialization

import (
	"reflect"
)

type configuration struct {
	Deserializers map[string]Deserializer
	ReadTypes     map[string]reflect.Type
}
