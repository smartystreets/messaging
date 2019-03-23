package messaging

import "reflect"

type RegistrationDiscovery struct {
	types map[reflect.Type]string
}

func NewRegistrationDiscovery(types map[reflect.Type]string) *RegistrationDiscovery {
	if types == nil {
		types = map[reflect.Type]string{}
	}

	return &RegistrationDiscovery{types: types}
}

func (this *RegistrationDiscovery) RegisterInstance(instance interface{}, typeName string) {
	this.RegisterType(reflect.TypeOf(instance), typeName)
}

func (this *RegistrationDiscovery) RegisterType(_type reflect.Type, typeName string) {
	this.types[_type] = typeName
}

func (this *RegistrationDiscovery) Discover(instance interface{}) (string, error) {
	if typeName, found := this.types[reflect.TypeOf(instance)]; found {
		return typeName, nil
	} else {
		return "", MessageTypeDiscoveryError
	}
}
