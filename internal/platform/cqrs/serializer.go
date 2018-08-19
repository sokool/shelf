package cqrs

import "encoding/json"

type Serializer interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
}

type jsonSerializer struct{}

func (*jsonSerializer) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (*jsonSerializer) Unmarshal(d []byte, o interface{}) error {
	return json.Unmarshal(d, o)
}
