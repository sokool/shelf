package cqrs

import (
	"fmt"
	"reflect"
)

type Registry map[string]interface{}

func (Registry) New(events ...interface{}) Registry {
	r := Registry{}
	for _, e := range events {
		if err := r.Register(e, ""); err != nil {
			panic(err)
		}
	}

	return r
}

func (r Registry) Register(v interface{}, n string) error {
	if n == "" {
		n = name(v)
	}

	if _, ok := r[n]; ok {
		return nil
	}

	r[n] = v
	return nil
}

func (r Registry) Names() []string {
	var o []string
	for n := range r {
		o = append(o, n)
	}
	return o
}

func (r Registry) Type(name string) (reflect.Value, error) {
	o, ok := r[name]
	if !ok {
		return reflect.Value{}, fmt.Errorf("not registered")
	}

	return reflect.New(reflect.TypeOf(o)), nil
}

func (r Registry) Is(name string) bool {
	_, ok := r[name]
	return ok
}

func (r Registry) List() []interface{} {
	var out []interface{}
	for _, e := range r {
		out = append(out, e)
	}

	return out
}
