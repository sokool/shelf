package cqrs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"
)

var (
	DefaultSerializer Serializer = &jsonSerializer{}
)

type Meta map[string]string

type AggregateRoot interface {
	EventHandler
	Details() (id, name string, version uint)
	Uncommitted(bool) []interface{}
}

type Event struct {
	Aggregate struct {
		ID   string
		Type string
	}
	Data      interface{}
	Meta      Meta
	Type      string
	Version   uint
	CreatedAt time.Time
}

func (e Event) String() string {
	return fmt.Sprintf("%s.%s.%s[v.%d]", e.Aggregate.ID, e.Aggregate.Type, e.Type, e.Version)
}

func (e Event) GoString() string {
	return fmt.Sprintf("%s.%s.%s[v.%d]\n\t%+v\n\t%+v", e.Aggregate.ID, e.Aggregate.Type, e.Type, e.Version, e.Data, e.Meta)
}

type Response struct {
	ID      string      `json:"id"`
	Name    string      `json:"name"`
	Error   error       `json:"errors"`
	Version uint        `json:"version"`
	Data    interface{} `json:"response"`
}

func (r Response) MarshalJSON() ([]byte, error) {
	var e string
	if r.Error != nil {
		e = r.Error.Error()
	}

	return json.Marshal(map[string]interface{}{
		"id":      r.ID,
		"name":    r.Name,
		"errors":  e,
		"version": r.Version,
		"data":    r.Data,
	})
}

type CommandHandler interface {
	Handle(id string, command interface{}, m Meta) Response
}

type CommandHandlerFunc func(string, interface{}, Meta) Response

func (c CommandHandlerFunc) Handle(id string, command interface{}, m Meta) Response {
	return c(id, command, m)
}

type EventHandler interface {
	Handle(Event) error
}

type EventHandlerFunc func(Event) error

func (f EventHandlerFunc) Handle(e Event) error {
	return f(e)
}

// Creator helps to initiate/build/create Projection when it is used for a first
// time or when it is rebuilded from scratch.
type Creator interface {
	Create(overwrite ...bool) error
}

// Purger might be used when rebuilding one element of Projection, that's why
// Purge method has ID as a parameter - it removes only one element from a set
// of particular Projection.
// todo implementation usually is used by Projection.?
type Purger interface {
	Delete(id string) error
}

type Type interface {
	Type() string
}

type Validator interface {
	Validate() error
}

type ValidatorFunc func() error

func (f ValidatorFunc) Validate() error {
	return f()
}

type Subscriber2 interface {
	Subscribe(Subscriptions) error
}

type Projection interface {
	//Creator
	Subscriber2
	EventHandler
}

func name(v interface{}) string {
	if v == nil {
		return ""
	}

	if s, ok := v.(string); ok {
		return s
	}

	if v, ok := v.(Type); ok {
		return v.Type()
	}

	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return strings.Replace(t.Name(), "*", "", -1)
}

func MetaFromHTTP(r *http.Request) Meta {
	m := make(Meta)
	for name, header := range r.Header {
		m[name] = header[0]
	}

	return m
}
