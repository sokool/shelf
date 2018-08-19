package cqrs

import (
	"fmt"

	"github.com/sokool/gokit/log"
	"github.com/sokool/shelf2/internal/platform/cqrs/es"
)

type Subscriptions map[string]Registry

func (s Subscriptions) Assign(aggregate string, events ...interface{}) error {
	if s[aggregate] == nil {
		s[aggregate] = Registry{}.New(events...)
		return nil
	}

	for _, e := range events {
		if err := s[aggregate].Register(e, ""); err != nil {
			return err
		}
	}

	return nil
}

type ProjectFunc func(Projection) error

func (s Subscriptions) Has(e Event) bool {
	a, ok := s[e.Aggregate.Type]
	if !ok {
		return false
	}

	return a.Is(e.Type)
}

type Subscriber struct {
	subscriber es.Subscriber
	serializer Serializer
}

func NewSubscriber(s es.Subscriber, m Serializer) *Subscriber {
	return &Subscriber{
		subscriber: s,
		serializer: m,
	}
}

func (p *Subscriber) Subscribe(h Projection) error {

	ss := Subscriptions{}
	h.Subscribe(ss)

	handler := func(a es.Aggregate, e es.Event) {
		events, ok := ss[a.Type]
		if !ok {
			log.Error("cqrs", fmt.Errorf("subscription %events not found", a.Type))
			return
		}

		evt, err := events.Type(e.Type)
		if err != nil {
			log.Error("cqrs", err)
			return
		}

		v := evt.Interface()
		if err := p.serializer.Unmarshal(e.Data, &v); err != nil {
			log.Error("cqrs", fmt.Errorf("unmarshal %events %s", a.Type, err))
			return
		}

		var m map[string]string
		if len(e.Meta) > 0 {
			if err := p.serializer.Unmarshal(e.Meta, &m); err != nil {
				//return err
			}
		}

		h.Handle(Event{
			Aggregate: a,
			Data:      evt.Elem().Interface(),
			Meta:      m,
			Type:      e.Type,
			Version:   e.Version,
			CreatedAt: e.CreatedAt,
		})

	}

	z := es.NewSubscription(handler)
	for aggregate, s := range ss {
		z.Name(name(h)).
			AggregateEvents(aggregate, s.Names()...)
	}

	return p.subscriber.Subscribe(*z)
}
