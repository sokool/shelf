package cqrs

import (
	"fmt"
	"sync"
	"time"

	"github.com/sokool/gokit/log"
	"github.com/sokool/shelf2/internal/platform/cqrs/es"
)

type EventReader struct {
	serializer Serializer
	store      es.Storage
	events     Registry
	aggregate  es.Aggregate
}

func NewEventReader(m Serializer, s es.Storage, r Registry, name, id string) *EventReader {
	return &EventReader{
		serializer: m,
		store:      s,
		events:     r,
		aggregate: es.Aggregate{
			ID:   id,
			Type: name,
		},
	}
}

func (r *EventReader) Read(h EventHandler) error {
	defer func(t time.Time) {
		log.Debug("read", "time %s", time.Since(t))
	}(time.Now())

	if r.aggregate.ID != "" {
		events, err := r.store.FromVersion(r.aggregate, 0)
		if err != nil {
			return err
		}
		return r.read(events, h)
	}

	events, err := r.store.All(r.aggregate.Type)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	count := len(events)
	jobs := make(chan es.AggregateEvents, 8)

	handler := func(w int, aggregateEvents <-chan es.AggregateEvents) {
		defer wg.Done()
		for a := range aggregateEvents {
			if err := r.read(a, h); err != nil {
				log.Error("cqrs.events.read", fmt.Errorf("[%T] %s", h, err))
			}
		}
	}

	for w := 1; w <= 4; w++ {
		wg.Add(1)
		go handler(w, jobs)
	}

	for i := range events {
		jobs <- events[i]
		if i%100 == 0 {
			log.Debug("read", "left %d", count-i)
		}
	}

	close(jobs)
	wg.Wait()

	return nil

}

func (r *EventReader) read(a es.AggregateEvents, h EventHandler) error {
	for i := range a.Events {
		value, err := r.events.Type(a.Events[i].Type)
		if err != nil {
			//log.Debug("cqrs.events.read", "loaded %s but handler do not want it", a.Events[i].Type)
			continue
		}

		ptr := value.Interface()
		if err := r.serializer.Unmarshal(a.Events[i].Data, &ptr); err != nil {
			return err
		}

		var m Meta
		if len(a.Events[i].Meta) > 0 {
			if err := r.serializer.Unmarshal(a.Events[i].Meta, &m); err != nil {
				return err
			}
		}

		if err := h.Handle(Event{
			Aggregate: a.Aggregate,
			Data:      value.Elem().Interface(),
			Meta:      m,
			Type:      a.Events[i].Type,
			Version:   a.Events[i].Version,
			CreatedAt: a.Events[i].CreatedAt}); err != nil {

			return err
		}
	}

	if len(a.Events) > 0 {
		//log.Debug("cqrs.events.read", "loaded %d events from %s.%s", len(a.Events), a.ID, a.Type)
	}

	return nil
}
