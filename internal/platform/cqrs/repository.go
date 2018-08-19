package cqrs

import (
	"fmt"
	"time"

	"github.com/sokool/gokit/log"
	"github.com/sokool/shelf2/internal/platform/cqrs/es"
)

type Repository struct {
	store      es.Storage
	publisher  es.Publisher
	serializer Serializer
}

func NewRepository(s es.Storage, p es.Publisher, m Serializer) *Repository {
	return &Repository{
		store:      s,
		publisher:  p,
		serializer: m,
	}
}

func (r *Repository) Reader(events Registry, aggregate, id string) *EventReader {
	return NewEventReader(r.serializer, r.store, events, aggregate, id)
}

func (r *Repository) Load(a AggregateRoot, events Registry) error {
	id, aggregate, _ := a.Details()

	payload, err := r.store.FromVersion(es.Aggregate{
		ID:   id,
		Type: aggregate}, 0)

	if err != nil {
		return fmt.Errorf("%s could not load events: %s", aggregate, err)
	}

	for i, d := range payload.Events {
		event, err := events.Type(string(payload.Events[i].Type))
		if err != nil {
			log.Debug("es.repository", "event %s exists, but %s aggregate ignores it",
				payload.Events[i].Type,
				aggregate)
			continue
		}

		v := event.Interface()
		if err := r.serializer.Unmarshal(payload.Events[i].Data, &v); err != nil {
			return fmt.Errorf("event %s decoding: %s", payload.Events[i].Type, err)
		}

		var m Meta
		if err := r.serializer.Unmarshal(payload.Events[i].Meta, &m); err != nil {
			return fmt.Errorf("meta %s decoding: %s", payload.Events[i].Type, err)
		}

		e := Event{
			Aggregate: payload.Aggregate,
			Data:      event.Elem().Interface(),
			Meta:      m,
			Type:      d.Type,
			Version:   d.Version,
			CreatedAt: d.CreatedAt,
		}

		if err := a.Handle(e); err != nil {
			return fmt.Errorf("event %s handling: %s", payload.Events[i].Type, err)
		}
	}

	return nil
}

func (r *Repository) Store(a AggregateRoot, m Meta) error {
	id, n, version := a.Details()
	payload := es.AggregateEvents{
		Aggregate: es.Aggregate{
			ID:   id,
			Type: n},
		Events: nil}

	date := time.Now()
	var nn []string
	for _, event := range a.Uncommitted(true) {
		en := name(event)
		nn = append(nn, en)
		data, err := r.serializer.Marshal(event)
		if err != nil {
			return fmt.Errorf("%s could not decode %s event: %s", n, en, err)
		}

		meta, err := r.serializer.Marshal(m)
		if err != nil {
			return fmt.Errorf("%s could not decode meta: %s", n, err)
		}

		//version++
		payload.Events = append(payload.Events, es.Event{
			Type:      en,
			Data:      data,
			Meta:      meta,
			CreatedAt: date,
		})
	}

	if err := r.store.Append(payload, version); err != nil {
		return fmt.Errorf("%s could not store events: %s", n, err)
	}

	if len(nn) > 0 {
		log.Debug("es.repository", "%s.%s%+v events stored", id, n, nn)
	}

	if r.publisher != nil {
		if err := r.publisher.Publish(payload); err != nil {
			log.Error("cqrs.repository.publisher", err)
		}
	}

	return nil
}

func (r *Repository) Copy(aggregate, src string, from uint, dst string) error {
	return r.store.Copy(aggregate, src, from, dst)
}
