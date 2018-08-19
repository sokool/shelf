package es

import "fmt"

type memStore struct {
	events map[string]AggregateEvents
}

func (m *memStore) Copy(aggregate, src string, after uint, dst string) error {
	source, ok := m.events[src+aggregate]
	if !ok {
		return fmt.Errorf("%s:%s source not found", aggregate, src)
	}

	a := AggregateEvents{
		Aggregate: Aggregate{
			ID:   dst,
			Type: source.Aggregate.Type,
		},
	}
	for i := range source.Events {
		if source.Events[i].Version < after {
			continue
		}
		a.Events = append(a.Events, source.Events[i])
	}

	return m.Append(a, 0)
}

func NewMemory() Storage {
	return &memStore{events: make(map[string]AggregateEvents)}
}

func (m *memStore) Append(ae AggregateEvents, v uint) error {
	a, ok := m.events[ae.ID+ae.Type]
	if ok {
		v := a.Events[len(a.Events)-1].Version
		for i := range ae.Events {
			v++
			ae.Events[i].Version = v
			a.Events = append(a.Events, ae.Events[i])
		}

		m.events[ae.ID+ae.Type] = a

		return nil
	}

	for i := range ae.Events {
		ae.Events[i].Version = uint(i + 1)
	}
	m.events[ae.ID+ae.Type] = ae

	return nil
}

func (m *memStore) FromVersion(a Aggregate, v uint) (AggregateEvents, error) {
	if v == 0 {
		return m.events[a.ID+a.Type], nil
	}

	panic("implement me")
}

func (m *memStore) All(aggregate string) ([]AggregateEvents, error) {
	panic("implement me")
}
