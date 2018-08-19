package es

import (
	"fmt"
	"time"
)

type Publisher interface {
	Publish(AggregateEvents) error
}

type Subscriber interface {
	Subscribe(Subscription) error
}

type PublishSubscriber interface {
	Publisher
	Subscriber
}

type Aggregate struct {
	ID   string
	Type string
	//Version ?!
}

type Event struct {
	Data    []byte
	Meta    []byte
	Type    string
	Version uint
	//Snapshot  bool
	CreatedAt time.Time
}

type AggregateEvents struct {
	Aggregate
	Events []Event
}

func (a AggregateEvents) GoString() string {
	s := fmt.Sprintf("%s.%s\n", a.Type, a.ID)
	for _, e := range a.Events {
		s += fmt.Sprintf("\t%d.[%s] \n\t\tData: %s\n\t\tMeta: %s\n", e.Version, e.Type, e.Data, e.Meta)
	}

	return s
}

func (a AggregateEvents) String() string {
	s := fmt.Sprintf("%s.%s\n", a.Type, a.ID)
	for _, e := range a.Events {
		s += fmt.Sprintf("\t%d.[%s] %s\n", e.Version, e.Type, e.Data)
	}

	return s
}

type Storage interface {
	Append(AggregateEvents, uint) error
	FromVersion(Aggregate, uint) (AggregateEvents, error)
	//FromDate(Aggregate, time.Time) (AggregateEvents, error)
	//Changes(buffer int) <-chan AggregateEvents //todo move outside storage
	//Event(version uint) (Event, error) //todo has any value?
	All(aggregate string) ([]AggregateEvents, error)
	//ByDate(id, aggregate string, from time.Time) ([]Event, error)
	//Stream([]Query) ([]Event, error)
	Copy(aggregate, src string, from uint, dst string) error
}

type Subscription struct {
	name          string
	routes        []string
	subscriptions map[string]map[string]bool
	handler       func(Aggregate, Event)
}

func NewSubscription(h func(Aggregate, Event)) *Subscription {
	return &Subscription{
		handler:       h,
		subscriptions: make(map[string]map[string]bool),
	}
}

func (s *Subscription) Name(n string) *Subscription { s.name = n; return s }

func (s *Subscription) AggregateEvents(aggregate string, events ...string) *Subscription {
	if len(events) == 0 {
		s.routes = append(s.routes, route("*", aggregate, "*"))
	}

	for _, e := range events {
		s.routes = append(s.routes, route("*", aggregate, e))
	}

	s.subscriptions[aggregate] = make(map[string]bool)
	for _, event := range events {
		s.subscriptions[aggregate][event] = true
	}

	return s
}

func (s *Subscription) handle(a AggregateEvents) {

}

func route(id, aggregate, event string) string {
	return fmt.Sprintf("%s.%s.%s", id, aggregate, event)
}
