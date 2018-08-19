package es_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/icrowley/fake"
	"github.com/livechat/developers-platform-service/internal/platform/cqrs/es"
	"github.com/sokool/gokit/test/is"
)

func TestPubX(t *testing.T) {
	cases := []struct {
		desc    string
		name    string
		listen  []es.Subscription
		publish []es.AggregateEvents
		expects []es.AggregateEvents
	}{
		{
			desc: "some example",
			name: "xyz",
			listen: []es.Subscription{
				subscription("", "User", "Deleted", "Created"),
				subscription("", "Profile", "EmailChanged", "Banned")},
			publish: []es.AggregateEvents{
				events("y", "User", "Created", "NameAdded", "AddressAdded", "Deleted"),
				events("X", "Profile", "Accepted", "EmailChanged", "AvatarChanged", "Banned", "Removed"),
				events("z", "User", "Deleted")},
			expects: []es.AggregateEvents{
				events("y", "User", "Created", "Deleted"),
				events("x", "Profile", "EmailChanged", "Banned"),
				events("z", "User", "Deleted")},
		},
	}

	ps := es.NewMemPubSub()
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			is.Ok(t, ps.Subscribe(c.name, handle(c.expects), c.listen...))
			for _, e := range c.publish {
				is.Ok(t, ps.Publish(e))
			}

		})
	}
}

func handle(ee ...[]es.AggregateEvents) func(es.Aggregate, es.Event) error {
	i := len(ee)
	return func(a es.Aggregate, e es.Event) error {
		fmt.Println(i, a, e.Type, e.Version)
		return nil
	}
}

func events(id, aggregate string, events ...string) es.AggregateEvents {
	var o es.AggregateEvents

	o.Aggregate = es.Aggregate{
		ID:   id,
		Type: aggregate}

	for i := range events {
		o.Events = append(o.Events, es.Event{
			Data:      []byte(fake.ParagraphsN(1)),
			Meta:      []byte(fake.Paragraph()),
			Type:      events[i],
			Version:   uint(i + 1),
			CreatedAt: time.Now(),
		})
	}

	return o
}

func subscription(id, aggregate string, events ...string) es.Subscription {
	return es.Subscription{
		ID:        id,
		Aggregate: aggregate,
		Events:    events,
	}
}
