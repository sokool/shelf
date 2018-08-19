package es

type memPubSub struct {
	subs2 []memSubscription
}

type memSubscription struct {
	name          string
	handler       func(Aggregate, Event)
	subscriptions map[string]map[string]bool
	stream        chan AggregateEvents
}

func (m *memSubscription) run() {
	for a := range m.stream {
		//go func(s memSubscription) {
		subscription, ok := m.subscriptions[a.Type]
		if !ok {
			continue
		}

		//all
		if len(subscription) == 0 {

		}

		for _, event := range a.Events {
			if !subscription[event.Type] {
				continue
			}

			m.handler(a.Aggregate, event)
		}
	}

}
func NewMemPubSub() *memPubSub {
	return &memPubSub{}
}

func (p *memPubSub) Publish(a AggregateEvents) error {
	for _, s := range p.subs2 {
		go func(s memSubscription) { s.stream <- a }(s)
	}

	return nil
}

func (p *memPubSub) Subscribe(s Subscription) error {
	z := memSubscription{
		name:          s.name,
		handler:       s.handler,
		subscriptions: s.subscriptions,
		stream:        make(chan AggregateEvents),
	}


	go z.run()

	p.subs2 = append(p.subs2, z)
	return nil
}
