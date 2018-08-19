package es

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sokool/gokit/log"
	"github.com/streadway/amqp"
)

const rtag = "es.rabbitMQ"

type rabbitMQ struct {
	url string

	errors chan error
	finish chan error

	subscribers struct {
		assign chan Subscription
		read   chan string
	}

	rabbit struct {
		connect   chan string
		subscribe chan Subscription
		publish   chan AggregateEvents
	}
}

// [options]
// - channel per subscription
// - channel per aggregate
// - with context to shutdown it
// - function receive info after X number of retries
// - wait x sec before reconnect or strategy function
// - message serialization method
//
func NewRabbitMQPubSub(url string) (*rabbitMQ, error) {
	r := &rabbitMQ{
		url:    url,
		errors: make(chan error),
		finish: make(chan error),

		subscribers: struct {
			assign chan Subscription
			read   chan string
		}{
			make(chan Subscription),
			make(chan string),
		},
		rabbit: struct {
			connect   chan string
			subscribe chan Subscription
			publish   chan AggregateEvents
		}{
			make(chan string),
			make(chan Subscription),
			make(chan AggregateEvents),
		},
	}

	var attempts int

	subscriber := func() {
		var ss []Subscription
		for {
			select {
			case s := <-r.subscribers.assign:
				ss = append(ss, s)
				r.rabbit.subscribe <- s

			case <-r.subscribers.read:
				for _, s := range ss {
					r.rabbit.subscribe <- s
				}
			}
		}
	}

	rabbit := func() {
		var connection *amqp.Connection
		var publisher *amqp.Channel
		var subscriber *amqp.Channel
		var start sync.Once

		for {
			select {
			case <-r.rabbit.connect:
				var err error
				connection, err = amqp.DialConfig(r.url, amqp.Config{Heartbeat: time.Second})
				if err != nil {
					r.errors <- err
					break
				}

				// Listen errors from rabbitmq. amqp.Error channel is closed
				// by streadway/amqp, after reconnection.
				go func() {
					for err := range connection.NotifyClose(make(chan *amqp.Error)) {
						r.errors <- err
					}
				}()
				log.Info(rtag, "connected to %s", connection.Config.Vhost)

				if publisher, err = connection.Channel(); err != nil {
					log.Error(rtag, fmt.Errorf("publisher %s", err))
					break
				}

				err = publisher.ExchangeDeclare("events", "topic", false, false, false, false, nil)
				if err != nil {
					log.Error(rtag, fmt.Errorf("publisher %s", err))
					break
				}
				log.Debug(rtag, "publish channel established")

				if subscriber, err = connection.Channel(); err != nil {
					log.Error(rtag, fmt.Errorf("channel %s", err))
					break
				}

				log.Debug(rtag, "subscribe channel established")
				attempts = 0
				r.subscribers.read <- "after reconnection"

				start.Do(func() {
					close(r.finish)
				})

			case s := <-r.rabbit.subscribe:
				if err := r.subscribe(s, subscriber); err != nil {
					log.Error(rtag, fmt.Errorf("%s subscribe %s", s.name, err))
				}

			case a := <-r.rabbit.publish:
				if err := r.publish(a, publisher); err != nil {
					log.Error(rtag, fmt.Errorf("%s publish %s", a.ID, err))
				}
			}
		}
	}

	errors := func() {

		for err := range r.errors {
			attempts++
			wait := time.Second
			if attempts > 10 {
				wait = time.Second * 3
			}

			log.Error(rtag, fmt.Errorf("%s, reconnect #%d after %s", err, attempts, wait))
			time.Sleep(wait)

			r.rabbit.connect <- fmt.Sprintf("reconnection #%d", attempts)
		}
	}

	go rabbit()
	go subscriber()
	go errors()

	r.rabbit.connect <- "connecting"

	return r, <-r.finish
}

func (r *rabbitMQ) Publish(a AggregateEvents) error {
	r.rabbit.publish <- a
	return nil
}

func (r *rabbitMQ) Subscribe(s Subscription) error {
	r.subscribers.assign <- s

	return nil
}

func (r *rabbitMQ) publish(a AggregateEvents, p *amqp.Channel) error {
	if p == nil {
		return fmt.Errorf("empty publisher channel")
	}

	for _, e := range a.Events {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}

		name := route(a.ID, a.Type, e.Type)
		err = p.Publish("events", name, true, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        b,
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func (r *rabbitMQ) subscribe(s Subscription, c *amqp.Channel) error {
	if c == nil {
		return fmt.Errorf("empty publisher channel")
	}

	queue, err := c.QueueDeclare(s.name, false, true, false, true, nil)
	if err != nil {
		return err
	}

	var l string
	for _, route := range s.routes {
		if err := c.QueueBind(queue.Name, route, "events", true, nil); err != nil {
			return err
		}

		l += fmt.Sprintf("%s\n\t", route)
	}
	log.Debug(rtag, "%s bind to\n\t%s", queue.Name, l)

	msg, err := c.Consume(queue.Name, "", true, false, false, true, nil)
	if err != nil {
		return err
	}

	go func(deliveries <-chan amqp.Delivery) {
		for d := range deliveries {
			e := Event{}
			if err := json.Unmarshal(d.Body, &e); err != nil {
				log.Error(fmt.Sprintf("%s.%s", rtag, queue.Name), err)
				continue
			}

			rk := strings.Split(d.RoutingKey, ".")
			a := Aggregate{
				ID:   rk[0],
				Type: rk[1]}

			s.handler(a, e)

			log.Debug(rtag, "%s %s.%s.%s[v.%d] delivered", queue.Name, a.ID, a.Type, e.Type, e.Version)
		}
	}(msg)

	return nil
}
