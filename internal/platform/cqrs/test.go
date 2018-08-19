package cqrs

import (
	"fmt"
	"strings"
	"testing"
)

type expectation func(*Scenario, error, *testing.T)

type expectations struct {
	Error     func(words ...string) expectation
	NoError   expectation
	Version   func(number uint) expectation
	Events    func(names ...string) expectation
	Event     func(name string, matcher ...func(interface{}) error) expectation
	NewEvents func(number int) expectation
}

type _case struct {
	description string
	command     interface{}
	expects     []expectation
}

type Scenario struct {
	Expect    expectations
	aggregate AggregateRoot
	handler   func(interface{}) error
	cases     []_case

	events    int
	newEvents int
}

func NewTestScenario(a AggregateRoot, h func(interface{}) error) *Scenario {
	return &Scenario{
		aggregate: a,
		handler:   h,
		Expect: expectations{
			Error:     hasError,
			NoError:   noError,
			Events:    hasEvents,
			Event:     hasEvent,
			NewEvents: hasNewEvents,
			Version:   inVersion,
		},
	}
}

func (s *Scenario) Case(description string, command interface{}, ee ...expectation) *Scenario {
	s.cases = append(s.cases, _case{
		description: description,
		command:     command,
		expects:     ee,
	})

	return s
}

func (s *Scenario) Run(test *testing.T) *Scenario {
	for _, _case := range s.cases {
		test.Run(_case.description, func(test *testing.T) {
			err := s.handler(_case.command)
			events := len(s.aggregate.Uncommitted(false))
			s.newEvents = events - s.events

			// when not expectations in test case, by default it checks
			// errors and number of generated events.
			if len(_case.expects) == 0 {
				_case.expects = append(_case.expects,
					s.Expect.NoError,
					s.Expect.NewEvents(0))
			}

			for i := range _case.expects {
				_case.expects[i](s, err, test)
			}

			s.events = events
		})
	}

	return s
}

func (s *Scenario) Info() *Scenario {
	id, n, v := s.aggregate.Details()
	fmt.Printf("\n%s.%s[v.%d]\n", id, n, v)
	for i, e := range s.aggregate.Uncommitted(false) {
		fmt.Printf("\tv.%d:%s%+v\n", i+1, name(e), e)
	}
	fmt.Printf("\n")

	return s
}

func noError(_ *Scenario, err error, t *testing.T) {
	if err != nil {
		t.Errorf("uexpected error: %s", err)
	}
}

func hasError(words ...string) expectation {
	return func(_ *Scenario, err error, t *testing.T) {
		var missing []string

		if err == nil {
			t.Errorf("%v error expected", words)
			return
		}

		for i := range words {
			if !strings.Contains(err.Error(), words[i]) {
				missing = append(missing, words[i])
			}
		}

		if len(missing) != 0 {
			t.Errorf("missing %v in [%s] error", missing, err)
			return
		}
	}
}

func inVersion(number uint) expectation {
	return func(r *Scenario, err error, t *testing.T) {
		_, name, v := r.aggregate.Details()
		if v != number {
			t.Errorf("%s aggregate version %d expected, got version %d", name, number, v)
		}
	}
}

func hasNewEvents(number int) expectation {
	return func(s *Scenario, e error, t *testing.T) {
		if s.newEvents != number {
			t.Errorf("expected %d events, got: %d", number, s.newEvents)
		}
		//s.newEvents
	}
}

func hasEvents(names ...string) expectation {
	return func(r *Scenario, err error, t *testing.T) {
		id, aggregate, _ := r.aggregate.Details()
		events := r.aggregate.Uncommitted(false)
		if len(events) < len(names) {
			t.Errorf("expected %d events but %s.%s has %d", len(events), id, aggregate, len(events))
			return
		}

		for i, e := range names {
			evt := name(events[i])
			if e != evt {
				t.Errorf("%s expected, got %s", e, evt)
				return
			}
		}
	}
}

func hasEvent(nam string, matcher ...func(interface{}) error) expectation {
	return func(r *Scenario, err error, t *testing.T) {
		var e string
		if err != nil {
			e = ": " + err.Error()
		}
		_, aggregate, _ := r.aggregate.Details()
		if r.newEvents != 1 {
			t.Errorf("%s.%s event expected%s", aggregate, nam, e)
			return
		}

		events := r.aggregate.Uncommitted(false)
		has := name(events[r.events])
		if nam != has {
			t.Errorf("%s.%s event expected, got %s.%s%s", aggregate, nam, aggregate, has, e)
			return
		}

		if len(matcher) == 1 {
			if err := matcher[0](events[r.events]); err != nil {
				t.Errorf("%s.%s event", aggregate, nam)
			}
		}
	}
}
