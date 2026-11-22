package fsm

import "fmt"

type Transition[S, E comparable] struct {
	From   S
	Event  E
	To     S
	Before func() error
	After  func()
}

type transitionKey[S, E comparable] struct {
	state S
	event E
}

type FSM[S, E comparable] struct {
	state       S
	transitions map[transitionKey[S, E]]Transition[S, E] // 在状态S 触发 E的时候 执行 Transition
}

func New[S, E comparable](initial S, transitions []Transition[S, E]) *FSM[S, E] {
	table := make(map[transitionKey[S, E]]Transition[S, E], len(transitions))
	for _, transition := range transitions {
		key := transitionKey[S, E]{
			state: transition.From,
			event: transition.Event,
		}
		if _, ex := table[key]; ex {
			panic(fmt.Sprintf("duplicate transition for state %v and event %v", key.state, key.event))
		}
		table[key] = transition
	}
	return &FSM[S, E]{
		state:       initial,
		transitions: table,
	}
}

func (f *FSM[S, E]) State() S {
	return f.state
}

func (f *FSM[S, E]) Fire(event E) error {
	key := transitionKey[S, E]{
		state: f.state,
		event: event,
	}
	transition, ok := f.transitions[key]
	if !ok {
		return fmt.Errorf("fsm: state %v does not accept event %v", f.state, event)
	}
	if transition.Before != nil {
		if err := transition.Before(); err != nil {
			return err
		}
	}
	f.state = transition.To
	if transition.After != nil {
		transition.After()
	}
	return nil
}
