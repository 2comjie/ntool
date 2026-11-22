package fsm2

import (
	"fmt"
	"time"
)

type SimpleState[T comparable] struct {
	PreEnter      func(previous T, timedOut bool) time.Duration
	PostEnter     func(previous T, timedOut bool)
	PreExit       func(next T, timedOut bool)
	PostExit      func(next T, timedOut bool)
	OnUpdate      func(dt time.Duration)
	OnEvent       func(event any)
	NextOnTimeout func() T
}

type FSM[T comparable] struct {
	BeforeSwitch func(previous, next T, timedOut bool)
	AfterSwitch  func(previous, next T, timedOut bool)

	states        map[T]SimpleState[T]
	state         T
	lastState     T
	leftDuration  time.Duration
	totalDuration time.Duration
	revision      uint64
	switching     bool
	pending       []T
}

func New[T comparable](initial T) *FSM[T] {
	return &FSM[T]{
		states:        make(map[T]SimpleState[T]),
		state:         initial,
		lastState:     initial,
		leftDuration:  -1,
		totalDuration: -1,
	}
}

func (f *FSM[T]) Register(id T, state SimpleState[T]) {
	if _, exists := f.states[id]; exists {
		panic(fmt.Sprintf("fsm2: state %v is already registered", id))
	}
	f.states[id] = state
}

func (f *FSM[T]) CurState() T {
	return f.state
}

func (f *FSM[T]) LastState() T {
	return f.lastState
}

func (f *FSM[T]) LeftDuration() time.Duration {
	return f.leftDuration
}

func (f *FSM[T]) TotalDuration() time.Duration {
	return f.totalDuration
}

func (f *FSM[T]) Switch(next T) {
	f.switchState(next, false)
}

func (f *FSM[T]) Update(dt time.Duration) {
	if dt < 0 {
		panic("fsm2: update duration must be non-negative")
	}
	state := f.registeredState(f.state)
	revision := f.revision
	if f.leftDuration >= 0 {
		dt = min(dt, f.leftDuration)
		f.leftDuration -= dt
	}
	if state.OnUpdate != nil {
		state.OnUpdate(dt)
	}
	// OnUpdate 可以主动切换或重新进入同一状态，旧阶段的超时此时已经失效。
	if f.revision == revision && f.leftDuration == 0 {
		next := state.NextOnTimeout()
		if f.revision == revision {
			f.switchState(next, true)
		}
	}
}

func (f *FSM[T]) TriggerEvent(event any) {
	if state := f.registeredState(f.state); state.OnEvent != nil {
		state.OnEvent(event)
	}
}

func (f *FSM[T]) switchState(next T, timedOut bool) {
	if f.switching {
		f.pending = append(f.pending, next)
		return
	}
	f.switching = true
	defer func() {
		f.switching = false
		clear(f.pending)
		f.pending = f.pending[:0]
	}()
	f.applySwitch(next, timedOut)
	for i := 0; i < len(f.pending); i++ {
		f.applySwitch(f.pending[i], false)
	}
}

func (f *FSM[T]) applySwitch(next T, timedOut bool) {
	previous := f.state
	before := f.registeredState(previous)
	after := f.registeredState(next)
	if f.BeforeSwitch != nil {
		f.BeforeSwitch(previous, next, timedOut)
	}
	if before.PreExit != nil {
		before.PreExit(next, timedOut)
	}
	duration := time.Duration(-1)
	if after.PreEnter != nil {
		duration = after.PreEnter(previous, timedOut)
	}
	if duration >= 0 && after.NextOnTimeout == nil {
		panic(fmt.Sprintf("fsm2: timed state %v requires NextOnTimeout", next))
	}

	f.lastState = previous
	f.state = next
	f.leftDuration = duration
	f.totalDuration = duration
	f.revision++

	if before.PostExit != nil {
		before.PostExit(next, timedOut)
	}
	if after.PostEnter != nil {
		after.PostEnter(previous, timedOut)
	}
	if f.AfterSwitch != nil {
		f.AfterSwitch(previous, next, timedOut)
	}
}

func (f *FSM[T]) registeredState(id T) SimpleState[T] {
	state, ok := f.states[id]
	if !ok {
		panic(fmt.Sprintf("fsm2: state %v is not registered", id))
	}
	return state
}
