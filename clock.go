package main

import "time"

// Clock is the minimal time source used by the demo, so failure scenarios
// can be driven without sleeping in real time.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time { return f.t }
func (f *fakeClock) Advance(d time.Duration) {
	f.t = f.t.Add(d)
}
