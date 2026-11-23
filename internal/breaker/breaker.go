// Package breaker implements the per-backend circuit breaker used to isolate a
// dying model server instead of sending every request into its timeout.
package breaker

import (
	"sync"
	"time"
)

type state int

const (
	closed state = iota
	open
	halfOpen
)

// Breaker is a small sliding-window circuit breaker.
type Breaker struct {
	mu        sync.Mutex
	state     state
	failures  int
	firstFail time.Time
	openedAt  time.Time
	probe     bool

	threshold int
	window    time.Duration
	cooldown  time.Duration
}

func New(threshold int, window, cooldown time.Duration) *Breaker {
	if threshold <= 0 {
		threshold = 5
	}
	return &Breaker{threshold: threshold, window: window, cooldown: cooldown}
}

// Allow reports whether a request may be attempted, consuming a half-open
// probe slot when applicable.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	switch b.state {
	case open:
		if now.Sub(b.openedAt) < b.cooldown {
			return false
		}
		b.state = halfOpen
		b.probe = true
		return true
	case halfOpen:
		if b.probe {
			return false
		}
		b.probe = true
		return true
	default:
		return true
	}
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = closed
	b.failures = 0
	b.firstFail = time.Time{}
	b.probe = false
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if b.state == halfOpen {
		b.trip(now)
		return
	}
	if b.failures == 0 || now.Sub(b.firstFail) > b.window {
		b.firstFail = now
		b.failures = 0
	}
	b.failures++
	if b.failures >= b.threshold {
		b.trip(now)
	}
}

// HealthOK records a successful health probe: it closes a half-open breaker
// but does not erase failures accumulated while the breaker was closed.
func (b *Breaker) HealthOK() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == halfOpen {
		b.state = closed
		b.failures = 0
		b.firstFail = time.Time{}
		b.probe = false
	}
}

func (b *Breaker) trip(now time.Time) {
	b.state = open
	b.openedAt = now
	b.probe = false
}

// State returns "closed", "open", or "half-open".
func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case open:
		return "open"
	case halfOpen:
		return "half-open"
	default:
		return "closed"
	}
}
