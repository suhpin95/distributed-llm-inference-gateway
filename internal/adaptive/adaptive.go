// Package adaptive implements AIMD admission control. Instead of accepting
// every request until latency collapses, the gateway grows its concurrency
// limit slowly and halves it on failure, shedding excess work with 429s.
package adaptive

import (
	"context"
	"sync"
	"time"
)

// Limiter is a dynamically-sized semaphore with a bounded waiting queue.
type Limiter struct {
	mu       sync.Mutex
	limit    int
	max      int
	inFlight int
	success  int
	queue    chan struct{}

	queueWait time.Duration
}

func New(initial, max, queueSize int, queueWait time.Duration) *Limiter {
	if initial <= 0 {
		initial = 1
	}
	if max < initial {
		max = initial
	}
	if queueSize < 0 {
		queueSize = 0
	}
	return &Limiter{
		limit: initial, max: max, queue: make(chan struct{}, queueSize),
		queueWait: queueWait,
	}
}

// Acquire reserves a slot or sheds the request. The returned function must be
// called when the request finishes.
func (l *Limiter) Acquire(ctx context.Context) (func(), bool) {
	l.mu.Lock()
	if l.inFlight < l.limit {
		l.inFlight++
		l.mu.Unlock()
		return l.release(false), true
	}
	l.mu.Unlock()

	waitCtx, cancel := context.WithTimeout(ctx, l.queueWait)
	defer cancel()
	select {
	case l.queue <- struct{}{}:
	case <-waitCtx.Done():
		return nil, false
	}

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		l.mu.Lock()
		if l.inFlight < l.limit {
			l.inFlight++
			l.mu.Unlock()
			return l.release(true), true
		}
		l.mu.Unlock()
		select {
		case <-ticker.C:
		case <-waitCtx.Done():
			<-l.queue
			return nil, false
		}
	}
}

func (l *Limiter) release(waited bool) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			if l.inFlight > 0 {
				l.inFlight--
			}
			l.mu.Unlock()
			if waited {
				<-l.queue
			}
		})
	}
}

// Report feeds the additive-increase/multiplicative-decrease controller.
func (l *Limiter) Report(success bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !success {
		l.limit = max(1, l.limit/2)
		l.success = 0
		return
	}
	l.success++
	if l.success >= 10 && l.limit < l.max {
		l.limit++
		l.success = 0
	}
}

// Stats exposes the current limiter state for metrics.
func (l *Limiter) Stats() (limit, inFlight, queued int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit, l.inFlight, len(l.queue)
}
