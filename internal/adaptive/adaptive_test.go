package adaptive

import (
	"context"
	"testing"
	"time"
)

func TestShedsWhenQueueIsFull(t *testing.T) {
	l := New(1, 4, 0, 10*time.Millisecond)
	release, ok := l.Acquire(context.Background())
	if !ok {
		t.Fatal("first acquire should succeed")
	}
	if _, ok := l.Acquire(context.Background()); ok {
		t.Fatal("second acquire should be shed when queue is empty")
	}
	release()
	if _, ok := l.Acquire(context.Background()); !ok {
		t.Fatal("slot should be reusable after release")
	}
}

func TestAIMDController(t *testing.T) {
	l := New(4, 8, 0, time.Millisecond)
	for i := 0; i < 10; i++ {
		l.Report(true)
	}
	if limit, _, _ := l.Stats(); limit != 5 {
		t.Fatalf("expected additive increase to 5, got %d", limit)
	}
	l.Report(false)
	if limit, _, _ := l.Stats(); limit != 2 {
		t.Fatalf("expected multiplicative decrease to 2, got %d", limit)
	}
}
