package breaker

import (
	"testing"
	"time"
)

func TestOpensAndRecoversThroughHalfOpen(t *testing.T) {
	b := New(2, time.Minute, 20*time.Millisecond)
	if !b.Allow() {
		t.Fatal("closed breaker should allow requests")
	}
	b.Failure()
	b.Failure()
	if b.State() != "open" || b.Allow() {
		t.Fatalf("breaker should be open, state=%s", b.State())
	}
	time.Sleep(25 * time.Millisecond)
	if !b.Allow() || b.State() != "half-open" {
		t.Fatalf("expected half-open probe, state=%s", b.State())
	}
	if b.Allow() {
		t.Fatal("only one half-open probe should be allowed")
	}
	b.Success()
	if b.State() != "closed" {
		t.Fatalf("expected closed after probe success, got %s", b.State())
	}
}

func TestHealthOKDoesNotEraseClosedFailures(t *testing.T) {
	b := New(3, time.Minute, time.Second)
	b.Failure()
	b.HealthOK()
	b.Failure()
	b.Failure()
	if b.State() != "open" {
		t.Fatalf("health checks should not reset failure count, got %s", b.State())
	}
}
