package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestMemoryBucketBurstAndRefill(t *testing.T) {
	l := New(nil, 2, 3)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if !l.Allow(ctx, "tenant-a") {
			t.Fatalf("burst token %d rejected", i)
		}
	}
	if l.Allow(ctx, "tenant-a") {
		t.Fatal("bucket should be empty after burst")
	}
	if !l.Allow(ctx, "tenant-b") {
		t.Fatal("other tenant should have its own bucket")
	}
	time.Sleep(600 * time.Millisecond)
	if !l.Allow(ctx, "tenant-a") {
		t.Fatal("bucket should refill")
	}
}
