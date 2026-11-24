package cache

import (
	"context"
	"testing"
	"time"
)

func TestPrefixIndexMemorySetGetOverwriteAndTTL(t *testing.T) {
	idx := NewPrefixIndex(nil, 20*time.Millisecond)
	ctx := context.Background()

	if _, ok := idx.Get(ctx, "missing"); ok {
		t.Fatal("missing key should be a miss")
	}

	idx.Set(ctx, "h1", "worker-1")
	if got, ok := idx.Get(ctx, "h1"); !ok || got != "worker-1" {
		t.Fatalf("get h1 = %q, %v", got, ok)
	}

	idx.Set(ctx, "h1", "worker-2")
	if got, _ := idx.Get(ctx, "h1"); got != "worker-2" {
		t.Fatalf("overwrite should return worker-2, got %q", got)
	}

	idx.Set(ctx, "h2", "worker-1")
	if idx.Size() != 2 {
		t.Fatalf("size = %d, want 2", idx.Size())
	}

	time.Sleep(40 * time.Millisecond)
	if _, ok := idx.Get(ctx, "h1"); ok {
		t.Fatal("expired entry should be a miss")
	}
	if idx.Size() != 1 {
		t.Fatalf("expired entry should be removed, size = %d", idx.Size())
	}
}

func TestPrefixIndexDefaultTTL(t *testing.T) {
	idx := NewPrefixIndex(nil, 0)
	ctx := context.Background()
	idx.Set(ctx, "h", "worker-1")
	if got, ok := idx.Get(ctx, "h"); !ok || got != "worker-1" {
		t.Fatalf("default TTL index get = %q, %v", got, ok)
	}
}
