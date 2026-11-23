package ring

import (
	"fmt"
	"testing"
)

func TestLookupStableAndMinimallyRemapped(t *testing.T) {
	r := New(64)
	r.Set([]string{"a", "b", "c"})

	owners := map[string]string{}
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("prefix-%d", i)
		owner := r.Lookup(key)
		if owner == "" {
			t.Fatalf("no owner for %s", key)
		}
		if again := r.Lookup(key); again != owner {
			t.Fatalf("unstable lookup for %s: %s then %s", key, owner, again)
		}
		owners[key] = owner
	}

	r.Remove("c")
	moved, intact := 0, 0
	for key, before := range owners {
		after := r.Lookup(key)
		if after == "c" {
			t.Fatalf("removed worker still owns %s", key)
		}
		if after != before {
			moved++
		} else {
			intact++
		}
	}
	if moved == 0 || intact == 0 {
		t.Fatalf("expected partial remapping, got moved=%d intact=%d", moved, intact)
	}
	// Roughly 1/3 of keys should move when one of three workers is removed.
	if moved > len(owners)/2 {
		t.Fatalf("too many keys remapped: %d/%d", moved, len(owners))
	}
}

func TestEmptyRing(t *testing.T) {
	if got := New(8).Lookup("anything"); got != "" {
		t.Fatalf("expected empty owner, got %q", got)
	}
}
