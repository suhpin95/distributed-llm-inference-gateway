package prefix

import (
	"testing"
)

func TestNormalize(t *testing.T) {
	if got := Normalize("  The   Gateway\tRoutes  "); got != "the gateway routes" {
		t.Fatalf("normalize = %q", got)
	}
}

func TestCumulativeHashes(t *testing.T) {
	full, blocks := Hashes("the gateway routes tokens. the gateway routes tokens.", 16)
	if len(blocks) < 2 {
		t.Fatalf("expected multiple blocks, got %d", len(blocks))
	}
	if blocks[len(blocks)-1] != full {
		t.Fatal("last block hash must equal full prompt hash")
	}
	_, again := Hashes("THE   GATEWAY routes tokens. the gateway routes tokens.", 16)
	if again[len(again)-1] != full {
		t.Fatal("normalization must produce stable hashes")
	}
	if _, other := Hashes("different prompt", 16); other[len(other)-1] == full {
		t.Fatal("different prompts must not collide")
	}
}
