// Package prefix turns prompts into stable prefix hashes used for KV-cache
// affinity. Hashes are cumulative: reusing block i implies blocks 0..i-1 are
// also present in the backend's KV cache.
package prefix

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
)

// Normalize collapses whitespace and case so semantically identical prompts
// map to the same cache key.
func Normalize(s string) string {
	var b strings.Builder
	space := false
	for _, r := range s {
		switch {
		case unicode.IsSpace(r):
			space = true
		case unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsPunct(r):
			if space && b.Len() > 0 {
				b.WriteByte(' ')
			}
			space = false
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return strings.TrimSpace(b.String())
}

// Hashes returns a hash for the whole prompt plus cumulative block hashes.
// The last block hash always equals the full prompt hash.
func Hashes(prompt string, blockSize int) (string, []string) {
	if blockSize <= 0 {
		blockSize = 64
	}
	n := Normalize(prompt)
	runes := []rune(n)
	blocks := make([]string, 0, len(runes)/blockSize+1)
	chain := sha256.New()
	for start := 0; start < len(runes); start += blockSize {
		end := start + blockSize
		if end > len(runes) {
			end = len(runes)
		}
		chain.Write([]byte(string(runes[start:end])))
		blocks = append(blocks, hex.EncodeToString(chain.Sum(nil)))
	}
	if len(blocks) == 0 {
		full := sha256.Sum256([]byte(n))
		h := hex.EncodeToString(full[:])
		return h, []string{h}
	}
	return blocks[len(blocks)-1], blocks
}

// EstimateTokens is a deliberately cheap approximation (4 chars per token)
// used only for routing and metrics, not for billing.
func EstimateTokens(prompt string) int64 {
	return int64(len(Normalize(prompt))/4) + 1
}
