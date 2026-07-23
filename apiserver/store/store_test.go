package store

import (
	"bytes"
	"testing"
)

func TestTokenHashIsDeterministicAndPeppered(t *testing.T) {
	first := TokenHash("token", "pepper-a")
	if len(first) != 32 {
		t.Fatalf("hash length = %d, want 32", len(first))
	}
	if !bytes.Equal(first, TokenHash("token", "pepper-a")) {
		t.Fatal("same token and pepper produced different hashes")
	}
	if bytes.Equal(first, TokenHash("token", "pepper-b")) {
		t.Fatal("different peppers produced the same hash")
	}
}
