package pkg

import (
	"regexp"
	"testing"
	"testing/quick"
)

var uuidV4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// TestJobIDUniquenessAndFormat is Property 1: Job ID Uniqueness and Format.
// Validates Requirements 1.4: generate 100+ random upload contexts, verify all
// Job IDs are unique and conform to RFC 4122 v4 format.
func TestJobIDUniquenessAndFormat(t *testing.T) {
	const iterations = 200
	seen := make(map[string]bool, iterations)

	f := func(_ uint32) bool {
		id, err := NewJobID()
		if err != nil {
			t.Fatalf("NewJobID: %v", err)
		}
		if !uuidV4Pattern.MatchString(id) {
			t.Errorf("job id %q does not match RFC 4122 v4 format", id)
			return false
		}
		if seen[id] {
			t.Errorf("duplicate job id generated: %q", id)
			return false
		}
		seen[id] = true
		return true
	}

	cfg := &quick.Config{MaxCount: iterations}
	if err := quick.Check(f, cfg); err != nil {
		t.Error(err)
	}
}
