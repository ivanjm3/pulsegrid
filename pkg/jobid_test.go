package pkg

import (
	"regexp"
	"testing"
	"testing/quick"
)

// uuidV4Regex matches RFC 4122 v4 UUID format.
var uuidV4Regex = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
)

// TestGenerateJobID_PropertyUniquenessAndFormat verifies that all generated
// Job IDs are unique and conform to RFC 4122 v4 format.
//
// **Validates: Requirements 1.4**
func TestGenerateJobID_PropertyUniquenessAndFormat(t *testing.T) {
	seen := make(map[string]struct{}, 200)

	f := func(_ byte) bool {
		id, err := GenerateJobID()
		if err != nil {
			t.Logf("GenerateJobID returned error: %v", err)
			return false
		}

		// Property: format must be RFC 4122 v4
		if !uuidV4Regex.MatchString(id) {
			t.Logf("ID %q does not match UUID v4 format", id)
			return false
		}

		// Property: uniqueness across all generated IDs
		if _, exists := seen[id]; exists {
			t.Logf("duplicate ID generated: %s", id)
			return false
		}
		seen[id] = struct{}{}

		return true
	}

	cfg := &quick.Config{MaxCount: 200}
	if err := quick.Check(f, cfg); err != nil {
		t.Fatalf("property test failed: %v", err)
	}
}
