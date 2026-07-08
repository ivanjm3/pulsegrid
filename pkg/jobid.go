package pkg

import (
	"crypto/rand"
	"fmt"
)

// GenerateJobID produces a UUID v4 string using crypto/rand.
// Format: xxxxxxxx-xxxx-4xxx-[89ab]xxx-xxxxxxxxxxxx (RFC 4122 v4).
func GenerateJobID() (string, error) {
	var uuid [16]byte
	_, err := rand.Read(uuid[:])
	if err != nil {
		return "", fmt.Errorf("generate job id: %w", err)
	}

	// Set version 4 (bits 4-7 of byte 6)
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	// Set variant 10 (bits 6-7 of byte 8)
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4],
		uuid[4:6],
		uuid[6:8],
		uuid[8:10],
		uuid[10:16],
	), nil
}
