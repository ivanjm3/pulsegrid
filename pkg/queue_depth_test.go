package pkg

import (
	"testing"
)

func TestItoa(t *testing.T) {
	tests := []struct {
		input    int
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{9, "9"},
		{10, "10"},
		{80, "80"},
		{443, "443"},
		{8080, "8080"},
		{9092, "9092"},
		{65535, "65535"},
	}
	for _, tc := range tests {
		got := itoa(tc.input)
		if got != tc.expected {
			t.Errorf("itoa(%d) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestFetchQueueDepth_NoBrokers(t *testing.T) {
	// With unreachable brokers, fetchQueueDepth should return error.
	ctx := t.Context()
	_, err := fetchQueueDepth(ctx, []string{"localhost:19999"}, "test-topic", "test-group")
	if err == nil {
		t.Error("expected error when brokers unreachable, got nil")
	}
}
