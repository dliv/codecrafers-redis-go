package parse

import (
	"testing"
)

func TestParseRedisDB(t *testing.T) {
	expected := map[string]string{"fruit": "apple", "foo": "bar"}
	result, err := ParseRedisDb("./dump.rdb", 0)
	if err != nil {
		t.Fatalf("Failed to parse Redis DB: %s", err)
	}

	if len(result) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(result))
	}

	for key, val := range expected {
		gotVal, ok := result[key]
		if gotVal != val || !ok {
			t.Errorf("Expected key '%s' with value '%s', got '%s'", key, val, result[key])
		}
	}
}

func TestLeft2Bits(t *testing.T) {
	expected := byte(3)
	result := Left_2_bits(byte(251))
	if result != expected {
		t.Errorf("Expected %d, got %d", expected, result)
	}
}
