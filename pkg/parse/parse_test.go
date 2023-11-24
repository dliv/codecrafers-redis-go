package parse

import (
	"testing"
)

func TestParseRedisDB(t *testing.T) {
	// expected := map[string]string{"foo": "1", "bar": "2"}
	expected := map[string]string{"foo": ""}
	result, err := ParseRedisDb("./dump.rdb", 0)
	if err != nil {
		t.Fatalf("Failed to parse Redis DB: %s", err)
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
