package parse

import (
	"testing"
)

func TestParseRedisDB(t *testing.T) {
	expected := map[string]string{"foo": "1", "bar": "2"}
	result, err := ParseRedisDb("path/to/sample.rdb")
	if err != nil {
		t.Fatalf("Failed to parse Redis DB: %s", err)
	}

	for key, val := range expected {
		if result[key] != val {
			t.Errorf("Expected key '%s' with value '%s', got '%s'", key, val, result[key])
		}
	}
}
