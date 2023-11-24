package parse

import (
	"testing"
)

const testExpMs = int64(1700854504)

func TestParseExpMs(t *testing.T) {
	result, err := ParseRedisDb("./dump.rdb", 0, 1)
	if err != nil {
		t.Fatalf("Failed to parse Redis DB: %s", err)
	}

	withExp := result["temp"]
	if withExp.Exp != testExpMs {
		t.Errorf("Expected exp %d, got %d", testExpMs, withExp.Exp)
	}
}

func TestParseRedisDbNotExpired(t *testing.T) {
	expected := map[string]string{"fruit": "apple", "foo": "bar", "temp": "tv"}
	unixNowNano := int64(testExpMs - 1)
	result, err := ParseRedisDb("./dump.rdb", 0, unixNowNano)
	if err != nil {
		t.Fatalf("Failed to parse Redis DB: %s", err)
	}

	if len(result) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(result))
	}

	for key, val := range expected {
		gotVal, ok := result[key]
		if gotVal.Payload != val || !ok {
			t.Errorf("Expected key '%s' with value '%s', got '%s'", key, val, result[key].Payload)
		}
	}
}

func TestParseRedisDbExpired(t *testing.T) {
	expected := map[string]string{"fruit": "apple", "foo": "bar"}
	unixNowNano := int64(testExpMs + 1)
	result, err := ParseRedisDb("./dump.rdb", 0, unixNowNano)
	if err != nil {
		t.Fatalf("Failed to parse Redis DB: %s", err)
	}

	if len(result) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(result))
	}

	for key, val := range expected {
		gotVal, ok := result[key]
		if gotVal.Payload != val || !ok {
			t.Errorf("Expected key '%s' with value '%s', got '%s'", key, val, result[key].Payload)
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
