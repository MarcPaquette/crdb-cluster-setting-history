package main

import (
	"os"
	"testing"
	"time"
)

func TestGetEnv(t *testing.T) {
	// Test with existing env var
	os.Setenv("TEST_GET_ENV", "test_value")
	defer os.Unsetenv("TEST_GET_ENV")

	result := getEnv("TEST_GET_ENV", "default")
	if result != "test_value" {
		t.Errorf("Expected 'test_value', got '%s'", result)
	}

	// Test with non-existing env var
	result = getEnv("NON_EXISTING_VAR_12345", "default")
	if result != "default" {
		t.Errorf("Expected 'default', got '%s'", result)
	}

	// Test with empty env var
	os.Setenv("TEST_EMPTY_ENV", "")
	defer os.Unsetenv("TEST_EMPTY_ENV")

	result = getEnv("TEST_EMPTY_ENV", "default")
	if result != "default" {
		t.Errorf("Expected 'default' for empty env, got '%s'", result)
	}
}

func TestGetEnvDuration(t *testing.T) {
	defaultDuration := 15 * time.Minute

	// Test with valid duration
	os.Setenv("TEST_DURATION", "30m")
	defer os.Unsetenv("TEST_DURATION")

	result := getEnvDuration("TEST_DURATION", defaultDuration)
	if result != 30*time.Minute {
		t.Errorf("Expected 30m, got %v", result)
	}

	// Test with non-existing env var
	result = getEnvDuration("NON_EXISTING_DURATION_12345", defaultDuration)
	if result != defaultDuration {
		t.Errorf("Expected default %v, got %v", defaultDuration, result)
	}

	// Test with invalid duration
	os.Setenv("TEST_INVALID_DURATION", "invalid")
	defer os.Unsetenv("TEST_INVALID_DURATION")

	result = getEnvDuration("TEST_INVALID_DURATION", defaultDuration)
	if result != defaultDuration {
		t.Errorf("Expected default %v for invalid duration, got %v", defaultDuration, result)
	}

	// Test various duration formats
	testCases := []struct {
		input    string
		expected time.Duration
	}{
		{"1s", time.Second},
		{"1m", time.Minute},
		{"1h", time.Hour},
		{"24h", 24 * time.Hour},
		{"720h", 720 * time.Hour}, // 30 days
	}

	for _, tc := range testCases {
		os.Setenv("TEST_DURATION", tc.input)
		result := getEnvDuration("TEST_DURATION", defaultDuration)
		if result != tc.expected {
			t.Errorf("For input '%s', expected %v, got %v", tc.input, tc.expected, result)
		}
	}
}
