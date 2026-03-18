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

func TestGetEnvBool(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		set      bool
		def      bool
		expected bool
	}{
		{"true", "true", true, false, true},
		{"false", "false", true, true, false},
		{"1", "1", true, false, true},
		{"0", "0", true, true, false},
		{"TRUE", "TRUE", true, false, true},
		{"unset", "", false, true, true},
		{"invalid", "notabool", true, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "TEST_BOOL_" + tt.name
			if tt.set {
				os.Setenv(key, tt.value)
				defer os.Unsetenv(key)
			}
			if got := getEnvBool(key, tt.def); got != tt.expected {
				t.Errorf("getEnvBool(%q, %v) = %v, want %v", key, tt.def, got, tt.expected)
			}
		})
	}
}

func TestGetEnvFloat(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		set      bool
		def      float64
		expected float64
	}{
		{"valid", "10.5", true, 0, 10.5},
		{"integer", "42", true, 0, 42},
		{"unset", "", false, 3.14, 3.14},
		{"invalid", "notafloat", true, 1.0, 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "TEST_FLOAT_" + tt.name
			if tt.set {
				os.Setenv(key, tt.value)
				defer os.Unsetenv(key)
			}
			if got := getEnvFloat(key, tt.def); got != tt.expected {
				t.Errorf("getEnvFloat(%q, %v) = %v, want %v", key, tt.def, got, tt.expected)
			}
		})
	}
}

func TestGetEnvInt(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		set      bool
		def      int
		expected int
	}{
		{"valid", "42", true, 0, 42},
		{"negative", "-5", true, 0, -5},
		{"unset", "", false, 20, 20},
		{"invalid", "notanint", true, 10, 10},
		{"float", "3.14", true, 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "TEST_INT_" + tt.name
			if tt.set {
				os.Setenv(key, tt.value)
				defer os.Unsetenv(key)
			}
			if got := getEnvInt(key, tt.def); got != tt.expected {
				t.Errorf("getEnvInt(%q, %v) = %v, want %v", key, tt.def, got, tt.expected)
			}
		})
	}
}
