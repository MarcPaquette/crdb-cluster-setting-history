package main

import (
	"os"
	"testing"

	"crdb-cluster-history/config"
)

func TestListenAddress(t *testing.T) {
	tests := []struct {
		name       string
		tlsEnabled bool
		port       string
		expected   string
	}{
		{"http", false, "8080", "http://localhost:8080"},
		{"https", true, "8443", "https://localhost:8443"},
		{"custom port", false, "3000", "http://localhost:3000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := listenAddress(tt.tlsEnabled, tt.port)
			if got != tt.expected {
				t.Errorf("listenAddress(%v, %q) = %q, want %q", tt.tlsEnabled, tt.port, got, tt.expected)
			}
		})
	}
}

func TestGetEnvDefault(t *testing.T) {
	os.Setenv("TEST_GET_ENV", "test_value")
	defer os.Unsetenv("TEST_GET_ENV")

	result := config.GetEnvDefault("TEST_GET_ENV", "default")
	if result != "test_value" {
		t.Errorf("Expected 'test_value', got '%s'", result)
	}

	result = config.GetEnvDefault("NON_EXISTING_VAR_12345", "default")
	if result != "default" {
		t.Errorf("Expected 'default', got '%s'", result)
	}

	os.Setenv("TEST_EMPTY_ENV", "")
	defer os.Unsetenv("TEST_EMPTY_ENV")

	result = config.GetEnvDefault("TEST_EMPTY_ENV", "default")
	if result != "default" {
		t.Errorf("Expected 'default' for empty env, got '%s'", result)
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
