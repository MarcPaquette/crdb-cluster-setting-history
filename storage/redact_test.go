package storage

import (
	"regexp"
	"testing"
)

func TestRedactor_Disabled(t *testing.T) {
	t.Parallel()
	r := NewRedactor(RedactorConfig{Enabled: false})

	if r.ShouldRedact("server.password") {
		t.Error("disabled redactor should not redact anything")
	}

	value := r.RedactValue("server.password", "secret123")
	if value != "secret123" {
		t.Errorf("expected original value, got %q", value)
	}
}

func TestRedactor_DefaultPatterns(t *testing.T) {
	t.Parallel()
	r := NewRedactor(RedactorConfig{Enabled: true})

	tests := []struct {
		variable string
		redact   bool
	}{
		{"server.password", true},
		{"cluster.secret_key", true},
		{"api.token", true},
		{"auth.credential", true},
		{"enterprise.license", true},
		{"db.encryption_key", true},
		{"user.auth_api_key", true},
		{"tls.private_key", true},
		// Should NOT be redacted
		{"server.host", false},
		{"cluster.name", false},
		{"log.level", false},
		{"sql.defaults.default_int_size", false},
	}

	for _, tt := range tests {
		result := r.ShouldRedact(tt.variable)
		if result != tt.redact {
			t.Errorf("ShouldRedact(%q) = %v, expected %v", tt.variable, result, tt.redact)
		}
	}
}

func TestRedactor_CaseInsensitive(t *testing.T) {
	t.Parallel()
	r := NewRedactor(RedactorConfig{Enabled: true})

	tests := []string{
		"SERVER.PASSWORD",
		"Server.Password",
		"ENTERPRISE.LICENSE",
	}

	for _, variable := range tests {
		if !r.ShouldRedact(variable) {
			t.Errorf("expected %q to be redacted (case insensitive)", variable)
		}
	}
}

func TestRedactor_AdditionalPatterns(t *testing.T) {
	t.Parallel()
	r := NewRedactor(RedactorConfig{
		Enabled:            true,
		AdditionalPatterns: "custom.sensitive, my.secret.setting",
	})

	tests := []struct {
		variable string
		redact   bool
	}{
		{"custom.sensitive", true},
		{"my.secret.setting", true},
		{"server.password", true}, // default pattern still works
		{"custom.normal", false},
	}

	for _, tt := range tests {
		result := r.ShouldRedact(tt.variable)
		if result != tt.redact {
			t.Errorf("ShouldRedact(%q) = %v, expected %v", tt.variable, result, tt.redact)
		}
	}
}

func TestRedactor_RedactValue(t *testing.T) {
	t.Parallel()
	r := NewRedactor(RedactorConfig{Enabled: true})

	value := r.RedactValue("server.password", "secret123")
	if value != RedactedPlaceholder {
		t.Errorf("expected [REDACTED], got %q", value)
	}

	value = r.RedactValue("server.host", "localhost")
	if value != "localhost" {
		t.Errorf("expected original value, got %q", value)
	}
}

func TestRedactor_RedactChange(t *testing.T) {
	t.Parallel()
	r := NewRedactor(RedactorConfig{Enabled: true})

	c := Change{
		Variable:    "server.password",
		OldValue:    "old_secret",
		NewValue:    "new_secret",
		Description: "Password setting",
	}

	redacted := r.RedactChange(c)
	if redacted.OldValue != RedactedPlaceholder || redacted.NewValue != RedactedPlaceholder {
		t.Errorf("expected redacted values, got old=%q new=%q", redacted.OldValue, redacted.NewValue)
	}
	if redacted.Variable != "server.password" {
		t.Error("variable name should not be redacted")
	}
	if redacted.Description != "Password setting" {
		t.Error("description should not be redacted")
	}

	c2 := Change{
		Variable:    "server.host",
		OldValue:    "old.host.com",
		NewValue:    "new.host.com",
	}

	redacted2 := r.RedactChange(c2)
	if redacted2.OldValue != "old.host.com" || redacted2.NewValue != "new.host.com" {
		t.Errorf("non-sensitive values should not be redacted, got old=%q new=%q", redacted2.OldValue, redacted2.NewValue)
	}
}

func TestRedactor_RedactChanges(t *testing.T) {
	t.Parallel()
	r := NewRedactor(RedactorConfig{Enabled: true})

	changes := []Change{
		{Variable: "server.password", OldValue: "secret1", NewValue: "secret2"},
		{Variable: "server.host", OldValue: "host1", NewValue: "host2"},
		{Variable: "api.token", OldValue: "token1", NewValue: "token2"},
	}

	redacted := r.RedactChanges(changes)

	if len(redacted) != 3 {
		t.Fatalf("expected 3 changes, got %d", len(redacted))
	}

	if redacted[0].OldValue != RedactedPlaceholder {
		t.Errorf("expected redacted, got %q", redacted[0].OldValue)
	}

	if redacted[1].OldValue != "host1" {
		t.Errorf("expected original value, got %q", redacted[1].OldValue)
	}

	if redacted[2].OldValue != RedactedPlaceholder {
		t.Errorf("expected redacted, got %q", redacted[2].OldValue)
	}

	if changes[0].OldValue != "secret1" {
		t.Error("original changes should not be modified")
	}
}

func TestGlobToRegex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		glob     string
		input    string
		expected bool
	}{
		{"*.password*", "server.password", true},
		{"*.password*", "server.password.hash", true},
		{"enterprise.license", "enterprise.license", true},
		{"*.key*", "encryption.key.file", true},
		{"test.setting", "test.setting", true},
		{"test.setting", "other.setting", false},
	}

	for _, tt := range tests {
		regex := globToRegex(tt.glob)
		re, err := regexp.Compile("(?i)^" + regex + "$")
		if err != nil {
			t.Fatalf("failed to compile regex for %q: %v", tt.glob, err)
		}
		result := re.MatchString(tt.input)
		if result != tt.expected {
			t.Errorf("pattern %q matching %q = %v, expected %v (regex: %s)", tt.glob, tt.input, result, tt.expected, regex)
		}
	}
}
