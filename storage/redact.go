package storage

import (
	"regexp"
	"strings"
)

// RedactedPlaceholder is the replacement value for redacted settings.
const RedactedPlaceholder = "[REDACTED]"

// defaultSensitivePatterns defines settings that may contain sensitive data.
var defaultSensitivePatterns = []string{
	"*.password*",
	"*.secret*",
	"*.key*",
	"*.token*",
	"*.credential*",
	"enterprise.license",
	"*encryption*",
	"*auth*key*",
	"*private*",
}

// Redactor filters sensitive setting values.
type Redactor struct {
	patterns []*regexp.Regexp
	enabled  bool
}

// RedactorConfig holds redaction configuration.
type RedactorConfig struct {
	// Enabled controls whether redaction is active.
	Enabled bool
	// AdditionalPatterns are extra patterns to redact (comma-separated).
	AdditionalPatterns string
}

// NewRedactor creates a new redactor with the given configuration.
func NewRedactor(cfg RedactorConfig) *Redactor {
	if !cfg.Enabled {
		return &Redactor{enabled: false}
	}

	// Combine default and additional patterns
	patterns := make([]string, len(defaultSensitivePatterns))
	copy(patterns, defaultSensitivePatterns)

	if cfg.AdditionalPatterns != "" {
		for _, p := range strings.Split(cfg.AdditionalPatterns, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				patterns = append(patterns, p)
			}
		}
	}

	// Compile glob patterns to regex
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		regex := globToRegex(p)
		if re, err := regexp.Compile("(?i)^" + regex + "$"); err == nil {
			compiled = append(compiled, re)
		}
	}

	return &Redactor{
		patterns: compiled,
		enabled:  true,
	}
}

// globToRegex converts a glob pattern to a regex pattern.
func globToRegex(glob string) string {
	// Escape regex special characters except * and ?
	var result strings.Builder
	for _, c := range glob {
		switch c {
		case '*':
			result.WriteString(".*")
		case '?':
			result.WriteString(".")
		case '.', '+', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\':
			result.WriteRune('\\')
			result.WriteRune(c)
		default:
			result.WriteRune(c)
		}
	}
	return result.String()
}

// ShouldRedact returns true if the variable name matches a sensitive pattern.
func (r *Redactor) ShouldRedact(variable string) bool {
	if !r.enabled {
		return false
	}

	for _, pattern := range r.patterns {
		if pattern.MatchString(variable) {
			return true
		}
	}
	return false
}

// RedactValue returns RedactedPlaceholder if the variable is sensitive, otherwise the original value.
func (r *Redactor) RedactValue(variable, value string) string {
	if r.ShouldRedact(variable) {
		return RedactedPlaceholder
	}
	return value
}

// RedactChange returns a copy of the change with sensitive values redacted.
func (r *Redactor) RedactChange(c Change) Change {
	result := c
	result.OldValue = r.RedactValue(c.Variable, c.OldValue)
	result.NewValue = r.RedactValue(c.Variable, c.NewValue)
	return result
}

// RedactChanges returns a copy of the changes with sensitive values redacted.
func (r *Redactor) RedactChanges(changes []Change) []Change {
	if !r.enabled {
		return changes
	}

	result := make([]Change, len(changes))
	for i, c := range changes {
		result[i] = r.RedactChange(c)
	}
	return result
}
