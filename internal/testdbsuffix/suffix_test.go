package testdbsuffix

import (
	"os"
	"testing"
)

func TestSuffix_EnvVarTakesPriority(t *testing.T) {
	t.Setenv("TEST_DB_SUFFIX", "my-ci-job")
	resetCached()

	got := Suffix()
	if got != "_my_ci_job" {
		t.Errorf("expected _my_ci_job, got %q", got)
	}
}

func TestSuffix_EnvVarSanitization(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "_simple"},
		{"WITH-UPPER", "_with_upper"},
		{"has spaces!", "_has_spaces_"},
		{"a-b_c.d", "_a_b_c_d"},
		{"abcdefghijklmnopqrstuvwxyz", "_abcdefghijklmnopqrst"}, // truncated to 20
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Setenv("TEST_DB_SUFFIX", tt.input)
			resetCached()

			got := Suffix()
			if got != tt.want {
				t.Errorf("Suffix() with TEST_DB_SUFFIX=%q: got %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSuffix_WorktreeDetection(t *testing.T) {
	t.Setenv("TEST_DB_SUFFIX", "")
	resetCached()

	// Override the working directory detector for testing
	origDetect := detectWorktree
	defer func() { detectWorktree = origDetect }()
	detectWorktree = func() string { return "fluttering-questing-shore" }

	got := Suffix()
	if got == "" {
		t.Error("expected non-empty suffix from worktree detection")
	}
	if got[0] != '_' {
		t.Errorf("suffix should start with _, got %q", got)
	}
	if len(got) != 9 { // _ + 8 hex chars
		t.Errorf("expected 9 chars (_ + 8 hex), got %d: %q", len(got), got)
	}
}

func TestSuffix_WorktreeDetectionDeterministic(t *testing.T) {
	t.Setenv("TEST_DB_SUFFIX", "")

	origDetect := detectWorktree
	defer func() { detectWorktree = origDetect }()
	detectWorktree = func() string { return "fluttering-questing-shore" }

	resetCached()
	first := Suffix()

	resetCached()
	second := Suffix()

	if first != second {
		t.Errorf("suffix not deterministic: %q != %q", first, second)
	}
}

func TestSuffix_NoWorktreeReturnsEmpty(t *testing.T) {
	t.Setenv("TEST_DB_SUFFIX", "")

	origDetect := detectWorktree
	defer func() { detectWorktree = origDetect }()
	detectWorktree = func() string { return "" }

	resetCached()
	got := Suffix()
	if got != "" {
		t.Errorf("expected empty suffix when no worktree, got %q", got)
	}
}

func TestSuffix_EmptyEnvVarFallsToWorktree(t *testing.T) {
	// Explicitly set to empty — should fall through to worktree detection
	os.Setenv("TEST_DB_SUFFIX", "")
	resetCached()

	origDetect := detectWorktree
	defer func() { detectWorktree = origDetect }()
	detectWorktree = func() string { return "some-worktree" }

	got := Suffix()
	if got == "" {
		t.Error("expected non-empty suffix from worktree fallback when env var is empty")
	}
}

func TestDetectWorktreeFromPath(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/Users/user/.claude/worktrees/fluttering-questing-shore/storage", "fluttering-questing-shore"},
		{"/Users/user/.claude/worktrees/my-branch", "my-branch"},
		{"/Users/user/.claude/worktrees/my-branch/", "my-branch"},
		{"/Users/user/workspace/cluster-history", ""},
		{"/tmp/test", ""},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := worktreeNameFromPath(tt.path)
			if got != tt.want {
				t.Errorf("worktreeNameFromPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}
