// Package testdbsuffix provides a deterministic, SQL-safe suffix for test
// database and user names, enabling concurrent test runs across worktrees.
package testdbsuffix

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"sync"
	"unicode"
)

var (
	cached     string
	cachedOnce sync.Once

	// detectWorktree is overridable in tests.
	detectWorktree = func() string {
		dir, _ := os.Getwd()
		return worktreeNameFromPath(dir)
	}
)

// resetCached allows tests to recompute the suffix.
func resetCached() {
	cachedOnce = sync.Once{}
	cached = ""
}

// Suffix returns a short, SQL-identifier-safe suffix for test database names.
//
// Resolution order:
//  1. TEST_DB_SUFFIX env var (sanitized, max 20 chars, prefixed with _)
//  2. Auto-detect from .claude/worktrees/<name> in cwd (hashed to _<8 hex>)
//  3. Empty string (backward compatible)
func Suffix() string {
	cachedOnce.Do(func() {
		if env := os.Getenv("TEST_DB_SUFFIX"); env != "" {
			cached = "_" + sanitize(env)
			return
		}
		if name := detectWorktree(); name != "" {
			h := sha256.Sum256([]byte(name))
			cached = fmt.Sprintf("_%x", h[:4])
		}
	})
	return cached
}

// sanitize lowercases and replaces non-alphanumeric/underscore chars,
// truncating to 20 characters.
func sanitize(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	result := b.String()
	if len(result) > 20 {
		result = result[:20]
	}
	return result
}

// worktreeNameFromPath extracts the worktree name from a path containing
// .claude/worktrees/<name>.
func worktreeNameFromPath(path string) string {
	const marker = ".claude/worktrees/"
	idx := strings.Index(path, marker)
	if idx < 0 {
		return ""
	}
	rest := path[idx+len(marker):]
	rest = strings.TrimRight(rest, "/")
	if name, _, ok := strings.Cut(rest, "/"); ok {
		rest = name
	}
	if rest == "" {
		return ""
	}
	return rest
}
