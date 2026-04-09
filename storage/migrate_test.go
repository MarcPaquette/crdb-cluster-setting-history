package storage

import (
	"testing"
)

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		count int
		first string // expected first statement (trimmed)
	}{
		{
			name:  "single statement",
			sql:   "CREATE TABLE foo (id INT)",
			count: 1,
			first: "CREATE TABLE foo (id INT)",
		},
		{
			name:  "multiple statements",
			sql:   "CREATE TABLE a (id INT); CREATE TABLE b (id INT);",
			count: 2,
			first: "CREATE TABLE a (id INT)",
		},
		{
			name:  "empty input",
			sql:   "",
			count: 0,
		},
		{
			name:  "only whitespace and semicolons",
			sql:   "  ; ; ; ",
			count: 0,
		},
		{
			name:  "comment-only segments are skipped",
			sql:   "-- this is a comment;\nCREATE TABLE foo (id INT);",
			count: 1,
			first: "CREATE TABLE foo (id INT)",
		},
		{
			name: "multiline with leading whitespace",
			sql: `
				CREATE TABLE snapshots (
					id SERIAL PRIMARY KEY
				);

				CREATE TABLE settings (
					id SERIAL PRIMARY KEY
				);
			`,
			count: 2,
		},
		{
			name: "mixed comments and SQL",
			sql: `
				-- This is handled specially in code
			`,
			count: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts := splitStatements(tt.sql)
			if len(stmts) != tt.count {
				t.Errorf("splitStatements() returned %d statements, want %d", len(stmts), tt.count)
				for i, s := range stmts {
					t.Logf("  stmt[%d]: %q", i, s)
				}
			}
			if tt.count > 0 && tt.first != "" && len(stmts) > 0 && stmts[0] != tt.first {
				t.Errorf("first statement = %q, want %q", stmts[0], tt.first)
			}
		})
	}
}

func TestStmtPreview(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "simple statement",
			sql:  "CREATE TABLE foo (id INT)",
			want: "CREATE TABLE foo (id INT)",
		},
		{
			name: "multiline returns first meaningful line",
			sql: `
				CREATE TABLE foo (
					id INT PRIMARY KEY
				)`,
			want: "CREATE TABLE foo (",
		},
		{
			name: "skips leading comments",
			sql: `-- This is a comment
				CREATE TABLE bar (id INT)`,
			want: "CREATE TABLE bar (id INT)",
		},
		{
			name: "long line is truncated at 80 chars",
			sql:  "CREATE TABLE very_long_table_name_that_exceeds_eighty_characters_in_the_first_line_of_sql (id INT PRIMARY KEY)",
			want: "CREATE TABLE very_long_table_name_that_exceeds_eighty_characters_in_the_first_li...",
		},
		{
			name: "empty input",
			sql:  "",
			want: "(empty)",
		},
		{
			name: "only comments",
			sql:  "-- just a comment\n-- another comment",
			want: "(empty)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stmtPreview(tt.sql)
			if got != tt.want {
				t.Errorf("stmtPreview() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestOnlyComments(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{
			name: "only comments",
			sql:  "-- this is a comment\n-- another comment",
			want: true,
		},
		{
			name: "comments with whitespace",
			sql:  "  -- comment  \n  \n  -- more comments  ",
			want: true,
		},
		{
			name: "has SQL",
			sql:  "-- comment\nCREATE TABLE foo (id INT)",
			want: false,
		},
		{
			name: "only whitespace",
			sql:  "   \n   \n   ",
			want: true,
		},
		{
			name: "empty string",
			sql:  "",
			want: true,
		},
		{
			name: "SQL without comments",
			sql:  "CREATE TABLE foo (id INT)",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := onlyComments(tt.sql)
			if got != tt.want {
				t.Errorf("onlyComments() = %v, want %v", got, tt.want)
			}
		})
	}
}
