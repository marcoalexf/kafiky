package main

import (
	"strings"
	"testing"

	"github.com/marcoalexf/golog/internal/log"
)

func TestCommandProcessor(t *testing.T) {
	logInstance := log.NewLog()

	tests := []struct {
		name        string
		command     Command
		wantOutput  string
		expectError bool
	}{
		{
			name:        "Append simple string",
			command:     Command{command: "APPEND", args: "hello world"},
			wantOutput:  "0", // offset should start at 0
			expectError: false,
		},
		{
			name:        "Read valid offset",
			command:     Command{command: "READ", args: "0"},
			wantOutput:  "hello world",
			expectError: false,
		},
		{
			name:        "Read invalid offset",
			command:     Command{command: "READ", args: "100"},
			wantOutput:  "",
			expectError: true,
		},
		{
			name:        "Invalid command",
			command:     Command{command: "INVALID", args: ""},
			wantOutput:  "",
			expectError: true,
		},
		{
			name:        "Append empty string",
			command:     Command{command: "APPEND", args: ""},
			wantOutput:  "1", // second append offset 1
			expectError: false,
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			output, err := CommandProcessor(tt.command, logInstance)
			if (err != nil) != tt.expectError {
				t.Errorf("expected error? %v, got error: %v", tt.expectError, err)
			}

			// Trim spaces to avoid false mismatch due to whitespace
			if strings.TrimSpace(output) != tt.wantOutput {
				t.Errorf("expected output %q, got %q", tt.wantOutput, output)
			}
		})
	}
}
