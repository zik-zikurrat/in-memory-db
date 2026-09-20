package compute

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseQuery(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Query
		wantErr error
	}{
		{
			name:  "keys without pattern defaults to *",
			input: "KEYS",
			want:  Query{Command: KeysCommand, Arguments: []string{DefaultPattern}},
		},
		{
			name:  "keys with pattern",
			input: "KEYS user:*",
			want:  Query{Command: KeysCommand, Arguments: []string{"user:*"}},
		},
		{
			name:    "keys with extra argument",
			input:   "KEYS a b",
			wantErr: ErrInvalidArguments,
		},
		{
			name:  "command is case insensitive",
			input: "keys *",
			want:  Query{Command: KeysCommand, Arguments: []string{"*"}},
		},
		{
			name:  "set without ttl gets default",
			input: "SET key value",
			want:  Query{Command: SetCommand, Arguments: []string{"key", "value", DefaultExpiry}},
		},
		{
			name:  "set with ttl",
			input: "SET key value 10",
			want:  Query{Command: SetCommand, Arguments: []string{"key", "value", "10"}},
		},
		{
			name:    "set with too many arguments",
			input:   "SET key value 10 extra",
			wantErr: ErrInvalidArguments,
		},
		{
			name:    "set without value",
			input:   "SET key",
			wantErr: ErrInvalidArguments,
		},
		{
			name:  "get",
			input: "GET key",
			want:  Query{Command: GetCommand, Arguments: []string{"key"}},
		},
		{
			name:  "del",
			input: "DEL key",
			want:  Query{Command: DelCommand, Arguments: []string{"key"}},
		},
		{
			name:  "scan",
			input: "SCAN 0",
			want:  Query{Command: ScanCommand, Arguments: []string{"0"}},
		},
		{
			name:    "empty query",
			input:   "   ",
			wantErr: ErrEmptyQuery,
		},
		{
			name:    "unknown command",
			input:   "PING",
			wantErr: ErrUnknownCommand,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseQuery(tt.input)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ParseQuery(%q) error = %v, want %v", tt.input, err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ParseQuery(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}
