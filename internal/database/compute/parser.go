package compute

import (
	"errors"
	"strings"
)

const (
	DefaultExpiry  = "0"
	DefaultPattern = "*"
)

var (
	ErrEmptyQuery       = errors.New("empty query")
	ErrUnknownCommand   = errors.New("unknown command")
	ErrInvalidArguments = errors.New("invalid number of arguments")
)

func ParseQuery(input string) (Query, error) {
	fields := strings.Fields(input)
	if len(fields) == 0 {
		return Query{}, ErrEmptyQuery
	}

	cmd := strings.ToUpper(fields[0])
	a, ok := commandArity[cmd]
	if !ok {
		return Query{}, ErrUnknownCommand
	}

	args := fields[1:]
	if len(args) < a.min || len(args) > a.max {
		return Query{}, ErrInvalidArguments
	}

	switch cmd {
	case SetCommand:
		if len(args) == 2 {
			args = append(args, DefaultExpiry)
		}
	case KeysCommand:
		if len(args) == 0 {
			args = append(args, DefaultPattern)
		}
	}

	return Query{Command: cmd, Arguments: args}, nil
}
