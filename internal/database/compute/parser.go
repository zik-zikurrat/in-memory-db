package compute

import (
	"errors"
	"strings"
)

const (
	DefaultExpirity = "30"
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

	cmd := fields[0]
	arity, ok := commandArity[cmd]
	if !ok {
		return Query{}, ErrUnknownCommand
	}

	args := fields[1:]
	if len(args) != arity {
		if cmd == "SET" {
			args = append(args, DefaultExpirity)
			return Query{Command: cmd, Arguments: args}, nil
		}
		return Query{}, ErrInvalidArguments
	}

	return Query{Command: cmd, Arguments: args}, nil
}
