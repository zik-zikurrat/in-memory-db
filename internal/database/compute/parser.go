package compute

import (
	"errors"
	"strings"
)

const (
	DefaultExpiry = "0"
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
			if len(fields) == 3 {
				args = append(args, DefaultExpiry)
			}
			return Query{Command: cmd, Arguments: args}, nil
		}
		if cmd == "KEYS" {
			args = append(args)
		}
		return Query{}, ErrInvalidArguments
	}

	return Query{Command: cmd, Arguments: args}, nil
}
