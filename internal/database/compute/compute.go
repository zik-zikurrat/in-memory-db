package compute

import (
	"in-memory-key-value-db/internal/database/storage/wal"

	"go.uber.org/zap"
)

type Storage interface {
	Set(key, value string) error
	Get(key string) (string, error)
	Del(key string) error
}

type Compute struct {
	storage Storage
	events  chan wal.WALEvent
	log     *zap.Logger
}

func NewCompute(storage Storage, log *zap.Logger, events chan wal.WALEvent) *Compute {
	return &Compute{
		storage: storage,
		events:  events,
		log:     log,
	}
}

func (c *Compute) Handle(input string) (string, error) {
	query, err := ParseQuery(input)
	if err != nil {
		c.log.Debug("failed to parse query",
			zap.String("input", input),
			zap.Error(err),
		)
		return "", err
	}

	c.log.Info("handling query",
		zap.String("command", query.Command),
		zap.Strings("arguments", query.Arguments),
	)

	switch query.Command {
	case SetCommand:
		done := make(chan error, 1)
		c.events <- wal.WALEvent{
			Command:   query.Command,
			Arguments: query.Arguments,
		}
		if err := <-done; err != nil {
			c.log.Error("wal write failed", zap.Error(err))
		}
		if err := c.storage.Set(query.Arguments[0], query.Arguments[1]); err != nil {
			c.log.Error("set failed", zap.Error(err))
			return "", err
		}

		return "OK", nil

	case GetCommand:
		value, err := c.storage.Get(query.Arguments[0])
		if err != nil {
			c.log.Error("get failed", zap.Error(err))
			return "", err
		}
		return value, nil

	case DelCommand:
		done := make(chan error, 1)
		c.events <- wal.WALEvent{
			Command:   query.Command,
			Arguments: query.Arguments,
		}
		if err := <-done; err != nil {
			c.log.Error("wal write failed", zap.Error(err))
		}
		if err := c.storage.Del(query.Arguments[0]); err != nil {
			c.log.Error("del failed", zap.Error(err))
			return "", err
		}

		return "OK", nil
	}

	return "", ErrUnknownCommand
}
