package compute

import (
	"fmt"
	"in-memory-key-value-db/internal/database/storage/expirity"
	"in-memory-key-value-db/internal/database/storage/wal"
	"strconv"
	"time"

	"go.uber.org/zap"
)

type Storage interface {
	Set(key, value string) error
	Get(key string) (string, error)
	Del(key string) error
}

type Compute struct {
	storage        Storage
	walEvents      chan wal.WALEvent
	expirityEvents chan expirity.ExpirityEvent
	log            *zap.Logger
}

func NewCompute(storage Storage, log *zap.Logger, walEvents chan wal.WALEvent, expirityEvents chan expirity.ExpirityEvent) *Compute {
	return &Compute{
		storage:        storage,
		walEvents:      walEvents,
		expirityEvents: expirityEvents,
		log:            log,
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
		n, err := strconv.Atoi(query.Arguments[2])
		if err != nil {
			return "", fmt.Errorf("Error converting ttl to int")
		}

		d := time.Duration(n) * time.Second

		c.expirityEvents <- expirity.ExpirityEvent{
			Key:  query.Arguments[0],
			Time: d,
		}

		done := make(chan error, 1)
		c.walEvents <- wal.WALEvent{
			Command:   query.Command,
			Arguments: query.Arguments,
			Done:      done,
		}
		if err := <-done; err != nil {
			c.log.Error("wal write failed", zap.Error(err))
			return "", err
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
		c.walEvents <- wal.WALEvent{
			Command:   query.Command,
			Arguments: query.Arguments,
			Done:      done,
		}
		if err := <-done; err != nil {
			c.log.Error("wal write failed", zap.Error(err))
			return "", err
		}
		if err := c.storage.Del(query.Arguments[0]); err != nil {
			c.log.Error("del failed", zap.Error(err))
			return "", err
		}

		return "OK", nil
	}

	return "", ErrUnknownCommand
}
