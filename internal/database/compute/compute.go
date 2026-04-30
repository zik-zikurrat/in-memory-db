package compute

import (
	"go.uber.org/zap"
)

type Storage interface {
	Set(key, value string) error
	Get(key string) (string, error)
	Del(key string) error
}

type Compute struct {
	storage Storage
	log     *zap.Logger
}

func NewCompute(storage Storage, log *zap.Logger) *Compute {
	return &Compute{
		storage: storage,
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
		if err := c.storage.Del(query.Arguments[0]); err != nil {
			c.log.Error("del failed", zap.Error(err))
			return "", err
		}
		return "OK", nil
	}

	return "", ErrUnknownCommand
}
