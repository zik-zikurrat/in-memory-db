package storage

import (
	"errors"

	"go.uber.org/zap"
)

var ErrKeyNotFound = errors.New("key not found")

type Engine interface {
	Set(key, value string)
	Get(key string) (string, bool)
	Del(key string) bool
}

type Storage struct {
	engine Engine
	log    *zap.Logger
}

func NewStorage(engine Engine, log *zap.Logger) *Storage {
	return &Storage{
		engine: engine,
		log:    log,
	}
}

func (s *Storage) Set(key, value string) error {
	s.engine.Set(key, value)
	s.log.Debug("set",
		zap.String("key", key),
		zap.String("value", value),
	)
	return nil
}

func (s *Storage) Get(key string) (string, error) {
	value, ok := s.engine.Get(key)
	if !ok {
		s.log.Debug("get miss", zap.String("key", key))
		return "", ErrKeyNotFound
	}
	s.log.Debug("get hit", zap.String("key", key))
	return value, nil
}

func (s *Storage) Del(key string) error {
	if !s.engine.Del(key) {
		s.log.Debug("del miss", zap.String("key", key))
		return ErrKeyNotFound
	}
	s.log.Debug("del ok", zap.String("key", key))
	return nil
}
