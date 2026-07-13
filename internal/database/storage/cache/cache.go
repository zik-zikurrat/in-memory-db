package cache

import "in-memory-key-value-db/internal/config"

type Cache struct {
	limit uint64
	used  uint64
}

func NewCache(cfg *config.Config) *Cache {
	return &Cache{
		limit: cfg.Cache.Limit,
	}
}
