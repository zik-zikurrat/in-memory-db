package cache

import (
	"fmt"
	"testing"
)

func TestPut(t *testing.T) {
	cache := &Cache{limit: 256}

	lru := NewLRU(cache)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%d_zak", i)
		value := fmt.Sprintf("%d_zlata", i)

		lru.Put(key, value)
	}

	if lru.head.key != "0_zak" {
		t.Fatalf(
			"expected head to be 0_zak, got %s",
			lru.head.key,
		)
	}

	if lru.tail.key != "99_zak" {
		t.Fatalf(
			"expected tail to be 99_zak, got %s",
			lru.tail.key,
		)
	}
}

func TestPutWithSameKey(t *testing.T) {
	cache := &Cache{limit: 256}

	lru := NewLRU(cache)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("%d_zak", i)
		value := fmt.Sprintf("%d_zlata", i)

		lru.Put(key, value)
	}
	lru.Put("0_zak", "0_zlata")
	if lru.head.key != "1_zak" {
		t.Fatalf(
			"expected head to be 1_zak, got %s",
			lru.head.key,
		)
	}

	if lru.tail.key != "0_zak" {
		t.Fatalf(
			"expected tail to be 0_zak, got %s",
			lru.tail.key,
		)
	}
}

func TestGet(t *testing.T) {
	cache := &Cache{limit: 256}

	lru := NewLRU(cache)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("%d_zak", i)
		value := fmt.Sprintf("%d_zlata", i)

		lru.Put(key, value)
	}

	lru.Get("0_zak")

	if lru.head.key != "1_zak" {
		t.Fatalf(
			"expected head to be 1_zak, got %s",
			lru.head.key,
		)
	}

	if lru.tail.key != "0_zak" {
		t.Fatalf(
			"expected tail to be 0_zak, got %s",
			lru.tail.key,
		)
	}
}
