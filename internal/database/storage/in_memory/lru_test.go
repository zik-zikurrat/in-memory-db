package inmemory

import (
	"context"
	"strconv"
	"testing"
)

const (
	_defaultLoadLRU = 100
)

func TestSetLRU(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := helperNewPartitionEngine(ctx)

	for i := 0; i < _defaultLoadLRU; i++ {
		e.Set("key"+strconv.Itoa(i), "value")
	}

	e.Set("key0", "value2")
	p := e.data.bucket("key0")

	if p.tail.key != "key0" {
		t.Fatalf("expected key: %s, got: %s", "key0", p.tail.key)
	}
}

func TestGetLRU(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := helperNewPartitionEngine(ctx)

	for i := 0; i < _defaultLoadLRU; i++ {
		e.Set("key"+strconv.Itoa(i), "value")
	}

	_, ok := e.Get("key0")
	if !ok {
		t.Fatalf("should be found")
	}

	p := e.data.bucket("key0")

	if p.tail.key != "key0" {
		t.Fatalf("expected key: %s, got: %s", "key0", p.tail.key)
	}
}
