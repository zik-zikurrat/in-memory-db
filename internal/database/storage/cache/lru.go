package cache

type Elem struct {
	value string
	next  *Elem
	prev  *Elem
}

type LRU struct {
	head  *Elem
	tail  *Elem
	cache *Cache
	kv    map[string]*Elem
}

func NewElem(value string) *Elem {
	return &Elem{
		value: value,
	}
}

func NewLRU(cache *Cache) *LRU {
	lru := &LRU{
		kv:   map[string]*Elem{},
		head: &Elem{},
		tail: &Elem{},
	}
	lru.head.next = lru.tail
	lru.tail.prev = lru.head
	return lru
}

func (lru *LRU) Put(key, value string) {
	elem, ok := lru.kv[key]
	if !ok {
		elem = NewElem(value)
	} else {
		lru.kv[key].value = value
	}
	lru.promote(elem)
}

func (lru *LRU) Get(key string) string {
	elem, ok := lru.kv[key]
	if !ok {
		return ""
	}
	lru.promote(elem)
	return elem.value
}

func (lru *LRU) promote(elem *Elem) {
	if elem.prev != nil {
		lru.detach(elem)
	}
	elem.next = lru.head.next
	elem.prev = lru.head
	elem.next.prev = elem
	elem.prev.next = elem
}

func (lru *LRU) detach(elem *Elem) {
	prev := elem.prev
	next := elem.next
	prev.next = next
	next.prev = prev
}
