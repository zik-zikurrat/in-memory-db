package cache

type Elem struct {
	key   string
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

func NewElem(key, value string) *Elem {
	return &Elem{
		key:   key,
		value: value,
	}
}

func NewLRU(cache *Cache) *LRU {
	return &LRU{
		cache: cache,
		kv:    make(map[string]*Elem),
	}
}

func (lru *LRU) Put(key, value string) {
	elem, ok := lru.kv[key]
	if ok {
		elem.value = value
		lru.promote(elem)
		return
	}
	elem = NewElem(key, value)
	lru.kv[key] = elem
	lru.append(elem)
}

func (lru *LRU) Get(key string) (string, bool) {
	elem, ok := lru.kv[key]
	if !ok {
		return "", false
	}

	lru.promote(elem)

	return elem.value, true
}

func (lru *LRU) promote(elem *Elem) {
	if elem == lru.tail {
		return
	}

	lru.detach(elem)
	lru.append(elem)
}

func (lru *LRU) append(elem *Elem) {
	elem.next = nil
	elem.prev = lru.tail

	if lru.tail != nil {
		lru.tail.next = elem
	} else {
		lru.head = elem
	}

	lru.tail = elem
}

func (lru *LRU) detach(elem *Elem) {
	if elem.prev != nil {
		elem.prev.next = elem.next
	} else {
		lru.head = elem.next
	}

	if elem.next != nil {
		elem.next.prev = elem.prev
	} else {
		lru.tail = elem.prev
	}
	elem.prev = nil
	elem.next = nil
}
