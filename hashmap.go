package wavelet

import (
	"github.com/perlin-network/wavelet/lru"
	"github.com/perlin-network/wavelet/store"
	"golang.org/x/crypto/blake2b"
)

type chunkHashMap struct {
	prefix []byte
	kv     store.KV
	cache  *lru.LRU
	keys   map[[blake2b.Size256]byte]struct{}
}

func newChunkHashMap(kv store.KV, prefix []byte) *chunkHashMap {
	return &chunkHashMap{
		prefix: prefix,
		kv:     kv,
		cache:  lru.NewLRU(512), // 512 * 64kb = 32MB
		keys:   make(map[[blake2b.Size256]byte]struct{}),
	}
}

func (h *chunkHashMap) Get(checksum [blake2b.Size256]byte) ([]byte, error) {
	value, ok := h.cache.Load(checksum)
	if ok {
		return value.([]byte), nil
	}

	if _, ok := h.keys[checksum]; !ok {
		return nil, nil
	}

	// Only touch KV if key is present but not in the cache
	chunk, err := h.kv.Get(append(h.prefix, checksum[:]...))
	if err != nil {
		return nil, err
	}

	h.cache.PutWithEvictCallback(checksum, chunk, func(key, value interface{}) {
		evictedKey := key.([blake2b.Size256]byte)
		evictedValue := value.([]byte)

		err = h.kv.Put(append(h.prefix, evictedKey[:]...), evictedValue)
	})
	if err != nil {
		return nil, err
	}

	return chunk, nil
}

func (h *chunkHashMap) Put(checksum [blake2b.Size256]byte, chunk []byte) error {
	var err error
	h.cache.PutWithEvictCallback(checksum, chunk, func(key, value interface{}) {
		evictedKey := key.([blake2b.Size256]byte)
		evictedValue := value.([]byte)

		err = h.kv.Put(append(h.prefix, evictedKey[:]...), evictedValue)
	})

	if err != nil {
		return err
	}

	// Keep track of all keys in the map to avoid querying the KV
	// if the key is not present
	h.keys[checksum] = struct{}{}
	return nil
}
