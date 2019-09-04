package avl

import (
	"bytes"

	"github.com/perlin-network/wavelet/store"
	"github.com/valyala/bytebufferpool"
)

type diffKV struct {
	prefix []byte
	kv     store.KV
	viewID uint64
	keys   map[[MerkleHashSize]byte]struct{}
}

func newDiffKV(kv store.KV, prefix []byte, viewID uint64) *diffKV {
	return &diffKV{
		prefix: prefix,
		kv:     kv,
		keys:   make(map[[MerkleHashSize]byte]struct{}),
	}
}

func (h *diffKV) Get(id [MerkleHashSize]byte) (*node, error) {
	if _, ok := h.keys[id]; !ok {
		return nil, nil
	}

	b, err := h.kv.Get(append(h.prefix, id[:]...))
	if err != nil {
		return nil, err
	}

	node, err := DeserializeFromDifference(bytes.NewReader(b), h.viewID)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (h *diffKV) Put(nd *node) error {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	nd.serializeForDifference(buf)

	if err := h.kv.Put(append(h.prefix, nd.id[:]...), buf.Bytes()); err != nil {
		return err
	}

	h.keys[nd.id] = struct{}{}
	return nil
}
