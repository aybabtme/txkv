// Defines an interface for a key-value store with transactions. Transactions
// are expected to provide read-commited consistency.
package txkv

import (
	"bytes"
	"context"
	"sync"

	"github.com/aybabtme/txkv/internal/ds"
)

type (
	Key   []byte
	Value []byte
)

// KV specifies the basic operations needed from a key-value store.
type KV interface {
	Put(ctx context.Context, key Key, value Value) error
	Get(ctx context.Context, key Key) (Value, bool, error)
	Delete(ctx context.Context, key Key) error
	List(ctx context.Context, prefix Key) ([]Key, error)
}

// TransactionalKV is a KV that has transactions. Full ACID is not guaranteed:
// - atomicity: as expected
// - consistency: as expected
// - isolation: only read-commited
// - durability: no, only in-memory
type TransactionalKV interface {
	KV
	Begin(ctx context.Context) (TxKV, error)
}

// TxKV is a KV that is a transaction on top of a KV.
type TxKV interface {
	KV
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// InMem returns an in-memory TransactionalKV.
func InMem() TransactionalKV {
	return newMemKV()
}

type memkv struct {
	mu   sync.Mutex
	smap *ds.SortedBytesToBytesMap
}

func newMemKV() *memkv {
	return &memkv{smap: ds.NewSortedBytesToBytesMap()}
}

func (k *memkv) Put(ctx context.Context, key Key, value Value) error {
	k.mu.Lock()
	k.put(key, value)
	k.mu.Unlock()
	return nil
}

func (k *memkv) put(key Key, value Value) { k.smap.Put(key, value) }

func (k *memkv) Get(ctx context.Context, key Key) (Value, bool, error) {
	k.mu.Lock()
	v, ok := k.get(key)
	k.mu.Unlock()
	return v, ok, nil
}

func (k *memkv) get(key Key) (Value, bool) {
	return k.smap.Get(key)
}

func (k *memkv) Delete(ctx context.Context, key Key) error {
	k.mu.Lock()
	k.delete(key)
	k.mu.Unlock()
	return nil
}

func (k *memkv) delete(key Key) { _, _ = k.smap.Delete(key) }

func (k *memkv) List(ctx context.Context, prefix Key) ([]Key, error) {
	k.mu.Lock()
	keys := k.list(prefix)
	k.mu.Unlock()
	return keys, nil
}

func (k *memkv) list(prefix Key) []Key {
	firstK, _, ok := k.smap.Ceiling(prefix)
	if !ok {
		return nil
	}
	lastK, _, _ := k.smap.Max()

	var keys []Key
	k.smap.RangedKeys(firstK, lastK, func(k, v []byte) bool {
		if !bytes.HasPrefix(k, prefix) {
			return false
		}
		keys = append(keys, k)
		return true
	})
	return keys
}

func (k *memkv) Begin(ctx context.Context) (TxKV, error) {
	return &txmemkv{
		root:       k,
		tx:         newMemKV(),
		updated:    make(map[string]struct{}),
		tombstones: make(map[string]struct{}),
	}, nil
}

type txmemkv struct {
	root *memkv
	tx   *memkv

	mu         sync.Mutex
	updated    map[string]struct{}
	tombstones map[string]struct{}
}

func (k *txmemkv) Put(ctx context.Context, key Key, value Value) error {
	k.mu.Lock()
	delete(k.tombstones, string(key)) // if it was delete, it's not anymore
	k.updated[string(key)] = struct{}{}
	err := k.tx.Put(ctx, key, value)
	k.mu.Unlock()
	return err
}

func (k *txmemkv) Get(ctx context.Context, key Key) (Value, bool, error) {
	k.mu.Lock()
	if _, ok := k.tombstones[string(key)]; ok {
		k.mu.Unlock()
		return nil, false, nil
	}
	if _, ok := k.updated[string(key)]; ok {
		k.mu.Unlock()
		return k.tx.Get(ctx, key)
	}
	// we offer read-commited, we don't offer repeatable-reads: we'll see
	// concurrently commited changes to the underlying KV
	v, ok, err := k.root.Get(ctx, key)
	k.mu.Unlock()
	return v, ok, err
}

func (k *txmemkv) Delete(ctx context.Context, key Key) error {
	k.mu.Lock()
	k.tombstones[string(key)] = struct{}{}
	delete(k.updated, string(key)) // remove from updated set, if it was there
	err := k.tx.Delete(ctx, key)
	k.mu.Unlock()
	return err
}

func (k *txmemkv) List(ctx context.Context, prefix Key) ([]Key, error) {
	k.mu.Lock()
	k.root.mu.Lock()
	k.tx.mu.Lock()

	keys := k.root.list(prefix)
	k.root.mu.Unlock()

	txkeys := k.tx.list(prefix)
	k.tx.mu.Unlock()

	merged := ds.NewSortedBytesSet()
	for _, key := range keys {
		if _, ok := k.tombstones[string(key)]; !ok {
			merged.Put(key)
		}
	}
	k.mu.Unlock()

	for _, key := range txkeys {
		merged.Put(key)
	}
	var out []Key
	merged.Keys(func(k []byte) bool {
		out = append(out, Key(k))
		return true
	})
	return out, nil
}

func (k *txmemkv) Commit(ctx context.Context) error {
	k.mu.Lock()
	k.root.mu.Lock()
	k.tx.mu.Lock()

	for deleted := range k.tombstones {
		k.root.delete(Key(deleted))
	}
	for updated := range k.updated {
		key := Key(updated)
		if v, ok := k.tx.get(key); ok {
			k.root.put(key, v)
		}
	}

	k.root.mu.Unlock()
	k.tx.mu.Unlock()
	k.mu.Unlock()
	return nil
}

func (k *txmemkv) Rollback(ctx context.Context) error {
	// do nothing
	return nil
}
