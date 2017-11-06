package txkv_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/aybabtme/txkv"
)

func TestInMem(t *testing.T) {
	testKV(t, func(t testing.TB) TransactionalKV { return InMem() })
}

func testKV(t *testing.T, mkKV func(t testing.TB) TransactionalKV) {
	t.Helper()
	tests := []struct {
		name string
		op   func(context.Context, *testing.T, TransactionalKV)
	}{

		{
			name: "add, get, delete",
			op: func(ctx context.Context, t *testing.T, kv TransactionalKV) {
				key := Key("hello")
				want := Value("world")

				// first it's not there
				mustNotFind(ctx, t, kv, key)

				// we add it
				mustPut(ctx, t, kv, key, want)

				// then it's there
				mustFind(ctx, t, kv, key, want)

				// we delete it
				mustDelete(ctx, t, kv, key)

				// at-last it's not there anymore
				mustNotFind(ctx, t, kv, key)
			},
		},
		{
			name: "add many, list a slice",
			op: func(ctx context.Context, t *testing.T, kv TransactionalKV) {
				prefix := "1"
				keys := []Key{
					Key("0"),
					Key(prefix),
					Key(prefix + "0"),
					Key(prefix + "1"),
					Key(prefix + "2"),
					Key(prefix + "3"),
					Key("2"),
				}
				want := []Key{
					Key(prefix),
					Key(prefix + "0"),
					Key(prefix + "1"),
					Key(prefix + "2"),
					Key(prefix + "3"),
				}
				dummy := Value("world")

				// add they keys
				for _, k := range keys {
					mustPut(ctx, t, kv, k, dummy)
				}

				// we can see our key
				mustList(ctx, t, kv, Key(prefix), want)

			},
		},

		{
			name: "tx: add, get, delete",
			op: func(ctx context.Context, t *testing.T, kv TransactionalKV) {
				key := Key("hello")
				want := Value("world")

				tx, err := kv.Begin(ctx)
				require.NoError(t, err)

				// first it's not there
				mustNotFind(ctx, t, tx, key)

				// we add it
				mustPut(ctx, t, tx, key, want)

				// then it's there in the tx
				mustFind(ctx, t, tx, key, want)

				// but not in the original
				mustNotFind(ctx, t, kv, key)

				err = tx.Commit(ctx)
				require.NoError(t, err)

				// we can now see our key
				mustFind(ctx, t, kv, key, want)
			},
		},
		{
			name: "tx: add, delete, get",
			op: func(ctx context.Context, t *testing.T, kv TransactionalKV) {
				key := Key("hello")
				want := Value("world")

				tx, err := kv.Begin(ctx)
				require.NoError(t, err)

				mustPut(ctx, t, tx, key, want)

				// then it's there in the tx
				// but not in the original
				mustFind(ctx, t, tx, key, want)
				mustNotFind(ctx, t, kv, key)

				// we delete it
				mustDelete(ctx, t, tx, key)

				// it's not anywhere anymore
				mustNotFind(ctx, t, kv, key)
				mustNotFind(ctx, t, tx, key)

				err = tx.Commit(ctx)
				require.NoError(t, err)

				// it's still not anywhere
				mustNotFind(ctx, t, kv, key)
				mustNotFind(ctx, t, tx, key)
			},
		},
		{
			name: "tx: add, delete, add, get",
			op: func(ctx context.Context, t *testing.T, kv TransactionalKV) {
				key := Key("hello")
				want := Value("world")

				tx, err := kv.Begin(ctx)
				require.NoError(t, err)

				mustPut(ctx, t, tx, key, want)

				// then it's there in the tx
				// but not in the original
				mustFind(ctx, t, tx, key, want)
				mustNotFind(ctx, t, kv, key)

				// we delete it
				mustDelete(ctx, t, tx, key)

				// it's not anywhere anymore
				mustNotFind(ctx, t, kv, key)
				mustNotFind(ctx, t, tx, key)

				// we add it again
				mustPut(ctx, t, tx, key, want)

				// then it's there in the tx
				// but not in the original
				mustFind(ctx, t, tx, key, want)
				mustNotFind(ctx, t, kv, key)

				err = tx.Commit(ctx)
				require.NoError(t, err)

				// it's found in both
				mustFind(ctx, t, kv, key, want)
				mustFind(ctx, t, tx, key, want)
			},
		},
		{
			name: "tx: add many, list a slice",
			op: func(ctx context.Context, t *testing.T, kv TransactionalKV) {
				prefix := "1"
				keys := []Key{
					Key("0"),
					Key(prefix),
					Key(prefix + "0"),
					Key(prefix + "1"),
					Key(prefix + "2"),
					Key(prefix + "3"),
					Key("2"),
				}
				txkeys := []Key{
					Key(prefix + "4"),
					Key(prefix + "5"),
				}
				wantBeforeTx := []Key{
					Key(prefix),
					Key(prefix + "0"),
					Key(prefix + "1"),
					Key(prefix + "2"),
					Key(prefix + "3"),
				}
				wantAfterTx := []Key{
					Key(prefix),
					Key(prefix + "0"),
					Key(prefix + "1"),
					Key(prefix + "2"),
					Key(prefix + "3"),
					Key(prefix + "4"),
					Key(prefix + "5"),
				}
				dummy := Value("world")

				// add they keys
				for _, k := range keys {
					mustPut(ctx, t, kv, k, dummy)
				}

				tx, err := kv.Begin(ctx)
				require.NoError(t, err)

				// we can see our key in both tx and original
				mustList(ctx, t, tx, Key(prefix), wantBeforeTx)
				mustList(ctx, t, kv, Key(prefix), wantBeforeTx)

				for _, k := range txkeys {
					mustPut(ctx, t, tx, k, dummy)
				}

				// changes are only visible in the tx
				mustList(ctx, t, tx, Key(prefix), wantAfterTx)
				mustList(ctx, t, kv, Key(prefix), wantBeforeTx)

				err = tx.Commit(ctx)
				require.NoError(t, err)

				// changes are visible in both tx and original
				mustList(ctx, t, tx, Key(prefix), wantAfterTx)
				mustList(ctx, t, kv, Key(prefix), wantAfterTx)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.op(context.Background(), t, mkKV(t))
		})
	}
}

func mustPut(ctx context.Context, t *testing.T, kv KV, key Key, want Value) {
	t.Helper()
	err := kv.Put(ctx, key, want)
	require.NoError(t, err)
}

func mustDelete(ctx context.Context, t *testing.T, kv KV, key Key) {
	t.Helper()
	err := kv.Delete(ctx, key)
	require.NoError(t, err)
}

func mustFind(ctx context.Context, t *testing.T, kv KV, key Key, want Value) {
	t.Helper()
	got, ok, err := kv.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, got)

	keys, err := kv.List(ctx, key)
	require.NoError(t, err)
	require.Contains(t, keys, key)
}

func mustNotFind(ctx context.Context, t *testing.T, kv KV, key Key) {
	t.Helper()
	_, ok, err := kv.Get(ctx, key)
	require.NoError(t, err)
	require.False(t, ok)

	keys, err := kv.List(ctx, key)
	require.NoError(t, err)
	require.NotContains(t, keys, key)
}

func mustList(ctx context.Context, t *testing.T, kv KV, prefix Key, want []Key) {
	got, err := kv.List(ctx, Key(prefix))
	require.NoError(t, err)
	require.Equal(t, want, got)
}
