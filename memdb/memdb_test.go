package memdb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"leveldb_go/util"
	"testing"
)

type testKV struct {
	key   string
	value string
}

var cmp = &util.StringComparator{}

func TestMemDB_GetPut(t *testing.T) {
	testKVs := []testKV{
		{"hello", "world"},
		{"hellllloooo", "x2"},
		{"hell", "x3"},
	}

	m := NewMemDB(cmp)

	for _, kv := range testKVs {
		m.Put([]byte(kv.key), []byte(kv.value))
	}

	for _, kv := range testKVs {
		v, ok := m.Get([]byte(kv.key))
		assert.True(t, ok)
		assert.Equal(t, kv.value, string(v))
	}
}

func TestMemDB_GetPutLarge(t *testing.T) {
	var testKVs []testKV
	for i := 0; i < 1000; i++ {
		testKVs = append(testKVs, testKV{
			fmt.Sprint("key", i),
			fmt.Sprint("value", i),
		})
	}

	m := NewMemDB(cmp)

	for _, kv := range testKVs {
		m.Put([]byte(kv.key), []byte(kv.value))
	}

	for _, kv := range testKVs {
		v, ok := m.Get([]byte(kv.key))
		assert.True(t, ok)
		assert.Equal(t, kv.value, string(v))
	}
}

func TestMemDB_GetPutDeleteLarge(t *testing.T) {
	var testKVs []testKV
	for i := 0; i < 1000; i++ {
		testKVs = append(testKVs, testKV{
			fmt.Sprint("key", i),
			fmt.Sprint("value", i),
		})
	}

	m := NewMemDB(cmp)

	for _, kv := range testKVs {
		m.Put([]byte(kv.key), []byte(kv.value))
	}

	for i := 500; i < 600; i++ {
		m.Delete([]byte(testKVs[i].key))
	}

	// try to delete again for good measure
	for i := 500; i < 600; i++ {
		m.Delete([]byte(testKVs[i].key))
	}

	for i, kv := range testKVs {
		v, ok := m.Get([]byte(kv.key))
		if i >= 500 && i < 600 {
			assert.False(t, ok)
			continue
		}
		assert.True(t, ok)
		assert.Equal(t, kv.value, string(v))
	}
	// putting them back fixes it
	for i := 500; i < 600; i++ {
		m.Put([]byte(testKVs[i].key), []byte(testKVs[i].value))
	}
	for _, kv := range testKVs {
		v, ok := m.Get([]byte(kv.key))
		assert.True(t, ok)
		assert.Equal(t, kv.value, string(v))
	}
}

func TestMemDB_Iterate(t *testing.T) {
	var testKVs []testKV
	for i := 0; i < 10; i++ {
		testKVs = append(testKVs, testKV{
			fmt.Sprint("key", i),
			fmt.Sprint("value", i),
		})
	}

	m := NewMemDB(cmp)

	for _, kv := range testKVs {
		m.Put([]byte(kv.key), []byte(kv.value))
	}

	for i := 5; i < 7; i++ {
		m.Delete([]byte(testKVs[i].key))
	}

	iter := m.Iterator()

	for i := 0; iter.Next() == nil; i++ {
		if i == 5 {
			i += 2
		}
		assert.Equal(t, testKVs[i].key, string(iter.Key()))
		assert.Equal(t, testKVs[i].value, string(iter.Value()))
	}
}

func TestMemDB_SeekLarge(t *testing.T) {
	var testKVs []testKV
	for i := 0; i < 1000; i++ {
		testKVs = append(testKVs, testKV{
			fmt.Sprintf("key%03d", i),
			fmt.Sprintf("value%03d", i),
		})
	}

	m := NewMemDB(cmp)

	for _, kv := range testKVs {
		m.Put([]byte(kv.key), []byte(kv.value))
	}

	for i := 500; i < 600; i++ {
		m.Delete([]byte(testKVs[i].key))
	}

	iter := m.Iterator()
	iter.Seek([]byte("key123"))
	assert.Equal(t, "value123", string(iter.Value()))

	iter.Seek([]byte("key543"))
	assert.Equal(t, "value600", string(iter.Value()))

	iter.Seek([]byte("key823"))
	assert.Equal(t, "value823", string(iter.Value()))
}
