package memdb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type testKV struct {
	key   string
	value string
}

func TestMemDB_GetPut(t *testing.T) {
	testKVs := []testKV{
		{"hello", "world"},
		{"hellllloooo", "x2"},
		{"hell", "x3"},
	}

	m := NewMemDB()

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

	m := NewMemDB()

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

	m := NewMemDB()

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
