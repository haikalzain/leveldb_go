package table

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type testKV struct {
	key   string
	value string
}

func testBlockKVs(t *testing.T, testKVs []testKV) {
	writer := newBlockWriter(16)
	for _, kv := range testKVs {
		writer.append([]byte(kv.key), []byte(kv.value))
	}
	data := writer.finish()

	iter := newBlockIter(data)
	i := 0
	for i = 0; iter.Next() == nil; i++ {
		assert.Equal(t, testKVs[i].key, string(iter.Key()))
		assert.Equal(t, testKVs[i].value, string(iter.Value()))
	}
	assert.Equal(t, len(testKVs), i)
}

func TestReadWriteBlockData(t *testing.T) {
	testKVs := []testKV{
		{"hello", "world"},
		{"hellllloooo", "x2"},
		{"hell", "x3"},
	}

	testBlockKVs(t, testKVs)
}

func TestReadWriteBlockDataRestarts(t *testing.T) {
	var testKVs []testKV
	for i := 0; i < 50; i++ {
		testKVs = append(testKVs, testKV{
			fmt.Sprint("key", i),
			fmt.Sprint("value", i),
		})
	}

	testBlockKVs(t, testKVs)
}

func TestBlockSeekSingle(t *testing.T) {
	var testKVs []testKV
	for i := 0; i < 10; i++ {
		testKVs = append(testKVs, testKV{
			fmt.Sprint("key", i),
			fmt.Sprint("value", i),
		})
	}

	writer := newBlockWriter(16)
	for _, kv := range testKVs {
		writer.append([]byte(kv.key), []byte(kv.value))
	}
	data := writer.finish()

	iter := newBlockIter(data)

	iter.Seek([]byte("key3"))
	assert.Equal(t, testKVs[3].key, string(iter.Key()))
	assert.Equal(t, testKVs[3].value, string(iter.Value()))

	iter.Seek([]byte("key41"))
	assert.Equal(t, testKVs[5].key, string(iter.Key()))
	assert.Equal(t, testKVs[5].value, string(iter.Value()))

}

func TestBlockSeekMultiple(t *testing.T) {
	var testKVs []testKV
	for i := 0; i < 10; i++ {
		testKVs = append(testKVs, testKV{
			fmt.Sprint("key", i),
			fmt.Sprint("value", i),
		})
	}

	writer := newBlockWriter(16)
	for _, kv := range testKVs {
		writer.append([]byte(kv.key), []byte(kv.value))
	}
	data := writer.finish()

	iter := newBlockIter(data)

	for i := 9; i >= 0; i-- {
		iter.Seek([]byte(fmt.Sprint("key", i)))
		assert.Equal(t, testKVs[i].key, string(iter.Key()))
		assert.Equal(t, testKVs[i].value, string(iter.Value()))
	}
}
