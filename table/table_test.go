package table

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

func testBlockKVs(t *testing.T, testKVs []testKV) {
	writer := newBlockWriter(16)
	for _, kv := range testKVs {
		writer.append([]byte(kv.key), []byte(kv.value))
	}
	data := writer.finish()

	iter := newBlockIter(data, cmp)
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

/*
*
Some of these tests are bad because for numbers >10 they aren't actually ordered
*/
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

	iter := newBlockIter(data, cmp)

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

	iter := newBlockIter(data, cmp)

	for i := 9; i >= 0; i-- {
		iter.Seek([]byte(fmt.Sprint("key", i)))
		assert.Equal(t, testKVs[i].key, string(iter.Key()))
		assert.Equal(t, testKVs[i].value, string(iter.Value()))
	}
}

func TestReadWriteTableSimple(t *testing.T) {
	testKVs := []testKV{
		{"helllllo", "world"},
		{"hello2", "x2"},
		{"hellp", "x3"},
	}
	buffer := make([]byte, 500)
	writer := newByteWriter(&buffer)
	w := NewWriter(writer, 50)
	for _, kv := range testKVs {
		err := w.Add([]byte(kv.key), []byte(kv.value))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	err := w.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	writer.Close()

	reader := newByteReader(buffer)
	r, err := NewReader(reader, len(buffer), cmp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	iter := r.Iterator()
	i := 0
	for i = 0; iter.Next() == nil; i++ {
		assert.Equal(t, testKVs[i].key, string(iter.Key()))
		assert.Equal(t, testKVs[i].value, string(iter.Value()))
	}
	assert.Equal(t, len(testKVs), i)
}

func TestReadWriteTableIKeySimple(t *testing.T) {
	testKVs := []testKV{
		{"hello1", "world"},
		{"hello2", "x2"},
		{"hello3", "x3"},
	}
	buffer := make([]byte, 500)
	writer := newByteWriter(&buffer)
	w := NewWriter(writer, 50)
	for _, kv := range testKVs {
		key := util.CreateIKey([]byte(kv.key), util.IKeyTypeSet, 0)
		err := w.Add(key, []byte(kv.value))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	err := w.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	writer.Close()

	reader := newByteReader(buffer)
	r, err := NewReader(reader, len(buffer), util.IKeyStringCmp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	iter := r.Iterator()
	i := 0
	for i = 0; iter.Next() == nil; i++ {
		key := util.IKey(iter.Key())

		assert.Equal(t, testKVs[i].key, string(key.Key()))
		assert.Equal(t, testKVs[i].value, string(iter.Value()))
	}

	assert.Equal(t, len(testKVs), i)

	key := util.CreateIKey([]byte("hello2"), util.IKeyTypeSet, 0)
	v, ok := iter.GetIKey(key)
	assert.True(t, ok)
	assert.Equal(t, "x2", string(v))

	key = util.CreateIKey([]byte("hello3"), util.IKeyTypeSet, 0)
	v, ok = iter.GetIKey(key)
	assert.True(t, ok)
	assert.Equal(t, "x3", string(v))
}

func TestReadWriteTableLarge(t *testing.T) {
	var testKVs []testKV
	for i := 0; i < 1000; i++ {
		testKVs = append(testKVs, testKV{
			fmt.Sprint("key", i),
			fmt.Sprint("value", i),
		})
	}
	buffer := make([]byte, 20000)
	writer := newByteWriter(&buffer)
	w := NewWriter(writer, 50)
	for _, kv := range testKVs {
		err := w.Add([]byte(kv.key), []byte(kv.value))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	err := w.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	writer.Close()

	reader := newByteReader(buffer)
	r, err := NewReader(reader, len(buffer), cmp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	iter := r.Iterator()
	i := 0
	for i = 0; iter.Next() == nil; i++ {
		assert.Equal(t, testKVs[i].key, string(iter.Key()))
		assert.Equal(t, testKVs[i].value, string(iter.Value()))
	}
	assert.Equal(t, len(testKVs), i)
}

func TestTableSeekMultiple(t *testing.T) {
	var testKVs []testKV
	for i := 0; i < 10; i++ {
		testKVs = append(testKVs, testKV{
			fmt.Sprint("key", i),
			fmt.Sprint("value", i),
		})
	}

	buffer := make([]byte, 500)
	writer := newByteWriter(&buffer)
	w := NewWriter(writer, 50)
	for _, kv := range testKVs {
		err := w.Add([]byte(kv.key), []byte(kv.value))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	err := w.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	writer.Close()

	reader := newByteReader(buffer)
	r, err := NewReader(reader, len(buffer), cmp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	iter := r.Iterator()

	for i := 9; i >= 0; i-- {
		iter.Seek([]byte(fmt.Sprint("key", i)))
		assert.Equal(t, testKVs[i].key, string(iter.Key()))
		assert.Equal(t, testKVs[i].value, string(iter.Value()))
	}
}
