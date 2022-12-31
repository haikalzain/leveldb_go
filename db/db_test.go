package db

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"syscall"
	"testing"
)

const testdbPath = "testdb/db"

type testKV struct {
	key   string
	value string
}

var opt = Opt{
	maxMemorySize: 100,
}

func clearDir() {
	err := os.RemoveAll(testdbPath)
	if err != nil {
		panic("cannot clean dir")
	}
}

func TestDBOpen(t *testing.T) {
	clearDir()

	db, err := Open(testdbPath, opt)
	defer db.Close()
	assert.Nil(t, err)
	_, err = db.Get([]byte("key"))
	assert.NotNil(t, err)

	err = db.Set([]byte("key"), []byte("value"))
	assert.Nil(t, err)

	v, _ := db.Get([]byte("key"))
	assert.Equal(t, "value", string(v))
}

func TestDBLock(t *testing.T) {
	clearDir()
	os.MkdirAll(testdbPath, 0666)

	fd, err := syscall.Open(dbFilename(testdbPath, fileTypeLock, 0), syscall.O_CREAT|syscall.O_WRONLY, 0666)
	defer syscall.Close(fd)
	syscall.Flock(fd, syscall.LOCK_EX)
	_, err = Open(testdbPath, opt)
	assert.Equal(t, LockErr, err)
}

func TestReadWriteDB(t *testing.T) {
	clearDir()

	var testKVs []testKV
	for i := 0; i < 50; i++ {
		testKVs = append(testKVs, testKV{
			fmt.Sprint("key", i),
			fmt.Sprint("value", i),
		})
	}

	db, _ := Open(testdbPath, Opt{maxMemorySize: 10000})
	defer db.Close()

	for _, kv := range testKVs {
		db.Set([]byte(kv.key), []byte(kv.value))
	}

	for _, kv := range testKVs {
		v, err := db.Get([]byte(kv.key))
		assert.Nil(t, err)
		assert.Equal(t, kv.value, string(v))
	}
}

func TestOverflowMemtable(t *testing.T) {
	clearDir()

	var testKVs []testKV
	for i := 0; i < 50; i++ {
		testKVs = append(testKVs, testKV{
			fmt.Sprint("key", i),
			fmt.Sprint("value", i),
		})
	}

	db, _ := Open(testdbPath, Opt{maxMemorySize: 50})
	defer db.Close()

	for _, kv := range testKVs {
		db.Set([]byte(kv.key), []byte(kv.value))
	}

	for _, kv := range testKVs {
		v, err := db.Get([]byte(kv.key))
		assert.Nil(t, err)
		assert.Equal(t, kv.value, string(v))
	}
}

func TestReadWriteDBClose1(t *testing.T) {
	clearDir()

	testKVs := []testKV{
		{"hello", "world"},
	}

	db, _ := Open(testdbPath, opt)

	for _, kv := range testKVs {
		db.Set([]byte(kv.key), []byte(kv.value))
	}

	db.Close()
	db2, _ := Open(testdbPath, opt)
	defer db2.Close()
	for _, kv := range testKVs {
		v, err := db2.Get([]byte(kv.key))
		assert.Nil(t, err)
		assert.Equal(t, kv.value, string(v))
	}
}

func TestReadWriteDBClose(t *testing.T) {
	clearDir()

	var testKVs []testKV
	for i := 0; i < 1000; i++ {
		testKVs = append(testKVs, testKV{
			fmt.Sprint("key", i),
			fmt.Sprint("value", i),
		})
	}

	db, _ := Open(testdbPath, Opt{maxMemorySize: 1000})

	for _, kv := range testKVs {
		db.Set([]byte(kv.key), []byte(kv.value))
	}

	db.Close()
	db2, _ := Open(testdbPath, opt)
	defer db2.Close()
	for _, kv := range testKVs {
		v, err := db2.Get([]byte(kv.key))
		assert.Nil(t, err)
		assert.Equal(t, kv.value, string(v))
	}
}

func TestSnapshotRead(t *testing.T) {

}
