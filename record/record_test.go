package record

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func blob(s string, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.Write([]byte(s))
	}
	return b.String()
}

func TestReadWrite(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	n, _ := writer.Write([]byte("hello"))
	assert.Equal(t, 5, n)
	writer.Flush()

	reader := NewReader(buf)
	data, _ := reader.ReadBlock()

	assert.Equal(t, "hello", string(data))
}

func TestBoundary(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	reader := NewReader(buf)
	first := blob("ab", 10000)
	second := blob("bc", 10000)
	third := blob("ab", 30000)
	_, _ = writer.Write([]byte(first))
	_, _ = writer.Write([]byte(second))
	_, _ = writer.Write([]byte(third))
	writer.Flush()

	r1, _ := reader.ReadBlock()
	assert.Equal(t, first, string(r1))
	r2, _ := reader.ReadBlock()
	assert.Equal(t, second, string(r2))
	r3, _ := reader.ReadBlock()
	assert.Equal(t, third, string(r3))
}
