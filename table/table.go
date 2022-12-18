package table

import (
	"io"
	"strings"
)

const (
	magic             = "\x57\xfb\x80\x8b\x24\x75\x47\xdb"
	blockTrailerLen   = 5
	tableFooterLen    = 40
	tableMaxBlockSize = 4096
)

const (
	kNoCompression     = 0
	kSnappyCompression = 1
)

type BlockHandle struct {
	offset uint64
	size   uint64
}

type Comparator interface {
	Compare(key1, key2 []byte) int
}

type StringComparator struct{}

func (s *StringComparator) Compare(key1, key2 []byte) int {
	return strings.Compare(string(key1), string(key2))
}

type RandomAccessReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}
