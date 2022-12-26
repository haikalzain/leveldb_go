package table

import "io"

const (
	magic             = "\x57\xfb\x80\x8b\x24\x75\x47\xdb"
	blockTrailerLen   = 5
	tableFooterLen    = 40
	TableMaxBlockSize = 4096
)

const (
	kNoCompression     = 0
	kSnappyCompression = 1
)

type BlockHandle struct {
	offset uint64
	size   uint64
}

type RandomAccessReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}
