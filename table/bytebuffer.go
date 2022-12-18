package table

import (
	"errors"
	"io"
)

type byteReader struct {
	bytes  []byte
	offset int64
}

type byteWriter struct {
	bytes    []byte
	bytesPtr *[]byte
	offset   int64
}

func newByteReader(bytes []byte) *byteReader {
	return &byteReader{
		bytes:  bytes,
		offset: 0,
	}
}

func (r *byteReader) Read(p []byte) (n int, err error) {
	n = copy(p, r.bytes[r.offset:])
	r.offset += int64(n)

	if n < len(p) {
		err = errors.New("eof")
	}
	return
}

func (r *byteReader) ReadAt(p []byte, off int64) (int, error) {
	r.offset = off
	return r.Read(p)
}
func (r *byteReader) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		return 0, errors.New("unsupported whence")
	}
	r.offset = offset
	return offset, nil // don't know if this is right
}

func (r *byteReader) Close() error {
	return nil
}

func newByteWriter(bytes *[]byte) *byteWriter {
	return &byteWriter{
		bytes:    *bytes,
		bytesPtr: bytes,
		offset:   0,
	}
}

func (w *byteWriter) Write(p []byte) (int, error) {
	n := copy(w.bytes[w.offset:], p)
	w.offset += int64(n)
	if n < len(p) {
		return n, errors.New("eof")
	}
	return n, nil
}

func (w *byteWriter) Close() error {
	*w.bytesPtr = w.bytes[:w.offset] // truncate original buffer
	return nil
}
