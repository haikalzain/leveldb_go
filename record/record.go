package record

import (
	"encoding/binary"
	"io"
	"leveldb_go/crc"
)

const (
	fullChunkType   byte = 1
	firstChunkType  byte = 2
	middleChunkType byte = 3
	lastChunkType   byte = 4

	chunkSize       = 32 * 1024
	blockHeaderSize = 7
)

type Writer struct {
	w       io.WriteCloser
	buf     [chunkSize]byte
	offset  int
	flushed int
}

func NewWriter(writer io.WriteCloser) *Writer {
	return &Writer{
		w: writer,
	}
}

func (w *Writer) Write(data []byte) (int, error) {
	n := 0
	written := 0
	for {
		rem := chunkSize - w.offset - blockHeaderSize
		if rem < 0 {
			err := w.finishBlock()
			if err != nil {
				return 0, err
			}
			continue
		}

		if rem >= len(data) {
			chunkType := fullChunkType
			if n > 0 {
				chunkType = lastChunkType
			}
			w.addBlock(data, chunkType)
			written += len(data)
			break
		}

		chunk := data[:rem]
		data = data[rem:]
		chunkType := middleChunkType
		if n == 0 {
			chunkType = firstChunkType
		}
		w.addBlock(chunk, chunkType)
		err := w.finishBlock()
		if err != nil {
			return 0, err
		}
		n++
		written += len(chunk)
	}
	return written, nil
}

func (w *Writer) finishBlock() error {
	for w.offset < chunkSize {
		w.buf[w.offset] = 0
		w.offset++
	}
	err := w.Flush()
	if err != nil {
		return err
	}
	w.offset = 0
	w.flushed = 0
	return nil
}

func (w *Writer) Flush() error {
	_, err := w.w.Write(w.buf[w.flushed:w.offset])
	if err != nil {
		return err
	}
	w.flushed = w.offset
	return nil
}

func (w *Writer) Close() error {
	if w.offset > 0 {
		w.finishBlock()
	}
	return w.w.Close()
}

func (w *Writer) addBlock(block []byte, chunkType byte) {
	checksum := crc.New(block).Value()
	binary.LittleEndian.PutUint32(w.buf[w.offset:], checksum)
	binary.LittleEndian.PutUint16(w.buf[w.offset+4:], uint16(len(block)))
	w.buf[w.offset+6] = chunkType
	copy(w.buf[w.offset+blockHeaderSize:], block)
	w.offset += len(block) + blockHeaderSize
}

type Reader struct {
	r      io.Reader
	buf    [chunkSize]byte
	offset int
	size   int
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r: r,
	}
}

func (r *Reader) readBlock() error {
	size, err := r.r.Read(r.buf[:])
	if err != nil {
		return err
	}
	r.offset = 0
	r.size = size
	return nil
}

func (r *Reader) Recover() {
	r.readBlock()
}

func (r *Reader) ReadBlock() ([]byte, error) {
	var data []byte
	first := true
	for {
		if r.size-r.offset < blockHeaderSize || r.buf[r.offset+6] == 0 {
			err := r.readBlock()
			if err != nil {
				return nil, err
			}
		}
		chunkType := r.buf[r.offset+6]
		chunkLen := binary.LittleEndian.Uint16(r.buf[r.offset+4:])

		if first && chunkType != firstChunkType && chunkType != fullChunkType {
			first = true
			data = data[:]
			err := r.readBlock()
			if err != nil {
				return nil, err
			}
			continue
		}

		if !first && chunkType != middleChunkType && chunkType != lastChunkType {
			// some other corruption
			first = true
			data = data[:]
			err := r.readBlock()
			if err != nil {
				return nil, err
			}
			continue
		}
		first = false

		expectedChecksum := binary.LittleEndian.Uint32(r.buf[r.offset:])

		chunk := r.buf[r.offset+blockHeaderSize : r.offset+blockHeaderSize+int(chunkLen)]
		checksum := crc.New(chunk).Value()
		if checksum != expectedChecksum {
			// corruption occurred
			first = true
			data = data[:]
			err := r.readBlock()
			if err != nil {
				return nil, err
			}
			continue
		}

		data = append(data, chunk...)
		r.offset += blockHeaderSize + int(chunkLen)

		if chunkType == lastChunkType || chunkType == fullChunkType {
			return data, nil
		}
	}

}
