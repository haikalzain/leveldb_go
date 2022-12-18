package table

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/golang/snappy"
	"io"
	"leveldb_go/crc"
)

type BlockWriter struct {
	writer          bytes.Buffer
	restartInterval int
	scratch         []byte
	restarts        []uint32

	counter int
	lastKey []byte
}

type Writer struct {
	writer      *CountingWriter
	closer      io.Closer
	blockWriter *BlockWriter
	indexWriter *BlockWriter

	pendingBH  BlockHandle
	pendingKey []byte

	maxBlockSize int

	compressBuf []byte
	buf         []byte
}

func newBlockWriter(restartInterval int) *BlockWriter {
	return &BlockWriter{
		restartInterval: restartInterval,
		restarts:        []uint32{0},
		scratch:         make([]byte, 50),
	}
}

func (w *BlockWriter) append(key, value []byte) {
	shared := 0
	if w.counter < w.restartInterval {
		for len(key) > shared && len(w.lastKey) > shared && key[shared] == w.lastKey[shared] {
			shared++
		}
	} else {
		w.restarts = append(w.restarts, uint32(w.writer.Len()))
		w.counter = 0
	}
	nonshared := len(key) - shared
	n := binary.PutUvarint(w.scratch, uint64(shared))
	w.writer.Write(w.scratch[:n])
	n = binary.PutUvarint(w.scratch, uint64(nonshared))
	w.writer.Write(w.scratch[:n])
	n = binary.PutUvarint(w.scratch, uint64(len(value)))
	w.writer.Write(w.scratch[:n])

	w.writer.Write(key[shared:])
	w.writer.Write(value)

	w.lastKey = key
	w.counter++

}

func (w *BlockWriter) finish() []byte {
	for _, idx := range w.restarts {
		binary.LittleEndian.PutUint32(w.scratch, idx)
		w.writer.Write(w.scratch[:4])
	}
	binary.LittleEndian.PutUint32(w.scratch, uint32(len(w.restarts)))
	w.writer.Write(w.scratch[:4])
	return w.writer.Bytes()
}

func (w *BlockWriter) reset() {
	w.writer.Reset()
	w.restarts = w.restarts[:1]
	w.lastKey = w.lastKey[:0]
	w.counter = 0
}

func (w *BlockWriter) estimatedSize() int {
	return w.writer.Len() + 4*(len(w.restarts)+1)
}

func (w *BlockWriter) LastKey() []byte {
	return w.lastKey
}

func (w *BlockWriter) Empty() bool {
	return w.writer.Len() == 0
}

func NewWriter(writer io.WriteCloser, maxBlockSize int) *Writer {
	return &Writer{
		writer:       newCountingWriter(*bufio.NewWriter(writer)),
		closer:       writer,
		blockWriter:  newBlockWriter(16),
		indexWriter:  newBlockWriter(1),
		maxBlockSize: maxBlockSize,
		buf:          make([]byte, 40),
	}
}

func (w *Writer) Add(key, value []byte) error {
	w.blockWriter.append(key, value)

	if w.blockWriter.estimatedSize() >= w.maxBlockSize {
		err := w.finishDataBlock()
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) finishDataBlock() error {
	if w.blockWriter.Empty() {
		return nil
	}
	data := w.blockWriter.finish()
	w.writePendingBH()
	bh, err := w.writeBlock(data)
	if err != nil {
		return err
	}

	w.pendingBH = bh
	w.pendingKey = append(w.pendingKey[:0], w.blockWriter.LastKey()...)
	w.blockWriter.reset()
	return nil
}

func (w *Writer) writePendingBH() {
	if w.pendingBH.size > 0 {
		n := encodeBlockHandle(w.buf, w.pendingBH)
		w.indexWriter.append(w.pendingKey, w.buf[:n])
	}
}

func (w *Writer) writeBlock(block []byte) (BlockHandle, error) {
	offset := w.writer.Offset()
	data := block
	compressionType := kNoCompression
	w.compressBuf = snappy.Encode(w.compressBuf, block)
	if len(w.compressBuf) < len(block)-len(block)/8 {
		data = w.compressBuf
		compressionType = 1
	}
	w.buf[0] = byte(compressionType)

	checksum := crc.New(data).Update(w.buf[:1]).Value()
	binary.LittleEndian.PutUint32(w.buf[1:], checksum)

	_, err := w.writer.Write(data)
	if err != nil {
		return BlockHandle{}, err
	}
	_, err = w.writer.Write(w.buf[:5])
	if err != nil {
		return BlockHandle{}, err
	}
	return BlockHandle{
		offset: offset,
		size:   w.writer.Offset() - offset - blockTrailerLen,
	}, nil

}

func (w *Writer) Close() error {
	// flush data block
	err := w.finishDataBlock()
	if err != nil {
		return err
	}
	// reuse blockWriter for metaIndex
	metaIndex := w.blockWriter.finish()
	metaIndexHandle, err := w.writeBlock(metaIndex)
	if err != nil {
		return err
	}

	w.writePendingBH()
	index := w.indexWriter.finish()
	indexHandle, err := w.writeBlock(index)
	if err != nil {
		return err
	}

	w.buf = w.buf[:40]
	offset := encodeBlockHandle(w.buf, metaIndexHandle)
	encodeBlockHandle(w.buf[offset:], indexHandle)

	_, err = w.writer.Write(w.buf)
	if err != nil {
		return err
	}

	_, err = w.writer.Write([]byte(magic))
	if err != nil {
		return err
	}

	err = w.writer.Flush()
	if err != nil {
		return err
	}

	err = w.closer.Close()
	if err != nil {
		return err
	}

	return nil
}

func encodeBlockHandle(buf []byte, bh BlockHandle) int {
	n := binary.PutUvarint(buf, bh.offset)
	n += binary.PutUvarint(buf[n:], bh.size)
	return n
}

type CountingWriter struct {
	writer bufio.Writer
	n      uint64
}

func newCountingWriter(writer bufio.Writer) *CountingWriter {
	return &CountingWriter{
		writer: writer,
		n:      0,
	}
}

func (w *CountingWriter) Write(p []byte) (int, error) {
	written, err := w.writer.Write(p)
	w.n += uint64(written)
	return written, err
}

func (w *CountingWriter) Offset() uint64 {
	return w.n
}

func (w *CountingWriter) Flush() error {
	return w.writer.Flush()
}
