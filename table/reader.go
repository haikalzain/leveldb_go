package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/snappy"
	"leveldb_go/crc"
	"leveldb_go/util"
	"sort"
	"unsafe"
)

type BlockIter struct {
	data []byte
	//nRestarts     int
	restartOffset int
	restarts      []uint32
	cmp           util.Comparator

	offset int
	key    []byte
	value  []byte
}

func newBlockIter(block []byte, cmp util.Comparator) *BlockIter {
	nRestarts := int(binary.LittleEndian.Uint32(block[len(block)-4:]))
	restartOffset := len(block) - 4*(nRestarts+1)
	if restartOffset < 0 {
		return &BlockIter{}
	}
	restarts := unsafe.Slice((*uint32)(unsafe.Pointer(&block[restartOffset])), nRestarts)

	return &BlockIter{
		data:          block[:restartOffset],
		restarts:      restarts,
		restartOffset: restartOffset,
		cmp:           cmp,
	}
}

func (b *BlockIter) Key() []byte {
	return b.key
}

func (b *BlockIter) Value() []byte {
	return b.value
}

func (b *BlockIter) decodeEntry(offset int) (int, int, int, int) {
	tmp := offset
	shared, n := binary.Uvarint(b.data[tmp:])
	tmp += n
	nonshared, n := binary.Uvarint(b.data[tmp:])
	tmp += n
	valLen, n := binary.Uvarint(b.data[tmp:])
	tmp += n

	return int(shared), int(nonshared), int(valLen), tmp
}

func (b *BlockIter) Next() error {
	if b.offset == b.restartOffset {
		return errors.New("end of block")
	}

	shared, nonshared, valLen, tmp := b.decodeEntry(b.offset)

	if len(b.key) < shared {
		return errors.New("corruption: key is shorter than shared")
	}

	key_nonshared := b.data[tmp : tmp+nonshared]
	tmp += nonshared
	value := b.data[tmp : tmp+valLen]
	tmp += valLen
	b.offset = tmp

	if b.offset > b.restartOffset {
		return errors.New("corruption: offset exceeded restart offset")
	}

	b.key = append(b.key[:shared], key_nonshared...)
	b.value = value
	return nil
}

func (b *BlockIter) Seek(key []byte) bool {
	i := sort.Search(len(b.restarts), func(i int) bool {
		restart := int(b.restarts[len(b.restarts)-i-1]) // need to invert
		_, nonshared, _, offset := b.decodeEntry(restart)

		// shared must be 0
		foundKey := b.data[offset : offset+nonshared]

		return b.cmp.Compare(key, foundKey) >= 0
	})

	// if smaller than all of them, choose the first restart point
	if i == len(b.restarts) {
		i = len(b.restarts) - 1
	}

	restart := int(b.restarts[len(b.restarts)-i-1])

	b.offset = restart
	b.key = b.key[:0]

	for b.Next() == nil {
		if b.cmp.Compare(b.Key(), key) >= 0 {
			return true
		}
	}

	return false
}

type Reader struct {
	reader         RandomAccessReader
	verifyChecksum bool
	buf            []byte

	metaBH  BlockHandle
	indexBH BlockHandle

	indexBlock []byte

	cmp util.Comparator
}

func NewReader(reader RandomAccessReader, size int, cmp util.Comparator) (*Reader, error) {
	r := &Reader{
		reader:         reader,
		verifyChecksum: true,
		buf:            make([]byte, 50),
		cmp:            cmp,
	}
	if size < tableFooterLen+8 {
		return nil, fmt.Errorf("corruption: table is too small")
	}

	_, err := r.reader.ReadAt(r.buf[:8], int64(size-8))
	if err != nil {
		return nil, err
	}
	if string(r.buf[:8]) != magic {
		return nil, fmt.Errorf("corruption: magic invalid")
	}

	meta, index, err := r.readFooter(int64(size - tableFooterLen - 8))
	if err != nil {
		return nil, err
	}
	r.metaBH = meta
	r.indexBH = index
	r.indexBlock, err = r.readBlock(r.indexBH)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Reader) readFooter(offset int64) (BlockHandle, BlockHandle, error) {
	_, err := r.reader.ReadAt(r.buf[:40], offset)
	if err != nil {
		return BlockHandle{}, BlockHandle{}, err
	}
	meta, n := decodeBlockHandle(r.buf)
	index, m := decodeBlockHandle(r.buf[n:])
	if n == 0 || m == 0 {
		return BlockHandle{}, BlockHandle{}, errors.New("corruption: invalid footer")
	}

	return meta, index, nil
}

func decodeBlockHandle(buf []byte) (BlockHandle, int) {
	offset, n := binary.Uvarint(buf)
	size, m := binary.Uvarint(buf[n:])
	if n == 0 || m == 0 {
		return BlockHandle{}, 0
	}
	return BlockHandle{
		offset: offset,
		size:   size,
	}, n + m
}

func (r *Reader) readBlock(bh BlockHandle) ([]byte, error) {
	// can optimize by using buffer pool
	b := make([]byte, bh.size+blockTrailerLen)
	_, err := r.reader.ReadAt(b, int64(bh.offset))
	if err != nil {
		return nil, err
	}
	if r.verifyChecksum {
		checksum := crc.New(b[:bh.size+1]).Value()
		obtained := binary.LittleEndian.Uint32(b[bh.size+1:])
		if checksum != obtained {
			return nil, fmt.Errorf("corruption: incorrect checksum. expected %v got %v", obtained, checksum)
		}
	}

	data := b[:bh.size]
	switch b[bh.size] {
	case kNoCompression:
	case kSnappyCompression:
		data, err = snappy.Decode(nil, b[:bh.size])
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("invalid compression type %d", b[bh.size])
	}

	return data, nil
}

func (r *Reader) Iterator() *TableIter {
	indexIter := newBlockIter(r.indexBlock, r.cmp)
	return &TableIter{
		r:         r,
		indexIter: indexIter,
		dataIter:  nil,
		cmp:       r.cmp,
	}
}

type TableIter struct {
	r         *Reader
	indexIter *BlockIter
	dataIter  *BlockIter
	cmp       util.Comparator
}

func (i *TableIter) Key() []byte {
	if i.dataIter == nil {
		return nil
	}
	return i.dataIter.Key()
}

func (i *TableIter) Value() []byte {
	if i.dataIter == nil {
		return nil
	}
	return i.dataIter.Value()
}

func (i *TableIter) Next() error {
	// the actual implementations actually loop on this. i'm assuming no blocks are empty
	// so we don't have to loop
	// should also consider setting an error state once the iterator gets messed up
	if i.dataIter != nil && i.dataIter.Next() == nil {
		return nil
	}

	if err := i.indexIter.Next(); err != nil {
		return err
	}
	bh, n := decodeBlockHandle(i.indexIter.Value())
	if n == 0 {
		return fmt.Errorf("corruption: invalid block handle")
	}
	block, err := i.r.readBlock(bh)
	if err != nil {
		return err
	}
	i.dataIter = newBlockIter(block, i.cmp)

	return i.dataIter.Next()

}

func (i *TableIter) Seek(key []byte) bool {
	i.dataIter = nil
	if !i.indexIter.Seek(key) {
		return false
	}
	bh, n := decodeBlockHandle(i.indexIter.Value())
	if n == 0 {
		return false
	}
	block, err := i.r.readBlock(bh)
	if err != nil {
		return false
	}
	i.dataIter = newBlockIter(block, i.cmp)
	return i.dataIter.Seek(key)
}

func (i *TableIter) GetIKey(ikey util.IKey) ([]byte, bool) {
	if !i.Seek(ikey) {
		return nil, false
	}

	ikey2 := util.IKey(i.Key())

	if ikey2.KeyType() == util.IKeyTypeDelete || !bytes.Equal(ikey.Key(), ikey2.Key()) {
		return nil, false
	}

	return i.Value(), true
}
