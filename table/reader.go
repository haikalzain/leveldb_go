package table

import (
	"encoding/binary"
	"errors"
	"sort"
	"unsafe"
)

type BlockIter struct {
	data []byte
	//nRestarts     int
	restartOffset int
	restarts      []uint32
	cmp           Comparator

	offset int
	key    []byte
	value  []byte
}

func newBlockIter(block []byte) *BlockIter {
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
		cmp:           &StringComparator{},
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
		shared, _, _, offset := b.decodeEntry(restart)

		// nonshared must be 0
		foundKey := b.data[offset : offset+shared]

		if b.cmp.Compare(key, foundKey) >= 0 {
			return true
		}
		return false
	})

	if i == len(b.restarts) {
		return false
	}

	restart := int(b.restarts[0])

	b.offset = restart
	b.key = b.key[:0]

	for b.Next() == nil {
		if b.cmp.Compare(b.Key(), key) >= 0 {
			return true
		}
	}

	return false
}
