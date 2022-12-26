package util

import (
	"strings"
)

type IKeyType byte

const (
	IKeyTypeDelete IKeyType = 0
	IKeyTypeSet    IKeyType = 1
)

type IKey []byte

func CreateIKey(key []byte, t IKeyType, seq int64) IKey {
	ikey := make(IKey, len(key)+8)
	copy(ikey, key)
	trailer := ikey[len(key):]
	trailer[0] = byte(t)
	trailer[1] = byte(seq)
	trailer[2] = byte(seq >> 8)
	trailer[3] = byte(seq >> 16)
	trailer[4] = byte(seq >> 24)
	trailer[5] = byte(seq >> 32)
	trailer[6] = byte(seq >> 40)
	trailer[7] = byte(seq >> 48)
	return ikey
}

func (k IKey) Key() []byte {
	return k[:len(k)-8]
}

func (k IKey) KeyType() IKeyType {
	return IKeyType(k[len(k)-8])
}

func (k IKey) seqNum() uint64 {
	i := len(k) - 7
	n := uint64(k[i])
	n |= uint64(k[i+1])
	n |= uint64(k[i+2])
	n |= uint64(k[i+3])
	n |= uint64(k[i+4])
	n |= uint64(k[i+5])
	n |= uint64(k[i+6])
	return n
}

type IKeyCmp struct {
	cmp Comparator
}

func (i IKeyCmp) Compare(key1, key2 []byte) int {
	/*if len(key2) == 0 {
		return 1
	}*/
	ak, bk := IKey(key1), IKey(key2)
	r := i.cmp.Compare(ak.Key(), bk.Key())
	if r != 0 {
		return r
	}

	if ak.seqNum() < bk.seqNum() {
		return 1
	}
	if ak.seqNum() > bk.seqNum() {
		return -1
	}
	return 0
}

func CreateIKeyCmp(cmp Comparator) Comparator {
	return IKeyCmp{
		cmp: cmp,
	}
}

var IKeyStringCmp = CreateIKeyCmp(&StringComparator{})

type Comparator interface {
	Compare(key1, key2 []byte) int
}

type StringComparator struct{}

func (s *StringComparator) Compare(key1, key2 []byte) int {
	return strings.Compare(string(key1), string(key2))
}
