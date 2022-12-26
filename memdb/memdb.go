package memdb

import (
	"bytes"
	"errors"
	"leveldb_go/util"
	"math/rand"
)

const maxHeight = 12

type MemDB struct {
	head *node
	cmp  util.Comparator
}

func NewMemDB(cmp util.Comparator) *MemDB {
	return &MemDB{
		head: newNode(maxHeight),
		cmp:  cmp,
	}
}

func (m *MemDB) Put(key, value []byte) {
	insertNode(m.head, m.cmp, key, value)
}

func (m *MemDB) Delete(key []byte) {
	n, exact := findNode(m.head, m.cmp, key, nil)
	if exact {
		n.deleted = true
	}
}

// Get /* do not use */
func (m *MemDB) Get(key []byte) ([]byte, bool) {
	n, exact := findNode(m.head, m.cmp, key, nil)
	if !exact || n.deleted {
		return nil, false
	}
	return n.value, true
}

func (m *MemDB) GetIKey(ikey util.IKey) ([]byte, bool) {
	n, _ := findNode(m.head, m.cmp, ikey, nil)
	if n == m.head {
		return nil, false
	}
	ikey2 := util.IKey(n.key)

	if ikey2.KeyType() == util.IKeyTypeDelete || !bytes.Equal(ikey.Key(), ikey2.Key()) {
		return nil, false
	}

	return n.value, true
}

type node struct {
	nextNode []*node
	prevNode []*node
	key      []byte
	value    []byte
	deleted  bool
}

func newNode(height int) *node {
	return &node{
		prevNode: make([]*node, height),
		nextNode: make([]*node, height),
	}
}

func findNode(head *node, cmp util.Comparator, key []byte, prev *[maxHeight]*node) (*node, bool) {
	height := len(head.nextNode) - 1
	current := head
	for height >= 0 {
		candidate := current.nextNode[height]
		if candidate == nil {
			if prev != nil {
				prev[height] = current
			}
			height--
			continue
		}
		result := cmp.Compare(candidate.key, key)
		if result == 0 {
			if prev != nil {
				for h := 0; h <= height; h++ {
					prev[height] = candidate
				}
			}
			return candidate, true
		}
		if result > 0 {
			if prev != nil {
				prev[height] = current
			}
			if height == 0 {
				return candidate, false
			}
			height--
			continue
		}
		current = candidate

	}
	return current, false
}

// need to add node type as well (tombstone deletion)

func insertNode(head *node, cmp util.Comparator, key []byte, value []byte) {
	// TODO inefficient buffer allocation
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	var prev [maxHeight]*node
	position, exactMatch := findNode(head, cmp, key, &prev)

	if exactMatch {
		position.value = v
		position.deleted = false
		return
	}
	h := 1
	for ; h < maxHeight; h++ {
		if rand.Int()%4 != 0 {
			break
		}
	}

	newNode := newNode(h)
	newNode.key = k
	newNode.value = v

	for i := 0; i < h; i++ {
		newNode.prevNode[i] = prev[i]
		newNode.nextNode[i] = prev[i].nextNode[i]
		prev[i].nextNode[i] = newNode
	}
}

type MemDBIter struct {
	m           *MemDB
	currentNode *node
}

func (m *MemDB) Iterator() *MemDBIter {
	return &MemDBIter{
		m:           m,
		currentNode: m.head,
	}
}
func (i *MemDBIter) Key() []byte {
	if i.currentNode == nil {
		return nil
	}
	return i.currentNode.key
}

func (i *MemDBIter) Value() []byte {
	if i.currentNode == nil {
		return nil
	}
	return i.currentNode.value
}

func (i *MemDBIter) Seek(key []byte) bool {
	n, exact := findNode(i.m.head, i.m.cmp, key, nil)
	i.currentNode = n
	if exact && n.deleted == false {
		return true
	}
	return i.Next() == nil
}

func (i *MemDBIter) Next() error {
	for i.currentNode != nil {
		i.currentNode = i.currentNode.nextNode[0]

		if i.currentNode == nil || !i.currentNode.deleted {
			break
		}
	}
	if i.currentNode == nil {
		return errors.New("eof")
	}
	return nil
}
