package db

import (
	"leveldb_go/util"
	"sort"
)

const numLevels = 7

type Version struct {
	seq        uint64
	files      [numLevels][]tableFile
	refs       int
	next, prev *Version
}

func newVersion(seq uint64) *Version {
	return &Version{
		seq: seq,
	}
}

func (v *Version) addTable(meta tableFile) {
	level := meta.level
	v.files[level] = append(v.files[level], meta)
}

type byFileNum []tableFile

func (b byFileNum) Less(i, j int) bool {
	return b[i].fileNum < b[j].fileNum
}

func (b byFileNum) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byFileNum) Len() int {
	return len(b)
}

type byMinKey []tableFile

func (b byMinKey) Less(i, j int) bool {
	return b[i].fileNum < b[j].fileNum
}

func (b byMinKey) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byMinKey) Len() int {
	return len(b)
}

func (v *Version) applyVersionEdit(ve VersionEdit) *Version {

	version := Version{
		seq:  ve.newSeq,
		refs: 0,
	}

	for i := range v.files {
		deleted := make(map[int]bool)
		for _, f := range ve.filesToRemove[i] {
			deleted[f.fileNum] = true
		}
		version.files[i] = make([]tableFile, len(v.files[i])+len(ve.filesToAdd)-len(ve.filesToRemove))

		for _, f := range v.files[i] {
			_, exists := deleted[f.fileNum]
			if !exists {
				version.files[i] = append(version.files[i], f)
			}
		}

		for _, f := range ve.filesToAdd[i] {
			version.files[i] = append(version.files[i], f)

		}

		if i == 0 {
			sort.Sort(byFileNum(version.files[i]))
		} else {
			sort.Sort(byMinKey(version.files[i]))

		}
	}
	return &version
}

type VersionEdit struct {
	newSeq        uint64
	filesToAdd    [numLevels][]tableFile
	filesToRemove [numLevels][]tableFile
}

type tableFile struct {
	fileNum int
	minKey  util.IKey
	maxKey  util.IKey
	level   int
}

type Snapshot struct {
	version *Version
}
