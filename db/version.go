package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"leveldb_go/record"
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

func (v *Version) seqNum() uint64 {
	return v.seq
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

func (v *Version) applyVersionEdit(ve *VersionEdit) *Version {
	version := Version{
		seq:  ve.newSeq,
		refs: 0,
	}

	var filesToAdd [numLevels][]tableFile
	var filesToRemove [numLevels][]tableFile

	for _, f := range ve.filesToAdd {
		filesToAdd[f.level] = append(filesToAdd[f.level], f)
	}
	for _, f := range ve.filesToRemove {
		filesToRemove[f.level] = append(filesToRemove[f.level], f)
	}

	for i := range v.files {
		deleted := make(map[int]bool)
		for _, f := range filesToRemove[i] {
			deleted[f.fileNum] = true
		}
		version.files[i] = make([]tableFile, 0, len(v.files[i])+len(filesToAdd[i])-len(filesToRemove[i]))

		for _, f := range v.files[i] {
			_, exists := deleted[f.fileNum]
			if !exists {
				version.files[i] = append(version.files[i], f)
			}
		}

		for _, f := range filesToAdd[i] {
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
	filesToAdd    []tableFile
	filesToRemove []tableFile
}

func NewVersionEdit(newSeq uint64, filesToAdd []tableFile, filesToRemove []tableFile) *VersionEdit {
	return &VersionEdit{
		newSeq:        newSeq,
		filesToAdd:    filesToAdd,
		filesToRemove: filesToRemove,
	}

}

type tableFile struct {
	fileNum int
	minKey  util.IKey
	maxKey  util.IKey
	level   int
	size    uint64
	lastSeq uint64
}

type Snapshot struct {
	version *Version
}

type VersionSet struct {
	currentVersion *Version
}

func NewVersionSet() *VersionSet {
	return &VersionSet{
		currentVersion: newVersion(0),
	}
}

func (ve *VersionEdit) decode(data []byte) error {
	r := bytes.NewReader(data)
	for {
		tag, err := binary.ReadUvarint(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch tag {
		case tagLastSequence:
			lastSeq, err := binary.ReadUvarint(r)
			if err != nil {
				return err
			}
			ve.newSeq = lastSeq
		case tagDeletedFile:
			level, err := binary.ReadUvarint(r)
			if err != nil {
				return err
			}
			fileNum, err := binary.ReadUvarint(r)
			if err != nil {
				return err
			}
			ve.filesToRemove = append(ve.filesToRemove, tableFile{
				fileNum: int(fileNum),
				level:   int(level),
			})
		case tagNewFile:
			level, err := binary.ReadUvarint(r)
			if err != nil {
				return err
			}
			fileNum, err := binary.ReadUvarint(r)
			if err != nil {
				return err
			}
			size, err := binary.ReadUvarint(r)
			if err != nil {
				return err
			}
			minKeyLen, err := binary.ReadUvarint(r)
			if err != nil {
				return err
			}
			minKey := make([]byte, minKeyLen)
			_, err = io.ReadFull(r, minKey)
			if err != nil {
				return err
			}
			maxKeyLen, err := binary.ReadUvarint(r)
			if err != nil {
				return err
			}
			maxKey := make([]byte, maxKeyLen)
			_, err = io.ReadFull(r, maxKey)
			if err != nil {
				return err
			}
			ve.filesToAdd = append(ve.filesToAdd, tableFile{
				fileNum: int(fileNum),
				minKey:  minKey,
				maxKey:  maxKey,
				level:   int(level),
				size:    size,
			})
		default:
			return errors.New("unexpected tag")
		}
	}
}

func ReadManifest(reader *record.Reader) (*VersionSet, error) {
	version := newVersion(0)
	for {
		block, err := reader.ReadBlock()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		var ve VersionEdit
		err = ve.decode(block)
		if err != nil {
			return nil, err
		}
		version = version.applyVersionEdit(&ve) // TODO should optimize
	}

	return &VersionSet{
		currentVersion: version,
	}, nil
}

const (
	tagLastSequence = 4
	tagDeletedFile  = 6
	tagNewFile      = 7
)

type ManifestWriter struct {
	w   *record.Writer
	buf []byte
}

func NewManifestWriter(w *record.Writer) *ManifestWriter {
	return &ManifestWriter{
		w:   w,
		buf: make([]byte, 1000), // terrible idea
	}
}

func (m *ManifestWriter) Append(ve *VersionEdit) error {
	m.buf = m.buf[:]
	n := 0
	if ve.newSeq != 0 {
		n += binary.PutUvarint(m.buf[n:], tagLastSequence)
		n += binary.PutUvarint(m.buf[n:], ve.newSeq)
	}
	for _, f := range ve.filesToRemove {
		n += binary.PutUvarint(m.buf[n:], tagDeletedFile)
		n += binary.PutUvarint(m.buf[n:], uint64(f.level))
		n += binary.PutUvarint(m.buf[n:], uint64(f.fileNum))
	}
	for _, f := range ve.filesToAdd {
		n += binary.PutUvarint(m.buf[n:], tagNewFile)
		n += binary.PutUvarint(m.buf[n:], uint64(f.level))
		n += binary.PutUvarint(m.buf[n:], uint64(f.fileNum))
		n += binary.PutUvarint(m.buf[n:], f.size)

		n += binary.PutUvarint(m.buf[n:], uint64(len(f.minKey)))
		n += copy(m.buf[n:], f.minKey)
		n += binary.PutUvarint(m.buf[n:], uint64(len(f.maxKey)))
		n += copy(m.buf[n:], f.maxKey)
	}
	_, err := m.w.Write(m.buf[:n])
	if err != nil {
		return err
	}

	err = m.w.Flush()
	if err != nil {
		return err
	}

	// TODO need to SYNC here

	return nil
}

func (m *ManifestWriter) Close() error {
	return m.w.Close()
}

// should we use *VersionEdit to reduce copying?
func (v *VersionSet) ApplyVersionEdit(ve *VersionEdit) {
	version := v.currentVersion.applyVersionEdit(ve)
	v.Append(version)
}

func (v *VersionSet) AsVersionEdit() *VersionEdit {
	var files []tableFile
	for _, filesForLevel := range v.currentVersion.files {
		files = append(files, filesForLevel...)
	}
	return &VersionEdit{
		newSeq:        0,
		filesToAdd:    files,
		filesToRemove: nil,
	}
}

func (v *VersionSet) Append(version *Version) {
	v.currentVersion.next = version
	version.prev = v.currentVersion
	v.currentVersion = version

	// update ref counts?
}

func AcquireCurrentVersion() {

}
