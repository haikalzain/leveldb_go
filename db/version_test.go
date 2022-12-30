package db

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"leveldb_go/record"
	"leveldb_go/util"
	"testing"
)

type closeableBuffer struct {
	bytes.Buffer
}

func (c *closeableBuffer) Close() error {
	return nil
}

func TestReadWriteManifest(t *testing.T) {
	var buf closeableBuffer
	recordWriter := record.NewWriter(&buf)
	recordReader := record.NewReader(&buf)
	w := NewManifestWriter(recordWriter)

	ve := VersionEdit{
		newSeq: 1,
		filesToAdd: []tableFile{
			{
				fileNum: 123,
				minKey:  util.CreateIKey([]byte("test"), util.IKeyTypeSet, 12),
				maxKey:  util.CreateIKey([]byte("testMax"), util.IKeyTypeSet, 12),
				level:   1,
				size:    1000,
			},
			{
				fileNum: 12,
				minKey:  util.CreateIKey([]byte("test"), util.IKeyTypeSet, 12),
				maxKey:  util.CreateIKey([]byte("testMax"), util.IKeyTypeSet, 12),
				level:   2,
				size:    1000,
			},
		},
	}
	ve2 := VersionEdit{
		newSeq: 1,
		filesToRemove: []tableFile{
			{
				fileNum: 123,
				level:   1,
			},
		},
	}

	w.Append(&ve)
	w.Append(&ve2)

	vs, err := ReadManifest(recordReader)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(vs.currentVersion.files[1]))
	assert.Equal(t, []tableFile{{
		fileNum: 12,
		minKey:  util.CreateIKey([]byte("test"), util.IKeyTypeSet, 12),
		maxKey:  util.CreateIKey([]byte("testMax"), util.IKeyTypeSet, 12),
		level:   2,
		size:    1000,
	}}, vs.currentVersion.files[2])

}
