package db

import (
	"errors"
	"leveldb_go/record"
	"os"
	"strconv"
)

type manifest struct {
	fileNum int
	writer  *ManifestWriter
}

func createNewManifest(dirname string, fileNum int) (*ManifestWriter, error) {
	m, err := os.Create(dbFilename(dirname, fileTypeManifest, fileNum))
	if err != nil {
		return nil, err
	}
	err = os.WriteFile(dbFilename(dirname, fileTypeCurrent, 0),
		[]byte(dbFilename(".", fileTypeManifest, fileNum)), 0644)
	if err != nil {
		return nil, err
	}

	return NewManifestWriter(record.NewWriter(m)), nil
}

func openManifest(dirname string) (*manifest, *VersionSet, error) {
	current, err := os.Open(dbFilename(dirname, fileTypeCurrent, 0))
	if err != nil {
		return nil, nil, err
	}
	defer current.Close()
	buf := make([]byte, 20)
	len, err := current.Read(buf)
	if err != nil {
		return nil, nil, err
	}
	buf = buf[:len]
	if len != 15 || string(buf[:9]) != "MANIFEST-" {
		return nil, nil, errors.New("current file corrupted")
	}
	fileNum, err := strconv.Atoi(string(buf[9:]))
	if err != nil {
		return nil, nil, err
	}
	m, err := os.Open(dbFilename(dirname, fileTypeManifest, fileNum))
	if err != nil {
		return nil, nil, err
	}
	defer m.Close()

	vs, err := ReadManifest(record.NewReader(m))
	if err != nil {
		return nil, nil, err
	}

	w, err := createNewManifest(dirname, fileNum+1)
	if err != nil {
		return nil, nil, err
	}

	err = w.Append(vs.AsVersionEdit())
	if err != nil {
		return nil, nil, err
	}

	return &manifest{
		fileNum: fileNum + 1,
		writer:  w,
	}, vs, nil

}

func initManifest(dirname string) error {
	current, err := os.Create(dbFilename(dirname, fileTypeCurrent, 0))
	if err != nil {
		return err
	}
	defer current.Close()
	m, err := os.Create(dbFilename(dirname, fileTypeManifest, 2))
	if err != nil {
		return err
	}
	m.Close()
	_, err = current.Write([]byte(dbFilename(".", fileTypeManifest, 2)))
	if err != nil {
		return err
	}
	return nil
}

func isManifestExist(dirname string) (bool, error) {
	_, err := os.Stat(dbFilename(dirname, fileTypeCurrent, 0))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (m *manifest) logVersionEdit(ve *VersionEdit) error {
	// should call sync
	return m.writer.Append(ve)
}

func (m *manifest) Close() error {
	return m.writer.Close()
}
