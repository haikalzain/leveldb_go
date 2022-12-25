package db

import (
	"fmt"
	"path/filepath"
)

type fileType int

const (
	fileTypeLog fileType = iota
	fileTypeManifest
	fileTypeLock
	fileTypeCurrent
	fileTypeTable
)

func dbFilename(dirname string, fileType fileType, fileNum int) string {
	switch fileType {
	case fileTypeLock:
		return filepath.Join(dirname, "LOCK")
	case fileTypeLog:
		return filepath.Join(dirname, fmt.Sprintf("%06d.log", fileNum))
	case fileTypeTable:
		return filepath.Join(dirname, fmt.Sprintf("%06d.ldb", fileNum))
	case fileTypeManifest:
		return filepath.Join(dirname, fmt.Sprintf("MANIFEST-%06d", fileNum))
	case fileTypeCurrent:
		return filepath.Join(dirname, "CURRENT")
	}
	panic("unreachable")
}
