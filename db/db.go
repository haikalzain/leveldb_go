package db

import (
	"errors"
	"io"
	"leveldb_go/memdb"
	"leveldb_go/record"
	"leveldb_go/table"
	"leveldb_go/util"
	"os"
	"syscall"
)

var LockErr = errors.New("cannot acquire file lock")

type DB struct {
	dirname      string
	mem          *memdb.MemDB
	lastTableNum int

	versionSet *VersionSet // version is created when memtable is filled or when compaction occurs
	seqNum     uint64

	flock io.Closer

	logWriter *record.Writer
	manifest  *manifest

	cmp  util.Comparator
	ucmp util.Comparator

	opt Opt
}

type Opt struct {
	maxMemorySize int
}

func (db *DB) Get(key []byte) ([]byte, error) {
	ikey := util.CreateIKey(key, util.IKeyTypeSet, db.seqNum)
	val, ok := db.mem.GetIKey(ikey)
	if ok {
		return val, nil
	}
	version := db.versionSet.currentVersion // should acquire and release version
	return db.getFromDisk(ikey, version)
}

func (db *DB) getFromDisk(ikey util.IKey, version *Version) ([]byte, error) {
	db.lookupTable(ikey, 0)
	for level := 0; level < numLevels; level++ {
		for _, meta := range version.files[level] {
			if db.ucmp.Compare(ikey.Key(), meta.minKey.Key()) >= 0 && db.cmp.Compare(ikey, meta.maxKey) <= 0 {
				v, err := db.lookupTable(ikey, meta.fileNum)
				if err == nil {
					return v, nil
				}
			}
		}
	}
	return nil, errors.New("not found")
}

func (db *DB) lookupTable(ikey util.IKey, fileNum int) ([]byte, error) {
	f, err := os.Open(dbFilename(db.dirname, fileTypeTable, fileNum))
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	reader, err := table.NewReader(f, int(stat.Size()), db.cmp)
	if err != nil {
		return nil, err
	}
	it := reader.Iterator()
	v, ok := it.GetIKey(ikey)
	if ok {
		return v, nil
	}
	return nil, errors.New("not found")
}

func (db *DB) nextSeqNum() uint64 {
	db.seqNum++
	return db.seqNum
}

func (db *DB) Set(key, value []byte) error {
	if len(value)+db.mem.ApproxSize() > db.opt.maxMemorySize {
		err := db.writeMemTable()
		if err != nil {
			return err
		}
		db.mem = memdb.NewMemDB(db.cmp)
	}

	ikey := util.CreateIKey(key, util.IKeyTypeSet, db.nextSeqNum())
	db.mem.Put(ikey, value)
	return nil
}

func (db *DB) Close() error {
	db.writeMemTable()
	db.manifest.Close()
	db.logWriter.Close()
	db.flock.Close()
	return nil
}

func (db *DB) writeMemTable() error {
	// need to add version with this table
	// do we need to copy memtable to keep iterator consistent?
	// optimizations for tombstoned entries/entries with more recent sequence num
	f, err := os.Create(dbFilename(db.dirname, fileTypeTable, db.lastTableNum))

	if err != nil {
		return err
	}
	writer := table.NewWriter(f, table.TableMaxBlockSize)

	var minKey, maxKey util.IKey
	it := db.mem.Iterator()
	for it.Next() == nil {
		if minKey == nil {
			minKey = it.Key()
			maxKey = it.Key()
		} else {
			if db.cmp.Compare(minKey, it.Key()) > 0 {
				minKey = it.Key()
			}
			if db.cmp.Compare(maxKey, it.Key()) < 0 {
				maxKey = it.Key()
			}
		}

		writer.Add(it.Key(), it.Value())
	}
	err = writer.Close()
	if err != nil {
		return err
	}

	ve := NewVersionEdit(db.seqNum, []tableFile{{
		fileNum: db.lastTableNum,
		minKey:  minKey,
		maxKey:  maxKey,
		level:   0,
		size:    writer.Len(),
		lastSeq: db.seqNum,
	}}, nil)
	db.lastTableNum++

	err = db.manifest.logVersionEdit(ve)
	if err != nil {
		return err
	}
	db.versionSet.ApplyVersionEdit(ve)

	return nil
}

func (db *DB) writeIterToTable() {

}

func Open(dirname string, opt Opt) (*DB, error) {
	// lock directory first
	err := os.MkdirAll(dirname, 0755)
	if err != nil {
		return nil, err
	}
	flock, err := lockDB(dirname)

	exist, err := isManifestExist(dirname)
	if err != nil {
		return nil, err
	}
	if !exist {
		err := initManifest(dirname)
		if err != nil {
			return nil, err
		}
	}

	logFile, err := os.Create(dbFilename(dirname, fileTypeLog, 0))
	memtable := memdb.NewMemDB(util.IKeyStringCmp)
	logWriter := record.NewWriter(logFile)

	// read manifest in, create vs and write out new manifest
	manifest, vs, err := openManifest(dirname)
	if err != nil {
		return nil, err
	}

	return &DB{
		dirname:    dirname,
		mem:        memtable,
		logWriter:  logWriter,
		flock:      flock,
		cmp:        util.IKeyStringCmp,
		ucmp:       &util.StringComparator{},
		opt:        opt,
		versionSet: vs,
		manifest:   manifest,
		seqNum:     vs.currentVersion.seqNum(),
	}, nil

}

func lockDB(dirname string) (io.Closer, error) {
	lockFile := dbFilename(dirname, fileTypeLock, 0)
	f, err := os.Create(lockFile)
	if err != nil {
		return nil, err
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		f.Close()
		return nil, LockErr
	}
	return f, nil
}
