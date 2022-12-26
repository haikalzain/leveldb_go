package db

type Version struct {
	seq uint64
}

type Snapshot struct {
	version Version
}
