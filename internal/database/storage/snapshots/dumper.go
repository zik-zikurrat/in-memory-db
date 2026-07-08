package snapshots

type Snapshotable interface {
	Dump() map[string]string
	Load(data map[string]string)
}

type Dumper struct {
}
