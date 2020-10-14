package snapshotfs

import (
	"os"
	"strings"
	"testing"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/snapshot"
)

func TestCheckpointRegistry(t *testing.T) {
	var cp checkpointRegistry

	d := mockfs.NewDirectory()
	dir1 := d.AddDir("dir1", os.FileMode(0o755))
	f1 := d.AddFile("f1", []byte{1, 2, 3}, os.FileMode(0o755))
	f2 := d.AddFile("f2", []byte{2, 3, 4}, os.FileMode(0o755))
	f3 := d.AddFile("f3", []byte{2, 3, 4}, os.FileMode(0o755))
	f4 := d.AddFile("f3", []byte{2, 3, 4}, os.FileMode(0o755))

	cp.addCheckpointCallback(dir1, func() (*snapshot.DirEntry, error) {
		return &snapshot.DirEntry{
			Name: "dir1",
			Type: snapshot.EntryTypeDirectory,
		}, nil
	})

	cp.addCheckpointCallback(f1, func() (*snapshot.DirEntry, error) {
		return &snapshot.DirEntry{
			Name: "f1",
		}, nil
	})

	cp.addCheckpointCallback(f2, func() (*snapshot.DirEntry, error) {
		return &snapshot.DirEntry{
			Name: "f2",
		}, nil
	})

	cp.addCheckpointCallback(f3, func() (*snapshot.DirEntry, error) {
		return &snapshot.DirEntry{
			Name: "other",
		}, nil
	})

	cp.addCheckpointCallback(f4, func() (*snapshot.DirEntry, error) {
		return nil, nil
	})

	// remove callback before it has a chance of firing
	cp.removeCheckpointCallback(f3)
	cp.removeCheckpointCallback(f3)

	var dmb dirManifestBuilder

	dmb.addEntry(&snapshot.DirEntry{
		Name: "pre-existing",
	})

	if err := cp.runCheckpoints(&dmb); err != nil {
		t.Fatalf("error running checkpoints: %v", err)
	}

	dm := dmb.Build(clock.Now(), "checkpoint")
	if got, want := len(dm.Entries), 4; got != want {
		t.Fatalf("got %v entries, wanted %v (%+#v)", got, want, dm.Entries)
	}

	// directory names don't get mangled
	if dm.Entries[0].Name != "dir1" {
		t.Errorf("invalid entry %v", dm.Entries[0])
	}

	if !strings.HasPrefix(dm.Entries[1].Name, ".checkpointed.f1.") {
		t.Errorf("invalid entry %v", dm.Entries[1])
	}

	if !strings.HasPrefix(dm.Entries[2].Name, ".checkpointed.f2.") {
		t.Errorf("invalid entry %v", dm.Entries[2])
	}

	if dm.Entries[3].Name != "pre-existing" {
		t.Errorf("invalid entry %v", dm.Entries[3])
	}
}
