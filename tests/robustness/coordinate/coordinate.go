package coordinate

import (
	"io"

	"github.com/kopia/kopia/tests/robustness"
)

// FSCoordination is a wrapper for the Snapshotter and FileWriter
// that allows for coordination between the two. Any changes to the
// data in the filesystem initiated by the FileWriter will be synchronized
// with the Snapshotter, such that no data can be manipulated at or below the
// path in which a snapshot is taking place (including the fingerprint gathering
// phase as well as the snapshot itself).
type FSCoordination struct {
	robustness.Snapshotter
	robustness.FileWriter
	PathLock *PathLock
}

var _ robustness.Snapshotter = (*FSCoordination)(nil)
var _ robustness.FileWriter = (*FSCoordination)(nil)

// CreateSnapshot is a wrapper on a snapshotter's CreateSnapshot method, where
// the PathLock will block any concurrent attempts at modifying the sourceDir
// or anything below it.
func (fsc *FSCoordination) CreateSnapshot(sourceDir string, opts map[string]string) (snapID string, fingerprint []byte, stats *robustness.CreateSnapshotStats, err error) {
	fsc.PathLock.Lock(sourceDir)
	defer fsc.PathLock.Unlock(sourceDir)

	return fsc.Snapshotter.CreateSnapshot(sourceDir, opts)
}

// RestoreSnapshot is a wrapper around a snapshotter's RestoreSnapshot, where
// the PathLock will block any concurrent attempts at modifying the directory
// being restored.
func (fsc *FSCoordination) RestoreSnapshot(snapID, restoreDir string, opts map[string]string) ([]byte, error) {
	fsc.PathLock.Lock(restoreDir)
	defer fsc.PathLock.Unlock(restoreDir)

	return fsc.Snapshotter.RestoreSnapshot(snapID, restoreDir, opts)
}

// RestoreSnapshotCompare is a wrapper around a snapshotter's RestoreSnapshotCompare,
// where the PathLock will block any concurrent attempts at modifying the directory
// being restored.
func (fsc *FSCoordination) RestoreSnapshotCompare(snapID, restoreDir string, validationData []byte, reportOut io.Writer, opts map[string]string) error {
	fsc.PathLock.Lock(restoreDir)
	defer fsc.PathLock.Unlock(restoreDir)

	return fsc.Snapshotter.RestoreSnapshotCompare(snapID, restoreDir, validationData, reportOut, opts)
}
