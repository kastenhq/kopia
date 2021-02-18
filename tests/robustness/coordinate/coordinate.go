package coordinate

import (
	"io"

	"github.com/kopia/kopia/tests/robustness"
)

type FSCoordination struct {
	robustness.Snapshotter
	robustness.FileWriter
	PathLock *PathLock
}

var _ robustness.Snapshotter = (*FSCoordination)(nil)
var _ robustness.FileWriter = (*FSCoordination)(nil)

func (fsc *FSCoordination) CreateSnapshot(sourceDir string, opts map[string]string) (snapID string, fingerprint []byte, stats *robustness.CreateSnapshotStats, err error) {
	fsc.PathLock.Lock(sourceDir)
	defer fsc.PathLock.Unlock(sourceDir)

	return fsc.Snapshotter.CreateSnapshot(sourceDir, opts)
}

func (fsc *FSCoordination) RestoreSnapshot(snapID, restoreDir string, opts map[string]string) ([]byte, error) {
	fsc.PathLock.Lock(restoreDir)
	defer fsc.PathLock.Unlock(restoreDir)

	return fsc.Snapshotter.RestoreSnapshot(snapID, restoreDir, opts)
}

func (fsc *FSCoordination) RestoreSnapshotCompare(snapID, restoreDir string, validationData []byte, reportOut io.Writer, opts map[string]string) error {
	fsc.PathLock.Lock(restoreDir)
	defer fsc.PathLock.Unlock(restoreDir)

	return fsc.Snapshotter.RestoreSnapshotCompare(snapID, restoreDir, validationData, reportOut, opts)
}
