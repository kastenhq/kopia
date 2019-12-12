package cli

import (
	"context"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	snapshotRestoreCommand    = snapshotCommands.Command("restore", "Restore a snapshot from the snapshot ID to the given target path")
	snapshotRestoreSnapID     = snapshotRestoreCommand.Arg("id", "Snapshot ID to be restored").Required().String()
	snapshotRestoreTargetPath = snapshotRestoreCommand.Arg("target-path", "Path of the directory for the contents to be restored").Required().String()
)

func runSnapRestoreCommand(ctx context.Context, rep *repo.Repository) error {
	manifestID := manifest.ID(*snapshotRestoreSnapID)
	m := &snapshot.Manifest{}
	err := rep.Manifests.Get(ctx, manifestID, m)
	if err != nil {
		return err
	}

	rootEntry, err := snapshotfs.SnapshotRoot(rep, m)
	if err != nil {
		return err
	}
	return localfs.Copy(ctx, *snapshotRestoreTargetPath, rootEntry)
}

func init() {
	snapshotRestoreCommand.Action(repositoryAction(runSnapRestoreCommand))
}
