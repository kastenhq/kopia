package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	restoreRootID     = restoreCommands.Arg("root-id", "Snapshot Root ID from which to restore.").Required().String()
	restoreTargetPath = restoreCommands.Arg("target-path", "Path to which the snapshot must be restored.").Required().String()
)

func runRestoreCommand(ctx context.Context, rep *repo.Repository) error {
	oid, err := parseObjectID(ctx, rep, *restoreRootID)
	if err != nil {
		return err
	}

	return snapshotfs.Restore(ctx, rep, *restoreTargetPath, oid)
}

func init() {
	restoreCommands.Action(repositoryAction(runRestoreCommand))
}
