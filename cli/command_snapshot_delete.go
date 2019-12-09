package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

var (
	snapshotDeleteCommand = snapshotCommands.Command("delete", "Explicitly delete a snapshot by providing a snapshot ID.")

	snapshotDeleteID = snapshotDeleteCommand.Arg("id", "Snapshot ID to be deleted").String()
)

func runDeleteCommand(ctx context.Context, rep *repo.Repository) error {
	return rep.Manifests.Delete(ctx, manifest.ID(*snapshotDeleteID))
}

func init() {
	addUserAndHostFlags(snapshotDeleteCommand)
	snapshotDeleteCommand.Action(repositoryAction(runDeleteCommand))
}
