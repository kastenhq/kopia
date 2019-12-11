package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/pkg/errors"
)

var (
	snapshotDeleteCommand        = snapshotCommands.Command("delete", "Explicitly delete a snapshot by providing a snapshot ID.")
	snapshotDeleteID             = snapshotDeleteCommand.Arg("id", "Snapshot ID to be deleted").Required().String()
	snapshotDeleteHostName       = snapshotDeleteCommand.Flag("hostname", "Specify the host name of the snapshot to be deleted").String()
	snapshotDeleteUserName       = snapshotDeleteCommand.Flag("username", "Specify the user name of the snapshot to be deleted").String()
	snapshotDeletePath           = snapshotDeleteCommand.Flag("path", "Specify the path of the snapshot to be deleted").String()
	snapshotDeleteIgnoreHostName = snapshotDeleteCommand.Flag("unsafe-ignore-host", "Override the requirement to specify a host name for the delete to succeed").Bool()
	snapshotDeleteIgnoreUserName = snapshotDeleteCommand.Flag("unsafe-ignore-user", "Override the requirement to specify a user name for the delete to succeed").Bool()
	snapshotDeleteIgnorePath     = snapshotDeleteCommand.Flag("unsafe-ignore-path", "Override the requirement to specify a path for the delete to succeed").Bool()
)

func runDeleteCommand(ctx context.Context, rep *repo.Repository) error {
	if !*snapshotDeleteIgnoreHostName && *snapshotDeleteHostName == "" {
		return errors.New("host name is required")
	}
	if !*snapshotDeleteIgnoreUserName && *snapshotDeleteUserName == "" {
		return errors.New("user name is required")
	}
	if !*snapshotDeleteIgnorePath && *snapshotDeletePath == "" {
		return errors.New("path is required")
	}

	manifestID := manifest.ID(*snapshotDeleteID)
	manifestMeta, err := rep.Manifests.GetMetadata(ctx, manifestID)
	if err != nil {
		return err
	}
	labels := manifestMeta.Labels
	if labels["type"] != "snapshot" {
		return errors.New("snapshot ID provided did not reference a snapshot")
	}
	if labels["hostname"] != *snapshotDeleteHostName && !*snapshotDeleteIgnoreHostName {
		return errors.New("host name does not match for deleting requested snapshot ID")
	}
	if labels["username"] != *snapshotDeleteUserName && !*snapshotDeleteIgnoreUserName {
		return errors.New("user name does not match for deleting requested snapshot ID")
	}
	if labels["path"] != *snapshotDeletePath && !*snapshotDeleteIgnorePath {
		return errors.New("path does not match for deleting requested snapshot ID")
	}

	return rep.Manifests.Delete(ctx, manifestID)
}

func init() {
	snapshotDeleteCommand.Action(repositoryAction(runDeleteCommand))
}
