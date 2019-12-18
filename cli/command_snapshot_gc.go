package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/gc"
)

var (
	snapshotGCCommand       = snapshotCommands.Command("gc", "Remove contents not used by any snapshot")
	snapshotGCMinContentAge = snapshotGCCommand.Flag("min-age", "Minimum content age to allow deletion").Default("24h").Duration()
	snapshotGCDelete        = snapshotGCCommand.Flag("delete", "Delete unreferenced contents").Bool()
)

func runSnapshotGCCommand(ctx context.Context, rep *repo.Repository) error {
	stats, err := gc.Run(ctx, rep, *snapshotGCMinContentAge, *snapshotGCDelete)

	log.Info("GC unused contents: ", stats.Unused.String())
	log.Info("GC unused contents that are too recent to delete: ", stats.TooRecent.String())
	log.Info("GC in-use contents: ", stats.InUse.String())
	log.Info("GC in-use system-contents: ", stats.System.String())
	log.Info("GC find in-use content duration: ", stats.FindInUseDuration)
	log.Info("GC mark-delete content duration: ", stats.FindUnusedDuration)

	return err
}

func init() {
	snapshotGCCommand.Action(repositoryAction(runSnapshotGCCommand))
}
