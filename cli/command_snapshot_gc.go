package cli

import (
	"context"

	"github.com/kopia/kopia/internal/units"
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

	logContentStat("GC unused contents", stats.Unused())
	logContentStat("GC unused contents that are too recent to delete", stats.TooRecent())
	logContentStat("GC in-use contents", stats.InUse())
	logContentStat("GC in-use system-contents", stats.System())
	log.Info("GC find in-use content duration: ", stats.FindInUseDuration)
	log.Info("GC mark-delete content duration: ", stats.FindUnusedDuration)

	return err
}

func logContentStat(contentState string, s gc.CountAndSum) {
	count, sum := s.Approximate()
	log.Infof("%s: %d (%v bytes)", contentState, count, units.BytesStringBase2(sum))
}

func init() {
	snapshotGCCommand.Action(repositoryAction(runSnapshotGCCommand))
}
