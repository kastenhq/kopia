package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/volume/volumefs"
)

var (
	volListCommand      = volumeCommands.Command("list", "List volume snapshots")
	volListCommandVolID = volListCommand.Flag("vol-id", "Optional volume identifier").Short('V').String()
)

func init() {
	volListCommand.Action(repositoryAction(runVolListCommand))
}

func runVolListCommand(ctx context.Context, rep repo.Repository) error {
	snaps, err := volumefs.ListSnapshots(ctx, rep, *volListCommandVolID)
	if err == nil && len(snaps) > 0 {
		sort.SliceStable(snaps, func(i, j int) bool {
			if snaps[i].VolumeID != snaps[j].VolumeID {
				return snaps[i].VolumeID < snaps[j].VolumeID
			}
			return snaps[i].Manifest.StartTime.Before(snaps[j].Manifest.StartTime)
		})

		fmt.Printf("      Timestamp                   Repository ID           Chain VolID / SnapID\n" +
			"----------------------- --------------------------------- ----- -------------\n")

		for _, s := range snaps {
			fmt.Printf("%s %s %5d %s %s\n", formatTimestamp(s.Manifest.StartTime), s.Manifest.RootObjectID(), s.ChainLength, s.VolumeID, s.VolumeSnapshotID)
		}
	}

	return err
}
