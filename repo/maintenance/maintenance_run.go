// Package maintenance manages automatic repository maintenance.
package maintenance

import (
	"context"
	"sort"
	"time"

	"github.com/gofrs/flock"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
)

// safetyMarginBetweenSnapshotGC is the minimal amount of time that must pass between snapshot
// GC cycles to allow all in-flight snapshots during earlier GC to be flushed to be flushed and
// visible to a following GC. The uploader will automatically create a checkpoint every 45 minutes,
// so ~1 hour should be enough but we're setting this to a higher conservative value for extra safety.
const (
	safetyMarginBetweenSnapshotGC = 4 * time.Hour

	extraSafetyMarginBeforeDroppingContentFromIndex = -1 * time.Hour
)

var log = logging.GetContextLoggerFunc("maintenance")

// Mode describes the mode of maintenance to perfor
type Mode string

// MaintainableRepository is a subset of Repository required for maintenance tasks.
type MaintainableRepository interface {
	Username() string
	Hostname() string
	Time() time.Time
	ConfigFilename() string

	BlobStorage() blob.Storage
	ContentManager() *content.Manager

	GetManifest(ctx context.Context, id manifest.ID, data interface{}) (*manifest.EntryMetadata, error)
	PutManifest(ctx context.Context, labels map[string]string, payload interface{}) (manifest.ID, error)
	FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error)
	DeleteManifest(ctx context.Context, id manifest.ID) error

	DeriveKey(purpose []byte, keyLength int) []byte
}

// Supported maintenance modes
const (
	ModeNone  Mode = "none"
	ModeQuick Mode = "quick"
	ModeFull  Mode = "full"
	ModeAuto  Mode = "auto" // run either quick of full if required by schedule
)

// shouldRun returns Mode if repository is due for periodic maintenance.
func shouldRun(ctx context.Context, rep MaintainableRepository, p *Params) (Mode, error) {
	if myUsername := rep.Username() + "@" + rep.Hostname(); p.Owner != myUsername {
		log(ctx).Debugf("maintenance owned by another user '%v'", p.Owner)
		return ModeNone, nil
	}

	s, err := GetSchedule(ctx, rep)
	if err != nil {
		return ModeNone, errors.Wrap(err, "error getting status")
	}

	// check full cycle first, as it does more than the quick cycle
	if p.FullCycle.Enabled {
		if rep.Time().After(s.NextFullMaintenanceTime) {
			log(ctx).Debugf("due for full manintenance cycle")
			return ModeFull, nil
		}

		log(ctx).Debugf("not due for full manintenance cycle until %v", s.NextFullMaintenanceTime)
	} else {
		log(ctx).Debugf("full manintenance cycle not enabled")
	}

	// no time for full cycle, check quick cycle
	if p.QuickCycle.Enabled {
		if rep.Time().After(s.NextQuickMaintenanceTime) {
			log(ctx).Debugf("due for quick manintenance cycle")
			return ModeQuick, nil
		}

		log(ctx).Debugf("not due for quick manintenance cycle until %v", s.NextQuickMaintenanceTime)
	} else {
		log(ctx).Debugf("quick manintenance cycle not enabled")
	}

	return ModeNone, nil
}

func updateSchedule(ctx context.Context, runParams RunParameters) error {
	rep := runParams.rep
	p := runParams.Params

	s, err := GetSchedule(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "error getting schedule")
	}

	switch runParams.Mode {
	case ModeFull:
		// on full cycle, also update the quick cycle
		s.NextFullMaintenanceTime = rep.Time().Add(p.FullCycle.Interval)
		s.NextQuickMaintenanceTime = s.NextFullMaintenanceTime.Add(p.QuickCycle.Interval)
		log(ctx).Debugf("scheduling next full cycle at %v", s.NextFullMaintenanceTime)
		log(ctx).Debugf("scheduling next quick cycle at %v", s.NextQuickMaintenanceTime)

		return SetSchedule(ctx, rep, s)

	case ModeQuick:
		log(ctx).Debugf("scheduling next quick cycle at %v", s.NextQuickMaintenanceTime)
		s.NextQuickMaintenanceTime = rep.Time().Add(p.QuickCycle.Interval)

		return SetSchedule(ctx, rep, s)

	default:
		return nil
	}
}

// RunParameters passes essential parameters for maintenance.
// It is generated by RunExclusive and can't be create outside of its package and
// is required to ensure all maintenance tasks run under an exclusive lock.
type RunParameters struct {
	rep MaintainableRepository

	Mode Mode

	Params *Params
}

// NotOwnedError is returned when maintenance cannot run because it is owned by another user.
type NotOwnedError struct {
	Owner string
}

func (e NotOwnedError) Error() string {
	return "maintenance must be run by designated user: " + e.Owner
}

// RunExclusive runs the provided callback if the maintenance is owned by local user and
// lock can be acquired. Lock is passed to the function, which ensures that every call to Run()
// is within the exclusive context.
func RunExclusive(ctx context.Context, rep MaintainableRepository, mode Mode, force bool, cb func(runParams RunParameters) error) error {
	p, err := GetParams(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to get maintenance params")
	}

	if !force {
		if myUsername := rep.Username() + "@" + rep.Hostname(); p.Owner != myUsername {
			return NotOwnedError{p.Owner}
		}
	}

	if mode == ModeAuto {
		mode, err = shouldRun(ctx, rep, p)
		if err != nil {
			return errors.Wrap(err, "unable to determine if maintenance is required")
		}
	}

	if mode == ModeNone {
		log(ctx).Debugf("not due for maintenance")
		return nil
	}

	runParams := RunParameters{rep, mode, p}

	// update schedule so that we don't run the maintenance again immediately if
	// this process crashes.
	if err = updateSchedule(ctx, runParams); err != nil {
		return errors.Wrap(err, "error updating maintenance schedule")
	}

	lockFile := rep.ConfigFilename() + ".mlock"
	log(ctx).Debugf("Acquiring maintenance lock in file %v", lockFile)

	// acquire local lock on a config file
	l := flock.New(lockFile)

	ok, err := l.TryLock()
	if err != nil {
		return errors.Wrap(err, "error acquiring maintenance lock")
	}

	if !ok {
		log(ctx).Debugf("maintenance is already in progress locally")
		return nil
	}

	defer l.Unlock() //nolint:errcheck

	log(ctx).Infof("Running %v maintenance...", runParams.Mode)
	defer log(ctx).Infof("Finished %v maintenance.", runParams.Mode)

	return cb(runParams)
}

// Run performs maintenance activities for a repository.
func Run(ctx context.Context, runParams RunParameters) error {
	switch runParams.Mode {
	case ModeQuick:
		return runQuickMaintenance(ctx, runParams)

	case ModeFull:
		return runFullMaintenance(ctx, runParams)

	default:
		return errors.Errorf("unknown mode %q", runParams.Mode)
	}
}

func runQuickMaintenance(ctx context.Context, runParams RunParameters) error {
	// find 'q' packs that are less than 80% full and rewrite contents in them into
	// new consolidated packs, orphaning old packs in the process.
	if err := ReportRun(ctx, runParams.rep, "quick-rewrite-contents", func() error {
		return RewriteContents(ctx, runParams.rep, &RewriteContentsOptions{
			ContentIDRange: content.AllPrefixedIDs,
			PackPrefix:     content.PackBlobIDPrefixSpecial,
			ShortPacks:     true,
		})
	}); err != nil {
		return errors.Wrap(err, "error rewriting metadata contents")
	}

	// delete orphaned 'q' packs after some time.
	if err := ReportRun(ctx, runParams.rep, "quick-delete-blobs", func() error {
		_, err := DeleteUnreferencedBlobs(ctx, runParams.rep, DeleteUnreferencedBlobsOptions{
			Prefix: content.PackBlobIDPrefixSpecial,
		})
		return err
	}); err != nil {
		return errors.Wrap(err, "error deleting unreferenced metadata blobs")
	}

	// consolidate many smaller indexes into fewer larger ones.
	if err := ReportRun(ctx, runParams.rep, "index-compaction", func() error {
		return IndexCompaction(ctx, runParams.rep)
	}); err != nil {
		return errors.Wrap(err, "error performing index compaction")
	}

	return nil
}

func runFullMaintenance(ctx context.Context, runParams RunParameters) error {
	s, err := GetSchedule(ctx, runParams.rep)
	if err != nil {
		return errors.Wrap(err, "unable to get schedule")
	}

	if safeDropTime := findSafeDropTime(s.Runs["snapshot-gc"]); !safeDropTime.IsZero() {
		log(ctx).Infof("Found safe time to drop indexes: %v", safeDropTime)

		// rewrite indexes by dropping content entries that have been marked
		// as deleted for a long time
		if err := ReportRun(ctx, runParams.rep, "full-drop-deleted-content", func() error {
			return DropDeletedContents(ctx, runParams.rep, safeDropTime)
		}); err != nil {
			return errors.Wrap(err, "error dropping deleted contents")
		}
	} else {
		log(ctx).Infof("Not enough time has passed since previous successful Snapshot GC. Will try again next time.")
	}

	// find packs that are less than 80% full and rewrite contents in them into
	// new consolidated packs, orphaning old packs in the process.
	if err := ReportRun(ctx, runParams.rep, "full-rewrite-contents", func() error {
		return RewriteContents(ctx, runParams.rep, &RewriteContentsOptions{
			ContentIDRange: content.AllIDs,
			ShortPacks:     true,
		})
	}); err != nil {
		return errors.Wrap(err, "error rewriting contents in short packs")
	}

	// delete orphaned packs after some time.
	if err := ReportRun(ctx, runParams.rep, "full-delete-blobs", func() error {
		_, err := DeleteUnreferencedBlobs(ctx, runParams.rep, DeleteUnreferencedBlobsOptions{})
		return err
	}); err != nil {
		return errors.Wrap(err, "error deleting unreferenced blobs")
	}

	return nil
}

// findSafeDropTime returns the latest timestamp for which it is safe to drop content entries
// deleted before that time, because at least two successful GC cycles have completed
// and minimum required time between the GCs has passed.
//
// The worst possible case we needf to handle is:
//
// Step #1 - race between GC and snapshot creation:
//
//  - 'snapshot gc' runs and marks unreachable contents as deleted
//  - 'snapshot create' runs at approximately the same time and creates manifest
//    which makes some contents live again.
//
// As a result of this race, GC has marked some entries as incorrectly deleted, but we
// can still return them since they are not dropped from the index.
//
// Step #2 - fix incorrectly deleted contents
//
//  - subsequent 'snapshot gc' runs and undeletes contents incorrectly
//    marked as deleted in Step 1.
//
// After Step 2 completes, we know for sure that all contents deleted before Step #1 has started
// are safe to drop from the index because Step #2 has fixed them, as long as all snapshots that
// were racing with snapshot GC in step #1 have flushed pending writes, hence the
// safetyMarginBetweenSnapshotGC.
func findSafeDropTime(runs []RunInfo) time.Time {
	var successfulRuns []RunInfo

	for _, r := range runs {
		if r.Success {
			successfulRuns = append(successfulRuns, r)
		}
	}

	if len(successfulRuns) <= 1 {
		return time.Time{}
	}

	// sort so that successfulRuns[0] is the latest
	sort.Slice(successfulRuns, func(i, j int) bool {
		return successfulRuns[i].Start.After(successfulRuns[j].Start)
	})

	// Look for previous successful run such that the time between GCs exceeds the safety margin.
	for _, r := range successfulRuns[1:] {
		diff := -r.End.Sub(successfulRuns[0].Start)
		if diff > safetyMarginBetweenSnapshotGC {
			return r.Start.Add(extraSafetyMarginBeforeDroppingContentFromIndex)
		}
	}

	return time.Time{}
}
