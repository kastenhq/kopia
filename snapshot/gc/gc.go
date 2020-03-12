// Package gc implements garbage collection of contents that are no longer referenced through snapshots.
package gc

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var log = logging.GetContextLoggerFunc("kopia/snapshot/gc")

func oidOf(entry fs.Entry) object.ID {
	return entry.(object.HasObjectID).ObjectID()
}

func findInUseContentIDs(ctx context.Context, rep repo.Repository, ids []manifest.ID, used *sync.Map) error {
	start := time.Now() // allow:no-inject-time
	manifests, err := snapshot.LoadSnapshots(ctx, rep, ids)
	if err != nil {
		return errors.Wrap(err, "unable to load manifest IDs")
	}

	w := snapshotfs.NewTreeWalker()
	w.EntryID = func(e fs.Entry) interface{} { return oidOf(e) }

	for _, m := range manifests {
		root, err := snapshotfs.SnapshotRoot(rep, m)
		if err != nil {
			return errors.Wrap(err, "unable to get snapshot root")
		}

		w.RootEntries = append(w.RootEntries, root)
	}

	w.ObjectCallback = func(entry fs.Entry) error {
		oid := oidOf(entry)
		contentIDs, err := rep.VerifyObject(ctx, oid)

		if err != nil {
			return errors.Wrapf(err, "error verifying %v", oid)
		}

		for _, cid := range contentIDs {
			used.Store(cid, nil)
		}

		return nil
	}

	log(ctx).Infof("looking for active contents")

	if err := w.Run(ctx); err != nil {
		return errors.Wrap(err, "error walking snapshot tree")
	}

	log(ctx).Infof("found in-use content in %s", time.Since(start)) // allow:no-inject-time

	return nil
}

// Run performs garbage collection on all the snapshots in the repository.
// nolint:gocognit
func Run(ctx context.Context, rep *repo.DirectRepository, minContentAge time.Duration, gcDelete bool) (Stats, error) {
	snapIDs, err := snapshot.ListSnapshotManifests(ctx, rep, nil)
	if err != nil {
		return Stats{}, errors.Wrap(err, "unable to list snapshot manifest IDs")
	}

	st, err := markUnusedContent(ctx, rep, snapIDs, minContentAge, gcDelete)
	if err != nil {
		return st, err
	}

	if st.UnusedCount > 0 && !gcDelete {
		return st, errors.Errorf("not deleting because '--delete' flag was not set")
	}

	return st, nil
}

func markUnusedContent(ctx context.Context, rep *repo.DirectRepository, snapIDs []manifest.ID, minContentAge time.Duration, gcDelete bool) (Stats, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var st Stats

	var used sync.Map

	if err := findInUseContentIDs(ctx, rep, snapIDs, &used); err != nil {
		return st, errors.Wrap(err, "unable to find in-use content ID")
	}

	var unused, inUse, system, tooRecent stats.CountSum

	log(ctx).Infof("looking for unreferenced contents")

	const toDeleteBufferLength = 10

	toDelete := make(chan content.ID, toDeleteBufferLength)
	errCh := make(chan error)

	go func() {
		defer close(errCh)
		defer close(toDelete)

		if err := rep.Content.IterateContents(ctx, content.IterateOptions{}, func(ci content.Info) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			if pr := ci.ID.Prefix(); manifest.ContentPrefix == pr || ContentPrefix == pr {
				system.Add(int64(ci.Length))
				return nil
			}

			if _, ok := used.Load(ci.ID); ok {
				inUse.Add(int64(ci.Length))
				return nil
			}

			if rep.Time().Sub(ci.Timestamp()) < minContentAge {
				log(ctx).Debugf("recent unreferenced content %v (%v bytes, modified %v)", ci.ID, ci.Length, ci.Timestamp())
				tooRecent.Add(int64(ci.Length))
				return nil
			}

			log(ctx).Debugf("unreferenced %v (%v bytes, modified %v)", ci.ID, ci.Length, ci.Timestamp())
			unused.Add(int64(ci.Length))

			if gcDelete {
				select {
				case toDelete <- ci.ID:
				case <-ctx.Done():
					return errors.Wrap(ctx.Err(), "canceled while buffering deleted content")
				}
			}

			return nil
		}); err != nil {
			errCh <- errors.Wrap(err, "error iterating contents")
		}
	}()

	const batchSize = 10000
	err := deleteUnused(ctx, rep, snapIDs, toDelete, batchSize)

	st.UnusedCount, st.UnusedBytes = unused.Approximate()
	st.InUseCount, st.InUseBytes = inUse.Approximate()
	st.SystemCount, st.SystemBytes = system.Approximate()
	st.TooRecentCount, st.TooRecentBytes = tooRecent.Approximate()

	if err != nil {
		return st, err
	}

	return st, <-errCh
}

func deleteUnused(ctx context.Context, rep *repo.DirectRepository, snaps []manifest.ID, toDelete <-chan content.ID, batchSize int) error {
	var cnt int

	ids := make([]content.ID, 0, batchSize)

	for id := range toDelete {
		if err := ctx.Err(); err != nil {
			return err
		}

		ids = append(ids, id)
		if len(ids) == batchSize {
			if err := markContentsDeleted(ctx, rep, snaps, ids); err != nil {
				return err
			}

			cnt += len(ids)
			ids = ids[:0]

			log(ctx).Infof("... unused contents found so far: %d", cnt)
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if len(ids) == 0 {
		return nil
	}

	return markContentsDeleted(ctx, rep, snaps, ids)
}
