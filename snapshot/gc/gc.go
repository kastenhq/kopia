// Package gc implements garbage collection of contents that are no longer referenced through snapshots.
package gc

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var log = kopialogging.Logger("kopia/snapshot/gc")

func oidOf(entry fs.Entry) object.ID {
	return entry.(object.HasObjectID).ObjectID()
}

func findInUseContentIDs(ctx context.Context, rep *repo.Repository, used *sync.Map) error {
	ids, err := snapshot.ListSnapshotManifests(ctx, rep, nil)
	if err != nil {
		return errors.Wrap(err, "unable to list snapshot manifest IDs")
	}

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
		contentIDs, err := rep.Objects.VerifyObject(ctx, oid)

		if err != nil {
			return errors.Wrapf(err, "error verifying %v", oid)
		}

		for _, cid := range contentIDs {
			used.Store(cid, nil)
		}

		return nil
	}

	log.Info("looking for active contents")

	if err := w.Run(ctx); err != nil {
		return errors.Wrap(err, "error walking snapshot tree")
	}

	return nil
}

// Run performs garbage collection on all the snapshots in the repository.
// nolint:gocognit
func Run(ctx context.Context, rep *repo.Repository, minContentAge time.Duration, gcDelete bool) (*Stats, error) {
	start := time.Now()

	st := &Stats{}

	var used sync.Map

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := findInUseContentIDs(ctx, rep, &used); err != nil {
		return st, errors.Wrap(err, "unable to find in-use content ID")
	}

	finishFindUsed := time.Now()
	st.FindInUseDuration = finishFindUsed.Sub(start)

	log.Info("looking for unreferenced contents")

	if err := rep.Content.IterateContents(content.IterateOptions{}, func(ci content.Info) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		if manifest.ContentPrefix == ci.ID.Prefix() {
			st.System().Add(ci.Length)
			return nil
		}

		if _, ok := used.Load(ci.ID); !ok {
			if time.Since(ci.Timestamp()) < minContentAge {
				log.Debugf("recent unreferenced content %v (%v bytes, modified %v)", ci.ID, ci.Length, ci.Timestamp())
				st.TooRecent().Add(ci.Length)
				return nil
			}
			log.Debugf("unreferenced %v (%v bytes, modified %v)", ci.ID, ci.Length, ci.Timestamp())
			cnt, totalSize := st.Unused().Add(ci.Length)
			if gcDelete {
				if err := rep.Content.DeleteContent(ci.ID); err != nil {
					return errors.Wrap(err, "error deleting content")
				}
			}

			if cnt%100000 == 0 {
				log.Infof("... found %v unused contents so far (%v bytes)", cnt, units.BytesStringBase2(totalSize))
				if gcDelete {
					if err := rep.Flush(ctx); err != nil {
						return errors.Wrap(err, "flush error")
					}
				}
			}
		} else {
			st.InUse().Add(ci.Length)
		}
		return nil
	}); err != nil {
		return st, errors.Wrap(err, "error iterating contents")
	}

	st.FindUnusedDuration = time.Since(finishFindUsed)

	if count, _ := st.Unused().Approximate(); count > 0 && !gcDelete {
		return st, errors.Errorf("not deleting because '--delete' flag was not set")
	}

	return st, nil
}
