package gc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
)

// ContentPrefix is the prefix used for GC related content
const ContentPrefix content.ID = "g"

const markManifestType = "gc-mark"

// MarkManifest represents information about a single GC mark run
type MarkManifest struct {
	ID manifest.ID `json:"-"`

	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`

	DetailsID content.ID `json:"detailsID"`
}

// MarkDetails contains metadata about a GC mark phase, which is later used
// by the GC delete phase
type MarkDetails struct {
	// Deleted snapshots encountered by a GC run. This is not strictly needed
	// in the GC delete phase. Currently used for debugging, may be removed.
	DeletedSnapshots []manifest.ID `json:"deletedSnapshots,omitempty"`
	// Snapshots that were visible to a GC run.
	Snapshots []manifest.ID `json:"liveSnapshots,omitempty"`
	// Set of contents marked for deletion by the GC mark phase.
	MarkedContent []content.ID `json:"markedContent,omitempty"`
}

func markContentsDeleted(ctx context.Context, rep *repo.Repository, snaps []manifest.ID, toDelete []content.ID) error {
	if err := markContentAndCreateManifest(ctx, rep, snaps, toDelete); err != nil {
		return err
	}

	return rep.Content.Flush(ctx)
}

func markContentAndCreateManifest(ctx context.Context, rep *repo.Repository, snaps []manifest.ID, toDelete []content.ID) error {
	// create mark details manifest, get back an id
	m := MarkManifest{
		StartTime: time.Now().UTC(),
	}

	did, err := writeMarkDetails(ctx, rep, snaps, toDelete)
	if err != nil {
		return err
	}

	m.DetailsID = did

	// disable flushing to ensure that the mark manifest and the deleted entries
	// are in a single index pack. May want to do this earlier to include the
	// details manifest
	rep.Content.DisableIndexFlush()
	defer rep.Content.EnableIndexFlush()

	// may want to have a batch call for this.
	for _, cid := range toDelete {
		if err2 := rep.Content.DeleteContent(cid); err2 != nil {
			return err2
		}
	}

	m.EndTime = time.Now().UTC()

	if _, err = rep.Manifests.Put(ctx, markManifestLabels(), m); err != nil {
		return err
	}

	return nil
}

func writeMarkDetails(ctx context.Context, rep *repo.Repository, snaps []manifest.ID, toDelete []content.ID) (content.ID, error) {
	content.SortIDs(toDelete)

	details := MarkDetails{
		Snapshots:     snaps,
		MarkedContent: toDelete,
	}

	b, err := json.Marshal(details)
	if err != nil {
		return "", nil
	}

	return rep.Content.WriteContent(ctx, b, ContentPrefix)
}

func markManifestLabels() map[string]string {
	return map[string]string{
		manifest.TypeLabelKey: markManifestType,
	}
}
