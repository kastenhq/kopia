package gc

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/content"
)

const configFileName = "kopia.config"

func TestMarkContentsDeleted(t *testing.T) {
	const contentCount = 10

	ctx := context.Background()
	check := require.New(t)
	th := createAndOpenRepo(t)

	defer th.Close(t)

	// setup: create contents
	cids := writeContents(ctx, t, th.repo.Content, contentCount)

	check.NoError(th.repo.Flush(ctx))

	// Ensure that deleted contents have a newer time stamp
	time.Sleep(time.Second)

	// delete half the contents
	snaps := nManifestIDs(t, 3)

	toDelete := cids[0:5]
	err := markContentsDeleted(ctx, th.repo, snaps, toDelete)
	check.NoError(err)

	// check: is there a GC manifest?
	gcMans, err := th.repo.Manifests.Find(ctx, markManifestLabels())
	check.NoError(err)
	check.Len(gcMans, 1, "expected a single GC mark manifest")

	var man MarkManifest
	err = th.repo.Manifests.Get(ctx, gcMans[0].ID, &man)
	check.NoError(err)

	// check: is there a content with GC mark details?
	var gcContents []content.ID

	opts := content.IterateOptions{Prefix: ContentPrefix}
	err = th.repo.Content.IterateContents(opts, func(i content.Info) error {
		gcContents = append(gcContents, i.ID)
		return nil
	})

	check.NoError(err)

	check.Len(gcContents, 1, "there must be a single GC details content")

	check.Equal(man.DetailsID, gcContents[0], "ID of the GC details content must match the mark manifest DetailsID field")

	// deserialize mark details
	b, err := th.repo.Content.GetContent(ctx, man.DetailsID)
	check.NoError(err)
	check.NotNil(b)

	var markDetails MarkDetails

	check.NoError(json.Unmarshal(b, &markDetails))

	check.Equal(snaps, markDetails.Snapshots, "markDetails.Snapshots must be the same as 'snaps'")

	check.Equal(toDelete, markDetails.MarkedContent, "MarkedContent must have the ids of the removed contents")

	// verify content not in `toDelete` was not deleted
	verifyContentDeletedState(ctx, t, th.repo.Content, cids[5:], false)

	// verify content in 'toDelete' was marked as deleted
	verifyContentDeletedState(ctx, t, th.repo.Content, toDelete, true)
}

func TestSortContentIDs(t *testing.T) {
	cids := []content.ID{"x", "c", "b", "a"}
	content.SortIDs(cids)

	for i, id := range cids[1:] {
		prev, current := string(cids[i]), string(id)
		require.LessOrEqual(t, prev, current, "content IDs not sorted")
	}
}
