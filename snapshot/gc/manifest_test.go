package gc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	assert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
)

type testManifestHarness struct {
	stateDir string
	repo     *repo.Repository
}

func TestMarkContentsDeleted(t *testing.T) {
	const contentCount = 10

	ctx := context.Background()
	cids := make([]content.ID, 0, contentCount)
	check := require.New(t)
	th := newManifestTestHarness(t)

	defer th.Close(t)

	// setup: create contents
	for i := 0; i < contentCount; i++ {
		b := seededRandomData(t, i, 10)
		id, err := th.repo.Content.WriteContent(ctx, b, "")
		check.NoError(err)

		cids = append(cids, id)
	}

	check.NoError(th.repo.Flush(ctx))

	// Ensure that deleted contents have a newer time stamp
	time.Sleep(time.Second)

	// delete half the contents
	snaps := []manifest.ID{
		manifest.ID(makeRandomHexString(t, 32)),
		manifest.ID(makeRandomHexString(t, 32)),
		manifest.ID(makeRandomHexString(t, 32)),
	}

	toDelete := cids[0:5]
	err := markContentsDeleted(ctx, th.repo, snaps, toDelete)
	check.NoError(err)

	// checks:
	// is there a GC manifest?
	gcMans, err := th.repo.Manifests.Find(ctx, markManifestLabels())
	check.NoError(err)
	check.Len(gcMans, 1, "Unexpected number of GC manifests")

	var man MarkManifest
	err = th.repo.Manifests.Get(ctx, gcMans[0].ID, &man)
	check.NoError(err)

	var gcContents []content.ID

	opts := content.IterateOptions{Prefix: ContentPrefix}
	err = th.repo.Content.IterateContents(opts, func(i content.Info) error {
		gcContents = append(gcContents, i.ID)
		return nil
	})

	check.NoError(err)

	// there must be a single gc details content
	check.Len(gcContents, 1, "unexpected number of GC mark details contents")

	// the id of the gc details content must match the DetailsID in MarkManifest
	check.Equal(man.DetailsID, gcContents[0], "GC mark detail content ids do not match")

	// deserialize mark details
	b, err := th.repo.Content.GetContent(ctx, man.DetailsID)
	check.NoError(err)
	check.NotNil(b)

	var markDetails MarkDetails

	check.NoError(json.Unmarshal(b, &markDetails))

	// the given snapshot ids must be included in mark details
	check.Equal(snaps, markDetails.Snapshots, "List of live snapshots does not match")

	// MarkedContent must have the ids of the removed contents
	check.Equal(toDelete, markDetails.MarkedContent, "mark-delete content does not match")

	// Verify content not in `toDelete` was not deleted
	for _, id := range cids[5:] {
		info, err := th.repo.Content.ContentInfo(ctx, id)
		check.NoError(err)
		check.False(info.Deleted, info)
	}

	// The contents have been marked as deleted => get content info and
	// verify delete bit
	for _, id := range toDelete {
		info, err := th.repo.Content.ContentInfo(ctx, id)
		check.NoError(err)
		check.True(info.Deleted, info)
	}
}

func TestSortContentIDs(t *testing.T) {
	cids := []content.ID{"x", "c", "b", "a"}
	content.SortIDs(cids)

	for i, id := range cids[1:] {
		prev, current := string(cids[i]), string(id)
		require.LessOrEqual(t, prev, current, "content IDs not sorted")
	}
}

func newManifestTestHarness(t *testing.T) *testManifestHarness {
	const masterPassword = "foo"

	t.Helper()

	ctx := context.Background()

	check := require.New(t)

	stateDir, err := ioutil.TempDir("", "manifest-test")
	check.NoError(err, "cannot create temp directory")
	t.Log("repo dir:", stateDir)

	repoDir := filepath.Join(stateDir, "repo")
	check.NoError(os.MkdirAll(repoDir, 0700), "cannot create repository directory")

	storage, err := filesystem.New(context.Background(), &filesystem.Options{
		Path: repoDir,
	})
	check.NoError(err, "cannot create storage directory")

	err = repo.Initialize(ctx, storage, &repo.NewRepositoryOptions{}, masterPassword)
	check.NoError(err, "cannot create repository")

	configFile := filepath.Join(stateDir, "kopia.config")
	err = repo.Connect(ctx, configFile, storage, masterPassword, repo.ConnectOptions{
		CachingOptions: content.CachingOptions{CacheDirectory: filepath.Join(stateDir, "cache")},
	})
	check.NoError(err, "unable to connect to repository")

	rep, err := repo.Open(ctx, configFile, masterPassword, &repo.Options{})
	check.NoError(err, "unable to open repository")

	return &testManifestHarness{
		stateDir: stateDir,
		repo:     rep,
	}
}

func (h *testManifestHarness) Close(t *testing.T) {
	t.Helper()

	ctx := context.Background()

	if h.repo != nil {
		assert.NoError(t, h.repo.Close(ctx), "unable to close repository")
	}

	if h.stateDir != "" {
		assert.NoError(t, os.RemoveAll(h.stateDir), "unable to cleanup test state directory")
	}
}

func seededRandomData(t *testing.T, seed, length int) []byte {
	t.Helper()

	b := make([]byte, length)
	rnd := rand.New(rand.NewSource(int64(seed)))
	rnd.Read(b)
	got, err := io.ReadFull(rnd, b)
	require.NoError(t, err)
	require.Equal(t, length, got)

	return b
}

func makeRandomHexString(t *testing.T, length int) string {
	t.Helper()

	b := make([]byte, (length-1)/2+1)
	_, err := rand.Read(b) // nolint:gosec

	require.NoError(t, err)

	return hex.EncodeToString(b)
}
