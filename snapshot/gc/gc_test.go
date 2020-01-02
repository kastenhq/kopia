package gc

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
)

func Test_deleteUnused(t *testing.T) {
	tests := []struct {
		name         string
		snapCount    uint
		contentCount int
		deleteCount  int
		batchSize    int
	}{
		{
			name:         "with no snap ids",
			snapCount:    0,
			contentCount: 4,
			deleteCount:  3,
			batchSize:    5,
		},

		{
			name:         "with single snap id",
			snapCount:    1,
			contentCount: 4,
			deleteCount:  3,
			batchSize:    5,
		},

		{
			name:         "0 contents to delete",
			snapCount:    3,
			contentCount: 4,
			deleteCount:  0,
			batchSize:    5,
		},

		{
			name:         "delete some of the content",
			snapCount:    3,
			contentCount: 8,
			deleteCount:  6,
			batchSize:    10,
		},

		{
			name:         "delete all the content",
			snapCount:    3,
			contentCount: 9,
			deleteCount:  9,
			batchSize:    10,
		},

		{
			name:         "delete count same as batch size",
			snapCount:    3,
			contentCount: 12,
			deleteCount:  10,
			batchSize:    10,
		},

		{
			name:         "delete count larger than batch size",
			snapCount:    3,
			contentCount: 21,
			deleteCount:  19,
			batchSize:    10,
		},

		{
			name:         "delete multiple batches",
			snapCount:    3,
			contentCount: 155,
			deleteCount:  147,
			batchSize:    50,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			check := assert.New(t)
			r := createAndOpenRepo(t)
			defer r.Close(t)

			cids := writeContents(ctx, t, r.repo.Content, tt.contentCount)
			snaps := nManifestIDs(t, tt.snapCount)
			toDeleteCh := make(chan content.ID)

			go func() {
				defer close(toDeleteCh)
				for _, id := range cids[:tt.deleteCount] {
					toDeleteCh <- id
				}
			}()

			check.NoError(r.repo.Flush(ctx))
			// Ensure that deleted contents have a newer time stamp
			time.Sleep(time.Second)

			err := deleteUnused(ctx, r.repo, snaps, toDeleteCh, tt.batchSize)
			check.NoError(err, "unexpected error")

			// verify that all the contents sent through the channel were
			// deleted and nothing else
			verifyContentDeletedState(ctx, t, r.repo.Content, cids[:tt.deleteCount], true)
			verifyContentDeletedState(ctx, t, r.repo.Content, cids[tt.deleteCount:], false)

			// check: are there GC manifests?
			gcMans, err := r.repo.Manifests.Find(ctx, markManifestLabels())
			check.NoError(err)
			check.Len(gcMans, (tt.deleteCount+tt.batchSize-1)/tt.batchSize, "expected a single GC mark manifest")

			var foundGCContentCount int

			opts := content.IterateOptions{Prefix: ContentPrefix}
			err = r.repo.Content.IterateContents(ctx, opts, func(i content.Info) error {
				foundGCContentCount++
				return nil
			})

			check.NoError(err)

			check.Equal(len(gcMans), foundGCContentCount, "GC details content count does not match number of GC mark manifests")
		})
	}
}

type testRepo struct {
	stateDir string
	repo     *repo.Repository
}

func createAndOpenRepo(t *testing.T) testRepo {
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

	configFile := filepath.Join(stateDir, configFileName)
	connOpts := repo.ConnectOptions{
		CachingOptions: content.CachingOptions{
			CacheDirectory: filepath.Join(stateDir, "cache"),
		},
	}
	err = repo.Connect(ctx, configFile, storage, masterPassword, &connOpts)

	check.NoError(err, "unable to connect to repository")

	rep, err := repo.Open(ctx, configFile, masterPassword, &repo.Options{})
	check.NoError(err, "unable to open repository")

	return testRepo{
		stateDir: stateDir,
		repo:     rep,
	}
}

func (r *testRepo) Close(t *testing.T) {
	t.Helper()

	ctx := context.Background()

	if r.repo != nil {
		assert.NoError(t, r.repo.Close(ctx), "unable to close repository")
	}

	if r.stateDir != "" {
		configFile := filepath.Join(r.stateDir, configFileName)
		err := repo.Disconnect(ctx, configFile)

		require.NoError(t, err, "failed to disconnect repo with config file: ", configFile)
		assert.NoError(t, os.RemoveAll(r.stateDir), "unable to cleanup test state directory")
	}
}

func nManifestIDs(t *testing.T, n uint) []manifest.ID {
	ids := make([]manifest.ID, n)

	for i := range ids {
		ids[i] = manifest.ID(makeRandomHexString(t, 32))
	}

	return ids
}

func makeRandomHexString(t *testing.T, length int) string {
	t.Helper()

	b := make([]byte, (length-1)/2+1)
	_, err := rand.Read(b) // nolint:gosec

	require.NoError(t, err)

	return hex.EncodeToString(b)
}

func verifyContentDeletedState(ctx context.Context, t *testing.T, cm *content.Manager, cids []content.ID, wantDeleted bool) {
	t.Helper()

	for _, id := range cids {
		info, err := cm.ContentInfo(ctx, id)
		assert.NoError(t, err)
		assert.Equal(t, wantDeleted, info.Deleted, "content deleted state does not match")
	}
}

func writeContents(ctx context.Context, t *testing.T, cm *content.Manager, n int) []content.ID {
	t.Helper()

	b := make([]byte, 8)
	ids := make([]content.ID, 0, n)

	for i := rand.Uint64(); n > 0; n-- {
		binary.BigEndian.PutUint64(b, i)
		i++

		id, err := cm.WriteContent(ctx, b, "")
		assert.NoError(t, err, "Failed to write content")

		ids = append(ids, id)
	}

	return ids
}
