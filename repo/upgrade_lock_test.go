package repo_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

func TestFormatUpgradeDuringOngoingWriteSessions(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, content.FormatVersion1)

	rep := env.Repository // read-only

	lw := rep.(repo.RepositoryWriter)

	// w1, w2, w3 are indepdendent sessions.
	_, w1, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "writer1"})
	require.NoError(t, err)

	defer w1.Close(ctx)

	_, w2, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "writer2"})
	require.NoError(t, err)

	defer w2.Close(ctx)

	_, w3, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "writer3"})
	require.NoError(t, err)

	defer w3.Close(ctx)

	o1Data := []byte{1, 2, 3}
	o2Data := []byte{2, 3, 4}
	o3Data := []byte{3, 4, 5}
	o4Data := []byte{4, 5, 6}

	writeObject(ctx, t, w1, o1Data, "o1")
	writeObject(ctx, t, w2, o2Data, "o2")
	writeObject(ctx, t, w3, o3Data, "o3")
	writeObject(ctx, t, lw, o4Data, "o4")

	l := content.NewUpgradeLock(env.Repository.Time(), "upgrade-owner", 0, repo.DefaultFormatBlobCacheDuration,
		repo.DefaultFormatBlobCacheDuration/2, repo.DefaultFormatBlobCacheDuration/3,
		env.RepositoryWriter.ContentManager().ContentFormat().MutableParameters.Version)

	_, err = env.RepositoryWriter.SetUpgradeLockIntent(ctx, *l)
	require.NoError(t, err)

	// ongoing writes should get interrupted
	require.ErrorIs(t, w1.Flush(ctx), repo.ErrRepositoryUnavailableDueToUpgrageInProgress)
	require.ErrorIs(t, w2.Flush(ctx), repo.ErrRepositoryUnavailableDueToUpgrageInProgress)
	require.ErrorIs(t, w3.Flush(ctx), repo.ErrRepositoryUnavailableDueToUpgrageInProgress)
	require.ErrorIs(t, lw.Flush(ctx), repo.ErrRepositoryUnavailableDueToUpgrageInProgress)
}
