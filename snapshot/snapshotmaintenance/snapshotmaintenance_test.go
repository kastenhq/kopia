package snapshotmaintenance_test

import (
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

const (
	defaultPermissions = 0777
)

type testHarness struct {
	repotesting.Environment
	fakeTime  *faketime.TimeAdvance
	sourceDir *mockfs.Directory
}

func TestSnapshotGCSimple(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newTestHarness(t)

	require.NotNil(t, th)
	require.NotNil(t, th.sourceDir)
	th.sourceDir.AddDir("d1", defaultPermissions)
	th.sourceDir.AddFile("d1/f2", []byte{1, 2, 3, 4}, defaultPermissions)

	// Create and delete a snapshot of th.sourceDir dir, which contains 'd1'
	si := snapshot.SourceInfo{
		Host:     "host",
		UserName: "user",
		Path:     "/foo",
	}
	s1 := mustSnapshot(t, th.RepositoryWriter, th.sourceDir, si)

	t.Log("snap 1:", pretty.Sprint(s1))
	mustFlush(t, th.RepositoryWriter)

	// Delete snapshot
	err := th.RepositoryWriter.DeleteManifest(ctx, s1.ID)
	require.NoError(t, err)

	mustFlush(t, th.RepositoryWriter)

	// Advance time to force GC to mark as deleted the contents from the previous snapshot
	th.fakeTime.Advance(maintenance.DefaultParams().SnapshotGC.MinContentAge + time.Hour)

	err = snapshotmaintenance.Run(ctx, th.RepositoryWriter, maintenance.ModeFull, true)
	require.NoError(t, err)

	mustFlush(t, th.RepositoryWriter)

	s2 := mustSnapshot(t, th.RepositoryWriter, th.sourceDir, si)
	t.Log("snap 2:", pretty.Sprint(s2))
	mustFlush(t, th.RepositoryWriter)

	info, err := th.RepositoryWriter.ContentReader().ContentInfo(ctx, content.ID(s2.RootObjectID()))
	require.NoError(t, err)

	t.Log("root info:", pretty.Sprint(info))
}

// Test maintenance when a directory is deleted and then reused.
// Scenario / events:
// - create snapshot s1 on a directory d is created
// - delete s1
// - let enough time pass so the contents in s1 are eligible for GC mark/deletion
// - concurrently create a snapshot s2 on directory d while performing full
//   maintenance
// - Check full maintenance can be run afterwards
// - Verify contents.
func TestMaintenanceReuseDirManifest(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newTestHarness(t)

	require.NotNil(t, th)
	require.NotNil(t, th.sourceDir)

	d1 := th.sourceDir.AddDir("d1", defaultPermissions)
	d1.AddFile("f1", []byte{1, 2, 3, 4}, defaultPermissions)

	// Create and delete a snapshot of th.sourceDir dir, which contains 'd1'
	si := snapshot.SourceInfo{
		Host:     "host",
		UserName: "user",
		Path:     "/foo",
	}
	s1 := mustSnapshot(t, th.RepositoryWriter, th.sourceDir, si)

	t.Log("snap 1:", pretty.Sprint(s1))
	mustFlush(t, th.RepositoryWriter)

	// Delete snapshot
	err := th.RepositoryWriter.DeleteManifest(ctx, s1.ID)
	require.NoError(t, err)

	mustFlush(t, th.RepositoryWriter)

	// Advance time to force GC to mark as deleted the contents from the previous snapshot
	th.fakeTime.Advance(maintenance.DefaultParams().SnapshotGC.MinContentAge + time.Hour)

	r2 := th.openAnother(t)

	s2 := mustSnapshot(t, r2, th.sourceDir, si)
	t.Log("snap 2:", pretty.Sprint(s2))

	// interleaving snapshot and maintenance and delaying flushing as well to
	// create dangling references to contents that were in the previously
	// deleted snapshot and that are reused in this new snapshot.
	err = snapshotmaintenance.Run(ctx, th.RepositoryWriter, maintenance.ModeFull, true)
	require.NoError(t, err)

	info, err := r2.(repo.DirectRepository).ContentReader().ContentInfo(ctx, content.ID(s2.RootObjectID()))
	require.NoError(t, err)
	require.False(t, info.Deleted, "content must not be deleted")

	_, err = r2.VerifyObject(ctx, s2.RootObjectID())
	require.NoError(t, err)

	mustFlush(t, r2) // finish snapshot
	require.NoError(t, r2.Close(ctx))

	mustFlush(t, th.RepositoryWriter) // finish maintenance

	th.MustReopen(t)

	info, err = th.RepositoryWriter.ContentReader().ContentInfo(ctx, content.ID(s2.RootObjectID()))
	require.NoError(t, err)
	require.True(t, info.Deleted, "content must be deleted")

	_, err = th.RepositoryWriter.VerifyObject(ctx, s2.RootObjectID())
	require.NoError(t, err)

	// Run maintenance again
	th.fakeTime.Advance(maintenance.DefaultParams().SnapshotGC.MinContentAge + time.Hour)
	err = snapshotmaintenance.Run(ctx, th.RepositoryWriter, maintenance.ModeFull, true)
	require.NoError(t, err)
	mustFlush(t, th.RepositoryWriter)

	// Was the previous root undeleted
	info, err = th.RepositoryWriter.ContentReader().ContentInfo(ctx, content.ID(s2.RootObjectID()))
	require.NoError(t, err)
	require.False(t, info.Deleted, "content must not be deleted")

	_, err = th.RepositoryWriter.VerifyObject(ctx, s2.RootObjectID())
	require.NoError(t, err)

	t.Log("root info:", pretty.Sprint(info))
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()

	baseTime := time.Date(2020, 9, 10, 0, 0, 0, 0, time.UTC)
	th := &testHarness{
		fakeTime:  faketime.NewTimeAdvance(baseTime, time.Second),
		sourceDir: mockfs.NewDirectory(),
	}

	th.Environment.Setup(t, repotesting.Options{OpenOptions: th.fakeTimeOpenRepoOption})

	require.NotNil(t, th.RepositoryWriter)

	t.Cleanup(func() {
		th.Environment.Close(testlogging.Context(t), t)
	})

	return th
}

func (th *testHarness) fakeTimeOpenRepoOption(o *repo.Options) {
	o.TimeNowFunc = th.fakeTime.NowFunc()
}

func (th *testHarness) openAnother(t *testing.T) repo.RepositoryWriter {
	t.Helper()

	r := th.MustConnectOpenAnother(t, th.fakeTimeOpenRepoOption)
	ctx := testlogging.Context(t)

	t.Cleanup(func() {
		r.Close(ctx)
	})

	w, err := r.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	if err != nil {
		t.Fatal(err)
	}

	return w
}

func mustFlush(t *testing.T, r repo.RepositoryWriter) {
	t.Helper()
	require.NotNil(t, r, "nil repository")
	require.NoError(t, r.Flush(testlogging.Context(t)))
}

func mustSnapshot(t *testing.T, r repo.RepositoryWriter, source fs.Entry, si snapshot.SourceInfo) *snapshot.Manifest {
	t.Helper()

	s1, err := createSnapshot(testlogging.Context(t), r, source, si, "")
	require.NoError(t, err)
	require.NotNil(t, s1)

	return s1
}
