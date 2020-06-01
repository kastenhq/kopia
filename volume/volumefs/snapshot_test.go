package volumefs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestSnapshotAnalysis(t *testing.T) {
	assert := assert.New(t)

	expSA := SnapshotAnalysis{
		BlockSizeBytes:   1000,
		Bytes:            20000,
		NumBlocks:        1000,
		NumDirs:          100,
		ChainLength:      2,
		ChainedBytes:     80000,
		ChainedNumBlocks: 2000,
		ChainedNumDirs:   200,
	}

	curStats := snapshot.Stats{
		TotalDirectoryCount: expSA.NumDirs + expSA.ChainedNumDirs,
		TotalFileCount:      expSA.NumBlocks + expSA.ChainedNumBlocks,
		TotalFileSize:       expSA.Bytes + expSA.ChainedBytes,

		ExcludedFileCount:     expSA.ChainedNumBlocks,
		ExcludedTotalFileSize: expSA.ChainedBytes,
		ExcludedDirCount:      expSA.ChainedNumDirs,

		CachedFiles:    int32(expSA.BlockSizeBytes),
		NonCachedFiles: int32(expSA.ChainLength),
	}
	curSM := &snapshot.Manifest{
		Stats: curStats,
	}
	s := Snapshot{
		Current: curSM,
	}

	assert.Equal(expSA, s.Analyze())
	assert.Equal(SnapshotAnalysis{}, (&Snapshot{}).Analyze())
}

// nolint:gocritic
func TestInitFromSnapshot(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	f := th.fs()
	f.logger = log(ctx)
	assert.NotNil(f.sp)

	manifests := []*snapshot.Manifest{
		{RootEntry: &snapshot.DirEntry{ObjectID: "k2313ef907f3b250b331aed988802e4c5", Type: snapshot.EntryTypeDirectory}},
		{RootEntry: &snapshot.DirEntry{ObjectID: "k72d04e1f67ac38946b29c146fbea44fc", Type: snapshot.EntryTypeDirectory}},
		{RootEntry: &snapshot.DirEntry{ObjectID: "kf9fb0e450bba821a1c35585d48eaff04", Type: snapshot.EntryTypeFile}}, // not-a-dir
	}

	expMD, _, expEntries := generateTestMetaData()

	for _, tc := range []string{
		"invalid-snapid", "list error", "not found", "not-a-dir", "metadata error", "initialized",
	} {
		t.Logf("Case: %s", tc)

		tsp := &testSnapshotProcessor{}
		tsp.retLsM = manifests
		f.sp = tsp

		var expError error

		oid := string(manifests[1].RootObjectID())

		switch tc {
		case "invalid-snapid":
			oid = "not-a-snap-id"
			expError = fmt.Errorf("invalid contentID suffix")
		case "list error":
			expError = ErrInternalError
			tsp.retLsM = nil
			tsp.retLsE = expError
		case "not found":
			oid = "k785f33b8bcccffa4aac1dabb89cf5a71"
			expError = ErrSnapshotNotFound
		case "not-a-dir":
			oid = "kf9fb0e450bba821a1c35585d48eaff04"
			expError = ErrInvalidSnapshot
		case "metadata error":
			expError = ErrOutOfRange
			tsp.retSrEntry = &testDirEntry{retReadDirErr: expError}
		case "initialized":
			tsp.retSrEntry = &testDirEntry{retReadDirE: expEntries}
		}

		m, de, md, err := f.initFromSnapshot(ctx, oid)

		if expError == nil {
			assert.NoError(err)
			assert.NotNil(m)
			assert.NotNil(de)
			assert.EqualValues(oid, m.RootObjectID())

			if tc == "initialized" {
				assert.Equal(expMD, md)
			}
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
			assert.Nil(m)
		}
	}
}

// nolint:gocritic
func TestCommitSnapshot(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	f := th.fs()
	f.logger = log(ctx)
	f.VolumeID = "VolumeID"
	f.VolumeSnapshotID = "VolumeSnapshotID"

	prevManifest := &snapshot.Manifest{
		RootEntry: &snapshot.DirEntry{
			ObjectID:   "k2313ef907f3b250b331aed988802e4c5",
			Type:       snapshot.EntryTypeDirectory,
			DirSummary: &fs.DirectorySummary{},
		},
		Stats: snapshot.Stats{
			TotalDirectoryCount: 1,
			TotalFileCount:      1,
			TotalFileSize:       1,
			CachedFiles:         int32(f.blockSzB),
		},
	}
	summary := &fs.DirectorySummary{
		TotalFileSize:  100,
		TotalFileCount: 10,
		TotalDirCount:  2,
	}

	for _, tc := range []string{
		"write root error", "save snapshot error", "repo flush error", "committed no prev", "committed with prev",
	} {
		t.Logf("Case: %s", tc)

		tU := &testUploader{}
		f.up = tU
		tSP := &testSnapshotProcessor{}
		f.sp = tSP
		tRepo := &testRepo{}
		f.repo = tRepo

		rootDir := &dirMeta{name: "/", summary: summary, oid: "root-dir-oid"}
		expMan := &snapshot.Manifest{
			Source:      f.SourceInfo(),
			Description: "volume:VolumeID:VolumeSnapshotID",
			StartTime:   f.epoch,
			Stats: snapshot.Stats{
				TotalDirectoryCount: 2,
				TotalFileCount:      10,
				TotalFileSize:       100,
				CachedFiles:         int32(f.blockSzB),
			},
			RootEntry: &snapshot.DirEntry{
				Name:       "/",
				Type:       snapshot.EntryTypeDirectory,
				ObjectID:   rootDir.oid,
				DirSummary: summary,
			},
		}

		var expError error

		dm := rootDir
		psm := prevManifest

		switch tc {
		case "write root error":
			expError = ErrOutOfRange
			tU.retWriteDirE = expError
		case "save snapshot error":
			expError = ErrInvalidSnapshot
			tSP.retSsE = expError
		case "repo flush error":
			expError = ErrInternalError
			tRepo.retFE = expError
		case "committed no prev":
			psm = nil
		case "committed with prev":
			expMan.Stats.ExcludedDirCount = psm.Stats.TotalDirectoryCount
			expMan.Stats.ExcludedFileCount = psm.Stats.TotalFileCount
			expMan.Stats.ExcludedTotalFileSize = psm.Stats.TotalFileSize
			expMan.Stats.NonCachedFiles = psm.Stats.NonCachedFiles + 1
		}

		tB := time.Now()
		man, err := f.commitSnapshot(ctx, dm, psm)
		tA := time.Now()

		if expError == nil {
			assert.NoError(err)
			assert.NotNil(man)
			assert.True(man.EndTime.After(tB))
			assert.True(man.EndTime.Before(tA))
			man.EndTime = expMan.EndTime
			assert.Equal(expMan, man)
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
		}
	}
}

// nolint:wsl,gocritic
func TestLinkPreviousSnapshot(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	manifests := []*snapshot.Manifest{
		{RootEntry: &snapshot.DirEntry{ObjectID: "k2313ef907f3b250b331aed988802e4c5", Type: snapshot.EntryTypeDirectory, DirSummary: &fs.DirectorySummary{}}},
	}
	expMD, _, expEntries := generateTestMetaData()

	for _, tc := range []string{
		"previous snapshot not found", "volSnapID mismatch", "linked",
	} {
		t.Logf("Case: %s", tc)

		tsp := &testSnapshotProcessor{}
		tsp.retLsM = manifests
		tsp.retSrEntry = &testDirEntry{retReadDirE: expEntries}

		f := th.fs()
		f.logger = log(ctx)
		f.sp = tsp
		f.setMetadata(metadata{}) // clear all

		var expError error
		oid := string(manifests[0].RootObjectID())
		prevVolSnapshotID := expMD.VolSnapID

		switch tc {
		case "previous snapshot not found":
			oid = "k785f33b8bcccffa4aac1dabb89cf5a71"
			expError = ErrSnapshotNotFound
		case "volSnapID mismatch":
			prevVolSnapshotID += "foo"
			expError = ErrInvalidSnapshot
		}

		dm, man, err := f.linkPreviousSnapshot(ctx, oid, prevVolSnapshotID)

		if expError == nil {
			assert.NoError(err)
			assert.Equal(manifests[0], man)
			expDM := &dirMeta{
				name:    previousSnapshotDirName,
				oid:     manifests[0].RootEntry.ObjectID,
				summary: manifests[0].RootEntry.DirSummary,
			}
			assert.Equal(expDM, dm)
			assert.Equal(prevVolSnapshotID, f.prevVolumeSnapshotID)
			assert.Equal(expMD.BlockSzB, f.blockSzB)
			assert.Equal(expMD.DirSz, f.dirSz)
			assert.Equal(expMD.Depth, f.depth)
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
			assert.Nil(dm)
			assert.Nil(man)
		}
	}
}

type testSnapshotProcessor struct {
	inLsR  repo.Repository
	inLsS  snapshot.SourceInfo
	retLsM []*snapshot.Manifest
	retLsE error

	retSrEntry fs.Entry

	inSsR   repo.Repository
	inSsM   *snapshot.Manifest
	retSsID manifest.ID
	retSsE  error
}

var _ snapshotProcessor = (*testSnapshotProcessor)(nil)

// nolint:gocritic
func (tsp *testSnapshotProcessor) ListSnapshots(ctx context.Context, repo repo.Repository, si snapshot.SourceInfo) ([]*snapshot.Manifest, error) {
	tsp.inLsR = repo
	tsp.inLsS = si

	if tsp.retLsE != nil {
		sh := &snapshotHelper{} // call the real thing to check that it works

		m, err := sh.ListSnapshots(ctx, repo, snapshot.SourceInfo{})

		if err != nil || len(m) == 0 { // appears to never fail!
			return nil, tsp.retLsE
		}

		panic("failed to fail")
	}

	return tsp.retLsM, tsp.retLsE
}

// nolint:gocritic
func (tsp *testSnapshotProcessor) SnapshotRoot(rep repo.Repository, man *snapshot.Manifest) (fs.Entry, error) {
	if tsp.retSrEntry != nil {
		return tsp.retSrEntry, nil
	}

	sh := &snapshotHelper{} // call the real thing

	return sh.SnapshotRoot(rep, man)
}

// nolint:gocritic
func (tsp *testSnapshotProcessor) SaveSnapshot(ctx context.Context, rep repo.Repository, man *snapshot.Manifest) (manifest.ID, error) {
	tsp.inSsR = rep
	tsp.inSsM = man

	if tsp.retSsE != nil {
		sh := &snapshotHelper{}        // call the real thing to check that it works
		sh.SaveSnapshot(ctx, rep, man) // does not fail

		return "", tsp.retSsE
	}

	return tsp.retSsID, tsp.retSsE
}
