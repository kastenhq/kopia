package volumefs

import (
	"context"
	"sync"
	"time"

	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/volume"
)

// Backup constants.
const (
	DefaultBackupConcurrency = 4
)

// BackupArgs contain arguments to the Backup method.
type BackupArgs struct {
	// The identifier of the previous snapshot if this is an incremental.
	PreviousSnapshotID string
	// The identifier of the previous volume snapshot if this is an incremental.
	PreviousVolumeSnapshotID string
	// TThe amount of concurrency during backup. 0 assigns a default value.
	BackupConcurrency int
}

// Validate checks the arguments for correctness.
func (a *BackupArgs) Validate() error {
	if (a.PreviousSnapshotID == "" && a.PreviousVolumeSnapshotID != "") ||
		(a.PreviousSnapshotID != "" && a.PreviousVolumeSnapshotID == "") ||
		a.BackupConcurrency < 0 {
		return ErrInvalidArgs
	}

	return nil
}

// Backup a volume.
// The volume manager must provide a BlockReader interface.
func (f *Filesystem) Backup(ctx context.Context, args BackupArgs) (*Snapshot, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}

	f.logger = log(ctx)
	f.epoch = time.Now()

	gbrArgs := volume.GetBlockReaderArgs{
		VolumeID:           f.VolumeID,
		SnapshotID:         f.VolumeSnapshotID,
		PreviousSnapshotID: args.PreviousVolumeSnapshotID,
		Profile:            f.VolumeAccessProfile,
	}

	br, err := f.VolumeManager.GetBlockReader(gbrArgs)
	if err != nil {
		return nil, err
	}

	var (
		prevRootDm *dirMeta
		curDm      *dirMeta
		rootDir    *dirMeta
		prevMan    *snapshot.Manifest
		curMan     *snapshot.Manifest
	)

	if args.PreviousSnapshotID != "" {
		prevRootDm, prevMan, err = f.bp.linkPreviousSnapshot(ctx, args.PreviousSnapshotID, args.PreviousVolumeSnapshotID)
		if err != nil {
			return nil, err
		}
	}

	numWorkers := DefaultBackupConcurrency
	if args.BackupConcurrency > 0 {
		numWorkers = args.BackupConcurrency
	}

	if curDm, err = f.bp.backupBlocks(ctx, br, numWorkers); err == nil {
		if rootDir, err = f.bp.createRoot(ctx, curDm, prevRootDm); err == nil {
			curMan, err = f.bp.commitSnapshot(ctx, rootDir, prevMan)
		}
	}

	if err != nil {
		return nil, err
	}

	return &Snapshot{Current: curMan}, nil
}

// backupProcessor aids in unit testing
type backupProcessor interface {
	backupBlocks(ctx context.Context, br volume.BlockReader, numWorkers int) (*dirMeta, error)
	commitSnapshot(ctx context.Context, rootDir *dirMeta, psm *snapshot.Manifest) (*snapshot.Manifest, error)
	createRoot(ctx context.Context, curDm, prevRootDm *dirMeta) (*dirMeta, error)
	linkPreviousSnapshot(ctx context.Context, previousSnapshotID, prevVolumeSnapshotID string) (*dirMeta, *snapshot.Manifest, error)
}

// createRoot creates the root directory with references to current, previous and meta.
func (f *Filesystem) createRoot(ctx context.Context, curDm, prevRootDm *dirMeta) (*dirMeta, error) {
	rootDir := &dirMeta{
		name: "/",
	}

	rootDir.insertSubdir(curDm)

	if prevRootDm != nil {
		rootDir.insertSubdir(prevRootDm)
	}

	if err := f.createMetadataFiles(ctx, rootDir); err != nil {
		return nil, err
	}

	return rootDir, nil
}

// linkToPreviousSnapshot finds the previous snapshot and returns its dirMeta entry.
func (f *Filesystem) linkPreviousSnapshot(ctx context.Context, previousSnapshotID, prevVolumeSnapshotID string) (*dirMeta, *snapshot.Manifest, error) {
	prevMan, _, prevMd, err := f.findPreviousSnapshot(ctx, previousSnapshotID)
	if err != nil {
		return nil, nil, err
	}

	if prevMd.VolSnapID != prevVolumeSnapshotID {
		f.logger.Debugf("previous volume snapshot exp[%s] got[%s]", prevVolumeSnapshotID, prevMd.VolSnapID)
		return nil, nil, ErrInvalidSnapshot
	}

	// import previous data
	f.logger.Debugf("found snapshot [%s, %s] %#v %#v", previousSnapshotID, prevVolumeSnapshotID, prevMd, prevMan)
	f.layoutProperties.initLayoutProperties(prevMd.BlockSzB, prevMd.DirSz, prevMd.Depth)
	f.prevVolumeSnapshotID = prevMd.VolSnapID

	// add the previous directory object to the root directory
	prevRootDm := &dirMeta{
		name:    previousSnapshotDirName,
		oid:     prevMan.RootObjectID(),
		summary: prevMan.RootEntry.DirSummary,
	}

	return prevRootDm, prevMan, nil
}

// backupBlocks writes the volume blocks and the block map hierarchy to the repo.
func (f *Filesystem) backupBlocks(ctx context.Context, br volume.BlockReader, numWorkers int) (*dirMeta, error) {
	bi, err := br.GetBlocks(ctx)
	if err != nil {
		return nil, err
	}

	bbh := &backupBlocksHelper{}
	bbh.init(f)
	bbh.bp.Iter = bi
	bbh.bp.Worker = bbh.worker
	bbh.bp.NumWorkers = numWorkers

	// process the snapshot blocks
	err = bbh.bp.Run(ctx)
	if err != nil {
		return nil, err
	}

	// upload the block map directory hierarchy
	err = f.up.writeDirToRepo(ctx, parsedPath{currentSnapshotDirName}, bbh.curRoot, true)
	if err != nil {
		return nil, err
	}

	return bbh.curRoot, nil
}

// backupBlocksHelper is a helper for backupBlocks
type backupBlocksHelper struct {
	bp        volume.BlockProcessor
	f         *Filesystem
	mux       sync.Mutex
	blockSize int
	bufPool   sync.Pool
	curRoot   *dirMeta
}

func (bbh *backupBlocksHelper) init(f *Filesystem) {
	bbh.f = f
	bbh.bufPool.New = func() interface{} {
		buf := make([]byte, bbh.blockSize) // size determined at runtime

		return &buf
	}
	bbh.curRoot = &dirMeta{
		name: currentSnapshotDirName,
	}
}

func (bbh *backupBlocksHelper) worker(ctx context.Context, block volume.Block) error {
	pp, err := bbh.f.addrToPath(block.Address())
	if err != nil {
		return err
	}

	fm := bbh.ensureFile(pp)
	bbh.f.logger.Debugf("block [%s] sz=%d", pp.String(), block.Size())

	bufPtr := bbh.getBuffer(block.Size())
	defer bbh.releaseBuffer(bufPtr)

	rc, err := block.Get(ctx)
	if err != nil {
		bbh.f.logger.Debugf("block [%s] get: %v", pp.String(), err)
		return err
	}

	oid, sz, err := bbh.f.up.writeFileToRepo(ctx, pp, rc, *bufPtr)
	if err == nil {
		fm.oid = oid

		if int(sz) != block.Size() {
			bbh.f.logger.Debugf("block [%s] writeFileToRepo: wrote %d/%d", pp.String(), sz, block.Size())
		}
	}

	closeErr := rc.Close()
	if closeErr != nil {
		bbh.f.logger.Debugf("block [%s] Close: %v", pp.String(), err)

		if err == nil {
			err = closeErr
		}
	}

	return err
}

func (bbh *backupBlocksHelper) getBuffer(blockSize int) *[]byte {
	if bbh.blockSize == 0 {
		bbh.mux.Lock()
		if bbh.blockSize == 0 {
			bbh.blockSize = blockSize // safe assumption that all blocks have the same size
		}
		bbh.mux.Unlock()
	}

	return bbh.bufPool.Get().(*[]byte)
}

func (bbh *backupBlocksHelper) releaseBuffer(bufPtr *[]byte) {
	bbh.bufPool.Put(bufPtr)
}

func (bbh *backupBlocksHelper) ensureFile(pp parsedPath) *fileMeta {
	bbh.mux.Lock()
	defer bbh.mux.Unlock()

	return bbh.f.ensureFileInTree(bbh.curRoot, pp)
}
