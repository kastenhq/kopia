package content

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

const verySmallContentFraction = 20 // blobs less than 1/verySmallContentFraction of maxPackSize are considered 'very small'

var autoCompactionOptions = CompactOptions{
	MaxSmallBlobs: 4 * parallelFetches, // nolint:gomnd
}

const (
	compactionLogBlobPrefix = "m"
)

// compactionLogEntry represents contents of compaction log entry stored in `m` blob.
type compactionLogEntry struct {
	// list of input blob names that were compacted together.
	InputBlobs []blob.Metadata `json:"inputMetadata"`

	// list of blobs that are results of compaction.
	OutputBlobs []blob.Metadata `json:"outputMetadata"`
}

// CompactOptions provides options for compaction
type CompactOptions struct {
	MaxSmallBlobs     int
	AllIndexes        bool
	DropDeletedBefore time.Time
}

// CompactIndexes performs compaction of index blobs ensuring that # of small index blobs is below opt.maxSmallBlobs
func (bm *Manager) CompactIndexes(ctx context.Context, opt CompactOptions) error {
	log(ctx).Debugf("CompactIndexes(%+v)", opt)

	bm.lock()
	defer bm.unlock()

	indexBlobs, _, err := bm.loadPackIndexesUnlocked(ctx)
	if err != nil {
		return errors.Wrap(err, "error loading indexes")
	}

	contentsToCompact := bm.getContentsToCompact(ctx, indexBlobs, opt)

	if err := bm.compactAndDeleteIndexBlobs(ctx, contentsToCompact, opt); err != nil {
		log(ctx).Warningf("error performing quick compaction: %v", err)
	}

	return nil
}

func (bm *Manager) getContentsToCompact(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) []IndexBlobInfo {
	var nonCompactedBlobs, verySmallBlobs []IndexBlobInfo

	var totalSizeNonCompactedBlobs, totalSizeVerySmallBlobs, totalSizeMediumSizedBlobs int64

	var mediumSizedBlobCount int

	for _, b := range indexBlobs {
		if b.Length > int64(bm.maxPackSize) && !opt.AllIndexes {
			continue
		}

		nonCompactedBlobs = append(nonCompactedBlobs, b)
		totalSizeNonCompactedBlobs += b.Length

		if b.Length < int64(bm.maxPackSize/verySmallContentFraction) {
			verySmallBlobs = append(verySmallBlobs, b)
			totalSizeVerySmallBlobs += b.Length
		} else {
			mediumSizedBlobCount++
			totalSizeMediumSizedBlobs += b.Length
		}
	}

	if len(nonCompactedBlobs) < opt.MaxSmallBlobs {
		// current count is below min allowed - nothing to do
		formatLog(ctx).Debugf("no small contents to compact")
		return nil
	}

	if len(verySmallBlobs) > len(nonCompactedBlobs)/2 && mediumSizedBlobCount+1 < opt.MaxSmallBlobs {
		formatLog(ctx).Debugf("compacting %v very small contents", len(verySmallBlobs))
		return verySmallBlobs
	}

	formatLog(ctx).Debugf("compacting all %v non-compacted contents", len(nonCompactedBlobs))

	return nonCompactedBlobs
}

func (bm *Manager) compactAndDeleteIndexBlobs(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) error {
	if len(indexBlobs) <= 1 {
		return nil
	}

	formatLog(ctx).Debugf("compacting %v index blobs", len(indexBlobs))

	bld := make(packIndexBuilder)

	var inputs, outputs []blob.Metadata

	for _, indexBlob := range indexBlobs {
		if err := bm.addIndexBlobsToBuilder(ctx, bld, indexBlob, opt); err != nil {
			return errors.Wrap(err, "error adding index to builder")
		}

		inputs = append(inputs, indexBlob.Metadata)
	}

	var buf bytes.Buffer
	if err := bld.Build(&buf); err != nil {
		return errors.Wrap(err, "unable to build an index")
	}

	compactedIndexBlob, err := bm.indexBlobManager.writeIndexBlob(ctx, buf.Bytes())
	if err != nil {
		return errors.Wrap(err, "unable to write compacted indexes")
	}

	// compaction wrote index blob that's the same as one of the sources
	// it must be a no-op.
	for _, indexBlob := range indexBlobs {
		if indexBlob.BlobID == compactedIndexBlob.BlobID {
			formatLog(ctx).Debugf("compaction was a no-op")
			return nil
		}
	}

	outputs = append(outputs, compactedIndexBlob)

	if err := bm.indexBlobManager.registerCompaction(ctx, inputs, outputs); err != nil {
		return errors.Wrap(err, "unable to register compaction")
	}

	return nil
}

func (bm *Manager) addIndexBlobsToBuilder(ctx context.Context, bld packIndexBuilder, indexBlob IndexBlobInfo, opt CompactOptions) error {
	data, err := bm.indexBlobManager.getIndexBlob(ctx, indexBlob.BlobID)
	if err != nil {
		return errors.Wrapf(err, "error getting index %q", indexBlob.BlobID)
	}

	index, err := openPackIndex(bytes.NewReader(data))
	if err != nil {
		return errors.Wrapf(err, "unable to open index blob %q", indexBlob)
	}

	_ = index.Iterate(AllIDs, func(i Info) error {
		if i.Deleted && !opt.DropDeletedBefore.IsZero() && i.Timestamp().Before(opt.DropDeletedBefore) {
			log(ctx).Debugf("skipping content %v deleted at %v", i.ID, i.Timestamp())
			return nil
		}
		bld.Add(i)
		return nil
	})

	return nil
}

func addBlobsToIndex(ndx map[blob.ID]*IndexBlobInfo, blobs []blob.Metadata) {
	for _, it := range blobs {
		if ndx[it.BlobID] == nil {
			ndx[it.BlobID] = &IndexBlobInfo{
				Metadata: blob.Metadata{
					BlobID:    it.BlobID,
					Length:    it.Length,
					Timestamp: it.Timestamp,
				},
			}
		}
	}
}

func removeCompactedIndexes(ctx context.Context, m map[blob.ID]*IndexBlobInfo, compactionLogs map[blob.ID]*compactionLogEntry, markAsSuperseded bool) {
	var validCompactionLogs []*compactionLogEntry

	for _, cl := range compactionLogs {
		// only process compaction logs for which we have found all the outputs.
		haveAllOutputs := true

		for _, o := range cl.OutputBlobs {
			if m[o.BlobID] == nil {
				haveAllOutputs = false

				log(ctx).Debugf("blob %v referenced by compaction log is not found", o.BlobID)

				break
			}
		}

		if haveAllOutputs {
			validCompactionLogs = append(validCompactionLogs, cl)
		}
	}

	// now remove all inputs from the set if there's a valid compaction log entry with all the outputs.
	for _, cl := range validCompactionLogs {
		for _, ib := range cl.InputBlobs {
			if md := m[ib.BlobID]; md != nil && md.Superseded == nil {
				log(ctx).Debugf("ignoring index blob %v (%v) because it's been compacted to %v", ib, md.Timestamp, cl.OutputBlobs)

				if markAsSuperseded {
					md.Superseded = cl.OutputBlobs
				} else {
					delete(m, ib.BlobID)
				}
			}
		}
	}
}
