package azure

import (
	"context"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/google/martian/v3/log"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
)

type azPointInTimeStorage struct {
	azStorage

	pointInTime time.Time
}

func (az *azPointInTimeStorage) ListBlobs(ctx context.Context, blobIDPrefix blob.ID, cb func(bm blob.Metadata) error) error {
	var (
		previousID blob.ID
		vs         []versionMetadata
	)

	err := az.listBlobVersions(ctx, blobIDPrefix, func(vm versionMetadata) error {
		if vm.BlobID != previousID {
			// different blob, process previous one
			if v, found := newestAt(vs, az.pointInTime); found {
				if err := cb(v.Metadata); err != nil {
					return err
				}
			}

			previousID = vm.BlobID
			vs = vs[:0] // reset for next blob
		}

		vs = append(vs, vm)

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "could not list blob versions at time %s", az.pointInTime)
	}

	// process last blob
	if v, found := newestAt(vs, az.pointInTime); found {
		if err := cb(v.Metadata); err != nil {
			return err
		}
	}

	return nil
}

func (az *azPointInTimeStorage) GetBlob(ctx context.Context, blobID blob.ID, offset, length int64, output blob.OutputBuffer) error {
	// getMetadata returns the specific blob version at time t
	m, err := az.getVersionedMetadata(ctx, blobID)
	if err != nil {
		return errors.Wrap(err, "getting metadata")
	}

	return az.getBlobWithVersion(ctx, blobID, m.Version, offset, length, output)
}

// newestAt returns the last version in the list older than the PIT.
// Azure sorts in ascending order so return the last element in the list.
func newestAt(vs []versionMetadata, t time.Time) (v versionMetadata, found bool) {
	vs = getOlderThan(vs, t)

	if len(vs) == 0 {
		return versionMetadata{}, false
	}

	v = vs[len(vs)-1]
	return v, true
}

// Removes versions that are newer than t. The filtering is done in place
// and uses the same slice storage as vs. Assumes entries in vs are in ascending
// timestamp order, unlike S3 which assumes descending.
func getOlderThan(vs []versionMetadata, t time.Time) []versionMetadata {
	for i := range vs {
		if vs[i].Timestamp.After(t) {
			return vs[:i]
		}
	}

	return vs
}

func (az *azPointInTimeStorage) getVersionedBlobsToSkip(ctx context.Context) (map[string]bool, error) {
	return az.getVersionedFilesPendingDeletion(ctx)
}

// getFilesPendingDeletion returns a list of blobs that have an associated delete marker file.
// The original blobs may have already been deleted but the delete marker files are still pending deletion.
func (az *azPointInTimeStorage) getVersionedFilesPendingDeletion(ctx context.Context) (map[string]bool, error) {
	dmBlobs, err := az.getVersionedDeleteMarkerBlobs(ctx)
	if err != nil {
		return nil, translateError(err)
	}

	return az.getDeleteMarkerOriginalFilesMap(dmBlobs), nil
}

func (az *azPointInTimeStorage) getVersionedDeleteMarkerBlobs(ctx context.Context) ([]blob.Metadata, error) {
	dmBlobs, err := az.getDeleteMarkerBlobs(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get delete marker blobs")
	}
	var versionedDMBlobs []blob.Metadata
	for _, dm := range dmBlobs {
		if dm.Timestamp.After(az.pointInTime) {
			continue
		}
		versionedDMBlobs = append(versionedDMBlobs, dm)
	}
	return versionedDMBlobs, nil
}

func (az *azPointInTimeStorage) listBlobVersions(ctx context.Context, prefix blob.ID, callback func(vm versionMetadata) error) error {
	prefixStr := az.getObjectNameString(prefix)

	blobsToSkip, err := az.getVersionedBlobsToSkip(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get blobs to skip")
	}

	pager := az.service.NewListBlobsFlatPager(az.container, &azblob.ListBlobsFlatOptions{
		Prefix: &prefixStr,
		Include: azblob.ListBlobsInclude{
			Metadata: true,
			Versions: true,
		},
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return translateError(err)
		}

		for _, it := range page.Segment.BlobItems {
			blobName := az.getBlobName(it)

			if blobsToSkip[blobName] {
				log.Debugf("excluded blob from ListBlobs: %s", blobName)
				// skip those set for deletion
				continue
			}

			vm := az.getVersionedBlobMeta(it)

			if err := callback(vm); err != nil {
				return err
			}
		}
	}

	return nil
}

func (az *azPointInTimeStorage) getVersionedMetadata(ctx context.Context, blobID blob.ID) (versionMetadata, error) {
	var vml []versionMetadata

	if err := az.getBlobVersions(ctx, blobID, func(vm versionMetadata) error {
		if !vm.Timestamp.After(az.pointInTime) {
			vml = append(vml, vm)
		}

		return nil
	}); err != nil {
		return versionMetadata{}, errors.Wrapf(err, "could not get version metadata for blob %s", blobID)
	}

	if v, found := newestAt(vml, az.pointInTime); found {
		return v, nil
	}

	return versionMetadata{}, blob.ErrBlobNotFound
}

// maybePointInTimeStore wraps s with a point-in-time store when s is versioned
// and a point-in-time value is specified. Otherwise, s is returned.
func maybePointInTimeStore(ctx context.Context, s *azStorage, pointInTime *time.Time) (blob.Storage, error) {
	if pit := s.Options.PointInTime; pit == nil || pit.IsZero() {
		return s, nil
	}

	// IsImmutableStorageWithVersioning is needed for PutBlob with RetentionPeriod being set.
	props, err := s.service.ServiceClient().NewContainerClient(s.container).GetProperties(ctx, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get determine if container '%s' supports versioning", s.container)
	}

	if props.IsImmutableStorageWithVersioningEnabled == nil || !*props.IsImmutableStorageWithVersioningEnabled {
		return nil, errors.Errorf("cannot create point-in-time view for non-versioned bucket '%s'", s.container)
	}

	return readonly.NewWrapper(&azPointInTimeStorage{
		azStorage:   *s,
		pointInTime: *pointInTime,
	}), nil
}
