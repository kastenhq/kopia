package repo

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

// SetParameters changes mutable repository parameters.
func (r *directRepository) SetParameters(ctx context.Context, m content.MutableParameters, ro content.RetentionOptions) error {
	f := r.formatBlob

	repoConfig, err := f.decryptFormatBytes(r.formatEncryptionKey)
	if err != nil {
		return errors.Wrap(err, "unable to decrypt repository config")
	}

	if err := m.Validate(); err != nil {
		return errors.Wrap(err, "invalid parameters")
	}

	if err := ro.Validate(); err != nil {
		return errors.Wrap(err, "invalid retention options")
	}

	repoConfig.FormattingOptions.MutableParameters = m

	if err := encryptFormatBytes(f, repoConfig, r.formatEncryptionKey, f.UniqueID); err != nil {
		return errors.Errorf("unable to encrypt format bytes")
	}

	if ro.IsNull() {
		// delete the retention blob when new settings are null, and ignore if
		// the blob did not pre-exist
		if err := r.blobs.DeleteBlob(ctx, RetentionBlobID); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
			return errors.Wrap(err, "unable to delete the retention blob")
		}
	} else {
		retentionBytes, err := serializeRetentionBytes(f, retentionBlobFromRetentionOptions(&ro), r.formatEncryptionKey)
		if err != nil {
			return errors.Wrap(err, "unable to encrypt retention bytes")
		}

		if err := r.blobs.PutBlob(ctx, RetentionBlobID, gather.FromSlice(retentionBytes), blob.PutOptions{
			RetentionMode:   ro.Mode,
			RetentionPeriod: ro.Period,
		}); err != nil {
			return errors.Wrap(err, "unable to write retention blob")
		}
	}

	if err := writeFormatBlob(ctx, r.blobs, f, r.retentionBlob); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	if cd := r.cachingOptions.CacheDirectory; cd != "" {
		if err := os.Remove(filepath.Join(cd, FormatBlobID)); err != nil {
			log(ctx).Errorf("unable to remove %s: %v", FormatBlobID, err)
		}

		if err := os.Remove(filepath.Join(cd, RetentionBlobID)); err != nil && !os.IsNotExist(err) {
			log(ctx).Errorf("unable to remove %s: %v", RetentionBlobID, err)
		}
	}

	return nil
}
