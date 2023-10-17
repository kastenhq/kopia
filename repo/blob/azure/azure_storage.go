// Package azure implements Azure Blob Storage.
package azure

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	azblobblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	azblobmodels "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/google/martian/v3/log"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/timestampmeta"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
	"github.com/kopia/kopia/repo/logging"
)

const (
	azStorageType = "azureBlob"

	timeMapKey = "Kopiamtime" // this must be capital letter followed by lowercase, to comply with AZ tags naming convention.
)

type azStorage struct {
	Options
	blob.DefaultProviderImplementation

	service   *azblob.Client
	container string
}

func (az *azStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	if offset < 0 {
		return errors.Wrap(blob.ErrInvalidRange, "invalid offset")
	}

	opt := &azblob.DownloadStreamOptions{}

	if length > 0 {
		opt.Range.Offset = offset
		opt.Range.Count = length
	}

	if length == 0 {
		l1 := int64(1)
		opt.Range.Offset = offset
		opt.Range.Count = l1
	}

	resp, err := az.service.DownloadStream(ctx, az.container, az.getObjectNameString(b), opt)
	if err != nil {
		return translateError(err)
	}

	body := resp.Body
	defer body.Close() //nolint:errcheck

	if length == 0 {
		return nil
	}

	if err := iocopy.JustCopy(output, body); err != nil {
		return translateError(err)
	}

	//nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (az *azStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	bc := az.service.ServiceClient().NewContainerClient(az.container).NewBlobClient(az.getObjectNameString(b))

	fi, err := bc.GetProperties(ctx, nil)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(translateError(err), "Attributes")
	}

	bm := blob.Metadata{
		BlobID:    b,
		Length:    *fi.ContentLength,
		Timestamp: *fi.LastModified,
	}

	if fi.Metadata[timeMapKey] != nil {
		if t, ok := timestampmeta.FromValue(*fi.Metadata[timeMapKey]); ok {
			bm.Timestamp = t
		}
	}

	return bm, nil
}

func translateError(err error) error {
	if err == nil {
		return nil
	}

	var re *azcore.ResponseError

	if errors.As(err, &re) {
		switch re.ErrorCode {
		case string(bloberror.BlobNotFound):
			return blob.ErrBlobNotFound
		case string(bloberror.InvalidRange):
			return blob.ErrInvalidRange
		case string(bloberror.BlobImmutableDueToPolicy):
			return blob.ErrBlobImmutableDueToPolicy
		}
	}

	return err
}

func (az *azStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	switch {
	case opts.HasRetentionOptions() && !opts.RetentionMode.IsValidAzure():
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "blob retention mode is not valid for Azure")
	case opts.DoNotRecreate:
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "do-not-recreate")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tsMetadata := timestampmeta.ToMap(opts.SetModTime, timeMapKey)

	metadata := make(map[string]*string, len(tsMetadata))

	for k, v := range tsMetadata {
		metadata[k] = to.Ptr(v)
	}

	uso := &azblob.UploadStreamOptions{
		Metadata: metadata,
	}

	resp, err := az.service.UploadStream(ctx, az.container, az.getObjectNameString(b), data.Reader(), uso)
	if err != nil {
		return translateError(err)
	}

	if opts.RetentionPeriod != 0 {
		// it will fail if the retentionPeriod set by the user is lower than that on the policy.
		retainUntilDate := clock.Now().Add(opts.RetentionPeriod).UTC()
		mode := azblobblob.ImmutabilityPolicySetting(opts.RetentionMode)

		_, err = az.service.ServiceClient().
			NewContainerClient(az.Container).
			NewBlobClient(az.getObjectNameString(b)).
			SetImmutabilityPolicy(ctx, retainUntilDate, &azblobblob.SetImmutabilityPolicyOptions{
				Mode: &mode,
			})
		if err != nil {
			return errors.Wrap(err, "unable to extend retention period")
		}
	}

	if opts.GetModTime != nil {
		*opts.GetModTime = *resp.LastModified
	}

	return nil
}

// DeleteBlob deletes azure blob from container with given ID.
func (az *azStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	_, err := az.service.DeleteBlob(ctx, az.container, az.getObjectNameString(b), nil)
	err = translateError(err)

	switch {
	case errors.Is(err, blob.ErrBlobNotFound):
		// don't return error if blob is already deleted
		return nil
	case errors.Is(err, blob.ErrBlobImmutableDueToPolicy):
		// if a policy prevents the deletion then try to create a delete marker file to hide it
		return az.createDeleteMarkerFile(ctx, string(b))
	}

	return err
}

// ExtendBlobRetention extends a blob unless it is a delete marker file and the original file no longer exists.
// If the original file still exists then the delete marker file will be extended but the original file will not.
// This ensures the delete marker file survives longer than the original.
func (az *azStorage) ExtendBlobRetention(ctx context.Context, b blob.ID, opts blob.ExtendOptions) error {
	if az.isOrphanedDeleteMarkerFile(ctx, b) {
		return blob.ErrOrphanedDeleteMarkerBlob
	}

	retainUntilDate := clock.Now().Add(opts.RetentionPeriod).UTC()
	mode := azblobblob.ImmutabilityPolicySetting(opts.RetentionMode)

	_, err := az.service.ServiceClient().
		NewContainerClient(az.Container).
		NewBlobClient(az.getObjectNameString(b)).
		SetImmutabilityPolicy(ctx, retainUntilDate, &azblobblob.SetImmutabilityPolicyOptions{
			Mode: &mode,
		})
	if err != nil {
		return errors.Wrap(err, "unable to extend retention period")
	}

	return nil
}

// Cleanup removes files that have a matching delete marker file, providing the retention period has passed.
// If the original file has been deleted then the delete marker file is also deleted, providing the retention period has passed.
func (az *azStorage) Cleanup(ctx context.Context, logger logging.Logger) error {
	deleteMarkerBlobs, err := blob.ListAllBlobs(ctx, az, blob.BlobIDPrefixDeleteMarker)
	if err != nil {
		return errors.Wrap(err, "failed to retrieve delete marker blobs")
	}

	deletedBlobs, deletedMarkers, err := az.cleanupParallel(ctx, logger, deleteMarkerBlobs)
	logger.Infof("deleted %d blobs and %d delete marker blobs", deletedBlobs, deletedMarkers)
	return err
}

func (az *azStorage) getObjectNameString(b blob.ID) string {
	return az.Prefix + string(b)
}

// ListBlobs list azure blobs with given prefix.
func (az *azStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	prefixStr := az.getObjectNameString(prefix)

	blobsToSkip, err := az.getBlobsToSkip(ctx, prefix)
	if err != nil {
		return errors.Wrap(err, "failed to get blobs to skip")
	}

	pager := az.service.NewListBlobsFlatPager(az.container, &azblob.ListBlobsFlatOptions{
		Prefix: &prefixStr,
		Include: azblob.ListBlobsInclude{
			Metadata: true,
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

			bm := az.getBlobMeta(it)

			if err := callback(bm); err != nil {
				return err
			}
		}
	}

	return nil
}

func stringDefault(s *string, def string) string {
	if s == nil {
		return def
	}

	return *s
}

func (az *azStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   azStorageType,
		Config: &az.Options,
	}
}

func (az *azStorage) DisplayName() string {
	return fmt.Sprintf("Azure: %v", az.Options.Container)
}

// createDeleteMarkerFile creates a delete marker file based on an originalFile to note that the originalFile should be ignored.
func (az *azStorage) createDeleteMarkerFile(ctx context.Context, originalFile string) error {
	fileName := fmt.Sprintf("%s_%s", blob.BlobIDPrefixDeleteMarker, originalFile)
	deleteMarkerFile := az.getObjectNameString(blob.ID(fileName))
	_, err := az.service.ServiceClient().
		NewContainerClient(az.container).
		NewBlobClient(deleteMarkerFile).
		GetProperties(ctx, nil)
	if err == nil {
		// file already exists
		return nil
	}

	_, err = az.service.UploadBuffer(ctx, az.container, deleteMarkerFile, []byte(nil), nil)
	return err
}

// getBlobsToSkip returns a list of blobs to skip, those marked for deletion via an associated delete marker file.
func (az *azStorage) getBlobsToSkip(ctx context.Context, prefix blob.ID) (map[string]bool, error) {
	if prefix == blob.BlobIDPrefixDeleteMarker {
		return nil, nil
	}
	return az.getFilesPendingDeletion(ctx)
}

// getFilesPendingDeletion returns a list of blobs that have an associated delete marker file.
// The original blobs may have already been deleted but the delete marker files are still pending deletion.
func (az *azStorage) getFilesPendingDeletion(ctx context.Context) (map[string]bool, error) {
	dmBlobs, err := az.getDeleteMarkerBlobs(ctx)
	if err != nil {
		return nil, translateError(err)
	}

	return az.getDeleteMarkerOriginalFilesMap(dmBlobs), nil
}

func (az *azStorage) getDeleteMarkerBlobs(ctx context.Context) ([]blob.Metadata, error) {
	prefixStr := az.getObjectNameString(blob.BlobIDPrefixDeleteMarker)
	pager := az.service.NewListBlobsFlatPager(az.container, &azblob.ListBlobsFlatOptions{
		Prefix: &prefixStr,
	})

	var deleteMarketBlobs []blob.Metadata
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, translateError(err)
		}

		for _, it := range page.Segment.BlobItems {
			bm := az.getBlobMeta(it)
			deleteMarketBlobs = append(deleteMarketBlobs, bm)
		}
	}

	return deleteMarketBlobs, nil
}

func (az *azStorage) getDeleteMarkerOriginalFilesMap(dmBlobs []blob.Metadata) map[string]bool {
	pendingDeletionFiles := make(map[string]bool)
	for _, dm := range dmBlobs {
		originalFile, ok := getOriginalFile(string(dm.BlobID))
		if !ok {
			continue
		}
		pendingDeletionFiles[originalFile] = true
	}

	return pendingDeletionFiles
}

// isOrphanedDeleteMarkerFile checks if the original blob the delete marker file represents still exists.
func (az *azStorage) isOrphanedDeleteMarkerFile(ctx context.Context, b blob.ID) bool {
	blobName := string(b)
	if !strings.HasPrefix(blobName, string(blob.BlobIDPrefixDeleteMarker)) {
		return false
	}
	originalFile, ok := getOriginalFile(blobName)
	if !ok {
		return false
	}

	originalBlob := blob.ID(originalFile)

	_, err := az.service.ServiceClient().
		NewContainerClient(az.container).
		NewBlobClient(az.getObjectNameString(originalBlob)).
		GetProperties(ctx, nil)

	return errors.Is(translateError(err), blob.ErrBlobNotFound)
}

func (az *azStorage) getBlobName(it *azblobmodels.BlobItem) string {
	n := *it.Name
	return n[len(az.Prefix):]
}

func (az *azStorage) getBlobMeta(it *azblobmodels.BlobItem) blob.Metadata {
	bm := blob.Metadata{
		BlobID: blob.ID(az.getBlobName(it)),
		Length: *it.Properties.ContentLength,
	}

	// see if we have 'Kopiamtime' metadata, if so - trust it.
	if t, ok := timestampmeta.FromValue(stringDefault(it.Metadata["kopiamtime"], "")); ok {
		bm.Timestamp = t
	} else {
		bm.Timestamp = *it.Properties.LastModified
	}
	return bm
}

// cleanupParallel removes files that have a matching delete marker file, providing the retention period has passed.
// If the original file has been deleted then the delete marker file is also deleted, providing the retention period has passed.
func (az *azStorage) cleanupParallel(ctx context.Context, logger logging.Logger, deleteMarkerFiles []blob.Metadata) (deletedBlobs, deletedMarkers int, err error) {
	var (
		parallel          = runtime.NumCPU()
		wg                sync.WaitGroup
		deletedBlobsCtr   = new(uint32)
		deletedMarkersCtr = new(uint32)
		errChan           = make(chan error, parallel)
	)

	deleteMarkerBlobs := make(chan blob.Metadata, len(deleteMarkerFiles))

	for i := 0; i < parallel; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for bm := range deleteMarkerBlobs {
				originalFile, ok := getOriginalFile(string(bm.BlobID))
				if !ok {
					continue
				}
				blobsDeleted, fileRemoved, err := az.attemptFileDeletion(ctx, blob.ID(originalFile))
				if err != nil {
					errChan <- errors.Wrap(err, "failed to delete original blob")
					return
				}
				atomic.AddUint32(deletedBlobsCtr, uint32(blobsDeleted))
				if !fileRemoved {
					logger.Debugf("skipped cleanup of file %s, too early to delete", originalFile)
					continue
				}

				blobsDeleted, fileRemoved, err = az.attemptFileDeletion(ctx, bm.BlobID)
				if err != nil {
					errChan <- errors.Wrap(err, "failed to delete original blob")
					return
				}
				if !fileRemoved {
					logger.Debugf("skipped cleanup of file %s, too early to delete", string(bm.BlobID))
				}
				atomic.AddUint32(deletedMarkersCtr, uint32(blobsDeleted))
			}
		}()
	}

	for _, bm := range deleteMarkerFiles {
		deleteMarkerBlobs <- bm
	}

	close(deleteMarkerBlobs)

	wg.Wait()
	close(errChan)

	return int(*deletedBlobsCtr), int(*deletedMarkersCtr), <-errChan
}

// attemptFileDeletion tries to delete a blob if it still exists and its retention period has passed.
func (az *azStorage) attemptFileDeletion(ctx context.Context, b blob.ID) (deletedBlobs int, fileRemoved bool, err error) {
	_, err = az.service.DeleteBlob(ctx, az.container, az.getObjectNameString(b), nil)
	if err == nil {
		return 1, true, nil
	}
	translatedErr := translateError(err)
	switch {
	case errors.Is(translatedErr, blob.ErrBlobNotFound):
		return 0, true, nil
	case errors.Is(translatedErr, blob.ErrBlobImmutableDueToPolicy):
		return 0, false, nil
	}
	return 0, false, errors.Wrap(translatedErr, "failed to delete blob")
}

// getOriginalFile takes a delete marker file and returns the name of the original file.
func getOriginalFile(deleteMarkerFile string) (originalFile string, ok bool) {
	fileName := strings.SplitN(deleteMarkerFile, "_", 2)
	if len(fileName) != 2 || fileName[0] != string(blob.BlobIDPrefixDeleteMarker) {
		return "", false
	}
	return fileName[1], true
}

// New creates new Azure Blob Storage-backed storage with specified options:
//
// - the 'Container', 'StorageAccount' and 'StorageKey' fields are required and all other parameters are optional.
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	_ = isCreate

	if opt.Container == "" {
		return nil, errors.New("container name must be specified")
	}

	var (
		service    *azblob.Client
		serviceErr error
	)

	storageDomain := opt.StorageDomain
	if storageDomain == "" {
		storageDomain = "blob.core.windows.net"
	}

	storageHostname := fmt.Sprintf("%v.%v", opt.StorageAccount, storageDomain)

	switch {
	// shared access signature
	case opt.SASToken != "":
		service, serviceErr = azblob.NewClientWithNoCredential(
			fmt.Sprintf("https://%s?%s", storageHostname, opt.SASToken), nil)

	// storage account access key
	case opt.StorageKey != "":
		// create a credentials object.
		cred, err := azblob.NewSharedKeyCredential(opt.StorageAccount, opt.StorageKey)
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize storage access key credentials")
		}

		service, serviceErr = azblob.NewClientWithSharedKeyCredential(
			fmt.Sprintf("https://%s/", storageHostname), cred, nil,
		)
	// client secret
	case opt.TenantID != "" && opt.ClientID != "" && opt.ClientSecret != "":
		cred, err := azidentity.NewClientSecretCredential(opt.TenantID, opt.ClientID, opt.ClientSecret, nil)
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize client secret credential")
		}

		service, serviceErr = azblob.NewClient(fmt.Sprintf("https://%s/", storageHostname), cred, nil)

	default:
		return nil, errors.Errorf("one of the storage key, SAS token or client secret must be provided")
	}

	if serviceErr != nil {
		return nil, errors.Wrap(serviceErr, "opening azure service")
	}

	raw := &azStorage{
		Options:   *opt,
		container: opt.Container,
		service:   service,
	}

	az := retrying.NewWrapper(raw)

	// verify Azure connection is functional by listing blobs in a bucket, which will fail if the container
	// does not exist. We list with a prefix that will not exist, to avoid iterating through any objects.
	nonExistentPrefix := fmt.Sprintf("kopia-azure-storage-initializing-%v", clock.Now().UnixNano())
	if err := raw.ListBlobs(ctx, blob.ID(nonExistentPrefix), func(md blob.Metadata) error {
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "unable to list from the bucket")
	}

	return az, nil
}

func init() {
	blob.AddSupportedStorage(azStorageType, Options{}, New)
}
