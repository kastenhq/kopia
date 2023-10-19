// Package azure implements Azure Blob Storage.
package azure

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	azblobblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	azblockblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	azblobmodels "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/timestampmeta"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	azStorageType   = "azureBlob"
	latestVersionID = ""

	deleteMarkerVersion = "delete marker"

	timeMapKey = "Kopiamtime" // this must be capital letter followed by lowercase, to comply with AZ tags naming convention.
)

type azStorage struct {
	Options
	blob.DefaultProviderImplementation

	service   *azblob.Client
	container string
}

func (az *azStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	return az.getBlobWithVersion(ctx, b, latestVersionID, offset, length, output)
}

func (az *azStorage) getBlobWithVersion(ctx context.Context, b blob.ID, versionID string, offset, length int64, output blob.OutputBuffer) error {
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

	bc, err := az.service.ServiceClient().
		NewContainerClient(az.container).
		NewBlobClient(az.getObjectNameString(b)).
		WithVersionID(versionID)
	if err != nil {
		return err
	}
	resp, err := bc.DownloadStream(ctx, opt)
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

	return az.putBlob(ctx, b, data, opts)
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
		// if a policy prevents the deletion then try to create a delete marker version & delete that instead.
		return az.retryDeleteBlob(ctx, b)
	}

	return err
}

// ExtendBlobRetention extends a blob retention period.
func (az *azStorage) ExtendBlobRetention(ctx context.Context, b blob.ID, opts blob.ExtendOptions) error {
	err := az.setImmutabilityPolicy(ctx, b, opts)
	if err != nil {
		return errors.Wrap(err, "extending a blob")
	}

	return nil
}

func (az *azStorage) getObjectNameString(b blob.ID) string {
	return az.Prefix + string(b)
}

// ListBlobs list azure blobs with given prefix.
func (az *azStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	prefixStr := az.getObjectNameString(prefix)

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

func (az *azStorage) putBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tsMetadata := timestampmeta.ToMap(opts.SetModTime, timeMapKey)

	metadata := make(map[string]*string, len(tsMetadata))

	for k, v := range tsMetadata {
		metadata[k] = to.Ptr(v)
	}

	uo := &azblockblob.UploadOptions{
		Metadata: metadata,
	}
	if opts.HasRetentionOptions() {
		mode := azblobblob.ImmutabilityPolicySetting(opts.RetentionMode)
		retainUntilDate := clock.Now().Add(opts.RetentionPeriod).UTC()
		uo.ImmutabilityPolicyMode = &mode
		uo.ImmutabilityPolicyExpiryTime = &retainUntilDate
	}

	resp, err := az.service.ServiceClient().
		NewContainerClient(az.container).
		NewBlockBlobClient(az.getObjectNameString(b)).
		Upload(ctx, data.Reader(), uo)
	if err != nil {
		return translateError(err)
	}

	if opts.GetModTime != nil {
		*opts.GetModTime = *resp.LastModified
	}

	return nil
}

func (az *azStorage) setImmutabilityPolicy(ctx context.Context, b blob.ID, opts blob.ExtendOptions) error {
	// it will fail if the retentionPeriod set by the user is lower than that on the policy.
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

// retryDeleteBlob creates a delete marker version which is set to unlocked state. This protection is then removed
// and the blob is deleted.
func (az *azStorage) retryDeleteBlob(ctx context.Context, b blob.ID) error {
	err := az.putBlob(ctx, b, gather.FromSlice([]byte(deleteMarkerVersion)), blob.PutOptions{
		RetentionMode:   blob.RetentionMode(azblobblob.ImmutabilityPolicySettingUnlocked),
		RetentionPeriod: time.Minute,
	})
	if err != nil {
		return errors.Wrap(err, "failed to put delete marker blob version")
	}

	_, err = az.service.ServiceClient().
		NewContainerClient(az.container).
		NewBlobClient(az.getObjectNameString(b)).
		DeleteImmutabilityPolicy(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to remove delete marker blob immutability protection")
	}

	_, err = az.service.DeleteBlob(ctx, az.container, az.getObjectNameString(b), nil)
	return err
}

// New creates new Azure Blob Storage-backed storage with specified options:
//
// - the 'Container', 'StorageAccount' and 'StorageKey' fields are required and all other parameters are optional.
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	_ = isCreate

	raw, err := newStorage(opt)
	if err != nil {
		return nil, err
	}

	st, err := maybePointInTimeStore(ctx, raw, opt.PointInTime)
	if err != nil {
		return nil, err
	}

	az := retrying.NewWrapper(st)

	// verify Azure connection is functional by listing blobs in a bucket, which will fail if the container
	// does not exist. We list with a prefix that will not exist, to avoid iterating through any objects.
	nonExistentPrefix := fmt.Sprintf("kopia-azure-storage-initializing-%v", clock.Now().UnixNano())
	if err := st.ListBlobs(ctx, blob.ID(nonExistentPrefix), func(md blob.Metadata) error {
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "unable to list from the bucket")
	}

	return az, nil
}

func newStorage(opt *Options) (*azStorage, error) {
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

	return &azStorage{
		Options:                *opt,
		container:              opt.Container,
		service:                service,
	}, nil
}
}

func init() {
	blob.AddSupportedStorage(azStorageType, Options{}, New)
}
