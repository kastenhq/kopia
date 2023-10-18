package azure_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	legacyazblob "github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/azure"
	"github.com/kopia/kopia/repo/content"
)

const (
	testContainerEnv           = "KOPIA_AZURE_TEST_CONTAINER"
	testStorageAccountEnv      = "KOPIA_AZURE_TEST_STORAGE_ACCOUNT"
	testStorageKeyEnv          = "KOPIA_AZURE_TEST_STORAGE_KEY"
	testStorageSASTokenEnv     = "KOPIA_AZURE_TEST_SAS_TOKEN"
	testStorageTenantIDEnv     = "KOPIA_AZURE_TEST_TENANT_ID"
	testStorageClientIDEnv     = "KOPIA_AZURE_TEST_CLIENT_ID"
	testStorageClientSecretEnv = "KOPIA_AZURE_TEST_CLIENT_SECRET"
)

func getEnvOrSkip(t *testing.T, name string) string {
	t.Helper()

	value := os.Getenv(name)
	if value == "" {
		t.Skipf("%s not provided", name)
	}

	return value
}

func createContainer(t *testing.T, container, storageAccount, storageKey string) {
	t.Helper()

	credential, err := legacyazblob.NewSharedKeyCredential(storageAccount, storageKey)
	if err != nil {
		t.Fatalf("failed to create Azure credentials: %v", err)
	}

	p := legacyazblob.NewPipeline(credential, legacyazblob.PipelineOptions{})

	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", storageAccount))
	if err != nil {
		t.Fatalf("failed to parse container URL: %v", err)
	}

	serviceURL := legacyazblob.NewServiceURL(*u, p)
	containerURL := serviceURL.NewContainerURL(container)

	_, err = containerURL.Create(context.Background(), legacyazblob.Metadata{}, legacyazblob.PublicAccessNone)
	if err == nil {
		return
	}

	// return if already exists
	var stgErr legacyazblob.StorageError
	if errors.As(err, &stgErr) {
		if stgErr.ServiceCode() == legacyazblob.ServiceCodeContainerAlreadyExists {
			return
		}
	}

	t.Fatalf("failed to create blob storage container: %v", err)
}

func TestCleanupOldData(t *testing.T) {
	ctx := testlogging.Context(t)

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	st, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	}, false)

	require.NoError(t, err)

	defer st.Close(ctx)

	blobtesting.CleanupOldData(ctx, t, st, blobtesting.MinCleanupAge)
}

func TestAzureStorage(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	// create container if does not exist
	createContainer(t, container, storageAccount, storageKey)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	st, err := azure.New(newctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
		Prefix:         fmt.Sprintf("test-%v-%x-", clock.Now().Unix(), data),
	}, false)

	cancel()
	require.NoError(t, err)

	defer st.Close(ctx)

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestAzureStorageSASToken(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	sasToken := getEnvOrSkip(t, testStorageSASTokenEnv)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after storage is initialize,
	// to verify we do not depend on the original context past initialization.
	newctx, cancel := context.WithCancel(ctx)
	st, err := azure.New(newctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		SASToken:       sasToken,
		Prefix:         fmt.Sprintf("sastest-%v-%x-", clock.Now().Unix(), data),
	}, false)

	require.NoError(t, err)
	cancel()

	defer st.Close(ctx)
	defer blobtesting.CleanupOldData(ctx, t, st, 0)

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestAzureStorageClientSecret(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	tenantID := getEnvOrSkip(t, testStorageTenantIDEnv)
	clientID := getEnvOrSkip(t, testStorageClientIDEnv)
	clientSecret := getEnvOrSkip(t, testStorageClientSecretEnv)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after storage is initialize,
	// to verify we do not depend on the original context past initialization.
	newctx, cancel := context.WithCancel(ctx)
	st, err := azure.New(newctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		TenantID:       tenantID,
		ClientID:       clientID,
		ClientSecret:   clientSecret,
		Prefix:         fmt.Sprintf("sastest-%v-%x-", clock.Now().Unix(), data),
	}, false)

	require.NoError(t, err)
	cancel()

	defer st.Close(ctx)
	defer blobtesting.CleanupOldData(ctx, t, st, 0)

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestAzureStorageInvalidBlob(t *testing.T) {
	testutil.ProviderTest(t)

	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	ctx := context.Background()

	st, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	}, false)
	if err != nil {
		t.Fatalf("unable to connect to Azure container: %v", err)
	}

	defer st.Close(ctx)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err = st.GetBlob(ctx, "xxx", 0, 30, &tmp)
	if err == nil {
		t.Errorf("unexpected success when adding to non-existent container")
	}
}

func TestAzureStorageInvalidContainer(t *testing.T) {
	testutil.ProviderTest(t)

	container := fmt.Sprintf("invalid-container-%v", clock.Now().UnixNano())
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	ctx := context.Background()
	_, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	}, false)

	if err == nil {
		t.Errorf("unexpected success connecting to Azure container, wanted error")
	}
}

func TestAzureStorageInvalidCreds(t *testing.T) {
	testutil.ProviderTest(t)

	storageAccount := "invalid-acc"
	storageKey := "invalid-key"
	container := "invalid-container"

	ctx := context.Background()
	_, err := azure.New(ctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
	}, false)

	if err == nil {
		t.Errorf("unexpected success connecting to Azure blob storage, wanted error")
	}
}

// TestAzureStorageRansomwareProtection runs through the behaviour of Azure ransomware protection.
// 1. blob is created then the retention is extended.
// 2. blob is logically deleted while the retention period is in place, by creating a delete marker (d_) file.
// 3. delete marker blob is extended.
// 4. original blob is deleted.
// 5. delete marker blob further extension fails, then it is deleted.
func TestAzureStorageRansomwareProtection(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	// must be without locked policy or the retention period will be too high (1+ days)
	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	// create container if does not exist
	createContainer(t, container, storageAccount, storageKey)

	data := make([]byte, 8)
	rand.Read(data)

	ctx := testlogging.Context(t)

	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	prefix := fmt.Sprintf("test-%v-%x-", clock.Now().Unix(), data)
	st, err := azure.New(newctx, &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
		Prefix:         prefix,
	}, false)

	cancel()
	require.NoError(t, err)

	defer st.Close(ctx)

	const blobName = "sExample"
	const dummyBlob = blob.ID(blobName)
	blobNameFullPath := prefix + blobName

	putOpts := blob.PutOptions{
		RetentionMode:   blob.Locked,
		RetentionPeriod: 3 * time.Second,
	}
	if err := st.PutBlob(ctx, dummyBlob, gather.FromSlice([]byte(nil)), putOpts); err != nil {
		t.Fatalf("couldn't put blob: %v", err)
	}

	if count := getBlobCount(ctx, t, st, content.BlobIDPrefixSession); count != 1 {
		t.Fatalf("got %d blobs but expected %d", count, 1)
	}

	currentTime := clock.Now().UTC()

	cli := getAzureCLI(t, storageAccount, storageKey)
	blobRetention := getBlobRetention(ctx, t, cli, container, blobNameFullPath)
	if !blobRetention.After(currentTime) {
		t.Fatalf("blob retention period not in the future: %v", blobRetention)
	}

	extendOpts := blob.ExtendOptions{
		RetentionMode:   blob.Locked,
		RetentionPeriod: 4 * time.Second,
	}
	if err := st.ExtendBlobRetention(ctx, dummyBlob, extendOpts); err != nil {
		t.Fatalf("couldn't extend blob: %v", err)
	}

	extendedRetention := getBlobRetention(ctx, t, cli, container, blobNameFullPath)
	if !extendedRetention.After(blobRetention) {
		t.Fatalf("blob retention period not extended. was %v, now %v", blobRetention, extendedRetention)
	}

	if err := st.DeleteBlob(ctx, dummyBlob); err != nil {
		t.Fatalf("can't delete blob: %v", err)
	}

	if count := getBlobCount(ctx, t, st, content.BlobIDPrefixSession); count != 0 {
		t.Fatalf("got %d blobs but expected %d", count, 0)
	}

	// blob still exists although ListBlobs can't see it
	_ = getBlobRetention(ctx, t, cli, container, blobNameFullPath)

	const deleteMarkerName string = string(blob.BlobIDPrefixDeleteMarker) + "_" + blobName
	deleteMarkerFullPath := prefix + deleteMarkerName

	// delete marker file exists
	if err := st.ExtendBlobRetention(ctx, blob.ID(deleteMarkerName), extendOpts); err != nil {
		t.Fatalf("couldn't extend blob: %v", err)
	}

	deleteMarkerRetention := getBlobRetention(ctx, t, cli, container, deleteMarkerFullPath)

	if err := deleteBlob(ctx, cli, container, blobNameFullPath); err != nil {
		t.Fatalf("failed to delete blob: %v", err)
	}
	t.Logf("blob %s deleted", blobNameFullPath)

	err = st.ExtendBlobRetention(ctx, blob.ID(deleteMarkerName), extendOpts)
	if err == nil || !errors.Is(err, blob.ErrOrphanedDeleteMarkerBlob) {
		t.Fatalf("delete marker extension should have had an orphaned blob error but didn't: %v", err)
	}

	deleteMarkerRetentionLatest := getBlobRetention(ctx, t, cli, container, deleteMarkerFullPath)
	if !deleteMarkerRetentionLatest.Equal(deleteMarkerRetention) {
		t.Fatalf("blob retention period should be unchanged. was %v, now %v", deleteMarkerRetention, deleteMarkerRetentionLatest)
	}

	if err := deleteBlob(ctx, cli, container, deleteMarkerFullPath); err != nil {
		t.Fatalf("failed to delete blob: %v", err)
	}
	t.Logf("blob %s deleted", deleteMarkerFullPath)
}

func deleteBlob(ctx context.Context, cli *azblob.Client, container, blob string) error {
	timeout := time.After(15 * time.Second)
	tick := time.Tick(1 * time.Second)
	for {
		select {
		case <-timeout:
			return errors.New("failed to delete blob")
		case <-tick:
			_, err := cli.DeleteBlob(ctx, container, blob, nil)
			if err == nil {
				return nil
			}
		}
	}
}

func getBlobCount(ctx context.Context, t *testing.T, st blob.Storage, prefix blob.ID) int {
	count := 0
	if err := st.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("can't list blobs: %v", err)
	}
	return count
}

func getBlobRetention(ctx context.Context, t *testing.T, cli *azblob.Client, container string, blobName string) time.Time {
	props, err := cli.ServiceClient().
		NewContainerClient(container).
		NewBlobClient(blobName).
		GetProperties(ctx, nil)
	if err != nil {
		t.Fatalf("can't get blob properties: %v", err)
	}
	return *props.ImmutabilityPolicyExpiresOn
}

// getAzureCLI returns a separate client to verify things the Storage interface doesn't support.
func getAzureCLI(t *testing.T, storageAccount, storageKey string) *azblob.Client {
	cred, err := azblob.NewSharedKeyCredential(storageAccount, storageKey)
	if err != nil {
		t.Fatal("can't create new credential")
	}

	storageHostname := fmt.Sprintf("%v.blob.core.windows.net", storageAccount)
	cli, err := azblob.NewClientWithSharedKeyCredential(
		fmt.Sprintf("https://%s/", storageHostname), cred, nil,
	)
	if err != nil {
		t.Fatal("can't create new client")
	}
	return cli
}
