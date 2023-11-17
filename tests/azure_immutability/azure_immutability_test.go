package azure_immutability_test

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/subscription/armsubscription"
	"github.com/stretchr/testify/require"
)

const (
	kopiaSubscriptionID                 = "<KOPIA-SUBSCRIPTION-ID-HERE>"
	kopiaResourceGroupName              = "<KOPIA-DEFAULT-RESOURCE-GROUP>" // probably good to reuse existing group name
	kopiaImmutabilityStorageAccountName = "kopiaimmutability"              // this needs to be globally unique
	kopiaDefaultLocation                = "westus3"
	kopiaBucketVersionImmUnlocked       = "kopia-immutability-testing"
)

func TestCreateStorageAccount(t *testing.T) {
	cred := getCreds(t)
	createStorageAccountWithVersionImmutability(t, cred, kopiaSubscriptionID, kopiaResourceGroupName, kopiaImmutabilityStorageAccountName, kopiaDefaultLocation)
}

func createStorageAccountWithVersionImmutability(t *testing.T, cred *azidentity.DefaultAzureCredential, subscriptionID, resourceGroupName, stAccountName, location string) {
	clientFactory, err := armstorage.NewClientFactory(subscriptionID, cred, nil)

	noErrorNorNil(t, clientFactory, err)

	ctx := context.Background()
	ac := clientFactory.NewAccountsClient()

	createParameters := armstorage.AccountCreateParameters{
		Kind: to.Ptr(armstorage.KindStorageV2),
		SKU: &armstorage.SKU{
			Name: to.Ptr(armstorage.SKUNameStandardLRS),
		},
		Location: to.Ptr(location),
		Properties: &armstorage.AccountPropertiesCreateParameters{
			AccessTier:                  to.Ptr(armstorage.AccessTierCool),
			AllowBlobPublicAccess:       to.Ptr(false),
			AllowSharedKeyAccess:        to.Ptr(true),
			AllowCrossTenantReplication: to.Ptr(false),
			EnableHTTPSTrafficOnly:      to.Ptr(true),
			MinimumTLSVersion:           to.Ptr(armstorage.MinimumTLSVersionTLS12),
			ImmutableStorageWithVersioning: &armstorage.ImmutableStorageAccount{
				Enabled: to.Ptr(true),
			},
		},
	}

	poller, err := ac.BeginCreate(ctx, resourceGroupName, stAccountName, createParameters, &armstorage.AccountsClientBeginCreateOptions{})
	noErrorNorNil(t, poller, err)

	resp, err := poller.PollUntilDone(ctx, nil)
	noErrorNorNil(t, resp, err)

	t.Log("account:", resp.Account.Name, resp.Account.ID)
	t.Log("details:", prettyJSON(t, resp.Account))
}

func TestCreateContainerWithVersionImmutabilityPolicyUnlocked(t *testing.T) {
	containerCreate(t, kopiaSubscriptionID, kopiaResourceGroupName, kopiaImmutabilityStorageAccountName, kopiaBucketVersionImmUnlocked, true)
}

func containerCreate(t *testing.T, subscription, resourceGroup, storageAccount, containerName string, immutableStorageVersioning bool) {
	t.Helper()

	cred := getCreds(t)
	clientFactory, err := armstorage.NewClientFactory(subscription, cred, nil)

	noErrorNorNil(t, clientFactory, err)

	bc := clientFactory.NewBlobContainersClient()
	ctx := context.Background()
	container, err := bc.Create(
		ctx,
		resourceGroup,
		storageAccount,
		containerName,
		armstorage.BlobContainer{
			ContainerProperties: &armstorage.ContainerProperties{
				PublicAccess: to.Ptr(armstorage.PublicAccessNone),
				ImmutableStorageWithVersioning: &armstorage.ImmutableStorageWithVersioning{
					Enabled: to.Ptr(immutableStorageVersioning),
				},
			},
		},
		nil,
	)

	noErrorNorNil(t, container, err)
	t.Log("container:", container.Name)
	t.Log("properties:", prettyJSON(t, container))

	// set immutability policy, since it cannot be set on container creation
	p, err := bc.CreateOrUpdateImmutabilityPolicy(
		ctx,
		resourceGroup,
		storageAccount,
		containerName,
		&armstorage.BlobContainersClientCreateOrUpdateImmutabilityPolicyOptions{
			Parameters: &armstorage.ImmutabilityPolicy{
				Properties: &armstorage.ImmutabilityPolicyProperty{
					AllowProtectedAppendWrites:            to.Ptr(false),
					AllowProtectedAppendWritesAll:         to.Ptr(false),
					ImmutabilityPeriodSinceCreationInDays: to.Ptr(int32(1)),
					State:                                 to.Ptr(armstorage.ImmutabilityPolicyStateUnlocked),
				},
			},
		},
	)

	require.NoError(t, err)
	require.NotZero(t, p)

	t.Log("container:", toString(p.Name))
	t.Log("policy:", prettyJSON(t, p.ImmutabilityPolicy))
}

func TestGetImmutabilityPolicy(t *testing.T) {
	cred := getCreds(t)
	clientFactory, err := armstorage.NewClientFactory(kopiaSubscriptionID, cred, nil)

	noErrorNorNil(t, clientFactory, err)

	bc := clientFactory.NewBlobContainersClient()
	ctx := context.Background()

	require.NotNil(t, bc)

	bgr, err := bc.Get(ctx, kopiaResourceGroupName, kopiaImmutabilityStorageAccountName, kopiaBucketVersionImmUnlocked, &armstorage.BlobContainersClientGetOptions{})

	require.NoError(t, err)
	require.NotZero(t, bgr)

	t.Log("container:", prettyJSON(t, bgr))

	ipr, err := bc.GetImmutabilityPolicy(ctx, kopiaResourceGroupName, kopiaImmutabilityStorageAccountName, kopiaBucketVersionImmUnlocked, &armstorage.BlobContainersClientGetImmutabilityPolicyOptions{})

	require.NoError(t, err)
	require.NotZero(t, ipr)

	t.Log("policy:", prettyJSON(t, ipr.ImmutabilityPolicy))
}

func TestGetSubscriptionInfo(t *testing.T) {
	cred := getCreds(t)

	subs, err := armsubscription.NewSubscriptionsClient(cred, nil)
	require.NoError(t, err)

	ctx := context.Background()
	pager := subs.NewListPager(&armsubscription.SubscriptionsClientListOptions{})

	for pager.More() {
		p, err := pager.NextPage(ctx)

		require.NoError(t, err)
		for _, v := range p.Value {
			require.NotNil(t, v)
			sid := toString(v.SubscriptionID)
			t.Log("subscriptionID:", sid)

			info, err := subs.Get(ctx, sid, nil)
			noErrorNorNil(t, info, err)
			t.Log("subscription info:", string(prettyJSON(t, info)))
			listStorageAccountsAndContainersForSubscription(t, ctx, cred, sid)
		}
	}
}

func TestListStorageAccounts(t *testing.T) {
	cred := getCreds(t)
	ctx := context.Background()

	listStorageAccountsForSubscription(t, ctx, cred, kopiaSubscriptionID)
}

func getCreds(t *testing.T) *azidentity.DefaultAzureCredential {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	require.NoError(t, err)

	return cred
}

func prettyJSON(t *testing.T, v any) string {
	return string(prettyJSONBytes(t, v))
}

func prettyJSONBytes(t *testing.T, v any) []byte {
	b, err := json.MarshalIndent(v, "", " ")
	require.NoError(t, err)

	return b
}

func toString(s *string) string {
	if s != nil {
		return *s
	}

	return ""
}

func noErrorNorNil(t *testing.T, v any, err error) {
	t.Helper()

	require.NoError(t, err)
	require.NotNil(t, v)
}

func listStorageAccountsAndContainersForSubscription(t *testing.T, ctx context.Context, cred *azidentity.DefaultAzureCredential, subscriptionID string) {
	clientFactory, err := armstorage.NewClientFactory(subscriptionID, cred, nil)

	noErrorNorNil(t, clientFactory, err)

	ac := clientFactory.NewAccountsClient()
	bc := clientFactory.NewBlobContainersClient()
	resourceGroupRE := regexp.MustCompile(`/resourceGroups/(.*?)/`)

	t.Log("storage accounts:")
	iterateStorageAccounts(t, ctx, ac, func(i int, a *armstorage.Account) {
		require.NotNil(t, a.Name)

		accountName := toString(a.Name)

		// infer resource group
		matches := resourceGroupRE.FindStringSubmatch(toString(a.ID))
		if len(matches) < 2 {
			t.Log("could not find resource group for storage account:", accountName)
			return
		}

		resourceGroupName := matches[1]
		t.Log(i, ":", accountName, "resource group:", resourceGroupName)

		listBlobContainers(t, ctx, bc, resourceGroupName, accountName)
	})
}

func listStorageAccountsForSubscription(t *testing.T, ctx context.Context, cred *azidentity.DefaultAzureCredential, subscriptionID string) {
	clientFactory, err := armstorage.NewClientFactory(subscriptionID, cred, nil)

	noErrorNorNil(t, clientFactory, err)

	ac := clientFactory.NewAccountsClient()
	listStorageAccounts(t, ctx, ac)
}

func listStorageAccounts(t *testing.T, ctx context.Context, ac *armstorage.AccountsClient) {
	t.Log("storage accounts:")
	iterateStorageAccounts(t, ctx, ac, func(i int, a *armstorage.Account) {
		require.NotNil(t, a.Name)
		t.Log(i, ":", *a.Name)
	})
}

func iterateStorageAccounts(t *testing.T, ctx context.Context, ac *armstorage.AccountsClient, cb func(int, *armstorage.Account)) {
	pager := ac.NewListPager(nil)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)

		for i, a := range page.Value {
			require.NotNil(t, a)
			cb(i, a)
		}
	}
}

func listBlobContainers(t *testing.T, ctx context.Context, bc *armstorage.BlobContainersClient, resourceGroupName, accountName string) {
	containers, err := getBlobContainers(ctx, bc, resourceGroupName, accountName)
	require.NoError(t, err)
	require.NotNil(t, containers)

	t.Log("storage containers:")

	for i, c := range containers {
		t.Log(i, ":", toString(c.Name))
	}
}

func getBlobContainers(ctx context.Context, bc *armstorage.BlobContainersClient, resourceGroupName, storageAccountName string) ([]*armstorage.ListContainerItem, error) {
	containerItemsPager := bc.NewListPager(resourceGroupName, storageAccountName, nil)
	listItems := make([]*armstorage.ListContainerItem, 0)

	for containerItemsPager.More() {
		pageResp, err := containerItemsPager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		listItems = append(listItems, pageResp.ListContainerItems.Value...)
	}

	return listItems, nil
}
