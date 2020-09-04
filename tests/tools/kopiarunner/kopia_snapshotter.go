package kopiarunner

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/tests/robustness/snap"
)

var _ snap.Snapshotter = &KopiaSnapshotter{}

const (
	contentCacheSizeMBFlag  = "--content-cache-size-mb"
	metadataCacheSizeMBFlag = "--metadata-cache-size-mb"
	noCheckForUpdatesFlag   = "--no-check-for-updates"
	noProgressFlag          = "--no-progress"
	parallelFlag            = "--parallel"
)

// Flag value settings
var (
	contentCacheSizeSettingMB  = strconv.Itoa(500)
	metadataCacheSizeSettingMB = strconv.Itoa(500)
	parallelSetting            = strconv.Itoa(8)
)

// KopiaSnapshotter implements the Snapshotter interface using Kopia commands
type KopiaSnapshotter struct {
	Runner *Runner
}

// NewKopiaSnapshotter instantiates a new KopiaSnapshotter and returns its pointer.
func NewKopiaSnapshotter(baseDir string) (*KopiaSnapshotter, error) {
	runner, err := NewRunner(baseDir)
	if err != nil {
		return nil, err
	}

	return &KopiaSnapshotter{
		Runner: runner,
	}, nil
}

// Cleanup cleans up the kopia Runner.
func (ks *KopiaSnapshotter) Cleanup() {
	if ks.Runner != nil {
		ks.Runner.Cleanup()
	}
}

func (ks *KopiaSnapshotter) repoConnectCreate(op string, args ...string) error {
	args = append([]string{"repo", op}, args...)

	args = append(args,
		contentCacheSizeMBFlag, contentCacheSizeSettingMB,
		metadataCacheSizeMBFlag, metadataCacheSizeSettingMB,
		noCheckForUpdatesFlag,
	)

	_, _, err := ks.Runner.Run(args...)

	return err
}

// CreateRepo creates a kopia repository with the provided arguments
func (ks *KopiaSnapshotter) CreateRepo(args ...string) (err error) {
	return ks.repoConnectCreate("create", args...)
}

// ConnectRepo connects to the repository described by the provided arguments
func (ks *KopiaSnapshotter) ConnectRepo(args ...string) (err error) {
	return ks.repoConnectCreate("connect", args...)
}

// ConnectOrCreateRepo attempts to connect to a repo described by the provided
// arguments, and attempts to create it if connection was unsuccessful.
func (ks *KopiaSnapshotter) ConnectOrCreateRepo(args ...string) error {
	err := ks.ConnectRepo(args...)
	if err == nil {
		return nil
	}

	return ks.CreateRepo(args...)
}

// ConnectOrCreateS3 attempts to connect to a kopia repo in the s3 bucket identified
// by the provided bucketName, at the provided path prefix. It will attempt to
// create one there if connection was unsuccessful.
func (ks *KopiaSnapshotter) ConnectOrCreateS3(bucketName, pathPrefix string) error {
	args := []string{"s3", "--bucket", bucketName, "--prefix", pathPrefix}

	return ks.ConnectOrCreateRepo(args...)
}

// ConnectOrCreateS3WithServer attempts to connect or create S3 bucket, but with Client/Server Model
func (ks *KopiaSnapshotter) ConnectOrCreateS3WithServer(serverAddr, bucketName, pathPrefix string) (*exec.Cmd, error) {
	args := []string{"s3", "--bucket", bucketName, "--prefix", pathPrefix}
	return ks.createAndConnectServer(serverAddr, args...)
}

// ConnectOrCreateFilesystem attempts to connect to a kopia repo in the local
// filesystem at the path provided. It will attempt to create one there if
// connection was unsuccessful.
func (ks *KopiaSnapshotter) ConnectOrCreateFilesystem(repoPath string) error {
	args := []string{"filesystem", "--path", repoPath}

	return ks.ConnectOrCreateRepo(args...)
}

// ConnectOrCreateFilesystemWithServer attempts to connect or create repo in local filesystem,
// but with Client/Server Model
func (ks *KopiaSnapshotter) ConnectOrCreateFilesystemWithServer(repoPath, serverAddr string) (*exec.Cmd, error) {
	args := []string{"filesystem", "--path", repoPath}
	return ks.createAndConnectServer(serverAddr, args...)
}

// CreateSnapshot implements the Snapshotter interface, issues a kopia snapshot
// create command on the provided source path.
func (ks *KopiaSnapshotter) CreateSnapshot(source string) (snapID string, err error) {
	_, errOut, err := ks.Runner.Run("snapshot", "create", parallelFlag, parallelSetting, noProgressFlag, source)
	if err != nil {
		return "", err
	}

	return parseSnapID(strings.Split(errOut, "\n"))
}

// RestoreSnapshot implements the Snapshotter interface, issues a kopia snapshot
// restore command of the provided snapshot ID to the provided restore destination.
func (ks *KopiaSnapshotter) RestoreSnapshot(snapID, restoreDir string) (err error) {
	_, _, err = ks.Runner.Run("snapshot", "restore", snapID, restoreDir)
	return err
}

// DeleteSnapshot implements the Snapshotter interface, issues a kopia snapshot
// delete of the provided snapshot ID.
func (ks *KopiaSnapshotter) DeleteSnapshot(snapID string) (err error) {
	_, _, err = ks.Runner.Run("snapshot", "delete", snapID, "--delete")
	return err
}

// RunGC implements the Snapshotter interface, issues a gc command to the kopia repo
func (ks *KopiaSnapshotter) RunGC() (err error) {
	_, _, err = ks.Runner.Run("snapshot", "gc")
	return err
}

// ListSnapshots implements the Snapshotter interface, issues a kopia snapshot
// list and parses the snapshot IDs.
func (ks *KopiaSnapshotter) ListSnapshots() ([]string, error) {
	snapIDListMan, err := ks.snapIDsFromManifestList()
	if err != nil {
		return nil, err
	}

	// Validate the list against kopia snapshot list --all
	snapIDListSnap, err := ks.snapIDsFromSnapListAll()
	if err != nil {
		return nil, err
	}

	if got, want := len(snapIDListSnap), len(snapIDListMan); got != want {
		return nil, errors.Errorf("Snapshot list len (%d) does not match manifest list len (%d)", got, want)
	}

	return snapIDListMan, nil
}

func (ks *KopiaSnapshotter) snapIDsFromManifestList() ([]string, error) {
	stdout, _, err := ks.Runner.Run("manifest", "list")
	if err != nil {
		return nil, errors.Wrap(err, "failure during kopia manifest list")
	}

	return parseManifestListForSnapshotIDs(stdout), nil
}

func (ks *KopiaSnapshotter) snapIDsFromSnapListAll() ([]string, error) {
	// Validate the list against kopia snapshot list --all
	stdout, _, err := ks.Runner.Run("snapshot", "list", "--all", "--manifest-id", "--show-identical")
	if err != nil {
		return nil, errors.Wrap(err, "failure during kopia snapshot list")
	}

	return parseSnapshotListForSnapshotIDs(stdout), nil
}

// Run implements the Snapshotter interface, issues an arbitrary kopia command and returns
// the output.
func (ks *KopiaSnapshotter) Run(args ...string) (stdout, stderr string, err error) {
	return ks.Runner.Run(args...)
}

// CreateServer creates a new instance of Kopia Server with provided address
func (ks *KopiaSnapshotter) CreateServer(addr string, args ...string) (*exec.Cmd, error) {
	args = append([]string{"server", "start", "--address", addr}, args...)

	cmd, err := ks.Runner.RunAsync(args...)
	if err != nil {
		return nil, err
	}

	err = ks.waitUntilServerStarted(context.TODO(), addr)
	return cmd, nil
}

// ConnectServer creates a new client, and connect it to Kopia Server with provided address.
func (ks *KopiaSnapshotter) ConnectServer(addr string, args ...string) error {
	args = append([]string{"repo", "connect", "server", "--url", fmt.Sprintf("http://%s", addr)}, args...)
	_, _, err := ks.Runner.Run(args...)

	return err
}

func parseSnapID(lines []string) (string, error) {
	pattern := regexp.MustCompile(`Created snapshot with root [\S]+ and ID ([\S]+)`)

	for _, l := range lines {
		match := pattern.FindAllStringSubmatch(l, 1)
		if len(match) > 0 && len(match[0]) > 1 {
			return match[0][1], nil
		}
	}

	return "", errors.New("snap ID could not be parsed")
}

func parseSnapshotListForSnapshotIDs(output string) []string {
	var ret []string

	lines := strings.Split(output, "\n")
	for _, l := range lines {
		fields := strings.Fields(l)

		for _, f := range fields {
			spl := strings.Split(f, "manifest:")
			if len(spl) == 2 {
				ret = append(ret, spl[1])
			}
		}
	}

	return ret
}

func parseManifestListForSnapshotIDs(output string) []string {
	var ret []string

	lines := strings.Split(output, "\n")
	for _, l := range lines {
		fields := strings.Fields(l)

		typeFieldIdx := 5
		if len(fields) > typeFieldIdx {
			if fields[typeFieldIdx] == "type:snapshot" {
				ret = append(ret, fields[0])
			}
		}
	}

	return ret
}

// waitUntilServerStarted returns error if the Kopia API server fails to start before timeout
func (ks *KopiaSnapshotter) waitUntilServerStarted(ctx context.Context, addr string) error {
	statusArgs := append([]string{"server", "status", "--address", fmt.Sprintf("http://%s", addr)})
	if err := retry.PeriodicallyNoValue(ctx, 1*time.Second, 180, "wait for server start", func() error {
		_, _, err := ks.Runner.Run(statusArgs...)
		return err
	}, retry.Always); err != nil {
		return errors.New("server failed to start")
	}

	return nil
}

// createAndConnectServer creates Repository and a server/client model for interaction
func (ks *KopiaSnapshotter) createAndConnectServer(serverAddr string, args ...string) (*exec.Cmd, error) {
	var cmd *exec.Cmd
	var err error

	if err = ks.ConnectOrCreateRepo(args...); err != nil {
		return nil, err
	}

	if cmd, err = ks.CreateServer(serverAddr); err != nil {
		return nil, err
	}

	if err = ks.ConnectServer(serverAddr); err != nil {
		return nil, err
	}

	return cmd, err
}
