package endtoend_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotFail(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--keep-latest", strconv.Itoa(1<<31-1))

	parentDirOfSource := makeScratchDir(t)
	source := filepath.Join(parentDirOfSource, "source")

	// Test snapshot of nonexistent directory fails
	e.RunAndExpectFailure(t, "snapshot", "create", source)

	testenv.MustCreateDirectoryTree(t, source, testenv.DirectoryTreeOptions{
		Depth:                  2,
		MaxSubdirsPerDirectory: 1,
		MaxFilesPerDirectory:   1,
	})

	// Create snapshot
	e.RunAndExpectSuccess(t, "snapshot", "create", source)

	numSuccessfulSnapshots := 1

	// Test the root dir permissions
	for _, tc := range []struct {
		desc      string
		parentDir string
	}{
		{
			desc:      "Modify permissions of the snapshot root directory",
			parentDir: filepath.Dir(source),
		},
		{
			desc:      "Modify permissions of entries in the snapshot root dir",
			parentDir: source,
		},
		{
			desc:      "Modify permissions of entries in a subdir of the snapshot root",
			parentDir: findASubDirFilePath(t, source),
		},
	} {
		t.Log(tc.desc)
		numSuccessfulSnapshots += testPermissions(e, t, source, tc.parentDir)
	}

	// check the number of snapshots that succeeded match the length of
	// a snap list output
	si := e.ListSnapshotsAndExpectSuccess(t)
	if got, want := len(si), 1; got != want {
		t.Fatalf("got %v sources, wanted %v", got, want)
	}

	if got, want := len(si[0].Snapshots), numSuccessfulSnapshots; got != want {
		t.Fatalf("got %v snapshots, wanted %v", got, want)
	}
}

func findASubDirFilePath(t *testing.T, parent string) string {
	fileInfoList, err := ioutil.ReadDir(parent)
	testenv.AssertNoError(t, err)

	for _, fi := range fileInfoList {
		if fi.IsDir() {
			return filepath.Join(parent, fi.Name())
		}
	}

	t.Fatalf("Could not find a subdirectory in parent dir %v", parent)
	return ""
}

// Perm constants
const (
	execOffset     = 0
	readOffset     = 2
	userPermOffset = 6
)

// testPermissions iterates over readable and executable permission states, testing
// files and directories (if present). It issues the kopia snapshot command
// against "source" and will test permissions against all entries in "parentDir".
// It returns the number of successful snapshot operations.
func testPermissions(e *testenv.CLITest, t *testing.T, source, parentDir string) int {
	t.Helper()

	fileList, err := ioutil.ReadDir(parentDir)
	testenv.AssertNoError(t, err)

	var numSuccessfulSnapshots int

	for _, changeFile := range fileList {
		// Iterate over all permission bit configurations
		for _, readPermission := range []uint32{0, 1} {
			for _, executePermission := range []uint32{0, 1} {
				name := changeFile.Name()
				mode := changeFile.Mode()
				fp := filepath.Join(parentDir, name)
				perm := readPermission<<readOffset | executePermission<<execOffset
				chmod := os.FileMode(perm << userPermOffset)
				t.Logf("Chmod: path: %s, isDir: %v, prevMode: %v, newMode: %v", fp, changeFile.IsDir(), mode, chmod)

				err := os.Chmod(fp, chmod)
				testenv.AssertNoError(t, err)

				// Directory listing will fail if either the read or executed permissions are unset on the directory itself
				if changeFile.IsDir() && readPermission&executePermission == 0 {
					e.RunAndExpectFailure(t, "snapshot", "create", source)
				} else {
					// Currently by default, the uploader has IgnoreFileErrors set to true.
					// Expect warning and successful snapshot creation
					e.RunAndExpectSuccess(t, "snapshot", "create", source)
					numSuccessfulSnapshots++
				}

				// Change permissions back and expect success
				os.Chmod(fp, mode.Perm())
				e.RunAndExpectSuccess(t, "snapshot", "create", source)
				numSuccessfulSnapshots++
			}
		}
	}

	return numSuccessfulSnapshots
}
