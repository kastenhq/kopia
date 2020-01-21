package endtoend_test

import (
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestRestoreFail(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--keep-latest", strconv.Itoa(1<<31-1))

	scratchDir := makeScratchDir(t)
	sourceDir := filepath.Join(scratchDir, "source")
	targetDir := filepath.Join(scratchDir, "target")
	testenv.MustCreateDirectoryTree(t, sourceDir, testenv.DirectoryTreeOptions{
		Depth:                  2,
		MaxSubdirsPerDirectory: 2,
		MaxFilesPerDirectory:   2,
	})

	beforeBlobList := e.RunAndExpectSuccess(t, "blob", "list")

	_, errOut := e.RunAndExpectSuccessWithErrOut(t, "snapshot", "create", sourceDir)
	snapID := parseSnapID(t, errOut)

	afterBlobList := e.RunAndExpectSuccess(t, "blob", "list")

	newBlobIDs := getNewBlobIDs(beforeBlobList, afterBlobList)

	for _, blobID := range newBlobIDs {
		e.RunAndExpectSuccess(t, "blob", "delete", blobID)
	}

	e.RunAndExpectFailure(t, "snapshot", "restore", snapID, targetDir)
}

func getNewBlobIDs(before []string, after []string) []string {
	newIDMap := make(map[string]struct{})
	// Add all blob IDs seen after the snapshot
	for _, outputStr := range after {
		blobID := parseBlobIDFromBlobList(outputStr)
		newIDMap[blobID] = struct{}{}
	}

	// Remove all blob IDs seen before the snapshot
	for _, outputStr := range before {
		blobID := parseBlobIDFromBlobList(outputStr)
		delete(newIDMap, blobID)
	}

	idList := make([]string, 0, len(newIDMap))
	for blobID := range newIDMap {
		idList = append(idList, blobID)
	}

	return idList
}

func parseBlobIDFromBlobList(str string) string {
	fields := strings.Fields(str)
	if len(fields) > 0 {
		return fields[0]
	}

	return ""
}
