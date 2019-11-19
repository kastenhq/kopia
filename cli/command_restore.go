package cli

import (
	"context"
	"io"
	"os"
	"path"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/pkg/errors"
)

var (
	restoreRootID     = restoreCommands.Arg("root-id", "Snapshot Root ID from which to restore.").Required().String()
	restoreTargetPath = restoreCommands.Arg("target-path", "Path to which the snapshot must be restored.").Required().String()
)

func runRestoreCommand(ctx context.Context, rep *repo.Repository) error {
	oid, err := parseObjectID(ctx, rep, *restoreRootID)
	if err != nil {
		return err
	}

	return restoreRecursively(ctx, rep, *restoreTargetPath, oid)
}

func init() {
	restoreCommands.Action(repositoryAction(runRestoreCommand))
}

func restoreRecursively(ctx context.Context, rep *repo.Repository, targetPath string, oid object.ID) error {
	d := snapshotfs.DirectoryEntry(rep, oid, nil)

	entries, err := d.Readdir(ctx)
	if err != nil {
		return err
	}

	for _, e := range entries {
		restorePath := path.Join(targetPath, e.Name())
		objectID := e.(object.HasObjectID).ObjectID()
		// Restore directories recursively
		if e.Mode().IsDir() {
			if direrr := createDirectory(restorePath); direrr != nil {
				return direrr
			}
			if recerr := restoreRecursively(ctx, rep, restorePath+"/", objectID); recerr != nil {
				return recerr
			}
			// Set permissions as stored in the snapshot
			if cherr := os.Chmod(restorePath, e.Mode()); cherr != nil && !os.IsPermission(cherr) {
				return cherr
			}
			continue
		}
		outFile, err := createFile(restorePath)
		if err != nil {
			return err
		}
		defer outFile.Close() //nolint:errcheck
		// Open the repo object and copy data into the new file
		r, err := rep.Objects.Open(ctx, objectID)
		if err != nil {
			return err
		}
		defer r.Close() //nolint:errcheck
		if _, cerr := io.Copy(outFile, r); cerr != nil {
			return cerr
		}
		// Set file permissions as stored in the snapshot
		if cherr := os.Chmod(outFile.Name(), e.Mode()); cherr != nil && !os.IsPermission(cherr) {
			return cherr
		}
	}

	return nil
}

func createDirectory(path string) error {
	stat, err := os.Stat(path)
	switch {
	case err == nil:
		if stat.Mode().IsRegular() {
			return errors.New("path already exists as a file")
		}
		return nil
	case os.IsNotExist(err):
		return os.MkdirAll(path, 0700)
	default:
		return err
	}
}

func createFile(path string) (*os.File, error) {
	_, err := os.Stat(path)
	switch {
	case err == nil:
		return nil, errors.New("unable to create file: already exists")
	case os.IsNotExist(err):
		return os.Create(path)
	default:
		return nil, err
	}
}
