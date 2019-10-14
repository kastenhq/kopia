package cli

import (
	"context"
	"io"
	"os"
	"path"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	restoreSnapshotID = restoreCommands.Arg("snapshot-id", "Snapshot ID from which to restore.").Required().String()
	restoreTargetPath = restoreCommands.Arg("target-path", "Path to which the snapshot must be restored.").Required().String()
)

func runRestoreCommand(ctx context.Context, rep *repo.Repository) error {
	oid, err := parseObjectID(ctx, rep, *restoreSnapshotID)
	if err != nil {
		return err
	}
	targetPath := *restoreTargetPath

	return restoreRecursively(ctx, rep, targetPath, oid)
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
			// Create the directory
			if direrr := os.MkdirAll(restorePath, 0777); direrr != nil {
				return direrr
			}
			if recerr := restoreRecursively(ctx, rep, restorePath+"/", objectID); recerr != nil {
				return recerr
			}
			// Set permissions as stored in the backup
			if cherr := os.Chmod(restorePath, e.Mode()); cherr != nil {
				return cherr
			}
			continue
		}
		// Create the output file
		outFile, err := os.Create(restorePath)
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
		// Set file permissions as stored in the backup
		if cherr := os.Chmod(outFile.Name(), e.Mode()); cherr != nil {
			return cherr
		}
	}

	return nil
}
