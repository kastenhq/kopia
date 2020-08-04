// +build !windows

package restore

import (
	"os"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/pkg/errors"
)

// set permission, modification time and user/group ids on targetPath
func (o *FilesystemOutput) setAttributes(targetPath string, e fs.Entry) error {
	const modBits = os.ModePerm | os.ModeSetgid | os.ModeSetuid | os.ModeSticky

	le, err := localfs.NewEntry(targetPath)
	if err != nil {
		return errors.Wrap(err, "could not create local FS entry for "+targetPath)
	}

	// Set owner user and group from e
	if le.Owner() != e.Owner() {
		if err = os.Chown(targetPath, int(e.Owner().UserID), int(e.Owner().GroupID)); err != nil && !os.IsPermission(err) {
			return errors.Wrap(err, "could not change owner/group for "+targetPath)
		}
	}

	// Set file permissions from e
	if (le.Mode() & modBits) != (e.Mode() & modBits) {
		if err = os.Chmod(targetPath, e.Mode()&modBits); err != nil && !os.IsPermission(err) {
			return errors.Wrap(err, "could not change permissions on "+targetPath)
		}
	}

	// Set mod time from e
	if !le.ModTime().Equal(e.ModTime()) {
		// Note: Set atime to ModTime as well
		if err = os.Chtimes(targetPath, e.ModTime(), e.ModTime()); err != nil && !os.IsPermission(err) {
			return errors.Wrap(err, "could not change mod time on "+targetPath)
		}
	}

	return nil
}
