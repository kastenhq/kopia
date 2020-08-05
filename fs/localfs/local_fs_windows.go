package localfs

import (
	"os"

	"github.com/kopia/kopia/fs"
)

func platformSpecificOwnerInfo(fi os.FileInfo) fs.OwnerInfo {
	return fs.OwnerInfo{}
}

// PlatformSpecificChown does nothing on Windows. Owner info is ignored
// so changing owner will do nothing.
func PlatformSpecificChown(targetPath string, oi fs.OwnerInfo) error {
	return nil
}
