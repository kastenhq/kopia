// +build !windows

package localfs

import (
	"os"
	"syscall"

	"github.com/kopia/kopia/fs"
)

func platformSpecificOwnerInfo(fi os.FileInfo) fs.OwnerInfo {
	var oi fs.OwnerInfo
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		oi.UserID = stat.Uid
		oi.GroupID = stat.Gid
	}

	return oi
}

// PlatformSpecificChown will issue os.Chown on all operating systems
// except Windows.
func PlatformSpecificChown(targetPath string, oi fs.OwnerInfo) error {
	return os.Chown(targetPath, int(oi.UserID), int(oi.GroupID))
}
