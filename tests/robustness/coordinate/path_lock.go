package coordinate

import (
	"path/filepath"
	"strings"
	"sync"
)

type PathLocker interface {
	Lock(path string)
	Unlock(path string)
}

type PathLock struct {
	mu          sync.Mutex
	lockedPaths map[string](chan struct{})
}

func NewPathLock() *PathLock {
	return &PathLock{
		lockedPaths: make(map[string](chan struct{})),
	}
}

func (pl *PathLock) Lock(path string) {
	for {
		ch := pl.tryToLockPath(path)
		if ch == nil {
			break
		}

		<-ch
	}
}

func (pl *PathLock) tryToLockPath(path string) chan struct{} {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	for lockedPath, ch := range pl.lockedPaths {
		if isInPath(path, lockedPath) || isInPath(lockedPath, path) {
			return ch
		}
	}

	pl.lockedPaths[path] = make(chan struct{})

	return nil
}

func (pl *PathLock) Unlock(path string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	close(pl.lockedPaths[path])
	delete(pl.lockedPaths, path)
}

func isInPath(path1, path2 string) bool {
	relFP, err := filepath.Rel(path2, path1)
	if err != nil {
		// Not sure - just wait anyway?
		return true
	}

	// If the relative path contains "..", it is not locked
	// by this hold, because it is a cousin path. Only children (recursive)
	// and the path itself are considered locked by this hold.
	return !strings.Contains(relFP, "..")
}
