// Package pathlock defines a PathLocker interface and an implementation
// that will synchronize based on filepath.
package pathlock

import (
	"path/filepath"
	"strings"
	"sync"
)

// PathLocker is an interface for synchronizing on a given filepath.
// A call to Lock a given path will block any asynchronous calls to Lock
// that same path, or any parent or child path in the same sub-tree.
// For example:
// 	- Lock path /a/b/c
//		- Blocks a Lock call for the same path /a/b/c
//		- Blocks a Lock call for path /a/b or /a
//		- Blocks a Lock call for path /a/b/c/d
//		- Allows a Lock call for path /a/b/x
//		- Allows a Lock call for path /a/x
type PathLocker interface {
	Lock(path string)
	Unlock(path string)
}

var _ PathLocker = (*PathLock)(nil)

// PathLock is a path-based mutex mechanism that allows for synchronization
// along subpaths. A call to Lock will block as long as the requested path
// is equal to, or otherwise in the path of (e.g. parent/child) another path
// that has already been Locked. The thread will be blocked until the holder
// of the lock calls Unlock.
type PathLock struct {
	mu          sync.Mutex
	lockedPaths map[string]chan struct{}
}

// NewPathLock instantiates a new PathLock and returns its pointer.
func NewPathLock() *PathLock {
	return &PathLock{
		lockedPaths: make(map[string]chan struct{}),
	}
}

// Lock will lock the given path, preventing concurrent calls to Lock
// for that path, or any parent/child path, until Unlock has been called.
// Any concurrent Lock calls will block until that path is available.
func (pl *PathLock) Lock(path string) {
	for {
		ch := pl.tryToLockPath(path)
		if ch == nil {
			break
		}

		<-ch
	}
}

// tryToLockPath is a helper for locking a given path/subpath.
// It locks the common mutex while accessing the internal map of locked
// paths. Each element in the list of locked paths is tested for whether
// or not it is within the same subtree as the requested path to lock.
//
// If none of the already-reserved paths coincide with this one, this
// goroutine can safely lock this path. To do so, it creates a
// new map entry whose key is the locked path, and whose value is
// a channel that other goroutines can wait on, should there be
// a collision.
//
// If this goroutine DOES find a conflicting path, that path's
// channel is returned. The caller can wait on that channel. After
// the channel is closed, the caller should try again by calling
// `tryToLockPath` until no channel is returned (indicating the lock
// has been claimed).
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

// Unlock will unlock the given path. It is assumed that Lock
// has already been called, and that Unlock will be called once
// and only once with the exact path provided to the Lock function.
func (pl *PathLock) Unlock(path string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	close(pl.lockedPaths[path])
	delete(pl.lockedPaths, path)
}

// isInPath is a helper to determine whether one path is
// either the same as another, or a child path (recursively) of it.
func isInPath(path1, path2 string) bool {
	relFP, err := filepath.Rel(path2, path1)
	if err != nil {
		// Not sure - just wait anyway?
		return true
	}

	// If the relative path contains "..", this function will
	// return false, because it is a cousin path. Only children (recursive)
	// and the path itself will return true.
	return !strings.Contains(relFP, "..")
}
