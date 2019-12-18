package gc

import (
	"sync/atomic"
	"time"
)

// CountAndSum keeps track of a count and associated total size sum (bytes)
type CountAndSum interface {
	Add(size uint32) (count uint32, sum int64)
	Approximate() (count uint32, sum int64)
}

// Stats contains statistics about a GC run
type Stats struct {
	unusedBytes, inUseBytes, systemBytes, tooRecentBytes int64
	unusedCount, inUseCount, systemCount, tooRecentCount uint32

	FindInUseDuration  time.Duration
	FindUnusedDuration time.Duration
}

// Unused returns the stat for total unused bytes and unused content count
func (s Stats) Unused() CountAndSum {
	return countAndSum{sum: &s.unusedBytes, count: &s.unusedCount}
}

// InUse returns the stat for total used bytes and used content count
func (s Stats) InUse() CountAndSum {
	return countAndSum{sum: &s.inUseBytes, count: &s.inUseCount}
}

// System returns the stat for total system bytes and system content count
func (s Stats) System() CountAndSum {
	return countAndSum{sum: &s.systemBytes, count: &s.systemCount}
}

// TooRecent returns the stat for total bytes and count for contents that are
// too recent to garbage collect.
func (s Stats) TooRecent() CountAndSum {
	return countAndSum{sum: &s.tooRecentBytes, count: &s.tooRecentCount}
}

type countAndSum struct {
	sum   *int64
	count *uint32
}

// Add adds size to s and returns approximate values for the current count
// and total bytes
func (s countAndSum) Add(size uint32) (count uint32, sum int64) {
	return atomic.AddUint32(s.count, 1), atomic.AddInt64(s.sum, int64(size))
}

// Approximate returns an approximation of the current count and sum values.
// It is approximate because retrieving both values is not an atomic operation.
func (s countAndSum) Approximate() (count uint32, sum int64) {
	return atomic.LoadUint32(s.count), atomic.LoadInt64(s.sum)
}
