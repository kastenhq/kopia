package pathlock

import (
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestPathLockBasic(t *testing.T) {
	pl := NewPathLock()

	for ti, tc := range []struct {
		name  string
		path1 string
		path2 string
	}{
		{
			name:  "same path",
			path1: "/a/b/c",
			path2: "/a/b/c",
		},
		{
			name:  "path2 is a descendent of path1",
			path1: "/a/b/c",
			path2: "/a/b/c/d/e",
		},
		{
			name:  "path1 is a descendent of path2",
			path1: "/a/b/c/d/e",
			path2: "/a/b/c",
		},
	} {
		t.Log(ti, tc.name)
		pl.Lock(tc.path1)

		triggerCh := make(chan struct{})
		trigger := false

		go func() {
			pl.Lock(tc.path2)
			trigger = true
			triggerCh <- struct{}{}
			pl.Unlock(tc.path2)
		}()

		time.Sleep(10 * time.Millisecond)

		if trigger == true {
			t.Fatalf("Lock unsuccessful")
		}

		pl.Unlock(tc.path1)

		<-triggerCh

		if trigger != true {
			t.Fatalf("Unlock unsuccessful")
		}
	}
}

func TestPathLockWithoutBlock(t *testing.T) {
	pl := NewPathLock()

	for ti, tc := range []struct {
		name  string
		path1 string
		path2 string
	}{
		{
			name:  "same parent",
			path1: "/a/b/c",
			path2: "/a/b/d",
		},
		{
			name:  "same grandparent",
			path1: "/a/b/c/x",
			path2: "/a/b/d/y",
		},
		{
			name:  "differing depths",
			path1: "/a/b/c/d/e",
			path2: "/a/b/c/x",
		},
	} {
		t.Log(ti, tc.name)

		goroutineLockedWg := new(sync.WaitGroup)
		goroutineLockedWg.Add(1)

		trigger := false

		go func() {
			pl.Lock(tc.path2)

			trigger = true

			goroutineLockedWg.Done()

			time.Sleep(10 * time.Millisecond)

			trigger = false

			pl.Unlock(tc.path2)
		}()

		// Wait for the goroutine to lock
		goroutineLockedWg.Wait()

		// This should not block; the paths should not interfere
		pl.Lock(tc.path1)

		if trigger != true {
			t.Fatalf("Lock blocked")
		}

		pl.Unlock(tc.path1)

		time.Sleep(20 * time.Millisecond)

		if trigger != false {
			t.Fatalf("Trigger should have been set false")
		}
	}
}

func TestPathLockRace(t *testing.T) {
	pl := NewPathLock()

	counter := 0

	wg := new(sync.WaitGroup)

	numGoroutines := 100
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Pick from three different path values that should all be
			// covered by the same lock.
			path := "/some/path/a/b/c"
			for i := 0; i < rand.Intn(3); i++ {
				path = filepath.Dir(path)
			}
			pl.Lock(path)
			counter++
			pl.Unlock(path)
		}()
	}

	wg.Wait()

	if counter != numGoroutines {
		t.Fatalf("counter %v != numgoroutines %v", counter, numGoroutines)
	}
}
