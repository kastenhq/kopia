package pathlock

import (
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestPathLockBasic(t *testing.T) {
	pl := NewLocker()

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
		lock1 := pl.Lock(tc.path1)

		triggerCh := make(chan struct{})
		trigger := false

		go func() {
			lock2 := pl.Lock(tc.path2)
			trigger = true
			triggerCh <- struct{}{}
			lock2.Unlock()
		}()

		time.Sleep(10 * time.Millisecond)

		if trigger == true {
			t.Fatalf("Lock unsuccessful")
		}

		lock1.Unlock()

		<-triggerCh

		if trigger != true {
			t.Fatalf("Unlock unsuccessful")
		}
	}
}

func TestPathLockWithoutBlock(t *testing.T) {
	pl := NewLocker()

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
			lock2 := pl.Lock(tc.path2)

			trigger = true

			goroutineLockedWg.Done()

			time.Sleep(10 * time.Millisecond)

			trigger = false

			lock2.Unlock()
		}()

		// Wait for the goroutine to lock
		goroutineLockedWg.Wait()

		// This should not block; the paths should not interfere
		lock1 := pl.Lock(tc.path1)

		if trigger != true {
			t.Fatalf("Lock blocked")
		}

		lock1.Unlock()

		time.Sleep(20 * time.Millisecond)

		if trigger != false {
			t.Fatalf("Trigger should have been set false")
		}
	}
}

func TestPathLockRace(t *testing.T) {
	pl := NewLocker()

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
			lock := pl.Lock(path)
			counter++
			lock.Unlock()
		}()
	}

	wg.Wait()

	if counter != numGoroutines {
		t.Fatalf("counter %v != numgoroutines %v", counter, numGoroutines)
	}
}
