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
			name:  "Blocks a Lock call for the same path /a/b/c",
			path1: "/a/b/c",
			path2: "/a/b/c",
		},
		{
			name:  "Blocks a Lock call for path /a/b/c/d",
			path1: "/a/b/c",
			path2: "/a/b/c/d",
		},
		{
			name:  "Blocks a Lock call for path /a/b",
			path1: "/a/b/c",
			path2: "/a/b",
		},
		{
			name:  "Blocks a Lock call for path /a",
			path1: "/a/b/c",
			path2: "/a",
		},
	} {
		t.Log(ti, tc.name)
		lock1, err := pl.Lock(tc.path1)
		if err != nil {
			t.Fatalf("Unexpected path lock error: %v", err)
		}

		triggerCh := make(chan struct{})
		trigger := false
		var path2Err error

		go func() {
			lock2, err := pl.Lock(tc.path2)
			if err != nil {
				path2Err = err
				close(triggerCh)
				return
			}

			trigger = true
			close(triggerCh)
			lock2.Unlock()
		}()

		time.Sleep(10 * time.Millisecond)

		if trigger == true {
			t.Fatalf("Lock unsuccessful")
		}

		lock1.Unlock()

		<-triggerCh

		if path2Err != nil {
			t.Fatalf("Error in second lock path: %v", path2Err)
		}

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
			name:  "Allows a Lock call for path /a/b/x",
			path1: "/a/b/c",
			path2: "/a/b/x",
		},
		{
			name:  "Allows a Lock call for path /a/x",
			path1: "/a/b/c",
			path2: "/a/x",
		},
	} {
		t.Log(ti, tc.name)

		goroutineLockedWg := new(sync.WaitGroup)
		goroutineLockedWg.Add(1)

		trigger := false
		var path2Err error

		go func() {
			lock2, err := pl.Lock(tc.path2)
			if err != nil {
				path2Err = err
				goroutineLockedWg.Done()
				return
			}

			trigger = true

			goroutineLockedWg.Done()

			time.Sleep(10 * time.Millisecond)

			trigger = false

			lock2.Unlock()
		}()

		// Wait for the goroutine to lock
		goroutineLockedWg.Wait()

		if path2Err != nil {
			t.Fatalf("Error in second lock path: %v", path2Err)
		}

		// This should not block; the paths should not interfere
		lock1, err := pl.Lock(tc.path1)
		if err != nil {
			t.Fatalf("Unexpected path lock error: %v", err)
		}

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
	hitError := false

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

			lock, err := pl.Lock(path)
			if err != nil {
				t.Logf("Unexpected path lock error: %v", err)
				hitError = true
				return
			}

			counter++
			lock.Unlock()
		}()
	}

	wg.Wait()

	if hitError {
		t.Fatal("Hit unexpected error locking paths")
	}

	if counter != numGoroutines {
		t.Fatalf("counter %v != numgoroutines %v", counter, numGoroutines)
	}
}
