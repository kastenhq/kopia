package engine

import (
	"testing"
	"time"
)

func TestPathLockBasic(t *testing.T) {
	pl := NewPathLock()

	for ti, tc := range []struct {
		name               string
		path1              string
		path2              string
		expectPath2Blocked bool
	}{
		{
			name:               "same path",
			path1:              "/a/b/c",
			path2:              "/a/b/c",
			expectPath2Blocked: true,
		},
		{
			name:               "path2 is a descendent of path1",
			path1:              "/a/b/c",
			path2:              "/a/b/c/d/e",
			expectPath2Blocked: true,
		},
		{
			name:               "path1 is a descendent of path2",
			path1:              "/a/b/c/d/e",
			path2:              "/a/b/c",
			expectPath2Blocked: true,
		},
	} {
		t.Log(ti, tc.name)
		pl.Lock(tc.path1)

		trigger := false

		go func() {
			pl.Lock(tc.path2)
			trigger = true
			pl.Unlock(tc.path2)
		}()

		time.Sleep(100 * time.Millisecond)

		if trigger == true {
			t.Fatalf("Lock unsuccessful")
		}

		pl.Unlock(tc.path1)

		time.Sleep(100 * time.Millisecond)

		if trigger != true {
			t.Fatalf("Unlock unsuccessful")
		}
	}

}
