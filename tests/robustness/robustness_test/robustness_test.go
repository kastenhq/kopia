// +build darwin,amd64 linux,amd64

package robustness

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/testenv"
)

func TestManySmallFiles(t *testing.T) {
	fileSize := 4096
	numFiles := 10000

	fileWriteOpts := map[string]string{
		engine.MaxDirDepthField:         strconv.Itoa(1),
		engine.MaxFileSizeField:         strconv.Itoa(fileSize),
		engine.MinFileSizeField:         strconv.Itoa(fileSize),
		engine.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		engine.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	var errs errgroup.Group

	for i := range eng.Checker {
		func(index int) {
			errs.Go(func() error {
				_, err := eng.ExecAction(engine.WriteRandomFilesActionKey, fileWriteOpts, index)
				if err != nil {
					return err
				}

				snapOut, err := eng.ExecAction(engine.SnapshotRootDirActionKey, nil, index)
				if err != nil {
					return err
				}
				_, err = eng.ExecAction(engine.RestoreSnapshotActionKey, snapOut, index)
				return err
			})
		}(i)
	}

	err := errs.Wait()
	testenv.AssertNoError(t, err)
}

func TestOneLargeFile(t *testing.T) {
	fileSize := 40 * 1024 * 1024
	numFiles := 1

	fileWriteOpts := map[string]string{
		engine.MaxDirDepthField:         strconv.Itoa(1),
		engine.MaxFileSizeField:         strconv.Itoa(fileSize),
		engine.MinFileSizeField:         strconv.Itoa(fileSize),
		engine.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		engine.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	var errs errgroup.Group

	for i := 0; i < eng.RunnerCount; i++ {
		func(index int) {
			errs.Go(func() error {
				_, err := eng.ExecAction(engine.WriteRandomFilesActionKey, fileWriteOpts, index)
				if err != nil {
					return err
				}

				snapOut, err := eng.ExecAction(engine.SnapshotRootDirActionKey, nil, index)
				if err != nil {
					return err
				}
				_, err = eng.ExecAction(engine.RestoreSnapshotActionKey, snapOut, index)
				return err
			})
		}(i)
	}

	err := errs.Wait()
	testenv.AssertNoError(t, err)
}

func TestManySmallFilesAcrossDirecoryTree(t *testing.T) {
	// TODO: Test takes too long - need to address performance issues with fio writes
	fileSize := 4096
	numFiles := 1000
	filesPerWrite := 10
	actionRepeats := numFiles / filesPerWrite

	fileWriteOpts := map[string]string{
		engine.MaxDirDepthField:         strconv.Itoa(15),
		engine.MaxFileSizeField:         strconv.Itoa(fileSize),
		engine.MinFileSizeField:         strconv.Itoa(fileSize),
		engine.MaxNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		engine.MinNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		engine.ActionRepeaterField:      strconv.Itoa(actionRepeats),
	}

	var errs errgroup.Group

	for i := 0; i < eng.RunnerCount; i++ {
		func(index int) {
			errs.Go(func() error {
				_, err := eng.ExecAction(engine.WriteRandomFilesActionKey, fileWriteOpts, index)
				if err != nil {
					return err
				}

				snapOut, err := eng.ExecAction(engine.SnapshotRootDirActionKey, nil, index)
				if err != nil {
					return err
				}
				_, err = eng.ExecAction(engine.RestoreSnapshotActionKey, snapOut, index)
				return err
			})
		}(i)
	}

	err := errs.Wait()
	testenv.AssertNoError(t, err)
}

func TestRandomizedSmall(t *testing.T) {
	st := time.Now()

	opts := engine.ActionOpts{
		engine.ActionControlActionKey: map[string]string{
			string(engine.SnapshotRootDirActionKey):          strconv.Itoa(2),
			string(engine.RestoreSnapshotActionKey):          strconv.Itoa(2),
			string(engine.DeleteRandomSnapshotActionKey):     strconv.Itoa(1),
			string(engine.WriteRandomFilesActionKey):         strconv.Itoa(8),
			string(engine.DeleteRandomSubdirectoryActionKey): strconv.Itoa(1),
		},
		engine.WriteRandomFilesActionKey: map[string]string{
			engine.IOLimitPerWriteAction:    fmt.Sprintf("%d", 512*1024*1024),
			engine.MaxNumFilesPerWriteField: strconv.Itoa(100),
			engine.MaxFileSizeField:         strconv.Itoa(64 * 1024 * 1024),
			engine.MaxDirDepthField:         strconv.Itoa(3),
		},
	}

	var errs errgroup.Group

	for i := 0; i < eng.RunnerCount; i++ {
		func(index int) {
			errs.Go(func() error {
				for time.Since(st) <= *randomizedTestDur {
					err := eng.RandomAction(opts, index)
					if errors.Is(err, engine.ErrNoOp) {
						t.Log("Random action resulted in no-op")
						err = nil
					}
					return err
				}
				return nil
			})
		}(i)
	}

	err := errs.Wait()
	testenv.AssertNoError(t, err)
}
