// +build darwin,amd64 linux,amd64

package multiclienttest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
	"github.com/kopia/kopia/tests/testenv"
)

const defaultTestDur = 5 * time.Minute

var randomizedTestDur = flag.Duration("rand-test-duration", defaultTestDur, "Set the duration for the randomized test")

func TestManySmallFiles(t *testing.T) {
	fileSize := 4096
	numFiles := 10000
	numClients := 4

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	f := func(ctx context.Context) {
		_, err := tryExecAction(ctx, t, engine.RestoreIntoDataDirectoryActionKey, nil)
		testenv.AssertNoError(t, err)

		_, err = eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
		testenv.AssertNoError(t, err)

		_, err = eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
		testenv.AssertNoError(t, err)

		_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, nil)
		testenv.AssertNoError(t, err)
	}

	ctx := testlogging.Context(t)
	ctxs := framework.NewClientContexts(ctx, numClients)
	framework.RunAllAndWait(ctxs, f)
}

func TestOneLargeFile(t *testing.T) {
	fileSize := 40 * 1024 * 1024
	numFiles := 1
	numClients := 4

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	f := func(ctx context.Context) {
		_, err := tryExecAction(ctx, t, engine.RestoreIntoDataDirectoryActionKey, nil)
		testenv.AssertNoError(t, err)

		_, err = eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
		testenv.AssertNoError(t, err)

		snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
		testenv.AssertNoError(t, err)

		_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
		testenv.AssertNoError(t, err)
	}

	ctx := testlogging.Context(t)
	ctxs := framework.NewClientContexts(ctx, numClients)
	framework.RunAllAndWait(ctxs, f)
}

func TestManySmallFilesAcrossDirecoryTree(t *testing.T) {
	// TODO: Test takes too long - need to address performance issues with fio writes
	fileSize := 4096
	numFiles := 1000
	filesPerWrite := 10
	actionRepeats := numFiles / filesPerWrite
	numClients := 4

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(15),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		engine.ActionRepeaterField:             strconv.Itoa(actionRepeats),
	}

	f := func(ctx context.Context) {
		_, err := tryExecAction(ctx, t, engine.RestoreIntoDataDirectoryActionKey, nil)
		testenv.AssertNoError(t, err)

		_, err = eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
		testenv.AssertNoError(t, err)

		snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
		testenv.AssertNoError(t, err)

		_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
		testenv.AssertNoError(t, err)
	}

	ctx := testlogging.Context(t)
	ctxs := framework.NewClientContexts(ctx, numClients)
	framework.RunAllAndWait(ctxs, f)
}

func TestRandomizedSmall(t *testing.T) {
	numClients := 2
	st := clock.Now()

	opts := engine.ActionOpts{
		engine.ActionControlActionKey: map[string]string{
			string(engine.SnapshotDirActionKey):              strconv.Itoa(2),
			string(engine.RestoreSnapshotActionKey):          strconv.Itoa(2),
			string(engine.DeleteRandomSnapshotActionKey):     strconv.Itoa(1),
			string(engine.WriteRandomFilesActionKey):         strconv.Itoa(8),
			string(engine.DeleteRandomSubdirectoryActionKey): strconv.Itoa(1),
		},
		engine.WriteRandomFilesActionKey: map[string]string{
			fiofilewriter.IOLimitPerWriteAction:    fmt.Sprintf("%d", 512*1024*1024),
			fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(100),
			fiofilewriter.MaxFileSizeField:         strconv.Itoa(64 * 1024 * 1024),
			fiofilewriter.MaxDirDepthField:         strconv.Itoa(3),
		},
	}

	f := func(ctx context.Context) {
		_, err := tryExecAction(ctx, t, engine.RestoreIntoDataDirectoryActionKey, nil)
		testenv.AssertNoError(t, err)

		for clock.Since(st) <= *randomizedTestDur {
			err := tryRandomAction(ctx, t, opts)
			testenv.AssertNoError(t, err)
		}
	}

	ctx := testlogging.Context(t)
	ctxs := framework.NewClientContexts(ctx, numClients)
	framework.RunAllAndWait(ctxs, f)
}

// tryExecAction runs eng.ExecAction on the given parameters and masks no-op errors.
func tryExecAction(ctx context.Context, t *testing.T, actionKey engine.ActionKey, opts map[string]string) (map[string]string, error) {
	t.Helper()

	out, err := eng.ExecAction(ctx, actionKey, opts)
	if errors.Is(err, robustness.ErrNoOp) {
		t.Log("Action resulted in no-op")
		return out, nil
	}

	return out, err
}

// tryRandomAction runs eng.ExecAction on the given parameters and masks no-op errors.
func tryRandomAction(ctx context.Context, t *testing.T, opts engine.ActionOpts) error {
	t.Helper()

	err := eng.RandomAction(ctx, opts)
	if errors.Is(err, robustness.ErrNoOp) {
		t.Log("Random action resulted in no-op")
		return nil
	}

	return err
}
