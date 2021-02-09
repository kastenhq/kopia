// +build darwin,amd64 linux,amd64

package engine

import (
	"bytes"
	"context"
	"errors"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/kopia/kopia/tests/tools/fio"
)

// Errors associated with action-picking.
var (
	ErrNoActionPicked = errors.New("unable to pick an action with the action control options provided")
	ErrInvalidOption  = errors.New("invalid option setting")
)

// ExecAction executes the action denoted by the provided ActionKey.
func (e *Engine) ExecAction(actionKey ActionKey, opts map[string]string, index int) (map[string]string, error) {
	if opts == nil {
		opts = make(map[string]string)
	}

	e.RunStats[index].ActionCounter++
	e.CumulativeStats[index].ActionCounter++
	log.Printf("Engine executing ACTION: name=%q actionCount=%v totActCount=%v t=%vs engineIndex=%d (%vs)", actionKey, e.RunStats[index].ActionCounter, e.CumulativeStats[index].ActionCounter, e.RunStats[index].getLifetimeSeconds(), index, e.getRuntimeSeconds(index))

	action := actions[actionKey]
	st := time.Now()

	logEntry := &LogEntry{
		StartTime:       st,
		EngineTimestamp: e.getTimestampS(index),
		Action:          actionKey,
		ActionOpts:      opts,
	}

	// Execute the action n times
	err := ErrNoOp // Default to no-op error

	// TODO: return more than the last output
	var out map[string]string

	n := getOptAsIntOrDefault(ActionRepeaterField, opts, defaultActionRepeats)
	for i := 0; i < n; i++ {
		out, err = action.f(e, opts, logEntry, index)
		if err != nil {
			break
		}
	}

	// If error was just a no-op, don't bother logging the action
	switch {
	case errors.Is(err, ErrNoOp):
		e.RunStats[index].NoOpCount++
		e.CumulativeStats[index].NoOpCount++

		return out, err

	case err != nil:
		log.Printf("error=%q engineIndex=%d", err.Error(), index)
	}

	if e.RunStats[index].PerActionStats != nil && e.RunStats[index].PerActionStats[actionKey] == nil {
		e.RunStats[index].PerActionStats[actionKey] = new(ActionStats)
	}

	if e.CumulativeStats[index].PerActionStats != nil && e.CumulativeStats[index].PerActionStats[actionKey] == nil {
		e.CumulativeStats[index].PerActionStats[actionKey] = new(ActionStats)
	}

	e.RunStats[index].PerActionStats[actionKey].Record(st, err)
	e.CumulativeStats[index].PerActionStats[actionKey].Record(st, err)

	e.EngineLog[index].AddCompleted(logEntry, err)

	return out, err
}

// RandomAction executes a random action picked by the relative weights given
// in actionOpts[ActionControlActionKey], or uniform probability if that
// key is not present in the input options.
func (e *Engine) RandomAction(actionOpts ActionOpts, index int) error {
	actionControlOpts := actionOpts.getActionControlOpts()

	actionName := pickActionWeighted(actionControlOpts, actions)
	if string(actionName) == "" {
		return ErrNoActionPicked
	}

	_, err := e.ExecAction(actionName, actionOpts[actionName], index)
	err = e.checkErrRecovery(err, actionOpts, index)

	return err
}

func (e *Engine) checkErrRecovery(incomingErr error, actionOpts ActionOpts, index int) (outgoingErr error) {
	outgoingErr = incomingErr

	if incomingErr == nil {
		return nil
	}

	ctrl := actionOpts.getActionControlOpts()

	if errIsNotEnoughSpace(incomingErr) && ctrl[ThrowNoSpaceOnDeviceErrField] == "" {
		// no space left on device
		// Delete everything in the data directory
		const hundredPcnt = 100

		deleteDirActionKey := DeleteDirectoryContentsActionKey
		deleteRootOpts := map[string]string{
			MaxDirDepthField:             strconv.Itoa(0),
			DeletePercentOfContentsField: strconv.Itoa(hundredPcnt),
		}

		_, outgoingErr = e.ExecAction(deleteDirActionKey, deleteRootOpts, index)
		if outgoingErr != nil {
			return outgoingErr
		}

		e.RunStats[index].DataPurgeCount++
		e.CumulativeStats[index].DataPurgeCount++

		// Restore a previoius snapshot to the data directory
		restoreActionKey := RestoreIntoDataDirectoryActionKey
		_, outgoingErr = e.ExecAction(restoreActionKey, actionOpts[restoreActionKey], index)

		if errors.Is(outgoingErr, ErrNoOp) {
			outgoingErr = nil
		} else {
			e.RunStats[index].DataRestoreCount++
			e.CumulativeStats[index].DataRestoreCount++
		}
	}

	if outgoingErr == nil {
		e.RunStats[index].ErrorRecoveryCount++
		e.CumulativeStats[index].ErrorRecoveryCount++
	}

	return outgoingErr
}

// List of action keys.
const (
	ActionControlActionKey            ActionKey = "action-control"
	SnapshotRootDirActionKey          ActionKey = "snapshot-root"
	RestoreSnapshotActionKey          ActionKey = "restore-random-snapID"
	DeleteRandomSnapshotActionKey     ActionKey = "delete-random-snapID"
	WriteRandomFilesActionKey         ActionKey = "write-random-files"
	DeleteRandomSubdirectoryActionKey ActionKey = "delete-random-subdirectory"
	DeleteDirectoryContentsActionKey  ActionKey = "delete-files"
	RestoreIntoDataDirectoryActionKey ActionKey = "restore-into-data-dir"
	GCActionKey                       ActionKey = "run-gc"
)

// ActionOpts is a structure that designates the options for
// picking and running an action.
type ActionOpts map[ActionKey]map[string]string

func (actionOpts ActionOpts) getActionControlOpts() map[string]string {
	actionControlOpts := defaultActionControls()
	if actionOpts != nil && actionOpts[ActionControlActionKey] != nil {
		actionControlOpts = actionOpts[ActionControlActionKey]
	}

	return actionControlOpts
}

// Action is a unit of functionality that can be executed by
// the engine.
type Action struct {
	f func(eng *Engine, opts map[string]string, l *LogEntry, index int) (out map[string]string, err error)
}

// ActionKey refers to an action that can be executed by the engine.
type ActionKey string

var actions = map[ActionKey]Action{
	SnapshotRootDirActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry, index int) (out map[string]string, err error) {
			log.Printf("Creating snapshot of root directory %s engineIndex=%d", e.FileWriter[index].LocalDataDir, index)

			ctx := context.TODO()
			snapID, err := e.Checker[index].TakeSnapshot(ctx, e.FileWriter[index].LocalDataDir)

			setLogEntryCmdOpts(l, map[string]string{
				"snap-dir": e.FileWriter[index].LocalDataDir,
				"snapID":   snapID,
			})

			return map[string]string{
				SnapshotIDField: snapID,
			}, err
		},
	},
	RestoreSnapshotActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry, index int) (out map[string]string, err error) {
			snapID, err := e.getSnapIDOptOrRandLive(opts, index)
			if err != nil {
				return nil, err
			}

			setLogEntryCmdOpts(l, map[string]string{"snapID": snapID})

			log.Printf("Restoring snapshot %s engineIndex=%d", snapID, index)

			ctx := context.Background()
			b := &bytes.Buffer{}

			err = e.Checker[index].RestoreSnapshot(ctx, snapID, b)
			if err != nil {
				log.Printf("%s engineIndex=%d", b.String(), index)
			}

			return nil, err
		},
	},
	DeleteRandomSnapshotActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry, index int) (out map[string]string, err error) {
			snapID, err := e.getSnapIDOptOrRandLive(opts, index)
			if err != nil {
				return nil, err
			}

			log.Printf("Deleting snapshot %s engineIndex=%d", snapID, index)

			setLogEntryCmdOpts(l, map[string]string{"snapID": snapID})

			ctx := context.Background()
			err = e.Checker[index].DeleteSnapshot(ctx, snapID)
			return nil, err
		},
	},
	GCActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry, index int) (out map[string]string, err error) {
			return nil, e.TestRepo[index].RunGC()
		},
	},
	WriteRandomFilesActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry, index int) (out map[string]string, err error) {
			// Directory depth
			maxDirDepth := getOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
			dirDepth := rand.Intn(maxDirDepth + 1)

			// File size range
			maxFileSizeB := getOptAsIntOrDefault(MaxFileSizeField, opts, defaultMaxFileSize)
			minFileSizeB := getOptAsIntOrDefault(MinFileSizeField, opts, defaultMinFileSize)

			// Number of files to write
			maxNumFiles := getOptAsIntOrDefault(MaxNumFilesPerWriteField, opts, defaultMaxNumFilesPerWrite)
			minNumFiles := getOptAsIntOrDefault(MinNumFilesPerWriteField, opts, defaultMinNumFilesPerWrite)

			numFiles := rand.Intn(maxNumFiles-minNumFiles+1) + minNumFiles //nolint:gosec

			// Dedup Percentage
			maxDedupPcnt := getOptAsIntOrDefault(MaxDedupePercentField, opts, defaultMaxDedupePercent)
			minDedupPcnt := getOptAsIntOrDefault(MinDedupePercentField, opts, defaultMinDedupePercent)

			dedupStep := getOptAsIntOrDefault(DedupePercentStepField, opts, defaultDedupePercentStep)

			dedupPcnt := dedupStep * (rand.Intn(maxDedupPcnt/dedupStep-minDedupPcnt/dedupStep+1) + minDedupPcnt/dedupStep) //nolint:gosec

			blockSize := int64(defaultMinFileSize)

			fioOpts := fio.Options{}.
				WithFileSizeRange(int64(minFileSizeB), int64(maxFileSizeB)).
				WithNumFiles(numFiles).
				WithBlockSize(blockSize).
				WithDedupePercentage(dedupPcnt).
				WithNoFallocate()

			ioLimit := getOptAsIntOrDefault(IOLimitPerWriteAction, opts, defaultIOLimitPerWriteAction)

			if ioLimit > 0 {
				freeSpaceLimitB := getOptAsIntOrDefault(FreeSpaceLimitField, opts, defaultFreeSpaceLimit)

				freeSpaceB, err := getFreeSpaceB(e.FileWriter[index].LocalDataDir)
				if err != nil {
					return nil, err
				}
				log.Printf("Free Space %v B, limit %v B, ioLimit %v B\n", freeSpaceB, freeSpaceLimitB, ioLimit)

				if int(freeSpaceB)-ioLimit < freeSpaceLimitB {
					ioLimit = int(freeSpaceB) - freeSpaceLimitB
					log.Printf("Cutting down I/O limit for space %v", ioLimit)
					if ioLimit <= 0 {
						return nil, ErrCannotPerformIO
					}
				}

				fioOpts = fioOpts.WithIOLimit(int64(ioLimit))
			}

			relBasePath := "."

			log.Printf("Writing files at depth %v (fileSize: %v-%v, numFiles: %v, blockSize: %v, dedupPcnt: %v, ioLimit: %v) engineIndex=%d\n", dirDepth, minFileSizeB, maxFileSizeB, numFiles, blockSize, dedupPcnt, ioLimit, index)

			setLogEntryCmdOpts(l, map[string]string{
				"dirDepth":    strconv.Itoa(dirDepth),
				"relBasePath": relBasePath,
			})

			for k, v := range fioOpts {
				l.CmdOpts[k] = v
			}

			return nil, e.FileWriter[index].WriteFilesAtDepthRandomBranch(relBasePath, dirDepth, fioOpts)
		},
	},
	DeleteRandomSubdirectoryActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry, index int) (out map[string]string, err error) {
			maxDirDepth := getOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
			if maxDirDepth <= 0 {
				return nil, ErrInvalidOption
			}
			dirDepth := rand.Intn(maxDirDepth) + 1 //nolint:gosec

			log.Printf("Deleting directory at depth %v engineIndex=%d\n", dirDepth, index)

			setLogEntryCmdOpts(l, map[string]string{"dirDepth": strconv.Itoa(dirDepth)})

			err = e.FileWriter[index].DeleteDirAtDepth("", dirDepth)
			if errors.Is(err, fio.ErrNoDirFound) {
				log.Printf("%s engineIndex=%d", err.Error(), index)
				return nil, ErrNoOp
			}

			return nil, err
		},
	},
	DeleteDirectoryContentsActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry, index int) (out map[string]string, err error) {
			maxDirDepth := getOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
			dirDepth := rand.Intn(maxDirDepth + 1) //nolint:gosec

			pcnt := getOptAsIntOrDefault(DeletePercentOfContentsField, opts, defaultDeletePercentOfContents)

			log.Printf("Deleting %d%% of directory contents at depth %v engineIndex=%d\n", pcnt, dirDepth, index)

			setLogEntryCmdOpts(l, map[string]string{
				"dirDepth": strconv.Itoa(dirDepth),
				"percent":  strconv.Itoa(pcnt),
			})

			const pcntConv = 100
			err = e.FileWriter[index].DeleteContentsAtDepth("", dirDepth, float32(pcnt)/pcntConv)
			if errors.Is(err, fio.ErrNoDirFound) {
				log.Printf("%s engineIndex=%d", err.Error(), index)
				return nil, ErrNoOp
			}

			return nil, err
		},
	},
	RestoreIntoDataDirectoryActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry, index int) (out map[string]string, err error) {
			snapID, err := e.getSnapIDOptOrRandLive(opts, index)
			if err != nil {
				return nil, err
			}

			log.Printf("Restoring snap ID %v into data directory engineIndex=%d\n", snapID, index)

			setLogEntryCmdOpts(l, map[string]string{"snapID": snapID})

			b := &bytes.Buffer{}
			err = e.Checker[index].RestoreSnapshotToPath(context.Background(), snapID, e.FileWriter[index].LocalDataDir, b)
			if err != nil {
				log.Printf("%s RestoreIntoDataDirectoryActionKey, engineIndex=%d", b.String(), index)
				return nil, err
			}

			return nil, nil
		},
	},
}

// Action constants.
const (
	defaultMaxDirDepth             = 20
	defaultMaxFileSize             = 1 * 1024 * 1024 * 1024 // 1GB
	defaultMinFileSize             = 4096
	defaultMaxNumFilesPerWrite     = 10000
	defaultMinNumFilesPerWrite     = 1
	defaultIOLimitPerWriteAction   = 0                 // A zero value does not impose any limit on IO
	defaultFreeSpaceLimit          = 100 * 1024 * 1024 // 100 MB
	defaultMaxDedupePercent        = 100
	defaultMinDedupePercent        = 0
	defaultDedupePercentStep       = 25
	defaultDeletePercentOfContents = 20
	defaultActionRepeats           = 1
)

// Option field names.
const (
	MaxDirDepthField             = "max-dir-depth"
	MaxFileSizeField             = "max-file-size"
	MinFileSizeField             = "min-file-size"
	MaxNumFilesPerWriteField     = "max-num-files-per-write"
	MinNumFilesPerWriteField     = "min-num-files-per-write"
	IOLimitPerWriteAction        = "io-limit-per-write"
	FreeSpaceLimitField          = "free-space-limit"
	MaxDedupePercentField        = "max-dedupe-percent"
	MinDedupePercentField        = "min-dedupe-percent"
	DedupePercentStepField       = "dedupe-percent"
	ActionRepeaterField          = "repeat-action"
	ThrowNoSpaceOnDeviceErrField = "throw-no-space-error"
	DeletePercentOfContentsField = "delete-contents-percent"
	SnapshotIDField              = "snapshot-ID"
)

func getOptAsIntOrDefault(key string, opts map[string]string, def int) int {
	if opts == nil {
		return def
	}

	if opts[key] == "" {
		return def
	}

	retInt, err := strconv.Atoi(opts[key])
	if err != nil {
		return def
	}

	return retInt
}

func defaultActionControls() map[string]string {
	ret := make(map[string]string, len(actions))

	for actionKey := range actions {
		switch actionKey {
		case RestoreIntoDataDirectoryActionKey:
			// Don't restore into data directory by default
			ret[string(actionKey)] = strconv.Itoa(0)
		default:
			ret[string(actionKey)] = strconv.Itoa(1)
		}
	}

	return ret
}

func pickActionWeighted(actionControlOpts map[string]string, actionList map[ActionKey]Action) ActionKey {
	var keepKey ActionKey

	sum := 0

	for actionName := range actionList {
		weight := getOptAsIntOrDefault(string(actionName), actionControlOpts, 0)
		if weight == 0 {
			continue
		}

		sum += weight
		if rand.Intn(sum) < weight { //nolint:gosec
			keepKey = actionName
		}
	}

	return keepKey
}

func errIsNotEnoughSpace(err error) bool {
	return errors.Is(err, ErrCannotPerformIO) || strings.Contains(err.Error(), noSpaceOnDeviceMatchStr)
}

func (e *Engine) getSnapIDOptOrRandLive(opts map[string]string, index int) (snapID string, err error) {
	snapID = opts[SnapshotIDField]
	if snapID != "" {
		return snapID, nil
	}

	snapIDList := e.Checker[index].GetLiveSnapIDs()
	if len(snapIDList) == 0 {
		return "", ErrNoOp
	}

	return snapIDList[rand.Intn(len(snapIDList))], nil //nolint:gosec
}
