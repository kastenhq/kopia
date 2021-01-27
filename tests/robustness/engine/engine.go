// +build darwin,amd64 linux,amd64

// Package engine provides the framework for a snapshot repository testing engine
package engine

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/checker"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/fswalker"
)

var (
	errNotAKopiaPersister   = fmt.Errorf("error: MetaStore is not a KopiaPersister")
	errNotAKopiaSnapshotter = fmt.Errorf("error: TestRepo is not a KopiaSnapshotter")
	noSpaceOnDeviceMatchStr = "no space left on device"
)

// Engine is the outer level testing framework for robustness testing.
type Engine struct {
	FileWriter      robustness.FileWriter
	TestRepo        robustness.Snapshotter
	MetaStore       robustness.Persister
	Checker         *checker.Checker
	cleanupRoutines []func()
	baseDirPath     string
	serverCmd       *exec.Cmd

	RunStats        Stats
	CumulativeStats Stats
	EngineLog       Log
}

// NewEngine instantiates a new Engine and returns its pointer. It is
// currently created with:
// - FIO file writer
// - Kopia test repo snapshotter
// - Kopia metadata storage repo
// - FSWalker data integrity checker.
func NewEngine(workingDir string) (*Engine, error) {
	baseDirPath, err := ioutil.TempDir(workingDir, "engine-data-")
	if err != nil {
		return nil, err
	}

	e := &Engine{
		baseDirPath: baseDirPath,
		RunStats: Stats{
			RunCounter:     1,
			CreationTime:   time.Now(),
			PerActionStats: make(map[ActionKey]*ActionStats),
		},
	}

	// Create an FIO file writer
	e.FileWriter, err = fiofilewriter.New()
	if err != nil {
		e.CleanComponents()
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, e.FileWriter.Cleanup)

	// Fill Snapshotter interface
	kopiaSnapper, err := snapmeta.NewSnapshotter(baseDirPath)
	if err != nil {
		e.CleanComponents()
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, kopiaSnapper.Cleanup)
	e.TestRepo = kopiaSnapper

	// Fill the snapshot store interface
	snapStore, err := snapmeta.NewPersister(baseDirPath)
	if err != nil {
		e.CleanComponents()
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, snapStore.Cleanup)

	e.MetaStore = snapStore

	err = e.setupLogging()
	if err != nil {
		e.CleanComponents()
		return nil, err
	}

	// Create the data integrity checker
	chk, err := checker.NewChecker(kopiaSnapper, snapStore, fswalker.NewWalkCompare(), baseDirPath)
	e.cleanupRoutines = append(e.cleanupRoutines, chk.Cleanup)

	if err != nil {
		e.CleanComponents()
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, e.cleanUpServer)

	e.Checker = chk

	return e, nil
}

// Cleanup cleans up after each component of the test engine.
func (e *Engine) Cleanup() error {
	// Perform a snapshot action to capture the state of the data directory
	// at the end of the run
	lastWriteEntry := e.EngineLog.FindLastThisRun(WriteRandomFilesActionKey)
	lastSnapEntry := e.EngineLog.FindLastThisRun(SnapshotRootDirActionKey)

	if lastWriteEntry != nil {
		if lastSnapEntry == nil || lastSnapEntry.Idx < lastWriteEntry.Idx {
			// Only force a final snapshot if the data tree has been modified since the last snapshot
			e.ExecAction(SnapshotRootDirActionKey, make(map[string]string)) //nolint:errcheck
		}
	}

	cleanupSummaryBuilder := new(strings.Builder)
	cleanupSummaryBuilder.WriteString("\n================\n")
	cleanupSummaryBuilder.WriteString("Cleanup Summary:\n\n")
	cleanupSummaryBuilder.WriteString(e.Stats())
	cleanupSummaryBuilder.WriteString("\n\n")
	cleanupSummaryBuilder.WriteString(e.EngineLog.StringThisRun())
	cleanupSummaryBuilder.WriteString("\n")

	log.Print(cleanupSummaryBuilder.String())

	e.RunStats.RunTime = time.Since(e.RunStats.CreationTime)
	e.CumulativeStats.RunTime += e.RunStats.RunTime

	defer e.CleanComponents()

	if e.MetaStore != nil {
		err := e.saveLog()
		if err != nil {
			return err
		}

		err = e.saveStats()
		if err != nil {
			return err
		}

		return e.MetaStore.FlushMetadata()
	}

	return nil
}

func (e *Engine) setupLogging() error {
	dirPath := e.MetaStore.GetPersistDir()

	newLogPath := filepath.Join(dirPath, e.formatLogName())

	f, err := os.Create(newLogPath)
	if err != nil {
		return err
	}

	// Write to both stderr and persistent log file
	wrt := io.MultiWriter(os.Stderr, f)
	log.SetOutput(wrt)

	return nil
}

func (e *Engine) formatLogName() string {
	st := e.RunStats.CreationTime
	return fmt.Sprintf("Log_%s", st.Format("2006_01_02_15_04_05"))
}

// CleanComponents cleans up each component part of the test engine.
func (e *Engine) CleanComponents() {
	for _, f := range e.cleanupRoutines {
		if f != nil {
			f()
		}
	}

	os.RemoveAll(e.baseDirPath) //nolint:errcheck
}

// Init initializes the Engine to a repository location according to the environment setup.
// - If S3_BUCKET_NAME is set, initialize S3
// - Else initialize filesystem.
func (e *Engine) Init(ctx context.Context, testRepoPath, metaRepoPath string) error {
	kp, ok := e.MetaStore.(*snapmeta.KopiaPersister)
	if !ok {
		return errNotAKopiaPersister
	}

	if err := kp.ConnectOrCreateRepo(metaRepoPath); err != nil {
		return err
	}

	ks, ok := e.TestRepo.(*snapmeta.KopiaSnapshotter)
	if !ok {
		return errNotAKopiaSnapshotter
	}

	if err := ks.ConnectOrCreateRepo(testRepoPath); err != nil {
		return err
	}

	e.serverCmd = ks.ServerCmd()

	err := e.MetaStore.LoadMetadata()
	if err != nil {
		return err
	}

	err = e.loadStats()
	if err != nil {
		return err
	}

	e.CumulativeStats.RunCounter++

	err = e.loadLog()
	if err != nil {
		return err
	}

	return e.Checker.VerifySnapshotMetadata()
}

// cleanUpServer cleans up the server process.
func (e *Engine) cleanUpServer() {
	if e.serverCmd == nil {
		return
	}

	if err := e.serverCmd.Process.Signal(syscall.SIGTERM); err != nil {
		log.Println("Failed to send termination signal to kopia server process:", err)
	}
}
