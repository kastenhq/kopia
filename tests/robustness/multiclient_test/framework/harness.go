package framework

import (
	"context"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path"
	"syscall"

	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

const (
	dataSubPath     = "robustness-data"
	metadataSubPath = "robustness-metadata"
)

var repoPathPrefix = flag.String("repo-path-prefix", "", "Point the robustness tests at this path prefix")

// NewHarness returns a test harness.
func NewHarness(ctx context.Context) *TestHarness {
	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)
	metadataRepoPath := path.Join(*repoPathPrefix, metadataSubPath)

	th := &TestHarness{}
	th.init(ctx, dataRepoPath, metadataRepoPath)

	return th
}

// TestHarness provides a Kopia robustness.Engine.
type TestHarness struct {
	dataRepoPath string
	metaRepoPath string

	baseDirPath string
	fileWriter  *MultiClientFileWriter
	snapshotter *MultiClientSnapshotter
	persister   *snapmeta.KopiaPersister
	engine      *engine.Engine

	skipTest bool
}

func (th *TestHarness) init(ctx context.Context, dataRepoPath, metaRepoPath string) {
	th.dataRepoPath = dataRepoPath
	th.metaRepoPath = metaRepoPath

	// Override ENGINE_MODE env variable. Multiclient tests can only run in SERVER mode.
	log.Printf("Setting %s to %s\n", snapmeta.EngineModeEnvKey, snapmeta.EngineModeServer)

	err := os.Setenv(snapmeta.EngineModeEnvKey, snapmeta.EngineModeServer)
	if err != nil {
		log.Printf("Error setting ENGINE_MODE to server: %s", err.Error())
		os.Exit(1)
	}

	// the initialization state machine is linear and bails out on first failure
	if th.makeBaseDir() && th.getFileWriter() && th.getSnapshotter() &&
		th.getPersister() && th.getEngine(ctx) {
		return // success!
	}

	err = th.Cleanup(ctx)
	if err != nil {
		log.Printf("Error cleaning up the engine: %s\n", err.Error())
		os.Exit(2)
	}

	if th.skipTest {
		os.Exit(0)
	}

	os.Exit(1)
}

func (th *TestHarness) makeBaseDir() bool {
	baseDir, err := ioutil.TempDir("", "engine-data-")
	if err != nil {
		log.Println("Error creating temp dir:", err)
		return false
	}

	th.baseDirPath = baseDir

	return true
}

func (th *TestHarness) getFileWriter() bool {
	fw, err := NewMultiClientFileWriter(
		func() (FileWriter, error) { return fiofilewriter.New() },
	)
	if err != nil {
		if errors.Is(err, fio.ErrEnvNotSet) {
			log.Println("Skipping robustness tests because FIO environment is not set")

			th.skipTest = true
		} else {
			log.Println("Error creating fio FileWriter:", err)
		}

		return false
	}

	th.fileWriter = fw

	return true
}

func (th *TestHarness) getSnapshotter() bool {
	server, err := snapmeta.NewSnapshotter(th.baseDirPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			log.Println("Skipping robustness tests because KOPIA_EXE is not set")

			th.skipTest = true
		} else {
			log.Println("Error creating kopia Snapshotter:", err)
		}

		return false
	}

	newClientFn := func(baseDirPath string) (ClientSnapshotter, error) {
		return snapmeta.NewSnapshotter(th.baseDirPath)
	}

	s, err := NewMultiClientSnapshotter(th.baseDirPath, newClientFn, server)
	if err != nil {
		log.Println("Error creating multiclient kopia Snapshotter:", err)
		return false
	}

	th.snapshotter = s

	if err = s.ConnectOrCreateRepo(th.dataRepoPath); err != nil {
		log.Println("Error initializing kopia Snapshotter:", err)
		return false
	}

	return true
}

func (th *TestHarness) getPersister() bool {
	kp, err := snapmeta.NewPersister(th.baseDirPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			log.Println("Skipping robustness tests because KOPIA_EXE is not set")

			th.skipTest = true
		} else {
			log.Println("Error creating kopia Persister:", err)
		}

		return false
	}

	th.persister = kp

	if err = kp.ConnectOrCreateRepo(th.metaRepoPath); err != nil {
		log.Println("Error initializing kopia Persister:", err)
		return false
	}

	return true
}

func (th *TestHarness) getEngine(ctx context.Context) bool {
	args := &engine.Args{
		MetaStore:        th.persister,
		TestRepo:         th.snapshotter,
		FileWriter:       th.fileWriter,
		WorkingDir:       th.baseDirPath,
		SyncRepositories: true,
	}

	eng, err := engine.New(args) // nolint:govet
	if err != nil {
		log.Println("Error on engine creation:", err)
		return false
	}

	// Initialize the engine, connecting it to the repositories.
	// Note that th.engine is not yet set so that metadata will not be
	// flushed on cleanup in case there was an issue while loading.
	err = eng.Init(ctx)
	if err != nil {
		log.Println("Error initializing engine for S3:", err)
		return false
	}

	th.engine = eng

	return true
}

// Engine returns the Kopia robustness test engine.
func (th *TestHarness) Engine() *engine.Engine {
	return th.engine
}

// Cleanup shuts down the engine and stops the test app.
func (th *TestHarness) Cleanup(ctx context.Context) (retErr error) {
	if th.engine != nil {
		retErr = th.engine.Shutdown(ctx)
	}

	if th.persister != nil {
		th.persister.Cleanup()
	}

	if th.snapshotter != nil {
		if sc := th.snapshotter.ServerCmd(); sc != nil {
			if err := sc.Process.Signal(syscall.SIGTERM); err != nil {
				log.Println("Warning: Failed to send termination signal to kopia server process:", err)
			}
		}

		th.snapshotter.Cleanup()
	}

	if th.fileWriter != nil {
		th.fileWriter.Cleanup()
	}

	if th.baseDirPath != "" {
		err := os.RemoveAll(th.baseDirPath)
		if err != nil {
			log.Printf("Error removing path: %s", err.Error())
		}
	}

	return retErr
}
