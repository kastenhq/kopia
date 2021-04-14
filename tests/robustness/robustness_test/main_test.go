// +build darwin,amd64 linux,amd64

package robustness

import (
	"context"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path"
	"syscall"
	"testing"
	"time"

	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

var eng *engine.Engine // for use in the test functions

const (
	dataSubPath     = "robustness-data"
	metadataSubPath = "robustness-metadata"
	defaultTestDur  = 5 * time.Minute
)

var (
	randomizedTestDur = flag.Duration("rand-test-duration", defaultTestDur, "Set the duration for the randomized test")
	repoPathPrefix    = flag.String("repo-path-prefix", "", "Point the robustness tests at this path prefix")
)

func TestMain(m *testing.M) {
	flag.Parse()

	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)
	metadataRepoPath := path.Join(*repoPathPrefix, metadataSubPath)

	ctx := context.Background()

	th := &kopiaRobustnessTestHarness{}
	th.init(ctx, dataRepoPath, metadataRepoPath)
	eng = th.engine

	// Restore a random snapshot into the data directory
	_, err := eng.ExecAction(ctx, engine.RestoreIntoDataDirectoryActionKey, nil)
	if err != nil && !errors.Is(err, robustness.ErrNoOp) {
		th.cleanup(ctx)
		log.Fatalln("Error restoring into the data directory:", err)
	}

	// run the tests
	result := m.Run()

	err = th.cleanup(ctx)
	if err != nil {
		log.Printf("Error cleaning up the engine: %s\n", err.Error())
		os.Exit(2)
	}

	os.Exit(result)
}

type kopiaRobustnessTestHarness struct {
	dataRepoPath string
	metaRepoPath string

	baseDirPath string
	fileWriter  *fiofilewriter.FileWriter
	snapshotter *snapmeta.KopiaSnapshotter
	persister   *snapmeta.KopiaPersister
	engine      *engine.Engine

	skipTest bool
}

func (th *kopiaRobustnessTestHarness) init(ctx context.Context, dataRepoPath, metaRepoPath string) {
	th.dataRepoPath = dataRepoPath
	th.metaRepoPath = metaRepoPath

	var ok bool

	if th.baseDirPath, ok = MakeBaseDir(); !ok {
		th.exitTest(ctx)
	}

	if th.fileWriter, th.skipTest, ok = GetFioFileWriter(); !ok {
		th.exitTest(ctx)
	}

	if th.snapshotter, th.skipTest, ok = GetKopiaSnapshotter(th.baseDirPath, th.dataRepoPath); !ok {
		th.exitTest(ctx)
	}

	if th.persister, th.skipTest, ok = GetKopiaPersister(th.baseDirPath, th.metaRepoPath); !ok {
		th.exitTest(ctx)
	}

	args := &engine.Args{
		MetaStore:        th.persister,
		TestRepo:         th.snapshotter,
		FileWriter:       th.fileWriter,
		WorkingDir:       th.baseDirPath,
		SyncRepositories: true,
	}

	if th.engine, ok = GetEngine(ctx, args); !ok {
		th.exitTest(ctx)
	}
}

// MakeBaseDir creates a temp directory for engine data.
func MakeBaseDir() (string, bool) {
	baseDir, err := ioutil.TempDir("", "engine-data-")
	if err != nil {
		log.Println("Error creating temp dir:", err)
		return "", false
	}

	return baseDir, true
}

// GetFioFileWriter creates an fiofilewriter.
func GetFioFileWriter() (fw *fiofilewriter.FileWriter, skipTest, ok bool) {
	fw, err := fiofilewriter.New()
	if err != nil {
		if errors.Is(err, fio.ErrEnvNotSet) {
			log.Println("Skipping robustness tests because FIO environment is not set")

			skipTest = true
		} else {
			log.Println("Error creating fio FileWriter:", err)
		}

		return nil, skipTest, false
	}

	return fw, false, true
}

// GetKopiaSnapshotter creates a KopiaSnapshotter.
func GetKopiaSnapshotter(baseDirPath, dataRepoPath string) (s *snapmeta.KopiaSnapshotter, skipTest, ok bool) {
	ks, err := snapmeta.NewSnapshotter(baseDirPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			log.Println("Skipping robustness tests because KOPIA_EXE is not set")

			skipTest = true
		} else {
			log.Println("Error creating kopia Snapshotter:", err)
		}

		return nil, skipTest, false
	}

	if err = ks.ConnectOrCreateRepo(dataRepoPath); err != nil {
		log.Println("Error initializing kopia Snapshotter:", err)
		return ks, false, false
	}

	return ks, false, true
}

// GetKopiaPersister creates a KopiaPersister.
func GetKopiaPersister(baseDirPath, metaRepoPath string) (kp *snapmeta.KopiaPersister, skipTest, ok bool) {
	kp, err := snapmeta.NewPersister(baseDirPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			log.Println("Skipping robustness tests because KOPIA_EXE is not set")

			skipTest = true
		} else {
			log.Println("Error creating kopia Persister:", err)
		}

		return nil, skipTest, false
	}

	if err = kp.ConnectOrCreateRepo(metaRepoPath); err != nil {
		log.Println("Error initializing kopia Persister:", err)
		return kp, false, false
	}

	return kp, false, true
}

// GetEngine creates an engine for use in robustness tests.
func GetEngine(ctx context.Context, args *engine.Args) (eng *engine.Engine, ok bool) {
	eng, err := engine.New(args) // nolint:govet
	if err != nil {
		log.Println("Error on engine creation:", err)
		return nil, false
	}

	// Initialize the engine, connecting it to the repositories.
	// Note that th.engine is not yet set so that metadata will not be
	// flushed on cleanup in case there was an issue while loading.
	err = eng.Init(ctx)
	if err != nil {
		log.Println("Error initializing engine for S3:", err)
		return nil, false
	}

	return eng, true
}

func (th *kopiaRobustnessTestHarness) cleanup(ctx context.Context) (retErr error) {
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
		os.RemoveAll(th.baseDirPath)
	}

	return
}

func (th *kopiaRobustnessTestHarness) exitTest(ctx context.Context) {
	th.cleanup(ctx)

	if th.skipTest {
		os.Exit(0)
	}

	os.Exit(1)
}
