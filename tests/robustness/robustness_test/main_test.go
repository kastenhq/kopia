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
	"github.com/kopia/kopia/tests/tools/fswalker"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

// test globals.
var (
	eng              *engine.Engine
	fioFileWriter    *fiofilewriter.FileWriter
	kopiaSnapshotter *snapmeta.KopiaSnapshotter
	kopiaPersister   *snapmeta.KopiaPersister
	fsComparer       *fswalker.WalkCompare
	baseDir          string
)

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

	var err error

	// create the work arena
	baseDir, err = ioutil.TempDir("", "engine-data-")
	if err != nil {
		log.Fatalln("error creating temp dir:", err)
	}

	// Create the interfaces
	fioFileWriter = getFioFileWriter()
	kopiaSnapshotter = getKopiaSnapshotter(baseDir, dataRepoPath)
	kopiaPersister = getKopiaPersister(baseDir, metadataRepoPath)
	fsComparer = fswalker.NewWalkCompare()

	// Create the engine
	args := &engine.Args{
		MetaStore:        kopiaPersister,
		TestRepo:         kopiaSnapshotter,
		Validator:        fsComparer,
		FileWriter:       fioFileWriter,
		WorkingDir:       baseDir,
		SyncRepositories: true,
	}
	if eng, err = engine.New(args); err != nil {
		logFatalln("error on engine creation:", err)
	}

	// Initialize the engine, connecting it to the repositories
	err = eng.Init(context.Background())
	if err != nil {
		// Don't write out the metadata, in case there was an issue loading it
		logFatalln("error initializing engine for S3:", err)
	}

	// Restore a random snapshot into the data directory
	_, err = eng.ExecAction(engine.RestoreIntoDataDirectoryActionKey, nil)
	if err != nil && !errors.Is(err, robustness.ErrNoOp) {
		eng.Shutdown()
		logFatalln("error restoring into the data directory:", err)
	}

	result := m.Run()

	err = eng.Shutdown()

	cleanup()

	if err != nil {
		log.Printf("error cleaning up the engine: %s\n", err.Error())
		os.Exit(2)
	}

	os.Exit(result)
}

// cleanup exists as a separate function because os.Exit(),
// used directly or indirectly via log.Fatal, does not invoke
// deferred functions.
func cleanup() {
	if kopiaPersister != nil {
		kopiaPersister.Cleanup()
	}

	if kopiaSnapshotter != nil {
		if sc := kopiaSnapshotter.ServerCmd(); sc != nil {
			if err := sc.Process.Signal(syscall.SIGTERM); err != nil {
				log.Println("Failed to send termination signal to kopia server process:", err)
			}
		}

		kopiaSnapshotter.Cleanup()
	}

	if fioFileWriter != nil {
		fioFileWriter.Cleanup()
	}

	if baseDir != "" {
		os.RemoveAll(baseDir)
	}
}

func logFatalln(v ...interface{}) {
	cleanup()
	log.Fatalln(v...)
}

func logPrintExit(rc int, v ...interface{}) {
	cleanup()
	log.Print(v...)
	os.Exit(rc)
}

func getFioFileWriter() *fiofilewriter.FileWriter {
	fw, err := fiofilewriter.New()
	if err != nil {
		if errors.Is(err, fio.ErrEnvNotSet) {
			logPrintExit(0, "Skipping robustness tests because FIO environment is not set")
		}

		log.Fatalln("error creating fio FileWriter:", err)
	}

	return fw
}

func getKopiaSnapshotter(baseDir, repoPath string) *snapmeta.KopiaSnapshotter {
	ks, err := snapmeta.NewSnapshotter(baseDir)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			logPrintExit(0, "Skipping robustness tests because KOPIA_EXE is not set")
		}

		logFatalln("error creating kopia Snapshotter:", err)
	}

	if err = ks.ConnectOrCreateRepo(repoPath); err != nil {
		ks.Cleanup()
		logFatalln("error initializing kopia Snapshotter:", err)
	}

	return ks
}

func getKopiaPersister(baseDir, repoPath string) *snapmeta.KopiaPersister {
	kp, err := snapmeta.NewPersister(baseDir)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			logPrintExit(0, "Skipping robustness tests because KOPIA_EXE is not set")
		}

		logFatalln("error creating kopia Persister:", err)
	}

	if err = kp.ConnectOrCreateRepo(repoPath); err != nil {
		kp.Cleanup()
		logFatalln("error initializing kopia Persister:", err)
	}

	return kp
}
