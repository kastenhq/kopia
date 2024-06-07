//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package multiclienttest

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/multiclient_test/framework"
)

// Variables for use in the test functions.
var (
	eng *engine.Engine
	th  *framework.TestHarness
)

func TestMain(m *testing.M) {
	flag.Parse()

	// A high-level client is required for harness initialization and cleanup steps.
	ctx := framework.NewClientContext(context.Background())

	th = framework.NewHarness(ctx)

	eng = th.Engine()

	// run the tests
	result := m.Run()

	// print storage stats
	// cache, logs,
	// fio-data-*
	dataDir := th.Engine().FileWriter.DataDirectory(ctx)
	dataDirSize, err := getDirSize(dataDir)
	log.Printf("data dir %s, data dir size %d\n", dataDir, dataDirSize)

	baseDirSize, err := getDirSize(dataDir + "/..")
	log.Printf("base dir %s, base dir size %d\n", dataDir+"/..", baseDirSize)

	// kopia-persistence-root-
	persistDir := th.Engine().MetaStore.GetPersistDir()
	persistDirSize, err := getDirSize(persistDir)
	log.Printf("persist dir %s, persist dir size %d\n", persistDir, persistDirSize)

	// engine-data-*/restore-data-*
	checkerRestoreDir := th.Engine().Checker.RestoreDir
	checkerRestoreDirSize, err := getDirSize(checkerRestoreDir)
	log.Printf("checkerRestore dir %s, checkerRestore dir size %d\n", checkerRestoreDir, checkerRestoreDirSize)

	engineDataDirSize, err := getDirSize(checkerRestoreDir + "/../")
	log.Printf("engineData dir %s, engineData dir size %d\n", checkerRestoreDir+"/../", engineDataDirSize)

	kopiaCacheDir := "/root/.cache/kopia"
	kopiaCacheDirSize, err := getDirSize(kopiaCacheDir)
	log.Printf("kopia cache dir %s, kopia cache dir size %d\n", kopiaCacheDir, kopiaCacheDirSize)

	dirsUnderKopiaCacheDir, err := findDirs(kopiaCacheDir)
	log.Printf("dirs under cache dir")
	for _, d := range dirsUnderKopiaCacheDir {
		log.Print(d)
		dSize, _ := getDirSize(d)
		log.Printf("size %d\n", dSize)
	}

	logsDir := "/root/.cache/kopia/cli-logs"
	logsDirSize, err := getDirSize(logsDir)
	log.Printf("logs dir %s, logs dir size %d\n", logsDir, logsDirSize)

	err = th.Cleanup(ctx)
	if err != nil {
		log.Printf("Error cleaning up the engine: %s\n", err.Error())
		os.Exit(2)
	}

	os.Exit(result)
}

func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.WalkDir(path, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// skip
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func findDirs(rootPath string) ([]string, error) {
	var dirs []string

	err := filepath.WalkDir(rootPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})
	return dirs, err
}
