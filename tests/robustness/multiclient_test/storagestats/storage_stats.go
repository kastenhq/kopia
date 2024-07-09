//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package storagestats

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"

	"github.com/kopia/kopia/tests/robustness/engine"
)

const (
	generatedDataBaseDirDesc = "Base dir for fio generated data"
	persistDirDesc           = "Dir used to persist metadata about the repo under test"
	checkerRestoreDirDesc    = "Engine checker restore data dir"
	configDirDesc            = "kopia config dir"
	cacheDirDesc             = "kopia cache dir"
	cliLogsDirDesc           = "cli logs dir"
	contentLogsDirDesc       = "content logs dir"
)

// DirDetails ...
type DirDetails struct {
	dirPath string
	dirSize int64
	desc    string
}

// StorageStats ...
type StorageStats struct {
	baseDataDir       DirDetails
	persistDir        DirDetails
	checkerRestoreDir DirDetails
	cfgDir            DirDetails
	kopiaCacheDir     DirDetails
	logsDir           DirDetails
}

// SetupStorageStats ...
func SetupStorageStats(ctx context.Context, eng *engine.Engine) []DirDetails {
	dirDetails := []DirDetails{}

	// LocalFioDataPathEnvKey
	dirDetails = append(dirDetails, DirDetails{
		dirPath: path.Join(eng.FileWriter.DataDirectory(ctx), ".."),
		desc:    generatedDataBaseDirDesc,
	})

	// kopia-persistence-root-
	dirDetails = append(dirDetails, DirDetails{
		dirPath: eng.MetaStore.GetPersistDir(),
		desc:    persistDirDesc,
	})

	// engine-data-*/restore-data-*
	dirDetails = append(dirDetails, DirDetails{
		dirPath: eng.Checker.RestoreDir,
		desc:    checkerRestoreDirDesc,
	})

	// ss.kopiaCacheDir.dirPath = "/root/.cache/kopia"
	dirDetails = append(dirDetails, DirDetails{
		dirPath: "/root/.cache/kopia",
		desc:    cacheDirDesc,
	})

	// ss.logsDir.dirPath = "/root/.cache/kopia/cli-logs"
	dirDetails = append(dirDetails, DirDetails{
		dirPath: "/root/.cache/kopia/cli-logs",
		desc:    cliLogsDirDesc,
	})

	// /root/.cache/kopia/content-logs
	dirDetails = append(dirDetails, DirDetails{
		dirPath: "/root/.cache/kopia/content-logs",
		desc:    contentLogsDirDesc,
	})

	return dirDetails
}

// LogStorageStats prints memory usage of file writer data dir, test-repo,
// robustness-data and robustness-metadata paths.
func LogStorageStats(ctx context.Context, dd []DirDetails) {
	// print storage stats
	// cache, logs,
	log.Printf("Logging storage stats")

	for _, d := range dd {
		dirSize, err := getDirSize(d.dirPath)
		d.dirSize = dirSize
		logDirDetails(d, err)
	}

	// LocalFioDataPathEnvKey
	// baseDirSize, err := getDirSize(ss.baseDataDir.dirPath)
	// ss.baseDataDir.dirSize = baseDirSize
	// logDirDetails(ss.baseDataDir, err)

	// // kopia-persistence-root-
	// ss.persistDir.dirSize, err = getDirSize(ss.persistDir.dirPath)
	// log.Printf("persist dir %s, persist dir size %d\n", ss.persistDir, ss.persistDir.dirSize)

	// // engine-data-*/restore-data-*
	// checkerRestoreDirSize, err := getDirSize(ss.checkerRestoreDir)
	// log.Printf("checkerRestore dir %s, checkerRestore dir size %d\n", ss.checkerRestoreDir, checkerRestoreDirSize)

	// engineDataDirSize, err := getDirSize(ss.checkerRestoreDir + "/../")
	// log.Printf("engineData dir %s, engineData dir size %d\n", ss.checkerRestoreDir+"/../", engineDataDirSize)

	// kopiaCacheDirSize, err := getDirSize(ss.kopiaCacheDir)
	// log.Printf("kopia cache dir %s, kopia cache dir size %d\n", ss.kopiaCacheDir, kopiaCacheDirSize)

	// dirsUnderKopiaCacheDir, err := findDirs(ss.kopiaCacheDir)
	// log.Printf("dirs under cache dir")
	// for _, d := range dirsUnderKopiaCacheDir {
	// 	log.Print(d)
	// 	dSize, _ := getDirSize(d)
	// 	log.Printf("size %d\n", dSize)
	// }

	// logsDirSize, err := getDirSize(ss.logsDir)
	// log.Printf("logs dir %s, logs dir size %d\n", ss.logsDir, logsDirSize)
}

func logDirDetails(dd DirDetails, err error) {
	if err != nil {
		log.Printf("error when getting dir size for %s %v", dd.dirPath, err)
		return
	}
	log.Printf("dir %s, dir size %d\n", dd.dirPath, dd.dirSize)
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

func catFilesInDir(dirPath string) error {
	err := filepath.WalkDir(dirPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		fmt.Printf("Contents of %s:\n", path)

		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return err
			}
			fmt.Print(line)
			if err == io.EOF {
				break
			}
		}
		fmt.Println()
		return nil
	})

	return err
}
