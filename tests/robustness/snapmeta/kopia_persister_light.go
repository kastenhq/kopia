// +build darwin,amd64 linux,amd64

package snapmeta

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/tools/kopiaclient"
)

// KopiaPersisterLight is a wrapper for KopiaClient that satisfies the Persister
// interface.
type KopiaPersisterLight struct {
	kc            *kopiaclient.KopiaClient
	keysInProcess map[string]bool
	c             *sync.Cond
	baseDir       string
}

type kmu struct {
	mu sync.Mutex
}

var _ robustness.Persister = (*KopiaPersisterLight)(nil)

const fileName = "data"

// NewPersisterLight returns a new KopiaPersisterLight.
func NewPersisterLight(baseDir string) (*KopiaPersisterLight, error) {
	persistenceDir, err := os.MkdirTemp(baseDir, "kopia-persistence-root-")
	if err != nil {
		return nil, err
	}

	return &KopiaPersisterLight{
		kc:            kopiaclient.NewKopiaClient(persistenceDir),
		keysInProcess: map[string]bool{},
		c:             sync.NewCond(&sync.Mutex{}),
		baseDir:       persistenceDir,
	}, nil
}

// ConnectOrCreateRepo creates a new Kopia repo or connects to an existing one if possible.
func (kpl *KopiaPersisterLight) ConnectOrCreateRepo(repoPath string) error {
	bucketName := os.Getenv(S3BucketNameEnvKey)
	return kpl.kc.CreateOrConnectRepo(context.Background(), repoPath, bucketName)
}

// Store pushes the key value pair to the Kopia repository.
func (kpl *KopiaPersisterLight) Store(key string, val []byte) error {
	kpl.waitFor(key)
	defer kpl.doneWith(key)

	dirPath, filePath := kpl.getPathsFromKey(key)

	if err := os.Mkdir(dirPath, 0o700); err != nil {
		return err
	}

	if err := os.WriteFile(filePath, val, 0o700); err != nil {
		return err
	}

	defer kpl.removeOrLog(dirPath)

	log.Println("pushing metadata for", key)

	return kpl.kc.SnapshotCreate(context.Background(), dirPath)
}

// Load pulls the key value pair from the Kopia repo and returns the value.
func (kpl *KopiaPersisterLight) Load(key string) ([]byte, error) {
	kpl.waitFor(key)
	defer kpl.doneWith(key)

	dirPath, filePath := kpl.getPathsFromKey(key)

	log.Println("pulling metadata for", key)

	if err := kpl.kc.SnapshotRestore(context.Background(), dirPath); err != nil {
		return nil, err
	}

	defer kpl.removeOrLog(dirPath)

	return os.ReadFile(filePath)
}

// Delete deletes all snapshots associated with the given key.
func (kpl *KopiaPersisterLight) Delete(key string) {
	kpl.waitFor(key)
	defer kpl.doneWith(key)

	log.Println("deleting metadata for", key)

	dirPath, _ := kpl.getPathsFromKey(key)
	if err := kpl.kc.SnapshotDelete(context.Background(), dirPath); err != nil {
		log.Printf("cannot delete metadata for %s, err: %s", key, err)
	}
}

// LoadMetadata is a no-op. It is included to satisfy the Persister interface.
func (kpl *KopiaPersisterLight) LoadMetadata() error {
	return nil
}

// FlushMetadata is a no-op. It is included to satisfy the Persister interface.
func (kpl *KopiaPersisterLight) FlushMetadata() error {
	return nil
}

// GetPersistDir returns the persistence directory.
func (kpl *KopiaPersisterLight) GetPersistDir() string {
	return kpl.baseDir
}

// Cleanup removes the persistence directory and closes the Kopia repo.
func (kpl *KopiaPersisterLight) Cleanup() {
	if err := os.RemoveAll(kpl.baseDir); err != nil {
		log.Println("cannot remove persistence dir")
	}
}

func (kpl *KopiaPersisterLight) getPathsFromKey(key string) (dirPath, filePath string) {
	dirPath = filepath.Join(kpl.baseDir, key)
	filePath = filepath.Join(dirPath, fileName)

	return dirPath, filePath
}

func (kpl *KopiaPersisterLight) removeOrLog(path string) {
	if err := os.RemoveAll(path); err != nil {
		log.Println("cannot remove path ", path)
	}
}

func (kpl *KopiaPersisterLight) waitFor(key string) {
	kpl.c.L.Lock()
	for kpl.keysInProcess[key] {
		kpl.c.Wait()
	}

	kpl.keysInProcess[key] = true
	kpl.c.L.Unlock()
}

func (kpl *KopiaPersisterLight) doneWith(key string) {
	kpl.c.L.Lock()
	delete(kpl.keysInProcess, key)
	kpl.c.L.Unlock()
	kpl.c.Signal()
}
