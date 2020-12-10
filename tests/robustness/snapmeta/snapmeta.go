// Package snapmeta describes entities that can accept
// arbitrary metadata and flush it to a persistent repository.
package snapmeta

import "github.com/kopia/kopia/tests/robustness/snap"

// Store describes the ability to store and retrieve
// a buffer of metadata, indexed by a string key.
type Store interface {
	Load(key string) ([]byte, error)
	Indexer
}

// Indexer describes methods surrounding categorization of keys via a named index.
type Indexer interface {
	IndexOperation(indexOperations ...OperationEntry) (interface{}, error)
}

// Persister describes the ability to flush metadata
// to, and load it again, from a repository.
type Persister interface {
	Store
	snap.RepoManager
	LoadMetadata() error
	FlushMetadata() error
	GetPersistDir() string
	Cleanup()
}
