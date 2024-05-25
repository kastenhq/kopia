package repo

import "github.com/kopia/kopia/internal/crypto"

// DefaultServerRepoCacheKeyDerivationAlgorithm is the default algorithm used to
// derive an encryption key for the local cache when connecting to a repository
// through the kopia API server.
const DefaultServerRepoCacheKeyDerivationAlgorithm = crypto.ScryptAlgorithm
