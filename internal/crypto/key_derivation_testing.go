//go:build testing
// +build testing

package crypto

import (
	"crypto/sha256"
)

const (
	testingOnlyInsecureAlgorithm = "testing-only-insecure"

	// DefaultKeyDerivationAlgorithm is the defaul key derivation algorithm for testing.
	DefaultKeyDerivationAlgorithm = testingOnlyInsecureAlgorithm
)

func init() {
	RegisterKeyDerivers(testingOnlyInsecureAlgorithm, &insecureKeyDeriver{})
}

type insecureKeyDeriver struct{}

func (s *insecureKeyDeriver) DeriveKeyFromPassword(password string, salt []byte) ([]byte, error) {
	h := sha256.New()
	if _, err := h.Write([]byte(password)); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

func (s *insecureKeyDeriver) RecommendedSaltLength() int {
	return V1SaltLength
}
