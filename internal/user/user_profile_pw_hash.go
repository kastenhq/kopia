package user

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/crypto"
)

//nolint:gochecknoglobals
var dummyHashThatNeverMatchesAnyPassword = initDummyHash()

func initDummyHash() []byte {
	s := make([]byte, passwordHashSaltLength+passwordHashLength)

	for i := range s {
		s[i] = 0xFF
	}

	return s
}

func (p *Profile) setPassword(password string) error {
	passwordHashAlgorithm, err := getPasswordHashAlgorithm(p.PasswordHashVersion)
	if err != nil {
		return err
	}

	salt := make([]byte, passwordHashSaltLength)

	for {
		if _, err := io.ReadFull(rand.Reader, salt); err != nil {
			return errors.Wrap(err, "error generating salt")
		}

		// Retry when the salt matches the salt portion in dummyHashThatNever...
		// The probability of this happening is 2^(-8*passwordHashSaltLength)
		if !bytes.Equal(salt, dummyHashThatNeverMatchesAnyPassword[:passwordHashSaltLength]) {
			break
		}
	}

	p.PasswordHash, err = computePasswordHash(password, salt, passwordHashAlgorithm)

	return err
}

func computePasswordHash(password string, salt []byte, keyDerivationAlgorithm string) ([]byte, error) {
	key, err := crypto.DeriveKeyFromPassword(password, salt, passwordHashLength, keyDerivationAlgorithm)
	if err != nil {
		return nil, errors.Wrap(err, "error hashing password")
	}

	payload := append(append([]byte(nil), salt...), key...)

	return payload, nil
}

func isValidPassword(password string, hashedPassword []byte, keyDerivationAlgorithm string) bool {
	if len(hashedPassword) != passwordHashSaltLength+passwordHashLength {
		return false
	}

	salt := hashedPassword[0:passwordHashSaltLength]

	h, err := computePasswordHash(password, salt, keyDerivationAlgorithm)
	if err != nil {
		panic(err)
	}

	return subtle.ConstantTimeCompare(h, hashedPassword) != 0
}
