package repo

import (
	"crypto/rand"
	"encoding/json"
	"io"
	"time"

	"github.com/pkg/errors"
)

type retentionBlob struct {
	Mode   string        `json:"mode,omitempty"`
	Period time.Duration `json:"period,omitempty"`
}

func retentionBlobFromOptions(opt *NewRepositoryOptions) *retentionBlob {
	return &retentionBlob{
		Mode:   opt.RetentionMode,
		Period: opt.RetentionPeriod,
	}
}

// TODO: return a stream instead of []byte
func serializeRetentionBytes(f *formatBlob, r *retentionBlob, masterKey, repositoryID []byte) ([]byte, error) {
	content, err := json.Marshal(r)
	if err != nil {
		return nil, errors.Wrap(err, "can't marshal retentionBlob to JSON")
	}

	// TODO: generalize this with encryptFormatBytes()
	switch f.EncryptionAlgorithm {
	case "NONE":
		return content, nil

	case "AES256_GCM":
		aead, authData, err := initCrypto(masterKey, repositoryID)
		if err != nil {
			return nil, errors.Wrap(err, "unable to initialize crypto")
		}

		nonceLength := aead.NonceSize()
		noncePlusContentLength := nonceLength + len(content)
		cipherText := make([]byte, noncePlusContentLength+aead.Overhead())

		// Store nonce at the beginning of ciphertext.
		nonce := cipherText[0:nonceLength]
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return nil, errors.Wrap(err, "error reading random bytes for nonce")
		}

		b := aead.Seal(cipherText[nonceLength:nonceLength], nonce, content, authData)
		content = nonce[0 : nonceLength+len(b)]

		return content, nil

	default:
		return nil, errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}
}

func deserializeRetentionBytes(f *formatBlob, encryptedRepositoryBytes []byte, masterKey []byte) (*retentionBlob, error) {
	var (
		plainText []byte
		r         = &retentionBlob{}
	)

	switch f.EncryptionAlgorithm {
	case "NONE": // do nothing
		plainText = encryptedRepositoryBytes

	case "AES256_GCM":
		aead, authData, err := initCrypto(masterKey, f.UniqueID)
		if err != nil {
			return nil, errors.Wrap(err, "cannot initialize cipher")
		}

		content := append([]byte(nil), encryptedRepositoryBytes...)
		if len(content) < aead.NonceSize() {
			return nil, errors.Errorf("invalid encrypted payload, too short")
		}

		nonce := content[0:aead.NonceSize()]
		payload := content[aead.NonceSize():]

		plainText, err = aead.Open(payload[:0], nonce, payload, authData)
		if err != nil {
			return nil, errors.Errorf("unable to decrypt repository blob, invalid credentials?")
		}

	default:
		return nil, errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}

	if err := json.Unmarshal(plainText, &r); err != nil {
		return nil, errors.Wrap(err, "invalid repository format")
	}

	return r, nil
}
