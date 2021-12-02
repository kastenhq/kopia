package repo

import (
	"encoding/json"
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

func serializeRetentionBytes(f *formatBlob, r *retentionBlob, masterKey []byte) ([]byte, error) {
	content, err := json.Marshal(r)
	if err != nil {
		return nil, errors.Wrap(err, "can't marshal retentionBlob to JSON")
	}

	switch f.EncryptionAlgorithm {
	case "NONE":
		return content, nil

	case "AES256_GCM":
		return encryptRepositoryBlobBytesAes256Gcm(content, masterKey, f.UniqueID)

	default:
		return nil, errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}
}

func deserializeRetentionBytes(f *formatBlob, encryptedRetentionBytes []byte, masterKey []byte) (*retentionBlob, error) {
	var (
		plainText []byte
		r         = &retentionBlob{}
		err       error
	)

	switch f.EncryptionAlgorithm {
	case "NONE": // do nothing
		plainText = encryptedRetentionBytes

	case "AES256_GCM":
		plainText, err = decryptRepositoryBlobBytesAes256Gcm(encryptedRetentionBytes, masterKey, f.UniqueID)
		if err != nil {
			return nil, errors.Errorf("unable to decrypt repository retention blob")
		}

	default:
		return nil, errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}

	if err = json.Unmarshal(plainText, &r); err != nil {
		return nil, errors.Wrap(err, "invalid repository retention blob")
	}

	return r, nil
}
