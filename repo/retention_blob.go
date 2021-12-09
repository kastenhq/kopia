package repo

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/content"
)

type retentionBlob struct {
	Mode   string        `json:"mode,omitempty"`
	Period time.Duration `json:"period,omitempty"`
}

func (r *retentionBlob) IsNull() bool {
	return r.Mode == "" || r.Period == 0
}

func retentionBlobFromOptions(opt *NewRepositoryOptions) *retentionBlob {
	return &retentionBlob{
		Mode:   opt.RetentionMode,
		Period: opt.RetentionPeriod,
	}
}

func retentionBlobFromRetentionOptions(opt *content.RetentionOptions) *retentionBlob {
	return &retentionBlob{
		Mode:   opt.Mode,
		Period: opt.Period,
	}
}

func serializeRetentionBytes(f *formatBlob, r *retentionBlob, masterKey []byte) ([]byte, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, errors.Wrap(err, "can't marshal retentionBlob to JSON")
	}

	switch f.EncryptionAlgorithm {
	case "NONE":
		return data, nil

	case aes256GcmEncryption:
		return encryptRepositoryBlobBytesAes256Gcm(data, masterKey, f.UniqueID)

	default:
		return nil, errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}
}

func deserializeRetentionBytes(f *formatBlob, encryptedRetentionBytes, masterKey []byte) (*retentionBlob, error) {
	var (
		plainText []byte
		r         = &retentionBlob{}
		err       error
	)

	if encryptedRetentionBytes == nil {
		return r, nil
	}

	switch f.EncryptionAlgorithm {
	case "NONE": // do nothing
		plainText = encryptedRetentionBytes

	case aes256GcmEncryption:
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
