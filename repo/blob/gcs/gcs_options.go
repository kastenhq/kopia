package gcs

import "encoding/json"

// Options defines options Google Cloud Storage-backed storage.
type Options struct {
	// BucketName is the name of the GCS bucket where data is stored.
	BucketName string `json:"bucket"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	// ServiceAccountCredentialsFile specifies the name of the file with GCS credentials.
	ServiceAccountCredentialsFile string `json:"credentialsFile,omitempty"`

	// ServiceAccountCredentialJSON specifies the raw JSON credentials.
	ServiceAccountCredentialJSON json.RawMessage `kopia:"sensitive" json:"credentials,omitempty"`

	// ReadOnly causes GCS connection to be opened with read-only scope to prevent accidental mutations.
	ReadOnly bool `json:"readOnly,omitempty"`

	MaxUploadSpeedBytesPerSecond int `json:"maxUploadSpeedBytesPerSecond,omitempty"`

	MaxDownloadSpeedBytesPerSecond int `json:"maxDownloadSpeedBytesPerSecond,omitempty"`
}
