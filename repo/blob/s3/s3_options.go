package s3

import (
	"time"

	"github.com/minio/minio-go/v7"
)

// Options defines options for S3-based storage.
type Options struct {
	// BucketName is the name of the bucket where data is stored.
	BucketName string `json:"bucket"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	Endpoint       string `json:"endpoint"`
	DoNotUseTLS    bool   `json:"doNotUseTLS,omitempty"`
	DoNotVerifyTLS bool   `json:"doNotVerifyTLS,omitempty"`

	AccessKeyID     string `json:"accessKeyID"`
	SecretAccessKey string `json:"secretAccessKey" kopia:"sensitive"`
	SessionToken    string `json:"sessionToken" kopia:"sensitive"`

	// Region is an optional region to pass in authorization header.
	Region string `json:"region,omitempty"`

	MaxUploadSpeedBytesPerSecond int `json:"maxUploadSpeedBytesPerSecond,omitempty"`

	MaxDownloadSpeedBytesPerSecond int `json:"maxDownloadSpeedBytesPerSecond,omitempty"`

	// PointInTime specifies a view of the (versioned) store at that time
	PointInTime *time.Time `json:"pointInTime,omitempty"`

	// Unexported field for internal use only, the retention mode/period is always
	// pulled from the repository format blob.
	retentionMode   minio.RetentionMode
	retentionPeriod time.Duration
}
