package cli

import (
	"context"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/s3"
)

func init() {
	var s3options s3.Options

	RegisterStorageConnectFlags(
		"s3",
		"an S3 bucket",
		func(cmd *kingpin.CmdClause) {
			cmd.Flag("bucket", "Name of the S3 bucket").Required().StringVar(&s3options.BucketName)
			cmd.Flag("endpoint", "Endpoint to use").Default("s3.amazonaws.com").StringVar(&s3options.Endpoint)
			cmd.Flag("region", "S3 Region").Default("").StringVar(&s3options.Region)
			cmd.Flag("access-key", "Access key ID (overrides AWS_ACCESS_KEY_ID environment variable)").Required().Envar("AWS_ACCESS_KEY_ID").StringVar(&s3options.AccessKeyID)
			cmd.Flag("secret-access-key", "Secret access key (overrides AWS_SECRET_ACCESS_KEY environment variable)").Required().Envar("AWS_SECRET_ACCESS_KEY").StringVar(&s3options.SecretAccessKey)
			cmd.Flag("session-token", "Session token (overrides AWS_SESSION_TOKEN environment variable)").Envar("AWS_SESSION_TOKEN").StringVar(&s3options.SessionToken)
			cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&s3options.Prefix)
			cmd.Flag("disable-tls", "Disable TLS security (HTTPS)").BoolVar(&s3options.DoNotUseTLS)
			cmd.Flag("disable-tls-verification", "Disable TLS (HTTPS) certificate verification").BoolVar(&s3options.DoNotVerifyTLS)
			cmd.Flag("max-download-speed", "Limit the download speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&s3options.MaxDownloadSpeedBytesPerSecond)
			cmd.Flag("max-upload-speed", "Limit the upload speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&s3options.MaxUploadSpeedBytesPerSecond)
			addPointInTimeFlag(cmd, &s3options.StoreOptions.PointInTimeView)
		},
		func(ctx context.Context, isNew bool) (blob.Storage, error) {
			if isNew && !s3options.StoreOptions.PointInTimeView.IsZero() {
				return nil, errors.New("Cannot specify a 'point-in-time' option when creating a repository")
			}

			return s3.New(ctx, &s3options)
		},
	)
}
