package cli

import (
	"context"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

var (
	connectFromConfigFile  string
	connectFromConfigToken string
)

func connectToStorageFromConfig(ctx context.Context, isNew bool) (blob.Storage, error) {
	if isNew {
		return nil, errors.New("not supported")
	}

	if connectFromConfigFile != "" {
		return connectToStorageFromConfigFile(ctx)
	}

	if connectFromConfigToken != "" {
		return connectToStorageFromConfigToken(ctx)
	}

	return nil, errors.New("either --file or --token must be provided")
}

func connectToStorageFromConfigFile(ctx context.Context) (blob.Storage, error) {
	cfg, err := repo.LoadConfigFromFile(connectFromConfigFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open config")
	}

	return blob.NewStorage(ctx, *cfg.Storage)
}

func connectToStorageFromConfigToken(ctx context.Context) (blob.Storage, error) {
	ci, pass, err := repo.DecodeToken(connectFromConfigToken)
	if err != nil {
		return nil, errors.Wrap(err, "invalid token")
	}

	passwordFromToken = pass

	return blob.NewStorage(ctx, ci)
}

func init() {
	RegisterStorageConnectFlags(
		"from-config",
		"the provided configuration file",
		func(cmd *kingpin.CmdClause) {
			cmd.Flag("file", "Path to the configuration file").StringVar(&connectFromConfigFile)
			cmd.Flag("token", "Configuration token").StringVar(&connectFromConfigToken)
		},
		connectToStorageFromConfig)
}
