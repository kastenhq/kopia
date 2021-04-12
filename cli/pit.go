package cli

import (
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/araddon/dateparse"
	"github.com/pkg/errors"
)

func addPointInTimeFlag(cmd *kingpin.CmdClause, pit *time.Time) {
	var pointInTimeStr string

	pitPreAction := func(pc *kingpin.ParseContext) error {
		if pointInTimeStr != "" {
			t, err := dateparse.ParseStrict(pointInTimeStr)
			if err != nil {
				return errors.Wrap(err, "invalid point-in-time argument")
			}

			*pit = t
		}

		return nil
	}

	cmd.Flag("point-in-time", "Use a point-in-time view of the storage repository when supported").PlaceHolder(time.RFC3339).PreAction(pitPreAction).StringVar(&pointInTimeStr)
}
