package maintenance

import (
	"context"
	"os"
	"strconv"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/index"
)

// Checks the consistency of the mapping from content index entries to packs,
// to verify that all the referenced packs are present in storage.
func checkContentIndexToPacks(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	const verifyContentsDefaultParallelism = 5

	opts := content.VerifyOptions{
		ContentIDRange:            index.AllIDs,
		ContentReadPercentage:     0,
		IncludeDeletedContents:    true,
		ContentIterateParallelism: verifyContentsDefaultParallelism,
	}

	if err := rep.ContentReader().VerifyContents(ctx, opts); err != nil {
		return errors.Wrap(err, "maintenance verify contents")
	}

	return nil
}

func maybeCheckContentIndexToPacks(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	if enabled, _ := strconv.ParseBool(os.Getenv("KOPIA_MAINTENANCE_CONTENT_CONSISTENCY_CHECK")); enabled {
		return checkContentIndexToPacks(ctx, rep)
	}

	return nil
}

func reportRunAndMaybeCheckContentIndex(ctx context.Context, rep repo.DirectRepositoryWriter, taskType TaskType, s *Schedule, run func() error) error {
	return ReportRun(ctx, rep, taskType, s, func() error {
		if err := maybeCheckContentIndexToPacks(ctx, rep); err != nil {
			return err
		}

		if err := run(); err != nil {
			return err
		}

		return maybeCheckContentIndexToPacks(ctx, rep)
	})
}
