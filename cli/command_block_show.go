package cli

import (
	"bytes"

	"github.com/kopia/kopia/repo"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	showBlockCommand = blockCommands.Command("show", "Show contents of a block.").Alias("cat")

	showBlockIDs = showBlockCommand.Arg("id", "IDs of blocks to show").Required().Strings()
)

func runShowBlockCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	defer rep.Close() //nolint: errcheck

	for _, blockID := range *showBlockIDs {
		if err := showBlock(rep, blockID); err != nil {
			return err
		}
	}

	return nil
}

func showBlock(r *repo.Repository, blockID string) error {
	data, err := r.Blocks.GetBlock(blockID)
	if err != nil {
		return err
	}

	return showContent(bytes.NewReader(data))
}

func init() {
	setupShowCommand(showBlockCommand)
	showBlockCommand.Action(runShowBlockCommand)
}
