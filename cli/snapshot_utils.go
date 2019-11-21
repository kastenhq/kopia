package cli

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	overrideHostName string
	overrideUserName string
)

func setupOverrideOptions(cmd *kingpin.CmdClause) {
	cmd.Flag("override-hostname", "Override system hostname.").StringVar(&overrideHostName)
	cmd.Flag("override-username", "Override system username.").StringVar(&overrideUserName)
}
