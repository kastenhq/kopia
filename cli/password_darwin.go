package cli

import (
	"github.com/alecthomas/kingpin"
)

func (c *App) setupOSSpecificKeychainFlags(app *kingpin.Application) {
	app.Flag("use-keychain", "Use macOS Keychain for storing repository password.").Default("true").BoolVar(&c.keyRingEnabled)
}
