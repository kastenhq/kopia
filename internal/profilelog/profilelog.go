// Package profilelog implements functionality for writing pprof profiles to logs.
package profilelog

import (
	"context"
	"os"
	"os/signal"

	"github.com/kopia/kopia/debug"
	"github.com/kopia/kopia/internal/ctxutil"
)

type stopper func(ctx context.Context)

func (s stopper) Stop(ctx context.Context) {
	s(ctx)
}

// MaybeStart may start the ability to send pprof buffers to the logs.
// The caller must call `v.Stop()` to finish profiling and dump the logs
func MaybeStart(ctx context.Context) interface {
	Stop(ctx context.Context)
} {
	if os.Getenv(debug.EnvVarKopiaDebugPprof) == "" {
		// no need to configure anything, thus nothing needs to be stopped.
		return stopper(func(context.Context) {})
	}

	debug.StartProfileBuffers(ctx)
	setupDumpSignalHandler(ctx)

	return stopper(debug.StopProfileBuffers)
}

func setupDumpSignalHandler(ctx context.Context) {
	s := make(chan os.Signal, 1)

	signal.Notify(s, dumpSignal)

	go func() {
		dctx := ctxutil.Detach(ctx)

		for {
			<-s
			debug.StopProfileBuffers(dctx)
			debug.StartProfileBuffers(dctx)
		}
	}()
}
