// +build darwin,amd64 linux,amd64

package multiclienttest

import (
	"context"
	"flag"
	"log"
	"os"
	"testing"

	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/multiclient_test/framework"
)

var eng *engine.Engine // for use in the test functions

func TestMain(m *testing.M) {
	flag.Parse()

	ctx := framework.NewClientContext(context.Background())

	th := framework.NewHarness(ctx)

	eng = th.Engine()

	// run the tests
	result := m.Run()

	err := th.Cleanup(ctx)
	if err != nil {
		log.Printf("Error cleaning up the engine: %s\n", err.Error())
		os.Exit(2)
	}

	os.Exit(result)
}
