// +build darwin,amd64 linux,amd64

// Package client manages client specific info
package client

import (
	"context"
	"sync"

	petname "github.com/dustinkirkland/golang-petname"
	"github.com/google/uuid"
)

type key int

const (
	clientKey key = 0
	nameLen   int = 2
)

// Client is a unique client for use in multiclient robustness tests.
type Client struct {
	ID string
}

func newClient() *Client {
	petname.NonDeterministicMode()

	return &Client{
		ID: petname.Generate(nameLen, "-") + "-" + uuid.NewString(),
	}
}

// WrapContext returns a copy of ctx with a new client.
func WrapContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, clientKey, newClient())
}

// WrapContexts returns copies of ctx with n new clients.
func WrapContexts(ctx context.Context, n int) []context.Context {
	ctxs := make([]context.Context, n)
	for i := range ctxs {
		ctxs[i] = WrapContext(ctx)
	}

	return ctxs
}

// UnwrapContext returns a client from the given context.
func UnwrapContext(ctx context.Context) (*Client, bool) {
	c, ok := ctx.Value(clientKey).(*Client)
	return c, ok
}

// RunAllAndWait runs the provided function asynchronously for each of the
// given client contexts and waits for all of them to finish.
func RunAllAndWait(ctxs []context.Context, f func(context.Context)) {
	var wg sync.WaitGroup

	for _, ctx := range ctxs {
		wg.Add(1)

		go func(ctx context.Context) {
			f(ctx)
			wg.Done()
		}(ctx)
	}

	wg.Wait()
}
