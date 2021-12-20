// Package beforeop implements wrapper around blob.Storage that run a given callback before all operations.
package beforeop

import (
	"context"

	"github.com/kopia/kopia/repo/blob"
)

type callback func() error

type beforeOp struct {
	blob.Storage
	cb callback
}

func (s beforeOp) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	if err := s.cb(); err != nil {
		return err
	}

	return s.Storage.GetBlob(ctx, id, offset, length, output) // nolint:wrapcheck
}

func (s beforeOp) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	if err := s.cb(); err != nil {
		return blob.Metadata{}, err
	}

	return s.Storage.GetMetadata(ctx, id) // nolint:wrapcheck
}

func (s beforeOp) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	if err := s.cb(); err != nil {
		return err
	}

	return s.Storage.PutBlob(ctx, id, data, opts) // nolint:wrapcheck
}

func (s beforeOp) DeleteBlob(ctx context.Context, id blob.ID) error {
	if err := s.cb(); err != nil {
		return err
	}

	return s.Storage.DeleteBlob(ctx, id) // nolint:wrapcheck
}

// NewWrapper creates a wrapped storage interface for data operations that need
// to run a callback before the actual operation.
func NewWrapper(wrapped blob.Storage, cb callback) blob.Storage {
	return &beforeOp{Storage: wrapped, cb: cb}
}
