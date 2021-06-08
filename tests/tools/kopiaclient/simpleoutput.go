// +build darwin,amd64 linux,amd64

package kopiaclient

import (
	"context"
	"io/ioutil"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot/restore"
)

// SimpleOutput implements the restore.Output interface. It is used to restore a
// single file into a byte slice, instead of writing to the filesystem.
type SimpleOutput struct {
	data []byte
}

var _ restore.Output = &SimpleOutput{}

// NewSimpleOutput returns a new SimpleOutput.
func NewSimpleOutput() *SimpleOutput {
	return &SimpleOutput{data: []byte{}}
}

// Parallelizable implements restore.Output interface.
func (o *SimpleOutput) Parallelizable() bool {
	return true
}

// BeginDirectory implements restore.Output interface.
func (o *SimpleOutput) BeginDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	return nil
}

// FinishDirectory implements restore.Output interface.
func (o *SimpleOutput) FinishDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	return nil
}

// WriteFile implements restore.Output interface.
func (o *SimpleOutput) WriteFile(ctx context.Context, relativePath string, f fs.File) error {
	r, err := f.Open(ctx)
	if err != nil {
		return err
	}

	defer r.Close()

	o.data, err = ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	return nil
}

// FileExists implements restore.Output interface.
func (o *SimpleOutput) FileExists(ctx context.Context, relativePath string, e fs.File) bool {
	return false
}

// CreateSymlink implements restore.Output interface.
func (o *SimpleOutput) CreateSymlink(ctx context.Context, relativePath string, e fs.Symlink) error {
	return nil
}

// SymlinkExists implements restore.Output interface.
func (o *SimpleOutput) SymlinkExists(ctx context.Context, relativePath string, e fs.Symlink) bool {
	return false
}

// Close implements restore.Output interface.
func (o *SimpleOutput) Close(ctx context.Context) error {
	return nil
}
