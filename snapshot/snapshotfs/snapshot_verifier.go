package snapshotfs

import (
	"context"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/object"
)

var verifierLog = logging.Module("verifier")

type verifyFileWorkItem struct {
	oid       object.ID
	entryPath string
}

// Verifier allows efficient verification of large amounts of filesystem entries in parallel.
type Verifier struct {
	throttle timetrack.Throttle

	queued      atomic.Int32
	processed   atomic.Int32
	readBytes   atomic.Int64
	readObjects atomic.Int32

	fileWorkQueue chan verifyFileWorkItem
	rep           repo.Repository
	opts          VerifierOptions
	workersWG     sync.WaitGroup

	blobMap map[blob.ID]blob.Metadata // when != nil, will check that each backing blob exists
}

// ShowStats logs verification statistics.
func (v *Verifier) ShowStats(ctx context.Context) {
	processed := v.processed.Load()

	verifierLog(ctx).Infof("Processed %v objects.", processed)
}

// ShowFinalStats logs final verification statistics.
func (v *Verifier) ShowFinalStats(ctx context.Context) {
	processed := v.processed.Load()

	verifierLog(ctx).Infof("Finished processing %v objects.", processed)
}

// VerifyFile verifies a single file object (using content check, blob map check or full read).
func (v *Verifier) VerifyFile(ctx context.Context, oid object.ID, entryPath string) error {
	verifierLog(ctx).Debugf("verifying object %v", oid)

	defer func() {
		v.processed.Add(1)
	}()

	contentIDs, err := v.rep.VerifyObject(ctx, oid)
	if err != nil {
		return errors.Wrap(err, "verify object")
	}

	if v.blobMap != nil {
		for _, cid := range contentIDs {
			ci, err := v.rep.ContentInfo(ctx, cid)
			if err != nil {
				return errors.Wrapf(err, "error verifying content %v", cid)
			}

			if _, ok := v.blobMap[ci.PackBlobID]; !ok {
				return errors.Errorf("object %v is backed by missing blob %v", oid, ci.PackBlobID)
			}
		}
	}

	//nolint:gosec
	if 100*rand.Float64() < v.opts.VerifyFilesPercent {
		if err := v.readEntireObject(ctx, oid, entryPath); err != nil {
			return errors.Wrapf(err, "error reading object %v", oid)
		}
	}

	return nil
}

// verifyObject enqueues a single object for verification.
func (v *Verifier) verifyObject(ctx context.Context, e fs.Entry, oid object.ID, entryPath string) error {
	if v.throttle.ShouldOutput(time.Second) {
		v.ShowStats(ctx)
	}

	if !e.IsDir() {
		v.fileWorkQueue <- verifyFileWorkItem{oid, entryPath}
		v.queued.Add(1)
	} else {
		v.queued.Add(1)
		v.processed.Add(1)
	}

	return nil
}

func (v *Verifier) readEntireObject(ctx context.Context, oid object.ID, path string) error {
	verifierLog(ctx).Debugf("reading object %v %v", oid, path)

	// read the entire file
	r, err := v.rep.OpenObject(ctx, oid)
	if err != nil {
		return errors.Wrapf(err, "unable to open object %v", oid)
	}
	defer r.Close() //nolint:errcheck

	n, err := iocopy.Copy(io.Discard, r)
	if err != nil {
		return errors.Wrap(err, "unable to read data")
	}

	v.readBytes.Add(n)
	v.readObjects.Add(1)

	return nil
}

// VerifierOptions provides options for the verifier.
type VerifierOptions struct {
	VerifyFilesPercent float64
	FileQueueLength    int
	Parallelism        int
	MaxErrors          int
	BlobMap            map[blob.ID]blob.Metadata
}

// VerifierResult returns results from the verifier.
type VerifierResult struct {
	ProcessedObjectCount int      `json:"processedObjectCount"`
	ReadObjectCount      int      `json:"readObjectCount"`
	ReadBytes            int      `json:"readBytes"`
	ErrorCount           int      `json:"errorCount"`
	Errors               []error  `json:"-"`
	ErrorStrings         []string `json:"errorStrings,omitempty"`
}

// InParallel starts parallel verification and invokes the provided function
// which can call Process() on in the provided TreeWalker. Errors and stats
// are accumulated into a VerifierResult and returned, independent of whether
// the error return is nil, that is, `VerifierResult` will contain useful,
// partial stats when an error is returned, including a collection of errors
// found in the verification process.
func (v *Verifier) InParallel(ctx context.Context, enqueue func(tw *TreeWalker) error) (VerifierResult, error) {
	tw, twerr := NewTreeWalker(ctx, TreeWalkerOptions{
		Parallelism:   v.opts.Parallelism,
		EntryCallback: v.verifyObject,
		MaxErrors:     v.opts.MaxErrors,
	})
	if twerr != nil {
		return VerifierResult{}, errors.Wrap(twerr, "tree walker")
	}
	defer tw.Close(ctx)

	v.fileWorkQueue = make(chan verifyFileWorkItem, v.opts.FileQueueLength)

	for range v.opts.Parallelism {
		v.workersWG.Add(1)

		go func() {
			defer v.workersWG.Done()

			for wi := range v.fileWorkQueue {
				if tw.TooManyErrors() {
					continue
				}

				if err := v.VerifyFile(ctx, wi.oid, wi.entryPath); err != nil {
					tw.ReportError(ctx, wi.entryPath, err)
				}
			}
		}()
	}

	err := enqueue(tw)
	if err != nil {
		// Pass the enqueue error to the tree walker for later accumulation.
		tw.ReportError(ctx, "tree walker enqueue", err)
	}

	close(v.fileWorkQueue)
	v.workersWG.Wait()
	v.fileWorkQueue = nil

	twErrs, numErrors := tw.GetErrors()

	errStrs := make([]string, 0, len(twErrs))
	for _, twErr := range twErrs {
		errStrs = append(errStrs, twErr.Error())
	}

	// Return the tree walker error output along with result details.
	return VerifierResult{
		ProcessedObjectCount: int(v.processed.Load()),
		ReadObjectCount:      int(v.readObjects.Load()),
		ReadBytes:            int(v.readBytes.Load()),
		ErrorCount:           numErrors,
		Errors:               twErrs,
		ErrorStrings:         errStrs,
	}, nil
}

// NewVerifier creates a verifier.
func NewVerifier(_ context.Context, rep repo.Repository, opts VerifierOptions) *Verifier {
	if opts.Parallelism == 0 {
		opts.Parallelism = runtime.NumCPU()
	}

	if opts.FileQueueLength == 0 {
		opts.FileQueueLength = 20000
	}

	return &Verifier{
		opts:    opts,
		rep:     rep,
		blobMap: opts.BlobMap,
	}
}
