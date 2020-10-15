// Package rclone implements blob storage provider proxied by rclone (http://rclone.org)
package rclone

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/foomo/htpasswd"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/tlsutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/webdav"
	"github.com/kopia/kopia/repo/logging"
)

const (
	rcloneStorageType = "rclone"

	defaultRCloneExe = "rclone"

	// rcloneStartupTimeout is the time we wait for rclone to print the https address it's serving at.
	rcloneStartupTimeout = 15 * time.Second
)

var log = logging.GetContextLoggerFunc("rclone")

type rcloneStorage struct {
	blob.Storage // the underlying WebDAV storage used to implement all methods.

	Options

	cmd          *exec.Cmd // running rclone
	temporaryDir string
}

func (r *rcloneStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   rcloneStorageType,
		Config: &r.Options,
	}
}

func (r *rcloneStorage) Close(ctx context.Context) error {
	if r.Storage != nil {
		if err := r.Storage.Close(ctx); err != nil {
			return errors.Wrap(err, "error closing webdav connection")
		}
	}

	// this will kill rclone process if any
	if r.cmd != nil && r.cmd.Process != nil {
		log(ctx).Debugf("killing rclone")
		r.cmd.Process.Kill() // nolint:errcheck
		r.cmd.Wait()         // nolint:errcheck
	}

	if r.temporaryDir != "" {
		if err := os.RemoveAll(r.temporaryDir); err != nil && !os.IsNotExist(err) {
			log(ctx).Warningf("error deleting temporary dir: %v", err)
		}
	}

	return nil
}

func (r *rcloneStorage) DisplayName() string {
	return "RClone " + r.Options.RemotePath
}

func runRCloneAndWaitForServerAddress(c *exec.Cmd, startupTimeout time.Duration) (string, error) {
	rcloneAddressChan := make(chan string)
	rcloneErrChan := make(chan error)

	go func() {
		stderr, err := c.StderrPipe()
		if err != nil {
			rcloneErrChan <- err
			return
		}

		if err := c.Start(); err != nil {
			rcloneErrChan <- err
			return
		}

		go func() {
			s := bufio.NewScanner(stderr)
			for s.Scan() {
				l := s.Text()
				if p := strings.Index(l, "https://"); p >= 0 {
					rcloneAddressChan <- l[p:]
					return
				}
			}
		}()
	}()

	select {
	case addr := <-rcloneAddressChan:
		return addr, nil

	case err := <-rcloneErrChan:
		return "", errors.Wrap(err, "rclone failed to start")

	case <-time.After(startupTimeout):
		return "", errors.Errorf("timed out waiting for rclone to start")
	}
}

// New creates new RClone storage with specified options.
// nolint:funlen
func New(ctx context.Context, opt *Options) (blob.Storage, error) {
	// generate directory for all temp files.
	td, err := ioutil.TempDir("", "kopia-rclone")
	if err != nil {
		return nil, errors.Wrap(err, "error getting temporary dir")
	}

	r := &rcloneStorage{
		Options:      *opt,
		temporaryDir: td,
	}

	// TLS key for rclone webdav server.
	temporaryKeyPath := filepath.Join(td, "webdav.key")

	// TLS cert for rclone webdav server.
	temporaryCertPath := filepath.Join(td, "webdav.cert")

	// password file for rclone webdav server.
	temporaryHtpassword := filepath.Join(td, "htpasswd")

	defer func() {
		// if we return this function without setting Storage, make sure to clean everything up.
		if r.Storage == nil {
			r.Close(ctx) //nolint:errcheck
		}
	}()

	// write TLS files.
	cert, key, err := tlsutil.GenerateServerCertificate(ctx, 2048, 365*24*time.Hour, []string{"127.0.0.1"})
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate server certificate")
	}

	if err = tlsutil.WritePrivateKeyToFile(temporaryKeyPath, key); err != nil {
		return nil, errors.Wrap(err, "unable to write WebDAV key")
	}

	if err = tlsutil.WriteCertificateToFile(temporaryCertPath, cert); err != nil {
		return nil, errors.Wrap(err, "unable to write WebDAV cert")
	}

	// temporary username and password to be used when communicating with rclone
	webdavUsername := "u" + uuid.New().String()
	webdavPassword := "p" + uuid.New().String()

	if err = htpasswd.SetPassword(temporaryHtpassword, webdavUsername, webdavPassword, htpasswd.HashBCrypt); err != nil {
		return nil, errors.Wrap(err, "unable to write htpasswd file")
	}

	rcloneExe := defaultRCloneExe
	if opt.RCloneExe != "" {
		rcloneExe = opt.RCloneExe
	}

	arguments := append([]string{
		"serve", "webdav", opt.RemotePath,
		"--addr", "127.0.0.1:0", // allocate random port,
		"--cert", temporaryCertPath,
		"--key", temporaryKeyPath,
		"--htpasswd", temporaryHtpassword,
	}, opt.RCloneArgs...)

	if opt.EmbeddedConfig != "" {
		tmpConfigFile := filepath.Join(r.temporaryDir, "rclone.conf")

		if err = ioutil.WriteFile(tmpConfigFile, []byte(opt.EmbeddedConfig), 0o600); err != nil {
			return nil, errors.Wrap(err, "unable to write config file")
		}

		arguments = append(arguments, "--config", tmpConfigFile)
	}

	r.cmd = exec.CommandContext(ctx, rcloneExe, arguments...) //nolint:gosec
	r.cmd.Env = append(r.cmd.Env, opt.RCloneEnv...)

	log(ctx).Debugf("starting %v %v", rcloneExe, arguments)

	startupTimeout := rcloneStartupTimeout
	if opt.StartupTimeout != 0 {
		startupTimeout = time.Duration(opt.StartupTimeout) * time.Second
	}

	rcloneAddr, err := runRCloneAndWaitForServerAddress(r.cmd, startupTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "unable to start rclone")
	}

	log(ctx).Debugf("detected webdav address: %v", rcloneAddr)

	fingerprintBytes := sha256.Sum256(cert.Raw)

	wst, err := webdav.New(ctx, &webdav.Options{
		URL:                                 rcloneAddr,
		Username:                            webdavUsername,
		Password:                            webdavPassword,
		TrustedServerCertificateFingerprint: hex.EncodeToString(fingerprintBytes[:]),
	})
	if err != nil {
		return nil, errors.Wrap(err, "error connecting to webdav storage")
	}

	r.Storage = wst

	return r, nil
}

func init() {
	blob.AddSupportedStorage(
		rcloneStorageType,
		func() interface{} {
			return &Options{}
		},
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
