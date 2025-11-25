package installer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	cp "github.com/otiai10/copy"
	"github.com/vinted/S3Grabber/internal/downloader"
)

func removeContents(dir string) error {
	return removeContentsWithPrefix(dir, "")
}

// removeContentsWithPrefix removes all files in dir that match the given prefix.
// If prefix is empty, all files are removed (equivalent to removeContents).
// If prefix is specified, only files starting with that prefix are removed.
func removeContentsWithPrefix(dir string, prefix string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("opening %s: %w", dir, err)
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		// If prefix is specified, only remove files matching the prefix
		if prefix != "" && !filepath.HasPrefix(name, prefix) {
			continue
		}
		fn := filepath.Join(dir, name)
		err = os.RemoveAll(fn)
		if err != nil {
			return fmt.Errorf("removing %s: %w", fn, err)
		}
	}
	return nil
}

// Adopted from
// https://stackoverflow.com/questions/57639648/how-to-decompress-tar-gz-file-in-go.
// Clears out dir before extracting. If replacePrefix is specified, only files
// matching that prefix are removed from dir, allowing partial updates.
func ExtractTarGz(l log.Logger, uniqueName string, dir string, replacePrefix string, gzipStream io.Reader) error {
	tmpDir, err := os.MkdirTemp("", uniqueName)
	if err != nil {
		return fmt.Errorf("creating temp dir: %w", err)
	}

	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			_ = level.Debug(l).Log("msg", "failed best effort clean up", "dir", tmpDir, "err", err)
		}
	}()
	uncompressedStream, err := gzip.NewReader(gzipStream)
	if err != nil {
		return fmt.Errorf("creating gzip reader: %w", err)
	}

	tarReader := tar.NewReader(uncompressedStream)

	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("reading tar: %w", err)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			fPath := filepath.Join(tmpDir, header.Name)
			if err := os.Mkdir(fPath, 0755); err != nil {
				return fmt.Errorf("creating dir %s: %w", fPath, err)
			}
		case tar.TypeReg:
			fPath := filepath.Join(tmpDir, header.Name)
			outFile, err := os.Create(fPath)
			if err != nil {
				return fmt.Errorf("creating file %s: %w", fPath, err)
			}
			defer outFile.Close()
			if _, err := io.Copy(outFile, tarReader); err != nil {
				return fmt.Errorf("copying file %s: %w", fPath, err)
			}

		default:
			return fmt.Errorf("unknown type %v in %s", header.Typeflag, header.Name)
		}
	}

	// Copy over from tmpDir.
	// If replacePrefix is specified, only remove files matching that prefix.
	// Otherwise, remove all files (default behavior).
	if err := removeContentsWithPrefix(dir, replacePrefix); err != nil {
		return fmt.Errorf("clearing %s: %w", dir, err)
	}

	if err := cp.Copy(tmpDir, dir, cp.Options{
		PermissionControl: cp.DoNothing,
		Sync:              true,
	}); err != nil {
		return fmt.Errorf("copying %s to %s: %w", tmpDir, dir, err)
	}
	return nil
}

// Installer extracts files and runs commands if needed.
type Installer struct {
	commands                []string
	installInto             string
	bucketPath              string
	lastModTimeByObjectPath map[string]time.Time

	bm       *downloader.BucketManager
	shellCmd string
	logger   log.Logger
	timeout  time.Duration

	extracter extracter
}

type extracter interface {
	findNewestFile(ctx context.Context) (lastUpdated time.Time, bucketIndex int, err error)
	extractFiles(ctx context.Context, bucketIndex int) (bool, error)
}

func NewArchiveInstaller(name string, bm *downloader.BucketManager, commands []string, bucketPath, installInto string, shellCmd string, timeout time.Duration, replacePrefix string, logger log.Logger) *Installer {
	extracter := &archiveExtracter{
		bucketPath:    bucketPath,
		bm:            bm,
		logger:        logger,
		name:          name,
		installInto:   installInto,
		replacePrefix: replacePrefix,
	}
	return newInstaller(name, bm, commands, bucketPath, installInto, shellCmd, timeout, logger, extracter)
}

func NewDirectoryInstaller(name string, bm *downloader.BucketManager, commands []string, bucketPath, installInto string, shellCmd string, timeout time.Duration, replacePrefix string, logger log.Logger) *Installer {
	extracter := &directoryExtracter{
		bucketPrefix:  bucketPath,
		bm:            bm,
		logger:        logger,
		name:          name,
		installInto:   installInto,
		replacePrefix: replacePrefix,
	}
	return newInstaller(name, bm, commands, bucketPath, installInto, shellCmd, timeout, logger, extracter)
}

func newInstaller(name string, bm *downloader.BucketManager, commands []string, bucketPath, installInto string, shellCmd string, timeout time.Duration, logger log.Logger, extracter extracter) *Installer {
	return &Installer{
		bm:                      bm,
		lastModTimeByObjectPath: make(map[string]time.Time),
		commands:                commands,
		installInto:             installInto,
		bucketPath:              bucketPath,
		shellCmd:                shellCmd,
		logger:                  logger,
		timeout:                 timeout,
		extracter:               extracter,
	}
}

func (i *Installer) GetTimeout() time.Duration {
	return i.timeout
}

func IsEmptyDir(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

func (i *Installer) Install(ctx context.Context) (attemptedInstall bool, rerr error) {
	isEmpty, err := IsEmptyDir(i.installInto)
	if err != nil {
		_ = level.Debug(i.logger).Log("msg", "failed to check if dir is empty", "err", err.Error(), "dir", i.installInto)
	}

	doInstall := false
	if isEmpty {
		_ = level.Debug(i.logger).Log("msg", "executing installation because the provided dir is empty", "dir", i.installInto)
		doInstall = true
	}

	bucketIndex, err := i.checkLastModTime(ctx, i.installInto)
	if err != nil && !errors.Is(err, ErrNoUpdate) {
		return false, err
	} else if err == nil {
		_ = level.Debug(i.logger).Log("msg", "executing installation because we have found an update", "dir", i.installInto, "path", i.bucketPath)
		doInstall = true
	}

	if !doInstall {
		return false, nil
	}

	attempted, err := i.extracter.extractFiles(ctx, bucketIndex)
	if err != nil {
		return attempted, fmt.Errorf("extracting files: %w", err)
	}

	// Execute each command one by one.
	for _, cmd := range i.commands {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		cmd := exec.CommandContext(ctx, i.shellCmd, "-c", cmd)
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		if err != nil {
			return true, fmt.Errorf("executing '%s': %w (stdout %s, stderr %s)", cmd, err, stdout.String(), stderr.String())
		}
	}
	return true, nil
}

// ErrNoUpdate is an error returned when there was no update in remote object storage
// since the last call.
var ErrNoUpdate = errors.New("no update since the last check")

// checkLastModTime finds the newest updated object in all provided buckets.
// If there was no update since the last check then it returns ErrNoUpdate.
// If there was an update then it returns the bucket's index.
func (i *Installer) checkLastModTime(ctx context.Context, installInto string) (int, error) {
	mTm, bi, err := i.extracter.findNewestFile(ctx)
	if err != nil {
		return bi, fmt.Errorf("finding newest file: %w", err)
	}

	// Check that modify time is ahead of the captured last mod time.
	// NOTE: this does not do anything useful in single-shot mode, just exists as a safe programming check.
	if mTm.Before(i.lastModTimeByObjectPath[i.bucketPath]) || mTm.Equal(i.lastModTimeByObjectPath[i.bucketPath]) {
		_ = level.Debug(i.logger).Log("msg", "last modified time is ahead of the modified time in remote object storage", "modifyTime", mTm, "lastLocalModifyTime", i.lastModTimeByObjectPath[i.bucketPath])
		return bi, ErrNoUpdate
	}

	// Ensure ctime is after modify time.
	fi, err := os.Stat(installInto)
	if err != nil {
		return bi, fmt.Errorf("calling stat %s: %w", installInto, err)
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return bi, fmt.Errorf("got wrong type (%T, expected syscall.Stat_t)", fi.Sys())
	}
	ctime := time.Unix(int64(StatCtime(stat).Sec), int64(StatCtime(stat).Nsec))
	if mTm.Before(ctime) {
		_ = level.Debug(i.logger).Log("msg", "object is older in remote object storage", "modifyTime", mTm, "ctime", ctime)
		return bi, ErrNoUpdate
	}

	i.lastModTimeByObjectPath[i.bucketPath] = mTm

	return bi, nil
}

type archiveExtracter struct {
	bucketPath    string
	bm            *downloader.BucketManager
	logger        log.Logger
	name          string
	installInto   string
	replacePrefix string
}

func (e *archiveExtracter) findNewestFile(ctx context.Context) (lastUpdated time.Time, bucketIndex int, err error) {
	return e.bm.FindNewestFile(ctx, e.bucketPath)
}

func (e *archiveExtracter) extractFiles(ctx context.Context, bucketIndex int) (bool, error) {
	rc, err := e.bm.GetFile(ctx, e.bucketPath, bucketIndex)
	if err != nil {
		return false, err
	}
	defer rc.Close()

	// Extract into given path.
	if err := ExtractTarGz(e.logger, e.name, e.installInto, e.replacePrefix, rc); err != nil {
		return true, fmt.Errorf("extracting %s: %w", e.bucketPath, err)
	}

	return true, nil
}

type directoryExtracter struct {
	bucketPrefix  string
	bm            *downloader.BucketManager
	logger        log.Logger
	name          string
	installInto   string
	replacePrefix string
}

func (e *directoryExtracter) findNewestFile(ctx context.Context) (lastUpdated time.Time, bucketIndex int, err error) {
	return e.bm.FindNewestInPrefix(ctx, e.bucketPrefix)
}

func (e *directoryExtracter) extractFiles(ctx context.Context, bucketIndex int) (bool, error) {
	filesCh, err := e.bm.GetFiles(ctx, e.bucketPrefix, bucketIndex)
	if err != nil {
		return false, fmt.Errorf("starting to download files from the bucket: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", e.name)
	if err != nil {
		return false, fmt.Errorf("creating temp dir: %w", err)
	}

	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			_ = level.Debug(e.logger).Log("msg", "failed best effort clean up", "dir", tmpDir, "err", err)
		}
	}()

	for f := range filesCh {
		err = func() error {
			if f.Content != nil {
				defer f.Content.Close()
			}
			if err != nil || f.Err != nil {
				return err
			}

			tmpFilePath := path.Join(tmpDir, f.Key)
			tmpFile, err := os.Create(tmpFilePath)
			if err != nil {
				return fmt.Errorf("creating temp file %s: %w", tmpFilePath, err)
			}
			defer tmpFile.Close()

			_, err = io.Copy(tmpFile, f.Content)
			if err != nil {
				return fmt.Errorf("copying object content to temp file %s: %w", tmpFilePath, err)
			}

			return nil
		}()
	}
	if err != nil {
		return false, fmt.Errorf("saving remote objects: %w", err)
	}

	// Copy over from tmpDir.
	// If replacePrefix is specified, only remove files matching that prefix.
	// Otherwise, remove all files (default behavior).
	if err := removeContentsWithPrefix(e.installInto, e.replacePrefix); err != nil {
		return true, fmt.Errorf("clearing %s: %w", e.installInto, err)
	}

	if err := cp.Copy(tmpDir, e.installInto, cp.Options{
		PermissionControl: cp.DoNothing,
		Sync:              true,
	}); err != nil {
		return true, fmt.Errorf("copying %s to %s: %w", tmpDir, e.installInto, err)
	}

	return true, nil
}
