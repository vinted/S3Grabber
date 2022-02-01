package installer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/vinted/S3Grabber/internal/downloader"
)

func removeContents(dir string) error {
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
// Clears out dir before extracting.
func ExtractTarGz(dir string, gzipStream io.Reader) error {
	uncompressedStream, err := gzip.NewReader(gzipStream)
	if err != nil {
		return fmt.Errorf("creating gzip reader: %w", err)
	}

	if err := removeContents(dir); err != nil {
		return fmt.Errorf("clearing %s: %w", dir, err)
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
			fPath := filepath.Join(dir, header.Name)
			if err := os.Mkdir(fPath, 0755); err != nil {
				return fmt.Errorf("creating dir %s: %w", fPath, err)
			}
		case tar.TypeReg:
			fPath := filepath.Join(dir, header.Name)
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
}

func NewInstaller(bm *downloader.BucketManager, commands []string, bucketPath, installInto string, shellCmd string, logger log.Logger) *Installer {
	return &Installer{
		bm:                      bm,
		lastModTimeByObjectPath: make(map[string]time.Time),
		commands:                commands,
		installInto:             installInto,
		bucketPath:              bucketPath,
		shellCmd:                shellCmd,
		logger:                  logger,
	}
}

func (i *Installer) Install(ctx context.Context) error {
	rc, err := i.getReader(ctx, i.bucketPath, i.installInto)
	if err != nil {
		return err
	}
	// No update.
	if rc == nil {
		return nil
	}
	defer rc.Close()

	// Extract into given path.
	if err := ExtractTarGz(i.installInto, rc); err != nil {
		return fmt.Errorf("extracting %s: %w", i.bucketPath, err)
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
			return fmt.Errorf("executing '%s': %w (stdout %s, stderr %s)", cmd, err, stdout.String(), stderr.String())
		}
	}
	return nil
}

// getReader gets a reader for the specified path if it has been updated
// since the last call.
func (i *Installer) getReader(ctx context.Context, bucketPath, installInto string) (io.ReadCloser, error) {
	mTm, bi, err := i.bm.FindNewestFile(ctx, bucketPath)
	if err != nil {
		return nil, fmt.Errorf("finding newest file: %w", err)
	}
	// Check that modify time is ahead of the captured last mod time.
	// NOTE: this does not do anything useful in single-shot mode, just exists as a safe programming check.
	if mTm.Before(i.lastModTimeByObjectPath[bucketPath]) || mTm.Equal(i.lastModTimeByObjectPath[bucketPath]) {
		_ = level.Debug(i.logger).Log("msg", "last modified time is ahead of the modified time in remote object storage", "modifyTime", mTm, "lastLocalModifyTime", i.lastModTimeByObjectPath[bucketPath])
		return nil, nil
	}

	// Ensure ctime is after modify time.
	fi, err := os.Stat(installInto)
	if err != nil {
		return nil, fmt.Errorf("calling stat %s: %w", installInto, err)
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("got wrong type (%T, expected syscall.Stat_t)", fi.Sys())
	}
	ctime := time.Unix(int64(StatCtime(stat).Sec), int64(StatCtime(stat).Nsec))
	if mTm.Before(ctime) {
		_ = level.Debug(i.logger).Log("msg", "object is older in remote object storage", "modifyTime", mTm, "ctime", ctime)
		return nil, nil
	}

	i.lastModTimeByObjectPath[bucketPath] = mTm

	return i.bm.GetFile(ctx, bucketPath, bi)
}
