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
	"time"

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
func extractTarGz(dir string, gzipStream io.Reader) error {
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
}

func NewInstaller(bm *downloader.BucketManager, commands []string, bucketPath, installInto string, shellCmd string) *Installer {
	return &Installer{
		bm:                      bm,
		lastModTimeByObjectPath: make(map[string]time.Time),
		commands:                commands,
		installInto:             installInto,
		bucketPath:              bucketPath,
		shellCmd:                shellCmd,
	}
}

func (i *Installer) Install(ctx context.Context) error {
	rc, err := i.getReader(ctx, i.bucketPath)
	if err != nil {
		return err
	}
	// No update.
	if rc == nil {
		return nil
	}
	defer rc.Close()

	// Extract into given path.
	if err := extractTarGz(i.installInto, rc); err != nil {
		return fmt.Errorf("extracting %s: %w", i.bucketPath, err)
	}

	// Execute each command one by one.
	for _, cmd := range i.commands {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		cmd := exec.Command(i.shellCmd, "-c", cmd)
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
func (i *Installer) getReader(ctx context.Context, path string) (io.ReadCloser, error) {
	mTm, bi, err := i.bm.FindNewestFile(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("finding newest file: %w", err)
	}
	if mTm.Before(i.lastModTimeByObjectPath[path]) || mTm.Equal(i.lastModTimeByObjectPath[path]) {
		return nil, nil
	}
	i.lastModTimeByObjectPath[path] = mTm

	return i.bm.GetFile(ctx, path, bi)
}
