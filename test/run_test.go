package test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/vinted/S3Grabber/internal/cfg"
	"github.com/vinted/S3Grabber/internal/downloader"
	"github.com/vinted/S3Grabber/internal/installer"
	"github.com/vinted/S3Grabber/internal/s3grabber"
)

func TestS3GrabberMain(t *testing.T) {
	t.Parallel()

	tmpDir := filepath.Join(os.TempDir(), "s3grabber")
	require.Nil(t, os.MkdirAll(tmpDir, os.ModePerm))
	t.Cleanup(func() {
		require.Nil(t, os.RemoveAll(tmpDir))
	})

	grabberCfg := cfg.GlobalConfig{
		Buckets: map[string]cfg.BucketConfig{
			"test1": {
				Host:      "minio1:9000",
				AccessKey: "foobar",
				SecretKey: "foobardd",
				Bucket:    "test",
			},
			"test2": {
				Host:      "minio2:9000",
				AccessKey: "foobar",
				SecretKey: "foobardd",
				Bucket:    "test",
			},
		},
		Grabbers: map[string]cfg.GrabberConfig{
			"testing": {
				Buckets:  []string{"test1", "test2"},
				File:     "example.tar.gz",
				Path:     tmpDir,
				Commands: []string{fmt.Sprintf("echo foobar > %s", filepath.Join(tmpDir, "somefile"))},
				Timeout:  5 * time.Second,
				Shell:    "/bin/sh",
			},
		},
	}
	err := s3grabber.RunS3Grabber(log.NewLogfmtLogger(os.Stderr), grabberCfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "The specified bucket does not exist")

	// Upload the file to both buckets.
	time.Sleep(1 * time.Second) // To ensure ctime < modify time.
	bm, err := downloader.NewBucketManager([]cfg.BucketConfig{
		grabberCfg.Buckets["test1"], grabberCfg.Buckets["test2"],
	})
	require.NoError(t, err)

	// Only upload to one bucket to check whether it works properly.
	require.NoError(t, bm.CreateBucket(context.Background(), "test", 0))
	require.NoError(t, bm.CreateBucket(context.Background(), "test", 1))
	require.NoError(t, bm.PutFile(context.Background(), "../internal/downloader/example.tar.gz", "/example.tar.gz", 1))

	err = s3grabber.RunS3Grabber(log.NewLogfmtLogger(os.Stderr), grabberCfg)
	require.NoError(t, err)

	checkFileContentEqual(t, filepath.Join(tmpDir, "test"), "Hello world!\n")
	checkFileContentEqual(t, filepath.Join(tmpDir, "somefile"), "foobar\n")

	require.Nil(t, os.RemoveAll(tmpDir))
	require.Nil(t, os.MkdirAll(tmpDir, os.ModePerm))
	err = s3grabber.RunS3Grabber(log.NewLogfmtLogger(os.Stderr), grabberCfg)
	require.NoError(t, err)

	isEmpty, err := installer.IsEmptyDir(tmpDir)
	require.NoError(t, err)
	require.Equal(t, false, isEmpty)
}

func checkFileContentEqual(t *testing.T, path, content string) {
	f, err := os.Open(path)
	require.Nil(t, err)
	t.Cleanup(func() {
		f.Close()
	})
	fileContent, err := io.ReadAll(f)
	require.Nil(t, err)
	require.Equal(t, string(fileContent), string(content))

}
