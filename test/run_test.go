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

var (
	archiveFilename = "example.tar.gz"
	dirName         = "exampledir"
)

func TestS3GrabberMain(t *testing.T) {
	t.Parallel()

	tmpDirArchive := filepath.Join(os.TempDir(), "s3grabber_archive")
	tmpDirDir := filepath.Join(os.TempDir(), "s3grabber_dir")
	require.Nil(t, os.MkdirAll(tmpDirArchive, os.ModePerm))
	require.Nil(t, os.MkdirAll(tmpDirDir, os.ModePerm))
	t.Cleanup(func() {
		require.Nil(t, os.RemoveAll(tmpDirArchive))
		require.Nil(t, os.RemoveAll(tmpDirDir))
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
			"testing_archive": {
				Buckets:  []string{"test1", "test2"},
				File:     &archiveFilename,
				Path:     tmpDirArchive,
				Commands: []string{fmt.Sprintf("echo foobar > %s", filepath.Join(tmpDirArchive, "somefile"))},
				Timeout:  5 * time.Second,
				Shell:    "/bin/sh",
			},
			"testing_dir": {
				Buckets:  []string{"test2"},
				Dir:      &dirName,
				Path:     tmpDirDir,
				Commands: []string{fmt.Sprintf("echo foobar > %s", filepath.Join(tmpDirDir, "some_dir_file"))},
				Timeout:  5 * time.Second,
				Shell:    "/bin/sh",
			},
		},
	}
	attemptedInstall, err := s3grabber.RunS3Grabber(log.NewLogfmtLogger(os.Stderr), grabberCfg)
	require.Error(t, err)
	require.False(t, attemptedInstall)
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
	require.NoError(t, bm.PutFile(context.Background(), "dir_file1.txt", "exampledir/dir_file1.txt", 1))
	require.NoError(t, bm.PutFile(context.Background(), "dir_file2.txt", "exampledir/dir_file2.txt", 1))

	attemptedInstall, err = s3grabber.RunS3Grabber(log.NewLogfmtLogger(os.Stderr), grabberCfg)
	require.NoError(t, err)
	require.True(t, attemptedInstall)

	checkFileContentEqual(t, filepath.Join(tmpDirArchive, "test"), "Hello world!\n")
	checkFileContentEqual(t, filepath.Join(tmpDirDir, "dir_file1.txt"), "test1\n")
	checkFileContentEqual(t, filepath.Join(tmpDirDir, "dir_file2.txt"), "test2\n")
	checkFileContentEqual(t, filepath.Join(tmpDirArchive, "somefile"), "foobar\n")
	checkFileContentEqual(t, filepath.Join(tmpDirDir, "some_dir_file"), "foobar\n")

	require.Nil(t, os.RemoveAll(tmpDirArchive))
	require.Nil(t, os.MkdirAll(tmpDirArchive, os.ModePerm))
	attemptedInstall, err = s3grabber.RunS3Grabber(log.NewLogfmtLogger(os.Stderr), grabberCfg)
	require.NoError(t, err)
	require.True(t, attemptedInstall)

	isEmpty, err := installer.IsEmptyDir(tmpDirArchive)
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
