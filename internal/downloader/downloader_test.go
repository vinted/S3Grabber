package downloader_test

import (
	"context"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/assert"
	"github.com/vinted/S3Grabber/internal/cfg"
	"github.com/vinted/S3Grabber/internal/downloader"
	"github.com/vinted/S3Grabber/internal/installer"
)

func TestDownloadFile(t *testing.T) {
	t.Run("negative tests", func(t *testing.T) {
		bm, err := downloader.NewBucketManager([]cfg.BucketConfig{})
		assert.Nil(t, err)

		rc, err := bm.GetFile(context.Background(), "/test/a", -1)
		assert.Nil(t, rc)
		assert.NotNil(t, err)

		rc, err = bm.GetFile(context.Background(), "/test/a", 0)
		assert.Nil(t, rc)
		assert.NotNil(t, err)
	})
	t.Run("happy path", func(t *testing.T) {
		backend1, backend2 := s3mem.New(), s3mem.New()
		faker1 := gofakes3.New(backend1)
		faker2 := gofakes3.New(backend2)

		ts1 := httptest.NewServer(faker1.Server())
		t.Cleanup(ts1.Close)
		ts2 := httptest.NewServer(faker2.Server())
		t.Cleanup(ts2.Close)

		bm, err := downloader.NewBucketManager([]cfg.BucketConfig{
			{
				Host:      strings.TrimPrefix(ts1.URL, "http://"),
				AccessKey: "something",
				SecretKey: "something",
				Bucket:    "coolbucket",
			},
			{
				Host:      strings.TrimPrefix(ts2.URL, "http://"),
				AccessKey: "something",
				SecretKey: "something",
				Bucket:    "coolbucket",
			},
		})
		assert.Nil(t, err)

		// Upload the same file to both buckets.
		assert.Nil(t, bm.CreateBucket(context.Background(), "coolbucket", 0))
		assert.Nil(t, bm.CreateBucket(context.Background(), "coolbucket", 1))
		assert.Nil(t, bm.PutFile(context.Background(), "./example.tar.gz", "/example.tar.gz", 0))
		assert.Nil(t, bm.PutFile(context.Background(), "./example.tar.gz", "/example.tar.gz", 1))

		// Get it -> extract, read file, delete.
		modTime1, bi, err := bm.FindNewestFile(context.Background(), "/example.tar.gz")
		assert.Nil(t, err)

		rc, err := bm.GetFile(context.Background(), "/example.tar.gz", bi)
		assert.Nil(t, err)

		tmpDir := filepath.Join(os.TempDir(), "downloader-test")
		t.Cleanup(func() {
			_ = os.RemoveAll(tmpDir)
		})
		assert.Nil(t, os.MkdirAll(tmpDir, os.ModePerm))

		assert.Nil(t, installer.ExtractTarGz(log.NewNopLogger(), "foo", tmpDir, rc))
		f, err := os.Open(filepath.Join(tmpDir, "test"))
		assert.Nil(t, err)
		t.Cleanup(func() {
			_ = f.Close()
		})

		content, err := io.ReadAll(f)
		assert.Nil(t, err)
		assert.Equal(t, "Hello world!\n", string(content))

		// Update that file.
		updateTo := 0
		if bi == 0 {
			updateTo = 1
		}

		time.Sleep(1 * time.Second)
		assert.Nil(t, bm.PutFile(context.Background(), "./example.tar.gz", "/example.tar.gz", updateTo))
		modTime2, bi2, err := bm.FindNewestFile(context.Background(), "/example.tar.gz")
		assert.Nil(t, err)
		assert.NotEqual(t, bi, bi2)
		assert.NotEqual(t, modTime1, modTime2)
	})
}
