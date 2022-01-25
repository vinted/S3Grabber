package downloader

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/vinted/S3Grabber/internal/cfg"
)

// BucketManager manages downloading files from multiple buckets.
// The intention is to construct a BucketManager for each Installer.
type BucketManager struct {
	bucketNames []string
	clients     []*minio.Client
}

// GetFile gets the provided file from the specified bucket index that was retrieved from FindNewestFile.
func (m *BucketManager) GetFile(ctx context.Context, path string, bucketIndex int) (io.ReadCloser, error) {
	if err := m.indexInBounds(bucketIndex); err != nil {
		return nil, err
	}
	obj, err := m.clients[bucketIndex].GetObject(ctx, m.bucketNames[bucketIndex], path, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting %s in %s: %w", path, m.clients[bucketIndex].EndpointURL(), err)
	}
	return obj, nil
}

func (m *BucketManager) indexInBounds(bucketIndex int) error {
	if bucketIndex < 0 || bucketIndex >= len(m.clients) {
		return fmt.Errorf("provided bucket index is out of bounds")
	}
	return nil
}

// CreateBucket creates the given bucket. Use only for tests.
func (m *BucketManager) CreateBucket(ctx context.Context, name string, bucketIndex int) error {
	if err := m.indexInBounds(bucketIndex); err != nil {
		return err
	}
	return m.clients[bucketIndex].MakeBucket(ctx, name, minio.MakeBucketOptions{})
}

// PutFile puts the given file into the given path. Use only for tests.
func (m *BucketManager) PutFile(ctx context.Context, filePath, bucketPath string, bucketIndex int) error {
	if err := m.indexInBounds(bucketIndex); err != nil {
		return err
	}
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := m.clients[bucketIndex].PutObject(ctx, m.bucketNames[bucketIndex], bucketPath, f, -1, minio.PutObjectOptions{}); err != nil {
		return err
	}

	return nil
}

// FindNewestFile finds the newest file in all of the buckets with the provided path.
// Returns the modification time and bucket index that later on needs to be passed to GetFile.
func (m *BucketManager) FindNewestFile(ctx context.Context, path string) (modTime time.Time, bucketIndex int, err error) {
	if len(m.clients) == 0 {
		return modTime, bucketIndex, fmt.Errorf("no clients configured")
	}

	const notFoundCode = "NoSuchKey"

	for i, cl := range m.clients {
		objInfo, err := cl.StatObject(ctx, m.bucketNames[i], path, minio.StatObjectOptions{})
		if err != nil && minio.ToErrorResponse(err).Code != notFoundCode {
			return modTime, bucketIndex, fmt.Errorf("getting %s info in %s: %w", path, cl.EndpointURL(), err)
		}
		if minio.ToErrorResponse(err).Code == notFoundCode {
			continue
		}

		if objInfo.LastModified.After(modTime) {
			modTime = objInfo.LastModified
			bucketIndex = i
		}

	}

	if modTime.Equal(time.Time{}) {
		return modTime, bucketIndex, fmt.Errorf("no file has been modified so either they do not exist or there are time synchronization problems")
	}
	return
}

func NewBucketManager(buckets []cfg.BucketConfig) (*BucketManager, error) {
	clients := make([]*minio.Client, 0, len(buckets))
	bucketNames := make([]string, 0, len(buckets))
	for _, bkt := range buckets {
		client, err := minio.New(bkt.Host, &minio.Options{
			Creds:        credentials.NewStaticV4(string(bkt.AccessKey), string(bkt.SecretKey), ""),
			Secure:       false,
			BucketLookup: minio.BucketLookupPath,
		})
		if err != nil {
			return nil, fmt.Errorf("creating client for %s: %w", bkt.Host, err)
		}
		clients = append(clients, client)
		bucketNames = append(bucketNames, bkt.Bucket)
	}
	return &BucketManager{
		clients:     clients,
		bucketNames: bucketNames,
	}, nil
}
