package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/signer"
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

type BucketFile struct {
	Key     string
	Content io.ReadCloser
	Err     error
}

// GetFiles gets all the files in provided path from the specified bucket index that was retrieved from FindNewestFile.
func (m *BucketManager) GetFiles(ctx context.Context, prefix string, bucketIndex int) (<-chan BucketFile, error) {
	if err := m.indexInBounds(bucketIndex); err != nil {
		return nil, err
	}
	bucketClient := m.clients[bucketIndex]
	bucketName := m.bucketNames[bucketIndex]

	bucketObjects := make(chan BucketFile, 1)

	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	go func() {
		defer close(bucketObjects)
		var err error
		objInfoCh := bucketClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: prefix})
		for objInfo := range objInfoCh {
			// stop fetching files as soon as first error is encountered
			if err != nil {
				continue
			}
			if objInfo.Err != nil {
				err = fmt.Errorf("listing objects: %w", err)
				bucketObjects <- BucketFile{
					Err: err,
				}
				continue
			}

			if objInfo.Key == prefix {
				continue // not a file: prefix (directory)
			}

			obj, err := bucketClient.GetObject(ctx, bucketName, objInfo.Key, minio.GetObjectOptions{})
			if err != nil {
				err = fmt.Errorf("getting object %s: %w", objInfo.Key, err)
				bucketObjects <- BucketFile{
					Err: err,
				}
				continue
			}
			if !strings.HasPrefix(objInfo.Key, prefix) {
				// should not happen, but just to ensure safe prefix removal
				err = fmt.Errorf("key does not have expected prefix %s: %s", prefix, objInfo.Key)
				bucketObjects <- BucketFile{
					Err: err,
				}
				continue
			}
			key := objInfo.Key[len(prefix):]

			bucketObjects <- BucketFile{
				Key:     key,
				Content: obj,
				Err:     nil,
			}
		}
	}()

	return bucketObjects, nil
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

	var (
		errs       error
		checkedOne bool
	)

	for i, cl := range m.clients {
		objInfo, err := cl.StatObject(ctx, m.bucketNames[i], path, minio.StatObjectOptions{})
		if err != nil && minio.ToErrorResponse(err).Code != notFoundCode {
			errs = multierror.Append(errs, err)
			continue
		}
		if minio.ToErrorResponse(err).Code == notFoundCode {
			continue
		}

		if objInfo.LastModified.After(modTime) {
			modTime = objInfo.LastModified
			bucketIndex = i
			checkedOne = true
		}
	}

	if !checkedOne {
		if errs != nil {
			return modTime, bucketIndex, errs
		}
		return modTime, bucketIndex, fmt.Errorf("no file has been modified so either they do not exist or there are time synchronization problems")
	}
	return
}

// FindNewestInPrefix finds the newest file in all of the buckets for the provided prefix.
// Returns the modification time and bucket index that later on needs to be passed to GetFiles.
func (m *BucketManager) FindNewestInPrefix(ctx context.Context, prefix string) (modTime time.Time, bucketIndex int, err error) {
	if len(m.clients) == 0 {
		return modTime, bucketIndex, fmt.Errorf("no clients configured")
	}

	const notFoundCode = "NoSuchKey"

	var (
		errs       error
		checkedOne bool
	)

	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	for i, cl := range m.clients {
		objCh := cl.ListObjects(ctx, m.bucketNames[i], minio.ListObjectsOptions{Prefix: prefix})
		for objInfo := range objCh {
			err := objInfo.Err
			if err != nil && minio.ToErrorResponse(err).Code != notFoundCode {
				errs = multierror.Append(errs, err)
				continue
			}
			if minio.ToErrorResponse(err).Code == notFoundCode {
				continue
			}

			if objInfo.LastModified.After(modTime) {
				modTime = objInfo.LastModified
				bucketIndex = i
				checkedOne = true
			}
		}
	}

	if !checkedOne {
		if errs != nil {
			return modTime, bucketIndex, errs
		}
		return modTime, bucketIndex, fmt.Errorf("no file has been modified so either they do not exist or there are time synchronization problems")
	}
	return
}

type hostHeaderAddRoundtripper struct {
	rt                   http.RoundTripper
	customHostHeader     string
	accessKey, secretKey string
}

func (rt *hostHeaderAddRoundtripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if rt.customHostHeader == "" {
		return rt.rt.RoundTrip(req)
	}
	req.Host = rt.customHostHeader
	req = signer.SignV4(*req, rt.accessKey, rt.secretKey, "", "")
	return rt.rt.RoundTrip(req)
}

func NewBucketManager(buckets []cfg.BucketConfig) (*BucketManager, error) {
	clients := make([]*minio.Client, 0, len(buckets))
	bucketNames := make([]string, 0, len(buckets))
	for _, bkt := range buckets {
		client, err := minio.New(bkt.Host, &minio.Options{
			Creds:        credentials.NewStaticV4(string(bkt.AccessKey), string(bkt.SecretKey), ""),
			Secure:       false,
			BucketLookup: minio.BucketLookupPath,
			Transport: &hostHeaderAddRoundtripper{
				customHostHeader: bkt.CustomHostHeader,
				rt:               http.DefaultTransport,
				accessKey:        string(bkt.AccessKey),
				secretKey:        string(bkt.SecretKey),
			},
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
