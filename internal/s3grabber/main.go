package s3grabber

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/oklog/run"
	"github.com/vinted/S3Grabber/internal/cfg"
	"github.com/vinted/S3Grabber/internal/downloader"
	"github.com/vinted/S3Grabber/internal/installer"
)

func RunS3Grabber(logger log.Logger, config cfg.GlobalConfig) error {
	installers := make([]*installer.Installer, 0, len(config.Grabbers))
	for grabberName, grabber := range config.Grabbers {
		bucketCfgs := []cfg.BucketConfig{}
		for _, bktName := range grabber.Buckets {
			bkt, ok := config.Buckets[bktName]
			if !ok {
				return fmt.Errorf("failed to find bucket %s for grabber %s", bktName, grabberName)
			}
			bucketCfgs = append(bucketCfgs, bkt)
		}

		bm, err := downloader.NewBucketManager(bucketCfgs)
		if err != nil {
			return fmt.Errorf("constructing bucket manager for grabber %s: %w", grabberName, err)
		}

		installers = append(installers, installer.NewInstaller(grabberName, bm, grabber.Commands, grabber.File, grabber.Path, grabber.Shell, grabber.Timeout, logger))
	}

	g := &run.Group{}
	gctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, i := range installers {
		i := *i
		g.Add(func() error {
			ctx, cancel := context.WithTimeout(gctx, i.GetTimeout())
			defer cancel()

			return i.Install(ctx)
		}, func(e error) {
			if e != nil {
				cancel()
			}
		})
	}

	if err := g.Run(); err != nil {
		return fmt.Errorf("failed running grabbers: %w", err)
	}

	return nil
}
