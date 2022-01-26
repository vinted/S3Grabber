package main

import (
	"context"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/vinted/S3Grabber/internal/cfg"
	"github.com/vinted/S3Grabber/internal/downloader"
	"github.com/vinted/S3Grabber/internal/installer"
	"gopkg.in/alecthomas/kingpin.v2"
)

// InitializeLogger initializes a logger with the given parameters.
func InitializeLogger(logFormat string, logLevel string) log.Logger {
	var logger log.Logger

	switch logFormat {
	case "JSON":
		logger = log.NewJSONLogger(os.Stdout)
	case "LOGFMT":
		logger = log.NewLogfmtLogger(os.Stdout)
	}

	switch logLevel {
	case "DEBUG":
		logger = level.NewFilter(logger, level.AllowDebug())
	case "INFO":
		logger = level.NewFilter(logger, level.AllowInfo())
	case "WARN":
		logger = level.NewFilter(logger, level.AllowWarn())
	case "ERROR":
		logger = level.NewFilter(logger, level.AllowError())
	}

	return logger
}

func main() {
	configFile := kingpin.Flag("config-file", "Path to the configuration file").Required().String()
	logFormat := kingpin.Flag("log-format", "Log format").Default("LOGFMT").Enum("JSON", "LOGFMT")
	logLevel := kingpin.Flag("log-level", "Log level").Default("DEBUG").Enum("DEBUG", "INFO", "WARN", "ERROR")

	kingpin.Parse()

	logger := InitializeLogger(*logFormat, *logLevel)
	cfg, err := cfg.ReadConfig(*configFile)
	if err != nil {
		_ = level.Error(logger).Log("msg", "failed to read config file", "path", *configFile, "err", err.Error())
		os.Exit(1)
	}

	runS3Grabber(logger, cfg)
}

func runS3Grabber(logger log.Logger, config cfg.GlobalConfig) {
	installers := make([]*installer.Installer, 0, len(config.Grabbers))
	for grabberName, grabber := range config.Grabbers {
		bucketCfgs := []cfg.BucketConfig{}
		for _, bktName := range grabber.Buckets {
			bkt, ok := config.Buckets[bktName]
			if !ok {
				_ = level.Error(logger).Log("msg", "failed to find bucket", "bucket_name", bktName, "grabber_name", grabberName)
				os.Exit(1)
			}
			bucketCfgs = append(bucketCfgs, bkt)
		}

		bm, err := downloader.NewBucketManager(bucketCfgs)
		if err != nil {
			_ = level.Error(logger).Log("msg", "failed to construct a new bucket manager", "grabber_name", grabberName)
		}

		installers = append(installers, installer.NewInstaller(bm, grabber.Commands, grabber.File, grabber.Path, config.Shell))
	}

	g := &run.Group{}
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()

	for _, i := range installers {
		g.Add(func() error {
			return i.Install(ctx)
		}, func(error) {
			cancel()
		})
	}

	if err := g.Run(); err != nil {
		_ = level.Error(logger).Log("failed to run grabber(-s)", "err", err.Error())
		os.Exit(1)
	}
}
