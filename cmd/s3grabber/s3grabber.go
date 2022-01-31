package main

import (
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/vinted/S3Grabber/internal/cfg"
	"github.com/vinted/S3Grabber/internal/s3grabber"
	"gopkg.in/alecthomas/kingpin.v2"
)

// initializeLogger initializes a logger with the given parameters.
func initializeLogger(logFormat string, logLevel string) log.Logger {
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

	logger := initializeLogger(*logFormat, *logLevel)
	cfg, err := cfg.ReadConfig(*configFile)
	if err != nil {
		_ = level.Error(logger).Log("msg", "failed to read config file", "path", *configFile, "err", err.Error())
		os.Exit(1)
	}

	if err := s3grabber.RunS3Grabber(logger, cfg); err != nil {
		os.Exit(1)
	}
}
