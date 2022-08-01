package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

func setupHTTPServer(httpAddress string) (prometheus.Registerer, *http.Server) {
	metricsRegistry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix("s3grabber_", metricsRegistry)

	mux := http.NewServeMux()
	server := &http.Server{Addr: httpAddress, Handler: mux}
	mux.Handle("/metrics", promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/-/ready", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "OK")
	}))
	mux.HandleFunc("/-/healthy", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "OK")
	}))

	return registerer, server
}

type s3grabberMetrics struct {
	syncErrorsTotal             prometheus.Counter
	lastSuccessfulSyncTimestamp prometheus.Gauge
}

func main() {
	configFile := kingpin.Flag("config-path", "Path to the configuration file or directory").Required().String()
	interval := kingpin.Flag("interval", "How often the process should do the synchronization").Duration()
	httpAddress := kingpin.Flag("http-address", "Listening address for the HTTP server").Default(":10010").String()
	logFormat := kingpin.Flag("log-format", "Log format").Default("LOGFMT").Enum("JSON", "LOGFMT")
	logLevel := kingpin.Flag("log-level", "Log level").Default("DEBUG").Enum("DEBUG", "INFO", "WARN", "ERROR")

	kingpin.Parse()

	logger := initializeLogger(*logFormat, *logLevel)
	cfg, err := cfg.ReadConfig(*configFile)
	if err != nil {
		_ = level.Error(logger).Log("msg", "failed to read config file", "path", *configFile, "err", err.Error())
		os.Exit(1)
	}

	registerer, server := setupHTTPServer(*httpAddress)

	m := &s3grabberMetrics{
		syncErrorsTotal: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "sync_errors_total",
			Help: "How many errors occurred during sync",
		}),
		lastSuccessfulSyncTimestamp: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "sync_last_success",
			Help: "Last time a sync was successful",
		}),
	}

	g := &run.Group{}
	ctx, cancel := context.WithCancel(context.Background())

	g.Add(func() error {
		if interval != nil && *interval != 0 {
			// NOTE(GiedriusS): start with true to avoid an alert at boot.
			var lastSyncSucceeded bool = true

			t := time.NewTicker(*interval)
			defer t.Stop()

			for {
				// If attempted && err == nil -> lastSyncSucceeded = true
				// If !attempted && err == nil -> nothing
				// If attempted && err != nil -> lastSyncSucceeded = false
				if attemptedInstall, err := s3grabber.RunS3Grabber(logger, cfg); err != nil {
					if attemptedInstall {
						lastSyncSucceeded = false
					}

					m.syncErrorsTotal.Inc()
					_ = level.Error(logger).Log("msg", "failed to run S3Grabber iteration", "err", err.Error())
				} else {
					if attemptedInstall {
						lastSyncSucceeded = true
					}

					if lastSyncSucceeded {
						m.lastSuccessfulSyncTimestamp.SetToCurrentTime()
					}
				}
				<-t.C
			}
		} else {
			if _, err := s3grabber.RunS3Grabber(logger, cfg); err != nil {
				return err
			} else {
				m.lastSuccessfulSyncTimestamp.SetToCurrentTime()
			}
		}
		return nil
	}, func(err error) {
		cancel()
	})

	g.Add(func() error {
		return server.ListenAndServe()
	}, func(err error) {
		timeoutCtx, tCancel := context.WithTimeout(ctx, 5*time.Second)
		defer tCancel()
		_ = server.Shutdown(timeoutCtx)
		cancel()
	})

	if err := g.Run(); err != nil {
		_ = level.Error(logger).Log("msg", "failed to run S3Grabber", "err", err.Error())
		os.Exit(1)
	}
}
