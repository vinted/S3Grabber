# Changelog

## [0.10.0](https://github.com/vinted/S3Grabber/releases/tag/0.10.0)

* Added automatic deletion of files from local storage when they are deleted from
  the S3 bucket. S3Grabber now synchronizes not only new and updated files but
  also removes files locally that no longer exist in the remote S3 storage.
* **Heads Up**: This version changes the synchronization behavior. Files deleted
  from the S3 bucket will now be automatically removed from the local storage.
  If you have files in your local directory that you want to preserve independently
  from S3, please review your setup before upgrading.

## [0.3.0](https://github.com/vinted/S3Grabber/releases/tag/0.3.0)

* Added waiting mode. Now if `--interval` is passed and it is not zero then
  `S3Grabber` continuously tries to do a synchronization.
* Added HTTP server with metrics. New parameter `--http-address` controls the
  listening address of the HTTP server. `/metrics` exposes those metrics.
  There is also `/-/healthy` and `/-/ready` endpoints which show whether
  S3Grabber is healthy and ready respectively.

## [0.2.0](https://github.com/vinted/S3Grabber/releases/tag/0.2.0)

* BREAKING: change `--config-file` into `--config-path`. Now if the provided
  path points to a file then that file is read as a configuration file; if it
  points to a directory then all files inside of it (not recursive) with the
  suffix `.yml` and/or `.yaml` are read and merged into one configuration that
  is later used. This simplifies deployment with configuration management
  systems such as Chef where you will be able to have multiple resources that
  later result in multiple configuration files.
* BREAKING: move `shell` and `timeout` parameters into `grabbers`. This allows
  more customization.
