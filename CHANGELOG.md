- 0.2.0

* BREAKING: change `--config-file` into `--config-path`. Now if the provided
  path points to a file then that file is read as a configuration file; if it
  points to a directory then all files inside of it (not recursive) with the
  suffix `.yml` and/or `.yaml` are read and merged into one configuration that
  is later used. This simplifies deployment with configuration management
  systems such as Chef where you will be able to have multiple resources that
  later result in multiple configuration files.
* BREAKING: move `shell` and `timeout` parameters into `grabbers`. This allows
  more customization.
