# S3Grabber

Service that periodically extracts data from S3 (if needed) and calls a command.

## How S3Grabber Works

* For each "grabber", it looks up for the newest object in the given buckets;
* If the path's `ctime` is _before_ the newest object's modify time then that object is downloaded and extracted into the given path;
* Then, the provided commands are executed using the given shell.

## Example Configuration

Example configuration is provided in `config.yml`.

## Running Tests

Unit tests can be run with `go test -v ./...`.

E2E tests together with unit tests can be run using `docker-compose build tester && docker-compose down -v --remove-orphans && docker-compose up -d && docker-compose run --rm tester go test ./...`.
