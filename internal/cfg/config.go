package cfg

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"gopkg.in/yaml.v3"
)

func replaceWithEnvironmentVariables(input string) string {
	re := regexp.MustCompile(`\${(\w+)}`)
	return re.ReplaceAllStringFunc(input, func(s string) string {
		trimmedKey := strings.Trim(input, `${}`)
		val := os.Getenv(trimmedKey)
		return val
	})
}

type credentialString string

func (c *credentialString) UnmarshalYAML(unmarshal func(interface{}) error) error {
	ret := ""
	if err := unmarshal(&ret); err != nil {
		return err
	}
	ret = replaceWithEnvironmentVariables(ret)
	*c = credentialString(ret)
	return nil
}

type BucketConfig struct {
	Host      string
	AccessKey credentialString `yaml:"access_key"`
	SecretKey credentialString `yaml:"secret_key"`
	Bucket    string
}

type GrabberConfig struct {
	Buckets  []string
	File     *string
	Dir      *string
	Path     string
	Commands []string
	Timeout  time.Duration
	Shell    string
}

type GlobalConfig struct {
	Buckets  map[string]BucketConfig  `yaml:"buckets"`
	Grabbers map[string]GrabberConfig `yaml:"grabbers"`
}

func (gc *GlobalConfig) Merge(other *GlobalConfig) error {
	for bucketName := range other.Buckets {
		if _, ok := gc.Buckets[bucketName]; ok {
			return fmt.Errorf("found duplicated bucket name %s", bucketName)
		}
	}

	for grabberName := range other.Grabbers {
		if _, ok := gc.Grabbers[grabberName]; ok {
			return fmt.Errorf("found duplicated grabber name %s", grabberName)
		}
	}

	for bucketName := range other.Buckets {
		gc.Buckets[bucketName] = other.Buckets[bucketName]
	}

	for grabberName, grabberCfg := range other.Grabbers {
		if grabberCfg.Shell == "" {
			grabberCfg.Shell = "/bin/sh"
		}
		if grabberCfg.Timeout == 0 {
			grabberCfg.Timeout = 5 * time.Second
		}
		gc.Grabbers[grabberName] = grabberCfg
	}

	return nil
}

func (gc *GlobalConfig) Validate() error {
	var errs error
	for name, g := range gc.Grabbers {
		if g.File == nil && g.Dir == nil {
			errs = multierror.Append(errs, fmt.Errorf("grabber %s: either file or dir should be specified", name))
		}
	}

	return errs
}

func readFromPath(originalPath string) (GlobalConfig, error) {
	cfg := GlobalConfig{
		Buckets:  map[string]BucketConfig{},
		Grabbers: map[string]GrabberConfig{},
	}

	if err := filepath.WalkDir(originalPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == originalPath {
			return nil
		}
		if d.IsDir() {
			return filepath.SkipDir
		}

		if !strings.HasSuffix(path, ".yml") && !strings.HasSuffix(path, ".yaml") {
			return nil
		}

		fileCfg, err := ReadConfig(path)
		if err != nil {
			return fmt.Errorf("reading config from %s: %w", path, err)
		}

		if err := cfg.Merge(&fileCfg); err != nil {
			return fmt.Errorf("merging %s: %w", path, err)
		}

		return nil
	}); err != nil {
		return cfg, fmt.Errorf("walking %s: %w", originalPath, err)
	}

	err := cfg.Validate()
	if err != nil {
		return cfg, fmt.Errorf("invalid config provided: %w", err)
	}

	return cfg, nil
}

func ReadConfig(path string) (GlobalConfig, error) {
	if fi, err := os.Stat(path); err != nil {
		return GlobalConfig{}, fmt.Errorf("calling stat on %s: %w", path, err)
	} else if fi.IsDir() {
		return readFromPath(path)
	}

	f, err := os.Open(path)
	if err != nil {
		return GlobalConfig{}, fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close()

	content, err := io.ReadAll(f)
	if err != nil {
		return GlobalConfig{}, fmt.Errorf("reading %s: %w", path, err)
	}

	ret := GlobalConfig{}
	if err := yaml.Unmarshal(content, &ret); err != nil {
		return GlobalConfig{}, fmt.Errorf("parsing YAML %s: %w", path, err)
	}

	return ret, nil
}
