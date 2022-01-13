package cfg

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

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
	File     string
	Paths    []string
	Commands []string
}

type GlobalConfig struct {
	Buckets  map[string]BucketConfig
	Grabbers map[string]GrabberConfig
}

func ReadConfig(path string) (GlobalConfig, error) {
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
