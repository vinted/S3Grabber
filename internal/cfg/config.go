package cfg

import (
	"os"
	"regexp"
	"strings"
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
