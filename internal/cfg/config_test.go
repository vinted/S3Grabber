package cfg

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestLoadFromPath(t *testing.T) {
	t.Parallel()

	for _, tcase := range []struct {
		fileContent map[string]string
		expectedErr bool
		name        string
		expectedCfg GlobalConfig
	}{
		{
			fileContent: map[string]string{
				"test2.yml": `---
buckets:
  lithuania:
    host: foo.bar
    access_key: aabb
    secret_key: bbaa
    bucket: test
grabbers:
  alerting_rules:
    shell: "/bin/sh"
    buckets:
      - lithuania
    file: "alerting_rules.tar.gz"
    path: "/etc/prometheus/rules"
    commands:
      - "kill -HUP $(pidof prometheus)"`,
				"test.yml": `---
buckets:
  lithuania:
    host: foo.bar
    access_key: aabb
    secret_key: bbaa
    bucket: test
grabbers:
  alerting_rules:
    shell: "/bin/sh"
    buckets:
      - lithuania
    file: "alerting_rules.tar.gz"
    path: "/etc/prometheus/rules"
    commands:
      - "kill -HUP $(pidof prometheus)"`,
			},
			expectedErr: true,
			name:        "duplicated names",
		},
		{
			name: "proper load",
			fileContent: map[string]string{
				"test2.yml": `---
buckets:
  aaa:
    host: foo.bar
    access_key: aabb
    secret_key: bbaa
    bucket: test
grabbers:
  bbb:
    shell: "/bin/sh"
    buckets:
      - aaa
    file: "alerting_rules.tar.gz"
    path: "/etc/prometheus/rules"
    commands:
      - "kill -HUP $(pidof prometheus)"`,
				"test.yml": `---
buckets:
  fff:
    host: foo.bar
    access_key: aabb
    secret_key: bbaa
    bucket: test
grabbers:
  bbb:
    shell: "/bin/sh"
    buckets:
      - fff
    file: "alerting_rules.tar.gz"
    path: "/etc/prometheus/rules"
    commands:
      - "kill -HUP $(pidof prometheus)"`,
			},
			expectedCfg: GlobalConfig{
				Buckets:  map[string]BucketConfig{"fff": {Host: "foo.bar", AccessKey: "aabb", SecretKey: "bbaa", Bucket: "test"}},
				Grabbers: map[string]GrabberConfig{"bbb": {Buckets: []string{"fff"}, File: "alerting_rules.tar.gz", Path: "/etc/prometheus/rules", Commands: []string{"kill -HUP $(pidof prometheus)"}, Timeout: 5 * time.Second, Shell: "/bin/sh"}},
			},
		},
		{
			name: "config with ResolveIP",
			fileContent: map[string]string{
				"test3.yml": `---
buckets:
  aaa:
    host: foo.bar
    access_key: aabb
    secret_key: bbaa
    bucket: test
  bbb:
    host: foo.bar
    resolve_ip: true
    access_key: aabb
    secret_key: bbaa
    bucket: test
grabbers:
  fff:
    shell: "/bin/sh"
    buckets:
      - aaa
      - bbb
    file: "alerting_rules.tar.gz"
    path: "/etc/prometheus/rules"
    commands:
      - "kill -HUP $(pidof prometheus)"`,
			},
			expectedCfg: GlobalConfig{
				Buckets: map[string]BucketConfig{
					"aaa": {Host: "foo.bar", ResolveIP: false, AccessKey: "aabb", SecretKey: "bbaa", Bucket: "test"},
					"bbb": {Host: "foo.bar", ResolveIP: true, AccessKey: "aabb", SecretKey: "bbaa", Bucket: "test"},
				},
				Grabbers: map[string]GrabberConfig{"fff": {Buckets: []string{"aaa", "bbb"}, File: "alerting_rules.tar.gz", Path: "/etc/prometheus/rules", Commands: []string{"kill -HUP $(pidof prometheus)"}, Timeout: 5 * time.Second, Shell: "/bin/sh"}},
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			tmpDir := filepath.Join(os.TempDir(), "downloader-test")
			t.Cleanup(func() {
				_ = os.RemoveAll(tmpDir)
			})
			require.Nil(t, os.MkdirAll(tmpDir, os.ModePerm))

			for fname, content := range tcase.fileContent {
				require.NoError(t, os.WriteFile(filepath.Join(tmpDir, fname), []byte(content), os.ModePerm))
			}

			cfg, err := readFromPath(tmpDir)
			if tcase.expectedErr {
				require.NotNil(t, err)
				return
			}

			require.Equal(t, tcase.expectedCfg, cfg)
		})
	}
}

func TestCredentialLoad(t *testing.T) {
	os.Setenv("test", "foo")

	input := `---
access_key: ${test}`
	cfg := &BucketConfig{}

	require.Nil(t, yaml.Unmarshal([]byte(input), &cfg))
	require.Equal(t, credentialString("foo"), cfg.AccessKey)
}
