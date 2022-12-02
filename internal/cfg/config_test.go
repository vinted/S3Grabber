package cfg

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

var (
	filename = "alerting_rules.tar.gz"
	dirname  = "slos"
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
			name: "neither file nor dir specified",
			fileContent: map[string]string{
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
    path: "/etc/prometheus/rules"
    commands:
      - "kill -HUP $(pidof prometheus)"`,
			},
			expectedErr: true,
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
  file_grabber:
    shell: "/bin/sh"
    buckets:
      - fff
    file: "alerting_rules.tar.gz"
    path: "/etc/prometheus/rules"
    commands:
      - "kill -HUP $(pidof prometheus)"
  dir_grabber:
    shell: "/bin/sh"
    buckets:
      - fff
    dir: "slos"
    path: "/etc/prometheus/rules"
    commands:
      - "kill -HUP $(pidof prometheus)"`,
			},
			expectedCfg: GlobalConfig{
				Buckets: map[string]BucketConfig{
					"aaa": {Host: "foo.bar", AccessKey: "aabb", SecretKey: "bbaa", Bucket: "test"},
					"fff": {Host: "foo.bar", AccessKey: "aabb", SecretKey: "bbaa", Bucket: "test"},
				},
				Grabbers: map[string]GrabberConfig{
					"bbb":          {Buckets: []string{"aaa"}, File: &filename, Path: "/etc/prometheus/rules", Commands: []string{"kill -HUP $(pidof prometheus)"}, Timeout: 5 * time.Second, Shell: "/bin/sh"},
					"file_grabber": {Buckets: []string{"fff"}, File: &filename, Path: "/etc/prometheus/rules", Commands: []string{"kill -HUP $(pidof prometheus)"}, Timeout: 5 * time.Second, Shell: "/bin/sh"},
					"dir_grabber":  {Buckets: []string{"fff"}, Dir: &dirname, Path: "/etc/prometheus/rules", Commands: []string{"kill -HUP $(pidof prometheus)"}, Timeout: 5 * time.Second, Shell: "/bin/sh"},
				},
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

			require.NoError(t, err)
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
