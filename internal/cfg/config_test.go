package cfg

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestCredentialLoad(t *testing.T) {
	os.Setenv("test", "foo")

	input := `---
access_key: ${test}`
	cfg := &BucketConfig{}

	require.Nil(t, yaml.Unmarshal([]byte(input), &cfg))
	require.Equal(t, credentialString("foo"), cfg.AccessKey)
}
