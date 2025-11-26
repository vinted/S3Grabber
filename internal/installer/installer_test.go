package installer_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vinted/S3Grabber/internal/installer"
)

// createTestTarGz creates a tar.gz archive with the given files.
// files is a map of filename -> content.
func createTestTarGz(t *testing.T, files map[string]string) io.Reader {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	for name, content := range files {
		hdr := &tar.Header{
			Name: name,
			Mode: 0600,
			Size: int64(len(content)),
		}
		require.NoError(t, tw.WriteHeader(hdr))
		_, err := tw.Write([]byte(content))
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())
	require.NoError(t, gw.Close())

	return bytes.NewReader(buf.Bytes())
}

// setupTestDir creates a temporary directory with test files.
func setupTestDir(t *testing.T, files map[string]string) string {
	tmpDir := t.TempDir()
	for name, content := range files {
		filePath := filepath.Join(tmpDir, name)
		require.NoError(t, os.WriteFile(filePath, []byte(content), 0644))
	}
	return tmpDir
}

// getFilesInDir returns a map of all files in a directory.
func getFilesInDir(t *testing.T, dir string) map[string]string {
	files := make(map[string]string)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, entry := range entries {
		if !entry.IsDir() {
			content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			require.NoError(t, err)
			files[entry.Name()] = string(content)
		}
	}
	return files
}

func TestExtractTarGz_WithoutPrefix(t *testing.T) {
	// Setup: Create a directory with existing files
	existingFiles := map[string]string{
		"monitoring.yml": "old monitoring config",
		"vita.yml":       "old vita config",
		"other.txt":      "other file",
	}
	tmpDir := setupTestDir(t, existingFiles)

	// Create archive with new files
	archiveFiles := map[string]string{
		"monitoring.yml": "new monitoring config",
		"new_file.txt":   "new file content",
	}
	archive := createTestTarGz(t, archiveFiles)

	// Extract without prefix (should remove all existing files)
	err := installer.ExtractTarGz(log.NewNopLogger(), "test", tmpDir, "", archive)
	require.NoError(t, err)

	// Verify: Only files from archive should exist
	resultFiles := getFilesInDir(t, tmpDir)
	assert.Len(t, resultFiles, 2, "Should have exactly 2 files")
	assert.Equal(t, "new monitoring config", resultFiles["monitoring.yml"])
	assert.Equal(t, "new file content", resultFiles["new_file.txt"])
	assert.NotContains(t, resultFiles, "vita.yml", "vita.yml should be removed")
	assert.NotContains(t, resultFiles, "other.txt", "other.txt should be removed")
}

func TestExtractTarGz_WithPrefix(t *testing.T) {
	// Setup: Create a directory with existing files
	existingFiles := map[string]string{
		"monitoring.yml":  "old monitoring config",
		"monitoring.conf": "old monitoring conf",
		"vita.yml":        "vita config",
		"other.txt":       "other file",
	}
	tmpDir := setupTestDir(t, existingFiles)

	// Create archive with new monitoring files
	archiveFiles := map[string]string{
		"monitoring.yml":     "new monitoring config",
		"monitoring.new.yml": "new monitoring file",
	}
	archive := createTestTarGz(t, archiveFiles)

	// Extract with "monitoring." prefix (should only remove monitoring.* files)
	err := installer.ExtractTarGz(log.NewNopLogger(), "test", tmpDir, "monitoring.", archive)
	require.NoError(t, err)

	// Verify: monitoring.* files replaced, others preserved
	resultFiles := getFilesInDir(t, tmpDir)
	assert.Len(t, resultFiles, 4, "Should have 4 files")
	assert.Equal(t, "new monitoring config", resultFiles["monitoring.yml"])
	assert.Equal(t, "new monitoring file", resultFiles["monitoring.new.yml"])
	assert.Equal(t, "vita config", resultFiles["vita.yml"], "vita.yml should be preserved")
	assert.Equal(t, "other file", resultFiles["other.txt"], "other.txt should be preserved")
	assert.NotContains(t, resultFiles, "monitoring.conf", "monitoring.conf should be removed")
}

func TestExtractTarGz_WithPrefix_NoMatchingFiles(t *testing.T) {
	// Setup: Create a directory without files matching the prefix
	existingFiles := map[string]string{
		"vita.yml":  "vita config",
		"other.txt": "other file",
	}
	tmpDir := setupTestDir(t, existingFiles)

	// Create archive with monitoring files
	archiveFiles := map[string]string{
		"monitoring.yml": "new monitoring config",
	}
	archive := createTestTarGz(t, archiveFiles)

	// Extract with "monitoring." prefix (no matching files to remove)
	err := installer.ExtractTarGz(log.NewNopLogger(), "test", tmpDir, "monitoring.", archive)
	require.NoError(t, err)

	// Verify: All existing files preserved + new file added
	resultFiles := getFilesInDir(t, tmpDir)
	assert.Len(t, resultFiles, 3, "Should have 3 files")
	assert.Equal(t, "new monitoring config", resultFiles["monitoring.yml"])
	assert.Equal(t, "vita config", resultFiles["vita.yml"])
	assert.Equal(t, "other file", resultFiles["other.txt"])
}

func TestExtractTarGz_EmptyDirectory(t *testing.T) {
	// Setup: Create an empty directory
	tmpDir := t.TempDir()

	// Create archive
	archiveFiles := map[string]string{
		"file1.txt": "content1",
		"file2.txt": "content2",
	}
	archive := createTestTarGz(t, archiveFiles)

	// Extract into empty directory
	err := installer.ExtractTarGz(log.NewNopLogger(), "test", tmpDir, "", archive)
	require.NoError(t, err)

	// Verify: Files extracted successfully
	resultFiles := getFilesInDir(t, tmpDir)
	assert.Len(t, resultFiles, 2)
	assert.Equal(t, "content1", resultFiles["file1.txt"])
	assert.Equal(t, "content2", resultFiles["file2.txt"])
}

func TestExtractTarGz_WithPrefix_MultiplePatterns(t *testing.T) {
	t.Run("vita prefix", func(t *testing.T) {
		// Setup with mixed files
		existingFiles := map[string]string{
			"monitoring.yml": "monitoring config",
			"vita.yml":       "old vita config",
			"vita.conf":      "old vita conf",
			"other.txt":      "other file",
		}
		tmpDir := setupTestDir(t, existingFiles)

		// Archive with vita files
		archiveFiles := map[string]string{
			"vita.yml":     "new vita config",
			"vita.new.yml": "new vita file",
		}
		archive := createTestTarGz(t, archiveFiles)

		// Extract with "vita." prefix
		err := installer.ExtractTarGz(log.NewNopLogger(), "test", tmpDir, "vita.", archive)
		require.NoError(t, err)

		// Verify
		resultFiles := getFilesInDir(t, tmpDir)
		assert.Len(t, resultFiles, 4)
		assert.Equal(t, "new vita config", resultFiles["vita.yml"])
		assert.Equal(t, "new vita file", resultFiles["vita.new.yml"])
		assert.Equal(t, "monitoring config", resultFiles["monitoring.yml"], "monitoring.yml preserved")
		assert.Equal(t, "other file", resultFiles["other.txt"], "other.txt preserved")
		assert.NotContains(t, resultFiles, "vita.conf", "vita.conf should be removed")
	})

	t.Run("app prefix", func(t *testing.T) {
		// Setup with files starting with different prefixes
		existingFiles := map[string]string{
			"app.config":   "old app config",
			"app.settings": "old app settings",
			"data.db":      "database",
		}
		tmpDir := setupTestDir(t, existingFiles)

		// Archive with new app files
		archiveFiles := map[string]string{
			"app.config": "new app config",
		}
		archive := createTestTarGz(t, archiveFiles)

		// Extract with "app." prefix
		err := installer.ExtractTarGz(log.NewNopLogger(), "test", tmpDir, "app.", archive)
		require.NoError(t, err)

		// Verify
		resultFiles := getFilesInDir(t, tmpDir)
		assert.Len(t, resultFiles, 2)
		assert.Equal(t, "new app config", resultFiles["app.config"])
		assert.Equal(t, "database", resultFiles["data.db"], "data.db preserved")
		assert.NotContains(t, resultFiles, "app.settings", "app.settings should be removed")
	})
}

func TestIsEmptyDir(t *testing.T) {
	t.Run("empty directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		isEmpty, err := installer.IsEmptyDir(tmpDir)
		require.NoError(t, err)
		assert.True(t, isEmpty)
	})

	t.Run("non-empty directory", func(t *testing.T) {
		tmpDir := setupTestDir(t, map[string]string{
			"file.txt": "content",
		})
		isEmpty, err := installer.IsEmptyDir(tmpDir)
		require.NoError(t, err)
		assert.False(t, isEmpty)
	})

	t.Run("non-existent directory", func(t *testing.T) {
		isEmpty, err := installer.IsEmptyDir("/non/existent/path")
		assert.Error(t, err)
		assert.False(t, isEmpty)
	})
}
