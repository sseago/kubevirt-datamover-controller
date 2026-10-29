/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

// mockObjectStore is a minimal in-memory velero.ObjectStore for testing.
type mockObjectStore struct {
	objects        map[string][]byte
	failPaths      map[string]bool
	closeFailPaths map[string]bool
}

func newMockObjectStore() *mockObjectStore {
	return &mockObjectStore{
		objects:        make(map[string][]byte),
		failPaths:      make(map[string]bool),
		closeFailPaths: make(map[string]bool),
	}
}

// failingCloseReader wraps a reader whose Close() reports an error, to
// simulate a late transport/checksum failure from an object-store client.
type failingCloseReader struct {
	io.Reader
}

func (failingCloseReader) Close() error {
	return errors.New("simulated close failure")
}

func (m *mockObjectStore) Init(_ map[string]string) error { return nil }

func (m *mockObjectStore) PutObject(_, key string, body io.Reader) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	m.objects[key] = data
	return nil
}

func (m *mockObjectStore) ObjectExists(_, key string) (bool, error) {
	_, ok := m.objects[key]
	return ok, nil
}

func (m *mockObjectStore) GetObject(_, key string) (io.ReadCloser, error) {
	if m.failPaths[key] {
		return nil, errors.New("simulated download failure")
	}
	data, ok := m.objects[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	if m.closeFailPaths[key] {
		return failingCloseReader{Reader: strings.NewReader(string(data))}, nil
	}
	return io.NopCloser(strings.NewReader(string(data))), nil
}

func (m *mockObjectStore) ListCommonPrefixes(_, _, _ string) ([]string, error) { return nil, nil }
func (m *mockObjectStore) ListObjects(_, _ string) ([]string, error)           { return nil, nil }
func (m *mockObjectStore) DeleteObject(_, _ string) error                      { return nil }
func (m *mockObjectStore) CreateSignedURL(_, _ string, _ time.Duration) (string, error) {
	return "", nil
}

func TestDownloadCheckpointFiles(t *testing.T) {
	t.Run("happy path downloads all files in order", func(t *testing.T) {
		store := newMockObjectStore()
		store.objects["checkpoints/ns/vm/cp1/disk1.qcow2"] = []byte("full-backup-data")
		store.objects["checkpoints/ns/vm/cp2/disk1.qcow2"] = []byte("incremental-data")

		files := []uploader.CheckpointFile{
			{Filename: "disk1.qcow2", DiskName: "disk1", ObjectPath: "checkpoints/ns/vm/cp1/disk1.qcow2", Size: 16},
			{Filename: "disk1.qcow2", DiskName: "disk1", ObjectPath: "checkpoints/ns/vm/cp2/disk1.qcow2", Size: 16},
		}

		scratchDir := t.TempDir()
		dummyConfig := &DownloaderConfig{
			ObjectStoreConfig: common.ObjectStoreConfig{BSLBucket: "bucket"},
			ScratchPath:       scratchDir,
		}
		paths, err := downloadCheckpointFiles(context.Background(), store, dummyConfig, files, nil, logr.Discard())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(paths) != 2 {
			t.Fatalf("expected 2 paths, got %d", len(paths))
		}

		data0, err := os.ReadFile(paths[0])
		if err != nil {
			t.Fatalf("failed to read downloaded file: %v", err)
		}
		if string(data0) != "full-backup-data" {
			t.Errorf("expected full-backup-data, got %s", data0)
		}

		data1, err := os.ReadFile(paths[1])
		if err != nil {
			t.Fatalf("failed to read downloaded file: %v", err)
		}
		if string(data1) != "incremental-data" {
			t.Errorf("expected incremental-data, got %s", data1)
		}

		if !strings.HasPrefix(filepath.Base(paths[0]), "00-") {
			t.Errorf("expected first file to be index-prefixed with 00-, got %s", filepath.Base(paths[0]))
		}
		if !strings.HasPrefix(filepath.Base(paths[1]), "01-") {
			t.Errorf("expected second file to be index-prefixed with 01-, got %s", filepath.Base(paths[1]))
		}
	})

	t.Run("partial failure mid-chain returns error", func(t *testing.T) {
		store := newMockObjectStore()
		store.objects["checkpoints/ns/vm/cp1/disk1.qcow2"] = []byte("full-backup-data")
		store.failPaths["checkpoints/ns/vm/cp2/disk1.qcow2"] = true

		files := []uploader.CheckpointFile{
			{Filename: "disk1.qcow2", DiskName: "disk1", ObjectPath: "checkpoints/ns/vm/cp1/disk1.qcow2", Size: 16},
			{Filename: "disk1.qcow2", DiskName: "disk1", ObjectPath: "checkpoints/ns/vm/cp2/disk1.qcow2", Size: 16},
		}

		scratchDir := t.TempDir()
		dummyConfig := &DownloaderConfig{
			ObjectStoreConfig: common.ObjectStoreConfig{BSLBucket: "bucket"},
			ScratchPath:       scratchDir,
		}
		_, err := downloadCheckpointFiles(context.Background(), store, dummyConfig, files, nil, logr.Discard())
		if err == nil {
			t.Fatal("expected error from simulated download failure")
		}

		entries, readErr := os.ReadDir(scratchDir)
		if readErr != nil {
			t.Fatalf("failed to read scratch dir: %v", readErr)
		}
		if len(entries) != 0 {
			t.Errorf("expected the earlier successfully downloaded cp1 file to be cleaned up, got %v", entries)
		}
	})

	t.Run("stops early when context is already canceled", func(t *testing.T) {
		store := newMockObjectStore()
		store.objects["checkpoints/ns/vm/cp1/disk1.qcow2"] = []byte("full-backup-data")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		files := []uploader.CheckpointFile{
			{Filename: "disk1.qcow2", DiskName: "disk1", ObjectPath: "checkpoints/ns/vm/cp1/disk1.qcow2", Size: 16},
		}
		scratchDir := t.TempDir()
		dummyConfig := &DownloaderConfig{
			ObjectStoreConfig: common.ObjectStoreConfig{BSLBucket: "bucket"},
			ScratchPath:       scratchDir,
		}
		_, err := downloadCheckpointFiles(ctx, store, dummyConfig, files, nil, logr.Discard())
		if err == nil {
			t.Fatal("expected error from canceled context")
		}
	})

	t.Run("empty file list returns empty paths", func(t *testing.T) {
		store := newMockObjectStore()
		scratchDir := t.TempDir()
		dummyConfig := &DownloaderConfig{
			ObjectStoreConfig: common.ObjectStoreConfig{BSLBucket: "bucket"},
			ScratchPath:       scratchDir,
		}
		paths, err := downloadCheckpointFiles(context.Background(), store, dummyConfig, nil, nil, logr.Discard())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(paths) != 0 {
			t.Fatalf("expected 0 paths, got %d", len(paths))
		}
	})

	t.Run("propagates reader close error and cleans up partial file", func(t *testing.T) {
		store := newMockObjectStore()
		store.objects["checkpoints/ns/vm/cp1/disk1.qcow2"] = []byte("full-backup-data")
		store.closeFailPaths["checkpoints/ns/vm/cp1/disk1.qcow2"] = true

		scratchDir := t.TempDir()
		dummyConfig := &DownloaderConfig{
			ObjectStoreConfig: common.ObjectStoreConfig{BSLBucket: "bucket"},
			ScratchPath:       scratchDir,
		}
		files := []uploader.CheckpointFile{
			{Filename: "disk1.qcow2", DiskName: "disk1", ObjectPath: "checkpoints/ns/vm/cp1/disk1.qcow2", Size: 16},
		}
		_, err := downloadCheckpointFiles(context.Background(), store, dummyConfig, files, nil, logr.Discard())
		if err == nil {
			t.Fatal("expected error from simulated reader close failure")
		}

		entries, readErr := os.ReadDir(scratchDir)
		if readErr != nil {
			t.Fatalf("failed to read scratch dir: %v", readErr)
		}
		if len(entries) != 0 {
			t.Errorf("expected no leftover files in scratch dir, got %v", entries)
		}
	})

	t.Run("rejects path traversal filename", func(t *testing.T) {
		store := newMockObjectStore()
		store.objects["checkpoints/ns/vm/cp1/disk1.qcow2"] = []byte("full-backup-data")

		scratchDir := t.TempDir()
		dummyConfig := &DownloaderConfig{
			ObjectStoreConfig: common.ObjectStoreConfig{BSLBucket: "bucket"},
			ScratchPath:       scratchDir,
		}
		files := []uploader.CheckpointFile{
			{Filename: "../escape.qcow2", DiskName: "disk1", ObjectPath: "checkpoints/ns/vm/cp1/disk1.qcow2", Size: 16},
		}
		_, err := downloadCheckpointFiles(context.Background(), store, dummyConfig, files, nil, logr.Discard())
		if err == nil {
			t.Fatal("expected error for path traversal filename")
		}

		if _, statErr := os.Stat(filepath.Join(filepath.Dir(scratchDir), "escape.qcow2")); !os.IsNotExist(statErr) {
			t.Error("expected no file to be written outside the scratch directory")
		}
	})

	t.Run("rejects an undersized object", func(t *testing.T) {
		store := newMockObjectStore()
		store.objects["checkpoints/ns/vm/cp1/disk1.qcow2"] = []byte("short")

		scratchDir := t.TempDir()
		dummyConfig := &DownloaderConfig{
			ObjectStoreConfig: common.ObjectStoreConfig{BSLBucket: "bucket"},
			ScratchPath:       scratchDir,
		}
		files := []uploader.CheckpointFile{
			{Filename: "disk1.qcow2", DiskName: "disk1", ObjectPath: "checkpoints/ns/vm/cp1/disk1.qcow2", Size: 100},
		}
		_, err := downloadCheckpointFiles(context.Background(), store, dummyConfig, files, nil, logr.Discard())
		if err == nil {
			t.Fatal("expected error when the downloaded object is smaller than its declared size")
		}
	})

	t.Run("rejects an oversized object", func(t *testing.T) {
		store := newMockObjectStore()
		store.objects["checkpoints/ns/vm/cp1/disk1.qcow2"] = []byte("this object is much larger than declared")

		scratchDir := t.TempDir()
		dummyConfig := &DownloaderConfig{
			ObjectStoreConfig: common.ObjectStoreConfig{BSLBucket: "bucket"},
			ScratchPath:       scratchDir,
		}
		files := []uploader.CheckpointFile{
			{Filename: "disk1.qcow2", DiskName: "disk1", ObjectPath: "checkpoints/ns/vm/cp1/disk1.qcow2", Size: 4},
		}
		_, err := downloadCheckpointFiles(context.Background(), store, dummyConfig, files, nil, logr.Discard())
		if err == nil {
			t.Fatal("expected error when the downloaded object is larger than its declared size")
		}

		entries, readErr := os.ReadDir(scratchDir)
		if readErr != nil {
			t.Fatalf("failed to read scratch dir: %v", readErr)
		}
		if len(entries) != 0 {
			t.Errorf("expected the oversized partial file to be cleaned up, got %v", entries)
		}
	})

	t.Run("rejects a negative declared size", func(t *testing.T) {
		store := newMockObjectStore()
		store.objects["checkpoints/ns/vm/cp1/disk1.qcow2"] = []byte("full-backup-data")

		scratchDir := t.TempDir()
		dummyConfig := &DownloaderConfig{
			ObjectStoreConfig: common.ObjectStoreConfig{BSLBucket: "bucket"},
			ScratchPath:       scratchDir,
		}
		files := []uploader.CheckpointFile{
			{Filename: "disk1.qcow2", DiskName: "disk1", ObjectPath: "checkpoints/ns/vm/cp1/disk1.qcow2", Size: -1},
		}
		_, err := downloadCheckpointFiles(context.Background(), store, dummyConfig, files, nil, logr.Discard())
		if err == nil {
			t.Fatal("expected error for a negative declared size")
		}
	})

	t.Run("preserves chain order in filenames past 100 checkpoints", func(t *testing.T) {
		const count = 101
		store := newMockObjectStore()
		files := make([]uploader.CheckpointFile, count)
		for i := range count {
			objectPath := fmt.Sprintf("checkpoints/ns/vm/cp%d/disk1.qcow2", i)
			data := fmt.Appendf(nil, "data-%d", i)
			store.objects[objectPath] = data
			files[i] = uploader.CheckpointFile{
				Filename: "disk1.qcow2", DiskName: "disk1", ObjectPath: objectPath, Size: int64(len(data)),
			}
		}

		scratchDir := t.TempDir()
		dummyConfig := &DownloaderConfig{
			ObjectStoreConfig: common.ObjectStoreConfig{BSLBucket: "bucket"},
			ScratchPath:       scratchDir,
		}
		paths, err := downloadCheckpointFiles(context.Background(), store, dummyConfig, files, nil, logr.Discard())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(paths) != count {
			t.Fatalf("expected %d paths, got %d", count, len(paths))
		}

		sorted := make([]string, len(paths))
		for i, p := range paths {
			sorted[i] = filepath.Base(p)
		}
		if !sort.StringsAreSorted(sorted) {
			t.Errorf("expected filenames to sort in chain order, got %v", sorted)
		}
	})
}

func TestDownloadCheckpointFilesReplacesPreExistingSymlink(t *testing.T) {
	store := newMockObjectStore()
	store.objects["checkpoints/ns/vm/cp1/disk1.qcow2"] = []byte("full-backup-data")

	scratchDir := t.TempDir()
	outsideDir := t.TempDir()
	outsideTarget := filepath.Join(outsideDir, "outside-file")
	if err := os.WriteFile(outsideTarget, []byte("untouched"), 0o600); err != nil {
		t.Fatalf("failed to seed outside target file: %v", err)
	}

	localPath := filepath.Join(scratchDir, "00-disk1.qcow2")
	if err := os.Symlink(outsideTarget, localPath); err != nil {
		t.Fatalf("failed to create pre-existing symlink: %v", err)
	}

	files := []uploader.CheckpointFile{
		{Filename: "disk1.qcow2", DiskName: "disk1", ObjectPath: "checkpoints/ns/vm/cp1/disk1.qcow2", Size: 16},
	}
	dummyConfig := &DownloaderConfig{
		ObjectStoreConfig: common.ObjectStoreConfig{BSLBucket: "bucket"},
		ScratchPath:       scratchDir,
	}
	paths, err := downloadCheckpointFiles(context.Background(), store, dummyConfig, files, nil, logr.Discard())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(paths) != 1 || paths[0] != localPath {
		t.Fatalf("expected paths = [%s], got %v", localPath, paths)
	}

	info, err := os.Lstat(localPath)
	if err != nil {
		t.Fatalf("failed to lstat local path: %v", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Error("expected the symlink to be replaced with a regular file")
	}

	data, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("failed to read local path: %v", err)
	}
	if string(data) != "full-backup-data" {
		t.Errorf("expected downloaded content, got %s", data)
	}

	outsideData, err := os.ReadFile(outsideTarget)
	if err != nil {
		t.Fatalf("failed to read outside target: %v", err)
	}
	if string(outsideData) != "untouched" {
		t.Errorf("expected outside target to be untouched, got %s", outsideData)
	}
}
