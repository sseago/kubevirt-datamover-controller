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
	"time"

	"github.com/go-logr/logr"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

// downloadCheckpointFiles downloads each checkpoint file to scratchDir, in
// order, and returns the ordered list of local file paths. Files are
// index-prefixed so they sort in chain order for debuggability.
//
// ctx is checked for cancellation between files: velero.ObjectStore.GetObject
// takes no context (an upstream interface limitation shared by the uploader's
// equivalent loop), so a single in-flight download can't be interrupted, but
// a canceled restore won't start any further downloads.
func downloadCheckpointFiles(
	ctx context.Context, store velero.ObjectStore, config *DownloaderConfig,
	files []uploader.CheckpointFile, k8sClient client.Client, logger logr.Logger,
) ([]string, error) {
	scratchDir := config.ScratchPath
	absScratchDir, err := filepath.Abs(scratchDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve scratch dir %q: %w", scratchDir, err)
	}
	scratchDir = absScratchDir
	if err := os.MkdirAll(scratchDir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create scratch dir %q: %w", scratchDir, err)
	}

	// Width the index prefix to the chain length so lexical and chain order
	// agree even past 100 checkpoints (e.g. "005-" sorts before "100-").
	indexWidth := max(len(fmt.Sprintf("%d", len(files)-1)), 2)

	var totalBytes int64
	for _, f := range files {
		if f.Size > 0 {
			totalBytes += f.Size
		}
	}

	var bytesDone int64
	reportProgress := func(currentDone int64) {
		if config.PodName != "" && config.PodNamespace != "" && k8sClient != nil {
			common.PatchPodProgress(ctx, k8sClient, config.PodName, config.PodNamespace, totalBytes, 0, currentDone, logger)
		}
	}
	reportProgress(0)

	localPaths := make([]string, 0, len(files))
	// cleanup removes every downloaded file so far. Its own removal
	// failures are reported (joined with the caller's error) rather than
	// discarded, since a silent failure here would leave restored VM disk
	// data behind in scratch storage with no indication anything is wrong.
	cleanup := func() error {
		var errs []error
		for _, p := range localPaths {
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				errs = append(errs, fmt.Errorf("failed to remove %q during cleanup: %w", p, err))
			}
		}
		return errors.Join(errs...)
	}
	for i, file := range files {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, cleanup())
		}
		if file.Filename == "" || file.Filename != filepath.Base(file.Filename) ||
			file.Filename == "." || file.Filename == ".." {
			return nil, errors.Join(fmt.Errorf("invalid checkpoint filename %q", file.Filename), cleanup())
		}
		localPath := filepath.Join(scratchDir, fmt.Sprintf("%0*d-%s", indexWidth, i, file.Filename))
		err := downloadOne(store, config.BSLBucket, file.ObjectPath, localPath, file.Size, &bytesDone, reportProgress)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("failed to download checkpoint file %q: %w", file.ObjectPath, err), cleanup())
		}
		localPaths = append(localPaths, localPath)
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, cleanup())
		}
	}

	return localPaths, nil
}

func downloadOne(
	store velero.ObjectStore, bucket, objectPath, localPath string,
	size int64, bytesDone *int64, reportProgress func(int64),
) error {
	if size < 0 {
		return fmt.Errorf("invalid declared size %d for object %q", size, objectPath)
	}

	reader, err := store.GetObject(bucket, objectPath)
	if err != nil {
		return fmt.Errorf("failed to get object %q: %w", objectPath, err)
	}

	// Remove any pre-existing entry first: os.Remove deletes a symlink itself
	// rather than following it, so a stale symlink left in scratch space (e.g.
	// from a killed prior attempt) can't cause the O_CREATE|O_EXCL open below
	// to write through it to an unintended target.
	if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
		_ = reader.Close()
		return fmt.Errorf("failed to clear existing local file %q: %w", localPath, err)
	}
	out, err := os.OpenFile(localPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		_ = reader.Close()
		return fmt.Errorf("failed to create local file %q: %w", localPath, err)
	}

	// Cap the read at size+1: a well-formed object of exactly size bytes
	// copies cleanly, while an oversized (corrupted or replaced) object hits
	// the limit at size+1 bytes instead of being downloaded unbounded, which
	// the length check below then rejects the same as an undersized object.
	progressReader := &common.ProgressReader{
		Reader:     io.LimitReader(reader, size+1),
		UpdateFreq: 5 * time.Second,
		Reporter: func(read int64) {
			reportProgress(*bytesDone + read)
		},
	}

	written, copyErr := io.Copy(out, progressReader)
	*bytesDone += written
	// Close() on an object-store reader can surface a late transport/checksum
	// error even when the preceding reads looked successful, so it must be
	// checked rather than discarded.
	closeReaderErr := reader.Close()
	closeOutErr := out.Close()

	if copyErr == nil && written != size {
		copyErr = fmt.Errorf("object %q size mismatch: downloaded %d bytes, expected %d", objectPath, written, size)
	}

	if copyErr != nil {
		_ = os.Remove(localPath)
		return fmt.Errorf("failed to write local file %q: %w", localPath, copyErr)
	}
	if closeReaderErr != nil {
		_ = os.Remove(localPath)
		return fmt.Errorf("failed to finalize remote read of %q: %w", objectPath, closeReaderErr)
	}
	if closeOutErr != nil {
		_ = os.Remove(localPath)
		return fmt.Errorf("failed to finalize local file %q: %w", localPath, closeOutErr)
	}

	return nil
}
