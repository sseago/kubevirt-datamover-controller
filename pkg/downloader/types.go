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

// Package downloader implements the datamover pod functionality for
// downloading qcow2 checkpoint chains from object storage and reconstructing
// them into a flat raw disk image for restore.
//
// See pkg/uploader's package doc for the full object storage layout
// (checkpoints/, manifests/, index.json structure) — the downloader reads
// exactly what the uploader writes, using the same pkg/uploader helpers.
//
// # Restore flow
//
//  1. Read the per-VM backup manifest (manifests/<backup>/<ns>-<vm>.json) to
//     get the ordered CheckpointChain (full backup first, incrementals after).
//  2. Read the per-VM index (checkpoints/<ns>/<vm>/index.json) to resolve each
//     checkpoint ID to its CheckpointEntry, and filter Files by TargetVolume
//     (one DataDownload restores one disk/PVC at a time).
//  3. Download each checkpoint's qcow2 file to local scratch storage, in
//     chain order.
//  4. Rebase each qcow2's backing-file pointer onto the local scratch path of
//     its predecessor (the path recorded at backup time won't exist on the
//     restore pod's filesystem), then flatten the chain to a single raw disk
//     image via qemu-img convert.
package downloader

import "github.com/migtools/kubevirt-datamover-controller/pkg/common"

// Environment variable names for downloader configuration.
// BSL/credentials env vars are shared with the uploader — see pkg/common.Env*
// for the canonical definitions and their values, referenced directly at
// call sites instead of re-exported here.
const (
	EnvPodName             = "POD_NAME"
	EnvPodNamespace        = "POD_NAMESPACE"
	EnvVMName              = "KUBEVIRT_DM_VM_NAME"
	EnvVMNamespace         = "KUBEVIRT_DM_VM_NAMESPACE"
	EnvVeleroBackupName    = "KUBEVIRT_DM_VELERO_BACKUP_NAME"
	EnvDataDownloadName    = "KUBEVIRT_DM_DATADOWNLOAD_NAME"
	EnvDataDownloadUID     = "KUBEVIRT_DM_DATADOWNLOAD_UID"
	EnvTargetVolume        = "KUBEVIRT_DM_TARGET_VOLUME"
	EnvTargetPath          = "KUBEVIRT_DM_TARGET_PATH"
	EnvScratchPath         = "KUBEVIRT_DM_SCRATCH_PATH"
	EnvTargetIsBlockDevice = "KUBEVIRT_DM_TARGET_IS_BLOCK_DEVICE"
)

// Default paths and values
const (
	// DefaultTargetPath is where the reconstructed raw disk image is written.
	DefaultTargetPath = "/restore-data/disk.raw"

	// DefaultScratchPath is where downloaded qcow2 checkpoint files are staged
	// before reconstruction.
	DefaultScratchPath = "/scratch"
)

// DownloaderConfig holds configuration loaded from environment variables.
type DownloaderConfig struct {
	// ObjectStoreConfig holds BSL/object-store connection settings, shared
	// with UploaderConfig so object-store init code isn't duplicated.
	common.ObjectStoreConfig

	// Pod context (used for progress reporting)
	PodName      string
	PodNamespace string

	// VM context
	VMName      string
	VMNamespace string

	// Backup context
	VeleroBackupName string
	DataDownloadName string
	DataDownloadUID  string

	// TargetVolume is the source disk/PVC name to restore. One DataDownload
	// restores one volume; multi-disk VMs are restored via multiple
	// DataDownloads, one per disk.
	TargetVolume string

	// TargetPath is where the reconstructed raw disk image is written.
	TargetPath string

	// TargetIsBlockDevice reports whether TargetPath is a raw block device
	// (a rebound Block-mode PV, mounted as a volumeDevice) rather than a
	// regular file path inside a mounted filesystem. Block-mode restores use
	// a second, separate volume for this -- the qcow2 chain is always staged
	// on the Filesystem-mode ScratchPath, regardless of the target's mode.
	TargetIsBlockDevice bool

	// ScratchPath is where downloaded qcow2 checkpoint files are staged.
	ScratchPath string
}
