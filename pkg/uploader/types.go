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

// Package uploader implements the datamover pod functionality for uploading
// qcow2 files to object storage and managing backup metadata.
//
// # S3 Storage Structure
//
// The uploader organizes backup data in object storage using the following layout:
//
//	<bsl-prefix>-kubevirt-datamover/
//	├── checkpoints/
//	│   └── <namespace>/
//	│       └── <vm-name>/
//	│           ├── <checkpoint-id>/
//	│           │   ├── <vmb-name>-<disk-name>.qcow2   # Backup data files
//	│           │   ├── vmb.json                        # Archived VMB CR
//	│           │   └── vmbt.json                       # Archived VMBT CR
//	│           └── index.json                          # Per-VM checkpoint index
//	└── manifests/
//	    └── <velero-backup-name>/
//	        ├── index.json                              # Per-backup manifest
//	        └── <namespace>-<vm-name>.json              # Per-VM backup manifest
//
// # File Relationships
//
// 1. Per-VM Index (checkpoints/<ns>/<vm>/index.json):
//   - Tracks all checkpoints for a single VM across all backups
//   - Each checkpoint has an "id", "parent" (for incremental), and "referencedBy" (backup names)
//   - Each checkpoint references its archived VMB/VMBT via "vmbObjectPath" and "vmbtObjectPath"
//   - Used by the controller to determine the latest checkpoint for incremental backups
//
// 2. Per-Backup Manifest (manifests/<backup>/index.json):
//   - Lists all VMs included in a Velero backup
//   - Points to per-VM manifests via "manifestPath"
//
// 3. Per-VM Backup Manifest (manifests/<backup>/<ns>-<vm>.json):
//   - Contains the full checkpoint chain needed to restore a specific VM
//   - Chain starts from the base (full) backup and includes all incrementals
//   - Self-contained: restore can be performed using only this file
//
// 4. Archived VMB/VMBT (checkpoints/<ns>/<vm>/<checkpoint>/vmb.json, vmbt.json):
//   - JSON serialization of the KubeVirt VMB and VMBT CRs at backup time
//   - VMB and VMBT are deleted from the cluster after archival to S3
//   - The controller recreates VMBT from the archived vmbt.json before each backup,
//     restoring Status.LatestCheckpoint to enable incremental backups
//
// # Incremental Backup Chain
//
// Checkpoints form a linked list via the "parent" field:
//
//	checkpoint-001 (full)  <-- checkpoint-002 (incremental) <-- checkpoint-003 (incremental)
//	     ^                           ^                                ^
//	     |                           |                                |
//	  referencedBy:              referencedBy:                   referencedBy:
//	  [backup-day1]              [backup-day2]                   [backup-day3]
//
// To restore backup-day3, the checkpointChain includes all three checkpoints.
package uploader

import (
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
)

// Environment variable names for uploader configuration.
// BSL/credentials env vars are shared with the downloader — see pkg/common.Env*
// for the canonical definitions and their values, referenced directly at
// call sites instead of re-exported here.
const (
	EnvVMName         = "KUBEVIRT_DM_VM_NAME"
	EnvVMNamespace    = "KUBEVIRT_DM_VM_NAMESPACE"
	EnvCheckpointName = "KUBEVIRT_DM_CHECKPOINT_NAME"
	EnvBackupType     = "KUBEVIRT_DM_BACKUP_TYPE"
	// EnvExpectedBackupType carries the backup type the controller intended
	// based on BSL chain validation. It is compared against the actual backup
	// type (EnvBackupType, derived from VMB.Status.Type) so the uploader can
	// correct the S3 index when virt-controller performs a full backup despite
	// the controller expecting an incremental one (e.g. the VM lost its
	// libvirt checkpoint).
	EnvExpectedBackupType = "KUBEVIRT_DM_EXPECTED_BACKUP_TYPE"
	EnvVeleroBackupName   = "KUBEVIRT_DM_VELERO_BACKUP_NAME"
	EnvSourcePVCPath      = "KUBEVIRT_DM_SOURCE_PVC_PATH"
	EnvDataUploadName     = "KUBEVIRT_DM_DATAUPLOAD_NAME"
	EnvDataUploadUID      = "KUBEVIRT_DM_DATAUPLOAD_UID"
	EnvVMBName            = "KUBEVIRT_DM_VMB_NAME"
	EnvVMBTName           = "KUBEVIRT_DM_VMBT_NAME"
	EnvTotalBytes         = "KUBEVIRT_DM_TOTAL_BYTES"
)

// Default paths and values
const (
	DefaultSourcePVCPath = "/backup-data"
)

// Backup type values
const (
	BackupTypeFull        = "full"
	BackupTypeIncremental = "incremental"
)

// UploaderConfig holds configuration loaded from environment variables.
type UploaderConfig struct {
	// ObjectStoreConfig holds BSL/object-store connection settings, shared
	// with DownloaderConfig so object-store init code isn't duplicated.
	common.ObjectStoreConfig

	// Pod context (used for progress reporting)
	PodName      string
	PodNamespace string

	// VM context
	VMName      string
	VMNamespace string

	// Backup context
	CheckpointName string
	BackupType     string // actual backup type performed: "full" or "incremental"
	// ExpectedBackupType is the backup type the controller intended based on
	// BSL chain validation. When it is "incremental" but BackupType is "full",
	// virt-controller performed a full backup unexpectedly and the S3 index
	// entry is recorded as full with no parent pointer.
	ExpectedBackupType string
	VeleroBackupName   string
	DataUploadName     string
	DataUploadUID      string
	VMBName            string
	VMBTName           string

	// TotalBytes is the total capacity of the source PVCs
	TotalBytes int64

	// Source PVC mount path
	SourcePVCPath string
}

// CheckpointFile represents a single qcow2 file within a checkpoint.
// This provides richer metadata than a simple filename string.
type CheckpointFile struct {
	// Filename is the name of the qcow2 file (e.g., "vmb-xxx-disk1.qcow2")
	Filename string `json:"filename"`

	// DiskName is the source disk/PVC name
	DiskName string `json:"diskName,omitempty"`

	// Size in bytes
	Size int64 `json:"size"`

	// ObjectPath is the full path in object storage
	ObjectPath string `json:"objectPath"`
}

// CheckpointEntry represents a single checkpoint in the per-VM index.json.
// Field names align with the approved design document.
type CheckpointEntry struct {
	// ID is the unique identifier for this checkpoint (design: "id")
	ID string `json:"id"`

	// Type indicates whether this is a "full" or "incremental" backup
	Type string `json:"type"`

	// Parent is the checkpoint this one is based on (design: "parent")
	Parent string `json:"parent,omitempty"`

	// Timestamp when this checkpoint was created
	Timestamp time.Time `json:"timestamp"`

	// VMBackup is the VirtualMachineBackup CR name (design: "vmBackup")
	VMBackup string `json:"vmBackup"`

	// Files is a list of qcow2 files in this checkpoint (enhanced with metadata)
	Files []CheckpointFile `json:"files"`

	// PVCs is a list of PVC names backed up in this checkpoint (design field)
	PVCs []string `json:"pvcs"`

	// PVCSizes is a list of PVC storage request sizes matching the PVCs list
	PVCSizes []resource.Quantity `json:"pvcSizes,omitempty"`

	// ReferencedBy is a list of Velero backup names that reference this checkpoint (design field)
	ReferencedBy []string `json:"referencedBy"`

	// VMBObjectPath is the S3 path to the archived VMB CR JSON for this checkpoint
	VMBObjectPath string `json:"vmbObjectPath,omitempty"`

	// VMBTObjectPath is the S3 path to the archived VMBT CR JSON for this checkpoint
	VMBTObjectPath string `json:"vmbtObjectPath,omitempty"`
}

// VMIndex is the per-VM index structure stored at checkpoints/<ns>/<vm>/index.json.
// Field names align with the approved design document.
type VMIndex struct {
	// VMName is the name of the VirtualMachine
	VMName string `json:"vmName"`

	// Namespace is the namespace of the VirtualMachine (design: "namespace")
	Namespace string `json:"namespace"`

	// Checkpoints is an ordered list of checkpoints (newest last)
	Checkpoints []CheckpointEntry `json:"checkpoints"`

	// LastUpdated is when this index was last modified
	LastUpdated time.Time `json:"lastUpdated,omitempty"`
}

// BackupManifest is the per-Velero-backup manifest at manifests/<backup>/index.json.
// Field names align with the approved design document.
type BackupManifest struct {
	// BackupName is the name of the Velero backup (design: "backupName")
	BackupName string `json:"backupName"`

	// Timestamp when this backup was created
	Timestamp time.Time `json:"timestamp"`

	// VMs is a list of VM references in this backup (enhancement for convenience)
	VMs []VMBackupReference `json:"vms,omitempty"`
}

// VMBackupReference is a reference to a VM's backup data within a Velero backup.
type VMBackupReference struct {
	// Name is the name of the VirtualMachine (design: "name")
	Name string `json:"name"`

	// Namespace is the namespace of the VirtualMachine (design: "namespace")
	Namespace string `json:"namespace"`

	// CheckpointID is the checkpoint created for this VM
	CheckpointID string `json:"checkpointId"`

	// ManifestPath is the path to the VM's detailed manifest
	ManifestPath string `json:"manifestPath"`
}

// VMBackupManifest is the per-VM manifest at manifests/<backup>/<vm>.json.
// Field names align with the approved design document.
type VMBackupManifest struct {
	// Namespace is the namespace of the VirtualMachine (design: "namespace")
	Namespace string `json:"namespace"`

	// Name is the name of the VirtualMachine (design: "name")
	Name string `json:"name"`

	// CheckpointChain is the ordered list of checkpoints needed for restore.
	// The first entry is the full backup, followed by incrementals.
	CheckpointChain []string `json:"checkpointChain"`

	// BackupName is the Velero backup this manifest belongs to
	BackupName string `json:"backupName,omitempty"`

	// Timestamp when this manifest was created
	Timestamp time.Time `json:"timestamp,omitempty"`
}

// CheckpointIDs returns a slice containing the ID field from each CheckpointEntry.
func CheckpointIDs(checkpoints []CheckpointEntry) []string {
	ids := make([]string, len(checkpoints))
	for i, cp := range checkpoints {
		ids[i] = cp.ID
	}
	return ids
}
