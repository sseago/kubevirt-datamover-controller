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

//nolint:goconst // Test files use repeated string literals for readability
package uploader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func getTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = kubevirtcorev1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	return scheme
}

func getTestClientObjects(config *UploaderConfig, files []CheckpointFile) []client.Object {
	var objects []client.Object
	var volumes []kubevirtcorev1.Volume
	for _, f := range files {
		if f.DiskName != "" {
			objects = append(objects, &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      f.DiskName,
					Namespace: config.VMNamespace,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("10Gi"),
						},
					},
				},
			})
			volumes = append(volumes, kubevirtcorev1.Volume{
				Name: f.DiskName,
				VolumeSource: kubevirtcorev1.VolumeSource{
					DataVolume: &kubevirtcorev1.DataVolumeSource{
						Name: f.DiskName,
					},
				},
			})
		}
	}
	objects = append(objects, &kubevirtcorev1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      config.VMName,
			Namespace: config.VMNamespace,
		},
		Spec: kubevirtcorev1.VirtualMachineSpec{
			Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
				Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
					Volumes: volumes,
				},
			},
		},
	})
	return objects
}

func TestExtractDiskName(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		vmbName  string
		expected string
	}{
		{
			name:     "standard vmb filename with vmb name",
			filename: "vmb-test-du-volume0.qcow2",
			vmbName:  "vmb-test-du",
			expected: "volume0",
		},
		{
			name:     "hyphenated disk name with vmb name",
			filename: "vmb-du-bkp-dm-win-baselinqt2dj-datadisk-1.qcow2",
			vmbName:  "vmb-du-bkp-dm-win-baselinqt2dj",
			expected: "datadisk-1",
		},
		{
			name:     "multiple hyphenated disks with vmb name",
			filename: "vmb-du-bkp-dm-win-baselinqt2dj-datadisk-2.qcow2",
			vmbName:  "vmb-du-bkp-dm-win-baselinqt2dj",
			expected: "datadisk-2",
		},
		{
			name:     "rootdisk with vmb name",
			filename: "vmb-du-bkp-dm-win-baselinqt2dj-rootdisk.qcow2",
			vmbName:  "vmb-du-bkp-dm-win-baselinqt2dj",
			expected: "rootdisk",
		},
		{
			name:     "fallback without vmb name - simple disk",
			filename: "vmb-test-du-volume0.qcow2",
			vmbName:  "",
			expected: "volume0",
		},
		{
			name:     "fallback without vmb name - last segment",
			filename: "vmb-test-2026-02-05_19-12-01-disk1.qcow2",
			vmbName:  "",
			expected: "disk1",
		},
		{
			name:     "simple filename",
			filename: "backup-rootdisk.qcow2",
			vmbName:  "",
			expected: "rootdisk",
		},
		{
			name:     "uppercase extension with vmb name",
			filename: "vmb-test-volume0.QCOW2",
			vmbName:  "vmb-test",
			expected: "volume0",
		},
		{
			name:     "no dash in filename",
			filename: "backup.qcow2",
			vmbName:  "",
			expected: "backup",
		},
		{
			name:     "single part",
			filename: "disk.qcow2",
			vmbName:  "",
			expected: "disk",
		},
		{
			name:     "vmb name does not match prefix - falls back to last segment",
			filename: "vmb-other-prefix-disk1.qcow2",
			vmbName:  "vmb-different",
			expected: "disk1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractDiskName(tt.filename, tt.vmbName)
			if result != tt.expected {
				t.Errorf("extractDiskName(%q, %q) = %q, want %q", tt.filename, tt.vmbName, result, tt.expected)
			}
		})
	}
}

func TestResolveDiskNameFromVolumes(t *testing.T) {
	tests := []struct {
		name      string
		filename  string
		volumeMap map[string]string
		expected  string
	}{
		{
			name:     "matches hyphenated disk name",
			filename: "vmb-du-bkp-baselinqt2dj-datadisk-1.qcow2",
			volumeMap: map[string]string{
				"datadisk-1": "pvc-data-1",
				"datadisk-2": "pvc-data-2",
				"rootdisk":   "pvc-root",
			},
			expected: "datadisk-1",
		},
		{
			name:     "matches simple disk name",
			filename: "vmb-test-rootdisk.qcow2",
			volumeMap: map[string]string{
				"rootdisk": "pvc-root",
			},
			expected: "rootdisk",
		},
		{
			name:     "longest match wins",
			filename: "vmb-test-my-data-disk.qcow2",
			volumeMap: map[string]string{
				"disk":         "pvc-1",
				"data-disk":    "pvc-2",
				"my-data-disk": "pvc-3",
			},
			expected: "my-data-disk",
		},
		{
			name:     "no match returns empty",
			filename: "vmb-test-unknown.qcow2",
			volumeMap: map[string]string{
				"rootdisk": "pvc-root",
			},
			expected: "",
		},
		{
			name:      "empty volume map returns empty",
			filename:  "vmb-test-disk1.qcow2",
			volumeMap: map[string]string{},
			expected:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := resolveDiskNameFromVolumes(tt.filename, tt.volumeMap)
			if result != tt.expected {
				t.Errorf("resolveDiskNameFromVolumes(%q) = %q, want %q", tt.filename, result, tt.expected)
			}
		})
	}
}

//nolint:gocyclo // Table-driven test with many validation cases
func TestLoadConfigFromEnv(t *testing.T) {
	// Save original env and restore after test
	originalEnv := map[string]string{
		common.EnvBSLBucket:                os.Getenv(common.EnvBSLBucket),
		EnvVMName:                          os.Getenv(EnvVMName),
		EnvVMNamespace:                     os.Getenv(EnvVMNamespace),
		common.EnvBSLProvider:              os.Getenv(common.EnvBSLProvider),
		common.EnvBSLPrefix:                os.Getenv(common.EnvBSLPrefix),
		common.EnvBSLRegion:                os.Getenv(common.EnvBSLRegion),
		EnvBackupType:                      os.Getenv(EnvBackupType),
		EnvSourcePVCPath:                   os.Getenv(EnvSourcePVCPath),
		EnvCheckpointName:                  os.Getenv(EnvCheckpointName),
		common.EnvBSLS3URL:                 os.Getenv(common.EnvBSLS3URL),
		common.EnvBSLS3ForcePathStyle:      os.Getenv(common.EnvBSLS3ForcePathStyle),
		common.EnvBSLInsecureSkipTLSVerify: os.Getenv(common.EnvBSLInsecureSkipTLSVerify),
		common.EnvBSLCACert:                os.Getenv(common.EnvBSLCACert),
	}
	defer func() {
		for k, v := range originalEnv {
			if v == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, v)
			}
		}
	}()

	tests := []struct {
		name        string
		envVars     map[string]string
		expectError bool
		validate    func(*testing.T, *UploaderConfig)
	}{
		{
			name: "valid config with all required fields",
			envVars: map[string]string{
				common.EnvBSLBucket: "test-bucket",
				EnvVMName:           "test-vm",
				EnvVMNamespace:      "test-ns",
				EnvCheckpointName:   "cp-001",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *UploaderConfig) {
				if cfg.BSLBucket != "test-bucket" {
					t.Errorf("BSLBucket = %q, want %q", cfg.BSLBucket, "test-bucket")
				}
				if cfg.VMName != "test-vm" {
					t.Errorf("VMName = %q, want %q", cfg.VMName, "test-vm")
				}
				if cfg.VMNamespace != "test-ns" {
					t.Errorf("VMNamespace = %q, want %q", cfg.VMNamespace, "test-ns")
				}
				if cfg.CheckpointName != "cp-001" {
					t.Errorf("CheckpointName = %q, want %q", cfg.CheckpointName, "cp-001")
				}
				// Check defaults
				if cfg.SourcePVCPath != DefaultSourcePVCPath {
					t.Errorf("SourcePVCPath = %q, want %q", cfg.SourcePVCPath, DefaultSourcePVCPath)
				}
				if cfg.CredentialsFile != common.DefaultCredentialsPath {
					t.Errorf("CredentialsFile = %q, want %q", cfg.CredentialsFile, common.DefaultCredentialsPath)
				}
				if cfg.BackupType != "full" {
					t.Errorf("BackupType = %q, want %q", cfg.BackupType, "full")
				}
			},
		},
		{
			name: "missing bucket returns error",
			envVars: map[string]string{
				EnvVMName:         "test-vm",
				EnvVMNamespace:    "test-ns",
				EnvCheckpointName: "cp-001",
			},
			expectError: true,
		},
		{
			name: "missing vm name returns error",
			envVars: map[string]string{
				common.EnvBSLBucket: "test-bucket",
				EnvVMNamespace:      "test-ns",
				EnvCheckpointName:   "cp-001",
			},
			expectError: true,
		},
		{
			name: "missing vm namespace returns error",
			envVars: map[string]string{
				common.EnvBSLBucket: "test-bucket",
				EnvVMName:           "test-vm",
				EnvCheckpointName:   "cp-001",
			},
			expectError: true,
		},
		{
			name: "missing checkpoint name returns error",
			envVars: map[string]string{
				common.EnvBSLBucket: "test-bucket",
				EnvVMName:           "test-vm",
				EnvVMNamespace:      "test-ns",
			},
			expectError: true,
		},
		{
			name: "invalid backup type returns error",
			envVars: map[string]string{
				common.EnvBSLBucket: "test-bucket",
				EnvVMName:           "test-vm",
				EnvVMNamespace:      "test-ns",
				EnvCheckpointName:   "cp-001",
				EnvBackupType:       "invalid",
			},
			expectError: true,
		},
		{
			name: "custom source path is used",
			envVars: map[string]string{
				common.EnvBSLBucket: "test-bucket",
				EnvVMName:           "test-vm",
				EnvVMNamespace:      "test-ns",
				EnvCheckpointName:   "cp-001",
				EnvSourcePVCPath:    "/custom/path",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *UploaderConfig) {
				if cfg.SourcePVCPath != "/custom/path" {
					t.Errorf("SourcePVCPath = %q, want %q", cfg.SourcePVCPath, "/custom/path")
				}
			},
		},
		{
			name: "backup type incremental is preserved",
			envVars: map[string]string{
				common.EnvBSLBucket: "test-bucket",
				EnvVMName:           "test-vm",
				EnvVMNamespace:      "test-ns",
				EnvCheckpointName:   "cp-001",
				EnvBackupType:       "incremental",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *UploaderConfig) {
				if cfg.BackupType != "incremental" {
					t.Errorf("BackupType = %q, want %q", cfg.BackupType, "incremental")
				}
			},
		},
		{
			name: "S3-compatible settings are parsed",
			envVars: map[string]string{
				common.EnvBSLBucket:                "test-bucket",
				EnvVMName:                          "test-vm",
				EnvVMNamespace:                     "test-ns",
				EnvCheckpointName:                  "cp-001",
				common.EnvBSLS3URL:                 "https://minio.example.com",
				common.EnvBSLS3ForcePathStyle:      "true",
				common.EnvBSLInsecureSkipTLSVerify: "true",
				common.EnvBSLCACert:                "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *UploaderConfig) {
				if cfg.BSLS3URL != "https://minio.example.com" {
					t.Errorf("BSLS3URL = %q, want %q", cfg.BSLS3URL, "https://minio.example.com")
				}
				if !cfg.BSLS3ForcePathStyle {
					t.Error("BSLS3ForcePathStyle = false, want true")
				}
				if !cfg.BSLInsecureSkipTLSVerify {
					t.Error("BSLInsecureSkipTLSVerify = false, want true")
				}
				if cfg.BSLCACert != "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----" {
					t.Errorf("BSLCACert = %q, want PEM content", cfg.BSLCACert)
				}
			},
		},
		{
			name: "S3-compatible booleans default to false when unset",
			envVars: map[string]string{
				common.EnvBSLBucket: "test-bucket",
				EnvVMName:           "test-vm",
				EnvVMNamespace:      "test-ns",
				EnvCheckpointName:   "cp-001",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *UploaderConfig) {
				if cfg.BSLS3URL != "" {
					t.Errorf("BSLS3URL = %q, want empty", cfg.BSLS3URL)
				}
				if cfg.BSLS3ForcePathStyle {
					t.Error("BSLS3ForcePathStyle = true, want false")
				}
				if cfg.BSLInsecureSkipTLSVerify {
					t.Error("BSLInsecureSkipTLSVerify = true, want false")
				}
				if cfg.BSLCACert != "" {
					t.Errorf("BSLCACert = %q, want empty", cfg.BSLCACert)
				}
			},
		},
		{
			name: "S3-compatible booleans reject non-true values",
			envVars: map[string]string{
				common.EnvBSLBucket:                "test-bucket",
				EnvVMName:                          "test-vm",
				EnvVMNamespace:                     "test-ns",
				EnvCheckpointName:                  "cp-001",
				common.EnvBSLS3ForcePathStyle:      "false",
				common.EnvBSLInsecureSkipTLSVerify: "yes",
			},
			expectError: false,
			validate: func(t *testing.T, cfg *UploaderConfig) {
				if cfg.BSLS3ForcePathStyle {
					t.Error("BSLS3ForcePathStyle = true for \"false\", want false")
				}
				if cfg.BSLInsecureSkipTLSVerify {
					t.Error("BSLInsecureSkipTLSVerify = true for \"yes\", want false")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all env vars first
			for k := range originalEnv {
				_ = os.Unsetenv(k)
			}
			// Set test env vars
			for k, v := range tt.envVars {
				_ = os.Setenv(k, v)
			}

			cfg, err := LoadConfigFromEnv()

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.expectError && tt.validate != nil {
				tt.validate(t, cfg)
			}
		})
	}
}

func TestUpdateVMIndex(t *testing.T) {
	scheme := getTestScheme()

	tests := []struct {
		name           string
		config         *UploaderConfig
		files          []CheckpointFile
		archived       *archivedPaths
		existingIndex  *VMIndex
		setupStore     func(*MockObjectStore) // optional: customize store after standard setup
		expectError    bool
		validateResult func(*testing.T, *MockObjectStore)
	}{
		{
			name: "creates new index for full backup",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-001",
				BackupType:       "full",
				VMBName:          "vmb-test",
				VeleroBackupName: "backup-001",
			},
			files: []CheckpointFile{
				{
					Filename:   "vmb-test-disk1.qcow2",
					DiskName:   "disk1",
					Size:       1024,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-test-disk1.qcow2",
				},
			},
			archived: &archivedPaths{
				VMBObjectPath:  "checkpoints/test-ns/test-vm/cp-001/vmb.json",
				VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmbt.json",
			},
			existingIndex: nil,
			expectError:   false,
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}
				if len(data) == 0 {
					t.Fatal("index is empty")
				}
				// Verify it contains expected fields
				if !containsBytes(data, "cp-001") {
					t.Error("index should contain checkpoint ID")
				}
				if !containsBytes(data, "full") {
					t.Error("index should contain backup type")
				}
				if !containsBytes(data, "disk1") {
					t.Error("index should contain PVC name")
				}
				// Verify archived paths are referenced
				if !containsBytes(data, "vmbObjectPath") {
					t.Error("index should contain vmbObjectPath")
				}
				if !containsBytes(data, "vmbtObjectPath") {
					t.Error("index should contain vmbtObjectPath")
				}
				if !containsBytes(data, "checkpoints/test-ns/test-vm/cp-001/vmb.json") {
					t.Error("index should reference correct vmb.json path")
				}
				if !containsBytes(data, "checkpoints/test-ns/test-vm/cp-001/vmbt.json") {
					t.Error("index should reference correct vmbt.json path")
				}
			},
		},
		{
			name: "updates existing index for incremental backup",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-002",
				BackupType:       "incremental",
				VMBName:          "vmb-test-2",
				VeleroBackupName: "backup-002",
			},
			files: []CheckpointFile{
				{
					Filename:   "vmb-test-2-disk1.qcow2",
					DiskName:   "disk1",
					Size:       512,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-test-2-disk1.qcow2",
				},
			},
			archived: &archivedPaths{
				VMBObjectPath:  "checkpoints/test-ns/test-vm/cp-002/vmb.json",
				VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmbt.json",
			},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{
						ID:       "cp-001",
						Type:     "full",
						VMBackup: "vmb-test",
						Files: []CheckpointFile{
							{
								Filename:   "vmb-test-disk1.qcow2",
								DiskName:   "disk1",
								ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-test-disk1.qcow2",
							},
						},
					},
				},
			},
			expectError: false,
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}
				// Should contain both checkpoints
				if !containsBytes(data, "cp-001") {
					t.Error("index should contain first checkpoint")
				}
				if !containsBytes(data, "cp-002") {
					t.Error("index should contain second checkpoint")
				}
				// Incremental should have parent reference (note: json.MarshalIndent adds space after colon)
				if !containsBytes(data, "\"parent\": \"cp-001\"") {
					t.Error("incremental checkpoint should have parent")
				}
			},
		},
		{
			name: "incremental with mid-chain break returns error",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-004",
				BackupType:       "incremental",
				VMBName:          "vmb-test-4",
				VeleroBackupName: "backup-004",
			},
			files: []CheckpointFile{
				{
					Filename:   "vmb-test-4-disk1.qcow2",
					DiskName:   "disk1",
					Size:       256,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-004/vmb-test-4-disk1.qcow2",
				},
			},
			archived: &archivedPaths{},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{
						ID:       "cp-001",
						Type:     "full",
						VMBackup: "vmb-test",
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-test-disk1.qcow2"},
						},
					},
					{
						ID:       "cp-002",
						Type:     "incremental",
						Parent:   "cp-001",
						VMBackup: "vmb-test-2",
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-test-2-disk1.qcow2"},
						},
					},
					{
						ID:       "cp-003",
						Type:     "incremental",
						Parent:   "cp-002",
						VMBackup: "vmb-test-3",
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-003/vmb-test-3-disk1.qcow2"},
						},
					},
				},
			},
			setupStore: func(store *MockObjectStore) {
				// Delete cp-002's file to simulate mid-chain break
				_ = store.DeleteObject("test-bucket", "checkpoints/test-ns/test-vm/cp-002/vmb-test-2-disk1.qcow2")
			},
			expectError: true, // Chain fell back — uploader rejects incremental as safety net
		},
		{
			name: "incremental fails when no valid chain exists",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-002",
				BackupType:       "incremental",
				VMBName:          "vmb-test-2",
				VeleroBackupName: "backup-002",
			},
			files: []CheckpointFile{
				{
					Filename:   "vmb-test-2-disk1.qcow2",
					DiskName:   "disk1",
					Size:       512,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-test-2-disk1.qcow2",
				},
			},
			archived: &archivedPaths{},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{
						ID:       "cp-001",
						Type:     "incremental",
						Parent:   "cp-000", // cp-000 doesn't exist - broken chain
						VMBackup: "vmb-test",
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-test-disk1.qcow2"},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "deduplicates checkpoint if already exists",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-001",
				BackupType:       "full",
				VMBName:          "vmb-test",
				VeleroBackupName: "backup-001",
			},
			files: []CheckpointFile{
				{
					Filename:   "vmb-test-disk1.qcow2",
					DiskName:   "disk1",
					Size:       2048,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-test-disk1.qcow2",
				},
			},
			archived: &archivedPaths{
				VMBObjectPath:  "checkpoints/test-ns/test-vm/cp-001/vmb.json",
				VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmbt.json",
			},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{ID: "cp-001", Type: "full", VMBackup: "vmb-old"},
				},
			},
			expectError: false,
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}
				// Should only have one checkpoint (updated) - check for unique id field
				// Note: json.MarshalIndent adds space after colon
				if countOccurrences(data, "\"id\": \"cp-001\"") != 1 {
					t.Errorf("should have exactly one cp-001 entry, got data: %s", string(data))
				}
				// Should have updated VMBackup name
				if !containsBytes(data, "vmb-test") {
					t.Error("should have updated vmBackup name")
				}
				// Old VMBackup should be gone
				if containsBytes(data, "vmb-old") {
					t.Error("old vmBackup name should be replaced")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMockObjectStore("test-bucket", "")

			// Set up existing index if provided
			if tt.existingIndex != nil {
				data, _ := json.Marshal(tt.existingIndex)
				indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json",
					tt.config.VMNamespace, tt.config.VMName)
				_ = store.PutObjectBytes(indexPath, data)

				// Create S3 objects for existing checkpoint files so chain validation passes
				for _, cp := range tt.existingIndex.Checkpoints {
					for _, f := range cp.Files {
						if f.ObjectPath != "" {
							_ = store.PutObjectBytes(f.ObjectPath, []byte("fake-qcow2"))
						}
					}
				}
			}

			// Optional: customize the store (e.g., delete specific files for break tests)
			if tt.setupStore != nil {
				tt.setupStore(store)
			}

			objects := getTestClientObjects(tt.config, tt.files)
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).Build()

			err := updateVMIndex(context.Background(), store, fakeClient, tt.config, tt.files, tt.archived, logr.Discard())

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.validateResult != nil {
				tt.validateResult(t, store)
			}
		})
	}
}

func TestUpdateVMIndex_BackupTypeMismatch(t *testing.T) {
	scheme := getTestScheme()

	tests := []struct {
		name           string
		config         *UploaderConfig
		existingIndex  *VMIndex
		validateResult func(*testing.T, *MockObjectStore)
	}{
		{
			name: "records full backup with no parent",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:             "test-vm",
				VMNamespace:        "test-ns",
				CheckpointName:     "cp-002",
				BackupType:         "full",        // virt-controller actually performed a full backup
				ExpectedBackupType: "incremental", // controller expected an incremental backup
				VMBName:            "vmb-test-2",
				VeleroBackupName:   "backup-002",
			},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{
						ID:       "cp-001",
						Type:     "full",
						VMBackup: "vmb-test",
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-test-disk1.qcow2"},
						},
					},
				},
			},
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}
				// The corrected entry must be recorded as a full backup.
				if !containsBytes(data, "\"type\": \"full\"") {
					t.Errorf("corrected checkpoint should have type full, got: %s", string(data))
				}
				// It must not carry a parent pointer (it is a standalone image).
				if containsBytes(data, "\"parent\"") {
					t.Errorf("corrected full checkpoint should not have a parent pointer, got: %s", string(data))
				}
			},
		},
		{
			name: "overwrites stale incremental entry",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:             "test-vm",
				VMNamespace:        "test-ns",
				CheckpointName:     "cp-002",
				BackupType:         "full",
				ExpectedBackupType: "incremental",
				VMBName:            "vmb-test-2",
				VeleroBackupName:   "backup-002",
			},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{
						ID:       "cp-001",
						Type:     "full",
						VMBackup: "vmb-test",
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-test-disk1.qcow2"},
						},
					},
					{
						// A previous run incorrectly recorded cp-002 as incremental
						// with a parent. The correction must overwrite it.
						ID:       "cp-002",
						Type:     "incremental",
						Parent:   "cp-001",
						VMBackup: "vmb-test-2",
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-test-2-disk1.qcow2"},
						},
					},
				},
			},
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}
				if countOccurrences(data, "\"id\": \"cp-002\"") != 1 {
					t.Errorf("should have exactly one cp-002 entry, got: %s", string(data))
				}
				// The stale parent pointer must be removed.
				if containsBytes(data, "\"parent\": \"cp-001\"") {
					t.Errorf("corrected entry should not retain parent pointer, got: %s", string(data))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files := []CheckpointFile{
				{
					Filename:   "vmb-test-2-disk1.qcow2",
					DiskName:   "disk1",
					Size:       512,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-test-2-disk1.qcow2",
				},
			}
			archived := &archivedPaths{
				VMBObjectPath:  "checkpoints/test-ns/test-vm/cp-002/vmb.json",
				VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmbt.json",
			}

			store := NewMockObjectStore("test-bucket", "")
			data, _ := json.Marshal(tt.existingIndex)
			indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json", tt.config.VMNamespace, tt.config.VMName)
			_ = store.PutObjectBytes(indexPath, data)
			for _, cp := range tt.existingIndex.Checkpoints {
				for _, f := range cp.Files {
					if f.ObjectPath != "" {
						_ = store.PutObjectBytes(f.ObjectPath, []byte("fake-qcow2"))
					}
				}
			}

			objects := getTestClientObjects(tt.config, files)
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).Build()

			if err := updateVMIndex(
				context.Background(), store, fakeClient, tt.config, files, archived, logr.Discard(),
			); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			tt.validateResult(t, store)
		})
	}
}

func TestUpdateVMIndex_HyphenatedDiskNames(t *testing.T) {
	scheme := getTestScheme()

	config := &UploaderConfig{
		ObjectStoreConfig: common.ObjectStoreConfig{
			BSLBucket: "test-bucket",
		},
		VMName:           "test-vm",
		VMNamespace:      "test-ns",
		CheckpointName:   "cp-001",
		BackupType:       "full",
		VMBName:          "vmb-du-bkp-baselinqt2dj",
		VeleroBackupName: "backup-001",
	}

	files := []CheckpointFile{
		{
			Filename:   "vmb-du-bkp-baselinqt2dj-datadisk-1.qcow2",
			DiskName:   "datadisk-1",
			Size:       1024,
			ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-du-bkp-baselinqt2dj-datadisk-1.qcow2",
		},
		{
			Filename:   "vmb-du-bkp-baselinqt2dj-rootdisk.qcow2",
			DiskName:   "rootdisk",
			Size:       512,
			ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-du-bkp-baselinqt2dj-rootdisk.qcow2",
		},
	}

	archived := &archivedPaths{
		VMBObjectPath:  "checkpoints/test-ns/test-vm/cp-001/vmb.json",
		VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmbt.json",
	}

	store := NewMockObjectStore("test-bucket", "")
	objects := getTestClientObjects(config, files)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).Build()

	err := updateVMIndex(context.Background(), store, fakeClient, config, files, archived, logr.Discard())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
	if err != nil {
		t.Fatalf("failed to get index: %v", err)
	}
	if !containsBytes(data, "datadisk-1") {
		t.Error("index should contain hyphenated disk name datadisk-1")
	}
	if !containsBytes(data, "rootdisk") {
		t.Error("index should contain rootdisk")
	}
}

func TestUpdateVMIndex_ResolveDiskNameFallback(t *testing.T) {
	scheme := getTestScheme()

	config := &UploaderConfig{
		ObjectStoreConfig: common.ObjectStoreConfig{
			BSLBucket: "test-bucket",
		},
		VMName:           "test-vm",
		VMNamespace:      "test-ns",
		CheckpointName:   "cp-001",
		BackupType:       "full",
		VMBName:          "vmb-du-bkp-baselinqt2dj",
		VeleroBackupName: "backup-001",
	}

	// Simulate the bug: extractDiskName returned "1" instead of "datadisk-1"
	// because the old last-segment logic split on the final dash.
	files := []CheckpointFile{
		{
			Filename:   "vmb-du-bkp-baselinqt2dj-datadisk-1.qcow2",
			DiskName:   "1",
			Size:       1024,
			ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-du-bkp-baselinqt2dj-datadisk-1.qcow2",
		},
		{
			Filename:   "vmb-du-bkp-baselinqt2dj-datadisk-2.qcow2",
			DiskName:   "2",
			Size:       512,
			ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-du-bkp-baselinqt2dj-datadisk-2.qcow2",
		},
	}

	archived := &archivedPaths{
		VMBObjectPath:  "checkpoints/test-ns/test-vm/cp-001/vmb.json",
		VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmbt.json",
	}

	// Build VM with hyphenated volume names and matching PVCs manually,
	// since getTestClientObjects ties volume names to the (wrong) DiskName.
	objects := []client.Object{
		&kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-vm",
				Namespace: "test-ns",
			},
			Spec: kubevirtcorev1.VirtualMachineSpec{
				Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
					Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
						Volumes: []kubevirtcorev1.Volume{
							{
								Name: "datadisk-1",
								VolumeSource: kubevirtcorev1.VolumeSource{
									DataVolume: &kubevirtcorev1.DataVolumeSource{Name: "datadisk-1"},
								},
							},
							{
								Name: "datadisk-2",
								VolumeSource: kubevirtcorev1.VolumeSource{
									DataVolume: &kubevirtcorev1.DataVolumeSource{Name: "datadisk-2"},
								},
							},
						},
					},
				},
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "datadisk-1", Namespace: "test-ns"},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "datadisk-2", Namespace: "test-ns"},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
		},
	}

	store := NewMockObjectStore("test-bucket", "")
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).Build()

	err := updateVMIndex(context.Background(), store, fakeClient, config, files, archived, logr.Discard())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the fallback corrected the DiskName
	if files[0].DiskName != "datadisk-1" {
		t.Errorf("files[0].DiskName = %q, want %q", files[0].DiskName, "datadisk-1")
	}
	if files[1].DiskName != "datadisk-2" {
		t.Errorf("files[1].DiskName = %q, want %q", files[1].DiskName, "datadisk-2")
	}

	// Verify the index was written with correct PVC names
	data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
	if err != nil {
		t.Fatalf("failed to get index: %v", err)
	}
	if !containsBytes(data, "datadisk-1") {
		t.Error("index should contain resolved PVC name datadisk-1")
	}
	if !containsBytes(data, "datadisk-2") {
		t.Error("index should contain resolved PVC name datadisk-2")
	}
}

func containsBytes(data []byte, substr string) bool {
	return bytes.Contains(data, []byte(substr))
}

func countOccurrences(data []byte, substr string) int {
	return bytes.Count(data, []byte(substr))
}

func TestMergeReferencedBy(t *testing.T) {
	tests := []struct {
		name       string
		existing   []string
		additional []string
		expected   []string
	}{
		{
			name:       "both empty",
			existing:   nil,
			additional: nil,
			expected:   []string{},
		},
		{
			name:       "existing empty, additional has entries",
			existing:   nil,
			additional: []string{"backup-1"},
			expected:   []string{"backup-1"},
		},
		{
			name:       "additional empty",
			existing:   []string{"backup-1"},
			additional: nil,
			expected:   []string{"backup-1"},
		},
		{
			name:       "no overlap",
			existing:   []string{"backup-1"},
			additional: []string{"backup-2"},
			expected:   []string{"backup-1", "backup-2"},
		},
		{
			name:       "full overlap",
			existing:   []string{"backup-1", "backup-2"},
			additional: []string{"backup-1", "backup-2"},
			expected:   []string{"backup-1", "backup-2"},
		},
		{
			name:       "partial overlap",
			existing:   []string{"backup-1", "backup-2"},
			additional: []string{"backup-2", "backup-3"},
			expected:   []string{"backup-1", "backup-2", "backup-3"},
		},
		{
			name:       "duplicates in existing only",
			existing:   []string{"backup-1", "backup-1", "backup-2"},
			additional: []string{"backup-3"},
			expected:   []string{"backup-1", "backup-2", "backup-3"},
		},
		{
			name:       "duplicates in both slices",
			existing:   []string{"backup-1", "backup-2", "backup-1"},
			additional: []string{"backup-2", "backup-3"},
			expected:   []string{"backup-1", "backup-2", "backup-3"},
		},
		{
			name:       "all entries are the same",
			existing:   []string{"backup-1", "backup-1"},
			additional: []string{"backup-1"},
			expected:   []string{"backup-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mergeReferencedBy(tt.existing, tt.additional)
			if len(result) != len(tt.expected) {
				t.Fatalf("len = %d, want %d; got %v", len(result), len(tt.expected), result)
			}
			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("result[%d] = %q, want %q", i, v, tt.expected[i])
				}
			}
		})
	}
}

func TestPropagateReferencedBy(t *testing.T) {
	tests := []struct {
		name        string
		checkpoints []CheckpointEntry
		startID     string
		backupName  string
		// expected maps checkpoint ID -> expected ReferencedBy after propagation
		expected map[string][]string
	}{
		{
			name: "single full checkpoint, no parent to propagate to",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full", ReferencedBy: []string{"backup-1"}},
			},
			startID:    "cp-001",
			backupName: "backup-1",
			expected: map[string][]string{
				"cp-001": {"backup-1"}, // unchanged
			},
		},
		{
			name: "one incremental adds backup to parent",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full", ReferencedBy: []string{"backup-1"}},
				{ID: "cp-002", Type: "incremental", Parent: "cp-001", ReferencedBy: []string{"backup-2"}},
			},
			startID:    "cp-002",
			backupName: "backup-2",
			expected: map[string][]string{
				"cp-001": {"backup-1", "backup-2"},
				"cp-002": {"backup-2"},
			},
		},
		{
			name: "three-level chain propagates to all ancestors",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full", ReferencedBy: []string{"backup-1"}},
				{ID: "cp-002", Type: "incremental", Parent: "cp-001", ReferencedBy: []string{"backup-2"}},
				{ID: "cp-003", Type: "incremental", Parent: "cp-002", ReferencedBy: []string{"backup-3"}},
			},
			startID:    "cp-003",
			backupName: "backup-3",
			expected: map[string][]string{
				"cp-001": {"backup-1", "backup-3"},
				"cp-002": {"backup-2", "backup-3"},
				"cp-003": {"backup-3"}, // unchanged (start is skipped)
			},
		},
		{
			name: "does not add duplicate",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full", ReferencedBy: []string{"backup-1", "backup-2"}},
				{ID: "cp-002", Type: "incremental", Parent: "cp-001", ReferencedBy: []string{"backup-2"}},
			},
			startID:    "cp-002",
			backupName: "backup-2",
			expected: map[string][]string{
				"cp-001": {"backup-1", "backup-2"}, // backup-2 not duplicated
				"cp-002": {"backup-2"},
			},
		},
		{
			name: "start checkpoint not found is a no-op",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full", ReferencedBy: []string{"backup-1"}},
			},
			startID:    "cp-999",
			backupName: "backup-2",
			expected: map[string][]string{
				"cp-001": {"backup-1"},
			},
		},
		{
			name: "cycle in parent chain terminates",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full", Parent: "cp-002", ReferencedBy: []string{}},
				{ID: "cp-002", Type: "incremental", Parent: "cp-001", ReferencedBy: []string{}},
			},
			startID:    "cp-002",
			backupName: "backup-1",
			expected: map[string][]string{
				"cp-001": {"backup-1"},
				"cp-002": {"backup-1"}, // cp-002 is also an ancestor via cycle
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			propagateReferencedBy(tt.checkpoints, tt.startID, tt.backupName)

			for _, cp := range tt.checkpoints {
				expected, ok := tt.expected[cp.ID]
				if !ok {
					continue
				}
				if len(cp.ReferencedBy) != len(expected) {
					t.Errorf("checkpoint %s: ReferencedBy len = %d, want %d; got %v",
						cp.ID, len(cp.ReferencedBy), len(expected), cp.ReferencedBy)
					continue
				}
				for i, v := range cp.ReferencedBy {
					if v != expected[i] {
						t.Errorf("checkpoint %s: ReferencedBy[%d] = %q, want %q",
							cp.ID, i, v, expected[i])
					}
				}
			}
		})
	}
}

func TestUpdateVMIndex_ReferencedByPropagation(t *testing.T) {
	scheme := getTestScheme()

	tests := []struct {
		name           string
		config         *UploaderConfig
		files          []CheckpointFile
		archived       *archivedPaths
		existingIndex  *VMIndex
		validateResult func(*testing.T, *MockObjectStore)
	}{
		{
			name: "incremental backup propagates referencedBy to parent (full)",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-002",
				BackupType:       "incremental",
				VMBName:          "vmb-2",
				VeleroBackupName: "backup-day2",
			},
			files: []CheckpointFile{
				{Filename: "vmb-2-disk1.qcow2", DiskName: "disk1", Size: 512,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-2-disk1.qcow2"},
			},
			archived: &archivedPaths{},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{
						ID: "cp-001", Type: "full", VMBackup: "vmb-1",
						ReferencedBy: []string{"backup-day1"},
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-1-disk1.qcow2"},
						},
					},
				},
			},
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}

				var idx VMIndex
				if err := json.Unmarshal(data, &idx); err != nil {
					t.Fatalf("failed to parse index: %v", err)
				}

				// cp-001 should now have both backup-day1 and backup-day2
				cp001 := findCheckpoint(idx.Checkpoints, "cp-001")
				if cp001 == nil {
					t.Fatal("cp-001 not found")
				}
				assertReferencedBy(t, "cp-001", cp001.ReferencedBy, []string{"backup-day1", "backup-day2"})

				// cp-002 should have backup-day2
				cp002 := findCheckpoint(idx.Checkpoints, "cp-002")
				if cp002 == nil {
					t.Fatal("cp-002 not found")
				}
				assertReferencedBy(t, "cp-002", cp002.ReferencedBy, []string{"backup-day2"})
			},
		},
		{
			name: "three-level chain propagates to all ancestors",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-003",
				BackupType:       "incremental",
				VMBName:          "vmb-3",
				VeleroBackupName: "backup-day3",
			},
			files: []CheckpointFile{
				{Filename: "vmb-3-disk1.qcow2", DiskName: "disk1", Size: 256,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-003/vmb-3-disk1.qcow2"},
			},
			archived: &archivedPaths{},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{
						ID: "cp-001", Type: "full", VMBackup: "vmb-1",
						ReferencedBy: []string{"backup-day1", "backup-day2"},
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-1-disk1.qcow2"},
						},
					},
					{
						ID: "cp-002", Type: "incremental", Parent: "cp-001", VMBackup: "vmb-2",
						ReferencedBy: []string{"backup-day2"},
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-2-disk1.qcow2"},
						},
					},
				},
			},
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}

				var idx VMIndex
				if err := json.Unmarshal(data, &idx); err != nil {
					t.Fatalf("failed to parse index: %v", err)
				}

				cp001 := findCheckpoint(idx.Checkpoints, "cp-001")
				if cp001 == nil {
					t.Fatal("cp-001 not found")
				}
				assertReferencedBy(t, "cp-001", cp001.ReferencedBy,
					[]string{"backup-day1", "backup-day2", "backup-day3"})

				cp002 := findCheckpoint(idx.Checkpoints, "cp-002")
				if cp002 == nil {
					t.Fatal("cp-002 not found")
				}
				assertReferencedBy(t, "cp-002", cp002.ReferencedBy,
					[]string{"backup-day2", "backup-day3"})

				cp003 := findCheckpoint(idx.Checkpoints, "cp-003")
				if cp003 == nil {
					t.Fatal("cp-003 not found")
				}
				assertReferencedBy(t, "cp-003", cp003.ReferencedBy,
					[]string{"backup-day3"})
			},
		},
		{
			name: "full backup does not propagate (no parent)",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-001",
				BackupType:       "full",
				VMBName:          "vmb-1",
				VeleroBackupName: "backup-day1",
			},
			files: []CheckpointFile{
				{Filename: "vmb-1-disk1.qcow2", DiskName: "disk1", Size: 1024,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-1-disk1.qcow2"},
			},
			archived:      &archivedPaths{},
			existingIndex: nil,
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}

				var idx VMIndex
				if err := json.Unmarshal(data, &idx); err != nil {
					t.Fatalf("failed to parse index: %v", err)
				}

				if len(idx.Checkpoints) != 1 {
					t.Fatalf("expected 1 checkpoint, got %d", len(idx.Checkpoints))
				}
				assertReferencedBy(t, "cp-001", idx.Checkpoints[0].ReferencedBy, []string{"backup-day1"})
			},
		},
		{
			name: "dedup checkpoint merges referencedBy",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-001",
				BackupType:       "full",
				VMBName:          "vmb-1",
				VeleroBackupName: "backup-retry",
			},
			files: []CheckpointFile{
				{Filename: "vmb-1-disk1.qcow2", DiskName: "disk1", Size: 1024,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-1-disk1.qcow2"},
			},
			archived: &archivedPaths{},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{
						ID: "cp-001", Type: "full", VMBackup: "vmb-1",
						ReferencedBy: []string{"backup-original", "backup-day2"},
					},
				},
			},
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}

				var idx VMIndex
				if err := json.Unmarshal(data, &idx); err != nil {
					t.Fatalf("failed to parse index: %v", err)
				}

				cp001 := findCheckpoint(idx.Checkpoints, "cp-001")
				if cp001 == nil {
					t.Fatal("cp-001 not found")
				}
				// Should preserve original entries and add the new one
				assertReferencedBy(t, "cp-001", cp001.ReferencedBy,
					[]string{"backup-original", "backup-day2", "backup-retry"})
			},
		},
		{
			name: "new full backup after broken chain does not propagate to old chain",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-004",
				BackupType:       "full",
				VMBName:          "vmb-4",
				VeleroBackupName: "backup-day4",
			},
			files: []CheckpointFile{
				{Filename: "vmb-4-disk1.qcow2", DiskName: "disk1", Size: 2048,
					ObjectPath: "checkpoints/test-ns/test-vm/cp-004/vmb-4-disk1.qcow2"},
			},
			archived: &archivedPaths{},
			existingIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{
						ID: "cp-001", Type: "full", VMBackup: "vmb-1",
						ReferencedBy: []string{"backup-day1", "backup-day2", "backup-day3"},
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-1-disk1.qcow2"},
						},
					},
					{
						ID: "cp-002", Type: "incremental", Parent: "cp-001", VMBackup: "vmb-2",
						ReferencedBy: []string{"backup-day2", "backup-day3"},
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-2-disk1.qcow2"},
						},
					},
					{
						ID: "cp-003", Type: "incremental", Parent: "cp-002", VMBackup: "vmb-3",
						ReferencedBy: []string{"backup-day3"},
						Files: []CheckpointFile{
							{ObjectPath: "checkpoints/test-ns/test-vm/cp-003/vmb-3-disk1.qcow2"},
						},
					},
				},
			},
			validateResult: func(t *testing.T, store *MockObjectStore) {
				data, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
				if err != nil {
					t.Fatalf("failed to get index: %v", err)
				}

				var idx VMIndex
				if err := json.Unmarshal(data, &idx); err != nil {
					t.Fatalf("failed to parse index: %v", err)
				}

				// Old chain checkpoints should be unchanged
				cp001 := findCheckpoint(idx.Checkpoints, "cp-001")
				if cp001 == nil {
					t.Fatal("cp-001 not found")
				}
				assertReferencedBy(t, "cp-001", cp001.ReferencedBy,
					[]string{"backup-day1", "backup-day2", "backup-day3"})

				cp002 := findCheckpoint(idx.Checkpoints, "cp-002")
				if cp002 == nil {
					t.Fatal("cp-002 not found")
				}
				assertReferencedBy(t, "cp-002", cp002.ReferencedBy,
					[]string{"backup-day2", "backup-day3"})

				cp003 := findCheckpoint(idx.Checkpoints, "cp-003")
				if cp003 == nil {
					t.Fatal("cp-003 not found")
				}
				assertReferencedBy(t, "cp-003", cp003.ReferencedBy,
					[]string{"backup-day3"})

				// New full backup starts fresh chain
				cp004 := findCheckpoint(idx.Checkpoints, "cp-004")
				if cp004 == nil {
					t.Fatal("cp-004 not found")
				}
				assertReferencedBy(t, "cp-004", cp004.ReferencedBy,
					[]string{"backup-day4"})

				// Verify cp-004 has no parent
				if cp004.Parent != "" {
					t.Errorf("cp-004 should have no parent, got %q", cp004.Parent)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMockObjectStore("test-bucket", "")

			if tt.existingIndex != nil {
				data, _ := json.Marshal(tt.existingIndex)
				indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json",
					tt.config.VMNamespace, tt.config.VMName)
				_ = store.PutObjectBytes(indexPath, data)

				for _, cp := range tt.existingIndex.Checkpoints {
					for _, f := range cp.Files {
						if f.ObjectPath != "" {
							_ = store.PutObjectBytes(f.ObjectPath, []byte("fake-qcow2"))
						}
					}
				}
			}

			objects := getTestClientObjects(tt.config, tt.files)
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).Build()

			err := updateVMIndex(context.Background(), store, fakeClient, tt.config, tt.files, tt.archived, logr.Discard())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.validateResult != nil {
				tt.validateResult(t, store)
			}
		})
	}
}

// findCheckpoint returns a pointer to the checkpoint with the given ID, or nil.
func findCheckpoint(checkpoints []CheckpointEntry, id string) *CheckpointEntry {
	for i := range checkpoints {
		if checkpoints[i].ID == id {
			return &checkpoints[i]
		}
	}
	return nil
}

// assertReferencedBy asserts that a checkpoint's ReferencedBy matches expected exactly.
func assertReferencedBy(t *testing.T, cpID string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("checkpoint %s: ReferencedBy = %v (len %d), want %v (len %d)",
			cpID, got, len(got), want, len(want))
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("checkpoint %s: ReferencedBy[%d] = %q, want %q",
				cpID, i, got[i], want[i])
		}
	}
}

// TestUpdateVMIndex_DedupIncrementalPreservesParent verifies that when an
// incremental backup re-runs with the same checkpoint ID, the Parent field
// is preserved from the existing entry (not corrupted to a self-reference).
func TestUpdateVMIndex_DedupIncrementalPreservesParent(t *testing.T) {
	scheme := getTestScheme()
	store := NewMockObjectStore("test-bucket", "")

	existingIndex := &VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []CheckpointEntry{
			{
				ID: "cp-001", Type: "full", VMBackup: "vmb-1",
				ReferencedBy: []string{"backup-day1", "backup-day2"},
				Files: []CheckpointFile{
					{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-1-disk1.qcow2"},
				},
			},
			{
				ID: "cp-002", Type: "incremental", Parent: "cp-001", VMBackup: "vmb-2",
				ReferencedBy: []string{"backup-day2"},
				Files: []CheckpointFile{
					{ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-2-disk1.qcow2"},
				},
			},
		},
	}

	// Seed the store with the existing index and checkpoint files
	data, _ := json.Marshal(existingIndex)
	indexPath := "checkpoints/test-ns/test-vm/index.json"
	_ = store.PutObjectBytes(indexPath, data)
	for _, cp := range existingIndex.Checkpoints {
		for _, f := range cp.Files {
			if f.ObjectPath != "" {
				_ = store.PutObjectBytes(f.ObjectPath, []byte("fake-qcow2"))
			}
		}
	}

	config := &UploaderConfig{
		ObjectStoreConfig: common.ObjectStoreConfig{
			BSLBucket: "test-bucket",
		},
		VMName:           "test-vm",
		VMNamespace:      "test-ns",
		CheckpointName:   "cp-002",
		BackupType:       "incremental",
		VMBName:          "vmb-2",
		VeleroBackupName: "backup-retry",
	}
	files := []CheckpointFile{
		{Filename: "vmb-2-disk1.qcow2", DiskName: "disk1", Size: 512,
			ObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmb-2-disk1.qcow2"},
	}

	objects := getTestClientObjects(config, files)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).Build()

	err := updateVMIndex(context.Background(), store, fakeClient, config, files, &archivedPaths{}, logr.Discard())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := store.GetObjectBytes(indexPath)
	if err != nil {
		t.Fatalf("failed to get index: %v", err)
	}

	var idx VMIndex
	if err := json.Unmarshal(result, &idx); err != nil {
		t.Fatalf("failed to parse index: %v", err)
	}

	cp002 := findCheckpoint(idx.Checkpoints, "cp-002")
	if cp002 == nil {
		t.Fatal("cp-002 not found")
	}
	// Parent must remain "cp-001", not get corrupted to "cp-002"
	if cp002.Parent != "cp-001" {
		t.Errorf("cp-002 Parent = %q, want %q", cp002.Parent, "cp-001")
	}
	// ReferencedBy should merge old and new
	assertReferencedBy(t, "cp-002", cp002.ReferencedBy,
		[]string{"backup-day2", "backup-retry"})

	// Parent checkpoint should also get the propagated backup name
	cp001 := findCheckpoint(idx.Checkpoints, "cp-001")
	if cp001 == nil {
		t.Fatal("cp-001 not found")
	}
	assertReferencedBy(t, "cp-001", cp001.ReferencedBy,
		[]string{"backup-day1", "backup-day2", "backup-retry"})
}

// TestUpdateVMIndex_PVCSizeUsesBoundPVCapacity covers issue #160: storage
// backends that enforce a minimum volume size above the request (e.g. AWS
// EBS's 1GiB floor) still expose the full underlying block device to the
// guest, so the recorded PVCSizes entry must reflect the bound PV's actual
// capacity, not the smaller PVC request -- otherwise a later restore
// undersizes its scratch volume against the real on-disk data.
func TestUpdateVMIndex_PVCSizeUsesBoundPVCapacity(t *testing.T) {
	scheme := getTestScheme()
	store := NewMockObjectStore("test-bucket", "")

	config := &UploaderConfig{
		ObjectStoreConfig: common.ObjectStoreConfig{
			BSLBucket: "test-bucket",
		},
		VMName:           "test-vm",
		VMNamespace:      "test-ns",
		CheckpointName:   "cp-001",
		BackupType:       "full",
		VMBName:          "vmb-1",
		VeleroBackupName: "backup-001",
	}
	files := []CheckpointFile{
		{Filename: "vmb-1-disk1.qcow2", DiskName: "disk1", Size: 512,
			ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-1-disk1.qcow2"},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "disk1", Namespace: "test-ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-disk1",
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("150Mi")},
			},
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-disk1"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
		},
	}
	vm := &kubevirtcorev1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: "test-vm", Namespace: "test-ns"},
		Spec: kubevirtcorev1.VirtualMachineSpec{
			Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
				Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
					Volumes: []kubevirtcorev1.Volume{
						{Name: "disk1", VolumeSource: kubevirtcorev1.VolumeSource{
							DataVolume: &kubevirtcorev1.DataVolumeSource{Name: "disk1"},
						}},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc, pv, vm).Build()

	err := updateVMIndex(context.Background(), store, fakeClient, config, files, &archivedPaths{}, logr.Discard())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/index.json")
	if err != nil {
		t.Fatalf("failed to get index: %v", err)
	}
	var idx VMIndex
	if err := json.Unmarshal(result, &idx); err != nil {
		t.Fatalf("failed to parse index: %v", err)
	}

	cp := findCheckpoint(idx.Checkpoints, "cp-001")
	if cp == nil {
		t.Fatal("cp-001 not found")
	}
	if len(cp.PVCSizes) != 1 {
		t.Fatalf("expected 1 PVCSizes entry, got %d", len(cp.PVCSizes))
	}
	want := resource.MustParse("1Gi")
	if cp.PVCSizes[0].Cmp(want) != 0 {
		t.Errorf("PVCSizes[0] = %s, want %s (bound PV's actual capacity, not the 150Mi PVC request)",
			cp.PVCSizes[0].String(), want.String())
	}
}

// TestUpdateVMIndex_BoundPVLookupFailureIsFatal covers the failure side of
// the bound-PV capacity lookup above: if the PV can't be fetched, falling
// back to the PVC's requested size would silently republish the exact
// undersized-scratch-space bug (#160) this lookup exists to prevent, just
// triggered by a different cause (a failed Get instead of a storage-class
// size floor). The upload must fail instead, so it can be retried rather
// than persist an unsafe index.
func TestUpdateVMIndex_BoundPVLookupFailureIsFatal(t *testing.T) {
	scheme := getTestScheme()
	store := NewMockObjectStore("test-bucket", "")

	config := &UploaderConfig{
		ObjectStoreConfig: common.ObjectStoreConfig{
			BSLBucket: "test-bucket",
		},
		VMName:           "test-vm",
		VMNamespace:      "test-ns",
		CheckpointName:   "cp-001",
		BackupType:       "full",
		VMBName:          "vmb-1",
		VeleroBackupName: "backup-001",
	}
	files := []CheckpointFile{
		{Filename: "vmb-1-disk1.qcow2", DiskName: "disk1", Size: 512,
			ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-1-disk1.qcow2"},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "disk1", Namespace: "test-ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-missing",
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("150Mi")},
			},
		},
	}
	// Deliberately no PersistentVolume object -- the PVC's VolumeName points at
	// one that doesn't exist in the fake client.
	vm := &kubevirtcorev1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: "test-vm", Namespace: "test-ns"},
		Spec: kubevirtcorev1.VirtualMachineSpec{
			Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
				Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
					Volumes: []kubevirtcorev1.Volume{
						{Name: "disk1", VolumeSource: kubevirtcorev1.VolumeSource{
							DataVolume: &kubevirtcorev1.DataVolumeSource{Name: "disk1"},
						}},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc, vm).Build()

	err := updateVMIndex(context.Background(), store, fakeClient, config, files, &archivedPaths{}, logr.Discard())
	if err == nil {
		t.Fatal("expected an error when the bound PV cannot be fetched, got nil")
	}
	if !strings.Contains(err.Error(), "pv-missing") {
		t.Errorf("error = %q, want it to mention the missing PV name", err.Error())
	}
}

// TestUpdateVMIndex_BoundPVNoCapacityIsFatal covers the other half of the
// bound-PV capacity lookup's failure contract: the PV exists but declares no
// storage capacity at all (Spec.Capacity has no ResourceStorage entry). Must
// fail the same way a failed Get does, not silently fall back to the PVC's
// requested size.
func TestUpdateVMIndex_BoundPVNoCapacityIsFatal(t *testing.T) {
	scheme := getTestScheme()
	store := NewMockObjectStore("test-bucket", "")

	config := &UploaderConfig{
		ObjectStoreConfig: common.ObjectStoreConfig{
			BSLBucket: "test-bucket",
		},
		VMName:           "test-vm",
		VMNamespace:      "test-ns",
		CheckpointName:   "cp-001",
		BackupType:       "full",
		VMBName:          "vmb-1",
		VeleroBackupName: "backup-001",
	}
	files := []CheckpointFile{
		{Filename: "vmb-1-disk1.qcow2", DiskName: "disk1", Size: 512,
			ObjectPath: "checkpoints/test-ns/test-vm/cp-001/vmb-1-disk1.qcow2"},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "disk1", Namespace: "test-ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-no-capacity",
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("150Mi")},
			},
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-no-capacity"},
		// Deliberately no Spec.Capacity entry for ResourceStorage.
	}
	vm := &kubevirtcorev1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: "test-vm", Namespace: "test-ns"},
		Spec: kubevirtcorev1.VirtualMachineSpec{
			Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
				Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
					Volumes: []kubevirtcorev1.Volume{
						{Name: "disk1", VolumeSource: kubevirtcorev1.VolumeSource{
							DataVolume: &kubevirtcorev1.DataVolumeSource{Name: "disk1"},
						}},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc, pv, vm).Build()

	err := updateVMIndex(context.Background(), store, fakeClient, config, files, &archivedPaths{}, logr.Discard())
	if err == nil {
		t.Fatal("expected an error when the bound PV has no storage capacity, got nil")
	}
	if !strings.Contains(err.Error(), "pv-no-capacity") || !strings.Contains(err.Error(), "no storage capacity") {
		t.Errorf("error = %q, want it to mention the PV name and missing capacity", err.Error())
	}
}

func TestUpdateBackupManifests(t *testing.T) {
	tests := []struct {
		name           string
		config         *UploaderConfig
		vmIndex        *VMIndex
		validateResult func(*testing.T, *MockObjectStore)
	}{
		{
			name: "creates backup manifest and VM manifest",
			config: &UploaderConfig{
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-001",
				VeleroBackupName: "velero-backup-001",
			},
			vmIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{ID: "cp-001", Type: "full"},
				},
			},
			validateResult: func(t *testing.T, store *MockObjectStore) {
				// Check backup index exists
				backupIndex, err := store.GetObjectBytes("manifests/velero-backup-001/index.json")
				if err != nil {
					t.Fatalf("backup index not created: %v", err)
				}
				if !containsBytes(backupIndex, "velero-backup-001") {
					t.Error("backup index should contain backup name")
				}

				// Check VM manifest exists
				vmManifest, err := store.GetObjectBytes("manifests/velero-backup-001/test-ns-test-vm.json")
				if err != nil {
					t.Fatalf("VM manifest not created: %v", err)
				}
				if !containsBytes(vmManifest, "test-vm") {
					t.Error("VM manifest should contain VM name")
				}
				if !containsBytes(vmManifest, "checkpointChain") {
					t.Error("VM manifest should contain checkpoint chain")
				}
			},
		},
		{
			name: "creates checkpoint chain for incremental backup",
			config: &UploaderConfig{
				VMName:           "test-vm",
				VMNamespace:      "test-ns",
				CheckpointName:   "cp-003",
				VeleroBackupName: "velero-backup-003",
			},
			vmIndex: &VMIndex{
				VMName:    "test-vm",
				Namespace: "test-ns",
				Checkpoints: []CheckpointEntry{
					{ID: "cp-001", Type: "full"},
					{ID: "cp-002", Type: "incremental", Parent: "cp-001"},
					{ID: "cp-003", Type: "incremental", Parent: "cp-002"},
				},
			},
			validateResult: func(t *testing.T, store *MockObjectStore) {
				vmManifest, err := store.GetObjectBytes("manifests/velero-backup-003/test-ns-test-vm.json")
				if err != nil {
					t.Fatalf("VM manifest not created: %v", err)
				}
				// Should contain all three checkpoints in chain
				if !containsBytes(vmManifest, "cp-001") {
					t.Error("chain should include base checkpoint")
				}
				if !containsBytes(vmManifest, "cp-002") {
					t.Error("chain should include intermediate checkpoint")
				}
				if !containsBytes(vmManifest, "cp-003") {
					t.Error("chain should include target checkpoint")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMockObjectStore("test-bucket", "")

			// Set up VM index
			indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json",
				tt.config.VMNamespace, tt.config.VMName)
			indexData, _ := json.Marshal(tt.vmIndex)
			_ = store.PutObjectBytes(indexPath, indexData)

			// Simulate updateBackupManifests logic
			chain := buildCheckpointChain(tt.vmIndex.Checkpoints, tt.config.CheckpointName)

			// Create backup manifest
			manifestPath := fmt.Sprintf("manifests/%s/%s-%s.json",
				tt.config.VeleroBackupName, tt.config.VMNamespace, tt.config.VMName)
			backupManifest := BackupManifest{
				BackupName: tt.config.VeleroBackupName,
				VMs: []VMBackupReference{
					{
						Name:         tt.config.VMName,
						Namespace:    tt.config.VMNamespace,
						CheckpointID: tt.config.CheckpointName,
						ManifestPath: manifestPath,
					},
				},
			}
			backupManifestData, _ := json.Marshal(backupManifest)
			backupIndexPath := fmt.Sprintf("manifests/%s/index.json", tt.config.VeleroBackupName)
			_ = store.PutObjectBytes(backupIndexPath, backupManifestData)

			// Create VM manifest
			vmManifest := VMBackupManifest{
				Namespace:       tt.config.VMNamespace,
				Name:            tt.config.VMName,
				CheckpointChain: CheckpointIDs(chain),
				BackupName:      tt.config.VeleroBackupName,
			}
			vmManifestData, _ := json.Marshal(vmManifest)
			_ = store.PutObjectBytes(manifestPath, vmManifestData)

			tt.validateResult(t, store)
		})
	}
}

func TestArchiveKubeResources(t *testing.T) {
	scheme := getTestScheme()

	apiGroup := "kubevirt.io"
	backupAPIGroup := "backup.kubevirt.io"

	tests := []struct {
		name           string
		config         *UploaderConfig
		objects        []client.Object
		expectError    bool
		validateResult func(*testing.T, *MockObjectStore, *archivedPaths)
	}{
		{
			name: "archives VMB and VMBT to checkpoint dir",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:         "test-vm",
				VMNamespace:    "test-ns",
				CheckpointName: "cp-001",
				VMBName:        "vmb-test",
				VMBTName:       "vmbt-test-vm",
			},
			objects: []client.Object{
				&kubevirtbackupv1alpha1.VirtualMachineBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmb-test",
						Namespace: "test-ns",
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
						Source: corev1.TypedLocalObjectReference{
							APIGroup: &backupAPIGroup,
							Kind:     "VirtualMachineBackupTracker",
							Name:     "vmbt-test-vm",
						},
					},
				},
				&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmbt-test-vm",
						Namespace: "test-ns",
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
						Source: corev1.TypedLocalObjectReference{
							APIGroup: &apiGroup,
							Kind:     "VirtualMachine",
							Name:     "test-vm",
						},
					},
					// No Status — KubeVirt doesn't set LatestCheckpoint.
					// archiveKubeResources sets it from cfg.CheckpointName before archiving.
				},
			},
			expectError: false,
			validateResult: func(t *testing.T, store *MockObjectStore, paths *archivedPaths) {
				// Verify vmb.json was uploaded to checkpoint dir
				vmbData, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/cp-001/vmb.json")
				if err != nil {
					t.Fatalf("vmb.json not found in store: %v", err)
				}
				if !containsBytes(vmbData, "vmb-test") {
					t.Error("vmb.json should contain VMB name")
				}

				// Verify vmbt.json was uploaded to checkpoint dir (not VM level)
				vmbtData, err := store.GetObjectBytes("checkpoints/test-ns/test-vm/cp-001/vmbt.json")
				if err != nil {
					t.Fatalf("vmbt.json not found in store: %v", err)
				}
				if !containsBytes(vmbtData, "vmbt-test-vm") {
					t.Error("vmbt.json should contain VMBT name")
				}

				// Verify that archiveKubeResources set LatestCheckpoint in the archived VMBT.
				// KubeVirt does NOT update this field — our code sets it in-memory before serializing.
				var archivedVMBT kubevirtbackupv1alpha1.VirtualMachineBackupTracker
				if err := json.Unmarshal(vmbtData, &archivedVMBT); err != nil {
					t.Fatalf("failed to unmarshal archived vmbt.json: %v", err)
				}
				if archivedVMBT.Status == nil || archivedVMBT.Status.LatestCheckpoint == nil {
					t.Fatal("archived VMBT should have Status.LatestCheckpoint set")
				}
				if archivedVMBT.Status.LatestCheckpoint.Name != "cp-001" {
					t.Errorf("archived VMBT LatestCheckpoint.Name = %q, want %q",
						archivedVMBT.Status.LatestCheckpoint.Name, "cp-001")
				}

				// Verify returned paths
				if paths.VMBObjectPath != "checkpoints/test-ns/test-vm/cp-001/vmb.json" {
					t.Errorf("VMBObjectPath = %q, want checkpoints/test-ns/test-vm/cp-001/vmb.json", paths.VMBObjectPath)
				}
				if paths.VMBTObjectPath != "checkpoints/test-ns/test-vm/cp-001/vmbt.json" {
					t.Errorf("VMBTObjectPath = %q, want checkpoints/test-ns/test-vm/cp-001/vmbt.json", paths.VMBTObjectPath)
				}
			},
		},
		{
			name: "fails when VMB not found (fatal)",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:         "test-vm",
				VMNamespace:    "test-ns",
				CheckpointName: "cp-001",
				VMBName:        "vmb-nonexistent",
				VMBTName:       "vmbt-test-vm",
			},
			objects: []client.Object{
				&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmbt-test-vm",
						Namespace: "test-ns",
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
						Source: corev1.TypedLocalObjectReference{
							APIGroup: &apiGroup,
							Kind:     "VirtualMachine",
							Name:     "test-vm",
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "reconstructs VMBT when not found in cluster",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:         "test-vm",
				VMNamespace:    "test-ns",
				CheckpointName: "cp-001",
				VMBName:        "vmb-test",
				VMBTName:       "vmbt-nonexistent",
			},
			objects: []client.Object{
				&kubevirtbackupv1alpha1.VirtualMachineBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmb-test",
						Namespace: "test-ns",
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
						Source: corev1.TypedLocalObjectReference{
							APIGroup: &backupAPIGroup,
							Kind:     "VirtualMachineBackupTracker",
							Name:     "vmbt-test-vm",
						},
					},
				},
			},
			expectError: false,
			validateResult: func(t *testing.T, store *MockObjectStore, paths *archivedPaths) {
				if paths.VMBTObjectPath == "" {
					t.Error("expected VMBT to be archived even when reconstructed")
				}
				// Verify the reconstructed VMBT was archived with correct checkpoint
				data, err := GetObjectBytes(store, "test-bucket", paths.VMBTObjectPath)
				if err != nil {
					t.Fatalf("failed to read archived VMBT: %v", err)
				}
				var vmbt kubevirtbackupv1alpha1.VirtualMachineBackupTracker
				if err := json.Unmarshal(data, &vmbt); err != nil {
					t.Fatalf("failed to unmarshal archived VMBT: %v", err)
				}
				if vmbt.Name != "vmbt-nonexistent" {
					t.Errorf("reconstructed VMBT name = %q, want %q", vmbt.Name, "vmbt-nonexistent")
				}
				if vmbt.Status == nil || vmbt.Status.LatestCheckpoint == nil {
					t.Fatal("expected LatestCheckpoint to be set on reconstructed VMBT")
				}
				if vmbt.Status.LatestCheckpoint.Name != "cp-001" {
					t.Errorf("LatestCheckpoint = %q, want %q", vmbt.Status.LatestCheckpoint.Name, "cp-001")
				}
			},
		},
		{
			name: "handles empty VMBName and VMBTName",
			config: &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLBucket: "test-bucket",
				},
				VMName:         "test-vm",
				VMNamespace:    "test-ns",
				CheckpointName: "cp-001",
				VMBName:        "",
				VMBTName:       "",
			},
			objects:     []client.Object{},
			expectError: false,
			validateResult: func(t *testing.T, store *MockObjectStore, paths *archivedPaths) {
				objs := store.GetAllObjects()
				if len(objs) != 0 {
					t.Errorf("expected no objects uploaded when names are empty, got %d", len(objs))
				}
				if paths.VMBObjectPath != "" {
					t.Errorf("VMBObjectPath should be empty, got %q", paths.VMBObjectPath)
				}
				if paths.VMBTObjectPath != "" {
					t.Errorf("VMBTObjectPath should be empty, got %q", paths.VMBTObjectPath)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMockObjectStore("test-bucket", "")

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.objects...).
				Build()

			paths, err := archiveKubeResources(context.Background(), store, fakeClient, tt.config, logr.Discard())

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.validateResult != nil {
				tt.validateResult(t, store, paths)
			}
		})
	}
}

func TestBuildCheckpointChain(t *testing.T) {
	tests := []struct {
		name        string
		checkpoints []CheckpointEntry
		targetID    string
		expectLen   int
		expectFirst string
		expectLast  string
	}{
		{
			name: "single checkpoint (full backup)",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full"},
			},
			targetID:    "cp-001",
			expectLen:   1,
			expectFirst: "cp-001",
			expectLast:  "cp-001",
		},
		{
			name: "chain of three checkpoints",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full"},
				{ID: "cp-002", Type: "incremental", Parent: "cp-001"},
				{ID: "cp-003", Type: "incremental", Parent: "cp-002"},
			},
			targetID:    "cp-003",
			expectLen:   3,
			expectFirst: "cp-001",
			expectLast:  "cp-003",
		},
		{
			name: "target in middle of chain",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full"},
				{ID: "cp-002", Type: "incremental", Parent: "cp-001"},
				{ID: "cp-003", Type: "incremental", Parent: "cp-002"},
			},
			targetID:    "cp-002",
			expectLen:   2,
			expectFirst: "cp-001",
			expectLast:  "cp-002",
		},
		{
			name: "target not found",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full"},
			},
			targetID:  "nonexistent",
			expectLen: 0,
		},
		{
			name:        "empty checkpoints",
			checkpoints: []CheckpointEntry{},
			targetID:    "cp-001",
			expectLen:   0,
		},
		{
			name: "broken chain (missing parent)",
			checkpoints: []CheckpointEntry{
				{ID: "cp-002", Type: "incremental", Parent: "cp-001"}, // cp-001 doesn't exist
				{ID: "cp-003", Type: "incremental", Parent: "cp-002"},
			},
			targetID:    "cp-003",
			expectLen:   2, // Only cp-002 and cp-003, stops when cp-001 not found
			expectFirst: "cp-002",
			expectLast:  "cp-003",
		},
		{
			name: "cycle in parent references",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full", Parent: "cp-003"},        // cycle: cp-001 → cp-003
				{ID: "cp-002", Type: "incremental", Parent: "cp-001"}, // cp-002 → cp-001
				{ID: "cp-003", Type: "incremental", Parent: "cp-002"}, // cp-003 → cp-002
			},
			targetID:    "cp-003",
			expectLen:   3, // Should terminate after visiting all 3 (cycle detected on revisit)
			expectFirst: "cp-001",
			expectLast:  "cp-003",
		},
		{
			name: "self-referencing checkpoint",
			checkpoints: []CheckpointEntry{
				{ID: "cp-001", Type: "full", Parent: "cp-001"}, // points to itself
			},
			targetID:    "cp-001",
			expectLen:   1,
			expectFirst: "cp-001",
			expectLast:  "cp-001",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain := buildCheckpointChain(tt.checkpoints, tt.targetID)

			if len(chain) != tt.expectLen {
				t.Errorf("chain length = %d, want %d", len(chain), tt.expectLen)
			}

			if tt.expectLen > 0 {
				if chain[0].ID != tt.expectFirst {
					t.Errorf("first checkpoint = %q, want %q", chain[0].ID, tt.expectFirst)
				}
				if chain[len(chain)-1].ID != tt.expectLast {
					t.Errorf("last checkpoint = %q, want %q", chain[len(chain)-1].ID, tt.expectLast)
				}
			}
		})
	}
}
