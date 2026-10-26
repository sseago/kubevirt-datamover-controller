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

package controller

import (
	"maps"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/downloader"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// OperationMode determines whether the datamover pod runs upload or download.
type OperationMode string

const (
	// OperationModeUpload runs the backup upload path.
	OperationModeUpload OperationMode = OperationMode(common.PodTypeUpload)

	// OperationModeDownload runs the restore download path.
	OperationModeDownload OperationMode = OperationMode(common.PodTypeDownload)
)

// downloadTargetFilename is where the reconstructed raw disk image is written,
// relative to the scratch PVC mount. It must live inside the scratch mount since
// the download pod has only one read-write volume (the scratch PVC): the same
// volume stages the downloaded qcow2 chain and holds the final flattened image
// until the PV is rebound into the restore target namespace. Named disk.img at
// the mount root (not a subdirectory) to match the CDI/KubeVirt convention for
// filesystem-mode PVC-backed disk images, since this PV is rebound directly as
// the restore target's volume. Downloaded qcow2 chain files are named
// "<index>-<filename>.qcow2" at this same root (see downloadCheckpointFiles),
// so there's no collision with disk.img.
const downloadTargetFilename = "disk.img"

// cloudCredentialsVolumeName is the name shared by the BSL credentials
// volume and its mount in both upload and download pods.
const cloudCredentialsVolumeName = "cloud-credentials"

// boundSATokenVolumeName is the name shared by the projected service-account
// token volume and its mount in both upload and download pods (STS auth).
const boundSATokenVolumeName = "bound-sa-token"

// scratchDataVolumeName is the download-mode scratch PVC volume/mount name.
const scratchDataVolumeName = "scratch-data"

// backupDataVolumeName is the upload-mode source PVC volume/mount name.
const backupDataVolumeName = "backup-data"

// restoreOutputVolumeName is the Block-mode target's second scratch volume
// name (a raw block device, mounted as a volumeDevice rather than a
// volumeMount) -- see DatamoverPodConfig.OutputPVCName.
const restoreOutputVolumeName = "restore-output"

// defaultOutputDevicePath is the default path inside the container
// restoreOutputVolumeName's volumeDevice is mapped to, used when
// DatamoverPodConfig.OutputDevicePath is unset.
const defaultOutputDevicePath = "/dev/restore-output"

// DatamoverPodConfig contains configuration for building a datamover pod.
type DatamoverPodConfig struct {
	// OperationMode selects upload or download. Defaults to upload.
	OperationMode OperationMode

	// Pod identity
	Name      string
	Namespace string

	// Container image
	Image           string
	ImagePullPolicy corev1.PullPolicy

	// BSL configuration
	BSLProvider string
	BSLBucket   string
	BSLPrefix   string
	BSLRegion   string

	// S3-compatible storage provider settings
	BSLS3URL                 string
	BSLS3ForcePathStyle      string
	BSLInsecureSkipTLSVerify string
	BSLCACert                string
	BSLServerSideEncryption  string
	BSLKMSKeyID              string
	BSLChecksumAlgorithm     string
	BSLProfile               string
	// SSE-C secret reference (parsed from BSL customerKeyEncryptionSecret "secretName/key")
	SSECSecretName string
	SSECSecretKey  string

	// GCP-specific storage provider settings
	BSLServiceAccount string
	BSLKMSKeyName     string

	// Azure-specific storage provider settings
	BSLResourceGroup               string
	BSLStorageAccount              string
	BSLStorageAccountKeyEnvVar     string
	BSLStorageAccountURI           string
	BSLSubscriptionID              string
	BSLUseAAD                      string
	BSLActiveDirectoryAuthorityURI string

	// Credentials
	CredentialSecretName string
	CredentialSecretKey  string

	// VM context
	VMName      string
	VMNamespace string

	// Backup context
	CheckpointName     string
	BackupType         string
	ExpectedBackupType string
	VeleroBackupName   string
	ResourceName       string
	ResourceUID        string
	VMBName            string
	VMBTName           string

	// Identity label/annotation keys for the owning resource (DataUpload or DataDownload).
	// These determine which label key carries the UID and which annotation carries the name.
	UIDLabelKey       string
	NameAnnotationKey string

	// Source PVC (upload mode only): the app PVC being backed up, mounted read-only.
	SourcePVCName string

	// ScratchPVCName (download mode only): read-write Filesystem-mode PVC that
	// stages the downloaded qcow2 chain. For a Filesystem-mode target volume,
	// this also holds the final flattened raw disk image and is the volume
	// rebound onto the target PVC. For a Block-mode target volume, the final
	// image instead goes onto OutputPVCName -- this PVC only ever holds the
	// qcow2 chain and is discarded (never rebound) after the pod completes.
	ScratchPVCName string

	// OutputPVCName (download mode, Block-mode target only): a second,
	// Block-mode PVC that receives only the final flattened raw disk image,
	// mounted as a raw volumeDevice rather than a filesystem mount. This is
	// the volume rebound onto the target PVC on completion. Empty for a
	// Filesystem-mode target, where ScratchPVCName alone serves both roles.
	OutputPVCName string

	// OutputDevicePath (download mode, Block-mode target only): the path
	// inside the container OutputPVCName's volumeDevice is mapped to.
	OutputDevicePath string

	// TargetVolume (download mode only): the disk/PVC name being restored, used to
	// filter the checkpoint chain down to the matching file.
	TargetVolume string

	// Labels for pod
	Labels map[string]string
}

// buildDatamoverPod creates a Pod spec for the datamover.
func buildDatamoverPod(config *DatamoverPodConfig) *corev1.Pod {
	mode := config.OperationMode
	if mode == "" {
		mode = OperationModeUpload
	}

	// Merge default labels with provided labels.
	// Use UID for labels (always ≤ 63 chars); name goes in annotations.
	// Defaults are mode-aware: an unset key must default to the constant matching
	// this pod's own operation mode, not unconditionally to DataUpload's -- a
	// caller that forgets to set these for a download-mode config would otherwise
	// silently mislabel the pod, breaking findPodForDataDownload's label lookup.
	uidLabelKey := config.UIDLabelKey
	nameAnnotationKey := config.NameAnnotationKey
	if mode == OperationModeDownload {
		if uidLabelKey == "" {
			uidLabelKey = common.LabelDataDownloadUID
		}
		if nameAnnotationKey == "" {
			nameAnnotationKey = common.AnnotationDataDownloadName
		}
	} else {
		if uidLabelKey == "" {
			uidLabelKey = common.LabelDataUploadUID
		}
		if nameAnnotationKey == "" {
			nameAnnotationKey = common.AnnotationDataUploadName
		}
	}

	labels := map[string]string{
		common.LabelDatamoverPod: string(mode),
		uidLabelKey:              config.ResourceUID,
	}
	maps.Copy(labels, config.Labels)

	annotations := map[string]string{
		nameAnnotationKey: config.ResourceName,
	}

	var envVars []corev1.EnvVar
	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount
	var volumeDevices []corev1.VolumeDevice

	if mode == OperationModeDownload {
		envVars, volumes, volumeMounts, volumeDevices = buildDownloadPodResources(config)
	} else {
		envVars, volumes, volumeMounts = buildUploadPodResources(config)
	}

	// Inject Azure Workload Identity env vars if present in the controller pod
	for _, envName := range []string{"AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_FEDERATED_TOKEN_FILE"} {
		if val := os.Getenv(envName); val != "" {
			envVars = append(envVars, corev1.EnvVar{Name: envName, Value: val})
		}
	}

	// Security context - follows Velero's pattern for CSI snapshot pods:
	// - Run as root (UID 0) to access qcow2 files which may be owned by different users
	//   (KubeVirt uses qemu user, and file ownership varies by storage provider)
	// - Use spc_t SELinux type to bypass SELinux label checking for cross-namespace volumes
	// - Uses velero service account which has the SCC (SecurityContextConstraints) needed
	//   for privileged access on OpenShift
	runAsUser := int64(0)
	readOnlyRootFilesystem := true

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        config.Name,
			Namespace:   config.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			// Use velero service account which has the SCC needed for root access
			ServiceAccountName: "velero",
			RestartPolicy:      corev1.RestartPolicyNever,
			// Pod-level security context: run as root with spc_t SELinux type
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser: &runAsUser,
				SELinuxOptions: &corev1.SELinuxOptions{
					Type: "spc_t",
				},
			},
			Containers: []corev1.Container{
				{
					Name:            string(mode),
					Image:           config.Image,
					ImagePullPolicy: config.ImagePullPolicy,
					Command:         []string{"/manager", string(mode)},
					Env:             envVars,
					VolumeMounts:    volumeMounts,
					VolumeDevices:   volumeDevices,
					SecurityContext: &corev1.SecurityContext{
						ReadOnlyRootFilesystem: &readOnlyRootFilesystem,
					},
				},
			},
			Volumes: volumes,
		},
	}

	if config.SSECSecretName != "" {
		pod.Spec.Volumes = append(pod.Spec.Volumes, sseCustomerKeyVolume(config))
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{
				Name:      "sse-c-key",
				MountPath: "/etc/sse-c",
				ReadOnly:  true,
			})
	}

	return pod
}

// buildSharedBSLEnvVars returns the BSL provider/connection env vars common to
// both upload and download pods (S3, GCP service-account auth, Azure
// storage-account/AAD auth, plus the shared credentials-file path). Extracted
// to a single list so the two pod-resource builders can't silently diverge on
// which BSL settings a datamover pod receives.
func buildSharedBSLEnvVars(config *DatamoverPodConfig) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: common.EnvBSLProvider, Value: config.BSLProvider},
		{Name: common.EnvBSLBucket, Value: config.BSLBucket},
		{Name: common.EnvBSLPrefix, Value: config.BSLPrefix},
		{Name: common.EnvBSLRegion, Value: config.BSLRegion},
		{Name: common.EnvBSLS3URL, Value: config.BSLS3URL},
		{Name: common.EnvBSLS3ForcePathStyle, Value: config.BSLS3ForcePathStyle},
		{Name: common.EnvBSLInsecureSkipTLSVerify, Value: config.BSLInsecureSkipTLSVerify},
		{Name: common.EnvBSLCACert, Value: config.BSLCACert},
		{Name: common.EnvBSLServerSideEncryption, Value: config.BSLServerSideEncryption},
		{Name: common.EnvBSLKMSKeyID, Value: config.BSLKMSKeyID},
		{Name: common.EnvBSLChecksumAlgorithm, Value: config.BSLChecksumAlgorithm},
		{Name: common.EnvBSLCustomerKeyEncryptionFile, Value: sseCustomerKeyPath(config)},
		{Name: common.EnvBSLProfile, Value: config.BSLProfile},
		{Name: common.EnvBSLServiceAccount, Value: config.BSLServiceAccount},
		{Name: common.EnvBSLResourceGroup, Value: config.BSLResourceGroup},
		{Name: common.EnvBSLStorageAccount, Value: config.BSLStorageAccount},
		{Name: common.EnvBSLStorageAccountKeyEnvVar, Value: config.BSLStorageAccountKeyEnvVar},
		{Name: common.EnvBSLStorageAccountURI, Value: config.BSLStorageAccountURI},
		{Name: common.EnvBSLSubscriptionID, Value: config.BSLSubscriptionID},
		{Name: common.EnvBSLUseAAD, Value: config.BSLUseAAD},
		{Name: common.EnvBSLActiveDirectoryAuthorityURI, Value: config.BSLActiveDirectoryAuthorityURI},
		{Name: common.EnvCredentialsFile, Value: common.DefaultCredentialsPath},
	}
}

// buildDownloadPodResources returns the env vars, volumes, volume mounts, and
// (Block-mode target only) volume devices for a download-mode
// (OperationModeDownload) datamover pod.
func buildDownloadPodResources(config *DatamoverPodConfig) ([]corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount, []corev1.VolumeDevice) {
	// For a Filesystem-mode target, the final raw image is written into the
	// scratch volume itself (matching CDI/KubeVirt's disk.img convention,
	// since this same volume's PV is rebound directly as the restore target).
	// For a Block-mode target, it instead goes onto the separate OutputPVCName
	// device -- the scratch volume only ever holds the qcow2 chain then, and
	// is discarded (never rebound) once the pod completes.
	targetIsBlockDevice := config.OutputPVCName != ""
	targetPath := path.Join(downloader.DefaultScratchPath, downloadTargetFilename)
	outputDevicePath := config.OutputDevicePath
	if outputDevicePath == "" {
		outputDevicePath = defaultOutputDevicePath
	}
	if targetIsBlockDevice {
		targetPath = outputDevicePath
	}

	envVars := append(buildSharedBSLEnvVars(config),
		corev1.EnvVar{Name: downloader.EnvVMName, Value: config.VMName},
		corev1.EnvVar{Name: downloader.EnvVMNamespace, Value: config.VMNamespace},
		corev1.EnvVar{Name: downloader.EnvVeleroBackupName, Value: config.VeleroBackupName},
		corev1.EnvVar{Name: downloader.EnvDataDownloadName, Value: config.ResourceName},
		corev1.EnvVar{Name: downloader.EnvDataDownloadUID, Value: config.ResourceUID},
		corev1.EnvVar{Name: downloader.EnvTargetVolume, Value: config.TargetVolume},
		corev1.EnvVar{Name: downloader.EnvScratchPath, Value: downloader.DefaultScratchPath},
		corev1.EnvVar{Name: downloader.EnvTargetPath, Value: targetPath},
		corev1.EnvVar{Name: downloader.EnvTargetIsBlockDevice, Value: strconv.FormatBool(targetIsBlockDevice)},
	)

	volumes := []corev1.Volume{
		{
			Name: scratchDataVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: config.ScratchPVCName,
				},
			},
		},
		buildCredentialsVolume(config),
		buildBoundSATokenVolume(),
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      scratchDataVolumeName,
			MountPath: downloader.DefaultScratchPath,
		},
		buildCredentialsVolumeMount(),
		buildBoundSATokenVolumeMount(),
	}

	var volumeDevices []corev1.VolumeDevice
	if targetIsBlockDevice {
		volumes = append(volumes, corev1.Volume{
			Name: restoreOutputVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: config.OutputPVCName,
				},
			},
		})
		volumeDevices = []corev1.VolumeDevice{
			{
				Name:       restoreOutputVolumeName,
				DevicePath: outputDevicePath,
			},
		}
	}

	return envVars, volumes, volumeMounts, volumeDevices
}

// buildCredentialsVolume returns the BSL credentials secret volume shared by
// upload and download pods. The secret key (from BSL config) is mounted at a
// fixed filename matching common.DefaultCredentialsPath -- simpler than
// Velero's dynamic path approach since we control both ends.
func buildCredentialsVolume(config *DatamoverPodConfig) corev1.Volume {
	credentialsMode := int32(0400)
	return corev1.Volume{
		Name: cloudCredentialsVolumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  config.CredentialSecretName,
				DefaultMode: &credentialsMode,
				Items: []corev1.KeyToPath{
					{
						Key:  config.CredentialSecretKey,
						Path: path.Base(common.DefaultCredentialsPath),
					},
				},
			},
		},
	}
}

// buildCredentialsVolumeMount returns the mount for buildCredentialsVolume.
func buildCredentialsVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      cloudCredentialsVolumeName,
		MountPath: path.Dir(common.DefaultCredentialsPath),
		ReadOnly:  true,
	}
}

// buildBoundSATokenVolume returns the projected service-account token volume
// shared by upload and download pods, used for STS auth to cloud storage.
func buildBoundSATokenVolume() corev1.Volume {
	saTokenExpSeconds := int64(3600)
	return corev1.Volume{
		Name: boundSATokenVolumeName,
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{
					{
						ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
							Audience:          "openshift",
							ExpirationSeconds: &saTokenExpSeconds,
							Path:              "token",
						},
					},
				},
			},
		},
	}
}

// buildBoundSATokenVolumeMount returns the mount for buildBoundSATokenVolume.
func buildBoundSATokenVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      boundSATokenVolumeName,
		MountPath: "/var/run/secrets/openshift/serviceaccount",
		ReadOnly:  true,
	}
}

// buildUploadPodResources returns the env vars, volumes, and volume mounts for
// an upload-mode (OperationModeUpload) datamover pod.
func buildUploadPodResources(config *DatamoverPodConfig) ([]corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount) {
	envVars := append(buildSharedBSLEnvVars(config),
		// EnvBSLKMSKeyName is upload-only: it names the SSE-KMS key used to
		// encrypt newly-uploaded objects. Downloads read existing objects and
		// rely on the bucket's own SSE-KMS config to decrypt transparently, so
		// they never need this.
		corev1.EnvVar{Name: common.EnvBSLKMSKeyName, Value: config.BSLKMSKeyName},
		corev1.EnvVar{Name: uploader.EnvVMName, Value: config.VMName},
		corev1.EnvVar{Name: uploader.EnvVMNamespace, Value: config.VMNamespace},
		corev1.EnvVar{Name: uploader.EnvCheckpointName, Value: config.CheckpointName},
		corev1.EnvVar{Name: uploader.EnvBackupType, Value: config.BackupType},
		corev1.EnvVar{Name: uploader.EnvExpectedBackupType, Value: config.ExpectedBackupType},
		corev1.EnvVar{Name: uploader.EnvVeleroBackupName, Value: config.VeleroBackupName},
		corev1.EnvVar{Name: uploader.EnvSourcePVCPath, Value: uploader.DefaultSourcePVCPath},
		corev1.EnvVar{Name: uploader.EnvDataUploadName, Value: config.ResourceName},
		corev1.EnvVar{Name: uploader.EnvDataUploadUID, Value: config.ResourceUID},
		corev1.EnvVar{Name: uploader.EnvVMBName, Value: config.VMBName},
		corev1.EnvVar{Name: uploader.EnvVMBTName, Value: config.VMBTName},
		corev1.EnvVar{
			Name: uploader.EnvPodName,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
		corev1.EnvVar{
			Name: uploader.EnvPodNamespace,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			},
		},
	)

	volumes := []corev1.Volume{
		{
			Name: backupDataVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: config.SourcePVCName,
					ReadOnly:  true,
				},
			},
		},
		buildCredentialsVolume(config),
		buildBoundSATokenVolume(),
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      backupDataVolumeName,
			MountPath: uploader.DefaultSourcePVCPath,
			ReadOnly:  true,
		},
		buildCredentialsVolumeMount(),
		buildBoundSATokenVolumeMount(),
	}

	return envVars, volumes, volumeMounts
}

const sseCustomerKeyMountPath = "/etc/sse-c/key"

// sseCustomerKeyPath returns the mounted file path for the SSE-C key,
// or empty string if SSE-C is not configured.
func sseCustomerKeyPath(config *DatamoverPodConfig) string {
	if config.SSECSecretName == "" {
		return ""
	}
	return sseCustomerKeyMountPath
}

// sseCustomerKeyVolume returns the volume for the SSE-C key secret.
func sseCustomerKeyVolume(config *DatamoverPodConfig) corev1.Volume {
	return corev1.Volume{
		Name: "sse-c-key",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: config.SSECSecretName,
				Items: []corev1.KeyToPath{
					{
						Key:  config.SSECSecretKey,
						Path: filepath.Base(sseCustomerKeyMountPath),
					},
				},
			},
		},
	}
}
