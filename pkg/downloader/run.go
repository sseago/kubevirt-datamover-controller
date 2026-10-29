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
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-logr/logr"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	restcfg "sigs.k8s.io/controller-runtime/pkg/client/config"
)

var newKubeClient = func() (client.Client, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := kubevirtbackupv1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to register KubeVirt backup scheme: %w", err)
	}
	if err := kubevirtcorev1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to register KubeVirt scheme: %w", err)
	}

	restConfig, err := restcfg.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	k8sClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create K8s client: %w", err)
	}

	return k8sClient, nil
}

// Run is the main entrypoint for the downloader.
func Run(ctx context.Context, logger logr.Logger) error {
	logger.Info("Starting kubevirt datamover downloader")

	config, err := LoadConfigFromEnv()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	logger.Info("Config loaded",
		"vm", config.VMNamespace+"/"+config.VMName,
		"backup", config.VeleroBackupName,
		"targetVolume", config.TargetVolume)

	if config.TargetIsBlockDevice {
		// The target is a raw block device (a rebound Block-mode PV mounted as
		// a volumeDevice), not a path inside a mounted filesystem -- there's no
		// containing directory to prepare, and attempting to would try to
		// MkdirAll/chmod something like "/dev" itself.
		logger.Info("Target is a block device, skipping target directory preparation", "target", config.TargetPath)
	} else if err := prepareTargetDir(config.TargetPath); err != nil {
		return err
	}
	if err := prepareDir(config.ScratchPath); err != nil {
		return fmt.Errorf("failed to prepare scratch dir: %w", err)
	}

	store, err := uploader.InitObjectStore(&config.ObjectStoreConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize object store: %w", err)
	}

	logger.Info("Object store initialized", "bucket", config.BSLBucket, "prefix", config.BSLPrefix)

	// Create K8s client early for progress reporting
	k8sClient, err := newKubeClient()
	if err != nil {
		return fmt.Errorf("failed to create K8s client: %w", err)
	}

	manifest, found, err := uploader.GetVMBackupManifest(
		store, config.VMNamespace, config.VMName, config.VeleroBackupName, config.BSLBucket, logger,
	)
	if err != nil {
		return fmt.Errorf("failed to get VM backup manifest: %w", err)
	}
	if !found {
		return fmt.Errorf("no backup manifest found for VM %s/%s in backup %s",
			config.VMNamespace, config.VMName, config.VeleroBackupName)
	}
	if len(manifest.CheckpointChain) == 0 {
		return fmt.Errorf("backup manifest for VM %s/%s in backup %s has an empty checkpoint chain",
			config.VMNamespace, config.VMName, config.VeleroBackupName)
	}

	vmIndex, found, err := uploader.GetVMIndex(store, config.VMNamespace, config.VMName, config.BSLBucket, logger)
	if err != nil {
		return fmt.Errorf("failed to get VM index: %w", err)
	}
	if !found {
		return fmt.Errorf("no VM index found for VM %s/%s", config.VMNamespace, config.VMName)
	}

	files, err := ResolveCheckpointFiles(
		config.VMNamespace, config.VMName, manifest.CheckpointChain, vmIndex, config.TargetVolume)
	if err != nil {
		return fmt.Errorf("failed to resolve checkpoint chain: %w", err)
	}

	logger.Info("Resolved checkpoint chain", "checkpoints", len(files))

	localPaths, err := downloadCheckpointFiles(ctx, store, config, files, k8sClient, logger)
	if err != nil {
		return fmt.Errorf("failed to download checkpoint files: %w", err)
	}

	logger.Info("Downloaded checkpoint files", "count", len(localPaths))
	defer func() {
		for _, path := range localPaths {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				logger.Error(err, "Failed to remove downloaded checkpoint file", "path", path)
			}
		}
	}()

	if err := rebaseChain(ctx, localPaths); err != nil {
		return fmt.Errorf("failed to rebase checkpoint chain: %w", err)
	}

	tip := localPaths[len(localPaths)-1]

	if config.TargetIsBlockDevice {
		// No atomic temp-file+rename publish for a device node: renaming a
		// regular file onto a device special-file path would just replace the
		// directory entry, not write bytes onto the device, and the device
		// node provides no atomicity of its own to preserve. Write directly.
		if err := flattenToRaw(ctx, tip, config.TargetPath, true); err != nil {
			return fmt.Errorf("failed to flatten checkpoint chain to raw: %w", err)
		}
	} else {
		tmpFile, err := os.CreateTemp(filepath.Dir(config.TargetPath), "."+filepath.Base(config.TargetPath)+".tmp-*")
		if err != nil {
			return fmt.Errorf("failed to create temporary target: %w", err)
		}
		tmpTargetPath := tmpFile.Name()
		if err := tmpFile.Close(); err != nil {
			_ = os.Remove(tmpTargetPath)
			return fmt.Errorf("failed to create temporary target: %w", err)
		}
		// Best-effort: after a successful rename below this is already gone, so
		// the removal here is a no-op that only matters on the error paths.
		defer func() { _ = os.Remove(tmpTargetPath) }()

		if err := flattenToRaw(ctx, tip, tmpTargetPath, false); err != nil {
			return fmt.Errorf("failed to flatten checkpoint chain to raw: %w", err)
		}
		if err := os.Rename(tmpTargetPath, config.TargetPath); err != nil {
			return fmt.Errorf("failed to publish restored disk image: %w", err)
		}
	}

	logger.Info("Download completed successfully", "target", config.TargetPath)

	return nil
}

// prepareTargetDir ensures the target file's parent directory exists and is
// restricted to owner-only access.
func prepareTargetDir(targetPath string) error {
	if !filepath.IsAbs(targetPath) {
		return fmt.Errorf("target path %q must be an absolute path", targetPath)
	}
	if err := prepareDir(filepath.Dir(targetPath)); err != nil {
		return fmt.Errorf("failed to prepare target dir for %q: %w", targetPath, err)
	}
	return nil
}

// prepareDir ensures dir exists and is restricted to owner-only access.
// os.MkdirAll alone won't tighten permissions on a directory that already
// existed (e.g. a pre-mounted /restore-data or /scratch), so a permissive
// pre-existing directory is chmod'd explicitly. Refuses to operate on the
// filesystem root, since chmod'ing "/" to 0700 would lock the pod itself out
// of most of its own filesystem.
func prepareDir(dir string) error {
	if dir == "/" {
		return fmt.Errorf("path %q must not resolve to the filesystem root", dir)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("failed to create dir %q: %w", dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("failed to harden dir %q permissions: %w", dir, err)
	}
	return nil
}

// LoadConfigFromEnv parses environment variables into DownloaderConfig.
func LoadConfigFromEnv() (*DownloaderConfig, error) {
	config := &DownloaderConfig{
		PodName:      os.Getenv(EnvPodName),
		PodNamespace: os.Getenv(EnvPodNamespace),
		ObjectStoreConfig: common.ObjectStoreConfig{
			BSLProvider:                    os.Getenv(common.EnvBSLProvider),
			BSLBucket:                      os.Getenv(common.EnvBSLBucket),
			BSLPrefix:                      os.Getenv(common.EnvBSLPrefix),
			BSLRegion:                      os.Getenv(common.EnvBSLRegion),
			BSLS3URL:                       os.Getenv(common.EnvBSLS3URL),
			BSLS3ForcePathStyle:            common.ParseBool(os.Getenv(common.EnvBSLS3ForcePathStyle)),
			BSLInsecureSkipTLSVerify:       common.ParseBool(os.Getenv(common.EnvBSLInsecureSkipTLSVerify)),
			BSLCACert:                      os.Getenv(common.EnvBSLCACert),
			BSLServerSideEncryption:        os.Getenv(common.EnvBSLServerSideEncryption),
			BSLKMSKeyID:                    os.Getenv(common.EnvBSLKMSKeyID),
			BSLChecksumAlgorithm:           os.Getenv(common.EnvBSLChecksumAlgorithm),
			BSLCustomerKeyEncryptionFile:   os.Getenv(common.EnvBSLCustomerKeyEncryptionFile),
			BSLProfile:                     os.Getenv(common.EnvBSLProfile),
			BSLServiceAccount:              os.Getenv(common.EnvBSLServiceAccount),
			BSLResourceGroup:               os.Getenv(common.EnvBSLResourceGroup),
			BSLStorageAccount:              os.Getenv(common.EnvBSLStorageAccount),
			BSLStorageAccountKeyEnvVar:     os.Getenv(common.EnvBSLStorageAccountKeyEnvVar),
			BSLStorageAccountURI:           os.Getenv(common.EnvBSLStorageAccountURI),
			BSLSubscriptionID:              os.Getenv(common.EnvBSLSubscriptionID),
			BSLUseAAD:                      common.ParseBool(os.Getenv(common.EnvBSLUseAAD)),
			BSLActiveDirectoryAuthorityURI: os.Getenv(common.EnvBSLActiveDirectoryAuthorityURI),
			CredentialsFile:                os.Getenv(common.EnvCredentialsFile),
		},
		VMName:              os.Getenv(EnvVMName),
		VMNamespace:         os.Getenv(EnvVMNamespace),
		VeleroBackupName:    os.Getenv(EnvVeleroBackupName),
		DataDownloadName:    os.Getenv(EnvDataDownloadName),
		DataDownloadUID:     os.Getenv(EnvDataDownloadUID),
		TargetVolume:        os.Getenv(EnvTargetVolume),
		TargetPath:          os.Getenv(EnvTargetPath),
		TargetIsBlockDevice: common.ParseBool(os.Getenv(EnvTargetIsBlockDevice)),
		ScratchPath:         os.Getenv(EnvScratchPath),
	}

	if config.TargetIsBlockDevice {
		// No sensible default for a raw device path -- the controller must
		// provide the exact volumeDevice path it configured on the pod.
		if config.TargetPath == "" {
			return nil, fmt.Errorf("%s is required when %s is true", EnvTargetPath, EnvTargetIsBlockDevice)
		}
		if !filepath.IsAbs(config.TargetPath) {
			return nil, fmt.Errorf("%s must be an absolute path", EnvTargetPath)
		}
	} else {
		if config.TargetPath == "" {
			config.TargetPath = DefaultTargetPath
		}
		if !filepath.IsAbs(config.TargetPath) {
			return nil, fmt.Errorf("%s must be an absolute path", EnvTargetPath)
		}
		if filepath.Dir(config.TargetPath) == "/" {
			return nil, fmt.Errorf("%s must not resolve to the filesystem root", EnvTargetPath)
		}
	}
	if config.ScratchPath == "" {
		config.ScratchPath = DefaultScratchPath
	}
	if !filepath.IsAbs(config.ScratchPath) {
		return nil, fmt.Errorf("%s must be an absolute path", EnvScratchPath)
	}
	if config.ScratchPath == "/" {
		return nil, fmt.Errorf("%s must not resolve to the filesystem root", EnvScratchPath)
	}
	if config.CredentialsFile == "" {
		config.CredentialsFile = common.DefaultCredentialsPath
	}

	if config.BSLProvider == "" {
		return nil, fmt.Errorf("%s is required", common.EnvBSLProvider)
	}
	if config.BSLBucket == "" {
		return nil, fmt.Errorf("%s is required", common.EnvBSLBucket)
	}
	if config.VMName == "" {
		return nil, fmt.Errorf("%s is required", EnvVMName)
	}
	if config.VMNamespace == "" {
		return nil, fmt.Errorf("%s is required", EnvVMNamespace)
	}
	if config.VeleroBackupName == "" {
		return nil, fmt.Errorf("%s is required", EnvVeleroBackupName)
	}
	if config.TargetVolume == "" {
		return nil, fmt.Errorf("%s is required", EnvTargetVolume)
	}

	return config, nil
}
