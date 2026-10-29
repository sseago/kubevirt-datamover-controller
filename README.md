# KubeVirt Datamover Controller

This project provides a Kubernetes controller for handling incremental qcow2-based VM backups using KubeVirt/libvirt Changed Block Tracking (CBT) instead of CSI snapshots.

> **Looking for more detail?** See [`docs/architecture.md`](docs/architecture.md) for a full
> walkthrough of the reconciliation phases, the checkpoint chain, and the object storage
> layout, and [`docs/testing.md`](docs/testing.md) for how to run this repo's tests and set up
> an end-to-end backup/restore environment. For end-user configuration and workflows, see the
> [OADP operator's KubeVirt datamover documentation](https://github.com/openshift/oadp-operator/tree/oadp-dev/docs/kubevirt-datamover).

## Current Implementation Status

The controller implements the full backup and restore flow end to end:

- Watches Velero `DataUpload`/`DataDownload` CRs and only acts on ones where
  `spec.datamover == "kubevirt"`.
- Phase-based reconciliation for both backup (`New -> Accepted -> Prepared -> InProgress ->
  Completed`) and restore, including cancellation support.
- Extracts the source VM reference from `DataUpload`/`DataDownload` annotations. For backup, it
  validates the VM is running with CBT enabled and creates/reuses `VirtualMachineBackupTracker`
  (VMBT) and `VirtualMachineBackup` (VMB) CRs to drive KubeVirt's native CBT backup; restore does
  not perform this validation or create VMB/VMBT objects, it resolves the checkpoint chain from
  object storage instead.
- Launches short-lived datamover pods that upload qcow2 files to the `BackupStorageLocation`
  (BSL) and maintain a per-VM checkpoint index for incremental backups, and downloader pods
  that reconstruct a VM disk from a checkpoint chain on restore.
- Supports AWS S3/S3-compatible, Azure Blob, and GCP Cloud Storage BSLs, including STS and
  workload-identity based credentials.

See [`docs/architecture.md`](docs/architecture.md) for the full design.

## Design Overview

This controller enables **incremental qcow2-based VM backups** using KubeVirt/libvirt tooling instead of CSI snapshots:

| Aspect | CSI Approach | KubeVirt qcow2 Approach |
|--------|--------------|-------------------------|
| Layer | Storage (CSI driver) | Hypervisor (QEMU/libvirt) |
| Snapshot mechanism | CSI VolumeSnapshot | VirtualMachineBackup CR |
| Incremental | Kopia deduplication (scans whole volume) | True block-level CBT (only changed blocks) |
| Data mover | Velero node-agent + kopia | This controller + qemu-img |
| VM awareness | None (just sees PVC) | Full (knows it's a VM disk) |

For full design details, see the [OADP KubeVirt Datamover Design Document](https://github.com/openshift/oadp-operator/blob/oadp-dev/docs/design/kubevirt-datamover.md).

## Progress Reporting

The controller reports real-time progress on `DataUpload` and `DataDownload` CRs. Because the KubeVirt datamover transfers `.qcow2` files rather than scanning filesystems like Velero's built-in Kopia datamover, the progress fields represent slightly different metrics:

**DataUpload (Backup)**
- `Status.Progress.TotalBytes`: The raw capacity of the source PVCs being backed up.
- `Status.IncrementalBytes`: The actual size of the `.qcow2` payload being transferred to S3. For incremental backups, this will be significantly smaller than `TotalBytes`, reflecting the efficiency of CBT.
- `Status.Progress.BytesDone`: The number of `.qcow2` bytes successfully uploaded to S3 so far.

**DataDownload (Restore)**
- `Status.Progress.TotalBytes`: The total size of all `.qcow2` files in the checkpoint chain that need to be downloaded from S3.
- `Status.Progress.BytesDone`: The number of `.qcow2` bytes successfully downloaded so far.
- *(Note: `IncrementalBytes` is not populated during restore, as the entire chain must be downloaded to reconstruct the flat raw disk).*

*Quirk:* During a restore, after `BytesDone` reaches `TotalBytes` (100%), the progress bar will pause while the downloader pod runs `qemu-img rebase` and `qemu-img commit` to flatten the chain into a raw image. This reconstruction phase can take time, but the progress will remain at 100% until it finishes.

## Prerequisites

- OpenShift cluster with OADP operator installed
- KubeVirt with Changed Block Tracking (CBT) enabled
- Virtual machines with `status.ChangedBlockTracking: Enabled`
- oc CLI configured to access the cluster

## Deployment and Testing

### 1. Verify OADP Setup

Before deploying the controller, ensure OADP is properly configured:

```bash
# Check OADP operator is installed
oc get csv -n openshift-adp | grep oadp

# Verify DataProtectionApplication (DPA) is configured
oc get dpa -n openshift-adp

# Check BackupStorageLocation is ready
oc get bsl -n openshift-adp
```

### 2. Deploy the Controller

#### Option A: Run Locally (Development)
```bash
# Install CRDs to the cluster (if any)
make install

# Run controller locally (recommended for testing)
make run
```

#### Option B: Deploy to OpenShift Cluster
```bash
# Build and deploy the controller
make docker-build docker-push IMG=<your-registry>/kubevirt-datamover-controller:latest
make deploy IMG=<your-registry>/kubevirt-datamover-controller:latest

# Check deployment status
oc get pods -n openshift-adp
```

#### Option C: Deploy to ttl.sh (Temporary Testing)
```bash
# Build for amd64 and push to ttl.sh (expires in 1 hour)
docker build --platform linux/amd64 -t ttl.sh/kubevirt-datamover-controller:1h .
docker push ttl.sh/kubevirt-datamover-controller:1h

# Deploy using the ttl.sh image
make deploy IMG=ttl.sh/kubevirt-datamover-controller:1h

# Check deployment
oc get pods -n openshift-adp
```

### 3. Testing with DataUpload Resources

The controller watches Velero `DataUpload`/`DataDownload` resources where
`spec.datamover: kubevirt`. In normal operation these are created by the
`kubevirt-datamover-plugin` when Velero processes a VM backup with a matching `VolumePolicy`
(see [`docs/architecture.md`](docs/architecture.md) for how that path works end to end and
[`docs/testing.md`](docs/testing.md) for a full manual backup/restore walkthrough). A
`DataUpload` also needs the `kubevirt-datamover.io/vm-name` (and optionally
`kubevirt-datamover.io/vm-namespace`) annotation set so the controller knows which VM to back
up; the plugin sets these automatically.

#### Monitor Controller Activity

```bash
# Watch controller logs
oc logs -f -n openshift-adp deployment/kubevirt-datamover-controller-manager

# Watch DataUpload status
oc get datauploads -n openshift-adp -w

# Check DataUpload details
oc get dataupload <name> -n openshift-adp -o yaml
```

### 4. Configuration Options

The controller supports the following CLI flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--metrics-bind-address` | `0` | Address for metrics endpoint (`:8443` for HTTPS, `:8080` for HTTP, or `0` to disable) |
| `--health-probe-bind-address` | `:8081` | Address for health probe endpoint |
| `--leader-elect` | `false` | Enable leader election for HA |
| `--metrics-secure` | `true` | Serve metrics via HTTPS |
| `--max-concurrent-reconciles` | `3` | Maximum concurrent reconciles for the DataUpload and DataDownload controllers |
| `--max-concurrent-data-movers` | `3` | Maximum number of active DataUploads or DataDownloads (per controller) allowed concurrently. Set `0` for unlimited concurrency. |
| `--max-incremental-backups` | `0` (unlimited) | Maximum number of incremental backups per VM before forcing a full backup |
| `--stale-dataupload-threshold` | `2h` | Duration after which a stale DataUpload stops blocking younger ones for the same VM |
| `--datamover-image` | `quay.io/konveyor/kubevirt-datamover-controller:latest` | Image used for datamover pods |
| `--datamover-image-pull-policy` | `Always` | Image pull policy for datamover pods |
| `--oadp-namespace` | `openshift-adp` | Namespace where OADP/Velero resources are located |

#### Quiesce Behavior

Before creating the `VirtualMachineBackup`, the controller decides whether KubeVirt should
quiesce (freeze) the guest filesystem for an application-consistent backup, or skip quiescing
for a crash-consistent backup:

- **Default (automatic detection)**: the controller inspects the VM's `VirtualMachineInstance`
  status and quiesces only when `status.conditions[type=AgentConnected].status == "True"`. VMs
  without a connected QEMU guest agent (e.g. Cirros test VMs) skip quiesce automatically, instead
  of failing/warning on every backup.
- **Explicit override**: set the `kubevirt-datamover.io/skipQuiesce` annotation on the
  `DataUpload` to override the automatic decision:
  - `"true"` forces a crash-consistent backup (skip quiesce), even if the guest agent is
    detected as connected.
  - `"false"` forces a quiesced (application-consistent) backup, even if the guest agent
    isn't connected.
  - Any other value, or leaving the annotation unset, falls back to automatic detection.

The override annotation always takes precedence over automatic detection.

### 5. Troubleshooting

#### Controller Pod Not Starting
```bash
# Check pod status
oc describe pod -n openshift-adp -l control-plane=controller-manager

# Check events
oc get events -n openshift-adp --sort-by='.lastTimestamp'
```

#### DataUpload Not Being Processed
```bash
# Verify datamover field is set correctly (lowercase!)
oc get dataupload <name> -n openshift-adp -o jsonpath='{.spec.datamover}'

# Check controller is watching
oc logs -n openshift-adp deployment/kubevirt-datamover-controller-manager | grep -i kubevirt
```

#### Architecture Mismatch (Exec Format Error)
If running on an amd64 cluster but built on arm64 Mac:
```bash
# Rebuild with correct platform
docker build --platform linux/amd64 -t <image> .
docker push <image>

# Use unique tag to avoid cached images
docker build --platform linux/amd64 -t ttl.sh/kubevirt-datamover-controller:amd64-$(date +%s) .
```

### 6. Development Commands

```bash
# Run tests
make test

# Build locally
make build

# Generate manifests after API changes
make manifests generate

# Format and lint code
make fmt vet lint

# Run locally against cluster
make run
```

## Kubebuilder

The project was generated using kubebuilder version `v4.11.0`, running the following commands:
```sh
kubebuilder init \
    --plugins go.kubebuilder.io/v4 \
    --project-version 3 \
    --project-name=kubevirt-datamover-controller \
    --repo=github.com/migtools/kubevirt-datamover-controller \
    --domain=openshift.io

# Note: This controller watches Velero's DataUpload CRD rather than defining its own
```

## Documentation

- [`docs/architecture.md`](docs/architecture.md): a developer-focused walkthrough of the
  reconciliation phases, the checkpoint chain design, and object storage layout.
- [`docs/testing.md`](docs/testing.md): running this repo's automated tests and setting up an
  end-to-end backup/restore environment.
- [kubevirt-datamover-plugin](https://github.com/migtools/kubevirt-datamover-plugin): the
  companion Velero plugin that creates the `DataUpload`/`DataDownload` CRs this controller
  reconciles.
- [OADP operator KubeVirt datamover docs](https://github.com/openshift/oadp-operator/tree/oadp-dev/docs/kubevirt-datamover):
  end-user configuration, backup/restore workflows, and troubleshooting.
- [OADP KubeVirt Datamover design document](https://github.com/openshift/oadp-operator/blob/oadp-dev/docs/design/kubevirt-datamover.md):
  the original design proposal.

## Related Resources

- [OADP Operator](https://github.com/openshift/oadp-operator)
- [KubeVirt Incremental Backup VEP](https://github.com/kubevirt/enhancements/blob/main/veps/sig-storage/incremental-backup.md)
- [Velero](https://velero.io/)
- [OADP Project Board](https://github.com/orgs/migtools/projects/7/views/20)
