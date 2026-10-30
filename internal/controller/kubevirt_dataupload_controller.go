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

// Package controller implements the KubeVirt DataMover controller
package controller

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	// DefaultMaxConcurrentReconciles is the default number of concurrent reconciles
	DefaultMaxConcurrentReconciles = 3

	// DefaultTempPVCSize is the default size for temporary backup PVC
	DefaultTempPVCSize = "10Gi"

	// RequeueAfterShort is the short requeue duration for polling
	RequeueAfterShort = 5 * time.Second

	// RequeueAfterLong is the longer requeue duration
	RequeueAfterLong = 30 * time.Second

	// bslValidatedValue is the annotation value indicating BSL validation is complete
	bslValidatedValue = "true"

	// AnnotationVMBTName is the annotation key for the generated VMBT name on a DataUpload
	AnnotationVMBTName = "kubevirt-datamover.io/vmbt-name"

	// k8sGenerateNameRandomLen is the number of random characters K8s appends to GenerateName
	k8sGenerateNameRandomLen = 5

	// maxVMBNameLen is the max allowed VMB name length. KubeVirt constructs a
	// hotplug volume name as "<vmb-name>-backup-target-pvc" which must be a
	// valid DNS label (≤ 63 chars). Reserve 18 chars for that suffix.
	maxVMBNameLen = 63 - len("-backup-target-pvc") // = 45
)

// KubeVirtDataUploadReconciler reconciles DataUpload objects where Spec.DataMover is "kubevirt"
type KubeVirtDataUploadReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger

	// APIReader is an uncached client, used as a fallback when the cached client's
	// List returns no results for a child resource this controller expects to
	// exist -- the informer cache is only eventually consistent, so a resource
	// created moments ago in an earlier reconcile may not yet be visible to it.
	// May be nil (falls back to cached-only lookups), so tests that don't wire
	// one keep working.
	APIReader client.Reader

	// OADPNamespace is the namespace where OADP and Velero resources are located
	OADPNamespace string

	// MaxConcurrentReconciles is the maximum number of concurrent Reconciles which can be run
	MaxConcurrentReconciles int

	// MaxConcurrentDataMovers caps how many DataUploads may be in an active
	// phase (Accepted/Prepared/InProgress) at once, gating pod creation in
	// handlePrepared -- covers the full resource window (backup PVC + pod),
	// not just running pods. 0 (default) disables the limit.
	MaxConcurrentDataMovers int

	// DatamoverImage is the image to use for datamover pods
	DatamoverImage string

	// DatamoverImagePullPolicy is the pull policy for the datamover image
	DatamoverImagePullPolicy corev1.PullPolicy

	// MaxIncrementalBackups is the maximum number of incremental backups per VM
	// before forcing a full backup. 0 means unlimited.
	MaxIncrementalBackups int

	// StaleDataUploadThreshold is the duration after which a DataUpload in an
	// active phase is considered stale and will no longer block younger
	// DataUploads for the same VM.
	StaleDataUploadThreshold time.Duration

	// ObjectStoreFactory creates an ObjectStore from an ObjectStoreConfig.
	// Defaults to uploader.InitObjectStore if nil. Override in tests to inject mocks.
	ObjectStoreFactory func(cfg *common.ObjectStoreConfig) (velero.ObjectStore, error)

	// PodLogCollector reads logs from a completed datamover pod.
	// If nil, pod log collection is skipped. Override in tests to inject mocks.
	PodLogCollector func(ctx context.Context, podName, podNamespace string) (string, error)
}

// +kubebuilder:rbac:groups=velero.io,resources=datauploads,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=velero.io,resources=datauploads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=velero.io,resources=backupstoragelocations,verbs=get;list;watch
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackups/status,verbs=get
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackuptrackers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackuptrackers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines,verbs=get;list;watch
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachineinstances,verbs=get;list;watch

// Reconcile handles DataUpload resources where Spec.DataMover is "kubevirt"
func (r *KubeVirtDataUploadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the DataUpload
	dataUpload := &velerov2alpha1.DataUpload{}
	if err := r.Get(ctx, req.NamespacedName, dataUpload); err != nil {
		// Ignore not-found errors, as the object may have been deleted
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Skip if DataMover is not "kubevirt"
	if dataUpload.Spec.DataMover != common.DataMoverKubeVirt {
		logger.V(1).Info("Skipping DataUpload - DataMover is not kubevirt",
			"dataUpload", req.NamespacedName,
			"dataMover", dataUpload.Spec.DataMover)
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling DataUpload with kubevirt datamover",
		"dataUpload", req.NamespacedName,
		"phase", dataUpload.Status.Phase)

	// Bound how long a DataUpload may sit in a non-terminal phase after being
	// Accepted: without this, any of the several unbounded-requeue branches below
	// (waiting on VMB status, waiting on the datamover pod, etc.) would retry
	// forever instead of eventually failing per Spec.OperationTimeout.
	//
	// A datamover pod that already reported success (AnnotationDatamoverPodSucceeded)
	// has already committed the uploaded data and the S3 index -- only local
	// PVC/PV cleanup remains, handled by handleInProgress's own requeue loop.
	// Failing on the deadline here would report an already-successful backup as
	// Failed purely because that trailing cleanup took longer than
	// Spec.OperationTimeout, so skip the timeout check entirely once the pod has
	// succeeded.
	timeoutBound := isDataUploadTimeoutBound(dataUpload.Status.Phase) &&
		dataUpload.Annotations[common.AnnotationDatamoverPodSucceeded] != bslValidatedValue
	if timeoutBound {
		if failed, err := r.checkOperationTimeout(ctx, logger, dataUpload); err != nil {
			if stderrors.Is(err, ErrPodsStillTerminating) {
				// Expected, self-resolving: kubelet just hasn't finished tearing the
				// stalled pod down yet. Requeue quickly without logging a reconcile
				// error or triggering controller-runtime's exponential backoff for
				// something that isn't wrong -- matches handleCanceling's treatment
				// of the same error. The DataUpload isn't marked Failed yet; the next
				// reconcile re-enters this same timeout check and retries the fail
				// callback (including re-persisting Failed) once cleanup succeeds.
				logger.V(1).Info("Datamover pod(s) still terminating during timeout cleanup, will retry", "error", err)
				return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
			}
			return ctrl.Result{}, err
		} else if failed {
			return ctrl.Result{}, nil
		}
	}

	// Handle based on current phase
	var result ctrl.Result
	var err error
	switch dataUpload.Status.Phase {
	case "", velerov2alpha1.DataUploadPhaseNew:
		result, err = r.handleNew(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhaseAccepted:
		result, err = r.handleAccepted(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhasePrepared:
		result, err = r.handlePrepared(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhaseInProgress:
		result, err = r.handleInProgress(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhaseCanceling:
		return r.handleCanceling(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhaseCompleted,
		velerov2alpha1.DataUploadPhaseFailed,
		velerov2alpha1.DataUploadPhaseCanceled:
		// Terminal states - nothing to do
		logger.V(1).Info("DataUpload is in terminal state", "phase", dataUpload.Status.Phase)
		return ctrl.Result{}, nil

	default:
		logger.Info("Unknown DataUpload phase", "phase", dataUpload.Status.Phase)
		return ctrl.Result{}, nil
	}

	// Cap the handler's RequeueAfter to the operation deadline. The condition keys off
	// dataUpload.Status.AcceptedTimestamp (rather than the pre-dispatch timeoutBound)
	// so a New DataUpload that handleNew just transitioned to Accepted in this same
	// reconcile -- setting AcceptedTimestamp along the way -- gets its first
	// RequeueAfter capped too, not just subsequent reconciles.
	if err == nil && dataUpload.Status.AcceptedTimestamp != nil {
		result = capRequeueToOperationDeadline(result, dataUpload.Status.AcceptedTimestamp, dataUpload.Spec.OperationTimeout.Duration)
	}
	return result, err
}

// handleNew processes DataUploads in New phase
// Validates prerequisites and transitions to Accepted
func (r *KubeVirtDataUploadReconciler) handleNew(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling New phase DataUpload")

	// Step 1: Validate VM annotation exists
	vmRef, err := common.GetVMReference(du)
	if err != nil {
		logger.Error(err, "Failed to get VM reference from DataUpload")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Missing VM reference: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	logger.Info("Found VM reference", "vmName", vmRef.Name, "vmNamespace", vmRef.Namespace)

	// Step 2: Fetch the VirtualMachine and validate prerequisites
	vm := &kubevirtcorev1.VirtualMachine{}
	if err := r.Get(ctx, types.NamespacedName{Name: vmRef.Name, Namespace: vmRef.Namespace}, vm); err != nil {
		if errors.IsNotFound(err) {
			logger.Error(err, "VirtualMachine not found")
			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("VirtualMachine %s/%s not found", vmRef.Namespace, vmRef.Name)); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get VirtualMachine: %w", err)
	}

	// Step 3: Validate VM is running and CBT is enabled
	if err := common.ValidateVMForBackup(vm); err != nil {
		logger.Error(err, "VM validation failed")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, err.Error()); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	logger.Info("VM validation passed", "vmName", vmRef.Name, "vmNamespace", vmRef.Namespace)

	// Record when this DataUpload was accepted so checkOperationTimeout can bound
	// how long it's allowed to remain non-terminal against Spec.OperationTimeout.
	now := metav1.Now()
	du.Status.AcceptedTimestamp = &now

	// Transition to Accepted phase
	if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseAccepted, "DataUpload accepted by kubevirt datamover"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// isDataUploadTimeoutBound reports whether phase is one of the non-terminal
// phases (Accepted, Prepared, InProgress) subject to Spec.OperationTimeout
// enforcement. Kept as the single source of truth for that phase set so a
// future phase added to the dispatch switch below can't silently drift out of
// sync with which phases get timeout-checked.
func isDataUploadTimeoutBound(phase velerov2alpha1.DataUploadPhase) bool {
	switch phase {
	case velerov2alpha1.DataUploadPhaseAccepted,
		velerov2alpha1.DataUploadPhasePrepared,
		velerov2alpha1.DataUploadPhaseInProgress:
		return true
	default:
		return false
	}
}

// checkOperationTimeout fails du if too much time has elapsed since it was
// accepted, per Spec.OperationTimeout (falling back to DefaultOperationTimeout
// when unset). Self-heals a missing AcceptedTimestamp -- e.g. a DataUpload
// already past New when this check was introduced -- by backfilling it to now
// rather than leaving the operation unbounded forever. A thin adapter over
// checkOperationTimeoutCore (in helpers.go), shared with DataDownload's own
// checkOperationTimeout: DataUpload and DataDownload are distinct vendored
// Velero types with no common interface to dispatch the backfill / exceeded-check
// / failure-message logic on directly, so each controller adapts via accessors
// instead of duplicating that logic.
func (r *KubeVirtDataUploadReconciler) checkOperationTimeout(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (failed bool, err error) {
	return checkOperationTimeoutCore(ctx, logger, "DataUpload", operationTimeoutTarget{
		acceptedTimestamp:    func() *metav1.Time { return du.Status.AcceptedTimestamp },
		setAcceptedTimestamp: func(t *metav1.Time) { du.Status.AcceptedTimestamp = t },
		operationTimeout:     du.Spec.OperationTimeout.Duration,
		phase:                func() string { return string(du.Status.Phase) },
		persist:              func(ctx context.Context) error { return r.Update(ctx, du) },
		fail: func(ctx context.Context, message string) error {
			// Capture the stalled pod's logs before deleting it -- on a timeout the
			// pod is usually still running, and its logs are the only evidence of
			// why it stalled. Best-effort: a lookup failure here shouldn't block
			// the actual cleanup/fail below.
			if pod, findErr := r.findPodForDataUpload(ctx, du, r.getPodNamespace(du)); findErr == nil && pod != nil {
				r.emitPodLogs(ctx, logger, pod)
			}
			// Stop the still-running datamover pod BEFORE persisting Failed, and
			// propagate a cleanup failure rather than swallowing it: a timeout can
			// fire while the pod is still Pending/Running (that's exactly the
			// unbounded-wait branch this timeout guards against), unlike the other
			// Failed paths where the pod has already terminated on its own. Failed
			// is a dead-end terminal state with no further reconciliation, so
			// persisting it before cleanup actually succeeds would leave the pod
			// running forever with no chance to retry -- returning the error here
			// instead lets the reconcile retry until cleanup succeeds.
			if cleanupNotReady, terminating := cleanupPodsByUID(ctx, r.Client, r.APIReader, common.LabelDataUploadUID, string(du.UID), r.getPodNamespace(du), logger); cleanupNotReady {
				if terminating {
					return fmt.Errorf("datamover pod still terminating before failing DataUpload on timeout: %w", ErrPodsStillTerminating)
				}
				return fmt.Errorf("datamover pod cleanup could not be confirmed before failing DataUpload on timeout")
			}
			return r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, message)
		},
	})
}

// handleAccepted processes DataUploads in Accepted phase
// Creates VMBT/VMB and transitions to Prepared when ready
//
//nolint:gocyclo // Phase handler with necessary validation steps
func (r *KubeVirtDataUploadReconciler) handleAccepted(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling Accepted phase DataUpload")

	// Extract VirtualMachine reference from annotation
	vmRef, err := common.GetVMReference(du)
	if err != nil {
		logger.Error(err, "Failed to get VM reference")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Missing VM reference: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Serialize per VM: only the oldest DataUpload for a given VM proceeds.
	// All younger DUs wait until the older one reaches a terminal phase (Completed/Failed).
	// This ensures the previous backup's checkpoint is uploaded to S3 before the
	// next backup starts, enabling incremental backups.
	shouldWait, blockingDU, err := r.hasOlderActiveDUForVM(ctx, du)
	if err != nil {
		return ctrl.Result{}, err
	}
	if shouldWait {
		logger.Info("Another DataUpload is still active for this VM, waiting",
			"vm", vmRef.Name, "blockingDU", blockingDU)
		return ctrl.Result{RequeueAfter: RequeueAfterLong}, nil
	}

	// Step 0: Verify BSL exists and is Available before creating any resources.
	// BSL must be in Available phase — if it's not, fail immediately rather than
	// creating resources (PVC, VMBT, VMB) that will be wasted.
	if du.Spec.BackupStorageLocation != "" {
		bslObj, err := r.getBackupStorageLocationForDU(ctx, du)
		if err != nil {
			if isTransientBSLLookupError(err, du.Spec.BackupStorageLocation) {
				return ctrl.Result{}, fmt.Errorf("failed to get BackupStorageLocation: %w", err)
			}
			logger.Error(err, "BackupStorageLocation not accessible")
			if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("BackupStorageLocation not accessible: %v", err)); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, nil
		}
		if bslObj.Status.Phase != velerov1.BackupStorageLocationPhaseAvailable {
			logger.Error(nil, "BackupStorageLocation is not in Available phase",
				"bsl", bslObj.Name, "phase", bslObj.Status.Phase)
			if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("BackupStorageLocation %q is not available (phase: %s)",
					bslObj.Name, bslObj.Status.Phase)); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, nil
		}
	}

	// Step 1: Create or get temporary PVC for backup output
	pvc, err := r.ensureTempPVC(ctx, logger, du, vmRef.Namespace)
	if err != nil {
		// If a referenced PVC is permanently missing, fail the DataUpload
		// instead of retrying forever. errors.IsNotFound matches wrapped errors.
		if errors.IsNotFound(err) {
			logger.Error(err, "PVC not found, failing DataUpload")
			if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("Failed to create backup PVC: %v", err)); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to ensure temporary PVC")
		return ctrl.Result{}, err
	}
	logger.Info("Temporary PVC ready", "pvc", pvc.Name)

	// Check if VMB already exists. If it does, the VMBT is already in the right
	// state (set before VMB creation) and we can skip straight to monitoring.
	// This avoids deleting/recreating the VMBT while an active VMB references it.
	vmb, err := r.findVMBForDataUpload(ctx, du, vmRef.Namespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check for existing VMB: %w", err)
	}

	// NOTE: an earlier version of this function short-circuited here (before Step
	// 2 below) whenever du.Annotations[AnnotationVMBTName] was already set but
	// findVMBForDataUpload came back nil, on the theory that a VMB created by a
	// prior reconcile just hadn't reached the informer cache yet -- requeuing
	// briefly to "let the cache catch up" rather than re-running Step 2.
	//
	// That's no longer the right call, for two reasons:
	//   1. findVMBForDataUpload itself already falls back to an uncached
	//      APIReader read when the cached client finds nothing (see its own
	//      doc comment) -- a genuine cache-lag false negative is resolved
	//      inside that call, before this point. Reaching here with vmb == nil
	//      means no VMB exists even by a live read, not merely an invisible one.
	//   2. prepareVMBackupTracker (Step 2) no longer deletes an existing VMBT --
	//      it lists by VM-name-hash label and reuses one if found (see its own
	//      comment), only creating fresh when none exists at all. The original
	//      guard's rationale ("prepareVMBackupTracker would delete the VMBT
	//      that the (not-yet-visible) VMB references") describes behavior this
	//      function no longer has.
	//
	// The real, reproduced-live failure mode this used to cause: Step 4 below
	// can defer VMB creation (KubeVirt's one-active-VMB-per-VM admission
	// webhook, "in progress for source", handled a few lines down) *after*
	// Step 2 has already persisted AnnotationVMBTName -- no VMB gets created
	// that reconcile. Every following reconcile then hit this guard (VMBTName
	// set, vmb still nil) and took the short-requeue-and-wait path forever,
	// since Step 4 -- the only code path that can ever create the VMB -- was
	// never reached again. The DataUpload logged "VMBT already prepared but
	// VMB not yet visible in cache, requeuing" every 5s indefinitely (bounded
	// only by Spec.OperationTimeout, e.g. 4h) even though no VMB would ever
	// appear on its own. Falling through to Step 2 instead retries Step 4 on
	// every reconcile until the conflicting VMB clears.
	if vmb == nil {
		// Step 2: Prepare VirtualMachineBackupTracker (recreate from S3 state)
		vmbt, err := r.prepareVMBackupTracker(ctx, logger, du, vmRef.Name, vmRef.Namespace)
		if err != nil {
			logger.Error(err, "Failed to prepare VirtualMachineBackupTracker")
			return ctrl.Result{}, err
		}
		logger.Info("VirtualMachineBackupTracker ready", "vmbt", vmbt.Name)

		// Store generated VMBT name in annotation for later stages
		if du.Annotations == nil {
			du.Annotations = make(map[string]string)
		}
		du.Annotations[AnnotationVMBTName] = vmbt.Name
		if err := r.Update(ctx, du); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to annotate DataUpload with VMBT name: %w", err)
		}

		// Step 3: Determine backup mode (full vs incremental).
		forceFullBackup, checkpointLookup := r.resolveBackupMode(ctx, logger, du, vmRef)

		// Step 3b: Cross-check VMBT checkpoint against BSL chain validation result.
		// The VMBT's LatestCheckpoint (restored from archived vmbt.json) must agree
		// with the BSL chain validation (which independently walks the S3 index).
		// If they diverge, the archived VMBT is stale — force a full backup.
		if !forceFullBackup && checkpointLookup != nil && checkpointLookup.Found {
			vmbtCheckpoint := ""
			if vmbt.Status != nil && vmbt.Status.LatestCheckpoint != nil {
				vmbtCheckpoint = vmbt.Status.LatestCheckpoint.Name
			}
			if vmbtCheckpoint != checkpointLookup.LatestCheckpoint {
				logger.Info("VMBT checkpoint does not match BSL chain validation result, forcing full backup",
					"vmbtCheckpoint", vmbtCheckpoint,
					"bslLatestCheckpoint", checkpointLookup.LatestCheckpoint)
				forceFullBackup = true
			}
		}

		// Step 3c: Record expected backup type for mismatch detection.
		// This annotation lets handlePrepared() compare the actual VMB result
		// against what the controller intended, and the datamover pod can
		// reconcile the S3 index if they differ (e.g., VM lost checkpoint).
		//
		// A merge patch (single API call, no read-modify-write) rather than
		// r.Update: Update requires du's in-memory resourceVersion to match the
		// server's, so it 409s the instant any concurrent writer touches this
		// object -- this repo's own README documents that Velero's built-in
		// DataUpload controller also reconciles these objects. A JSON merge
		// patch that names only this one annotation key carries no
		// resourceVersion precondition at all, so it can't conflict on a
		// concurrent change to any other field, and it merges rather than
		// overwrites, so a concurrent writer's own change survives alongside
		// it. That matters here specifically because Step 4 below creates the
		// VMB in this same reconcile call; once it exists, "vmb == nil" is
		// false on every future reconcile and this code never runs again, so
		// there is no second chance to persist this annotation if this one
		// write is lost.
		if checkpointLookup != nil {
			expectedType := uploader.BackupTypeFull
			if !forceFullBackup && checkpointLookup.Found && checkpointLookup.IsChainValid {
				expectedType = uploader.BackupTypeIncremental
			}
			original := du.DeepCopy()
			if du.Annotations == nil {
				du.Annotations = make(map[string]string)
			}
			du.Annotations[common.AnnotationExpectedBackupType] = expectedType
			if err := r.Patch(ctx, du, client.MergeFrom(original)); err != nil {
				logger.Info("Failed to set expected backup type annotation, will retry",
					"reason", err.Error())
			}
		}

		// Step 3d: Determine whether the backup should be quiesced
		// (application-consistent) or skip quiesce (crash-consistent).
		// See determineSkipQuiesce for the precedence between the explicit
		// user override annotation and automatic guest-agent detection.
		skipQuiesce := r.determineSkipQuiesce(ctx, logger, du, vmRef)

		// Step 4: Create VirtualMachineBackup
		var created bool
		vmb, created, err = r.ensureVMBackup(ctx, logger, du, vmbt, pvc.Name, vmRef.Namespace, forceFullBackup, skipQuiesce)
		if err != nil {
			// Check if the error is due to another VMB being in progress for the same VM.
			// KubeVirt's admission webhook only allows one active (non-terminal) VMB per VM.
			// Requeue with a longer delay instead of returning an error (which causes
			// exponential backoff retry storm).
			if strings.Contains(err.Error(), "in progress for source") {
				logger.Info("Another VirtualMachineBackup is in progress for this VM, will retry",
					"reason", err.Error())
				return ctrl.Result{RequeueAfter: RequeueAfterLong}, nil
			}
			logger.Error(err, "Failed to ensure VirtualMachineBackup")
			return ctrl.Result{}, err
		}

		if created {
			logger.Info("Created VirtualMachineBackup", "generateName", vmb.GenerateName, "namespace", vmRef.Namespace)
			// Requeue to check status
			return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
		}
	} else if vmb != nil {
		logger.V(1).Info("VirtualMachineBackup already exists, skipping VMBT preparation", "vmb", vmb.Name)
	}

	// Refresh vmb's status via an uncached read before deciding anything below.
	// The informer cache's own watch can intermittently miss or delay a
	// status-only update event for an object it already knows about --
	// confirmed live: virt-controller had already written
	// status.conditions[type=Done,status=True] to the API server, but this
	// controller's cached copy of the same VMB never reflected it, looping
	// "VirtualMachineBackup in progress, requeuing" until Spec.OperationTimeout
	// even though the backup had long since finished. Distinct from #211/#212's
	// fix (the VMB not existing in the cache at all): here the object is found
	// fine, just with a stale Status. A single-object Get is cheap enough to do
	// on every reconcile of this phase. Best-effort: if the read fails (or
	// r.APIReader isn't configured, as in some test fixtures), fall back to the
	// cached copy already in hand rather than failing the reconcile over a
	// freshness check.
	if r.APIReader != nil {
		liveVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
		if err := r.APIReader.Get(ctx, types.NamespacedName{Name: vmb.Name, Namespace: vmb.Namespace}, liveVMB); err == nil {
			var liveConditions []kubevirtbackupv1alpha1.Condition
			if liveVMB.Status != nil {
				liveConditions = liveVMB.Status.Conditions
			}
			logger.Info("Refreshed VirtualMachineBackup status via uncached read",
				"vmb", vmb.Name, "conditions", liveConditions)
			vmb.Status = liveVMB.Status
		} else if errors.IsNotFound(err) {
			logger.Info("VirtualMachineBackup not found via uncached read, using cached copy",
				"vmb", vmb.Name)
		} else {
			logger.Info("Failed to refresh VirtualMachineBackup status via uncached read, using cached copy",
				"vmb", vmb.Name, "reason", err.Error())
		}
	}

	// Step 5: Check VMB status
	if vmb.Status == nil {
		// Before blindly requeuing, check if the VMB's BackupTracker still exists.
		// If the VMBT was deleted (by a concurrent DataUpload), KubeVirt will never
		// set status on this VMB, so we'd requeue forever.
		vmbtName := vmb.Spec.Source.Name
		vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
		if err := r.Get(ctx, types.NamespacedName{Name: vmbtName, Namespace: vmb.Namespace}, vmbt); err != nil {
			if errors.IsNotFound(err) {
				logger.Error(nil, "VirtualMachineBackup's BackupTracker was deleted before processing started",
					"vmb", vmb.Name, "vmbt", vmbtName)
				if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
					fmt.Sprintf("VMBackup stuck: BackupTracker %s no longer exists", vmbtName)); updateErr != nil {
					return ctrl.Result{}, updateErr
				}
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, fmt.Errorf("failed to check BackupTracker %s existence: %w", vmbtName, err)
		}
		logger.Info("VirtualMachineBackup status not yet available, requeuing")
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	return r.evaluateVMBackupStatus(ctx, logger, du, vmb)
}

// conditionComplete is KubeVirt nightly (v1.20.0+) virt-controller's renamed
// replacement for VirtualMachineBackup's "Done" condition type. Our vendored
// kubevirt.io/api (v1.8.0-alpha.0) predates the rename and has no
// ConditionComplete constant of its own, so it is declared here as the raw
// string literal virt-controller actually emits. Without recognizing both
// names, evaluateVMBackupStatus/isVMBTerminal never see doneCond populated
// against a nightly cluster and loop "VirtualMachineBackup in progress,
// requeuing" until Spec.OperationTimeout, even though
// vmb.Status.Conditions already has Complete=True.
const conditionComplete kubevirtbackupv1alpha1.ConditionType = "Complete"

// evaluateVMBackupStatus checks conditions on a VirtualMachineBackup to determine
// whether the backup succeeded, failed, or is still running.
// KubeVirt condition combinations:
//
//	Progressing=True  + Done=False       → backup still running
//	Progressing=False + Done=True        → backup completed successfully
//	Progressing=False + Done=False       → backup failed
//	Initializing=True + Progressing=False + Done=absent → stuck (e.g. VMBT deleted)
func (r *KubeVirtDataUploadReconciler) evaluateVMBackupStatus(
	ctx context.Context, logger logr.Logger,
	du *velerov2alpha1.DataUpload, vmb *kubevirtbackupv1alpha1.VirtualMachineBackup,
) (ctrl.Result, error) {
	var doneCond, progressingCond, initializingCond *kubevirtbackupv1alpha1.Condition
	for i := range vmb.Status.Conditions {
		switch vmb.Status.Conditions[i].Type {
		case kubevirtbackupv1alpha1.ConditionDone, conditionComplete:
			doneCond = &vmb.Status.Conditions[i]
		case kubevirtbackupv1alpha1.ConditionProgressing:
			progressingCond = &vmb.Status.Conditions[i]
		case kubevirtbackupv1alpha1.ConditionInitializing:
			initializingCond = &vmb.Status.Conditions[i]
		}
	}

	if doneCond != nil && doneCond.Status == corev1.ConditionTrue {
		// Done=True can mean "finished with error" — KubeVirt sets Done=True together with
		// Progressing=False when the backup fails (e.g., "No space left on device"), but the
		// Reason is a descriptive string like "Backup has failed: <details>" rather than the
		// literal "Failed". Detect failure via a case-insensitive "failed" substring match on
		// either condition, and take the failure detail from whichever condition actually
		// carries it — never from a condition just because it happens to have non-empty text.
		var progressingCandidate *kubevirtbackupv1alpha1.Condition
		if progressingCond != nil && progressingCond.Status == corev1.ConditionFalse {
			progressingCandidate = progressingCond
		}
		if conditionIndicatesFailure(progressingCandidate) || conditionIndicatesFailure(doneCond) {
			reason, failureMessage := pickFailureDetail(progressingCandidate, doneCond)
			logger.Error(nil, "VirtualMachineBackup failed (Done=True with failure)",
				"vmb", vmb.Name, "reason", reason, "message", failureMessage)
			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("VMBackup failed: %s", failureMessage)); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}

		logger.Info("VirtualMachineBackup completed",
			"vmb", vmb.Name,
			"type", vmb.Status.Type,
			"checkpoint", vmb.Status.CheckpointName)

		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhasePrepared,
			fmt.Sprintf("VMBackup completed (type=%s)", vmb.Status.Type)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	if doneCond != nil && doneCond.Status == corev1.ConditionFalse {
		if progressingCond != nil && progressingCond.Status == corev1.ConditionFalse {
			// Progressing=False + Done=False → actual failure
			reason, failureMessage := pickFailureDetail(doneCond, progressingCond)
			if failureMessage == "" {
				failureMessage = fmt.Sprintf("VirtualMachineBackup %s failed (Done=False, Progressing=False) with no failure detail reported", vmb.Name)
			}
			logger.Error(nil, "VirtualMachineBackup failed", "reason", reason, "message", failureMessage)
			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("VMBackup failed: %s", failureMessage)); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		// Done=False but Progressing is True or absent → backup still running
		logger.Info("VirtualMachineBackup still in progress (Done=False, waiting for completion)", "vmb", vmb.Name)
	}

	// Detect stuck initialization: Initializing=True + Progressing=False + no Done condition.
	// This can be transient (e.g. PVC being hotplugged to VMI) or permanent (VMBT deleted).
	// To distinguish, we verify the VMB's referenced BackupTracker still exists.
	// If it's gone, the VMB will never progress — fail immediately.
	// If it still exists, this is a normal transient state — keep requeuing.
	if doneCond == nil && initializingCond != nil && initializingCond.Status == corev1.ConditionTrue &&
		progressingCond != nil && progressingCond.Status == corev1.ConditionFalse {
		vmbtName := vmb.Spec.Source.Name
		vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
		if err := r.Get(ctx, types.NamespacedName{Name: vmbtName, Namespace: vmb.Namespace}, vmbt); err != nil {
			if errors.IsNotFound(err) {
				logger.Error(nil, "VirtualMachineBackup's BackupTracker was deleted, failing",
					"vmb", vmb.Name, "vmbt", vmbtName)
				if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
					fmt.Sprintf("VMBackup stuck: BackupTracker %s no longer exists", vmbtName)); updateErr != nil {
					return ctrl.Result{}, updateErr
				}
				return ctrl.Result{}, nil
			}
			// Transient API error — requeue
			return ctrl.Result{}, fmt.Errorf("failed to check BackupTracker %s existence: %w", vmbtName, err)
		}
		// VMBT exists, initialization is in progress (e.g. PVC being attached) — keep waiting
		logger.Info("VirtualMachineBackup initializing (VMBT exists, waiting for progress)",
			"vmb", vmb.Name, "reason", initializingCond.Reason)
	}

	// No Done condition yet, or backup still running - requeue
	logger.Info("VirtualMachineBackup in progress, requeuing")
	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// conditionIndicatesFailure reports whether a VirtualMachineBackup condition's Reason or Message
// signals a failure. KubeVirt uses "Completed VirtualMachineBackup" as a prefix for successful
// backups (which may include warnings like "Failed freezing guest filesystem"), and patterns
// like "Backup has failed: <details>" for actual failures. We treat a condition as success
// when the reason starts with "Completed", even if a warning substring contains "failed".
func conditionIndicatesFailure(cond *kubevirtbackupv1alpha1.Condition) bool {
	if cond == nil {
		return false
	}
	reason := strings.ToLower(cond.Reason)
	message := strings.ToLower(cond.Message)
	if strings.HasPrefix(reason, "completed") {
		return false
	}
	return strings.Contains(reason, "failed") ||
		strings.Contains(message, "failed")
}

// vmBackupConditionDetail extracts a human-readable failure detail from a VirtualMachineBackup
// condition. KubeVirt's CBT backup controller never populates Message on Done/Progressing/
// Initializing conditions — only Reason carries the descriptive text (e.g. "Backup has failed:
// No space left on device") — so Reason is used whenever Message is empty.
func vmBackupConditionDetail(cond *kubevirtbackupv1alpha1.Condition) string {
	if cond == nil {
		return ""
	}
	if cond.Message != "" {
		return cond.Message
	}
	return cond.Reason
}

// pickFailureDetail returns the Reason/detail pair from whichever of the two conditions
// actually indicates a failure (preferring the first argument on a tie), so a neutral
// status reason on one condition never overrides the real failure detail on the other.
// Falls back to any non-empty text if neither condition's text matches "failed".
func pickFailureDetail(preferred, other *kubevirtbackupv1alpha1.Condition) (reason, detail string) {
	for _, cond := range []*kubevirtbackupv1alpha1.Condition{preferred, other} {
		if conditionIndicatesFailure(cond) {
			return cond.Reason, vmBackupConditionDetail(cond)
		}
	}
	for _, cond := range []*kubevirtbackupv1alpha1.Condition{preferred, other} {
		if cond != nil && vmBackupConditionDetail(cond) != "" {
			return cond.Reason, vmBackupConditionDetail(cond)
		}
	}
	return "", ""
}

// resolveBackupMode determines whether to force a full backup or allow incremental.
// Returns (forceFullBackup, checkpointLookup) where checkpointLookup is the BSL
// chain validation result (nil if BSL was unreachable or validation was skipped).
// This covers three scenarios:
//  1. User explicitly requested force-full-backup via annotation on DataUpload.
//  2. BSL checkpoint validation found a broken chain, requiring a forced full backup.
//  3. Max incremental backups limit reached (global or per-VM override).
func (r *KubeVirtDataUploadReconciler) resolveBackupMode(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmRef *common.VMReference) (bool, *uploader.CheckpointLookupResult) {
	// Check if force full backup is requested via annotation.
	if du.Annotations[common.AnnotationForceFullBackup] == bslValidatedValue {
		logger.Info("Force full backup requested via annotation, skipping BSL checkpoint lookup")
		return true, nil
	}

	// Validate checkpoint chain in BSL for incremental backup support.
	// This runs once per DataUpload (tracked via annotation) to avoid redundant
	// S3 queries on every reconcile, while still validating the BSL state for
	// each new backup.
	if du.Annotations[common.AnnotationBSLValidated] == bslValidatedValue {
		logger.V(1).Info("BSL validation already completed for this DataUpload")
		return false, nil
	}

	forceFullBackup, checkpointLookup := r.validateBSLCheckpoint(ctx, logger, du, vmRef)

	// Check if max incremental backups limit is reached.
	// ChainLength includes the root full backup, so incrementals = ChainLength - 1.
	if !forceFullBackup &&
		checkpointLookup != nil && checkpointLookup.Found && checkpointLookup.IsChainValid {
		maxInc := r.getEffectiveMaxIncrementalBackups(ctx, logger, vmRef)
		if maxInc > 0 {
			incrementalCount := checkpointLookup.ChainLength - 1
			if incrementalCount >= maxInc {
				logger.Info("Max incremental backups reached, forcing full backup",
					"incrementalCount", incrementalCount,
					"maxIncrementalBackups", maxInc,
					"chainLength", checkpointLookup.ChainLength)
				forceFullBackup = true
			}
		}
	}

	return forceFullBackup, checkpointLookup
}

// determineSkipQuiesce decides whether the VirtualMachineBackup created for
// this DataUpload should skip quiesce (crash-consistent, SkipQuiesce=true)
// or attempt to quiesce the guest filesystem first (application-consistent,
// SkipQuiesce=false).
//
// Precedence (highest first):
//  1. Explicit user override via the AnnotationSkipQuiesce annotation on
//     the DataUpload: "true" forces skip-quiesce, "false" forces quiesce,
//     regardless of guest-agent state.
//  2. Automatic detection: quiesce only when the VM's
//     VirtualMachineInstance reports its guest agent as connected (see
//     common.IsGuestAgentConnected). This avoids the noisy "Failed
//     freezing guest filesystem" warnings KubeVirt reports when backing
//     up VMs without a guest agent, e.g. Cirros test VMs, without
//     requiring a workload-specific annotation for every such VM.
//
// If the VMI can't be fetched (not found, transient error, etc.), quiesce
// is skipped -- the safe, crash-consistent default -- rather than risking a
// backup failure from attempting to quiesce a VM whose agent state is
// unknown.
func (r *KubeVirtDataUploadReconciler) determineSkipQuiesce(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmRef *common.VMReference) bool {
	if raw, ok := du.Annotations[common.AnnotationSkipQuiesce]; ok {
		if override, err := strconv.ParseBool(raw); err == nil {
			logger.Info("SkipQuiesce explicitly overridden via annotation", "skipQuiesce", override)
			return override
		}
		logger.Info("Ignoring unparseable skip-quiesce annotation value, falling back to guest-agent auto-detection",
			"annotation", common.AnnotationSkipQuiesce, "value", raw)
	}

	vmi := &kubevirtcorev1.VirtualMachineInstance{}
	var err error
	if r.APIReader != nil {
		err = r.APIReader.Get(ctx, types.NamespacedName{Name: vmRef.Name, Namespace: vmRef.Namespace}, vmi)
	} else {
		err = r.Get(ctx, types.NamespacedName{Name: vmRef.Name, Namespace: vmRef.Namespace}, vmi)
	}
	if err != nil {
		logger.V(1).Info("Could not fetch VirtualMachineInstance, defaulting to skip quiesce",
			"vmi", vmRef.Name, "reason", err.Error())
		return true
	}

	if common.IsGuestAgentConnected(vmi) {
		logger.Info("Guest agent connected, quiescing before backup", "vmi", vmi.Name)
		return false
	}
	logger.Info("Guest agent not connected, skipping quiesce", "vmi", vmi.Name)
	return true
}

// getEffectiveMaxIncrementalBackups returns the max incremental backups limit
// for the given VM. A per-VM annotation takes precedence over the global setting.
func (r *KubeVirtDataUploadReconciler) getEffectiveMaxIncrementalBackups(ctx context.Context, logger logr.Logger, vmRef *common.VMReference) int {
	vm := &kubevirtcorev1.VirtualMachine{}
	if err := r.Get(ctx, types.NamespacedName{Name: vmRef.Name, Namespace: vmRef.Namespace}, vm); err != nil {
		logger.V(1).Info("Could not fetch VM for max-incremental-backups annotation, using global setting",
			"reason", err.Error())
		return r.MaxIncrementalBackups
	}
	if val, ok := vm.Annotations[common.AnnotationMaxIncrementalBackups]; ok {
		if parsed, err := strconv.Atoi(val); err == nil && parsed >= 0 {
			logger.Info("Using per-VM max incremental backups override",
				"vm", vmRef.Name, "maxIncrementalBackups", parsed)
			return parsed
		}
		logger.Info("Invalid max-incremental-backups annotation value, using global setting",
			"vm", vmRef.Name, "annotationValue", val)
	}
	return r.MaxIncrementalBackups
}

// validateBSLCheckpoint queries the BSL for a valid checkpoint chain and determines
// whether a forced full backup is required because the chain is broken.
// Returns (forceFullBackup, checkpointLookup) where checkpointLookup is the BSL
// chain validation result (nil if BSL was unreachable or lookup failed).
// It does NOT modify VMBT status — ForceFullBackup on the VMB is the sole mechanism
// for forcing full backups.
func (r *KubeVirtDataUploadReconciler) validateBSLCheckpoint(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmRef *common.VMReference) (bool, *uploader.CheckpointLookupResult) {
	forceFullBackup := true

	var checkpointLookup *uploader.CheckpointLookupResult
	bsl, bslErr := r.getBackupStorageLocationForDU(ctx, du)
	if bslErr != nil {
		// BSL lookup failure is non-fatal. Validation will be retried on the
		// next reconcile.
		logger.Info("BSL not available for checkpoint lookup, skipping validation",
			"reason", bslErr.Error())
	} else {
		var err error
		checkpointLookup, err = r.lookupCheckpointFromBSL(ctx, bsl, vmRef.Namespace, vmRef.Name)
		if err != nil {
			// Checkpoint lookup failure is non-fatal. Validation will be retried
			// on the next reconcile.
			logger.Info("Checkpoint lookup failed, skipping validation",
				"reason", err.Error())
		} else if checkpointLookup != nil {
			logger.Info("Checkpoint lookup completed",
				"found", checkpointLookup.Found,
				"message", checkpointLookup.Message)
		}
	}

	// Default to full backup. Only allow incremental when BSL validation
	// positively confirms a valid chain exists.
	if checkpointLookup != nil && checkpointLookup.Found && checkpointLookup.IsChainValid {
		forceFullBackup = false
		logger.Info("BSL checkpoint chain is valid, allowing incremental backup",
			"checkpoint", checkpointLookup.LatestCheckpoint,
			"chainLength", checkpointLookup.ChainLength)
	} else if checkpointLookup != nil && !checkpointLookup.IsChainValid {
		// Chain is broken — force full backup via VMB.Spec.ForceFullBackup
		logger.Info("BSL checkpoint chain is invalid, forcing full backup",
			"message", checkpointLookup.Message)
	} else if checkpointLookup != nil && !checkpointLookup.Found {
		// No checkpoint found — first backup or all data deleted.
		// The default full-backup decision remains in effect (KubeVirt would
		// also do a full backup without a checkpoint).
		logger.Info("No valid checkpoint found in BSL, will perform full backup",
			"message", checkpointLookup.Message)
	}

	// Mark BSL validation as done for this DataUpload to avoid redundant S3 queries.
	// Only set the annotation when we got a definitive result from BSL (checkpointLookup != nil).
	// If BSL was unreachable or the lookup failed due to transient errors (e.g., read-only
	// filesystem, network errors), don't set the annotation so validation will be retried
	// on the next reconcile.
	if checkpointLookup != nil {
		if du.Annotations == nil {
			du.Annotations = make(map[string]string)
		}
		du.Annotations[common.AnnotationBSLValidated] = bslValidatedValue
		if err := r.Update(ctx, du); err != nil {
			// Non-fatal: worst case we re-run BSL validation on next reconcile
			logger.Info("Failed to set BSL validated annotation, will retry",
				"reason", err.Error())
		}
	}

	return forceFullBackup, checkpointLookup
}

// handlePrepared processes DataUploads in Prepared phase
// Rebinds PV to OADP namespace, launches datamover pod, and transitions to InProgress
//
//nolint:gocyclo // Phase handler with necessary validation steps
func (r *KubeVirtDataUploadReconciler) handlePrepared(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling Prepared phase DataUpload")

	// Get VM reference for namespace context
	vmRef, err := common.GetVMReference(du)
	if err != nil {
		logger.Error(err, "Failed to get VM reference")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Missing VM reference: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Datamover pod runs in OADP namespace (where credentials are accessible)
	podNamespace := r.getPodNamespace(du)

	// Check if datamover pod already exists (idempotency)
	pod, err := r.findPodForDataUpload(ctx, du, podNamespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check for existing datamover pod: %w", err)
	}
	if pod != nil {
		// Pod exists, transition to InProgress and monitor
		logger.Info("Datamover pod already exists, transitioning to InProgress", "pod", pod.Name)
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseInProgress, "Datamover pod running"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	// Checked before any of the lookups below (BSL, VMB, PV rebind) since none
	// of that is needed while gated -- a deferred DataUpload would otherwise
	// redo it every 30s for no reason.
	if gated, result, err := r.checkConcurrentDataMoverLimit(ctx, logger, du); err != nil {
		return ctrl.Result{}, err
	} else if gated {
		return result, nil
	}

	// Validate BSL and VMB exist BEFORE rebinding PV
	// This prevents leaving PV in a bad state if these checks fail

	// Get BackupStorageLocation
	bsl, err := r.getBackupStorageLocationForDU(ctx, du)
	if err != nil {
		if isTransientBSLLookupError(err, du.Spec.BackupStorageLocation) {
			return ctrl.Result{}, fmt.Errorf("failed to get BackupStorageLocation: %w", err)
		}
		logger.Error(err, "Failed to get BackupStorageLocation")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Failed to get BSL: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Get VMB to extract checkpoint info
	vmb, err := r.findVMBForDataUpload(ctx, du, vmRef.Namespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get VirtualMachineBackup: %w", err)
	}
	if vmb == nil {
		logger.Error(nil, "VirtualMachineBackup not found for DataUpload")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, "VirtualMachineBackup not found"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Check if PV has already been rebound (idempotency)
	reboundPVCName := common.SafeResourceName(common.ReboundPVCNamePrefix, du.Name)
	reboundPVC := &corev1.PersistentVolumeClaim{}
	err = r.Get(ctx, types.NamespacedName{Name: reboundPVCName, Namespace: podNamespace}, reboundPVC)
	pvAlreadyRebound := err == nil && reboundPVC.Status.Phase == corev1.ClaimBound

	if !pvAlreadyRebound {
		// Rebind PV from VM namespace to OADP namespace
		// This allows the datamover pod to access both the backup data AND credentials
		//
		// The temp PVC was created with GenerateName (random suffix), so we read
		// the actual name from the VMB spec rather than recomputing it.
		if vmb.Spec.PvcName == nil || *vmb.Spec.PvcName == "" {
			err := fmt.Errorf("VirtualMachineBackup %s has no PVC name in spec", vmb.Name)
			logger.Error(err, "Cannot determine source PVC for rebinding")
			if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, err.Error()); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, nil
		}
		sourcePVCName := *vmb.Spec.PvcName

		logger.Info("Rebinding PV from VM namespace to OADP namespace",
			"sourcePVC", sourcePVCName,
			"sourceNamespace", vmRef.Namespace,
			"targetNamespace", podNamespace)

		rebindResult, err := rebindPVToNamespace(ctx, r.Client, logger, sourcePVCName, vmRef.Namespace, podNamespace, du.Name, string(du.UID), common.LabelDataUploadUID, common.AnnotationDataUploadName, BindTargetCreate, "")
		if err != nil {
			// Fail without retry: PV rebind is a multi-step operation (delete PVC, patch PV, create new PVC).
			// If it fails partway through, automatic retries could leave resources in an inconsistent state.
			// Failing allows the user to investigate and take corrective action.
			logger.Error(err, "Failed to rebind PV to OADP namespace")
			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Failed to rebind PV: %v", err)); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}

		logger.Info("Successfully rebound PV to OADP namespace",
			"newPVC", rebindResult.NewPVCName,
			"pv", rebindResult.PVName)

		reboundPVCName = rebindResult.NewPVCName
	} else {
		logger.Info("PV already rebound to OADP namespace", "pvc", reboundPVCName)
	}

	// Get backup type from VMB status
	backupType := "full"
	if vmb.Status != nil && vmb.Status.Type != "" {
		backupType = string(vmb.Status.Type)
	}

	// Detect mismatch between expected and actual backup type.
	// This catches the case where the controller allowed incremental but
	// virt-controller performed a full backup (e.g., VM lost its libvirt checkpoint).
	// The expected type is forwarded to the datamover pod so the uploader can
	// correct the S3 checkpoint index (record a full backup with no parent).
	expectedBackupType := du.Annotations[common.AnnotationExpectedBackupType]
	if expectedBackupType != "" && !strings.EqualFold(expectedBackupType, backupType) {
		logger.Info("Backup type mismatch detected: VM may have lost its libvirt checkpoint",
			"expected", expectedBackupType,
			"actual", backupType)
	}

	// Get checkpoint name from VMB status
	checkpointName := ""
	if vmb.Status != nil && vmb.Status.CheckpointName != nil {
		checkpointName = *vmb.Status.CheckpointName
	}

	// Get VMBT name from annotation
	vmbtName := ""
	if du.Annotations != nil {
		vmbtName = du.Annotations[AnnotationVMBTName]
	}
	if vmbtName == "" {
		err := fmt.Errorf("VMBT name annotation %s not found on DataUpload", AnnotationVMBTName)
		logger.Error(err, "Failed to get VMBT name")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, err.Error()); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Ensure the rebound PVC exists before proceeding
	if err := r.Get(ctx, types.NamespacedName{Name: reboundPVCName, Namespace: podNamespace}, reboundPVC); err != nil {
		err := fmt.Errorf("failed to get rebound PVC %s: %w", reboundPVCName, err)
		logger.Error(err, "Rebound PVC not found")
		if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, err.Error()); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, nil
	}

	rawSize := r.getRawBackupSize(ctx, du, vmRef.Namespace)

	// Build datamover pod config - now using OADP namespace and rebound PVC
	podConfig, err := r.buildDatamoverPodConfig(du, bsl, vmb, vmRef, backupType, expectedBackupType, checkpointName, vmbtName, rawSize)
	if err != nil {
		logger.Error(err, "Failed to build datamover pod config")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Failed to build pod config: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Override namespace and PVC name for the rebound resources
	podConfig.Namespace = podNamespace
	podConfig.SourcePVCName = reboundPVCName

	// Create the datamover pod
	podToCreate := buildDatamoverPod(podConfig)

	// Use GenerateName instead of a fixed name
	podToCreate.GenerateName = safeGenerateNamePrefix(fmt.Sprintf("%s%s-", common.DatamoverPodNamePrefix, du.Name), 63)
	podToCreate.Name = ""

	// Set owner reference so pod is cleaned up when DataUpload is deleted
	// This works now because pod is in the same namespace as DataUpload
	if err := controllerutil.SetOwnerReference(du, podToCreate, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to set owner reference on pod: %w", err)
	}

	if err := r.Create(ctx, podToCreate); err != nil {
		if errors.IsAlreadyExists(err) {
			// Race condition - pod was created between check and create
			logger.Info("Datamover pod already exists (race)", "generateName", podToCreate.GenerateName)
		} else {
			return ctrl.Result{}, fmt.Errorf("failed to create datamover pod: %w", err)
		}
	} else {
		logger.Info("Created datamover pod", "generateName", podToCreate.GenerateName, "namespace", podNamespace)
	}

	// Transition to InProgress
	if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseInProgress, "Datamover pod launched"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// handleInProgress processes DataUploads in InProgress phase
// Monitors datamover pod and transitions to Completed/Failed
func (r *KubeVirtDataUploadReconciler) handleInProgress(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling InProgress phase DataUpload")

	// Datamover pod runs in OADP namespace
	podNamespace := r.getPodNamespace(du)

	// Get the datamover pod
	pod, err := r.findPodForDataUpload(ctx, du, podNamespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get datamover pod: %w", err)
	}
	if pod == nil {
		if du.Annotations[common.AnnotationDatamoverPodSucceeded] == bslValidatedValue {
			// The pod succeeded on an earlier reconcile and has now fully
			// terminated (kubelet finished unmounting its volumes). Finish
			// the PVC/PV cleanup that was deferred and complete.
			if !r.cleanupDatamoverResources(ctx, logger, du, podNamespace) {
				return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
			}

			vmRef, _ := common.GetVMReference(du)
			if vmRef != nil {
				r.cleanupVMBackupResources(ctx, logger, du, vmRef.Namespace)
			}

			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseCompleted, "Data upload completed"); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		// Pod not found - this is unexpected in InProgress phase
		logger.Error(nil, "Datamover pod not found", "dataUpload", du.Name, "namespace", podNamespace)
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, "Datamover pod not found"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Check pod status
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		logger.Info("Datamover pod completed successfully", "pod", pod.Name)

		r.syncProgressFromPod(ctx, logger, du, pod)

		if du.Annotations[common.AnnotationDatamoverPodSucceeded] != bslValidatedValue {
			r.emitPodLogs(ctx, logger, pod)
			if err := r.markDatamoverPodSucceeded(ctx, du); err != nil {
				return ctrl.Result{}, err
			}
		}

		// Cleanup resources. If the pod is still terminating, defer PVC/PV
		// cleanup and the Completed transition to a later reconcile rather
		// than blocking this one on waitForPVCDeletion.
		if !r.cleanupDatamoverResources(ctx, logger, du, podNamespace) {
			return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
		}

		vmRef, _ := common.GetVMReference(du)
		if vmRef != nil {
			r.cleanupVMBackupResources(ctx, logger, du, vmRef.Namespace)
		}

		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseCompleted, "Data upload completed"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case corev1.PodFailed:
		failureMessage := extractPodFailureMessage(pod)
		logger.Error(nil, "Datamover pod failed", "pod", pod.Name, "message", failureMessage)

		r.syncProgressFromPod(ctx, logger, du, pod)

		r.emitPodLogs(ctx, logger, pod)

		// Skip cleanup on failure to preserve resources for debugging.
		// Resources (pod, rebound PVC/PV) can be manually cleaned up after investigation.

		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Datamover pod failed: %s", failureMessage)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case corev1.PodPending, corev1.PodRunning:
		r.syncProgressFromPod(ctx, logger, du, pod)

		logger.V(1).Info("Datamover pod still running", "pod", pod.Name, "phase", pod.Status.Phase)
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil

	default:
		logger.Info("Datamover pod in unknown phase", "pod", pod.Name, "phase", pod.Status.Phase)
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}
}

// syncProgressFromPod reads progress annotations from the datamover pod and updates the DataUpload CR.
func (r *KubeVirtDataUploadReconciler) syncProgressFromPod(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, pod *corev1.Pod) {
	if pod.Annotations == nil {
		return
	}
	totalStr := pod.Annotations["kubevirt-datamover.io/total-bytes"]
	incStr := pod.Annotations["kubevirt-datamover.io/incremental-bytes"]
	doneStr := pod.Annotations["kubevirt-datamover.io/bytes-done"]

	if totalStr != "" && doneStr != "" {
		total, _ := strconv.ParseInt(totalStr, 10, 64)
		done, _ := strconv.ParseInt(doneStr, 10, 64)

		var inc int64
		if incStr != "" {
			inc, _ = strconv.ParseInt(incStr, 10, 64)
		} else {
			inc = total
		}

		if du.Status.Progress.TotalBytes != total || du.Status.Progress.BytesDone != done || du.Status.IncrementalBytes != inc {
			patch := client.MergeFrom(du.DeepCopy())
			du.Status.Progress.TotalBytes = total
			du.Status.Progress.BytesDone = done
			du.Status.IncrementalBytes = inc
			if err := r.Patch(ctx, du, patch); err != nil {
				logger.Error(err, "Failed to patch DataUpload progress")
			}
		}
	}
}

// markDatamoverPodSucceeded persists AnnotationDatamoverPodSucceeded so a
// later reconcile that finds no datamover pod (because it has since fully
// terminated) knows this is expected rather than a genuine failure.
func (r *KubeVirtDataUploadReconciler) markDatamoverPodSucceeded(ctx context.Context, du *velerov2alpha1.DataUpload) error {
	if du.Annotations == nil {
		du.Annotations = make(map[string]string)
	}
	du.Annotations[common.AnnotationDatamoverPodSucceeded] = bslValidatedValue
	return r.Update(ctx, du)
}

// emitPodLogs collects logs from a completed datamover pod and emits them
// through the controller's logger. Failures are logged as warnings and
// never block the reconcile loop.
func (r *KubeVirtDataUploadReconciler) emitPodLogs(ctx context.Context, logger logr.Logger, pod *corev1.Pod) {
	if r.PodLogCollector == nil {
		return
	}
	logs, err := r.PodLogCollector(ctx, pod.Name, pod.Namespace)
	if err != nil {
		logger.Info("Warning: failed to collect datamover pod logs", "pod", pod.Name, "error", err)
		return
	}
	if logs == "" {
		return
	}
	lines := strings.Split(strings.TrimRight(logs, "\n"), "\n")
	skipped := 0
	if len(lines) > maxEmittedPodLogLines {
		skipped = len(lines) - maxEmittedPodLogLines
		lines = lines[skipped:]
	}
	if skipped > 0 {
		logger.Info("Datamover pod log truncated",
			"source", "datamover-pod",
			"pod", pod.Name,
			"skippedLeadingLines", skipped)
	}
	for i, line := range lines {
		logger.Info("Datamover pod log",
			"source", "datamover-pod",
			"pod", pod.Name,
			"line", skipped+i+1,
			"message", line)
	}
}

// cleanupDatamoverResources cleans up resources created during the datamover process.
// Returns false if the datamover pod is still terminating — deleting the rebound
// PVC while the pod still mounts it would block synchronously in
// waitForPVCDeletion for up to PVRebindTimeout, so callers should defer and let
// a later reconcile retry once the pod is confirmed gone.
// See https://github.com/migtools/kubevirt-datamover-controller/issues/171.
func (r *KubeVirtDataUploadReconciler) cleanupDatamoverResources(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, podNamespace string) bool {
	if cleanupNotReady, _ := cleanupPodsByUID(ctx, r.Client, r.APIReader, common.LabelDataUploadUID, string(du.UID), podNamespace, logger); cleanupNotReady {
		logger.Info("Datamover pod still terminating (or its status couldn't be confirmed), deferring PVC/PV cleanup to next reconcile")
		return false
	}

	reboundPVCName := common.SafeResourceName(common.ReboundPVCNamePrefix, du.Name)
	if err := cleanupReboundPVCAndPV(ctx, r.Client, logger, reboundPVCName, podNamespace, string(du.UID), common.LabelDataUploadUID); err != nil {
		logger.Error(err, "Failed to cleanup rebound PVC and PV", "pvc", reboundPVCName)
		// Continue - don't block completion on cleanup failures
	}

	return true
}

// cleanupVMBackupResources deletes VMB CRs in the VM namespace.
// Used during cancellation when the datamover pod won't run its own cleanup.
// The VMBT is intentionally preserved so KubeVirt can use it during VM lifecycle
// events (restarts, migrations) to redefine libvirt checkpoints.
// See https://github.com/migtools/kubevirt-datamover-controller/issues/32.
//
// Not called on a Failed transition: the team decided to preserve the VMB
// (like the datamover pod/PVC/PV, see the PodFailed case in handleInProgress)
// for debugging failed backups rather than deleting it automatically.
// TODO: consider a configurable cleanup-on-failure option in the future.
// See https://github.com/migtools/kubevirt-datamover-controller/issues/168.
func (r *KubeVirtDataUploadReconciler) cleanupVMBackupResources(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmNamespace string) {
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := r.List(ctx, vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		logger.Error(err, "Failed to list VMBs for cleanup")
	} else {
		for i := range vmbList.Items {
			vmb := &vmbList.Items[i]
			if err := r.Delete(ctx, vmb); err != nil && !errors.IsNotFound(err) {
				logger.Error(err, "Failed to delete VMB", "vmb", vmb.Name)
			} else {
				logger.Info("Deleted VMB", "vmb", vmb.Name, "namespace", vmNamespace)
			}
		}
	}
}

// handleCanceling processes DataUploads in Canceling phase
// Cleans up resources and transitions to Canceled
func (r *KubeVirtDataUploadReconciler) handleCanceling(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling Canceling phase DataUpload")

	// Datamover pod runs in OADP namespace
	podNamespace := r.getPodNamespace(du)

	// Clean up datamover resources in OADP namespace. If the pod is still
	// terminating, defer VMB cleanup and the Canceled transition to a later
	// reconcile rather than blocking this one on waitForPVCDeletion.
	if !r.cleanupDatamoverResources(ctx, logger, du, podNamespace) {
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	// Clean up VMB and VMBT in the VM namespace.
	// When canceling, the datamover pod won't run its cleanup, so we handle it here.
	vmRef, _ := common.GetVMReference(du)
	if vmRef != nil {
		r.cleanupVMBackupResources(ctx, logger, du, vmRef.Namespace)
	}

	if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseCanceled, "DataUpload canceled"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// updatePhase updates the DataUpload phase and status message
// Uses Update instead of Status().Patch() to match Velero's approach,
// which works regardless of whether the CRD has status subresource enabled --
// confirmed via `oc get crd datauploads.velero.io -o jsonpath='{.spec.versions[*].subresources}'`
// (empty {}): it does not, so Status().Update() would be a no-op here, not an
// equally-valid alternative. checkOperationTimeoutCore's AcceptedTimestamp
// backfill persist callback uses the same r.Update() for the same reason.
func (r *KubeVirtDataUploadReconciler) updatePhase(ctx context.Context, du *velerov2alpha1.DataUpload, phase velerov2alpha1.DataUploadPhase, message string) error {
	logger := log.FromContext(ctx)

	// Skip update if already at target phase with same message (idempotency)
	if du.Status.Phase == phase && du.Status.Message == message {
		logger.V(1).Info("DataUpload already at target phase with same message, skipping update",
			"dataUpload", du.Name,
			"phase", phase)
		return nil
	}

	du.Status.Phase = phase
	du.Status.Message = message

	now := metav1.Now()
	if phase == velerov2alpha1.DataUploadPhaseInProgress && du.Status.StartTimestamp == nil {
		du.Status.StartTimestamp = &now
	}
	if isTerminalDataUploadPhase(phase) && du.Status.CompletionTimestamp == nil {
		du.Status.CompletionTimestamp = &now
	}

	if err := r.Update(ctx, du); err != nil {
		logger.Error(err, "Failed to update DataUpload phase",
			"dataUpload", du.Name,
			"phase", phase)
		return fmt.Errorf("failed to update DataUpload phase to %s: %w", phase, err)
	}

	logger.Info("Updated DataUpload phase",
		"dataUpload", du.Name,
		"phase", phase,
		"message", message)

	return nil
}

// isTerminalDataUploadPhase reports whether phase is one of DataUpload's
// terminal phases (Completed/Failed/Canceled), used by updatePhase to know
// when to set Status.CompletionTimestamp.
func isTerminalDataUploadPhase(phase velerov2alpha1.DataUploadPhase) bool {
	switch phase {
	case velerov2alpha1.DataUploadPhaseCompleted, velerov2alpha1.DataUploadPhaseFailed, velerov2alpha1.DataUploadPhaseCanceled:
		return true
	default:
		return false
	}
}

// getPodNamespace returns the namespace where datamover pods should run.
// Uses OADPNamespace if configured, otherwise falls back to the DataUpload's namespace.
func (r *KubeVirtDataUploadReconciler) getPodNamespace(du *velerov2alpha1.DataUpload) string {
	if r.OADPNamespace != "" {
		return r.OADPNamespace
	}
	return du.Namespace
}

// SetupWithManager sets up the controller with the Manager
func (r *KubeVirtDataUploadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrent := r.MaxConcurrentReconciles
	if maxConcurrent <= 0 {
		maxConcurrent = DefaultMaxConcurrentReconciles
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov2alpha1.DataUpload{}).
		WithEventFilter(r.filterKubeVirtDataMover()).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrent,
		}).
		Named("kubevirt-dataupload").
		Complete(r)
}

// filterKubeVirtDataMover returns a predicate that filters for DataUploads
// where Spec.DataMover is "kubevirt"
func (r *KubeVirtDataUploadReconciler) filterKubeVirtDataMover() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		du, ok := obj.(*velerov2alpha1.DataUpload)
		if !ok {
			return false
		}
		return du.Spec.DataMover == common.DataMoverKubeVirt
	})
}

// ensureTempPVC creates or retrieves the temporary PVC for backup output.
// The PVC is sized based on the source VM's disk size per issue #5:
//  1. User override via annotation kubevirt-datamover.io/backup-pvc-size
//  2. Source PVC capacity (from du.Spec.SourcePVC)
//  3. Fallback to DefaultTempPVCSize (10Gi)
//
// Note: We don't set an owner reference because the PVC is in VM namespace
// while DataUpload is in OADP namespace (cross-namespace owner refs not allowed).
// The PVC will be cleaned up during PV rebinding or explicit cleanup.
func (r *KubeVirtDataUploadReconciler) ensureTempPVC(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, namespace string) (*corev1.PersistentVolumeClaim, error) {
	existing, err := r.findTempPVC(ctx, du, namespace)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		logger.V(1).Info("Temporary PVC already exists", "pvc", existing.Name)
		return existing, nil
	}

	pvcSize, err := r.calculateBackupPVCSize(ctx, logger, du, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate backup PVC size: %w", err)
	}

	// Create new PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: safeGenerateNamePrefix(fmt.Sprintf("kubevirt-backup-%s-", du.Name), 63),
			Namespace:    namespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: du.Name,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: pvcSize,
				},
			},
		},
	}

	if err := r.Create(ctx, pvc); err != nil {
		return nil, fmt.Errorf("failed to create temporary PVC: %w", err)
	}

	logger.Info("Created temporary PVC", "generateName", pvc.GenerateName, "namespace", namespace, "size", pvcSize.String())
	return pvc, nil
}

// findTempPVC finds the unique temporary backup PVC for a DataUpload, if any.
// Tries the cached client first; if it finds nothing, retries via APIReader
// (an uncached read) before the caller concludes none exists -- the informer
// cache is only eventually consistent, so a PVC created moments ago in an
// earlier reconcile may not yet be visible to a cached List.
func (r *KubeVirtDataUploadReconciler) findTempPVC(ctx context.Context, du *velerov2alpha1.DataUpload, namespace string) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := listTempPVC(ctx, r.Client, du, namespace)
	if err != nil || pvc != nil || r.APIReader == nil {
		return pvc, err
	}
	return listTempPVC(ctx, r.APIReader, du, namespace)
}

func listTempPVC(ctx context.Context, reader client.Reader, du *velerov2alpha1.DataUpload, namespace string) (*corev1.PersistentVolumeClaim, error) {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := reader.List(ctx, pvcList, client.InNamespace(namespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		return nil, fmt.Errorf("failed to list temporary PVCs: %w", err)
	}
	if len(pvcList.Items) > 1 {
		return nil, fmt.Errorf("found multiple temporary PVCs for DataUpload %s", du.Name)
	}
	if len(pvcList.Items) == 1 {
		return &pvcList.Items[0], nil
	}
	return nil, nil
}

// sizeOverheadPercent is the percentage added on top of source PVC capacity when
// sizing the temporary backup PVC. This accounts for filesystem overhead (~6% on
// ext4/xfs) and qcow2 metadata, ensuring the backup has enough room to complete.
const sizeOverheadPercent = 20

// calculateBackupPVCSize determines the appropriate size for the temporary backup PVC.
//
// Priority:
//  1. User override via annotation kubevirt-datamover.io/backup-pvc-size
//  2. Sum of all VM disk PVC capacities + 20% overhead
//  3. Source PVC capacity + 20% overhead (fallback if VM lookup fails)
//  4. Fallback to DefaultTempPVCSize (10Gi)
//
// For multi-disk VMs, KubeVirt writes all volumes' backup data into a single temp PVC,
// so we must sum all disk sizes. The 20% overhead accounts for:
//   - Filesystem-mode PVCs losing ~6% to filesystem structures (ext4/xfs)
//   - qcow2 metadata (header, L1/L2 tables, refcount blocks)
//   - Without overhead, a full backup of a 30Gi disk fails with ENOSPC on a 30Gi PVC
//
// Error handling: NotFound errors on PVC lookups are fatal (the VM references a
// volume that doesn't exist — the backup will fail anyway). Other errors (RBAC,
// cache not synced) fall back to the default to avoid infinite reconcile retries.
func (r *KubeVirtDataUploadReconciler) calculateBackupPVCSize(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, namespace string) (resource.Quantity, error) {
	minSize := resource.MustParse("1Gi")
	defaultSize := resource.MustParse(DefaultTempPVCSize)

	// 1. Check user override annotation — allows explicit size when heuristics don't fit
	if du.Annotations != nil {
		if override := du.Annotations[common.AnnotationBackupPVCSize]; override != "" {
			qty, err := resource.ParseQuantity(override)
			if err != nil {
				logger.Info("Invalid backup-pvc-size annotation, ignoring", "value", override, "error", err)
			} else {
				logger.Info("Using user-specified backup PVC size", "size", qty.String())
				return qty, nil
			}
		}
	}

	sourceNamespace := du.Spec.SourceNamespace
	if sourceNamespace == "" {
		sourceNamespace = namespace
	}

	// 2. Sum all VM disk PVC capacities (handles multi-disk VMs correctly since
	// KubeVirt writes all volumes' backup data into a single temp PVC)
	vmRef, err := common.GetVMReference(du)
	if err == nil {
		vm := &kubevirtcorev1.VirtualMachine{}
		if err := r.Get(ctx, types.NamespacedName{Name: vmRef.Name, Namespace: vmRef.Namespace}, vm); err == nil {
			pvcNames := common.GetVolumesForVm(vm)
			if len(pvcNames) > 0 {
				totalCapacity := resource.Quantity{}
				for _, pvcName := range pvcNames {
					pvc := &corev1.PersistentVolumeClaim{}
					if err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: vmRef.Namespace}, pvc); err != nil {
						if errors.IsNotFound(err) {
							return resource.Quantity{}, fmt.Errorf("VM PVC %s/%s not found, cannot determine backup size: %w", vmRef.Namespace, pvcName, err)
						}
						// Transient error — abort VM-based sizing and fall to source PVC fallback
						// to avoid creating an undersized PVC from a partial sum
						logger.Info("Could not fetch VM PVC for sizing, falling back to source PVC", "pvc", pvcName, "error", err)
						totalCapacity = resource.Quantity{}
						break
					}
					if capacity, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok {
						totalCapacity.Add(capacity)
					}
				}
				if !totalCapacity.IsZero() {
					withOverhead := addOverhead(totalCapacity, sizeOverheadPercent)
					if withOverhead.Cmp(minSize) < 0 {
						withOverhead = minSize
					}
					logger.Info("Sizing backup PVC from VM disk capacities",
						"vmName", vmRef.Name,
						"diskCount", len(pvcNames),
						"totalCapacity", totalCapacity.String(),
						"overheadPercent", sizeOverheadPercent,
						"finalSize", withOverhead.String())
					return withOverhead, nil
				}
			}
		}
	}

	// 3. Fall back to source PVC from DataUpload spec (single-disk fallback path)
	sourcePVCName := du.Spec.SourcePVC
	if sourcePVCName != "" {
		sourcePVC := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Name: sourcePVCName, Namespace: sourceNamespace}, sourcePVC); err != nil {
			if errors.IsNotFound(err) {
				return resource.Quantity{}, fmt.Errorf("source PVC %s/%s not found, cannot determine backup size: %w", sourceNamespace, sourcePVCName, err)
			}
			logger.Info("Could not fetch source PVC for sizing, using default", "pvc", sourcePVCName, "error", err)
			return defaultSize, nil
		}

		if capacity, ok := sourcePVC.Status.Capacity[corev1.ResourceStorage]; ok {
			withOverhead := addOverhead(capacity, sizeOverheadPercent)
			if withOverhead.Cmp(minSize) < 0 {
				withOverhead = minSize
			}
			logger.Info("Sizing backup PVC from source PVC capacity",
				"sourcePVC", sourcePVCName,
				"sourceCapacity", capacity.String(),
				"overheadPercent", sizeOverheadPercent,
				"finalSize", withOverhead.String())
			return withOverhead, nil
		}
	}

	// 4. Fallback
	return defaultSize, nil
}

// prepareVMBackupTracker returns an existing on-cluster VirtualMachineBackupTracker
// for the VM if one exists, or creates a new one (restoring LatestCheckpoint from
// S3 if available). The VMBT is left on-cluster between backups so KubeVirt can
// use it during VM lifecycle events (restarts, migrations) to redefine libvirt
// checkpoints. See https://github.com/migtools/kubevirt-datamover-controller/issues/32.
func (r *KubeVirtDataUploadReconciler) prepareVMBackupTracker(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmName, vmNamespace string) (*kubevirtbackupv1alpha1.VirtualMachineBackupTracker, error) {
	// Check for an existing on-cluster VMBT for this VM.
	existingVMBTList := &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerList{}
	if err := r.List(ctx, existingVMBTList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelVMNameHash: common.HashForLabel(vmName)}); err != nil {
		return nil, fmt.Errorf("failed to list existing VMBTs: %w", err)
	}

	// Reuse the on-cluster VMBT if one exists. KubeVirt needs it to persist
	// across backups for checkpoint redefinition during VM lifecycle events.
	if len(existingVMBTList.Items) > 0 {
		vmbt := &existingVMBTList.Items[0]
		logger.Info("Reusing existing on-cluster VMBT", "vmbt", vmbt.Name)
		return vmbt, nil
	}

	// No VMBT on-cluster (e.g., first backup or namespace was recreated).
	// Try to fetch the archived VMBT from S3 to restore LatestCheckpoint.
	// This is non-fatal: if BSL is unreachable or no VMBT exists yet (first backup),
	// we create a fresh VMBT without a checkpoint.
	var archivedVMBT *kubevirtbackupv1alpha1.VirtualMachineBackupTracker
	bsl, bslErr := r.getBackupStorageLocationForDU(ctx, du)
	if bslErr != nil {
		logger.Info("BSL not available for VMBT lookup, creating fresh VMBT",
			"reason", bslErr.Error())
	} else {
		var err error
		archivedVMBT, err = r.lookupLatestVMBTFromBSL(ctx, bsl, vmNamespace, vmName)
		if err != nil {
			logger.Info("Failed to lookup VMBT from BSL, creating fresh VMBT",
				"reason", err.Error())
		} else if archivedVMBT != nil {
			cpName := ""
			if archivedVMBT.Status != nil && archivedVMBT.Status.LatestCheckpoint != nil {
				cpName = archivedVMBT.Status.LatestCheckpoint.Name
			}
			logger.Info("Found archived VMBT in BSL",
				"latestCheckpoint", cpName)
		}
	}

	// Create new VMBT
	apiGroup := "kubevirt.io"
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: safeGenerateNamePrefix(fmt.Sprintf("vmbt-%s-", vmName), 63),
			Namespace:    vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash:    common.HashForLabel(vmName),
				common.LabelDataUploadUID: string(du.UID),
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: du.Name,
				common.AnnotationVMName:         vmName,
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	if err := r.Create(ctx, vmbt); err != nil {
		return nil, fmt.Errorf("failed to create VirtualMachineBackupTracker: %w", err)
	}

	logger.Info("Created VirtualMachineBackupTracker", "generateName", vmbt.GenerateName, "namespace", vmNamespace)

	// If we have an archived VMBT with LatestCheckpoint, set it on the new VMBT's status.
	// K8s ignores Status during creation, so we must use Status().Update() separately.
	if archivedVMBT != nil && archivedVMBT.Status != nil && archivedVMBT.Status.LatestCheckpoint != nil {
		vmbt.Status = &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: archivedVMBT.Status.LatestCheckpoint.DeepCopy(),
		}
		if err := r.Status().Update(ctx, vmbt); err != nil {
			return nil, fmt.Errorf("failed to update VMBT status with LatestCheckpoint: %w", err)
		}
		logger.Info("Set VMBT LatestCheckpoint from S3 archive",
			"vmbt", vmbt.Name,
			"latestCheckpoint", archivedVMBT.Status.LatestCheckpoint.Name)
	}

	return vmbt, nil
}

// lookupLatestVMBTFromBSL reads the VM's checkpoint index from the BSL, finds the
// latest checkpoint's vmbtObjectPath, fetches the archived vmbt.json, and returns
// the deserialized VMBT. Returns nil if no VMBT is archived (first backup or old-format index).
func (r *KubeVirtDataUploadReconciler) lookupLatestVMBTFromBSL(ctx context.Context, bsl *velerov1.BackupStorageLocation, vmNamespace, vmName string) (*kubevirtbackupv1alpha1.VirtualMachineBackupTracker, error) {
	store, cfg, err := uploader.InitObjectStoreFromBSL(ctx, r.Client, r.OADPNamespace, bsl, r.ObjectStoreFactory)
	if err != nil {
		return nil, err
	}

	// Read the VM checkpoint index
	indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json", vmNamespace, vmName)
	exists, err := store.ObjectExists(cfg.Bucket, indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check VM index existence: %w", err)
	}
	if !exists {
		return nil, nil // No index = first backup
	}

	reader, err := store.GetObject(cfg.Bucket, indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read VM index: %w", err)
	}
	indexData, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read VM index data: %w", err)
	}

	var vmIndex uploader.VMIndex
	if err := json.Unmarshal(indexData, &vmIndex); err != nil {
		return nil, fmt.Errorf("failed to parse VM index: %w", err)
	}

	if len(vmIndex.Checkpoints) == 0 {
		return nil, nil // Empty index
	}

	// Get the latest checkpoint's VMBTObjectPath
	latestCP := vmIndex.Checkpoints[len(vmIndex.Checkpoints)-1]
	if latestCP.VMBTObjectPath == "" {
		return nil, nil // Old-format index without vmbtObjectPath
	}

	// Fetch the archived vmbt.json
	vmbtReader, err := store.GetObject(cfg.Bucket, latestCP.VMBTObjectPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read archived vmbt.json at %s: %w", latestCP.VMBTObjectPath, err)
	}
	vmbtData, err := io.ReadAll(vmbtReader)
	_ = vmbtReader.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read vmbt.json data: %w", err)
	}

	var vmbt kubevirtbackupv1alpha1.VirtualMachineBackupTracker
	if err := json.Unmarshal(vmbtData, &vmbt); err != nil {
		return nil, fmt.Errorf("failed to parse archived vmbt.json: %w", err)
	}

	return &vmbt, nil
}

// ensureVMBackup creates or retrieves the VirtualMachineBackup for this DataUpload.
// Returns the VMB, whether it was created (vs already existed), and any error.
// When forceFullBackup is true, the VMB is created with ForceFullBackup=true in its spec,
// which tells KubeVirt to perform a full backup regardless of any existing checkpoint.
// skipQuiesce is set verbatim as VMB.Spec.SkipQuiesce -- see determineSkipQuiesce for
// how the controller derives it (guest-agent auto-detection, overridable via annotation).
// Note: We don't set an owner reference because VMB is in VM namespace
// while DataUpload is in OADP namespace (cross-namespace owner refs not allowed).
// VMB and VMBT are archived to S3 and deleted by the datamover pod after upload.
func (r *KubeVirtDataUploadReconciler) ensureVMBackup(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmbt *kubevirtbackupv1alpha1.VirtualMachineBackupTracker, pvcName, namespace string, forceFullBackup, skipQuiesce bool) (*kubevirtbackupv1alpha1.VirtualMachineBackup, bool, error) {
	// Find existing VMB for this DataUpload
	existingVMB, err := r.findVMBForDataUpload(ctx, du, namespace)
	if err != nil {
		return nil, false, fmt.Errorf("failed to check for existing VMB: %w", err)
	}
	if existingVMB != nil {
		logger.V(1).Info("VirtualMachineBackup already exists", "vmb", existingVMB.Name)
		return existingVMB, false, nil
	}

	// Create new VMB referencing the VMBT (enables incremental backups)
	apiGroup := "backup.kubevirt.io"
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: safeGenerateNamePrefix(fmt.Sprintf("vmb-%s-", du.Name), maxVMBNameLen),
			Namespace:    namespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: du.Name,
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     "VirtualMachineBackupTracker",
				Name:     vmbt.Name,
			},
			PvcName:         &pvcName,
			SkipQuiesce:     skipQuiesce,
			ForceFullBackup: forceFullBackup,
		},
	}

	if err := r.Create(ctx, vmb); err != nil {
		return nil, false, fmt.Errorf("failed to create VirtualMachineBackup: %w", err)
	}
	if forceFullBackup {
		logger.Info("Creating VirtualMachineBackup with ForceFullBackup=true", "vmb", vmb.Name)
	}
	if skipQuiesce {
		logger.Info("Creating VirtualMachineBackup with SkipQuiesce=true (crash-consistent)", "vmb", vmb.Name)
	} else {
		logger.Info("Creating VirtualMachineBackup with SkipQuiesce=false (quiesced, application-consistent)", "vmb", vmb.Name)
	}

	logger.Info("Created VirtualMachineBackup", "generateName", vmb.GenerateName, "namespace", namespace, "tracker", vmbt.Name)
	return vmb, true, nil
}

// getBackupStorageLocationForDU is a convenience wrapper around getBackupStorageLocation
// that extracts the BSL name from a DataUpload.
func (r *KubeVirtDataUploadReconciler) getBackupStorageLocationForDU(ctx context.Context, du *velerov2alpha1.DataUpload) (*velerov1.BackupStorageLocation, error) {
	bsl, err := getBackupStorageLocation(ctx, r.Client, du.Spec.BackupStorageLocation, r.OADPNamespace, du.Namespace)
	if err != nil {
		return nil, fmt.Errorf("DataUpload %s/%s: %w", du.Namespace, du.Name, err)
	}
	return bsl, nil
}

// findVMBForDataUpload finds the unique VirtualMachineBackup associated with a
// DataUpload. Tries the cached client first; if it finds nothing, retries via
// APIReader (an uncached read) before the caller concludes none exists -- the
// informer cache is only eventually consistent, so a VMB created moments ago in
// an earlier reconcile may not yet be visible to a cached List.
func (r *KubeVirtDataUploadReconciler) findVMBForDataUpload(ctx context.Context, du *velerov2alpha1.DataUpload, namespace string) (*kubevirtbackupv1alpha1.VirtualMachineBackup, error) {
	vmb, err := listVMBForDataUpload(ctx, r.Client, du, namespace)
	if err != nil || vmb != nil || r.APIReader == nil {
		return vmb, err
	}
	return listVMBForDataUpload(ctx, r.APIReader, du, namespace)
}

func listVMBForDataUpload(ctx context.Context, reader client.Reader, du *velerov2alpha1.DataUpload, namespace string) (*kubevirtbackupv1alpha1.VirtualMachineBackup, error) {
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := reader.List(ctx, vmbList, client.InNamespace(namespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		return nil, err
	}
	if len(vmbList.Items) == 0 {
		return nil, nil
	}
	if len(vmbList.Items) > 1 {
		return nil, fmt.Errorf("found multiple VirtualMachineBackups for DataUpload %s", du.Name)
	}
	return &vmbList.Items[0], nil
}

// isVMBTerminal returns true if the VMB has reached a terminal state:
// Done=True (success) or Done=False + Progressing=False (failure).
func isVMBTerminal(vmb *kubevirtbackupv1alpha1.VirtualMachineBackup) bool {
	if vmb.Status == nil {
		return false
	}
	var doneCond, progressingCond *kubevirtbackupv1alpha1.Condition
	for i := range vmb.Status.Conditions {
		switch vmb.Status.Conditions[i].Type {
		case kubevirtbackupv1alpha1.ConditionDone, conditionComplete:
			doneCond = &vmb.Status.Conditions[i]
		case kubevirtbackupv1alpha1.ConditionProgressing:
			progressingCond = &vmb.Status.Conditions[i]
		}
	}
	if doneCond == nil {
		return false
	}
	if doneCond.Status == corev1.ConditionTrue {
		return true
	}
	return progressingCond != nil && progressingCond.Status == corev1.ConditionFalse
}

// hasOlderActiveDUForVM checks if an older DataUpload targeting the same VM is
// still in an active phase (Accepted, Prepared, InProgress). This serializes
// backups per VM so that each backup completes (including S3 upload) before the
// next one starts, enabling incremental backups through checkpoint chaining.
// The oldest DU (by CreationTimestamp, then UID) always wins.
// DUs older than StaleDataUploadThreshold are skipped to prevent a stuck DU
// from permanently blocking all future backups for a VM.
func (r *KubeVirtDataUploadReconciler) hasOlderActiveDUForVM(ctx context.Context, du *velerov2alpha1.DataUpload) (bool, string, error) {
	logger := log.FromContext(ctx)
	vmName := du.Annotations[common.AnnotationVMName]
	if vmName == "" {
		return false, "", nil
	}
	vmNamespace := du.Annotations[common.AnnotationVMNamespace]
	if vmNamespace == "" {
		vmNamespace = du.Spec.SourceNamespace
	}

	duList := &velerov2alpha1.DataUploadList{}
	if err := r.List(ctx, duList, client.InNamespace(du.Namespace)); err != nil {
		return false, "", fmt.Errorf("failed to list DataUploads: %w", err)
	}

	for i := range duList.Items {
		other := &duList.Items[i]
		if other.UID == du.UID {
			continue
		}
		if other.Spec.DataMover != common.DataMoverKubeVirt {
			continue
		}

		// Must target the same VM
		otherVMName := other.Annotations[common.AnnotationVMName]
		if otherVMName != vmName {
			continue
		}
		otherVMNs := other.Annotations[common.AnnotationVMNamespace]
		if otherVMNs == "" {
			otherVMNs = other.Spec.SourceNamespace
		}
		if otherVMNs != vmNamespace {
			continue
		}

		// Must be in an active (non-terminal) phase
		switch other.Status.Phase {
		case "", velerov2alpha1.DataUploadPhaseNew,
			velerov2alpha1.DataUploadPhaseAccepted,
			velerov2alpha1.DataUploadPhasePrepared,
			velerov2alpha1.DataUploadPhaseInProgress:
		default:
			continue
		}

		if r.StaleDataUploadThreshold > 0 && time.Since(other.CreationTimestamp.Time) > r.StaleDataUploadThreshold {
			logger.Info("Ignoring stale DataUpload that is no longer blocking",
				"staleDU", other.Name, "phase", other.Status.Phase,
				"age", time.Since(other.CreationTimestamp.Time).Round(time.Second))
			continue
		}

		// Older DU takes priority; same timestamp uses UID as tiebreaker
		if other.CreationTimestamp.Before(&du.CreationTimestamp) {
			return true, other.Name, nil
		}
		if other.CreationTimestamp.Equal(&du.CreationTimestamp) && string(other.UID) < string(du.UID) {
			return true, other.Name, nil
		}
	}
	return false, "", nil
}

// checkConcurrentDataMoverLimit gates datamover pod creation against
// MaxConcurrentDataMovers, called from handlePrepared right after the
// existing-pod idempotency check -- before any of the BSL/VMB/PV-rebind
// lookups that only exist to build the pod, since none of that work is
// needed while gated. Returns gated=true (with a RequeueAfterLong result)
// when du must wait its turn; gated=false when it's clear to proceed
// (including when the limit is disabled, MaxConcurrentDataMovers <= 0).
func (r *KubeVirtDataUploadReconciler) checkConcurrentDataMoverLimit(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (gated bool, result ctrl.Result, err error) {
	if r.MaxConcurrentDataMovers <= 0 {
		return false, ctrl.Result{}, nil
	}

	higherPriorityCount, err := r.countHigherPriorityActiveDataUploads(ctx, du)
	if err != nil {
		return false, ctrl.Result{}, err
	}
	if higherPriorityCount < r.MaxConcurrentDataMovers {
		return false, ctrl.Result{}, nil
	}

	logger.Info("Deferring datamover pod creation, at concurrent data mover limit",
		"higherPriorityCount", higherPriorityCount, "limit", r.MaxConcurrentDataMovers)
	// Waiting for a concurrent-data-mover slot is intentional throttling, not a
	// stalled operation -- advance AcceptedTimestamp by the same duration we're
	// about to wait so this deferral doesn't consume the DataUpload's
	// Spec.OperationTimeout budget (checked unconditionally on every reconcile
	// in Reconcile, before dispatch reaches handlePrepared). A genuinely stuck
	// higher-priority peer still ages out normally (its own AcceptedTimestamp
	// is untouched by this), freeing this DU's slot once that peer times out
	// or completes -- so this can't cause indefinite starvation, only push the
	// deadline out for as long as this DU is legitimately waiting its turn.
	if du.Status.AcceptedTimestamp != nil {
		// Re-fetch immediately before writing: this path runs every
		// RequeueAfterLong for as long as du stays gated -- potentially many
		// times over a long queue wait -- far more often than any other write
		// in this reconciler, so it's the one most likely to collide with a
		// concurrent external update (e.g. an annotation patch) and hit a
		// resource-version conflict. A Get failure here only costs this one
		// cycle's timeout-budget exemption, not the gating decision itself, so
		// it's logged and skipped rather than propagated as a reconcile error.
		latest := du.DeepCopy()
		if getErr := r.Get(ctx, client.ObjectKeyFromObject(du), latest); getErr != nil {
			logger.Error(getErr, "Failed to re-fetch DataUpload before advancing AcceptedTimestamp, skipping this cycle's advance")
		} else if latest.Status.AcceptedTimestamp != nil {
			advanced := metav1.NewTime(latest.Status.AcceptedTimestamp.Add(RequeueAfterLong))
			latest.Status.AcceptedTimestamp = &advanced
			if err := r.Update(ctx, latest); err != nil {
				return false, ctrl.Result{}, fmt.Errorf("failed to advance AcceptedTimestamp while gated: %w", err)
			}
			*du = *latest
		}
	}
	return true, ctrl.Result{RequeueAfter: RequeueAfterLong}, nil
}

// countHigherPriorityActiveDataUploads counts kubevirt-datamover DataUploads
// in du's namespace, excluding du itself, that are in an active phase
// (Accepted, Prepared, InProgress) AND outrank du per outranksDataUpload's
// ordering. Used by handlePrepared to gate pod creation against
// MaxConcurrentDataMovers.
//
// Ranking (rather than a raw active-CR count) is what guarantees forward
// progress when N DataUploads all reach Prepared together (e.g. a Backup
// targeting several VMs at once). A raw "count of other active CRs >= limit"
// check is symmetric across all of them: each sibling sees N-1 others
// active, and if N-1 >= limit, every single one defers, forever -- none can
// ever create a pod because reaching InProgress requires passing a gate
// that's held by peers stuck at the very same gate. Ranking breaks that
// symmetry: it's a stable total order (by CreationTimestamp, with UID as a
// final tiebreak for exact ties) that every reconciler computes
// independently from the same List, so exactly the first
// MaxConcurrentDataMovers-ranked siblings ever see a higher-priority count
// below the limit and proceed. As earlier-ranked ones complete (leaving the
// active set), later-ranked siblings' count drops and they get their turn.
// Ranking deliberately does NOT use Status.AcceptedTimestamp:
// checkConcurrentDataMoverLimit's gate-defer path advances that field
// forward to exempt legitimate queue-wait time from Spec.OperationTimeout,
// and CreationTimestamp staying untouched by that is what keeps ranking a
// fixed, fair order instead of one a deferred DU could perturb by
// continuing to wait. New (pre-provisioning) is excluded from the count,
// same as the phase set the full resource window (backup PVC, mover pod)
// actually covers.
func (r *KubeVirtDataUploadReconciler) countHigherPriorityActiveDataUploads(ctx context.Context, du *velerov2alpha1.DataUpload) (int, error) {
	duList := &velerov2alpha1.DataUploadList{}
	if err := r.List(ctx, duList, client.InNamespace(du.Namespace)); err != nil {
		return 0, fmt.Errorf("failed to list DataUploads: %w", err)
	}

	count := 0
	for i := range duList.Items {
		other := &duList.Items[i]
		if other.UID == du.UID {
			continue
		}
		if other.Spec.DataMover != common.DataMoverKubeVirt {
			continue
		}
		switch other.Status.Phase {
		case velerov2alpha1.DataUploadPhaseAccepted,
			velerov2alpha1.DataUploadPhasePrepared,
			velerov2alpha1.DataUploadPhaseInProgress:
		default:
			continue
		}
		if outranksDataUpload(other, du) {
			count++
		}
	}
	return count, nil
}

// outranksDataUpload reports whether a has priority over b for the
// concurrent-data-mover gate: an earlier CreationTimestamp wins, with UID as
// a tiebreaker for exact ties -- same convention as hasOlderActiveDUForVM's
// per-VM serialization. A strict total order over any set of DataUploads,
// computed identically by every reconciler from the same List, is what lets
// countHigherPriorityActiveDataUploads avoid the gate-deadlock a raw active
// count would hit when siblings reach Prepared together.
func outranksDataUpload(a, b *velerov2alpha1.DataUpload) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.UID < b.UID
}

// buildDatamoverPodConfig assembles the configuration for the datamover pod
func (r *KubeVirtDataUploadReconciler) buildDatamoverPodConfig(
	du *velerov2alpha1.DataUpload,
	bsl *velerov1.BackupStorageLocation,
	vmb *kubevirtbackupv1alpha1.VirtualMachineBackup,
	vmRef *common.VMReference,
	backupType string,
	expectedBackupType string,
	checkpointName string,
	vmbtName string,
	rawSize int64,
) (*DatamoverPodConfig, error) {
	cfg, err := uploader.ExtractBSLConfig(bsl)
	if err != nil {
		return nil, err
	}

	if cfg.CredentialName == "" {
		return nil, fmt.Errorf("BSL %s has no credential secret configured", bsl.Name)
	}

	// Determine datamover image
	image := r.DatamoverImage
	if image == "" {
		return nil, fmt.Errorf("datamover image not configured")
	}

	pullPolicy := r.DatamoverImagePullPolicy
	if pullPolicy == "" {
		pullPolicy = corev1.PullAlways
	}

	ssecName, ssecKey, err := parseSSECSecretRef(cfg.CustomerKeyEncryptionSecret)
	if err != nil {
		return nil, err
	}

	return &DatamoverPodConfig{
		OperationMode:                  OperationModeUpload,
		Name:                           du.Name, // Used as a prefix for GenerateName
		Namespace:                      vmRef.Namespace,
		Image:                          image,
		ImagePullPolicy:                pullPolicy,
		BSLProvider:                    cfg.Provider,
		BSLBucket:                      cfg.Bucket,
		BSLPrefix:                      cfg.Prefix,
		BSLRegion:                      cfg.Region,
		BSLS3URL:                       cfg.S3URL,
		BSLS3ForcePathStyle:            strconv.FormatBool(cfg.S3ForcePathStyle),
		BSLInsecureSkipTLSVerify:       strconv.FormatBool(cfg.InsecureSkipTLSVerify),
		BSLCACert:                      cfg.CACert,
		BSLServerSideEncryption:        cfg.ServerSideEncryption,
		BSLKMSKeyID:                    cfg.KMSKeyID,
		BSLChecksumAlgorithm:           cfg.ChecksumAlgorithm,
		BSLProfile:                     cfg.Profile,
		SSECSecretName:                 ssecName,
		SSECSecretKey:                  ssecKey,
		BSLServiceAccount:              cfg.ServiceAccount,
		BSLKMSKeyName:                  cfg.KMSKeyName,
		BSLResourceGroup:               cfg.ResourceGroup,
		BSLStorageAccount:              cfg.StorageAccount,
		BSLStorageAccountKeyEnvVar:     cfg.StorageAccountKeyEnvVar,
		BSLStorageAccountURI:           cfg.StorageAccountURI,
		BSLSubscriptionID:              cfg.SubscriptionID,
		BSLUseAAD:                      strconv.FormatBool(cfg.UseAAD),
		BSLActiveDirectoryAuthorityURI: cfg.ActiveDirectoryAuthorityURI,
		CredentialSecretName:           cfg.CredentialName,
		CredentialSecretKey:            cfg.CredentialKey,
		VMName:                         vmRef.Name,
		VMNamespace:                    vmRef.Namespace,
		CheckpointName:                 checkpointName,
		BackupType:                     backupType,
		ExpectedBackupType:             expectedBackupType,
		VeleroBackupName:               getVeleroBackupName(du.Labels),
		ResourceName:                   du.Name,
		ResourceUID:                    string(du.UID),
		UIDLabelKey:                    common.LabelDataUploadUID,
		NameAnnotationKey:              common.AnnotationDataUploadName,
		VMBName:                        vmb.Name,
		VMBTName:                       vmbtName,
		SourcePVCName:                  "", // overridden by handlePrepared with the rebound PVC name
		TotalBytes:                     rawSize,
		Labels:                         make(map[string]string),
	}, nil
}

// parseSSECSecretRef parses a "secretName/key" reference into its components.
// Returns ("", "", nil) when ref is empty (SSE-C not configured).
// Returns an error for malformed references.
func parseSSECSecretRef(ref string) (secretName, key string, err error) {
	if ref == "" {
		return "", "", nil
	}
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid customerKeyEncryptionSecret %q: must be in secretName/key format", ref)
	}
	return parts[0], parts[1], nil
}

// lookupCheckpointFromBSL reads the VM's checkpoint index from the BSL and returns
// the latest valid checkpoint for incremental backup support.
func (r *KubeVirtDataUploadReconciler) lookupCheckpointFromBSL(ctx context.Context, bsl *velerov1.BackupStorageLocation, vmNamespace, vmName string) (*uploader.CheckpointLookupResult, error) {
	store, cfg, err := uploader.InitObjectStoreFromBSL(ctx, r.Client, r.OADPNamespace, bsl, r.ObjectStoreFactory)
	if err != nil {
		return nil, err
	}

	// Lookup the latest checkpoint
	result, err := uploader.LookupLatestCheckpoint(ctx, store, cfg.Bucket, vmNamespace, vmName)
	if err != nil {
		return nil, fmt.Errorf("checkpoint lookup failed: %w", err)
	}

	return result, nil
}

// findPodForDataUpload finds the unique datamover pod associated with a DataUpload.
func (r *KubeVirtDataUploadReconciler) findPodForDataUpload(ctx context.Context, du *velerov2alpha1.DataUpload, namespace string) (*corev1.Pod, error) {
	return findPodByUID(ctx, r.Client, r.APIReader, common.LabelDataUploadUID, string(du.UID), namespace)
}

// getRawBackupSize determines the raw capacity of the source PVCs without any overhead.
func (r *KubeVirtDataUploadReconciler) getRawBackupSize(ctx context.Context, du *velerov2alpha1.DataUpload, namespace string) int64 {
	sourceNamespace := du.Spec.SourceNamespace
	if sourceNamespace == "" {
		sourceNamespace = namespace
	}

	vmRef, err := common.GetVMReference(du)
	if err == nil {
		vm := &kubevirtcorev1.VirtualMachine{}
		if err := r.Get(ctx, types.NamespacedName{Name: vmRef.Name, Namespace: vmRef.Namespace}, vm); err == nil {
			pvcNames := common.GetVolumesForVm(vm)
			if len(pvcNames) > 0 {
				totalCapacity := resource.Quantity{}
				for _, pvcName := range pvcNames {
					pvc := &corev1.PersistentVolumeClaim{}
					if err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: vmRef.Namespace}, pvc); err == nil {
						if capacity, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok {
							totalCapacity.Add(capacity)
						} else if req, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
							totalCapacity.Add(req)
						}
					}
				}
				if !totalCapacity.IsZero() {
					return totalCapacity.Value()
				}
			}
		}
	}

	sourcePVCName := du.Spec.SourcePVC
	if sourcePVCName != "" {
		sourcePVC := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Name: sourcePVCName, Namespace: sourceNamespace}, sourcePVC); err == nil {
			if capacity, ok := sourcePVC.Status.Capacity[corev1.ResourceStorage]; ok {
				return capacity.Value()
			} else if req, ok := sourcePVC.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
				return req.Value()
			}
		}
	}

	defaultSize := resource.MustParse(DefaultTempPVCSize)
	return defaultSize.Value()
}
