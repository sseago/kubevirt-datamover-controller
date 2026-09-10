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
	"context"
	stderrors "errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/downloader"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// DefaultScratchPVCSize is the fallback size for the scratch PVC when neither
// the matched checkpoint chain's PVCSizes nor CheckpointFile.Size are populated
// (older/partial index). This is a much weaker guess than the sized path, so
// calculateScratchPVCSize logs loudly when it falls back to this value.
const DefaultScratchPVCSize = "10Gi"

// AnnotationTargetDiskName is the annotation key used to persist the resolved
// KubeVirt disk/volume name (translated from dd.Spec.TargetVolume.PVC via
// resolveTargetDiskName in handleAccepted) across reconciles, so handlePrepared
// can build the downloader pod's env vars without re-reading the manifest/index
// from S3. Mirrors DataUpload's AnnotationVMBTName pattern.
const AnnotationTargetDiskName = "kubevirt-datamover.io/target-disk-name"

// AnnotationRestoreBlockMode records whether dd's restore target PVC is Block
// volumeMode, decided once in handleAccepted from the target PVC's own
// VolumeMode (immutable once a PVC is created) and persisted across
// reconciles so later phases (handlePrepared, completeSuccessfulDownload)
// know whether a single Filesystem-mode scratch PVC or a work+output PVC pair
// was provisioned, without needing the target PVC object themselves. See
// isBlockModeRestore and DatamoverPodConfig.OutputPVCName's doc comment.
const AnnotationRestoreBlockMode = "kubevirt-datamover.io/restore-block-mode"

// AnnotationDownloaderPodSucceeded records that this DataDownload's downloader
// pod reached PodSucceeded, persisted on the DataDownload BEFORE the pod is
// deleted ahead of the rebind. Without it, the success is only recorded in the
// pod object itself, which the success path destroys: cleanupPodsByUID reports
// not-ready while the pod is still terminating (routine with a cached client, whose
// re-list can't see the deletion yet), so the retry reconcile regularly arrives
// after the pod object is fully gone -- and would misread that absence as
// "Downloader pod not found", terminally failing a restore whose download
// actually succeeded. handleInProgress consults this marker in its pod-absent
// branch to resume the rebind instead.
const AnnotationDownloaderPodSucceeded = "kubevirt-datamover.io/downloader-pod-succeeded"

// downloaderPodSucceededValue is the value AnnotationDownloaderPodSucceeded is
// set to; anything else (including absence) means "not recorded as succeeded".
const downloaderPodSucceededValue = "true"

// KubeVirtDataDownloadReconciler reconciles DataDownload objects where Spec.DataMover is "kubevirt"
type KubeVirtDataDownloadReconciler struct {
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

	// EventRecorder emits Kubernetes Events on DataDownload objects. May be nil,
	// in which case event emission is skipped.
	EventRecorder record.EventRecorder

	// OADPNamespace is the namespace where OADP and Velero resources are located
	OADPNamespace string

	// MaxConcurrentReconciles is the maximum number of concurrent Reconciles which can be run
	MaxConcurrentReconciles int

	// MaxConcurrentDataMovers caps how many DataDownloads may be in an active
	// phase (Accepted/Prepared/InProgress) at once, gating pod creation in
	// handlePrepared -- covers the full resource window (scratch/work/output
	// PVCs + pod), not just running pods. 0 (default) disables the limit.
	// This is a soft limit: the active count is read from the cached client,
	// so with MaxConcurrentReconciles > 1 two reconciles can observe the same
	// pre-update count and both proceed, briefly overshooting the cap by up
	// to MaxConcurrentReconciles-1. Acceptable for a throttle; not a hard quota.
	MaxConcurrentDataMovers int

	// DatamoverImage is the image to use for datamover pods
	DatamoverImage string

	// DatamoverImagePullPolicy is the pull policy for the datamover image
	DatamoverImagePullPolicy corev1.PullPolicy

	// ObjectStoreFactory creates an ObjectStore from an ObjectStoreConfig.
	// Defaults to uploader.InitObjectStore if nil. Override in tests to inject mocks.
	ObjectStoreFactory func(cfg *common.ObjectStoreConfig) (velero.ObjectStore, error)

	// PodLogCollector reads logs from a completed datamover pod.
	// If nil, pod log collection is skipped. Override in tests to inject mocks.
	PodLogCollector func(ctx context.Context, podName, podNamespace string) (string, error)
}

// +kubebuilder:rbac:groups=velero.io,resources=datadownloads,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=velero.io,resources=datadownloads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=velero.io,resources=backupstoragelocations,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines,verbs=get;update

// Reconcile handles DataDownload resources where Spec.DataMover is "kubevirt"
func (r *KubeVirtDataDownloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	dataDownload := &velerov2alpha1.DataDownload{}
	if err := r.Get(ctx, req.NamespacedName, dataDownload); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if dataDownload.Spec.DataMover != common.DataMoverKubeVirt {
		logger.V(1).Info("Skipping DataDownload - DataMover is not kubevirt",
			"dataDownload", req.NamespacedName,
			"dataMover", dataDownload.Spec.DataMover)
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling DataDownload with kubevirt datamover",
		"dataDownload", req.NamespacedName,
		"phase", dataDownload.Status.Phase)

	// Handle cancellation requests before the normal phase dispatch: Spec.Cancel
	// can be set at any point while the restore is active, and must route into
	// Canceling regardless of which phase handler would otherwise run.
	switch dataDownload.Status.Phase {
	case velerov2alpha1.DataDownloadPhaseCompleted,
		velerov2alpha1.DataDownloadPhaseFailed,
		velerov2alpha1.DataDownloadPhaseCanceled,
		velerov2alpha1.DataDownloadPhaseCanceling:
		// Already terminal or already canceling -- fall through to normal dispatch.
	default:
		if dataDownload.Spec.Cancel {
			// A cancel request can race with a restore that already finished
			// provisioning the target volume (e.g. Completed didn't persist yet
			// after a successful rebind). Honoring Cancel at that point would
			// misreport a successful restore as canceled while leaving the
			// already-rebound data in place -- finalize as Completed instead.
			if done, err := r.isRestoreAlreadyProvisioned(ctx, dataDownload); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to check restore completion state: %w", err)
			} else if done {
				logger.Info("Cancel requested but restore already provisioned the target volume, completing instead")
				if err := r.updatePhase(ctx, dataDownload, velerov2alpha1.DataDownloadPhaseCompleted, "Restored disk provisioned to target volume"); err != nil {
					return ctrl.Result{}, err
				}
				if cleanupNotReady, _ := cleanupPodsByUID(ctx, r.Client, r.APIReader, common.LabelDataDownloadUID, string(dataDownload.UID), r.getPodNamespace(dataDownload), logger); cleanupNotReady {
					logger.Info("Datamover pod still terminating (or its status couldn't be confirmed)")
					// Continue -- the restore already completed, don't block on cleanup failures
				}
				r.cleanupScratchPVCIfPresent(ctx, dataDownload, logger)
				return ctrl.Result{}, nil
			}
			if err := r.updatePhase(ctx, dataDownload, velerov2alpha1.DataDownloadPhaseCanceling, "Cancellation requested"); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
		}
	}

	// Bound how long a DataDownload may sit in a non-terminal phase after being
	// Accepted: without this, any of the several unbounded-requeue branches below
	// (target PVC never appearing, downloader pod never reaching a terminal state,
	// etc.) would retry forever instead of eventually failing per Spec.OperationTimeout.
	timeoutBound := isDataDownloadTimeoutBound(dataDownload.Status.Phase)
	if timeoutBound {
		// Mirrors the Cancel-vs-provisioned race handled above: a restore that
		// already rebound the target volume (Completed didn't persist yet after a
		// successful rebind, e.g. a transient API error) must not be failed by an
		// expired timeout -- let handleInProgress's own idempotent-resume check
		// finalize it as Completed instead.
		provisioned := false
		if dataDownload.Status.Phase == velerov2alpha1.DataDownloadPhaseInProgress {
			done, err := r.isRestoreAlreadyProvisioned(ctx, dataDownload)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to check restore completion state: %w", err)
			}
			provisioned = done
		}
		if !provisioned {
			if failed, err := r.checkOperationTimeout(ctx, logger, dataDownload); err != nil {
				if stderrors.Is(err, ErrPodsStillTerminating) {
					// Expected, self-resolving: kubelet just hasn't finished tearing the
					// stalled pod down yet. Requeue quickly without logging a reconcile
					// error or triggering controller-runtime's exponential backoff for
					// something that isn't wrong -- matches handleCanceling's treatment
					// of the same error. The DataDownload isn't marked Failed yet; the
					// next reconcile re-enters this same timeout check and retries the
					// fail callback (including re-persisting Failed) once cleanup succeeds.
					logger.V(1).Info("Datamover pod(s) still terminating during timeout cleanup, will retry", "error", err)
					return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
				}
				return ctrl.Result{}, err
			} else if failed {
				return ctrl.Result{}, nil
			}
		}
	}

	var result ctrl.Result
	var err error
	switch dataDownload.Status.Phase {
	case "", velerov2alpha1.DataDownloadPhaseNew:
		result, err = r.handleNew(ctx, logger, dataDownload)

	case velerov2alpha1.DataDownloadPhaseAccepted:
		result, err = r.handleAccepted(ctx, logger, dataDownload)

	case velerov2alpha1.DataDownloadPhasePrepared:
		result, err = r.handlePrepared(ctx, logger, dataDownload)

	case velerov2alpha1.DataDownloadPhaseInProgress:
		result, err = r.handleInProgress(ctx, logger, dataDownload)

	case velerov2alpha1.DataDownloadPhaseCanceling:
		return r.handleCanceling(ctx, logger, dataDownload)

	case velerov2alpha1.DataDownloadPhaseCompleted:
		logger.V(1).Info("DataDownload is in terminal state", "phase", dataDownload.Status.Phase)
		if err := r.restoreVMRunStateIfAllSiblingsCompleted(ctx, logger, dataDownload); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case velerov2alpha1.DataDownloadPhaseFailed,
		velerov2alpha1.DataDownloadPhaseCanceled:
		logger.V(1).Info("DataDownload is in terminal state", "phase", dataDownload.Status.Phase)
		return ctrl.Result{}, nil

	default:
		logger.Info("Unknown DataDownload phase", "phase", dataDownload.Status.Phase)
		return ctrl.Result{}, nil
	}

	// Cap the handler's RequeueAfter to the operation deadline. The condition keys off
	// dataDownload.Status.AcceptedTimestamp (rather than the pre-dispatch timeoutBound)
	// so a New DataDownload that handleNew just transitioned to Accepted in this same
	// reconcile -- setting AcceptedTimestamp along the way -- gets its first
	// RequeueAfter capped too, not just subsequent reconciles.
	if err == nil && dataDownload.Status.AcceptedTimestamp != nil {
		result = capRequeueToOperationDeadline(result, dataDownload.Status.AcceptedTimestamp, dataDownload.Spec.OperationTimeout.Duration)
	}
	return result, err
}

// handleNew processes DataDownloads in New phase.
// Validates the VM identity annotations and BSL accessibility, then transitions to Accepted.
// Unlike DataUpload's handleNew, this never fetches a live VirtualMachine object: nothing in
// the restore data path needs one, since VMName/VMNamespace are consumed only as S3-path
// string components (manifest/index lookups), not as a liveness check.
func (r *KubeVirtDataDownloadReconciler) handleNew(ctx context.Context, logger logr.Logger, dd *velerov2alpha1.DataDownload) (ctrl.Result, error) {
	logger.Info("Handling New phase DataDownload")

	if _, err := common.GetVMReferenceFromDataDownload(dd); err != nil {
		logger.Error(err, "Failed to get VM reference from DataDownload")
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, fmt.Sprintf("Missing VM reference: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if dd.Spec.TargetVolume.PVC == "" || dd.Spec.TargetVolume.Namespace == "" {
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed,
			"Spec.TargetVolume.PVC and Spec.TargetVolume.Namespace must both be set"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	bsl, err := r.getBackupStorageLocationForDD(ctx, dd)
	if err != nil {
		if isTransientBSLLookupError(err, dd.Spec.BackupStorageLocation) {
			return ctrl.Result{}, fmt.Errorf("failed to get BackupStorageLocation: %w", err)
		}
		logger.Error(err, "BackupStorageLocation not accessible")
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed,
			fmt.Sprintf("BackupStorageLocation not accessible: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	if bsl.Status.Phase != velerov1.BackupStorageLocationPhaseAvailable {
		logger.Error(nil, "BackupStorageLocation is not in Available phase",
			"bsl", bsl.Name, "phase", bsl.Status.Phase)
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed,
			fmt.Sprintf("BackupStorageLocation %q is not available (phase: %s)", bsl.Name, bsl.Status.Phase)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Record when this DataDownload was accepted so checkOperationTimeout can bound
	// how long it's allowed to remain non-terminal against Spec.OperationTimeout.
	now := metav1.Now()
	dd.Status.AcceptedTimestamp = &now

	if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseAccepted, "DataDownload accepted by kubevirt datamover"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// isDataDownloadTimeoutBound reports whether phase is one of the non-terminal
// phases (Accepted, Prepared, InProgress) subject to Spec.OperationTimeout
// enforcement. Kept as the single source of truth for that phase set so a
// future phase added to the dispatch switch below can't silently drift out of
// sync with which phases get timeout-checked.
func isDataDownloadTimeoutBound(phase velerov2alpha1.DataDownloadPhase) bool {
	switch phase {
	case velerov2alpha1.DataDownloadPhaseAccepted,
		velerov2alpha1.DataDownloadPhasePrepared,
		velerov2alpha1.DataDownloadPhaseInProgress:
		return true
	default:
		return false
	}
}

// checkOperationTimeout fails dd if too much time has elapsed since it was
// accepted, per Spec.OperationTimeout (falling back to DefaultOperationTimeout
// when unset). Self-heals a missing AcceptedTimestamp -- e.g. a DataDownload
// already past New when this check was introduced -- by backfilling it to now
// rather than leaving the operation unbounded forever. A thin adapter over
// checkOperationTimeoutCore, which DataUpload's own checkOperationTimeout
// shares -- see that method's doc comment for why the core logic isn't
// directly shared as a method on a common type.
func (r *KubeVirtDataDownloadReconciler) checkOperationTimeout(ctx context.Context, logger logr.Logger, dd *velerov2alpha1.DataDownload) (failed bool, err error) {
	return checkOperationTimeoutCore(ctx, logger, "DataDownload", operationTimeoutTarget{
		acceptedTimestamp:    func() *metav1.Time { return dd.Status.AcceptedTimestamp },
		setAcceptedTimestamp: func(t *metav1.Time) { dd.Status.AcceptedTimestamp = t },
		operationTimeout:     dd.Spec.OperationTimeout.Duration,
		phase:                func() string { return string(dd.Status.Phase) },
		persist:              func(ctx context.Context) error { return r.Update(ctx, dd) },
		fail: func(ctx context.Context, message string) error {
			// Capture the stalled pod's logs before deleting it -- on a timeout the
			// pod is usually still running, and its logs are the only evidence of
			// why it stalled. Best-effort: a lookup failure here shouldn't block
			// the actual cleanup/fail below.
			if pod, findErr := r.findPodForDataDownload(ctx, dd, r.getPodNamespace(dd)); findErr == nil && pod != nil {
				r.emitPodLogs(ctx, logger, pod)
			}
			// Stop the still-running downloader pod BEFORE persisting Failed, and
			// propagate a cleanup failure rather than swallowing it: a timeout can
			// fire while the pod is still Pending/Running (that's exactly the
			// unbounded-wait branch this timeout guards against), unlike the other
			// Failed paths where the pod has already terminated on its own. Failed
			// is a dead-end terminal state with no further reconciliation, so
			// persisting it before cleanup actually succeeds would leave the pod
			// running forever with no chance to retry -- returning the error here
			// instead lets the reconcile retry until cleanup succeeds.
			if cleanupNotReady, terminating := cleanupPodsByUID(ctx, r.Client, r.APIReader, common.LabelDataDownloadUID, string(dd.UID), r.getPodNamespace(dd), logger); cleanupNotReady {
				if terminating {
					return fmt.Errorf("downloader pod still terminating before failing DataDownload on timeout: %w", ErrPodsStillTerminating)
				}
				return fmt.Errorf("downloader pod cleanup could not be confirmed before failing DataDownload on timeout")
			}
			return r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, message)
		},
	})
}

// handleAccepted processes DataDownloads in Accepted phase.
// Validates the restore target PVC exists, resolves the checkpoint chain from the
// backup manifest, and creates a sized scratch PVC before transitioning to Prepared.
//
//nolint:gocyclo // Phase handler with necessary validation steps
func (r *KubeVirtDataDownloadReconciler) handleAccepted(ctx context.Context, logger logr.Logger, dd *velerov2alpha1.DataDownload) (ctrl.Result, error) {
	logger.Info("Handling Accepted phase DataDownload")

	vmRef, err := common.GetVMReferenceFromDataDownload(dd)
	if err != nil {
		logger.Error(err, "Failed to get VM reference")
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, fmt.Sprintf("Missing VM reference: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// The target PVC is created by Velero's restore before this controller acts
	// (per the vendored TargetVolumeSpec doc comment). Requeue rather than fail if
	// it isn't visible yet -- ordering between Velero's item restore and this
	// controller's reconciliation isn't guaranteed.
	targetPVC := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Name: dd.Spec.TargetVolume.PVC, Namespace: dd.Spec.TargetVolume.Namespace}, targetPVC); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Target PVC not found yet, will retry",
				"pvc", dd.Spec.TargetVolume.PVC, "namespace", dd.Spec.TargetVolume.Namespace)
			return ctrl.Result{RequeueAfter: RequeueAfterLong}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get target PVC: %w", err)
	}

	// The final rebind patches the scratch PV's claimRef onto this PVC. A
	// matchLabels-only Spec.Selector is accepted -- validateExistingPVCForBind
	// (called from completeSuccessfulDownload's rebindPVToNamespace) reconciles
	// the rebound PV's labels to satisfy it rather than treating it as a
	// conflict. This is the same technique Velero's own built-in CSI
	// DataDownload restore path uses (velero.io/dynamic-pv-restore) to keep the
	// dynamic provisioner from racing this rebind, regardless of the target
	// StorageClass's volumeBindingMode. Only matchExpressions has no general
	// way to be satisfied, so that shape is still rejected here, before any
	// download work, instead of after a full download fails to bind.
	if targetPVC.Spec.Selector != nil && len(targetPVC.Spec.Selector.MatchExpressions) > 0 {
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed,
			fmt.Sprintf("target PVC %s/%s declares spec.selector.matchExpressions, which is not supported for restore rebinding (only matchLabels)",
				dd.Spec.TargetVolume.Namespace, dd.Spec.TargetVolume.PVC)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// No scratch PV exists yet at this point in the phase machine, so any
	// pre-existing bind (or a pending bind request via Spec.VolumeName) on the
	// target PVC can only be fatal at the eventual rebind step -- e.g. an
	// Immediate-binding StorageClass lets the provisioner bind this PVC to a
	// foreign PV within seconds of Velero creating it, long before the download
	// completes. Reject it now rather than after a full download only to fail
	// in validateExistingPVCForBind. This often occurs when attempting to restore
	// a VM that already exists, which the KubeVirt datamover does not support.
	if targetPVC.Spec.VolumeName != "" || targetPVC.Status.Phase == corev1.ClaimBound {
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed,
			fmt.Sprintf("target PVC %s/%s is already bound or requests volume %q, which conflicts with restore rebinding (this may indicate an attempt to restore a VM that already exists, which the KubeVirt datamover does not support)",
				dd.Spec.TargetVolume.Namespace, dd.Spec.TargetVolume.PVC, targetPVC.Spec.VolumeName)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	bsl, err := r.getBackupStorageLocationForDD(ctx, dd)
	if err != nil {
		if isTransientBSLLookupError(err, dd.Spec.BackupStorageLocation) {
			return ctrl.Result{}, fmt.Errorf("failed to get BackupStorageLocation: %w", err)
		}
		logger.Error(err, "Failed to get BackupStorageLocation")
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, fmt.Sprintf("Failed to get BSL: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	store, cfg, err := uploader.InitObjectStoreFromBSL(ctx, r.Client, r.OADPNamespace, bsl, r.ObjectStoreFactory)
	if err != nil {
		logger.Error(err, "Failed to initialize object store")
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, fmt.Sprintf("Failed to initialize object store: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	veleroBackupName := getVeleroBackupName(dd.Labels)
	if veleroBackupName == "" {
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed,
			fmt.Sprintf("DataDownload missing %s label", common.LabelVeleroBackupName)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	manifest, found, err := uploader.GetVMBackupManifest(store, vmRef.Namespace, vmRef.Name, veleroBackupName, cfg.Bucket, logger)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to read VM backup manifest: %w", err)
	}
	if !found || len(manifest.CheckpointChain) == 0 {
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed,
			fmt.Sprintf("no backup manifest found for VM %s/%s in backup %s", vmRef.Namespace, vmRef.Name, veleroBackupName)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	vmIndex, found, err := uploader.GetVMIndex(store, vmRef.Namespace, vmRef.Name, cfg.Bucket, logger)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to read VM index: %w", err)
	}
	if !found {
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed,
			fmt.Sprintf("no VM index found for VM %s/%s", vmRef.Namespace, vmRef.Name)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// dd.Spec.TargetVolume.PVC is a PVC/claim name (Velero's restore-created target),
	// but pkg/downloader.ResolveCheckpointFiles matches on CheckpointFile.DiskName --
	// the KubeVirt volume name recorded at backup time, which differs from the PVC
	// name whenever a VM's volume reference doesn't literally match its backing
	// PVC/DataVolume name. Translate PVC name -> disk name using this backup's own
	// index (CheckpointEntry.PVCs/Files are index-aligned) rather than assuming
	// they're the same string.
	targetVolume, err := resolveTargetDiskName(vmIndex, manifest.CheckpointChain, dd.Spec.TargetVolume.PVC)
	if err != nil {
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed,
			fmt.Sprintf("Failed to resolve target disk name: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	files, err := downloader.ResolveCheckpointFiles(vmRef.Namespace, vmRef.Name, manifest.CheckpointChain, vmIndex, targetVolume)
	if err != nil {
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed,
			fmt.Sprintf("Failed to resolve checkpoint chain: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// The target PVC's own requested capacity is an authoritative floor for the
	// raw-disk-size component of the scratch sizing calculation (see
	// calculateScratchPVCSize) -- it must be folded in BEFORE the qcow2 chain size
	// and overhead are added, not clamped onto the final result afterward, since
	// the scratch volume needs room for the chain AND the full raw image at once.
	var targetDiskCapacity resource.Quantity
	if req, ok := targetPVC.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		targetDiskCapacity = req
	}
	// A Block-mode target needs two scratch PVCs instead of one -- see
	// DatamoverPodConfig.ScratchPVCName/OutputPVCName's doc comments for the
	// work/output split rationale. Sizing splits the same way: the work PVC
	// only needs room for the qcow2 chain (no final-image component), and the
	// output PVC only needs room for the flattened raw image (no chain-file
	// component), whereas a Filesystem-mode target's single scratch PVC needs
	// both at once (calculateScratchPVCSize's existing behavior, unchanged).
	isBlockTarget := targetPVC.Spec.VolumeMode != nil && *targetPVC.Spec.VolumeMode == corev1.PersistentVolumeBlock
	if isBlockTarget {
		workSize := calculateWorkPVCSize(logger, files)
		if _, err := r.ensureWorkPVC(ctx, logger, dd, targetPVC, workSize); err != nil {
			if failed, updateErr := r.failIfImmutableScratchPVCMismatch(ctx, dd, err); failed {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, fmt.Errorf("failed to ensure work PVC: %w", err)
		}
		outputSize := calculateOutputPVCSize(logger, vmIndex, manifest.CheckpointChain, targetVolume, targetDiskCapacity)
		if _, err := r.ensureOutputPVC(ctx, logger, dd, targetPVC, outputSize); err != nil {
			if failed, updateErr := r.failIfImmutableScratchPVCMismatch(ctx, dd, err); failed {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, fmt.Errorf("failed to ensure output PVC: %w", err)
		}
	} else {
		scratchPVCSize := calculateScratchPVCSize(logger, vmIndex, manifest.CheckpointChain, targetVolume, files, targetDiskCapacity)
		if _, err := r.ensureScratchPVC(ctx, logger, dd, targetPVC, scratchPVCSize); err != nil {
			if failed, updateErr := r.failIfImmutableScratchPVCMismatch(ctx, dd, err); failed {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, fmt.Errorf("failed to ensure scratch PVC: %w", err)
		}
	}

	// Persist the resolved disk name so handlePrepared (a later, separate reconcile)
	// can build the downloader pod's KUBEVIRT_DM_TARGET_VOLUME without re-reading the
	// manifest/index from S3. Mirrors DataUpload's AnnotationVMBTName pattern. Set in
	// memory here and persisted together with the Prepared phase transition below in
	// a single r.Update call, rather than two separate API writes.
	if dd.Annotations == nil {
		dd.Annotations = make(map[string]string)
	}
	dd.Annotations[AnnotationTargetDiskName] = targetVolume
	if isBlockTarget {
		dd.Annotations[AnnotationRestoreBlockMode] = "true"
	}

	if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhasePrepared, "Checkpoint chain resolved, scratch PVC ready"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// resolveTargetDiskName translates a PVC/claim name into the KubeVirt volume/disk
// name paired with it in the backup index, by locating a checkpoint in the chain
// whose PVCs list contains targetPVCName and reading the Files entry at the same
// index (CheckpointEntry.PVCs/Files/PVCSizes are index-aligned per checkpoint,
// per pkg/uploader.updateVMIndex). This is necessary because
// pkg/downloader.ResolveCheckpointFiles matches on CheckpointFile.DiskName (the
// KubeVirt volume name), not the PVC name.
//
// chain is ordered full-backup-first, incrementals-after (see
// pkg/downloader.types.go's CheckpointChain doc comment); a VM's disk-to-PVC
// mapping is normally stable across a chain, but if it ever changed (e.g. a
// differently-named DataVolume reattached to the same PVC slot between
// incrementals), the newest (chain-tip) checkpoint's mapping is authoritative,
// not whichever checkpoint happens to be found first. Iterate the chain in its
// given order and keep the last (most recent) match rather than returning on
// the first one found -- including when the most recent match is itself
// malformed (missing file entry or empty disk name): that supersedes an
// earlier, valid match rather than being silently shadowed by it, and a
// malformed entry earlier in the chain doesn't block a later, valid one from
// being found.
func resolveTargetDiskName(vmIndex uploader.VMIndex, chain []string, targetPVCName string) (string, error) {
	entriesByID := make(map[string]*uploader.CheckpointEntry, len(vmIndex.Checkpoints))
	for i := range vmIndex.Checkpoints {
		entriesByID[vmIndex.Checkpoints[i].ID] = &vmIndex.Checkpoints[i]
	}

	var diskName string
	var lastErr error
	for _, id := range chain {
		entry, ok := entriesByID[id]
		if !ok {
			continue
		}
		// A single checkpoint's PVCs list isn't expected to name targetPVCName
		// more than once, but if it ever does (e.g. the same PVC attached as two
		// VM volumes), prefer whichever duplicate is well-formed over one that
		// isn't -- unlike the cross-checkpoint case above, there's no chronological
		// reason to let a malformed duplicate shadow a valid one within the same
		// checkpoint, so scan all of them before deciding this entry's result.
		var entryDiskName string
		var entryErr error
		for i, pvcName := range entry.PVCs {
			if pvcName != targetPVCName {
				continue
			}
			if i >= len(entry.Files) {
				if entryDiskName == "" && entryErr == nil {
					entryErr = fmt.Errorf("checkpoint %q has PVC %q at index %d but no matching file entry", entry.ID, targetPVCName, i)
				}
				continue
			}
			if entry.Files[i].DiskName == "" {
				if entryDiskName == "" && entryErr == nil {
					entryErr = fmt.Errorf("checkpoint %q has PVC %q at index %d with an empty disk name", entry.ID, targetPVCName, i)
				}
				continue
			}
			entryDiskName = entry.Files[i].DiskName
			entryErr = nil
		}
		switch {
		case entryDiskName != "":
			diskName = entryDiskName
			lastErr = nil
		case entryErr != nil:
			diskName = ""
			lastErr = entryErr
		}
	}

	if diskName == "" {
		if lastErr != nil {
			return "", lastErr
		}
		return "", fmt.Errorf("target PVC %q not found in any checkpoint's PVCs list", targetPVCName)
	}
	return diskName, nil
}

// calculateScratchPVCSize sizes the scratch PVC to hold both the downloaded qcow2
// chain and the flattened raw output simultaneously (the downloader keeps both on
// disk at once -- cleanup of the qcow2 chain only happens after the raw image is
// fully written). Sized from the max original-disk capacity (CheckpointEntry.PVCSizes
// across the matched chain, floored by targetDiskCapacity -- the restore target PVC's
// own authoritative requested size) plus the sum of the resolved checkpoint files'
// sizes, plus sizeOverheadPercent headroom. targetDiskCapacity is folded in as the
// floor for the raw-disk-size component BEFORE the chain size and overhead are
// added (not clamped onto the final result), since the scratch volume must hold
// both simultaneously. Falls back to DefaultScratchPVCSize (logged loudly) only if
// neither PVCSizes, targetDiskCapacity, nor file sizes are populated.
//
// targetVolume here is the KubeVirt disk name (matching CheckpointFile.DiskName,
// the same field pkg/downloader.ResolveCheckpointFiles matches on) -- NOT the PVC
// name -- so PVCSizes is looked up via the index-aligned Files entry rather than
// matching CheckpointEntry.PVCs directly, keeping this in lockstep with
// ResolveCheckpointFiles's own matching key.
func calculateScratchPVCSize(logger logr.Logger, vmIndex uploader.VMIndex, chain []string, targetVolume string, files []uploader.CheckpointFile, targetDiskCapacity resource.Quantity) resource.Quantity {
	minSize := resource.MustParse("1Gi")
	defaultSize := resource.MustParse(DefaultScratchPVCSize)

	maxDiskSize := maxDiskSizeFromIndex(vmIndex, chain, targetVolume, targetDiskCapacity)

	var totalFileSize int64
	for _, f := range files {
		if f.Size < 0 {
			// A negative CheckpointFile.Size can only come from corrupt/invalid
			// index data -- never a real file. Treat it as absent (0) rather than
			// letting it subtract from the total: summing it in could silently
			// undersize the scratch PVC below what the OTHER, legitimate files in
			// the same list actually need (the 1Gi floor below only protects
			// against an all-negative/all-absent list, not a mixed one).
			logger.Info("Ignoring negative checkpoint file size", "targetVolume", targetVolume, "size", f.Size)
			continue
		}
		totalFileSize += f.Size
	}

	if maxDiskSize.IsZero() {
		// Without a raw-disk-size floor (no PVCSizes metadata and no target PVC
		// capacity), the checkpoint chain's own size can't be trusted to cover
		// the flattened raw disk the downloader writes alongside it. Fall back
		// to the default, unless the chain doubled (chain + flattened raw
		// coexist on the same scratch volume) plus overhead already exceeds it
		// -- in which case size off that instead of an insufficient default.
		doubledWithOverhead := addOverhead(*resource.NewQuantity(totalFileSize*2, resource.BinarySI), sizeOverheadPercent)
		if defaultSize.Cmp(doubledWithOverhead) < 0 {
			logger.Info("No PVC size or target capacity metadata available, sizing scratch PVC from doubled chain size",
				"targetVolume", targetVolume, "totalCheckpointFileSize", totalFileSize, "finalSize", doubledWithOverhead.String())
			return doubledWithOverhead
		}
		logger.Info("No PVC size or target capacity metadata available, falling back to default scratch PVC size",
			"targetVolume", targetVolume, "totalCheckpointFileSize", totalFileSize, "finalSize", defaultSize.String())
		return defaultSize
	}

	total := maxDiskSize.DeepCopy()
	total.Add(*resource.NewQuantity(totalFileSize, resource.BinarySI))
	withOverhead := addOverhead(total, sizeOverheadPercent)

	finalSize := withOverhead
	if withOverhead.Cmp(minSize) < 0 {
		finalSize = minSize
	}

	logger.Info("Sizing scratch PVC from checkpoint chain metadata",
		"targetVolume", targetVolume,
		"maxDiskSize", maxDiskSize.String(),
		"totalCheckpointFileSize", totalFileSize,
		"overheadPercent", sizeOverheadPercent,
		"finalSize", finalSize.String())
	return finalSize
}

// maxDiskSizeFromIndex returns the largest known original-disk capacity for
// targetVolume across the matched checkpoint chain (CheckpointEntry.PVCSizes,
// index-aligned with Files per pkg/uploader.updateVMIndex), floored by
// targetDiskCapacity -- the restore target PVC's own authoritative requested
// size. Shared by calculateScratchPVCSize (Filesystem-mode target, where this
// is the raw-disk-size component of the single scratch PVC) and
// calculateOutputPVCSize (Block-mode target, where it's the entire output
// PVC size before overhead).
func maxDiskSizeFromIndex(vmIndex uploader.VMIndex, chain []string, targetVolume string, targetDiskCapacity resource.Quantity) resource.Quantity {
	chainSet := make(map[string]bool, len(chain))
	for _, id := range chain {
		chainSet[id] = true
	}

	maxDiskSize := targetDiskCapacity
	for _, entry := range vmIndex.Checkpoints {
		if !chainSet[entry.ID] {
			continue
		}
		for i, f := range entry.Files {
			if f.DiskName != targetVolume || i >= len(entry.PVCSizes) {
				continue
			}
			if entry.PVCSizes[i].Cmp(maxDiskSize) > 0 {
				maxDiskSize = entry.PVCSizes[i]
			}
		}
	}
	return maxDiskSize
}

// calculateWorkPVCSize sizes the Filesystem-mode work PVC that stages the
// downloaded qcow2 chain for a Block-mode restore target (see
// DatamoverPodConfig.ScratchPVCName's doc comment). Unlike
// calculateScratchPVCSize, this drops the raw-disk-size component entirely --
// the final flattened image lands on the separate output PVC instead, so this
// volume only ever needs to hold the chain files. Sized from the sum of the
// resolved checkpoint files' sizes, floored at 1Gi, plus sizeOverheadPercent
// headroom.
func calculateWorkPVCSize(logger logr.Logger, files []uploader.CheckpointFile) resource.Quantity {
	minSize := resource.MustParse("1Gi")
	defaultSize := resource.MustParse(DefaultScratchPVCSize)

	var totalFileSize int64
	for _, f := range files {
		if f.Size < 0 {
			// Same corrupt-index guard as calculateScratchPVCSize: a negative
			// CheckpointFile.Size can only come from invalid index data, never
			// a real file. Treat it as absent (0) rather than letting it
			// subtract from the total, which could silently undersize this
			// PVC below what the OTHER, legitimate files in the same list
			// actually need.
			logger.Info("Ignoring negative checkpoint file size", "size", f.Size)
			continue
		}
		totalFileSize += f.Size
	}

	if totalFileSize == 0 {
		// No checkpoint file size metadata at all (e.g. an older/partial index)
		// -- flooring at 1Gi would be an unfoundedly optimistic guess for an
		// unknown-but-plausibly-large qcow2 chain. Match calculateScratchPVCSize's
		// own no-metadata fallback instead of guessing smaller.
		logger.Info("No checkpoint file size metadata available, falling back to default work PVC size",
			"finalSize", defaultSize.String())
		return defaultSize
	}

	withOverhead := addOverhead(*resource.NewQuantity(totalFileSize, resource.BinarySI), sizeOverheadPercent)
	finalSize := withOverhead
	if withOverhead.Cmp(minSize) < 0 {
		finalSize = minSize
	}

	logger.Info("Sizing Block-mode restore's work PVC from checkpoint chain file sizes",
		"totalCheckpointFileSize", totalFileSize,
		"overheadPercent", sizeOverheadPercent,
		"finalSize", finalSize.String())
	return finalSize
}

// calculateOutputPVCSize sizes the Block-mode output PVC that receives the
// final flattened raw disk image for a Block-mode restore target (see
// DatamoverPodConfig.OutputPVCName's doc comment). Unlike
// calculateScratchPVCSize, this drops the chain-file component entirely --
// the qcow2 chain lives on the separate work PVC instead, so this volume only
// ever needs to hold the flattened raw image. Sized from the max original-disk
// capacity (maxDiskSizeFromIndex, floored by targetDiskCapacity) plus
// sizeOverheadPercent headroom. Falls back to DefaultScratchPVCSize (logged
// loudly) if neither PVCSizes metadata nor targetDiskCapacity are populated.
func calculateOutputPVCSize(logger logr.Logger, vmIndex uploader.VMIndex, chain []string, targetVolume string, targetDiskCapacity resource.Quantity) resource.Quantity {
	minSize := resource.MustParse("1Gi")
	defaultSize := resource.MustParse(DefaultScratchPVCSize)

	maxDiskSize := maxDiskSizeFromIndex(vmIndex, chain, targetVolume, targetDiskCapacity)
	if maxDiskSize.IsZero() {
		logger.Info("No PVC size or target capacity metadata available, falling back to default output PVC size",
			"targetVolume", targetVolume, "finalSize", defaultSize.String())
		return defaultSize
	}

	withOverhead := addOverhead(maxDiskSize, sizeOverheadPercent)
	finalSize := withOverhead
	if withOverhead.Cmp(minSize) < 0 {
		finalSize = minSize
	}

	logger.Info("Sizing Block-mode restore's output PVC from checkpoint chain metadata",
		"targetVolume", targetVolume,
		"maxDiskSize", maxDiskSize.String(),
		"overheadPercent", sizeOverheadPercent,
		"finalSize", finalSize.String())
	return finalSize
}

// findScratchPVC finds the unique scratch PVC associated with a DataDownload
// carrying the given role label, if any. role is "" for the legacy, unlabeled
// single scratch PVC a Filesystem-mode restore target uses (see
// common.LabelScratchVolumeRole's doc comment for the Block-mode work/output
// split). Tries the cached client first; if it finds nothing, retries via
// APIReader (an uncached read) before the caller concludes none exists -- the
// informer cache is only eventually consistent, so a PVC created moments ago
// in an earlier reconcile may not yet be visible to a cached List.
func (r *KubeVirtDataDownloadReconciler) findScratchPVC(ctx context.Context, dd *velerov2alpha1.DataDownload, role string) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := listScratchPVC(ctx, r.Client, r.getPodNamespace(dd), dd, role)
	if err != nil || pvc != nil || r.APIReader == nil {
		return pvc, err
	}
	return listScratchPVC(ctx, r.APIReader, r.getPodNamespace(dd), dd, role)
}

func listScratchPVC(ctx context.Context, reader client.Reader, namespace string, dd *velerov2alpha1.DataDownload, role string) (*corev1.PersistentVolumeClaim, error) {
	selector := labels.SelectorFromSet(labels.Set{common.LabelDataDownloadUID: string(dd.UID)})
	// role == "" means the single Filesystem-mode PVC, which carries no role
	// label at all -- require the label's absence rather than leaving it
	// unconstrained, so this can't also match a Block-mode target's
	// role-labeled "work"/"output" PVC if the other one has already been
	// cleaned up (leaving just one to accidentally satisfy an unconstrained
	// UID-only match).
	var requirement *labels.Requirement
	var err error
	if role == "" {
		requirement, err = labels.NewRequirement(common.LabelScratchVolumeRole, selection.DoesNotExist, nil)
	} else {
		requirement, err = labels.NewRequirement(common.LabelScratchVolumeRole, selection.Equals, []string{role})
	}
	if err != nil {
		return nil, fmt.Errorf("failed to build scratch PVC role selector: %w", err)
	}
	selector = selector.Add(*requirement)

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := reader.List(ctx, pvcList, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list scratch PVCs: %w", err)
	}
	if len(pvcList.Items) > 1 {
		return nil, fmt.Errorf("found multiple scratch PVCs for DataDownload %s (role %q)", dd.Name, role)
	}
	if len(pvcList.Items) == 0 {
		return nil, nil
	}
	return &pvcList.Items[0], nil
}

// listAllScratchPVCs lists every scratch PVC associated with a DataDownload
// regardless of role label -- a Filesystem-mode restore target has one
// (unlabeled), a Block-mode target has two (role-labeled "work" and
// "output"). Used by cleanup paths that need to remove all of them without
// caring which role each one carries. Tries the cached client first; if it
// finds nothing, retries via APIReader (an uncached read) before the caller
// concludes none exist -- the informer cache is only eventually consistent,
// so a PVC created moments ago in an earlier reconcile may not yet be visible
// to a cached List, and deleteAllScratchPVCs's caller (handleCanceling) is a
// terminal path where missing one here means it's never cleaned up at all.
func (r *KubeVirtDataDownloadReconciler) listAllScratchPVCs(ctx context.Context, dd *velerov2alpha1.DataDownload) ([]corev1.PersistentVolumeClaim, error) {
	pvcs, err := listAllScratchPVCsFrom(ctx, r.Client, r.getPodNamespace(dd), dd)
	if err != nil || r.APIReader == nil {
		return pvcs, err
	}
	// A Block-mode restore provisions two scratch PVCs (work + output); the
	// cached client can have one visible and not the other (each created in a
	// separate handleAccepted Create call, moments apart), so finding *some*
	// PVCs isn't proof the list is complete the way it is for a Filesystem-mode
	// target's single PVC. Retry via APIReader whenever the cached count is
	// under the expected total, not just when it's entirely empty. A role-labeled
	// PVC in the cached result is also treated as Block-mode even when
	// isBlockModeRestore's annotation isn't persisted yet (e.g. a Cancel racing
	// in after handleAccepted created the first scratch PVC but before it
	// persisted AnnotationRestoreBlockMode) -- otherwise the still-invisible
	// second PVC would never trigger the APIReader retry.
	expected := 1
	if isBlockModeRestore(dd) || hasRoleLabeledPVC(pvcs) {
		expected = 2
	}
	if len(pvcs) >= expected {
		return pvcs, nil
	}
	return listAllScratchPVCsFrom(ctx, r.APIReader, r.getPodNamespace(dd), dd)
}

// hasRoleLabeledPVC reports whether any listed scratch PVC carries a role
// label, which only a Block-mode restore's work/output pair does. Used as a
// fallback signal in listAllScratchPVCs when AnnotationRestoreBlockMode
// hasn't been persisted yet.
func hasRoleLabeledPVC(pvcs []corev1.PersistentVolumeClaim) bool {
	for i := range pvcs {
		if pvcs[i].Labels[common.LabelScratchVolumeRole] != "" {
			return true
		}
	}
	return false
}

func listAllScratchPVCsFrom(ctx context.Context, reader client.Reader, namespace string, dd *velerov2alpha1.DataDownload) ([]corev1.PersistentVolumeClaim, error) {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := reader.List(ctx, pvcList, client.InNamespace(namespace), client.MatchingLabels{common.LabelDataDownloadUID: string(dd.UID)}); err != nil {
		return nil, fmt.Errorf("failed to list scratch PVCs: %w", err)
	}
	return pvcList.Items, nil
}

// cleanupScratchPVCIfPresent best-effort deletes all of this DataDownload's
// scratch PVCs, if any are still found (one for a Filesystem-mode restore
// target, two for a Block-mode target -- see listAllScratchPVCs). Used by the
// idempotent-resume short-circuit path (handleInProgress), where a prior
// attempt already got the rebind done (which deletes the rebound scratch PVC
// as part of its own flow) without persisting the terminal phase update
// afterward -- the restore already completed by that point, so a cleanup
// failure here shouldn't block reporting success; in the common case this
// finds nothing anyway. handleCanceling uses the stricter deleteAllScratchPVCs
// instead, since Canceled is terminal and a swallowed failure there would
// leak the PVC(s) forever with no further reconcile to retry the delete.
func (r *KubeVirtDataDownloadReconciler) cleanupScratchPVCIfPresent(ctx context.Context, dd *velerov2alpha1.DataDownload, logger logr.Logger) {
	pvcs, err := r.listAllScratchPVCs(ctx, dd)
	if err != nil {
		logger.Error(err, "Failed to list scratch PVCs for cleanup")
		return
	}
	for i := range pvcs {
		if err := r.Delete(ctx, &pvcs[i]); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete scratch PVC", "pvc", pvcs[i].Name)
		}
	}
}

// deleteAllScratchPVCs deletes all of this DataDownload's scratch PVCs,
// returning the first deletion failure (ignoring NotFound) instead of
// swallowing it -- unlike cleanupScratchPVCIfPresent's best-effort cleanup,
// used by handleCanceling where Canceled being terminal means a swallowed
// failure would leak the PVC(s) forever rather than retry on the next
// reconcile (which handleCanceling's caller triggers by returning an error).
func (r *KubeVirtDataDownloadReconciler) deleteAllScratchPVCs(ctx context.Context, dd *velerov2alpha1.DataDownload) error {
	pvcs, err := r.listAllScratchPVCs(ctx, dd)
	if err != nil {
		return err
	}
	for i := range pvcs {
		if err := r.Delete(ctx, &pvcs[i]); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete scratch PVC %s: %w", pvcs[i].Name, err)
		}
	}
	return nil
}

// isRestoreAlreadyProvisioned reports whether the rebind that provisions the
// restore target has already been committed by rebindPVToNamespace in a prior
// reconcile -- i.e. a PV carrying this DataDownload's UID label already has its
// claimRef pointing at the target PVC. Checked via the PV's claimRef (set
// synchronously by patchPVBinding) rather than the target PVC's own Status.Phase/
// Spec.VolumeName, which the Kubernetes PV controller only updates asynchronously
// after the claimRef is set -- using the PVC's fields would miss the narrow
// window where the rebind is already committed but not yet reflected as Bound.
func (r *KubeVirtDataDownloadReconciler) isRestoreAlreadyProvisioned(ctx context.Context, dd *velerov2alpha1.DataDownload) (bool, error) {
	// The PV's claimRef is only ever set by rebindPVToNamespace, which only ever
	// runs after AnnotationDownloaderPodSucceeded is persisted (see its own doc
	// comment) -- so this can only be true once that annotation is set. Skip the
	// PV list/lookup entirely otherwise, rather than issuing it on every InProgress
	// reconcile while the downloader pod is still running.
	if dd.Annotations[AnnotationDownloaderPodSucceeded] != downloaderPodSucceededValue {
		return false, nil
	}

	matched, err := findProvisionedPV(ctx, r.Client, dd)
	if err != nil {
		return false, err
	}
	// The informer cache is only eventually consistent, so a PV whose claimRef
	// was just set by patchPVBinding in an earlier reconcile (crashed/requeued
	// before this check ran again) may not yet be visible to a cached List --
	// retry via APIReader (an uncached read) before concluding it's unprovisioned,
	// matching the fallback convention used elsewhere for cached child-resource
	// lookups (e.g. findScratchPVC, listAllScratchPVCs).
	if matched == nil && r.APIReader != nil {
		if matched, err = findProvisionedPV(ctx, r.APIReader, dd); err != nil {
			return false, err
		}
	}
	if matched == nil {
		return false, nil
	}

	// A claimRef with no UID predates this check (patchPVBinding now always sets
	// one) -- fall back to the name/namespace match above rather than rejecting it.
	if matched.Spec.ClaimRef.UID == "" {
		return true, nil
	}

	// The target PVC name/namespace could, in principle, have been deleted and
	// recreated (a new PVC, new UID) after this PV's claimRef was set -- comparing
	// the live PVC's UID against the recorded claimRef.UID catches that instead of
	// mistaking a fresh, unrelated PVC for the one this restore actually provisioned.
	targetPVC := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Name: dd.Spec.TargetVolume.PVC, Namespace: dd.Spec.TargetVolume.Namespace}, targetPVC); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get target PVC to verify claimRef UID: %w", err)
	}
	return targetPVC.UID == matched.Spec.ClaimRef.UID, nil
}

// findProvisionedPV lists PVs carrying dd's UID label via reader (either the
// cached client or an uncached APIReader -- see isRestoreAlreadyProvisioned)
// and returns the one whose claimRef names dd's target PVC, or nil if none
// match.
func findProvisionedPV(ctx context.Context, reader client.Reader, dd *velerov2alpha1.DataDownload) (*corev1.PersistentVolume, error) {
	pvList := &corev1.PersistentVolumeList{}
	if err := reader.List(ctx, pvList, client.MatchingLabels{common.LabelDataDownloadUID: string(dd.UID)}); err != nil {
		return nil, fmt.Errorf("failed to list PVs by UID label: %w", err)
	}
	for i, pv := range pvList.Items {
		if pv.Spec.ClaimRef != nil &&
			pv.Spec.ClaimRef.Name == dd.Spec.TargetVolume.PVC &&
			pv.Spec.ClaimRef.Namespace == dd.Spec.TargetVolume.Namespace {
			return &pvList.Items[i], nil
		}
	}
	return nil, nil
}

// isBlockModeRestore reports whether dd's restore target PVC was Block
// volumeMode, which determines whether this DataDownload provisioned a single
// Filesystem-mode scratch PVC (ensureScratchPVC) or a work+output PVC pair
// (ensureWorkPVC/ensureOutputPVC) -- see DatamoverPodConfig.OutputPVCName's
// doc comment. Reads the AnnotationRestoreBlockMode marker handleAccepted set
// from the target PVC's own VolumeMode (immutable once a PVC is created)
// rather than re-fetching the target PVC on every later-phase call: several
// call sites (handlePrepared, completeSuccessfulDownload) have no other need
// for the target PVC object, so this avoids adding one just to answer this
// one question, mirroring AnnotationTargetDiskName's own rationale.
func isBlockModeRestore(dd *velerov2alpha1.DataDownload) bool {
	return dd.Annotations[AnnotationRestoreBlockMode] == "true"
}

// ensureScratchPVC creates or retrieves the read-write scratch PVC that stages
// the downloaded qcow2 chain for a Filesystem-mode restore target. For a
// Filesystem-mode target, this single, unlabeled PVC also holds the final
// flattened raw disk image and is the volume rebound onto the target
// (unchanged from before Block-mode support existed). StorageClassName and
// VolumeMode are derived from the restore target PVC so the eventual rebind
// (Completed transition) is compatible by construction. A Block-mode target
// instead calls ensureWorkPVC/ensureOutputPVC below.
func (r *KubeVirtDataDownloadReconciler) ensureScratchPVC(
	ctx context.Context,
	logger logr.Logger,
	dd *velerov2alpha1.DataDownload,
	targetPVC *corev1.PersistentVolumeClaim,
	size resource.Quantity,
) (*corev1.PersistentVolumeClaim, error) {
	return r.ensureScratchPVCWithRole(ctx, logger, dd, "", targetPVC.Spec.AccessModes, targetPVC.Spec.StorageClassName, targetPVC.Spec.VolumeMode, size)
}

// ensureWorkPVC creates or retrieves the Filesystem-mode work PVC that stages
// the downloaded qcow2 chain for a Block-mode restore target (see
// DatamoverPodConfig.ScratchPVCName's doc comment). Always Filesystem mode
// regardless of the target's own VolumeMode: the downloader stages multiple
// named chain files on it, which a Block-mode volume has no way to hold.
// Always ReadWriteOnce too, deliberately not the target's own AccessModes:
// exactly one pod (this controller's downloader) ever mounts it, on one
// node, and several CSI drivers (e.g. Ceph RBD) reject ReadWriteMany on a
// Filesystem-mode volume outright even when the target itself is RWX.
func (r *KubeVirtDataDownloadReconciler) ensureWorkPVC(
	ctx context.Context,
	logger logr.Logger,
	dd *velerov2alpha1.DataDownload,
	targetPVC *corev1.PersistentVolumeClaim,
	size resource.Quantity,
) (*corev1.PersistentVolumeClaim, error) {
	filesystemMode := corev1.PersistentVolumeFilesystem
	workAccessModes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	return r.ensureScratchPVCWithRole(ctx, logger, dd, common.ScratchVolumeRoleWork, workAccessModes, targetPVC.Spec.StorageClassName, &filesystemMode, size)
}

// ensureOutputPVC creates or retrieves the Block-mode output PVC that
// receives the final flattened raw disk image for a Block-mode restore target
// (see DatamoverPodConfig.OutputPVCName's doc comment). This is the PVC/PV
// completeSuccessfulDownload rebinds onto the target, not the work PVC.
func (r *KubeVirtDataDownloadReconciler) ensureOutputPVC(
	ctx context.Context,
	logger logr.Logger,
	dd *velerov2alpha1.DataDownload,
	targetPVC *corev1.PersistentVolumeClaim,
	size resource.Quantity,
) (*corev1.PersistentVolumeClaim, error) {
	blockMode := corev1.PersistentVolumeBlock
	return r.ensureScratchPVCWithRole(ctx, logger, dd, common.ScratchVolumeRoleOutput, targetPVC.Spec.AccessModes, targetPVC.Spec.StorageClassName, &blockMode, size)
}

// ensureScratchPVCWithRole is the shared implementation behind
// ensureScratchPVC/ensureWorkPVC/ensureOutputPVC: creates or retrieves a
// scratch PVC labeled with the given role ("" for the legacy unlabeled
// single-PVC Filesystem-mode path), with an explicit VolumeMode rather than
// always copying the restore target's own -- a Block-mode target's work PVC
// must stay Filesystem mode even though the target itself is Block mode.
func (r *KubeVirtDataDownloadReconciler) ensureScratchPVCWithRole(
	ctx context.Context,
	logger logr.Logger,
	dd *velerov2alpha1.DataDownload,
	role string,
	accessModesIn []corev1.PersistentVolumeAccessMode,
	storageClassName *string,
	volumeMode *corev1.PersistentVolumeMode,
	size resource.Quantity,
) (*corev1.PersistentVolumeClaim, error) {
	accessModes := accessModesIn
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}

	if existing, err := r.findScratchPVC(ctx, dd, role); err != nil {
		return nil, err
	} else if existing != nil {
		// The values this role would request could in principle have drifted since
		// this PVC was created (e.g. a fuller checkpoint chain resolved on a later
		// reconcile now needs more space, or -- for the "" role -- the target PVC
		// was deleted and recreated with a different spec; StorageClassName/
		// VolumeMode/AccessModes are immutable on a live PVC, so delete+recreate is
		// the only way that last one can happen). Detect that here rather than
		// silently reusing a now-wrong-shaped scratch volume, which would only
		// surface much later as an opaque bindExistingPVC failure (or worse, a
		// successful-looking restore onto incompatible storage).
		if err := validateScratchPVCShape(existing, storageClassName, volumeMode, accessModes, size); err != nil {
			return nil, fmt.Errorf("existing scratch PVC %s (role %q) no longer matches what would be requested now: %w", existing.Name, role, err)
		}
		logger.V(1).Info("Scratch PVC already exists", "pvc", existing.Name, "role", role)
		return existing, nil
	}

	generateNamePrefix := fmt.Sprintf("%s%s-", common.ScratchPVCNamePrefix, dd.Name)
	pvcLabels := map[string]string{
		common.LabelDataDownloadUID: string(dd.UID),
	}
	if role != "" {
		generateNamePrefix = fmt.Sprintf("%s%s-%s-", common.ScratchPVCNamePrefix, dd.Name, role)
		pvcLabels[common.LabelScratchVolumeRole] = role
	}

	podNamespace := r.getPodNamespace(dd)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: safeGenerateNamePrefix(generateNamePrefix, 63),
			Namespace:    podNamespace,
			Labels:       pvcLabels,
			Annotations: map[string]string{
				common.AnnotationDataDownloadName: dd.Name,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: accessModes,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: size,
				},
			},
			StorageClassName: storageClassName,
			VolumeMode:       volumeMode,
		},
	}

	// Owner ref is valid here (unlike DataUpload's cross-namespace temp PVC): the
	// scratch PVC lives in the same namespace as the DataDownload CR (both in
	// r.OADPNamespace by convention), so Kubernetes GC applies automatically.
	if err := setOwnerReferenceIfSameNamespace(dd, pvc, r.Scheme, logger); err != nil {
		return nil, fmt.Errorf("failed to set owner reference on scratch PVC: %w", err)
	}

	if err := r.Create(ctx, pvc); err != nil {
		return nil, fmt.Errorf("failed to create scratch PVC: %w", err)
	}
	logger.Info("Created scratch PVC", "generateName", pvc.GenerateName, "namespace", podNamespace, "role", role, "size", size.String())
	return pvc, nil
}

// failIfImmutableScratchPVCMismatch reports whether err indicates a
// scratch/work/output PVC's immutable shape no longer matches what would be
// requested (errScratchPVCShapeMismatch), and if so transitions dd to Failed
// with err's own message rather than letting the caller treat it as a
// retryable reconcile error. Retrying can never resolve this class of
// mismatch, so failing fast here beats leaving the DataDownload to retry via
// controller-runtime's exponential backoff until Spec.OperationTimeout
// eventually catches it -- often hours later, for a condition that was
// knowable immediately. failed=false (err not this kind of mismatch, or nil)
// means the caller should handle err itself, same as before this check existed.
func (r *KubeVirtDataDownloadReconciler) failIfImmutableScratchPVCMismatch(ctx context.Context, dd *velerov2alpha1.DataDownload, err error) (failed bool, updateErr error) {
	if !stderrors.Is(err, errScratchPVCShapeMismatch) {
		return false, nil
	}
	return true, r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, err.Error())
}

// errScratchPVCShapeMismatch wraps the immutable-field mismatches
// validateScratchPVCShape reports (StorageClassName, VolumeMode,
// AccessModes) -- deliberately not the size-too-small case, which is left as
// a plain (non-wrapped) error. Those three fields can't change on a live PVC
// without deleting and recreating it, so retrying can never resolve a
// mismatch on them; size, by contrast, can genuinely differ on a later
// reconcile as the checkpoint chain resolves further, so it stays retryable.
// Callers use errors.Is(err, errScratchPVCShapeMismatch) to fail fast on the
// immutable cases instead of leaving the DataDownload to retry via
// reconcile-error backoff until Spec.OperationTimeout eventually catches it,
// often hours later for a condition that was knowable immediately.
var errScratchPVCShapeMismatch = stderrors.New("scratch PVC has an immutable shape mismatch")

// validateScratchPVCShape checks an already-existing scratch PVC's requested
// storage, StorageClassName, VolumeMode, and AccessModes against the values
// ensureScratchPVC would request now (wantAccessModes is the caller's
// already-defaulted value -- see the ReadWriteOnce fallback in ensureScratchPVC --
// not the target PVC's raw, possibly-empty spec), so a false mismatch isn't
// reported against a default that was only ever implicit. An existing PVC
// requesting less than minSize is rejected -- e.g. a fuller checkpoint chain
// resolved on a later reconcile than the one that originally created it -- but
// one requesting the same or more is reused as-is (its size is never shrunk).
func validateScratchPVCShape(existing *corev1.PersistentVolumeClaim, wantStorageClass *string, wantVolumeMode *corev1.PersistentVolumeMode, wantAccessModes []corev1.PersistentVolumeAccessMode, minSize resource.Quantity) error {
	existingSize, hasSize := existing.Spec.Resources.Requests[corev1.ResourceStorage]
	if !hasSize || existingSize.Cmp(minSize) < 0 {
		existingSizeStr := "<unset>"
		if hasSize {
			existingSizeStr = existingSize.String()
		}
		return fmt.Errorf("requested storage %s is smaller than the required %s", existingSizeStr, minSize.String())
	}
	if !ptrEqual(existing.Spec.StorageClassName, wantStorageClass) {
		return fmt.Errorf("%w: storageClassName %s does not match expected %s", errScratchPVCShapeMismatch,
			ptrOrNone(existing.Spec.StorageClassName), ptrOrNone(wantStorageClass))
	}
	if !ptrEqual(existing.Spec.VolumeMode, wantVolumeMode) {
		return fmt.Errorf("%w: volumeMode %s does not match expected %s", errScratchPVCShapeMismatch,
			ptrOrNone(existing.Spec.VolumeMode), ptrOrNone(wantVolumeMode))
	}
	if !accessModesEqual(existing.Spec.AccessModes, wantAccessModes) {
		return fmt.Errorf("%w: accessModes %v do not match expected %v", errScratchPVCShapeMismatch, existing.Spec.AccessModes, wantAccessModes)
	}
	return nil
}

// ptrEqual reports whether two pointers are both nil or point to equal values
// (nil and non-nil never match). Nil-safe replacement for *a == *b.
func ptrEqual[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// ptrOrNone renders a possibly-nil string-kinded pointer for error messages.
func ptrOrNone[T ~string](p *T) string {
	if p == nil {
		return "<none>"
	}
	return string(*p)
}

// accessModesEqual compares two access mode lists as sets: order doesn't reflect any
// semantic difference in a PVC spec, only membership does.
func accessModesEqual(a, b []corev1.PersistentVolumeAccessMode) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[corev1.PersistentVolumeAccessMode]int, len(a))
	for _, m := range a {
		set[m]++
	}
	for _, m := range b {
		set[m]--
	}
	for _, count := range set {
		if count != 0 {
			return false
		}
	}
	return true
}

// handlePrepared processes DataDownloads in Prepared phase.
// Launches the downloader pod against the scratch PVC and transitions to InProgress.
func (r *KubeVirtDataDownloadReconciler) handlePrepared(ctx context.Context, logger logr.Logger, dd *velerov2alpha1.DataDownload) (ctrl.Result, error) {
	logger.Info("Handling Prepared phase DataDownload")

	podNamespace := r.getPodNamespace(dd)

	pod, err := r.findPodForDataDownload(ctx, dd, podNamespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check for existing downloader pod: %w", err)
	}
	if pod != nil {
		logger.Info("Downloader pod already exists, transitioning to InProgress", "pod", pod.Name)
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseInProgress, "Downloader pod running"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	// Checked before any of the lookups below (VM ref, BSL, scratch/work/output
	// PVCs) since none of that is needed while gated -- a deferred DataDownload
	// would otherwise redo it every 30s for no reason.
	if gated, result, err := r.checkConcurrentDataMoverLimit(ctx, logger, dd); err != nil {
		return ctrl.Result{}, err
	} else if gated {
		return result, nil
	}

	vmRef, err := common.GetVMReferenceFromDataDownload(dd)
	if err != nil {
		logger.Error(err, "Failed to get VM reference")
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, fmt.Sprintf("Missing VM reference: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	bsl, err := r.getBackupStorageLocationForDD(ctx, dd)
	if err != nil {
		if isTransientBSLLookupError(err, dd.Spec.BackupStorageLocation) {
			return ctrl.Result{}, fmt.Errorf("failed to get BackupStorageLocation: %w", err)
		}
		logger.Error(err, "Failed to get BackupStorageLocation")
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, fmt.Sprintf("Failed to get BSL: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	isBlockMode := isBlockModeRestore(dd)

	var scratchPVCName, outputPVCName string
	if isBlockMode {
		workPVC, err := r.findScratchPVC(ctx, dd, common.ScratchVolumeRoleWork)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to find work PVC: %w", err)
		}
		outputPVC, err := r.findScratchPVC(ctx, dd, common.ScratchVolumeRoleOutput)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to find output PVC: %w", err)
		}
		if workPVC == nil || outputPVC == nil {
			logger.Error(nil, "Work or output PVC not found for Block-mode DataDownload",
				"workFound", workPVC != nil, "outputFound", outputPVC != nil)
			if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, "Work or output PVC not found"); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		scratchPVCName = workPVC.Name
		outputPVCName = outputPVC.Name
	} else {
		scratchPVC, err := r.findScratchPVC(ctx, dd, "")
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to find scratch PVC: %w", err)
		}
		if scratchPVC == nil {
			logger.Error(nil, "Scratch PVC not found for DataDownload")
			if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, "Scratch PVC not found"); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		scratchPVCName = scratchPVC.Name
	}

	targetDiskName := dd.Annotations[AnnotationTargetDiskName]
	if targetDiskName == "" {
		err := fmt.Errorf("DataDownload %s/%s missing %s annotation set during Accepted phase", dd.Namespace, dd.Name, AnnotationTargetDiskName)
		logger.Error(err, "Cannot build downloader pod config")
		if updateErr := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, err.Error()); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, nil
	}

	podConfig, err := r.buildDownloaderPodConfig(dd, bsl, vmRef, scratchPVCName, outputPVCName, targetDiskName)
	if err != nil {
		logger.Error(err, "Failed to build downloader pod config")
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, fmt.Sprintf("Failed to build pod config: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	podConfig.Namespace = podNamespace

	podToCreate := buildDatamoverPod(podConfig)
	podToCreate.GenerateName = safeGenerateNamePrefix(fmt.Sprintf("%s%s-", common.DownloaderPodNamePrefix, dd.Name), 63)
	podToCreate.Name = ""

	if err := setOwnerReferenceIfSameNamespace(dd, podToCreate, r.Scheme, logger); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to set owner reference on pod: %w", err)
	}

	if err := r.Create(ctx, podToCreate); err != nil {
		if errors.IsAlreadyExists(err) {
			logger.Info("Downloader pod already exists (race)", "generateName", podToCreate.GenerateName)
		} else {
			return ctrl.Result{}, fmt.Errorf("failed to create downloader pod: %w", err)
		}
	} else {
		logger.Info("Created downloader pod", "generateName", podToCreate.GenerateName, "namespace", podNamespace)
	}

	if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseInProgress, "Downloader pod launched"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// buildDownloaderPodConfig assembles the configuration for the downloader pod.
// targetDiskName is the KubeVirt disk/volume name resolved by resolveTargetDiskName
// in handleAccepted (NOT dd.Spec.TargetVolume.PVC, which is a PVC/claim name) --
// it's what the pod's KUBEVIRT_DM_TARGET_VOLUME env var must carry, since
// pkg/downloader.ResolveCheckpointFiles matches on CheckpointFile.DiskName.
// outputPVCName is "" for a Filesystem-mode restore target (scratchPVCName
// alone serves both roles); for a Block-mode target it names the separate
// output PVC (see DatamoverPodConfig.OutputPVCName's doc comment).
func (r *KubeVirtDataDownloadReconciler) buildDownloaderPodConfig(
	dd *velerov2alpha1.DataDownload,
	bsl *velerov1.BackupStorageLocation,
	vmRef *common.VMReference,
	scratchPVCName string,
	outputPVCName string,
	targetDiskName string,
) (*DatamoverPodConfig, error) {
	cfg, err := uploader.ExtractBSLConfig(bsl)
	if err != nil {
		return nil, err
	}
	if cfg.CredentialName == "" {
		return nil, fmt.Errorf("BSL %s has no credential secret configured", bsl.Name)
	}

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
		OperationMode:                  OperationModeDownload,
		Name:                           dd.Name, // Used as a prefix for GenerateName
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
		VeleroBackupName:               getVeleroBackupName(dd.Labels),
		ResourceName:                   dd.Name,
		ResourceUID:                    string(dd.UID),
		UIDLabelKey:                    common.LabelDataDownloadUID,
		NameAnnotationKey:              common.AnnotationDataDownloadName,
		ScratchPVCName:                 scratchPVCName,
		OutputPVCName:                  outputPVCName,
		TargetVolume:                   targetDiskName,
		Labels:                         make(map[string]string),
	}, nil
}

// handleInProgress processes DataDownloads in InProgress phase.
// Monitors the downloader pod; on success, rebinds the scratch PV into the restore
// target namespace under the exact PVC name Velero created, then transitions to
// Completed/Failed.
func (r *KubeVirtDataDownloadReconciler) handleInProgress(ctx context.Context, logger logr.Logger, dd *velerov2alpha1.DataDownload) (ctrl.Result, error) {
	logger.Info("Handling InProgress phase DataDownload")

	// Idempotent resume: if a prior reconcile already completed the rebind (scratch
	// PVC deleted, target PVC bound to the rebound PV) but failed to persist the
	// Completed phase afterward -- e.g. a transient API error on updatePhase -- the
	// scratch PVC and possibly the pod are already gone, and re-attempting the
	// rebind would fail. Detect this by checking whether the target PVC is already
	// bound to a PV carrying this DataDownload's UID label, and finish idempotently
	// instead of misreporting a successful restore as failed.
	if done, err := r.isRestoreAlreadyProvisioned(ctx, dd); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check restore completion state: %w", err)
	} else if done {
		logger.Info("Restored volume already provisioned in a prior reconcile, completing idempotently")
		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseCompleted, "Restored disk provisioned to target volume"); err != nil {
			return ctrl.Result{}, err
		}
		// Best-effort: the pod (and scratch PVC, if somehow still present) may not
		// have been cleaned up by whichever prior attempt got the rebind done,
		// since this path returns before reaching the normal cleanup call below.
		if cleanupNotReady, _ := cleanupPodsByUID(ctx, r.Client, r.APIReader, common.LabelDataDownloadUID, string(dd.UID), r.getPodNamespace(dd), logger); cleanupNotReady {
			logger.Info("Datamover pod still terminating (or its status couldn't be confirmed)")
			// Continue -- the restore already completed, don't block on cleanup failures
		}
		r.cleanupScratchPVCIfPresent(ctx, dd, logger)
		return ctrl.Result{}, nil
	}

	podNamespace := r.getPodNamespace(dd)

	pod, err := r.findPodForDataDownload(ctx, dd, podNamespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get downloader pod: %w", err)
	}
	if pod == nil {
		if dd.Annotations[AnnotationDownloaderPodSucceeded] == downloaderPodSucceededValue {
			// The downloader pod already succeeded and was deleted by a prior
			// reconcile (the marker is persisted before the pod delete), so its
			// absence is expected -- resume the rebind instead of misreading it
			// as a failure.
			logger.Info("Downloader pod already succeeded and was cleaned up, proceeding to rebind")
			return r.completeSuccessfulDownload(ctx, logger, dd, podNamespace)
		}
		// A pod handlePrepared just created in an earlier reconcile may not yet be
		// visible to this reconcile's cached client -- watch-based informer
		// propagation isn't synchronous with the create call, so this can be a
		// transient miss rather than the pod actually being gone. Requeue instead
		// of failing immediately: Spec.OperationTimeout (checked before this
		// handler runs on every reconcile) already bounds how long a genuinely
		// vanished pod goes undetected, so a short cache lag self-heals within
		// seconds while a pod that truly never reappears still eventually fails.
		logger.Info("Downloader pod not found, will retry before failing", "dataDownload", dd.Name, "namespace", podNamespace)
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		logger.Info("Downloader pod completed successfully", "pod", pod.Name)

		// Record the success on the DataDownload BEFORE deleting the pod (see
		// AnnotationDownloaderPodSucceeded's doc comment): the strict cleanup
		// below routinely errors while the pod is still terminating, and the
		// retry reconcile often lands after the pod object is gone -- the marker
		// is what lets that reconcile resume the rebind rather than fail. Gating
		// log emission on it also keeps those retries from re-emitting the pod's
		// logs each pass.
		if dd.Annotations[AnnotationDownloaderPodSucceeded] != downloaderPodSucceededValue {
			r.emitPodLogs(ctx, logger, pod)
			if dd.Annotations == nil {
				dd.Annotations = make(map[string]string)
			}
			dd.Annotations[AnnotationDownloaderPodSucceeded] = downloaderPodSucceededValue
			if err := r.Update(ctx, dd); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to record downloader pod success: %w", err)
			}
		}

		// Delete the pod before rebinding: rebindPVToNamespace deletes the scratch
		// PVC and waits for it to fully terminate, but Kubernetes' pvc-protection
		// finalizer won't clear while any pod object (even a Completed one) still
		// references it in spec.volumes -- leaving this pod around would deadlock
		// that wait until it times out. Propagate a cleanup failure instead of
		// proceeding into that deadlock-prone wait: the retry re-enters either
		// this branch (pod still visible) or the pod-absent marker branch above.
		if cleanupNotReady, _ := cleanupPodsByUID(ctx, r.Client, r.APIReader, common.LabelDataDownloadUID, string(dd.UID), podNamespace, logger); cleanupNotReady {
			return ctrl.Result{}, fmt.Errorf("downloader pod still terminating (or its status couldn't be confirmed) before rebinding restored volume")
		}

		return r.completeSuccessfulDownload(ctx, logger, dd, podNamespace)

	case corev1.PodFailed:
		failureMessage := extractPodFailureMessage(pod)
		logger.Error(nil, "Downloader pod failed", "pod", pod.Name, "message", failureMessage)

		r.emitPodLogs(ctx, logger, pod)

		// Skip cleanup on failure to preserve resources (pod, scratch PVC) for debugging.

		if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed,
			fmt.Sprintf("Downloader pod failed: %s", failureMessage)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case corev1.PodPending, corev1.PodRunning:
		logger.V(1).Info("Downloader pod still running", "pod", pod.Name, "phase", pod.Status.Phase)
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil

	default:
		logger.Info("Downloader pod in unknown phase", "pod", pod.Name, "phase", pod.Status.Phase)
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}
}

// completeSuccessfulDownload finishes a DataDownload whose downloader pod has
// succeeded (and been deleted): rebinds the scratch PV into the restore target
// namespace under the exact PVC name Velero created, then transitions to
// Completed. Reached from handleInProgress either directly after the pod
// cleanup, or from the pod-absent branch via the AnnotationDownloaderPodSucceeded
// marker when a prior reconcile deleted the pod but didn't get this far.
func (r *KubeVirtDataDownloadReconciler) completeSuccessfulDownload(ctx context.Context, logger logr.Logger, dd *velerov2alpha1.DataDownload, podNamespace string) (ctrl.Result, error) {
	isBlockMode := isBlockModeRestore(dd)

	// A Filesystem-mode target's single scratch PVC is what gets rebound (role
	// ""); a Block-mode target instead rebinds the separate output PVC -- the
	// work PVC (staging the qcow2 chain) is never rebound and gets deleted
	// below once the rebind succeeds.
	rebindRole := ""
	if isBlockMode {
		rebindRole = common.ScratchVolumeRoleOutput
	}
	reboundPVC, err := r.findScratchPVC(ctx, dd, rebindRole)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to find scratch PVC: %w", err)
	}
	if reboundPVC == nil {
		err := fmt.Errorf("scratch PVC not found for completed DataDownload %s", dd.Name)
		logger.Error(err, "Cannot rebind restored volume")
		if updateErr := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed, err.Error()); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, nil
	}

	rebindResult, err := rebindPVToNamespace(
		ctx, r.Client, logger,
		reboundPVC.Name, podNamespace, dd.Spec.TargetVolume.Namespace,
		dd.Name, string(dd.UID),
		common.LabelDataDownloadUID, common.AnnotationDataDownloadName,
		BindTargetExisting, dd.Spec.TargetVolume.PVC,
	)
	if err != nil {
		// Fail without retry, matching DataUpload's rebind failure handling: PV
		// rebind is a multi-step operation and a partial failure needs investigation.
		logger.Error(err, "Failed to rebind restored volume to target namespace")
		if updateErr := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseFailed,
			fmt.Sprintf("Failed to provision restored volume: %v", err)); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, nil
	}
	logger.Info("Successfully provisioned restored volume to target namespace",
		"pvc", rebindResult.NewPVCName, "pv", rebindResult.PVName)

	// The work PVC staged the qcow2 chain and is never rebound (only the
	// output PVC above is) -- delete it now that the download is done. Best-
	// effort: a leftover work PVC is just extra storage usage, not worth
	// failing an otherwise-successful restore over.
	if isBlockMode {
		if workPVC, err := r.findScratchPVC(ctx, dd, common.ScratchVolumeRoleWork); err != nil {
			logger.Error(err, "Failed to find work PVC for cleanup")
		} else if workPVC != nil {
			if err := r.Delete(ctx, workPVC); err != nil && !errors.IsNotFound(err) {
				logger.Error(err, "Failed to delete work PVC", "pvc", workPVC.Name)
			}
		}
	}

	// The rebound PV is intentionally left with the Retain policy rebindPVToNamespace
	// sets on it (rebindResult.OriginalReclaimPolicy is not restored here, unlike
	// DataUpload's cleanupReboundPVCAndPV): this is restored user data, so it should
	// survive deletion of the target PVC rather than being auto-deleted. That does
	// mean deleting the restored PVC later orphans the PV/backing storage until an
	// operator manually reclaims it -- surface that via an Event since there's no
	// other operational signal pointing at it.
	if r.EventRecorder != nil {
		r.EventRecorder.Eventf(dd, corev1.EventTypeWarning, "PVLeftInRetainPolicy",
			"PV %s retained after restore completion; manual reclaim required if PVC %s/%s is deleted",
			rebindResult.PVName, dd.Spec.TargetVolume.Namespace, dd.Spec.TargetVolume.PVC)
	}

	if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseCompleted, "Restored disk provisioned to target volume"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// emitPodLogs collects logs from a completed downloader pod and emits them
// through the controller's logger. Failures are logged as warnings and
// never block the reconcile loop.
func (r *KubeVirtDataDownloadReconciler) emitPodLogs(ctx context.Context, logger logr.Logger, pod *corev1.Pod) {
	if r.PodLogCollector == nil {
		return
	}
	logs, err := r.PodLogCollector(ctx, pod.Name, pod.Namespace)
	if err != nil {
		logger.Info("Warning: failed to collect downloader pod logs", "pod", pod.Name, "error", err)
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
		logger.Info("Downloader pod log truncated",
			"source", "downloader-pod",
			"pod", pod.Name,
			"skippedLeadingLines", skipped)
	}
	for i, line := range lines {
		logger.Info("Downloader pod log",
			"source", "downloader-pod",
			"pod", pod.Name,
			"line", skipped+i+1,
			"message", line)
	}
}

// handleCanceling processes DataDownloads in Canceling phase.
// Cleans up the downloader pod and scratch PVC, then transitions to Canceled.
func (r *KubeVirtDataDownloadReconciler) handleCanceling(ctx context.Context, logger logr.Logger, dd *velerov2alpha1.DataDownload) (ctrl.Result, error) {
	logger.Info("Handling Canceling phase DataDownload")

	podNamespace := r.getPodNamespace(dd)

	if cleanupNotReady, terminating := cleanupPodsByUID(ctx, r.Client, r.APIReader, common.LabelDataDownloadUID, string(dd.UID), podNamespace, logger); cleanupNotReady {
		if terminating {
			// The expected, self-resolving case: Delete was accepted, kubelet
			// just hasn't finished tearing the pod(s) down yet. Requeue quickly
			// without logging a reconcile error for something that isn't wrong.
			logger.V(1).Info("Downloader pod(s) still terminating, will retry")
			return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
		}
		// Canceled is terminal -- no further reconciliation ever runs for this
		// object once it's persisted. Returning here instead of continuing
		// means a cleanup failure retries (this handler runs again) rather than
		// permanently abandoning a still-running pod and, worse, the scratch
		// PVC deletion below racing ahead of it: a PVC delete with a still-
		// attached pod just wedges in Terminating behind the pvc-protection
		// finalizer, and nothing would ever revisit it after Canceled persists.
		return ctrl.Result{}, fmt.Errorf("downloader pod still terminating (or its status couldn't be confirmed) before canceling DataDownload")
	}

	// The scratch PVC(s) are never rebound out of podNamespace before Completed
	// (only the InProgress->Completed transition rebinds the output/scratch
	// PVC), so on cancel they can be deleted directly rather than via
	// cleanupReboundPVCAndPV (which expects a PV whose claimRef has already
	// been reset for a rebind). Canceled is terminal like the pod cleanup
	// above, so a swallowed delete failure here would leak the PVC(s) forever
	// with no further reconcile to retry it -- propagate the error instead.
	if err := r.deleteAllScratchPVCs(ctx, dd); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to delete scratch PVC(s) before canceling DataDownload: %w", err)
	}

	if err := r.updatePhase(ctx, dd, velerov2alpha1.DataDownloadPhaseCanceled, "DataDownload canceled"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// updatePhase updates the DataDownload phase and status message.
// Uses Update instead of Status().Patch() to match Velero's approach, which works
// regardless of whether the CRD has the status subresource enabled -- confirmed via
// `oc get crd datadownloads.velero.io -o jsonpath='{.spec.versions[*].subresources}'`
// (empty {}): it does not, so Status().Update() would be a no-op here, not an
// equally-valid alternative. checkOperationTimeoutCore's AcceptedTimestamp backfill
// persist callback uses the same r.Update() for the same reason.
//
// When phase and message already match, this returns early WITHOUT calling
// r.Update -- so any other in-memory field a caller set on dd first (an
// annotation, a status subfield) is silently discarded rather than persisted.
// A caller that needs such a field written (e.g. handleAccepted setting an
// annotation before transitioning phase) must persist it itself rather than
// relying on this call to do so incidentally.
func (r *KubeVirtDataDownloadReconciler) updatePhase(ctx context.Context, dd *velerov2alpha1.DataDownload, phase velerov2alpha1.DataDownloadPhase, message string) error {
	logger := log.FromContext(ctx)

	if dd.Status.Phase == phase && dd.Status.Message == message {
		logger.V(1).Info("DataDownload already at target phase with same message, skipping update",
			"dataDownload", dd.Name,
			"phase", phase)
		return nil
	}

	dd.Status.Phase = phase
	dd.Status.Message = message

	now := metav1.Now()
	if phase == velerov2alpha1.DataDownloadPhaseInProgress && dd.Status.StartTimestamp == nil {
		dd.Status.StartTimestamp = &now
	}
	if isTerminalDataDownloadPhase(phase) && dd.Status.CompletionTimestamp == nil {
		dd.Status.CompletionTimestamp = &now
	}

	if err := r.Update(ctx, dd); err != nil {
		logger.Error(err, "Failed to update DataDownload phase",
			"dataDownload", dd.Name,
			"phase", phase)
		return fmt.Errorf("failed to update DataDownload phase to %s: %w", phase, err)
	}

	logger.Info("Updated DataDownload phase",
		"dataDownload", dd.Name,
		"phase", phase,
		"message", message)

	return nil
}

// isTerminalDataDownloadPhase reports whether phase is one of DataDownload's
// terminal phases (Completed/Failed/Canceled), used by updatePhase to know
// when to set Status.CompletionTimestamp.
func isTerminalDataDownloadPhase(phase velerov2alpha1.DataDownloadPhase) bool {
	switch phase {
	case velerov2alpha1.DataDownloadPhaseCompleted, velerov2alpha1.DataDownloadPhaseFailed, velerov2alpha1.DataDownloadPhaseCanceled:
		return true
	default:
		return false
	}
}

// restoreVMRunStateIfAllSiblingsCompleted flips a restored VM back to its
// pre-restore run state once every DataDownload targeting it has reached
// Completed. This is the controller-side half of the VM-restore race fix: the
// kubevirt-datamover-plugin halts the VM (RestoreItemActionV2) and stashes its
// original run state in AnnotationOriginalRunStrategy(+Source) before Velero
// recreates it, so virt-controller doesn't start the VM -- and rebind the
// target PVC to the wrong volume -- before this controller has finished
// restoring the disk. Idempotent: the flip deletes both stash annotations, so
// a VM with neither present (already flipped, or never stashed) is a no-op.
// Also tolerates the VM having been deleted since restore.
func (r *KubeVirtDataDownloadReconciler) restoreVMRunStateIfAllSiblingsCompleted(ctx context.Context, logger logr.Logger, dd *velerov2alpha1.DataDownload) error {
	vmRef, err := common.GetVMReferenceFromDataDownload(dd)
	if err != nil {
		return nil
	}

	allCompleted, err := r.allSiblingDataDownloadsCompleted(ctx, dd, vmRef)
	if err != nil {
		return err
	}
	if !allCompleted {
		return nil
	}

	// vmRef.Namespace is the original/source namespace.
	// TargetVolume.Namespace is the namespace after restore mapping.
	targetVMNamespace := dd.Spec.TargetVolume.Namespace
	if targetVMNamespace == "" {
		targetVMNamespace = vmRef.Namespace
	}

	vm := &kubevirtcorev1.VirtualMachine{}
	if err := r.Get(ctx, types.NamespacedName{Name: vmRef.Name, Namespace: targetVMNamespace}, vm); err != nil {
		if errors.IsNotFound(err) {
			logger.V(1).Info("VirtualMachine not found, skipping run-state restore",
				"vm", vmRef.Name, "namespace", targetVMNamespace)
			return nil
		}
		return fmt.Errorf("failed to get VirtualMachine %s/%s for run-state restore: %w", targetVMNamespace, vmRef.Name, err)
	}

	value, hasValue := vm.Annotations[common.AnnotationOriginalRunStrategy]
	source, hasSource := vm.Annotations[common.AnnotationOriginalRunStrategySource]
	if !hasValue || !hasSource {
		logger.V(1).Info("VirtualMachine has no stashed run state, skipping restore",
			"vm", vmRef.Name, "namespace", targetVMNamespace)
		return nil
	}

	switch source {
	case common.RunStrategySourceRunning:
		running := value == string(kubevirtcorev1.RunStrategyAlways)
		vm.Spec.Running = &running
		vm.Spec.RunStrategy = nil
	case common.RunStrategySourceRunStrategy:
		strategy := kubevirtcorev1.VirtualMachineRunStrategy(value)
		vm.Spec.RunStrategy = &strategy
		vm.Spec.Running = nil
	default:
		return fmt.Errorf("VirtualMachine %s/%s has unrecognized %s value %q",
			targetVMNamespace, vm.Name, common.AnnotationOriginalRunStrategySource, source)
	}

	delete(vm.Annotations, common.AnnotationOriginalRunStrategy)
	delete(vm.Annotations, common.AnnotationOriginalRunStrategySource)

	if err := r.Update(ctx, vm); err != nil {
		return fmt.Errorf("failed to restore VirtualMachine %s/%s run state: %w", targetVMNamespace, vm.Name, err)
	}
	logger.Info("Restored VirtualMachine run state after all DataDownloads completed",
		"vm", vmRef.Name, "namespace", targetVMNamespace, "source", source, "value", value)
	return nil
}

// allSiblingDataDownloadsCompleted reports whether every kubevirt-datamover
// DataDownload in dd's namespace correlated to the same VM (via
// AnnotationVMName/AnnotationVMNamespace -- the same correlation key the
// plugin stamps on every DataDownload it creates, see pvc/restore.go) AND the
// same Velero restore (via the velero.io/restore-name label Velero itself
// stamps on every DataDownload it creates during a restore) has reached
// Completed. Scoping to the current restore matters because Velero doesn't
// immediately GC a prior restore's Failed/Canceled DataDownloads for the same
// VM -- without this, a leftover sibling from an earlier, unrelated restore
// attempt would make this return false forever, permanently leaving the VM
// halted on every later restore of that VM. A single same-restore sibling
// that hasn't completed (including one that failed or was canceled) means
// the VM's disks aren't all restored yet, so it must stay halted.
func (r *KubeVirtDataDownloadReconciler) allSiblingDataDownloadsCompleted(ctx context.Context, dd *velerov2alpha1.DataDownload, vmRef *common.VMReference) (bool, error) {
	ddList := &velerov2alpha1.DataDownloadList{}
	if err := r.List(ctx, ddList, client.InNamespace(dd.Namespace)); err != nil {
		return false, fmt.Errorf("failed to list DataDownloads: %w", err)
	}

	restoreName := dd.Labels[common.LabelVeleroRestoreName]
	for i := range ddList.Items {
		other := &ddList.Items[i]
		if other.Spec.DataMover != common.DataMoverKubeVirt {
			continue
		}
		if other.Labels[common.LabelVeleroRestoreName] != restoreName {
			continue
		}
		otherRef, err := common.GetVMReferenceFromDataDownload(other)
		if err != nil || otherRef.Name != vmRef.Name || otherRef.Namespace != vmRef.Namespace {
			continue
		}
		if other.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
			return false, nil
		}
	}
	return true, nil
}

// checkConcurrentDataMoverLimit gates downloader pod creation against
// MaxConcurrentDataMovers, called from handlePrepared right after the
// existing-pod idempotency check -- before any of the VM-ref/BSL/scratch-PVC
// lookups that only exist to build the pod, since none of that work is
// needed while gated. Returns gated=true (with a RequeueAfterLong result)
// when dd must wait its turn; gated=false when it's clear to proceed
// (including when the limit is disabled, MaxConcurrentDataMovers <= 0).
// Extracted out of handlePrepared to keep that function's cyclomatic
// complexity down (gocyclo).
func (r *KubeVirtDataDownloadReconciler) checkConcurrentDataMoverLimit(ctx context.Context, logger logr.Logger, dd *velerov2alpha1.DataDownload) (gated bool, result ctrl.Result, err error) {
	if r.MaxConcurrentDataMovers <= 0 {
		return false, ctrl.Result{}, nil
	}

	higherPriorityCount, err := r.countHigherPriorityActiveDataDownloads(ctx, dd)
	if err != nil {
		return false, ctrl.Result{}, err
	}
	if higherPriorityCount < r.MaxConcurrentDataMovers {
		return false, ctrl.Result{}, nil
	}

	logger.Info("Deferring downloader pod creation, at concurrent data mover limit",
		"higherPriorityCount", higherPriorityCount, "limit", r.MaxConcurrentDataMovers)
	// Waiting for a concurrent-data-mover slot is intentional throttling, not a
	// stalled operation -- advance AcceptedTimestamp by the same duration we're
	// about to wait so this deferral doesn't consume the DataDownload's
	// Spec.OperationTimeout budget (checked unconditionally on every reconcile
	// in Reconcile, before dispatch reaches handlePrepared). A genuinely stuck
	// higher-priority peer still ages out normally (its own AcceptedTimestamp
	// is untouched by this), freeing this DD's slot once that peer times out
	// or completes -- so this can't cause indefinite starvation, only push the
	// deadline out for as long as this DD is legitimately waiting its turn.
	if dd.Status.AcceptedTimestamp != nil {
		// Re-fetch immediately before writing: this path runs every
		// RequeueAfterLong for as long as dd stays gated -- potentially many
		// times over a long queue wait -- far more often than any other write
		// in this reconciler, so it's the one most likely to collide with a
		// concurrent external update (e.g. an annotation patch) and hit a
		// resource-version conflict. A Get failure here only costs this one
		// cycle's timeout-budget exemption, not the gating decision itself, so
		// it's logged and skipped rather than propagated as a reconcile error.
		latest := dd.DeepCopy()
		if getErr := r.Get(ctx, client.ObjectKeyFromObject(dd), latest); getErr != nil {
			logger.Error(getErr, "Failed to re-fetch DataDownload before advancing AcceptedTimestamp, skipping this cycle's advance")
		} else if latest.Status.AcceptedTimestamp != nil {
			advanced := metav1.NewTime(latest.Status.AcceptedTimestamp.Add(RequeueAfterLong))
			latest.Status.AcceptedTimestamp = &advanced
			if err := r.Update(ctx, latest); err != nil {
				return false, ctrl.Result{}, fmt.Errorf("failed to advance AcceptedTimestamp while gated: %w", err)
			}
			*dd = *latest
		}
	}
	return true, ctrl.Result{RequeueAfter: RequeueAfterLong}, nil
}

// countHigherPriorityActiveDataDownloads counts kubevirt-datamover
// DataDownloads in dd's namespace, excluding dd itself, that are in an active
// phase (Accepted, Prepared, InProgress) AND outranks dd per
// outranksDataDownload's ordering. Used by handlePrepared to gate pod
// creation against MaxConcurrentDataMovers.
//
// Ranking (rather than a raw active-CR count) is what guarantees forward
// progress when N siblings all reach Prepared together -- the normal
// multi-disk-VM-restore case, since Velero creates every disk's DataDownload
// for a VM restore at once. A raw "count of other active CRs >= limit" check
// is symmetric across all of them: each sibling sees N-1 others active, and
// if N-1 >= limit, every single one defers, forever -- none can ever create
// a pod because reaching InProgress requires passing a gate that's held by
// peers stuck at the very same gate. Ranking breaks that symmetry: it's a
// stable total order (by CreationTimestamp, with UID as a final tiebreak for
// exact ties) that every reconciler computes independently from the same
// List, so exactly the first MaxConcurrentDataMovers-ranked siblings ever
// see a higher-priority count below the limit and proceed. As earlier-ranked
// ones complete (leaving the active set), later-ranked siblings' count drops
// and they get their turn. Ranking deliberately does NOT use
// Status.AcceptedTimestamp: handlePrepared's gate-defer path advances that
// field forward to exempt legitimate queue-wait time from
// Spec.OperationTimeout (see the defer branch below), and CreationTimestamp
// staying untouched by that is what keeps ranking a fixed, fair order
// instead of one a deferred DD could perturb by continuing to wait. New
// (pre-provisioning) is excluded from the count, same as the phase set the
// full resource window (scratch/work/output PVCs, mover pod) actually
// covers.
func (r *KubeVirtDataDownloadReconciler) countHigherPriorityActiveDataDownloads(ctx context.Context, dd *velerov2alpha1.DataDownload) (int, error) {
	ddList := &velerov2alpha1.DataDownloadList{}
	if err := r.List(ctx, ddList, client.InNamespace(dd.Namespace)); err != nil {
		return 0, fmt.Errorf("failed to list DataDownloads: %w", err)
	}

	count := 0
	for i := range ddList.Items {
		other := &ddList.Items[i]
		if other.UID == dd.UID {
			continue
		}
		if other.Spec.DataMover != common.DataMoverKubeVirt {
			continue
		}
		switch other.Status.Phase {
		case velerov2alpha1.DataDownloadPhaseAccepted,
			velerov2alpha1.DataDownloadPhasePrepared,
			velerov2alpha1.DataDownloadPhaseInProgress:
		default:
			continue
		}
		if outranksDataDownload(other, dd) {
			count++
		}
	}
	return count, nil
}

// outranksDataDownload reports whether a has priority over b for the
// concurrent-data-mover gate: an earlier CreationTimestamp wins, with UID as
// a tiebreaker for exact ties -- same convention as hasOlderActiveDUForVM's
// per-VM serialization on the upload side. A strict total order over any set
// of DataDownloads, computed identically by every reconciler from the same
// List, is what lets countHigherPriorityActiveDataDownloads avoid the
// gate-deadlock a raw active count would hit when siblings reach Prepared
// together.
func outranksDataDownload(a, b *velerov2alpha1.DataDownload) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.UID < b.UID
}

// getPodNamespace returns the namespace where downloader pods (and the scratch PVC)
// should live. Uses OADPNamespace if configured, otherwise falls back to the
// DataDownload's own namespace.
func (r *KubeVirtDataDownloadReconciler) getPodNamespace(dd *velerov2alpha1.DataDownload) string {
	if r.OADPNamespace != "" {
		return r.OADPNamespace
	}
	return dd.Namespace
}

// setOwnerReferenceIfSameNamespace sets dd as owner of obj if they share a
// namespace -- owner references are rejected across namespaces. Mirrors
// DataUpload's existing precedent for its cross-namespace temp PVC (which skips
// the owner ref entirely rather than erroring): r.OADPNamespace is expected to
// equal dd.Namespace by convention, but that isn't guaranteed by any check, so
// this degrades gracefully instead of failing the whole reconcile.
func setOwnerReferenceIfSameNamespace(dd *velerov2alpha1.DataDownload, obj client.Object, scheme *runtime.Scheme, logger logr.Logger) error {
	if obj.GetNamespace() != dd.Namespace {
		logger.V(1).Info("Skipping owner reference: object namespace differs from DataDownload namespace",
			"objectNamespace", obj.GetNamespace(), "dataDownloadNamespace", dd.Namespace)
		return nil
	}
	return controllerutil.SetOwnerReference(dd, obj, scheme)
}

// getBackupStorageLocationForDD fetches the BSL referenced by a DataDownload.
func (r *KubeVirtDataDownloadReconciler) getBackupStorageLocationForDD(ctx context.Context, dd *velerov2alpha1.DataDownload) (*velerov1.BackupStorageLocation, error) {
	bsl, err := getBackupStorageLocation(ctx, r.Client, dd.Spec.BackupStorageLocation, r.OADPNamespace, dd.Namespace)
	if err != nil {
		return nil, fmt.Errorf("DataDownload %s/%s: %w", dd.Namespace, dd.Name, err)
	}
	return bsl, nil
}

// findPodForDataDownload finds the unique downloader pod associated with a DataDownload.
func (r *KubeVirtDataDownloadReconciler) findPodForDataDownload(ctx context.Context, dd *velerov2alpha1.DataDownload, namespace string) (*corev1.Pod, error) {
	return findPodByUID(ctx, r.Client, r.APIReader, common.LabelDataDownloadUID, string(dd.UID), namespace)
}

// SetupWithManager sets up the controller with the Manager
func (r *KubeVirtDataDownloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrent := r.MaxConcurrentReconciles
	if maxConcurrent <= 0 {
		maxConcurrent = DefaultMaxConcurrentReconciles
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov2alpha1.DataDownload{}).
		WithEventFilter(r.filterKubeVirtDataMoverDownload()).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrent,
		}).
		Named("kubevirt-datadownload").
		Complete(r)
}

// filterKubeVirtDataMoverDownload returns a predicate that filters for DataDownloads
// where Spec.DataMover is "kubevirt"
func (r *KubeVirtDataDownloadReconciler) filterKubeVirtDataMoverDownload() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		dd, ok := obj.(*velerov2alpha1.DataDownload)
		if !ok {
			return false
		}
		return dd.Spec.DataMover == common.DataMoverKubeVirt
	})
}
