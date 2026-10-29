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
package controller

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// ddScheme returns a scheme with only the types the DataDownload controller
// needs. Deliberately does NOT register kubevirtcorev1 -- handleNew/handleAccepted
// must never fetch a live VirtualMachine object; if they tried, the fake client
// would fail with "no kind registered" and these tests would catch it.
func ddScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	return scheme
}

func TestDataDownloadReconcile(t *testing.T) {
	scheme := ddScheme()

	tests := []struct {
		name            string
		dataDownload    *velerov2alpha1.DataDownload
		expectedRequeue bool
		expectedPhase   velerov2alpha1.DataDownloadPhase
	}{
		{
			name: "skip non-kubevirt datamover",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: "velero"},
			},
			expectedRequeue: false,
			expectedPhase:   "",
		},
		{
			name: "new phase without VM annotation fails",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataDownloadPhaseFailed,
		},
		{
			name: "terminal phase is a no-op",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
				Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseCompleted},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataDownloadPhaseCompleted,
		},
		{
			name: "Cancel requested while InProgress routes to Canceling instead of the normal handler",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt, Cancel: true},
				Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseInProgress},
			},
			expectedRequeue: true,
			expectedPhase:   velerov2alpha1.DataDownloadPhaseCanceling,
		},
		{
			name: "Cancel requested while already terminal is ignored",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt, Cancel: true},
				Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseFailed},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataDownloadPhaseFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.dataDownload).Build()
			r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

			result, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Name: tt.dataDownload.Name, Namespace: tt.dataDownload.Namespace},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if (result.RequeueAfter > 0) != tt.expectedRequeue {
				t.Errorf("requeue = %v, want %v", result.RequeueAfter > 0, tt.expectedRequeue)
			}

			var updated velerov2alpha1.DataDownload
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: tt.dataDownload.Name, Namespace: tt.dataDownload.Namespace}, &updated); err != nil {
				t.Fatalf("failed to get DataDownload: %v", err)
			}
			if updated.Status.Phase != tt.expectedPhase {
				t.Errorf("phase = %q, want %q", updated.Status.Phase, tt.expectedPhase)
			}
		})
	}

	t.Run("not found is ignored", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}
		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "missing", Namespace: "ns"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue, got %v", result.RequeueAfter)
		}
	})

	t.Run("Cancel requested but restore already provisioned completes instead of canceling", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-dd", Namespace: "openshift-adp", UID: types.UID("dd-uid-provisioned"),
				Annotations: map[string]string{AnnotationDownloaderPodSucceeded: downloaderPodSucceededValue},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover: common.DataMoverKubeVirt,
				Cancel:    true,
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC:       "restored-disk-1",
					Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseInProgress},
		}
		targetPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "restored-disk-1", Namespace: "restore-ns", UID: types.UID("restored-disk-1-uid"),
			},
		}
		reboundPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "pv-rebound",
				Labels: map[string]string{common.LabelDataDownloadUID: string(dd.UID)},
			},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{Name: "restored-disk-1", Namespace: "restore-ns", UID: targetPVC.UID},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, reboundPV, targetPVC).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue, got %v", result.RequeueAfter)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
			t.Errorf("phase = %q, want %q (Cancel must not override an already-provisioned restore)",
				updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted)
		}
	})
}

// TestDataDownloadReconcile_OperationTimeout covers Spec.OperationTimeout
// enforcement across several independent t.Run subtests.
//
//nolint:gocyclo // Table of independent subtests, not complex control flow
func TestDataDownloadReconcile_OperationTimeout(t *testing.T) {
	scheme := ddScheme()

	get := func(t *testing.T, c client.Client, name, namespace string) *velerov2alpha1.DataDownload {
		t.Helper()
		var out velerov2alpha1.DataDownload
		if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &out); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		return &out
	}

	t.Run("Accepted phase past default operation timeout fails", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "dd-timeout", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseAccepted,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue after timeout failure, got %v", result.RequeueAfter)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
		if !strings.Contains(updated.Status.Message, "operation timed out") {
			t.Errorf("message = %q, want it to mention the timeout", updated.Status.Message)
		}
	})

	t.Run("Prepared phase past default operation timeout fails", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "dd-prepared-timeout", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhasePrepared,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue after timeout failure, got %v", result.RequeueAfter)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
		if !strings.Contains(updated.Status.Message, "operation timed out") {
			t.Errorf("message = %q, want it to mention the timeout", updated.Status.Message)
		}
	})

	t.Run("InProgress phase respects custom Spec.OperationTimeout", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "dd-custom-timeout", Namespace: "openshift-adp"},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover:        common.DataMoverKubeVirt,
				OperationTimeout: metav1.Duration{Duration: time.Hour},
			},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseInProgress,
				AcceptedTimestamp: ptrTime(time.Now().Add(-2 * time.Hour)),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q (custom 1h OperationTimeout exceeded by 2h elapsed)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
	})

	t.Run("nil AcceptedTimestamp is backfilled without failing", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-backfill", Namespace: "openshift-adp",
				Annotations: map[string]string{common.AnnotationVMName: "vm-1", common.AnnotationVMNamespace: "vm-ns"},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover: common.DataMoverKubeVirt,
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC: "restored-disk-1", Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseAccepted},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseAccepted {
			t.Errorf("phase = %q, want %q (backfilling a missing AcceptedTimestamp must not fail the DataDownload, message: %q)",
				updated.Status.Phase, velerov2alpha1.DataDownloadPhaseAccepted, updated.Status.Message)
		}
		if updated.Status.AcceptedTimestamp == nil {
			t.Errorf("expected AcceptedTimestamp to be backfilled, got nil")
		}
	})

	t.Run("Canceling phase is not subject to operation timeout", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "dd-canceling", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseCanceling,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Hour))),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCanceled {
			t.Errorf("phase = %q, want %q (Canceling must run to completion, not be preempted by the timeout check)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCanceled)
		}
	})

	t.Run("handler's RequeueAfterLong is capped to the remaining custom OperationTimeout", func(t *testing.T) {
		// handleAccepted's target-PVC-not-found branch normally requeues after
		// RequeueAfterLong (30s) -- with a custom OperationTimeout that has
		// nearly elapsed, that would overshoot the deadline. The returned
		// RequeueAfter must be capped to (roughly) what's left instead. Uses a
		// 30s timeout with 27s elapsed (a wider margin than a few-second
		// timeout) so the test isn't flaky against real wall-clock execution
		// overhead.
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-cap-requeue", Namespace: "openshift-adp",
				Annotations: map[string]string{common.AnnotationVMName: "vm-1", common.AnnotationVMNamespace: "vm-ns"},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover:        common.DataMoverKubeVirt,
				OperationTimeout: metav1.Duration{Duration: 30 * time.Second},
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC: "not-yet-created", Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseAccepted,
				AcceptedTimestamp: ptrTime(time.Now().Add(-27 * time.Second)),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter <= 0 || result.RequeueAfter >= RequeueAfterLong {
			t.Errorf("RequeueAfter = %v, want it capped below RequeueAfterLong (%v) to the ~3s remaining before the 30s OperationTimeout deadline", result.RequeueAfter, RequeueAfterLong)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseAccepted {
			t.Errorf("phase = %q, want %q (timeout not yet exceeded)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseAccepted)
		}
	})

	t.Run("handler's RequeueAfterLong is capped using the default timeout when Spec.OperationTimeout is unset", func(t *testing.T) {
		// Same as above, but with Spec.OperationTimeout left at its zero value:
		// capRequeueToOperationDeadline must fall back to DefaultOperationTimeout
		// (via operationTimeoutExceeded), consistently with checkOperationTimeout,
		// rather than treating an unset timeout as "no cap."
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-cap-requeue-default-timeout", Namespace: "openshift-adp",
				Annotations: map[string]string{common.AnnotationVMName: "vm-1", common.AnnotationVMNamespace: "vm-ns"},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover: common.DataMoverKubeVirt,
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC: "not-yet-created", Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseAccepted,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout - 3*time.Second))),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter <= 0 || result.RequeueAfter >= RequeueAfterLong {
			t.Errorf("RequeueAfter = %v, want it capped below RequeueAfterLong (%v) to the ~3s remaining before the DefaultOperationTimeout deadline", result.RequeueAfter, RequeueAfterLong)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseAccepted {
			t.Errorf("phase = %q, want %q (timeout not yet exceeded)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseAccepted)
		}
	})

	t.Run("New phase's first requeue is capped when Spec.OperationTimeout is shorter than RequeueAfterShort", func(t *testing.T) {
		// handleNew sets AcceptedTimestamp and transitions New -> Accepted in the
		// same reconcile that creates it, returning RequeueAfterShort (5s). With a
		// custom OperationTimeout shorter than that, the very first requeue --
		// not just subsequent ones -- must already be capped to the deadline.
		bslAvailable := &velerov1.BackupStorageLocation{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-adp"},
			Status:     velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
		}
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-new-cap-requeue", Namespace: "openshift-adp",
				Annotations: map[string]string{common.AnnotationVMName: "vm-1", common.AnnotationVMNamespace: "vm-ns"},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover:             common.DataMoverKubeVirt,
				BackupStorageLocation: "default",
				OperationTimeout:      metav1.Duration{Duration: 2 * time.Second},
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC: "restored-disk-1", Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseNew},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, bslAvailable).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseAccepted {
			t.Fatalf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseAccepted)
		}
		if result.RequeueAfter <= 0 || result.RequeueAfter >= RequeueAfterShort {
			t.Errorf("RequeueAfter = %v, want it capped below RequeueAfterShort (%v) to the ~2s remaining before the 2s OperationTimeout deadline", result.RequeueAfter, RequeueAfterShort)
		}
	})

	t.Run("terminal transition within the same reconcile is never given a nonzero requeue by the cap", func(t *testing.T) {
		// handleAccepted's own (unrelated-to-timeout) failure path -- e.g. a
		// missing VM reference -- transitions straight to Failed and returns
		// ctrl.Result{} (no requeue) in the same reconcile that ran
		// checkOperationTimeout. Guards against capRequeueToOperationDeadline
		// ever being applied to a terminal transition's result.
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "dd-terminal-no-requeue", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseAccepted,
				AcceptedTimestamp: ptrTime(time.Now()),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("RequeueAfter = %v, want 0 (terminal transition must not be requeued)", result.RequeueAfter)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Fatalf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
		if strings.Contains(updated.Status.Message, "operation timed out") {
			t.Errorf("message = %q, want a handler-level failure (missing VM reference), not a timeout", updated.Status.Message)
		}
	})

	t.Run("timeout does not override a restore that already provisioned the target volume", func(t *testing.T) {
		// Mirrors the Cancel-vs-provisioned race already handled at the top of
		// Reconcile: a restore whose rebind already committed (PV claimRef set)
		// but whose Completed phase update hasn't persisted yet (e.g. a transient
		// API error right after a successful rebind) must not be misreported as
		// Failed just because Spec.OperationTimeout has since expired.
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-timeout-provisioned", Namespace: "openshift-adp", UID: types.UID("dd-timeout-provisioned-uid"),
				Annotations: map[string]string{AnnotationDownloaderPodSucceeded: downloaderPodSucceededValue},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover: common.DataMoverKubeVirt,
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC:       "restored-disk-1",
					Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseInProgress,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			},
		}
		targetPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "restored-disk-1", Namespace: "restore-ns", UID: types.UID("restored-disk-1-uid"),
			},
		}
		reboundPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "pv-rebound-timeout",
				Labels: map[string]string{common.LabelDataDownloadUID: string(dd.UID)},
			},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{Name: "restored-disk-1", Namespace: "restore-ns", UID: targetPVC.UID},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, reboundPV, targetPVC).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
			t.Errorf("phase = %q, want %q (an already-provisioned restore must finalize as Completed, not be failed by an expired timeout)",
				updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted)
		}
	})

	t.Run("timeout failure stops the still-running downloader pod", func(t *testing.T) {
		// A timeout can fire while the downloader pod is still Pending/Running --
		// that's exactly the unbounded-wait branch being guarded against -- unlike
		// other Failed paths where the pod has already terminated on its own.
		// Verifies checkOperationTimeoutCore's fail callback actually stops it
		// rather than leaving it running indefinitely against a terminal DataDownload.
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-timeout-pod-cleanup", Namespace: "openshift-adp",
				UID: types.UID("dd-timeout-pod-cleanup-uid"),
			},
			Spec: velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseInProgress,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			},
		}
		runningPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-timeout-pod-cleanup-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(dd.UID)},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, runningPod).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Fatalf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
		var pod corev1.Pod
		err := fakeClient.Get(context.Background(), types.NamespacedName{Name: runningPod.Name, Namespace: runningPod.Namespace}, &pod)
		if !errors.IsNotFound(err) {
			t.Errorf("expected downloader pod to be deleted after timeout failure, got err=%v", err)
		}
	})

	t.Run("timeout failure requeues quietly instead of erroring while the downloader pod is still terminating", func(t *testing.T) {
		// The pod-cleanup step inside the timeout fail callback can hit the same
		// expected, self-resolving ErrPodsStillTerminating as handleCanceling's own
		// cleanup -- kubelet just hasn't finished tearing the pod down yet. Must be
		// treated the same way: a quiet short requeue, not a logged reconcile error
		// with controller-runtime's (much slower) exponential backoff.
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-timeout-pod-terminating", Namespace: "openshift-adp",
				UID: types.UID("dd-timeout-pod-terminating-uid"),
			},
			Spec: velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseInProgress,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			},
		}
		terminatingPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-timeout-pod-terminating-pod", Namespace: "openshift-adp",
				Labels:     map[string]string{common.LabelDataDownloadUID: string(dd.UID)},
				Finalizers: []string{"example.com/still-cleaning-up"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, terminatingPod).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter == 0 {
			t.Error("expected a short requeue while the downloader pod is still terminating")
		}

		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseFailed {
			t.Error("phase must not be persisted Failed until pod cleanup actually succeeds")
		}
	})
}

func TestDataDownloadUpdatePhase(t *testing.T) {
	scheme := ddScheme()
	dd := &velerov2alpha1.DataDownload{
		ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
		Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseNew},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

	if err := r.updatePhase(context.Background(), dd, velerov2alpha1.DataDownloadPhaseAccepted, "accepted"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dd.Status.Phase != velerov2alpha1.DataDownloadPhaseAccepted {
		t.Errorf("phase = %q, want %q", dd.Status.Phase, velerov2alpha1.DataDownloadPhaseAccepted)
	}

	var before velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}, &before); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}

	// Idempotent: same phase + message should skip the update (no error, no-op).
	if err := r.updatePhase(context.Background(), dd, velerov2alpha1.DataDownloadPhaseAccepted, "accepted"); err != nil {
		t.Fatalf("unexpected error on idempotent update: %v", err)
	}

	var after velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}, &after); err != nil {
		t.Fatalf("failed to re-get DataDownload: %v", err)
	}
	if before.ResourceVersion != after.ResourceVersion {
		t.Errorf("expected no write on idempotent update, resourceVersion changed %s -> %s",
			before.ResourceVersion, after.ResourceVersion)
	}
}

// TestDataDownloadUpdatePhase_TimestampsSet covers #155: updatePhase must
// populate Status.StartTimestamp on entering InProgress and
// Status.CompletionTimestamp on entering any terminal phase, matching
// Velero's own built-in data movers so `velero restore describe` reports
// timing consistently. Both must be idempotent -- set once, not reset on a
// later call with an already-set value.
func TestDataDownloadUpdatePhase_TimestampsSet(t *testing.T) {
	scheme := ddScheme()

	t.Run("StartTimestamp set on entering InProgress", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
			Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhasePrepared},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.updatePhase(context.Background(), dd, velerov2alpha1.DataDownloadPhaseInProgress, "Downloader pod launched"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dd.Status.StartTimestamp == nil {
			t.Fatal("expected StartTimestamp to be set")
		}
	})

	t.Run("CompletionTimestamp set on entering a terminal phase", func(t *testing.T) {
		for _, phase := range []velerov2alpha1.DataDownloadPhase{
			velerov2alpha1.DataDownloadPhaseCompleted,
			velerov2alpha1.DataDownloadPhaseFailed,
			velerov2alpha1.DataDownloadPhaseCanceled,
		} {
			dd := &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd-" + string(phase), Namespace: "openshift-adp"},
				Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseInProgress},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
			r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

			if err := r.updatePhase(context.Background(), dd, phase, "done"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if dd.Status.CompletionTimestamp == nil {
				t.Errorf("phase %s: expected CompletionTimestamp to be set", phase)
			}
		}
	})

	t.Run("timestamps are idempotent, not reset by a later call", func(t *testing.T) {
		earlier := metav1.NewTime(time.Now().Add(-time.Hour))
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:               velerov2alpha1.DataDownloadPhaseInProgress,
				StartTimestamp:      &earlier,
				CompletionTimestamp: &earlier,
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.updatePhase(context.Background(), dd, velerov2alpha1.DataDownloadPhaseCompleted, "done"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !dd.Status.StartTimestamp.Equal(&earlier) {
			t.Errorf("StartTimestamp = %v, want it unchanged from %v", dd.Status.StartTimestamp, earlier)
		}
		if !dd.Status.CompletionTimestamp.Equal(&earlier) {
			t.Errorf("CompletionTimestamp = %v, want it unchanged from %v", dd.Status.CompletionTimestamp, earlier)
		}
	})
}

func TestHandleNewDataDownload(t *testing.T) {
	scheme := ddScheme()

	bslAvailable := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-adp"},
		Status:     velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
	}
	bslUnavailable := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-adp"},
		Status:     velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseUnavailable},
	}

	tests := []struct {
		name          string
		dd            *velerov2alpha1.DataDownload
		bsl           *velerov1.BackupStorageLocation
		expectedPhase velerov2alpha1.DataDownloadPhase
	}{
		{
			name: "missing VM annotation fails",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "dd-1", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default"},
			},
			bsl:           bslAvailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseFailed,
		},
		{
			name: "BSL not found fails",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dd-2", Namespace: "openshift-adp",
					Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
				},
				Spec: velerov2alpha1.DataDownloadSpec{
					DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "missing-bsl",
					SourceNamespace: "vm-ns",
					TargetVolume:    velerov2alpha1.TargetVolumeSpec{PVC: "restored-disk-1", Namespace: "restore-ns"},
				},
			},
			bsl:           bslAvailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseFailed,
		},
		{
			name: "BSL not available fails",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dd-3", Namespace: "openshift-adp",
					Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
				},
				Spec: velerov2alpha1.DataDownloadSpec{
					DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default",
					SourceNamespace: "vm-ns",
					TargetVolume:    velerov2alpha1.TargetVolumeSpec{PVC: "restored-disk-1", Namespace: "restore-ns"},
				},
			},
			bsl:           bslUnavailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseFailed,
		},
		{
			name: "valid annotation and available BSL transitions to Accepted",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dd-4", Namespace: "openshift-adp",
					Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
				},
				Spec: velerov2alpha1.DataDownloadSpec{
					DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default",
					SourceNamespace: "vm-ns",
					TargetVolume:    velerov2alpha1.TargetVolumeSpec{PVC: "restored-disk-1", Namespace: "restore-ns"},
				},
			},
			bsl:           bslAvailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseAccepted,
		},
		{
			name: "missing TargetVolume fails",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dd-5", Namespace: "openshift-adp",
					Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
				},
				Spec: velerov2alpha1.DataDownloadSpec{
					DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default",
					SourceNamespace: "vm-ns",
				},
			},
			bsl:           bslAvailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.dd, tt.bsl).Build()
			r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

			// This would panic/error if handleNew ever tried to Get a VirtualMachine,
			// since kubevirtcorev1 isn't registered in ddScheme().
			if _, err := r.handleNew(context.Background(), logr.Discard(), tt.dd); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var updated velerov2alpha1.DataDownload
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: tt.dd.Name, Namespace: tt.dd.Namespace}, &updated); err != nil {
				t.Fatalf("failed to get DataDownload: %v", err)
			}
			if updated.Status.Phase != tt.expectedPhase {
				t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, tt.expectedPhase, updated.Status.Message)
			}
			if tt.expectedPhase == velerov2alpha1.DataDownloadPhaseAccepted && updated.Status.AcceptedTimestamp == nil {
				t.Error("AcceptedTimestamp not set when transitioning to Accepted")
			}
		})
	}
}

// TestHandleNewDataDownload_BSLTransientError_RequeuesWithoutFailing covers
// #123: a transient error from the BSL lookup (API hiccup, cache-not-yet-synced
// -- anything other than a genuine NotFound) must not terminally fail the
// DataDownload the way a real "BSL doesn't exist" does. It should be returned
// so controller-runtime retries with backoff, since the condition may resolve
// on its own.
func TestHandleNewDataDownload_BSLTransientError_RequeuesWithoutFailing(t *testing.T) {
	scheme := ddScheme()

	dd := &velerov2alpha1.DataDownload{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dd-bsl-transient", Namespace: "openshift-adp",
			Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
		},
		Spec: velerov2alpha1.DataDownloadSpec{
			DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default",
			SourceNamespace: "vm-ns",
			TargetVolume:    velerov2alpha1.TargetVolumeSpec{PVC: "restored-disk-1", Namespace: "restore-ns"},
		},
	}

	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
	transientErr := fmt.Errorf("simulated transient API error")
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*velerov1.BackupStorageLocation); ok {
				return transientErr
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	r := &KubeVirtDataDownloadReconciler{Client: interceptedClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	_, err := r.handleNew(context.Background(), logr.Discard(), dd)
	if err == nil {
		t.Fatal("expected the transient error to be returned for retry, got nil")
	}
	if !strings.Contains(err.Error(), transientErr.Error()) {
		t.Errorf("error = %v, want it to contain %v", err, transientErr)
	}

	var updated velerov2alpha1.DataDownload
	if getErr := baseClient.Get(context.Background(), types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}, &updated); getErr != nil {
		t.Fatalf("failed to get DataDownload: %v", getErr)
	}
	if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseFailed {
		t.Error("phase must not be Failed on a transient BSL lookup error -- only a genuine NotFound is terminal")
	}
}

func TestCalculateScratchPVCSize(t *testing.T) {
	vmIndex := uploader.VMIndex{
		VMName:    "vm-1",
		Namespace: "vm-ns",
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:       "cp-1",
				PVCs:     []string{"pvc-1", "pvc-2"},
				PVCSizes: []resource.Quantity{resource.MustParse("10Gi"), resource.MustParse("5Gi")},
				Files: []uploader.CheckpointFile{
					{DiskName: "disk-1"},
					{DiskName: "disk-2"},
				},
			},
			{
				ID:       "cp-2",
				PVCs:     []string{"pvc-1"},
				PVCSizes: []resource.Quantity{resource.MustParse("15Gi")},
				Files: []uploader.CheckpointFile{
					{DiskName: "disk-1"},
				},
			},
			{
				// PVCSizes deliberately shorter than Files -- unlike cp-1/cp-2, this
				// entry IS in the matched chain but its PVCSizes entry for disk-1 is
				// missing, exercising the `i >= len(entry.PVCSizes)` skip itself
				// rather than the chain-never-matched case (see cp-99 tests below).
				ID:       "cp-3",
				PVCs:     []string{"pvc-3"},
				PVCSizes: nil,
				Files: []uploader.CheckpointFile{
					{DiskName: "disk-1"},
				},
			},
		},
	}

	// Computed via the same addOverhead helper the production code uses, rather
	// than a hand-picked literal, so this pins the formula (max PVCSize + sum file
	// sizes + overhead) without risking a hand-computed value drifting from it.
	maxPVCSize := resource.MustParse("15Gi") // max(10Gi, 15Gi) across the chain
	expectedBase := maxPVCSize.DeepCopy()
	expectedBase.Add(resource.MustParse("2Gi")) // sum of file sizes (1Gi + 1Gi)
	expectedSized := addOverhead(expectedBase, sizeOverheadPercent)

	tests := []struct {
		name               string
		chain              []string
		targetVolume       string
		files              []uploader.CheckpointFile
		targetDiskCapacity resource.Quantity
		expectExact        *resource.Quantity
	}{
		{
			name:         "sized from max PVCSizes across chain plus file sizes",
			chain:        []string{"cp-1", "cp-2"},
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi
			},
			expectExact: &expectedSized,
		},
		{
			name:         "no matching checkpoints or files falls back to default",
			chain:        []string{"cp-99"},
			targetVolume: "disk-1",
			files:        nil,
			expectExact:  new(resource.MustParse(DefaultScratchPVCSize)),
		},
		{
			name:         "missing PVCSizes floored by target disk capacity before overhead",
			chain:        []string{"cp-99"}, // no chain match -> maxDiskSize from PVCSizes stays zero
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi
			},
			targetDiskCapacity: resource.MustParse("20Gi"),
			// floor(20Gi) + 1Gi file = 21Gi, then overhead -- computed the same way
			// as expectedSized above, not hand-picked.
			expectExact: func() *resource.Quantity {
				base := resource.MustParse("20Gi")
				base.Add(resource.MustParse("1Gi"))
				q := addOverhead(base, sizeOverheadPercent)
				return &q
			}(),
		},
		{
			// No raw-disk-size floor at all (no PVCSizes match, no target capacity),
			// but the chain itself has files -- sizing off the chain size alone
			// (undoubled) would under-provision for the flattened raw output the
			// downloader writes alongside the still-present chain. Small chain size
			// stays under the default, so the plain default wins.
			name:         "maxDiskSize zero with small file chain returns plain default",
			chain:        []string{"cp-99"},
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi, well under the 10Gi default
			},
			expectExact: new(resource.MustParse(DefaultScratchPVCSize)),
		},
		{
			// Same no-floor scenario, but the chain size itself exceeds the default --
			// must size off the chain doubled (chain + flattened raw coexist), not the
			// now-insufficient default.
			name:         "maxDiskSize zero with large file chain doubles chain size instead of default",
			chain:        []string{"cp-99"},
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 15 * 1024 * 1024 * 1024}, // 15Gi, exceeds the 10Gi default
			},
			expectExact: func() *resource.Quantity {
				q := addOverhead(*resource.NewQuantity(30*1024*1024*1024, resource.BinarySI), sizeOverheadPercent)
				return &q
			}(),
		},
		{
			name:         "matched checkpoint with missing PVCSizes entry is skipped, still floored by target capacity",
			chain:        []string{"cp-3"}, // chain DOES match this entry, unlike the case above
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi
			},
			targetDiskCapacity: resource.MustParse("20Gi"),
			expectExact: func() *resource.Quantity {
				base := resource.MustParse("20Gi")
				base.Add(resource.MustParse("1Gi"))
				q := addOverhead(base, sizeOverheadPercent)
				return &q
			}(),
		},
		{
			// A nonzero (unlike the two "maxDiskSize zero" cases above) but tiny
			// target disk capacity: this still reaches the final branch (total +
			// overhead), which is what the 1Gi minimum actually guards -- the
			// "maxDiskSize zero" branch above never reaches that floor at all.
			name:               "small target disk capacity is floored to the 1Gi minimum",
			chain:              []string{"cp-99"}, // no chain match
			targetVolume:       "disk-1",
			files:              nil,
			targetDiskCapacity: resource.MustParse("100Mi"),
			expectExact:        new(resource.MustParse("1Gi")),
		},
		{
			name:         "negative file size is ignored, not summed, falling back to default",
			chain:        []string{"cp-99"},
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: -1024},
			},
			// The negative entry is dropped rather than summed (see
			// calculateScratchPVCSize), so with no other size signal this hits the
			// same default-fallback path as "maxDiskSize zero with small file chain"
			// above.
			expectExact: new(resource.MustParse(DefaultScratchPVCSize)),
		},
		{
			name:         "negative file size does not offset a legitimate positive file size in the same list",
			chain:        []string{"cp-99"},
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 20 * 1024 * 1024 * 1024}, // 20Gi -- legitimate, exceeds the default so doubling applies
				{Size: -5 * 1024 * 1024 * 1024}, // -5Gi -- corrupt/invalid entry; must not subtract from the 20Gi above
			},
			expectExact: func() *resource.Quantity {
				q := addOverhead(*resource.NewQuantity(40*1024*1024*1024, resource.BinarySI), sizeOverheadPercent)
				return &q
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateScratchPVCSize(logr.Discard(), vmIndex, tt.chain, tt.targetVolume, tt.files, tt.targetDiskCapacity)
			if got.Cmp(*tt.expectExact) != 0 {
				t.Errorf("size = %s, want exactly %s", got.String(), tt.expectExact.String())
			}
		})
	}
}

// TestCalculateWorkPVCSize covers the Block-mode restore's work PVC sizing:
// unlike calculateScratchPVCSize, it must drop the raw-disk-size component
// entirely (the flattened image lands on the separate output PVC instead) and
// size purely off the resolved checkpoint files' sizes.
func TestCalculateWorkPVCSize(t *testing.T) {
	tests := []struct {
		name        string
		files       []uploader.CheckpointFile
		expectExact resource.Quantity
	}{
		{
			name: "sized from file sizes plus overhead",
			files: []uploader.CheckpointFile{
				{Size: 4 * 1024 * 1024 * 1024}, // 4Gi
				{Size: 2 * 1024 * 1024 * 1024}, // 2Gi
			},
			expectExact: addOverhead(*resource.NewQuantity(6*1024*1024*1024, resource.BinarySI), sizeOverheadPercent),
		},
		{
			name:        "no file size metadata at all falls back to default",
			files:       nil,
			expectExact: resource.MustParse(DefaultScratchPVCSize),
		},
		{
			name: "genuinely tiny non-zero chain floors at 1Gi",
			files: []uploader.CheckpointFile{
				{Size: 1024}, // 1KiB -- not zero, so no-metadata fallback must not apply
			},
			expectExact: resource.MustParse("1Gi"),
		},
		{
			name: "negative file size is ignored, not summed, falling back to default",
			files: []uploader.CheckpointFile{
				{Size: -1024},
			},
			// The negative entry is dropped rather than summed (same guard as
			// calculateScratchPVCSize), so with no other size signal totalFileSize
			// stays 0 and this hits the no-metadata default fallback.
			expectExact: resource.MustParse(DefaultScratchPVCSize),
		},
		{
			name: "negative file size does not offset a legitimate positive file size in the same list",
			files: []uploader.CheckpointFile{
				{Size: 4 * 1024 * 1024 * 1024},  // 4Gi -- legitimate
				{Size: -2 * 1024 * 1024 * 1024}, // -2Gi -- corrupt/invalid entry; must not subtract from the 4Gi above
			},
			expectExact: addOverhead(*resource.NewQuantity(4*1024*1024*1024, resource.BinarySI), sizeOverheadPercent),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateWorkPVCSize(logr.Discard(), tt.files)
			if got.Cmp(tt.expectExact) != 0 {
				t.Errorf("size = %s, want exactly %s", got.String(), tt.expectExact.String())
			}
		})
	}
}

// TestCalculateOutputPVCSize covers the Block-mode restore's output PVC
// sizing: unlike calculateScratchPVCSize, it must drop the chain-file
// component entirely (the qcow2 chain lives on the separate work PVC instead)
// and size purely off the max known original-disk capacity.
func TestCalculateOutputPVCSize(t *testing.T) {
	vmIndex := uploader.VMIndex{
		VMName:    "vm-1",
		Namespace: "vm-ns",
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:       "cp-1",
				PVCs:     []string{"pvc-1"},
				PVCSizes: []resource.Quantity{resource.MustParse("10Gi")},
				Files:    []uploader.CheckpointFile{{DiskName: "disk-1"}},
			},
		},
	}

	t.Run("sized from max PVCSizes across chain, ignoring file sizes", func(t *testing.T) {
		got := calculateOutputPVCSize(logr.Discard(), vmIndex, []string{"cp-1"}, "disk-1", resource.Quantity{})
		want := addOverhead(resource.MustParse("10Gi"), sizeOverheadPercent)
		if got.Cmp(want) != 0 {
			t.Errorf("size = %s, want exactly %s", got.String(), want.String())
		}
	})

	t.Run("floored by target disk capacity when no chain match", func(t *testing.T) {
		got := calculateOutputPVCSize(logr.Discard(), vmIndex, []string{"cp-99"}, "disk-1", resource.MustParse("20Gi"))
		want := addOverhead(resource.MustParse("20Gi"), sizeOverheadPercent)
		if got.Cmp(want) != 0 {
			t.Errorf("size = %s, want exactly %s", got.String(), want.String())
		}
	})

	t.Run("no metadata at all falls back to default", func(t *testing.T) {
		got := calculateOutputPVCSize(logr.Discard(), vmIndex, []string{"cp-99"}, "disk-1", resource.Quantity{})
		want := resource.MustParse(DefaultScratchPVCSize)
		if got.Cmp(want) != 0 {
			t.Errorf("size = %s, want exactly %s", got.String(), want.String())
		}
	})
}

// TestResolveTargetDiskName_ChainTipWins covers a real fix: a VM's disk-to-PVC
// mapping is normally stable across a chain, but if it changed between the full
// backup and a later incremental (e.g. a differently-named DataVolume reattached
// to the same PVC slot), the newest (chain-tip) checkpoint's mapping must win,
// not whichever checkpoint happens to be found first.
func TestResolveTargetDiskName_ChainTipWins(t *testing.T) {
	vmIndex := uploader.VMIndex{
		VMName:    "vm-1",
		Namespace: "vm-ns",
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:   "cp-full",
				PVCs: []string{"restored-disk"},
				Files: []uploader.CheckpointFile{
					{DiskName: "old-disk-name"},
				},
			},
			{
				ID:   "cp-incremental",
				PVCs: []string{"restored-disk"},
				Files: []uploader.CheckpointFile{
					{DiskName: "new-disk-name"},
				},
			},
		},
	}

	// Chain is ordered full-first, incrementals-after (matching
	// pkg/downloader.types.go's documented CheckpointChain convention).
	got, err := resolveTargetDiskName(vmIndex, []string{"cp-full", "cp-incremental"}, "restored-disk")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "new-disk-name" {
		t.Errorf("resolveTargetDiskName() = %q, want %q (the chain-tip/newest checkpoint's mapping)", got, "new-disk-name")
	}
}

// TestResolveTargetDiskName_SkipsMalformedOlderEntry covers a checkpoint
// chain where an OLDER (non-authoritative) entry's mapping for the target
// PVC is malformed -- must not abort the scan there: a later, chain-tip
// entry with a valid mapping for the same PVC should still resolve, per
// this function's own "newest checkpoint is authoritative" design.
func TestResolveTargetDiskName_SkipsMalformedOlderEntry(t *testing.T) {
	vmIndex := uploader.VMIndex{
		VMName:    "vm-1",
		Namespace: "vm-ns",
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:   "cp-full",
				PVCs: []string{"restored-disk"},
				Files: []uploader.CheckpointFile{
					{DiskName: ""}, // malformed: empty disk name
				},
			},
			{
				ID:   "cp-incremental",
				PVCs: []string{"restored-disk"},
				Files: []uploader.CheckpointFile{
					{DiskName: "new-disk-name"},
				},
			},
		},
	}

	got, err := resolveTargetDiskName(vmIndex, []string{"cp-full", "cp-incremental"}, "restored-disk")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "new-disk-name" {
		t.Errorf("resolveTargetDiskName() = %q, want %q (later valid entry, despite the earlier malformed one)", got, "new-disk-name")
	}
}

// TestResolveTargetDiskName_NewestDefectSupersedesOlderValidEntry covers the
// opposite ordering from the malformed-older-entry case above: the
// chain-tip (newest, most authoritative) entry for the target PVC is itself
// malformed, after an earlier entry had a valid mapping. The chain-tip's
// defect must be surfaced, not silently shadowed by the earlier, now-stale
// valid value.
func TestResolveTargetDiskName_NewestDefectSupersedesOlderValidEntry(t *testing.T) {
	vmIndex := uploader.VMIndex{
		VMName:    "vm-1",
		Namespace: "vm-ns",
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:   "cp-full",
				PVCs: []string{"restored-disk"},
				Files: []uploader.CheckpointFile{
					{DiskName: "old-disk-name"},
				},
			},
			{
				ID:   "cp-incremental",
				PVCs: []string{"restored-disk"},
				Files: []uploader.CheckpointFile{
					{DiskName: ""}, // malformed: empty disk name
				},
			},
		},
	}

	_, err := resolveTargetDiskName(vmIndex, []string{"cp-full", "cp-incremental"}, "restored-disk")
	if err == nil {
		t.Fatal("expected the chain-tip's defect to be surfaced, not shadowed by the earlier valid entry")
	}
	if !strings.Contains(err.Error(), "cp-incremental") {
		t.Errorf("error = %q, want it to reference the chain-tip checkpoint %q", err, "cp-incremental")
	}
}

// TestResolveTargetDiskName_ValidDuplicateWinsWithinEntry covers a single
// checkpoint whose PVCs list names targetPVCName more than once (e.g. the
// same PVC attached as two VM volumes): a well-formed duplicate must resolve
// even when another duplicate in the very same entry is malformed, and this
// holds regardless of which duplicate comes first.
func TestResolveTargetDiskName_ValidDuplicateWinsWithinEntry(t *testing.T) {
	tests := []struct {
		name  string
		pvcs  []string
		files []uploader.CheckpointFile
	}{
		{
			name:  "malformed duplicate first",
			pvcs:  []string{"restored-disk", "restored-disk"},
			files: []uploader.CheckpointFile{{DiskName: ""}, {DiskName: "new-disk-name"}},
		},
		{
			name:  "malformed duplicate last",
			pvcs:  []string{"restored-disk", "restored-disk"},
			files: []uploader.CheckpointFile{{DiskName: "new-disk-name"}, {DiskName: ""}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vmIndex := uploader.VMIndex{
				VMName:    "vm-1",
				Namespace: "vm-ns",
				Checkpoints: []uploader.CheckpointEntry{
					{ID: "cp-full", PVCs: tt.pvcs, Files: tt.files},
				},
			}
			got, err := resolveTargetDiskName(vmIndex, []string{"cp-full"}, "restored-disk")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != "new-disk-name" {
				t.Errorf("resolveTargetDiskName() = %q, want %q (the well-formed duplicate)", got, "new-disk-name")
			}
		})
	}
}

func TestResolveTargetDiskName_Errors(t *testing.T) {
	tests := []struct {
		name          string
		vmIndex       uploader.VMIndex
		chain         []string
		targetPVCName string
		errorContains string
	}{
		{
			name: "PVC listed but no matching file entry",
			vmIndex: uploader.VMIndex{
				Checkpoints: []uploader.CheckpointEntry{
					{ID: "cp-full", PVCs: []string{"restored-disk"}, Files: []uploader.CheckpointFile{}},
				},
			},
			chain:         []string{"cp-full"},
			targetPVCName: "restored-disk",
			errorContains: "no matching file entry",
		},
		{
			name: "matching file has an empty disk name",
			vmIndex: uploader.VMIndex{
				Checkpoints: []uploader.CheckpointEntry{
					{ID: "cp-full", PVCs: []string{"restored-disk"}, Files: []uploader.CheckpointFile{{DiskName: ""}}},
				},
			},
			chain:         []string{"cp-full"},
			targetPVCName: "restored-disk",
			errorContains: "empty disk name",
		},
		{
			name: "target PVC not found in any checkpoint",
			vmIndex: uploader.VMIndex{
				Checkpoints: []uploader.CheckpointEntry{
					{ID: "cp-full", PVCs: []string{"some-other-pvc"}, Files: []uploader.CheckpointFile{{DiskName: "disk1"}}},
				},
			},
			chain:         []string{"cp-full"},
			targetPVCName: "restored-disk",
			errorContains: "not found in any checkpoint",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := resolveTargetDiskName(tt.vmIndex, tt.chain, tt.targetPVCName)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.errorContains) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tt.errorContains)
			}
		})
	}
}

// startFakeBinder mimics the real Kubernetes PV binder, which fake clients
// don't run: once handleInProgress's rebind sets the PV's claimRef to point at
// the target PVC (what the code under test actually writes), this completes
// the bind by setting the PVC's Spec.VolumeName and Status.Phase, polling
// rather than a fixed sleep that could race ahead of the patch. Call site must
// `defer` the returned function immediately, so the goroutine is waited on
// (bounded by timeout) and its status-update error is asserted before the
// test returns -- extracted out of the caller to keep its cyclomatic
// complexity down (gocyclo) as much as to avoid duplicating this boilerplate.
func startFakeBinder(t *testing.T, fakeClient client.Client, scratchPV *corev1.PersistentVolume, targetPVC *corev1.PersistentVolumeClaim, timeout time.Duration) func() {
	t.Helper()
	binderDone := make(chan struct{})
	var binderErr error
	bound := false
	go func() {
		defer close(binderDone)
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			pv := &corev1.PersistentVolume{}
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: scratchPV.Name}, pv); err != nil {
				binderErr = fmt.Errorf("get PV: %w", err)
				return
			}
			if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == targetPVC.Name && pv.Spec.ClaimRef.Namespace == targetPVC.Namespace {
				pvc := &corev1.PersistentVolumeClaim{}
				if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: targetPVC.Name, Namespace: targetPVC.Namespace}, pvc); err != nil {
					binderErr = fmt.Errorf("get target PVC: %w", err)
					return
				}
				pvc.Spec.VolumeName = scratchPV.Name
				if err := fakeClient.Update(context.Background(), pvc); err != nil {
					binderErr = fmt.Errorf("update target PVC: %w", err)
					return
				}
				pvc.Status.Phase = corev1.ClaimBound
				binderErr = fakeClient.Status().Update(context.Background(), pvc)
				bound = binderErr == nil
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	return func() {
		select {
		case <-binderDone:
			if binderErr != nil {
				t.Errorf("fake-binder goroutine failed: %v", binderErr)
			} else if !bound {
				t.Errorf("fake-binder never observed a claimRef naming target PVC %s/%s", targetPVC.Namespace, targetPVC.Name)
			}
		case <-time.After(timeout + time.Second):
			t.Errorf("timed out waiting for fake-binder goroutine to finish")
		}
	}
}

// TestEnsureScratchPVC covers ensureScratchPVC's reuse path, including the
// drift-validation branch added for Phase 4: an existing scratch PVC found for
// dd.UID must still match what would be requested now (StorageClassName,
// VolumeMode, AccessModes -- with the ReadWriteOnce default applied the same way
// ensureScratchPVC itself applies it), or the reconcile should fail clearly
// instead of silently reusing a wrong-shaped scratch volume.
func TestEnsureScratchPVC(t *testing.T) {
	scheme := ddScheme()

	newFixture := func() (*velerov2alpha1.DataDownload, *corev1.PersistentVolumeClaim) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp", UID: types.UID("dd-uid-1")},
		}
		targetPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "restored-disk-1", Namespace: "restore-ns"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: new("standard"),
				VolumeMode:       new(corev1.PersistentVolumeFilesystem),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				},
			},
		}
		return dd, targetPVC
	}

	existingScratchPVC := func(dd *velerov2alpha1.DataDownload, mutate func(*corev1.PersistentVolumeClaim)) *corev1.PersistentVolumeClaim {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scratch-pvc-existing", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(dd.UID)},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: new("standard"),
				VolumeMode:       new(corev1.PersistentVolumeFilesystem),
				// Matches the 15Gi requested size most subtests below pass to
				// ensureScratchPVC, so the capacity check doesn't reject reuse
				// unless a subtest explicitly mutates it to test that check.
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("15Gi")},
				},
			},
		}
		if mutate != nil {
			mutate(pvc)
		}
		return pvc
	}

	t.Run("creates scratch PVC when none exists", func(t *testing.T) {
		dd, targetPVC := newFixture()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		pvc, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pvc == nil {
			t.Fatal("expected scratch PVC to be created")
		}
	})

	t.Run("reuses existing scratch PVC with matching shape", func(t *testing.T) {
		dd, targetPVC := newFixture()
		existing := existingScratchPVC(dd, nil)
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, existing).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		pvc, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pvc.Name != existing.Name {
			t.Errorf("expected existing scratch PVC %q to be reused, got %q", existing.Name, pvc.Name)
		}
	})

	t.Run("errors when existing scratch PVC's storage class drifted from target PVC", func(t *testing.T) {
		dd, targetPVC := newFixture()
		existing := existingScratchPVC(dd, func(p *corev1.PersistentVolumeClaim) {
			p.Spec.StorageClassName = new("different-class")
		})
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, existing).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		_, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
		if err == nil {
			t.Fatal("expected error on storage class drift, got nil")
		}
		if !stderrors.Is(err, errScratchPVCShapeMismatch) {
			t.Errorf("expected errScratchPVCShapeMismatch (immutable-field drift, must fail fast rather than retry), got: %v", err)
		}
	})

	t.Run("errors when existing scratch PVC's volume mode drifted from target PVC", func(t *testing.T) {
		dd, targetPVC := newFixture()
		existing := existingScratchPVC(dd, func(p *corev1.PersistentVolumeClaim) {
			p.Spec.VolumeMode = new(corev1.PersistentVolumeBlock)
		})
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, existing).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		_, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
		if err == nil {
			t.Fatal("expected error on volume mode drift, got nil")
		}
		if !stderrors.Is(err, errScratchPVCShapeMismatch) {
			t.Errorf("expected errScratchPVCShapeMismatch (immutable-field drift, must fail fast rather than retry), got: %v", err)
		}
	})

	t.Run("errors when existing scratch PVC's access modes drifted from target PVC", func(t *testing.T) {
		dd, targetPVC := newFixture()
		existing := existingScratchPVC(dd, func(p *corev1.PersistentVolumeClaim) {
			p.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
		})
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, existing).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		_, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
		if err == nil {
			t.Fatal("expected error on access modes drift, got nil")
		}
		if !stderrors.Is(err, errScratchPVCShapeMismatch) {
			t.Errorf("expected errScratchPVCShapeMismatch (immutable-field drift, must fail fast rather than retry), got: %v", err)
		}
	})

	t.Run("defaults target PVC's empty access modes to ReadWriteOnce before comparing", func(t *testing.T) {
		dd, targetPVC := newFixture()
		targetPVC.Spec.AccessModes = nil // target PVC itself has no access modes set
		existing := existingScratchPVC(dd, nil)
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, existing).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi")); err != nil {
			t.Errorf("unexpected error comparing existing scratch PVC against the effective ReadWriteOnce default: %v", err)
		}
	})

	t.Run("treats nil storage class and volume mode on both sides as matching (default storage class case)", func(t *testing.T) {
		dd, targetPVC := newFixture()
		targetPVC.Spec.StorageClassName = nil // rely on the cluster's default storage class
		targetPVC.Spec.VolumeMode = nil
		existing := existingScratchPVC(dd, func(p *corev1.PersistentVolumeClaim) {
			p.Spec.StorageClassName = nil // scratch PVC was created from the same nil-valued target spec
			p.Spec.VolumeMode = nil
		})
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, existing).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		pvc, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
		if err != nil {
			t.Fatalf("nil==nil storage class/volume mode must not be reported as drift: %v", err)
		}
		if pvc.Name != existing.Name {
			t.Errorf("expected existing scratch PVC %q to be reused, got %q", existing.Name, pvc.Name)
		}
	})

	t.Run("errors when only one side's storage class is nil (nil vs explicit is drift, not a match)", func(t *testing.T) {
		dd, targetPVC := newFixture() // target keeps its explicit "standard" storage class
		existing := existingScratchPVC(dd, func(p *corev1.PersistentVolumeClaim) {
			p.Spec.StorageClassName = nil // scratch was created back when the target relied on the cluster default
		})
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, existing).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		_, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
		if err == nil {
			t.Fatal("expected error when existing scratch PVC has nil storage class but target PVC has an explicit one, got nil")
		}
		if !stderrors.Is(err, errScratchPVCShapeMismatch) {
			t.Errorf("expected errScratchPVCShapeMismatch (immutable-field drift, must fail fast rather than retry), got: %v", err)
		}
	})

	t.Run("treats access modes as a set: same modes in a different order still match", func(t *testing.T) {
		dd, targetPVC := newFixture()
		targetPVC.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce, corev1.ReadOnlyMany}
		existing := existingScratchPVC(dd, func(p *corev1.PersistentVolumeClaim) {
			p.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany, corev1.ReadWriteOnce}
		})
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, existing).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		pvc, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
		if err != nil {
			t.Fatalf("reordered-but-identical access modes must not be reported as drift: %v", err)
		}
		if pvc.Name != existing.Name {
			t.Errorf("expected existing scratch PVC %q to be reused, got %q", existing.Name, pvc.Name)
		}
	})

	t.Run("errors when existing scratch PVC's storage is smaller than required", func(t *testing.T) {
		dd, targetPVC := newFixture()
		existing := existingScratchPVC(dd, func(p *corev1.PersistentVolumeClaim) {
			p.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}
		})
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, existing).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		_, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
		if err == nil {
			t.Fatal("expected error when existing scratch PVC (10Gi) is smaller than the newly required size (15Gi), got nil")
		}
		if stderrors.Is(err, errScratchPVCShapeMismatch) {
			t.Error("size-too-small must stay a plain retryable error, not errScratchPVCShapeMismatch -- a later reconcile's recalculated size could genuinely differ and resolve on its own")
		}
	})

	t.Run("errors when existing scratch PVC has no storage request at all", func(t *testing.T) {
		dd, targetPVC := newFixture()
		existing := existingScratchPVC(dd, func(p *corev1.PersistentVolumeClaim) {
			// No storage request recorded at all -- validateScratchPVCShape must
			// treat this the same as too-small (reject), not silently reuse a PVC
			// whose capacity it can't verify.
			p.Spec.Resources.Requests = nil
		})
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, existing).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		_, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
		if err == nil {
			t.Fatal("expected error when existing scratch PVC has no storage request set, got nil")
		}
		if stderrors.Is(err, errScratchPVCShapeMismatch) {
			t.Error("size-too-small must stay a plain retryable error, not errScratchPVCShapeMismatch")
		}
	})

	t.Run("reuses existing scratch PVC whose size exactly matches what's required", func(t *testing.T) {
		dd, targetPVC := newFixture()
		existing := existingScratchPVC(dd, func(p *corev1.PersistentVolumeClaim) {
			p.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("15Gi")}
		})
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, existing).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		pvc, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
		if err != nil {
			t.Fatalf("unexpected error reusing an exactly-sized scratch PVC: %v", err)
		}
		if pvc.Name != existing.Name {
			t.Errorf("expected existing scratch PVC %q to be reused, got %q", existing.Name, pvc.Name)
		}
	})

	t.Run("reuses existing scratch PVC whose size is larger than what's required", func(t *testing.T) {
		dd, targetPVC := newFixture()
		existing := existingScratchPVC(dd, func(p *corev1.PersistentVolumeClaim) {
			p.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("50Gi")}
		})
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, existing).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		pvc, err := r.ensureScratchPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
		if err != nil {
			t.Fatalf("unexpected error reusing an oversized scratch PVC: %v", err)
		}
		if pvc.Name != existing.Name {
			t.Errorf("expected existing scratch PVC %q to be reused (never shrunk), got %q", existing.Name, pvc.Name)
		}
	})
}

// TestEnsureWorkPVC pins the fix for #197: ensureWorkPVC must always request
// ReadWriteOnce for the Filesystem-mode work PVC, regardless of the restore
// target's own AccessModes. Ceph RBD's CSI driver rejects ReadWriteMany on a
// Filesystem-mode volume outright ("multi node access modes are only
// supported on rbd block type volumes"), and the work PVC -- mounted by
// exactly one pod, on one node -- never needed RWX in the first place.
func TestEnsureWorkPVC(t *testing.T) {
	scheme := ddScheme()

	dd := &velerov2alpha1.DataDownload{
		ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp", UID: types.UID("dd-uid-1")},
	}
	targetPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-disk-1", Namespace: "restore-ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			// A typical KubeVirt VM disk PVC's AccessModes -- the shape that
			// caused #197 when copied verbatim onto the Filesystem-mode work PVC.
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			StorageClassName: new("standard"),
			VolumeMode:       new(corev1.PersistentVolumeBlock),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	pvc, err := r.ensureWorkPVC(context.Background(), logr.Discard(), dd, targetPVC, resource.MustParse("15Gi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pvc == nil {
		t.Fatal("expected work PVC to be created")
	}
	if want := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}; !slices.Equal(pvc.Spec.AccessModes, want) {
		t.Errorf("work PVC AccessModes = %v, want %v (must never inherit the target's RWX)", pvc.Spec.AccessModes, want)
	}
	if pvc.Spec.VolumeMode == nil || *pvc.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		t.Errorf("work PVC VolumeMode = %v, want %q", pvc.Spec.VolumeMode, corev1.PersistentVolumeFilesystem)
	}
}

// ddTestFixture bundles the objects needed to exercise handleAccepted/handlePrepared/
// handleInProgress against a mock object store seeded with a manifest+index.
type ddTestFixture struct {
	dd        *velerov2alpha1.DataDownload
	bsl       *velerov1.BackupStorageLocation
	credSec   *corev1.Secret
	targetPVC *corev1.PersistentVolumeClaim
	mockStore *uploader.MockObjectStore
}

func newDDTestFixture(t *testing.T) *ddTestFixture {
	t.Helper()

	const (
		vmName      = "test-vm"
		vmNamespace = "vm-ns"
		oadpNS      = "openshift-adp"
		backupName  = "backup-001"
		targetPVC   = "restored-disk-1"
		diskName    = "disk1" // KubeVirt volume name -- deliberately different from targetPVC
		restoreNS   = "restore-ns"
	)

	dd := &velerov2alpha1.DataDownload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dd",
			Namespace: oadpNS,
			UID:       types.UID("dd-uid-123"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
			Labels: map[string]string{
				common.LabelVeleroBackupName: backupName,
			},
		},
		Spec: velerov2alpha1.DataDownloadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
			TargetVolume: velerov2alpha1.TargetVolumeSpec{
				PVC:       targetPVC,
				Namespace: restoreNS,
			},
		},
		Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseAccepted},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: oadpNS},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{Bucket: "test-bucket", Prefix: "velero"},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
	}

	credSec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-creds", Namespace: oadpNS},
		Data:       map[string][]byte{"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n")},
	}

	targetPVCObj := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: targetPVC, Namespace: restoreNS},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: new("standard"),
			VolumeMode:       new(corev1.PersistentVolumeFilesystem),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}

	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	checkpointID := "cp-001"
	vmIndex := uploader.VMIndex{
		VMName:    vmName,
		Namespace: vmNamespace,
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:       checkpointID,
				Type:     "full",
				PVCs:     []string{targetPVC},
				PVCSizes: []resource.Quantity{resource.MustParse("10Gi")},
				Files: []uploader.CheckpointFile{
					{
						Filename:   "vmb-" + checkpointID + "-" + diskName + ".qcow2",
						DiskName:   diskName,
						Size:       1024 * 1024 * 1024,
						ObjectPath: "checkpoints/" + vmNamespace + "/" + vmName + "/" + checkpointID + "/vmb-" + checkpointID + "-" + diskName + ".qcow2",
					},
				},
			},
		},
	}
	if err := uploader.PutVMIndex(mockStore, vmNamespace, vmName, "test-bucket", vmIndex); err != nil {
		t.Fatalf("failed to seed VM index: %v", err)
	}

	manifest := uploader.VMBackupManifest{
		Namespace:       vmNamespace,
		Name:            vmName,
		CheckpointChain: []string{checkpointID},
		BackupName:      backupName,
	}
	if err := uploader.PutVMBackupManifest(mockStore, vmNamespace, vmName, backupName, "test-bucket", manifest); err != nil {
		t.Fatalf("failed to seed VM backup manifest: %v", err)
	}

	return &ddTestFixture{dd: dd, bsl: bsl, credSec: credSec, targetPVC: targetPVCObj, mockStore: mockStore}
}

// TestFindScratchPVC_APIReaderFallback covers the informer-cache-lag case: a
// scratch PVC that genuinely exists but hasn't yet propagated to the cached
// client's informer (e.g. created by this same controller moments ago in an
// earlier reconcile) must still be found via the uncached APIReader fallback,
// not misreported as absent -- which would otherwise create a duplicate
// scratch PVC via ensureScratchPVC's GenerateName create.
func TestFindScratchPVC_APIReaderFallback(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()

	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}

	t.Run("cached client is empty (cache lag), APIReader fallback finds it", func(t *testing.T) {
		cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd).Build()
		apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, scratchPVC.DeepCopy()).Build()
		r := &KubeVirtDataDownloadReconciler{Client: cached, APIReader: apiReader, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		got, err := r.findScratchPVC(context.Background(), f.dd, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil || got.Name != scratchPVC.Name {
			t.Errorf("expected APIReader fallback to find scratch PVC, got %+v", got)
		}
	})

	t.Run("nil APIReader falls back to cached-only behavior", func(t *testing.T) {
		cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: cached, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		got, err := r.findScratchPVC(context.Background(), f.dd, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})
}

// TestListScratchPVC_EmptyRoleExcludesRoleLabeled covers a Block-mode restore
// where the unlabeled Filesystem-style lookup (role == "") must not
// accidentally match a role-labeled "work"/"output" PVC -- e.g. if its
// sibling scratch PVC has already been cleaned up, leaving just the one
// role-labeled PVC to satisfy an unconstrained UID-only match.
func TestListScratchPVC_EmptyRoleExcludesRoleLabeled(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()

	roleLabeledPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-work", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleWork,
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, roleLabeledPVC).Build()

	got, err := listScratchPVC(context.Background(), fakeClient, "openshift-adp", f.dd, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected role=\"\" lookup to find nothing when only a role-labeled PVC exists, got %+v", got)
	}

	gotRoleLabeled, err := listScratchPVC(context.Background(), fakeClient, "openshift-adp", f.dd, common.ScratchVolumeRoleWork)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotRoleLabeled == nil || gotRoleLabeled.Name != roleLabeledPVC.Name {
		t.Errorf("expected role=%q lookup to find the role-labeled PVC, got %+v", common.ScratchVolumeRoleWork, gotRoleLabeled)
	}
}

// TestListAllScratchPVCs_APIReaderFallback covers the same informer-cache-lag
// case as TestFindScratchPVC_APIReaderFallback, but for listAllScratchPVCs --
// used by handleCanceling's deleteAllScratchPVCs, a terminal path where a
// scratch PVC missed here (cached client hasn't caught up yet) would never
// get cleaned up at all, since Canceled never reconciles again.
func TestListAllScratchPVCs_APIReaderFallback(t *testing.T) {
	scheme := ddScheme()

	newScratchPVC := func(f *ddTestFixture) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scratch-pvc-1", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
		}
	}

	t.Run("cached client is empty (cache lag), APIReader fallback finds it", func(t *testing.T) {
		f := newDDTestFixture(t)
		scratchPVC := newScratchPVC(f)
		cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd).Build()
		apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, scratchPVC.DeepCopy()).Build()
		r := &KubeVirtDataDownloadReconciler{Client: cached, APIReader: apiReader, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		got, err := r.listAllScratchPVCs(context.Background(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].Name != scratchPVC.Name {
			t.Errorf("expected APIReader fallback to find scratch PVC, got %+v", got)
		}
	})

	t.Run("nil APIReader falls back to cached-only behavior", func(t *testing.T) {
		f := newDDTestFixture(t)
		cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: cached, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		got, err := r.listAllScratchPVCs(context.Background(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("expected empty, got %+v", got)
		}
	})

	t.Run("Block-mode target with only one of two PVCs cached retries via APIReader", func(t *testing.T) {
		// A Block-mode restore provisions two scratch PVCs (work + output),
		// created moments apart -- the cached client can have one visible and
		// not the other. "Found one" must not be mistaken for "list complete"
		// the way it correctly is for a Filesystem-mode target's single PVC.
		f := newDDTestFixture(t)
		f.dd.Annotations[AnnotationRestoreBlockMode] = "true"
		workPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "work-pvc-1", Namespace: "openshift-adp",
				Labels: map[string]string{
					common.LabelDataDownloadUID:   string(f.dd.UID),
					common.LabelScratchVolumeRole: common.ScratchVolumeRoleWork,
				},
			},
		}
		outputPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "output-pvc-1", Namespace: "openshift-adp",
				Labels: map[string]string{
					common.LabelDataDownloadUID:   string(f.dd.UID),
					common.LabelScratchVolumeRole: common.ScratchVolumeRoleOutput,
				},
			},
		}
		cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, workPVC.DeepCopy()).Build()
		apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, workPVC.DeepCopy(), outputPVC.DeepCopy()).Build()
		r := &KubeVirtDataDownloadReconciler{Client: cached, APIReader: apiReader, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		got, err := r.listAllScratchPVCs(context.Background(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 2 {
			t.Errorf("expected both work and output PVCs via APIReader retry, got %d: %+v", len(got), got)
		}
	})

	t.Run("Block-mode target with only one of two PVCs cached retries via APIReader even without AnnotationRestoreBlockMode yet", func(t *testing.T) {
		// AnnotationRestoreBlockMode deliberately left unset here: a Cancel can
		// race in after handleAccepted created the first scratch PVC but before
		// it persisted the annotation. The cached, role-labeled work PVC alone
		// must still be enough to know a second (output) PVC might exist and is
		// worth an APIReader retry for -- not just the annotation.
		f := newDDTestFixture(t)
		workPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "work-pvc-2", Namespace: "openshift-adp",
				Labels: map[string]string{
					common.LabelDataDownloadUID:   string(f.dd.UID),
					common.LabelScratchVolumeRole: common.ScratchVolumeRoleWork,
				},
			},
		}
		outputPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "output-pvc-2", Namespace: "openshift-adp",
				Labels: map[string]string{
					common.LabelDataDownloadUID:   string(f.dd.UID),
					common.LabelScratchVolumeRole: common.ScratchVolumeRoleOutput,
				},
			},
		}
		cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, workPVC.DeepCopy()).Build()
		apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, workPVC.DeepCopy(), outputPVC.DeepCopy()).Build()
		r := &KubeVirtDataDownloadReconciler{Client: cached, APIReader: apiReader, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		got, err := r.listAllScratchPVCs(context.Background(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 2 {
			t.Errorf("expected both work and output PVCs via APIReader retry, got %d: %+v", len(got), got)
		}
	})
}

func TestHandleAcceptedDataDownload(t *testing.T) {
	t.Run("target PVC not found requeues without failing", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec).Build() // no targetPVC
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
		}

		result, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter == 0 {
			t.Error("expected requeue when target PVC is missing")
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseFailed {
			t.Error("missing target PVC should requeue, not fail -- Velero may not have created it yet")
		}
	})

	t.Run("missing manifest fails", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC).Build()
		emptyStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return emptyStore, nil },
		}

		if _, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
	})

	t.Run("happy path resolves chain, creates scratch PVC, transitions to Prepared", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
		}

		result, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter == 0 {
			t.Error("expected requeue after transitioning to Prepared")
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhasePrepared {
			t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhasePrepared, updated.Status.Message)
		}

		scratchPVC, err := r.findScratchPVC(context.Background(), f.dd, "")
		if err != nil {
			t.Fatalf("failed to find scratch PVC: %v", err)
		}
		if scratchPVC == nil {
			t.Fatal("expected scratch PVC to be created")
		}
		if scratchPVC.Namespace != "openshift-adp" {
			t.Errorf("scratch PVC namespace = %q, want %q", scratchPVC.Namespace, "openshift-adp")
		}
		if scratchPVC.Spec.StorageClassName == nil || *scratchPVC.Spec.StorageClassName != "standard" {
			t.Errorf("scratch PVC storageClassName = %v, want %q", scratchPVC.Spec.StorageClassName, "standard")
		}
		if scratchPVC.Spec.VolumeMode == nil || *scratchPVC.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
			t.Errorf("scratch PVC volumeMode = %v, want %q", scratchPVC.Spec.VolumeMode, corev1.PersistentVolumeFilesystem)
		}
		if got := updated.Annotations[AnnotationTargetDiskName]; got != "disk1" {
			t.Errorf("annotation %s = %q, want %q", AnnotationTargetDiskName, got, "disk1")
		}
	})

	fastFailCases := []struct {
		name         string
		mutate       func(pvc *corev1.PersistentVolumeClaim)
		noScratchMsg string
	}{
		{
			name: "target PVC with spec.selector.matchExpressions fails without creating a scratch PVC",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.Selector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "some-label", Operator: metav1.LabelSelectorOpExists},
				}}
			},
			noScratchMsg: "expected no scratch PVC to be created for a target PVC with spec.selector.matchExpressions set",
		},
		{
			name: "target PVC already bound to a foreign PV fails without creating a scratch PVC",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.VolumeName = "some-other-pv"
				pvc.Status.Phase = corev1.ClaimBound
			},
			noScratchMsg: "expected no scratch PVC to be created for an already-bound target PVC",
		},
		{
			name:         "target PVC with pending Spec.VolumeName fails without creating a scratch PVC",
			mutate:       func(pvc *corev1.PersistentVolumeClaim) { pvc.Spec.VolumeName = "some-other-pv" },
			noScratchMsg: "expected no scratch PVC to be created for a target PVC with a pending Spec.VolumeName",
		},
	}

	for _, tc := range fastFailCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newDDTestFixture(t)
			scheme := ddScheme()
			tc.mutate(f.targetPVC)
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC).Build()
			r := &KubeVirtDataDownloadReconciler{
				Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
				ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
			}

			if _, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var updated velerov2alpha1.DataDownload
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
				t.Fatalf("failed to get DataDownload: %v", err)
			}
			if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
				t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed, updated.Status.Message)
			}

			scratchPVC, err := r.findScratchPVC(context.Background(), f.dd, "")
			if err != nil {
				t.Fatalf("failed to check for scratch PVC: %v", err)
			}
			if scratchPVC != nil {
				t.Error(tc.noScratchMsg)
			}
		})
	}
}

// TestHandleAcceptedDataDownload_MatchLabelsSelectorSucceeds pins that a
// matchLabels-only Spec.Selector on the target PVC (the shape Velero's own
// built-in CSI DataDownload restore path sets via velero.io/dynamic-pv-restore
// to keep the dynamic provisioner from racing the rebind) is accepted, not
// treated as a conflict: validateExistingPVCForBind reconciles the rebound
// PV's labels to satisfy it later, at completeSuccessfulDownload, rather than
// requiring the caller to pre-arrange a matching PV label.
func TestHandleAcceptedDataDownload_MatchLabelsSelectorSucceeds(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	f.targetPVC.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"velero.io/dynamic-pv-restore": "some-unique-value"}}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC).Build()
	r := &KubeVirtDataDownloadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
	}

	if _, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhasePrepared {
		t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhasePrepared, updated.Status.Message)
	}

	scratchPVC, err := r.findScratchPVC(context.Background(), f.dd, "")
	if err != nil {
		t.Fatalf("failed to check for scratch PVC: %v", err)
	}
	if scratchPVC == nil {
		t.Error("expected a scratch PVC to be created for a target PVC with a matchLabels-only selector")
	}
}

// TestHandleAcceptedDataDownload_ScratchPVCShapeMismatch pins the fail-fast
// behavior introduced for CodeRabbit's finding on validateScratchPVCShape's
// immutable-field mismatches: an existing scratch PVC whose StorageClassName/
// VolumeMode/AccessModes no longer matches what would be requested must fail
// the DataDownload immediately (errScratchPVCShapeMismatch, via
// failIfImmutableScratchPVCMismatch), not leave it retrying every reconcile
// via a plain error -- retrying can never resolve an immutable-field mismatch,
// so the prior behavior only surfaced it hours later, once
// Spec.OperationTimeout finally caught it.
func TestHandleAcceptedDataDownload_ScratchPVCShapeMismatch(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	// Pre-seed a scratch PVC (role "", the Filesystem-mode single-PVC path
	// f.targetPVC's VolumeMode selects) whose StorageClassName ("different")
	// no longer matches the target PVC's ("standard") -- immutable on a live
	// PVC, so this can only have happened via delete+recreate of the target,
	// not a normal in-place drift.
	existingScratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-preexisting", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: new("different"),
			VolumeMode:       new(corev1.PersistentVolumeFilesystem),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("50Gi")},
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC, existingScratchPVC).Build()
	r := &KubeVirtDataDownloadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd)
	if err != nil {
		t.Fatalf("unexpected error: %v (immutable shape mismatch must fail the DataDownload, not return a retryable reconcile error)", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0 (a Failed DataDownload should not be requeued)", result.RequeueAfter)
	}

	var updated velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
		t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed, updated.Status.Message)
	}
	if !strings.Contains(updated.Status.Message, "storageClassName") {
		t.Errorf("Status.Message = %q, want it to mention the storageClassName mismatch", updated.Status.Message)
	}
}

// TestHandleAcceptedDataDownload_BlockMode covers a Block-mode restore target:
// handleAccepted must provision both a Filesystem-mode work PVC (staging the
// qcow2 chain) and a Block-mode output PVC (sized to target capacity, holding
// the final flattened image) instead of the single Filesystem-mode scratch
// PVC a Filesystem-mode target uses. Split out from TestHandleAcceptedDataDownload
// to keep that function's cyclomatic complexity down (gocyclo).
func TestHandleAcceptedDataDownload_BlockMode(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	f.targetPVC.Spec.VolumeMode = new(corev1.PersistentVolumeBlock)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC).Build()
	r := &KubeVirtDataDownloadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected requeue after transitioning to Prepared")
	}

	var updated velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhasePrepared {
		t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhasePrepared, updated.Status.Message)
	}
	if got := updated.Annotations[AnnotationRestoreBlockMode]; got != "true" {
		t.Errorf("annotation %s = %q, want %q", AnnotationRestoreBlockMode, got, "true")
	}

	workPVC, err := r.findScratchPVC(context.Background(), f.dd, common.ScratchVolumeRoleWork)
	if err != nil {
		t.Fatalf("failed to find work PVC: %v", err)
	}
	if workPVC == nil {
		t.Fatal("expected work PVC to be created")
	}
	if workPVC.Spec.VolumeMode == nil || *workPVC.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		t.Errorf("work PVC volumeMode = %v, want %q", workPVC.Spec.VolumeMode, corev1.PersistentVolumeFilesystem)
	}

	outputPVC, err := r.findScratchPVC(context.Background(), f.dd, common.ScratchVolumeRoleOutput)
	if err != nil {
		t.Fatalf("failed to find output PVC: %v", err)
	}
	if outputPVC == nil {
		t.Fatal("expected output PVC to be created")
	}
	if outputPVC.Spec.VolumeMode == nil || *outputPVC.Spec.VolumeMode != corev1.PersistentVolumeBlock {
		t.Errorf("output PVC volumeMode = %v, want %q", outputPVC.Spec.VolumeMode, corev1.PersistentVolumeBlock)
	}
	if outputPVC.Spec.StorageClassName == nil || *outputPVC.Spec.StorageClassName != "standard" {
		t.Errorf("output PVC storageClassName = %v, want %q", outputPVC.Spec.StorageClassName, "standard")
	}
}

func TestHandlePreparedDataDownload(t *testing.T) {
	t.Run("existing pod transitions to InProgress without creating a new one", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		existingPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "existing-downloader-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, existingPod).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseInProgress {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseInProgress)
		}

		var pods corev1.PodList
		_ = fakeClient.List(context.Background(), &pods)
		if len(pods.Items) != 1 {
			t.Errorf("expected exactly 1 pod (no new one created), got %d", len(pods.Items))
		}
	})

	t.Run("happy path creates downloader pod and transitions to InProgress", func(t *testing.T) {
		f := newDDTestFixture(t)
		// handlePrepared reads the disk name resolved by handleAccepted in an
		// earlier reconcile; set it explicitly since this test calls handlePrepared
		// directly, bypassing Accepted.
		f.dd.Annotations[AnnotationTargetDiskName] = "disk1"
		scheme := ddScheme()
		scratchPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scratch-pvc-1", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, scratchPVC).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest",
		}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseInProgress {
			t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseInProgress, updated.Status.Message)
		}

		pod, err := r.findPodForDataDownload(context.Background(), f.dd, "openshift-adp")
		if err != nil {
			t.Fatalf("failed to find pod: %v", err)
		}
		if pod == nil {
			t.Fatal("expected downloader pod to be created")
		}
		if pod.Labels[common.LabelDatamoverPod] != "download" {
			t.Errorf("pod label %s = %q, want %q", common.LabelDatamoverPod, pod.Labels[common.LabelDatamoverPod], "download")
		}
		if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Name != f.dd.Name {
			t.Errorf("expected owner reference to DataDownload %q, got %+v", f.dd.Name, pod.OwnerReferences)
		}

		// The pod's target-volume env var must carry the resolved KubeVirt disk
		// name ("disk1"), not the target PVC name ("restored-disk-1") -- these
		// deliberately differ in this fixture to prove resolveTargetDiskName's
		// translation is what actually reaches the pod, not the raw PVC name.
		var targetVolumeEnv string
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == downloader.EnvTargetVolume {
				targetVolumeEnv = env.Value
				break
			}
		}
		if targetVolumeEnv != "disk1" {
			t.Errorf("env %s = %q, want %q", downloader.EnvTargetVolume, targetVolumeEnv, "disk1")
		}
		if targetVolumeEnv == "restored-disk-1" {
			t.Error("env carries the target PVC name instead of the resolved disk name")
		}
	})

	t.Run("missing target disk name annotation fails without creating a pod", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		scratchPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scratch-pvc-1", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, scratchPVC).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest",
		}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed, updated.Status.Message)
		}

		var pods corev1.PodList
		_ = fakeClient.List(context.Background(), &pods)
		if len(pods.Items) != 0 {
			t.Errorf("expected no pod to be created, got %d", len(pods.Items))
		}
	})

	t.Run("missing scratch PVC fails without creating a pod", func(t *testing.T) {
		f := newDDTestFixture(t)
		f.dd.Annotations[AnnotationTargetDiskName] = "disk1"
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest",
		}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed, updated.Status.Message)
		}

		var pods corev1.PodList
		_ = fakeClient.List(context.Background(), &pods)
		if len(pods.Items) != 0 {
			t.Errorf("expected no pod to be created, got %d", len(pods.Items))
		}
	})
}

// TestCountHigherPriorityActiveDataDownloads pins the counting semantics
// countHigherPriorityActiveDataDownloads relies on for the concurrency gate:
// only Accepted/Prepared/InProgress peers that outrank dd (per
// outranksDataDownload) count -- New is pre-provisioning and always
// excluded, the DD being reconciled itself is never counted, non-kubevirt
// DataMover CRs are ignored, and a peer that's active but ranks *behind* dd
// (later timestamp, or a tiebreak-losing UID at an equal timestamp) does not
// count against dd. This ranking is what lets the gate avoid deadlocking
// when N siblings all reach Prepared together (see the placement comment on
// countHigherPriorityActiveDataDownloads).
func TestCountHigherPriorityActiveDataDownloads(t *testing.T) {
	scheme := ddScheme()
	oadpNS := "openshift-adp"
	baseTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	olderTime := metav1.NewTime(time.Date(2026, 1, 1, 11, 59, 0, 0, time.UTC))
	newerTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 1, 0, 0, time.UTC))

	makeDD := func(name string, uid types.UID, phase velerov2alpha1.DataDownloadPhase, dataMover string, created *metav1.Time) *velerov2alpha1.DataDownload {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: oadpNS, UID: uid},
			Spec:       velerov2alpha1.DataDownloadSpec{DataMover: dataMover},
			Status:     velerov2alpha1.DataDownloadStatus{Phase: phase},
		}
		if created != nil {
			dd.CreationTimestamp = *created
		}
		return dd
	}

	self := makeDD("dd-self", "self-uid", velerov2alpha1.DataDownloadPhasePrepared, common.DataMoverKubeVirt, &baseTime)

	tests := []struct {
		name      string
		otherDDs  []client.Object
		wantCount int
	}{
		{name: "no other DDs", otherDDs: nil, wantCount: 0},
		{
			name: "counts higher-priority (older) Accepted/Prepared/InProgress peers",
			otherDDs: []client.Object{
				makeDD("dd-accepted", "uid-1", velerov2alpha1.DataDownloadPhaseAccepted, common.DataMoverKubeVirt, &olderTime),
				makeDD("dd-prepared", "uid-2", velerov2alpha1.DataDownloadPhasePrepared, common.DataMoverKubeVirt, &olderTime),
				makeDD("dd-inprogress", "uid-3", velerov2alpha1.DataDownloadPhaseInProgress, common.DataMoverKubeVirt, &olderTime),
			},
			wantCount: 3,
		},
		{
			name: "excludes terminal phases even if older",
			otherDDs: []client.Object{
				makeDD("dd-completed", "uid-1", velerov2alpha1.DataDownloadPhaseCompleted, common.DataMoverKubeVirt, &olderTime),
				makeDD("dd-failed", "uid-2", velerov2alpha1.DataDownloadPhaseFailed, common.DataMoverKubeVirt, &olderTime),
				makeDD("dd-canceled", "uid-3", velerov2alpha1.DataDownloadPhaseCanceled, common.DataMoverKubeVirt, &olderTime),
			},
			wantCount: 0,
		},
		{
			name: "excludes New (pre-provisioning) even if older",
			otherDDs: []client.Object{
				makeDD("dd-new", "uid-1", velerov2alpha1.DataDownloadPhaseNew, common.DataMoverKubeVirt, &olderTime),
			},
			wantCount: 0,
		},
		{
			name: "excludes non-kubevirt DataMover CRs even if older and active",
			otherDDs: []client.Object{
				makeDD("dd-other-mover", "uid-1", velerov2alpha1.DataDownloadPhaseInProgress, "csi", &olderTime),
			},
			wantCount: 0,
		},
		{
			name: "excludes an active peer with a later timestamp (lower priority)",
			otherDDs: []client.Object{
				makeDD("dd-newer", "uid-1", velerov2alpha1.DataDownloadPhaseInProgress, common.DataMoverKubeVirt, &newerTime),
			},
			wantCount: 0,
		},
		{
			name: "counts an equal-timestamp peer whose UID tiebreaks ahead",
			otherDDs: []client.Object{
				makeDD("aaa-uid-wins-tiebreak", "aaa-uid", velerov2alpha1.DataDownloadPhaseInProgress, common.DataMoverKubeVirt, &baseTime),
			},
			wantCount: 1,
		},
		{
			name: "excludes an equal-timestamp peer whose UID tiebreaks behind",
			otherDDs: []client.Object{
				makeDD("zzz-uid-loses-tiebreak", "zzz-uid", velerov2alpha1.DataDownloadPhaseInProgress, common.DataMoverKubeVirt, &baseTime),
			},
			wantCount: 0,
		},
		{
			name: "ranking is unaffected by Status.AcceptedTimestamp (only CreationTimestamp matters)",
			otherDDs: []client.Object{
				func() *velerov2alpha1.DataDownload {
					// A newer CreationTimestamp but an older AcceptedTimestamp --
					// if ranking still consulted AcceptedTimestamp (as it did
					// before handlePrepared started advancing that field to
					// exempt gate-wait time from Spec.OperationTimeout) this peer
					// would wrongly outrank self. It must not: CreationTimestamp
					// alone decides rank now.
					dd := makeDD("dd-newer-creation-older-accepted", "uid-1", velerov2alpha1.DataDownloadPhaseInProgress, common.DataMoverKubeVirt, &newerTime)
					dd.Status.AcceptedTimestamp = &olderTime
					return dd
				}(),
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := append([]client.Object{self}, tt.otherDDs...)
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
			r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

			count, err := r.countHigherPriorityActiveDataDownloads(context.Background(), self)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if count != tt.wantCount {
				t.Errorf("count = %d, want %d", count, tt.wantCount)
			}
		})
	}
}

// TestHandlePreparedDataDownload_ConcurrencyLimit covers issue #175: gating
// downloader pod creation in handlePrepared against MaxConcurrentDataMovers.
func TestHandlePreparedDataDownload_ConcurrencyLimit(t *testing.T) {
	baseTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	olderTime := metav1.NewTime(time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC))

	// newFixtureWithOthers seeds otherActiveCount peers strictly OLDER than
	// f.dd (by CreationTimestamp) so they always outrank f.dd regardless of
	// UID -- these tests are exercising the count/limit comparison itself,
	// not ranking direction (that's TestCountHigherPriorityActiveDataDownloads's
	// job). f.dd's own AcceptedTimestamp is separately seeded to exercise the
	// gate's advance-on-defer behavior (see "requeues ... advances
	// AcceptedTimestamp" below) -- ranking never reads it.
	newFixtureWithOthers := func(t *testing.T, otherActiveCount int) (*ddTestFixture, []client.Object) {
		t.Helper()
		f := newDDTestFixture(t)
		f.dd.Annotations[AnnotationTargetDiskName] = "disk1"
		f.dd.Status.Phase = velerov2alpha1.DataDownloadPhasePrepared
		f.dd.CreationTimestamp = baseTime
		f.dd.Status.AcceptedTimestamp = &baseTime
		scratchPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scratch-pvc-1", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
		}
		objs := make([]client.Object, 0, 4+otherActiveCount)
		objs = append(objs, f.dd, f.bsl, f.credSec, scratchPVC)
		for i := range otherActiveCount {
			other := &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("other-dd-%d", i), Namespace: "openshift-adp", UID: types.UID(fmt.Sprintf("other-uid-%d", i)),
					CreationTimestamp: olderTime,
				},
				Spec:   velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
				Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseInProgress, AcceptedTimestamp: &olderTime},
			}
			objs = append(objs, other)
		}
		return f, objs
	}

	t.Run("proceeds when under the limit", func(t *testing.T) {
		f, objs := newFixtureWithOthers(t, 2)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest", MaxConcurrentDataMovers: 3,
		}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseInProgress {
			t.Errorf("phase = %q, want %q (should proceed: 2 others < limit 3)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseInProgress)
		}
	})

	t.Run("requeues without creating a pod when at the limit", func(t *testing.T) {
		f, objs := newFixtureWithOthers(t, 3)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest", MaxConcurrentDataMovers: 3,
		}

		result, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != RequeueAfterLong {
			t.Errorf("RequeueAfter = %v, want %v", result.RequeueAfter, RequeueAfterLong)
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhasePrepared {
			t.Errorf("phase = %q, want still %q (at limit 3, must not proceed)", updated.Status.Phase, velerov2alpha1.DataDownloadPhasePrepared)
		}

		pod, err := r.findPodForDataDownload(context.Background(), f.dd, "openshift-adp")
		if err != nil {
			t.Fatalf("failed to find pod: %v", err)
		}
		if pod != nil {
			t.Error("expected no downloader pod to be created while gated")
		}

		// Deferring due to the concurrency limit is intentional throttling, not a
		// stalled operation -- AcceptedTimestamp must advance by exactly the
		// requeue interval so this wait doesn't consume Spec.OperationTimeout's
		// budget (see the gate's defer branch in handlePrepared).
		wantAdvanced := baseTime.Add(RequeueAfterLong)
		if updated.Status.AcceptedTimestamp == nil || !updated.Status.AcceptedTimestamp.Time.Equal(wantAdvanced) {
			t.Errorf("AcceptedTimestamp = %v, want %v (baseTime + RequeueAfterLong)", updated.Status.AcceptedTimestamp, wantAdvanced)
		}
	})

	t.Run("unlimited (MaxConcurrentDataMovers=0) always proceeds", func(t *testing.T) {
		f, objs := newFixtureWithOthers(t, 50)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest", MaxConcurrentDataMovers: 0,
		}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseInProgress {
			t.Errorf("phase = %q, want %q (limit disabled, must proceed regardless of active count)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseInProgress)
		}
	})
}

// TestHandlePreparedDataDownload_MultiDiskDeadlockRegression pins the fix for
// a deadlock CodeRabbit and a human reviewer both independently found in
// #186: when N DataDownloads for the same multi-disk VM restore all reach
// Prepared together (Velero creates every disk's DataDownload for a restore
// at once, so this is the *normal* case, not an edge case), a raw
// active-count gate is symmetric -- every sibling sees N-1 others active,
// and if N-1 >= limit, all of them defer forever, since none can reach
// InProgress without passing a gate that's held by peers stuck at the same
// gate. Ranking (countHigherPriorityActiveDataDownloads) breaks that
// symmetry: exactly the first MaxConcurrentDataMovers-ranked siblings must
// proceed regardless of the order handlePrepared happens to be called in.
func TestHandlePreparedDataDownload_MultiDiskDeadlockRegression(t *testing.T) {
	const (
		vmName      = "test-vm"
		vmNamespace = "vm-ns"
		oadpNS      = "openshift-adp"
		siblings    = 5
		limit       = 2
	)
	baseTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: oadpNS},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{Bucket: "test-bucket", Prefix: "velero"},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
	}
	credSec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-creds", Namespace: oadpNS},
		Data:       map[string][]byte{"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n")},
	}

	scheme := ddScheme()
	objs := make([]client.Object, 0, 2+2*siblings)
	objs = append(objs, bsl, credSec)
	dds := make([]*velerov2alpha1.DataDownload, siblings)
	for i := range siblings {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("dd-disk-%d", i), Namespace: oadpNS, UID: types.UID(fmt.Sprintf("dd-disk-%d-uid", i)),
				// Same CreationTimestamp for every sibling -- exactly what
				// Velero creating all of a VM's DataDownloads together
				// produces. Ranking falls through to the UID tiebreak, which
				// is still a strict total order, so this is the deadlock
				// scenario at its sharpest: no timestamp differences to
				// accidentally break the symmetry.
				CreationTimestamp: baseTime,
				Annotations: map[string]string{
					common.AnnotationVMName:      vmName,
					common.AnnotationVMNamespace: vmNamespace,
					AnnotationTargetDiskName:     fmt.Sprintf("disk%d", i),
				},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover:             common.DataMoverKubeVirt,
				SourceNamespace:       vmNamespace,
				BackupStorageLocation: "default",
				TargetVolume:          velerov2alpha1.TargetVolumeSpec{PVC: fmt.Sprintf("restored-disk-%d", i), Namespace: "restore-ns"},
			},
			Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhasePrepared, AcceptedTimestamp: &baseTime},
		}
		scratchPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("scratch-pvc-%d", i), Namespace: oadpNS,
				Labels: map[string]string{common.LabelDataDownloadUID: string(dd.UID)},
			},
		}
		dds[i] = dd
		objs = append(objs, dd, scratchPVC)
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	r := &KubeVirtDataDownloadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: oadpNS,
		DatamoverImage: "quay.io/test/datamover:latest", MaxConcurrentDataMovers: limit,
	}

	// Reconcile every sibling exactly once, in reverse order -- if the gate's
	// decisions depended on call order rather than each DD's own precomputed
	// rank, this would expose it (the deadlocked version of the gate defers
	// every single one regardless of order, so this alone would already
	// catch that bug; processing backwards additionally rules out an
	// order-dependent partial fix).
	for i := siblings - 1; i >= 0; i-- {
		if _, err := r.handlePrepared(context.Background(), logr.Discard(), dds[i]); err != nil {
			t.Fatalf("dd-disk-%d: unexpected error: %v", i, err)
		}
	}

	inProgress, prepared := 0, 0
	for i := range siblings {
		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: dds[i].Name, Namespace: oadpNS}, &updated); err != nil {
			t.Fatalf("dd-disk-%d: failed to get DataDownload: %v", i, err)
		}
		switch updated.Status.Phase {
		case velerov2alpha1.DataDownloadPhaseInProgress:
			inProgress++
		case velerov2alpha1.DataDownloadPhasePrepared:
			prepared++
		default:
			t.Errorf("dd-disk-%d: phase = %q, want InProgress or Prepared", i, updated.Status.Phase)
		}
	}

	if inProgress != limit {
		t.Errorf("got %d DataDownloads InProgress, want exactly %d (MaxConcurrentDataMovers) -- %d stuck deferred forever means the gate deadlocked",
			inProgress, limit, prepared)
	}
	if prepared != siblings-limit {
		t.Errorf("got %d DataDownloads still Prepared (deferred), want %d", prepared, siblings-limit)
	}
}

// TestHandlePreparedDataDownload_BlockMode covers a Block-mode restore target:
// the downloader pod must mount the work PVC as a filesystem volume (staging
// the qcow2 chain) and the output PVC as a raw volumeDevice (receiving the
// final flattened image), with KUBEVIRT_DM_TARGET_IS_BLOCK_DEVICE set so the
// downloader knows which publish path to take. Split out from
// TestHandlePreparedDataDownload to keep that function's cyclomatic
// complexity down (gocyclo).
func TestHandlePreparedDataDownload_BlockMode(t *testing.T) {
	f := newDDTestFixture(t)
	f.dd.Annotations[AnnotationTargetDiskName] = "disk1"
	f.dd.Annotations[AnnotationRestoreBlockMode] = "true"
	scheme := ddScheme()
	workPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "work-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleWork,
			},
		},
	}
	outputPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "output-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleOutput,
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, workPVC, outputPVC).Build()
	r := &KubeVirtDataDownloadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
		DatamoverImage: "quay.io/test/datamover:latest",
	}

	if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseInProgress {
		t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseInProgress, updated.Status.Message)
	}

	pod, err := r.findPodForDataDownload(context.Background(), f.dd, "openshift-adp")
	if err != nil {
		t.Fatalf("failed to find pod: %v", err)
	}
	if pod == nil {
		t.Fatal("expected downloader pod to be created")
	}
	if len(pod.Spec.Containers[0].VolumeDevices) != 1 {
		t.Fatalf("expected exactly 1 volumeDevice, got %d", len(pod.Spec.Containers[0].VolumeDevices))
	}
	if pod.Spec.Containers[0].VolumeDevices[0].Name != restoreOutputVolumeName {
		t.Errorf("volumeDevice name = %q, want %q", pod.Spec.Containers[0].VolumeDevices[0].Name, restoreOutputVolumeName)
	}

	var foundWorkVolume, foundOutputVolume bool
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}
		switch v.PersistentVolumeClaim.ClaimName {
		case "work-pvc-1":
			foundWorkVolume = true
		case "output-pvc-1":
			foundOutputVolume = true
		}
	}
	if !foundWorkVolume {
		t.Error("expected pod to have a volume sourced from the work PVC")
	}
	if !foundOutputVolume {
		t.Error("expected pod to have a volume sourced from the output PVC")
	}

	var isBlockDeviceEnv string
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == downloader.EnvTargetIsBlockDevice {
			isBlockDeviceEnv = env.Value
			break
		}
	}
	if isBlockDeviceEnv != "true" {
		t.Errorf("env %s = %q, want %q", downloader.EnvTargetIsBlockDevice, isBlockDeviceEnv, "true")
	}
}

func TestHandleInProgressDataDownload_AlreadyProvisioned(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()

	// Simulates: a prior reconcile's rebindPVToNamespace succeeded (scratch PVC
	// deleted, PV's claimRef patched to point at the target PVC, PV labeled
	// with our UID) but the subsequent updatePhase(Completed) failed to
	// persist, so this reconcile starts from InProgress again with no pod and
	// no scratch PVC left. The target PVC's own Status.Phase is deliberately
	// left Pending (not Bound) to prove isRestoreAlreadyProvisioned detects
	// the committed rebind via the PV's claimRef, not the PVC's Bound status
	// (which the Kubernetes PV controller only sets asynchronously). Both
	// UIDs are set to the same real, non-empty value so this exercises the
	// actual claimRef-UID-vs-live-PVC-UID comparison, not the empty-UID
	// legacy fallback (see the mismatch test below for that branch).
	f.targetPVC.UID = types.UID("target-pvc-uid")
	f.dd.Annotations[AnnotationDownloaderPodSucceeded] = downloaderPodSucceededValue
	reboundPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "pv-already-rebound",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Name:      f.targetPVC.Name,
				Namespace: f.targetPVC.Namespace,
				UID:       f.targetPVC.UID,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.targetPVC, reboundPV).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
		t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted, updated.Status.Message)
	}
}

func TestHandleInProgressDataDownload_ClaimRefUIDMismatch(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	f.dd.Annotations[AnnotationDownloaderPodSucceeded] = downloaderPodSucceededValue

	// The claimRef names the right PVC/namespace, but a different UID -- as if
	// the target PVC had been deleted and recreated after this PV's claimRef was
	// set. isRestoreAlreadyProvisioned must not treat this as the same restore.
	reboundPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "pv-already-rebound",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Name:      f.targetPVC.Name,
				Namespace: f.targetPVC.Namespace,
				UID:       types.UID("some-other-uid"),
			},
		},
	}
	f.targetPVC.UID = types.UID("actual-target-pvc-uid")

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.targetPVC, reboundPV).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	done, err := r.isRestoreAlreadyProvisioned(context.Background(), f.dd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if done {
		t.Error("expected isRestoreAlreadyProvisioned = false for a claimRef UID mismatch")
	}
}

func TestHandleInProgressDataDownload_SkipsPVLookupWhenPodNotSucceeded(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()

	// A matching, already-provisioned PV is present, but AnnotationDownloaderPodSucceeded
	// is deliberately left unset: the PV's claimRef can only ever have been set by
	// rebindPVToNamespace, which never runs before that annotation is persisted (see
	// its own doc comment), so this scenario cannot occur for a real DataDownload --
	// but it proves the guard short-circuits on the annotation rather than reaching
	// the PV list at all.
	reboundPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "pv-already-rebound",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Name:      f.targetPVC.Name,
				Namespace: f.targetPVC.Namespace,
				UID:       f.targetPVC.UID,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.targetPVC, reboundPV).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	done, err := r.isRestoreAlreadyProvisioned(context.Background(), f.dd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if done {
		t.Error("expected isRestoreAlreadyProvisioned = false when AnnotationDownloaderPodSucceeded isn't set, regardless of a matching PV existing")
	}
}

func TestHandleInProgressDataDownload_PodFailed(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{
				{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: "qemu-img failed"}}},
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var updated velerov2alpha1.DataDownload
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
		t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
	}
	if updated.Status.Message == "" {
		t.Error("expected non-empty failure message")
	}
}

func TestHandleInProgressDataDownload_PodRunning(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	result, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected requeue while pod is running")
	}
}

func TestHandleInProgressDataDownload_PodSucceeded(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()

	// Target PVC starts Pending/unbound (the realistic Velero-created placeholder
	// state) so this test exercises the real validateExistingPVCForBind mutation path
	// (retain, delete scratch PVC, patch claimRef, wait for bound) rather than
	// the already-bound idempotent short-circuit.
	origInterval, origTimeout := pvRebindPollInterval, pvRebindTimeout
	pvRebindPollInterval = 10 * time.Millisecond
	pvRebindTimeout = 2 * time.Second
	defer func() {
		pvRebindPollInterval = origInterval
		pvRebindTimeout = origTimeout
	}()

	scratchPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              "standard",
			VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
		},
	}
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "pv-scratch",
			StorageClassName: new("standard"),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(f.dd, f.targetPVC, scratchPV, scratchPVC, pod).
		Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	// Fake clients don't run the real Kubernetes PV binder, so simulate it.
	defer startFakeBinder(t, fakeClient, scratchPV, f.targetPVC, pvRebindTimeout)()

	if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
		t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted, updated.Status.Message)
	}
	if updated.Annotations[AnnotationDownloaderPodSucceeded] != "true" {
		t.Errorf("expected %s annotation to be persisted before the pod delete", AnnotationDownloaderPodSucceeded)
	}

	var pv corev1.PersistentVolume
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pv-scratch"}, &pv); err != nil {
		t.Fatalf("failed to get PV: %v", err)
	}
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Name != f.targetPVC.Name || pv.Spec.ClaimRef.Namespace != f.targetPVC.Namespace {
		t.Errorf("PV claimRef = %+v, want %s/%s", pv.Spec.ClaimRef, f.targetPVC.Namespace, f.targetPVC.Name)
	}

	var pods corev1.PodList
	_ = fakeClient.List(context.Background(), &pods)
	if len(pods.Items) != 0 {
		t.Errorf("expected downloader pod to be cleaned up after completion, found %d", len(pods.Items))
	}
}

func TestHandleInProgressDataDownload_PodAbsentWithSuccessMarker(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()

	// Same rebind fixtures as the pod-succeeded case above, but the pod is
	// already gone and the DataDownload carries the success marker -- the state
	// a retry reconcile sees after a prior pass persisted the marker, deleted
	// the pod, then errored on the strict still-terminating cleanup check. The
	// pod's absence must resume the rebind, not fail as "Downloader pod not found".
	origInterval, origTimeout := pvRebindPollInterval, pvRebindTimeout
	pvRebindPollInterval = 10 * time.Millisecond
	pvRebindTimeout = 2 * time.Second
	defer func() {
		pvRebindPollInterval = origInterval
		pvRebindTimeout = origTimeout
	}()

	if f.dd.Annotations == nil {
		f.dd.Annotations = map[string]string{}
	}
	f.dd.Annotations[AnnotationDownloaderPodSucceeded] = "true"

	scratchPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              "standard",
			VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
		},
	}
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "pv-scratch",
			StorageClassName: new("standard"),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(f.dd, f.targetPVC, scratchPV, scratchPVC).
		Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	defer startFakeBinder(t, fakeClient, scratchPV, f.targetPVC, pvRebindTimeout)()

	if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
		t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted, updated.Status.Message)
	}

	var pv corev1.PersistentVolume
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pv-scratch"}, &pv); err != nil {
		t.Fatalf("failed to get PV: %v", err)
	}
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Name != f.targetPVC.Name || pv.Spec.ClaimRef.Namespace != f.targetPVC.Namespace {
		t.Errorf("PV claimRef = %+v, want %s/%s", pv.Spec.ClaimRef, f.targetPVC.Namespace, f.targetPVC.Name)
	}
}

func TestHandleInProgressDataDownload_SyncsProgress(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			Annotations: map[string]string{
				"kubevirt-datamover.io/total-bytes": "2048",
				"kubevirt-datamover.io/bytes-done":  "1024",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)

	if updated.Status.Progress.TotalBytes != 2048 {
		t.Errorf("expected TotalBytes=2048, got %d", updated.Status.Progress.TotalBytes)
	}
	if updated.Status.Progress.BytesDone != 1024 {
		t.Errorf("expected BytesDone=1024, got %d", updated.Status.Progress.BytesDone)
	}
}

// TestCompleteSuccessfulDownload_EmitsRetainedPVEvent covers #152: since the
// rebound PV is intentionally left in the Retain policy forever (restored user
// data must survive deletion of the target PVC), there's no other operational
// signal pointing an operator at it -- completeSuccessfulDownload must emit an
// Event so it's discoverable without already knowing to look.
func TestCompleteSuccessfulDownload_EmitsRetainedPVEvent(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()

	origInterval, origTimeout := pvRebindPollInterval, pvRebindTimeout
	pvRebindPollInterval = 10 * time.Millisecond
	pvRebindTimeout = 2 * time.Second
	defer func() {
		pvRebindPollInterval = origInterval
		pvRebindTimeout = origTimeout
	}()

	scratchPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              "standard",
			VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
		},
	}
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "pv-scratch",
			StorageClassName: new("standard"),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.targetPVC, scratchPV, scratchPVC).Build()
	recorder := record.NewFakeRecorder(10)
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, EventRecorder: recorder, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	defer startFakeBinder(t, fakeClient, scratchPV, f.targetPVC, pvRebindTimeout)()

	if _, err := r.completeSuccessfulDownload(context.Background(), logr.Discard(), f.dd, "openshift-adp"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "PVLeftInRetainPolicy") {
			t.Errorf("event = %q, want it to reference PVLeftInRetainPolicy", event)
		}
	default:
		t.Error("expected an Event to be recorded for the retained PV, got none")
	}
}

// TestCompleteSuccessfulDownload_BlockMode_RebindsOutputPVCAndDeletesWorkPVC
// covers the Block-mode restore path: only the output PVC/PV (which holds the
// final flattened raw image) gets rebound onto the target -- the work PVC
// (which only ever staged the qcow2 chain) must be deleted afterward rather
// than left behind or rebound itself.
func TestCompleteSuccessfulDownload_BlockMode_RebindsOutputPVCAndDeletesWorkPVC(t *testing.T) {
	f := newDDTestFixture(t)
	f.dd.Annotations[AnnotationRestoreBlockMode] = "true"
	f.targetPVC.Spec.VolumeMode = new(corev1.PersistentVolumeBlock)
	scheme := ddScheme()

	origInterval, origTimeout := pvRebindPollInterval, pvRebindTimeout
	pvRebindPollInterval = 10 * time.Millisecond
	pvRebindTimeout = 2 * time.Second
	defer func() {
		pvRebindPollInterval = origInterval
		pvRebindTimeout = origTimeout
	}()

	outputPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-output"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              "standard",
			VolumeMode:                    new(corev1.PersistentVolumeBlock),
		},
	}
	outputPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "output-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleOutput,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "pv-output",
			StorageClassName: new("standard"),
			VolumeMode:       new(corev1.PersistentVolumeBlock),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	workPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "work-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleWork,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.targetPVC, outputPV, outputPVC, workPVC).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	defer startFakeBinder(t, fakeClient, outputPV, f.targetPVC, pvRebindTimeout)()

	if _, err := r.completeSuccessfulDownload(context.Background(), logr.Discard(), f.dd, "openshift-adp"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
		t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted, updated.Status.Message)
	}

	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: workPVC.Name, Namespace: workPVC.Namespace}, &corev1.PersistentVolumeClaim{}); !errors.IsNotFound(err) {
		t.Errorf("expected work PVC to be deleted after a successful Block-mode restore, get returned: %v", err)
	}
}

// TestEmitPodLogsDataDownload_TruncatesLongOutput covers #154: a downloader
// pod that produced a very large log (e.g. a crash loop with verbose output)
// must not flood the controller's own logs unbounded -- only the last
// maxEmittedPodLogLines lines are kept, with a truncation notice recording how
// many were dropped.
func TestEmitPodLogsDataDownload_TruncatesLongOutput(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "downloader-test-pod", Namespace: "openshift-adp"}}

	const totalLines = maxEmittedPodLogLines + 50
	var sb strings.Builder
	for i := 1; i <= totalLines; i++ {
		fmt.Fprintf(&sb, "line-%d\n", i)
	}

	var logBuf bytes.Buffer
	logger := funcr.New(func(prefix, args string) {
		logBuf.WriteString(args + "\n")
	}, funcr.Options{})

	r := &KubeVirtDataDownloadReconciler{
		PodLogCollector: func(ctx context.Context, podName, podNamespace string) (string, error) {
			return sb.String(), nil
		},
	}

	r.emitPodLogs(context.Background(), logger, pod)

	output := logBuf.String()
	if got := strings.Count(output, "\"message\"=\"line-"); got != maxEmittedPodLogLines {
		t.Errorf("expected %d emitted lines, got %d", maxEmittedPodLogLines, got)
	}
	if !strings.Contains(output, "Downloader pod log truncated") {
		t.Error("expected a truncation notice, found none")
	}
	if !strings.Contains(output, fmt.Sprintf("\"skippedLeadingLines\"=%d", totalLines-maxEmittedPodLogLines)) {
		t.Errorf("expected skippedLeadingLines=%d in the truncation notice, output: %s", totalLines-maxEmittedPodLogLines, output)
	}
	if strings.Contains(output, "\"message\"=\"line-1\"") {
		t.Error("expected the earliest lines to be dropped, but line-1 was emitted")
	}
	if !strings.Contains(output, fmt.Sprintf("\"message\"=\"line-%d\"", totalLines)) {
		t.Errorf("expected the final line (line-%d) to be kept", totalLines)
	}
}

// TestHandleInProgressDataDownload_PodSucceededDeleteFailureDoesNotPersistCompleted
// covers the same terminal-phase contract as the Canceling-path tests: once the
// downloader pod succeeds, cleanupPodsByUID's failure must block the rebind and
// the Completed transition (returning an error so the reconcile retries) rather
// than proceeding into rebindPVToNamespace's PVC-delete-and-wait with the pod
// object still present, which would just deadlock behind the pvc-protection
// finalizer. Split out from TestHandleInProgressDataDownload to keep that
// function's cyclomatic complexity down (gocyclo).
func TestHandleInProgressDataDownload_PodSucceededDeleteFailureDoesNotPersistCompleted(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()

	scratchPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              "standard",
			VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
		},
	}
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "pv-scratch",
			StorageClassName: new("standard"),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(f.dd, f.targetPVC, scratchPV, scratchPVC, pod).
		Build()
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				return fmt.Errorf("simulated delete failure")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	r := &KubeVirtDataDownloadReconciler{Client: interceptedClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err == nil {
		t.Fatal("expected an error when the downloader pod can't be deleted, got nil")
	}

	var updated velerov2alpha1.DataDownload
	if err := baseClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseCompleted {
		t.Error("phase must not be Completed until the downloader pod is actually gone")
	}
	if updated.Annotations[AnnotationDownloaderPodSucceeded] != downloaderPodSucceededValue {
		t.Errorf("expected %s annotation to be persisted before the failed delete attempt", AnnotationDownloaderPodSucceeded)
	}

	var pv corev1.PersistentVolume
	if err := baseClient.Get(context.Background(), types.NamespacedName{Name: "pv-scratch"}, &pv); err != nil {
		t.Fatalf("failed to get PV: %v", err)
	}
	if pv.Spec.ClaimRef != nil {
		t.Errorf("expected scratch PV claimRef to remain unset (rebind must not run before pod cleanup succeeds), got %+v", pv.Spec.ClaimRef)
	}
}

// TestHandleInProgressDataDownload_PodNotFoundRequeuesInsteadOfFailing covers
// the pod-absent branch when no success marker is present: a pod handlePrepared
// just created may not yet be visible to this reconcile's cached client
// (informer propagation isn't synchronous with the create call), so this must
// requeue and let Spec.OperationTimeout bound how long a genuinely vanished pod
// goes undetected, rather than failing on what's often just a transient cache
// miss. Split out from TestHandleInProgressDataDownload to keep that function's
// cyclomatic complexity down (gocyclo).
func TestHandleInProgressDataDownload_PodNotFoundRequeuesInsteadOfFailing(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	result, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected a requeue when the downloader pod isn't found yet")
	}
	var updated velerov2alpha1.DataDownload
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
	if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseFailed {
		t.Errorf("phase = %q, want it to stay non-terminal (a missing pod alone isn't proof it's gone for good)", updated.Status.Phase)
	}
}

// ddDiskFixture is one disk's worth of state for TestDataDownloadReconcile_ConcurrentMultiDisk:
// a DataDownload targeting one disk of a shared multi-disk VM backup, its Velero-created
// target PVC, and the name to use for its simulated scratch PV.
type ddDiskFixture struct {
	diskName      string
	targetPVCName string
	scratchPVName string
	dd            *velerov2alpha1.DataDownload
	targetPVC     *corev1.PersistentVolumeClaim
}

// simulateDDDiskLifecycle simulates the parts of a real Kubernetes/KubeVirt
// environment a fake client doesn't run for us, for one disk's restore, in the
// order the controller itself depends on:
//  1. Dynamic-provision the scratch PVC once handleAccepted creates it (bind it to
//     a PV).
//  2. Complete the downloader pod once handlePrepared creates it (mark it
//     Succeeded) -- only after step 1, since handleInProgress's rebind requires the
//     scratch PVC already Bound at the moment it sees the pod Succeeded.
//  3. Finish the PV controller's side of the rebind once handleInProgress patches
//     the PV's claimRef at the target PVC (what rebindPVToNamespace itself writes).
//
// Runs as a single goroutine per disk -- since each disk rebinds an independent
// PV/target-PVC pair, one instance must never observe (or complete) another disk's
// PVC/pod/PV. timeout is captured by the caller before spawning, not read from the
// pvRebindTimeout package var here: the caller's test shrinks that var for the
// duration of the test and restores it via defer once the pipeline goroutines
// finish; if this goroutine's first statement ran after that restore, it would
// silently pick up the real multi-minute production default. Each of the three
// stages above gets its own fresh timeout window (the deadline is reset at the
// start of each stage, not computed once for all three) -- they're sequential
// and each depends on the last, so a single shared budget would let a slow
// stage eat into a later one's time, or even start the last stage with the
// deadline already expired. ctx lets the caller force an exit once its own
// Reconcile loop is done, win or lose.
func simulateDDDiskLifecycle(ctx context.Context, r *KubeVirtDataDownloadReconciler, timeout time.Duration, d *ddDiskFixture) {
	var deadline time.Time
	wait := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Millisecond):
			return time.Now().Before(deadline)
		}
	}
	bg := context.Background()

	// deadline is reset at the start of each stage below (not computed once
	// up front) -- these three stages are sequential and each depends on the
	// prior one completing, so a single shared budget would let a slow first
	// stage eat into a later one's time, or even leave the deadline already
	// expired by the time the last stage starts. Each stage gets its own full
	// timeout window instead.

	deadline = time.Now().Add(timeout)
	for {
		scratchPVC, err := r.findScratchPVC(bg, d.dd, "")
		if err == nil && scratchPVC != nil {
			if scratchPVC.Status.Phase == corev1.ClaimBound {
				break
			}
			scratchPV := &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: d.scratchPVName},
				Spec: corev1.PersistentVolumeSpec{
					Capacity:                      corev1.ResourceList{corev1.ResourceStorage: scratchPVC.Spec.Resources.Requests[corev1.ResourceStorage]},
					AccessModes:                   scratchPVC.Spec.AccessModes,
					PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					StorageClassName:              *scratchPVC.Spec.StorageClassName,
					VolumeMode:                    scratchPVC.Spec.VolumeMode,
				},
			}
			if r.Create(bg, scratchPV) == nil {
				scratchPVC.Spec.VolumeName = d.scratchPVName
				if r.Update(bg, scratchPVC) == nil {
					scratchPVC.Status.Phase = corev1.ClaimBound
					if r.Status().Update(bg, scratchPVC) == nil {
						break
					}
				}
			}
		}
		if !wait() {
			return
		}
	}

	deadline = time.Now().Add(timeout)
	for {
		pod, err := r.findPodForDataDownload(bg, d.dd, r.getPodNamespace(d.dd))
		if err == nil && pod != nil {
			if pod.Status.Phase == corev1.PodSucceeded {
				break
			}
			pod.Status.Phase = corev1.PodSucceeded
			if r.Status().Update(bg, pod) == nil {
				break
			}
		}
		if !wait() {
			return
		}
	}

	deadline = time.Now().Add(timeout)
	for {
		pv := &corev1.PersistentVolume{}
		if err := r.Get(bg, types.NamespacedName{Name: d.scratchPVName}, pv); err == nil &&
			pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == d.targetPVC.Name && pv.Spec.ClaimRef.Namespace == d.targetPVC.Namespace {
			pvc := &corev1.PersistentVolumeClaim{}
			if err := r.Get(bg, types.NamespacedName{Name: d.targetPVC.Name, Namespace: d.targetPVC.Namespace}, pvc); err == nil {
				pvc.Spec.VolumeName = d.scratchPVName
				if r.Update(bg, pvc) == nil {
					pvc.Status.Phase = corev1.ClaimBound
					if r.Status().Update(bg, pvc) == nil {
						return
					}
				}
			}
		}
		if !wait() {
			return
		}
	}
}

// runDDDiskPipeline drives one disk's DataDownload through New->Accepted->Prepared->
// InProgress->Completed by repeatedly calling the reconciler's public Reconcile
// method -- the same production entry point controller-runtime itself calls, and
// the same one MaxConcurrentReconciles lets run concurrently for different
// objects -- rather than invoking internal phase handlers directly, so this test
// exercises the actual concurrency surface instead of just the handlers in
// isolation. A background goroutine (simulateDDDiskLifecycle) plays the parts of
// the cluster a fake client doesn't run. Intended to be run concurrently (one
// goroutine per disk) against a reconciler/fake client shared with other disks of
// the same VM.
func runDDDiskPipeline(r *KubeVirtDataDownloadReconciler, d *ddDiskFixture) error {
	ctx := context.Background()
	nn := types.NamespacedName{Name: d.dd.Name, Namespace: d.dd.Namespace}

	// lifecycleTimeout is captured synchronously, before spawning -- see
	// simulateDDDiskLifecycle's doc comment for why. cancelLifecycle+lifecycleWG.Wait()
	// below guarantee that goroutine exits before this function returns on any
	// path, so it never outlives the pipeline (or the test's own deferred
	// pvRebindTimeout restore).
	lifecycleTimeout := pvRebindTimeout
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	var lifecycleWG sync.WaitGroup
	lifecycleWG.Go(func() {
		simulateDDDiskLifecycle(lifecycleCtx, r, lifecycleTimeout, d)
	})
	defer func() {
		cancelLifecycle()
		lifecycleWG.Wait()
	}()

	const maxReconciles = 50
	for i := range maxReconciles {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
			return fmt.Errorf("%s: Reconcile (call %d): %w", d.dd.Name, i, err)
		}

		var current velerov2alpha1.DataDownload
		if err := r.Get(ctx, nn, &current); err != nil {
			return fmt.Errorf("%s: get after Reconcile: %w", d.dd.Name, err)
		}

		switch current.Status.Phase {
		case velerov2alpha1.DataDownloadPhaseCompleted:
			if got := current.Annotations[AnnotationTargetDiskName]; got != d.diskName {
				return fmt.Errorf("%s: target disk annotation = %q, want %q (cross-contamination if this is the other disk's name)",
					d.dd.Name, got, d.diskName)
			}
			var boundPV corev1.PersistentVolume
			if err := r.Get(ctx, types.NamespacedName{Name: d.scratchPVName}, &boundPV); err != nil {
				return fmt.Errorf("%s: get rebound PV: %w", d.dd.Name, err)
			}
			if boundPV.Spec.ClaimRef == nil || boundPV.Spec.ClaimRef.Name != d.targetPVC.Name || boundPV.Spec.ClaimRef.Namespace != d.targetPVC.Namespace {
				return fmt.Errorf("%s: PV %s claimRef = %+v, want %s/%s (cross-contamination if it points elsewhere)",
					d.dd.Name, d.scratchPVName, boundPV.Spec.ClaimRef, d.targetPVC.Namespace, d.targetPVC.Name)
			}
			return nil
		case velerov2alpha1.DataDownloadPhaseFailed:
			return fmt.Errorf("%s: reached Failed phase: %s", d.dd.Name, current.Status.Message)
		}
	}
	return fmt.Errorf("%s: did not reach Completed within %d Reconcile calls", d.dd.Name, maxReconciles)
}

// TestDataDownloadReconcile_ConcurrentMultiDisk drives two DataDownloads for the same
// VM's different disks through the full Accepted->Prepared->InProgress->Completed
// sequence concurrently against one shared reconciler/fake client, to prove Phase 4's
// isolation goal: every child-resource lookup (scratch PVC, downloader pod, PV rebind)
// is keyed by dd.UID or dd.Spec.TargetVolume.{PVC,Namespace}, so two disks of the same
// VM never cross-contaminate even when reconciled at the same time. Run with -race.
func TestDataDownloadReconcile_ConcurrentMultiDisk(t *testing.T) {
	origInterval, origTimeout := pvRebindPollInterval, pvRebindTimeout
	pvRebindPollInterval = 10 * time.Millisecond
	pvRebindTimeout = 2 * time.Second
	defer func() {
		pvRebindPollInterval = origInterval
		pvRebindTimeout = origTimeout
	}()

	const (
		vmName       = "test-vm"
		vmNamespace  = "vm-ns"
		oadpNS       = "openshift-adp"
		backupName   = "backup-001"
		restoreNS    = "restore-ns"
		checkpointID = "cp-001"
	)

	scheme := ddScheme()

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: oadpNS},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{Bucket: "test-bucket", Prefix: "velero"},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
	}
	credSec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-creds", Namespace: oadpNS},
		Data:       map[string][]byte{"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n")},
	}

	disks := []*ddDiskFixture{
		{diskName: "disk1", targetPVCName: "restored-disk-1", scratchPVName: "pv-scratch-disk1"},
		{diskName: "disk2", targetPVCName: "restored-disk-2", scratchPVName: "pv-scratch-disk2"},
	}

	// One checkpoint entry covering both disks of the same VM backup -- realistic
	// shape for a multi-disk VM, and it exercises resolveTargetDiskName/
	// calculateScratchPVCSize's index-aligned PVCs/Files/PVCSizes matching per disk.
	checkpointEntry := uploader.CheckpointEntry{
		ID:       checkpointID,
		Type:     "full",
		PVCs:     make([]string, len(disks)),
		PVCSizes: make([]resource.Quantity, len(disks)),
		Files:    make([]uploader.CheckpointFile, len(disks)),
	}
	for i, d := range disks {
		checkpointEntry.PVCs[i] = d.targetPVCName
		checkpointEntry.PVCSizes[i] = resource.MustParse("10Gi")
		checkpointEntry.Files[i] = uploader.CheckpointFile{
			Filename:   "vmb-" + checkpointID + "-" + d.diskName + ".qcow2",
			DiskName:   d.diskName,
			Size:       1024 * 1024 * 1024,
			ObjectPath: "checkpoints/" + vmNamespace + "/" + vmName + "/" + checkpointID + "/vmb-" + checkpointID + "-" + d.diskName + ".qcow2",
		}
	}

	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")
	vmIndex := uploader.VMIndex{
		VMName:      vmName,
		Namespace:   vmNamespace,
		Checkpoints: []uploader.CheckpointEntry{checkpointEntry},
	}
	if err := uploader.PutVMIndex(mockStore, vmNamespace, vmName, "test-bucket", vmIndex); err != nil {
		t.Fatalf("failed to seed VM index: %v", err)
	}
	manifest := uploader.VMBackupManifest{
		Namespace:       vmNamespace,
		Name:            vmName,
		CheckpointChain: []string{checkpointID},
		BackupName:      backupName,
	}
	if err := uploader.PutVMBackupManifest(mockStore, vmNamespace, vmName, backupName, "test-bucket", manifest); err != nil {
		t.Fatalf("failed to seed VM backup manifest: %v", err)
	}

	objs := make([]client.Object, 0, 2+2*len(disks))
	objs = append(objs, bsl, credSec)
	for _, d := range disks {
		d.dd = &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dd-" + d.diskName,
				Namespace: oadpNS,
				UID:       types.UID("dd-uid-" + d.diskName),
				Annotations: map[string]string{
					common.AnnotationVMName:      vmName,
					common.AnnotationVMNamespace: vmNamespace,
				},
				Labels: map[string]string{
					common.LabelVeleroBackupName: backupName,
				},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover:             common.DataMoverKubeVirt,
				SourceNamespace:       vmNamespace,
				BackupStorageLocation: "default",
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC:       d.targetPVCName,
					Namespace: restoreNS,
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseAccepted},
		}
		d.targetPVC = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: d.targetPVCName, Namespace: restoreNS},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: new("standard"),
				VolumeMode:       new(corev1.PersistentVolumeFilesystem),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
		}
		objs = append(objs, d.dd, d.targetPVC)
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	r := &KubeVirtDataDownloadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: oadpNS,
		DatamoverImage:     "quay.io/test/datamover:latest",
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return mockStore, nil },
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(disks))
	for _, d := range disks {
		wg.Add(1)
		go func(d *ddDiskFixture) {
			defer wg.Done()
			errCh <- runDDDiskPipeline(r, d)
		}(d)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Error(err)
		}
	}

	// No leftover downloader pods: both disks' handleInProgress should have
	// cleaned up their own pod after reaching Completed, independently.
	var pods corev1.PodList
	_ = fakeClient.List(context.Background(), &pods)
	if len(pods.Items) != 0 {
		t.Errorf("expected all downloader pods cleaned up, found %d", len(pods.Items))
	}
}

func TestHandleCancelingDataDownload(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod, scratchPVC).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleCanceling(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCanceled {
		t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCanceled)
	}

	var pods corev1.PodList
	_ = fakeClient.List(context.Background(), &pods)
	if len(pods.Items) != 0 {
		t.Errorf("expected pod to be cleaned up, found %d", len(pods.Items))
	}

	var pvcs corev1.PersistentVolumeClaimList
	_ = fakeClient.List(context.Background(), &pvcs)
	if len(pvcs.Items) != 0 {
		t.Errorf("expected scratch PVC to be cleaned up, found %d", len(pvcs.Items))
	}
}

// TestHandleCancelingDataDownload_BlockMode_CleansUpBothWorkAndOutputPVCs
// covers cancellation of a Block-mode restore, which provisioned two scratch
// PVCs instead of one -- both the work and output PVCs must be cleaned up,
// not just whichever one an UID-label-only lookup happens to find first.
func TestHandleCancelingDataDownload_BlockMode_CleansUpBothWorkAndOutputPVCs(t *testing.T) {
	f := newDDTestFixture(t)
	f.dd.Annotations[AnnotationRestoreBlockMode] = "true"
	scheme := ddScheme()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	workPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "work-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleWork,
			},
		},
	}
	outputPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "output-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleOutput,
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod, workPVC, outputPVC).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleCanceling(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCanceled {
		t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCanceled)
	}

	var pvcs corev1.PersistentVolumeClaimList
	_ = fakeClient.List(context.Background(), &pvcs)
	if len(pvcs.Items) != 0 {
		t.Errorf("expected both work and output PVCs to be cleaned up, found %d", len(pvcs.Items))
	}
}

// TestHandleCancelingDataDownload_PodCleanupFailureDoesNotPersistCanceled
// covers a case Canceled being terminal makes important: if the pod can't be
// stopped, handleCanceling must return an error (so it retries) rather than
// deleting the scratch PVC and persisting Canceled anyway -- once Canceled
// persists, no further reconciliation ever runs for this object, so a still-
// running pod (and, if the scratch PVC delete raced ahead of it, a PVC wedged
// in Terminating behind the pvc-protection finalizer) would be abandoned
// forever with no chance to retry.
func TestHandleCancelingDataDownload_PodCleanupFailureDoesNotPersistCanceled(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod, scratchPVC).Build()
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				return fmt.Errorf("simulated delete failure")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	r := &KubeVirtDataDownloadReconciler{Client: interceptedClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleCanceling(context.Background(), logr.Discard(), f.dd); err == nil {
		t.Fatal("expected an error when the downloader pod can't be deleted, got nil")
	}

	var updated velerov2alpha1.DataDownload
	if err := baseClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseCanceled {
		t.Error("phase must not be Canceled until pod cleanup actually succeeds")
	}

	var pvcs corev1.PersistentVolumeClaimList
	_ = baseClient.List(context.Background(), &pvcs)
	if len(pvcs.Items) != 1 {
		t.Errorf("expected scratch PVC to be left alone (pod cleanup failed first), found %d", len(pvcs.Items))
	}
}

// TestHandleCancelingDataDownload_PodStillTerminatingRequeuesWithoutError
// covers the expected, self-resolving case ErrPodsStillTerminating exists
// for: a downloader pod blocked on a finalizer (Delete accepted, kubelet just
// hasn't finished tearing it down yet) must requeue quickly without being
// treated as a reconcile error -- unlike a genuine cleanup failure, this
// isn't something worth logging as broken.
func TestHandleCancelingDataDownload_PodStillTerminatingRequeuesWithoutError(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels:     map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			Finalizers: []string{"example.com/still-cleaning-up"},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	result, err := r.handleCanceling(context.Background(), logr.Discard(), f.dd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected a short requeue while the pod is still terminating")
	}

	var updated velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseCanceled {
		t.Error("phase must not be Canceled until the pod actually finishes terminating")
	}
}

// TestHandleCancelingDataDownload_ScratchPVCDeleteFailureDoesNotPersistCanceled
// covers the same terminal-phase contract as the pod-cleanup-failure test
// above, but for the scratch PVC delete step: Canceled is terminal (no further
// reconciliation ever runs for this object once it persists), so a swallowed
// scratch PVC delete failure would leak it forever with nothing left to retry
// the delete -- deleteAllScratchPVCs must propagate the failure instead of the
// best-effort cleanupScratchPVCIfPresent used by other, non-terminal paths.
func TestHandleCancelingDataDownload_ScratchPVCDeleteFailureDoesNotPersistCanceled(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, scratchPVC).Build()
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
				return fmt.Errorf("simulated delete failure")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	r := &KubeVirtDataDownloadReconciler{Client: interceptedClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleCanceling(context.Background(), logr.Discard(), f.dd); err == nil {
		t.Fatal("expected an error when the scratch PVC can't be deleted, got nil")
	}

	var updated velerov2alpha1.DataDownload
	if err := baseClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseCanceled {
		t.Error("phase must not be Canceled until scratch PVC cleanup actually succeeds")
	}

	var pvcs corev1.PersistentVolumeClaimList
	_ = baseClient.List(context.Background(), &pvcs)
	if len(pvcs.Items) != 1 {
		t.Errorf("expected scratch PVC to still exist (delete failed), found %d", len(pvcs.Items))
	}
}

func TestBuildDownloaderPodConfig(t *testing.T) {
	f := newDDTestFixture(t)
	f.bsl.Spec.Provider = "azure"
	f.bsl.Spec.Config = map[string]string{
		"resourceGroup":               "my-rg",
		"storageAccount":              "my-account",
		"storageAccountKeyEnvVar":     "AZURE_STORAGE_KEY",
		"storageAccountURI":           "https://my-account.blob.core.windows.net",
		"subscriptionId":              "sub-id",
		"useAAD":                      "true",
		"activeDirectoryAuthorityURI": "https://login.microsoftonline.com",
	}
	scheme := ddScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec).Build()
	r := &KubeVirtDataDownloadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
		DatamoverImage: "quay.io/test/datamover:latest",
	}
	vmRef := &common.VMReference{Name: "test-vm", Namespace: "vm-ns"}

	cfg, err := r.buildDownloaderPodConfig(f.dd, f.bsl, vmRef, "scratch-pvc-1", "", "disk1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ScratchPVCName != "scratch-pvc-1" {
		t.Errorf("ScratchPVCName = %q, want %q", cfg.ScratchPVCName, "scratch-pvc-1")
	}
	if cfg.TargetVolume != "disk1" {
		t.Errorf("TargetVolume = %q, want %q", cfg.TargetVolume, "disk1")
	}
	if cfg.BSLResourceGroup != "my-rg" {
		t.Errorf("BSLResourceGroup = %q, want %q", cfg.BSLResourceGroup, "my-rg")
	}
	if cfg.BSLStorageAccount != "my-account" {
		t.Errorf("BSLStorageAccount = %q, want %q", cfg.BSLStorageAccount, "my-account")
	}
	if cfg.BSLStorageAccountKeyEnvVar != "AZURE_STORAGE_KEY" {
		t.Errorf("BSLStorageAccountKeyEnvVar = %q, want %q", cfg.BSLStorageAccountKeyEnvVar, "AZURE_STORAGE_KEY")
	}
	if cfg.BSLStorageAccountURI != "https://my-account.blob.core.windows.net" {
		t.Errorf("BSLStorageAccountURI = %q, want %q", cfg.BSLStorageAccountURI, "https://my-account.blob.core.windows.net")
	}
	if cfg.BSLSubscriptionID != "sub-id" {
		t.Errorf("BSLSubscriptionID = %q, want %q", cfg.BSLSubscriptionID, "sub-id")
	}
	if cfg.BSLUseAAD != "true" {
		t.Errorf("BSLUseAAD = %q, want %q", cfg.BSLUseAAD, "true")
	}
	if cfg.BSLActiveDirectoryAuthorityURI != "https://login.microsoftonline.com" {
		t.Errorf("BSLActiveDirectoryAuthorityURI = %q, want %q", cfg.BSLActiveDirectoryAuthorityURI, "https://login.microsoftonline.com")
	}
}

// TestBuildDownloaderPodConfig_S3Fields pins issue #184: buildDownloaderPodConfig
// must propagate the same S3-specific BSL fields buildDatamoverPodConfig (upload)
// does -- SSE, KMS key ID, checksum algorithm, profile, and the SSEC secret
// reference -- so a restore from a BSL configured with these doesn't silently
// ignore them.
func TestBuildDownloaderPodConfig_S3Fields(t *testing.T) {
	f := newDDTestFixture(t)
	f.bsl.Spec.Provider = "aws"
	f.bsl.Spec.Config = map[string]string{
		"region":                      "us-east-1",
		"serverSideEncryption":        "aws:kms",
		"kmsKeyId":                    "arn:aws:kms:us-east-1:123456789012:key/test-key",
		"checksumAlgorithm":           "SHA256",
		"profile":                     "minio",
		"customerKeyEncryptionSecret": "my-ssec-secret/key.b64",
	}
	scheme := ddScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec).Build()
	r := &KubeVirtDataDownloadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
		DatamoverImage: "quay.io/test/datamover:latest",
	}
	vmRef := &common.VMReference{Name: "test-vm", Namespace: "vm-ns"}

	cfg, err := r.buildDownloaderPodConfig(f.dd, f.bsl, vmRef, "scratch-pvc-1", "", "disk1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.BSLServerSideEncryption != "aws:kms" {
		t.Errorf("BSLServerSideEncryption = %q, want %q", cfg.BSLServerSideEncryption, "aws:kms")
	}
	if cfg.BSLKMSKeyID != "arn:aws:kms:us-east-1:123456789012:key/test-key" {
		t.Errorf("BSLKMSKeyID = %q, want %q", cfg.BSLKMSKeyID, "arn:aws:kms:us-east-1:123456789012:key/test-key")
	}
	if cfg.BSLChecksumAlgorithm != "SHA256" {
		t.Errorf("BSLChecksumAlgorithm = %q, want %q", cfg.BSLChecksumAlgorithm, "SHA256")
	}
	if cfg.BSLProfile != "minio" {
		t.Errorf("BSLProfile = %q, want %q", cfg.BSLProfile, "minio")
	}
	if cfg.SSECSecretName != "my-ssec-secret" {
		t.Errorf("SSECSecretName = %q, want %q", cfg.SSECSecretName, "my-ssec-secret")
	}
	if cfg.SSECSecretKey != "key.b64" {
		t.Errorf("SSECSecretKey = %q, want %q", cfg.SSECSecretKey, "key.b64")
	}
}

// ddSchemeWithKubeVirt extends ddScheme with kubevirtcorev1, for tests that
// (unlike handleNew/handleAccepted) legitimately need to fetch/patch a live
// VirtualMachine object -- the run-state restore flip.
func ddSchemeWithKubeVirt() *runtime.Scheme {
	scheme := ddScheme()
	_ = kubevirtcorev1.AddToScheme(scheme)
	return scheme
}

//nolint:gocyclo // Table of independent subtests, not complex control flow
func TestRestoreVMRunStateIfAllSiblingsCompleted(t *testing.T) {
	const (
		vmName      = "test-vm"
		vmNamespace = "vm-ns"
		oadpNS      = "openshift-adp"
	)

	newDD := func(name string, phase velerov2alpha1.DataDownloadPhase) *velerov2alpha1.DataDownload {
		return &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: oadpNS,
				Annotations: map[string]string{
					common.AnnotationVMName:      vmName,
					common.AnnotationVMNamespace: vmNamespace,
				},
			},
			Spec:   velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataDownloadStatus{Phase: phase},
		}
	}

	t.Run("flips RunStrategy-sourced VM back and clears stash annotations", func(t *testing.T) {
		scheme := ddSchemeWithKubeVirt()
		dd := newDD("dd-1", velerov2alpha1.DataDownloadPhaseCompleted)
		vm := &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vmName,
				Namespace: vmNamespace,
				Annotations: map[string]string{
					common.AnnotationOriginalRunStrategy:       string(kubevirtcorev1.RunStrategyManual),
					common.AnnotationOriginalRunStrategySource: common.RunStrategySourceRunStrategy,
				},
			},
			Spec: kubevirtcorev1.VirtualMachineSpec{
				RunStrategy: new(kubevirtcorev1.RunStrategyHalted),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, vm).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.restoreVMRunStateIfAllSiblingsCompleted(context.Background(), logr.Discard(), dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated kubevirtcorev1.VirtualMachine
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: vmName, Namespace: vmNamespace}, &updated); err != nil {
			t.Fatalf("failed to get VM: %v", err)
		}
		if updated.Spec.RunStrategy == nil || *updated.Spec.RunStrategy != kubevirtcorev1.RunStrategyManual {
			t.Errorf("RunStrategy = %v, want %q", updated.Spec.RunStrategy, kubevirtcorev1.RunStrategyManual)
		}
		if updated.Spec.Running != nil {
			t.Errorf("Running = %v, want nil", *updated.Spec.Running)
		}
		if _, ok := updated.Annotations[common.AnnotationOriginalRunStrategy]; ok {
			t.Error("AnnotationOriginalRunStrategy should have been deleted")
		}
		if _, ok := updated.Annotations[common.AnnotationOriginalRunStrategySource]; ok {
			t.Error("AnnotationOriginalRunStrategySource should have been deleted")
		}
	})

	t.Run("flips Running-sourced VM back to true for Always", func(t *testing.T) {
		scheme := ddSchemeWithKubeVirt()
		dd := newDD("dd-1", velerov2alpha1.DataDownloadPhaseCompleted)
		vm := &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vmName,
				Namespace: vmNamespace,
				Annotations: map[string]string{
					common.AnnotationOriginalRunStrategy:       string(kubevirtcorev1.RunStrategyAlways),
					common.AnnotationOriginalRunStrategySource: common.RunStrategySourceRunning,
				},
			},
			Spec: kubevirtcorev1.VirtualMachineSpec{Running: new(false)},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, vm).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.restoreVMRunStateIfAllSiblingsCompleted(context.Background(), logr.Discard(), dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated kubevirtcorev1.VirtualMachine
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: vmName, Namespace: vmNamespace}, &updated); err != nil {
			t.Fatalf("failed to get VM: %v", err)
		}
		if updated.Spec.Running == nil || !*updated.Spec.Running {
			t.Errorf("Running = %v, want true", updated.Spec.Running)
		}
		if updated.Spec.RunStrategy != nil {
			t.Errorf("RunStrategy = %v, want nil", *updated.Spec.RunStrategy)
		}
	})

	t.Run("flips Running-sourced VM back to false for Halted", func(t *testing.T) {
		scheme := ddSchemeWithKubeVirt()
		dd := newDD("dd-1", velerov2alpha1.DataDownloadPhaseCompleted)
		vm := &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vmName,
				Namespace: vmNamespace,
				Annotations: map[string]string{
					common.AnnotationOriginalRunStrategy:       string(kubevirtcorev1.RunStrategyHalted),
					common.AnnotationOriginalRunStrategySource: common.RunStrategySourceRunning,
				},
			},
			Spec: kubevirtcorev1.VirtualMachineSpec{Running: new(false)},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, vm).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.restoreVMRunStateIfAllSiblingsCompleted(context.Background(), logr.Discard(), dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated kubevirtcorev1.VirtualMachine
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: vmName, Namespace: vmNamespace}, &updated); err != nil {
			t.Fatalf("failed to get VM: %v", err)
		}
		if updated.Spec.Running == nil || *updated.Spec.Running {
			t.Errorf("Running = %v, want false", updated.Spec.Running)
		}
	})

	t.Run("does not flip while a sibling DataDownload is incomplete", func(t *testing.T) {
		scheme := ddSchemeWithKubeVirt()
		dd1 := newDD("dd-1", velerov2alpha1.DataDownloadPhaseCompleted)
		dd2 := newDD("dd-2", velerov2alpha1.DataDownloadPhaseInProgress)
		vm := &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vmName,
				Namespace: vmNamespace,
				Annotations: map[string]string{
					common.AnnotationOriginalRunStrategy:       string(kubevirtcorev1.RunStrategyAlways),
					common.AnnotationOriginalRunStrategySource: common.RunStrategySourceRunStrategy,
				},
			},
			Spec: kubevirtcorev1.VirtualMachineSpec{RunStrategy: new(kubevirtcorev1.RunStrategyHalted)},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd1, dd2, vm).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.restoreVMRunStateIfAllSiblingsCompleted(context.Background(), logr.Discard(), dd1); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated kubevirtcorev1.VirtualMachine
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: vmName, Namespace: vmNamespace}, &updated); err != nil {
			t.Fatalf("failed to get VM: %v", err)
		}
		if updated.Spec.RunStrategy == nil || *updated.Spec.RunStrategy != kubevirtcorev1.RunStrategyHalted {
			t.Errorf("RunStrategy = %v, want still %q (sibling dd-2 not Completed)", updated.Spec.RunStrategy, kubevirtcorev1.RunStrategyHalted)
		}
		if _, ok := updated.Annotations[common.AnnotationOriginalRunStrategy]; !ok {
			t.Error("stash annotation should not have been removed while a sibling is incomplete")
		}
	})

	t.Run("flips despite a stale Failed sibling from a different restore", func(t *testing.T) {
		scheme := ddSchemeWithKubeVirt()
		dd := newDD("dd-1", velerov2alpha1.DataDownloadPhaseCompleted)
		dd.Labels = map[string]string{common.LabelVeleroRestoreName: "restore-current"}
		staleSibling := newDD("dd-stale", velerov2alpha1.DataDownloadPhaseFailed)
		staleSibling.Labels = map[string]string{common.LabelVeleroRestoreName: "restore-previous"}
		vm := &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vmName,
				Namespace: vmNamespace,
				Annotations: map[string]string{
					common.AnnotationOriginalRunStrategy:       string(kubevirtcorev1.RunStrategyAlways),
					common.AnnotationOriginalRunStrategySource: common.RunStrategySourceRunStrategy,
				},
			},
			Spec: kubevirtcorev1.VirtualMachineSpec{RunStrategy: new(kubevirtcorev1.RunStrategyHalted)},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, staleSibling, vm).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.restoreVMRunStateIfAllSiblingsCompleted(context.Background(), logr.Discard(), dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated kubevirtcorev1.VirtualMachine
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: vmName, Namespace: vmNamespace}, &updated); err != nil {
			t.Fatalf("failed to get VM: %v", err)
		}
		if updated.Spec.RunStrategy == nil || *updated.Spec.RunStrategy != kubevirtcorev1.RunStrategyAlways {
			t.Errorf("RunStrategy = %v, want %q (stale sibling from a different restore must not block)",
				updated.Spec.RunStrategy, kubevirtcorev1.RunStrategyAlways)
		}
		if _, ok := updated.Annotations[common.AnnotationOriginalRunStrategy]; ok {
			t.Error("AnnotationOriginalRunStrategy should have been deleted")
		}
	})

	t.Run("does not flip while an incomplete sibling from the same restore exists", func(t *testing.T) {
		scheme := ddSchemeWithKubeVirt()
		dd := newDD("dd-1", velerov2alpha1.DataDownloadPhaseCompleted)
		dd.Labels = map[string]string{common.LabelVeleroRestoreName: "restore-current"}
		sibling := newDD("dd-2", velerov2alpha1.DataDownloadPhaseInProgress)
		sibling.Labels = map[string]string{common.LabelVeleroRestoreName: "restore-current"}
		vm := &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vmName,
				Namespace: vmNamespace,
				Annotations: map[string]string{
					common.AnnotationOriginalRunStrategy:       string(kubevirtcorev1.RunStrategyAlways),
					common.AnnotationOriginalRunStrategySource: common.RunStrategySourceRunStrategy,
				},
			},
			Spec: kubevirtcorev1.VirtualMachineSpec{RunStrategy: new(kubevirtcorev1.RunStrategyHalted)},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, sibling, vm).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.restoreVMRunStateIfAllSiblingsCompleted(context.Background(), logr.Discard(), dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated kubevirtcorev1.VirtualMachine
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: vmName, Namespace: vmNamespace}, &updated); err != nil {
			t.Fatalf("failed to get VM: %v", err)
		}
		if updated.Spec.RunStrategy == nil || *updated.Spec.RunStrategy != kubevirtcorev1.RunStrategyHalted {
			t.Errorf("RunStrategy = %v, want still %q (sibling from the SAME restore is still InProgress)", updated.Spec.RunStrategy, kubevirtcorev1.RunStrategyHalted)
		}
		if _, ok := updated.Annotations[common.AnnotationOriginalRunStrategy]; !ok {
			t.Error("stash annotation should not have been removed while a same-restore sibling is incomplete")
		}
	})

	t.Run("tolerates VM already gone", func(t *testing.T) {
		scheme := ddSchemeWithKubeVirt()
		dd := newDD("dd-1", velerov2alpha1.DataDownloadPhaseCompleted)
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.restoreVMRunStateIfAllSiblingsCompleted(context.Background(), logr.Discard(), dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("is a no-op when the VM has no stashed run state (already flipped)", func(t *testing.T) {
		scheme := ddSchemeWithKubeVirt()
		dd := newDD("dd-1", velerov2alpha1.DataDownloadPhaseCompleted)
		vm := &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{Name: vmName, Namespace: vmNamespace},
			Spec:       kubevirtcorev1.VirtualMachineSpec{RunStrategy: new(kubevirtcorev1.RunStrategyManual)},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, vm).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.restoreVMRunStateIfAllSiblingsCompleted(context.Background(), logr.Discard(), dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated kubevirtcorev1.VirtualMachine
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: vmName, Namespace: vmNamespace}, &updated); err != nil {
			t.Fatalf("failed to get VM: %v", err)
		}
		if updated.Spec.RunStrategy == nil || *updated.Spec.RunStrategy != kubevirtcorev1.RunStrategyManual {
			t.Errorf("RunStrategy = %v, want unchanged %q", updated.Spec.RunStrategy, kubevirtcorev1.RunStrategyManual)
		}
	})

	t.Run("returns an error for an unrecognized stash source value", func(t *testing.T) {
		scheme := ddSchemeWithKubeVirt()
		dd := newDD("dd-1", velerov2alpha1.DataDownloadPhaseCompleted)
		vm := &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vmName,
				Namespace: vmNamespace,
				Annotations: map[string]string{
					common.AnnotationOriginalRunStrategy:       string(kubevirtcorev1.RunStrategyAlways),
					common.AnnotationOriginalRunStrategySource: "bogus",
				},
			},
			Spec: kubevirtcorev1.VirtualMachineSpec{RunStrategy: new(kubevirtcorev1.RunStrategyHalted)},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, vm).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.restoreVMRunStateIfAllSiblingsCompleted(context.Background(), logr.Discard(), dd); err == nil {
			t.Fatal("expected error for unrecognized stash source value")
		}

		var updated kubevirtcorev1.VirtualMachine
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: vmName, Namespace: vmNamespace}, &updated); err != nil {
			t.Fatalf("failed to get VM: %v", err)
		}
		if updated.Spec.RunStrategy == nil || *updated.Spec.RunStrategy != kubevirtcorev1.RunStrategyHalted {
			t.Errorf("RunStrategy = %v, want unchanged %q (VM should be left untouched on error)", updated.Spec.RunStrategy, kubevirtcorev1.RunStrategyHalted)
		}
		if _, ok := updated.Annotations[common.AnnotationOriginalRunStrategy]; !ok {
			t.Error("stash annotations should not have been removed when the source value is unrecognized")
		}
	})

	t.Run("Reconcile wires the flip on the Completed terminal path", func(t *testing.T) {
		scheme := ddSchemeWithKubeVirt()
		dd := newDD("dd-1", velerov2alpha1.DataDownloadPhaseCompleted)
		vm := &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vmName,
				Namespace: vmNamespace,
				Annotations: map[string]string{
					common.AnnotationOriginalRunStrategy:       string(kubevirtcorev1.RunStrategyAlways),
					common.AnnotationOriginalRunStrategySource: common.RunStrategySourceRunStrategy,
				},
			},
			Spec: kubevirtcorev1.VirtualMachineSpec{RunStrategy: new(kubevirtcorev1.RunStrategyHalted)},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, vm).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace},
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated kubevirtcorev1.VirtualMachine
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: vmName, Namespace: vmNamespace}, &updated); err != nil {
			t.Fatalf("failed to get VM: %v", err)
		}
		if updated.Spec.RunStrategy == nil || *updated.Spec.RunStrategy != kubevirtcorev1.RunStrategyAlways {
			t.Errorf("RunStrategy = %v, want %q", updated.Spec.RunStrategy, kubevirtcorev1.RunStrategyAlways)
		}
	})

	t.Run("uses TargetVolume.Namespace when set (namespace mapping)", func(t *testing.T) {
		const targetNS = "mapped-target-ns"
		scheme := ddSchemeWithKubeVirt()
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "dd-mapped",
				Namespace: oadpNS,
				Annotations: map[string]string{
					common.AnnotationVMName:      vmName,
					common.AnnotationVMNamespace: vmNamespace,
				},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover: common.DataMoverKubeVirt,
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC:       "restored-pvc",
					Namespace: targetNS,
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseCompleted},
		}
		vm := &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vmName,
				Namespace: targetNS,
				Annotations: map[string]string{
					common.AnnotationOriginalRunStrategy:       string(kubevirtcorev1.RunStrategyManual),
					common.AnnotationOriginalRunStrategySource: common.RunStrategySourceRunStrategy,
				},
			},
			Spec: kubevirtcorev1.VirtualMachineSpec{RunStrategy: func() *kubevirtcorev1.VirtualMachineRunStrategy { s := kubevirtcorev1.RunStrategyHalted; return &s }()},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, vm).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.restoreVMRunStateIfAllSiblingsCompleted(context.Background(), logr.Discard(), dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated kubevirtcorev1.VirtualMachine
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: vmName, Namespace: targetNS}, &updated); err != nil {
			t.Fatalf("failed to get VM in target namespace: %v", err)
		}
		if updated.Spec.RunStrategy == nil || *updated.Spec.RunStrategy != kubevirtcorev1.RunStrategyManual {
			t.Errorf("RunStrategy = %v, want %q", updated.Spec.RunStrategy, kubevirtcorev1.RunStrategyManual)
		}
	})

	t.Run("falls back to vmRef.Namespace when TargetVolume.Namespace is empty", func(t *testing.T) {
		scheme := ddSchemeWithKubeVirt()
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "dd-fallback",
				Namespace: oadpNS,
				Annotations: map[string]string{
					common.AnnotationVMName:      vmName,
					common.AnnotationVMNamespace: vmNamespace,
				},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover: common.DataMoverKubeVirt,
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC:       "restored-pvc",
					Namespace: "",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseCompleted},
		}
		vm := &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      vmName,
				Namespace: vmNamespace,
				Annotations: map[string]string{
					common.AnnotationOriginalRunStrategy:       string(kubevirtcorev1.RunStrategyManual),
					common.AnnotationOriginalRunStrategySource: common.RunStrategySourceRunStrategy,
				},
			},
			Spec: kubevirtcorev1.VirtualMachineSpec{RunStrategy: func() *kubevirtcorev1.VirtualMachineRunStrategy { s := kubevirtcorev1.RunStrategyHalted; return &s }()},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, vm).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.restoreVMRunStateIfAllSiblingsCompleted(context.Background(), logr.Discard(), dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated kubevirtcorev1.VirtualMachine
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: vmName, Namespace: vmNamespace}, &updated); err != nil {
			t.Fatalf("failed to get VM in fallback namespace: %v", err)
		}
		if updated.Spec.RunStrategy == nil || *updated.Spec.RunStrategy != kubevirtcorev1.RunStrategyManual {
			t.Errorf("RunStrategy = %v, want %q", updated.Spec.RunStrategy, kubevirtcorev1.RunStrategyManual)
		}
	})
}
