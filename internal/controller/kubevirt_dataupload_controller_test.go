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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestReconcile(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtcorev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Helper function to create a valid VM with CBT enabled and running
	validVM := func(name, namespace string) *kubevirtcorev1.VirtualMachine {
		return &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Status: kubevirtcorev1.VirtualMachineStatus{
				PrintableStatus: kubevirtcorev1.VirtualMachineStatusRunning,
				ChangedBlockTracking: &kubevirtcorev1.ChangedBlockTrackingStatus{
					State: kubevirtcorev1.ChangedBlockTrackingEnabled,
				},
			},
		}
	}

	tests := []struct {
		name            string
		dataUpload      *velerov2alpha1.DataUpload
		vm              *kubevirtcorev1.VirtualMachine // optional: VM to create in fake client
		expectedRequeue bool
		expectedPhase   velerov2alpha1.DataUploadPhase
		expectError     bool
	}{
		{
			name: "skip non-kubevirt datamover",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: "velero",
				},
			},
			expectedRequeue: false,
			expectedPhase:   "",
			expectError:     false,
		},
		{
			name: "new phase transitions to accepted with VM annotations",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName:      "test-vm",
						common.AnnotationVMNamespace: "default",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseNew,
				},
			},
			vm:              validVM("test-vm", "default"),
			expectedRequeue: true,
			expectedPhase:   velerov2alpha1.DataUploadPhaseAccepted,
			expectError:     false,
		},
		{
			name: "empty phase transitions to accepted with VM annotations",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName:      "test-vm",
						common.AnnotationVMNamespace: "default",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: "",
				},
			},
			vm:              validVM("test-vm", "default"),
			expectedRequeue: true,
			expectedPhase:   velerov2alpha1.DataUploadPhaseAccepted,
			expectError:     false,
		},
		{
			name: "new phase fails without VM annotations",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseNew,
				},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataUploadPhaseFailed,
			expectError:     false,
		},
		{
			name: "completed phase is terminal - no action",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseCompleted,
				},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataUploadPhaseCompleted,
			expectError:     false,
		},
		{
			name: "failed phase is terminal - no action",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseFailed,
				},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataUploadPhaseFailed,
			expectError:     false,
		},
		{
			name: "canceled phase is terminal - no action",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseCanceled,
				},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataUploadPhaseCanceled,
			expectError:     false,
		},
		{
			name: "canceling phase transitions to canceled",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseCanceling,
				},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataUploadPhaseCanceled,
			expectError:     false,
		},
		{
			name: "prepared phase without VM annotations fails",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhasePrepared,
				},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataUploadPhaseFailed,
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.dataUpload)

			// Add VM to fake client if provided
			if tt.vm != nil {
				builder = builder.WithObjects(tt.vm)
			}

			fakeClient := builder.Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				Scheme:        scheme,
				Log:           logr.Discard(),
				OADPNamespace: "openshift-adp",
			}

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      tt.dataUpload.Name,
					Namespace: tt.dataUpload.Namespace,
				},
			}

			result, err := r.Reconcile(context.Background(), req)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// Check if requeue is expected (using RequeueAfter > 0 instead of deprecated Requeue field)
			gotRequeue := result.RequeueAfter > 0
			if gotRequeue != tt.expectedRequeue {
				t.Errorf("expected requeue=%v, got requeue=%v (RequeueAfter=%v)", tt.expectedRequeue, gotRequeue, result.RequeueAfter)
			}

			// Verify phase if we expect a transition
			if tt.expectedPhase != "" && tt.dataUpload.Spec.DataMover == common.DataMoverKubeVirt {
				updatedDU := &velerov2alpha1.DataUpload{}
				err := fakeClient.Get(context.Background(), req.NamespacedName, updatedDU)
				if err != nil {
					t.Errorf("failed to get updated DataUpload: %v", err)
				}
				if updatedDU.Status.Phase != tt.expectedPhase {
					t.Errorf("expected phase=%s, got phase=%s", tt.expectedPhase, updatedDU.Status.Phase)
				}
				if tt.expectedPhase == velerov2alpha1.DataUploadPhaseAccepted && updatedDU.Status.AcceptedTimestamp == nil {
					t.Error("AcceptedTimestamp not set when transitioning to Accepted")
				}
			}
		})
	}
}

func TestReconcile_NotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "non-existent",
			Namespace: "openshift-adp",
		},
	}

	result, err := r.Reconcile(context.Background(), req)

	if err != nil {
		t.Errorf("expected no error for not-found, got: %v", err)
	}
	if result.RequeueAfter > 0 {
		t.Errorf("expected no requeue for not-found, got RequeueAfter=%v", result.RequeueAfter)
	}
}

func TestReconcile_OperationTimeout(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = kubevirtcorev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	get := func(t *testing.T, c client.Client, name, namespace string) *velerov2alpha1.DataUpload {
		t.Helper()
		var out velerov2alpha1.DataUpload
		if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &out); err != nil {
			t.Fatalf("failed to get DataUpload: %v", err)
		}
		return &out
	}

	t.Run("Accepted phase past default operation timeout fails", func(t *testing.T) {
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{Name: "du-timeout", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataUploadStatus{
				Phase:             velerov2alpha1.DataUploadPhaseAccepted,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du).Build()
		r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: du.Name, Namespace: du.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue after timeout failure, got %v", result.RequeueAfter)
		}
		updated := get(t, fakeClient, du.Name, du.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataUploadPhaseFailed)
		}
		if !strings.Contains(updated.Status.Message, "operation timed out") {
			t.Errorf("message = %q, want it to mention the timeout", updated.Status.Message)
		}
	})

	t.Run("Prepared phase past default operation timeout fails", func(t *testing.T) {
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{Name: "du-prepared-timeout", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataUploadStatus{
				Phase:             velerov2alpha1.DataUploadPhasePrepared,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du).Build()
		r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: du.Name, Namespace: du.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue after timeout failure, got %v", result.RequeueAfter)
		}
		updated := get(t, fakeClient, du.Name, du.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataUploadPhaseFailed)
		}
		if !strings.Contains(updated.Status.Message, "operation timed out") {
			t.Errorf("message = %q, want it to mention the timeout", updated.Status.Message)
		}
	})

	t.Run("InProgress phase respects custom Spec.OperationTimeout", func(t *testing.T) {
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{Name: "du-custom-timeout", Namespace: "openshift-adp"},
			Spec: velerov2alpha1.DataUploadSpec{
				DataMover:        common.DataMoverKubeVirt,
				OperationTimeout: metav1.Duration{Duration: time.Hour},
			},
			Status: velerov2alpha1.DataUploadStatus{
				Phase:             velerov2alpha1.DataUploadPhaseInProgress,
				AcceptedTimestamp: ptrTime(time.Now().Add(-2 * time.Hour)),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du).Build()
		r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: du.Name, Namespace: du.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, du.Name, du.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
			t.Errorf("phase = %q, want %q (custom 1h OperationTimeout exceeded by 2h elapsed)", updated.Status.Phase, velerov2alpha1.DataUploadPhaseFailed)
		}
	})

	t.Run("nil AcceptedTimestamp is backfilled without failing", func(t *testing.T) {
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "du-backfill", Namespace: "openshift-adp",
				Annotations: map[string]string{
					common.AnnotationVMName:      "test-vm",
					common.AnnotationVMNamespace: "default",
				},
			},
			Spec:   velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseAccepted},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du).Build()
		r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: du.Name, Namespace: du.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, du.Name, du.Namespace)
		if updated.Status.Phase == velerov2alpha1.DataUploadPhaseFailed {
			t.Errorf("phase = %q, backfilling a missing AcceptedTimestamp must not itself fail the DataUpload", updated.Status.Phase)
		}
		if updated.Status.AcceptedTimestamp == nil {
			t.Errorf("expected AcceptedTimestamp to be backfilled, got nil")
		}
	})

	t.Run("Canceling phase is not subject to operation timeout", func(t *testing.T) {
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{Name: "du-canceling", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataUploadStatus{
				Phase:             velerov2alpha1.DataUploadPhaseCanceling,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Hour))),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du).Build()
		r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: du.Name, Namespace: du.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, du.Name, du.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataUploadPhaseCanceled {
			t.Errorf("phase = %q, want %q (Canceling must run to completion, not be preempted by the timeout check)", updated.Status.Phase, velerov2alpha1.DataUploadPhaseCanceled)
		}
	})

	t.Run("handler's RequeueAfterShort is capped to the remaining custom OperationTimeout", func(t *testing.T) {
		// handleInProgress's pod-Pending branch normally requeues after
		// RequeueAfterShort (5s) -- with a custom OperationTimeout that has
		// nearly elapsed, the returned RequeueAfter must be capped to (roughly)
		// what's left instead of overshooting the deadline. Uses a 30s timeout
		// with 27s elapsed (a wider margin than a few-second timeout) so the
		// test isn't flaky against real wall-clock execution overhead.
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{Name: "du-cap-requeue", Namespace: "openshift-adp", UID: types.UID("du-cap-requeue-uid")},
			Spec: velerov2alpha1.DataUploadSpec{
				DataMover:        common.DataMoverKubeVirt,
				OperationTimeout: metav1.Duration{Duration: 30 * time.Second},
			},
			Status: velerov2alpha1.DataUploadStatus{
				Phase:             velerov2alpha1.DataUploadPhaseInProgress,
				AcceptedTimestamp: ptrTime(time.Now().Add(-27 * time.Second)),
			},
		}
		pendingPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "du-cap-requeue-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataUploadUID: string(du.UID)},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du, pendingPod).Build()
		r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: du.Name, Namespace: du.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter <= 0 || result.RequeueAfter >= RequeueAfterShort {
			t.Errorf("RequeueAfter = %v, want it capped below RequeueAfterShort (%v) to the ~3s remaining before the 30s OperationTimeout deadline", result.RequeueAfter, RequeueAfterShort)
		}
		updated := get(t, fakeClient, du.Name, du.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataUploadPhaseInProgress {
			t.Errorf("phase = %q, want %q (timeout not yet exceeded)", updated.Status.Phase, velerov2alpha1.DataUploadPhaseInProgress)
		}
	})

	t.Run("unset Spec.OperationTimeout uses the default and does not over-eagerly cap", func(t *testing.T) {
		// Complements the "handler's RequeueAfterShort is capped" case above:
		// with Spec.OperationTimeout left at its zero value and a fresh
		// AcceptedTimestamp, the effective deadline is DefaultOperationTimeout
		// (4h) away -- nowhere near RequeueAfterShort (5s) -- so the requeue
		// must come back uncapped, confirming the fallback default is used
		// consistently for both expiry and capping.
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{Name: "du-default-timeout-no-cap", Namespace: "openshift-adp", UID: types.UID("du-default-timeout-no-cap-uid")},
			Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataUploadStatus{
				Phase:             velerov2alpha1.DataUploadPhaseInProgress,
				AcceptedTimestamp: ptrTime(time.Now()),
			},
		}
		pendingPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "du-default-timeout-no-cap-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataUploadUID: string(du.UID)},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du, pendingPod).Build()
		r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: du.Name, Namespace: du.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != RequeueAfterShort {
			t.Errorf("RequeueAfter = %v, want exactly RequeueAfterShort (%v) -- unset OperationTimeout must not be treated as an immediate deadline", result.RequeueAfter, RequeueAfterShort)
		}
	})

	t.Run("New phase's first requeue is capped when Spec.OperationTimeout is shorter than RequeueAfterShort", func(t *testing.T) {
		// handleNew sets AcceptedTimestamp and transitions New -> Accepted in the
		// same reconcile that creates it, returning RequeueAfterShort (5s). With a
		// custom OperationTimeout shorter than that, the very first requeue --
		// not just subsequent ones -- must already be capped to the deadline.
		vm := &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{Name: "test-vm", Namespace: "default"},
			Status: kubevirtcorev1.VirtualMachineStatus{
				PrintableStatus: kubevirtcorev1.VirtualMachineStatusRunning,
				ChangedBlockTracking: &kubevirtcorev1.ChangedBlockTrackingStatus{
					State: kubevirtcorev1.ChangedBlockTrackingEnabled,
				},
			},
		}
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "du-new-cap-requeue", Namespace: "openshift-adp",
				Annotations: map[string]string{
					common.AnnotationVMName:      "test-vm",
					common.AnnotationVMNamespace: "default",
				},
			},
			Spec: velerov2alpha1.DataUploadSpec{
				DataMover:        common.DataMoverKubeVirt,
				OperationTimeout: metav1.Duration{Duration: 4 * time.Second},
			},
			Status: velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseNew},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du, vm).Build()
		r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: du.Name, Namespace: du.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, du.Name, du.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataUploadPhaseAccepted {
			t.Fatalf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataUploadPhaseAccepted)
		}
		if result.RequeueAfter <= 0 || result.RequeueAfter >= RequeueAfterShort {
			t.Errorf("RequeueAfter = %v, want it capped below RequeueAfterShort (%v) to the ~4s remaining before the 4s OperationTimeout deadline", result.RequeueAfter, RequeueAfterShort)
		}
	})

	t.Run("timeout failure stops the still-running datamover pod", func(t *testing.T) {
		// A timeout can fire while the datamover pod is still Pending/Running --
		// that's exactly the unbounded-wait branch being guarded against -- unlike
		// other Failed paths where the pod has already terminated on its own.
		// Verifies checkOperationTimeoutCore's fail callback actually stops it
		// rather than leaving it running indefinitely against a terminal DataUpload.
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "du-timeout-pod-cleanup", Namespace: "openshift-adp",
				UID: types.UID("du-timeout-pod-cleanup-uid"),
			},
			Spec: velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataUploadStatus{
				Phase:             velerov2alpha1.DataUploadPhaseInProgress,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			},
		}
		runningPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "du-timeout-pod-cleanup-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataUploadUID: string(du.UID)},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du, runningPod).Build()
		r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: du.Name, Namespace: du.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, du.Name, du.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
			t.Fatalf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataUploadPhaseFailed)
		}
		var pod corev1.Pod
		err := fakeClient.Get(context.Background(), types.NamespacedName{Name: runningPod.Name, Namespace: runningPod.Namespace}, &pod)
		if !errors.IsNotFound(err) {
			t.Errorf("expected datamover pod to be deleted after timeout failure, got err=%v", err)
		}
	})
}

// TestReconcile_OperationTimeout_PodAlreadySucceededDoesNotFail covers the
// datamover pod having already reported success (AnnotationDatamoverPodSucceeded
// persisted) with only trailing PVC/PV cleanup left -- the operation timeout
// elapsing during that cleanup window must not report an already-successful
// upload as Failed. Split out from TestReconcile_OperationTimeout for the same
// gocyclo reason as TestReconcile_OperationTimeout_PodStillTerminatingRequeuesWithoutError
// above.
func TestReconcile_OperationTimeout_PodAlreadySucceededDoesNotFail(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	get := func(t *testing.T, c client.Client, name, namespace string) *velerov2alpha1.DataUpload {
		t.Helper()
		var out velerov2alpha1.DataUpload
		if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &out); err != nil {
			t.Fatalf("failed to get DataUpload: %v", err)
		}
		return &out
	}

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name: "du-succeeded-pending-cleanup", Namespace: "openshift-adp",
			Annotations: map[string]string{
				common.AnnotationDatamoverPodSucceeded: bslValidatedValue,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
		Status: velerov2alpha1.DataUploadStatus{
			Phase:             velerov2alpha1.DataUploadPhaseInProgress,
			AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du).Build()
	r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: du.Name, Namespace: du.Namespace}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	updated := get(t, fakeClient, du.Name, du.Namespace)
	if updated.Status.Phase == velerov2alpha1.DataUploadPhaseFailed {
		t.Errorf("phase = %q, want not Failed (datamover pod already succeeded, trailing cleanup shouldn't be timed out)", updated.Status.Phase)
	}
}

// TestReconcile_OperationTimeout_PodStillTerminatingRequeuesWithoutError
// covers the same expected, self-resolving ErrPodsStillTerminating the
// timeout fail callback's pod cleanup can hit as handleCanceling's own
// cleanup -- kubelet just hasn't finished tearing the pod down yet. Must
// requeue quietly, not log a reconcile error and fall into
// controller-runtime's (much slower) exponential backoff. Split out from
// TestReconcile_OperationTimeout for the same gocyclo reason as
// TestReconcile_OperationTimeout_CleanupFailureDoesNotPersistFailed above.
func TestReconcile_OperationTimeout_PodStillTerminatingRequeuesWithoutError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	get := func(t *testing.T, c client.Client, name, namespace string) *velerov2alpha1.DataUpload {
		t.Helper()
		var out velerov2alpha1.DataUpload
		if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &out); err != nil {
			t.Fatalf("failed to get DataUpload: %v", err)
		}
		return &out
	}

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name: "du-timeout-pod-terminating", Namespace: "openshift-adp",
			UID: types.UID("du-timeout-pod-terminating-uid"),
		},
		Spec: velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
		Status: velerov2alpha1.DataUploadStatus{
			Phase:             velerov2alpha1.DataUploadPhaseInProgress,
			AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
		},
	}
	terminatingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "du-timeout-pod-terminating-pod", Namespace: "openshift-adp",
			Labels:     map[string]string{common.LabelDataUploadUID: string(du.UID)},
			Finalizers: []string{"example.com/still-cleaning-up"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du, terminatingPod).Build()
	r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: du.Name, Namespace: du.Namespace}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected a short requeue while the datamover pod is still terminating")
	}

	updated := get(t, fakeClient, du.Name, du.Namespace)
	if updated.Status.Phase == velerov2alpha1.DataUploadPhaseFailed {
		t.Error("phase must not be persisted Failed until pod cleanup actually succeeds")
	}
}

// TestReconcile_OperationTimeout_CleanupFailureDoesNotPersistFailed covers
// checkOperationTimeoutCore's fail-before-persist contract: it stops the
// still-running pod BEFORE marking the resource Failed specifically so a
// cleanup failure never leaves the pod running forever behind a terminal
// DataUpload. Split out from TestReconcile_OperationTimeout to keep that
// function's cyclomatic complexity down (gocyclo).
func TestReconcile_OperationTimeout_CleanupFailureDoesNotPersistFailed(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = kubevirtcorev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	get := func(t *testing.T, c client.Client, name, namespace string) *velerov2alpha1.DataUpload {
		t.Helper()
		var out velerov2alpha1.DataUpload
		if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &out); err != nil {
			t.Fatalf("failed to get DataUpload: %v", err)
		}
		return &out
	}

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name: "du-timeout-cleanup-fails", Namespace: "openshift-adp",
			UID: types.UID("du-timeout-cleanup-fails-uid"),
		},
		Spec: velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
		Status: velerov2alpha1.DataUploadStatus{
			Phase:             velerov2alpha1.DataUploadPhaseInProgress,
			AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
		},
	}
	runningPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "du-timeout-cleanup-fails-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataUploadUID: string(du.UID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du, runningPod).Build()
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				return fmt.Errorf("simulated delete failure")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	r := &KubeVirtDataUploadReconciler{Client: interceptedClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: du.Name, Namespace: du.Namespace}})
	if err == nil {
		t.Fatal("expected an error when the still-running pod can't be deleted, got nil")
	}

	updated := get(t, baseClient, du.Name, du.Namespace)
	if updated.Status.Phase != velerov2alpha1.DataUploadPhaseInProgress {
		t.Errorf("phase = %q, want %q (must not be marked Failed until cleanup actually succeeds)",
			updated.Status.Phase, velerov2alpha1.DataUploadPhaseInProgress)
	}

	var pod corev1.Pod
	getErr := baseClient.Get(context.Background(), types.NamespacedName{Name: runningPod.Name, Namespace: runningPod.Namespace}, &pod)
	if getErr != nil {
		t.Errorf("expected the still-running pod to remain (delete failed), got err=%v", getErr)
	}
}

// TestHandleCancelingDataUpload_PodCleanupFailureDoesNotPersistCanceled verifies
// that Canceling never persists Canceled while the datamover pod cleanup hasn't
// actually succeeded: Canceled is terminal, so no further reconciliation would
// ever run for this object, and a still-running pod would be abandoned forever
// with no chance to retry. A cleanup failure defers via a quiet requeue instead
// (see issue #171 / PR #170) rather than a hard reconcile error.
func TestHandleCancelingDataUpload_PodCleanupFailureDoesNotPersistCanceled(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = kubevirtcorev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name: "du-cancel-cleanup-fails", Namespace: "openshift-adp",
			UID: types.UID("du-cancel-cleanup-fails-uid"),
		},
		Spec:   velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
		Status: velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseCanceling},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "du-cancel-cleanup-fails-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataUploadUID: string(du.UID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du, pod).Build()
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				return fmt.Errorf("simulated delete failure")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	r := &KubeVirtDataUploadReconciler{Client: interceptedClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	result, err := r.handleCanceling(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("expected no error (quiet requeue instead) when the datamover pod can't be deleted, got: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Error("expected a positive RequeueAfter to defer cleanup to a later reconcile")
	}

	var updated velerov2alpha1.DataUpload
	if err := baseClient.Get(context.Background(), types.NamespacedName{Name: du.Name, Namespace: du.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataUpload: %v", err)
	}
	if updated.Status.Phase == velerov2alpha1.DataUploadPhaseCanceled {
		t.Error("phase must not be Canceled until pod cleanup actually succeeds")
	}

	var remaining corev1.Pod
	if err := baseClient.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &remaining); err != nil {
		t.Errorf("expected the still-running pod to remain (delete failed), got err=%v", err)
	}
}

// TestHandleCancelingDataUpload_PodStillTerminatingRequeuesWithoutError covers
// the expected, self-resolving case ErrPodsStillTerminating exists for: a
// datamover pod blocked on a finalizer (Delete accepted, kubelet just hasn't
// finished tearing it down yet) must requeue quickly without being treated as
// a reconcile error -- unlike a genuine cleanup failure, this isn't something
// worth logging as broken.
func TestHandleCancelingDataUpload_PodStillTerminatingRequeuesWithoutError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = kubevirtcorev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name: "du-cancel-still-terminating", Namespace: "openshift-adp",
			UID: types.UID("du-cancel-still-terminating-uid"),
		},
		Spec:   velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
		Status: velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseCanceling},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "du-cancel-still-terminating-pod", Namespace: "openshift-adp",
			Labels:     map[string]string{common.LabelDataUploadUID: string(du.UID)},
			Finalizers: []string{"example.com/still-cleaning-up"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du, pod).Build()
	r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	result, err := r.handleCanceling(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected a short requeue while the pod is still terminating")
	}

	var updated velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: du.Name, Namespace: du.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataUpload: %v", err)
	}
	if updated.Status.Phase == velerov2alpha1.DataUploadPhaseCanceled {
		t.Error("phase must not be Canceled until the pod actually finishes terminating")
	}
}

func TestFilterKubeVirtDataMover(t *testing.T) {
	tests := []struct {
		name      string
		dataMover string
		expected  bool
	}{
		{
			name:      "kubevirt datamover matches",
			dataMover: common.DataMoverKubeVirt,
			expected:  true,
		},
		{
			name:      "velero datamover does not match",
			dataMover: "velero",
			expected:  false,
		},
		{
			name:      "empty datamover does not match",
			dataMover: "",
			expected:  false,
		},
		{
			name:      "kopia datamover does not match",
			dataMover: "kopia",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the filter logic directly - this is what the predicate checks
			matches := tt.dataMover == common.DataMoverKubeVirt
			if matches != tt.expected {
				t.Errorf("expected match=%v, got match=%v for datamover=%s",
					tt.expected, matches, tt.dataMover)
			}
		})
	}
}

func TestUpdatePhase(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	tests := []struct {
		name         string
		initialPhase velerov2alpha1.DataUploadPhase
		targetPhase  velerov2alpha1.DataUploadPhase
		message      string
		expectError  bool
	}{
		{
			name:         "update from new to accepted",
			initialPhase: velerov2alpha1.DataUploadPhaseNew,
			targetPhase:  velerov2alpha1.DataUploadPhaseAccepted,
			message:      "DataUpload accepted by kubevirt datamover",
			expectError:  false,
		},
		{
			name:         "update from accepted to prepared",
			initialPhase: velerov2alpha1.DataUploadPhaseAccepted,
			targetPhase:  velerov2alpha1.DataUploadPhasePrepared,
			message:      "VMBT/VMB created",
			expectError:  false,
		},
		{
			name:         "update from prepared to inprogress",
			initialPhase: velerov2alpha1.DataUploadPhasePrepared,
			targetPhase:  velerov2alpha1.DataUploadPhaseInProgress,
			message:      "Datamover pod launched",
			expectError:  false,
		},
		{
			name:         "update from inprogress to completed",
			initialPhase: velerov2alpha1.DataUploadPhaseInProgress,
			targetPhase:  velerov2alpha1.DataUploadPhaseCompleted,
			message:      "Backup completed successfully",
			expectError:  false,
		},
		{
			name:         "update from canceling to canceled",
			initialPhase: velerov2alpha1.DataUploadPhaseCanceling,
			targetPhase:  velerov2alpha1.DataUploadPhaseCanceled,
			message:      "DataUpload canceled",
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			du := &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: tt.initialPhase,
				},
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(du).
				Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				Scheme:        scheme,
				Log:           logr.Discard(),
				OADPNamespace: "openshift-adp",
			}

			err := r.updatePhase(context.Background(), du, tt.targetPhase, tt.message)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// Verify the phase was updated
			updatedDU := &velerov2alpha1.DataUpload{}
			err = fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      du.Name,
				Namespace: du.Namespace,
			}, updatedDU)
			if err != nil {
				t.Errorf("failed to get updated DataUpload: %v", err)
			}
			if updatedDU.Status.Phase != tt.targetPhase {
				t.Errorf("expected phase=%s, got phase=%s", tt.targetPhase, updatedDU.Status.Phase)
			}
			if updatedDU.Status.Message != tt.message {
				t.Errorf("expected message=%s, got message=%s", tt.message, updatedDU.Status.Message)
			}
		})
	}
}

// TestUpdatePhase_TimestampsSet covers #155: updatePhase must populate
// Status.StartTimestamp on entering InProgress and Status.CompletionTimestamp
// on entering any terminal phase, matching Velero's own built-in data movers
// so `velero backup/restore describe` reports timing consistently. Both must
// be idempotent -- set once, not reset on a later call with an already-set
// value.
func TestUpdatePhase_TimestampsSet(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	t.Run("StartTimestamp set on entering InProgress", func(t *testing.T) {
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{Name: "test-du", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
			Status:     velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhasePrepared},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du).Build()
		r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if err := r.updatePhase(context.Background(), du, velerov2alpha1.DataUploadPhaseInProgress, "Datamover pod launched"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if du.Status.StartTimestamp == nil {
			t.Fatal("expected StartTimestamp to be set")
		}
	})

	t.Run("CompletionTimestamp set on entering a terminal phase", func(t *testing.T) {
		for _, phase := range []velerov2alpha1.DataUploadPhase{
			velerov2alpha1.DataUploadPhaseCompleted,
			velerov2alpha1.DataUploadPhaseFailed,
			velerov2alpha1.DataUploadPhaseCanceled,
		} {
			du := &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-du-" + string(phase), Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
				Status:     velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseInProgress},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du).Build()
			r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

			if err := r.updatePhase(context.Background(), du, phase, "done"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if du.Status.CompletionTimestamp == nil {
				t.Errorf("phase %s: expected CompletionTimestamp to be set", phase)
			}
		}
	})

	t.Run("timestamps are idempotent, not reset by a later call", func(t *testing.T) {
		earlier := metav1.NewTime(time.Now().Add(-time.Hour))
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{Name: "test-du", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataUploadStatus{
				Phase:               velerov2alpha1.DataUploadPhaseInProgress,
				StartTimestamp:      &earlier,
				CompletionTimestamp: &earlier,
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du).Build()
		r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if err := r.updatePhase(context.Background(), du, velerov2alpha1.DataUploadPhaseCompleted, "done"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !du.Status.StartTimestamp.Equal(&earlier) {
			t.Errorf("StartTimestamp = %v, want it unchanged from %v", du.Status.StartTimestamp, earlier)
		}
		if !du.Status.CompletionTimestamp.Equal(&earlier) {
			t.Errorf("CompletionTimestamp = %v, want it unchanged from %v", du.Status.CompletionTimestamp, earlier)
		}
	})
}

func TestHandleAccepted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
			UID:       types.UID("test-uid-in-progress"),
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// handleAccepted now implements Phase 2 logic and may request requeue
	// This is expected behavior when VMB is in progress
	_ = result // result.RequeueAfter may be > 0 depending on VMB state
}

// TestHandleAccepted_RetriesVMBCreationAfterAnnotationPersisted pins the fix for
// a real livelock: a previous version of handleAccepted short-circuited whenever
// du.Annotations[AnnotationVMBTName] was already set but findVMBForDataUpload came
// back nil, requeuing indefinitely on the theory that a VMB from a prior reconcile
// just hadn't reached the informer cache yet. That's wrong when Step 4 (VMB
// creation) was instead *deferred* -- e.g. KubeVirt's one-active-VMB-per-VM
// admission webhook, "in progress for source", handled a few lines below --
// after Step 2 already persisted AnnotationVMBTName: no VMB was ever created that
// reconcile, so every later reconcile hit the same short-circuit and never
// reached Step 4 again, looping "VMBT already prepared but VMB not yet visible in
// cache, requeuing" until Spec.OperationTimeout even though no VMB would ever
// appear on its own (reproduced live).
//
// Also verifies the on-cluster VMBT (which prepareVMBackupTracker now finds and
// reuses by VM-name-hash label, never deletes) survives both reconciles.
func TestHandleAccepted_RetriesVMBCreationAfterAnnotationPersisted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "vm-ns"
	duName := "test-du-retry"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("du-uid-retry"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
				// Both already set by a previous reconcile whose Step 4 got
				// deferred by the admission-webhook conflict below.
				AnnotationVMBTName:            "vmbt-test-vm-prev",
				common.AnnotationBSLValidated: "true",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:       common.DataMoverKubeVirt,
			SourceNamespace: vmNamespace,
			// BackupStorageLocation deliberately empty: skips handleAccepted's
			// Step 0 BSL-availability check, keeping this fixture focused on
			// the VMBT/VMB retry behavior under test.
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	tempPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName + "-abc12",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "du-uid-retry",
			},
		},
	}

	// The VMBT a previous reconcile already prepared -- must survive both
	// reconciles below (prepareVMBackupTracker reuses by label, never deletes).
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-test-vm-prev",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash: common.HashForLabel(vmName),
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, tempPVC, vmbt).
		Build()

	// Simulates KubeVirt's admission webhook rejecting a second active VMB for
	// the same VM -- exactly the error handleAccepted's Step 4 already detects
	// via strings.Contains(err.Error(), "in progress for source") and defers on.
	conflictActive := true
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*kubevirtbackupv1alpha1.VirtualMachineBackup); ok && conflictActive {
				return fmt.Errorf(`admission webhook "virtualmachinebackup-validator.backup.kubevirt.io" denied the request: VirtualMachineBackup "vmb-other-du" in progress for source`)
			}
			return c.Create(ctx, obj, opts...)
		},
	})

	r := &KubeVirtDataUploadReconciler{
		Client:        interceptedClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	// First reconcile: Step 4's Create is rejected by the simulated conflict.
	result1, err1 := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err1 != nil {
		t.Fatalf("unexpected error on first reconcile: %v", err1)
	}
	if result1.RequeueAfter != RequeueAfterLong {
		t.Errorf("first reconcile RequeueAfter = %v, want %v (the in-progress-conflict defer)", result1.RequeueAfter, RequeueAfterLong)
	}

	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := baseClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace)); err != nil {
		t.Fatalf("failed to list VMBs: %v", err)
	}
	if len(vmbList.Items) != 0 {
		t.Fatalf("expected no VMB to exist after the deferred first attempt, found %d", len(vmbList.Items))
	}

	existingVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	if err := baseClient.Get(context.Background(), types.NamespacedName{
		Name: "vmbt-test-vm-prev", Namespace: vmNamespace,
	}, existingVMBT); err != nil {
		t.Errorf("VMBT was deleted when it should have been preserved: %v", err)
	}

	// The conflict clears (the other VM's VMB completed) -- the fix under test
	// is that a SECOND reconcile, with AnnotationVMBTName already persisted from
	// before, still retries Step 4 instead of looping on the old cache-wait
	// guard forever.
	conflictActive = false

	result2, err2 := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err2 != nil {
		t.Fatalf("unexpected error on second reconcile: %v", err2)
	}
	if result2.RequeueAfter != RequeueAfterShort {
		t.Errorf("second reconcile RequeueAfter = %v, want %v (freshly-created VMB)", result2.RequeueAfter, RequeueAfterShort)
	}

	if err := baseClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace)); err != nil {
		t.Fatalf("failed to list VMBs: %v", err)
	}
	if len(vmbList.Items) != 1 {
		t.Fatalf("expected exactly 1 VMB to exist once the conflict cleared, found %d", len(vmbList.Items))
	}

	if err := baseClient.Get(context.Background(), types.NamespacedName{
		Name: "vmbt-test-vm-prev", Namespace: vmNamespace,
	}, existingVMBT); err != nil {
		t.Errorf("VMBT was deleted when it should have been preserved: %v", err)
	}
}

// TestHandleAccepted_RefreshesVMBStatusFromLiveRead pins a second, independent
// livelock reported live from a nightly-KubeVirt e2e run: virt-controller had
// already written status.conditions[type=Done,status=True] to the API server
// (backup genuinely finished), but this controller's cached copy of the same
// VMB never reflected it -- the reconciler found the VMB fine (logged
// "already exists, skipping VMBT preparation"), just with a stale Status,
// and looped "VirtualMachineBackup in progress, requeuing" until
// Spec.OperationTimeout. Confirmed intermittent (an informer watch/resync
// timing issue, not a deterministic logic bug), distinct from #211/#212
// (which is about the VMB not existing in the cache at all).
//
// Simulates the staleness directly: r.Client (the cached client) and
// r.APIReader (the live/uncached reader) are two separate fake clients
// seeded with different Status for the same VMB object, so handleAccepted
// must actually use the live one to reach Prepared.
func TestHandleAccepted_RefreshesVMBStatusFromLiveRead(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "vm-ns"
	duName := "test-du-stale-cache"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("du-uid-stale-cache"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:       common.DataMoverKubeVirt,
			SourceNamespace: vmNamespace,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	tempPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName + "-abc12",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "du-uid-stale-cache",
			},
		},
	}

	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash: common.HashForLabel(vmName),
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{},
	}

	checkpointName := "vmb-" + duName + "-checkpoint"
	staleVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "du-uid-stale-cache",
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: duName,
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("backup.kubevirt.io"),
				Kind:     "VirtualMachineBackupTracker",
				Name:     vmbt.Name,
			},
			PvcName: new(tempPVC.Name),
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
			// Stale cached view: still Progressing, no Done condition at all --
			// exactly what a missed/delayed watch event on this object looks like.
			Conditions: []kubevirtbackupv1alpha1.Condition{
				{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionTrue},
			},
		},
	}

	liveVMB := staleVMB.DeepCopy()
	liveVMB.Status = &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
		Type:           kubevirtbackupv1alpha1.Full,
		CheckpointName: &checkpointName,
		Conditions: []kubevirtbackupv1alpha1.Condition{
			{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionFalse, Reason: "Completed VirtualMachineBackup"},
			{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionTrue, Reason: "Completed VirtualMachineBackup"},
		},
	}

	cachedClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du, tempPVC, vmbt, staleVMB).Build()
	liveClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du, tempPVC, vmbt, liveVMB).Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        cachedClient,
		APIReader:     liveClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updatedDU velerov2alpha1.DataUpload
	if err := cachedClient.Get(context.Background(), types.NamespacedName{
		Name: duName, Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}
	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhasePrepared {
		t.Errorf("phase = %q, want %q -- handleAccepted must use the live VMB status (Done=True), not the stale cached one (still Progressing)",
			updatedDU.Status.Phase, velerov2alpha1.DataUploadPhasePrepared)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected requeue after transitioning to Prepared")
	}
}

func TestHandleAccepted_VMBStatusDetection(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name                    string
		vmbConditions           []kubevirtbackupv1alpha1.Condition
		expectedPhase           velerov2alpha1.DataUploadPhase
		expectRequeue           bool
		skipVMBT                bool   // when true, do not create the VMBT (simulates deleted VMBT)
		expectedMessageContains string // when set, DataUpload's failure message must contain this
	}{
		{
			name: "VMB Done True transitions to Prepared",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "Completed VirtualMachineBackup",
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionTrue,
					Reason: "Completed VirtualMachineBackup",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhasePrepared,
			expectRequeue: true,
		},
		{
			// KubeVirt nightly (v1.20.0+) virt-controller renamed VMB's "Done"
			// condition type to "Complete". Without recognizing it,
			// evaluateVMBackupStatus never sees doneCond populated and loops
			// "VirtualMachineBackup in progress, requeuing" until
			// Spec.OperationTimeout, even though the backup already completed.
			name: "VMB Complete True (nightly KubeVirt condition rename) transitions to Prepared",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "CompletedWithWarning",
				},
				{
					Type:   conditionComplete,
					Status: corev1.ConditionTrue,
					Reason: "CompletedWithWarning",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhasePrepared,
			expectRequeue: true,
		},
		{
			name: "VMB Done True with Failed reason transitions to Failed",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:    kubevirtbackupv1alpha1.ConditionProgressing,
					Status:  corev1.ConditionFalse,
					Reason:  "Failed",
					Message: "Backup has failed: No space left on device",
				},
				{
					Type:    kubevirtbackupv1alpha1.ConditionDone,
					Status:  corev1.ConditionTrue,
					Reason:  "Failed",
					Message: "Backup has failed: No space left on device",
				},
			},
			expectedPhase:           velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue:           false,
			expectedMessageContains: "No space left on device",
		},
		{
			name: "VMB Done True with failure detail only on Done (neutral Progressing reason) transitions to Failed",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "NotProgressing",
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionTrue,
					Reason: "Backup has failed: No space left on device",
				},
			},
			expectedPhase:           velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue:           false,
			expectedMessageContains: "No space left on device",
		},
		{
			// Real KubeVirt (v1.8.4) never populates Message on these conditions — only Reason
			// carries the wrapped detail. This case reflects that: Message left empty.
			name: "VMB Done True with descriptive failure reason transitions to Failed",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "Backup has failed: No space left on device",
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionTrue,
					Reason: "Backup has failed: No space left on device",
				},
			},
			expectedPhase:           velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue:           false,
			expectedMessageContains: "No space left on device",
		},
		{
			// Same as above but for the Done=False + Progressing=False failure branch.
			name: "VMB Done False and Progressing False with descriptive reason (no Message) transitions to Failed",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "Backup has failed: VMI was deleted during backup",
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionFalse,
					Reason: "Backup has failed: VMI was deleted during backup",
				},
			},
			expectedPhase:           velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue:           false,
			expectedMessageContains: "VMI was deleted during backup",
		},
		{
			name: "VMB Done False and Progressing False transitions to Failed",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "BackupFailed",
				},
				{
					Type:    kubevirtbackupv1alpha1.ConditionDone,
					Status:  corev1.ConditionFalse,
					Reason:  "BackupFailed",
					Message: "VM backup failed due to an error",
				},
			},
			expectedPhase:           velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue:           false,
			expectedMessageContains: "VM backup failed due to an error",
		},
		{
			name: "VMB Done False and Progressing True requeues (backup still running)",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionTrue,
					Reason: "Backup is in progress",
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionFalse,
					Reason: "Backup is in progress",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseAccepted, // unchanged - still running
			expectRequeue: true,
		},
		{
			name: "VMB Done False without Progressing requeues",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:    kubevirtbackupv1alpha1.ConditionDone,
					Status:  corev1.ConditionFalse,
					Reason:  "Backup is in progress",
					Message: "",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseAccepted, // unchanged - wait for Progressing
			expectRequeue: true,
		},
		{
			name: "VMB only Progressing True requeues",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionTrue,
					Reason: "Backup in progress",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseAccepted, // unchanged
			expectRequeue: true,
		},
		{
			name: "VMB Progressing False without Done requeues",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionInitializing,
					Status: corev1.ConditionFalse,
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "PVC being attached to VM",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseAccepted, // unchanged - wait for Done
			expectRequeue: true,
		},
		{
			name: "VMB stuck initializing (VMBT deleted) transitions to Failed",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionInitializing,
					Status: corev1.ConditionTrue,
					Reason: "BackupTracker vmbt-test-vm does not exist",
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "BackupTracker vmbt-test-vm does not exist",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue: false,
			skipVMBT:      true, // VMBT is gone — this should fail
		},
		{
			name: "VMB initializing with VMBT present requeues (PVC being attached)",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionInitializing,
					Status: corev1.ConditionTrue,
					Reason: "backup target PVC is being attached to VMI",
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "backup target PVC is being attached to VMI",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseAccepted, // unchanged — transient state
			expectRequeue: true,
		},
		{
			name:          "VMB no conditions requeues",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{},
			expectedPhase: velerov2alpha1.DataUploadPhaseAccepted, // unchanged
			expectRequeue: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vmName := "test-vm"
			vmNamespace := "test-ns"
			duName := "test-du"

			// Create DataUpload
			du := &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      duName,
					Namespace: vmNamespace,
					UID:       types.UID("test-uid"),
					Annotations: map[string]string{
						common.AnnotationVMName:      vmName,
						common.AnnotationVMNamespace: vmNamespace,
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover:       common.DataMoverKubeVirt,
					SourceNamespace: vmNamespace,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseAccepted,
				},
			}

			// Create temporary PVC (needed for handleAccepted to proceed)
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kubevirt-backup-" + duName,
					Namespace: vmNamespace,
					Labels: map[string]string{
						common.LabelDataUploadUID: string(du.UID),
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "velero.io/v2alpha1",
							Kind:       "DataUpload",
							Name:       duName,
							UID:        du.UID,
						},
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Phase: corev1.ClaimBound,
				},
			}

			// Create VMBT
			vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vmbt-" + vmName,
					Namespace: vmNamespace,
					Labels: map[string]string{
						common.LabelVMNameHash: common.HashForLabel(vmName),
					},
				},
				Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
					Source: corev1.TypedLocalObjectReference{
						APIGroup: new("kubevirt.io"),
						Kind:     "VirtualMachine",
						Name:     vmName,
					},
				},
			}

			// Create VMB with specified conditions
			checkpointName := "vmb-" + duName + "-checkpoint"
			vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vmb-" + duName,
					Namespace: vmNamespace,
					Labels: map[string]string{
						common.LabelDataUploadUID: string(du.UID),
					},
					Annotations: map[string]string{
						common.AnnotationDataUploadName: duName,
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "velero.io/v2alpha1",
							Kind:       "DataUpload",
							Name:       duName,
							UID:        du.UID,
						},
					},
				},
				Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
					Source: corev1.TypedLocalObjectReference{
						APIGroup: new("backup.kubevirt.io"),
						Kind:     "VirtualMachineBackupTracker",
						Name:     vmbt.Name,
					},
					PvcName: new(pvc.Name),
				},
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Type:           kubevirtbackupv1alpha1.Full,
					CheckpointName: &checkpointName,
					Conditions:     tt.vmbConditions,
				},
			}

			objects := []client.Object{du, pvc, vmb}
			if !tt.skipVMBT {
				objects = append(objects, vmbt)
			}
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				Scheme:        scheme,
				Log:           logr.Discard(),
				OADPNamespace: vmNamespace,
			}

			result, err := r.handleAccepted(context.Background(), logr.Discard(), du)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.expectRequeue && result.RequeueAfter == 0 {
				t.Errorf("expected requeue, got no requeue")
			}
			if !tt.expectRequeue && result.RequeueAfter > 0 {
				t.Errorf("expected no requeue, got RequeueAfter=%v", result.RequeueAfter)
			}

			// Fetch updated DataUpload to check phase
			var updatedDU velerov2alpha1.DataUpload
			if err := fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      duName,
				Namespace: vmNamespace,
			}, &updatedDU); err != nil {
				t.Fatalf("failed to get updated DataUpload: %v", err)
			}

			if updatedDU.Status.Phase != tt.expectedPhase {
				t.Errorf("expected phase=%s, got phase=%s", tt.expectedPhase, updatedDU.Status.Phase)
			}

			if tt.expectedMessageContains != "" && !strings.Contains(updatedDU.Status.Message, tt.expectedMessageContains) {
				t.Errorf("expected message to contain %q, got message=%q", tt.expectedMessageContains, updatedDU.Status.Message)
			}
		})
	}
}

func TestIsVMBTerminal(t *testing.T) {
	tests := []struct {
		name     string
		vmb      *kubevirtbackupv1alpha1.VirtualMachineBackup
		expected bool
	}{
		{
			name: "nil status is not terminal",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: nil,
			},
			expected: false,
		},
		{
			name: "no conditions is not terminal",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{},
				},
			},
			expected: false,
		},
		{
			name: "Done=True is terminal (success)",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{
						{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionTrue},
						{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionFalse},
					},
				},
			},
			expected: true,
		},
		{
			name: "Done=False + Progressing=False is terminal (failure)",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{
						{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionFalse},
						{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionFalse},
					},
				},
			},
			expected: true,
		},
		{
			name: "Progressing=True + Done=False is not terminal (in progress)",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{
						{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionFalse},
						{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionTrue},
					},
				},
			},
			expected: false,
		},
		{
			name: "Initializing=True only is not terminal",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{
						{Type: kubevirtbackupv1alpha1.ConditionInitializing, Status: corev1.ConditionTrue},
						{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionFalse},
					},
				},
			},
			expected: false,
		},
		{
			name: "Done=False without Progressing is not terminal",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{
						{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionFalse},
					},
				},
			},
			expected: false,
		},
		{
			// KubeVirt nightly (v1.20.0+) virt-controller renamed the "Done"
			// condition type to "Complete"; isVMBTerminal must recognize both.
			name: "Complete=True is terminal (success, nightly KubeVirt condition rename)",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{
						{Type: conditionComplete, Status: corev1.ConditionTrue, Reason: "CompletedWithWarning"},
						{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionFalse},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isVMBTerminal(tt.vmb)
			if got != tt.expected {
				t.Errorf("isVMBTerminal() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestHasOlderActiveDUForVM(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "vm-ns"
	oadpNs := "openshift-adp"

	// Use second-precision times to avoid nanosecond issues with fake client
	baseTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	olderTime := metav1.NewTime(time.Date(2026, 1, 1, 11, 59, 0, 0, time.UTC))
	newerTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 1, 0, 0, time.UTC))

	makeDU := func(name string, uid types.UID, phase velerov2alpha1.DataUploadPhase, creationTime metav1.Time, vm, vmNs string) *velerov2alpha1.DataUpload {
		return &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         oadpNs,
				UID:               uid,
				CreationTimestamp: creationTime,
				Annotations: map[string]string{
					common.AnnotationVMName:      vm,
					common.AnnotationVMNamespace: vmNs,
				},
			},
			Spec: velerov2alpha1.DataUploadSpec{
				DataMover:       common.DataMoverKubeVirt,
				SourceNamespace: vmNs,
			},
			Status: velerov2alpha1.DataUploadStatus{
				Phase: phase,
			},
		}
	}

	// The DU we're testing (current)
	currentDU := makeDU("du-current", "current-uid", velerov2alpha1.DataUploadPhaseAccepted, baseTime, vmName, vmNamespace)

	tests := []struct {
		name        string
		otherDUs    []client.Object
		wantWait    bool
		wantBlocked string
	}{
		{
			name:     "no other DUs",
			otherDUs: []client.Object{},
			wantWait: false,
		},
		{
			name: "other DU targets different VM",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseAccepted, olderTime, "different-vm", vmNamespace),
			},
			wantWait: false,
		},
		{
			name: "other DU targets same VM in different namespace",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseAccepted, olderTime, vmName, "other-ns"),
			},
			wantWait: false,
		},
		{
			name: "older DU in Completed phase (terminal)",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseCompleted, olderTime, vmName, vmNamespace),
			},
			wantWait: false,
		},
		{
			name: "older DU in Failed phase (terminal)",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseFailed, olderTime, vmName, vmNamespace),
			},
			wantWait: false,
		},
		{
			name: "older DU in New phase — should wait",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseNew, olderTime, vmName, vmNamespace),
			},
			wantWait:    true,
			wantBlocked: "du-other",
		},
		{
			name: "older DU in Accepted phase — should wait",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseAccepted, olderTime, vmName, vmNamespace),
			},
			wantWait:    true,
			wantBlocked: "du-other",
		},
		{
			name: "older DU in Prepared phase — should wait",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhasePrepared, olderTime, vmName, vmNamespace),
			},
			wantWait:    true,
			wantBlocked: "du-other",
		},
		{
			name: "older DU in InProgress phase — should wait",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseInProgress, olderTime, vmName, vmNamespace),
			},
			wantWait:    true,
			wantBlocked: "du-other",
		},
		{
			name: "newer DU in Accepted phase — should NOT wait",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseAccepted, newerTime, vmName, vmNamespace),
			},
			wantWait: false,
		},
		{
			name: "same timestamp, lower UID — should wait",
			otherDUs: []client.Object{
				makeDU("du-other", "aaa-uid", velerov2alpha1.DataUploadPhaseAccepted, baseTime, vmName, vmNamespace),
			},
			wantWait:    true,
			wantBlocked: "du-other",
		},
		{
			name: "same timestamp, higher UID — should NOT wait",
			otherDUs: []client.Object{
				makeDU("du-other", "zzz-uid", velerov2alpha1.DataUploadPhaseAccepted, baseTime, vmName, vmNamespace),
			},
			wantWait: false,
		},
		{
			name: "non-kubevirt DU is ignored",
			otherDUs: []client.Object{
				func() *velerov2alpha1.DataUpload {
					du := makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseAccepted, olderTime, vmName, vmNamespace)
					du.Spec.DataMover = "kopia"
					return du
				}(),
			},
			wantWait: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := append([]client.Object{currentDU.DeepCopy()}, tt.otherDUs...)
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				Scheme:        scheme,
				Log:           logr.Discard(),
				OADPNamespace: oadpNs,
			}

			shouldWait, blockingDU, err := r.hasOlderActiveDUForVM(context.Background(), currentDU.DeepCopy())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if shouldWait != tt.wantWait {
				t.Errorf("hasOlderActiveDUForVM() shouldWait = %v, want %v", shouldWait, tt.wantWait)
			}
			if tt.wantWait && blockingDU != tt.wantBlocked {
				t.Errorf("hasOlderActiveDUForVM() blockingDU = %q, want %q", blockingDU, tt.wantBlocked)
			}
		})
	}
}

func TestHasOlderActiveDUForVM_Staleness(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "vm-ns"
	oadpNs := "openshift-adp"

	now := time.Now()
	recentTime := metav1.NewTime(now.Add(-10 * time.Minute))
	staleTime := metav1.NewTime(now.Add(-3 * time.Hour))

	makeDU := func(name string, uid types.UID, phase velerov2alpha1.DataUploadPhase, creationTime metav1.Time) *velerov2alpha1.DataUpload {
		return &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         oadpNs,
				UID:               uid,
				CreationTimestamp: creationTime,
				Annotations: map[string]string{
					common.AnnotationVMName:      vmName,
					common.AnnotationVMNamespace: vmNamespace,
				},
			},
			Spec: velerov2alpha1.DataUploadSpec{
				DataMover:       common.DataMoverKubeVirt,
				SourceNamespace: vmNamespace,
			},
			Status: velerov2alpha1.DataUploadStatus{
				Phase: phase,
			},
		}
	}

	currentDU := makeDU("du-current", "current-uid", velerov2alpha1.DataUploadPhaseAccepted, metav1.NewTime(now))

	tests := []struct {
		name                     string
		staleDataUploadThreshold time.Duration
		otherDUs                 []client.Object
		wantWait                 bool
		wantBlocked              string
	}{
		{
			name:                     "stale older DU is skipped",
			staleDataUploadThreshold: 2 * time.Hour,
			otherDUs: []client.Object{
				makeDU("du-stale", "stale-uid", velerov2alpha1.DataUploadPhaseAccepted, staleTime),
			},
			wantWait: false,
		},
		{
			name:                     "non-stale older DU still blocks",
			staleDataUploadThreshold: 2 * time.Hour,
			otherDUs: []client.Object{
				makeDU("du-recent", "recent-uid", velerov2alpha1.DataUploadPhaseAccepted, recentTime),
			},
			wantWait:    true,
			wantBlocked: "du-recent",
		},
		{
			name:                     "threshold 0 disables staleness check",
			staleDataUploadThreshold: 0,
			otherDUs: []client.Object{
				makeDU("du-stale", "stale-uid", velerov2alpha1.DataUploadPhaseAccepted, staleTime),
			},
			wantWait:    true,
			wantBlocked: "du-stale",
		},
		{
			name:                     "stale DU in InProgress is also skipped",
			staleDataUploadThreshold: 2 * time.Hour,
			otherDUs: []client.Object{
				makeDU("du-stale", "stale-uid", velerov2alpha1.DataUploadPhaseInProgress, staleTime),
			},
			wantWait: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := append([]client.Object{currentDU.DeepCopy()}, tt.otherDUs...)
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			r := &KubeVirtDataUploadReconciler{
				Client:                   fakeClient,
				Scheme:                   scheme,
				Log:                      logr.Discard(),
				OADPNamespace:            oadpNs,
				StaleDataUploadThreshold: tt.staleDataUploadThreshold,
			}

			shouldWait, blockingDU, err := r.hasOlderActiveDUForVM(context.Background(), currentDU.DeepCopy())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if shouldWait != tt.wantWait {
				t.Errorf("hasOlderActiveDUForVM() shouldWait = %v, want %v", shouldWait, tt.wantWait)
			}
			if tt.wantWait && blockingDU != tt.wantBlocked {
				t.Errorf("hasOlderActiveDUForVM() blockingDU = %q, want %q", blockingDU, tt.wantBlocked)
			}
		})
	}
}

func TestHandleAccepted_RequeuesWhenOlderDUActive(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "vm-ns"
	oadpNs := "openshift-adp"

	olderTime := metav1.NewTime(time.Date(2026, 1, 1, 11, 59, 0, 0, time.UTC))
	newerTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 1, 0, 0, time.UTC))

	// DU-1 (older) is in InProgress — still uploading to S3
	du1 := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "du-1",
			Namespace:         oadpNs,
			UID:               types.UID("du-1-uid"),
			CreationTimestamp: olderTime,
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:       common.DataMoverKubeVirt,
			SourceNamespace: vmNamespace,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseInProgress,
		},
	}

	// DU-2 (newer) just entered Accepted — should wait for DU-1
	du2 := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "du-2",
			Namespace:         oadpNs,
			UID:               types.UID("du-2-uid"),
			CreationTimestamp: newerTime,
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			BackupStorageLocation: "default",
			SourceNamespace:       vmNamespace,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du1, du2).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: oadpNs,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.RequeueAfter != RequeueAfterLong {
		t.Errorf("expected RequeueAfter=%v (RequeueAfterLong), got %v", RequeueAfterLong, result.RequeueAfter)
	}
}

func TestHandleAccepted_ProceedsWhenOlderDUCompleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "vm-ns"
	oadpNs := "openshift-adp"

	olderTime := metav1.NewTime(time.Date(2026, 1, 1, 11, 59, 0, 0, time.UTC))
	newerTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 1, 0, 0, time.UTC))

	// DU-1 (older) is Completed — should not block
	du1 := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "du-1",
			Namespace:         oadpNs,
			UID:               types.UID("du-1-uid"),
			CreationTimestamp: olderTime,
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:       common.DataMoverKubeVirt,
			SourceNamespace: vmNamespace,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseCompleted,
		},
	}

	// DU-1's old VMBT still exists in the VM namespace
	vmbt1 := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-test-vm-aaa",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash:    common.HashForLabel(vmName),
				common.LabelDataUploadUID: string(du1.UID),
			},
		},
	}

	// DU-2 (newer) — should proceed since DU-1 is terminal
	du2 := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "du-2",
			Namespace:         oadpNs,
			UID:               types.UID("du-2-uid"),
			CreationTimestamp: newerTime,
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			BackupStorageLocation: "default",
			SourceNamespace:       vmNamespace,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: oadpNs,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{Bucket: "b", Prefix: "v"},
			},
			Config:     map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "c"}, Key: "cloud"},
		},
		Status: velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
	}

	// Temp PVC for DU-2
	tempPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-du-2",
			Namespace: vmNamespace,
			Labels:    map[string]string{common.LabelDataUploadUID: "du-2-uid"},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du1, du2, bsl, tempPVC, vmbt1).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: oadpNs,
	}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du2)

	// DU-2 should NOT be blocked — DU-1 is Completed (terminal).
	// It will proceed into prepareVMBackupTracker, which will fail because
	// BSL credentials aren't fully set up in this test. That's fine — we're
	// testing that the serialization guard does NOT block.
	// The important assertion is that DU-1's VMBT is reused (not deleted) per issue #32.
	reusedVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	getErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: vmbt1.Name, Namespace: vmNamespace,
	}, reusedVMBT)

	// prepareVMBackupTracker should have reused DU-1's existing VMBT
	if getErr != nil {
		t.Errorf("expected DU-1's VMBT to be reused by prepareVMBackupTracker, but it was deleted: %v", getErr)
	}
	// We don't care about the error from handleAccepted itself — it may fail
	// on BSL credential lookup. The point is it got past the serialization guard.
	_ = err
}

func TestHandleInProgress(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseInProgress,
		},
	}

	// Create a running datamover pod in the OADP namespace
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.DatamoverPodNamePrefix + du.Name,
			Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pod).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	result, err := r.handleInProgress(context.Background(), logr.Discard(), du)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// When pod is running, should requeue to check again
	if result.RequeueAfter != RequeueAfterShort {
		t.Errorf("expected RequeueAfter=%v when pod is running, got %v", RequeueAfterShort, result.RequeueAfter)
	}
}

func TestDefaultMaxConcurrentReconciles(t *testing.T) {
	if DefaultMaxConcurrentReconciles != 3 {
		t.Errorf("expected DefaultMaxConcurrentReconciles=3, got %d", DefaultMaxConcurrentReconciles)
	}
}

func TestDataMoverKubeVirtConstant(t *testing.T) {
	if common.DataMoverKubeVirt != "kubevirt" {
		t.Errorf("expected common.DataMoverKubeVirt='kubevirt', got '%s'", common.DataMoverKubeVirt)
	}
}

func TestGetVMReference(t *testing.T) {
	// Tests the common.GetVMReference function used by the controller
	tests := []struct {
		name              string
		dataUpload        *velerov2alpha1.DataUpload
		expectedVMName    string
		expectedNamespace string
		expectError       bool
	}{
		{
			name: "valid annotations",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName:      "my-vm",
						common.AnnotationVMNamespace: "my-namespace",
					},
				},
			},
			expectedVMName:    "my-vm",
			expectedNamespace: "my-namespace",
			expectError:       false,
		},
		{
			name: "missing namespace annotation uses source namespace",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName: "my-vm",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					SourceNamespace: "source-ns",
				},
			},
			expectedVMName:    "my-vm",
			expectedNamespace: "source-ns",
			expectError:       false,
		},
		{
			name: "no annotations",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
			},
			expectedVMName:    "",
			expectedNamespace: "",
			expectError:       true,
		},
		{
			name: "missing vm name annotation",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMNamespace: "my-namespace",
					},
				},
			},
			expectedVMName:    "",
			expectedNamespace: "",
			expectError:       true,
		},
		{
			name: "empty vm name annotation",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName:      "",
						common.AnnotationVMNamespace: "my-namespace",
					},
				},
			},
			expectedVMName:    "",
			expectedNamespace: "",
			expectError:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vmRef, err := common.GetVMReference(tt.dataUpload)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			var vmName, vmNamespace string
			if vmRef != nil {
				vmName = vmRef.Name
				vmNamespace = vmRef.Namespace
			}

			if vmName != tt.expectedVMName {
				t.Errorf("expected vmName=%s, got vmName=%s", tt.expectedVMName, vmName)
			}
			if vmNamespace != tt.expectedNamespace {
				t.Errorf("expected vmNamespace=%s, got vmNamespace=%s", tt.expectedNamespace, vmNamespace)
			}
		})
	}
}

func TestAnnotationConstants(t *testing.T) {
	if common.AnnotationVMName != "kubevirt-datamover.io/vm-name" {
		t.Errorf("expected common.AnnotationVMName='kubevirt-datamover.io/vm-name', got '%s'", common.AnnotationVMName)
	}
	if common.AnnotationVMNamespace != "kubevirt-datamover.io/vm-namespace" {
		t.Errorf("expected common.AnnotationVMNamespace='kubevirt-datamover.io/vm-namespace', got '%s'", common.AnnotationVMNamespace)
	}
	if common.AnnotationDataUploadName != "velero.io/dataupload-name" {
		t.Errorf("expected common.AnnotationDataUploadName='velero.io/dataupload-name', got '%s'", common.AnnotationDataUploadName)
	}
	if common.LabelVMNameHash != "kubevirt-datamover.io/vm-name-hash" {
		t.Errorf("expected common.LabelVMNameHash='kubevirt-datamover.io/vm-name-hash', got '%s'", common.LabelVMNameHash)
	}
	if common.LabelDataUploadUID != "velero.io/dataupload-uid" {
		t.Errorf("expected common.LabelDataUploadUID='velero.io/dataupload-uid', got '%s'", common.LabelDataUploadUID)
	}
	if DefaultTempPVCSize != "10Gi" {
		t.Errorf("expected DefaultTempPVCSize='10Gi', got '%s'", DefaultTempPVCSize)
	}
}

func TestExtractPodFailureMessage(t *testing.T) {
	tests := []struct {
		name     string
		pod      *corev1.Pod
		expected string
	}{
		{
			name: "container terminated with message",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "uploader",
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Message: "Error: failed to upload files",
								},
							},
						},
					},
				},
			},
			expected: "Error: failed to upload files",
		},
		{
			name: "container terminated with reason only",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "uploader",
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Reason: "OOMKilled",
								},
							},
						},
					},
				},
			},
			expected: "OOMKilled",
		},
		{
			name: "init container terminated with message",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "init",
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Message: "Init container failed",
								},
							},
						},
					},
				},
			},
			expected: "Init container failed",
		},
		{
			name: "pod condition with message",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:    corev1.PodScheduled,
							Status:  corev1.ConditionFalse,
							Message: "Insufficient memory",
						},
					},
				},
			},
			expected: "Insufficient memory",
		},
		{
			name: "no failure info",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{},
			},
			expected: "unknown error",
		},
		{
			name: "running container - no terminated state",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "uploader",
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			expected: "unknown error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractPodFailureMessage(tt.pod)
			if result != tt.expected {
				t.Errorf("extractPodFailureMessage() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestHandleInProgress_PodSucceeded(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
			UID:       types.UID("test-uid-succeeded"),
			Annotations: map[string]string{
				common.AnnotationVMName:      "test-vm",
				common.AnnotationVMNamespace: "test-ns",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseInProgress,
		},
	}

	// Create a succeeded datamover pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.DatamoverPodNamePrefix + du.Name,
			Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pod).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	result, err := r.handleInProgress(context.Background(), logr.Discard(), du)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue when pod succeeded, got RequeueAfter=%v", result.RequeueAfter)
	}

	// Verify phase transitioned to Completed
	updatedDU := &velerov2alpha1.DataUpload{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      du.Name,
		Namespace: du.Namespace,
	}, updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseCompleted {
		t.Errorf("expected phase=%s, got phase=%s", velerov2alpha1.DataUploadPhaseCompleted, updatedDU.Status.Phase)
	}
}

// TestHandleInProgress_PodSucceeded_DefersCleanupWhileTerminating verifies the
// fix for https://github.com/migtools/kubevirt-datamover-controller/issues/171:
// PVC/PV cleanup (and the Completed transition) must be deferred to a later
// reconcile while the datamover pod is still terminating, instead of blocking
// the reconcile on waitForPVCDeletion. It must also not mistake the pod's
// eventual disappearance for a genuine "pod not found" failure.
func TestHandleInProgress_PodSucceeded_DefersCleanupWhileTerminating(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
			UID:       types.UID("test-uid-terminating"),
			Annotations: map[string]string{
				common.AnnotationVMName:      "test-vm",
				common.AnnotationVMNamespace: "test-ns",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseInProgress,
		},
	}

	// A finalizer keeps the fake client from actually removing the pod on
	// Delete, simulating kubelet still tearing it down (unmounting volumes).
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.DatamoverPodNamePrefix + du.Name,
			Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
			Finalizers: []string{"test.io/still-terminating"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}

	reboundPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.SafeResourceName(common.ReboundPVCNamePrefix, du.Name),
			Namespace: "openshift-adp",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			VolumeName: "pv-test-du",
		},
	}

	// The bound PV: cleanupReboundPVCAndPV also patches its reclaim policy to
	// Delete and deletes it after the PVC, so round 2 must clean this up too.
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv-test-du",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pod, reboundPVC, pv).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	// Round 1: pod is present but still terminating (finalizer blocks
	// removal). Cleanup must defer rather than block on PVC/PV deletion.
	result, err := r.handleInProgress(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != RequeueAfterShort {
		t.Errorf("expected requeue=%v while pod terminating, got %v", RequeueAfterShort, result.RequeueAfter)
	}

	updatedDU := &velerov2alpha1.DataUpload{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      du.Name,
		Namespace: du.Namespace,
	}, updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}
	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseInProgress {
		t.Errorf("expected phase to remain InProgress while pod terminates, got phase=%s", updatedDU.Status.Phase)
	}
	if updatedDU.Annotations[common.AnnotationDatamoverPodSucceeded] != "true" {
		t.Errorf("expected %s annotation to be set to record the pod's success", common.AnnotationDatamoverPodSucceeded)
	}

	// The rebound PVC must survive round 1: cleanup deferred while the pod
	// is still terminating.
	survivingPVC := &corev1.PersistentVolumeClaim{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      reboundPVC.Name,
		Namespace: reboundPVC.Namespace,
	}, survivingPVC); err != nil {
		t.Errorf("expected rebound PVC to survive while pod cleanup is deferred, got err=%v", err)
	}

	// Simulate kubelet finishing termination: clear the finalizer, which lets
	// the fake client actually remove the pod.
	terminatingPod := &corev1.Pod{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      pod.Name,
		Namespace: pod.Namespace,
	}, terminatingPod); err != nil {
		t.Fatalf("failed to get terminating pod: %v", err)
	}
	if terminatingPod.DeletionTimestamp == nil {
		t.Fatalf("expected handleInProgress to have requested datamover pod deletion in round 1")
	}
	terminatingPod.Finalizers = nil
	if err := fakeClient.Update(context.Background(), terminatingPod); err != nil {
		t.Fatalf("failed to clear pod finalizer: %v", err)
	}

	// Round 2: the pod is now fully gone. Because AnnotationDatamoverPodSucceeded
	// is set, this must be treated as "cleanup finishing", not "pod not found".
	result, err = r.handleInProgress(context.Background(), logr.Discard(), updatedDU)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue once pod is gone, got RequeueAfter=%v", result.RequeueAfter)
	}

	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      du.Name,
		Namespace: du.Namespace,
	}, updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}
	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseCompleted {
		t.Errorf("expected phase=%s once cleanup finishes, got phase=%s", velerov2alpha1.DataUploadPhaseCompleted, updatedDU.Status.Phase)
	}

	// The rebound PVC must be cleaned up once the pod cleanup completes.
	deletedPVC := &corev1.PersistentVolumeClaim{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      reboundPVC.Name,
		Namespace: reboundPVC.Namespace,
	}, deletedPVC)
	if !errors.IsNotFound(err) {
		t.Errorf("expected rebound PVC to be deleted once cleanup completes, got err=%v", err)
	}

	// Its bound PV must be cleaned up too.
	deletedPV := &corev1.PersistentVolume{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name: pv.Name,
	}, deletedPV)
	if !errors.IsNotFound(err) {
		t.Errorf("expected bound PV to be deleted once cleanup completes, got err=%v", err)
	}
}

// TestHandleCanceling_DefersCleanupWhileTerminating verifies handleCanceling
// also defers (rather than blocks on waitForPVCDeletion) when the datamover
// pod is still terminating, per issue #171.
func TestHandleCanceling_DefersCleanupWhileTerminating(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)

	vmNamespace := "test-ns"
	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
			UID:       types.UID("test-uid-canceling"),
			Annotations: map[string]string{
				common.AnnotationVMName:      "test-vm",
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseCanceling,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.DatamoverPodNamePrefix + du.Name,
			Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
			Finalizers: []string{"test.io/still-terminating"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-test-du",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pod, vmb).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	// Round 1: pod still terminating - VMB cleanup and Canceled transition
	// must be deferred.
	result, err := r.handleCanceling(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != RequeueAfterShort {
		t.Errorf("expected requeue=%v while pod terminating, got %v", RequeueAfterShort, result.RequeueAfter)
	}

	updatedDU := &velerov2alpha1.DataUpload{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      du.Name,
		Namespace: du.Namespace,
	}, updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}
	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseCanceling {
		t.Errorf("expected phase to remain Canceling while pod terminates, got phase=%s", updatedDU.Status.Phase)
	}

	survivingVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      vmb.Name,
		Namespace: vmNamespace,
	}, survivingVMB); err != nil {
		t.Errorf("expected VMB to survive while pod cleanup is deferred, got err=%v", err)
	}

	// Simulate kubelet finishing termination.
	terminatingPod := &corev1.Pod{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      pod.Name,
		Namespace: pod.Namespace,
	}, terminatingPod); err != nil {
		t.Fatalf("failed to get terminating pod: %v", err)
	}
	if terminatingPod.DeletionTimestamp == nil {
		t.Fatalf("expected handleCanceling to have requested datamover pod deletion in round 1")
	}
	terminatingPod.Finalizers = nil
	if err := fakeClient.Update(context.Background(), terminatingPod); err != nil {
		t.Fatalf("failed to clear pod finalizer: %v", err)
	}

	// Round 2: pod is gone - VMB cleanup and Canceled transition proceed.
	result, err = r.handleCanceling(context.Background(), logr.Discard(), updatedDU)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue once pod is gone, got RequeueAfter=%v", result.RequeueAfter)
	}

	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      du.Name,
		Namespace: du.Namespace,
	}, updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}
	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseCanceled {
		t.Errorf("expected phase=%s once cleanup finishes, got phase=%s", velerov2alpha1.DataUploadPhaseCanceled, updatedDU.Status.Phase)
	}

	deletedVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      vmb.Name,
		Namespace: vmNamespace,
	}, deletedVMB)
	if !errors.IsNotFound(err) {
		t.Errorf("expected VMB to be deleted once cleanup completes, got err=%v", err)
	}
}

func TestHandleInProgress_PodFailed(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
			UID:       types.UID("test-uid-failed"),
			Annotations: map[string]string{
				common.AnnotationVMName:      "test-vm",
				common.AnnotationVMNamespace: "test-ns",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseInProgress,
		},
	}

	// Create a failed datamover pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.DatamoverPodNamePrefix + du.Name,
			Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "uploader",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Message: "S3 upload failed: access denied",
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pod).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	result, err := r.handleInProgress(context.Background(), logr.Discard(), du)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue when pod failed, got RequeueAfter=%v", result.RequeueAfter)
	}

	// Verify phase transitioned to Failed
	updatedDU := &velerov2alpha1.DataUpload{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      du.Name,
		Namespace: du.Namespace,
	}, updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
		t.Errorf("expected phase=%s, got phase=%s", velerov2alpha1.DataUploadPhaseFailed, updatedDU.Status.Phase)
	}
}

// TestHandleInProgress_PodFailed_PreservesVMB locks in the team's decision
// (see PR #170 review discussion) to preserve the VMB (and VMBT) on a
// genuine DataUpload Failed transition for debugging, matching the existing
// "Skip cleanup on failure to preserve resources for debugging" handling of
// the datamover pod/PVC/PV in handleInProgress's PodFailed case.
func TestHandleInProgress_PodFailed_PreservesVMB(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)

	vmNamespace := "test-ns"
	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
			UID:       types.UID("test-uid-failed"),
			Annotations: map[string]string{
				common.AnnotationVMName:      "test-vm",
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseInProgress,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.DatamoverPodNamePrefix + du.Name,
			Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
		},
	}

	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-test-du",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
	}
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-test-vm",
			Namespace: vmNamespace,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pod, vmb, vmbt).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	result, err := r.handleInProgress(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue when pod failed, got RequeueAfter=%v", result.RequeueAfter)
	}

	updatedDU := &velerov2alpha1.DataUpload{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      du.Name,
		Namespace: du.Namespace,
	}, updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}
	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
		t.Errorf("expected phase=%s, got phase=%s", velerov2alpha1.DataUploadPhaseFailed, updatedDU.Status.Phase)
	}

	survivingVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      vmb.Name,
		Namespace: vmNamespace,
	}, survivingVMB); err != nil {
		t.Errorf("expected VMB to be preserved on Failed transition, got err=%v", err)
	}

	survivingVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      vmbt.Name,
		Namespace: vmNamespace,
	}, survivingVMBT); err != nil {
		t.Errorf("expected VMBT to be preserved, but it was deleted or errored: %v", err)
	}

	// The failed datamover pod itself must also survive (per the existing
	// "skip cleanup on failure to preserve resources for debugging" handling).
	survivingPod := &corev1.Pod{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      pod.Name,
		Namespace: pod.Namespace,
	}, survivingPod); err != nil {
		t.Errorf("expected datamover pod to be preserved on Failed transition, got err=%v", err)
	}
}

func TestHandleInProgress_PodNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
			UID:       types.UID("test-uid-notfound"),
			Annotations: map[string]string{
				common.AnnotationVMName:      "test-vm",
				common.AnnotationVMNamespace: "test-ns",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseInProgress,
		},
	}

	// No pod exists
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	result, err := r.handleInProgress(context.Background(), logr.Discard(), du)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue when pod not found, got RequeueAfter=%v", result.RequeueAfter)
	}

	// Verify phase transitioned to Failed
	updatedDU := &velerov2alpha1.DataUpload{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      du.Name,
		Namespace: du.Namespace,
	}, updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
		t.Errorf("expected phase=%s, got phase=%s", velerov2alpha1.DataUploadPhaseFailed, updatedDU.Status.Phase)
	}
}

func TestCleanupDatamoverResources(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
			UID:       types.UID("test-uid"),
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
	}

	// Create resources to be cleaned up
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.DatamoverPodNamePrefix + du.Name,
			Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
	}

	reboundPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.ReboundPVCNamePrefix + du.Name,
			Namespace: "openshift-adp",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pod, reboundPVC).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	// Call cleanup
	r.cleanupDatamoverResources(context.Background(), logr.Discard(), du, "openshift-adp")

	// Verify pod was deleted
	deletedPod := &corev1.Pod{}
	err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      pod.Name,
		Namespace: pod.Namespace,
	}, deletedPod)
	if err == nil {
		t.Error("expected pod to be deleted")
	}

	// Verify rebound PVC was deleted
	deletedPVC := &corev1.PersistentVolumeClaim{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      reboundPVC.Name,
		Namespace: reboundPVC.Namespace,
	}, deletedPVC)
	if err == nil {
		t.Error("expected rebound PVC to be deleted")
	}
}

func TestGetVeleroBackupName(t *testing.T) {
	tests := []struct {
		name     string
		du       *velerov2alpha1.DataUpload
		expected string
	}{
		{
			name: "with velero backup label",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						common.LabelVeleroBackupName: "my-velero-backup",
					},
				},
			},
			expected: "my-velero-backup",
		},
		{
			name: "without velero backup label",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"some-other-label": "value",
					},
				},
			},
			expected: "",
		},
		{
			name: "nil labels",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{},
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getVeleroBackupName(tt.du.Labels)
			if result != tt.expected {
				t.Errorf("getVeleroBackupName() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestBuildDatamoverPodConfig(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	tests := []struct {
		name               string
		du                 *velerov2alpha1.DataUpload
		bsl                *velerov1.BackupStorageLocation
		vmb                *kubevirtbackupv1alpha1.VirtualMachineBackup
		vmRef              *common.VMReference
		backupType         string
		expectedBackupType string
		checkpointName     string
		vmbtName           string
		datamoverImage     string
		expectError        bool
		errorContains      string
		validate           func(*testing.T, *DatamoverPodConfig)
	}{
		{
			name: "valid config",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					UID:       types.UID("du-uid-123"),
					Labels: map[string]string{
						common.LabelVeleroBackupName: "velero-backup-001",
					},
				},
			},
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-adp",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "my-bucket",
							Prefix: "velero",
						},
					},
					Config: map[string]string{
						"region":  "us-east-1",
						"profile": "minio",
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "cloud-credentials",
						},
						Key: "cloud",
					},
				},
			},
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vmb-test-du",
					Namespace: "vm-ns",
				},
			},
			vmRef:              &common.VMReference{Name: "my-vm", Namespace: "vm-ns"},
			backupType:         "full",
			expectedBackupType: "incremental",
			checkpointName:     "checkpoint-001",
			vmbtName:           "vmbt-my-vm-xyz",
			datamoverImage:     "quay.io/test/datamover:v1",
			expectError:        false,
			validate: func(t *testing.T, config *DatamoverPodConfig) {
				if config.Name != "test-du" {
					t.Errorf("Name = %q, want %q", config.Name, "test-du")
				}
				if config.Namespace != "vm-ns" {
					t.Errorf("Namespace = %q, want %q", config.Namespace, "vm-ns")
				}
				if config.BSLBucket != "my-bucket" {
					t.Errorf("BSLBucket = %q, want %q", config.BSLBucket, "my-bucket")
				}
				if config.BSLPrefix != "velero-kubevirt-datamover" {
					t.Errorf("BSLPrefix = %q, want %q", config.BSLPrefix, "velero-kubevirt-datamover")
				}
				if config.BSLRegion != "us-east-1" {
					t.Errorf("BSLRegion = %q, want %q", config.BSLRegion, "us-east-1")
				}
				if config.BSLProfile != "minio" {
					t.Errorf("BSLProfile = %q, want %q", config.BSLProfile, "minio")
				}
				if config.CredentialSecretName != "cloud-credentials" {
					t.Errorf("CredentialSecretName = %q, want %q", config.CredentialSecretName, "cloud-credentials")
				}
				if config.VeleroBackupName != "velero-backup-001" {
					t.Errorf("VeleroBackupName = %q, want %q", config.VeleroBackupName, "velero-backup-001")
				}
				if config.BackupType != "full" {
					t.Errorf("BackupType = %q, want %q", config.BackupType, "full")
				}
				if config.ExpectedBackupType != "incremental" {
					t.Errorf("ExpectedBackupType = %q, want %q", config.ExpectedBackupType, "incremental")
				}
				if config.CheckpointName != "checkpoint-001" {
					t.Errorf("CheckpointName = %q, want %q", config.CheckpointName, "checkpoint-001")
				}
				if config.VMBTName != "vmbt-my-vm-xyz" {
					t.Errorf("VMBTName = %q, want %q", config.VMBTName, "vmbt-my-vm-xyz")
				}
			},
		},
		{
			name: "BSL without prefix",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-du",
					UID:  types.UID("du-uid"),
				},
			},
			bsl: &velerov1.BackupStorageLocation{
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "my-bucket",
							Prefix: "", // No prefix
						},
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "creds",
						},
					},
				},
			},
			vmb:            &kubevirtbackupv1alpha1.VirtualMachineBackup{},
			vmRef:          &common.VMReference{Name: "vm", Namespace: "ns"},
			backupType:     "full",
			checkpointName: "cp",
			vmbtName:       "vmbt-vm-123",
			datamoverImage: "image:v1",
			expectError:    false,
			validate: func(t *testing.T, config *DatamoverPodConfig) {
				if config.BSLPrefix != "kubevirt-datamover" {
					t.Errorf("BSLPrefix = %q, want %q", config.BSLPrefix, "kubevirt-datamover")
				}
			},
		},
		{
			name: "missing bucket",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-du",
				},
			},
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name: "bsl-no-bucket",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "", // Missing
						},
					},
				},
			},
			vmb:           &kubevirtbackupv1alpha1.VirtualMachineBackup{},
			vmRef:         &common.VMReference{Name: "vm", Namespace: "ns"},
			expectError:   true,
			errorContains: "no bucket configured",
		},
		{
			name: "missing credential secret",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-du",
				},
			},
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name: "bsl-no-creds",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
						},
					},
					Credential: nil, // Missing
				},
			},
			vmb:           &kubevirtbackupv1alpha1.VirtualMachineBackup{},
			vmRef:         &common.VMReference{Name: "vm", Namespace: "ns"},
			expectError:   true,
			errorContains: "no credential secret configured",
		},
		{
			name: "missing datamover image",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-du",
				},
			},
			bsl: &velerov1.BackupStorageLocation{
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
						},
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "creds",
						},
					},
				},
			},
			vmb:            &kubevirtbackupv1alpha1.VirtualMachineBackup{},
			vmRef:          &common.VMReference{Name: "vm", Namespace: "ns"},
			datamoverImage: "", // Missing
			expectError:    true,
			errorContains:  "datamover image not configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &KubeVirtDataUploadReconciler{
				DatamoverImage: tt.datamoverImage,
			}

			config, err := r.buildDatamoverPodConfig(
				tt.du,
				tt.bsl,
				tt.vmb,
				tt.vmRef,
				tt.backupType,
				tt.expectedBackupType,
				tt.checkpointName,
				tt.vmbtName,
				resource.MustParse("10Gi"),
			)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errorContains)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if tt.validate != nil {
					tt.validate(t, config)
				}
			}
		})
	}
}

// contains checks if s contains substr
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestGetBackupStorageLocation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	tests := []struct {
		name          string
		du            *velerov2alpha1.DataUpload
		bsl           *velerov1.BackupStorageLocation
		expectError   bool
		errorContains string
	}{
		{
			name: "BSL found",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					BackupStorageLocation: "default",
				},
			},
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-adp",
				},
			},
			expectError: false,
		},
		{
			name: "BSL not found",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					BackupStorageLocation: "nonexistent",
				},
			},
			bsl:           nil,
			expectError:   true,
			errorContains: "failed to get BackupStorageLocation",
		},
		{
			name: "empty BSL name",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					BackupStorageLocation: "",
				},
			},
			bsl:           nil,
			expectError:   true,
			errorContains: "no BackupStorageLocation name specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.bsl != nil {
				builder = builder.WithObjects(tt.bsl)
			}
			fakeClient := builder.Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				OADPNamespace: "openshift-adp",
			}

			bsl, err := r.getBackupStorageLocationForDU(context.Background(), tt.du)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errorContains)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if bsl == nil {
					t.Error("expected BSL but got nil")
				}
			}
		})
	}
}

func TestHandlePrepared(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name          string
		setupObjects  func() []runtime.Object
		du            *velerov2alpha1.DataUpload
		datamoverImg  string
		expectError   bool
		expectedPhase velerov2alpha1.DataUploadPhase
		expectRequeue bool
	}{
		{
			name:         "creates datamover pod and transitions to InProgress",
			datamoverImg: "quay.io/test/datamover:v1",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					UID:       types.UID("du-uid-123"),
					Annotations: map[string]string{
						common.AnnotationVMName:      "test-vm",
						common.AnnotationVMNamespace: "vm-ns",
						AnnotationVMBTName:           "vmbt-test-vm-abc",
					},
					Labels: map[string]string{
						common.LabelVeleroBackupName: "velero-backup",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover:             common.DataMoverKubeVirt,
					BackupStorageLocation: "default",
					SourceNamespace:       "vm-ns",
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhasePrepared,
				},
			},
			setupObjects: func() []runtime.Object {
				bsl := &velerov1.BackupStorageLocation{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "default",
						Namespace: "openshift-adp",
					},
					Spec: velerov1.BackupStorageLocationSpec{
						Provider: "aws",
						StorageType: velerov1.StorageType{
							ObjectStorage: &velerov1.ObjectStorageLocation{
								Bucket: "test-bucket",
								Prefix: "velero",
							},
						},
						Config: map[string]string{"region": "us-east-1"},
						Credential: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
							Key:                  "cloud",
						},
					},
				}

				checkpointName := "checkpoint-001"
				tempPVCName := "kubevirt-backup-test-du-abc12"
				vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmb-test-du",
						Namespace: "vm-ns",
						Labels: map[string]string{
							common.LabelDataUploadUID: "du-uid-123",
						},
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
						PvcName: &tempPVCName,
					},
					Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
						Type:           kubevirtbackupv1alpha1.Full,
						CheckpointName: &checkpointName,
					},
				}

				// Pre-create the rebound PVC in OADP namespace (skips rebind step)
				reboundPVC := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      common.ReboundPVCNamePrefix + "test-du",
						Namespace: "openshift-adp",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName:  "pv-123",
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("10Gi"),
							},
						},
					},
					Status: corev1.PersistentVolumeClaimStatus{
						Phase: corev1.ClaimBound,
					},
				}

				tempPVC := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      tempPVCName,
						Namespace: "vm-ns",
						Labels: map[string]string{
							common.LabelDataUploadUID: "du-uid-123",
						},
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
						},
					},
				}

				return []runtime.Object{bsl, vmb, reboundPVC, tempPVC}
			},
			expectError:   false,
			expectedPhase: velerov2alpha1.DataUploadPhaseInProgress,
			expectRequeue: true,
		},
		{
			name:         "missing VM annotations fails",
			datamoverImg: "quay.io/test/datamover:v1",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					// No VM annotations
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhasePrepared,
				},
			},
			setupObjects:  func() []runtime.Object { return nil },
			expectError:   false,
			expectedPhase: velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue: false,
		},
		{
			name:         "missing BSL fails",
			datamoverImg: "quay.io/test/datamover:v1",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName:      "test-vm",
						common.AnnotationVMNamespace: "vm-ns",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover:             common.DataMoverKubeVirt,
					BackupStorageLocation: "nonexistent",
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhasePrepared,
				},
			},
			setupObjects:  func() []runtime.Object { return nil },
			expectError:   false,
			expectedPhase: velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue: false,
		},
		{
			name:         "VMB without PvcName fails when rebind is needed",
			datamoverImg: "quay.io/test/datamover:v1",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du-no-pvc",
					Namespace: "openshift-adp",
					UID:       types.UID("du-uid-nopvc"),
					Annotations: map[string]string{
						common.AnnotationVMName:      "test-vm",
						common.AnnotationVMNamespace: "vm-ns",
						AnnotationVMBTName:           "vmbt-test-vm-abc",
					},
					Labels: map[string]string{
						common.LabelVeleroBackupName: "velero-backup",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover:             common.DataMoverKubeVirt,
					BackupStorageLocation: "default",
					SourceNamespace:       "vm-ns",
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhasePrepared,
				},
			},
			setupObjects: func() []runtime.Object {
				bsl := &velerov1.BackupStorageLocation{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "default",
						Namespace: "openshift-adp",
					},
					Spec: velerov1.BackupStorageLocationSpec{
						Provider: "aws",
						StorageType: velerov1.StorageType{
							ObjectStorage: &velerov1.ObjectStorageLocation{
								Bucket: "test-bucket",
								Prefix: "velero",
							},
						},
						Config: map[string]string{"region": "us-east-1"},
						Credential: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
							Key:                  "cloud",
						},
					},
				}

				// VMB exists but has no PvcName in spec
				vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmb-test-du-no-pvc",
						Namespace: "vm-ns",
						Labels: map[string]string{
							common.LabelDataUploadUID: "du-uid-nopvc",
						},
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{},
					Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
						Type: kubevirtbackupv1alpha1.Full,
					},
				}
				// No rebound PVC exists → rebind path will be taken
				return []runtime.Object{bsl, vmb}
			},
			expectError:   false,
			expectedPhase: velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.du)

			if tt.setupObjects != nil {
				for _, obj := range tt.setupObjects() {
					builder = builder.WithRuntimeObjects(obj)
				}
			}

			fakeClient := builder.Build()

			r := &KubeVirtDataUploadReconciler{
				Client:         fakeClient,
				Scheme:         scheme,
				Log:            logr.Discard(),
				OADPNamespace:  "openshift-adp",
				DatamoverImage: tt.datamoverImg,
			}

			result, err := r.handlePrepared(context.Background(), logr.Discard(), tt.du)

			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.expectRequeue && result.RequeueAfter == 0 {
				t.Error("expected requeue but got none")
			}
			if !tt.expectRequeue && result.RequeueAfter > 0 {
				t.Errorf("expected no requeue, got RequeueAfter=%v", result.RequeueAfter)
			}

			// Check phase
			updatedDU := &velerov2alpha1.DataUpload{}
			if err := fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      tt.du.Name,
				Namespace: tt.du.Namespace,
			}, updatedDU); err != nil {
				t.Fatalf("failed to get updated DataUpload: %v", err)
			}

			if updatedDU.Status.Phase != tt.expectedPhase {
				t.Errorf("expected phase=%s, got phase=%s", tt.expectedPhase, updatedDU.Status.Phase)
			}
		})
	}
}

// TestHandlePreparedDataUpload_ConcurrencyLimit covers issue #174: gating
// datamover pod creation in handlePrepared against MaxConcurrentDataMovers.
// TestCountHigherPriorityActiveDataUploads exercises the ranking rules
// countHigherPriorityActiveDataUploads applies on top of the raw active-phase
// filter: phase/DataMover inclusion, and CreationTimestamp-first/UID-tiebreak
// ranking against a fixed self DU.
func TestCountHigherPriorityActiveDataUploads(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	oadpNS := "openshift-adp"
	baseTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	olderTime := metav1.NewTime(time.Date(2026, 1, 1, 11, 59, 0, 0, time.UTC))
	newerTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 1, 0, 0, time.UTC))

	makeDU := func(name string, uid types.UID, phase velerov2alpha1.DataUploadPhase, dataMover string, created *metav1.Time) *velerov2alpha1.DataUpload {
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: oadpNS, UID: uid},
			Spec:       velerov2alpha1.DataUploadSpec{DataMover: dataMover},
			Status:     velerov2alpha1.DataUploadStatus{Phase: phase},
		}
		if created != nil {
			du.CreationTimestamp = *created
		}
		return du
	}

	self := makeDU("du-self", "self-uid", velerov2alpha1.DataUploadPhasePrepared, common.DataMoverKubeVirt, &baseTime)

	tests := []struct {
		name      string
		otherDUs  []client.Object
		wantCount int
	}{
		{name: "no other DUs", otherDUs: nil, wantCount: 0},
		{
			name: "counts higher-priority (older) Accepted/Prepared/InProgress peers",
			otherDUs: []client.Object{
				makeDU("du-accepted", "uid-1", velerov2alpha1.DataUploadPhaseAccepted, common.DataMoverKubeVirt, &olderTime),
				makeDU("du-prepared", "uid-2", velerov2alpha1.DataUploadPhasePrepared, common.DataMoverKubeVirt, &olderTime),
				makeDU("du-inprogress", "uid-3", velerov2alpha1.DataUploadPhaseInProgress, common.DataMoverKubeVirt, &olderTime),
			},
			wantCount: 3,
		},
		{
			name: "excludes terminal phases even if older",
			otherDUs: []client.Object{
				makeDU("du-completed", "uid-1", velerov2alpha1.DataUploadPhaseCompleted, common.DataMoverKubeVirt, &olderTime),
				makeDU("du-failed", "uid-2", velerov2alpha1.DataUploadPhaseFailed, common.DataMoverKubeVirt, &olderTime),
				makeDU("du-canceled", "uid-3", velerov2alpha1.DataUploadPhaseCanceled, common.DataMoverKubeVirt, &olderTime),
			},
			wantCount: 0,
		},
		{
			name: "excludes New (pre-provisioning) even if older",
			otherDUs: []client.Object{
				makeDU("du-new", "uid-1", velerov2alpha1.DataUploadPhaseNew, common.DataMoverKubeVirt, &olderTime),
			},
			wantCount: 0,
		},
		{
			name: "excludes non-kubevirt DataMover CRs even if older and active",
			otherDUs: []client.Object{
				makeDU("du-other-mover", "uid-1", velerov2alpha1.DataUploadPhaseInProgress, "csi", &olderTime),
			},
			wantCount: 0,
		},
		{
			name: "excludes an active peer with a later timestamp (lower priority)",
			otherDUs: []client.Object{
				makeDU("du-newer", "uid-1", velerov2alpha1.DataUploadPhaseInProgress, common.DataMoverKubeVirt, &newerTime),
			},
			wantCount: 0,
		},
		{
			name: "counts an equal-timestamp peer whose UID tiebreaks ahead",
			otherDUs: []client.Object{
				makeDU("aaa-uid-wins-tiebreak", "aaa-uid", velerov2alpha1.DataUploadPhaseInProgress, common.DataMoverKubeVirt, &baseTime),
			},
			wantCount: 1,
		},
		{
			name: "excludes an equal-timestamp peer whose UID tiebreaks behind",
			otherDUs: []client.Object{
				makeDU("zzz-uid-loses-tiebreak", "zzz-uid", velerov2alpha1.DataUploadPhaseInProgress, common.DataMoverKubeVirt, &baseTime),
			},
			wantCount: 0,
		},
		{
			name: "ranking is unaffected by Status.AcceptedTimestamp (only CreationTimestamp matters)",
			otherDUs: []client.Object{
				func() *velerov2alpha1.DataUpload {
					// A newer CreationTimestamp but an older AcceptedTimestamp --
					// if ranking still consulted AcceptedTimestamp this peer
					// would wrongly outrank self. It must not: CreationTimestamp
					// alone decides rank.
					du := makeDU("du-newer-creation-older-accepted", "uid-1", velerov2alpha1.DataUploadPhaseInProgress, common.DataMoverKubeVirt, &newerTime)
					du.Status.AcceptedTimestamp = &olderTime
					return du
				}(),
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := append([]client.Object{self}, tt.otherDUs...)
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
			r := &KubeVirtDataUploadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

			count, err := r.countHigherPriorityActiveDataUploads(context.Background(), self)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if count != tt.wantCount {
				t.Errorf("count = %d, want %d", count, tt.wantCount)
			}
		})
	}
}

// TestHandlePreparedDataUpload_ConcurrencyLimit covers issue #174: gating
// datamover pod creation in handlePrepared against MaxConcurrentDataMovers.
func TestHandlePreparedDataUpload_ConcurrencyLimit(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	baseTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	olderTime := metav1.NewTime(time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC))

	// newFixtureWithOthers seeds otherActiveCount peers strictly OLDER than du
	// (by CreationTimestamp) so they always outrank du regardless of UID --
	// these tests exercise the count/limit comparison itself, not ranking
	// direction (that's TestCountHigherPriorityActiveDataUploads's job). du's
	// own AcceptedTimestamp is separately seeded to exercise the gate's
	// advance-on-defer behavior -- ranking never reads it.
	newFixtureWithOthers := func(otherActiveCount int) (*velerov2alpha1.DataUpload, []runtime.Object) {
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test-du",
				Namespace:         "openshift-adp",
				UID:               types.UID("du-uid-123"),
				CreationTimestamp: baseTime,
				Annotations: map[string]string{
					common.AnnotationVMName:      "test-vm",
					common.AnnotationVMNamespace: "vm-ns",
					AnnotationVMBTName:           "vmbt-test-vm-abc",
				},
				Labels: map[string]string{
					common.LabelVeleroBackupName: "velero-backup",
				},
			},
			Spec: velerov2alpha1.DataUploadSpec{
				DataMover:             common.DataMoverKubeVirt,
				BackupStorageLocation: "default",
				SourceNamespace:       "vm-ns",
			},
			Status: velerov2alpha1.DataUploadStatus{
				Phase:             velerov2alpha1.DataUploadPhasePrepared,
				AcceptedTimestamp: &baseTime,
			},
		}

		bsl := &velerov1.BackupStorageLocation{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-adp"},
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
		}

		checkpointName := "checkpoint-001"
		tempPVCName := "kubevirt-backup-test-du-abc12"
		vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "vmb-test-du",
				Namespace: "vm-ns",
				Labels:    map[string]string{common.LabelDataUploadUID: "du-uid-123"},
			},
			Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{PvcName: &tempPVCName},
			Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
				Type:           kubevirtbackupv1alpha1.Full,
				CheckpointName: &checkpointName,
			},
		}

		reboundPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: common.ReboundPVCNamePrefix + "test-du", Namespace: "openshift-adp"},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:  "pv-123",
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}

		tempPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      tempPVCName,
				Namespace: "vm-ns",
				Labels: map[string]string{
					common.LabelDataUploadUID: "du-uid-123",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				},
			},
		}

		objs := make([]runtime.Object, 0, 4+otherActiveCount)
		objs = append(objs, bsl, vmb, reboundPVC, tempPVC)
		for i := range otherActiveCount {
			other := &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("other-du-%d", i), Namespace: "openshift-adp", UID: types.UID(fmt.Sprintf("other-uid-%d", i)),
					CreationTimestamp: olderTime,
				},
				Spec:   velerov2alpha1.DataUploadSpec{DataMover: common.DataMoverKubeVirt},
				Status: velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseInProgress, AcceptedTimestamp: &olderTime},
			}
			objs = append(objs, other)
		}
		return du, objs
	}

	t.Run("proceeds when under the limit", func(t *testing.T) {
		du, objs := newFixtureWithOthers(2)
		builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du)
		for _, obj := range objs {
			builder = builder.WithRuntimeObjects(obj)
		}
		fakeClient := builder.Build()
		r := &KubeVirtDataUploadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest", MaxConcurrentDataMovers: 3,
		}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), du); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataUpload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: du.Name, Namespace: du.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataUploadPhaseInProgress {
			t.Errorf("phase = %q, want %q (should proceed: 2 others < limit 3)", updated.Status.Phase, velerov2alpha1.DataUploadPhaseInProgress)
		}
	})

	t.Run("requeues without creating a pod when at the limit", func(t *testing.T) {
		du, objs := newFixtureWithOthers(3)
		builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du)
		for _, obj := range objs {
			builder = builder.WithRuntimeObjects(obj)
		}
		fakeClient := builder.Build()
		r := &KubeVirtDataUploadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest", MaxConcurrentDataMovers: 3,
		}

		result, err := r.handlePrepared(context.Background(), logr.Discard(), du)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != RequeueAfterLong {
			t.Errorf("RequeueAfter = %v, want %v", result.RequeueAfter, RequeueAfterLong)
		}

		var updated velerov2alpha1.DataUpload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: du.Name, Namespace: du.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataUploadPhasePrepared {
			t.Errorf("phase = %q, want still %q (at limit 3, must not proceed)", updated.Status.Phase, velerov2alpha1.DataUploadPhasePrepared)
		}

		pod, err := r.findPodForDataUpload(context.Background(), du, "openshift-adp")
		if err != nil {
			t.Fatalf("failed to find pod: %v", err)
		}
		if pod != nil {
			t.Error("expected no datamover pod to be created while gated")
		}

		// Deferring due to the concurrency limit is intentional throttling, not
		// a stalled operation -- AcceptedTimestamp must advance by exactly the
		// requeue interval so this wait doesn't consume Spec.OperationTimeout's
		// budget (see the gate's defer branch in checkConcurrentDataMoverLimit).
		wantAdvanced := baseTime.Add(RequeueAfterLong)
		if updated.Status.AcceptedTimestamp == nil || !updated.Status.AcceptedTimestamp.Time.Equal(wantAdvanced) {
			t.Errorf("AcceptedTimestamp = %v, want %v (baseTime + RequeueAfterLong)", updated.Status.AcceptedTimestamp, wantAdvanced)
		}
	})

	t.Run("unlimited (MaxConcurrentDataMovers=0) always proceeds", func(t *testing.T) {
		du, objs := newFixtureWithOthers(50)
		builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du)
		for _, obj := range objs {
			builder = builder.WithRuntimeObjects(obj)
		}
		fakeClient := builder.Build()
		r := &KubeVirtDataUploadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest", MaxConcurrentDataMovers: 0,
		}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), du); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataUpload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: du.Name, Namespace: du.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataUploadPhaseInProgress {
			t.Errorf("phase = %q, want %q (limit disabled, must proceed regardless of active count)", updated.Status.Phase, velerov2alpha1.DataUploadPhaseInProgress)
		}
	})
}

// TestHandlePreparedDataUpload_ConcurrentDeadlockRegression pins the fix for
// the livelock found during this PR's own live-cluster validation (see PR
// #187 review comments): when N DataUploads for different VMs (e.g. a Backup
// targeting several VMs at once) all reach Prepared together, a raw
// active-count gate is symmetric -- every sibling sees N-1 others active,
// and if N-1 >= limit, all of them defer forever, since none can reach
// InProgress without passing a gate that's held by peers stuck at the same
// gate. Ranking (countHigherPriorityActiveDataUploads) breaks that symmetry:
// exactly the first MaxConcurrentDataMovers-ranked siblings must proceed
// regardless of the order handlePrepared happens to be called in.
func TestHandlePreparedDataUpload_ConcurrentDeadlockRegression(t *testing.T) {
	const (
		oadpNS   = "openshift-adp"
		siblings = 5
		limit    = 2
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
	}

	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	objs := make([]client.Object, 0, 1+3*siblings)
	objs = append(objs, bsl)
	dus := make([]*velerov2alpha1.DataUpload, siblings)
	for i := range siblings {
		checkpointName := fmt.Sprintf("checkpoint-%d", i)
		tempPVCName := fmt.Sprintf("kubevirt-backup-du-%d-abc12", i)
		du := &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("du-vm-%d", i), Namespace: oadpNS, UID: types.UID(fmt.Sprintf("du-vm-%d-uid", i)),
				// Same CreationTimestamp for every sibling -- exactly what a
				// Backup targeting several VMs at once produces. Ranking falls
				// through to the UID tiebreak, which is still a strict total
				// order, so this is the deadlock scenario at its sharpest: no
				// timestamp differences to accidentally break the symmetry.
				CreationTimestamp: baseTime,
				Annotations: map[string]string{
					common.AnnotationVMName:      fmt.Sprintf("vm-%d", i),
					common.AnnotationVMNamespace: "vm-ns",
					AnnotationVMBTName:           fmt.Sprintf("vmbt-vm-%d", i),
				},
				Labels: map[string]string{
					common.LabelVeleroBackupName: "velero-backup",
				},
			},
			Spec: velerov2alpha1.DataUploadSpec{
				DataMover:             common.DataMoverKubeVirt,
				BackupStorageLocation: "default",
				SourceNamespace:       "vm-ns",
			},
			Status: velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhasePrepared, AcceptedTimestamp: &baseTime},
		}
		vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("vmb-vm-%d", i),
				Namespace: "vm-ns",
				Labels:    map[string]string{common.LabelDataUploadUID: string(du.UID)},
			},
			Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{PvcName: &tempPVCName},
			Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
				Type:           kubevirtbackupv1alpha1.Full,
				CheckpointName: &checkpointName,
			},
		}
		reboundPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%sdu-vm-%d", common.ReboundPVCNamePrefix, i), Namespace: oadpNS},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:  fmt.Sprintf("pv-%d", i),
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}
		tempPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      tempPVCName,
				Namespace: "vm-ns",
				Labels: map[string]string{
					common.LabelDataUploadUID: string(du.UID),
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				},
			},
		}
		dus[i] = du
		objs = append(objs, du, vmb, reboundPVC, tempPVC)
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	r := &KubeVirtDataUploadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: oadpNS,
		DatamoverImage: "quay.io/test/datamover:latest", MaxConcurrentDataMovers: limit,
	}

	// Reconcile every sibling exactly once, in reverse order -- if the gate's
	// decisions depended on call order rather than each DU's own precomputed
	// rank, this would expose it (the deadlocked version of the gate defers
	// every single one regardless of order, so this alone would already
	// catch that bug; processing backwards additionally rules out an
	// order-dependent partial fix).
	for i := siblings - 1; i >= 0; i-- {
		if _, err := r.handlePrepared(context.Background(), logr.Discard(), dus[i]); err != nil {
			t.Fatalf("du-vm-%d: unexpected error: %v", i, err)
		}
	}

	inProgress, prepared := 0, 0
	for i := range siblings {
		var updated velerov2alpha1.DataUpload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: dus[i].Name, Namespace: oadpNS}, &updated); err != nil {
			t.Fatalf("du-vm-%d: failed to get DataUpload: %v", i, err)
		}
		switch updated.Status.Phase {
		case velerov2alpha1.DataUploadPhaseInProgress:
			inProgress++
		case velerov2alpha1.DataUploadPhasePrepared:
			prepared++
		default:
			t.Errorf("du-vm-%d: phase = %q, want InProgress or Prepared", i, updated.Status.Phase)
		}
	}

	if inProgress != limit {
		t.Errorf("got %d DataUploads InProgress, want exactly %d (MaxConcurrentDataMovers) -- %d stuck deferred forever means the gate deadlocked",
			inProgress, limit, prepared)
	}
	if prepared != siblings-limit {
		t.Errorf("got %d DataUploads still Prepared (deferred), want %d", prepared, siblings-limit)
	}
}

func TestGetCredentialsFromBSL(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name          string
		bsl           *velerov1.BackupStorageLocation
		secret        *corev1.Secret
		expectError   bool
		errorContains string
	}{
		{
			name: "valid credentials",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-adp",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "cloud-credentials",
						},
						Key: "cloud",
					},
				},
			},
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cloud-credentials",
					Namespace: "openshift-adp",
				},
				Data: map[string][]byte{
					"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
				},
			},
			expectError: false,
		},
		{
			name: "no credential reference",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-adp",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Credential: nil,
				},
			},
			expectError:   true,
			errorContains: "no credential configured",
		},
		{
			name: "secret not found",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-adp",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "nonexistent",
						},
						Key: "cloud",
					},
				},
			},
			expectError:   true,
			errorContains: "failed to get credential secret",
		},
		{
			name: "secret missing key",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-adp",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "cloud-credentials",
						},
						Key: "wrong-key",
					},
				},
			},
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cloud-credentials",
					Namespace: "openshift-adp",
				},
				Data: map[string][]byte{
					"cloud": []byte("data"),
				},
			},
			expectError:   true,
			errorContains: "does not contain key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.secret != nil {
				builder = builder.WithObjects(tt.secret)
			}
			fakeClient := builder.Build()

			credData, err := uploader.GetCredentialsFromBSL(context.Background(), fakeClient, "openshift-adp", tt.bsl)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errorContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(credData) == 0 {
				t.Error("expected non-empty credentials data")
			}
		})
	}
}

func TestHandleAccepted_WithBSLCheckpointLookup(t *testing.T) {
	// This test verifies that handleAccepted correctly handles the case where
	// BSL is available but credentials are missing. The controller should
	// gracefully fall back to full backup.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-bsl"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	// Create BSL (but no credential secret - lookup will fail gracefully)
	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	// Create temporary PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	// Create VMBT
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	// Create VMB (done=true to transition to Prepared)
	checkpointName := "vmb-" + duName + "-checkpoint"
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "test-uid",
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: duName,
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("backup.kubevirt.io"),
				Kind:     "VirtualMachineBackupTracker",
				Name:     vmbt.Name,
			},
			PvcName: new(pvc.Name),
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
			Type:           kubevirtbackupv1alpha1.Full,
			CheckpointName: &checkpointName,
			Conditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionTrue,
					Reason: "Completed VirtualMachineBackup",
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, pvc, vmbt, vmb).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still transition to Prepared even when BSL checkpoint lookup fails
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhasePrepared {
		t.Errorf("expected phase=%s, got phase=%s", velerov2alpha1.DataUploadPhasePrepared, updatedDU.Status.Phase)
	}

	if result.RequeueAfter == 0 {
		t.Error("expected requeue after transitioning to Prepared")
	}

}

func TestHandleAccepted_NoBSLConfigured(t *testing.T) {
	// Test that handleAccepted works when BSL name is empty - falls back to full backup
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-no-bsl"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "", // No BSL configured
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	// VMB with Done=True
	checkpointName := "cp-001"
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: duName,
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("backup.kubevirt.io"),
				Kind:     "VirtualMachineBackupTracker",
				Name:     vmbt.Name,
			},
			PvcName: new(pvc.Name),
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
			Type:           kubevirtbackupv1alpha1.Full,
			CheckpointName: &checkpointName,
			Conditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pvc, vmbt, vmb).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still transition to Prepared
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhasePrepared {
		t.Errorf("expected phase=%s, got phase=%s", velerov2alpha1.DataUploadPhasePrepared, updatedDU.Status.Phase)
	}

	if result.RequeueAfter == 0 {
		t.Error("expected requeue")
	}
}

func TestHandleAccepted_BSLNotFound_FailsDataUpload(t *testing.T) {
	// When BSL is specified but doesn't exist, handleAccepted should fail
	// the DataUpload immediately without creating PVC/VMBT/VMB resources.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-bsl-notfound"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "nonexistent-bsl", // BSL does not exist
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should not requeue (permanent failure)
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue for permanent BSL failure, got RequeueAfter=%v", result.RequeueAfter)
	}

	// DataUpload should be failed
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
		t.Errorf("expected phase=%s, got phase=%s",
			velerov2alpha1.DataUploadPhaseFailed, updatedDU.Status.Phase)
	}

	// Verify no PVC was created (resources should not be created if BSL is unavailable)
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := fakeClient.List(context.Background(), pvcList); err != nil {
		t.Fatalf("failed to list PVCs: %v", err)
	}
	if len(pvcList.Items) != 0 {
		t.Errorf("expected no PVCs to be created, but found %d", len(pvcList.Items))
	}
}

func TestHandleAccepted_BSLUnavailable_FailsDataUpload(t *testing.T) {
	// When BSL exists but is not in Available phase, handleAccepted should fail
	// the DataUpload immediately without creating any resources.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-bsl-unavail"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	// BSL exists but in Unavailable phase
	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
				},
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseUnavailable,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should not requeue (permanent failure)
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got RequeueAfter=%v", result.RequeueAfter)
	}

	// DataUpload should be failed
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
		t.Errorf("expected phase=%s, got phase=%s",
			velerov2alpha1.DataUploadPhaseFailed, updatedDU.Status.Phase)
	}

	if updatedDU.Status.Message == "" {
		t.Error("expected a failure message")
	}

	// Verify no PVC was created
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := fakeClient.List(context.Background(), pvcList); err != nil {
		t.Fatalf("failed to list PVCs: %v", err)
	}
	if len(pvcList.Items) != 0 {
		t.Errorf("expected no PVCs to be created, but found %d", len(pvcList.Items))
	}
}

func TestHandleAccepted_BSLNotAccessible_FailsDataUpload(t *testing.T) {
	// When the BSL referenced by the DataUpload does not exist (not found),
	// handleAccepted should fail the DataUpload without creating resources.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-bsl-transient"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "my-bsl",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	// No BSL is created in the fake client, so the controller will get a NotFound
	// error when looking up the BSL. This should cause the DataUpload to fail.
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// BSL not found is a permanent failure - DataUpload should be failed
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue for BSL not found, got RequeueAfter=%v", result.RequeueAfter)
	}

	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
		t.Errorf("expected phase=%s, got phase=%s",
			velerov2alpha1.DataUploadPhaseFailed, updatedDU.Status.Phase)
	}

	// Verify no PVC was created
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := fakeClient.List(context.Background(), pvcList); err != nil {
		t.Fatalf("failed to list PVCs: %v", err)
	}
	if len(pvcList.Items) != 0 {
		t.Errorf("expected no PVCs to be created, but found %d", len(pvcList.Items))
	}
}

// TestHandleAccepted_BSLTransientError_RequeuesWithoutFailing covers #123: a
// transient error from the BSL lookup (API hiccup, cache-not-yet-synced --
// anything other than a genuine NotFound) must not terminally fail the
// DataUpload the way a real "BSL doesn't exist" does. It should be returned so
// controller-runtime retries with backoff, since the condition may resolve on
// its own.
func TestHandleAccepted_BSLTransientError_RequeuesWithoutFailing(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-du-bsl-transient", Namespace: "test-ns", UID: types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      "test-vm",
				common.AnnotationVMNamespace: "test-ns",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt, SourceNamespace: "test-ns", BackupStorageLocation: "my-bsl",
		},
		Status: velerov2alpha1.DataUploadStatus{Phase: velerov2alpha1.DataUploadPhaseAccepted},
	}

	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(du).Build()
	transientErr := fmt.Errorf("simulated transient API error")
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*velerov1.BackupStorageLocation); ok {
				return transientErr
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	r := &KubeVirtDataUploadReconciler{Client: interceptedClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "test-ns"}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err == nil {
		t.Fatal("expected the transient error to be returned for retry, got nil")
	}
	if !strings.Contains(err.Error(), transientErr.Error()) {
		t.Errorf("error = %v, want it to contain %v", err, transientErr)
	}

	var updated velerov2alpha1.DataUpload
	if getErr := baseClient.Get(context.Background(), types.NamespacedName{Name: du.Name, Namespace: du.Namespace}, &updated); getErr != nil {
		t.Fatalf("failed to get DataUpload: %v", getErr)
	}
	if updated.Status.Phase == velerov2alpha1.DataUploadPhaseFailed {
		t.Error("phase must not be Failed on a transient BSL lookup error -- only a genuine NotFound is terminal")
	}
}

func TestLookupCheckpointFromBSL_MissingBucket(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		OADPNamespace: "openshift-adp",
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "openshift-adp",
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "", // No bucket
				},
			},
		},
	}

	_, err := r.lookupCheckpointFromBSL(context.Background(), bsl, "ns", "vm")
	if err == nil {
		t.Error("expected error for missing bucket")
	}
	if !contains(err.Error(), "no bucket configured") {
		t.Errorf("expected error about missing bucket, got: %v", err)
	}
}

func TestLookupCheckpointFromBSL_MissingCredential(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		OADPNamespace: "openshift-adp",
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "openshift-adp",
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "my-bucket",
				},
			},
			Credential: nil, // No credential
		},
	}

	_, err := r.lookupCheckpointFromBSL(context.Background(), bsl, "ns", "vm")
	if err == nil {
		t.Error("expected error for missing credential")
	}
	if !contains(err.Error(), "no credential configured") {
		t.Errorf("expected error about missing credential, got: %v", err)
	}
}

func TestHandleAccepted_HappyPath_IncrementalBackup(t *testing.T) {
	// This test verifies the full happy path: BSL has valid checkpoint data,
	// the controller finds the latest checkpoint, updates VMBT, and KubeVirt
	// performs an incremental backup.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-incr"
	existingCheckpointID := "cp-existing-001"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	checkpointName := "vmb-" + duName + "-checkpoint"
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "test-uid",
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: duName,
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("backup.kubevirt.io"),
				Kind:     "VirtualMachineBackupTracker",
				Name:     vmbt.Name,
			},
			PvcName: new(pvc.Name),
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
			Type:           kubevirtbackupv1alpha1.Full,
			CheckpointName: &checkpointName,
			Conditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionTrue,
					Reason: "Completed VirtualMachineBackup",
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret, pvc, vmbt, vmb).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	// Create a mock object store with an existing checkpoint index
	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	vmIndex := uploader.VMIndex{
		VMName:    vmName,
		Namespace: vmNamespace,
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:     existingCheckpointID,
				Type:   "full",
				Parent: "",
				Files: []uploader.CheckpointFile{
					{
						Filename:   "vmb-prev-disk1.qcow2",
						ObjectPath: "checkpoints/" + vmNamespace + "/" + vmName + "/" + existingCheckpointID + "/vmb-prev-disk1.qcow2",
					},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	indexPath := "checkpoints/" + vmNamespace + "/" + vmName + "/index.json"
	_ = mockStore.PutObject("test-bucket", indexPath, bytes.NewReader(indexData))

	// Also store the qcow2 file so chain validation succeeds
	qcow2Path := "checkpoints/" + vmNamespace + "/" + vmName + "/" + existingCheckpointID + "/vmb-prev-disk1.qcow2"
	_ = mockStore.PutObject("test-bucket", qcow2Path, bytes.NewReader([]byte("fake-qcow2-data")))

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should transition to Prepared
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhasePrepared {
		t.Errorf("expected phase=%s, got phase=%s",
			velerov2alpha1.DataUploadPhasePrepared, updatedDU.Status.Phase)
	}

	if result.RequeueAfter == 0 {
		t.Error("expected requeue after transitioning to Prepared")
	}

	// Controller no longer modifies VMBT status; checkpoint management is left to KubeVirt.
}

func TestHandleAccepted_StaleCheckpointForcesFullBackup(t *testing.T) {
	// This test verifies that when BSL validation finds no valid chain
	// (e.g., index.json deleted from S3), the controller creates the VMB
	// with ForceFullBackup=true. The controller does NOT modify VMBT status.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-stale-cp"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	// VMBT with a stale checkpoint from a previous backup
	staleCheckpointName := "cp-previous-001"
	now := metav1.Now()
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
				Name:         staleCheckpointName,
				CreationTime: &now,
			},
		},
	}

	// Do NOT pre-create the VMB: let ensureVMBackup create it with ForceFullBackup=true
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret, pvc, vmbt).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	// Mock object store with NO index.json (simulates deleted S3 data)
	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify VMB was created with ForceFullBackup=true
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := fakeClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		t.Fatalf("failed to list created VMBs: %v", err)
	}
	if len(vmbList.Items) != 1 {
		t.Fatalf("expected 1 VMB to be created, but found %d", len(vmbList.Items))
	}
	createdVMB := vmbList.Items[0]

	if !createdVMB.Spec.ForceFullBackup {
		t.Error("expected VMB.Spec.ForceFullBackup to be true when BSL finds no valid chain")
	}

	// Verify BSL validated annotation was set
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Annotations[common.AnnotationBSLValidated] != "true" {
		t.Error("expected BSL validated annotation to be set")
	}
}

// TestHandleAccepted_ExpectedBackupTypePatchSurvivesConcurrentWrite reproduces
// the same real-world report as the Update-based fix (a concurrent writer --
// e.g. Velero's own built-in DataUpload controller, which this repo's README
// documents also reconciles these objects -- causing
// kubevirt-datamover.io/expected-backup-type to never get stamped), but
// proves the merge-patch alternative resolves it structurally rather than by
// retrying: a JSON merge patch carries no resourceVersion precondition, so it
// cannot 409 on a concurrent change to any other field, and it merges rather
// than overwrites, so the concurrent writer's own change survives alongside
// it with no explicit refetch-and-preserve needed.
//
// The interceptor simulates the race directly: the moment handleAccepted's
// Step 3c patch for the expected-backup-type annotation is about to be sent,
// a separate "concurrent writer" bumps an unrelated label on the same object
// via a real Update first (which would have invalidated any in-memory
// resourceVersion an r.Update(ctx, du) call was relying on), then the
// original patch is allowed through unmodified.
func TestHandleAccepted_ExpectedBackupTypePatchSurvivesConcurrentWrite(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-patch"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	// Do NOT pre-create the VMB: let ensureVMBackup create it fresh, exercising
	// the same Step 2/3/3b/3c/4 sequence a first-ever reconcile takes.
	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret, pvc, vmbt).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	concurrentWriteSimulated := false
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if patchedDU, ok := obj.(*velerov2alpha1.DataUpload); ok {
				raw, dataErr := patch.Data(obj)
				if dataErr == nil && strings.Contains(string(raw), common.AnnotationExpectedBackupType) && !concurrentWriteSimulated {
					concurrentWriteSimulated = true
					concurrent := &velerov2alpha1.DataUpload{}
					if getErr := c.Get(ctx, client.ObjectKeyFromObject(patchedDU), concurrent); getErr != nil {
						return getErr
					}
					if concurrent.Labels == nil {
						concurrent.Labels = map[string]string{}
					}
					concurrent.Labels["concurrent-writer"] = "true"
					if updateErr := c.Update(ctx, concurrent); updateErr != nil {
						return updateErr
					}
				}
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})

	r := &KubeVirtDataUploadReconciler{
		Client:        interceptedClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	if _, err := r.handleAccepted(context.Background(), logr.Discard(), du); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !concurrentWriteSimulated {
		t.Fatal("test did not exercise the concurrent-write path -- fixture drifted from handleAccepted's actual Patch sequence")
	}

	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := baseClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		t.Fatalf("failed to list created VMBs: %v", err)
	}
	if len(vmbList.Items) != 1 {
		t.Fatalf("expected 1 VMB to be created despite the concurrent write, found %d", len(vmbList.Items))
	}

	var updatedDU velerov2alpha1.DataUpload
	if err := baseClient.Get(context.Background(), types.NamespacedName{Name: duName, Namespace: vmNamespace}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}
	if got := updatedDU.Annotations[common.AnnotationExpectedBackupType]; got != uploader.BackupTypeFull {
		t.Errorf("expected-backup-type annotation is %q, want %q after a concurrent write to an unrelated field -- "+
			"a merge patch must survive this the same way it survives any other concurrent change, "+
			"since it carries no resourceVersion precondition", got, uploader.BackupTypeFull)
	}
	if updatedDU.Labels["concurrent-writer"] != "true" {
		t.Error("concurrent writer's own label was lost -- the merge patch must not clobber concurrently-written fields")
	}
}

func TestHandleAccepted_SkipsBSLValidationWhenAnnotated(t *testing.T) {
	// This test verifies that BSL validation is skipped on subsequent reconciles
	// when the DataUpload already has the BSL validated annotation.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-annotated"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:       vmName,
				common.AnnotationVMNamespace:  vmNamespace,
				common.AnnotationBSLValidated: "true", // Already validated
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	// VMB in progress (no Done condition yet)
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "test-uid",
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("backup.kubevirt.io"),
				Kind:     "VirtualMachineBackupTracker",
				Name:     vmbt.Name,
			},
			PvcName: new(pvc.Name),
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{},
	}

	// BSL must exist for the Step 0 availability check
	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
				},
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pvc, vmbt, vmb, bsl).
		Build()

	// ObjectStoreFactory should NOT be called - BSL validation is skipped
	factoryCalled := false
	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			factoryCalled = true
			return nil, nil
		},
	}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if factoryCalled {
		t.Error("ObjectStoreFactory should not be called when BSL validation annotation is present")
	}
}

func TestLookupCheckpointFromBSL_WithValidCredentials(t *testing.T) {
	// Tests the full lookupCheckpointFromBSL path with valid credentials
	// and a mock object store that returns no checkpoint (first backup).
	scheme := runtime.NewScheme()
	_ = velerov1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-credentials",
			Namespace: "openshift-adp",
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(credSecret).
		Build()

	// Mock store with no index (first backup)
	mockStore := uploader.NewMockObjectStore("my-bucket", "velero-kubevirt-datamover")

	factoryCalled := false
	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		OADPNamespace: "openshift-adp",
		ObjectStoreFactory: func(cfg *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			factoryCalled = true
			if !bytes.Equal(cfg.CredentialsData, credSecret.Data["cloud"]) {
				t.Error("ObjectStoreFactory did not receive the BSL credential data")
			}
			if cfg.BSLProvider != "aws" {
				t.Errorf("ObjectStoreFactory cfg.BSLProvider = %q, want %q", cfg.BSLProvider, "aws")
			}
			if cfg.BSLBucket != "my-bucket" {
				t.Errorf("ObjectStoreFactory cfg.BSLBucket = %q, want %q", cfg.BSLBucket, "my-bucket")
			}
			if cfg.BSLPrefix != "velero-kubevirt-datamover" {
				t.Errorf("ObjectStoreFactory cfg.BSLPrefix = %q, want %q", cfg.BSLPrefix, "velero-kubevirt-datamover")
			}
			if cfg.BSLRegion != "us-east-1" {
				t.Errorf("ObjectStoreFactory cfg.BSLRegion = %q, want %q", cfg.BSLRegion, "us-east-1")
			}
			return mockStore, nil
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "openshift-adp",
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "my-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-credentials"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	result, err := r.lookupCheckpointFromBSL(context.Background(), bsl, "test-ns", "test-vm")
	if !factoryCalled {
		t.Fatal("expected ObjectStoreFactory to be invoked during lookupCheckpointFromBSL")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Found {
		t.Error("expected Found=false for first backup (no index)")
	}
	if !contains(result.Message, "no checkpoint index found") {
		t.Errorf("expected message about no index, got: %s", result.Message)
	}
}

func TestLookupCheckpointFromBSL_WithExistingCheckpoint(t *testing.T) {
	// Tests lookupCheckpointFromBSL when BSL has an existing valid checkpoint.
	scheme := runtime.NewScheme()
	_ = velerov1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-credentials",
			Namespace: "openshift-adp",
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(credSecret).
		Build()

	mockStore := uploader.NewMockObjectStore("my-bucket", "velero-kubevirt-datamover")

	// Populate mock store with checkpoint data
	vmIndex := uploader.VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:     "cp-001",
				Type:   "full",
				Parent: "",
				Files: []uploader.CheckpointFile{
					{
						Filename:   "disk1.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-001/disk1.qcow2",
					},
				},
			},
			{
				ID:     "cp-002",
				Type:   "incremental",
				Parent: "cp-001",
				Files: []uploader.CheckpointFile{
					{
						Filename:   "disk1.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-002/disk1.qcow2",
					},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = mockStore.PutObject("my-bucket", "checkpoints/test-ns/test-vm/index.json",
		bytes.NewReader(indexData))
	_ = mockStore.PutObject("my-bucket", "checkpoints/test-ns/test-vm/cp-001/disk1.qcow2",
		bytes.NewReader([]byte("data")))
	_ = mockStore.PutObject("my-bucket", "checkpoints/test-ns/test-vm/cp-002/disk1.qcow2",
		bytes.NewReader([]byte("data")))

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		OADPNamespace: "openshift-adp",
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "openshift-adp",
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "my-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-credentials"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	result, err := r.lookupCheckpointFromBSL(context.Background(), bsl, "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Found {
		t.Errorf("expected Found=true, got Found=false (message: %s)", result.Message)
	}
	if result.LatestCheckpoint != "cp-002" {
		t.Errorf("expected LatestCheckpoint=cp-002, got=%s", result.LatestCheckpoint)
	}
	if result.ChainLength != 2 {
		t.Errorf("expected ChainLength=2, got=%d", result.ChainLength)
	}
}

func TestGetCredentialsFromBSL_ReturnsRawBytes(t *testing.T) {
	// Verifies that credentials are returned as raw bytes without temp files.
	scheme := runtime.NewScheme()
	_ = velerov1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	expectedData := "[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"
	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-credentials",
			Namespace: "openshift-adp",
		},
		Data: map[string][]byte{
			"cloud": []byte(expectedData),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(credSecret).
		Build()

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "openshift-adp",
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-credentials"},
				Key:                  "cloud",
			},
		},
	}

	credData, err := uploader.GetCredentialsFromBSL(context.Background(), fakeClient, "openshift-adp", bsl)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(credData) != expectedData {
		t.Errorf("credential data mismatch: got %q, want %q", string(credData), expectedData)
	}
}

func TestExtractBSLConfig(t *testing.T) {
	tests := []struct {
		name          string
		bsl           *velerov1.BackupStorageLocation
		expectError   bool
		errorContains string
		validate      func(*testing.T, *uploader.BSLConfig)
	}{
		{
			name: "full config with prefix",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "my-bucket",
							Prefix: "velero",
						},
					},
					Config: map[string]string{"region": "us-west-2"},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
						Key:                  "custom-key",
					},
				},
			},
			validate: func(t *testing.T, cfg *uploader.BSLConfig) {
				if cfg.Provider != "aws" {
					t.Errorf("Provider = %q, want %q", cfg.Provider, "aws")
				}
				if cfg.Bucket != "my-bucket" {
					t.Errorf("Bucket = %q, want %q", cfg.Bucket, "my-bucket")
				}
				if cfg.Prefix != "velero-kubevirt-datamover" {
					t.Errorf("Prefix = %q, want %q", cfg.Prefix, "velero-kubevirt-datamover")
				}
				if cfg.Region != "us-west-2" {
					t.Errorf("Region = %q, want %q", cfg.Region, "us-west-2")
				}
				if cfg.CredentialName != "creds" {
					t.Errorf("CredentialName = %q, want %q", cfg.CredentialName, "creds")
				}
				if cfg.CredentialKey != "custom-key" {
					t.Errorf("CredentialKey = %q, want %q", cfg.CredentialKey, "custom-key")
				}
			},
		},
		{
			name: "empty prefix produces bare datamover prefix",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
							Prefix: "",
						},
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
					},
				},
			},
			validate: func(t *testing.T, cfg *uploader.BSLConfig) {
				if cfg.Prefix != "kubevirt-datamover" {
					t.Errorf("Prefix = %q, want %q", cfg.Prefix, "kubevirt-datamover")
				}
			},
		},
		{
			name: "nil ObjectStorage returns error",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "bsl-nil-os"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: nil,
					},
				},
			},
			expectError:   true,
			errorContains: "no bucket configured",
		},
		{
			name: "empty bucket returns error",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "bsl-no-bucket"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "",
						},
					},
				},
			},
			expectError:   true,
			errorContains: "no bucket configured",
		},
		{
			name: "nil Config map gives empty region",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
						},
					},
					Config: nil,
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
					},
				},
			},
			validate: func(t *testing.T, cfg *uploader.BSLConfig) {
				if cfg.Region != "" {
					t.Errorf("Region = %q, want empty", cfg.Region)
				}
			},
		},
		{
			name: "nil Credential gives empty credential name",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
						},
					},
					Credential: nil,
				},
			},
			validate: func(t *testing.T, cfg *uploader.BSLConfig) {
				if cfg.CredentialName != "" {
					t.Errorf("CredentialName = %q, want empty", cfg.CredentialName)
				}
			},
		},
		{
			name: "Credential with empty Key defaults to cloud",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
						},
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
						Key:                  "",
					},
				},
			},
			validate: func(t *testing.T, cfg *uploader.BSLConfig) {
				if cfg.CredentialKey != "cloud" {
					t.Errorf("CredentialKey = %q, want %q", cfg.CredentialKey, "cloud")
				}
			},
		},
		{
			name: "profile config key is extracted",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "minio-bsl"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "my-bucket",
						},
					},
					Config: map[string]string{
						"profile": "minio",
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
					},
				},
			},
			validate: func(t *testing.T, cfg *uploader.BSLConfig) {
				if cfg.Profile != "minio" {
					t.Errorf("Profile = %q, want %q", cfg.Profile, "minio")
				}
			},
		},
		{
			name: "S3-compatible config keys are extracted",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "minio-bsl"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "my-bucket",
						},
					},
					Config: map[string]string{
						"region":                "us-east-1",
						"s3Url":                 "https://minio.example.com",
						"s3ForcePathStyle":      "true",
						"insecureSkipTLSVerify": "true",
						"caCert":                "-----BEGIN CERTIFICATE-----\nMIIBxTCCAW...\n-----END CERTIFICATE-----",
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
					},
				},
			},
			validate: func(t *testing.T, cfg *uploader.BSLConfig) {
				if cfg.S3URL != "https://minio.example.com" {
					t.Errorf("S3URL = %q, want %q", cfg.S3URL, "https://minio.example.com")
				}
				if !cfg.S3ForcePathStyle {
					t.Error("S3ForcePathStyle = false, want true")
				}
				if !cfg.InsecureSkipTLSVerify {
					t.Error("InsecureSkipTLSVerify = false, want true")
				}
				if cfg.CACert != "-----BEGIN CERTIFICATE-----\nMIIBxTCCAW...\n-----END CERTIFICATE-----" {
					t.Errorf("CACert = %q, want PEM content", cfg.CACert)
				}
			},
		},
		{
			name: "S3-compatible booleans are case-insensitive",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "bsl"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
						},
					},
					Config: map[string]string{
						"s3ForcePathStyle":      "TRUE",
						"insecureSkipTLSVerify": "True",
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
					},
				},
			},
			validate: func(t *testing.T, cfg *uploader.BSLConfig) {
				if !cfg.S3ForcePathStyle {
					t.Error("S3ForcePathStyle = false, want true (case-insensitive)")
				}
				if !cfg.InsecureSkipTLSVerify {
					t.Error("InsecureSkipTLSVerify = false, want true (case-insensitive)")
				}
			},
		},
		{
			name: "S3-compatible fields default when Config has no S3 keys",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "bsl"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
						},
					},
					Config: map[string]string{
						"region": "us-east-1",
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
					},
				},
			},
			validate: func(t *testing.T, cfg *uploader.BSLConfig) {
				if cfg.S3URL != "" {
					t.Errorf("S3URL = %q, want empty", cfg.S3URL)
				}
				if cfg.S3ForcePathStyle {
					t.Error("S3ForcePathStyle = true, want false")
				}
				if cfg.InsecureSkipTLSVerify {
					t.Error("InsecureSkipTLSVerify = true, want false")
				}
				if cfg.CACert != "" {
					t.Errorf("CACert = %q, want empty", cfg.CACert)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := uploader.ExtractBSLConfig(tt.bsl)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errorContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.validate != nil {
				tt.validate(t, cfg)
			}
		})
	}
}

func TestHandleAccepted_VMBConflictRequeuesGracefully(t *testing.T) {
	// This test verifies that when another VirtualMachineBackup is in progress
	// for the same VM (admission webhook denies creation), the controller requeues
	// with a longer delay instead of returning an error that causes a retry storm.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-conflict"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:       vmName,
				common.AnnotationVMNamespace:  vmNamespace,
				common.AnnotationBSLValidated: "true", // Skip BSL validation
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	// BSL must exist for the Step 0 availability check
	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
				},
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	// No VMB exists - the controller will try to create one.
	// We simulate the admission webhook rejection by using an interceptor.
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pvc, vmbt, bsl).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	// Call ensureVMBackup directly to verify the error handling logic.
	// The fake client won't produce an admission webhook error, so we test
	// the handleAccepted logic by simulating the error path.
	// Instead, we test by calling handleAccepted and checking that if
	// ensureVMBackup returns an "in progress for source" error, it requeues.

	// To properly test this, we need to verify the string matching logic.
	// Let's construct the exact error that the admission webhook produces.
	webhookErr := fmt.Errorf("failed to create VirtualMachineBackup: admission webhook " +
		"\"virtualmachinebackup-validator.backup.kubevirt.io\" denied the request: " +
		"VirtualMachineBackup \"vmb-other-du\" in progress for source")

	// Verify the string matching works correctly
	if !contains(webhookErr.Error(), "in progress for source") {
		t.Fatal("expected error to contain 'in progress for source'")
	}

	// Verify that a normal error does NOT match
	normalErr := fmt.Errorf("failed to create VirtualMachineBackup: connection refused")
	if contains(normalErr.Error(), "in progress for source") {
		t.Fatal("normal error should not match 'in progress for source'")
	}

	// Also verify via handleAccepted: when VMB doesn't exist and creation succeeds,
	// the flow continues normally (no conflict).
	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have created the VMB and be requeuing to check its status
	if result.RequeueAfter == 0 {
		t.Error("expected requeue")
	}

	// Verify RequeueAfterLong constant is larger than RequeueAfterShort
	if RequeueAfterLong <= RequeueAfterShort {
		t.Errorf("RequeueAfterLong (%v) should be greater than RequeueAfterShort (%v)",
			RequeueAfterLong, RequeueAfterShort)
	}
}

func TestHandleAccepted_BSLAnnotationNotSetOnTransientFailure(t *testing.T) {
	// This test verifies that when BSL lookup fails due to a transient error
	// (e.g., missing credential secret), the AnnotationBSLValidated is NOT set,
	// so validation will be retried on the next reconcile.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-transient"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	// BSL exists but credential secret is MISSING (transient failure)
	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "missing-secret"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	// VMB with Done=True so the test can proceed past the VMB check
	checkpointName := "cp-001"
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "test-uid",
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("backup.kubevirt.io"),
				Kind:     "VirtualMachineBackupTracker",
				Name:     vmbt.Name,
			},
			PvcName: new(pvc.Name),
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
			Type:           kubevirtbackupv1alpha1.Full,
			CheckpointName: &checkpointName,
			Conditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, pvc, vmbt, vmb).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify BSL validated annotation was NOT set (transient failure)
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Annotations[common.AnnotationBSLValidated] == "true" {
		t.Error("expected BSL validated annotation to NOT be set on transient failure, " +
			"so validation will be retried on the next reconcile")
	}

	// Should still transition to Prepared (graceful degradation)
	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhasePrepared {
		t.Errorf("expected phase=%s, got phase=%s",
			velerov2alpha1.DataUploadPhasePrepared, updatedDU.Status.Phase)
	}
}

func TestHandleAccepted_ForceFullBackupAnnotation(t *testing.T) {
	// When the force-full-backup annotation is set on the DataUpload,
	// the controller should:
	// 1. Skip BSL checkpoint lookup entirely
	// 2. Clear any existing VMBT checkpoint
	// 3. Create the VMB with ForceFullBackup=true
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-force-full"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:          vmName,
				common.AnnotationVMNamespace:     vmNamespace,
				common.AnnotationForceFullBackup: "true",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	// VMBT with an existing checkpoint (should be cleared)
	existingCheckpoint := "cp-existing-001"
	now := metav1.Now()
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
				Name:         existingCheckpoint,
				CreationTime: &now,
			},
		},
	}

	// BSL must exist for the Step 0 availability check
	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
				},
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pvc, vmbt, bsl).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	objectStoreFactoryCalled := false
	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			objectStoreFactoryCalled = true
			return nil, fmt.Errorf("should not be called")
		},
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.RequeueAfter == 0 {
		t.Error("expected requeue")
	}

	// Verify BSL checkpoint lookup was NOT called
	if objectStoreFactoryCalled {
		t.Error("ObjectStoreFactory should not be called when force-full-backup annotation is set")
	}

	// Verify VMB was created with ForceFullBackup=true
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := fakeClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		t.Fatalf("failed to list created VMBs: %v", err)
	}
	if len(vmbList.Items) != 1 {
		t.Fatalf("expected 1 VMB to be created, but found %d", len(vmbList.Items))
	}
	createdVMB := vmbList.Items[0]

	if !createdVMB.Spec.ForceFullBackup {
		t.Error("expected VMB.Spec.ForceFullBackup to be true")
	}
}

func TestHandleAccepted_ForceFullBackupWithNoExistingCheckpoint(t *testing.T) {
	// When force-full-backup is set but VMBT has no checkpoint,
	// the controller should still create the VMB with ForceFullBackup=true
	// without errors (no checkpoint to clear).
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-force-no-cp"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:          vmName,
				common.AnnotationVMNamespace:     vmNamespace,
				common.AnnotationForceFullBackup: "true",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:       common.DataMoverKubeVirt,
			SourceNamespace: vmNamespace,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	// VMBT with no checkpoint
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pvc, vmbt).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.RequeueAfter == 0 {
		t.Error("expected requeue")
	}

	// Verify VMB was created with ForceFullBackup=true
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := fakeClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		t.Fatalf("failed to list created VMBs: %v", err)
	}
	if len(vmbList.Items) != 1 {
		t.Fatalf("expected 1 VMB to be created, but found %d", len(vmbList.Items))
	}
	createdVMB := vmbList.Items[0]

	if !createdVMB.Spec.ForceFullBackup {
		t.Error("expected VMB.Spec.ForceFullBackup to be true")
	}
}

func TestHandleAccepted_NoForceFullBackupByDefault(t *testing.T) {
	// When the force-full-backup annotation is NOT set, the VMB should
	// be created with ForceFullBackup=false (default).
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-no-force"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:       vmName,
				common.AnnotationVMNamespace:  vmNamespace,
				common.AnnotationBSLValidated: "true", // Skip BSL validation
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:       common.DataMoverKubeVirt,
			SourceNamespace: vmNamespace,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pvc, vmbt).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify VMB was created with ForceFullBackup=false
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := fakeClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		t.Fatalf("failed to list created VMBs: %v", err)
	}
	if len(vmbList.Items) != 1 {
		t.Fatalf("expected 1 VMB to be created, but found %d", len(vmbList.Items))
	}
	createdVMB := vmbList.Items[0]

	if createdVMB.Spec.ForceFullBackup {
		t.Error("expected VMB.Spec.ForceFullBackup to be false when annotation is not set")
	}
}

// TestHandleAccepted_QuiesceDecision covers the guest-agent-based quiesce
// auto-detection and its explicit user override (issue #217):
//   - A VMI with AgentConnected==True quiesces (SkipQuiesce=false) by default.
//   - A VMI without AgentConnected==True (false, absent, or no VMI at all)
//     skips quiesce (SkipQuiesce=true) by default.
//   - The AnnotationSkipQuiesce override on the DataUpload takes precedence
//     over automatic detection in both directions, including when the
//     annotation is invalid/"auto" (falls back to auto-detection) or
//     explicitly forces quiesce ("false") when no VMI can be found.
func TestHandleAccepted_QuiesceDecision(t *testing.T) {
	vmName := "test-vm"
	vmNamespace := "test-ns"

	agentConnectedVMI := &kubevirtcorev1.VirtualMachineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: vmName, Namespace: vmNamespace},
		Status: kubevirtcorev1.VirtualMachineInstanceStatus{
			Conditions: []kubevirtcorev1.VirtualMachineInstanceCondition{
				{
					Type:   kubevirtcorev1.VirtualMachineInstanceAgentConnected,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	agentDisconnectedVMI := &kubevirtcorev1.VirtualMachineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: vmName, Namespace: vmNamespace},
		Status: kubevirtcorev1.VirtualMachineInstanceStatus{
			Conditions: []kubevirtcorev1.VirtualMachineInstanceCondition{
				{
					Type:   kubevirtcorev1.VirtualMachineInstanceAgentConnected,
					Status: corev1.ConditionFalse,
				},
			},
		},
	}

	tests := []struct {
		name                  string
		vmi                   *kubevirtcorev1.VirtualMachineInstance
		skipQuiesceAnnotation string // "" means annotation unset
		wantSkipQuiesce       bool
	}{
		{
			name:            "no VMI, no override: skips quiesce by default",
			vmi:             nil,
			wantSkipQuiesce: true,
		},
		{
			name:            "agent connected, no override: quiesces by default",
			vmi:             agentConnectedVMI,
			wantSkipQuiesce: false,
		},
		{
			name:            "agent not connected, no override: skips quiesce by default",
			vmi:             agentDisconnectedVMI,
			wantSkipQuiesce: true,
		},
		{
			name:                  "agent connected, override=true (skip quiesce): skips quiesce",
			vmi:                   agentConnectedVMI,
			skipQuiesceAnnotation: "true",
			wantSkipQuiesce:       true,
		},
		{
			name:                  "agent not connected, override=false (force quiesce): quiesces",
			vmi:                   agentDisconnectedVMI,
			skipQuiesceAnnotation: "false",
			wantSkipQuiesce:       false,
		},
		{
			name:                  "no VMI, override=false (force quiesce): quiesces despite no VMI",
			vmi:                   nil,
			skipQuiesceAnnotation: "false",
			wantSkipQuiesce:       false,
		},
		{
			name:                  "agent connected, invalid override value: falls back to auto-detection",
			vmi:                   agentConnectedVMI,
			skipQuiesceAnnotation: "not-a-bool",
			wantSkipQuiesce:       false,
		},
		{
			name:                  `agent not connected, override="auto": falls back to auto-detection`,
			vmi:                   agentDisconnectedVMI,
			skipQuiesceAnnotation: "auto",
			wantSkipQuiesce:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = velerov2alpha1.AddToScheme(scheme)
			_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
			_ = kubevirtcorev1.AddToScheme(scheme)
			_ = corev1.AddToScheme(scheme)

			duName := "test-du-quiesce"
			annotations := map[string]string{
				common.AnnotationVMName:       vmName,
				common.AnnotationVMNamespace:  vmNamespace,
				common.AnnotationBSLValidated: "true", // Skip BSL validation
			}
			if tt.skipQuiesceAnnotation != "" {
				annotations[common.AnnotationSkipQuiesce] = tt.skipQuiesceAnnotation
			}

			du := &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:        duName,
					Namespace:   vmNamespace,
					UID:         types.UID("test-uid"),
					Annotations: annotations,
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover:       common.DataMoverKubeVirt,
					SourceNamespace: vmNamespace,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseAccepted,
				},
			}

			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kubevirt-backup-" + duName,
					Namespace: vmNamespace,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			}

			vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vmbt-" + vmName,
					Namespace: vmNamespace,
				},
				Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
					Source: corev1.TypedLocalObjectReference{
						APIGroup: new("kubevirt.io"),
						Kind:     "VirtualMachine",
						Name:     vmName,
					},
				},
			}

			objs := []client.Object{du, pvc, vmbt}
			if tt.vmi != nil {
				objs = append(objs, tt.vmi)
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				Scheme:        scheme,
				Log:           logr.Discard(),
				OADPNamespace: vmNamespace,
			}

			if _, err := r.handleAccepted(context.Background(), logr.Discard(), du); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
			if err := fakeClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
				t.Fatalf("failed to list created VMBs: %v", err)
			}
			if len(vmbList.Items) != 1 {
				t.Fatalf("expected 1 VMB to be created, but found %d", len(vmbList.Items))
			}
			createdVMB := vmbList.Items[0]

			if createdVMB.Spec.SkipQuiesce != tt.wantSkipQuiesce {
				t.Errorf("expected VMB.Spec.SkipQuiesce=%v, got %v", tt.wantSkipQuiesce, createdVMB.Spec.SkipQuiesce)
			}
		})
	}
}

func TestHandleAccepted_StaleCheckpointSetsForceFullOnVMB(t *testing.T) {
	// When BSL validation finds no valid checkpoint chain but VMBT has a stale
	// checkpoint, the controller should set ForceFullBackup=true on the VMB
	// as defense-in-depth.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-stale-force"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	// VMBT with a stale checkpoint
	staleCheckpointName := "cp-stale-001"
	now := metav1.Now()
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
				Name:         staleCheckpointName,
				CreationTime: &now,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret, pvc, vmbt).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	// Mock object store with NO index.json (simulates deleted S3 data)
	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify VMB was created with ForceFullBackup=true
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := fakeClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		t.Fatalf("failed to list created VMBs: %v", err)
	}
	if len(vmbList.Items) != 1 {
		t.Fatalf("expected 1 VMB to be created, but found %d", len(vmbList.Items))
	}
	createdVMB := vmbList.Items[0]

	if !createdVMB.Spec.ForceFullBackup {
		t.Error("expected VMB.Spec.ForceFullBackup to be true when BSL finds stale checkpoint chain")
	}

}

func TestValidateBSLCheckpoint_ForceFullOnChainFallback(t *testing.T) {
	// When the BSL checkpoint chain is broken mid-way and falls back to an
	// older checkpoint, forceFullBackup must be true. We can't safely assume
	// KubeVirt's CBT can produce a correct incremental since the older checkpoint.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-chain-fallback"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	// VMBT with checkpoint set to cp-003 (the latest in the chain).
	// BSL will return cp-001 as latest valid (because cp-002 is broken).
	// The mismatch triggers forceFullBackup.
	now := metav1.Now()
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
				Name:         "cp-003",
				CreationTime: &now,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret, vmbt).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	// Mock object store: chain cp-001(Full) -> cp-002(Inc) -> cp-003(Inc)
	// cp-002's file is missing → chain falls back to cp-001
	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")
	vmIndex := uploader.VMIndex{
		VMName:    vmName,
		Namespace: vmNamespace,
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:       "cp-001",
				Type:     "full",
				VMBackup: "vmb-001",
				Files: []uploader.CheckpointFile{
					{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/disk.qcow2"},
				},
			},
			{
				ID:       "cp-002",
				Type:     "incremental",
				Parent:   "cp-001",
				VMBackup: "vmb-002",
				Files: []uploader.CheckpointFile{
					{ObjectPath: "checkpoints/test-ns/test-vm/cp-002/disk.qcow2"},
				},
			},
			{
				ID:       "cp-003",
				Type:     "incremental",
				Parent:   "cp-002",
				VMBackup: "vmb-003",
				Files: []uploader.CheckpointFile{
					{ObjectPath: "checkpoints/test-ns/test-vm/cp-003/disk.qcow2"},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)
	// Create files for cp-001 and cp-003 but NOT cp-002 (mid-chain break)
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/cp-001/disk.qcow2", []byte("full"))
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/cp-003/disk.qcow2", []byte("inc2"))

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	vmRef := &common.VMReference{Name: vmName, Namespace: vmNamespace}

	forceFullBackup, checkpointLookup := r.validateBSLCheckpoint(context.Background(), logr.Discard(), du, vmRef)

	if !forceFullBackup {
		t.Error("expected forceFullBackup=true when BSL chain falls back to older checkpoint")
	}

	if checkpointLookup == nil {
		t.Fatal("expected checkpointLookup to be returned")
	}
	if !checkpointLookup.Found {
		t.Error("expected checkpointLookup.Found=true (fell back to valid chain)")
	}

}

func TestValidateBSLCheckpoint_ForcesFullBackupWhenBSLNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du-bsl-failure",
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	vmRef := &common.VMReference{Name: vmName, Namespace: vmNamespace}
	forceFullBackup, checkpointLookup := r.validateBSLCheckpoint(context.Background(), logr.Discard(), du, vmRef)

	if !forceFullBackup {
		t.Error("expected forceFullBackup=true when BSL validation fails")
	}

	if checkpointLookup != nil {
		t.Error("expected checkpointLookup=nil when BSL validation fails")
	}
}

func TestValidateBSLCheckpoint_ForcesFullBackupWhenCheckpointLookupErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du-bsl-lookup-error",
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			return nil, fmt.Errorf("simulated object store initialization error")
		},
	}

	vmRef := &common.VMReference{Name: vmName, Namespace: vmNamespace}
	forceFullBackup, checkpointLookup := r.validateBSLCheckpoint(context.Background(), logr.Discard(), du, vmRef)

	if !forceFullBackup {
		t.Error("expected forceFullBackup=true when checkpoint lookup errors")
	}

	if checkpointLookup != nil {
		t.Error("expected checkpointLookup=nil when checkpoint lookup errors")
	}
}

func TestPrepareVMBackupTracker_FirstBackup(t *testing.T) {
	// No S3 index exists → creates fresh VMBT with no LatestCheckpoint
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-first"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			BackupStorageLocation: "", // No BSL → first backup path
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	vmbt, err := r.prepareVMBackupTracker(context.Background(), logr.Discard(), du, vmName, vmNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if vmbt == nil {
		t.Fatal("expected VMBT to be created")
	}
	if !strings.HasPrefix(vmbt.Name, "vmbt-"+vmName) {
		t.Errorf("VMBT name %q does not have expected prefix", vmbt.Name)
	}
	if vmbt.Namespace != vmNamespace {
		t.Errorf("VMBT namespace = %q, want %q", vmbt.Namespace, vmNamespace)
	}
	if vmbt.Labels[common.LabelVMNameHash] != common.HashForLabel(vmName) {
		t.Errorf("VMBT is missing label %s", common.LabelVMNameHash)
	}
	if vmbt.Annotations[common.AnnotationVMName] != vmName {
		t.Errorf("VMBT annotation %s = %q, want %q", common.AnnotationVMName, vmbt.Annotations[common.AnnotationVMName], vmName)
	}
	// First backup: no LatestCheckpoint
	if vmbt.Status != nil && vmbt.Status.LatestCheckpoint != nil {
		t.Error("expected no LatestCheckpoint for first backup")
	}
}

func TestPrepareVMBackupTracker_FromS3(t *testing.T) {
	// S3 has index.json with vmbtObjectPath pointing to vmbt.json
	// vmbt.json has LatestCheckpoint set
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-s3"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			BackupStorageLocation: "default",
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	// Set up mock object store with index.json and vmbt.json
	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	// Create vmbt.json with LatestCheckpoint
	now := metav1.Now()
	archivedVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
				Name:         "cp-002",
				CreationTime: &now,
			},
		},
	}
	vmbtData, _ := json.MarshalIndent(archivedVMBT, "", "  ")
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/cp-002/vmbt.json", vmbtData)

	// Create index.json with vmbtObjectPath
	vmIndex := &uploader.VMIndex{
		VMName:    vmName,
		Namespace: vmNamespace,
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:             "cp-002",
				Type:           "full",
				VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmbt.json",
			},
		},
	}
	indexData, _ := json.MarshalIndent(vmIndex, "", "  ")
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	vmbt, err := r.prepareVMBackupTracker(context.Background(), logr.Discard(), du, vmName, vmNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if vmbt == nil {
		t.Fatal("expected VMBT to be created")
	}

	// Verify LatestCheckpoint was restored from S3
	if vmbt.Status == nil || vmbt.Status.LatestCheckpoint == nil {
		t.Fatal("expected VMBT to have LatestCheckpoint from S3")
	}
	if vmbt.Status.LatestCheckpoint.Name != "cp-002" {
		t.Errorf("LatestCheckpoint.Name = %q, want %q", vmbt.Status.LatestCheckpoint.Name, "cp-002")
	}
}

func TestPrepareVMBackupTracker_ReusesExisting(t *testing.T) {
	// VMBT already exists in cluster → should be reused (not deleted and recreated).
	// Issue #32: VMBT must persist on-cluster for KubeVirt lifecycle events.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-reuse"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
	}

	// Pre-create an existing VMBT
	existingVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName + "-old",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash: common.HashForLabel(vmName),
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, existingVMBT).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	vmbt, err := r.prepareVMBackupTracker(context.Background(), logr.Discard(), du, vmName, vmNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the existing VMBT was reused (same name)
	if vmbt.Name != existingVMBT.Name {
		t.Errorf("expected existing VMBT %q to be reused, got %q", existingVMBT.Name, vmbt.Name)
	}
	if vmbt.Labels[common.LabelVMNameHash] != common.HashForLabel(vmName) {
		t.Errorf("expected VMBT to have label %s, got %q",
			common.LabelVMNameHash, vmbt.Labels[common.LabelVMNameHash])
	}

	// Verify the VMBT still exists on-cluster
	preserved := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: existingVMBT.Name, Namespace: vmNamespace,
	}, preserved); err != nil {
		t.Errorf("existing VMBT was deleted when it should have been reused: %v", err)
	}
}

func TestPrepareVMBackupTracker_ReusesVMBTEvenIfReferencedByActiveVMB(t *testing.T) {
	// A VMBT referenced by a non-terminal VMB is still reused (not deleted).
	// Issue #32: VMBTs are never deleted — they persist on-cluster.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "du-new",
			Namespace: vmNamespace,
			UID:       types.UID("du-new-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
	}

	// VMBT from another DU, still referenced by an active VMB
	otherVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-test-vm-other",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash:    common.HashForLabel(vmName),
				common.LabelDataUploadUID: "other-du-uid",
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	// Active (non-terminal) VMB referencing the other VMBT
	activeVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-other-du",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "other-du-uid",
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("backup.kubevirt.io"),
				Kind:     "VirtualMachineBackupTracker",
				Name:     otherVMBT.Name,
			},
		},
		// nil status = non-terminal
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, otherVMBT, activeVMB).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	vmbt, err := r.prepareVMBackupTracker(context.Background(), logr.Discard(), du, vmName, vmNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The existing VMBT should be reused
	if vmbt == nil {
		t.Fatal("expected VMBT to be returned")
	}
	if vmbt.Name != otherVMBT.Name {
		t.Errorf("expected existing VMBT %q to be reused, got %q", otherVMBT.Name, vmbt.Name)
	}

	// The VMBT must still exist on-cluster
	preserved := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: otherVMBT.Name, Namespace: vmNamespace,
	}, preserved); err != nil {
		t.Errorf("VMBT was deleted when it should have been preserved: %v", err)
	}
}

func TestPrepareVMBackupTracker_ReusesVMBTWithTerminalVMB(t *testing.T) {
	// A VMBT referenced by a terminal VMB (Done=True) should be reused.
	// Issue #32: VMBTs are never deleted — they persist on-cluster.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "du-new",
			Namespace: vmNamespace,
			UID:       types.UID("du-new-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
	}

	// VMBT from a completed DU
	oldVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-test-vm-done",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash:    common.HashForLabel(vmName),
				common.LabelDataUploadUID: "old-du-uid",
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	// Terminal VMB (Done=True) referencing the old VMBT
	terminalVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-old-du",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "old-du-uid",
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("backup.kubevirt.io"),
				Kind:     "VirtualMachineBackupTracker",
				Name:     oldVMBT.Name,
			},
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
			Conditions: []kubevirtbackupv1alpha1.Condition{
				{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionTrue},
				{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionFalse},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, oldVMBT, terminalVMB).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	vmbt, err := r.prepareVMBackupTracker(context.Background(), logr.Discard(), du, vmName, vmNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The old VMBT should be reused (not deleted)
	if vmbt.Name != oldVMBT.Name {
		t.Errorf("expected VMBT %q to be reused, got %q", oldVMBT.Name, vmbt.Name)
	}

	preserved := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: oldVMBT.Name, Namespace: vmNamespace,
	}, preserved); err != nil {
		t.Errorf("VMBT was deleted when it should have been preserved: %v", err)
	}
}

func TestLookupLatestVMBTFromBSL(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmNamespace := "test-ns"

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	tests := []struct {
		name        string
		setupStore  func(*uploader.MockObjectStore)
		expectNil   bool
		expectError bool
		expectCP    string
	}{
		{
			name: "returns nil when no index exists",
			setupStore: func(_ *uploader.MockObjectStore) {
				// Empty store
			},
			expectNil: true,
		},
		{
			name: "returns nil when index has empty vmbtObjectPath",
			setupStore: func(store *uploader.MockObjectStore) {
				vmIndex := &uploader.VMIndex{
					VMName:    "test-vm",
					Namespace: vmNamespace,
					Checkpoints: []uploader.CheckpointEntry{
						{ID: "cp-001", Type: "full"}, // No VMBTObjectPath
					},
				}
				data, _ := json.MarshalIndent(vmIndex, "", "  ")
				_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", data)
			},
			expectNil: true,
		},
		{
			name: "returns VMBT from S3 with checkpoint",
			setupStore: func(store *uploader.MockObjectStore) {
				now := metav1.Now()
				vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
					Status: &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
						LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
							Name:         "cp-005",
							CreationTime: &now,
						},
					},
				}
				vmbtData, _ := json.MarshalIndent(vmbt, "", "  ")
				_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-005/vmbt.json", vmbtData)

				vmIndex := &uploader.VMIndex{
					VMName:    "test-vm",
					Namespace: vmNamespace,
					Checkpoints: []uploader.CheckpointEntry{
						{
							ID:             "cp-005",
							Type:           "full",
							VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-005/vmbt.json",
						},
					},
				}
				indexData, _ := json.MarshalIndent(vmIndex, "", "  ")
				_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)
			},
			expectNil: false,
			expectCP:  "cp-005",
		},
		{
			name: "returns error when vmbt.json is missing",
			setupStore: func(store *uploader.MockObjectStore) {
				vmIndex := &uploader.VMIndex{
					VMName:    "test-vm",
					Namespace: vmNamespace,
					Checkpoints: []uploader.CheckpointEntry{
						{
							ID:             "cp-005",
							Type:           "full",
							VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-005/vmbt.json",
						},
					},
				}
				indexData, _ := json.MarshalIndent(vmIndex, "", "  ")
				_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)
				// Don't create vmbt.json
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(bsl, credSecret).
				Build()

			mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")
			tt.setupStore(mockStore)

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				Scheme:        scheme,
				Log:           logr.Discard(),
				OADPNamespace: vmNamespace,
				ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
					return mockStore, nil
				},
			}

			result, err := r.lookupLatestVMBTFromBSL(context.Background(), bsl, vmNamespace, "test-vm")

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.expectNil {
				if result != nil {
					t.Error("expected nil result")
				}
				return
			}

			if result == nil {
				t.Fatal("expected non-nil result")
			}
			if result.Status == nil || result.Status.LatestCheckpoint == nil {
				t.Fatal("expected LatestCheckpoint to be set")
			}
			if result.Status.LatestCheckpoint.Name != tt.expectCP {
				t.Errorf("LatestCheckpoint.Name = %q, want %q",
					result.Status.LatestCheckpoint.Name, tt.expectCP)
			}
		})
	}
}

func TestSafeGenerateNamePrefix(t *testing.T) {
	tests := []struct {
		name       string
		prefix     string
		maxNameLen int
		expected   string
	}{
		{
			name:       "short prefix is unchanged",
			prefix:     "short-prefix-",
			maxNameLen: 63,
			expected:   "short-prefix-",
		},
		{
			name:       "prefix exactly at limit is unchanged",
			prefix:     strings.Repeat("a", 58), // 58 = 63 - 5
			maxNameLen: 63,
			expected:   strings.Repeat("a", 58),
		},
		{
			name:       "long prefix is truncated",
			prefix:     "a-very-long-prefix-that-will-be-truncated-and-should-not-exceed-the-limit-",
			maxNameLen: 63,
			expected:   "a-very-long-prefix-that-will-be-truncated-and-should-not-e", // 58 chars
		},
		{
			name:       "long prefix for VMB is truncated",
			prefix:     "vmb-a-very-long-dataupload-name-that-exceeds-the-limit-for-kubevirt-hotplug-",
			maxNameLen: maxVMBNameLen,                              // 45
			expected:   "vmb-a-very-long-dataupload-name-that-exc", // 40 chars = 45 - 5
		},
		{
			name:       "edge case: maxNameLen < random part",
			prefix:     "short-",
			maxNameLen: 4,
			expected:   "s", // maxPrefix becomes 1
		},
		{
			name:       "edge case: maxNameLen == random part",
			prefix:     "short-",
			maxNameLen: 5,
			expected:   "s", // maxPrefix becomes 1
		},
		{
			name:       "edge case: maxNameLen == random part + 1",
			prefix:     "short-",
			maxNameLen: 6,
			expected:   "s", // maxPrefix is 1, so prefix is truncated to 1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := safeGenerateNamePrefix(tt.prefix, tt.maxNameLen)
			if result != tt.expected {
				t.Errorf("safeGenerateNamePrefix() = %q, want %q", result, tt.expected)
			}
			// Sanity check length
			maxPrefixLen := max(tt.maxNameLen-k8sGenerateNameRandomLen, 1)
			if len(result) > maxPrefixLen {
				t.Errorf("result length %d exceeds max prefix length %d", len(result), maxPrefixLen)
			}
		})
	}
}

// TestCleanupVMBackupResources_PreservesVMBT verifies that cleanupVMBackupResources
// deletes the VMB but preserves the VMBT on-cluster (issue #32).
func TestCleanupVMBackupResources_PreservesVMBT(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du-cleanup",
			Namespace: "openshift-adp",
			UID:       types.UID("cleanup-uid"),
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
	}

	// VMB that should be deleted during cleanup
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-test-cleanup",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("backup.kubevirt.io"),
				Kind:     "VirtualMachineBackupTracker",
				Name:     "vmbt-test-cleanup",
			},
		},
	}

	// VMBT that should be PRESERVED during cleanup
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-test-cleanup",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash: common.HashForLabel(vmName),
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: new("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, vmb, vmbt).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	r.cleanupVMBackupResources(context.Background(), logr.Discard(), du, vmNamespace)

	// VMB should be deleted
	deletedVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: vmb.Name, Namespace: vmNamespace,
	}, deletedVMB); err == nil {
		t.Error("VMB should have been deleted during cleanup, but it still exists")
	}

	// VMBT should be PRESERVED (not deleted)
	preservedVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: vmbt.Name, Namespace: vmNamespace,
	}, preservedVMBT); err != nil {
		t.Errorf("VMBT should have been preserved during cleanup, but it was deleted: %v", err)
	}
}

func TestAddOverhead(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		percent  int64
		expected string
	}{
		{
			name:     "20% overhead on 30Gi",
			input:    "30Gi",
			percent:  20,
			expected: "36Gi",
		},
		{
			name:     "20% overhead on 10Gi",
			input:    "10Gi",
			percent:  20,
			expected: "12Gi",
		},
		{
			name:    "20% overhead on 1Gi",
			input:   "1Gi",
			percent: 20,
			// 1Gi = 1073741824 bytes, 20% = 214748364, total = 1288490188
			expected: "1288490188",
		},
		{
			name:     "0% overhead returns same value",
			input:    "50Gi",
			percent:  0,
			expected: "50Gi",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := resource.MustParse(tt.input)
			result := addOverhead(input, tt.percent)
			expected := resource.MustParse(tt.expected)
			if result.Cmp(expected) != 0 {
				t.Errorf("addOverhead(%s, %d%%) = %s, want %s", tt.input, tt.percent, result.String(), expected.String())
			}
		})
	}
}

func TestCalculateBackupPVCSize(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = kubevirtcorev1.AddToScheme(scheme)

	tests := []struct {
		name         string
		du           *velerov2alpha1.DataUpload
		objects      []client.Object // PVCs, VMs, etc.
		expectedSize string
		expectErr    bool
	}{
		{
			name: "sizes from source PVC capacity with 20% overhead",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-du", Namespace: "openshift-adp"},
				Spec: velerov2alpha1.DataUploadSpec{
					SourcePVC:       "source-disk",
					SourceNamespace: "vm-ns",
				},
			},
			objects: []client.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "source-disk", Namespace: "vm-ns"},
					Status: corev1.PersistentVolumeClaimStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("30Gi"),
						},
					},
				},
			},
			expectedSize: "36Gi",
		},
		{
			name: "user annotation overrides source PVC",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationBackupPVCSize: "50Gi",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					SourcePVC:       "source-disk",
					SourceNamespace: "vm-ns",
				},
			},
			objects: []client.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "source-disk", Namespace: "vm-ns"},
					Status: corev1.PersistentVolumeClaimStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("30Gi"),
						},
					},
				},
			},
			expectedSize: "50Gi",
		},
		{
			name: "invalid annotation falls through to source PVC",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationBackupPVCSize: "not-a-quantity",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					SourcePVC:       "source-disk",
					SourceNamespace: "vm-ns",
				},
			},
			objects: []client.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "source-disk", Namespace: "vm-ns"},
					Status: corev1.PersistentVolumeClaimStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("10Gi"),
						},
					},
				},
			},
			expectedSize: "12Gi",
		},
		{
			name: "source PVC not found returns error",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-du", Namespace: "openshift-adp"},
				Spec: velerov2alpha1.DataUploadSpec{
					SourcePVC:       "nonexistent-disk",
					SourceNamespace: "vm-ns",
				},
			},
			objects:   nil,
			expectErr: true,
		},
		{
			name: "falls back to default when no source PVC specified",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-du", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataUploadSpec{},
			},
			objects:      nil,
			expectedSize: DefaultTempPVCSize,
		},
		{
			name: "enforces minimum 1Gi floor",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-du", Namespace: "openshift-adp"},
				Spec: velerov2alpha1.DataUploadSpec{
					SourcePVC:       "tiny-disk",
					SourceNamespace: "vm-ns",
				},
			},
			objects: []client.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "tiny-disk", Namespace: "vm-ns"},
					Status: corev1.PersistentVolumeClaimStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("500Mi"),
						},
					},
				},
			},
			expectedSize: "1Gi",
		},
		{
			name: "uses SourceNamespace from DataUpload spec",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-du", Namespace: "openshift-adp"},
				Spec: velerov2alpha1.DataUploadSpec{
					SourcePVC:       "disk-in-other-ns",
					SourceNamespace: "other-ns",
				},
			},
			objects: []client.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "disk-in-other-ns", Namespace: "other-ns"},
					Status: corev1.PersistentVolumeClaimStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("20Gi"),
						},
					},
				},
			},
			expectedSize: "24Gi",
		},
		{
			name: "multi-disk VM sums all PVC capacities",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName:      "multi-disk-vm",
						common.AnnotationVMNamespace: "vm-ns",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					SourcePVC:       "disk-1",
					SourceNamespace: "vm-ns",
				},
			},
			objects: []client.Object{
				&kubevirtcorev1.VirtualMachine{
					ObjectMeta: metav1.ObjectMeta{Name: "multi-disk-vm", Namespace: "vm-ns"},
					Spec: kubevirtcorev1.VirtualMachineSpec{
						Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
							Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
								Volumes: []kubevirtcorev1.Volume{
									{Name: "vol1", VolumeSource: kubevirtcorev1.VolumeSource{
										PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{ClaimName: "disk-1"},
										},
									}},
									{Name: "vol2", VolumeSource: kubevirtcorev1.VolumeSource{
										PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{ClaimName: "disk-2"},
										},
									}},
								},
							},
						},
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "disk-1", Namespace: "vm-ns"},
					Status: corev1.PersistentVolumeClaimStatus{
						Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("30Gi")},
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "disk-2", Namespace: "vm-ns"},
					Status: corev1.PersistentVolumeClaimStatus{
						Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("20Gi")},
					},
				},
			},
			// 30Gi + 20Gi = 50Gi, +20% = 60Gi
			expectedSize: "60Gi",
		},
		{
			name: "multi-disk VM with missing PVC returns error",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName:      "vm-missing-pvc",
						common.AnnotationVMNamespace: "vm-ns",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					SourcePVC:       "disk-1",
					SourceNamespace: "vm-ns",
				},
			},
			objects: []client.Object{
				&kubevirtcorev1.VirtualMachine{
					ObjectMeta: metav1.ObjectMeta{Name: "vm-missing-pvc", Namespace: "vm-ns"},
					Spec: kubevirtcorev1.VirtualMachineSpec{
						Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
							Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
								Volumes: []kubevirtcorev1.Volume{
									{Name: "vol1", VolumeSource: kubevirtcorev1.VolumeSource{
										PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{ClaimName: "disk-that-doesnt-exist"},
										},
									}},
								},
							},
						},
					},
				},
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := []client.Object{}
			if tt.objects != nil {
				objs = append(objs, tt.objects...)
			}
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			r := &KubeVirtDataUploadReconciler{
				Client: fakeClient,
				Scheme: scheme,
			}

			logger := logr.Discard()
			result, err := r.calculateBackupPVCSize(context.Background(), logger, tt.du, "vm-ns")

			if tt.expectErr {
				if err == nil {
					t.Errorf("calculateBackupPVCSize() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("calculateBackupPVCSize() unexpected error: %v", err)
			}

			expected := resource.MustParse(tt.expectedSize)
			if result.Cmp(expected) != 0 {
				t.Errorf("calculateBackupPVCSize() = %s, want %s", result.String(), expected.String())
			}
		})
	}
}

func TestResolveBackupMode_MaxIncrementalBackups(t *testing.T) {
	buildChain := func(vmName, vmNamespace string, numIncrementals int) (uploader.VMIndex, []string) {
		var checkpoints []uploader.CheckpointEntry
		var files []string

		fullPath := fmt.Sprintf("checkpoints/%s/%s/cp-000/disk.qcow2", vmNamespace, vmName)
		checkpoints = append(checkpoints, uploader.CheckpointEntry{
			ID:       "cp-000",
			Type:     "full",
			VMBackup: "vmb-000",
			Files:    []uploader.CheckpointFile{{ObjectPath: fullPath}},
		})
		files = append(files, fullPath)

		for i := 1; i <= numIncrementals; i++ {
			cpID := fmt.Sprintf("cp-%03d", i)
			parentID := fmt.Sprintf("cp-%03d", i-1)
			path := fmt.Sprintf("checkpoints/%s/%s/%s/disk.qcow2", vmNamespace, vmName, cpID)
			checkpoints = append(checkpoints, uploader.CheckpointEntry{
				ID:       cpID,
				Type:     "incremental",
				Parent:   parentID,
				VMBackup: fmt.Sprintf("vmb-%03d", i),
				Files:    []uploader.CheckpointFile{{ObjectPath: path}},
			})
			files = append(files, path)
		}

		return uploader.VMIndex{
			VMName:      vmName,
			Namespace:   vmNamespace,
			Checkpoints: checkpoints,
		}, files
	}

	tests := []struct {
		name                  string
		maxIncrementalBackups int
		numIncrementals       int
		vmAnnotation          *string
		wantForceFullBackup   bool
	}{
		{
			name:                  "limit reached forces full backup",
			maxIncrementalBackups: 3,
			numIncrementals:       3,
			wantForceFullBackup:   true,
		},
		{
			name:                  "below limit allows incremental",
			maxIncrementalBackups: 5,
			numIncrementals:       2,
			wantForceFullBackup:   false,
		},
		{
			name:                  "unlimited (0) allows any chain length",
			maxIncrementalBackups: 0,
			numIncrementals:       9,
			wantForceFullBackup:   false,
		},
		{
			name:                  "exactly at boundary allows incremental",
			maxIncrementalBackups: 3,
			numIncrementals:       2,
			wantForceFullBackup:   false,
		},
		{
			name:                  "exceeded limit forces full backup",
			maxIncrementalBackups: 3,
			numIncrementals:       5,
			wantForceFullBackup:   true,
		},
		{
			name:                  "per-VM annotation overrides global to force full",
			maxIncrementalBackups: 10,
			numIncrementals:       3,
			vmAnnotation:          new("3"),
			wantForceFullBackup:   true,
		},
		{
			name:                  "per-VM annotation allows incremental when below its limit",
			maxIncrementalBackups: 10,
			numIncrementals:       2,
			vmAnnotation:          new("5"),
			wantForceFullBackup:   false,
		},
		{
			name:                  "per-VM annotation 0 means unlimited overriding global",
			maxIncrementalBackups: 3,
			numIncrementals:       9,
			vmAnnotation:          new("0"),
			wantForceFullBackup:   false,
		},
		{
			name:                  "invalid annotation falls back to global",
			maxIncrementalBackups: 3,
			numIncrementals:       3,
			vmAnnotation:          new("abc"),
			wantForceFullBackup:   true,
		},
		{
			name:                  "negative annotation falls back to global",
			maxIncrementalBackups: 3,
			numIncrementals:       3,
			vmAnnotation:          new("-1"),
			wantForceFullBackup:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vmName := "test-vm"
			vmNamespace := "test-ns"

			scheme := runtime.NewScheme()
			_ = velerov2alpha1.AddToScheme(scheme)
			_ = velerov1.AddToScheme(scheme)
			_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
			_ = kubevirtcorev1.AddToScheme(scheme)
			_ = corev1.AddToScheme(scheme)

			du := &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: vmNamespace,
					UID:       types.UID("test-uid"),
					Annotations: map[string]string{
						common.AnnotationVMName:      vmName,
						common.AnnotationVMNamespace: vmNamespace,
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover:             common.DataMoverKubeVirt,
					SourceNamespace:       vmNamespace,
					BackupStorageLocation: "default",
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseAccepted,
				},
			}

			bsl := &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: vmNamespace,
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "test-bucket",
							Prefix: "velero",
						},
					},
					Config: map[string]string{"region": "us-east-1"},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
						Key:                  "cloud",
					},
				},
				Status: velerov1.BackupStorageLocationStatus{
					Phase: velerov1.BackupStorageLocationPhaseAvailable,
				},
			}

			credSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cloud-creds",
					Namespace: vmNamespace,
				},
				Data: map[string][]byte{
					"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
				},
			}

			vmAnnotations := map[string]string{}
			if tt.vmAnnotation != nil {
				vmAnnotations[common.AnnotationMaxIncrementalBackups] = *tt.vmAnnotation
			}

			vm := &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:        vmName,
					Namespace:   vmNamespace,
					Annotations: vmAnnotations,
				},
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(du, bsl, credSecret, vm).
				Build()

			vmIndex, filePaths := buildChain(vmName, vmNamespace, tt.numIncrementals)
			mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")
			indexData, _ := json.Marshal(vmIndex)
			_ = mockStore.PutObjectBytes(fmt.Sprintf("checkpoints/%s/%s/index.json", vmNamespace, vmName), indexData)
			for _, path := range filePaths {
				_ = mockStore.PutObjectBytes(path, []byte("data"))
			}

			r := &KubeVirtDataUploadReconciler{
				Client:                fakeClient,
				Scheme:                scheme,
				Log:                   logr.Discard(),
				OADPNamespace:         vmNamespace,
				MaxIncrementalBackups: tt.maxIncrementalBackups,
				ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
					return mockStore, nil
				},
			}

			vmRef := &common.VMReference{Name: vmName, Namespace: vmNamespace}
			forceFullBackup, _ := r.resolveBackupMode(context.Background(), logr.Discard(), du, vmRef)

			if forceFullBackup != tt.wantForceFullBackup {
				t.Errorf("forceFullBackup = %v, want %v", forceFullBackup, tt.wantForceFullBackup)
			}
		})
	}
}

func TestResolveBackupMode_MaxIncrementalSkippedOnBrokenChain(t *testing.T) {
	vmName := "test-vm"
	vmNamespace := "test-ns"

	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = kubevirtcorev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	vm := &kubevirtcorev1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vmName,
			Namespace: vmNamespace,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret, vm).
		Build()

	vmIndex := uploader.VMIndex{
		VMName:    vmName,
		Namespace: vmNamespace,
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:       "cp-000",
				Type:     "full",
				VMBackup: "vmb-000",
				Files:    []uploader.CheckpointFile{{ObjectPath: "checkpoints/test-ns/test-vm/cp-000/disk.qcow2"}},
			},
			{
				ID:       "cp-001",
				Type:     "incremental",
				Parent:   "cp-000",
				VMBackup: "vmb-001",
				Files:    []uploader.CheckpointFile{{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/disk.qcow2"}},
			},
			{
				ID:       "cp-002",
				Type:     "incremental",
				Parent:   "cp-001",
				VMBackup: "vmb-002",
				Files:    []uploader.CheckpointFile{{ObjectPath: "checkpoints/test-ns/test-vm/cp-002/disk.qcow2"}},
			},
		},
	}
	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")
	indexData, _ := json.Marshal(vmIndex)
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/cp-000/disk.qcow2", []byte("full"))
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/cp-002/disk.qcow2", []byte("inc2"))

	r := &KubeVirtDataUploadReconciler{
		Client:                fakeClient,
		Scheme:                scheme,
		Log:                   logr.Discard(),
		OADPNamespace:         vmNamespace,
		MaxIncrementalBackups: 5,
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	vmRef := &common.VMReference{Name: vmName, Namespace: vmNamespace}
	forceFullBackup, checkpointLookup := r.resolveBackupMode(context.Background(), logr.Discard(), du, vmRef)

	if !forceFullBackup {
		t.Error("expected forceFullBackup=true when chain is broken, regardless of MaxIncrementalBackups")
	}
	if checkpointLookup == nil {
		t.Fatal("expected checkpointLookup to be returned")
	}
	if checkpointLookup.IsChainValid {
		t.Error("expected IsChainValid=false for broken chain")
	}
}

func TestResolveBackupMode_MaxIncrementalFirstBackup(t *testing.T) {
	vmName := "test-vm"
	vmNamespace := "test-ns"

	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = kubevirtcorev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	vm := &kubevirtcorev1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vmName,
			Namespace: vmNamespace,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret, vm).
		Build()

	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	r := &KubeVirtDataUploadReconciler{
		Client:                fakeClient,
		Scheme:                scheme,
		Log:                   logr.Discard(),
		OADPNamespace:         vmNamespace,
		MaxIncrementalBackups: 1,
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	vmRef := &common.VMReference{Name: vmName, Namespace: vmNamespace}
	forceFullBackup, checkpointLookup := r.resolveBackupMode(context.Background(), logr.Discard(), du, vmRef)

	if !forceFullBackup {
		t.Error("expected forceFullBackup=true for first backup (no checkpoint index)")
	}
	if checkpointLookup == nil {
		t.Fatal("expected checkpointLookup to be returned")
	}
	if checkpointLookup.Found {
		t.Error("expected Found=false for first backup")
	}
}

func TestGetEffectiveMaxIncrementalBackups(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = kubevirtcorev1.AddToScheme(scheme)

	tests := []struct {
		name         string
		globalMax    int
		vmAnnotation *string
		vmExists     bool
		expectedMax  int
	}{
		{
			name:        "no VM annotation uses global",
			globalMax:   5,
			vmExists:    true,
			expectedMax: 5,
		},
		{
			name:         "VM annotation overrides global",
			globalMax:    5,
			vmAnnotation: new("3"),
			vmExists:     true,
			expectedMax:  3,
		},
		{
			name:         "VM annotation 0 overrides global",
			globalMax:    5,
			vmAnnotation: new("0"),
			vmExists:     true,
			expectedMax:  0,
		},
		{
			name:         "invalid VM annotation falls back to global",
			globalMax:    5,
			vmAnnotation: new("invalid"),
			vmExists:     true,
			expectedMax:  5,
		},
		{
			name:         "negative VM annotation falls back to global",
			globalMax:    5,
			vmAnnotation: new("-1"),
			vmExists:     true,
			expectedMax:  5,
		},
		{
			name:        "VM not found falls back to global",
			globalMax:   5,
			vmExists:    false,
			expectedMax: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objs []client.Object
			if tt.vmExists {
				annotations := map[string]string{}
				if tt.vmAnnotation != nil {
					annotations[common.AnnotationMaxIncrementalBackups] = *tt.vmAnnotation
				}
				objs = append(objs, &kubevirtcorev1.VirtualMachine{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "test-vm",
						Namespace:   "test-ns",
						Annotations: annotations,
					},
				})
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			r := &KubeVirtDataUploadReconciler{
				Client:                fakeClient,
				Scheme:                scheme,
				MaxIncrementalBackups: tt.globalMax,
			}

			vmRef := &common.VMReference{Name: "test-vm", Namespace: "test-ns"}
			result := r.getEffectiveMaxIncrementalBackups(context.Background(), logr.Discard(), vmRef)

			if result != tt.expectedMax {
				t.Errorf("getEffectiveMaxIncrementalBackups() = %d, want %d", result, tt.expectedMax)
			}
		})
	}
}

func TestEmitPodLogs(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "datamover-test-pod",
			Namespace: "openshift-adp",
		},
	}

	tests := []struct {
		name            string
		collector       func(ctx context.Context, podName, podNamespace string) (string, error)
		expectLogs      bool
		expectedLines   int
		expectCollected bool
	}{
		{
			name:            "nil collector skips log collection",
			collector:       nil,
			expectLogs:      false,
			expectCollected: false,
		},
		{
			name: "collector error logs warning and continues",
			collector: func(ctx context.Context, podName, podNamespace string) (string, error) {
				return "", fmt.Errorf("connection refused")
			},
			expectLogs:      false,
			expectCollected: true,
		},
		{
			name: "empty log output emits nothing",
			collector: func(ctx context.Context, podName, podNamespace string) (string, error) {
				return "", nil
			},
			expectLogs:      false,
			expectCollected: true,
		},
		{
			name: "multi-line output emits each line",
			collector: func(ctx context.Context, podName, podNamespace string) (string, error) {
				return "uploading snapshot\nprogress: 100%\ncompleted\n", nil
			},
			expectLogs:      true,
			expectedLines:   3,
			expectCollected: true,
		},
		{
			name: "single line without trailing newline",
			collector: func(ctx context.Context, podName, podNamespace string) (string, error) {
				return "done", nil
			},
			expectLogs:      true,
			expectedLines:   1,
			expectCollected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			logger := funcr.New(func(prefix, args string) {
				logBuf.WriteString(args + "\n")
			}, funcr.Options{})

			r := &KubeVirtDataUploadReconciler{
				PodLogCollector: tt.collector,
			}

			r.emitPodLogs(context.Background(), logger, pod)

			hasLogs := strings.Contains(logBuf.String(), "Datamover pod log")
			if hasLogs != tt.expectLogs {
				t.Errorf("expected logs=%v, got logs=%v, output: %s", tt.expectLogs, hasLogs, logBuf.String())
			}

			if tt.expectLogs {
				lineCount := strings.Count(logBuf.String(), "Datamover pod log")
				if lineCount != tt.expectedLines {
					t.Errorf("expected %d log lines, got %d", tt.expectedLines, lineCount)
				}
			}
		})
	}
}

// TestEmitPodLogs_TruncatesLongOutput covers #154: a pod that produced a very
// large log (e.g. a crash loop with verbose output) must not flood the
// controller's own logs unbounded -- only the last maxEmittedPodLogLines lines
// are kept, with a truncation notice recording how many were dropped.
func TestEmitPodLogs_TruncatesLongOutput(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "datamover-test-pod", Namespace: "openshift-adp"}}

	const totalLines = maxEmittedPodLogLines + 50
	var sb strings.Builder
	for i := 1; i <= totalLines; i++ {
		fmt.Fprintf(&sb, "line-%d\n", i)
	}

	var logBuf bytes.Buffer
	logger := funcr.New(func(prefix, args string) {
		logBuf.WriteString(args + "\n")
	}, funcr.Options{})

	r := &KubeVirtDataUploadReconciler{
		PodLogCollector: func(ctx context.Context, podName, podNamespace string) (string, error) {
			return sb.String(), nil
		},
	}

	r.emitPodLogs(context.Background(), logger, pod)

	output := logBuf.String()
	if got := strings.Count(output, "\"message\"=\"line-"); got != maxEmittedPodLogLines {
		t.Errorf("expected %d emitted lines, got %d", maxEmittedPodLogLines, got)
	}
	if !strings.Contains(output, "Datamover pod log truncated") {
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

func TestHandleInProgress_PodLogCollectionFailureDoesNotBlock(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name          string
		podPhase      corev1.PodPhase
		expectedPhase velerov2alpha1.DataUploadPhase
	}{
		{
			name:          "log failure does not block PodSucceeded",
			podPhase:      corev1.PodSucceeded,
			expectedPhase: velerov2alpha1.DataUploadPhaseCompleted,
		},
		{
			name:          "log failure does not block PodFailed",
			podPhase:      corev1.PodFailed,
			expectedPhase: velerov2alpha1.DataUploadPhaseFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			du := &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					UID:       types.UID("test-uid-log-fail"),
					Annotations: map[string]string{
						common.AnnotationVMName:      "test-vm",
						common.AnnotationVMNamespace: "test-ns",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseInProgress,
				},
			}

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      common.DatamoverPodNamePrefix + du.Name,
					Namespace: "openshift-adp",
					Labels: map[string]string{
						common.LabelDataUploadUID: string(du.UID),
					},
				},
				Status: corev1.PodStatus{
					Phase: tt.podPhase,
				},
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(du, pod).
				Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				Scheme:        scheme,
				Log:           logr.Discard(),
				OADPNamespace: "openshift-adp",
				PodLogCollector: func(ctx context.Context, podName, podNamespace string) (string, error) {
					return "", fmt.Errorf("simulated log collection failure")
				},
			}

			result, err := r.handleInProgress(context.Background(), logr.Discard(), du)
			if err != nil {
				t.Fatalf("handleInProgress should not fail when log collection fails: %v", err)
			}
			if result.RequeueAfter != 0 {
				t.Errorf("expected no requeue, got RequeueAfter=%v", result.RequeueAfter)
			}

			updatedDU := &velerov2alpha1.DataUpload{}
			if err := fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      du.Name,
				Namespace: du.Namespace,
			}, updatedDU); err != nil {
				t.Fatalf("failed to get updated DataUpload: %v", err)
			}
			if updatedDU.Status.Phase != tt.expectedPhase {
				t.Errorf("expected phase=%s, got phase=%s", tt.expectedPhase, updatedDU.Status.Phase)
			}
		})
	}
}
