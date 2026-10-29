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
		Spec: velerov2alpha