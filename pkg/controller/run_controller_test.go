// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/internal/fakes"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/artifacts/gcs"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/pki"
	"github.com/gke-labs/eval-runner/pkg/sandboxclient"
	"github.com/gke-labs/eval-runner/pkg/stats"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	fakek8s "k8s.io/client-go/kubernetes/fake"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

func setupScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add corev1 scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add v1alpha1 scheme: %v", err)
	}
	if err := sandboxv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add sandbox scheme: %v", err)
	}
	return s
}

func TestReconcilePendingToDiscovering(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected RequeueAfter to be set")
	}

	// Assert Sandbox was created
	sandboxName := discovery.SandboxNameForRun("test-run")
	var sb sandboxv1beta1.Sandbox
	if err := client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &sb); err != nil {
		t.Fatalf("Failed to find created sandbox: %v", err)
	}
	if sb.Labels[discovery.LabelRunName] != "test-run" {
		t.Errorf("Expected sandbox to have run label, got: %v", sb.Labels)
	}

	// Assert Run updated to Discovering
	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovering {
		t.Errorf("Expected phase Discovering, got %s", updatedRun.Status.Phase)
	}
	if !controllerutil.ContainsFinalizer(&updatedRun, v1alpha1.FinalizerDiscoverySandbox) {
		t.Errorf("Expected finalizer %s on run, got: %v", v1alpha1.FinalizerDiscoverySandbox, updatedRun.Finalizers)
	}
	if updatedRun.Status.DiscoveryDeadline == nil {
		t.Errorf("Expected DiscoveryDeadline to be set in status")
	}
}

func TestReconcilePodReadySuccess(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.5",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	validManifestJSON := `{
		"tasks": [
			{"id": "triage/runbook_lookup", "workdir": "evals", "spec": "runbook.py@lookup"},
			{"id": "triage/summary_quality", "workdir": "evals", "spec": "summary.py@quality"}
		]
	}`

	executor := &sandboxclient.FakeClient{
		ListStdout: validManifestJSON,
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Did not expect RequeueAfter on success")
	}

	// Assert Run updated to Discovered with Manifest
	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
		t.Errorf("Expected phase Discovered, got %s", updatedRun.Status.Phase)
	}
	if updatedRun.Status.Discovery == nil || updatedRun.Status.Discovery.TaskCount != 2 {
		t.Fatalf("Expected 2 tasks in discovery, got: %+v", updatedRun.Status.Discovery)
	}
	if updatedRun.Status.Discovery.ManifestPath == "" || updatedRun.Status.Discovery.ManifestHash == "" {
		t.Fatalf("Expected ManifestPath and ManifestHash populated in discovery, got: %+v", updatedRun.Status.Discovery)
	}

	// Assert Sandbox was cleaned up
	var checkSb sandboxv1beta1.Sandbox
	err = client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected sandbox to be deleted, got err: %v", err)
	}
}

func TestReconcilePodReadySuccess_GCSSink_EmptyBucketFirstWrite(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-gcs-first-write",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "a1b23a0",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run-gcs-first-write")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.5",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	validManifestJSON := `{
		"tasks": [
			{"id": "triage/runbook_lookup", "workdir": "evals", "spec": "runbook.py@lookup"},
			{"id": "triage/summary_quality", "workdir": "evals", "spec": "summary.py@quality"}
		]
	}`

	executor := &sandboxclient.FakeClient{
		ListStdout: validManifestJSON,
	}

	fakeGCSClient := fakes.NewFakeStorageClient()
	gcsSink, err := gcs.New(t.Context(), "gs://justinbs-knotai-dev-eval-runner", gcs.WithStorageClient(fakeGCSClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
		ArtifactSink:     gcsSink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-gcs-first-write"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Did not expect RequeueAfter on success")
	}

	// Assert Run updated to Discovered with Manifest stored in GCS
	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
		t.Fatalf("Expected phase Discovered, got %s (message: %s)", updatedRun.Status.Phase, updatedRun.Status.Message)
	}
	if updatedRun.Status.Discovery == nil || updatedRun.Status.Discovery.TaskCount != 2 {
		t.Fatalf("Expected 2 tasks in discovery, got: %+v", updatedRun.Status.Discovery)
	}
	expectedURI := "gs://justinbs-knotai-dev-eval-runner/runs/a1b23a0/manifest.json"
	if updatedRun.Status.Discovery.ManifestPath != expectedURI {
		t.Errorf("Expected ManifestPath %q, got %q", expectedURI, updatedRun.Status.Discovery.ManifestPath)
	}
	if updatedRun.Status.Discovery.ManifestHash == "" {
		t.Errorf("Expected ManifestHash populated in discovery")
	}

	// Verify the manifest object actually exists in the fake GCS client
	if _, ok := fakeGCSClient.GetObject("justinbs-knotai-dev-eval-runner", "runs/a1b23a0/manifest.json"); !ok {
		t.Fatalf("Expected manifest object to be written to fake GCS client")
	}
	if fakeGCSClient.WriteCount() != 1 {
		t.Errorf("Expected 1 write call, got %d", fakeGCSClient.WriteCount())
	}
}

func TestReconcileBadCommitFailsAndCleansUp(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "deadbeef00000000000000000000000000000000",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.5",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	executor := &sandboxclient.FakeClient{
		CheckoutStderr: "fatal: reference is not a tree: deadbeef00000000000000000000000000000000",
		CheckoutExit:   128,
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	// Assert Run updated to Failed
	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "checkout") {
		t.Errorf("Expected failure message to name the checkout step, got: %s", updatedRun.Status.Message)
	}
	if !strings.Contains(updatedRun.Status.Message, "deadbeef") {
		t.Errorf("Expected failure message to contain stderr tail, got: %s", updatedRun.Status.Message)
	}
	if strings.Contains(updatedRun.Status.Message, "attempts") {
		t.Errorf("Expected permanent failure to not mention attempts, got: %s", updatedRun.Status.Message)
	}

	// Assert Sandbox was cleaned up
	var checkSb sandboxv1beta1.Sandbox
	err = client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected sandbox to be deleted on failure, got err: %v", err)
	}
}

func TestReconcileControllerRestartMidReconcile(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected RequeueAfter when pod is pending")
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovering {
		t.Errorf("Expected phase to remain Discovering, got %s", updatedRun.Status.Phase)
	}
}

func TestReconcileTerminalRunIsNoop(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovered,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue for terminal Run")
	}

	sandboxName := discovery.SandboxNameForRun("test-run")
	var sb sandboxv1beta1.Sandbox
	err = client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &sb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected no sandbox created for terminal Run, got err: %v", err)
	}
}

func TestReconcileCompletedRun_ForeignPrefixClean(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-foreign-completed",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Model: "test-model",
		},
		Status: v1alpha1.RunStatus{
			Phase:     v1alpha1.RunPhaseCompleted,
			Artifacts: "gs://old-bucket/runs/abc1234/",
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      "task-1",
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://old-bucket/runs/abc1234/task-1/log.json",
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	sink := artifacts.NewInMemorySink()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		ArtifactSink:     sink,
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: "eval-runner",
			Name:      "test-run-foreign-completed",
		},
	}

	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("unexpected reconcile error on completed run: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected no requeue for completed run, got %+v", res)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("failed to get updated run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseCompleted {
		t.Errorf("expected phase %s, got %s", v1alpha1.RunPhaseCompleted, updatedRun.Status.Phase)
	}
	if updatedRun.Status.Artifacts != "gs://old-bucket/runs/abc1234/" {
		t.Errorf("expected artifacts unchanged, got %s", updatedRun.Status.Artifacts)
	}
}

func TestReconcileDeleteRunMidDiscoveryRemovesSandbox(t *testing.T) {
	scheme := setupScheme(t)

	now := metav1.Now()
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-run",
			Namespace:         "eval-runner",
			DeletionTimestamp: &now,
			Finalizers:        []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue on deletion")
	}

	var checkSb sandboxv1beta1.Sandbox
	err = client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected sandbox to be deleted on Run deletion, got err: %v", err)
	}

	var updatedRun v1alpha1.Run
	err = client.Get(t.Context(), req.NamespacedName, &updatedRun)
	if err == nil && controllerutil.ContainsFinalizer(&updatedRun, v1alpha1.FinalizerDiscoverySandbox) {
		t.Errorf("Expected finalizer to be removed from run")
	}
}

func TestReconcileDeletedRunNotFoundCleansUpSandbox(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-ghost"
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      runName,
			Namespace: "eval-runner",
		},
	}

	sandboxName := discovery.SandboxNameForRun(runName)
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sb).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue for deleted Run")
	}

	var checkSb sandboxv1beta1.Sandbox
	err = client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected leftover sandbox to be deleted when Run is not found, got err: %v", err)
	}
}

func TestReconcileTerminalRunWithLeftoverSandboxRemovesSandbox(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-terminal"
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      runName,
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovered,
		},
	}

	sandboxName := discovery.SandboxNameForRun(runName)
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue for terminal Run")
	}

	var checkSb sandboxv1beta1.Sandbox
	err = client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected leftover sandbox to be deleted for terminal Run, got err: %v", err)
	}
}

func TestReconcileStatusUpdateConflictDoesNotRerunDiscovery(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-conflict"
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       runName,
			Namespace:  "eval-runner",
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun(runName)
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.5",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	validManifestJSON := `{"tasks":[{"id":"triage/lookup","spec":"runbook.py@lookup"}]}`
	executor := &sandboxclient.FakeClient{
		ListStdout: validManifestJSON,
	}

	firstStatusUpdate := true
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if subResourceName == "status" && firstStatusUpdate {
					firstStatusUpdate = false
					return apierrors.NewConflict(v1alpha1.GroupVersion.WithResource("runs").GroupResource(), runName, errors.New("conflict"))
				}
				return cl.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		}).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue on success, got %v", res.RequeueAfter)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
		t.Errorf("Expected phase Discovered after conflict retry, got %s", updatedRun.Status.Phase)
	}
}

type discoveringRunFixture struct {
	client      client.Client
	reconciler  *RunReconciler
	req         ctrl.Request
	sandboxName string
}

func newDiscoveringRunFixture(t *testing.T, clientInstance sandboxclient.Client) *discoveringRunFixture {
	scheme := setupScheme(t)
	runName := "test-run"
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       runName,
			Namespace:  "eval-runner",
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun(runName)
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.5",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    clientInstance,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	return &discoveringRunFixture{
		client:      cl,
		reconciler:  reconciler,
		req:         req,
		sandboxName: sandboxName,
	}
}

func TestReconcileStdoutOverLimitFails(t *testing.T) {
	f := newDiscoveringRunFixture(t, &sandboxclient.FakeClient{
		ListErr: &discovery.LimitError{
			Stream: "stdout",
			Limit:  discovery.MaxStdoutBytes,
		},
	})

	_, err := f.reconciler.Reconcile(t.Context(), f.req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := f.client.Get(t.Context(), f.req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "stdout limit exceeded") {
		t.Errorf("Expected message to contain 'stdout limit exceeded', got: %s", updatedRun.Status.Message)
	}
}

func TestReconcileStderrOverLimitFails(t *testing.T) {
	f := newDiscoveringRunFixture(t, &sandboxclient.FakeClient{
		ListErr: &discovery.LimitError{
			Stream: "stderr",
			Limit:  discovery.MaxStderrBytes,
		},
	})

	_, err := f.reconciler.Reconcile(t.Context(), f.req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := f.client.Get(t.Context(), f.req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "stderr limit exceeded") {
		t.Errorf("Expected message to contain 'stderr limit exceeded', got: %s", updatedRun.Status.Message)
	}
}

func TestReconcileOversizedManifestFails(t *testing.T) {
	var tasks []string
	for i := 0; i < 1001; i++ {
		tasks = append(tasks, fmt.Sprintf(`{"id": "task_%d", "spec": "runbook.py"}`, i))
	}
	oversizedJSON := fmt.Sprintf(`{"tasks": [%s]}`, strings.Join(tasks, ","))

	f := newDiscoveringRunFixture(t, &sandboxclient.FakeClient{
		ListStdout: oversizedJSON,
	})

	_, err := f.reconciler.Reconcile(t.Context(), f.req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := f.client.Get(t.Context(), f.req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "exceeding maximum allowed") {
		t.Errorf("Expected message to contain 'exceeding maximum allowed', got: %s", updatedRun.Status.Message)
	}
}

func TestReconcileStatusMessageBoundedOnFailure(t *testing.T) {
	hugeStderr := strings.Repeat("error line for testing\n", 500)
	f := newDiscoveringRunFixture(t, &sandboxclient.FakeClient{
		ListStderr: hugeStderr,
		ListErr:    errors.New("exit code 1"),
	})

	_, err := f.reconciler.Reconcile(t.Context(), f.req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := f.client.Get(t.Context(), f.req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if len(updatedRun.Status.Message) > discovery.MaxMessageBytes {
		t.Errorf("Expected status message length <= %d, got %d", discovery.MaxMessageBytes, len(updatedRun.Status.Message))
	}
}

func TestReconcileResourceExhaustedMapsToLimitError(t *testing.T) {
	f := newDiscoveringRunFixture(t, &sandboxclient.FakeClient{
		ListErr: status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000000 vs 4194304)"),
	})

	_, err := f.reconciler.Reconcile(t.Context(), f.req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := f.client.Get(t.Context(), f.req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "stdout limit exceeded") {
		t.Errorf("Expected message to contain 'stdout limit exceeded', got: %s", updatedRun.Status.Message)
	}
}

func TestReconcileNoSecondLimitInReconciler(t *testing.T) {
	// Manifest with valid tasks; reconciler does not re-check byte size when client reports no limit error
	validJSON := `{"tasks":[{"id":"t1","spec":"s1"}]}`
	f := newDiscoveringRunFixture(t, &sandboxclient.FakeClient{
		ListStdout: validJSON,
	})

	_, err := f.reconciler.Reconcile(t.Context(), f.req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := f.client.Get(t.Context(), f.req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
		t.Errorf("Expected phase Discovered, got %s (message: %s)", updatedRun.Status.Phase, updatedRun.Status.Message)
	}
}

func TestReconcileTransientCloneFailureRetriesAndSucceeds(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-retry",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run-retry")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.5",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	validManifestJSON := `{
		"tasks": [
			{"id": "triage/runbook_lookup", "workdir": "evals", "spec": "runbook.py@lookup"},
			{"id": "triage/summary_quality", "workdir": "evals", "spec": "summary.py@quality"}
		]
	}`

	cloneAttempts := 0
	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 1 && cfg.Command[1] == "clone" {
				cloneAttempts++
				if cloneAttempts <= 2 {
					// Simulate transient connection failure for first 2 attempts
					return &sandboxclient.ExecuteResult{
						ExitCode: 128,
						Stderr:   "fatal: unable to connect to git-server.eval-runner-sandbox.svc.cluster.local: Connection refused",
					}, nil
				}
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			}
			if len(cfg.Command) > 0 && (cfg.Command[0] == "./.evals/list" || strings.Contains(strings.Join(cfg.Command, " "), "./.evals/list")) {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: validManifestJSON}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ManifestBytes: []byte(validManifestJSON),
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
		Sleep: func(ctx context.Context, d time.Duration) error {
			return nil
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-retry"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Did not expect RequeueAfter on success")
	}

	if cloneAttempts != 3 {
		t.Errorf("Expected 3 clone attempts (2 failures + 1 success), got %d", cloneAttempts)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
		t.Errorf("Expected phase Discovered, got %s", updatedRun.Status.Phase)
	}
	if updatedRun.Status.Discovery == nil || updatedRun.Status.Discovery.TaskCount != 2 {
		t.Fatalf("Expected 2 tasks in discovery, got: %+v", updatedRun.Status.Discovery)
	}
	if updatedRun.Status.Discovery.ManifestPath == "" || updatedRun.Status.Discovery.ManifestHash == "" {
		t.Fatalf("Expected ManifestPath and ManifestHash populated in discovery, got: %+v", updatedRun.Status.Discovery)
	}
	// Assert attempts are recorded on success
	if !strings.Contains(updatedRun.Status.Message, "3 attempts") {
		t.Errorf("Expected status.Message to record 3 attempts, got: %s", updatedRun.Status.Message)
	}

	var checkSb sandboxv1beta1.Sandbox
	err = client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected sandbox to be deleted, got err: %v", err)
	}
}

func TestReconcileTransientCloneFailureExhaustsRetries(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-exhaust",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run-exhaust")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.5",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	cloneAttempts := 0
	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 1 && cfg.Command[1] == "clone" {
				cloneAttempts++
				return &sandboxclient.ExecuteResult{
					ExitCode: 128,
					Stderr:   "fatal: unable to connect to git-server:\ngit-server: errno=Connection refused",
				}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:               client,
		Scheme:               scheme,
		SandboxClient:        executor,
		SandboxNamespace:     "eval-runner-sandbox",
		MaxDiscoveryAttempts: 5,
		Sleep: func(ctx context.Context, d time.Duration) error {
			return nil
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-exhaust"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	if cloneAttempts != 5 {
		t.Errorf("Expected 5 clone attempts, got %d", cloneAttempts)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "step git clone failed after 5 attempts") {
		t.Errorf("Expected status.message to indicate clone step failed after 5 attempts, got: %s", updatedRun.Status.Message)
	}
	if !strings.Contains(updatedRun.Status.Message, "Connection refused") {
		t.Errorf("Expected status.message to include stderr detail, got: %s", updatedRun.Status.Message)
	}

	// Assert Sandbox was cleaned up
	var checkSb sandboxv1beta1.Sandbox
	err = client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected sandbox to be deleted on failure, got err: %v", err)
	}
}

func TestStepBasedFailures(t *testing.T) {
	tests := []struct {
		name              string
		client            *sandboxclient.FakeClient
		expectedMsgSubstr []string
	}{
		{
			name: "checkout failure fails immediately without retry",
			client: &sandboxclient.FakeClient{
				CheckoutExit:   1,
				CheckoutStderr: "error: pathspec 'bad-branch' did not match any file(s) known to git",
			},
			expectedMsgSubstr: []string{"step git checkout failed", "pathspec"},
		},
		{
			name: ".evals/list missing fails immediately without retry",
			client: &sandboxclient.FakeClient{
				ListErr: status.Errorf(codes.NotFound, "command or path not found: ./.evals/list"),
			},
			expectedMsgSubstr: []string{"step ./.evals/list failed", "missing .evals/list"},
		},
		{
			name: ".evals/list non-executable fails immediately without retry",
			client: &sandboxclient.FakeClient{
				ListErr: status.Errorf(codes.PermissionDenied, "permission denied: ./.evals/list"),
			},
			expectedMsgSubstr: []string{"step ./.evals/list failed", "not executable"},
		},
		{
			name: ".evals/list non-zero exit fails immediately without retry",
			client: &sandboxclient.FakeClient{
				ListErr: nil,
				ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
					if len(cfg.Command) > 0 && (cfg.Command[0] == "./.evals/list" || strings.Contains(strings.Join(cfg.Command, " "), "./.evals/list")) {
						return &sandboxclient.ExecuteResult{
							ExitCode: 2,
							Stderr:   "Traceback: invalid arguments",
						}, nil
					}
					return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
				},
			},
			expectedMsgSubstr: []string{"step ./.evals/list failed with exit code 2", "invalid arguments"},
		},
		{
			name: ".evals/list invalid JSON fails immediately without retry",
			client: &sandboxclient.FakeClient{
				ListStdout: "{invalid json content",
				ListStderr: "warning: parsing config",
			},
			expectedMsgSubstr: []string{"step ./.evals/list failed", "invalid JSON"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newDiscoveringRunFixture(t, tt.client)
			_, err := f.reconciler.Reconcile(t.Context(), f.req)
			if err != nil {
				t.Fatalf("Reconcile error: %v", err)
			}

			var updatedRun v1alpha1.Run
			if err := f.client.Get(t.Context(), f.req.NamespacedName, &updatedRun); err != nil {
				t.Fatalf("Failed to get run: %v", err)
			}
			if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
				t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
			}
			for _, substr := range tt.expectedMsgSubstr {
				if !strings.Contains(updatedRun.Status.Message, substr) {
					t.Errorf("Expected status message to contain %q, got: %s", substr, updatedRun.Status.Message)
				}
			}
			if strings.Contains(updatedRun.Status.Message, "attempts") {
				t.Errorf("Expected non-clone failure not to mention retry attempts, got: %s", updatedRun.Status.Message)
			}
		})
	}
}

func TestReconcilePodNeverReadyPastDeadline(t *testing.T) {
	scheme := setupScheme(t)

	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	pastDeadline := metav1.NewTime(now.Add(-5 * time.Minute))
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-run",
			Namespace:  "eval-runner",
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase:             v1alpha1.RunPhaseDiscovering,
			DiscoveryDeadline: &pastDeadline,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
		Now:              func() time.Time { return now },
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue after deadline failure, got %v", res.RequeueAfter)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "Discovery deadline exceeded") {
		t.Errorf("Expected failure message to mention deadline exceeded, got: %s", updatedRun.Status.Message)
	}
}

func TestReconcilePodSucceededFailsAndCleansUp(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-run",
			Namespace:  "eval-runner",
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase:   corev1.PodSucceeded,
			Message: "Container terminated with exit code 0 prematurely",
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue on PodSucceeded failure, got %v", res.RequeueAfter)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "succeeded unexpectedly") {
		t.Errorf("Expected failure message to mention 'succeeded unexpectedly', got: %s", updatedRun.Status.Message)
	}
	if !strings.Contains(updatedRun.Status.Message, "Container terminated with exit code 0 prematurely") {
		t.Errorf("Expected failure message to include pod status message, got: %s", updatedRun.Status.Message)
	}
}

func TestReconcilePodImagePullBackOffSurfacesWaitingReason(t *testing.T) {
	scheme := setupScheme(t)

	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	pastDeadline := metav1.NewTime(now.Add(-1 * time.Minute))
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-run",
			Namespace:  "eval-runner",
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase:             v1alpha1.RunPhaseDiscovering,
			DiscoveryDeadline: &pastDeadline,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "agent",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "Back-off pulling image bad-image:v1",
						},
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
		Now:              func() time.Time { return now },
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "ImagePullBackOff") {
		t.Errorf("Expected message to contain ImagePullBackOff, got: %s", updatedRun.Status.Message)
	}
	if !strings.Contains(updatedRun.Status.Message, "Back-off pulling image bad-image:v1") {
		t.Errorf("Expected message to contain container message, got: %s", updatedRun.Status.Message)
	}
}

func TestReconcilePodUnschedulableSurfacesPodScheduledFalse(t *testing.T) {
	scheme := setupScheme(t)

	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	pastDeadline := metav1.NewTime(now.Add(-1 * time.Minute))
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-run",
			Namespace:  "eval-runner",
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase:             v1alpha1.RunPhaseDiscovering,
			DiscoveryDeadline: &pastDeadline,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{
				{
					Type:    corev1.PodScheduled,
					Status:  corev1.ConditionFalse,
					Reason:  "Unschedulable",
					Message: "0/1 nodes are available: 1 Insufficient cpu.",
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
		Now:              func() time.Time { return now },
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "Unschedulable") {
		t.Errorf("Expected message to contain Unschedulable, got: %s", updatedRun.Status.Message)
	}
	if !strings.Contains(updatedRun.Status.Message, "Insufficient cpu") {
		t.Errorf("Expected message to contain Insufficient cpu, got: %s", updatedRun.Status.Message)
	}
}

func TestReconcileDeadlineSurvivesControllerRestart(t *testing.T) {
	scheme := setupScheme(t)

	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	initialDeadline := metav1.NewTime(now.Add(10 * time.Minute))

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-run",
			Namespace:  "eval-runner",
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase:             v1alpha1.RunPhaseDiscovering,
			DiscoveryDeadline: &initialDeadline,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconcilerRestarted := &RunReconciler{
		Client:            client,
		Scheme:            scheme,
		SandboxClient:     &sandboxclient.FakeClient{},
		SandboxNamespace:  "eval-runner-sandbox",
		DiscoveryDeadline: 20 * time.Minute,
		Now:               func() time.Time { return now.Add(2 * time.Minute) },
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	_, err := reconcilerRestarted.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var runAfterReconcile v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &runAfterReconcile); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if !runAfterReconcile.Status.DiscoveryDeadline.Time.Equal(initialDeadline.Time) {
		t.Errorf("Expected DiscoveryDeadline to remain %v, got %v", initialDeadline.Time, runAfterReconcile.Status.DiscoveryDeadline.Time)
	}

	reconcilerPastDeadline := &RunReconciler{
		Client:            client,
		Scheme:            scheme,
		SandboxClient:     &sandboxclient.FakeClient{},
		SandboxNamespace:  "eval-runner-sandbox",
		DiscoveryDeadline: 20 * time.Minute,
		Now:               func() time.Time { return now.Add(11 * time.Minute) },
	}
	_, err = reconcilerPastDeadline.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var runAfterExpiry v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &runAfterExpiry); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if runAfterExpiry.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected run to fail once original deadline passed, got %s", runAfterExpiry.Status.Phase)
	}
	if !strings.Contains(runAfterExpiry.Status.Message, "Discovery deadline exceeded") {
		t.Errorf("Expected failure message to mention deadline exceeded, got: %s", runAfterExpiry.Status.Message)
	}
}

func TestReconcileRetryLoopCutShortByDeadline(t *testing.T) {
	scheme := setupScheme(t)

	startTime := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	currentTime := startTime
	deadline := metav1.NewTime(startTime.Add(15 * time.Second))

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-run",
			Namespace:  "eval-runner",
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase:             v1alpha1.RunPhaseDiscovering,
			DiscoveryDeadline: &deadline,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.5",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	cloneAttempts := 0
	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 1 && cfg.Command[1] == "clone" {
				cloneAttempts++
				return &sandboxclient.ExecuteResult{
					ExitCode: 128,
					Stderr:   "fatal: unable to connect to git-server: Connection refused",
				}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:               client,
		Scheme:               scheme,
		SandboxClient:        executor,
		SandboxNamespace:     "eval-runner-sandbox",
		MaxDiscoveryAttempts: 5,
		DiscoveryRetryBackoff: []time.Duration{
			10 * time.Second,
			10 * time.Second,
		},
		Now: func() time.Time { return currentTime },
		Sleep: func(ctx context.Context, d time.Duration) error {
			// Advance simulated clock by 20s, which pushes time past the 15s deadline
			currentTime = currentTime.Add(20 * time.Second)
			return nil
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue on failure, got %v", res.RequeueAfter)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "Discovery deadline exceeded") {
		t.Errorf("Expected message to contain 'Discovery deadline exceeded', got: %s", updatedRun.Status.Message)
	}
	if !strings.Contains(updatedRun.Status.Message, "Connection refused") {
		t.Errorf("Expected message to contain stderr tail, got: %s", updatedRun.Status.Message)
	}

	// Verify retry loop was cut short: should only have made 1 attempt instead of 5
	if cloneAttempts != 1 {
		t.Errorf("Expected exactly 1 clone attempt before deadline cut the loop short, got %d", cloneAttempts)
	}

	// Assert Sandbox was cleaned up
	var checkSb sandboxv1beta1.Sandbox
	err = client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected sandbox to be deleted on failure, got err: %v", err)
	}
}

func TestReconcileCustomDiscoveryDeadlineConfigured(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-run",
			Namespace:  "eval-runner",
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	customDeadline := 25 * time.Minute
	fixedNow := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	reconciler := &RunReconciler{
		Client:            client,
		Scheme:            scheme,
		SandboxClient:     &sandboxclient.FakeClient{},
		SandboxNamespace:  "eval-runner-sandbox",
		DiscoveryDeadline: customDeadline,
		Now:               func() time.Time { return fixedNow },
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.DiscoveryDeadline == nil {
		t.Fatalf("Expected DiscoveryDeadline to be set")
	}

	expectedDeadline := fixedNow.Add(customDeadline)
	if !updatedRun.Status.DiscoveryDeadline.Time.Equal(expectedDeadline) {
		t.Errorf("DiscoveryDeadline %v does not equal expected %v", updatedRun.Status.DiscoveryDeadline.Time, expectedDeadline)
	}
}

func TestReconcileSandboxReadyConditionUsed(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-run",
			Namespace:  "eval-runner",
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun("test-run")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
			Reason: sandboxv1beta1.SandboxReasonDependenciesReady,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.5",
		},
	}

	validManifestJSON := `{"tasks":[{"id":"triage/lookup","spec":"runbook.py@lookup"}]}`
	executor := &sandboxclient.FakeClient{ListStdout: validManifestJSON}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Did not expect RequeueAfter on success")
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
		t.Errorf("Expected phase Discovered, got %s", updatedRun.Status.Phase)
	}
}

func TestReconcilePodAdoptedFromAnnotation(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-run",
			Namespace:  "eval-runner",
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))
	sb.Annotations = map[string]string{
		sandboxv1beta1.SandboxPodNameAnnotation: "custom-adopted-pod",
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-adopted-pod",
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.42",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	var dialedPodIP string
	validManifestJSON := `{"tasks":[{"id":"triage/lookup","spec":"runbook.py@lookup"}]}`
	factory := sandboxclient.FactoryFunc(func(podIP string, opts ...sandboxclient.Option) (sandboxclient.Client, error) {
		dialedPodIP = podIP
		return &sandboxclient.FakeClient{ListStdout: validManifestJSON}, nil
	})

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		ClientFactory:    factory,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Did not expect RequeueAfter on success")
	}

	if dialedPodIP != "10.244.0.42" {
		t.Errorf("Expected ClientFactory.NewClient to be called with pod IP 10.244.0.42, got %q", dialedPodIP)
	}
}

func TestRequeueDelay(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	r := &RunReconciler{
		Now: func() time.Time { return now },
	}

	// 1. Nil deadline -> 2s
	if got := r.requeueDelay(nil); got != 2*time.Second {
		t.Errorf("requeueDelay(nil) = %v, want 2s", got)
	}

	// 2. Future deadline > 2s -> 2s
	dl1 := metav1.NewTime(now.Add(10 * time.Second))
	if got := r.requeueDelay(&dl1); got != 2*time.Second {
		t.Errorf("requeueDelay(10s) = %v, want 2s", got)
	}

	// 3. Future deadline < 2s -> remaining
	dl2 := metav1.NewTime(now.Add(1200 * time.Millisecond))
	if got := r.requeueDelay(&dl2); got != 1200*time.Millisecond {
		t.Errorf("requeueDelay(1.2s) = %v, want 1.2s", got)
	}

	// 4. Past deadline -> 0
	dl3 := metav1.NewTime(now.Add(-5 * time.Second))
	if got := r.requeueDelay(&dl3); got != 0 {
		t.Errorf("requeueDelay(-5s) = %v, want 0", got)
	}
}

func TestReconcileBearerAuthSecretLifecycleAndRestart(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-auth-lifecycle",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
	}

	var passedToken string
	validManifestJSON := `{"tasks":[{"id":"triage/lookup","spec":"runbook.py@lookup"}]}`
	factory := sandboxclient.FactoryFunc(func(podIP string, opts ...sandboxclient.Option) (sandboxclient.Client, error) {
		var o sandboxclient.ClientOptions
		for _, opt := range opts {
			opt(&o)
		}
		passedToken = o.AuthToken
		return &sandboxclient.FakeClient{ListStdout: validManifestJSON}, nil
	})

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler1 := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		ClientFactory:    factory,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-auth-lifecycle"}}

	// 1. Initial reconcile: Pending -> Discovering
	// Sandbox and label-free Secret should be created
	res, err := reconciler1.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected RequeueAfter when sandbox was created")
	}

	// Verify label-free Secret in eval-runner namespace
	secretName := discovery.AuthSecretNameForRun(run.Name)
	var authSecret corev1.Secret
	if err := client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: secretName}, &authSecret); err != nil {
		t.Fatalf("Failed to get auth Secret in eval-runner namespace: %v", err)
	}
	if len(authSecret.Labels) != 0 {
		t.Errorf("Expected Secret to be label-free, but got labels: %+v", authSecret.Labels)
	}
	secretToken := string(authSecret.Data["token"])
	if secretToken == "" {
		t.Fatalf("Expected Secret to contain non-empty token")
	}

	// Verify Sandbox created with matching --auth-token-sha256 digest and LabelSandbox: "true"
	sandboxName := discovery.SandboxNameForRun(run.Name)
	var createdSb sandboxv1beta1.Sandbox
	if err := client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &createdSb); err != nil {
		t.Fatalf("Failed to get created Sandbox: %v", err)
	}
	if createdSb.Labels[discovery.LabelSandbox] != "true" {
		t.Errorf("Expected Sandbox label %s=true, got: %s", discovery.LabelSandbox, createdSb.Labels[discovery.LabelSandbox])
	}
	if createdSb.Spec.PodTemplate.ObjectMeta.Labels[discovery.LabelSandbox] != "true" {
		t.Errorf("Expected PodTemplate label %s=true, got: %s", discovery.LabelSandbox, createdSb.Spec.PodTemplate.ObjectMeta.Labels[discovery.LabelSandbox])
	}

	expectedDigest := discovery.AuthTokenDigest(secretToken)
	expectedFlag := "--auth-token-sha256=" + expectedDigest
	containers := createdSb.Spec.PodTemplate.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("Expected 1 container, got %d", len(containers))
	}
	var foundAuthFlag bool
	for _, arg := range containers[0].Command {
		if arg == expectedFlag {
			foundAuthFlag = true
			break
		}
	}
	if !foundAuthFlag {
		t.Errorf("Expected container command to contain %q, got: %v", expectedFlag, containers[0].Command)
	}

	// 2. Simulate controller restart by instantiating a fresh RunReconciler without memory state
	reconciler2 := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		ClientFactory:    factory,
		SandboxNamespace: "eval-runner-sandbox",
	}

	// Create backing pod in Ready state
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
			Labels:    createdSb.Spec.PodTemplate.ObjectMeta.Labels,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.55",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if err := client.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Reconcile with restarted controller: should load token from Secret and present to client factory
	res, err = reconciler2.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error after restart: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue after successful discovery")
	}

	if passedToken != secretToken {
		t.Errorf("Expected factory to receive token %q from Secret, got %q", secretToken, passedToken)
	}

	// Verify Run is Discovered
	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get updated run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
		t.Errorf("Expected phase Discovered, got %s", updatedRun.Status.Phase)
	}

	// 3. Verify Sandbox AND Secret are cleaned up after completion
	var checkSb sandboxv1beta1.Sandbox
	if err := client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb); err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected Sandbox to be deleted after discovery, got err: %v", err)
	}
	var checkSecret corev1.Secret
	if err := client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: secretName}, &checkSecret); err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected auth Secret to be deleted after discovery, got err: %v", err)
	}
}

func TestReconcileAuthSecretCleanedUpOnFailure(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-auth-failure",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "deadbeef00000000000000000000000000000000",
			},
		},
	}

	factory := sandboxclient.FactoryFunc(func(podIP string, opts ...sandboxclient.Option) (sandboxclient.Client, error) {
		return &sandboxclient.FakeClient{
			CloneErr: errors.New("fatal: repository not found"),
		}, nil
	})

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		ClientFactory:    factory,
		SandboxNamespace: "eval-runner-sandbox",
		Sleep:            func(ctx context.Context, d time.Duration) error { return nil },
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-auth-failure"}}

	// 1. First reconcile: create sandbox and secret
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	sandboxName := discovery.SandboxNameForRun(run.Name)
	secretName := discovery.AuthSecretNameForRun(run.Name)

	// Verify Secret was created
	var secret corev1.Secret
	if err := client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: secretName}, &secret); err != nil {
		t.Fatalf("Expected Secret to exist: %v", err)
	}

	// 2. Add ready pod and reconcile to fail
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.66",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if err := client.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	// 3. Verify Secret was cleaned up on failure
	if err := client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: secretName}, &secret); err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected auth Secret to be deleted on failure, got err: %v", err)
	}
}

func TestReconcileUnauthenticatedFailsFastWithoutRetry(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-unauth",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
	}

	fakeClient := &sandboxclient.FakeClient{
		CloneErr: status.Error(codes.Unauthenticated, "missing or invalid bearer token"),
	}
	factory := sandboxclient.FactoryFunc(func(podIP string, opts ...sandboxclient.Option) (sandboxclient.Client, error) {
		return fakeClient, nil
	})

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		ClientFactory:    factory,
		SandboxNamespace: "eval-runner-sandbox",
		Sleep:            func(ctx context.Context, d time.Duration) error { return nil },
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-unauth"}}

	// 1. Initial reconcile: create sandbox and secret
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	sandboxName := discovery.SandboxNameForRun(run.Name)

	// 2. Add ready pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.77",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if err := client.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// 3. Reconcile: unauthenticated error should fail immediately on attempt 1 without retry
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	cloneCount := 0
	for _, call := range fakeClient.Calls {
		if len(call.Command) > 1 && call.Command[1] == "clone" {
			cloneCount++
		}
	}
	if cloneCount != 1 {
		t.Errorf("Expected exactly 1 clone call (no retries for Unauthenticated), got %d", cloneCount)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get updated run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "unauthenticated") {
		t.Errorf("Expected message to mention unauthenticated, got: %s", updatedRun.Status.Message)
	}
}

func TestReconcileExistingSecretLacksTokenFails(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-bad-secret",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
	}

	// Pre-create Secret with no token data
	secretName := discovery.AuthSecretNameForRun(run.Name)
	badSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: "eval-runner",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, badSecret).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-bad-secret"}}

	// Reconcile should fail because existing Secret lacks a token
	_, err := reconciler.Reconcile(t.Context(), req)
	if err == nil {
		t.Fatalf("Expected reconcile to fail when existing Secret lacks token")
	}
	if !strings.Contains(err.Error(), "lacks a token") {
		t.Errorf("Expected error to mention 'lacks a token', got: %v", err)
	}
}

func TestTaskEligibilityAndFiltering(t *testing.T) {
	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-eligible-1", Spec: "test.py@1", Resources: []v1alpha1.ManifestResource{}},
			{ID: "task-eligible-2", Spec: "test.py@2"},
			{ID: "task-with-resources", Spec: "test.py@3", Resources: []v1alpha1.ManifestResource{{Type: "docker"}}},
		},
	}

	r := &RunReconciler{}

	// Case 1: spec.Tasks empty -> all tasks considered
	tasks1 := r.initializeTasks(v1alpha1.RunSpec{}, manifest)
	if len(tasks1) != 3 {
		t.Fatalf("Expected 3 tasks, got %d", len(tasks1))
	}
	if tasks1[0].Phase != v1alpha1.TaskPhasePending {
		t.Errorf("Expected task-eligible-1 to be Pending, got %s", tasks1[0].Phase)
	}
	if tasks1[1].Phase != v1alpha1.TaskPhasePending {
		t.Errorf("Expected task-eligible-2 to be Pending, got %s", tasks1[1].Phase)
	}
	if tasks1[2].Phase != v1alpha1.TaskPhaseFailed || tasks1[2].Message != "resources not supported yet" {
		t.Errorf("Expected task-with-resources to be Failed with 'resources not supported yet', got %s (%s)", tasks1[2].Phase, tasks1[2].Message)
	}

	// Case 2: spec.Tasks allow-list
	tasks2 := r.initializeTasks(v1alpha1.RunSpec{
		Tasks: []string{"task-eligible-1", "nonexistent-task", "task-with-resources"},
	}, manifest)
	if len(tasks2) != 3 {
		t.Fatalf("Expected 3 tasks, got %d", len(tasks2))
	}
	if tasks2[0].ID != "task-eligible-1" || tasks2[0].Phase != v1alpha1.TaskPhasePending {
		t.Errorf("Expected task-eligible-1 to be Pending, got %+v", tasks2[0])
	}
	if tasks2[1].ID != "nonexistent-task" || tasks2[1].Phase != v1alpha1.TaskPhaseFailed || tasks2[1].Message != "task not found in manifest" {
		t.Errorf("Expected nonexistent-task to be Failed, got %+v", tasks2[1])
	}
	if tasks2[2].ID != "task-with-resources" || tasks2[2].Phase != v1alpha1.TaskPhaseFailed || tasks2[2].Message != "resources not supported yet" {
		t.Errorf("Expected task-with-resources to be Failed, got %+v", tasks2[2])
	}
}

func TestReconcileDiscoveryToRunningWhenModelSet(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	token := "discovery-token-12345"
	digest := discovery.AuthTokenDigest(token)
	sandboxName := discovery.SandboxNameForRun("test-run")
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", digest)

	secretName := discovery.AuthSecretNameForRun("test-run")
	authSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: "eval-runner",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"token": []byte(token),
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.5",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	manifestJSON := `{
		"tasks": [
			{"id": "docs/readme_comprehension", "workdir": "evals", "spec": "docs.py@readme"},
			{"id": "docs/readme_comprehension_blind", "workdir": "evals", "spec": "docs.py@blind"}
		]
	}`

	executor := &sandboxclient.FakeClient{
		ListStdout: manifestJSON,
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod, authSecret).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected Requeue: true to transition to Running")
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get updated run: %v", err)
	}

	if updatedRun.Status.Phase != v1alpha1.RunPhaseRunning {
		t.Errorf("Expected phase %s, got %s", v1alpha1.RunPhaseRunning, updatedRun.Status.Phase)
	}
	if updatedRun.Status.ExecutionDeadline == nil {
		t.Errorf("Expected ExecutionDeadline to be set")
	}
	if len(updatedRun.Status.Tasks) != 2 {
		t.Fatalf("Expected 2 tasks initialized in status, got %d", len(updatedRun.Status.Tasks))
	}
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhasePending {
		t.Errorf("Expected task 0 to be Pending, got %s", updatedRun.Status.Tasks[0].Phase)
	}
}

func TestReconcileSampleExecutionFullLifecycle(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-1", Spec: "eval.py@task1", Workdir: "evals"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-1"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-1", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	inspectLog := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "task-1", "scorers": [{"name": "choice"}]},
		"samples": [
			{"id": 1, "scores": {"choice": {"value": "C"}}},
			{"id": 2, "scores": {"choice": {"value": "I"}}}
		]
	}`

	executor := &sandboxclient.FakeClient{
		InspectLog: []byte(inspectLog),
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}

	// Turn 1: Starts pending task, creates task sandbox
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 1 error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	if err := cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox")); err != nil {
		t.Fatalf("Failed to list sandboxes: %v", err)
	}
	if len(sbList.Items) != 1 {
		t.Fatalf("Expected 1 task sandbox created, got %d", len(sbList.Items))
	}
	taskSB := sbList.Items[0]

	// Add backing ready Pod for task sandbox
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskSB.Name,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.12",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if err := cl.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Turn 2: Executes task in sandbox and records scores
	res, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 2 error: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected Requeue to check completion")
	}

	// Turn 3: All tasks are completed, Run marked Completed
	res, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 3 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get updated run: %v", err)
	}

	if updatedRun.Status.Phase != v1alpha1.RunPhaseCompleted {
		t.Errorf("Expected Run phase Completed, got %s", updatedRun.Status.Phase)
	}
	if len(updatedRun.Status.Tasks) != 1 {
		t.Fatalf("Expected 1 task status, got %d", len(updatedRun.Status.Tasks))
	}
	t1 := updatedRun.Status.Tasks[0]
	if t1.Phase != v1alpha1.TaskPhaseCompleted {
		t.Errorf("Expected task phase Completed, got %s", t1.Phase)
	}
	if t1.Samples != 2 {
		t.Errorf("Expected 2 samples, got %d", t1.Samples)
	}
	if t1.Successes != 1 {
		t.Errorf("Expected 1 success, got %d", t1.Successes)
	}
	if t1.MeanScore == nil || *t1.MeanScore != 0.5 {
		t.Errorf("Expected meanScore 0.5, got %v", t1.MeanScore)
	}

	// Assert task sandbox is deleted
	var postSBList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &postSBList, client.InNamespace("eval-runner-sandbox"))
	if len(postSBList.Items) != 0 {
		t.Errorf("Expected task sandbox to be deleted, found %d", len(postSBList.Items))
	}
}

func TestReconcileTaskFailureDoesNotFailOtherTasks(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-fail", Spec: "eval.py@fail"},
			{ID: "task-ok", Spec: "eval.py@ok"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         2,
				EligibleTaskCount: 2,
				TaskIDs:           []string{"task-fail", "task-ok"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-fail", Phase: v1alpha1.TaskPhasePending},
				{ID: "task-ok", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	okLog := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "task-ok"},
		"samples": [{"id": 1, "scores": {"choice": {"value": 1.0}}}]
	}`

	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 0 && cfg.Command[0] == "inspect" {
				if strings.Contains(cfg.Command[2], "fail") {
					return &sandboxclient.ExecuteResult{ExitCode: 1, Stderr: "syntax error in task-fail"}, nil
				}
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			}
			if len(cfg.Command) > 0 && cfg.Command[0] == "ls" {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "log.json\n"}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			return []byte(okLog), nil
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}

	// Turn 1: Create sandboxes
	_, _ = reconciler.Reconcile(t.Context(), req)

	// Create pods for all sandboxes
	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	for _, sb := range sbList.Items {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sb.Name,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.10",
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		_ = cl.Create(t.Context(), pod)
	}

	// Turn 2: Run both tasks
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	// Turn 3: Finish Run
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)

	if updatedRun.Status.Phase != v1alpha1.RunPhaseCompleted {
		t.Errorf("Expected Run to be Completed, got %s", updatedRun.Status.Phase)
	}

	taskMap := make(map[string]v1alpha1.TaskStatus)
	for _, t := range updatedRun.Status.Tasks {
		taskMap[t.ID] = t
	}

	tFail := taskMap["task-fail"]
	if tFail.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task-fail to be Failed, got %s", tFail.Phase)
	}
	expectedFailMsg := "step inspect eval failed with exit code 1: syntax error in task-fail"
	if tFail.Message != expectedFailMsg {
		t.Errorf("Expected failure message %q, got %q", expectedFailMsg, tFail.Message)
	}

	tOk := taskMap["task-ok"]
	if tOk.Phase != v1alpha1.TaskPhaseCompleted {
		t.Errorf("Expected task-ok to be Completed, got %s", tOk.Phase)
	}
	if tOk.Samples != 1 || tOk.Successes != 1 {
		t.Errorf("Expected task-ok to have 1 sample, 1 success, got %+v", tOk)
	}
}

func TestReconcileAllTasksFailedYieldsRunFailed(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-1", Spec: "eval.py@task1"},
			{ID: "task-2", Spec: "eval.py@task2"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Model: "google/gemini-3.5-flash-lite",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         2,
				EligibleTaskCount: 2,
				TaskIDs:           []string{"task-1", "task-2"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-1", Phase: v1alpha1.TaskPhasePending},
				{ID: "task-2", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 0 && cfg.Command[0] == "inspect" {
				if strings.Contains(cfg.Command[2], "task1") {
					return &sandboxclient.ExecuteResult{
						ExitCode: 1,
						Stderr:   "ERROR: Google API requires optional dependencies. Install with:\npip install google-genai",
					}, nil
				}
				if strings.Contains(cfg.Command[2], "task2") {
					return &sandboxclient.ExecuteResult{
						ExitCode: 2,
						Stderr:   "SyntaxError: invalid syntax",
					}, nil
				}
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultModelSecret,
			Namespace: "eval-runner-sandbox",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, secret).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
		ModelSecret:      DefaultModelSecret,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}

	// Turn 1: Create sandboxes
	_, _ = reconciler.Reconcile(t.Context(), req)

	// Create pods for all sandboxes
	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	if len(sbList.Items) != 2 {
		t.Fatalf("Expected 2 sandboxes created, got %d", len(sbList.Items))
	}
	for _, sb := range sbList.Items {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sb.Name,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.10",
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		_ = cl.Create(t.Context(), pod)
	}

	// Turn 2: Run both tasks (both will fail)
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	// Turn 3: Finish Run
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)

	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected Run phase Failed when all tasks fail, got %s", updatedRun.Status.Phase)
	}
	expectedRunMsg := "all 2 tasks failed"
	if updatedRun.Status.Message != expectedRunMsg {
		t.Errorf("Expected Run message %q, got %q", expectedRunMsg, updatedRun.Status.Message)
	}

	taskMap := make(map[string]v1alpha1.TaskStatus)
	for _, t := range updatedRun.Status.Tasks {
		taskMap[t.ID] = t
	}

	t1 := taskMap["task-1"]
	if t1.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task-1 to be Failed, got %s", t1.Phase)
	}
	expectedT1Msg := "step inspect eval failed with exit code 1: ERROR: Google API requires optional dependencies. Install with:\npip install google-genai"
	if t1.Message != expectedT1Msg {
		t.Errorf("Expected task-1 message %q, got %q", expectedT1Msg, t1.Message)
	}

	t2 := taskMap["task-2"]
	if t2.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task-2 to be Failed, got %s", t2.Phase)
	}
	expectedT2Msg := "step inspect eval failed with exit code 2: SyntaxError: invalid syntax"
	if t2.Message != expectedT2Msg {
		t.Errorf("Expected task-2 message %q, got %q", expectedT2Msg, t2.Message)
	}

	// Assert all task sandboxes are deleted
	var postSBList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &postSBList, client.InNamespace("eval-runner-sandbox"))
	if len(postSBList.Items) != 0 {
		t.Errorf("Expected all task sandboxes to be deleted, found %d", len(postSBList.Items))
	}
}

func TestReconcileExecutionDeadlineExceeded(t *testing.T) {
	scheme := setupScheme(t)

	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	pastDeadline := metav1.NewTime(now.Add(-10 * time.Second))

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase:             v1alpha1.RunPhaseRunning,
			ExecutionDeadline: &pastDeadline,
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-1", Phase: v1alpha1.TaskPhaseRunning},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		Now:              func() time.Time { return now },
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)

	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected Run phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "Execution deadline exceeded") {
		t.Errorf("Expected deadline exceeded message, got %q", updatedRun.Status.Message)
	}
}

func TestReconcileModelSecretPassedToSandbox(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-gemini", Spec: "eval.py@gemini"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "google/gemini-3.5-flash-lite",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-gemini"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-gemini", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gemini-api-key-secret",
			Namespace: "eval-runner-sandbox",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, secret).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		ModelSecret:      "gemini-api-key-secret",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	if len(sbList.Items) != 1 {
		t.Fatalf("Expected 1 sandbox created, got %d", len(sbList.Items))
	}

	c := sbList.Items[0].Spec.PodTemplate.Spec.Containers[0]
	if len(c.EnvFrom) != 1 || c.EnvFrom[0].SecretRef == nil || c.EnvFrom[0].SecretRef.Name != "gemini-api-key-secret" {
		t.Errorf("Expected EnvFrom pointing to gemini-api-key-secret, got: %+v", c.EnvFrom)
	}
}

func TestReconcileModelSecret_AbsentWithNonMockModel(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-gemini", Spec: "eval.py@gemini"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-absent-secret",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "google/gemini-3.5-flash-lite",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-gemini"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-gemini", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		ModelSecret:      DefaultModelSecret,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-absent-secret"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	// No sandbox should be created
	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	if len(sbList.Items) != 0 {
		t.Fatalf("Expected 0 sandboxes created when secret absent for non-mock model, got %d", len(sbList.Items))
	}

	// Task should be marked Failed with clear message
	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if len(updatedRun.Status.Tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(updatedRun.Status.Tasks))
	}
	task := updatedRun.Status.Tasks[0]
	if task.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task Phase Failed, got %s", task.Phase)
	}
	expectedMsg := "no model credentials configured: create Secret eval-runner-sandbox/model-credentials or set --model-secret"
	if task.Message != expectedMsg {
		t.Errorf("Expected task Message %q, got %q", expectedMsg, task.Message)
	}

	// Second reconcile transitions Run phase to Failed
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Second reconcile error: %v", err)
	}
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected Run Phase Failed when all tasks fail, got %s", updatedRun.Status.Phase)
	}
}

func TestReconcileModelSecret_AbsentWithMockModel(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-mock", Spec: "eval.py@mock"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-mockllm",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-mock"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-mock", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		ModelSecret:      DefaultModelSecret,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-mockllm"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	// Sandbox should be created without credentials (EnvFrom empty)
	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	if len(sbList.Items) != 1 {
		t.Fatalf("Expected 1 sandbox created for mockllm when secret absent, got %d", len(sbList.Items))
	}

	c := sbList.Items[0].Spec.PodTemplate.Spec.Containers[0]
	if len(c.EnvFrom) != 0 {
		t.Errorf("Expected 0 EnvFrom for mockllm without secret, got: %+v", c.EnvFrom)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseRunning {
		t.Errorf("Expected task Phase Running for mockllm, got %s", updatedRun.Status.Tasks[0].Phase)
	}
}

func TestReconcileModelSecret_PresentWithMockModel(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-mock", Spec: "eval.py@mock"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-mockllm-secret-present",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-mock"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-mock", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "model-credentials",
			Namespace: "eval-runner-sandbox",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, secret).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		ModelSecret:      DefaultModelSecret,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-mockllm-secret-present"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	if len(sbList.Items) != 1 {
		t.Fatalf("Expected 1 sandbox created, got %d", len(sbList.Items))
	}

	c := sbList.Items[0].Spec.PodTemplate.Spec.Containers[0]
	if len(c.EnvFrom) != 1 || c.EnvFrom[0].SecretRef == nil || c.EnvFrom[0].SecretRef.Name != "model-credentials" {
		t.Errorf("Expected EnvFrom pointing to model-credentials, got: %+v", c.EnvFrom)
	}
}

func TestReconcileModelSecret_ExplicitlyDisabledWithNonMockModel(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-gemini", Spec: "eval.py@gemini"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-disabled-secret",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "google/gemini-3.5-flash-lite",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-gemini"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-gemini", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	// Even if Secret exists in namespace, ModelSecret == "" means explicitly disabled
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "model-credentials",
			Namespace: "eval-runner-sandbox",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, secret).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		ModelSecret:      "", // explicitly disabled
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-disabled-secret"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	if len(sbList.Items) != 0 {
		t.Fatalf("Expected 0 sandboxes created, got %d", len(sbList.Items))
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	task := updatedRun.Status.Tasks[0]
	if task.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task Phase Failed, got %s", task.Phase)
	}
	expectedMsg := "no model credentials configured: create Secret eval-runner-sandbox/model-credentials or set --model-secret"
	if task.Message != expectedMsg {
		t.Errorf("Expected task Message %q, got %q", expectedMsg, task.Message)
	}
}

func TestReconcileModelSecret_ExplicitlyDisabledWithMockModel(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-mock", Spec: "eval.py@mock"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-disabled-mock",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-mock"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-mock", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		ModelSecret:      "", // explicitly disabled
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-disabled-mock"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	if len(sbList.Items) != 1 {
		t.Fatalf("Expected 1 sandbox created, got %d", len(sbList.Items))
	}

	c := sbList.Items[0].Spec.PodTemplate.Spec.Containers[0]
	if len(c.EnvFrom) != 0 {
		t.Errorf("Expected 0 EnvFrom, got: %+v", c.EnvFrom)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseRunning {
		t.Errorf("Expected task Phase Running, got %s", updatedRun.Status.Tasks[0].Phase)
	}
}

func TestReconcileModelSecret_CustomNameAbsentNonMockModel(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-gemini", Spec: "eval.py@gemini"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-custom-absent",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "google/gemini-3.5-flash-lite",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-gemini"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-gemini", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		ModelSecret:      "custom-secret",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-custom-absent"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	if len(sbList.Items) != 0 {
		t.Fatalf("Expected 0 sandboxes created, got %d", len(sbList.Items))
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	task := updatedRun.Status.Tasks[0]
	if task.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task Phase Failed, got %s", task.Phase)
	}
	expectedMsg := "no model credentials configured: create Secret eval-runner-sandbox/custom-secret or set --model-secret"
	if task.Message != expectedMsg {
		t.Errorf("Expected task Message %q, got %q", expectedMsg, task.Message)
	}
}

func TestCheckModelSecretAtStartup(t *testing.T) {
	ctx := t.Context()
	logger := zap.New(zap.UseDevMode(true))

	// Case 1: Secret exists
	clientWithSecret := fakek8s.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "model-credentials",
			Namespace: "eval-runner-sandbox",
		},
	})
	CheckModelSecretAtStartup(ctx, clientWithSecret, "eval-runner-sandbox", "model-credentials", logger)

	// Case 2: Secret does not exist
	clientEmpty := fakek8s.NewSimpleClientset()
	CheckModelSecretAtStartup(ctx, clientEmpty, "eval-runner-sandbox", "model-credentials", logger)

	// Case 3: Explicitly disabled
	CheckModelSecretAtStartup(ctx, clientEmpty, "eval-runner-sandbox", "", logger)
}

func TestIsMockModel(t *testing.T) {
	tests := []struct {
		model    string
		expected bool
	}{
		{"mockllm/model", true},
		{"mockllm", true},
		{"mockllm/custom-model", true},
		{"  MOCKLLM/Model  ", true},
		{"google/gemini-3.5-flash-lite", false},
		{"openai/gpt-4o", false},
		{"anthropic/claude-3-5-sonnet", false},
		{"", false},
		{"   ", false},
	}

	for _, tt := range tests {
		got := IsMockModel(tt.model)
		if got != tt.expected {
			t.Errorf("IsMockModel(%q) = %v; expected %v", tt.model, got, tt.expected)
		}
	}
}

func TestReconcileRunningTaskWithMissingSandboxFailsFast(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-missing", Spec: "eval.py@task"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-missing-sb",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-missing"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-missing", Phase: v1alpha1.TaskPhaseRunning},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-missing-sb"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)

	if len(updatedRun.Status.Tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(updatedRun.Status.Tasks))
	}
	tMissing := updatedRun.Status.Tasks[0]
	if tMissing.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task to fail fast when sandbox missing, got phase %s", tMissing.Phase)
	}
	if !strings.Contains(tMissing.Message, "task sandbox missing") {
		t.Errorf("Expected failure message to mention sandbox missing, got: %s", tMissing.Message)
	}
}

func TestReconcileRunningTaskSandboxCacheLagGracePeriodAndExpiry(t *testing.T) {
	scheme := setupScheme(t)

	startTime := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	currentTime := startTime

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-lag", Spec: "eval.py@lag"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-cache-lag",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-lag"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-lag", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	hideSandboxesFromCache := false
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if hideSandboxesFromCache {
					if _, ok := list.(*sandboxv1beta1.SandboxList); ok {
						return nil
					}
				}
				return c.List(ctx, list, opts...)
			},
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if hideSandboxesFromCache {
					if _, ok := obj.(*sandboxv1beta1.Sandbox); ok {
						return apierrors.NewNotFound(sandboxv1beta1.GroupVersion.WithResource("sandboxes").GroupResource(), key.Name)
					}
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		Now:              func() time.Time { return currentTime },
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-cache-lag"}}

	// Turn 1: Starts pending task, creates sandbox in API server, records StartedAt in status
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run after Turn 1: %v", err)
	}
	if len(updatedRun.Status.Tasks) != 1 || updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseRunning {
		t.Fatalf("Expected task to be Running, got %+v", updatedRun.Status.Tasks)
	}
	if updatedRun.Status.Tasks[0].StartedAt == nil {
		t.Fatalf("Expected StartedAt to be set on Running task")
	}

	// Turn 2: Simulate cache lag. Cache does not see sandbox.
	// Current time is 5 seconds after start (within 30s grace period).
	hideSandboxesFromCache = true
	currentTime = startTime.Add(5 * time.Second)

	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 reconcile error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected RequeueAfter > 0 during grace period, got %v", res.RequeueAfter)
	}

	// Verify task is STILL Running and not failed spuriously
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run after Turn 2: %v", err)
	}
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseRunning {
		t.Errorf("Expected task to remain Running during grace period, got %s", updatedRun.Status.Tasks[0].Phase)
	}

	// Turn 3: Grace period has now elapsed (35 seconds after start > 30s default grace period).
	// Sandbox is still absent from cache.
	currentTime = startTime.Add(35 * time.Second)

	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 3 reconcile error: %v", err)
	}

	// Verify task now fails terminal
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run after Turn 3: %v", err)
	}
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task to fail after grace period elapsed, got %s", updatedRun.Status.Tasks[0].Phase)
	}
	if !strings.Contains(updatedRun.Status.Tasks[0].Message, "task sandbox missing") {
		t.Errorf("Expected failure message to mention sandbox missing, got: %s", updatedRun.Status.Tasks[0].Message)
	}
}

func TestReconcileRunningTaskSandboxCacheLagRecoversBeforeGracePeriod(t *testing.T) {
	scheme := setupScheme(t)

	startTime := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	currentTime := startTime

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-recover", Spec: "eval.py@recover"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-cache-lag-recover",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-recover"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-recover", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	hideSandboxesFromCache := false
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if hideSandboxesFromCache {
					if _, ok := list.(*sandboxv1beta1.SandboxList); ok {
						return nil
					}
				}
				return c.List(ctx, list, opts...)
			},
		}).
		Build()

	inspectLog := `{"version":2,"status":"success","eval":{"task":"task-recover"},"samples":[{"id":1,"scores":{"choice":{"value":1.0}}}]}`
	executor := &sandboxclient.FakeClient{
		InspectLog: []byte(inspectLog),
	}

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
		Now:              func() time.Time { return currentTime },
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-cache-lag-recover"}}

	// Turn 1: Create sandbox
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}

	// Turn 2: Cache lags (5s in). Task stays Running.
	hideSandboxesFromCache = true
	currentTime = startTime.Add(5 * time.Second)
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected RequeueAfter > 0 during grace period")
	}

	// Turn 3: Cache catches up (10s in), backing Pod is ready
	hideSandboxesFromCache = false
	currentTime = startTime.Add(10 * time.Second)

	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	if len(sbList.Items) != 1 {
		t.Fatalf("Expected 1 sandbox in client, got %d", len(sbList.Items))
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sbList.Items[0].Name,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.15",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	if err := cl.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Reconcile Turn 3: Executes task
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 3 error: %v", err)
	}

	// Turn 4: Completes Run
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 4 error: %v", err)
	}

	var finalRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &finalRun)
	if finalRun.Status.Phase != v1alpha1.RunPhaseCompleted {
		t.Errorf("Expected Run to be Completed, got %s", finalRun.Status.Phase)
	}
	if finalRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseCompleted {
		t.Errorf("Expected task to be Completed, got %s", finalRun.Status.Tasks[0].Phase)
	}
}

func TestReconcileRunningTaskSandboxCacheLagDirectAPIReader(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-direct", Spec: "eval.py@direct"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-api-reader",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-direct"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-direct", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	hideFromCacheList := false
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if hideFromCacheList {
					if _, ok := list.(*sandboxv1beta1.SandboxList); ok {
						return nil
					}
				}
				return c.List(ctx, list, opts...)
			},
		}).
		Build()

	// APIReader points to uncached client which can read the sandbox directly
	apiReader := cl

	inspectLog := `{"version":2,"status":"success","eval":{"task":"task-direct"},"samples":[{"id":1,"scores":{"choice":{"value":1.0}}}]}`
	executor := &sandboxclient.FakeClient{
		InspectLog: []byte(inspectLog),
	}

	reconciler := &RunReconciler{
		Client:           cl,
		APIReader:        apiReader,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-api-reader"}}

	// Turn 1: Create sandbox
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}

	// Simulate cache lag on List, but create ready Pod
	hideFromCacheList = true

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.TaskSandboxNameForRun(run.Name, "task-direct"),
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.18",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	if err := cl.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Turn 2: Cache List returns 0 sandboxes, but APIReader finds it and executes
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected Requeue after executing task via APIReader")
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseCompleted {
		t.Errorf("Expected task to complete via APIReader uncached read, got %s (msg: %s)", updatedRun.Status.Tasks[0].Phase, updatedRun.Status.Tasks[0].Message)
	}
}

func TestReconcileRunningTaskGenuinelyDeletedFailsAfterGracePeriod(t *testing.T) {
	scheme := setupScheme(t)

	startTime := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	currentTime := startTime

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-del", Spec: "eval.py@del"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-genuine-delete",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-del"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-del", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		Now:              func() time.Time { return currentTime },
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-genuine-delete"}}

	// Turn 1: Create sandbox
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}

	// Genuinely delete the sandbox from the API server
	sbName := discovery.TaskSandboxNameForRun(run.Name, "task-del")
	var sb sandboxv1beta1.Sandbox
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sbName}, &sb); err != nil {
		t.Fatalf("Failed to get created sandbox: %v", err)
	}
	if err := cl.Delete(t.Context(), &sb); err != nil {
		t.Fatalf("Failed to delete sandbox: %v", err)
	}

	// Turn 2: Reconcile at t = 5s (within grace period). Task must stay Running and requeue.
	currentTime = startTime.Add(5 * time.Second)
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected RequeueAfter > 0 during grace period")
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseRunning {
		t.Errorf("Expected task to remain Running within grace period, got %s", updatedRun.Status.Tasks[0].Phase)
	}

	// Turn 3: Reconcile at t = 35s (past grace period). Task must now fail.
	currentTime = startTime.Add(35 * time.Second)
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 3 error: %v", err)
	}

	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected genuinely deleted sandbox to fail task after grace period, got %s", updatedRun.Status.Tasks[0].Phase)
	}
	if !strings.Contains(updatedRun.Status.Tasks[0].Message, "task sandbox missing") {
		t.Errorf("Expected failure message to mention sandbox missing, got: %s", updatedRun.Status.Tasks[0].Message)
	}
}

func TestReconcileRunningTaskGracePeriodFromSecretCreationTimestamp(t *testing.T) {
	scheme := setupScheme(t)

	startTime := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	currentTime := startTime

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-secret-time", Spec: "eval.py@test"},
		},
	}

	// Task has StartedAt == nil, but token Secret exists with CreationTimestamp
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-secret-time",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-secret-time"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-secret-time", Phase: v1alpha1.TaskPhaseRunning}, // StartedAt is nil
			},
		},
	}

	secretName := discovery.TaskAuthSecretNameForRun(run.Name, "task-secret-time")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:              secretName,
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(startTime),
		},
		Data: map[string][]byte{"token": []byte("dummy-token")},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, secret).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		Now:              func() time.Time { return currentTime },
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-secret-time"}}

	// At t = 5s (within grace period of 30s): stays Running
	currentTime = startTime.Add(5 * time.Second)
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error at 5s: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected RequeueAfter > 0 during grace period")
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseRunning {
		t.Errorf("Expected task to remain Running within grace period, got %s", updatedRun.Status.Tasks[0].Phase)
	}

	// At t = 35s (past grace period): fails
	currentTime = startTime.Add(35 * time.Second)
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error at 35s: %v", err)
	}

	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task to fail after grace period, got %s", updatedRun.Status.Tasks[0].Phase)
	}
}

func TestReconcileArtifactSink_GCSArtifactsAndLogPathRecorded(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "docs/readme_comprehension", Spec: "eval.py@task"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-gcs",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abcdef123456",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"docs/readme_comprehension"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "docs/readme_comprehension", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	inspectLog := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "docs/readme_comprehension", "model": "mockllm/model"},
		"samples": [
			{"id": 1, "scores": {"accuracy": {"value": 1.0}}},
			{"id": 2, "scores": {"accuracy": {"value": 0.0}}}
		]
	}`

	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 0 && cfg.Command[0] == "ls" {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "2026-10-17_readme_eval.json\n"}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			return []byte(inspectLog), nil
		},
	}

	fakeGCSClient := fakes.NewFakeStorageClient()
	gcsSink, err := gcs.New(t.Context(), "gs://justinbs-knotai-dev-eval-runner", gcs.WithStorageClient(fakeGCSClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		SandboxClient:    executor,
		ArtifactSink:     gcsSink,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-gcs"}}

	// Turn 1: Creates sandbox
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 1 error: %v", err)
	}

	// Create ready pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.TaskSandboxNameForRun("test-run-gcs", "docs/readme_comprehension"),
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.50",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if err := cl.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Turn 2: Executes task and writes artifacts to GCS
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 2 error: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected Requeue to check completion")
	}

	// Turn 3: Run marked Completed
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 3 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get updated run: %v", err)
	}

	if updatedRun.Status.Phase != v1alpha1.RunPhaseCompleted {
		t.Errorf("Expected Run phase Completed, got %s", updatedRun.Status.Phase)
	}

	expectedArtifactPrefix := "gs://justinbs-knotai-dev-eval-runner/runs/abcdef123456/"
	if updatedRun.Status.Artifacts != expectedArtifactPrefix {
		t.Errorf("Expected status.artifacts %q, got %q", expectedArtifactPrefix, updatedRun.Status.Artifacts)
	}

	if len(updatedRun.Status.Tasks) != 1 {
		t.Fatalf("Expected 1 task status, got %d", len(updatedRun.Status.Tasks))
	}
	t1 := updatedRun.Status.Tasks[0]
	if t1.Phase != v1alpha1.TaskPhaseCompleted {
		t.Errorf("Expected task phase Completed, got %s", t1.Phase)
	}
	expectedLogPath := "gs://justinbs-knotai-dev-eval-runner/runs/abcdef123456/docs/readme_comprehension/2026-10-17_readme_eval.json"
	if t1.LogPath != expectedLogPath {
		t.Errorf("Expected task logPath %q, got %q", expectedLogPath, t1.LogPath)
	}

	// Verify log object exists in fake GCS
	logObj, exists := fakeGCSClient.GetObject("justinbs-knotai-dev-eval-runner", "runs/abcdef123456/docs/readme_comprehension/2026-10-17_readme_eval.json")
	if !exists {
		t.Fatalf("Log object was not uploaded to GCS")
	}
	if string(logObj.Data) != inspectLog {
		t.Errorf("Log content mismatch")
	}

	// Verify summary.json exists in fake GCS
	summaryObj, exists := fakeGCSClient.GetObject("justinbs-knotai-dev-eval-runner", "runs/abcdef123456/docs/readme_comprehension/summary.json")
	if !exists {
		t.Fatalf("summary.json was not uploaded to GCS")
	}

	var summary artifacts.TaskSummary
	if err := json.Unmarshal(summaryObj.Data, &summary); err != nil {
		t.Fatalf("Failed to parse summary.json: %v", err)
	}
	if summary.SchemaVersion != 1 {
		t.Errorf("Expected SchemaVersion 1, got %d", summary.SchemaVersion)
	}
	if summary.TaskID != "docs/readme_comprehension" {
		t.Errorf("Expected TaskID docs/readme_comprehension, got %s", summary.TaskID)
	}
	if summary.RunName != "test-run-gcs" {
		t.Errorf("Expected RunName test-run-gcs, got %s", summary.RunName)
	}
	if summary.RunNamespace != "eval-runner" {
		t.Errorf("Expected RunNamespace eval-runner, got %s", summary.RunNamespace)
	}
	if summary.Scorer != "accuracy" {
		t.Errorf("Expected Scorer accuracy, got %s", summary.Scorer)
	}
	if len(summary.Samples) != 2 {
		t.Errorf("Expected 2 samples, got %d", len(summary.Samples))
	}
	if summary.Successes != 1 {
		t.Errorf("Expected 1 success, got %d", summary.Successes)
	}
	if summary.MeanScore == nil || *summary.MeanScore != 0.5 {
		t.Errorf("Expected meanScore 0.5, got %v", summary.MeanScore)
	}
	if summary.SuccessRate == nil || *summary.SuccessRate != 0.5 {
		t.Errorf("Expected successRate 0.5, got %v", summary.SuccessRate)
	}
	if summary.SuccessRateCI == nil || summary.SuccessRateCI.Method != "wilson" {
		t.Errorf("Expected SuccessRateCI.Method wilson for binary scores, got %+v", summary.SuccessRateCI)
	}
	if summary.StartedAt == nil || summary.FinishedAt == nil {
		t.Errorf("Expected StartedAt and FinishedAt to be populated")
	}
}

func TestReconcileArtifactSink_StoresSummaryJSON_ContinuousScorerMethodT(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "continuous/partial_credit", Spec: "eval.py@task"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-partial",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "fedcba654321",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"continuous/partial_credit"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "continuous/partial_credit", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	inspectLog := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "continuous/partial_credit", "model": "mockllm/model"},
		"samples": [
			{"id": 1, "scores": {"f1": {"value": 0.5}}},
			{"id": 2, "scores": {"f1": {"value": 0.75}}}
		]
	}`

	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 0 && cfg.Command[0] == "ls" {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "partial_eval.json\n"}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			return []byte(inspectLog), nil
		},
	}

	fakeGCSClient := fakes.NewFakeStorageClient()
	gcsSink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCSClient))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:            cl,
		Scheme:            scheme,
		SandboxClient:     executor,
		SandboxNamespace:  "eval-runner-sandbox",
		ArtifactSink:      gcsSink,
		SampleRunnerImage: "runner:test",
		ControllerVersion: "v1.5.0",
		Now: func() time.Time {
			return time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
		},
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: "eval-runner",
			Name:      "test-run-partial",
		},
	}

	// Turn 1: Creates sandbox
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 1 error: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.TaskSandboxNameForRun("test-run-partial", "continuous/partial_credit"),
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.51",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if err := cl.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Turn 2: Executes task and writes artifacts to GCS
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 2 error: %v", err)
	}

	summaryObj, exists := fakeGCSClient.GetObject("test-bucket", "runs/fedcba654321/continuous/partial_credit/summary.json")
	if !exists {
		t.Fatalf("summary.json was not uploaded to GCS")
	}

	var summary artifacts.TaskSummary
	if err := json.Unmarshal(summaryObj.Data, &summary); err != nil {
		t.Fatalf("Failed to parse summary.json: %v", err)
	}

	if summary.SchemaVersion != 1 {
		t.Errorf("Expected SchemaVersion=1, got %d", summary.SchemaVersion)
	}
	if summary.ControllerVersion != "v1.5.0" {
		t.Errorf("Expected ControllerVersion=v1.5.0, got %s", summary.ControllerVersion)
	}
	if summary.Scorer != "f1" {
		t.Errorf("Expected Scorer=f1, got %s", summary.Scorer)
	}
	if summary.SuccessRateCI == nil || summary.SuccessRateCI.Method != "t" {
		t.Errorf("Expected SuccessRateCI.Method=t for partial credit, got %+v", summary.SuccessRateCI)
	}
	if len(summary.Samples) != 2 {
		t.Fatalf("Expected 2 samples, got %d", len(summary.Samples))
	}
}

func TestReconcileArtifactSink_TokenAccountingInvariantWithFixture(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "docs/readme_comprehension", Spec: "eval.py@task"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-token-invariant",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abcdef123456",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"docs/readme_comprehension"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "docs/readme_comprehension", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	fixturePath := filepath.Join("..", "artifacts", "testdata", "model_usage_cache_reasoning.json")
	fixtureData, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("Failed to read fixture: %v", err)
	}

	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 0 && cfg.Command[0] == "ls" {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "2026-10-18_readme_eval.json\n"}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			return fixtureData, nil
		},
	}

	fakeGCSClient := fakes.NewFakeStorageClient()
	gcsSink, err := gcs.New(t.Context(), "gs://justinbs-knotai-dev-eval-runner", gcs.WithStorageClient(fakeGCSClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		ArtifactSink:     gcsSink,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-token-invariant"}}

	// Turn 1: Creates pod
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 1 error: %v", err)
	}

	// Create ready pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.TaskSandboxNameForRun("test-run-token-invariant", "docs/readme_comprehension"),
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.50",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if err := cl.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Turn 2: Executes task and writes artifacts to GCS
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 2 error: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected Requeue to check completion")
	}

	// Turn 3: Run marked Completed
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 3 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get updated run: %v", err)
	}

	if updatedRun.Status.Phase != v1alpha1.RunPhaseCompleted {
		t.Fatalf("Expected Run phase Completed, got %s", updatedRun.Status.Phase)
	}
	if len(updatedRun.Status.Tasks) != 1 {
		t.Fatalf("Expected 1 task status, got %d", len(updatedRun.Status.Tasks))
	}

	taskTokens := updatedRun.Status.Tasks[0].Tokens
	if taskTokens == nil {
		t.Fatalf("Expected non-nil task Tokens in Run status")
	}
	if taskTokens.Input != 140708 || taskTokens.Output != 788 {
		t.Errorf("Expected {Input: 140708, Output: 788}, got %+v", taskTokens)
	}
	if taskTokens.Total != taskTokens.Input+taskTokens.Output {
		t.Errorf("Run status token accounting invariant violated: Total (%d) != Input (%d) + Output (%d)",
			taskTokens.Total, taskTokens.Input, taskTokens.Output)
	}
	if taskTokens.Total != 141496 {
		t.Errorf("Expected Run status total=141496 (Inspect total 272146 dropped), got %d", taskTokens.Total)
	}
	if taskTokens.Reasoning != 150 {
		t.Errorf("Expected Run status Reasoning=150, got %d", taskTokens.Reasoning)
	}
	if taskTokens.CacheRead != 130000 {
		t.Errorf("Expected Run status CacheRead=130000, got %d", taskTokens.CacheRead)
	}
	if taskTokens.CacheWrite != 650 {
		t.Errorf("Expected Run status CacheWrite=650, got %d", taskTokens.CacheWrite)
	}

	// Verify summary.json exists in fake GCS and enforces the invariant
	summaryObj, exists := fakeGCSClient.GetObject("justinbs-knotai-dev-eval-runner", "runs/abcdef123456/docs/readme_comprehension/summary.json")
	if !exists {
		t.Fatalf("summary.json was not uploaded to GCS")
	}
	var summary artifacts.TaskSummary
	if err := json.Unmarshal(summaryObj.Data, &summary); err != nil {
		t.Fatalf("Failed to parse summary.json: %v", err)
	}
	if summary.Tokens == nil {
		t.Fatalf("Expected non-nil Tokens in summary.json")
	}
	if summary.Tokens.Input != 140708 || summary.Tokens.Output != 788 {
		t.Errorf("Expected summary.Tokens {Input: 140708, Output: 788}, got %+v", summary.Tokens)
	}
	if summary.Tokens.Total != summary.Tokens.Input+summary.Tokens.Output {
		t.Errorf("summary.json token accounting invariant violated: Total (%d) != Input (%d) + Output (%d)",
			summary.Tokens.Total, summary.Tokens.Input, summary.Tokens.Output)
	}
	if summary.Tokens.Total != 141496 {
		t.Errorf("Expected summary.json total=141496 (Inspect total 272146 dropped), got %d", summary.Tokens.Total)
	}
	if summary.Tokens.Reasoning != 150 {
		t.Errorf("Expected summary.Tokens Reasoning=150, got %d", summary.Tokens.Reasoning)
	}
	if summary.Tokens.CacheRead != 130000 {
		t.Errorf("Expected summary.Tokens CacheRead=130000, got %d", summary.Tokens.CacheRead)
	}
	if summary.Tokens.CacheWrite != 650 {
		t.Errorf("Expected summary.Tokens CacheWrite=650, got %d", summary.Tokens.CacheWrite)
	}
}

func TestReconcileArtifactSink_TokenAccountingFallbackPerSample(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "docs/readme_comprehension", Spec: "eval.py@task"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-token-fallback",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abcdef123456",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"docs/readme_comprehension"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "docs/readme_comprehension", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	sampleLogJSON := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "docs/readme_comprehension", "model": "mockllm/model"},
		"samples": [
			{
				"id": 1,
				"model_usage": {
					"mockllm/model": {
						"input_tokens": 100,
						"output_tokens": 20,
						"total_tokens": 200,
						"reasoning_tokens": 15,
						"input_tokens_cache_read": 80,
						"input_tokens_cache_write": 10
					}
				},
				"scores": {"choice": {"value": "C"}}
			},
			{
				"id": 2,
				"model_usage": {
					"mockllm/model": {
						"input_tokens": 50,
						"output_tokens": 10,
						"total_tokens": 100,
						"reasoning_tokens": 5,
						"input_tokens_cache_read": 40,
						"input_tokens_cache_write": 5
					}
				},
				"scores": {"choice": {"value": "C"}}
			}
		]
	}`

	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 0 && cfg.Command[0] == "ls" {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "2026-10-18_readme_eval.json\n"}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			return []byte(sampleLogJSON), nil
		},
	}

	fakeGCSClient := fakes.NewFakeStorageClient()
	gcsSink, err := gcs.New(t.Context(), "gs://justinbs-knotai-dev-eval-runner", gcs.WithStorageClient(fakeGCSClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		ArtifactSink:     gcsSink,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-token-fallback"}}

	// Turn 1: Creates pod
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 1 error: %v", err)
	}

	// Create ready pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.TaskSandboxNameForRun("test-run-token-fallback", "docs/readme_comprehension"),
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.50",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if err := cl.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Turn 2: Executes task and writes artifacts to GCS
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 2 error: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected Requeue to check completion")
	}

	// Turn 3: Run marked Completed
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 3 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get updated run: %v", err)
	}

	taskTokens := updatedRun.Status.Tasks[0].Tokens
	if taskTokens == nil {
		t.Fatalf("Expected non-nil task Tokens in Run status")
	}
	if taskTokens.Input != 150 || taskTokens.Output != 30 || taskTokens.Total != 180 {
		t.Errorf("Expected {Input: 150, Output: 30, Total: 180}, got %+v", taskTokens)
	}
	if taskTokens.Reasoning != 20 || taskTokens.CacheRead != 120 || taskTokens.CacheWrite != 15 {
		t.Errorf("Expected {Reasoning: 20, CacheRead: 120, CacheWrite: 15}, got %+v", taskTokens)
	}

	summaryObj, exists := fakeGCSClient.GetObject("justinbs-knotai-dev-eval-runner", "runs/abcdef123456/docs/readme_comprehension/summary.json")
	if !exists {
		t.Fatalf("summary.json was not uploaded to GCS")
	}
	var summary artifacts.TaskSummary
	if err := json.Unmarshal(summaryObj.Data, &summary); err != nil {
		t.Fatalf("Failed to parse summary.json: %v", err)
	}
	if summary.Tokens == nil {
		t.Fatalf("Expected non-nil Tokens in summary.json")
	}
	if summary.Tokens.Input != 150 || summary.Tokens.Output != 30 || summary.Tokens.Total != 180 {
		t.Errorf("Expected summary.Tokens {150, 30, 180}, got %+v", summary.Tokens)
	}
	if summary.Tokens.Reasoning != 20 || summary.Tokens.CacheRead != 120 || summary.Tokens.CacheWrite != 15 {
		t.Errorf("Expected summary.Tokens {Reasoning: 20, CacheRead: 120, CacheWrite: 15}, got %+v", summary.Tokens)
	}
}

func TestReconcileArtifactSink_CostAndLatencyAccountingWithFixture(t *testing.T) {
	scheme := setupScheme(t)

	fixtureData, err := os.ReadFile("../artifacts/testdata/cost_latency_fixture.json")
	if err != nil {
		t.Fatalf("Failed to read fixture: %v", err)
	}

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "docs/readme_comprehension", Spec: "docs.py@readme"},
		},
	}

	t.Run("CostAndLatencyComputedWhenPriceKnown", func(t *testing.T) {
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-cost-latency",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    "git://git-server/repo.git",
					Commit: "abcdef123456",
				},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
			},
		}

		executor := &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				if len(cfg.Command) > 0 && cfg.Command[0] == "ls" {
					return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "log.json\n"}, nil
				}
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				return fixtureData, nil
			},
		}

		fakeGCSClient := fakes.NewFakeStorageClient()
		gcsSink, err := gcs.New(t.Context(), "gs://justinbs-knotai-dev-eval-runner", gcs.WithStorageClient(fakeGCSClient))
		if err != nil {
			t.Fatalf("Failed to create GCSSink: %v", err)
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(run).
			WithStatusSubresource(run).
			Build()

		reconciler := &RunReconciler{
			Client:           cl,
			Scheme:           scheme,
			SandboxClient:    executor,
			ArtifactSink:     gcsSink,
			SandboxNamespace: "eval-runner-sandbox",
			ModelPrices: map[string]ModelPrice{
				"mockllm/model": {InputPer1M: 1.0, OutputPer1M: 2.0},
			},
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-cost-latency"}}

		// Turn 1: Creates pod
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile turn 1 error: %v", err)
		}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      discovery.TaskSandboxNameForRun("test-run-cost-latency", "docs/readme_comprehension"),
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.50",
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		if err := cl.Create(t.Context(), pod); err != nil {
			t.Fatalf("Failed to create pod: %v", err)
		}

		// Turn 2: Executes task and writes artifacts
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile turn 2 error: %v", err)
		}
		if !res.Requeue {
			t.Errorf("Expected Requeue to check completion")
		}

		// Turn 3: Run completed
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile turn 3 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get updated run: %v", err)
		}

		task := updatedRun.Status.Tasks[0]
		if task.CostUSD == nil {
			t.Fatalf("Expected non-nil task.CostUSD")
		}
		// Input=100k, Output=20k -> (100000*1.0 + 20000*2.0)/1e6 = 0.14
		expectedCost := 0.14
		if math.Abs(*task.CostUSD-expectedCost) > 1e-9 {
			t.Errorf("Expected task.CostUSD=%f, got %f", expectedCost, *task.CostUSD)
		}
		if updatedRun.Status.CostUSD == nil {
			t.Fatalf("Expected non-nil Run.Status.CostUSD")
		}
		if math.Abs(*updatedRun.Status.CostUSD-expectedCost) > 1e-9 {
			t.Errorf("Expected Run.Status.CostUSD=%f, got %f", expectedCost, *updatedRun.Status.CostUSD)
		}

		if task.Latency == nil {
			t.Fatalf("Expected non-nil task.Latency")
		}
		if math.Abs(task.Latency.P50-2.0) > 1e-9 {
			t.Errorf("Expected P50=2.0, got %f", task.Latency.P50)
		}
		if math.Abs(task.Latency.P95-2.9) > 1e-9 {
			t.Errorf("Expected P95=2.9, got %f", task.Latency.P95)
		}

		if task.PossibleSamples != 21 {
			t.Errorf("Expected PossibleSamples=21, got %d", task.PossibleSamples)
		}

		// Verify summary.json
		summaryObj, exists := fakeGCSClient.GetObject("justinbs-knotai-dev-eval-runner", "runs/abcdef123456/docs/readme_comprehension/summary.json")
		if !exists {
			t.Fatalf("summary.json was not uploaded to GCS")
		}
		var summary artifacts.TaskSummary
		if err := json.Unmarshal(summaryObj.Data, &summary); err != nil {
			t.Fatalf("Failed to parse summary.json: %v", err)
		}
		if summary.CostUSD == nil || math.Abs(*summary.CostUSD-expectedCost) > 1e-9 {
			t.Errorf("Expected summary.CostUSD=%f, got %v", expectedCost, summary.CostUSD)
		}
		if summary.Latency == nil || math.Abs(summary.Latency.P50-2.0) > 1e-9 || math.Abs(summary.Latency.P95-2.9) > 1e-9 {
			t.Errorf("Expected summary.Latency P50=2.0, P95=2.9, got %+v", summary.Latency)
		}
		if summary.PossibleSamples != 21 {
			t.Errorf("Expected summary.PossibleSamples=21, got %d", summary.PossibleSamples)
		}
	})

	t.Run("CostOmittedWhenNoPriceKnown", func(t *testing.T) {
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-cost-omitted",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    "git://git-server/repo.git",
					Commit: "abcdef123456",
				},
				Model: "mockllm/no-price",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
			},
		}

		executor := &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				if len(cfg.Command) > 0 && cfg.Command[0] == "ls" {
					return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "log.json\n"}, nil
				}
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				return fixtureData, nil
			},
		}

		fakeGCSClient := fakes.NewFakeStorageClient()
		gcsSink, err := gcs.New(t.Context(), "gs://justinbs-knotai-dev-eval-runner", gcs.WithStorageClient(fakeGCSClient))
		if err != nil {
			t.Fatalf("Failed to create GCSSink: %v", err)
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(run).
			WithStatusSubresource(run).
			Build()

		reconciler := &RunReconciler{
			Client:           cl,
			Scheme:           scheme,
			SandboxClient:    executor,
			ArtifactSink:     gcsSink,
			SandboxNamespace: "eval-runner-sandbox",
			ModelPrices: map[string]ModelPrice{
				"other/model": {InputPer1M: 1.0, OutputPer1M: 2.0},
			},
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-cost-omitted"}}

		// Turn 1
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile turn 1 error: %v", err)
		}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      discovery.TaskSandboxNameForRun("test-run-cost-omitted", "docs/readme_comprehension"),
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.50",
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		if err := cl.Create(t.Context(), pod); err != nil {
			t.Fatalf("Failed to create pod: %v", err)
		}

		// Turn 2
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile turn 2 error: %v", err)
		}

		// Turn 3
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile turn 3 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get updated run: %v", err)
		}

		task := updatedRun.Status.Tasks[0]
		if task.CostUSD != nil {
			t.Errorf("Expected nil task.CostUSD when no price known, got %v", task.CostUSD)
		}
		if updatedRun.Status.CostUSD != nil {
			t.Errorf("Expected nil Run.Status.CostUSD when no price known, got %v", updatedRun.Status.CostUSD)
		}

		summaryObj, exists := fakeGCSClient.GetObject("justinbs-knotai-dev-eval-runner", "runs/abcdef123456/docs/readme_comprehension/summary.json")
		if !exists {
			t.Fatalf("summary.json was not uploaded to GCS")
		}
		var summary artifacts.TaskSummary
		if err := json.Unmarshal(summaryObj.Data, &summary); err != nil {
			t.Fatalf("Failed to parse summary.json: %v", err)
		}
		if summary.CostUSD != nil {
			t.Errorf("Expected nil summary.CostUSD, got %v", summary.CostUSD)
		}
	})
}

func TestReconcileArtifactSink_UploadCapRecordedInSummary(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "docs/readme_comprehension", Spec: "docs.py@readme"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-gcs-capped",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abcdef123456",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
		},
	}

	inspectLog := `{
		"version": 2,
		"eval": {
			"task": "docs/readme_comprehension",
			"model": "mockllm/model",
			"created": "2026-10-17T12:00:00Z"
		},
		"results": {
			"scores": [
				{"name": "accuracy", "metrics": {"accuracy": {"value": 0.5}}}
			],
			"total_samples": 2,
			"completed_samples": 2
		},
		"samples": [
			{"id": "1", "scores": {"accuracy": {"value": 1.0}}},
			{"id": "2", "scores": {"accuracy": {"value": 0.0}}}
		]
	}`

	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 0 && cfg.Command[0] == "ls" {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "2026-10-17_readme_eval.json\n"}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			return []byte(inspectLog), nil
		},
	}

	fakeGCSClient := fakes.NewFakeStorageClient()
	capLimit := int64(100)
	gcsSink, err := gcs.New(t.Context(), "gs://justinbs-knotai-dev-eval-runner",
		gcs.WithStorageClient(fakeGCSClient),
		gcs.WithMaxUploadBytes(capLimit),
	)
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		SandboxClient:    executor,
		ArtifactSink:     gcsSink,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-gcs-capped"}}

	// Turn 1: Creates sandbox
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 1 error: %v", err)
	}

	// Create ready pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.TaskSandboxNameForRun("test-run-gcs-capped", "docs/readme_comprehension"),
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.50",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if err := cl.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Turn 2: Executes task and writes artifacts to GCS
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 2 error: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected Requeue to check completion")
	}

	// Turn 3: Run marked Completed
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 3 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get updated run: %v", err)
	}

	if updatedRun.Status.Phase != v1alpha1.RunPhaseCompleted {
		t.Errorf("Expected Run phase Completed, got %s", updatedRun.Status.Phase)
	}

	// Verify log object exists and is truncated to capLimit
	logObj, exists := fakeGCSClient.GetObject("justinbs-knotai-dev-eval-runner", "runs/abcdef123456/docs/readme_comprehension/2026-10-17_readme_eval.json")
	if !exists {
		t.Fatalf("Log object was not uploaded to GCS")
	}
	if int64(len(logObj.Data)) != capLimit {
		t.Errorf("Expected uploaded log data capped to %d bytes, got %d", capLimit, len(logObj.Data))
	}
	if string(logObj.Data) != inspectLog[:capLimit] {
		t.Errorf("Expected uploaded log to match prefix of inspectLog")
	}

	// Verify summary.json has Capped=true and CapBytes=capLimit
	summaryObj, exists := fakeGCSClient.GetObject("justinbs-knotai-dev-eval-runner", "runs/abcdef123456/docs/readme_comprehension/summary.json")
	if !exists {
		t.Fatalf("summary.json was not uploaded to GCS")
	}

	var summary artifacts.TaskSummary
	if err := json.Unmarshal(summaryObj.Data, &summary); err != nil {
		t.Fatalf("Failed to parse summary.json: %v", err)
	}
	if !summary.Capped {
		t.Errorf("Expected summary.Capped to be true when upload exceeds capLimit")
	}
	if summary.CapBytes != capLimit {
		t.Errorf("Expected summary.CapBytes=%d, got %d", capLimit, summary.CapBytes)
	}
}

func TestReconcileArtifactSink_StorageErrorPropagatesToTaskStatus(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-1", Spec: "eval.py@task"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-gcs-err",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abcdef123456",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-1"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-1", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	inspectLog := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "task-1"},
		"samples": [{"id": 1, "scores": {"accuracy": {"value": 1.0}}}]
	}`

	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 0 && cfg.Command[0] == "ls" {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "log.json\n"}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			return []byte(inspectLog), nil
		},
	}

	fakeGCSClient := fakes.NewFakeStorageClient()
	// Inject storage write error
	fakeGCSClient.Errors["my-bucket/runs/abcdef123456/task-1/log.json"] = errors.New("storage: 403 Forbidden: permission denied")

	gcsSink, err := gcs.New(t.Context(), "gs://my-bucket", gcs.WithStorageClient(fakeGCSClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		SandboxClient:    executor,
		ArtifactSink:     gcsSink,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-gcs-err"}}

	// Turn 1: Creates sandbox
	_, _ = reconciler.Reconcile(t.Context(), req)

	// Create ready pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.TaskSandboxNameForRun("test-run-gcs-err", "task-1"),
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.51",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	_ = cl.Create(t.Context(), pod)

	// Turn 2: Executes task, storage fails
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 2 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)

	if len(updatedRun.Status.Tasks) != 1 {
		t.Fatalf("Expected 1 task status, got %d", len(updatedRun.Status.Tasks))
	}
	t1 := updatedRun.Status.Tasks[0]
	if t1.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task phase Failed, got %s", t1.Phase)
	}
	if !strings.Contains(t1.Message, "failed to store artifact") || !strings.Contains(t1.Message, "permission denied") {
		t.Errorf("Expected storage error in task message, got: %s", t1.Message)
	}
	if t1.LogPath != "" {
		t.Errorf("Expected empty logPath on storage failure, got: %s", t1.LogPath)
	}
	if t1.Samples != 1 {
		t.Errorf("Expected Samples=1 to survive storage failure, got %d", t1.Samples)
	}
	if t1.Successes != 1 {
		t.Errorf("Expected Successes=1 to survive storage failure, got %d", t1.Successes)
	}
	if t1.MeanScore == nil || *t1.MeanScore != 1.0 {
		t.Errorf("Expected MeanScore=1.0 to survive storage failure, got %v", t1.MeanScore)
	}
}

func TestReconcileSampleExecution_BinaryScorer(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-binary", Spec: "eval.py@task1", Workdir: "evals"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-binary"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-binary", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	inspectLog := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "task-binary", "scorers": [{"name": "choice"}]},
		"samples": [
			{"id": 1, "scores": {"choice": {"value": 1.0}}},
			{"id": 2, "scores": {"choice": {"value": 1.0}}},
			{"id": 3, "scores": {"choice": {"value": 1.0}}},
			{"id": 4, "scores": {"choice": {"value": 0.0}}}
		]
	}`

	executor := &sandboxclient.FakeClient{
		InspectLog: []byte(inspectLog),
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}

	// Turn 1: create sandbox
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	taskSB := sbList.Items[0]

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskSB.Name,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.12",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	_ = cl.Create(t.Context(), pod)

	// Turn 2: execute task
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)

	t1 := updatedRun.Status.Tasks[0]
	if t1.Phase != v1alpha1.TaskPhaseCompleted {
		t.Fatalf("Expected Completed, got %s", t1.Phase)
	}
	if t1.Samples != 4 {
		t.Errorf("Expected 4 samples, got %d", t1.Samples)
	}
	if t1.Successes != 3 {
		t.Errorf("Expected 3 successes, got %d", t1.Successes)
	}
	if t1.SuccessRate == nil || *t1.SuccessRate != 0.75 {
		t.Errorf("Expected successRate 0.75, got %v", t1.SuccessRate)
	}
	if t1.SuccessRateCI == nil {
		t.Fatalf("Expected non-nil successRateCI")
	}
	if t1.SuccessRateCI.Method != "wilson" {
		t.Errorf("Expected method wilson, got %s", t1.SuccessRateCI.Method)
	}
	if t1.SuccessRateCI.Confidence != 0.95 {
		t.Errorf("Expected confidence 0.95, got %f", t1.SuccessRateCI.Confidence)
	}
	expectedEst := stats.Proportion(3, 4, 0.95)
	if math.Abs(t1.SuccessRateCI.Lower-expectedEst.CI.Lower) > 1e-6 {
		t.Errorf("Expected CI lower %f, got %f", expectedEst.CI.Lower, t1.SuccessRateCI.Lower)
	}
	if math.Abs(t1.SuccessRateCI.Upper-expectedEst.CI.Upper) > 1e-6 {
		t.Errorf("Expected CI upper %f, got %f", expectedEst.CI.Upper, t1.SuccessRateCI.Upper)
	}

	// Verify status.summary
	if updatedRun.Status.Summary == nil {
		t.Fatalf("Expected non-nil status.Summary")
	}
	if updatedRun.Status.Summary.Tasks != "1/1" {
		t.Errorf("Expected Summary.Tasks 1/1, got %s", updatedRun.Status.Summary.Tasks)
	}
	if updatedRun.Status.Summary.TasksCompleted != 1 {
		t.Errorf("Expected TasksCompleted 1, got %d", updatedRun.Status.Summary.TasksCompleted)
	}
	if updatedRun.Status.Summary.TasksFailed != 0 {
		t.Errorf("Expected TasksFailed 0, got %d", updatedRun.Status.Summary.TasksFailed)
	}
	if updatedRun.Status.Summary.TotalSamples != 4 {
		t.Errorf("Expected TotalSamples 4, got %d", updatedRun.Status.Summary.TotalSamples)
	}
	if updatedRun.Status.Summary.MeanSuccessRate == nil || math.Abs(*updatedRun.Status.Summary.MeanSuccessRate-0.75) > 1e-6 {
		t.Errorf("Expected MeanSuccessRate 0.75, got %v", updatedRun.Status.Summary.MeanSuccessRate)
	}
	if updatedRun.Status.Summary.SuccessRateRange == nil || updatedRun.Status.Summary.SuccessRateRange.Min != 0.75 || updatedRun.Status.Summary.SuccessRateRange.Max != 0.75 {
		t.Errorf("Expected SuccessRateRange [0.75, 0.75], got %+v", updatedRun.Status.Summary.SuccessRateRange)
	}
}

func TestReconcileSampleExecution_PartialCreditScorer(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-partial", Spec: "eval.py@task_partial", Workdir: "evals"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-partial"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-partial", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	inspectLog := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "task-partial", "scorers": [{"name": "factuality"}]},
		"samples": [
			{"id": 1, "scores": {"factuality": {"value": 0.2}}},
			{"id": 2, "scores": {"factuality": {"value": 0.4}}},
			{"id": 3, "scores": {"factuality": {"value": 0.6}}},
			{"id": 4, "scores": {"factuality": {"value": 0.8}}}
		]
	}`

	executor := &sandboxclient.FakeClient{
		InspectLog: []byte(inspectLog),
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}

	// Turn 1: create sandbox
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	taskSB := sbList.Items[0]

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskSB.Name,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.12",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	_ = cl.Create(t.Context(), pod)

	// Turn 2: execute task
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)

	t1 := updatedRun.Status.Tasks[0]
	if t1.Phase != v1alpha1.TaskPhaseCompleted {
		t.Fatalf("Expected Completed, got %s", t1.Phase)
	}
	if t1.Samples != 4 {
		t.Errorf("Expected 4 samples, got %d", t1.Samples)
	}
	// Successes: only scores >= 1.0; here none >= 1.0
	if t1.Successes != 0 {
		t.Errorf("Expected 0 successes, got %d", t1.Successes)
	}
	if t1.SuccessRate == nil || math.Abs(*t1.SuccessRate-0.5) > 1e-6 {
		t.Errorf("Expected successRate 0.5, got %v", t1.SuccessRate)
	}
	if t1.SuccessRateCI == nil {
		t.Fatalf("Expected non-nil successRateCI")
	}
	if t1.SuccessRateCI.Method != "t" {
		t.Errorf("Expected method 't', got %s", t1.SuccessRateCI.Method)
	}
	if t1.SuccessRateCI.Confidence != 0.95 {
		t.Errorf("Expected confidence 0.95, got %f", t1.SuccessRateCI.Confidence)
	}
	scores := []float64{0.2, 0.4, 0.6, 0.8}
	expectedEst := stats.Mean(scores, 0.95)
	if math.Abs(t1.SuccessRateCI.Lower-expectedEst.CI.Lower) > 1e-6 {
		t.Errorf("Expected CI lower %f, got %f", expectedEst.CI.Lower, t1.SuccessRateCI.Lower)
	}
	if math.Abs(t1.SuccessRateCI.Upper-expectedEst.CI.Upper) > 1e-6 {
		t.Errorf("Expected CI upper %f, got %f", expectedEst.CI.Upper, t1.SuccessRateCI.Upper)
	}
}

func TestReconcileSampleExecution_Wilson20_20(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-perfect", Spec: "eval.py@task_perfect", Workdir: "evals"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-perfect"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-perfect", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	var samplesJSON []string
	for i := 1; i <= 20; i++ {
		samplesJSON = append(samplesJSON, fmt.Sprintf(`{"id": %d, "scores": {"choice": {"value": "C"}}}`, i))
	}
	inspectLog := fmt.Sprintf(`{
		"version": 2,
		"status": "success",
		"eval": {"task": "task-perfect", "scorers": [{"name": "choice"}]},
		"samples": [%s]
	}`, strings.Join(samplesJSON, ","))

	executor := &sandboxclient.FakeClient{
		InspectLog: []byte(inspectLog),
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}

	// Turn 1: create sandbox
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	taskSB := sbList.Items[0]

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskSB.Name,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.12",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	_ = cl.Create(t.Context(), pod)

	// Turn 2: execute task
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)

	t1 := updatedRun.Status.Tasks[0]
	if t1.Samples != 20 || t1.Successes != 20 {
		t.Fatalf("Expected 20/20, got %d/%d", t1.Successes, t1.Samples)
	}
	if t1.SuccessRate == nil || *t1.SuccessRate != 1.0 {
		t.Errorf("Expected successRate 1.0, got %v", t1.SuccessRate)
	}
	if t1.SuccessRateCI == nil {
		t.Fatalf("Expected non-nil successRateCI")
	}
	if t1.SuccessRateCI.Method != "wilson" {
		t.Errorf("Expected method wilson, got %s", t1.SuccessRateCI.Method)
	}
	// Wilson lower bound for 20/20 must be bounded and < 1.0 (~0.8389)
	if t1.SuccessRateCI.Lower >= 1.0 || t1.SuccessRateCI.Lower < 0.80 {
		t.Errorf("Expected Wilson lower bound < 1.0 and >= 0.80, got %f", t1.SuccessRateCI.Lower)
	}
	// Wilson upper bound for 20/20 is exactly 1.0
	if t1.SuccessRateCI.Upper != 1.0 {
		t.Errorf("Expected Wilson upper bound 1.0, got %f", t1.SuccessRateCI.Upper)
	}
}

func TestReconcileSampleExecution_ZeroSamplesFailed(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-fail", Spec: "eval.py@task_fail", Workdir: "evals"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-fail"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-fail", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	executor := &sandboxclient.FakeClient{
		InspectExit: 1, // fails inspect eval step with exit code 1
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run"}}

	// Turn 1: create sandbox
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	taskSB := sbList.Items[0]

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskSB.Name,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.12",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	_ = cl.Create(t.Context(), pod)

	// Turn 2: execute task fails
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)

	t1 := updatedRun.Status.Tasks[0]
	if t1.Phase != v1alpha1.TaskPhaseFailed {
		t.Fatalf("Expected Failed phase, got %s", t1.Phase)
	}
	if t1.Samples != 0 {
		t.Errorf("Expected 0 samples, got %d", t1.Samples)
	}
	if t1.SuccessRate != nil {
		t.Errorf("Expected nil SuccessRate on zero samples/failed task, got %v", t1.SuccessRate)
	}
	if t1.SuccessRateCI != nil {
		t.Errorf("Expected nil SuccessRateCI on zero samples/failed task, got %v", t1.SuccessRateCI)
	}

	if updatedRun.Status.Summary == nil {
		t.Fatalf("Expected non-nil Summary")
	}
	if updatedRun.Status.Summary.Tasks != "0/1" {
		t.Errorf("Expected Tasks '0/1', got %s", updatedRun.Status.Summary.Tasks)
	}
	if updatedRun.Status.Summary.TasksCompleted != 0 {
		t.Errorf("Expected TasksCompleted 0, got %d", updatedRun.Status.Summary.TasksCompleted)
	}
	if updatedRun.Status.Summary.TasksFailed != 1 {
		t.Errorf("Expected TasksFailed 1, got %d", updatedRun.Status.Summary.TasksFailed)
	}
	if updatedRun.Status.Summary.TotalSamples != 0 {
		t.Errorf("Expected TotalSamples 0, got %d", updatedRun.Status.Summary.TotalSamples)
	}
	if updatedRun.Status.Summary.MeanSuccessRate != nil {
		t.Errorf("Expected nil MeanSuccessRate when no tasks completed, got %v", updatedRun.Status.Summary.MeanSuccessRate)
	}
	if updatedRun.Status.Summary.SuccessRateRange != nil {
		t.Errorf("Expected nil SuccessRateRange when no tasks completed, got %v", updatedRun.Status.Summary.SuccessRateRange)
	}
	if updatedRun.Status.Summary.Rates != "task-fail=failed" {
		t.Errorf("Expected Rates 'task-fail=failed', got %s", updatedRun.Status.Summary.Rates)
	}
}

func TestReconcileSampleExecution_ScorelessLogFailsWithNoScoredSamples(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-scoreless", Spec: "eval.py@task_scoreless", Workdir: "evals"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-scoreless",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-scoreless"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-scoreless", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	// Inspect log where all samples errored without scores
	inspectLog := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "task-scoreless", "scorers": [{"name": "choice"}]},
		"samples": [
			{"id": 1, "error": "model timeout"},
			{"id": 2, "error": "connection refused"}
		]
	}`

	executor := &sandboxclient.FakeClient{
		InspectLog: []byte(inspectLog),
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-scoreless"}}

	// Turn 1: create sandbox
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	taskSB := sbList.Items[0]

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskSB.Name,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.12",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	_ = cl.Create(t.Context(), pod)

	// Turn 2: execute task
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)

	t1 := updatedRun.Status.Tasks[0]
	if t1.Phase != v1alpha1.TaskPhaseFailed {
		t.Fatalf("Expected Failed phase, got %s", t1.Phase)
	}
	if t1.Message != "no scored samples" {
		t.Errorf("Expected message 'no scored samples', got %q", t1.Message)
	}
	if t1.Samples != 0 {
		t.Errorf("Expected 0 samples, got %d", t1.Samples)
	}
	if t1.SuccessRate != nil {
		t.Errorf("Expected nil SuccessRate on scoreless task, got %v", t1.SuccessRate)
	}
	if t1.SuccessRateCI != nil {
		t.Errorf("Expected nil SuccessRateCI on scoreless task, got %v", t1.SuccessRateCI)
	}

	if updatedRun.Status.Summary == nil {
		t.Fatalf("Expected non-nil Summary")
	}
	if updatedRun.Status.Summary.Tasks != "0/1" {
		t.Errorf("Expected Tasks '0/1', got %s", updatedRun.Status.Summary.Tasks)
	}
	if updatedRun.Status.Summary.TasksCompleted != 0 {
		t.Errorf("Expected TasksCompleted 0, got %d", updatedRun.Status.Summary.TasksCompleted)
	}
	if updatedRun.Status.Summary.TasksFailed != 1 {
		t.Errorf("Expected TasksFailed 1, got %d", updatedRun.Status.Summary.TasksFailed)
	}
	if updatedRun.Status.Summary.TotalSamples != 0 {
		t.Errorf("Expected TotalSamples 0, got %d", updatedRun.Status.Summary.TotalSamples)
	}
}

func TestPrinterColumnFormatting(t *testing.T) {
	// 1. Empty tasks
	if s := ComputeRunSummary(nil); s != nil {
		t.Errorf("Expected nil summary for empty tasks, got %v", s)
	}

	// 2. Single completed task with Wilson CI
	rate1 := 0.85
	tasks := []v1alpha1.TaskStatus{
		{
			ID:          "docs/readme_comprehension",
			Phase:       v1alpha1.TaskPhaseCompleted,
			Samples:     20,
			Successes:   17,
			SuccessRate: &rate1,
			SuccessRateCI: &v1alpha1.ConfidenceInterval{
				Lower:      0.64,
				Upper:      0.95,
				Confidence: 0.95,
				Method:     "wilson",
			},
		},
	}
	s1 := ComputeRunSummary(tasks)
	if s1.Tasks != "1/1" {
		t.Errorf("Expected Tasks '1/1', got %s", s1.Tasks)
	}
	if s1.TotalSamples != 20 {
		t.Errorf("Expected TotalSamples 20, got %d", s1.TotalSamples)
	}
	if s1.Rates != "docs/readme_comprehension=0.85 [0.64, 0.95]" {
		t.Errorf("Expected Rates 'docs/readme_comprehension=0.85 [0.64, 0.95]', got %s", s1.Rates)
	}
	if s1.MeanSuccessRate == nil || *s1.MeanSuccessRate != 0.85 {
		t.Errorf("Expected MeanSuccessRate 0.85, got %v", s1.MeanSuccessRate)
	}
	if s1.SuccessRateRange == nil || s1.SuccessRateRange.Min != 0.85 || s1.SuccessRateRange.Max != 0.85 {
		t.Errorf("Expected SuccessRateRange [0.85, 0.85], got %+v", s1.SuccessRateRange)
	}

	// 3. Two completed tasks
	rate2 := 0.15
	tasks = append(tasks, v1alpha1.TaskStatus{
		ID:          "docs/readme_comprehension_blind",
		Phase:       v1alpha1.TaskPhaseCompleted,
		Samples:     20,
		Successes:   3,
		SuccessRate: &rate2,
		SuccessRateCI: &v1alpha1.ConfidenceInterval{
			Lower:      0.05,
			Upper:      0.36,
			Confidence: 0.95,
			Method:     "wilson",
		},
	})
	s2 := ComputeRunSummary(tasks)
	if s2.Tasks != "2/2" {
		t.Errorf("Expected Tasks '2/2', got %s", s2.Tasks)
	}
	if s2.TotalSamples != 40 {
		t.Errorf("Expected TotalSamples 40, got %d", s2.TotalSamples)
	}
	expectedRates := "docs/readme_comprehension=0.85 [0.64, 0.95], docs/readme_comprehension_blind=0.15 [0.05, 0.36]"
	if s2.Rates != expectedRates {
		t.Errorf("Expected Rates %q, got %q", expectedRates, s2.Rates)
	}
	expectedMean := (0.85 + 0.15) / 2.0
	if s2.MeanSuccessRate == nil || math.Abs(*s2.MeanSuccessRate-expectedMean) > 1e-6 {
		t.Errorf("Expected MeanSuccessRate %f, got %v", expectedMean, s2.MeanSuccessRate)
	}
	if s2.SuccessRateRange == nil || s2.SuccessRateRange.Min != 0.15 || s2.SuccessRateRange.Max != 0.85 {
		t.Errorf("Expected SuccessRateRange [0.15, 0.85], got %+v", s2.SuccessRateRange)
	}

	// 4. Length capping / truncation
	capped := FormatRatesSummary(tasks, 50)
	if len(capped) > 50 {
		t.Errorf("Expected capped length <= 50, got %d (%s)", len(capped), capped)
	}
	if !strings.HasSuffix(capped, "...") {
		t.Errorf("Expected capped string to end with '...', got %s", capped)
	}
}

func TestReconcile_MTLSLifecycleAndSANVerification(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = sandboxv1beta1.AddToScheme(scheme)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-mtls",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abcdef123456",
			},
		},
	}

	fakeClient := sandboxclient.NewFakeClient()
	fakeClient.ListStdout = `{"tasks": [{"id": "task1", "spec": "eval.py@task1"}]}`

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	pkiMgr := pki.NewManager(cl, "eval-runner", "eval-runner-sandbox")
	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		ClientFactory:    fakeClient,
		PKIManager:       pkiMgr,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-mtls"}}

	// Turn 1: Create discovery sandbox
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}
	if !res.Requeue && res.RequeueAfter == 0 {
		t.Errorf("Expected requeue from turn 1")
	}

	// Verify discovery sandbox has mTLS volumes and arguments
	var sb sandboxv1beta1.Sandbox
	sbName := discovery.SandboxNameForRun("test-run-mtls")
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sbName}, &sb); err != nil {
		t.Fatalf("Failed to get discovery sandbox: %v", err)
	}
	podSpec := sb.Spec.PodTemplate.Spec
	var hasTLSVol, hasCAVol bool
	for _, v := range podSpec.Volumes {
		if v.Name == "sandbox-tls" {
			hasTLSVol = true
		}
		if v.Name == "sandbox-ca" {
			hasCAVol = true
		}
	}
	if !hasTLSVol || !hasCAVol {
		t.Errorf("Expected sandbox-tls and sandbox-ca volumes on sandbox, got: %+v", podSpec.Volumes)
	}

	// Verify server cert Secret created in eval-runner-sandbox
	secretName := pki.ServerSecretName(sbName)
	var serverSecret corev1.Secret
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: secretName}, &serverSecret); err != nil {
		t.Fatalf("Server cert secret %q not found in eval-runner-sandbox: %v", secretName, err)
	}
	if len(serverSecret.Data[pki.TLSCertKey]) == 0 || len(serverSecret.Data[pki.TLSKeyKey]) == 0 {
		t.Errorf("Server cert secret missing tls.crt or tls.key")
	}

	// Verify CA ConfigMap published in eval-runner-sandbox
	var caCM corev1.ConfigMap
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: pki.DefaultCAConfigMapName}, &caCM); err != nil {
		t.Fatalf("CA configmap not found in eval-runner-sandbox: %v", err)
	}
	if len(caCM.Data[pki.CACertKey]) == 0 {
		t.Errorf("CA configmap missing ca.crt")
	}

	// Simulate Pod Ready
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sbName,
			Namespace: "eval-runner-sandbox",
			Labels: map[string]string{
				discovery.LabelRunName:      "test-run-mtls",
				discovery.LabelRunNamespace: "eval-runner",
				discovery.LabelRole:         "discovery",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.77",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if err := cl.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create mock pod: %v", err)
	}

	// Turn 2: Run discovery over mTLS
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}

	// Verify FakeClient received TLSConfig pinning expected SAN
	if fakeClient.LastTLSConfig == nil {
		t.Fatalf("Expected ClientFactory to receive non-nil TLSConfig")
	}
	expectedSAN := pki.ServerSAN(sbName, "eval-runner-sandbox")
	if fakeClient.LastTLSConfig.ServerName != expectedSAN {
		t.Errorf("Expected TLSConfig.ServerName %q, got %q", expectedSAN, fakeClient.LastTLSConfig.ServerName)
	}
	if len(fakeClient.LastTLSConfig.Certificates) != 1 {
		t.Errorf("Expected 1 client certificate in TLSConfig, got %d", len(fakeClient.LastTLSConfig.Certificates))
	}
	if fakeClient.LastTLSConfig.RootCAs == nil {
		t.Errorf("Expected RootCAs in TLSConfig to be populated")
	}

	// Verify server cert secret was cleaned up after discovery completed
	var deletedSecret corev1.Secret
	err = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: secretName}, &deletedSecret)
	if err == nil {
		t.Errorf("Expected server cert secret to be deleted upon sandbox cleanup, but it was found")
	}

	// Security assertion: no private key in Run status or conditions
	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "test-run-mtls"}, &updatedRun)
	for _, cond := range updatedRun.Status.Conditions {
		if strings.Contains(cond.Message, "PRIVATE KEY") {
			t.Errorf("Private key leaked into Condition message: %s", cond.Message)
		}
	}
}

func TestValidateRunSpec(t *testing.T) {
	intPtr := func(i int) *int { return &i }

	tests := []struct {
		name        string
		spec        v1alpha1.RunSpec
		expectError bool
		errContains string
	}{
		{
			name: "neither limit nor sampleRange",
			spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{URL: "https://example.com/repo.git", Commit: "abc1234"},
			},
			expectError: false,
		},
		{
			name: "valid limit",
			spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "https://example.com/repo.git", Commit: "abc1234"},
				Limit: intPtr(10),
			},
			expectError: false,
		},
		{
			name: "valid sampleRange",
			spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "https://example.com/repo.git", Commit: "abc1234"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 10},
			},
			expectError: false,
		},
		{
			name: "both limit and sampleRange set - rejected",
			spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "https://example.com/repo.git", Commit: "abc1234"},
				Limit:       intPtr(10),
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 10},
			},
			expectError: true,
			errContains: "limit and sampleRange are mutually exclusive",
		},
		{
			name: "sampleRange start < 1 - rejected",
			spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "https://example.com/repo.git", Commit: "abc1234"},
				SampleRange: &v1alpha1.SampleRange{Start: 0, End: 10},
			},
			expectError: true,
			errContains: "start must be >= 1",
		},
		{
			name: "sampleRange end < start - rejected",
			spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "https://example.com/repo.git", Commit: "abc1234"},
				SampleRange: &v1alpha1.SampleRange{Start: 10, End: 5},
			},
			expectError: true,
			errContains: "end (5) must be >= start (10)",
		},
		{
			name: "limit < 1 - rejected",
			spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "https://example.com/repo.git", Commit: "abc1234"},
				Limit: intPtr(0),
			},
			expectError: true,
			errContains: "limit must be >= 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRunSpec(tt.spec)
			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestInspectEvalArgs(t *testing.T) {
	intPtr := func(i int) *int { return &i }
	task := v1alpha1.ManifestTask{
		ID:   "docs/readme_comprehension",
		Spec: "docs_comprehension.py@readme_comprehension",
	}

	t.Run("neither", func(t *testing.T) {
		spec := v1alpha1.RunSpec{
			Model: "mockllm/model",
		}
		args := InspectEvalArgs(task, spec)
		expected := []string{"inspect", "eval", "docs_comprehension.py@readme_comprehension", "--model", "mockllm/model", "--log-dir", "/out", "--log-format", "json", "--log-buffer", "1"}
		if !slices.Equal(args, expected) {
			t.Errorf("expected %v, got %v", expected, args)
		}
	})

	t.Run("limit", func(t *testing.T) {
		spec := v1alpha1.RunSpec{
			Model: "mockllm/model",
			Limit: intPtr(10),
		}
		args := InspectEvalArgs(task, spec)
		expected := []string{"inspect", "eval", "docs_comprehension.py@readme_comprehension", "--model", "mockllm/model", "--log-dir", "/out", "--log-format", "json", "--log-buffer", "1", "--limit", "10"}
		if !slices.Equal(args, expected) {
			t.Errorf("expected %v, got %v", expected, args)
		}
	})

	t.Run("sampleRange", func(t *testing.T) {
		spec := v1alpha1.RunSpec{
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
		}
		args := InspectEvalArgs(task, spec)
		expected := []string{"inspect", "eval", "docs_comprehension.py@readme_comprehension", "--model", "mockllm/model", "--log-dir", "/out", "--log-format", "json", "--log-buffer", "1", "--limit", "9-16"}
		if !slices.Equal(args, expected) {
			t.Errorf("expected %v, got %v", expected, args)
		}
	})
}

func TestEffectiveSampleRange(t *testing.T) {
	intPtr := func(i int) *int { return &i }

	t.Run("neither", func(t *testing.T) {
		spec := v1alpha1.RunSpec{}
		eff := EffectiveSampleRange(spec)
		if eff != nil {
			t.Errorf("expected nil, got %+v", eff)
		}
	})

	t.Run("limit", func(t *testing.T) {
		spec := v1alpha1.RunSpec{
			Limit: intPtr(15),
		}
		eff := EffectiveSampleRange(spec)
		if eff == nil || eff.Start != 1 || eff.End != 15 {
			t.Errorf("expected {1, 15}, got %+v", eff)
		}
	})

	t.Run("sampleRange", func(t *testing.T) {
		spec := v1alpha1.RunSpec{
			SampleRange: &v1alpha1.SampleRange{Start: 11, End: 20},
		}
		eff := EffectiveSampleRange(spec)
		if eff == nil || eff.Start != 11 || eff.End != 20 {
			t.Errorf("expected {11, 20}, got %+v", eff)
		}
	})
}

func TestReconcileInvalidRunSpecFails(t *testing.T) {
	scheme := setupScheme(t)
	intPtr := func(i int) *int { return &i }

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "invalid-spec-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Limit:       intPtr(5),
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 5},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(&v1alpha1.Run{}).
		Build()

	r := &RunReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "invalid-spec-run"}}
	res, err := r.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue for failed invalid run, got %+v", res)
	}

	var updated v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Failed to get updated run: %v", err)
	}
	if updated.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updated.Status.Phase)
	}
	if !strings.Contains(updated.Status.Message, "limit and sampleRange are mutually exclusive") {
		t.Errorf("Expected message to contain exclusivity error, got: %s", updated.Status.Message)
	}
}

func TestLiveTaskProgressPolling_SuccessiveSnapshots(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "progress-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
			Limit: func() *int { i := 10; return &i }(),
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:             "task-1",
					Phase:          v1alpha1.TaskPhaseRunning,
					SamplesPlanned: 10,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(&v1alpha1.Run{}).
		Build()

	startTime := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	var currentTime = startTime
	var timeMu sync.Mutex

	nowFn := func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		return currentTime
	}

	advanceTime := func(d time.Duration) {
		timeMu.Lock()
		currentTime = currentTime.Add(d)
		timeMu.Unlock()
	}

	var currentLog []byte
	var logMu sync.Mutex
	setLog := func(samples int) {
		logMu.Lock()
		defer logMu.Unlock()
		var sampleItems []string
		for i := 1; i <= samples; i++ {
			sampleItems = append(sampleItems, fmt.Sprintf(`{"id": %d}`, i))
		}
		currentLog = []byte(fmt.Sprintf(`{
			"version": 2,
			"status": "started",
			"eval": {"dataset": {"samples": 10}},
			"samples": [%s]
		}`, strings.Join(sampleItems, ",")))
	}

	// Start with 0 samples
	setLog(0)

	fc := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, config sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(config.Command) > 0 && config.Command[0] == "ls" {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "log_20261019.json\n"}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			logMu.Lock()
			defer logMu.Unlock()
			return currentLog, nil
		},
	}

	r := &RunReconciler{
		Client:                   cl,
		Scheme:                   scheme,
		Now:                      nowFn,
		PollProgressInterval:     10 * time.Millisecond,
		MinProgressWriteInterval: 10 * time.Second,
	}

	stopCh := make(chan struct{})
	pollerDone := make(chan struct{})

	go func() {
		defer close(pollerDone)
		r.pollTaskProgress(t.Context(), fc, run, "task-1", nil, 10*time.Millisecond, startTime, stopCh)
	}()

	// 1. Initial snapshot with 0 samples: wait 50ms, verify no write without change
	time.Sleep(50 * time.Millisecond)
	var r1 v1alpha1.Run
	_ = cl.Get(t.Context(), client.ObjectKeyFromObject(run), &r1)
	if r1.Status.Tasks[0].SamplesDone != 0 {
		t.Errorf("expected 0 samplesDone initially, got %d", r1.Status.Tasks[0].SamplesDone)
	}

	// 2. Set snapshot to 2 samples, but advance time only 5 seconds (less than 10s write limit)
	advanceTime(5 * time.Second)
	setLog(2)
	time.Sleep(50 * time.Millisecond)
	_ = cl.Get(t.Context(), client.ObjectKeyFromObject(run), &r1)
	if r1.Status.Tasks[0].SamplesDone != 0 {
		t.Errorf("expected at most one write per 10s: should NOT write after only 5s, got %d", r1.Status.Tasks[0].SamplesDone)
	}

	// 3. Now advance time by another 5 seconds (total 10s since start): write should happen!
	advanceTime(5 * time.Second)
	time.Sleep(50 * time.Millisecond)
	_ = cl.Get(t.Context(), client.ObjectKeyFromObject(run), &r1)
	if r1.Status.Tasks[0].SamplesDone != 2 {
		t.Fatalf("expected 2 samplesDone after 10s, got %d", r1.Status.Tasks[0].SamplesDone)
	}
	if r1.Status.Tasks[0].LastProgressAt == nil {
		t.Fatalf("expected lastProgressAt to be set")
	}

	// 4. Leave snapshot at 2 samples, advance time by 15s. No write without change!
	advanceTime(15 * time.Second)
	firstProgressTime := r1.Status.Tasks[0].LastProgressAt
	time.Sleep(50 * time.Millisecond)
	_ = cl.Get(t.Context(), client.ObjectKeyFromObject(run), &r1)
	if !r1.Status.Tasks[0].LastProgressAt.Equal(firstProgressTime) {
		t.Errorf("expected no write without change, but lastProgressAt was updated")
	}

	// 5. Test monotonicity: snapshot drops to 1 sample (e.g. truncated / rewritten). Poller must ignore it!
	advanceTime(15 * time.Second)
	setLog(1)
	time.Sleep(50 * time.Millisecond)
	_ = cl.Get(t.Context(), client.ObjectKeyFromObject(run), &r1)
	if r1.Status.Tasks[0].SamplesDone != 2 {
		t.Errorf("expected monotonicity preserved (still 2), got %d", r1.Status.Tasks[0].SamplesDone)
	}

	// 6. Set snapshot to 5 samples. Total time advanced > 10s: write should happen!
	setLog(5)
	time.Sleep(50 * time.Millisecond)
	_ = cl.Get(t.Context(), client.ObjectKeyFromObject(run), &r1)
	if r1.Status.Tasks[0].SamplesDone != 5 {
		t.Fatalf("expected 5 samplesDone, got %d", r1.Status.Tasks[0].SamplesDone)
	}

	// 7. Verify poller exits when stopCh closed
	close(stopCh)
	select {
	case <-pollerDone:
		// Exited cleanly!
	case <-time.After(2 * time.Second):
		t.Fatal("poller failed to exit after stopCh closed")
	}
}

func TestLiveTaskProgressPolling_WriteFloorFollowsInterval(t *testing.T) {
	// Verify helper methods
	rDefault := &RunReconciler{}
	if rDefault.pollProgressInterval() != DefaultPollProgressInterval {
		t.Errorf("expected default pollProgressInterval %v, got %v", DefaultPollProgressInterval, rDefault.pollProgressInterval())
	}
	if rDefault.minProgressWriteInterval() != DefaultPollProgressInterval {
		t.Errorf("expected default minProgressWriteInterval %v, got %v", DefaultPollProgressInterval, rDefault.minProgressWriteInterval())
	}

	rConfigured := &RunReconciler{
		PollProgressInterval: 2 * time.Second,
	}
	if rConfigured.pollProgressInterval() != 2*time.Second {
		t.Errorf("expected pollProgressInterval 2s, got %v", rConfigured.pollProgressInterval())
	}
	if rConfigured.minProgressWriteInterval() != 2*time.Second {
		t.Errorf("expected minProgressWriteInterval 2s following pollProgressInterval, got %v", rConfigured.minProgressWriteInterval())
	}

	rOverridden := &RunReconciler{
		PollProgressInterval:     2 * time.Second,
		MinProgressWriteInterval: 5 * time.Second,
	}
	if rOverridden.minProgressWriteInterval() != 5*time.Second {
		t.Errorf("expected explicit minProgressWriteInterval 5s, got %v", rOverridden.minProgressWriteInterval())
	}

	// Now verify in actual poller execution that the write floor follows PollProgressInterval (2s)
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-floor",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:             "task-1",
					Phase:          v1alpha1.TaskPhaseRunning,
					SamplesDone:    0,
					SamplesPlanned: 10,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	startTime := time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC)
	var currentTime = startTime
	var timeMu sync.Mutex

	nowFn := func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		return currentTime
	}

	advanceTime := func(d time.Duration) {
		timeMu.Lock()
		currentTime = currentTime.Add(d)
		timeMu.Unlock()
	}

	var currentLog []byte
	var logMu sync.Mutex
	setLog := func(samples int) {
		logMu.Lock()
		defer logMu.Unlock()
		var sampleItems []string
		for i := 1; i <= samples; i++ {
			sampleItems = append(sampleItems, fmt.Sprintf(`{"id": %d}`, i))
		}
		currentLog = []byte(fmt.Sprintf(`{
			"version": 2,
			"status": "started",
			"eval": {"dataset": {"samples": 10}},
			"samples": [%s]
		}`, strings.Join(sampleItems, ",")))
	}
	setLog(0)

	fc := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, config sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(config.Command) > 0 && config.Command[0] == "ls" {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "eval.json\n"}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			logMu.Lock()
			defer logMu.Unlock()
			return currentLog, nil
		},
	}

	// PollProgressInterval is 2s, MinProgressWriteInterval is 0 (unspecified).
	// Write floor should follow PollProgressInterval (2s).
	r := &RunReconciler{
		Client:               cl,
		Scheme:               scheme,
		Now:                  nowFn,
		PollProgressInterval: 2 * time.Second,
	}

	stopCh := make(chan struct{})
	pollerDone := make(chan struct{})

	go func() {
		defer close(pollerDone)
		r.pollTaskProgress(t.Context(), fc, run, "task-1", nil, 10*time.Millisecond, startTime, stopCh)
	}()

	// 1. Advance virtual time by 1s (less than 2s write floor), update log to 1 sample:
	advanceTime(1 * time.Second)
	setLog(1)
	time.Sleep(50 * time.Millisecond)

	var r1 v1alpha1.Run
	_ = cl.Get(t.Context(), client.ObjectKeyFromObject(run), &r1)
	if r1.Status.Tasks[0].SamplesDone != 0 {
		t.Errorf("expected 0 samplesDone after only 1s (less than 2s write floor), got %d", r1.Status.Tasks[0].SamplesDone)
	}

	// 2. Advance virtual time by another 1.5s (total 2.5s >= 2s): write should happen!
	advanceTime(1500 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	_ = cl.Get(t.Context(), client.ObjectKeyFromObject(run), &r1)
	if r1.Status.Tasks[0].SamplesDone != 1 {
		t.Fatalf("expected 1 samplesDone after 2.5s, got %d", r1.Status.Tasks[0].SamplesDone)
	}

	close(stopCh)
	<-pollerDone
}

func TestLiveTaskProgressPolling_FailureModes(t *testing.T) {
	scheme := setupScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "failure-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:             "task-1",
					Phase:          v1alpha1.TaskPhaseRunning,
					SamplesDone:    3,
					SamplesPlanned: 10,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(&v1alpha1.Run{}).
		Build()

	startTime := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)

	t.Run("404_And_ParseError_KeepLastValue", func(t *testing.T) {
		stopCh := make(chan struct{})
		pollerDone := make(chan struct{})
		callCount := 0

		fc := &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, config sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "log.json\n"}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				callCount++
				if callCount == 1 {
					return nil, fmt.Errorf("file %q not found (HTTP 404)", path)
				}
				return []byte("{invalid-json"), nil
			},
		}

		r := &RunReconciler{
			Client:               cl,
			Scheme:               scheme,
			Now:                  func() time.Time { return startTime.Add(20 * time.Second) },
			PollProgressInterval: 10 * time.Millisecond,
		}

		go func() {
			defer close(pollerDone)
			r.pollTaskProgress(t.Context(), fc, run, "task-1", nil, 10*time.Millisecond, startTime, stopCh)
		}()

		time.Sleep(50 * time.Millisecond)
		close(stopCh)
		<-pollerDone

		var updated v1alpha1.Run
		_ = cl.Get(t.Context(), client.ObjectKeyFromObject(run), &updated)
		if updated.Status.Tasks[0].SamplesDone != 3 {
			t.Errorf("expected samplesDone to stay at 3, got %d", updated.Status.Tasks[0].SamplesDone)
		}
	})

	t.Run("LimitError_StopsPolling", func(t *testing.T) {
		stopCh := make(chan struct{})
		pollerDone := make(chan struct{})

		fc := &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, config sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "log.json\n"}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				return nil, &discovery.LimitError{
					Limit:  10 * 1024 * 1024,
					Stream: "body",
				}
			},
		}

		r := &RunReconciler{
			Client:               cl,
			Scheme:               scheme,
			Now:                  func() time.Time { return startTime.Add(20 * time.Second) },
			PollProgressInterval: 10 * time.Millisecond,
		}

		go func() {
			defer close(pollerDone)
			r.pollTaskProgress(t.Context(), fc, run, "task-1", nil, 10*time.Millisecond, startTime, stopCh)
		}()

		select {
		case <-pollerDone:
			// Successfully stopped polling upon encountering LimitError!
		case <-time.After(1 * time.Second):
			close(stopCh)
			t.Fatal("expected poller to exit on LimitError, but it kept running")
		}

		var updated v1alpha1.Run
		_ = cl.Get(t.Context(), client.ObjectKeyFromObject(run), &updated)
		if updated.Status.Tasks[0].SamplesDone != 3 {
			t.Errorf("expected samplesDone to keep last value 3, got %d", updated.Status.Tasks[0].SamplesDone)
		}
	})
}

func TestReconcileSampleExecution_LiveProgressPollerLifecycle(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-1", Spec: "eval.py@task1", Workdir: "evals"},
		},
	}

	limit := 5
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-poller",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Model: "mockllm/model",
			Limit: &limit,
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-1"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-1", Phase: v1alpha1.TaskPhasePending, SamplesPlanned: 5},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	startTime := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	var currentTime = startTime
	var timeMu sync.Mutex

	nowFn := func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		return currentTime
	}

	intermediateLog := `{
		"version": 2,
		"status": "started",
		"eval": {"task": "task-1", "scorers": [{"name": "choice"}], "dataset": {"samples": 5}},
		"samples": [
			{"id": 1, "scores": {"choice": {"value": "C"}}},
			{"id": 2, "scores": {"choice": {"value": "C"}}}
		]
	}`

	finalLog := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "task-1", "scorers": [{"name": "choice"}], "dataset": {"samples": 5}},
		"samples": [
			{"id": 1, "scores": {"choice": {"value": "C"}}},
			{"id": 2, "scores": {"choice": {"value": "C"}}},
			{"id": 3, "scores": {"choice": {"value": "C"}}},
			{"id": 4, "scores": {"choice": {"value": "I"}}},
			{"id": 5, "scores": {"choice": {"value": "C"}}}
		]
	}`

	evalExecuting := make(chan struct{})
	continueEval := make(chan struct{})

	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, config sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(config.Command) > 0 && config.Command[0] == "inspect" {
				close(evalExecuting)
				<-continueEval
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			}
			if len(config.Command) > 0 && config.Command[0] == "ls" {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "sample_log.json\n"}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			select {
			case <-continueEval:
				return []byte(finalLog), nil
			default:
				return []byte(intermediateLog), nil
			}
		},
	}

	reconciler := &RunReconciler{
		Client:               cl,
		Scheme:               scheme,
		SandboxClient:        executor,
		SandboxNamespace:     "eval-runner-sandbox",
		Now:                  nowFn,
		PollProgressInterval: 10 * time.Millisecond,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-poller"}}

	// Turn 1: Start pending task and create sandbox
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}
	_ = res

	var sbList sandboxv1beta1.SandboxList
	if err := cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox")); err != nil {
		t.Fatalf("Failed to list sandboxes: %v", err)
	}
	taskSB := sbList.Items[0]

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskSB.Name,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.12",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	if err := cl.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Turn 2 in background: Reconcile will block in inspect eval
	reconcileDone := make(chan struct{})
	go func() {
		defer close(reconcileDone)
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Errorf("Turn 2 error: %v", err)
		}
	}()

	// Wait until inspect eval begins
	<-evalExecuting

	// Advance time past 10s so poller can write status
	timeMu.Lock()
	currentTime = currentTime.Add(15 * time.Second)
	timeMu.Unlock()

	// Wait for poller to perform live status update
	time.Sleep(50 * time.Millisecond)
	var midRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &midRun)
	if midRun.Status.Tasks[0].SamplesDone != 2 {
		t.Errorf("expected live samplesDone to be 2 mid-execution, got %d", midRun.Status.Tasks[0].SamplesDone)
	}
	if midRun.Status.Tasks[0].SamplesPlanned != 5 {
		t.Errorf("expected samplesPlanned to be 5, got %d", midRun.Status.Tasks[0].SamplesPlanned)
	}

	// Let inspect eval finish
	close(continueEval)
	<-reconcileDone

	// Check final run status after execution
	var completedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &completedRun)
	if completedRun.Status.Tasks[0].SamplesDone != 5 {
		t.Errorf("expected final samplesDone == samples == 5, got %d", completedRun.Status.Tasks[0].SamplesDone)
	}
	if completedRun.Status.Tasks[0].Samples != 5 {
		t.Errorf("expected final samples == 5, got %d", completedRun.Status.Tasks[0].Samples)
	}
}

func TestFailedTaskPreservesSamplesDone(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-fail", Spec: "eval.py@task_fail", Workdir: "evals"},
		},
	}

	limit := 10
	progressTime := metav1.NewTime(time.Date(2026, 10, 19, 12, 0, 10, 0, time.UTC))
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-fail-preserves",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
			Model: "mockllm/model",
			Limit: &limit,
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"task-fail"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:             "task-fail",
					Phase:          v1alpha1.TaskPhaseRunning,
					SamplesDone:    7,
					SamplesPlanned: 10,
					LastProgressAt: &progressTime,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, config sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(config.Command) > 0 && config.Command[0] == "inspect" {
				return &sandboxclient.ExecuteResult{ExitCode: 1, Stderr: "inspect crashed mid-eval"}, nil
			}
			if len(config.Command) > 0 && config.Command[0] == "ls" {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "sample_log.json\n"}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			return nil, fmt.Errorf("no log")
		},
	}

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	// Create ready pod and sandbox for running task
	sbName := discovery.TaskSandboxNameForRun(run.Name, "task-fail")
	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sbName,
			Namespace: "eval-runner-sandbox",
			Labels: map[string]string{
				discovery.LabelRunName:      run.Name,
				discovery.LabelRunNamespace: run.Namespace,
				discovery.LabelRole:         discovery.RoleSample,
			},
			Annotations: map[string]string{
				discovery.AnnotationTaskID: "task-fail",
			},
		},
	}
	_ = cl.Create(t.Context(), sb)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sbName,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.12",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	_ = cl.Create(t.Context(), pod)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-fail-preserves"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("expected task to be Failed, got %s", updatedRun.Status.Tasks[0].Phase)
	}
	// Assert samplesDone stays 7 (preserved from live progress) and lastProgressAt remains set!
	if updatedRun.Status.Tasks[0].SamplesDone != 7 {
		t.Errorf("expected failed task to preserve samplesDone=7, got %d", updatedRun.Status.Tasks[0].SamplesDone)
	}
	if updatedRun.Status.Tasks[0].LastProgressAt == nil || !updatedRun.Status.Tasks[0].LastProgressAt.Equal(&progressTime) {
		t.Errorf("expected failed task to preserve lastProgressAt=%v, got %v", progressTime, updatedRun.Status.Tasks[0].LastProgressAt)
	}
}

func TestComputeSamplesPlanned_Clamping(t *testing.T) {
	limit50 := 50
	limit10 := 10

	// 1. Limit 50 exceeds dataset size 21 -> clamped to 21
	if got := computeSamplesPlanned(nil, &limit50, 21); got != 21 {
		t.Errorf("expected limit 50 clamped to dataset size 21, got %d", got)
	}

	// 2. Limit 10 within dataset size 21 -> 10
	if got := computeSamplesPlanned(nil, &limit10, 21); got != 10 {
		t.Errorf("expected limit 10, got %d", got)
	}

	// 3. Limit 50 with dataset size unknown (0) -> 50
	if got := computeSamplesPlanned(nil, &limit50, 0); got != 50 {
		t.Errorf("expected limit 50 when dataset unknown, got %d", got)
	}

	// 4. SampleRange [1, 30] (width 30) exceeds dataset size 21 -> clamped to 21
	sr := &v1alpha1.SampleRange{Start: 1, End: 30}
	if got := computeSamplesPlanned(sr, nil, 21); got != 21 {
		t.Errorf("expected range width 30 clamped to dataset size 21, got %d", got)
	}

	// 5. No limit or range, dataset size 21 -> 21
	if got := computeSamplesPlanned(nil, nil, 21); got != 21 {
		t.Errorf("expected dataset size 21 when no limit/range, got %d", got)
	}
}
