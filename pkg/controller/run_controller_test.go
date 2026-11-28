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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/internal/fakes"
	"github.com/gke-labs/eval-runner/pkg/agent"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/artifacts/gcs"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/modelclient/gemini"
	"github.com/gke-labs/eval-runner/pkg/pki"
	"github.com/gke-labs/eval-runner/pkg/sandboxclient"
	"github.com/gke-labs/eval-runner/pkg/stats"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

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
			switch gitCommandSubcommand(cfg.Command) {
			case "fetch":
				cloneAttempts++
				if cloneAttempts <= 2 {
					// Simulate transient connection failure for first 2 attempts
					return &sandboxclient.ExecuteResult{
						ExitCode: 128,
						Stderr:   "fatal: unable to connect to git-server.eval-runner-sandbox.svc.cluster.local: Connection refused",
					}, nil
				}
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			case "clone":
				if cloneAttempts <= 2 {
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
			switch gitCommandSubcommand(cfg.Command) {
			case "fetch":
				cloneAttempts++
				return &sandboxclient.ExecuteResult{
					ExitCode: 128,
					Stderr:   "fatal: unable to connect to git-server:\ngit-server: errno=Connection refused",
				}, nil
			case "clone":
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
			expectedMsgSubstr: []string{evalsListNotFoundOrNotExecutableMessage},
		},
		{
			name: ".evals/list non-executable fails immediately without retry",
			client: &sandboxclient.FakeClient{
				ListErr: status.Errorf(codes.PermissionDenied, "permission denied: ./.evals/list"),
			},
			expectedMsgSubstr: []string{evalsListNotFoundOrNotExecutableMessage},
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

func TestReconcileDiscovery_MissingEvalsListFails(t *testing.T) {
	fakeClient := &sandboxclient.FakeClient{
		ListExit:   127,
		ListStderr: "sh: 1: ./.evals/list: not found",
	}
	f := newDiscoveringRunFixture(t, fakeClient)
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
	expectedMsg := evalsListNotFoundOrNotExecutableMessage
	if !strings.Contains(updatedRun.Status.Message, expectedMsg) {
		t.Errorf("Expected status message to contain %q, got: %s", expectedMsg, updatedRun.Status.Message)
	}
	if !strings.Contains(updatedRun.Status.Message, "sh: 1: ./.evals/list: not found") {
		t.Errorf("Expected status message to contain stderr tail, got: %s", updatedRun.Status.Message)
	}

	// Assert the fake saw exactly the repo setup steps (clean, init, shallow fetch, checkout) and list step, and no scan step.
	if len(fakeClient.Calls) != 5 {
		t.Fatalf("Expected exactly 5 calls, got %d: %+v", len(fakeClient.Calls), fakeClient.Calls)
	}
	for _, call := range fakeClient.Calls {
		for _, arg := range call.Command {
			if strings.Contains(arg, "scan.tar") || strings.Contains(arg, "test -f .evals/list") {
				t.Errorf("Unexpected scan or test step executed: %v", call.Command)
			}
		}
	}
	lastCall := fakeClient.Calls[4]
	if len(lastCall.Command) < 3 || !strings.Contains(lastCall.Command[2], "./.evals/list") {
		t.Errorf("Expected step 5 to be ./.evals/list, got: %v", lastCall.Command)
	}
}

func TestReconcileDiscovery_NonExecutableEvalsListFails(t *testing.T) {
	fakeClient := &sandboxclient.FakeClient{
		ListExit:   126,
		ListStderr: "sh: 1: ./.evals/list: Permission denied",
	}
	f := newDiscoveringRunFixture(t, fakeClient)
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
	expectedMsg := evalsListNotFoundOrNotExecutableMessage
	if !strings.Contains(updatedRun.Status.Message, expectedMsg) {
		t.Errorf("Expected status message to contain %q, got: %s", expectedMsg, updatedRun.Status.Message)
	}
	if !strings.Contains(updatedRun.Status.Message, "sh: 1: ./.evals/list: Permission denied") {
		t.Errorf("Expected status message to contain stderr tail, got: %s", updatedRun.Status.Message)
	}
}

func TestReconcileDiscoveringRunRecoversStaleSandboxEndpoint(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-stale-endpoint"
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
	sb.Status.PodIPs = []string{"10.244.0.5"}

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

	var dialedIPs []string
	var dialCount int
	var cl client.Client

	fakeClient := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if subcommand := gitCommandSubcommand(cfg.Command); subcommand == "fetch" || subcommand == "clone" {
				dialCount++
				if dialCount == 1 {
					// Pod is replaced with a new IP while Sandbox.status remains stale at 10.244.0.5
					pod.Status.PodIP = "10.244.0.42"
					_ = cl.Status().Update(ctx, pod)
					return nil, status.Error(codes.Unavailable, `rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 10.244.0.5:9090: i/o timeout"`)
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

	factory := sandboxclient.FactoryFunc(func(podIP string, opts ...sandboxclient.Option) (sandboxclient.Client, error) {
		dialedIPs = append(dialedIPs, podIP)
		return fakeClient, nil
	})

	cl = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run, sb, pod).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		ClientFactory:    factory,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no RequeueAfter on successful discovery completion, got %v", res.RequeueAfter)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Get updated run error: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
		t.Errorf("Expected phase %s, got %s (message: %s)", v1alpha1.RunPhaseDiscovered, updatedRun.Status.Phase, updatedRun.Status.Message)
	}

	if len(dialedIPs) != 2 || dialedIPs[0] != "10.244.0.5" || dialedIPs[1] != "10.244.0.42" {
		t.Errorf("Expected dialed IPs [10.244.0.5, 10.244.0.42], got %v", dialedIPs)
	}

	if strings.Contains(updatedRun.Status.Message, "retry") || strings.Contains(updatedRun.Status.Message, "failed") {
		t.Errorf("Expected no retry or failure message in status.message, got %q", updatedRun.Status.Message)
	}
}

func TestReconcileDiscoveringRunUnchangedEndpointRequeuesWithoutCountingAttempt(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-unchanged-endpoint"
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
	sb.Status.PodIPs = []string{"10.244.0.5"}

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

	var dialCount int
	fakeClient := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if subcommand := gitCommandSubcommand(cfg.Command); subcommand == "fetch" || subcommand == "clone" {
				dialCount++
				return nil, status.Error(codes.Unavailable, `rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 10.244.0.5:9090: i/o timeout"`)
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
	}

	var dialedIPs []string
	factory := sandboxclient.FactoryFunc(func(podIP string, opts ...sandboxclient.Option) (sandboxclient.Client, error) {
		dialedIPs = append(dialedIPs, podIP)
		return fakeClient, nil
	})

	// Pod and Sandbox status remain at 10.244.0.5 (unchanged)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run, sb).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		ClientFactory:    factory,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected RequeueAfter when endpoint unchanged, got 0")
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Get updated run error: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovering {
		t.Errorf("Expected phase %s, got %s", v1alpha1.RunPhaseDiscovering, updatedRun.Status.Phase)
	}

	// Assert only 1 dial was performed (no re-dialing against identical dead endpoint!)
	if dialCount != 1 {
		t.Errorf("Expected exactly 1 clone dial, got %d", dialCount)
	}

	// Assert attempt was not counted
	if strings.Contains(updatedRun.Status.Message, "failed") || strings.Contains(updatedRun.Status.Message, "retry") {
		t.Errorf("Expected no failure or retry message in status.message, got %q", updatedRun.Status.Message)
	}

	// Assert status.message carries the dial error
	if !strings.Contains(updatedRun.Status.Message, "transport: Error while dialing") {
		t.Errorf("Expected status.message to carry dial error, got %q", updatedRun.Status.Message)
	}
}

func TestReconcileDiscoveringRunNewPodIPUnavailableNoAlternation(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-new-ip-unavailable"
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
	// Sandbox status is stale: still lists 10.244.0.5
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))
	sb.Status.PodIPs = []string{"10.244.0.5"}

	// Pod has been replaced to a new IP: 10.244.0.42
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
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

	var dialedIPs []string
	fakeClient := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if subcommand := gitCommandSubcommand(cfg.Command); subcommand == "fetch" || subcommand == "clone" {
				return nil, status.Error(codes.Unavailable, `rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 10.244.0.42:9090: connect: connection refused"`)
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
	}

	factory := sandboxclient.FactoryFunc(func(podIP string, opts ...sandboxclient.Option) (sandboxclient.Client, error) {
		dialedIPs = append(dialedIPs, podIP)
		return fakeClient, nil
	})

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run, sb, pod).
		Build()

	maxAttempts := 3
	maxUncounted := 2
	reconciler := &RunReconciler{
		Client:                         cl,
		Scheme:                         scheme,
		ClientFactory:                  factory,
		SandboxNamespace:               "eval-runner-sandbox",
		MaxDiscoveryAttempts:           maxAttempts,
		MaxUncountedRequeuesPerAttempt: maxUncounted,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}

	// Reconcile across uncounted requeues and counted attempts until termination
	totalReconciles := maxAttempts * maxUncounted
	for i := 0; i < totalReconciles; i++ {
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile iteration %d error: %v", i+1, err)
		}
		if i < totalReconciles-1 {
			if res.RequeueAfter == 0 {
				t.Fatalf("Iteration %d: expected RequeueAfter > 0, got 0", i+1)
			}
		} else {
			// Final iteration should not requeue, but fail the run
			if res.RequeueAfter != 0 {
				t.Fatalf("Final iteration: expected RequeueAfter == 0 on failure, got %v", res.RequeueAfter)
			}
		}
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Get updated run error: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase %s, got %s (message: %s)", v1alpha1.RunPhaseFailed, updatedRun.Status.Phase, updatedRun.Status.Message)
	}

	// Verify failed message contains attempts and dial error
	if !strings.Contains(updatedRun.Status.Message, "step git clone failed after 3 attempts") {
		t.Errorf("Expected status.Message to contain 'step git clone failed after 3 attempts', got: %s", updatedRun.Status.Message)
	}
	if !strings.Contains(updatedRun.Status.Message, "dial tcp 10.244.0.42:9090") {
		t.Errorf("Expected status.Message to contain dial error, got: %s", updatedRun.Status.Message)
	}

	// Verify that every dialed IP was exactly the new IP (10.244.0.42) and never alternated to stale 10.244.0.5
	if len(dialedIPs) == 0 {
		t.Fatalf("Expected dials, got none")
	}
	for i, ip := range dialedIPs {
		if ip != "10.244.0.42" {
			t.Errorf("Dial %d: expected new IP 10.244.0.42, got %s (alternation detected)", i, ip)
		}
	}
}

func TestReconcileDiscoveringRunUnchangedEndpointCapsUncountedRequeuesAndFails(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-unchanged-cap"
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
	sb.Status.PodIPs = []string{"10.244.0.5"}

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

	fakeClient := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if subcommand := gitCommandSubcommand(cfg.Command); subcommand == "fetch" || subcommand == "clone" {
				return nil, status.Error(codes.Unavailable, `rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 10.244.0.5:9090: i/o timeout"`)
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
	}

	factory := sandboxclient.FactoryFunc(func(podIP string, opts ...sandboxclient.Option) (sandboxclient.Client, error) {
		return fakeClient, nil
	})

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run, sb).
		Build()

	maxAttempts := 2
	maxUncounted := 2
	reconciler := &RunReconciler{
		Client:                         cl,
		Scheme:                         scheme,
		ClientFactory:                  factory,
		SandboxNamespace:               "eval-runner-sandbox",
		MaxDiscoveryAttempts:           maxAttempts,
		MaxUncountedRequeuesPerAttempt: maxUncounted,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}

	totalReconciles := maxAttempts * maxUncounted
	for i := 0; i < totalReconciles; i++ {
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error at iteration %d: %v", i+1, err)
		}
		var currentRun v1alpha1.Run
		if getErr := cl.Get(t.Context(), req.NamespacedName, &currentRun); getErr != nil {
			t.Fatalf("Get run error at iteration %d: %v", i+1, getErr)
		}

		if i < totalReconciles-1 {
			if res.RequeueAfter == 0 {
				t.Fatalf("Expected RequeueAfter at iteration %d, got 0", i+1)
			}
			if currentRun.Status.Phase != v1alpha1.RunPhaseDiscovering {
				t.Fatalf("Expected phase Discovering at iteration %d, got %s", i+1, currentRun.Status.Phase)
			}
			if !strings.Contains(currentRun.Status.Message, "transport: Error while dialing") {
				t.Errorf("Iteration %d: expected status.Message to carry dial error, got %q", i+1, currentRun.Status.Message)
			}
		} else {
			if res.RequeueAfter != 0 {
				t.Fatalf("Expected no RequeueAfter on final failure, got %v", res.RequeueAfter)
			}
			if currentRun.Status.Phase != v1alpha1.RunPhaseFailed {
				t.Fatalf("Expected phase Failed on final iteration, got %s", currentRun.Status.Phase)
			}
			if !strings.Contains(currentRun.Status.Message, "step git clone failed after 2 attempts") {
				t.Errorf("Expected status.Message to contain 'step git clone failed after 2 attempts', got: %s", currentRun.Status.Message)
			}
			if !strings.Contains(currentRun.Status.Message, "transport: Error while dialing") {
				t.Errorf("Expected status.Message to contain dial error, got: %s", currentRun.Status.Message)
			}
		}
	}

	// Verify discovery sandbox was deleted on failure
	var checkSb sandboxv1beta1.Sandbox
	getErr := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb)
	if getErr == nil || !apierrors.IsNotFound(getErr) {
		t.Errorf("Expected sandbox to be deleted on failure, getErr=%v", getErr)
	}
}

func TestReconcileDiscoveringRunMissingSandboxRecreated(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-missing-sb"
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
	sb.Status.PodIPs = []string{"10.244.0.5"}

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

	fakeClient := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			return nil, status.Error(codes.Unavailable, `rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 10.244.0.5:9090: i/o timeout"`)
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run, sb).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if sbList, ok := list.(*sandboxv1beta1.SandboxList); ok {
					sbList.Items = nil
					return nil
				}
				return c.List(ctx, list, opts...)
			},
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*sandboxv1beta1.Sandbox); ok {
					return apierrors.NewNotFound(v1alpha1.GroupVersion.WithResource("sandboxes").GroupResource(), key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    fakeClient,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected RequeueAfter when restarting discovery, got 0")
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Get updated run error: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovering {
		t.Errorf("Expected phase %s to remain Discovering, got %s", v1alpha1.RunPhaseDiscovering, updatedRun.Status.Phase)
	}
}

func TestReconcileDiscoveringRunRecoversStaleEndpointOnCleanRepo(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-stale-clean"
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
	sb.Status.PodIPs = []string{"10.244.0.5"}

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

	var dialedIPs []string
	cleanCalls := 0
	var cl client.Client

	fakeClient := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 0 && cfg.Command[0] == "rm" {
				cleanCalls++
				if cleanCalls == 1 {
					// Pod is replaced with a new IP while Sandbox.status remains stale at 10.244.0.5
					pod.Status.PodIP = "10.244.0.42"
					_ = cl.Status().Update(ctx, pod)
					return nil, status.Error(codes.Unavailable, `rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 10.244.0.5:9090: i/o timeout"`)
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

	factory := sandboxclient.FactoryFunc(func(podIP string, opts ...sandboxclient.Option) (sandboxclient.Client, error) {
		dialedIPs = append(dialedIPs, podIP)
		return fakeClient, nil
	})

	cl = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run, sb, pod).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		ClientFactory:    factory,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no RequeueAfter on successful discovery completion, got %v", res.RequeueAfter)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Get updated run error: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
		t.Errorf("Expected phase %s, got %s (message: %s)", v1alpha1.RunPhaseDiscovered, updatedRun.Status.Phase, updatedRun.Status.Message)
	}

	if len(dialedIPs) != 2 || dialedIPs[0] != "10.244.0.5" || dialedIPs[1] != "10.244.0.42" {
		t.Errorf("Expected dialed IPs [10.244.0.5, 10.244.0.42], got %v", dialedIPs)
	}
}

func TestReconcileDiscoveringRunRebuildsClientWhenPodIPChangesOnSubsequentAttempt(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-rebuild-client"
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
	sb.Status.PodIPs = []string{"10.244.0.5"}

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

	var dialedIPs []string
	var cloneAttempts int
	var cl client.Client

	fakeClient := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			switch gitCommandSubcommand(cfg.Command) {
			case "fetch":
				cloneAttempts++
				if cloneAttempts == 1 {
					// Attempt 1 fails at git level (non-Unavailable)
					// Update pod IP in informer so before attempt 2 it re-resolves new IP
					pod.Status.PodIP = "10.244.0.42"
					_ = cl.Status().Update(ctx, pod)
					return &sandboxclient.ExecuteResult{ExitCode: 128, Stderr: "fatal: early EOF"}, nil
				}
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			case "clone":
				if cloneAttempts == 1 {
					return &sandboxclient.ExecuteResult{ExitCode: 128, Stderr: "fatal: early EOF"}, nil
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

	factory := sandboxclient.FactoryFunc(func(podIP string, opts ...sandboxclient.Option) (sandboxclient.Client, error) {
		dialedIPs = append(dialedIPs, podIP)
		return fakeClient, nil
	})

	cl = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run, sb, pod).
		Build()

	reconciler := &RunReconciler{
		Client:                cl,
		Scheme:                scheme,
		ClientFactory:         factory,
		SandboxNamespace:      "eval-runner-sandbox",
		DiscoveryRetryBackoff: []time.Duration{time.Millisecond},
		Sleep:                 func(ctx context.Context, d time.Duration) error { return nil },
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no RequeueAfter on completion, got %v", res.RequeueAfter)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Get updated run error: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
		t.Errorf("Expected phase %s, got %s (message: %s)", v1alpha1.RunPhaseDiscovered, updatedRun.Status.Phase, updatedRun.Status.Message)
	}

	// Verify that client was rebuilt with 10.244.0.42 before attempt 2
	if len(dialedIPs) != 2 || dialedIPs[0] != "10.244.0.5" || dialedIPs[1] != "10.244.0.42" {
		t.Errorf("Expected dialed IPs [10.244.0.5, 10.244.0.42], got %v", dialedIPs)
	}
}

func TestIsUsableSandboxPod(t *testing.T) {
	now := metav1.Now()
	isController := true
	notController := false

	tests := []struct {
		name string
		pod  *corev1.Pod
		uid  types.UID
		want bool
	}{
		{"nil pod", nil, "sb-1", false},
		{"terminating", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now, OwnerReferences: []metav1.OwnerReference{{UID: "sb-1", Controller: &isController}}}}, "sb-1", false},
		{"matching UID", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{UID: "sb-1", Controller: &isController}}}}, "sb-1", true},
		{"mismatched UID", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{UID: "sb-old", Controller: &isController}}}}, "sb-1", false},
		{"not controller", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{UID: "sb-1", Controller: &notController}}}}, "sb-1", false},
		{"no owner ref with UID", &corev1.Pod{}, "sb-1", false},
		{"empty UID and no controller ref", &corev1.Pod{}, "", true},
		{"empty UID with controller ref", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{UID: "sb-1", Controller: &isController}}}}, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUsableSandboxPod(tt.pod, tt.uid); got != tt.want {
				t.Errorf("isUsableSandboxPod() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReconcileDiscovering_WaitsForCurrentSandboxPodWhenTerminatingOrOldPodExists(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-rediscovery-terminating-pod"
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       runName,
			Namespace:  "eval-runner",
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec:   v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{URL: "git://git-server/repo.git", Commit: "abc1234"}},
		Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseDiscovering},
	}

	sandboxName := discovery.SandboxNameForRun(runName)
	newSandbox := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))
	newSandbox.UID = "sandbox-new-uid"

	isController := true
	now := metav1.Now()
	terminatingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: sandboxName, Namespace: "eval-runner-sandbox",
			DeletionTimestamp: &now, Finalizers: []string{"test/cleanup"},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: sandboxv1beta1.GroupVersion.String(), Kind: "Sandbox", Name: sandboxName, UID: "sandbox-old-uid", Controller: &isController},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: "10.244.0.5",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}

	var executed bool
	fakeClient := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			executed = true
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, newSandbox, terminatingPod).
		WithStatusSubresource(run, newSandbox, terminatingPod).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    fakeClient,
		SandboxNamespace: "eval-runner-sandbox",
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected RequeueAfter when waiting for current sandbox pod, got 0")
	}
	if executed {
		t.Errorf("Discovery executed in terminating old pod, expected it to wait")
	}

	var currentRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &currentRun); err != nil {
		t.Fatalf("Get run error: %v", err)
	}
	if currentRun.Status.Phase != v1alpha1.RunPhaseDiscovering {
		t.Errorf("Expected run to stay Discovering, got %s", currentRun.Status.Phase)
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
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
		Config: ClusterConfig{
			DiscoveryDeadline: 20 * time.Minute,
		},
		Now: func() time.Time { return now.Add(2 * time.Minute) },
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
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
		Config: ClusterConfig{
			DiscoveryDeadline: 20 * time.Minute,
		},
		Now: func() time.Time { return now.Add(11 * time.Minute) },
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
			switch gitCommandSubcommand(cfg.Command) {
			case "fetch":
				cloneAttempts++
				return &sandboxclient.ExecuteResult{
					ExitCode: 128,
					Stderr:   "fatal: unable to connect to git-server: Connection refused",
				}, nil
			case "clone":
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
		Client:           client,
		Scheme:           scheme,
		SandboxClient:    &sandboxclient.FakeClient{},
		SandboxNamespace: "eval-runner-sandbox",
		Config: ClusterConfig{
			DiscoveryDeadline: customDeadline,
		},
		Now: func() time.Time { return fixedNow },
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
		if subcommand := gitCommandSubcommand(call.Command); subcommand == "fetch" || subcommand == "clone" {
			cloneCount++
		}
	}
	if cloneCount != 1 {
		t.Errorf("Expected exactly 1 clone/fetch call (no retries for Unauthenticated), got %d", cloneCount)
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
			{ID: "task-sandbox", Spec: "test.py@4", Image: "sandbox-image:v1", Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}}},
			{ID: "task-mixed", Spec: "test.py@5", Image: "sandbox-image:v1", Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}, {Type: "docker"}}},
		},
	}

	reconciler := &RunReconciler{}

	// Case 1: spec.Tasks empty -> all tasks considered
	tasks1 := reconciler.initializeTasks(v1alpha1.RunSpec{}, manifest)
	if len(tasks1) != 5 {
		t.Fatalf("Expected 5 tasks, got %d", len(tasks1))
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
	if tasks1[3].Phase != v1alpha1.TaskPhasePending {
		t.Errorf("Expected task-sandbox to be Pending, got %s", tasks1[3].Phase)
	}
	if tasks1[4].Phase != v1alpha1.TaskPhaseFailed || tasks1[4].Message != "resources not supported yet" {
		t.Errorf("Expected task-mixed to be Failed with 'resources not supported yet', got %s (%s)", tasks1[4].Phase, tasks1[4].Message)
	}

	// Case 2: spec.Tasks allow-list
	tasks2 := reconciler.initializeTasks(v1alpha1.RunSpec{
		Tasks: []string{"task-eligible-1", "nonexistent-task", "task-with-resources", "task-sandbox", "task-mixed"},
	}, manifest)
	if len(tasks2) != 5 {
		t.Fatalf("Expected 5 tasks, got %d", len(tasks2))
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
	if tasks2[3].ID != "task-sandbox" || tasks2[3].Phase != v1alpha1.TaskPhasePending {
		t.Errorf("Expected task-sandbox to be Pending, got %+v", tasks2[3])
	}
	if tasks2[4].ID != "task-mixed" || tasks2[4].Phase != v1alpha1.TaskPhaseFailed || tasks2[4].Message != "resources not supported yet" {
		t.Errorf("Expected task-mixed to be Failed, got %+v", tasks2[4])
	}
}

func TestRunController_SandboxTaskExecution(t *testing.T) {
	scheme := setupScheme(t)

	t.Run("CompletedWithRightSampleCounts", func(t *testing.T) {
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:        "task-sandbox-1",
					Spec:      "eval.py@1",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-eval", "--all"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-sandbox-complete",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "commit1"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
			},
		}

		fakeClient := &sandboxclient.FakeClient{
			PollAsyncFn: func(ctx context.Context, executionID string) (*sandboxclient.AsyncTaskStatus, error) {
				return &sandboxclient.AsyncTaskStatus{State: sandboxclient.AsyncTaskCompleted, ExitCode: 0, Stdout: "task finished"}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				if path == "/out/results.json" {
					return []byte(`{"schemaVersion": 1, "samples": [{"id": "sample-1", "passed": true, "score": 1.0, "message": ""}]}`), nil
				}
				return nil, fmt.Errorf("file %q not found (HTTP 404)", path)
			},
		}

		sink := artifacts.NewInMemorySink()
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(run).
			WithStatusSubresource(run).
			Build()

		reconciler := &RunReconciler{
			Client:           cl,
			Scheme:           scheme,
			SandboxNamespace: "eval-runner-sandbox",
			SandboxClient:    fakeClient,
			ArtifactSink:     sink,
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-sandbox-complete"}}

		// Turn 1: initialize task as Pending and create Sandbox
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 1 error: %v", err)
		}

		var sbList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
		if len(sbList.Items) != 1 {
			t.Fatalf("Expected 1 sandbox created, got %d", len(sbList.Items))
		}
		if sandboxImage := sbList.Items[0].Spec.PodTemplate.Spec.Containers[0].Image; sandboxImage != "custom-task-image:v1" {
			t.Errorf("Expected sandbox container image 'custom-task-image:v1', got %q", sandboxImage)
		}

		// Create Pod for sandbox
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbList.Items[0].Name,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.50",
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		if err := cl.Create(t.Context(), pod); err != nil {
			t.Fatalf("failed to create pod: %v", err)
		}

		// Turn 2: start async task command and persist ExecutionID
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}
		if len(fakeClient.StartAsyncCalls) != 1 {
			t.Fatalf("Expected 1 StartAsync call, got %d", len(fakeClient.StartAsyncCalls))
		}
		executedCommand := fakeClient.StartAsyncCalls[0].Config.Command
		executedCwd := fakeClient.StartAsyncCalls[0].Config.Cwd
		if len(executedCommand) != 2 || executedCommand[0] != "run-eval" || executedCommand[1] != "--all" {
			t.Errorf("Expected executed command [run-eval --all], got %v", executedCommand)
		}
		if executedCwd != "repo" {
			t.Errorf("Expected working directory 'repo', got %q", executedCwd)
		}

		// Turn 3: poll async task completion and finalize results
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 3 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("get run error: %v", err)
		}

		if len(updatedRun.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task status, got %d", len(updatedRun.Status.Tasks))
		}
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseCompleted {
			t.Errorf("Expected task Completed, got %s (msg: %s)", task.Phase, task.Message)
		}
		if task.ExecutionID != "" {
			t.Errorf("Expected ExecutionID cleared on completion, got %q", task.ExecutionID)
		}
		if task.Samples != 1 {
			t.Errorf("Expected 1 sample, got %d", task.Samples)
		}
		if task.Successes != 1 {
			t.Errorf("Expected 1 success, got %d", task.Successes)
		}
		if task.SamplesDone != 1 {
			t.Errorf("Expected samplesDone 1, got %d", task.SamplesDone)
		}
		if task.MeanScore == nil || *task.MeanScore != 1.0 {
			t.Errorf("Expected meanScore 1.0, got %v", task.MeanScore)
		}
		if task.SuccessRate == nil || *task.SuccessRate != 1.0 {
			t.Errorf("Expected successRate 1.0, got %v", task.SuccessRate)
		}
		if task.LogPath == "" {
			t.Errorf("Expected non-empty logPath")
		}

		// Turn 4: marks Run as Completed
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 4 error: %v", err)
		}
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("get run error: %v", err)
		}
		if updatedRun.Status.Phase != v1alpha1.RunPhaseCompleted {
			t.Errorf("Expected Run phase Completed, got %s", updatedRun.Status.Phase)
		}
	})

	t.Run("NonZeroExitFails", func(t *testing.T) {
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:        "task-fail",
					Spec:      "eval.py@fail",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-failing-eval"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-sandbox-fail",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "commit1"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
			},
		}

		fakeClient := &sandboxclient.FakeClient{
			PollAsyncFn: func(ctx context.Context, executionID string) (*sandboxclient.AsyncTaskStatus, error) {
				return &sandboxclient.AsyncTaskStatus{State: sandboxclient.AsyncTaskCompleted, ExitCode: 42, Stderr: "fatal: test suite crashed"}, nil
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
			SandboxClient:    fakeClient,
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-sandbox-fail"}}

		// Turn 1: create sandbox
		_, _ = reconciler.Reconcile(t.Context(), req)
		var sbList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbList.Items[0].Name,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.51",
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		_ = cl.Create(t.Context(), pod)

		// Turn 2: start async task
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}
		// Turn 3: poll non-zero exit
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 3 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("Expected task Failed, got %s", task.Phase)
		}
		if !strings.Contains(task.Message, "exit code 42") || !strings.Contains(task.Message, "fatal: test suite crashed") {
			t.Errorf("Expected failure message with exit code and stderr tail, got %q", task.Message)
		}
	})

	t.Run("MissingResultsFileFails", func(t *testing.T) {
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:        "task-missing-results",
					Spec:      "eval.py@missing",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-eval"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-missing-results",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "commit1"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
			},
		}

		fakeClient := &sandboxclient.FakeClient{
			PollAsyncFn: func(ctx context.Context, executionID string) (*sandboxclient.AsyncTaskStatus, error) {
				return &sandboxclient.AsyncTaskStatus{State: sandboxclient.AsyncTaskCompleted, ExitCode: 0}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				return nil, fmt.Errorf("file %q not found (HTTP 404)", path)
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
			SandboxClient:    fakeClient,
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-missing-results"}}

		_, _ = reconciler.Reconcile(t.Context(), req)
		var sbList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbList.Items[0].Name,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.52",
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		_ = cl.Create(t.Context(), pod)

		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 3 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("Expected task Failed, got %s", task.Phase)
		}
		if !strings.Contains(task.Message, "failed to read results file") || !strings.Contains(task.Message, "not found") {
			t.Errorf("Expected failed to read results file message, got %q", task.Message)
		}
	})

	t.Run("WrongSchemaVersionFails", func(t *testing.T) {
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:        "task-wrong-schema",
					Spec:      "eval.py@wrong",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-eval"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-wrong-schema",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "commit1"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
			},
		}

		fakeClient := &sandboxclient.FakeClient{
			PollAsyncFn: func(ctx context.Context, executionID string) (*sandboxclient.AsyncTaskStatus, error) {
				return &sandboxclient.AsyncTaskStatus{State: sandboxclient.AsyncTaskCompleted, ExitCode: 0}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				return []byte(`{"schemaVersion": 99, "samples": [{"id": "s1", "passed": true}]}`), nil
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
			SandboxClient:    fakeClient,
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-wrong-schema"}}

		_, _ = reconciler.Reconcile(t.Context(), req)
		var sbList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbList.Items[0].Name,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.53",
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		_ = cl.Create(t.Context(), pod)

		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 3 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("Expected task Failed, got %s", task.Phase)
		}
		if !strings.Contains(task.Message, "failed to parse results file") || !strings.Contains(task.Message, "unsupported results schemaVersion 99") {
			t.Errorf("Expected unsupported schemaVersion message, got %q", task.Message)
		}
	})

	t.Run("FailedCheckoutFailsTask", func(t *testing.T) {
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:        "task-failed-checkout",
					Spec:      "eval.py@checkout",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-eval"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-sandbox-failed-checkout",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "badcommit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
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

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-sandbox-failed-checkout"}}

		// Turn 1: create sandbox
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 1 error: %v", err)
		}

		var sbList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
		if len(sbList.Items) != 1 {
			t.Fatalf("Expected 1 sandbox created, got %d", len(sbList.Items))
		}

		// Create pod for sandbox with failed checkout init container
		expectedFailureMessage := "step git checkout failed with exit code 1: error: pathspec 'badcommit' did not match any file(s) known to git"
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbList.Items[0].Name,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodFailed,
				InitContainerStatuses: []corev1.ContainerStatus{
					{
						Name: discovery.SandboxdInitContainerName,
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								ExitCode: 0,
							},
						},
					},
					{
						Name: discovery.RepoCheckoutInitContainerName,
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								ExitCode: 1,
								Message:  expectedFailureMessage,
							},
						},
					},
				},
			},
		}
		_ = cl.Create(t.Context(), pod)

		// Turn 2: reconcile discovers pod failure and marks task Failed
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		if len(updatedRun.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task status, got %d", len(updatedRun.Status.Tasks))
		}
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("Expected task Failed, got %s", task.Phase)
		}
		if !strings.Contains(task.Message, "step git checkout failed") {
			t.Errorf("Expected task message to contain 'step git checkout failed', got %q", task.Message)
		}
		if task.Message != expectedFailureMessage {
			t.Errorf("Expected exact failure message %q, got %q", expectedFailureMessage, task.Message)
		}

		// Verify task sandbox was cleaned up
		var postSBList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &postSBList, client.InNamespace("eval-runner-sandbox"))
		if len(postSBList.Items) != 0 {
			t.Errorf("Expected sandbox to be deleted on failure, found %d", len(postSBList.Items))
		}
	})

	t.Run("FailedCloneFailsTask", func(t *testing.T) {
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:        "task-failed-clone",
					Spec:      "eval.py@clone",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-eval"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-sandbox-failed-clone",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://invalid-repo", Commit: "commit1"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
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

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-sandbox-failed-clone"}}

		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 1 error: %v", err)
		}

		var sbList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))

		expectedFailureMessage := "step git clone failed with exit code 128: fatal: repository 'git://invalid-repo' not found"
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbList.Items[0].Name,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodFailed,
				InitContainerStatuses: []corev1.ContainerStatus{
					{
						Name: discovery.RepoCheckoutInitContainerName,
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								ExitCode: 128,
								Message:  expectedFailureMessage,
							},
						},
					},
				},
			},
		}
		_ = cl.Create(t.Context(), pod)

		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("Expected task Failed, got %s", task.Phase)
		}
		if !strings.Contains(task.Message, "step git clone failed") {
			t.Errorf("Expected task message to contain 'step git clone failed', got %q", task.Message)
		}
		if task.Message != expectedFailureMessage {
			t.Errorf("Expected exact failure message %q, got %q", expectedFailureMessage, task.Message)
		}
	})

	t.Run("CommandTimeoutSeconds_Explicit", func(t *testing.T) {
		timeoutSeconds := 600
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:             "task-explicit-timeout",
					Spec:           "eval.py@timeout",
					Image:          "custom-task-image:v1",
					Command:        []string{"run-eval"},
					Resources:      []v1alpha1.ManifestResource{{Type: "sandbox"}},
					TimeoutSeconds: &timeoutSeconds,
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-explicit-timeout",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "commit1"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
			},
		}

		fakeClient := &sandboxclient.FakeClient{
			PollAsyncFn: func(ctx context.Context, executionID string) (*sandboxclient.AsyncTaskStatus, error) {
				return &sandboxclient.AsyncTaskStatus{State: sandboxclient.AsyncTaskCompleted, ExitCode: 0, Stdout: "task finished"}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				if path == "/out/results.json" {
					return []byte(`{"schemaVersion": 1, "samples": [{"id": "s1", "passed": true}]}`), nil
				}
				return nil, fmt.Errorf("file %q not found", path)
			},
		}

		sink := artifacts.NewInMemorySink()
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(run).
			WithStatusSubresource(run).
			Build()

		reconciler := &RunReconciler{
			Client:           cl,
			Scheme:           scheme,
			SandboxNamespace: "eval-runner-sandbox",
			SandboxClient:    fakeClient,
			ArtifactSink:     sink,
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-explicit-timeout"}}

		// Turn 1: create sandbox
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 1 error: %v", err)
		}

		var sbList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbList.Items[0].Name,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.60",
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		_ = cl.Create(t.Context(), pod)

		// Turn 2: start async task
		if _, err = reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}
		// Turn 3: poll async completion
		if _, err = reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 3 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseCompleted {
			t.Errorf("Expected task Completed, got %s (msg: %s)", task.Phase, task.Message)
		}
		if task.TimeoutSeconds != 600 {
			t.Errorf("Expected task.TimeoutSeconds == 600, got %d", task.TimeoutSeconds)
		}
	})

	t.Run("CommandTimeoutSeconds_DefaultUnset", func(t *testing.T) {
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:        "task-default-timeout",
					Spec:      "eval.py@default",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-eval"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-default-timeout",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "commit1"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
			},
		}

		fakeClient := &sandboxclient.FakeClient{
			PollAsyncFn: func(ctx context.Context, executionID string) (*sandboxclient.AsyncTaskStatus, error) {
				return &sandboxclient.AsyncTaskStatus{State: sandboxclient.AsyncTaskCompleted, ExitCode: 0, Stdout: "task finished"}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				if path == "/out/results.json" {
					return []byte(`{"schemaVersion": 1, "samples": [{"id": "s1", "passed": true}]}`), nil
				}
				return nil, fmt.Errorf("file %q not found", path)
			},
		}

		sink := artifacts.NewInMemorySink()
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(run).
			WithStatusSubresource(run).
			Build()

		reconciler := &RunReconciler{
			Client:               cl,
			Scheme:               scheme,
			SandboxNamespace:     "eval-runner-sandbox",
			SandboxClient:        fakeClient,
			ArtifactSink:         sink,
			ExecutionStepTimeout: 2 * time.Minute,
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-default-timeout"}}

		// Turn 1: create sandbox
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 1 error: %v", err)
		}

		var sbList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbList.Items[0].Name,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.61",
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		_ = cl.Create(t.Context(), pod)

		// Turn 2: start async task
		if _, err = reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}
		// Turn 3: poll async completion
		if _, err = reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 3 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseCompleted {
			t.Errorf("Expected task Completed, got %s", task.Phase)
		}
		if task.TimeoutSeconds != 120 {
			t.Errorf("Expected task.TimeoutSeconds == 120, got %d", task.TimeoutSeconds)
		}
	})

	t.Run("CommandTimeoutSeconds_ExecutionDeadlineWins", func(t *testing.T) {
		timeoutSeconds := 600
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:             "task-capped-timeout",
					Spec:           "eval.py@timeout",
					Image:          "custom-task-image:v1",
					Command:        []string{"run-eval"},
					Resources:      []v1alpha1.ManifestResource{{Type: "sandbox"}},
					TimeoutSeconds: &timeoutSeconds,
				},
			},
		}

		fixedNow := time.Now().Truncate(time.Second)
		deadlineTime := metav1.NewTime(fixedNow.Add(300 * time.Second))
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-capped-timeout",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "commit1"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase:             v1alpha1.RunPhaseRunning,
				ExecutionDeadline: &deadlineTime,
			},
		}

		fakeClient := &sandboxclient.FakeClient{
			PollAsyncFn: func(ctx context.Context, executionID string) (*sandboxclient.AsyncTaskStatus, error) {
				return &sandboxclient.AsyncTaskStatus{State: sandboxclient.AsyncTaskCompleted, ExitCode: 0, Stdout: "task finished"}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				if path == "/out/results.json" {
					return []byte(`{"schemaVersion": 1, "samples": [{"id": "s1", "passed": true}]}`), nil
				}
				return nil, fmt.Errorf("file %q not found", path)
			},
		}

		sink := artifacts.NewInMemorySink()
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(run).
			WithStatusSubresource(run).
			Build()

		reconciler := &RunReconciler{
			Client:           cl,
			Scheme:           scheme,
			SandboxNamespace: "eval-runner-sandbox",
			SandboxClient:    fakeClient,
			ArtifactSink:     sink,
			Now:              func() time.Time { return fixedNow },
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-capped-timeout"}}

		// Turn 1: create sandbox
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 1 error: %v", err)
		}

		var sbList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbList.Items[0].Name,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.62",
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		_ = cl.Create(t.Context(), pod)

		// Turn 2: start async task
		if _, err = reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}
		// Turn 3: poll async completion
		if _, err = reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 3 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseCompleted {
			t.Errorf("Expected task Completed, got %s", task.Phase)
		}
		if task.TimeoutSeconds != 300 {
			t.Errorf("Expected task.TimeoutSeconds == 300, got %d", task.TimeoutSeconds)
		}
	})

	t.Run("CommandTimeoutSeconds_OutOfRangeFailsTask", func(t *testing.T) {
		timeoutZero := 0
		timeoutExcessive := 9000
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:             "task-zero-timeout",
					Spec:           "eval.py@zero",
					Image:          "custom-task-image:v1",
					Command:        []string{"run-eval"},
					Resources:      []v1alpha1.ManifestResource{{Type: "sandbox"}},
					TimeoutSeconds: &timeoutZero,
				},
				{
					ID:             "task-excessive-timeout",
					Spec:           "eval.py@excessive",
					Image:          "custom-task-image:v1",
					Command:        []string{"run-eval"},
					Resources:      []v1alpha1.ManifestResource{{Type: "sandbox"}},
					TimeoutSeconds: &timeoutExcessive,
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-out-of-range-timeout",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "commit1"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
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

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-out-of-range-timeout"}}

		// Turn 1: initialize tasks and reconcile
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 1 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		if len(updatedRun.Status.Tasks) != 2 {
			t.Fatalf("Expected 2 tasks, got %d", len(updatedRun.Status.Tasks))
		}

		taskZero := updatedRun.Status.Tasks[0]
		if taskZero.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("Expected taskZero Phase to be Failed, got %s", taskZero.Phase)
		}
		expectedZeroMessage := "invalid timeoutSeconds 0: must be between 1 and 7200"
		if taskZero.Message != expectedZeroMessage {
			t.Errorf("Expected taskZero message %q, got %q", expectedZeroMessage, taskZero.Message)
		}

		taskExcessive := updatedRun.Status.Tasks[1]
		if taskExcessive.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("Expected taskExcessive Phase to be Failed, got %s", taskExcessive.Phase)
		}
		expectedExcessiveMessage := "invalid timeoutSeconds 9000: must be between 1 and 7200"
		if taskExcessive.Message != expectedExcessiveMessage {
			t.Errorf("Expected taskExcessive message %q, got %q", expectedExcessiveMessage, taskExcessive.Message)
		}
	})

	t.Run("CommandTimeout_FailureMessageShape", func(t *testing.T) {
		timeoutSeconds := 5
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:             "task-timed-out",
					Spec:           "eval.py@timeout",
					Image:          "custom-task-image:v1",
					Command:        []string{"run-eval"},
					Resources:      []v1alpha1.ManifestResource{{Type: "sandbox"}},
					TimeoutSeconds: &timeoutSeconds,
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-timed-out-command",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "commit1"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
			},
		}

		currentTime := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
		fakeClient := &sandboxclient.FakeClient{}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(run).
			WithStatusSubresource(run).
			Build()

		reconciler := &RunReconciler{
			Client:           cl,
			Scheme:           scheme,
			SandboxNamespace: "eval-runner-sandbox",
			SandboxClient:    fakeClient,
			Now:              func() time.Time { return currentTime },
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-timed-out-command"}}

		// Turn 1: create sandbox
		_, _ = reconciler.Reconcile(t.Context(), req)
		var sbList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbList.Items[0].Name,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.63",
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		_ = cl.Create(t.Context(), pod)

		// Turn 2: start async task
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}

		// Turn 3: advance past task timeout and reconcile
		currentTime = currentTime.Add(6 * time.Second)
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 3 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("Expected task Failed, got %s", task.Phase)
		}
		expectedTimeoutMessage := "step task command failed: sandbox did not report completion"
		if task.Message != expectedTimeoutMessage {
			t.Errorf("Expected task message %q, got %q", expectedTimeoutMessage, task.Message)
		}
	})

	t.Run("TaskResourceLimits_ManifestLimitsAppliedToSandboxAndStatus", func(t *testing.T) {
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:        "task-custom-limits",
					Spec:      "eval.py@custom",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-eval"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}}},
				},
				{
					ID:        "task-default-limits",
					Spec:      "eval.py@default",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-eval"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-resource-limits",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "commit1"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
			},
		}

		fakeClient := &sandboxclient.FakeClient{
			PollAsyncFn: func(ctx context.Context, executionID string) (*sandboxclient.AsyncTaskStatus, error) {
				return &sandboxclient.AsyncTaskStatus{State: sandboxclient.AsyncTaskCompleted, ExitCode: 0, Stdout: "task finished"}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				if path == "/out/results.json" {
					return []byte(`{"schemaVersion": 1, "samples": [{"id": "s1", "passed": true}]}`), nil
				}
				return nil, fmt.Errorf("file %q not found", path)
			},
		}

		sink := artifacts.NewInMemorySink()
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(run).
			WithStatusSubresource(run).
			Build()

		reconciler := &RunReconciler{
			Client:           cl,
			Scheme:           scheme,
			SandboxNamespace: "eval-runner-sandbox",
			SandboxClient:    fakeClient,
			ArtifactSink:     sink,
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-resource-limits"}}

		// Turn 1: Sandboxes created and tasks marked Running
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("turn 1 error: %v", err)
		}

		var sandboxList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sandboxList, client.InNamespace("eval-runner-sandbox"))
		if len(sandboxList.Items) != 2 {
			t.Fatalf("Expected 2 sandboxes, got %d", len(sandboxList.Items))
		}

		expectedCustomCPULimit := resource.MustParse("2")
		expectedCustomMemoryLimit := resource.MustParse("4Gi")
		expectedDefaultCPULimit := resource.MustParse("500m")
		expectedDefaultMemoryLimit := resource.MustParse("512Mi")

		for _, sandbox := range sandboxList.Items {
			taskID := sandbox.Annotations[discovery.AnnotationTaskID]
			container := sandbox.Spec.PodTemplate.Spec.Containers[0]
			switch taskID {
			case "task-custom-limits":
				if container.Resources.Limits.Cpu().Cmp(expectedCustomCPULimit) != 0 {
					t.Errorf("task-custom-limits sandbox CPU limit want %v, got %v", expectedCustomCPULimit, container.Resources.Limits.Cpu())
				}
				if container.Resources.Limits.Memory().Cmp(expectedCustomMemoryLimit) != 0 {
					t.Errorf("task-custom-limits sandbox Memory limit want %v, got %v", expectedCustomMemoryLimit, container.Resources.Limits.Memory())
				}
			case "task-default-limits":
				if container.Resources.Limits.Cpu().Cmp(expectedDefaultCPULimit) != 0 {
					t.Errorf("task-default-limits sandbox CPU limit want %v, got %v", expectedDefaultCPULimit, container.Resources.Limits.Cpu())
				}
				if container.Resources.Limits.Memory().Cmp(expectedDefaultMemoryLimit) != 0 {
					t.Errorf("task-default-limits sandbox Memory limit want %v, got %v", expectedDefaultMemoryLimit, container.Resources.Limits.Memory())
				}
			default:
				t.Errorf("Unexpected sandbox task ID: %s", taskID)
			}
		}

		var midRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &midRun)
		for _, task := range midRun.Status.Tasks {
			if task.Resources == nil || task.Resources.Limits == nil {
				t.Fatalf("midRun task %s missing Resources.Limits", task.ID)
			}
			cpuLimit := task.Resources.Limits[corev1.ResourceCPU]
			memoryLimit := task.Resources.Limits[corev1.ResourceMemory]
			switch task.ID {
			case "task-custom-limits":
				if cpuLimit.String() != "2" || memoryLimit.String() != "4Gi" {
					t.Errorf("midRun task-custom-limits status want cpuLimit=2, memoryLimit=4Gi; got %q, %q", cpuLimit.String(), memoryLimit.String())
				}
			case "task-default-limits":
				if cpuLimit.String() != "500m" || memoryLimit.String() != "512Mi" {
					t.Errorf("midRun task-default-limits status want cpuLimit=500m, memoryLimit=512Mi; got %q, %q", cpuLimit.String(), memoryLimit.String())
				}
			}
		}

		// Create mock ready pods for both sandboxes
		for i, sandbox := range sandboxList.Items {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      sandbox.Name,
					Namespace: "eval-runner-sandbox",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					PodIP: fmt.Sprintf("10.244.0.7%d", i),
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					},
				},
			}
			_ = cl.Create(t.Context(), pod)
		}

		// Turn 2: Start async tasks
		if _, err = reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}
		// Turn 3: Poll async tasks to completion
		if _, err = reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 3 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		for _, task := range updatedRun.Status.Tasks {
			if task.Phase != v1alpha1.TaskPhaseCompleted {
				t.Errorf("task %s expected Completed, got %s (msg: %s)", task.ID, task.Phase, task.Message)
			}
			if task.Resources == nil || task.Resources.Limits == nil {
				t.Fatalf("completed task %s missing Resources.Limits", task.ID)
			}
			cpuLimit := task.Resources.Limits[corev1.ResourceCPU]
			memoryLimit := task.Resources.Limits[corev1.ResourceMemory]
			switch task.ID {
			case "task-custom-limits":
				if cpuLimit.String() != "2" || memoryLimit.String() != "4Gi" {
					t.Errorf("completed task-custom-limits status want cpuLimit=2, memoryLimit=4Gi; got %q, %q", cpuLimit.String(), memoryLimit.String())
				}
			case "task-default-limits":
				if cpuLimit.String() != "500m" || memoryLimit.String() != "512Mi" {
					t.Errorf("completed task-default-limits status want cpuLimit=500m, memoryLimit=512Mi; got %q, %q", cpuLimit.String(), memoryLimit.String())
				}
			}
		}
	})

	t.Run("TaskResourceLimits_InvalidResourcesFailsTask", func(t *testing.T) {
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:        "task-over-cpu",
					Spec:      "eval.py@overcpu",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-eval"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32")}}},
				},
				{
					ID:        "task-over-mem",
					Spec:      "eval.py@overmem",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-eval"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Gi")}}},
				},
				{
					ID:        "task-zero-cpu",
					Spec:      "eval.py@zerocpu",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-eval"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}}},
				},
				{
					ID:        "task-valid",
					Spec:      "eval.py@valid",
					Image:     "custom-task-image:v1",
					Command:   []string{"run-eval"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2G")}}},
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-invalid-resources",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "commit1"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
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

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-invalid-resources"}}

		_, _ = reconciler.Reconcile(t.Context(), req)

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		if len(updatedRun.Status.Tasks) != 4 {
			t.Fatalf("Expected 4 tasks, got %d", len(updatedRun.Status.Tasks))
		}

		if updatedRun.Status.Phase != v1alpha1.RunPhaseRunning {
			t.Errorf("Expected Run phase to remain %s while valid tasks continue, got %s", v1alpha1.RunPhaseRunning, updatedRun.Status.Phase)
		}

		var foundValid bool
		for _, task := range updatedRun.Status.Tasks {
			if task.ID == "task-valid" {
				foundValid = true
				if task.Phase == v1alpha1.TaskPhaseFailed {
					t.Errorf("task-valid should not have Failed, got phase %s", task.Phase)
				}
				continue
			}

			if task.Phase != v1alpha1.TaskPhaseFailed {
				t.Errorf("task %s Phase expected Failed, got %s", task.ID, task.Phase)
			}
			switch task.ID {
			case "task-over-cpu":
				if !strings.Contains(task.Message, "task-over-cpu") || !strings.Contains(task.Message, "cpu") || !strings.Contains(task.Message, "32") {
					t.Errorf("task-over-cpu message %q should name task, field and value", task.Message)
				}
			case "task-over-mem":
				if !strings.Contains(task.Message, "task-over-mem") || !strings.Contains(task.Message, "memory") || !strings.Contains(task.Message, "128Gi") {
					t.Errorf("task-over-mem message %q should name task, field and value", task.Message)
				}
			case "task-zero-cpu":
				if !strings.Contains(task.Message, "task-zero-cpu") || !strings.Contains(task.Message, "cpu") || !strings.Contains(task.Message, "0") {
					t.Errorf("task-zero-cpu message %q should name task, field and value", task.Message)
				}
			}
		}
		if !foundValid {
			t.Errorf("Expected task-valid to be present in updatedRun.Status.Tasks")
		}
	})

	t.Run("TaskAgent_ReconcileValidAndInvalidTasks", func(t *testing.T) {
		timeout := 900
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{
					ID:        "task-invalid-agent",
					Spec:      "eval.py@invalid",
					Image:     "task-image:v1",
					Command:   []string{"verify.sh"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
					Agent:     &v1alpha1.ManifestTaskAgent{InstructionPath: "relative.md"},
				},
				{
					ID:        "task-valid-agent",
					Spec:      "eval.py@valid",
					Image:     "task-image:v1",
					Command:   []string{"verify.sh"},
					Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
					Agent: &v1alpha1.ManifestTaskAgent{
						PrepareCommand:  []string{"prepare.sh"},
						InstructionPath: "/workspace/out/inst.md",
						TimeoutSeconds:  &timeout,
					},
				},
			},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "test-run-agent", Namespace: "eval-runner"},
			Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{URL: "git://repo", Commit: "c1"}, Model: "mockllm/model"},
			Status:     v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseRunning},
		}

		var executedCommands [][]string
		fakeClient := &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				if len(cfg.Command) > 0 && cfg.Command[0] == "mkdir" {
					return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
				}
				executedCommands = append(executedCommands, cfg.Command)
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				if path == "/out/results.json" {
					return []byte(`{"schemaVersion": 1, "samples": [{"id": "s1", "passed": true, "score": 1.0}]}`), nil
				}
				if path == "/out/inst.md" || path == "out/inst.md" {
					return []byte("```scripted-bash\ntrue\n```"), nil
				}
				return nil, fmt.Errorf("file not found: %s", path)
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).WithStatusSubresource(run).Build()
		reconciler := &RunReconciler{
			Client: cl, Scheme: scheme, SandboxNamespace: "eval-runner-sandbox",
			SandboxClient: fakeClient, ArtifactSink: artifacts.NewInMemorySink(),
			AgentModel: agent.NewScriptedModel(),
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)
		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}

		// Turn 1: initialize tasks - invalid task ends Failed, valid task is Pending and creates sandbox
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 1 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		if updatedRun.Status.Phase != v1alpha1.RunPhaseRunning {
			t.Errorf("expected Run to remain Running, got %s", updatedRun.Status.Phase)
		}
		if len(updatedRun.Status.Tasks) != 2 {
			t.Fatalf("expected 2 tasks, got %d", len(updatedRun.Status.Tasks))
		}
		if t0 := updatedRun.Status.Tasks[0]; t0.Phase != v1alpha1.TaskPhaseFailed || !strings.Contains(t0.Message, "invalid agent.instructionPath") {
			t.Errorf("expected task-invalid-agent Failed, got phase=%s msg=%q", t0.Phase, t0.Message)
		}
		if t1 := updatedRun.Status.Tasks[1]; t1.Phase != v1alpha1.TaskPhaseRunning {
			t.Errorf("expected task-valid-agent Running, got %s", t1.Phase)
		}

		// Create Pod for valid task sandbox
		var sbList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
		if len(sbList.Items) != 1 {
			t.Fatalf("expected 1 sandbox created, got %d", len(sbList.Items))
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: sbList.Items[0].Name, Namespace: "eval-runner-sandbox"},
			Status: corev1.PodStatus{
				Phase:      corev1.PodRunning,
				PodIP:      "10.244.0.51",
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		}
		if err := cl.Create(t.Context(), pod); err != nil {
			t.Fatalf("failed to create pod: %v", err)
		}

		// Turn 2: execute valid task - runs prepareCommand, agent loop in sandbox, then verify command
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}

		if len(executedCommands) != 3 ||
			!reflect.DeepEqual(executedCommands[0], []string{"prepare.sh"}) ||
			!reflect.DeepEqual(executedCommands[1], []string{"bash", "-c", "true"}) ||
			!reflect.DeepEqual(executedCommands[2], []string{"verify.sh"}) {
			t.Errorf("expected prepareCommand, agent bash command, and verify command to execute, got %v", executedCommands)
		}

		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		if t1 := updatedRun.Status.Tasks[1]; t1.Phase != v1alpha1.TaskPhaseCompleted {
			t.Errorf("expected task-valid-agent Completed, got %s", t1.Phase)
		}
	})

	t.Run("TaskAgent_GeminiClient_SuccessWithTokensAndCost", func(t *testing.T) {
		timeout := 900
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{{
				ID:        "task-gemini-agent",
				Spec:      "eval.py@valid",
				Image:     "task-image:v1",
				Command:   []string{"verify.sh"},
				Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				Agent: &v1alpha1.ManifestTaskAgent{
					PrepareCommand:  []string{"prepare.sh"},
					InstructionPath: "/workspace/out/inst.md",
					TimeoutSeconds:  &timeout,
				},
			}},
		}

		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "test-run-gemini-agent", Namespace: "eval-runner"},
			Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{URL: "git://repo", Commit: "c1"}, Model: "google/gemini-2.5-flash"},
			Status:     v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseRunning},
		}

		var geminiCalls int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			geminiCalls++
			w.WriteHeader(http.StatusOK)
			if geminiCalls == 1 {
				_, _ = w.Write([]byte(`{"candidates":[{"content":{"parts":[{"functionCall":{"name":"bash","args":{"command":"echo solved"}}}]}}],"usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":20}}`))
				return
			}
			_, _ = w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"Done"}]}}],"usageMetadata":{"promptTokenCount":150,"candidatesTokenCount":30}}`))
		}))
		defer server.Close()

		var executedCommands [][]string
		fakeClient := &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				if len(cfg.Command) > 0 && cfg.Command[0] == "mkdir" {
					return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
				}
				executedCommands = append(executedCommands, cfg.Command)
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "echo output"}, nil
			},
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				if path == "/out/results.json" {
					return []byte(`{"schemaVersion": 1, "samples": [{"id": "s1", "passed": true, "score": 1.0}]}`), nil
				}
				if path == "/out/inst.md" || path == "out/inst.md" {
					return []byte("instruction prompt"), nil
				}
				return nil, fmt.Errorf("file not found: %s", path)
			},
		}

		sink := artifacts.NewInMemorySink()
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).WithStatusSubresource(run).Build()
		reconciler := &RunReconciler{
			Client: cl, Scheme: scheme, SandboxNamespace: "eval-runner-sandbox",
			SandboxClient: fakeClient,
			ArtifactSink:  sink,
			NewAgentModel: func(r *v1alpha1.Run) (agent.Model, error) {
				return gemini.New(gemini.Options{
					BaseURL:    server.URL,
					Model:      r.Spec.Model,
					APIKey:     "test-secret-key",
					HTTPClient: server.Client(),
				})
			},
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)
		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}

		// Turn 1: create sandbox pod
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 1 error: %v", err)
		}

		var sbList sandboxv1beta1.SandboxList
		_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
		if len(sbList.Items) != 1 {
			t.Fatalf("expected 1 sandbox created, got %d", len(sbList.Items))
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: sbList.Items[0].Name, Namespace: "eval-runner-sandbox"},
			Status: corev1.PodStatus{
				Phase:      corev1.PodRunning,
				PodIP:      "10.244.0.52",
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		}
		if err := cl.Create(t.Context(), pod); err != nil {
			t.Fatalf("failed to create pod: %v", err)
		}

		// Turn 2: execute agent task with Gemini client
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}

		if geminiCalls != 2 {
			t.Errorf("expected 2 calls to Gemini server, got %d", geminiCalls)
		}

		if len(executedCommands) != 3 ||
			!reflect.DeepEqual(executedCommands[0], []string{"prepare.sh"}) ||
			!reflect.DeepEqual(executedCommands[1], []string{"bash", "-c", "echo solved"}) ||
			!reflect.DeepEqual(executedCommands[2], []string{"verify.sh"}) {
			t.Errorf("expected prepareCommand, agent bash command, and verify command to execute, got %v", executedCommands)
		}

		var updatedRun v1alpha1.Run
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		if len(updatedRun.Status.Tasks) != 1 {
			t.Fatalf("expected 1 task in status, got %d", len(updatedRun.Status.Tasks))
		}
		task0 := updatedRun.Status.Tasks[0]
		if task0.Phase != v1alpha1.TaskPhaseCompleted {
			t.Fatalf("expected task Completed, got %s (msg: %s)", task0.Phase, task0.Message)
		}
		if task0.Tokens == nil {
			t.Fatal("expected task tokens to be recorded, got nil")
		}
		if task0.Tokens.Input != 250 || task0.Tokens.Output != 50 || task0.Tokens.Total != 300 {
			t.Errorf("expected tokens Input=250, Output=50, Total=300, got %+v", task0.Tokens)
		}
		if task0.CostUSD == nil || *task0.CostUSD <= 0 {
			t.Errorf("expected task CostUSD > 0, got %v", task0.CostUSD)
		}
		if updatedRun.Status.CostUSD == nil || *updatedRun.Status.CostUSD != *task0.CostUSD {
			t.Errorf("expected run CostUSD to match task CostUSD %v, got %v", task0.CostUSD, updatedRun.Status.CostUSD)
		}

		// Verify artifact metadata has tokens and cost
		summary, err := artifacts.ReadTaskSummary(t.Context(), sink, task0.LogPath)
		if err != nil {
			t.Fatalf("failed to read task summary from sink: %v", err)
		}
		if summary.Tokens == nil || summary.Tokens.Total != 300 {
			t.Errorf("expected summary Tokens.Total=300, got %+v", summary.Tokens)
		}
		if summary.CostUSD == nil || *summary.CostUSD <= 0 {
			t.Errorf("expected summary CostUSD > 0, got %v", summary.CostUSD)
		}

		// Verify agent loop record artifact is stored next to results.json
		agentURI := strings.Replace(task0.LogPath, "results.json", "agent.json", 1)
		agentBytes, err := sink.Load(t.Context(), agentURI)
		if err != nil {
			t.Fatalf("failed to load agent artifact from sink at %s: %v", agentURI, err)
		}
		var agentRecord agent.Result
		if err := json.Unmarshal(agentBytes, &agentRecord); err != nil {
			t.Fatalf("failed to unmarshal agent artifact: %v", err)
		}
		if agentRecord.StopReason != agent.StopReasonCompleted {
			t.Errorf("expected agent StopReason Completed, got %s", agentRecord.StopReason)
		}
		if agentRecord.InputTokens != 250 || agentRecord.OutputTokens != 50 {
			t.Errorf("expected agent tokens 250 in / 50 out, got %d in / %d out", agentRecord.InputTokens, agentRecord.OutputTokens)
		}
		if len(agentRecord.Steps) != 2 {
			t.Fatalf("expected 2 agent steps, got %d", len(agentRecord.Steps))
		}
		if len(agentRecord.Steps[0].ToolCalls) != 1 || agentRecord.Steps[0].ToolCalls[0].Command != "echo solved" {
			t.Errorf("expected step 0 tool call 'echo solved', got %+v", agentRecord.Steps[0].ToolCalls)
		}
		if len(agentRecord.Steps[0].ToolResults) != 1 || agentRecord.Steps[0].ToolResults[0].Output != "echo output" {
			t.Errorf("expected step 0 tool result 'echo output', got %+v", agentRecord.Steps[0].ToolResults)
		}
		if agentRecord.Steps[1].Text != "Done" {
			t.Errorf("expected step 1 text 'Done', got %q", agentRecord.Steps[1].Text)
		}

		// Verify stored results.json is enriched with agent trajectory and usage
		storedResultsBytes, err := sink.Load(t.Context(), task0.LogPath)
		if err != nil {
			t.Fatalf("failed to load results artifact from sink at %s: %v", task0.LogPath, err)
		}
		parsedSamples, err := artifacts.ParseResultsSamples(storedResultsBytes)
		if err != nil {
			t.Fatalf("failed to parse enriched results.json: %v", err)
		}
		if len(parsedSamples) != 1 {
			t.Fatalf("expected 1 parsed sample, got %d", len(parsedSamples))
		}
		parsedSample := parsedSamples[0]
		if parsedSample.Input != "instruction prompt" {
			t.Errorf("expected enriched Input %q, got %q", "instruction prompt", parsedSample.Input)
		}
		if parsedSample.Output != "Done" {
			t.Errorf("expected enriched Output %q, got %q", "Done", parsedSample.Output)
		}
		if parsedSample.Latency == nil || *parsedSample.Latency < 0 {
			t.Errorf("expected non-nil non-negative Latency, got %v", parsedSample.Latency)
		}
		if parsedSample.InputTokens != 250 || parsedSample.OutputTokens != 50 || parsedSample.TotalTokens != 300 {
			t.Errorf("expected enriched tokens 250 in / 50 out / 300 total, got %d / %d / %d", parsedSample.InputTokens, parsedSample.OutputTokens, parsedSample.TotalTokens)
		}
		expectedTranscript := []artifacts.InspectTranscriptRow{
			{Kind: "user", Content: "instruction prompt"},
			{Kind: "tool_call", Content: "echo solved"},
			{Kind: "tool", Content: "echo output"},
			{Kind: "assistant", Content: "Done"},
		}
		if !reflect.DeepEqual(parsedSample.Transcript, expectedTranscript) {
			t.Errorf("enriched transcript mismatch:\ngot:  %+v\nwant: %+v", parsedSample.Transcript, expectedTranscript)
		}
	})

	t.Run("TaskAgent_InvalidOrMissingModelCredentials_RejectsFast", func(t *testing.T) {
		testCases := []struct {
			name        string
			model       string
			apiKey      string
			expectedMsg string
		}{
			{
				name:        "GeminiMissingKey",
				model:       "google/gemini-2.5-flash",
				apiKey:      "",
				expectedMsg: "controller has no model credential",
			},
			{
				name:        "UnsupportedModel",
				model:       "openai/gpt-4o",
				apiKey:      "key-present",
				expectedMsg: "only Gemini models are supported",
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				timeout := 900
				manifest := &v1alpha1.Manifest{
					Tasks: []v1alpha1.ManifestTask{{
						ID:        "agent-task",
						Spec:      "eval.py@valid",
						Image:     "task-image:v1",
						Command:   []string{"verify.sh"},
						Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
						Agent:     &v1alpha1.ManifestTaskAgent{InstructionPath: "/workspace/out/inst.md", TimeoutSeconds: &timeout},
					}},
				}
				run := &v1alpha1.Run{
					ObjectMeta: metav1.ObjectMeta{Name: "test-run-" + strings.ToLower(testCase.name), Namespace: "eval-runner"},
					Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{URL: "git://repo", Commit: "c1"}, Model: testCase.model},
					Status:     v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseRunning},
				}
				cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).WithStatusSubresource(run).Build()
				reconciler := &RunReconciler{
					Client:        cl,
					Scheme:        scheme,
					NewAgentModel: DefaultAgentModelConstructor(testCase.apiKey),
				}
				reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)
				req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}

				if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
					t.Fatalf("reconcile error: %v", err)
				}
				var sbList sandboxv1beta1.SandboxList
				_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
				if len(sbList.Items) != 0 {
					t.Errorf("expected 0 sandboxes created, got %d", len(sbList.Items))
				}
				var updatedRun v1alpha1.Run
				_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
				if len(updatedRun.Status.Tasks) != 1 || updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseFailed {
					t.Fatalf("expected task Failed, got %+v", updatedRun.Status.Tasks)
				}
				if !strings.Contains(updatedRun.Status.Tasks[0].Message, testCase.expectedMsg) {
					t.Errorf("expected message to contain %q, got %q", testCase.expectedMsg, updatedRun.Status.Tasks[0].Message)
				}
			})
		}
	})
}

func TestExecuteAgentPhase(t *testing.T) {
	ctx := t.Context()
	task := v1alpha1.ManifestTask{
		ID:      "test-task",
		Command: []string{"verify.sh"},
		Agent: &v1alpha1.ManifestTaskAgent{
			InstructionPath: "/workspace/inst.md",
		},
	}

	t.Run("instruction outside workspace fails", func(t *testing.T) {
		reconciler := &RunReconciler{AgentModel: agent.NewScriptedModel()}
		run := &v1alpha1.Run{Spec: v1alpha1.RunSpec{Model: "mockllm/model"}}
		badTask := task
		badTask.Agent = &v1alpha1.ManifestTaskAgent{InstructionPath: "/etc/passwd"}
		_, failed := reconciler.executeAgentPhase(ctx, run, "task", nil, badTask, "repo", 60, nil, nil)
		if failed == nil || !strings.Contains(failed.message, "must be an absolute path inside /workspace") {
			t.Errorf("expected failure for instruction outside workspace, got %v", failed)
		}
	})

	t.Run("agent timeout proceeds to verify with message", func(t *testing.T) {
		timeout := 1
		timedOutTask := task
		timedOutTask.Agent = &v1alpha1.ManifestTaskAgent{
			InstructionPath: "/workspace/inst.md",
			TimeoutSeconds:  &timeout,
		}
		fakeClient := &sandboxclient.FakeClient{
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				return []byte("```scripted-bash\nslow\n```"), nil
			},
		}
		reconciler := &RunReconciler{
			AgentModel: &timingOutModel{},
		}
		run := &v1alpha1.Run{Spec: v1alpha1.RunSpec{Model: "mockllm/model"}}
		outcome, failed := reconciler.executeAgentPhase(ctx, run, "task", fakeClient, timedOutTask, "repo", 60, nil, nil)
		if failed != nil {
			t.Fatalf("expected timeout to proceed to verify, got failedResult: %v", failed)
		}
		if outcome.loopEndingMessage != "agent loop stopped: timeout" {
			t.Errorf("expected timeout loop message, got %q", outcome.loopEndingMessage)
		}
	})

	t.Run("captures instruction and duration using reconciler clock", func(t *testing.T) {
		instructionText := "Solve the task.\n```scripted-bash\necho hi\n```"
		fakeClient := &sandboxclient.FakeClient{
			ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
				return []byte(instructionText), nil
			},
			ExecFn: func(call int, execContext context.Context, config sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "hi\n"}, nil
			},
		}

		baseTime := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
		currentTime := baseTime
		agentModel := &clockAdvancingModel{
			delegate: agent.NewScriptedModel(),
			advance: func() {
				currentTime = currentTime.Add(3500 * time.Millisecond)
			},
		}
		reconciler := &RunReconciler{
			AgentModel: agentModel,
			Now: func() time.Time {
				return currentTime
			},
		}
		run := &v1alpha1.Run{Spec: v1alpha1.RunSpec{Model: "mockllm/model"}}
		outcome, failed := reconciler.executeAgentPhase(ctx, run, "task", fakeClient, task, "repo", 60, nil, nil)
		if failed != nil {
			t.Fatalf("expected success, got failedResult: %v", failed)
		}
		if outcome.instruction != instructionText {
			t.Errorf("expected instruction %q, got %q", instructionText, outcome.instruction)
		}
		expectedDuration := 7 * time.Second
		if outcome.duration != expectedDuration {
			t.Errorf("expected duration %v, got %v", expectedDuration, outcome.duration)
		}
		if outcome.result == nil {
			t.Fatal("expected non-nil agent result")
		}
	})

	t.Run("prepare failure reports prepare failure message and uses task timeout", func(t *testing.T) {
		taskTimeout := 500
		prepareTask := task
		prepareTask.TimeoutSeconds = &taskTimeout
		prepareTask.Agent = &v1alpha1.ManifestTaskAgent{
			PrepareCommand:  []string{"prepare.sh"},
			InstructionPath: "/workspace/inst.md",
		}

		var observedTimeout time.Duration
		fakeClient := &sandboxclient.FakeClient{
			ExecFn: func(call int, execContext context.Context, config sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				if deadline, ok := execContext.Deadline(); ok {
					observedTimeout = time.Until(deadline)
				}
				return nil, context.DeadlineExceeded
			},
		}

		reconciler := &RunReconciler{
			AgentModel: agent.NewScriptedModel(),
		}
		run := &v1alpha1.Run{Spec: v1alpha1.RunSpec{Model: "mockllm/model"}}
		_, failed := reconciler.executeAgentPhase(ctx, run, "task", fakeClient, prepareTask, "repo", 500, nil, nil)
		if failed == nil {
			t.Fatalf("expected prepare timeout to fail the task, got nil")
		}
		if !strings.Contains(failed.message, "step agent prepare command failed") {
			t.Errorf("expected message to report prepare failure, got %q", failed.message)
		}
		if observedTimeout < 490*time.Second || observedTimeout > 510*time.Second {
			t.Errorf("expected prepare timeout around 500s, got %v", observedTimeout)
		}
	})
}

type clockAdvancingModel struct {
	delegate agent.Model
	advance  func()
}

func (model *clockAdvancingModel) Generate(ctx context.Context, request agent.Request) (*agent.Response, error) {
	model.advance()
	return model.delegate.Generate(ctx, request)
}

func TestEnrichResultsWithAgent(t *testing.T) {
	t.Run("populates missing fields and round-trips through ParseResultsSamples", func(t *testing.T) {
		rawResults := []byte(`{
			"schemaVersion": 1,
			"samples": [
				{
					"id": "fix-bug",
					"passed": true,
					"score": 1.0,
					"message": "all tests passed"
				}
			]
		}`)
		instruction := "Fix the failing unit test."
		agentDuration := 12500 * time.Millisecond
		agentResult := &agent.Result{
			StopReason:   agent.StopReasonCompleted,
			InputTokens:  1200,
			OutputTokens: 350,
			Steps: []agent.Step{
				{
					Text: "Let me run the test suite first.",
					ToolCalls: []agent.ToolCall{
						{ID: "call-1", Command: "go test ./..."},
					},
					ToolResults: []agent.ToolResult{
						{ID: "call-1", Command: "go test ./...", Output: "FAIL: TestFoo", ExitCode: 1},
					},
				},
				{
					ToolCalls: []agent.ToolCall{
						{ID: "call-2", Command: "sed -i 's/bad/good/' foo.go"},
						{ID: "call-3", Command: "false"},
					},
					ToolResults: []agent.ToolResult{
						{ID: "call-2", Command: "sed -i 's/bad/good/' foo.go", Output: "", ExitCode: 0},
						{ID: "call-3", Command: "false", Output: "", ExitCode: 2},
					},
				},
				{
					Text: "Fixed the bug and verified all tests pass.",
				},
			},
		}

		enrichedBytes, err := enrichResultsWithAgent(rawResults, instruction, agentDuration, agentResult)
		if err != nil {
			t.Fatalf("enrichResultsWithAgent failed: %v", err)
		}

		parsedSamples, err := artifacts.ParseResultsSamples(enrichedBytes)
		if err != nil {
			t.Fatalf("ParseResultsSamples failed on enriched bytes: %v", err)
		}

		expectedScore := 1.0
		expectedLatency := 12.5
		expected := []artifacts.ParsedInspectSample{
			{
				ID:           "fix-bug",
				Status:       "pass",
				Score:        &expectedScore,
				HasScore:     true,
				ScoreStr:     "1.00",
				Input:        instruction,
				Explanation:  "all tests passed",
				Output:       "Fixed the bug and verified all tests pass.",
				Latency:      &expectedLatency,
				TotalTokens:  1550,
				InputTokens:  1200,
				OutputTokens: 350,
				Transcript: []artifacts.InspectTranscriptRow{
					{Kind: "user", Content: instruction},
					{Kind: "assistant", Content: "Let me run the test suite first."},
					{Kind: "tool_call", Content: "go test ./..."},
					{Kind: "tool", Content: "exit 1\nFAIL: TestFoo"},
					{Kind: "tool_call", Content: "sed -i 's/bad/good/' foo.go"},
					{Kind: "tool_call", Content: "false"},
					{Kind: "tool", Content: ""},
					{Kind: "tool", Content: "exit 2"},
					{Kind: "assistant", Content: "Fixed the bug and verified all tests pass."},
				},
			},
		}

		if !reflect.DeepEqual(parsedSamples, expected) {
			t.Errorf("round-tripped sample mismatch:\ngot:  %+v\nwant: %+v", parsedSamples, expected)
		}
	})

	t.Run("preserves fields already set by verifier", func(t *testing.T) {
		rawResults := []byte(`{
			"schemaVersion": 1,
			"samples": [
				{
					"id": "verifier-populated",
					"passed": true,
					"score": 1.0,
					"input": "verifier input",
					"output": "verifier output",
					"latencySeconds": 4.25,
					"inputTokens": 99,
					"outputTokens": 11,
					"transcript": [
						{"kind": "user", "content": "verifier transcript row"}
					]
				}
			]
		}`)
		agentResult := &agent.Result{
			StopReason:   agent.StopReasonCompleted,
			InputTokens:  5000,
			OutputTokens: 1000,
			Steps: []agent.Step{
				{Text: "agent final output"},
			},
		}

		enrichedBytes, err := enrichResultsWithAgent(rawResults, "agent instruction", 30*time.Second, agentResult)
		if err != nil {
			t.Fatalf("enrichResultsWithAgent failed: %v", err)
		}

		parsedSamples, err := artifacts.ParseResultsSamples(enrichedBytes)
		if err != nil {
			t.Fatalf("ParseResultsSamples failed: %v", err)
		}
		if len(parsedSamples) != 1 {
			t.Fatalf("expected 1 sample, got %d", len(parsedSamples))
		}
		sample := parsedSamples[0]
		if sample.Input != "verifier input" {
			t.Errorf("expected verifier Input preserved, got %q", sample.Input)
		}
		if sample.Output != "verifier output" {
			t.Errorf("expected verifier Output preserved, got %q", sample.Output)
		}
		if sample.Latency == nil || *sample.Latency != 4.25 {
			t.Errorf("expected verifier Latency 4.25 preserved, got %v", sample.Latency)
		}
		if sample.InputTokens != 99 || sample.OutputTokens != 11 || sample.TotalTokens != 110 {
			t.Errorf("expected verifier tokens 99/11/110 preserved, got %d/%d/%d", sample.InputTokens, sample.OutputTokens, sample.TotalTokens)
		}
		expectedTranscript := []artifacts.InspectTranscriptRow{
			{Kind: "user", Content: "verifier transcript row"},
		}
		if !reflect.DeepEqual(sample.Transcript, expectedTranscript) {
			t.Errorf("expected verifier Transcript preserved:\ngot:  %+v\nwant: %+v", sample.Transcript, expectedTranscript)
		}
	})

	t.Run("uses last non-empty step text as output", func(t *testing.T) {
		rawResults := []byte(`{"schemaVersion": 1, "samples": [{"id": "s1", "passed": true}]}`)
		agentResult := &agent.Result{
			StopReason: agent.StopReasonMaxSteps,
			Steps: []agent.Step{
				{Text: "first step text"},
				{Text: "second step text", ToolCalls: []agent.ToolCall{{Command: "ls"}}},
				{ToolCalls: []agent.ToolCall{{Command: "pwd"}}},
			},
		}

		enrichedBytes, err := enrichResultsWithAgent(rawResults, "inspect repo", time.Second, agentResult)
		if err != nil {
			t.Fatalf("enrichResultsWithAgent failed: %v", err)
		}
		parsedSamples, err := artifacts.ParseResultsSamples(enrichedBytes)
		if err != nil {
			t.Fatalf("ParseResultsSamples failed: %v", err)
		}
		if parsedSamples[0].Output != "second step text" {
			t.Errorf("expected last non-empty step text %q, got %q", "second step text", parsedSamples[0].Output)
		}
	})

	t.Run("leaves multi-sample results untouched", func(t *testing.T) {
		rawResults := []byte(`{"schemaVersion": 1, "samples": [{"id": "s1", "passed": true}, {"id": "s2", "passed": false}]}`)
		agentResult := &agent.Result{
			StopReason:   agent.StopReasonCompleted,
			InputTokens:  100,
			OutputTokens: 50,
			Steps:        []agent.Step{{Text: "done"}},
		}

		enrichedBytes, err := enrichResultsWithAgent(rawResults, "do two things", 5*time.Second, agentResult)
		if err != nil {
			t.Fatalf("enrichResultsWithAgent failed: %v", err)
		}
		if !bytes.Equal(enrichedBytes, rawResults) {
			t.Errorf("expected multi-sample results to be untouched:\ngot:  %s\nwant: %s", string(enrichedBytes), string(rawResults))
		}
	})

	t.Run("nil agent result returns original bytes", func(t *testing.T) {
		rawResults := []byte(`{"schemaVersion": 1, "samples": [{"id": "s1", "passed": true}]}`)
		enrichedBytes, err := enrichResultsWithAgent(rawResults, "instruction", time.Second, nil)
		if err != nil {
			t.Fatalf("enrichResultsWithAgent failed: %v", err)
		}
		if !bytes.Equal(enrichedBytes, rawResults) {
			t.Errorf("expected original bytes unchanged when agentResult is nil")
		}
	})

	t.Run("invalid JSON returns error", func(t *testing.T) {
		_, err := enrichResultsWithAgent([]byte(`{invalid`), "instruction", time.Second, &agent.Result{})
		if err == nil {
			t.Fatal("expected error for invalid JSON, got nil")
		}
	})
}

type timingOutModel struct{}

func (model *timingOutModel) Generate(ctx context.Context, request agent.Request) (*agent.Response, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestRunControllerWritesTaskCohort(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-orig", Spec: "eval.py@task_orig", Workdir: "evals"},
			{ID: "task-cohort", Spec: "eval.py@task_cohort", Cohort: "customer-reported", Workdir: "evals"},
		},
	}

	r := &RunReconciler{}
	initTasks := r.initializeTasks(v1alpha1.RunSpec{}, manifest)
	if len(initTasks) != 2 {
		t.Fatalf("Expected 2 initialized tasks, got %d", len(initTasks))
	}
	if initTasks[0].Cohort != "default" {
		t.Errorf("Expected initTasks[0].Cohort 'default', got %q", initTasks[0].Cohort)
	}
	if initTasks[1].Cohort != "customer-reported" {
		t.Errorf("Expected initTasks[1].Cohort 'customer-reported', got %q", initTasks[1].Cohort)
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-cohorts",
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
				TaskIDs:           []string{"task-orig", "task-cohort"},
			},
			Tasks: initTasks,
		},
	}

	inspectLog := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "task", "scorers": [{"name": "choice"}]},
		"samples": [
			{"id": 1, "scores": {"choice": {"value": 1.0}}}
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

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-cohorts"}}

	// Turn 1: create sandboxes for pending tasks
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	_ = cl.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox"))
	if len(sbList.Items) == 0 {
		t.Fatalf("Expected sandboxes created, got none")
	}

	for _, sb := range sbList.Items {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sb.Name,
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
	}

	// Turn 2: execute tasks
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)

	if len(updatedRun.Status.Tasks) != 2 {
		t.Fatalf("Expected 2 tasks in status, got %d", len(updatedRun.Status.Tasks))
	}
	taskMap := make(map[string]v1alpha1.TaskStatus)
	for _, tStat := range updatedRun.Status.Tasks {
		taskMap[tStat.ID] = tStat
	}

	tOrig := taskMap["task-orig"]
	if tOrig.Phase != v1alpha1.TaskPhaseCompleted {
		t.Errorf("Expected task-orig Completed, got %s", tOrig.Phase)
	}
	if tOrig.Cohort != "default" {
		t.Errorf("Expected task-orig Cohort 'default', got %q", tOrig.Cohort)
	}

	tCohort := taskMap["task-cohort"]
	if tCohort.Phase != v1alpha1.TaskPhaseCompleted {
		t.Errorf("Expected task-cohort Completed, got %s", tCohort.Phase)
	}
	if tCohort.Cohort != "customer-reported" {
		t.Errorf("Expected task-cohort Cohort 'customer-reported', got %q", tCohort.Cohort)
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
	runningStartedAt := metav1.NewTime(now.Add(-3 * time.Minute))

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
				{ID: "task-running", Phase: v1alpha1.TaskPhaseRunning, StartedAt: &runningStartedAt},
				{ID: "task-pending", Phase: v1alpha1.TaskPhasePending},
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
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Get error: %v", err)
	}

	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected Run phase Failed, got %s", updatedRun.Status.Phase)
	}
	expectedRunMessage := "Execution deadline exceeded (1 running, 1 not started cut off)"
	if updatedRun.Status.Message != expectedRunMessage {
		t.Errorf("Expected deadline exceeded message %q, got %q", expectedRunMessage, updatedRun.Status.Message)
	}
	if len(updatedRun.Status.Tasks) != 2 {
		t.Fatalf("Expected 2 tasks, got %d", len(updatedRun.Status.Tasks))
	}
	taskRunning := updatedRun.Status.Tasks[0]
	if taskRunning.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task-running Phase Failed, got %s", taskRunning.Phase)
	}
	expectedRunningMessage := "cut off by Run execution deadline after running for 3m0s"
	if taskRunning.Message != expectedRunningMessage {
		t.Errorf("Expected task-running Message %q, got %q", expectedRunningMessage, taskRunning.Message)
	}
	if taskRunning.StartedAt == nil || !taskRunning.StartedAt.Equal(&runningStartedAt) {
		t.Errorf("Expected task-running StartedAt %v, got %v", runningStartedAt, taskRunning.StartedAt)
	}
	if taskRunning.FinishedAt == nil || !taskRunning.FinishedAt.Equal(&metav1.Time{Time: now}) {
		t.Errorf("Expected task-running FinishedAt %v, got %v", now, taskRunning.FinishedAt)
	}
	taskPending := updatedRun.Status.Tasks[1]
	if taskPending.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task-pending Phase Failed, got %s", taskPending.Phase)
	}
	expectedPendingMessage := "cut off by Run execution deadline (not started)"
	if taskPending.Message != expectedPendingMessage {
		t.Errorf("Expected task-pending Message %q, got %q", expectedPendingMessage, taskPending.Message)
	}
	if taskPending.StartedAt != nil {
		t.Errorf("Expected task-pending StartedAt nil, got %v", taskPending.StartedAt)
	}
	if taskPending.FinishedAt == nil || !taskPending.FinishedAt.Equal(&metav1.Time{Time: now}) {
		t.Errorf("Expected task-pending FinishedAt %v, got %v", now, taskPending.FinishedAt)
	}
}

func TestReconcileExecutionDeadline_TaskCutOffDuringCommandExecution(t *testing.T) {
	scheme := setupScheme(t)

	startTime := time.Date(2026, 11, 19, 12, 27, 0, 0, time.UTC)
	deadline := time.Date(2026, 11, 19, 12, 30, 0, 0, time.UTC)
	deadlineTime := metav1.NewTime(deadline)

	currentTime := startTime
	var timeMutex sync.Mutex
	nowFunc := func() time.Time {
		timeMutex.Lock()
		defer timeMutex.Unlock()
		return currentTime
	}

	timeoutSeconds := 3600
	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{
				ID:             "polyglot-c-py",
				Spec:           "eval.py@polyglot",
				Image:          "custom-task-image:v1",
				Command:        []string{"run-eval"},
				Resources:      []v1alpha1.ManifestResource{{Type: "sandbox"}},
				TimeoutSeconds: &timeoutSeconds,
			},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-cutoff-exec",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "commit1"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase:             v1alpha1.RunPhaseRunning,
			ExecutionDeadline: &deadlineTime,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:    "polyglot-c-py",
					Phase: v1alpha1.TaskPhasePending,
				},
			},
		},
	}

	fakeClient := &sandboxclient.FakeClient{
		PollAsyncFn: func(ctx context.Context, executionID string) (*sandboxclient.AsyncTaskStatus, error) {
			timeMutex.Lock()
			currentTime = deadline
			timeMutex.Unlock()
			return nil, context.DeadlineExceeded
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
		SandboxClient:    fakeClient,
		Now:              nowFunc,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-cutoff-exec"}}

	// Turn 1: start pending task, transition to Running with StartedAt = 12:27:00, create sandbox
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}

	var sandboxList sandboxv1beta1.SandboxList
	if err := cl.List(t.Context(), &sandboxList, client.InNamespace("eval-runner-sandbox")); err != nil {
		t.Fatalf("List error: %v", err)
	}
	if len(sandboxList.Items) == 0 {
		t.Fatalf("Expected sandbox to be created")
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxList.Items[0].Name,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.75",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	if err := cl.Create(t.Context(), pod); err != nil {
		t.Fatalf("Create pod error: %v", err)
	}

	// Turn 2: start async task command and persist ExecutionID
	if _, err = reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}

	// Turn 3: poll async task during which deadline expires (time reaches 12:30:00)
	if _, err = reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Turn 3 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Get error: %v", err)
	}

	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected Run phase Failed, got %s", updatedRun.Status.Phase)
	}
	expectedRunMessage := "Execution deadline exceeded (1 running cut off)"
	if updatedRun.Status.Message != expectedRunMessage {
		t.Errorf("Expected Run Message %q, got %q", expectedRunMessage, updatedRun.Status.Message)
	}

	if len(updatedRun.Status.Tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(updatedRun.Status.Tasks))
	}
	task := updatedRun.Status.Tasks[0]
	if task.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task Phase Failed, got %s", task.Phase)
	}
	if task.ExecutionID != "" {
		t.Errorf("Expected task ExecutionID cleared on deadline cutoff, got %q", task.ExecutionID)
	}
	expectedTaskMessage := "cut off by Run execution deadline after running for 3m0s"
	if task.Message != expectedTaskMessage {
		t.Errorf("Expected task Message %q, got %q", expectedTaskMessage, task.Message)
	}
	expectedStartedAt := metav1.NewTime(startTime)
	if task.StartedAt == nil || !task.StartedAt.Equal(&expectedStartedAt) {
		t.Errorf("Expected StartedAt %v, got %v", expectedStartedAt, task.StartedAt)
	}
	expectedFinishedAt := metav1.NewTime(deadline)
	if task.FinishedAt == nil || !task.FinishedAt.Equal(&expectedFinishedAt) {
		t.Errorf("Expected FinishedAt %v, got %v", expectedFinishedAt, task.FinishedAt)
	}
}

func TestReconcile_TaskPod_NoModelSecretEnv(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{
				ID:   "sample-runner-task",
				Spec: "eval.py@sample",
			},
			{
				ID: "sandbox-only-task",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox},
				},
			},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-no-model-secret",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "google/gemini-3.5-flash-lite",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         2,
				EligibleTaskCount: 2,
				TaskIDs:           []string{"sample-runner-task", "sandbox-only-task"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "sample-runner-task", Phase: v1alpha1.TaskPhasePending},
				{ID: "sandbox-only-task", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	testClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           testClient,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		Config:           ClusterConfig{MaxParallelTasks: 4},
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-no-model-secret"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var sandboxList sandboxv1beta1.SandboxList
	if err := testClient.List(t.Context(), &sandboxList, client.InNamespace("eval-runner-sandbox")); err != nil {
		t.Fatalf("List sandboxes error: %v", err)
	}
	if len(sandboxList.Items) != 2 {
		t.Fatalf("Expected 2 sandboxes created, got %d", len(sandboxList.Items))
	}

	for _, taskSandbox := range sandboxList.Items {
		taskID := taskSandbox.Annotations[discovery.AnnotationTaskID]
		for _, container := range taskSandbox.Spec.PodTemplate.Spec.Containers {
			if len(container.EnvFrom) != 0 {
				t.Errorf("[%s] Expected no EnvFrom in container %q, got: %+v", taskID, container.Name, container.EnvFrom)
			}
			for _, envVar := range container.Env {
				if envVar.ValueFrom != nil {
					t.Errorf("[%s] Expected no ValueFrom in container %q env %q: %+v", taskID, container.Name, envVar.Name, envVar)
				}
			}
			if taskID == "sample-runner-task" {
				var foundGeminiKey bool
				for _, envVar := range container.Env {
					if envVar.Name == "GEMINI_API_KEY" && envVar.Value == discovery.PlaceholderModelCredential {
						foundGeminiKey = true
					}
				}
				if !foundGeminiKey {
					t.Errorf("Expected placeholder GEMINI_API_KEY in sample-runner-task container")
				}
			}
			if taskID == "sandbox-only-task" {
				if len(container.Env) != 0 {
					t.Errorf("Expected 0 environment variables in sandbox-only-task, got: %+v", container.Env)
				}
			}
		}
	}
}

func TestExecuteTask_SampleRunnerPromptAuthFailureWithPlaceholderCredential(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{
				ID:   "docs/readme_comprehension",
				Spec: "eval.py@sample",
			},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-real-model-auth-fail",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "abcdef123456",
			},
			Model: "google/gemini-3.5-flash-lite",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{"docs/readme_comprehension"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:    "docs/readme_comprehension",
					Phase: v1alpha1.TaskPhasePending,
				},
			},
		},
	}

	authErrorStderr := "google.genai.errors.ClientError: 400 API_KEY_INVALID: API key not valid. Please pass a valid API key."
	fakeClient := &sandboxclient.FakeClient{
		InspectExit:   1,
		InspectStderr: authErrorStderr,
	}

	testClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           testClient,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		SandboxClient:    fakeClient,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-real-model-auth-fail"}}

	// Turn 1: Starts pending task, creates sandbox with placeholder credentials
	startTime := time.Now()
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}

	var sandboxList sandboxv1beta1.SandboxList
	if err := testClient.List(t.Context(), &sandboxList, client.InNamespace("eval-runner-sandbox")); err != nil {
		t.Fatalf("List sandboxes error: %v", err)
	}
	if len(sandboxList.Items) != 1 {
		t.Fatalf("Expected 1 sandbox created, got %d", len(sandboxList.Items))
	}

	sampleContainer := sandboxList.Items[0].Spec.PodTemplate.Spec.Containers[0]
	var foundGeminiKey bool
	for _, envVar := range sampleContainer.Env {
		if envVar.Name == "GEMINI_API_KEY" && envVar.Value == discovery.PlaceholderModelCredential {
			foundGeminiKey = true
		}
	}
	if !foundGeminiKey {
		t.Errorf("Expected GEMINI_API_KEY placeholder on sample-runner pod")
	}

	// Create running pod to simulate ready pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxList.Items[0].Name,
			Namespace: "eval-runner-sandbox",
			Labels: map[string]string{
				discovery.LabelRole: discovery.RoleSample,
			},
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
	if err := testClient.Create(t.Context(), pod); err != nil {
		t.Fatalf("Create pod error: %v", err)
	}

	// Turn 2: Reconcile executes task, which fails promptly with provider authentication error
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}
	elapsed := time.Since(startTime)

	var updatedRun v1alpha1.Run
	if err := testClient.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Get run error: %v", err)
	}

	task := updatedRun.Status.Tasks[0]
	if task.Phase != v1alpha1.TaskPhaseFailed {
		t.Fatalf("Expected task Phase Failed, got %s", task.Phase)
	}
	if !strings.Contains(task.Message, "API_KEY_INVALID") {
		t.Errorf("Expected task Message to contain provider authentication error API_KEY_INVALID, got: %s", task.Message)
	}
	if !strings.Contains(task.Message, "step inspect eval failed with exit code 1") {
		t.Errorf("Expected task Message to indicate inspect eval failure, got: %s", task.Message)
	}
	t.Logf("Simulated real-model sample-runner prompt failure completed in %v with bounded message: %s", elapsed, task.Message)
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
				{ID: "task-missing", Phase: v1alpha1.TaskPhaseRunning, FailureAttempts: 2},
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
	if tMissing.FailureAttempts != 3 {
		t.Errorf("Expected FailureAttempts 3, got %d", tMissing.FailureAttempts)
	}
	if !strings.Contains(tMissing.Message, "task sandbox missing") {
		t.Errorf("Expected failure message to mention sandbox missing, got: %s", tMissing.Message)
	}
	if !strings.Contains(tMissing.Message, "failed after 3 attempts") {
		t.Errorf("Expected failure message to mention attempts, got: %s", tMissing.Message)
	}
}

func TestReconcileRunningTask_RestartRescue(t *testing.T) {
	scheme := setupScheme(t)

	t.Run("Running task with Samples == requested and no sandbox -> Completed", func(t *testing.T) {
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{ID: "task-rescued", Spec: "eval.py@rescued"},
			},
		}

		limit := 8
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-rescue-complete",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Model: "mockllm/model",
				Limit: &limit,
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					TaskCount:         1,
					EligibleTaskCount: 1,
					TaskIDs:           []string{"task-rescued"},
				},
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:             "task-rescued",
						Phase:          v1alpha1.TaskPhaseRunning,
						Samples:        8,
						Successes:      8,
						SamplesPlanned: 8,
					},
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

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-rescue-complete"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}
		if !res.Requeue {
			t.Errorf("Expected Requeue after restart rescue")
		}

		var updatedRun v1alpha1.Run
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get updated run: %v", err)
		}

		if len(updatedRun.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task, got %d", len(updatedRun.Status.Tasks))
		}
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseCompleted {
			t.Errorf("Expected task to be Completed via restart rescue, got phase %s (msg: %s)", task.Phase, task.Message)
		}
		if !strings.Contains(task.Message, "restart rescue") {
			t.Errorf("Expected note 'restart rescue' in message, got %q", task.Message)
		}
		if task.Samples != 8 || task.Successes != 8 {
			t.Errorf("Expected 8 samples and 8 successes, got samples=%d successes=%d", task.Samples, task.Successes)
		}
		if task.SuccessRate == nil || *task.SuccessRate != 1.0 {
			t.Errorf("Expected successRate 1.0, got %v", task.SuccessRate)
		}

		// Reconcile again to verify Run reaches Completed
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Second reconcile error: %v", err)
		}
		_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
		if updatedRun.Status.Phase != v1alpha1.RunPhaseCompleted {
			t.Errorf("Expected Run phase to be Completed, got %s", updatedRun.Status.Phase)
		}
	})

	t.Run("Running task with Samples == 0 and no sandbox -> Failed", func(t *testing.T) {
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{ID: "task-zero", Spec: "eval.py@zero"},
			},
		}

		limit := 8
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-rescue-fail",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Model: "mockllm/model",
				Limit: &limit,
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					TaskCount:         1,
					EligibleTaskCount: 1,
					TaskIDs:           []string{"task-zero"},
				},
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:              "task-zero",
						Phase:           v1alpha1.TaskPhaseRunning,
						FailureAttempts: 2,
						Samples:         0,
						SamplesPlanned:  8,
					},
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

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-rescue-fail"}}
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updatedRun v1alpha1.Run
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get updated run: %v", err)
		}

		if len(updatedRun.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task, got %d", len(updatedRun.Status.Tasks))
		}
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("Expected task to fail with Samples==0, got phase %s", task.Phase)
		}
		if !strings.Contains(task.Message, "task sandbox missing") {
			t.Errorf("Expected failure message to mention sandbox missing, got: %s", task.Message)
		}
	})
}

type orderRecordingClient struct {
	client.Client
	events []string
	mu     sync.Mutex
}

func (c *orderRecordingClient) record(event string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, event)
}

func (c *orderRecordingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if sb, ok := obj.(*sandboxv1beta1.Sandbox); ok {
		c.record("delete-sandbox:" + sb.Name)
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func (c *orderRecordingClient) Status() client.SubResourceWriter {
	return &orderRecordingStatusWriter{
		SubResourceWriter: c.Client.Status(),
		parent:            c,
	}
}

type orderRecordingStatusWriter struct {
	client.SubResourceWriter
	parent *orderRecordingClient
}

func (w *orderRecordingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if run, ok := obj.(*v1alpha1.Run); ok {
		for _, t := range run.Status.Tasks {
			if t.Phase == v1alpha1.TaskPhaseCompleted {
				w.parent.record(fmt.Sprintf("status-update-task-completed:%s:samples=%d", t.ID, t.Samples))
			}
		}
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestReconcileTaskCompletion_StatusWritePrecedesSandboxDelete(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-1", Spec: "eval.py@task1"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-order",
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
				TaskIDs:           []string{"task-1"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-1", Phase: v1alpha1.TaskPhasePending},
			},
		},
	}

	inspectLog := `{
		"version": 2,
		"status": "started",
		"eval": {"task": "task-1"},
		"samples": [
			{"id": "s1", "scores": {"accuracy": {"value": 1.0}}},
			{"id": "s2", "scores": {"accuracy": {"value": 0.0}}}
		]
	}`

	executor := &sandboxclient.FakeClient{
		InspectLog: []byte(inspectLog),
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	recClient := &orderRecordingClient{
		Client: baseClient,
	}

	reconciler := &RunReconciler{
		Client:           recClient,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-order"}}

	// Turn 1: Starts pending task, creates task sandbox
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 1 error: %v", err)
	}

	var sbList sandboxv1beta1.SandboxList
	if err := recClient.List(t.Context(), &sbList, client.InNamespace("eval-runner-sandbox")); err != nil {
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
	if err := recClient.Create(t.Context(), pod); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Turn 2: Executes task, writes Completed status, and deletes sandbox
	res, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile turn 2 error: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected Requeue after executing task")
	}

	statusEvent := "status-update-task-completed:task-1:samples=2"
	deleteEvent := "delete-sandbox:" + taskSB.Name

	statusIdx := -1
	deleteIdx := -1
	for idx, ev := range recClient.events {
		if ev == statusEvent && statusIdx == -1 {
			statusIdx = idx
		}
		if ev == deleteEvent && deleteIdx == -1 {
			deleteIdx = idx
		}
	}

	if statusIdx == -1 {
		t.Fatalf("Expected event %q in client events, got: %v", statusEvent, recClient.events)
	}
	if deleteIdx == -1 {
		t.Fatalf("Expected event %q in client events, got: %v", deleteEvent, recClient.events)
	}
	if statusIdx >= deleteIdx {
		t.Errorf("Expected status write (idx %d) to precede sandbox delete (idx %d), events: %v", statusIdx, deleteIdx, recClient.events)
	}
}

func TestReconcileRunningTask_LingeringSandboxCleanedOnNextReconcile(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-completed", Spec: "eval.py@done"},
			{ID: "task-running", Spec: "eval.py@run"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-lingering",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         2,
				EligibleTaskCount: 2,
				TaskIDs:           []string{"task-completed", "task-running"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      "task-completed",
					Phase:   v1alpha1.TaskPhaseCompleted,
					Samples: 8,
				},
				{
					ID:    "task-running",
					Phase: v1alpha1.TaskPhaseRunning,
				},
			},
		},
	}

	// Lingering sandbox for task-completed
	sbDone := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.TaskSandboxNameForRun(run.Name, "task-completed"),
			Namespace: "eval-runner-sandbox",
			Labels: map[string]string{
				discovery.LabelRunName:      run.Name,
				discovery.LabelRunNamespace: run.Namespace,
				discovery.LabelRole:         discovery.RoleSample,
			},
			Annotations: map[string]string{
				discovery.AnnotationTaskID: "task-completed",
			},
		},
	}

	// Sandbox and Pod for task-running
	sbRun := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.TaskSandboxNameForRun(run.Name, "task-running"),
			Namespace: "eval-runner-sandbox",
			Labels: map[string]string{
				discovery.LabelRunName:      run.Name,
				discovery.LabelRunNamespace: run.Namespace,
				discovery.LabelRole:         discovery.RoleSample,
			},
			Annotations: map[string]string{
				discovery.AnnotationTaskID: "task-running",
			},
		},
	}
	podRun := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sbRun.Name,
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.15",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	inspectLog := `{
		"version": 2,
		"status": "started",
		"eval": {"task": "task-running"},
		"samples": [
			{"id": "s1", "scores": {"accuracy": {"value": 1.0}}}
		]
	}`

	executor := &sandboxclient.FakeClient{
		InspectLog: []byte(inspectLog),
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sbDone, sbRun, podRun).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-lingering"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	// Verify that lingering sandbox for task-completed was deleted
	var checkSBDone sandboxv1beta1.Sandbox
	err = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sbDone.Name}, &checkSBDone)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected lingering sandbox %s to be deleted, got err: %v", sbDone.Name, err)
	}
}

func TestReconcileRunningTask_RestartRescueFromReadableLog(t *testing.T) {
	scheme := setupScheme(t)

	inspectLog := `{
		"version": 2,
		"status": "started",
		"eval": {"task": "task-log-rescue"},
		"samples": [
			{"id": "s1", "scores": {"accuracy": {"value": 1.0}}},
			{"id": "s2", "scores": {"accuracy": {"value": 1.0}}}
		]
	}`

	t.Run("Readable LogPath rescues task with Samples == 0", func(t *testing.T) {
		sink := artifacts.NewInMemorySink()
		meta := artifacts.RunMetadata{
			RunName:      "test-run-log-rescue",
			RunNamespace: "eval-runner",
			TaskID:       "task-log-rescue",
			Commit:       "c0ffee1",
		}
		stored, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", []byte(inspectLog))
		if err != nil {
			t.Fatalf("Failed to store artifact: %v", err)
		}
		logURI := stored.Location

		limit := 2
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-log-rescue",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Model: "mockllm/model",
				Limit: &limit,
				Repo: v1alpha1.RepoSpec{
					Commit: "c0ffee1",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					TaskCount:         1,
					EligibleTaskCount: 1,
					TaskIDs:           []string{"task-log-rescue"},
				},
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:             "task-log-rescue",
						Phase:          v1alpha1.TaskPhaseRunning,
						Samples:        0,
						SamplesPlanned: 2,
						LogPath:        logURI,
					},
				},
			},
		}

		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{ID: "task-log-rescue", Spec: "eval.py@log"},
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
			ArtifactSink:     sink,
			SandboxNamespace: "eval-runner-sandbox",
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-log-rescue"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}
		if !res.Requeue {
			t.Errorf("Expected Requeue after log rescue")
		}

		var updatedRun v1alpha1.Run
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get updated run: %v", err)
		}

		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseCompleted {
			t.Errorf("Expected task to be Completed via log rescue, got %s (msg: %s)", task.Phase, task.Message)
		}
		if !strings.Contains(task.Message, "restart rescue") {
			t.Errorf("Expected message to contain 'restart rescue', got %q", task.Message)
		}
		if task.Samples != 2 || task.Successes != 2 {
			t.Errorf("Expected 2 samples and 2 successes from log, got samples=%d successes=%d", task.Samples, task.Successes)
		}
	})

	t.Run("Missing LogPath fails task with Samples == 0", func(t *testing.T) {
		sink := artifacts.NewInMemorySink()
		limit := 2
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-missing-log",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Model: "mockllm/model",
				Limit: &limit,
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					TaskCount:         1,
					EligibleTaskCount: 1,
					TaskIDs:           []string{"task-missing-log"},
				},
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:              "task-missing-log",
						Phase:           v1alpha1.TaskPhaseRunning,
						FailureAttempts: 2,
						Samples:         0,
						SamplesPlanned:  2,
						LogPath:         "mem://runs/nonexistent/inspect.json",
					},
				},
			},
		}

		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{ID: "task-missing-log", Spec: "eval.py@log"},
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
			ArtifactSink:     sink,
			SandboxNamespace: "eval-runner-sandbox",
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-missing-log"}}
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updatedRun v1alpha1.Run
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get updated run: %v", err)
		}

		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("Expected task to fail with missing log, got %s", task.Phase)
		}
	})
}

type transientErrorSink struct {
	err error
}

func (s *transientErrorSink) StoreArtifact(ctx context.Context, meta artifacts.RunMetadata, filename string, data []byte) (artifacts.StoredArtifact, error) {
	return artifacts.StoredArtifact{}, s.err
}

func (s *transientErrorSink) StoreComparison(ctx context.Context, comparisonName string, data []byte) (string, error) {
	return "", s.err
}

func (s *transientErrorSink) StoreManifest(ctx context.Context, meta artifacts.RunMetadata, data []byte) (string, error) {
	return "", s.err
}

func (s *transientErrorSink) Load(ctx context.Context, path string) ([]byte, error) {
	return nil, s.err
}

func (s *transientErrorSink) OpenStream(ctx context.Context, path string, hash string) (io.ReadCloser, error) {
	return nil, s.err
}

func (s *transientErrorSink) ArtifactPrefix(commit string) string {
	return "mem://runs/" + commit + "/"
}

func (s *transientErrorSink) IsDurable() bool {
	return false
}

func (s *transientErrorSink) Close() error {
	return nil
}

func TestReconcileRunningTask_RestartRescueFromDerivedLogPath(t *testing.T) {
	scheme := setupScheme(t)

	completeInspectLog := `{
		"version": 2,
		"status": "started",
		"eval": {"task": "task-probe-rescue"},
		"samples": [
			{"id": "s1", "scores": {"accuracy": {"value": 1.0}}},
			{"id": "s2", "scores": {"accuracy": {"value": 1.0}}}
		]
	}`

	partialInspectLog := `{
		"version": 2,
		"status": "started",
		"eval": {"task": "task-probe-rescue"},
		"samples": [
			{"id": "s1", "scores": {"accuracy": {"value": 1.0}}}
		]
	}`

	t.Run("Running task with no Samples/LogPath and complete summary at derived path -> Completed with correct counts", func(t *testing.T) {
		sink := artifacts.NewInMemorySink()
		rate := 1.0
		meta := artifacts.RunMetadata{
			RunName:         "test-run-probe-rescue",
			RunNamespace:    "eval-runner",
			TaskID:          "task-probe-rescue",
			Commit:          "c0ffee1",
			SampleCount:     2,
			Successes:       2,
			MeanScore:       &rate,
			SuccessRate:     &rate,
			PossibleSamples: 2,
			SampleRange:     &artifacts.SampleRange{Start: 1, End: 2},
		}
		logFilename := "2026-10-31T15-27-47-00-00_task-probe-rescue_ByhDsNK8zVf5BWpRqxd9Y9.json"
		stored, err := sink.StoreArtifact(t.Context(), meta, logFilename, []byte(completeInspectLog))
		if err != nil {
			t.Fatalf("Failed to store artifact: %v", err)
		}
		expectedLogPath := stored.Location
		expectedLogHash := stored.Hash

		limit := 2
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-probe-rescue",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Model: "mockllm/model",
				Limit: &limit,
				Repo: v1alpha1.RepoSpec{
					Commit: "c0ffee1",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					TaskCount:         1,
					EligibleTaskCount: 1,
					TaskIDs:           []string{"task-probe-rescue"},
				},
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:             "task-probe-rescue",
						Phase:          v1alpha1.TaskPhaseRunning,
						Samples:        0,
						SamplesPlanned: 2,
						LogPath:        "",
					},
				},
			},
		}

		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{ID: "task-probe-rescue", Spec: "eval.py@rescue"},
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
			ArtifactSink:     sink,
			SandboxNamespace: "eval-runner-sandbox",
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-probe-rescue"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}
		if !res.Requeue {
			t.Errorf("Expected Requeue after restart rescue")
		}

		var updatedRun v1alpha1.Run
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get updated run: %v", err)
		}

		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseCompleted {
			t.Errorf("Expected task to be Completed via restart rescue, got %s (msg: %s)", task.Phase, task.Message)
		}
		if !strings.Contains(task.Message, "restart rescue") {
			t.Errorf("Expected message to contain 'restart rescue', got %q", task.Message)
		}
		if task.Samples != 2 || task.Successes != 2 {
			t.Errorf("Expected 2 samples and 2 successes, got samples=%d successes=%d", task.Samples, task.Successes)
		}
		if task.LogPath != expectedLogPath {
			t.Errorf("Expected task LogPath to be %q, got %q", expectedLogPath, task.LogPath)
		}
		if task.LogHash != expectedLogHash {
			t.Errorf("Expected task LogHash to be %q, got %q", expectedLogHash, task.LogHash)
		}
		if task.PossibleSamples != 2 {
			t.Errorf("Expected task PossibleSamples to be 2, got %d", task.PossibleSamples)
		}
		if task.SampleRange == nil || task.SampleRange.Start != 1 || task.SampleRange.End != 2 {
			t.Errorf("Expected task SampleRange to be [1, 2], got %+v", task.SampleRange)
		}
	})

	t.Run("LocalDirSink with hierarchical taskID rescues and unescapes path properly", func(t *testing.T) {
		tempDir := t.TempDir()
		localSink := &artifacts.LocalDirSink{BaseDir: tempDir}
		rate := 1.0
		meta := artifacts.RunMetadata{
			RunName:         "test-run-local-rescue",
			RunNamespace:    "eval-runner",
			TaskID:          "docs/readme_comprehension",
			Commit:          "c0ffee1",
			SampleCount:     2,
			Successes:       2,
			MeanScore:       &rate,
			SuccessRate:     &rate,
			PossibleSamples: 2,
		}
		logFilename := "2026-10-31T15-27-47-00-00_readme-comprehension_ByhDsNK8zVf5BWpRqxd9Y9.json"
		stored, err := localSink.StoreArtifact(t.Context(), meta, logFilename, []byte(completeInspectLog))
		if err != nil {
			t.Fatalf("Failed to store artifact in LocalDirSink: %v", err)
		}
		expectedLogPath := stored.Location
		expectedLogHash := stored.Hash

		limit := 2
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-local-rescue",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Model: "mockllm/model",
				Limit: &limit,
				Repo: v1alpha1.RepoSpec{
					Commit: "c0ffee1",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					TaskCount:         1,
					EligibleTaskCount: 1,
					TaskIDs:           []string{"docs/readme_comprehension"},
				},
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:             "docs/readme_comprehension",
						Phase:          v1alpha1.TaskPhaseRunning,
						Samples:        0,
						SamplesPlanned: 2,
						LogPath:        "",
					},
				},
			},
		}

		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{ID: "docs/readme_comprehension", Spec: "eval.py@rescue"},
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
			ArtifactSink:     localSink,
			SandboxNamespace: "eval-runner-sandbox",
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-local-rescue"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}
		if !res.Requeue {
			t.Errorf("Expected Requeue after restart rescue")
		}

		var updatedRun v1alpha1.Run
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get updated run: %v", err)
		}

		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseCompleted {
			t.Errorf("Expected task to be Completed via restart rescue, got %s (msg: %s)", task.Phase, task.Message)
		}
		if task.LogPath != expectedLogPath {
			t.Errorf("Expected task LogPath to be %q, got %q", expectedLogPath, task.LogPath)
		}
		if task.LogHash != expectedLogHash {
			t.Errorf("Expected task LogHash to be %q, got %q", expectedLogHash, task.LogHash)
		}
	})

	t.Run("Running task with no Samples/LogPath and missing summary -> Failed as before", func(t *testing.T) {
		sink := artifacts.NewInMemorySink()
		limit := 2
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-probe-missing",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Model: "mockllm/model",
				Limit: &limit,
				Repo: v1alpha1.RepoSpec{
					Commit: "c0ffee1",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					TaskCount:         1,
					EligibleTaskCount: 1,
					TaskIDs:           []string{"task-probe-rescue"},
				},
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:              "task-probe-rescue",
						Phase:           v1alpha1.TaskPhaseRunning,
						FailureAttempts: 2,
						Samples:         0,
						SamplesPlanned:  2,
						LogPath:         "",
					},
				},
			},
		}

		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{ID: "task-probe-rescue", Spec: "eval.py@rescue"},
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
			ArtifactSink:     sink,
			SandboxNamespace: "eval-runner-sandbox",
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-probe-missing"}}
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updatedRun v1alpha1.Run
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get updated run: %v", err)
		}

		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("Expected task to fail with missing log, got %s", task.Phase)
		}
		if !strings.Contains(task.Message, "task sandbox missing while in Running phase") {
			t.Errorf("Expected failure message to mention sandbox missing, got: %s", task.Message)
		}
	})

	t.Run("Running task with no Samples/LogPath and partial summary -> Failed as before", func(t *testing.T) {
		sink := artifacts.NewInMemorySink()
		rate := 1.0
		meta := artifacts.RunMetadata{
			RunName:      "test-run-probe-partial",
			RunNamespace: "eval-runner",
			TaskID:       "task-probe-rescue",
			Commit:       "c0ffee1",
			SampleCount:  1,
			Successes:    1,
			MeanScore:    &rate,
			SuccessRate:  &rate,
		}
		logFilename := "2026-10-31T15-27-47-00-00_task-probe-rescue_ByhDsNK8zVf5BWpRqxd9Y9.json"
		_, err := sink.StoreArtifact(t.Context(), meta, logFilename, []byte(partialInspectLog))
		if err != nil {
			t.Fatalf("Failed to store artifact: %v", err)
		}

		limit := 2
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-probe-partial",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Model: "mockllm/model",
				Limit: &limit,
				Repo: v1alpha1.RepoSpec{
					Commit: "c0ffee1",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					TaskCount:         1,
					EligibleTaskCount: 1,
					TaskIDs:           []string{"task-probe-rescue"},
				},
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:              "task-probe-rescue",
						Phase:           v1alpha1.TaskPhaseRunning,
						FailureAttempts: 2,
						Samples:         0,
						SamplesPlanned:  2,
						LogPath:         "",
					},
				},
			},
		}

		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{ID: "task-probe-rescue", Spec: "eval.py@rescue"},
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
			ArtifactSink:     sink,
			SandboxNamespace: "eval-runner-sandbox",
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-probe-partial"}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updatedRun v1alpha1.Run
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get updated run: %v", err)
		}

		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("Expected task to fail with partial log, got %s", task.Phase)
		}
		if !strings.Contains(task.Message, "task sandbox missing while in Running phase") {
			t.Errorf("Expected failure message to mention sandbox missing, got: %s", task.Message)
		}
	})

	t.Run("Running task with no Samples/LogPath and probe error other than not-found returns error", func(t *testing.T) {
		transientErrSink := &transientErrorSink{
			err: errors.New("storage network timeout"),
		}

		limit := 2
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-probe-error",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Model: "mockllm/model",
				Limit: &limit,
				Repo: v1alpha1.RepoSpec{
					Commit: "c0ffee1",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					TaskCount:         1,
					EligibleTaskCount: 1,
					TaskIDs:           []string{"task-probe-rescue"},
				},
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:             "task-probe-rescue",
						Phase:          v1alpha1.TaskPhaseRunning,
						Samples:        0,
						SamplesPlanned: 2,
						LogPath:        "",
					},
				},
			},
		}

		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{ID: "task-probe-rescue", Spec: "eval.py@rescue"},
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
			ArtifactSink:     transientErrSink,
			SandboxNamespace: "eval-runner-sandbox",
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-probe-error"}}
		_, err := reconciler.Reconcile(t.Context(), req)
		if err == nil {
			t.Fatalf("Expected error from Reconcile when probe fails with non-not-found error, got nil")
		}
		if !strings.Contains(err.Error(), "storage network timeout") {
			t.Errorf("Expected error to mention 'storage network timeout', got: %v", err)
		}

		// Verify task was NOT marked Failed
		var updatedRun v1alpha1.Run
		if getErr := cl.Get(t.Context(), req.NamespacedName, &updatedRun); getErr != nil {
			t.Fatalf("Failed to get updated run: %v", getErr)
		}
		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhaseRunning {
			t.Errorf("Expected task to remain Running on transient probe error, got phase %s", task.Phase)
		}
	})

	t.Run("Running task with summary from another Run at same commit is not rescued and restarts if dirty", func(t *testing.T) {
		sink := artifacts.NewInMemorySink()
		rate := 1.0
		// summary written by earlier run "earlier-run"
		meta := artifacts.RunMetadata{
			RunName:         "earlier-run",
			RunNamespace:    "eval-runner",
			TaskID:          "task-probe-rescue",
			Commit:          "c0ffee1",
			SampleCount:     2,
			Successes:       2,
			MeanScore:       &rate,
			SuccessRate:     &rate,
			PossibleSamples: 2,
		}
		logFilename := "2026-10-31T15-27-47-00-00_task-probe-rescue_ByhDsNK8zVf5BWpRqxd9Y9.json"
		if _, err := sink.StoreArtifact(t.Context(), meta, logFilename, []byte(completeInspectLog)); err != nil {
			t.Fatalf("Failed to store artifact: %v", err)
		}

		limit := 2
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "second-run",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Model: "mockllm/model",
				Limit: &limit,
				Repo: v1alpha1.RepoSpec{
					Commit: "c0ffee1",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					TaskCount:         1,
					EligibleTaskCount: 1,
					TaskIDs:           []string{"task-probe-rescue"},
				},
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:             "task-probe-rescue",
						Phase:          v1alpha1.TaskPhaseRunning,
						Samples:        0,
						SamplesPlanned: 2,
						LogPath:        "",
					},
				},
			},
		}

		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{ID: "task-probe-rescue", Spec: "eval.py@rescue"},
			},
		}

		dirtySB := &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      discovery.TaskSandboxNameForRun("second-run", "task-probe-rescue"),
				Namespace: "eval-runner-sandbox",
				Labels: map[string]string{
					discovery.LabelRunName:      "second-run",
					discovery.LabelRunNamespace: "eval-runner",
					discovery.LabelRole:         discovery.RoleSample,
				},
				Annotations: map[string]string{
					discovery.AnnotationTaskID:             "task-probe-rescue",
					discovery.AnnotationControllerInstance: "instance-old",
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(run, dirtySB).
			WithStatusSubresource(run).
			Build()

		reconciler := &RunReconciler{
			Client:           cl,
			Scheme:           scheme,
			ArtifactSink:     sink,
			SandboxNamespace: "eval-runner-sandbox",
			InstanceID:       "instance-new",
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "second-run"}}
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updatedRun v1alpha1.Run
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get updated run: %v", err)
		}

		task := updatedRun.Status.Tasks[0]
		if task.Phase != v1alpha1.TaskPhasePending {
			t.Errorf("Expected task to be reset to Pending, got phase %s (msg: %s)", task.Phase, task.Message)
		}
		if task.Restarts != 1 {
			t.Errorf("Expected task restarts to be 1, got %d", task.Restarts)
		}
	})
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

	// Verify task now restarts in fresh sandbox
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run after Turn 3: %v", err)
	}
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhasePending {
		t.Errorf("Expected task to restart to Pending after grace period elapsed, got %s", updatedRun.Status.Tasks[0].Phase)
	}
	if updatedRun.Status.Tasks[0].Restarts != 1 {
		t.Errorf("Expected task Restarts 1, got %d", updatedRun.Status.Tasks[0].Restarts)
	}
	if updatedRun.Status.Tasks[0].FailureAttempts != 1 {
		t.Errorf("Expected task FailureAttempts 1, got %d", updatedRun.Status.Tasks[0].FailureAttempts)
	}
	if !strings.Contains(updatedRun.Status.Tasks[0].RestartReason, "task sandbox missing") {
		t.Errorf("Expected restart reason to mention sandbox missing, got: %s", updatedRun.Status.Tasks[0].RestartReason)
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

func TestReconcileRunningTaskGenuinelyDeletedRestartsAfterGracePeriod(t *testing.T) {
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

	// Turn 3: Reconcile at t = 35s (past grace period). Genuinely deleted sandbox now causes task to restart in fresh sandbox.
	currentTime = startTime.Add(35 * time.Second)
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 3 error: %v", err)
	}

	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhasePending {
		t.Errorf("Expected genuinely deleted sandbox to restart task to Pending after grace period, got %s", updatedRun.Status.Tasks[0].Phase)
	}
	if updatedRun.Status.Tasks[0].Restarts != 1 {
		t.Errorf("Expected task Restarts 1, got %d", updatedRun.Status.Tasks[0].Restarts)
	}
	if updatedRun.Status.Tasks[0].FailureAttempts != 1 {
		t.Errorf("Expected task FailureAttempts 1, got %d", updatedRun.Status.Tasks[0].FailureAttempts)
	}
	if !strings.Contains(updatedRun.Status.Tasks[0].RestartReason, "task sandbox missing") {
		t.Errorf("Expected restart reason to mention sandbox missing, got: %s", updatedRun.Status.Tasks[0].RestartReason)
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

	// At t = 35s (past grace period): restarts
	currentTime = startTime.Add(35 * time.Second)
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error at 35s: %v", err)
	}

	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if updatedRun.Status.Tasks[0].Phase != v1alpha1.TaskPhasePending {
		t.Errorf("Expected task to restart to Pending after grace period, got %s", updatedRun.Status.Tasks[0].Phase)
	}
	if updatedRun.Status.Tasks[0].Restarts != 1 {
		t.Errorf("Expected task Restarts 1, got %d", updatedRun.Status.Tasks[0].Restarts)
	}
	if updatedRun.Status.Tasks[0].FailureAttempts != 1 {
		t.Errorf("Expected task FailureAttempts 1, got %d", updatedRun.Status.Tasks[0].FailureAttempts)
	}
	if !strings.Contains(updatedRun.Status.Tasks[0].RestartReason, "task sandbox missing") {
		t.Errorf("Expected restart reason to mention sandbox missing, got: %s", updatedRun.Status.Tasks[0].RestartReason)
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
	expectedLogHash := artifacts.ArtifactHash([]byte(inspectLog))
	if t1.LogHash != expectedLogHash {
		t.Errorf("Expected task logHash %q, got %q", expectedLogHash, t1.LogHash)
	}

	// Verify log object exists in fake GCS, stored compressed with gzip
	logObj, exists := fakeGCSClient.GetObject("justinbs-knotai-dev-eval-runner", "runs/abcdef123456/docs/readme_comprehension/2026-10-17_readme_eval.json")
	if !exists {
		t.Fatalf("Log object was not uploaded to GCS")
	}
	if len(logObj.Data) < 2 || logObj.Data[0] != 0x1f || logObj.Data[1] != 0x8b {
		t.Errorf("Expected log object to be stored compressed with gzip magic bytes 0x1f, 0x8b")
	}
	if logObj.ContentEncoding != "gzip" {
		t.Errorf("Expected log object ContentEncoding %q, got %q", "gzip", logObj.ContentEncoding)
	}
	if logObj.Metadata == nil || logObj.Metadata["sha256"] != expectedLogHash {
		t.Errorf("Expected log object metadata sha256=%q, got %+v", expectedLogHash, logObj.Metadata)
	}
	loadedLog, err := gcsSink.Load(t.Context(), t1.LogPath)
	if err != nil {
		t.Fatalf("Failed to load log via gcsSink.Load: %v", err)
	}
	if string(loadedLog) != inspectLog {
		t.Errorf("gcsSink.Load content mismatch")
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
	if summary.LogHash != expectedLogHash {
		t.Errorf("Expected summary LogHash %q, got %q", expectedLogHash, summary.LogHash)
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

	// Pin the invariant that summary.json and status.tasks[] agree on rate, CI, and sample count
	if t1.SuccessRate == nil || summary.SuccessRate == nil || *summary.SuccessRate != *t1.SuccessRate {
		t.Errorf("SuccessRate mismatch: summary=%v, task=%v", summary.SuccessRate, t1.SuccessRate)
	}
	if t1.SuccessRateCI == nil || summary.SuccessRateCI == nil {
		t.Fatalf("SuccessRateCI missing: summary=%+v, task=%+v", summary.SuccessRateCI, t1.SuccessRateCI)
	}
	if summary.SuccessRateCI.Lower != t1.SuccessRateCI.Lower ||
		summary.SuccessRateCI.Upper != t1.SuccessRateCI.Upper ||
		summary.SuccessRateCI.Confidence != t1.SuccessRateCI.Confidence ||
		summary.SuccessRateCI.Method != t1.SuccessRateCI.Method {
		t.Errorf("SuccessRateCI mismatch: summary=%+v, task=%+v", summary.SuccessRateCI, t1.SuccessRateCI)
	}
	if summary.SampleCount != t1.Samples {
		t.Errorf("SampleCount mismatch: summary.SampleCount=%d, task.Samples=%d", summary.SampleCount, t1.Samples)
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
	if summary.SuccessRateSD == nil {
		t.Errorf("Expected SuccessRateSD to be populated for partial credit task, got nil")
	}
	if len(summary.Samples) != 2 {
		t.Fatalf("Expected 2 samples, got %d", len(summary.Samples))
	}

	// Pin the invariant that summary.json and status.tasks[] agree on rate, CI, and sample count
	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), client.ObjectKeyFromObject(run), &updatedRun); err != nil {
		t.Fatalf("Failed to fetch updated Run: %v", err)
	}
	if len(updatedRun.Status.Tasks) == 0 {
		t.Fatalf("Expected at least 1 task in updated Run status")
	}
	t1 := updatedRun.Status.Tasks[0]

	if t1.SuccessRate == nil || summary.SuccessRate == nil || *summary.SuccessRate != *t1.SuccessRate {
		t.Errorf("SuccessRate mismatch: summary=%v, task=%v", summary.SuccessRate, t1.SuccessRate)
	}
	if t1.SuccessRateCI == nil || summary.SuccessRateCI == nil {
		t.Fatalf("SuccessRateCI missing: summary=%+v, task=%+v", summary.SuccessRateCI, t1.SuccessRateCI)
	}
	if summary.SuccessRateCI.Lower != t1.SuccessRateCI.Lower ||
		summary.SuccessRateCI.Upper != t1.SuccessRateCI.Upper ||
		summary.SuccessRateCI.Confidence != t1.SuccessRateCI.Confidence ||
		summary.SuccessRateCI.Method != t1.SuccessRateCI.Method {
		t.Errorf("SuccessRateCI mismatch: summary=%+v, task=%+v", summary.SuccessRateCI, t1.SuccessRateCI)
	}
	if (summary.SuccessRateSD == nil) != (t1.SuccessRateSD == nil) ||
		(summary.SuccessRateSD != nil && *summary.SuccessRateSD != *t1.SuccessRateSD) {
		t.Errorf("SuccessRateSD mismatch: summary=%v, task=%v", summary.SuccessRateSD, t1.SuccessRateSD)
	}
	if summary.SampleCount != t1.Samples {
		t.Errorf("SampleCount mismatch: summary.SampleCount=%d, task.Samples=%d", summary.SampleCount, t1.Samples)
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
			Config: ClusterConfig{
				Model: ModelConfig{
					Prices: map[string]ModelPrice{
						"mockllm/model": {InputPer1M: 1.0, OutputPer1M: 2.0},
					},
				},
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
		if task.CostSource != v1alpha1.CostSourceConfigMap {
			t.Errorf("Expected task.CostSource=%q, got %q", v1alpha1.CostSourceConfigMap, task.CostSource)
		}
		if summary.CostSource != v1alpha1.CostSourceConfigMap {
			t.Errorf("Expected summary.CostSource=%q, got %q", v1alpha1.CostSourceConfigMap, summary.CostSource)
		}
		if summary.PossibleSamples != 21 {
			t.Errorf("Expected summary.PossibleSamples=21, got %d", summary.PossibleSamples)
		}
	})

	t.Run("DefaultModel_NoConfigMap_CostAndProvenanceDefault", func(t *testing.T) {
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-run-cost-default",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    "git://git-server/repo.git",
					Commit: "abcdef123456",
				},
				Model: "google/gemini-3.5-flash-lite",
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
			// ModelPrices is nil simulating no ConfigMap configured
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-cost-default"}}

		// Turn 1: Creates pod
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile turn 1 error: %v", err)
		}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      discovery.TaskSandboxNameForRun("test-run-cost-default", "docs/readme_comprehension"),
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
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile turn 2 error: %v", err)
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
			t.Fatalf("Expected non-nil task.CostUSD for default model")
		}
		// Input=100k (at $0.30/1M), Output=20k (at $2.50/1M) -> (100000*0.30 + 20000*2.50)/1e6 = 0.08
		expectedCost := 0.08
		if math.Abs(*task.CostUSD-expectedCost) > 1e-9 {
			t.Errorf("Expected task.CostUSD=%f, got %f", expectedCost, *task.CostUSD)
		}
		if task.CostSource != v1alpha1.CostSourceDefault {
			t.Errorf("Expected task.CostSource=%q, got %q", v1alpha1.CostSourceDefault, task.CostSource)
		}
		if updatedRun.Status.CostUSD == nil || math.Abs(*updatedRun.Status.CostUSD-expectedCost) > 1e-9 {
			t.Errorf("Expected Run.Status.CostUSD=%f, got %v", expectedCost, updatedRun.Status.CostUSD)
		}

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
		if summary.CostSource != v1alpha1.CostSourceDefault {
			t.Errorf("Expected summary.CostSource=%q, got %q", v1alpha1.CostSourceDefault, summary.CostSource)
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
			Config: ClusterConfig{
				Model: ModelConfig{
					Prices: map[string]ModelPrice{
						"other/model": {InputPer1M: 1.0, OutputPer1M: 2.0},
					},
				},
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

func TestReconcileArtifactSink_UploadCapRefused(t *testing.T) {
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

	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected Run phase Failed, got %s", updatedRun.Status.Phase)
	}

	if len(updatedRun.Status.Tasks) != 1 {
		t.Fatalf("Expected 1 task in Run status, got %d", len(updatedRun.Status.Tasks))
	}
	task := updatedRun.Status.Tasks[0]
	if task.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task phase Failed, got %s", task.Phase)
	}
	expectedSize := fmt.Sprintf("%d", len(inspectLog))
	expectedLimit := fmt.Sprintf("%d", capLimit)
	if !strings.Contains(task.Message, "artifact was too large") ||
		!strings.Contains(task.Message, "docs/readme_comprehension") ||
		!strings.Contains(task.Message, "2026-10-17_readme_eval.json") ||
		!strings.Contains(task.Message, expectedSize) ||
		!strings.Contains(task.Message, expectedLimit) {
		t.Errorf("Expected task message to describe refusal with task, file, size %s, and limit %s, got: %s", expectedSize, expectedLimit, task.Message)
	}
}

func TestRunController_AgentExecution_OversizedAgentRecordUnaffected(t *testing.T) {
	scheme := setupScheme(t)
	timeout := 900
	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{{
			ID:        "task-agent-oversized",
			Spec:      "eval.py@valid",
			Image:     "task-image:v1",
			Command:   []string{"verify.sh"},
			Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
			Agent: &v1alpha1.ManifestTaskAgent{
				InstructionPath: "/workspace/out/inst.md",
				TimeoutSeconds:  &timeout,
			},
		}},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "test-run-agent-oversized", Namespace: "eval-runner"},
		Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{Commit: "abcdef123456"}, Model: "mockllm/model"},
		Status:     v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseRunning},
	}

	fakeClient := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "echo output"}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			if path == "/out/results.json" {
				return []byte(`{"schemaVersion": 1, "samples": [{"id": "s1", "passed": true, "score": 1.0}]}`), nil
			}
			if path == "/out/inst.md" || path == "out/inst.md" {
				return []byte("```scripted-bash\ntrue\ntrue\ntrue\ntrue\ntrue\n```"), nil
			}
			return nil, fmt.Errorf("file not found: %s", path)
		},
	}

	fakeGCSClient := fakes.NewFakeStorageClient()
	// Cap limit between enriched results.json (~580 bytes) and agent.json (>900 bytes)
	// so agent.json is refused while enriched results.json still uploads.
	capLimit := int64(750)
	gcsSink, err := gcs.New(t.Context(), "gs://my-bucket",
		gcs.WithStorageClient(fakeGCSClient),
		gcs.WithMaxUploadBytes(capLimit),
	)
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	scriptedModel := agent.NewScriptedModel()
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).WithStatusSubresource(run).Build()
	reconciler := &RunReconciler{
		Client: cl, Scheme: scheme, SandboxNamespace: "eval-runner-sandbox",
		SandboxClient: fakeClient,
		ArtifactSink:  gcsSink,
		AgentModel:    scriptedModel,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}

	// Turn 1: create sandbox pod
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 1 error: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.TaskSandboxNameForRun(run.Name, "task-agent-oversized"),
			Namespace: "eval-runner-sandbox",
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      "10.244.0.52",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if err := cl.Create(t.Context(), pod); err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	// Turn 2: executes agent loop (oversized agent.json refused) then verify command, writes results.json
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("turn 2 error: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected Requeue to check completion")
	}

	// Turn 3: Run marked Completed
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 3 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("failed to get updated run: %v", err)
	}

	// Task result is unaffected by agent record refusal: phase must be Completed
	if updatedRun.Status.Phase != v1alpha1.RunPhaseCompleted {
		t.Errorf("expected Run phase Completed, got %s", updatedRun.Status.Phase)
	}
	if len(updatedRun.Status.Tasks) != 1 {
		t.Fatalf("expected 1 task in Run status, got %d", len(updatedRun.Status.Tasks))
	}
	task := updatedRun.Status.Tasks[0]
	if task.Phase != v1alpha1.TaskPhaseCompleted {
		t.Errorf("expected task Phase Completed despite agent record refusal, got %s (message: %s)", task.Phase, task.Message)
	}
	if _, exists := fakeGCSClient.GetObject("my-bucket", "runs/abcdef123456/task-agent-oversized/agent.json"); exists {
		t.Errorf("expected oversized agent.json to be refused and not uploaded to GCS")
	}
	if _, exists := fakeGCSClient.GetObject("my-bucket", "runs/abcdef123456/task-agent-oversized/results.json"); !exists {
		t.Errorf("expected enriched results.json to be uploaded to GCS")
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
	if t1.SuccessRateSD != nil {
		t.Errorf("Expected nil SuccessRateSD for binary task, got %v", t1.SuccessRateSD)
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
	if t1.SuccessRateSD == nil {
		t.Fatalf("Expected non-nil successRateSD")
	}
	if expectedEst.SD == nil || math.Abs(*t1.SuccessRateSD-*expectedEst.SD) > 1e-6 {
		t.Errorf("Expected SD %v, got %v", expectedEst.SD, t1.SuccessRateSD)
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
		Config:                   ClusterConfig{PollProgressInterval: 10 * time.Millisecond},
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
		Config: ClusterConfig{
			PollProgressInterval: 2 * time.Second,
		},
	}
	if rConfigured.pollProgressInterval() != 2*time.Second {
		t.Errorf("expected pollProgressInterval 2s, got %v", rConfigured.pollProgressInterval())
	}
	if rConfigured.minProgressWriteInterval() != 2*time.Second {
		t.Errorf("expected minProgressWriteInterval 2s following pollProgressInterval, got %v", rConfigured.minProgressWriteInterval())
	}

	rOverridden := &RunReconciler{
		Config: ClusterConfig{
			PollProgressInterval: 2 * time.Second,
		},
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
		Client: cl,
		Scheme: scheme,
		Now:    nowFn,
		Config: ClusterConfig{
			PollProgressInterval: 2 * time.Second,
		},
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
			Client: cl,
			Scheme: scheme,
			Now:    func() time.Time { return startTime.Add(20 * time.Second) },
			Config: ClusterConfig{
				PollProgressInterval: 10 * time.Millisecond,
			},
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
			Client: cl,
			Scheme: scheme,
			Now:    func() time.Time { return startTime.Add(20 * time.Second) },
			Config: ClusterConfig{
				PollProgressInterval: 10 * time.Millisecond,
			},
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
		Client:           cl,
		Scheme:           scheme,
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
		Now:              nowFn,
		Config: ClusterConfig{
			PollProgressInterval: 10 * time.Millisecond,
		},
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

func TestReconcile_HungExecuteStepTimesOutAndFailsRun(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-hung-exec"
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
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun(runName)
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
			Labels:    sb.Spec.PodTemplate.ObjectMeta.Labels,
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

	// Fake client where step 1 & 2 succeed, but step 3 (./.evals/list) never returns
	hungCh := make(chan struct{})
	defer close(hungCh)
	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 0 && (cfg.Command[0] == "./.evals/list" || strings.Contains(strings.Join(cfg.Command, " "), "./.evals/list")) {
				// Simulates a hung Execute call that ignores ctx.Done() and never returns
				<-hungCh
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
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
		Config:               ClusterConfig{DiscoveryDeadline: 50 * time.Millisecond},
		DiscoveryStepTimeout: 50 * time.Millisecond,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	start := time.Now()
	res, err := reconciler.Reconcile(t.Context(), req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue on step timeout failure, got %v", res.RequeueAfter)
	}
	if elapsed > 3*time.Second {
		t.Errorf("Reconcile took too long (%v), expected ~50ms", elapsed)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	expectedMsg := "step ./.evals/list failed: sandbox did not report completion"
	if !strings.Contains(updatedRun.Status.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got: %s", expectedMsg, updatedRun.Status.Message)
	}

	// Verify sandbox is deleted
	var checkSb sandboxv1beta1.Sandbox
	err = client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected sandbox to be deleted on failure, got err: %v", err)
	}
}

func TestReconcile_HungExecuteDoesNotBlockOtherRunsFromReconciling(t *testing.T) {
	scheme := setupScheme(t)

	hungCh := make(chan struct{})
	defer close(hungCh)

	const hungRunCount = 4
	var objects []client.Object

	for i := 1; i <= hungRunCount; i++ {
		name := fmt.Sprintf("hung-run-%d", i)
		r := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
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
		sbName := discovery.SandboxNameForRun(name)
		sb := discovery.BuildSandbox(r, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbName,
				Namespace: "eval-runner-sandbox",
				Labels:    sb.Spec.PodTemplate.ObjectMeta.Labels,
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: fmt.Sprintf("10.244.0.%d", 10+i),
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		objects = append(objects, r, sb, pod)
	}

	// Normal run 5
	normalRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "normal-run",
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
	normalSBName := discovery.SandboxNameForRun("normal-run")
	normalSB := discovery.BuildSandbox(normalRun, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))
	normalPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      normalSBName,
			Namespace: "eval-runner-sandbox",
			Labels:    normalSB.Spec.PodTemplate.ObjectMeta.Labels,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.99",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	objects = append(objects, normalRun, normalSB, normalPod)

	manifestJSON := `{"tasks":[{"id":"t1","spec":"run.py@t1"}]}`

	factory := sandboxclient.FactoryFunc(func(podIP string, opts ...sandboxclient.Option) (sandboxclient.Client, error) {
		if podIP == "10.244.0.99" {
			// Normal run succeeds immediately
			return &sandboxclient.FakeClient{
				ListStdout:    manifestJSON,
				ManifestBytes: []byte(manifestJSON),
			}, nil
		}
		// Hung runs hang on step 3
		return &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				if len(cfg.Command) > 0 && (cfg.Command[0] == "./.evals/list" || strings.Contains(strings.Join(cfg.Command, " "), "./.evals/list")) {
					<-hungCh
					return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
				}
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			},
		}, nil
	})

	builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...)
	for i := 1; i <= hungRunCount; i++ {
		name := fmt.Sprintf("hung-run-%d", i)
		var r v1alpha1.Run
		r.Name = name
		r.Namespace = "eval-runner"
		builder = builder.WithStatusSubresource(&r)
	}
	builder = builder.WithStatusSubresource(normalRun)
	cl := builder.Build()

	reconciler := &RunReconciler{
		Client:               cl,
		Scheme:               scheme,
		ClientFactory:        factory,
		SandboxNamespace:     "eval-runner-sandbox",
		Config:               ClusterConfig{DiscoveryDeadline: 50 * time.Millisecond},
		DiscoveryStepTimeout: 50 * time.Millisecond,
	}

	// Reconcile all 4 hung runs in parallel goroutines (representing the 4 MaxConcurrentReconciles workers)
	var wg sync.WaitGroup
	for i := 1; i <= hungRunCount; i++ {
		wg.Add(1)
		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: fmt.Sprintf("hung-run-%d", i)}}
		go func() {
			defer wg.Done()
			_, _ = reconciler.Reconcile(t.Context(), req)
		}()
	}

	// Wait for the hung reconciles to finish
	wg.Wait()

	// Verify all 4 hung runs failed with sandbox did not report completion
	for i := 1; i <= hungRunCount; i++ {
		name := fmt.Sprintf("hung-run-%d", i)
		var r v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: name}, &r); err != nil {
			t.Fatalf("Failed to get %s: %v", name, err)
		}
		if r.Status.Phase != v1alpha1.RunPhaseFailed {
			t.Errorf("Expected %s phase Failed, got %s", name, r.Status.Phase)
		}
		if !strings.Contains(r.Status.Message, "sandbox did not report completion") {
			t.Errorf("Expected %s message to contain 'sandbox did not report completion', got %s", name, r.Status.Message)
		}
	}

	// Reconcile normal run - workers are not pinned and normal run reconciles successfully
	normalReq := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "normal-run"}}
	res, err := reconciler.Reconcile(t.Context(), normalReq)
	if err != nil {
		t.Fatalf("Reconcile normal run error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue on normal run success, got %v", res.RequeueAfter)
	}

	var updatedNormalRun v1alpha1.Run
	if err := cl.Get(t.Context(), normalReq.NamespacedName, &updatedNormalRun); err != nil {
		t.Fatalf("Failed to get normal run: %v", err)
	}
	if updatedNormalRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
		t.Errorf("Expected normal run phase Discovered, got %s", updatedNormalRun.Status.Phase)
	}
}

func TestReconcile_HungGitCloneStepTimesOutAndFailsRun(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-hung-clone"
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
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun(runName)
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
			Labels:    sb.Spec.PodTemplate.ObjectMeta.Labels,
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

	hungCh := make(chan struct{})
	defer close(hungCh)
	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if subcommand := gitCommandSubcommand(cfg.Command); subcommand == "fetch" || subcommand == "clone" {
				<-hungCh
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
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
		DiscoveryStepTimeout: 50 * time.Millisecond,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
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
	expectedMsg := "step git clone failed: sandbox did not report completion"
	if !strings.Contains(updatedRun.Status.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got: %s", expectedMsg, updatedRun.Status.Message)
	}
}

func TestReconcile_HungGitCheckoutStepTimesOutAndFailsRun(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-hung-checkout"
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
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun(runName)
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
			Labels:    sb.Spec.PodTemplate.ObjectMeta.Labels,
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

	hungCh := make(chan struct{})
	defer close(hungCh)
	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if gitCommandSubcommand(cfg.Command) == "checkout" {
				<-hungCh
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
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
		DiscoveryStepTimeout: 50 * time.Millisecond,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
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
	expectedMsg := "step git checkout failed: sandbox did not report completion"
	if !strings.Contains(updatedRun.Status.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got: %s", expectedMsg, updatedRun.Status.Message)
	}
}

func TestReconcile_DiscoveryWatchdogFailsExpiredRun(t *testing.T) {
	scheme := setupScheme(t)

	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	pastDeadline := metav1.NewTime(now.Add(-5 * time.Minute))

	runName := "test-run-watchdog-expiry"
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
			Phase:             v1alpha1.RunPhaseDiscovering,
			DiscoveryDeadline: &pastDeadline,
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
		SandboxNamespace: "eval-runner-sandbox",
		Now:              func() time.Time { return now },
	}

	// Trigger watchdog check directly
	failedCount, err := reconciler.CheckDiscoveryDeadlines(t.Context())
	if err != nil {
		t.Fatalf("CheckDiscoveryDeadlines error: %v", err)
	}
	if failedCount != 1 {
		t.Errorf("Expected 1 failed run from watchdog, got %d", failedCount)
	}

	var updatedRun v1alpha1.Run
	if err := client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: runName}, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "Discovery deadline exceeded") {
		t.Errorf("Expected message to contain 'Discovery deadline exceeded', got: %s", updatedRun.Status.Message)
	}

	// Verify sandbox was deleted
	var checkSb sandboxv1beta1.Sandbox
	err = client.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected sandbox to be deleted by watchdog, got err: %v", err)
	}
}

func TestReconcile_HungExecuteInTaskExecutionTimesOut(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-task-hung"
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
			Model: "mockllm/test",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:    "task-hung",
					Phase: v1alpha1.TaskPhasePending,
				},
			},
		},
	}

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-hung", Spec: "eval.py@hung"},
		},
	}

	hungCh := make(chan struct{})
	defer close(hungCh)
	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if subcommand := gitCommandSubcommand(cfg.Command); subcommand == "fetch" || subcommand == "clone" {
				<-hungCh
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:               cl,
		Scheme:               scheme,
		SandboxClient:        executor,
		SandboxNamespace:     "eval-runner-sandbox",
		ExecutionStepTimeout: 50 * time.Millisecond,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}

	// Turn 1: creates task sandbox
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 1 error: %v", err)
	}

	// Create ready pod for the task sandbox
	taskSBName := discovery.TaskSandboxNameForRun(runName, "task-hung")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskSBName,
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
	_ = cl.Create(t.Context(), pod)

	// Turn 2: executes task; git clone hangs until step deadline (50ms)
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}

	var updatedRun v1alpha1.Run
	_ = cl.Get(t.Context(), req.NamespacedName, &updatedRun)
	if len(updatedRun.Status.Tasks) == 0 {
		t.Fatalf("Expected tasks in status")
	}
	task := updatedRun.Status.Tasks[0]
	if task.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("Expected task phase Failed, got %s", task.Phase)
	}
	expectedMsg := "step git clone failed: sandbox did not report completion"
	if !strings.Contains(task.Message, expectedMsg) {
		t.Errorf("Expected task message to contain %q, got: %s", expectedMsg, task.Message)
	}
}

type fakeBlockingOrFailingSink struct {
	storeManifestFn func(ctx context.Context, meta artifacts.RunMetadata, data []byte) (string, error)
	storeArtifactFn func(ctx context.Context, meta artifacts.RunMetadata, filename string, data []byte) (artifacts.StoredArtifact, error)
}

func (s *fakeBlockingOrFailingSink) StoreManifest(ctx context.Context, meta artifacts.RunMetadata, data []byte) (string, error) {
	if s.storeManifestFn != nil {
		return s.storeManifestFn(ctx, meta, data)
	}
	return "gs://bucket/manifest.json", nil
}

func (s *fakeBlockingOrFailingSink) StoreArtifact(ctx context.Context, meta artifacts.RunMetadata, filename string, data []byte) (artifacts.StoredArtifact, error) {
	if s.storeArtifactFn != nil {
		return s.storeArtifactFn(ctx, meta, filename, data)
	}
	return artifacts.StoredArtifact{Location: "gs://bucket/artifact.json", Hash: "sha256:fake"}, nil
}

func (s *fakeBlockingOrFailingSink) StoreComparison(ctx context.Context, comparisonName string, data []byte) (string, error) {
	return "gs://bucket/comparison.json", nil
}

func (s *fakeBlockingOrFailingSink) Load(ctx context.Context, path string) ([]byte, error) {
	return []byte(`{"tasks":[{"id":"task-1","spec":"eval.py@task1"}]}`), nil
}

func (s *fakeBlockingOrFailingSink) OpenStream(ctx context.Context, path string, hash string) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}

func (s *fakeBlockingOrFailingSink) ArtifactPrefix(commit string) string {
	return "gs://bucket/runs/" + commit + "/"
}

func (s *fakeBlockingOrFailingSink) IsDurable() bool {
	return true
}

func (s *fakeBlockingOrFailingSink) Close() error {
	return nil
}

func TestReconcile_HungStoreManifestStepTimesOutAndFailsRun(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-hung-store-manifest"
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
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun(runName)
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
			Labels:    sb.Spec.PodTemplate.ObjectMeta.Labels,
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
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			return []byte(`{"tasks":[{"id":"task-1","spec":"eval.py@task1"}]}`), nil
		},
	}

	hungCh := make(chan struct{})
	defer close(hungCh)
	sink := &fakeBlockingOrFailingSink{
		storeManifestFn: func(ctx context.Context, meta artifacts.RunMetadata, data []byte) (string, error) {
			<-hungCh
			return "gs://bucket/manifest.json", nil
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:               cl,
		Scheme:               scheme,
		SandboxClient:        executor,
		SandboxNamespace:     "eval-runner-sandbox",
		ArtifactSink:         sink,
		Config:               ClusterConfig{DiscoveryDeadline: 10 * time.Minute},
		DiscoveryStepTimeout: 50 * time.Millisecond,
		ArtifactSinkTimeout:  50 * time.Millisecond,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	start := time.Now()
	res, err := reconciler.Reconcile(t.Context(), req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue on step timeout failure, got %v", res.RequeueAfter)
	}
	if elapsed > 3*time.Second {
		t.Errorf("Reconcile took too long (%v), expected ~50ms", elapsed)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "step store manifest failed") {
		t.Errorf("Expected message to contain 'step store manifest failed', got: %s", updatedRun.Status.Message)
	}
	if !strings.Contains(updatedRun.Status.Message, "runs/abc1234/manifest.json") {
		t.Errorf("Expected message to contain manifest target 'runs/abc1234/manifest.json', got: %s", updatedRun.Status.Message)
	}
	if !strings.Contains(updatedRun.Status.Message, "sink did not report completion within deadline") {
		t.Errorf("Expected message to contain 'sink did not report completion within deadline', got: %s", updatedRun.Status.Message)
	}

	// Verify sandbox is deleted
	var checkSb sandboxv1beta1.Sandbox
	err = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner-sandbox", Name: sandboxName}, &checkSb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected sandbox to be deleted on failure, got err: %v", err)
	}
}

func TestReconcile_StoreManifestErrorSurfacedVerbatim(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-fail-store-manifest"
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
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun(runName)
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
			Labels:    sb.Spec.PodTemplate.ObjectMeta.Labels,
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
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			return []byte(`{"tasks":[{"id":"task-1","spec":"eval.py@task1"}]}`), nil
		},
	}

	fakeErr := errors.New("credentials: cannot fetch token: dial tcp 169.254.169.254:80: connect: connection refused")
	sink := &fakeBlockingOrFailingSink{
		storeManifestFn: func(ctx context.Context, meta artifacts.RunMetadata, data []byte) (string, error) {
			return "", fakeErr
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
		SandboxClient:    executor,
		SandboxNamespace: "eval-runner-sandbox",
		ArtifactSink:     sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "step store manifest failed") {
		t.Errorf("Expected message to name sink operation, got: %s", updatedRun.Status.Message)
	}
	if !strings.Contains(updatedRun.Status.Message, fakeErr.Error()) {
		t.Errorf("Expected message to surface underlying error verbatim (%q), got: %s", fakeErr.Error(), updatedRun.Status.Message)
	}
}

func TestReconcile_UnavailableArtifactSinkFailsFast(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-unavail-sink"
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
			Phase: v1alpha1.RunPhasePending,
		},
	}

	unavailErr := errors.New("dial tcp 169.254.169.254:80: connect: connection refused")
	sink := artifacts.NewUnavailableSink("eval-bucket", unavailErr)

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:           cl,
		Scheme:           scheme,
		SandboxNamespace: "eval-runner-sandbox",
		ArtifactSink:     sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	expectedMsg := "artifactBucket is set but artifact storage is unavailable: dial tcp 169.254.169.254:80: connect: connection refused"
	if !strings.Contains(updatedRun.Status.Message, expectedMsg) {
		t.Errorf("Expected message to contain %q, got: %s", expectedMsg, updatedRun.Status.Message)
	}

	// Verify no sandbox was created
	var sbList sandboxv1beta1.SandboxList
	if err := cl.List(t.Context(), &sbList); err != nil {
		t.Fatalf("Failed to list sandboxes: %v", err)
	}
	if len(sbList.Items) > 0 {
		t.Errorf("Expected no sandboxes to be created, got %d", len(sbList.Items))
	}
}

func TestReconcile_HungStoreArtifactStepTimesOut(t *testing.T) {
	scheme := setupScheme(t)

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-1", Spec: "eval.py@task1", Workdir: "evals"},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-hung-artifact",
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

	hungCh := make(chan struct{})
	defer close(hungCh)
	sink := &fakeBlockingOrFailingSink{
		storeArtifactFn: func(ctx context.Context, meta artifacts.RunMetadata, filename string, data []byte) (artifacts.StoredArtifact, error) {
			<-hungCh
			return artifacts.StoredArtifact{Location: "gs://bucket/artifact.json", Hash: "sha256:fake"}, nil
		},
	}

	executor := &sandboxclient.FakeClient{
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			if len(cfg.Command) > 0 && cfg.Command[0] == "ls" {
				return &sandboxclient.ExecuteResult{ExitCode: 0, Stdout: "task-1.json\n"}, nil
			}
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			return []byte(`{"eval":{"dataset":{"samples":1}},"samples":[{"id":1,"score":{"value":1.0}}]}`), nil
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:              cl,
		Scheme:              scheme,
		SandboxNamespace:    "eval-runner-sandbox",
		SandboxClient:       executor,
		ArtifactSink:        sink,
		ArtifactSinkTimeout: 50 * time.Millisecond,
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-run-hung-artifact"}}

	// Turn 1: Creates sandbox
	_, _ = reconciler.Reconcile(t.Context(), req)

	// Create ready pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.TaskSandboxNameForRun("test-run-hung-artifact", "task-1"),
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

	// Turn 2: Executes task; StoreArtifact hangs until timeout (50ms)
	start := time.Now()
	_, err := reconciler.Reconcile(t.Context(), req)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Turn 2 error: %v", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("Reconcile took too long (%v), expected ~50ms", elapsed)
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
	if !strings.Contains(t1.Message, "failed to store artifact") {
		t.Errorf("Expected task message to contain 'failed to store artifact', got: %s", t1.Message)
	}
	if !strings.Contains(t1.Message, "task-1") {
		t.Errorf("Expected task message to contain target 'task-1', got: %s", t1.Message)
	}
	if !strings.Contains(t1.Message, "sink did not report completion within deadline") {
		t.Errorf("Expected task message to contain 'sink did not report completion within deadline', got: %s", t1.Message)
	}
}

func TestReconcile_StoreManifestGraceWindowSurfacesSinkError(t *testing.T) {
	scheme := setupScheme(t)

	runName := "test-run-grace-manifest"
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
			Phase: v1alpha1.RunPhaseDiscovering,
		},
	}

	sandboxName := discovery.SandboxNameForRun(runName)
	sb := discovery.BuildSandbox(run, "eval-runner-sandbox", discovery.AuthTokenDigest("test"))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
			Labels:    sb.Spec.PodTemplate.ObjectMeta.Labels,
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
		ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
			return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			return []byte(`{"tasks":[{"id":"task-1","spec":"eval.py@task1"}]}`), nil
		},
	}

	graceErr := errors.New("gcs retry exhausted: dial tcp 169.254.169.254:80: connect: connection refused")
	sink := &fakeBlockingOrFailingSink{
		storeManifestFn: func(ctx context.Context, meta artifacts.RunMetadata, data []byte) (string, error) {
			<-ctx.Done()
			time.Sleep(10 * time.Millisecond)
			return "", graceErr
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, sb, pod).
		WithStatusSubresource(run).
		Build()

	reconciler := &RunReconciler{
		Client:               cl,
		Scheme:               scheme,
		SandboxClient:        executor,
		SandboxNamespace:     "eval-runner-sandbox",
		ArtifactSink:         sink,
		Config:               ClusterConfig{DiscoveryDeadline: 10 * time.Minute},
		DiscoveryStepTimeout: 30 * time.Millisecond,
		ArtifactSinkTimeout:  30 * time.Millisecond,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
	start := time.Now()
	_, err := reconciler.Reconcile(t.Context(), req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("Reconcile took too long (%v), expected ~40ms", elapsed)
	}

	var updatedRun v1alpha1.Run
	if err := cl.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("Failed to get run: %v", err)
	}
	if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Errorf("Expected phase Failed, got %s", updatedRun.Status.Phase)
	}
	if !strings.Contains(updatedRun.Status.Message, "connection refused") {
		t.Errorf("Expected message to surface sink's own error from grace window, got: %s", updatedRun.Status.Message)
	}
}

func TestDecideRunningTask(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	gracePeriod := 30 * time.Second
	startedWithinGrace := metav1.NewTime(now.Add(-10 * time.Second))
	startedPastGrace := metav1.NewTime(now.Add(-45 * time.Second))
	rescued := &rescuedTaskInfo{logPath: "gs://bucket/task.json"}

	cleanSandbox := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: "run-task", UID: "sandbox-uid"},
	}
	readySandbox := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: "run-task", UID: "sandbox-uid"},
		Status: sandboxv1beta1.SandboxStatus{
			Conditions: []metav1.Condition{
				{Type: string(sandboxv1beta1.SandboxConditionReady), Status: metav1.ConditionTrue},
			},
		},
	}
	finishedSandbox := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: "run-task", UID: "sandbox-uid"},
		Status: sandboxv1beta1.SandboxStatus{
			Conditions: []metav1.Condition{
				{Type: string(sandboxv1beta1.SandboxConditionFinished), Status: metav1.ConditionTrue, Message: "pod exited"},
			},
		},
	}
	runningPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "run-task", UID: "pod-uid"},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.2"},
	}
	readyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "run-task", UID: "pod-uid"},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.2",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}

	testCases := []struct {
		name                string
		task                v1alpha1.TaskStatus
		observation         runningTaskObservation
		wantAction          runningTaskAction
		wantMessage         string
		wantRestartReason   string
		wantCountsAsFailure bool
		wantGraceRemaining  time.Duration
		wantUnmark          bool
	}{
		{
			name:               "missing sandbox within grace period from task StartedAt",
			task:               v1alpha1.TaskStatus{ID: "task-1", StartedAt: &startedWithinGrace},
			wantAction:         runningTaskActionWait,
			wantGraceRemaining: 20 * time.Second,
		},
		{
			name:               "missing sandbox within grace period from secret timestamp",
			task:               v1alpha1.TaskStatus{ID: "task-1"},
			observation:        runningTaskObservation{startedAt: startedWithinGrace.Time},
			wantAction:         runningTaskActionWait,
			wantGraceRemaining: 20 * time.Second,
		},
		{
			name:        "missing sandbox past grace period rescued",
			task:        v1alpha1.TaskStatus{ID: "task-1", StartedAt: &startedPastGrace},
			observation: runningTaskObservation{rescued: rescued},
			wantAction:  runningTaskActionRescue,
		},
		{
			name:                "missing sandbox past grace period retries",
			task:                v1alpha1.TaskStatus{ID: "task-1", StartedAt: &startedPastGrace},
			wantAction:          runningTaskActionRestart,
			wantRestartReason:   "task sandbox missing while in Running phase",
			wantCountsAsFailure: true,
		},
		{
			name:                "missing sandbox past grace period fails after max attempts",
			task:                v1alpha1.TaskStatus{ID: "task-1", StartedAt: &startedPastGrace, FailureAttempts: 2},
			wantAction:          runningTaskActionFail,
			wantMessage:         "failed after 3 attempts: task sandbox missing while in Running phase",
			wantCountsAsFailure: true,
		},
		{
			name:        "dirty sandbox rescued",
			task:        v1alpha1.TaskStatus{ID: "task-1"},
			observation: runningTaskObservation{sandbox: cleanSandbox, sandboxDirty: true, rescued: rescued},
			wantAction:  runningTaskActionRescue,
		},
		{
			name:                "dirty sandbox restarts without counting toward failure bound",
			task:                v1alpha1.TaskStatus{ID: "task-1", FailureAttempts: 2},
			observation:         runningTaskObservation{sandbox: cleanSandbox, sandboxDirty: true},
			wantAction:          runningTaskActionRestart,
			wantRestartReason:   "controller restart",
			wantCountsAsFailure: false,
		},
		{
			name:        "pod not yet created waits",
			task:        v1alpha1.TaskStatus{ID: "task-1"},
			observation: runningTaskObservation{sandbox: cleanSandbox},
			wantAction:  runningTaskActionWait,
		},
		{
			name:        "unusable pod waits",
			task:        v1alpha1.TaskStatus{ID: "task-1"},
			observation: runningTaskObservation{sandbox: cleanSandbox, pod: readyPod, podUsable: false},
			wantAction:  runningTaskActionWait,
		},
		{
			name: "unbound pod without node waits before service portal bind",
			task: v1alpha1.TaskStatus{ID: "task-1"},
			observation: runningTaskObservation{
				sandbox:              cleanSandbox,
				pod:                  &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod-uid"}},
				podUsable:            true,
				servicePortalEnabled: true,
			},
			wantAction: runningTaskActionWait,
		},
		{
			name: "scheduled unbound pod requests service portal bind",
			task: v1alpha1.TaskStatus{ID: "task-1"},
			observation: runningTaskObservation{
				sandbox:              cleanSandbox,
				pod:                  runningPod,
				podUsable:            true,
				servicePortalEnabled: true,
			},
			wantAction: runningTaskActionBindServicePortal,
		},
		{
			name: "failed pod retries and unmarks service portal binding",
			task: v1alpha1.TaskStatus{ID: "task-1"},
			observation: runningTaskObservation{
				sandbox:   cleanSandbox,
				pod:       &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}},
				podUsable: true,
			},
			wantAction:          runningTaskActionRestart,
			wantRestartReason:   "task pod failed",
			wantCountsAsFailure: true,
			wantUnmark:          true,
		},
		{
			name: "failed pod fails after max attempts and unmarks service portal binding",
			task: v1alpha1.TaskStatus{ID: "task-1", FailureAttempts: 2},
			observation: runningTaskObservation{
				sandbox:   cleanSandbox,
				pod:       &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}},
				podUsable: true,
			},
			wantAction:          runningTaskActionFail,
			wantMessage:         "failed after 3 attempts: task pod failed",
			wantCountsAsFailure: true,
			wantUnmark:          true,
		},
		{
			name: "succeeded pod retries and unmarks service portal binding",
			task: v1alpha1.TaskStatus{ID: "task-1"},
			observation: runningTaskObservation{
				sandbox:   cleanSandbox,
				pod:       &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
				podUsable: true,
			},
			wantAction:          runningTaskActionRestart,
			wantRestartReason:   "task pod succeeded unexpectedly before execution",
			wantCountsAsFailure: true,
			wantUnmark:          true,
		},
		{
			name: "init container failure stays terminal even on failed pod",
			task: v1alpha1.TaskStatus{ID: "task-1"},
			observation: runningTaskObservation{
				sandbox: cleanSandbox,
				pod: &corev1.Pod{
					Status: corev1.PodStatus{
						Phase: corev1.PodFailed,
						InitContainerStatuses: []corev1.ContainerStatus{
							{
								Name: "clone",
								State: corev1.ContainerState{
									Terminated: &corev1.ContainerStateTerminated{ExitCode: 128, Message: "clone failed"},
								},
							},
						},
					},
				},
				podUsable: true,
			},
			wantAction:          runningTaskActionFail,
			wantMessage:         "clone failed",
			wantCountsAsFailure: false,
			wantUnmark:          true,
		},
		{
			name: "finished sandbox retries",
			task: v1alpha1.TaskStatus{ID: "task-1"},
			observation: runningTaskObservation{
				sandbox:   finishedSandbox,
				pod:       runningPod,
				podUsable: true,
			},
			wantAction:          runningTaskActionRestart,
			wantRestartReason:   "task sandbox finished: pod exited",
			wantCountsAsFailure: true,
		},
		{
			name: "finished sandbox fails after max attempts",
			task: v1alpha1.TaskStatus{ID: "task-1", FailureAttempts: 2},
			observation: runningTaskObservation{
				sandbox:   finishedSandbox,
				pod:       runningPod,
				podUsable: true,
			},
			wantAction:          runningTaskActionFail,
			wantMessage:         "failed after 3 attempts: task sandbox finished: pod exited",
			wantCountsAsFailure: true,
		},
		{
			name: "ready sandbox with pod IP executes",
			task: v1alpha1.TaskStatus{ID: "task-1"},
			observation: runningTaskObservation{
				sandbox:   readySandbox,
				pod:       runningPod,
				podUsable: true,
			},
			wantAction: runningTaskActionExecute,
		},
		{
			name: "ready pod with pod IP executes",
			task: v1alpha1.TaskStatus{ID: "task-1"},
			observation: runningTaskObservation{
				sandbox:   cleanSandbox,
				pod:       readyPod,
				podUsable: true,
			},
			wantAction: runningTaskActionExecute,
		},
		{
			name: "ready sandbox without pod IP waits",
			task: v1alpha1.TaskStatus{ID: "task-1"},
			observation: runningTaskObservation{
				sandbox:   readySandbox,
				pod:       &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}},
				podUsable: true,
			},
			wantAction: runningTaskActionWait,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			decision := decideRunningTask(testCase.task, testCase.observation, now, gracePeriod, discovery.MaxMessageBytes)
			if decision.action != testCase.wantAction {
				t.Errorf("action = %q, want %q", decision.action, testCase.wantAction)
			}
			if testCase.wantMessage != "" && decision.message != testCase.wantMessage {
				t.Errorf("message = %q, want %q", decision.message, testCase.wantMessage)
			}
			if testCase.wantRestartReason != "" && decision.restartReason != testCase.wantRestartReason {
				t.Errorf("restartReason = %q, want %q", decision.restartReason, testCase.wantRestartReason)
			}
			if decision.countsAsFailure != testCase.wantCountsAsFailure {
				t.Errorf("countsAsFailure = %v, want %v", decision.countsAsFailure, testCase.wantCountsAsFailure)
			}
			if decision.graceRemaining != testCase.wantGraceRemaining {
				t.Errorf("graceRemaining = %v, want %v", decision.graceRemaining, testCase.wantGraceRemaining)
			}
			if decision.unmarkServicePortalPod != testCase.wantUnmark {
				t.Errorf("unmarkServicePortalPod = %v, want %v", decision.unmarkServicePortalPod, testCase.wantUnmark)
			}
		})
	}
}

func TestDecidePendingTasks(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC))
	currentInstance := "controller-current"
	previousInstance := "controller-previous"

	currentSandbox := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name: "run-task-current",
			Annotations: map[string]string{
				discovery.AnnotationControllerInstance: currentInstance,
				discovery.AnnotationTaskAttempt:        "1",
			},
		},
	}
	terminatingSandbox := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "run-task-terminating",
			DeletionTimestamp: &now,
			Annotations: map[string]string{
				discovery.AnnotationControllerInstance: currentInstance,
				discovery.AnnotationTaskAttempt:        "0",
			},
		},
	}
	previousInstanceSandbox := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name: "run-task-previous-instance",
			Annotations: map[string]string{
				discovery.AnnotationControllerInstance: previousInstance,
				discovery.AnnotationTaskAttempt:        "0",
			},
		},
	}
	earlierAttemptSandbox := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name: "run-task-earlier-attempt",
			Annotations: map[string]string{
				discovery.AnnotationControllerInstance: currentInstance,
				discovery.AnnotationTaskAttempt:        "0",
			},
		},
	}

	type expectedDecision struct {
		taskID string
		action pendingTaskAction
	}

	testCases := []struct {
		name           string
		tasks          []v1alpha1.TaskStatus
		observations   map[string]pendingTaskObservation
		availableSlots int
		want           []expectedDecision
	}{
		{
			name: "zero available slots emits no decisions",
			tasks: []v1alpha1.TaskStatus{
				{ID: "task-1", Phase: v1alpha1.TaskPhasePending},
			},
			availableSlots: 0,
			want:           nil,
		},
		{
			name: "starts pending tasks in order up to available slots and skips non-pending tasks",
			tasks: []v1alpha1.TaskStatus{
				{ID: "task-running", Phase: v1alpha1.TaskPhaseRunning},
				{ID: "task-1", Phase: v1alpha1.TaskPhasePending},
				{ID: "task-completed", Phase: v1alpha1.TaskPhaseCompleted},
				{ID: "task-2", Phase: v1alpha1.TaskPhasePending},
				{ID: "task-3", Phase: v1alpha1.TaskPhasePending},
			},
			observations: map[string]pendingTaskObservation{
				"task-3": {sandbox: previousInstanceSandbox},
			},
			availableSlots: 2,
			want: []expectedDecision{
				{taskID: "task-1", action: pendingTaskActionStart},
				{taskID: "task-2", action: pendingTaskActionStart},
			},
		},
		{
			name: "deletes old sandboxes without consuming slots and starts subsequent pending task",
			tasks: []v1alpha1.TaskStatus{
				{ID: "task-terminating", Phase: v1alpha1.TaskPhasePending, Restarts: 0},
				{ID: "task-previous-instance", Phase: v1alpha1.TaskPhasePending, Restarts: 0},
				{ID: "task-old-attempt", Phase: v1alpha1.TaskPhasePending, Restarts: 1},
				{ID: "task-ready", Phase: v1alpha1.TaskPhasePending, Restarts: 0},
				{ID: "task-after-slots", Phase: v1alpha1.TaskPhasePending, Restarts: 0},
			},
			observations: map[string]pendingTaskObservation{
				"task-terminating":       {sandbox: terminatingSandbox},
				"task-previous-instance": {sandbox: previousInstanceSandbox},
				"task-old-attempt":       {sandbox: earlierAttemptSandbox},
				"task-after-slots":       {existingPod: true},
			},
			availableSlots: 1,
			want: []expectedDecision{
				{taskID: "task-terminating", action: pendingTaskActionDeleteOldSandbox},
				{taskID: "task-previous-instance", action: pendingTaskActionDeleteOldSandbox},
				{taskID: "task-old-attempt", action: pendingTaskActionDeleteOldSandbox},
				{taskID: "task-ready", action: pendingTaskActionStart},
			},
		},
		{
			name: "waits for existing pod without consuming a slot",
			tasks: []v1alpha1.TaskStatus{
				{ID: "task-pod-terminating", Phase: v1alpha1.TaskPhasePending},
				{ID: "task-ready", Phase: v1alpha1.TaskPhasePending},
			},
			observations: map[string]pendingTaskObservation{
				"task-pod-terminating": {existingPod: true},
			},
			availableSlots: 1,
			want: []expectedDecision{
				{taskID: "task-pod-terminating", action: pendingTaskActionWait},
				{taskID: "task-ready", action: pendingTaskActionStart},
			},
		},
		{
			name: "reuses current-instance current-attempt sandbox and consumes a slot",
			tasks: []v1alpha1.TaskStatus{
				{ID: "task-reuse", Phase: v1alpha1.TaskPhasePending, Restarts: 1},
				{ID: "task-next", Phase: v1alpha1.TaskPhasePending},
			},
			observations: map[string]pendingTaskObservation{
				"task-reuse": {sandbox: currentSandbox},
			},
			availableSlots: 1,
			want: []expectedDecision{
				{taskID: "task-reuse", action: pendingTaskActionStart},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			decisions := decidePendingTasks(testCase.tasks, testCase.observations, testCase.availableSlots, currentInstance)
			if len(decisions) != len(testCase.want) {
				t.Fatalf("len(decisions) = %d, want %d (%+v)", len(decisions), len(testCase.want), decisions)
			}
			for i, want := range testCase.want {
				if decisions[i].task.ID != want.taskID || decisions[i].action != want.action {
					t.Errorf("decisions[%d] = (%q, %q), want (%q, %q)", i, decisions[i].task.ID, decisions[i].action, want.taskID, want.action)
				}
			}
		})
	}
}

func TestDecideAndApplyExecutionResults(t *testing.T) {
	earlierProgress := metav1.NewTime(time.Date(2026, 11, 24, 11, 55, 0, 0, time.UTC))
	applyTime := metav1.NewTime(time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC))
	finishedAt := metav1.NewTime(time.Date(2026, 11, 24, 11, 59, 0, 0, time.UTC))
	limit := 10
	priorCost := 0.25
	completedCost := 0.50
	meanScore := 0.8

	status := &v1alpha1.RunStatus{
		Tasks: []v1alpha1.TaskStatus{
			{
				ID:      "task-already-done",
				Cohort:  v1alpha1.DefaultCohort,
				Phase:   v1alpha1.TaskPhaseCompleted,
				Samples: 4,
				CostUSD: &priorCost,
			},
			{
				ID:          "task-complete",
				Phase:       v1alpha1.TaskPhaseRunning,
				SamplesDone: 2,
			},
			{
				ID:             "task-failed",
				Cohort:         "custom-cohort",
				Phase:          v1alpha1.TaskPhaseRunning,
				SamplesDone:    7,
				SamplesPlanned: 10,
				LastProgressAt: &earlierProgress,
			},
		},
	}

	results := map[string]taskExecutionResult{
		"task-complete": {
			taskID:          "task-complete",
			phase:           v1alpha1.TaskPhaseCompleted,
			samples:         5,
			successes:       4,
			meanScore:       &meanScore,
			costUSD:         &completedCost,
			costSource:      v1alpha1.CostSourceDefault,
			possibleSamples: 20,
			logPath:         "gs://bucket/task-complete.json",
			logHash:         "sha256:abc",
			timeoutSeconds:  120,
			finishedAt:      &finishedAt,
		},
		"task-failed": {
			taskID:     "task-failed",
			phase:      v1alpha1.TaskPhaseFailed,
			message:    "step inspect eval failed",
			finishedAt: &finishedAt,
		},
	}

	decisions := decideExecutionResults(status.Tasks, results)
	if len(decisions) != 2 {
		t.Fatalf("len(decisions) = %d, want 2", len(decisions))
	}
	for _, taskID := range []string{"task-complete", "task-failed"} {
		if decisions[taskID].action != executionResultActionApply {
			t.Errorf("decisions[%q].action = %q, want %q", taskID, decisions[taskID].action, executionResultActionApply)
		}
	}

	applyExecutionResultDecisions(status, v1alpha1.RunSpec{Limit: &limit}, decisions, applyTime)

	completedTask := status.Tasks[1]
	if completedTask.Phase != v1alpha1.TaskPhaseCompleted || completedTask.Cohort != v1alpha1.DefaultCohort {
		t.Errorf("completedTask phase/cohort = (%q, %q), want (%q, %q)", completedTask.Phase, completedTask.Cohort, v1alpha1.TaskPhaseCompleted, v1alpha1.DefaultCohort)
	}
	if completedTask.SamplesDone != 5 || completedTask.LastProgressAt == nil || !completedTask.LastProgressAt.Equal(&applyTime) {
		t.Errorf("completedTask progress = (%d, %v), want (5, %v)", completedTask.SamplesDone, completedTask.LastProgressAt, applyTime)
	}
	if completedTask.SamplesPlanned != 10 || completedTask.SampleRange == nil || completedTask.SampleRange.Start != 1 || completedTask.SampleRange.End != 10 {
		t.Errorf("completedTask planned/range = (%d, %+v), want (10, 1..10)", completedTask.SamplesPlanned, completedTask.SampleRange)
	}
	if completedTask.TimeoutSeconds != 120 || completedTask.LogPath != "gs://bucket/task-complete.json" || completedTask.LogHash != "sha256:abc" {
		t.Errorf("completedTask metadata mismatch: %+v", completedTask)
	}

	failedTask := status.Tasks[2]
	if failedTask.Phase != v1alpha1.TaskPhaseFailed || failedTask.Cohort != "custom-cohort" {
		t.Errorf("failedTask phase/cohort = (%q, %q), want (%q, %q)", failedTask.Phase, failedTask.Cohort, v1alpha1.TaskPhaseFailed, "custom-cohort")
	}
	if failedTask.SamplesDone != 7 || failedTask.LastProgressAt == nil || !failedTask.LastProgressAt.Equal(&earlierProgress) {
		t.Errorf("failedTask progress = (%d, %v), want preserved (7, %v)", failedTask.SamplesDone, failedTask.LastProgressAt, earlierProgress)
	}
	if failedTask.SamplesPlanned != 10 {
		t.Errorf("failedTask SamplesPlanned = %d, want preserved 10", failedTask.SamplesPlanned)
	}

	if status.CostUSD == nil || *status.CostUSD != 0.75 {
		t.Errorf("status.CostUSD = %v, want 0.75", status.CostUSD)
	}
	if status.Summary == nil || status.Summary.TasksCompleted != 2 || status.Summary.TasksFailed != 1 {
		t.Errorf("status.Summary = %+v, want 2 completed and 1 failed", status.Summary)
	}
}

func TestTaskStatusGCPProjectClearedOnLifecycleTransitions(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC))
	startedAt := metav1.NewTime(now.Add(-2 * time.Minute))

	t.Run("cleared on restart, fail, and rescue decisions", func(t *testing.T) {
		restarted := v1alpha1.TaskStatus{
			ID:         "task-restart",
			Phase:      v1alpha1.TaskPhaseRunning,
			StartedAt:  &startedAt,
			GCPProject: "eval-proj-1",
		}
		applyRunningTaskDecision(&restarted, runningTaskDecision{
			action:        runningTaskActionRestart,
			restartReason: "sandbox lost",
		}, now)
		if restarted.Phase != v1alpha1.TaskPhasePending || restarted.GCPProject != "" {
			t.Errorf("restarted task = (%q, %q), want (Pending, \"\")", restarted.Phase, restarted.GCPProject)
		}

		failed := v1alpha1.TaskStatus{
			ID:         "task-fail",
			Phase:      v1alpha1.TaskPhaseRunning,
			StartedAt:  &startedAt,
			GCPProject: "eval-proj-2",
		}
		applyRunningTaskDecision(&failed, runningTaskDecision{
			action:  runningTaskActionFail,
			message: "sandbox failed",
		}, now)
		if failed.Phase != v1alpha1.TaskPhaseFailed || failed.GCPProject != "" {
			t.Errorf("failed task = (%q, %q), want (Failed, \"\")", failed.Phase, failed.GCPProject)
		}

		rescued := v1alpha1.TaskStatus{
			ID:         "task-rescue",
			Phase:      v1alpha1.TaskPhaseRunning,
			StartedAt:  &startedAt,
			GCPProject: "eval-proj-3",
		}
		applyRunningTaskDecision(&rescued, runningTaskDecision{
			action:  runningTaskActionRescue,
			rescued: &rescuedTaskInfo{},
		}, now)
		if rescued.Phase != v1alpha1.TaskPhaseCompleted || rescued.GCPProject != "" {
			t.Errorf("rescued task = (%q, %q), want (Completed, \"\")", rescued.Phase, rescued.GCPProject)
		}
	})

	t.Run("cleared when pending task fails to start", func(t *testing.T) {
		status := &v1alpha1.RunStatus{
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-start-fail", Phase: v1alpha1.TaskPhasePending, GCPProject: "eval-proj-4"},
			},
		}
		applyPendingTaskStartOutcomes(status, map[string]pendingTaskStartOutcome{
			"task-start-fail": {started: false, failureMessage: "failed to start"},
		}, now)
		if status.Tasks[0].Phase != v1alpha1.TaskPhaseFailed || status.Tasks[0].GCPProject != "" {
			t.Errorf("task = (%q, %q), want (Failed, \"\")", status.Tasks[0].Phase, status.Tasks[0].GCPProject)
		}
	})

	t.Run("cleared when task finishes execution", func(t *testing.T) {
		task := v1alpha1.TaskStatus{
			ID:         "task-finish",
			Phase:      v1alpha1.TaskPhaseRunning,
			StartedAt:  &startedAt,
			GCPProject: "eval-proj-5",
		}
		applyTaskExecutionResult(&task, v1alpha1.RunSpec{}, taskExecutionResult{
			taskID:     "task-finish",
			phase:      v1alpha1.TaskPhaseCompleted,
			finishedAt: &now,
		}, now)
		if task.Phase != v1alpha1.TaskPhaseCompleted || task.GCPProject != "" {
			t.Errorf("finished task = (%q, %q), want (Completed, \"\")", task.Phase, task.GCPProject)
		}
	})

	t.Run("cleared when tasks are cut off by execution deadline", func(t *testing.T) {
		reconciler := &RunReconciler{
			Now: func() time.Time { return now.Time },
		}
		status := &v1alpha1.RunStatus{
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-cutoff-result", Phase: v1alpha1.TaskPhaseRunning, StartedAt: &startedAt, GCPProject: "eval-proj-6"},
				{ID: "task-running", Phase: v1alpha1.TaskPhaseRunning, StartedAt: &startedAt, GCPProject: "eval-proj-7"},
				{ID: "task-pending", Phase: v1alpha1.TaskPhasePending, GCPProject: "eval-proj-8"},
			},
		}
		reconciler.markDeadlineCutOff(status, 1, map[string]taskExecutionResult{
			"task-cutoff-result": {taskID: "task-cutoff-result", cutOff: true},
		})
		for _, task := range status.Tasks {
			if task.Phase != v1alpha1.TaskPhaseFailed || task.GCPProject != "" {
				t.Errorf("task %q = (%q, %q), want (Failed, \"\")", task.ID, task.Phase, task.GCPProject)
			}
		}
	})

	t.Run("tasks declaring gcp-project without sandbox remain rejected and with sandbox fail closed without config", func(t *testing.T) {
		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{ID: "task-gcp-only", Spec: "eval.py@1", Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeGCPProject}}},
				{ID: "task-sandbox-and-gcp", Spec: "eval.py@2", Image: "img:v1", Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeSandbox}, {Type: v1alpha1.ResourceTypeGCPProject}}},
			},
		}
		reconciler := &RunReconciler{}
		tasks := reconciler.initializeTasks(v1alpha1.RunSpec{}, manifest)
		if tasks[0].Phase != v1alpha1.TaskPhaseFailed || tasks[0].Message != "resources not supported yet" {
			t.Errorf("task %q = (%q, %q), want (Failed, \"resources not supported yet\")", tasks[0].ID, tasks[0].Phase, tasks[0].Message)
		}
		if tasks[1].Phase != v1alpha1.TaskPhaseFailed || !strings.Contains(tasks[1].Message, "service portal") {
			t.Errorf("task %q = (%q, %q), want (Failed, message mentioning service portal)", tasks[1].ID, tasks[1].Phase, tasks[1].Message)
		}
	})
}

func TestKindClusterTaskEligibilityAndSandboxConstruction(t *testing.T) {
	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{
				ID:      "task-sandbox-and-kind",
				Spec:    "eval.py@1",
				Image:   "kube-agents-eval:v1",
				Command: []string{"./hack/kind-up.sh"},
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}},
					{Type: v1alpha1.ResourceTypeKindCluster},
				},
			},
			{
				ID:      "task-kind-then-sandbox",
				Spec:    "eval.py@2",
				Image:   "kube-agents-eval:v1",
				Command: []string{"./hack/kind-up.sh"},
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeKindCluster},
					{Type: v1alpha1.ResourceTypeSandbox},
				},
			},
			{
				ID:        "task-kind-only",
				Spec:      "eval.py@3",
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeKindCluster}},
			},
			{
				ID:    "task-sandbox-kind-gcp",
				Spec:  "eval.py@4",
				Image: "kube-agents-eval:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox},
					{Type: v1alpha1.ResourceTypeKindCluster},
					{Type: v1alpha1.ResourceTypeGCPProject},
				},
			},
			{
				ID:    "task-sandbox-kind-docker",
				Spec:  "eval.py@5",
				Image: "kube-agents-eval:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox},
					{Type: v1alpha1.ResourceTypeKindCluster},
					{Type: "docker"},
				},
			},
			{
				ID:    "task-sandbox-kind-custom-limits",
				Spec:  "eval.py@6",
				Image: "kube-agents-eval:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}},
					{Type: v1alpha1.ResourceTypeKindCluster, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("16Gi")}},
				},
			},
			{
				ID:    "task-sandbox-kind-partial-limits",
				Spec:  "eval.py@7",
				Image: "kube-agents-eval:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox},
					{Type: v1alpha1.ResourceTypeKindCluster, Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("12Gi")}},
				},
			},
			{
				ID:    "task-sandbox-kind-invalid-limits",
				Spec:  "eval.py@8",
				Image: "kube-agents-eval:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox},
					{Type: v1alpha1.ResourceTypeKindCluster, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32")}},
				},
			},
			{
				ID:      "task-sandbox-only",
				Spec:    "eval.py@9",
				Image:   "regular-eval:v1",
				Command: []string{"./run.sh"},
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox},
				},
			},
		},
	}

	reconciler := &RunReconciler{}

	t.Run("initializeTasks accepts sandbox + kind-cluster and rejects unsupported combinations", func(t *testing.T) {
		tasks := reconciler.initializeTasks(v1alpha1.RunSpec{}, manifest)
		if len(tasks) != len(manifest.Tasks) {
			t.Fatalf("len(tasks) = %d, want %d", len(tasks), len(manifest.Tasks))
		}

		if tasks[0].Phase != v1alpha1.TaskPhasePending {
			t.Errorf("task-sandbox-and-kind phase = %q (%q), want Pending", tasks[0].Phase, tasks[0].Message)
		}
		if tasks[0].Resources == nil || tasks[0].Resources.Limits.Cpu().Cmp(resource.MustParse("2")) != 0 || tasks[0].Resources.Limits.Memory().Cmp(resource.MustParse("4Gi")) != 0 {
			t.Errorf("task-sandbox-and-kind resources = %+v, want 2 CPU / 4Gi memory", tasks[0].Resources)
		}

		if tasks[1].Phase != v1alpha1.TaskPhasePending {
			t.Errorf("task-kind-then-sandbox phase = %q (%q), want Pending", tasks[1].Phase, tasks[1].Message)
		}
		if tasks[1].Resources == nil || tasks[1].Resources.Limits.Cpu().Cmp(resource.MustParse(v1alpha1.DefaultTaskCPULimit)) != 0 {
			t.Errorf("task-kind-then-sandbox resources = %+v, want default sandbox limits", tasks[1].Resources)
		}

		if tasks[2].Phase != v1alpha1.TaskPhaseFailed || tasks[2].Message != "resources not supported yet" {
			t.Errorf("task-kind-only = (%q, %q), want (Failed, \"resources not supported yet\")", tasks[2].Phase, tasks[2].Message)
		}

		if tasks[3].Phase != v1alpha1.TaskPhaseFailed || !strings.Contains(tasks[3].Message, "service portal") {
			t.Errorf("task-sandbox-kind-gcp = (%q, %q), want (Failed, message mentioning service portal)", tasks[3].Phase, tasks[3].Message)
		}

		if tasks[4].Phase != v1alpha1.TaskPhaseFailed || tasks[4].Message != "resources not supported yet" {
			t.Errorf("task-sandbox-kind-docker = (%q, %q), want (Failed, \"resources not supported yet\")", tasks[4].Phase, tasks[4].Message)
		}

		if tasks[5].Phase != v1alpha1.TaskPhasePending {
			t.Errorf("task-sandbox-kind-custom-limits phase = %q (%q), want Pending", tasks[5].Phase, tasks[5].Message)
		}
		if tasks[5].Resources == nil || tasks[5].Resources.Limits.Cpu().Cmp(resource.MustParse("2")) != 0 || tasks[5].Resources.Limits.Memory().Cmp(resource.MustParse("4Gi")) != 0 {
			t.Errorf("task-sandbox-kind-custom-limits task resources = %+v, want 2 CPU / 4Gi memory", tasks[5].Resources)
		}

		if tasks[6].Phase != v1alpha1.TaskPhasePending {
			t.Errorf("task-sandbox-kind-partial-limits phase = %q (%q), want Pending", tasks[6].Phase, tasks[6].Message)
		}

		if tasks[7].Phase != v1alpha1.TaskPhaseFailed || !strings.Contains(tasks[7].Message, "exceeds maximum allowed") {
			t.Errorf("task-sandbox-kind-invalid-limits = (%q, %q), want Failed with exceeds maximum allowed error", tasks[7].Phase, tasks[7].Message)
		}

		if tasks[8].Phase != v1alpha1.TaskPhasePending {
			t.Errorf("task-sandbox-only phase = %q (%q), want Pending", tasks[8].Phase, tasks[8].Message)
		}
	})

	t.Run("initializeTasks accepts sandbox + kind-cluster + gcp-project when GCP config is set", func(t *testing.T) {
		configuredReconciler := &RunReconciler{
			ServicePortalClient: &fakeServicePortalClient{},
			Config: ClusterConfig{
				GCPProjectParentFolderID:   "folders/123456",
				GCPProjectBillingAccountID: "012345-6789AB-CDEF01",
			},
		}
		tasks := configuredReconciler.initializeTasks(v1alpha1.RunSpec{
			Tasks: []string{"task-sandbox-kind-gcp"},
		}, manifest)
		if len(tasks) != 1 {
			t.Fatalf("len(tasks) = %d, want 1", len(tasks))
		}
		if tasks[0].Phase != v1alpha1.TaskPhasePending {
			t.Errorf("task-sandbox-kind-gcp phase = %q (%q), want Pending", tasks[0].Phase, tasks[0].Message)
		}
	})

	t.Run("initializeTasks allow-list accepts sandbox + kind-cluster", func(t *testing.T) {
		tasks := reconciler.initializeTasks(v1alpha1.RunSpec{
			Tasks: []string{"task-sandbox-and-kind", "task-kind-only"},
		}, manifest)
		if len(tasks) != 2 {
			t.Fatalf("len(tasks) = %d, want 2", len(tasks))
		}
		if tasks[0].Phase != v1alpha1.TaskPhasePending {
			t.Errorf("task-sandbox-and-kind phase = %q (%q), want Pending", tasks[0].Phase, tasks[0].Message)
		}
		if tasks[1].Phase != v1alpha1.TaskPhaseFailed || tasks[1].Message != "resources not supported yet" {
			t.Errorf("task-kind-only = (%q, %q), want (Failed, \"resources not supported yet\")", tasks[1].Phase, tasks[1].Message)
		}
	})

	t.Run("command-only sandbox + kind-cluster task is eligible for async execution", func(t *testing.T) {
		if !isCommandOnlySandboxTask(&manifest.Tasks[0]) {
			t.Errorf("expected task-sandbox-and-kind to be a command-only sandbox task")
		}
		if isCommandOnlySandboxTask(&manifest.Tasks[2]) {
			t.Errorf("expected task-kind-only not to be a command-only sandbox task")
		}
	})

	t.Run("buildPendingTaskSandbox configures kind nodeSelector, dind sidecar, and DOCKER_HOST only for kind-cluster tasks", func(t *testing.T) {
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "run-kind", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{URL: "https://github.com/example/kube-agents.git", Commit: "abcdef1"},
			},
		}

		kindSandbox := reconciler.buildPendingTaskSandbox(
			run,
			v1alpha1.TaskStatus{ID: manifest.Tasks[0].ID},
			&manifest.Tasks[0],
			"task-run-kind-sandbox",
			"token-123",
		)
		kindPodSpec := kindSandbox.Spec.PodTemplate.Spec
		if got := kindPodSpec.NodeSelector[discovery.KindNodeSelectorLabel]; got != "true" {
			t.Errorf("kind sandbox nodeSelector[%q] = %q, want \"true\"", discovery.KindNodeSelectorLabel, got)
		}
		if len(kindPodSpec.Containers) != 2 || kindPodSpec.Containers[1].Name != discovery.DockerInDockerContainerName {
			t.Fatalf("expected 2 containers with %q sidecar, got %d containers", discovery.DockerInDockerContainerName, len(kindPodSpec.Containers))
		}
		defaultSidecar := kindPodSpec.Containers[1]
		if defaultSidecar.Resources.Requests.Cpu().Cmp(resource.MustParse("500m")) != 0 {
			t.Errorf("default dind CPU request = %v, want 500m", defaultSidecar.Resources.Requests.Cpu())
		}
		if defaultSidecar.Resources.Requests.Memory().Cmp(resource.MustParse("1Gi")) != 0 {
			t.Errorf("default dind Memory request = %v, want 1Gi", defaultSidecar.Resources.Requests.Memory())
		}
		if defaultSidecar.Resources.Limits.Cpu().Cmp(resource.MustParse("4")) != 0 {
			t.Errorf("default dind CPU limit = %v, want 4", defaultSidecar.Resources.Limits.Cpu())
		}
		if defaultSidecar.Resources.Limits.Memory().Cmp(resource.MustParse("8Gi")) != 0 {
			t.Errorf("default dind Memory limit = %v, want 8Gi", defaultSidecar.Resources.Limits.Memory())
		}
		dockerHostFound := false
		for _, environmentVariable := range kindPodSpec.Containers[0].Env {
			if environmentVariable.Name == "DOCKER_HOST" && environmentVariable.Value == discovery.DockerInDockerHost {
				dockerHostFound = true
			}
		}
		if !dockerHostFound {
			t.Errorf("expected DOCKER_HOST=%q in kind task container env", discovery.DockerInDockerHost)
		}

		customKindSandbox := reconciler.buildPendingTaskSandbox(
			run,
			v1alpha1.TaskStatus{ID: manifest.Tasks[5].ID},
			&manifest.Tasks[5],
			"task-run-kind-custom-sandbox",
			"token-custom",
		)
		customKindPodSpec := customKindSandbox.Spec.PodTemplate.Spec
		if len(customKindPodSpec.Containers) != 2 {
			t.Fatalf("expected 2 containers for custom kind limits task, got %d", len(customKindPodSpec.Containers))
		}
		customTaskContainer := customKindPodSpec.Containers[0]
		if customTaskContainer.Resources.Limits.Cpu().Cmp(resource.MustParse("2")) != 0 || customTaskContainer.Resources.Limits.Memory().Cmp(resource.MustParse("4Gi")) != 0 {
			t.Errorf("custom kind task container limits = %v, want 2 CPU / 4Gi memory", customTaskContainer.Resources.Limits)
		}
		customSidecar := customKindPodSpec.Containers[1]
		if customSidecar.Resources.Requests.Cpu().Cmp(resource.MustParse("500m")) != 0 || customSidecar.Resources.Requests.Memory().Cmp(resource.MustParse("1Gi")) != 0 {
			t.Errorf("custom dind requests = %v, want 500m CPU / 1Gi memory", customSidecar.Resources.Requests)
		}
		if customSidecar.Resources.Limits.Cpu().Cmp(resource.MustParse("8")) != 0 || customSidecar.Resources.Limits.Memory().Cmp(resource.MustParse("16Gi")) != 0 {
			t.Errorf("custom dind limits = %v, want 8 CPU / 16Gi memory", customSidecar.Resources.Limits)
		}

		partialKindSandbox := reconciler.buildPendingTaskSandbox(
			run,
			v1alpha1.TaskStatus{ID: manifest.Tasks[6].ID},
			&manifest.Tasks[6],
			"task-run-kind-partial-sandbox",
			"token-partial",
		)
		partialSidecar := partialKindSandbox.Spec.PodTemplate.Spec.Containers[1]
		if partialSidecar.Resources.Limits.Cpu().Cmp(resource.MustParse("4")) != 0 || partialSidecar.Resources.Limits.Memory().Cmp(resource.MustParse("12Gi")) != 0 {
			t.Errorf("partial dind limits = %v, want 4 CPU / 12Gi memory", partialSidecar.Resources.Limits)
		}

		regularSandbox := reconciler.buildPendingTaskSandbox(
			run,
			v1alpha1.TaskStatus{ID: manifest.Tasks[8].ID},
			&manifest.Tasks[8],
			"task-run-regular-sandbox",
			"token-456",
		)
		regularPodSpec := regularSandbox.Spec.PodTemplate.Spec
		if len(regularPodSpec.NodeSelector) != 0 {
			t.Errorf("expected empty nodeSelector on non-kind sandbox, got %v", regularPodSpec.NodeSelector)
		}
		if len(regularPodSpec.Containers) != 1 {
			t.Errorf("expected 1 container on non-kind sandbox, got %d", len(regularPodSpec.Containers))
		}
	})

	t.Run("buildPendingTaskSandbox injects EVAL_IMAGE_REGISTRY and DinD --insecure-registry when registryServicePortalEnabled is true", func(t *testing.T) {
		registryReconciler := &RunReconciler{
			Config: ClusterConfig{
				ServicePortal: ServicePortalOptions{
					RegistryServicePortalEnabled: true,
					ServicePortalsNamespace:      "custom-service-portals",
				},
			},
		}
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "run-kind-registry", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{URL: "https://github.com/example/kube-agents.git", Commit: "abcdef1"},
			},
		}

		wantAddress := "registry-service-portal.custom-service-portals.svc:5000"
		kindSandbox := registryReconciler.buildPendingTaskSandbox(
			run,
			v1alpha1.TaskStatus{ID: manifest.Tasks[0].ID},
			&manifest.Tasks[0],
			"task-run-kind-registry-sandbox",
			"token-registry",
		)
		kindPodSpec := kindSandbox.Spec.PodTemplate.Spec
		if len(kindPodSpec.Containers) != 2 {
			t.Fatalf("expected 2 containers, got %d", len(kindPodSpec.Containers))
		}
		registryEnvironmentVariableFound := false
		for _, environmentVariable := range kindPodSpec.Containers[0].Env {
			if environmentVariable.Name == discovery.EvalImageRegistryEnvironmentVariable && environmentVariable.Value == wantAddress {
				registryEnvironmentVariableFound = true
			}
		}
		if !registryEnvironmentVariableFound {
			t.Errorf("expected %s=%q in task container env, got %+v", discovery.EvalImageRegistryEnvironmentVariable, wantAddress, kindPodSpec.Containers[0].Env)
		}
		wantArgs := []string{"--insecure-registry=" + wantAddress}
		if !reflect.DeepEqual(kindPodSpec.Containers[1].Args, wantArgs) {
			t.Errorf("dind sidecar Args = %v, want %v", kindPodSpec.Containers[1].Args, wantArgs)
		}
	})
}

func gitCommandSubcommand(command []string) string {
	if len(command) < 2 || command[0] != "git" {
		return ""
	}
	if command[1] == "-C" {
		if len(command) >= 4 {
			return command[3]
		}
		return ""
	}
	return command[1]
}

func TestDiscoveryShallowFetchAndFallback(t *testing.T) {
	validManifestJSON := `{"tasks":[{"id":"triage/lookup","spec":"runbook.py@lookup"}]}`

	t.Run("ShallowFetchSuccessSkipsFullClone", func(t *testing.T) {
		fakeClient := &sandboxclient.FakeClient{
			ManifestBytes: []byte(validManifestJSON),
		}
		fixture := newDiscoveringRunFixture(t, fakeClient)

		if _, err := fixture.reconciler.Reconcile(t.Context(), fixture.req); err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updatedRun v1alpha1.Run
		if err := fixture.client.Get(t.Context(), fixture.req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get run: %v", err)
		}
		if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
			t.Fatalf("Expected phase Discovered, got %s (message: %s)", updatedRun.Status.Phase, updatedRun.Status.Message)
		}

		expectedCommands := []string{
			"rm -rf repo",
			"git init repo",
			"git -C repo fetch --depth 1 -- git://git-server/repo.git abc1234",
			"git -C repo checkout --detach FETCH_HEAD",
		}
		if len(fakeClient.Calls) < len(expectedCommands) {
			t.Fatalf("Expected at least %d calls, got %d: %+v", len(expectedCommands), len(fakeClient.Calls), fakeClient.Calls)
		}
		for i, expectedCommand := range expectedCommands {
			gotCommand := strings.Join(fakeClient.Calls[i].Command, " ")
			if gotCommand != expectedCommand {
				t.Errorf("call %d = %q, want %q", i, gotCommand, expectedCommand)
			}
		}
		for _, call := range fakeClient.Calls {
			if gitCommandSubcommand(call.Command) == "clone" {
				t.Errorf("did not expect git clone when shallow fetch succeeds, got call: %v", call.Command)
			}
		}
	})

	t.Run("ShallowFetchRejectedFallsBackToFullCloneAndCheckout", func(t *testing.T) {
		fakeClient := &sandboxclient.FakeClient{
			FetchExit:     128,
			FetchStderr:   "error: Server does not allow request for unadvertised object abc1234",
			ManifestBytes: []byte(validManifestJSON),
		}
		fixture := newDiscoveringRunFixture(t, fakeClient)

		if _, err := fixture.reconciler.Reconcile(t.Context(), fixture.req); err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updatedRun v1alpha1.Run
		if err := fixture.client.Get(t.Context(), fixture.req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get run: %v", err)
		}
		if updatedRun.Status.Phase != v1alpha1.RunPhaseDiscovered {
			t.Fatalf("Expected phase Discovered, got %s (message: %s)", updatedRun.Status.Phase, updatedRun.Status.Message)
		}

		expectedCommands := []string{
			"rm -rf repo",
			"git init repo",
			"git -C repo fetch --depth 1 -- git://git-server/repo.git abc1234",
			"rm -rf repo",
			"git clone -- git://git-server/repo.git repo",
			"git checkout abc1234",
		}
		if len(fakeClient.Calls) < len(expectedCommands) {
			t.Fatalf("Expected at least %d calls, got %d: %+v", len(expectedCommands), len(fakeClient.Calls), fakeClient.Calls)
		}
		for i, expectedCommand := range expectedCommands {
			gotCommand := strings.Join(fakeClient.Calls[i].Command, " ")
			if gotCommand != expectedCommand {
				t.Errorf("call %d = %q, want %q", i, gotCommand, expectedCommand)
			}
		}
		if fakeClient.Calls[5].Cwd != "repo" {
			t.Errorf("expected fallback checkout cwd \"repo\", got %q", fakeClient.Calls[5].Cwd)
		}
	})

	t.Run("ShallowFetchTimeoutDoesNotFallBackToFullClone", func(t *testing.T) {
		hungChannel := make(chan struct{})
		defer close(hungChannel)

		cloneCalled := false
		fakeClient := &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				switch gitCommandSubcommand(cfg.Command) {
				case "fetch":
					<-hungChannel
					return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
				case "clone":
					cloneCalled = true
					return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
				}
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			},
		}
		fixture := newDiscoveringRunFixture(t, fakeClient)
		fixture.reconciler.DiscoveryStepTimeout = 50 * time.Millisecond

		if _, err := fixture.reconciler.Reconcile(t.Context(), fixture.req); err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		if cloneCalled {
			t.Errorf("expected shallow fetch timeout to fail immediately without falling back to full git clone")
		}

		var updatedRun v1alpha1.Run
		if err := fixture.client.Get(t.Context(), fixture.req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("Failed to get run: %v", err)
		}
		if updatedRun.Status.Phase != v1alpha1.RunPhaseFailed {
			t.Fatalf("Expected phase Failed, got %s", updatedRun.Status.Phase)
		}
		expectedMessage := "step git clone failed: sandbox did not report completion"
		if !strings.Contains(updatedRun.Status.Message, expectedMessage) {
			t.Errorf("Expected message containing %q, got %q", expectedMessage, updatedRun.Status.Message)
		}
	})
}

func TestTaskSandboxShallowFetchAndFallback(t *testing.T) {
	scheme := setupScheme(t)
	validInspectLog := []byte(`{"status":"success","results":{"scores":[{"name":"accuracy","metrics":{"accuracy":{"name":"accuracy","value":1.0}}}]},"samples":[{"id":1,"scores":{"accuracy":{"value":"C"}}}]}`)

	runTaskExecution := func(t *testing.T, runName string, fakeClient *sandboxclient.FakeClient) v1alpha1.TaskStatus {
		t.Helper()
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:       runName,
				Namespace:  "eval-runner",
				Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    "https://github.com/gke-labs/kube-agents.git",
					Commit: "0123456789abcdef0123456789abcdef01234567",
				},
				Model: "mockllm/test",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseRunning,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:    "task-shallow",
						Phase: v1alpha1.TaskPhasePending,
					},
				},
			},
		}

		manifest := &v1alpha1.Manifest{
			Tasks: []v1alpha1.ManifestTask{
				{ID: "task-shallow", Spec: "eval.py@task"},
			},
		}

		kubeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(run).
			WithStatusSubresource(run).
			Build()

		reconciler := &RunReconciler{
			Client:               kubeClient,
			Scheme:               scheme,
			SandboxClient:        fakeClient,
			SandboxNamespace:     "eval-runner-sandbox",
			ExecutionStepTimeout: 50 * time.Millisecond,
		}
		reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: runName}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 1 error: %v", err)
		}

		taskSandboxName := discovery.TaskSandboxNameForRun(runName, "task-shallow")
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      taskSandboxName,
				Namespace: "eval-runner-sandbox",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.244.0.21",
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		if err := kubeClient.Create(t.Context(), pod); err != nil {
			t.Fatalf("failed to create task pod: %v", err)
		}

		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("turn 2 error: %v", err)
		}

		var updatedRun v1alpha1.Run
		if err := kubeClient.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
			t.Fatalf("failed to get updated run: %v", err)
		}
		if len(updatedRun.Status.Tasks) != 1 {
			t.Fatalf("expected 1 task status, got %d", len(updatedRun.Status.Tasks))
		}
		return updatedRun.Status.Tasks[0]
	}

	t.Run("ShallowFetchSuccessSkipsFullClone", func(t *testing.T) {
		fakeClient := &sandboxclient.FakeClient{
			InspectLog: validInspectLog,
		}
		task := runTaskExecution(t, "test-task-shallow-success", fakeClient)
		if task.Phase != v1alpha1.TaskPhaseCompleted {
			t.Fatalf("expected task Completed, got %s (message: %s)", task.Phase, task.Message)
		}

		expectedCommands := []string{
			"mkdir -p /workspace/out",
			"rm -rf repo",
			"git init repo",
			"git -C repo fetch --depth 1 -- https://github.com/gke-labs/kube-agents.git 0123456789abcdef0123456789abcdef01234567",
			"git -C repo checkout --detach FETCH_HEAD",
		}
		if len(fakeClient.Calls) < len(expectedCommands) {
			t.Fatalf("expected at least %d calls, got %d: %+v", len(expectedCommands), len(fakeClient.Calls), fakeClient.Calls)
		}
		for i, expectedCommand := range expectedCommands {
			gotCommand := strings.Join(fakeClient.Calls[i].Command, " ")
			if gotCommand != expectedCommand {
				t.Errorf("call %d = %q, want %q", i, gotCommand, expectedCommand)
			}
		}
		for _, call := range fakeClient.Calls {
			if gitCommandSubcommand(call.Command) == "clone" {
				t.Errorf("did not expect git clone when shallow fetch succeeds, got call: %v", call.Command)
			}
		}
	})

	t.Run("ShallowFetchRejectedFallsBackToFullClone", func(t *testing.T) {
		fakeClient := &sandboxclient.FakeClient{
			FetchExit:   128,
			FetchStderr: "error: Server does not allow request for unadvertised object 0123456789abcdef0123456789abcdef01234567",
			InspectLog:  validInspectLog,
		}
		task := runTaskExecution(t, "test-task-shallow-fallback", fakeClient)
		if task.Phase != v1alpha1.TaskPhaseCompleted {
			t.Fatalf("expected task Completed, got %s (message: %s)", task.Phase, task.Message)
		}

		expectedCommands := []string{
			"mkdir -p /workspace/out",
			"rm -rf repo",
			"git init repo",
			"git -C repo fetch --depth 1 -- https://github.com/gke-labs/kube-agents.git 0123456789abcdef0123456789abcdef01234567",
			"rm -rf repo",
			"git clone -- https://github.com/gke-labs/kube-agents.git repo",
			"git checkout 0123456789abcdef0123456789abcdef01234567",
		}
		if len(fakeClient.Calls) < len(expectedCommands) {
			t.Fatalf("expected at least %d calls, got %d: %+v", len(expectedCommands), len(fakeClient.Calls), fakeClient.Calls)
		}
		for i, expectedCommand := range expectedCommands {
			gotCommand := strings.Join(fakeClient.Calls[i].Command, " ")
			if gotCommand != expectedCommand {
				t.Errorf("call %d = %q, want %q", i, gotCommand, expectedCommand)
			}
		}
		if fakeClient.Calls[6].Cwd != "repo" {
			t.Errorf("expected fallback checkout cwd \"repo\", got %q", fakeClient.Calls[6].Cwd)
		}
	})

	t.Run("ShallowFetchTimeoutDoesNotFallBackToFullClone", func(t *testing.T) {
		hungChannel := make(chan struct{})
		defer close(hungChannel)

		cloneCalled := false
		fakeClient := &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				switch gitCommandSubcommand(cfg.Command) {
				case "fetch":
					<-hungChannel
					return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
				case "clone":
					cloneCalled = true
					return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
				}
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			},
		}
		task := runTaskExecution(t, "test-task-shallow-timeout", fakeClient)
		if cloneCalled {
			t.Errorf("expected shallow fetch timeout to fail immediately without falling back to full git clone")
		}
		if task.Phase != v1alpha1.TaskPhaseFailed {
			t.Fatalf("expected task Failed, got %s", task.Phase)
		}
		expectedMessage := "step git clone failed: sandbox did not report completion"
		if !strings.Contains(task.Message, expectedMessage) {
			t.Errorf("expected task message containing %q, got %q", expectedMessage, task.Message)
		}
	})
}

func TestReconcile_ControllerRestartInFlightTaskRunsInFreshSandbox(t *testing.T) {
	scheme := setupScheme(t)
	sink := artifacts.NewInMemorySink()

	const (
		runNamespace       = "eval-runner"
		sandboxNamespace   = "eval-runner-sandbox"
		runName            = "restart-inflight-run"
		taskID             = "docs/readme_comprehension_blind"
		previousInstanceID = "controller-instance-old"
		currentInstanceID  = "controller-instance-new"
	)

	limit := 2
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       runName,
			Namespace:  runNamespace,
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
			Limit: &limit,
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/kube-agents.git",
				Commit: "0123456789abcdef0123456789abcdef01234567",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         1,
				EligibleTaskCount: 1,
				TaskIDs:           []string{taskID},
			},
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:             taskID,
					Phase:          v1alpha1.TaskPhaseRunning,
					SamplesPlanned: 2,
				},
			},
		},
	}

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: taskID, Spec: "evals/docs_comprehension.py@readme_comprehension_blind"},
		},
	}

	sandboxName := discovery.TaskSandboxNameForRun(runName, taskID)
	dirtySandbox := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: sandboxNamespace,
			UID:       "dirty-sandbox-uid",
			Labels: map[string]string{
				discovery.LabelRunName:      runName,
				discovery.LabelRunNamespace: runNamespace,
				discovery.LabelRole:         discovery.RoleSample,
			},
			Annotations: map[string]string{
				discovery.AnnotationTaskID:             taskID,
				discovery.AnnotationControllerInstance: previousInstanceID,
				discovery.AnnotationTaskAttempt:        "0",
			},
		},
		Status: sandboxv1beta1.SandboxStatus{
			ServiceFQDN: sandboxName + "." + sandboxNamespace + ".svc.cluster.local",
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
			},
		},
	}
	dirtyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: sandboxNamespace,
			UID:       "dirty-pod-uid",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.10",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}

	fakeKubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, dirtySandbox, dirtyPod).
		WithStatusSubresource(run, dirtySandbox).
		Build()

	validInspectLog := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "readme_comprehension_blind", "model": "mockllm/model"},
		"samples": [
			{"id": "sample-1", "scores": {"match": {"value": "C"}}},
			{"id": "sample-2", "scores": {"match": {"value": "C"}}}
		],
		"results": {"total_samples": 2, "completed_samples": 2, "scores": [{"name": "match", "metrics": {"accuracy": {"value": 1.0}}}]}
	}`
	fakeSandboxClient := &sandboxclient.FakeClient{
		InspectLog: []byte(validInspectLog),
	}

	reconciler := &RunReconciler{
		Client:           fakeKubeClient,
		Scheme:           scheme,
		ArtifactSink:     sink,
		SandboxNamespace: sandboxNamespace,
		InstanceID:       currentInstanceID,
		SandboxClient:    fakeSandboxClient,
	}
	reconciler.SetCachedManifest(runNamespace, runName, manifest)

	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: runNamespace, Name: runName}}

	// 1. First reconcile after controller restart: dirty sandbox is deleted and task resets to Pending.
	if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
		t.Fatalf("first Reconcile failed: %v", err)
	}

	var sandboxAfterFirst sandboxv1beta1.Sandbox
	if err := fakeKubeClient.Get(t.Context(), types.NamespacedName{Namespace: sandboxNamespace, Name: sandboxName}, &sandboxAfterFirst); !apierrors.IsNotFound(err) {
		t.Fatalf("expected dirty sandbox to be deleted, got err=%v", err)
	}
	// Simulate pod garbage collection after sandbox deletion.
	if err := fakeKubeClient.Delete(t.Context(), dirtyPod); err != nil {
		t.Fatalf("failed to delete dirty pod: %v", err)
	}

	var runAfterFirst v1alpha1.Run
	if err := fakeKubeClient.Get(t.Context(), request.NamespacedName, &runAfterFirst); err != nil {
		t.Fatalf("failed to get Run after first reconcile: %v", err)
	}
	taskAfterFirst := runAfterFirst.Status.Tasks[0]
	if taskAfterFirst.Phase != v1alpha1.TaskPhasePending {
		t.Fatalf("expected task Pending after dirty sandbox deletion, got %s", taskAfterFirst.Phase)
	}
	if taskAfterFirst.Restarts != 1 {
		t.Errorf("expected task Restarts=1, got %d", taskAfterFirst.Restarts)
	}
	if taskAfterFirst.FailureAttempts != 0 {
		t.Errorf("expected task FailureAttempts=0, got %d", taskAfterFirst.FailureAttempts)
	}
	if taskAfterFirst.RestartReason != "controller restart" {
		t.Errorf("expected task RestartReason %q, got %q", "controller restart", taskAfterFirst.RestartReason)
	}

	// 2. Second reconcile: creates a fresh sandbox tagged with the new controller InstanceID and attempt 1.
	if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
		t.Fatalf("second Reconcile failed: %v", err)
	}

	var freshSandbox sandboxv1beta1.Sandbox
	if err := fakeKubeClient.Get(t.Context(), types.NamespacedName{Namespace: sandboxNamespace, Name: sandboxName}, &freshSandbox); err != nil {
		t.Fatalf("expected fresh sandbox to be created: %v", err)
	}
	if gotInstance := freshSandbox.Annotations[discovery.AnnotationControllerInstance]; gotInstance != currentInstanceID {
		t.Errorf("fresh sandbox instance annotation = %q, want %q", gotInstance, currentInstanceID)
	}
	if gotAttempt := freshSandbox.Annotations[discovery.AnnotationTaskAttempt]; gotAttempt != "1" {
		t.Errorf("fresh sandbox attempt annotation = %q, want %q", gotAttempt, "1")
	}

	// Mark the fresh sandbox Ready with a brand-new running Pod.
	freshSandbox.Status.ServiceFQDN = sandboxName + "." + sandboxNamespace + ".svc.cluster.local"
	freshSandbox.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}}
	if err := fakeKubeClient.Status().Update(t.Context(), &freshSandbox); err != nil {
		t.Fatalf("failed to update fresh sandbox status: %v", err)
	}
	freshPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: sandboxNamespace,
			UID:       "fresh-pod-uid",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.20",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	if err := fakeKubeClient.Create(t.Context(), freshPod); err != nil {
		t.Fatalf("failed to create fresh pod: %v", err)
	}

	// 3. Reconcile until the task completes in the fresh sandbox.
	for attempt := 0; attempt < 5; attempt++ {
		if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
			t.Fatalf("reconcile %d failed: %v", attempt+3, err)
		}
		var currentRun v1alpha1.Run
		if err := fakeKubeClient.Get(t.Context(), request.NamespacedName, &currentRun); err != nil {
			t.Fatalf("failed to get Run: %v", err)
		}
		if currentRun.Status.Phase == v1alpha1.RunPhaseCompleted {
			completedTask := currentRun.Status.Tasks[0]
			if completedTask.Phase != v1alpha1.TaskPhaseCompleted {
				t.Errorf("expected task Completed, got %s", completedTask.Phase)
			}
			if completedTask.Restarts != 1 {
				t.Errorf("expected Restarts=1, got %d", completedTask.Restarts)
			}
			if completedTask.FailureAttempts != 0 {
				t.Errorf("expected FailureAttempts=0, got %d", completedTask.FailureAttempts)
			}
			if completedTask.RestartReason != "controller restart" {
				t.Errorf("expected RestartReason %q, got %q", "controller restart", completedTask.RestartReason)
			}
			if completedTask.Samples != 2 || completedTask.SamplesDone != 2 {
				t.Errorf("expected Samples=2 and SamplesDone=2, got Samples=%d SamplesDone=%d", completedTask.Samples, completedTask.SamplesDone)
			}
			return
		}
	}

	var finalRun v1alpha1.Run
	_ = fakeKubeClient.Get(t.Context(), request.NamespacedName, &finalRun)
	t.Fatalf("expected Run to reach Completed in fresh sandbox, got phase=%s task=%+v", finalRun.Status.Phase, finalRun.Status.Tasks)
}

func TestFetchOrCloneRepository(t *testing.T) {
	reconciler := &RunReconciler{}
	repositoryURL := "git://git-server/repo.git"
	commit := "abc1234"

	t.Run("CleanTimeoutReportsTimedOutWithoutContinuing", func(t *testing.T) {
		hungChannel := make(chan struct{})
		defer close(hungChannel)

		var callsMutex sync.Mutex
		var gitCalled bool
		fakeClient := &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				if len(cfg.Command) > 0 && cfg.Command[0] == "rm" {
					<-hungChannel
					return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
				}
				callsMutex.Lock()
				gitCalled = true
				callsMutex.Unlock()
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			},
		}

		result, err := reconciler.fetchOrCloneRepository(t.Context(), fakeClient, nil, 50*time.Millisecond, repositoryURL, commit)
		if !result.timedOut {
			t.Fatalf("expected timedOut=true, got false (err=%v)", err)
		}
		if result.shallowFetched {
			t.Errorf("expected shallowFetched=false on clean timeout")
		}
		if err == nil {
			t.Errorf("expected non-nil error on clean timeout")
		}
		callsMutex.Lock()
		calledAfterClean := gitCalled
		callsMutex.Unlock()
		if calledAfterClean {
			t.Errorf("expected no git commands to run after clean timeout")
		}
	})

	t.Run("CleanUnavailableAndUnauthenticatedPropagateWithoutTimedOut", func(t *testing.T) {
		for _, testCase := range []struct {
			name string
			code codes.Code
		}{
			{name: "Unavailable", code: codes.Unavailable},
			{name: "Unauthenticated", code: codes.Unauthenticated},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				fakeClient := &sandboxclient.FakeClient{
					ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
						return nil, status.Error(testCase.code, "rpc failure")
					},
				}

				result, err := reconciler.fetchOrCloneRepository(t.Context(), fakeClient, nil, time.Second, repositoryURL, commit)
				if result.timedOut {
					t.Errorf("expected timedOut=false for %s error", testCase.name)
				}
				if status.Code(err) != testCase.code {
					t.Errorf("status.Code(err) = %v, want %v", status.Code(err), testCase.code)
				}
				if len(fakeClient.Calls) != 1 {
					t.Errorf("expected only rm call, got %d calls", len(fakeClient.Calls))
				}
			})
		}
	})

	t.Run("CleanNonFatalErrorIgnoredAndShallowFetchSucceeds", func(t *testing.T) {
		fakeClient := &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				if len(cfg.Command) > 0 && cfg.Command[0] == "rm" {
					return nil, errors.New("non-fatal clean error")
				}
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			},
		}

		result, err := reconciler.fetchOrCloneRepository(t.Context(), fakeClient, nil, time.Second, repositoryURL, commit)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result.shallowFetched || result.timedOut {
			t.Errorf("got shallowFetched=%v timedOut=%v, want shallowFetched=true timedOut=false", result.shallowFetched, result.timedOut)
		}
		if result.executeResult == nil || result.executeResult.ExitCode != 0 {
			t.Errorf("expected exit code 0 executeResult, got %+v", result.executeResult)
		}
	})

	t.Run("GitInitTimeoutReportsTimedOutWithoutFallback", func(t *testing.T) {
		hungChannel := make(chan struct{})
		defer close(hungChannel)

		var callsMutex sync.Mutex
		var cloneCalled bool
		fakeClient := &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				switch gitCommandSubcommand(cfg.Command) {
				case "init":
					<-hungChannel
				case "clone":
					callsMutex.Lock()
					cloneCalled = true
					callsMutex.Unlock()
				}
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			},
		}

		result, err := reconciler.fetchOrCloneRepository(t.Context(), fakeClient, nil, 50*time.Millisecond, repositoryURL, commit)
		if !result.timedOut {
			t.Fatalf("expected timedOut=true, got false (err=%v)", err)
		}
		if err == nil {
			t.Errorf("expected non-nil error on git init timeout")
		}
		callsMutex.Lock()
		fallbackCloneCalled := cloneCalled
		callsMutex.Unlock()
		if fallbackCloneCalled {
			t.Errorf("did not expect fallback git clone after git init timeout")
		}
	})

	t.Run("FallbackCloneTimeoutReportsTimedOut", func(t *testing.T) {
		hungChannel := make(chan struct{})
		defer close(hungChannel)

		fakeClient := &sandboxclient.FakeClient{
			ExecFn: func(call int, ctx context.Context, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
				switch gitCommandSubcommand(cfg.Command) {
				case "fetch":
					return &sandboxclient.ExecuteResult{ExitCode: 128, Stderr: "unadvertised object"}, nil
				case "clone":
					<-hungChannel
				}
				return &sandboxclient.ExecuteResult{ExitCode: 0}, nil
			},
		}

		result, err := reconciler.fetchOrCloneRepository(t.Context(), fakeClient, nil, 50*time.Millisecond, repositoryURL, commit)
		if !result.timedOut {
			t.Fatalf("expected timedOut=true, got false (err=%v)", err)
		}
		if result.shallowFetched {
			t.Errorf("expected shallowFetched=false on fallback clone timeout")
		}
		if err == nil {
			t.Errorf("expected non-nil error on fallback clone timeout")
		}
	})

	t.Run("EmptyCommitSkipsShallowFetchAndClonesDirectly", func(t *testing.T) {
		fakeClient := &sandboxclient.FakeClient{}

		result, err := reconciler.fetchOrCloneRepository(t.Context(), fakeClient, nil, time.Second, repositoryURL, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.shallowFetched || result.timedOut {
			t.Errorf("got shallowFetched=%v timedOut=%v, want both false", result.shallowFetched, result.timedOut)
		}
		expectedCommands := []string{
			"rm -rf repo",
			"git clone -- git://git-server/repo.git repo",
		}
		if len(fakeClient.Calls) != len(expectedCommands) {
			t.Fatalf("expected %d calls, got %d: %+v", len(expectedCommands), len(fakeClient.Calls), fakeClient.Calls)
		}
		for i, expectedCommand := range expectedCommands {
			gotCommand := strings.Join(fakeClient.Calls[i].Command, " ")
			if gotCommand != expectedCommand {
				t.Errorf("call %d = %q, want %q", i, gotCommand, expectedCommand)
			}
		}
	})
}
