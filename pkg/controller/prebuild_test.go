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
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/sandboxclient"
)

func createReadySandboxPodWithNode(t *testing.T, kubeClient client.Client, sandboxName, podUID, nodeName, podIP string) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "eval-runner-sandbox",
			UID:       types.UID(podUID),
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: podIP,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	if err := kubeClient.Create(t.Context(), pod); err != nil {
		t.Fatalf("create ready sandbox pod %s: %v", sandboxName, err)
	}
}

func TestInitializePrebuilds_DeduplicatesAndRespectsTaskFilter(t *testing.T) {
	manifest := &v1alpha1.Manifest{
		Prebuilds: map[string]v1alpha1.ManifestPrebuild{
			"images-a": {
				Image:   "builder:v1",
				Command: []string{"./build-a.sh"},
			},
			"images-b": {
				Image:   "builder:v1",
				Command: []string{"./build-b.sh"},
			},
		},
		Tasks: []v1alpha1.ManifestTask{
			{ID: "task-1", Spec: "eval.py@t1", Prebuild: "images-a"},
			{ID: "task-2", Spec: "eval.py@t2", Prebuild: "images-a"},
			{ID: "task-3", Spec: "eval.py@t3", Prebuild: "images-b"},
			{ID: "task-4", Spec: "eval.py@t4"},
		},
	}

	reconciler := &RunReconciler{}
	filteredTasks := reconciler.initializeTasks(v1alpha1.RunSpec{
		Model: "mockllm/model",
		Tasks: []string{"task-1", "task-2", "task-4"},
	}, manifest)

	prebuilds := initializePrebuilds(filteredTasks, manifest)
	if len(prebuilds) != 1 {
		t.Fatalf("len(prebuilds) = %d, want 1 (%+v)", len(prebuilds), prebuilds)
	}
	if prebuilds[0].Name != "images-a" || prebuilds[0].Phase != v1alpha1.PrebuildPhasePending {
		t.Errorf("prebuilds[0] = %+v, want Name=images-a Phase=Pending", prebuilds[0])
	}
}

func TestPrebuild_RunsOnceForMultipleDependentTasksAndGatesUntilComplete(t *testing.T) {
	scheme := setupScheme(t)
	now := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)

	manifest := &v1alpha1.Manifest{
		Prebuilds: map[string]v1alpha1.ManifestPrebuild{
			"kind-images": {
				Image:          "builder:v1",
				Command:        []string{"./scripts/build-images.sh"},
				TimeoutSeconds: 600,
				Resources: []v1alpha1.ManifestResource{
					{
						Type: v1alpha1.ResourceTypeSandbox,
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
					{
						Type: v1alpha1.ResourceTypeKindCluster,
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("4"),
							corev1.ResourceMemory: resource.MustParse("8Gi"),
						},
					},
				},
			},
		},
		Tasks: []v1alpha1.ManifestTask{
			{
				ID:        "task-a",
				Spec:      "eval.py@a",
				Image:     "task-image:v1",
				Command:   []string{"run-task", "a"},
				Prebuild:  "kind-images",
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeSandbox}},
			},
			{
				ID:        "task-b",
				Spec:      "eval.py@b",
				Image:     "task-image:v1",
				Command:   []string{"run-task", "b"},
				Prebuild:  "kind-images",
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeSandbox}},
			},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run-prebuild-gate", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "https://github.com/example/repo.git", Commit: "commit-abc123"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseRunning},
	}

	prebuildPollState := sandboxclient.AsyncTaskRunning
	prebuildExitCode := 0
	fakeSandboxClient := &sandboxclient.FakeClient{
		PollAsyncFn: func(ctx context.Context, executionID string) (*sandboxclient.AsyncTaskStatus, error) {
			return &sandboxclient.AsyncTaskStatus{
				State:    prebuildPollState,
				ExitCode: prebuildExitCode,
			}, nil
		},
	}

	servicePortalClient := &fakeServicePortalClient{}
	sink := artifacts.NewInMemorySink()
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).WithStatusSubresource(run).Build()

	reconciler := &RunReconciler{
		Client:              kubeClient,
		Scheme:              scheme,
		InstanceID:          "controller-1",
		SandboxNamespace:    "eval-runner-sandbox",
		SandboxClient:       fakeSandboxClient,
		ServicePortalClient: servicePortalClient,
		ArtifactSink:        sink,
		Now:                 func() time.Time { return now },
		Config:              ClusterConfig{PollProgressInterval: 5 * time.Second},
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}

	// Turn 1: initialize prebuilds & tasks, create exactly one prebuild sandbox, and gate dependent tasks.
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 1 reconcile error: %v", err)
	}

	var prebuildSandboxes sandboxv1beta1.SandboxList
	if err := kubeClient.List(t.Context(), &prebuildSandboxes, client.InNamespace("eval-runner-sandbox"), client.MatchingLabels{
		discovery.LabelRole: discovery.RolePrebuild,
	}); err != nil {
		t.Fatalf("list prebuild sandboxes: %v", err)
	}
	if len(prebuildSandboxes.Items) != 1 {
		t.Fatalf("expected 1 prebuild sandbox, got %d", len(prebuildSandboxes.Items))
	}
	prebuildSandbox := prebuildSandboxes.Items[0]
	if prebuildSandbox.Annotations[discovery.AnnotationPrebuildName] != "kind-images" {
		t.Errorf("AnnotationPrebuildName = %q, want kind-images", prebuildSandbox.Annotations[discovery.AnnotationPrebuildName])
	}
	if prebuildSandbox.Labels[discovery.LabelServicePortalIntercept] != "true" {
		t.Errorf("expected service portal intercept label on prebuild sandbox")
	}
	if len(prebuildSandbox.Spec.PodTemplate.Spec.Containers) != 2 || prebuildSandbox.Spec.PodTemplate.Spec.Containers[1].Name != discovery.DockerInDockerContainerName {
		t.Errorf("expected kind-cluster DinD sidecar on prebuild sandbox, got %d containers", len(prebuildSandbox.Spec.PodTemplate.Spec.Containers))
	}
	if len(prebuildSandbox.Spec.PodTemplate.Spec.InitContainers) != 2 || prebuildSandbox.Spec.PodTemplate.Spec.InitContainers[1].Name != discovery.RepoCheckoutInitContainerName {
		t.Errorf("expected repo checkout init container on prebuild sandbox")
	}

	var taskSandboxes sandboxv1beta1.SandboxList
	if err := kubeClient.List(t.Context(), &taskSandboxes, client.InNamespace("eval-runner-sandbox"), client.MatchingLabels{
		discovery.LabelRole: discovery.RoleSample,
	}); err != nil {
		t.Fatalf("list task sandboxes: %v", err)
	}
	if len(taskSandboxes.Items) != 0 {
		t.Fatalf("expected 0 task sandboxes while prebuild is running, got %d", len(taskSandboxes.Items))
	}

	var updatedRun v1alpha1.Run
	if err := kubeClient.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("get run after turn 1: %v", err)
	}
	if len(updatedRun.Status.Prebuilds) != 1 || updatedRun.Status.Prebuilds[0].Phase != v1alpha1.PrebuildPhaseRunning {
		t.Fatalf("expected prebuild Running after turn 1, got %+v", updatedRun.Status.Prebuilds)
	}
	for _, task := range updatedRun.Status.Tasks {
		if task.Phase != v1alpha1.TaskPhasePending {
			t.Errorf("task %q should remain Pending while prebuild runs, got %s", task.ID, task.Phase)
		}
	}

	// Make prebuild pod Ready with NodeName and PodIP.
	createReadySandboxPodWithNode(t, kubeClient, prebuildSandbox.Name, "prebuild-pod-uid-1", "node-1", "10.244.0.20")

	// Turn 2: bind service portal policy and start async prebuild command; tasks stay gated.
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 2 reconcile error: %v", err)
	}
	if len(servicePortalClient.binds) != 1 || servicePortalClient.binds[0].podUID != "prebuild-pod-uid-1" {
		t.Errorf("expected 1 service portal bind call for prebuild pod, got %+v", servicePortalClient.binds)
	}
	if len(fakeSandboxClient.StartAsyncCalls) != 1 {
		t.Fatalf("expected 1 StartAsync call for prebuild, got %d", len(fakeSandboxClient.StartAsyncCalls))
	}
	if strings.Join(fakeSandboxClient.StartAsyncCalls[0].Config.Command, " ") != "./scripts/build-images.sh" {
		t.Errorf("unexpected prebuild command: %v", fakeSandboxClient.StartAsyncCalls[0].Config.Command)
	}
	if fakeSandboxClient.StartAsyncCalls[0].Config.Cwd != "repo" {
		t.Errorf("prebuild Cwd = %q, want repo", fakeSandboxClient.StartAsyncCalls[0].Config.Cwd)
	}

	// Turn 3: PollAsync returns Running -> prebuild keeps running and tasks stay gated.
	turn3Result, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("turn 3 reconcile error: %v", err)
	}
	if turn3Result.RequeueAfter != 5*time.Second {
		t.Errorf("turn 3 RequeueAfter = %v, want 5s", turn3Result.RequeueAfter)
	}
	if err := kubeClient.List(t.Context(), &taskSandboxes, client.InNamespace("eval-runner-sandbox"), client.MatchingLabels{
		discovery.LabelRole: discovery.RoleSample,
	}); err != nil {
		t.Fatalf("list task sandboxes after turn 3: %v", err)
	}
	if len(taskSandboxes.Items) != 0 {
		t.Fatalf("expected 0 task sandboxes while prebuild is still polling, got %d", len(taskSandboxes.Items))
	}

	// Turn 4: Prebuild exits 0 -> writes completion marker, cleans up prebuild sandbox, unblocks both dependent tasks!
	prebuildPollState = sandboxclient.AsyncTaskCompleted
	prebuildExitCode = 0
	now = now.Add(30 * time.Second)
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 4 reconcile error: %v", err)
	}

	if err := kubeClient.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("get run after turn 4: %v", err)
	}
	if updatedRun.Status.Prebuilds[0].Phase != v1alpha1.PrebuildPhaseCompleted {
		t.Fatalf("expected prebuild Completed, got %+v", updatedRun.Status.Prebuilds[0])
	}
	if updatedRun.Status.Prebuilds[0].FinishedAt == nil {
		t.Errorf("expected prebuild FinishedAt to be set")
	}
	if updatedRun.Status.Prebuilds[0].ExecutionID != "" {
		t.Errorf("expected prebuild ExecutionID to be cleared, got %q", updatedRun.Status.Prebuilds[0].ExecutionID)
	}

	// Verify prebuild sandbox was cleaned up.
	if err := kubeClient.List(t.Context(), &prebuildSandboxes, client.InNamespace("eval-runner-sandbox"), client.MatchingLabels{
		discovery.LabelRole: discovery.RolePrebuild,
	}); err != nil {
		t.Fatalf("list prebuild sandboxes after turn 4: %v", err)
	}
	if len(prebuildSandboxes.Items) != 0 {
		t.Errorf("expected prebuild sandbox to be deleted after completion, got %d", len(prebuildSandboxes.Items))
	}

	// Verify completion marker was written to artifact store.
	markerTarget := artifacts.PrebuildMarkerTarget(sink, "commit-abc123", "kind-images")
	markerBytes, err := sink.Load(t.Context(), markerTarget)
	if err != nil {
		t.Fatalf("expected completion marker at %s, got err: %v", markerTarget, err)
	}
	var marker artifacts.PrebuildCompletionMarker
	if err := json.Unmarshal(markerBytes, &marker); err != nil {
		t.Fatalf("unmarshal marker: %v", err)
	}
	if marker.Commit != "commit-abc123" || marker.PrebuildName != "kind-images" || marker.CompletedAt.IsZero() {
		t.Errorf("unexpected completion marker contents: %+v", marker)
	}

	// Verify BOTH dependent tasks were unblocked and started!
	if err := kubeClient.List(t.Context(), &taskSandboxes, client.InNamespace("eval-runner-sandbox"), client.MatchingLabels{
		discovery.LabelRole: discovery.RoleSample,
	}); err != nil {
		t.Fatalf("list task sandboxes after turn 4: %v", err)
	}
	if len(taskSandboxes.Items) != 2 {
		t.Fatalf("expected 2 task sandboxes after prebuild completed, got %d", len(taskSandboxes.Items))
	}
	if len(fakeSandboxClient.StartAsyncCalls) != 1 {
		t.Errorf("expected prebuild StartAsync to be called exactly once, got %d", len(fakeSandboxClient.StartAsyncCalls))
	}

	// Verify a subsequent Run for the same commit hits the marker written by Run 1 without creating a prebuild sandbox.
	secondRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run-prebuild-second", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "https://github.com/example/repo.git", Commit: "commit-abc123"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseRunning},
	}
	if err := kubeClient.Create(t.Context(), secondRun); err != nil {
		t.Fatalf("create secondRun: %v", err)
	}
	reconciler.SetCachedManifest(secondRun.Namespace, secondRun.Name, manifest)
	secondReq := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: secondRun.Namespace, Name: secondRun.Name}}
	if _, err := reconciler.Reconcile(t.Context(), secondReq); err != nil {
		t.Fatalf("secondRun reconcile error: %v", err)
	}

	var updatedSecondRun v1alpha1.Run
	if err := kubeClient.Get(t.Context(), secondReq.NamespacedName, &updatedSecondRun); err != nil {
		t.Fatalf("get secondRun: %v", err)
	}
	if len(updatedSecondRun.Status.Prebuilds) != 1 || updatedSecondRun.Status.Prebuilds[0].Phase != v1alpha1.PrebuildPhaseCompleted {
		t.Fatalf("expected secondRun prebuild Completed via cache hit, got %+v", updatedSecondRun.Status.Prebuilds)
	}
	var secondRunPrebuildSandboxes sandboxv1beta1.SandboxList
	if err := kubeClient.List(t.Context(), &secondRunPrebuildSandboxes, client.InNamespace("eval-runner-sandbox"), client.MatchingLabels{
		discovery.LabelRunName: secondRun.Name,
		discovery.LabelRole:    discovery.RolePrebuild,
	}); err != nil {
		t.Fatalf("list secondRun prebuild sandboxes: %v", err)
	}
	if len(secondRunPrebuildSandboxes.Items) != 0 {
		t.Errorf("expected 0 prebuild sandboxes for secondRun on cache hit, got %d", len(secondRunPrebuildSandboxes.Items))
	}
}

func TestPrebuild_CacheHitSkipsSandboxAndUnblocksTasksImmediately(t *testing.T) {
	scheme := setupScheme(t)
	now := time.Date(2026, 11, 28, 13, 0, 0, 0, time.UTC)

	manifest := &v1alpha1.Manifest{
		Prebuilds: map[string]v1alpha1.ManifestPrebuild{
			"kind-images": {
				Image:   "builder:v1",
				Command: []string{"./scripts/build-images.sh"},
			},
		},
		Tasks: []v1alpha1.ManifestTask{
			{
				ID:        "task-a",
				Spec:      "eval.py@a",
				Image:     "task-image:v1",
				Command:   []string{"run-task", "a"},
				Prebuild:  "kind-images",
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeSandbox}},
			},
			{
				ID:        "task-b",
				Spec:      "eval.py@b",
				Image:     "task-image:v1",
				Command:   []string{"run-task", "b"},
				Prebuild:  "kind-images",
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeSandbox}},
			},
		},
	}

	// Pre-populate cache marker in artifact store for this commit & prebuild.
	sink := artifacts.NewInMemorySink()
	markerData, err := json.Marshal(artifacts.PrebuildCompletionMarker{
		Commit:       "commit-cached-999",
		PrebuildName: "kind-images",
		CompletedAt:  now.Add(-10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("marshal marker: %v", err)
	}
	if _, err := sink.StorePrebuildMarker(t.Context(), "commit-cached-999", "kind-images", markerData); err != nil {
		t.Fatalf("StorePrebuildMarker: %v", err)
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run-prebuild-cache-hit", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "https://github.com/example/repo.git", Commit: "commit-cached-999"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseRunning},
	}

	fakeSandboxClient := &sandboxclient.FakeClient{}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).WithStatusSubresource(run).Build()

	reconciler := &RunReconciler{
		Client:           kubeClient,
		Scheme:           scheme,
		InstanceID:       "controller-1",
		SandboxNamespace: "eval-runner-sandbox",
		SandboxClient:    fakeSandboxClient,
		ArtifactSink:     sink,
		Now:              func() time.Time { return now },
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}

	// Single reconcile: cache hit marks prebuild Completed with zero prebuild sandboxes and starts both tasks immediately!
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	var prebuildSandboxes sandboxv1beta1.SandboxList
	if err := kubeClient.List(t.Context(), &prebuildSandboxes, client.InNamespace("eval-runner-sandbox"), client.MatchingLabels{
		discovery.LabelRole: discovery.RolePrebuild,
	}); err != nil {
		t.Fatalf("list prebuild sandboxes: %v", err)
	}
	if len(prebuildSandboxes.Items) != 0 {
		t.Fatalf("expected 0 prebuild sandboxes on cache hit, got %d", len(prebuildSandboxes.Items))
	}

	var updatedRun v1alpha1.Run
	if err := kubeClient.Get(t.Context(), req.NamespacedName, &updatedRun); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if len(updatedRun.Status.Prebuilds) != 1 || updatedRun.Status.Prebuilds[0].Phase != v1alpha1.PrebuildPhaseCompleted {
		t.Fatalf("expected prebuild Completed on cache hit, got %+v", updatedRun.Status.Prebuilds)
	}
	if !strings.Contains(updatedRun.Status.Prebuilds[0].Message, "cached") {
		t.Errorf("expected cache hit message, got %q", updatedRun.Status.Prebuilds[0].Message)
	}

	var taskSandboxes sandboxv1beta1.SandboxList
	if err := kubeClient.List(t.Context(), &taskSandboxes, client.InNamespace("eval-runner-sandbox"), client.MatchingLabels{
		discovery.LabelRole: discovery.RoleSample,
	}); err != nil {
		t.Fatalf("list task sandboxes: %v", err)
	}
	if len(taskSandboxes.Items) != 2 {
		t.Fatalf("expected 2 task sandboxes started immediately on cache hit, got %d", len(taskSandboxes.Items))
	}
	if len(fakeSandboxClient.StartAsyncCalls) != 0 {
		t.Errorf("expected 0 StartAsync calls on cache hit, got %d", len(fakeSandboxClient.StartAsyncCalls))
	}
}

func TestPrebuild_FailureFailsDependentTasksWhileIndependentTasksRun(t *testing.T) {
	scheme := setupScheme(t)
	now := time.Date(2026, 11, 28, 14, 0, 0, 0, time.UTC)

	manifest := &v1alpha1.Manifest{
		Prebuilds: map[string]v1alpha1.ManifestPrebuild{
			"images": {
				Image:   "builder:v1",
				Command: []string{"./scripts/build-images.sh"},
			},
		},
		Tasks: []v1alpha1.ManifestTask{
			{
				ID:        "task-dep-1",
				Spec:      "eval.py@dep1",
				Image:     "task-image:v1",
				Command:   []string{"run-task", "dep1"},
				Prebuild:  "images",
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeSandbox}},
			},
			{
				ID:        "task-dep-2",
				Spec:      "eval.py@dep2",
				Image:     "task-image:v1",
				Command:   []string{"run-task", "dep2"},
				Prebuild:  "images",
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeSandbox}},
			},
			{
				ID:        "task-independent",
				Spec:      "eval.py@indep",
				Image:     "task-image:v1",
				Command:   []string{"run-task", "indep"},
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeSandbox}},
			},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run-prebuild-fail-mixed", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "https://github.com/example/repo.git", Commit: "commit-fail-1"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseRunning},
	}

	prebuildExecutionIdentifier := prebuildExecutionID(run.Name, "images")
	fakeSandboxClient := &sandboxclient.FakeClient{
		PollAsyncFn: func(ctx context.Context, executionID string) (*sandboxclient.AsyncTaskStatus, error) {
			if executionID == prebuildExecutionIdentifier {
				return &sandboxclient.AsyncTaskStatus{
					State:    sandboxclient.AsyncTaskCompleted,
					ExitCode: 2,
					Stderr:   "docker build failed: missing Dockerfile",
				}, nil
			}
			return &sandboxclient.AsyncTaskStatus{
				State:    sandboxclient.AsyncTaskCompleted,
				ExitCode: 0,
			}, nil
		},
		ReadFileFn: func(ctx context.Context, path string) ([]byte, error) {
			if path == "/out/results.json" {
				return []byte(`{"schemaVersion": 1, "samples": [{"id": "s1", "passed": true, "score": 1.0}]}`), nil
			}
			return nil, fmt.Errorf("unexpected file %q", path)
		},
	}

	sink := artifacts.NewInMemorySink()
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).WithStatusSubresource(run).Build()

	reconciler := &RunReconciler{
		Client:           kubeClient,
		Scheme:           scheme,
		InstanceID:       "controller-1",
		SandboxNamespace: "eval-runner-sandbox",
		SandboxClient:    fakeSandboxClient,
		ArtifactSink:     sink,
		Now:              func() time.Time { return now },
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}

	// Turn 1: prebuild sandbox AND independent task sandbox are created; dependent tasks stay Pending.
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 1 error: %v", err)
	}

	var taskSandboxes sandboxv1beta1.SandboxList
	if err := kubeClient.List(t.Context(), &taskSandboxes, client.InNamespace("eval-runner-sandbox"), client.MatchingLabels{
		discovery.LabelRole: discovery.RoleSample,
	}); err != nil {
		t.Fatalf("list task sandboxes: %v", err)
	}
	if len(taskSandboxes.Items) != 1 || taskSandboxes.Items[0].Annotations[discovery.AnnotationTaskID] != "task-independent" {
		t.Fatalf("expected only task-independent sandbox on turn 1, got %+v", taskSandboxes.Items)
	}

	prebuildSandboxName := discovery.PrebuildSandboxNameForRun(run.Name, "images")
	createReadySandboxPodWithNode(t, kubeClient, prebuildSandboxName, "prebuild-fail-uid", "node-1", "10.244.0.30")
	createReadySandboxPodWithNode(t, kubeClient, taskSandboxes.Items[0].Name, "indep-task-uid", "node-1", "10.244.0.31")

	// Turn 2: StartAsync called for prebuild and task-independent.
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 2 error: %v", err)
	}

	// Turn 3: prebuild fails (exit 2), dependent tasks fail immediately, independent task completes.
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 3 error: %v", err)
	}

	// Turn 4: Run reaches terminal phase cleanly.
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 4 error: %v", err)
	}

	var finalRun v1alpha1.Run
	if err := kubeClient.Get(t.Context(), req.NamespacedName, &finalRun); err != nil {
		t.Fatalf("get final run: %v", err)
	}

	if finalRun.Status.Phase != v1alpha1.RunPhaseCompleted {
		t.Fatalf("expected Run Completed (with 1 completed independent task and 2 failed dependent tasks), got %s (%s)", finalRun.Status.Phase, finalRun.Status.Message)
	}
	if len(finalRun.Status.Prebuilds) != 1 || finalRun.Status.Prebuilds[0].Phase != v1alpha1.PrebuildPhaseFailed {
		t.Fatalf("expected prebuild Failed, got %+v", finalRun.Status.Prebuilds)
	}
	if !strings.Contains(finalRun.Status.Prebuilds[0].Message, "exited with code 2") || !strings.Contains(finalRun.Status.Prebuilds[0].Message, "missing Dockerfile") {
		t.Errorf("unexpected prebuild failure message: %q", finalRun.Status.Prebuilds[0].Message)
	}

	tasksByID := make(map[string]v1alpha1.TaskStatus, len(finalRun.Status.Tasks))
	for _, task := range finalRun.Status.Tasks {
		tasksByID[task.ID] = task
	}
	for _, dependentTaskID := range []string{"task-dep-1", "task-dep-2"} {
		dependentTask := tasksByID[dependentTaskID]
		if dependentTask.Phase != v1alpha1.TaskPhaseFailed {
			t.Errorf("dependent task %q phase = %s, want Failed", dependentTaskID, dependentTask.Phase)
		}
		if !strings.Contains(dependentTask.Message, `prebuild "images" failed`) {
			t.Errorf("dependent task %q message = %q, expected reference to failed prebuild", dependentTaskID, dependentTask.Message)
		}
	}
	if tasksByID["task-independent"].Phase != v1alpha1.TaskPhaseCompleted {
		t.Errorf("independent task phase = %s, want Completed", tasksByID["task-independent"].Phase)
	}

	// Verify no completion marker was written for the failed prebuild.
	markerTarget := artifacts.PrebuildMarkerTarget(sink, "commit-fail-1", "images")
	if _, err := sink.Load(t.Context(), markerTarget); !artifacts.IsNotFound(err) {
		t.Errorf("expected no completion marker for failed prebuild, got err=%v", err)
	}

	// Verify prebuild sandbox was cleaned up.
	var prebuildSandboxes sandboxv1beta1.SandboxList
	if err := kubeClient.List(t.Context(), &prebuildSandboxes, client.InNamespace("eval-runner-sandbox"), client.MatchingLabels{
		discovery.LabelRole: discovery.RolePrebuild,
	}); err != nil {
		t.Fatalf("list prebuild sandboxes: %v", err)
	}
	if len(prebuildSandboxes.Items) != 0 {
		t.Errorf("expected failed prebuild sandbox to be deleted, got %d", len(prebuildSandboxes.Items))
	}
}

func TestPrebuild_AllDependentTasksFailRunCleanly(t *testing.T) {
	scheme := setupScheme(t)
	now := time.Date(2026, 11, 28, 15, 0, 0, 0, time.UTC)

	manifest := &v1alpha1.Manifest{
		Prebuilds: map[string]v1alpha1.ManifestPrebuild{
			"images": {
				Image:          "builder:v1",
				Command:        []string{"./build.sh"},
				TimeoutSeconds: 60,
			},
		},
		Tasks: []v1alpha1.ManifestTask{
			{
				ID:        "task-only",
				Spec:      "eval.py@only",
				Image:     "task-image:v1",
				Command:   []string{"run-task"},
				Prebuild:  "images",
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeSandbox}},
			},
		},
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run-prebuild-timeout", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "https://github.com/example/repo.git", Commit: "commit-timeout"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseRunning},
	}

	fakeSandboxClient := &sandboxclient.FakeClient{
		PollAsyncFn: func(ctx context.Context, executionID string) (*sandboxclient.AsyncTaskStatus, error) {
			return &sandboxclient.AsyncTaskStatus{State: sandboxclient.AsyncTaskRunning}, nil
		},
	}

	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).WithStatusSubresource(run).Build()
	reconciler := &RunReconciler{
		Client:           kubeClient,
		Scheme:           scheme,
		InstanceID:       "controller-1",
		SandboxNamespace: "eval-runner-sandbox",
		SandboxClient:    fakeSandboxClient,
		ArtifactSink:     artifacts.NewInMemorySink(),
		Now:              func() time.Time { return now },
	}
	reconciler.SetCachedManifest(run.Namespace, run.Name, manifest)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Name}}

	// Turn 1: launch prebuild sandbox.
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 1 error: %v", err)
	}
	prebuildSandboxName := discovery.PrebuildSandboxNameForRun(run.Name, "images")
	createReadySandboxPodWithNode(t, kubeClient, prebuildSandboxName, "prebuild-timeout-uid", "node-1", "10.244.0.40")

	// Turn 2: StartAsync.
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 2 error: %v", err)
	}

	// Advance clock past prebuild TimeoutSeconds (60s).
	now = now.Add(65 * time.Second)

	// Turn 3: prebuild times out, cleans up sandbox, fails dependent task.
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 3 error: %v", err)
	}

	// Turn 4: Run transitions cleanly to Failed.
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("turn 4 error: %v", err)
	}

	var finalRun v1alpha1.Run
	if err := kubeClient.Get(t.Context(), req.NamespacedName, &finalRun); err != nil {
		t.Fatalf("get final run: %v", err)
	}
	if finalRun.Status.Phase != v1alpha1.RunPhaseFailed {
		t.Fatalf("expected Run Failed, got %s (%s)", finalRun.Status.Phase, finalRun.Status.Message)
	}
	if finalRun.Status.Prebuilds[0].Phase != v1alpha1.PrebuildPhaseFailed {
		t.Errorf("expected prebuild Failed, got %+v", finalRun.Status.Prebuilds[0])
	}
	if finalRun.Status.Tasks[0].Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("expected task-only Failed, got %+v", finalRun.Status.Tasks[0])
	}
	if !strings.Contains(finalRun.Status.Tasks[0].Message, `prebuild "images" failed`) {
		t.Errorf("expected task message to reference prebuild failure, got %q", finalRun.Status.Tasks[0].Message)
	}

	var prebuildSandboxes sandboxv1beta1.SandboxList
	if err := kubeClient.List(t.Context(), &prebuildSandboxes, client.InNamespace("eval-runner-sandbox"), client.MatchingLabels{
		discovery.LabelRole: discovery.RolePrebuild,
	}); err != nil {
		t.Fatalf("list prebuild sandboxes: %v", err)
	}
	if len(prebuildSandboxes.Items) != 0 {
		t.Errorf("expected timed-out prebuild sandbox to be deleted, got %d", len(prebuildSandboxes.Items))
	}
}
