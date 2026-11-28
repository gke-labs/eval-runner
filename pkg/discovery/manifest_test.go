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

package discovery

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestParseManifest(t *testing.T) {
	validJSON := []byte(`{
		"images": [
			{ "name": "support-agent-env:v12", "context": "hack/env" }
		],
		"tasks": [
			{
				"id": "triage/runbook_lookup",
				"workdir": "evals",
				"spec": "runbook_lookup.py@runbook_lookup",
				"resources": [ { "type": "docker" } ]
			},
			{
				"id": "triage/escalation_policy",
				"workdir": "evals",
				"spec": "escalation.py@escalation_policy",
				"images": [ "support-agent-env:v12" ]
			}
		]
	}`)

	m, err := ParseManifest(validJSON)
	if err != nil {
		t.Fatalf("ParseManifest failed unexpectedly: %v", err)
	}

	if len(m.Images) != 1 || m.Images[0].Name != "support-agent-env:v12" {
		t.Errorf("Unexpected images: %+v", m.Images)
	}
	if len(m.Tasks) != 2 {
		t.Fatalf("Expected 2 tasks, got %d", len(m.Tasks))
	}
	if m.Tasks[0].ID != "triage/runbook_lookup" {
		t.Errorf("Unexpected task 0 ID: %s", m.Tasks[0].ID)
	}
	if m.Tasks[1].ID != "triage/escalation_policy" {
		t.Errorf("Unexpected task 1 ID: %s", m.Tasks[1].ID)
	}

	// Invalid JSON test
	_, err = ParseManifest([]byte(`{ not valid json }`))
	if err == nil {
		t.Error("ParseManifest should have failed on invalid JSON")
	}
}

func TestParseManifest_ImageCommandSandbox(t *testing.T) {
	manifestJSON := []byte(`{
		"tasks": [
			{
				"id": "triage/sandbox_eval",
				"workdir": "evals",
				"spec": "sandbox.py@eval",
				"image": "my-sandbox-image:v1",
				"command": ["inspect", "eval", "sandbox.py@eval"],
				"resources": [
					{ "type": "sandbox" }
				]
			}
		]
	}`)

	manifest, err := ParseManifest(manifestJSON)
	if err != nil {
		t.Fatalf("ParseManifest failed: %v", err)
	}
	if len(manifest.Tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(manifest.Tasks))
	}
	task := manifest.Tasks[0]
	if task.Image != "my-sandbox-image:v1" {
		t.Errorf("task.Image = %q, want 'my-sandbox-image:v1'", task.Image)
	}
	if len(task.Command) != 3 || task.Command[0] != "inspect" || task.Command[1] != "eval" || task.Command[2] != "sandbox.py@eval" {
		t.Errorf("task.Command = %v, want ['inspect', 'eval', 'sandbox.py@eval']", task.Command)
	}
	if len(task.Resources) != 1 || task.Resources[0].Type != v1alpha1.ResourceTypeSandbox {
		t.Errorf("task.Resources = %v, want [{Type: 'sandbox'}]", task.Resources)
	}
}

func TestParseManifest_TimeoutSeconds(t *testing.T) {
	manifestJSON := []byte(`{
		"tasks": [
			{
				"id": "triage/timeout_explicit",
				"spec": "eval.py@timeout",
				"image": "my-sandbox-image:v1",
				"command": ["run-eval"],
				"resources": [{ "type": "sandbox" }],
				"timeoutSeconds": 600
			},
			{
				"id": "triage/timeout_default",
				"spec": "eval.py@default",
				"image": "my-sandbox-image:v1",
				"command": ["run-eval"],
				"resources": [{ "type": "sandbox" }]
			}
		]
	}`)

	manifest, err := ParseManifest(manifestJSON)
	if err != nil {
		t.Fatalf("ParseManifest failed: %v", err)
	}
	if len(manifest.Tasks) != 2 {
		t.Fatalf("Expected 2 tasks, got %d", len(manifest.Tasks))
	}

	explicitTask := manifest.Tasks[0]
	if explicitTask.TimeoutSeconds == nil {
		t.Fatalf("Expected explicitTask.TimeoutSeconds to be non-nil")
	}
	if *explicitTask.TimeoutSeconds != 600 {
		t.Errorf("explicitTask.TimeoutSeconds = %d, want 600", *explicitTask.TimeoutSeconds)
	}

	defaultTask := manifest.Tasks[1]
	if defaultTask.TimeoutSeconds != nil {
		t.Errorf("defaultTask.TimeoutSeconds = %v, want nil (default unset)", *defaultTask.TimeoutSeconds)
	}
}

func TestValidateManifest(t *testing.T) {
	tests := []struct {
		name        string
		manifest    *v1alpha1.Manifest
		expectError bool
	}{
		{
			name:        "nil manifest",
			manifest:    nil,
			expectError: true,
		},
		{
			name: "empty tasks",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{},
			},
			expectError: true,
		},
		{
			name: "valid manifest",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task1", Spec: "a.py@task1"},
					{ID: "task2", Spec: "b.py@task2"},
				},
			},
			expectError: false,
		},
		{
			name: "empty task id",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "", Spec: "a.py@task1"},
				},
			},
			expectError: true,
		},
		{
			name: "whitespace task id",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "   ", Spec: "a.py@task1"},
				},
			},
			expectError: true,
		},
		{
			name: "duplicate task id",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task1", Spec: "a.py@task1"},
					{ID: "task1", Spec: "b.py@task2"},
				},
			},
			expectError: true,
		},
		{
			name: "empty spec",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task1", Spec: ""},
				},
			},
			expectError: true,
		},
		{
			name: "spec starting with dash",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task1", Spec: "--flag"},
				},
			},
			expectError: true,
		},
		{
			name: "workdir with ..",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task1", Spec: "eval.py@task", Workdir: "../evals"},
				},
			},
			expectError: true,
		},
		{
			name: "workdir absolute",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task1", Spec: "eval.py@task", Workdir: "/evals"},
				},
			},
			expectError: true,
		},
		{
			name: "valid workdir",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task1", Spec: "eval.py@task", Workdir: "evals/sub"},
				},
			},
			expectError: false,
		},
		{
			name: "valid paths",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task1", Spec: "eval.py@task", Paths: []string{"docs/**", "README.md"}},
				},
			},
			expectError: false,
		},
		{
			name: "empty path pattern in paths",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task1", Spec: "eval.py@task", Paths: []string{""}},
				},
			},
			expectError: true,
		},
		{
			name: "invalid glob pattern in paths",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task1", Spec: "eval.py@task", Paths: []string{"docs/["}},
				},
			},
			expectError: true,
		},
		{
			name: "path pattern exceeds max length",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task1", Spec: "eval.py@task", Paths: []string{strings.Repeat("a", MaxPathLength+1)}},
				},
			},
			expectError: true,
		},
		{
			name: "too many paths in task",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:   "task1",
						Spec: "eval.py@task",
						Paths: func() []string {
							p := make([]string, MaxTaskPaths+1)
							for i := range p {
								p[i] = fmt.Sprintf("path/%d", i)
							}
							return p
						}(),
					},
				},
			},
			expectError: true,
		},
		{
			name: "too many tasks",
			manifest: func() *v1alpha1.Manifest {
				tasks := make([]v1alpha1.ManifestTask, MaxTasks+1)
				for i := range tasks {
					tasks[i] = v1alpha1.ManifestTask{
						ID:   string(rune('a'+(i%26))) + string(rune('0'+(i/26))),
						Spec: "task.py@eval",
					}
				}
				return &v1alpha1.Manifest{Tasks: tasks}
			}(),
			expectError: true,
		},
		{
			name: "valid cohort names and defaulting",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-cohort-default", Spec: "eval.py@task", Cohort: "default"},
					{ID: "task-cohort-dashes", Spec: "eval.py@task", Cohort: "customer-reported-q4"},
					{ID: "task-cohort-dots", Spec: "eval.py@task", Cohort: "v1.2"},
					{ID: "task-cohort-underscores", Spec: "eval.py@task", Cohort: "cohort_alpha"},
					{ID: "task-cohort-empty", Spec: "eval.py@task", Cohort: ""},
				},
			},
			expectError: false,
		},
		{
			name: "cohort exceeding 64 chars",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-long-cohort", Spec: "eval.py@task", Cohort: strings.Repeat("a", 65)},
				},
			},
			expectError: true,
		},
		{
			name: "cohort starting with dash",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-dash-start", Spec: "eval.py@task", Cohort: "-bad"},
				},
			},
			expectError: true,
		},
		{
			name: "cohort ending with dash",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-dash-end", Spec: "eval.py@task", Cohort: "bad-"},
				},
			},
			expectError: true,
		},
		{
			name: "cohort with space",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-space", Spec: "eval.py@task", Cohort: "bad cohort"},
				},
			},
			expectError: true,
		},
		{
			name: "cohort with slash",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-slash", Spec: "eval.py@task", Cohort: "bad/cohort"},
				},
			},
			expectError: true,
		},
		{
			name: "cohort with special characters",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-special", Spec: "eval.py@task", Cohort: "bad@cohort"},
				},
			},
			expectError: true,
		},
		{
			name: "command without image fails validation",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:      "task-command-no-image",
						Spec:    "test.py@eval",
						Command: []string{"inspect", "eval"},
					},
				},
			},
			expectError: true,
		},
		{
			name: "sandbox resource without image fails validation",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:        "task-sandbox-no-image",
						Spec:      "test.py@eval",
						Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
					},
				},
			},
			expectError: true,
		},
		{
			name: "command and sandbox resource with image passes validation",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:        "task-valid-sandbox",
						Spec:      "test.py@eval",
						Image:     "eval-image:v1",
						Command:   []string{"inspect", "eval"},
						Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
					},
				},
			},
			expectError: false,
		},
		{
			name: "standalone gcp-project resource passes validation without image",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:        "task-gcp-project",
						Spec:      "test.py@eval",
						Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeGCPProject}},
					},
				},
			},
			expectError: false,
		},
		{
			name: "sandbox and gcp-project resources with image pass validation",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:    "task-sandbox-gcp-project",
						Spec:  "test.py@eval",
						Image: "eval-image:v1",
						Resources: []v1alpha1.ManifestResource{
							{Type: v1alpha1.ResourceTypeSandbox, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}},
							{Type: v1alpha1.ResourceTypeGCPProject},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "sandbox and gcp-project resources without image fails validation",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:   "task-sandbox-gcp-project-no-image",
						Spec: "test.py@eval",
						Resources: []v1alpha1.ManifestResource{
							{Type: v1alpha1.ResourceTypeSandbox},
							{Type: v1alpha1.ResourceTypeGCPProject},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "standalone gcp-project resource with limits fails validation",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:   "task-gcp-project-with-limits",
						Spec: "test.py@eval",
						Resources: []v1alpha1.ManifestResource{
							{Type: v1alpha1.ResourceTypeGCPProject, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "sandbox and gcp-project with limits on gcp-project fails validation",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:    "task-sandbox-gcp-project-limits",
						Spec:  "test.py@eval",
						Image: "eval-image:v1",
						Resources: []v1alpha1.ManifestResource{
							{Type: v1alpha1.ResourceTypeSandbox},
							{Type: v1alpha1.ResourceTypeGCPProject, Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "standalone kind-cluster resource passes validation without image",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:        "task-kind-cluster",
						Spec:      "test.py@eval",
						Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeKindCluster}},
					},
				},
			},
			expectError: false,
		},
		{
			name: "sandbox and kind-cluster resources with image pass validation",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:    "task-sandbox-kind-cluster",
						Spec:  "test.py@eval",
						Image: "eval-image:v1",
						Resources: []v1alpha1.ManifestResource{
							{Type: v1alpha1.ResourceTypeSandbox, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}},
							{Type: v1alpha1.ResourceTypeKindCluster},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "sandbox, kind-cluster, and gcp-project resources with image pass validation",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:    "task-sandbox-kind-gcp",
						Spec:  "test.py@eval",
						Image: "eval-image:v1",
						Resources: []v1alpha1.ManifestResource{
							{Type: v1alpha1.ResourceTypeSandbox},
							{Type: v1alpha1.ResourceTypeKindCluster},
							{Type: v1alpha1.ResourceTypeGCPProject},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "sandbox and kind-cluster resources without image fails validation",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:   "task-sandbox-kind-no-image",
						Spec: "test.py@eval",
						Resources: []v1alpha1.ManifestResource{
							{Type: v1alpha1.ResourceTypeSandbox},
							{Type: v1alpha1.ResourceTypeKindCluster},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "standalone kind-cluster resource with limits passes validation",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:   "task-kind-cluster-with-limits",
						Spec: "test.py@eval",
						Resources: []v1alpha1.ManifestResource{
							{Type: v1alpha1.ResourceTypeKindCluster, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6")}},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "sandbox and kind-cluster with limits on kind-cluster passes validation",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{
						ID:    "task-sandbox-kind-cluster-limits",
						Spec:  "test.py@eval",
						Image: "eval-image:v1",
						Resources: []v1alpha1.ManifestResource{
							{Type: v1alpha1.ResourceTypeSandbox},
							{Type: v1alpha1.ResourceTypeKindCluster, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("12Gi")}},
						},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateManifest(tc.manifest)
			if tc.expectError && err == nil {
				t.Errorf("ValidateManifest expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("ValidateManifest unexpected error: %v", err)
			}
			if tc.name == "valid cohort names and defaulting" && err == nil {
				if tc.manifest.Tasks[4].Cohort != v1alpha1.DefaultCohort {
					t.Errorf("Expected empty cohort to default to %q, got %q", v1alpha1.DefaultCohort, tc.manifest.Tasks[4].Cohort)
				}
			}
		})
	}
}

func TestDiscoveryPathsAgreement(t *testing.T) {
	// Sandboxd starts with --root-dir=/workspace.
	// REST ReadFile paths are interpreted relative to /workspace.
	// Therefore, joining /workspace with DiscoveryManifestReadPath must exactly yield DiscoveryManifestWritePath.
	resolvedPath := filepath.Clean(filepath.Join("/workspace", DiscoveryManifestReadPath))
	if resolvedPath != DiscoveryManifestWritePath {
		t.Errorf("Read path %q under /workspace resolved to %q, but write path is %q",
			DiscoveryManifestReadPath, resolvedPath, DiscoveryManifestWritePath)
	}

	// Verify that DiscoveryCommand creates the parent directory and writes to DiscoveryManifestWritePath
	expectedCmd := "mkdir -p " + DiscoveryManifestWorkspaceDir + " && ./.evals/list > " + DiscoveryManifestWritePath
	if DiscoveryCommand != expectedCmd {
		t.Errorf("DiscoveryCommand mismatch: got %q, want %q", DiscoveryCommand, expectedCmd)
	}

	// Verify directory and filename match write path
	expectedWritePath := filepath.Join(DiscoveryManifestWorkspaceDir, DiscoveryManifestFilename)
	if DiscoveryManifestWritePath != expectedWritePath {
		t.Errorf("DiscoveryManifestWritePath %q does not match %q", DiscoveryManifestWritePath, expectedWritePath)
	}
}

func assertTerminalBenchCommonFields(t *testing.T, task *v1alpha1.ManifestTask, taskName string) {
	t.Helper()
	expectedImage := "alexgshaw/" + taskName + ":20251031"
	if task.Image != expectedImage {
		t.Errorf("task %s Image = %q, want %q", task.ID, task.Image, expectedImage)
	}
	if task.TimeoutSeconds == nil || *task.TimeoutSeconds < 1 || *task.TimeoutSeconds > 7200 {
		t.Errorf("task %s expected timeoutSeconds in [1, 7200], got %v", task.ID, task.TimeoutSeconds)
	}
	if len(task.Resources) != 1 || task.Resources[0].Type != v1alpha1.ResourceTypeSandbox {
		t.Errorf("task %s Resources = %v, want [{Type: 'sandbox'}]", task.ID, task.Resources)
	}
	cpuLimitQuantity := task.Resources[0].Limits[corev1.ResourceCPU]
	memoryLimitQuantity := task.Resources[0].Limits[corev1.ResourceMemory]
	if task.Resources[0].Limits == nil || cpuLimitQuantity.IsZero() || memoryLimitQuantity.IsZero() {
		t.Errorf("task %s Resources missing CPU or Memory limits: %v", task.ID, task.Resources[0])
	}
}

func assertTerminalBenchOracleTask(t *testing.T, task *v1alpha1.ManifestTask, taskName string) {
	t.Helper()
	expectedID := "terminal-bench/" + taskName + "/oracle"
	if task.ID != expectedID {
		t.Errorf("task ID = %q, want %q", task.ID, expectedID)
	}
	assertTerminalBenchCommonFields(t, task, taskName)
	if len(task.Command) != 5 || task.Command[0] != "bash" || task.Command[1] != "examples/terminal-bench/run-task.sh" || task.Command[2] != taskName || task.Command[3] != "oracle" || task.Command[4] == "" {
		t.Errorf("task %s Command = %v, want [bash examples/terminal-bench/run-task.sh %s oracle <workdir>]", task.ID, task.Command, taskName)
	}
	if task.Agent != nil {
		t.Errorf("task %s expected nil Agent for oracle variant, got %+v", task.ID, task.Agent)
	}
}

func assertTerminalBenchAgentTask(t *testing.T, task *v1alpha1.ManifestTask, taskName string) {
	t.Helper()
	expectedID := "terminal-bench/" + taskName
	if task.ID != expectedID {
		t.Errorf("task ID = %q, want %q", task.ID, expectedID)
	}
	assertTerminalBenchCommonFields(t, task, taskName)
	if len(task.Command) != 5 || task.Command[0] != "bash" || task.Command[1] != "examples/terminal-bench/run-task.sh" || task.Command[2] != taskName || task.Command[3] != "verify" || task.Command[4] == "" {
		t.Errorf("task %s Command = %v, want [bash examples/terminal-bench/run-task.sh %s verify <workdir>]", task.ID, task.Command, taskName)
	}
	if task.Agent == nil {
		t.Errorf("task %s expected non-nil Agent block", task.ID)
	}
}

func parseAndValidateScriptOutput(t *testing.T, scriptRelativePath string) *v1alpha1.Manifest {
	t.Helper()
	scriptPath, err := filepath.Abs(scriptRelativePath)
	if err != nil {
		t.Fatalf("failed to resolve script %s: %v", scriptRelativePath, err)
	}
	command := exec.Command("sh", scriptPath)
	command.Dir = filepath.Dir(scriptPath)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("executing script %s failed: %v", scriptRelativePath, err)
	}
	manifest, err := ParseManifest(output)
	if err != nil {
		t.Fatalf("ParseManifest failed for %s: %v\noutput: %s", scriptRelativePath, err, string(output))
	}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("ValidateManifest failed for %s: %v", scriptRelativePath, err)
	}
	return manifest
}

func TestTerminalBenchManifest(t *testing.T) {
	if os.Getenv("OFFLINE") != "" || os.Getenv("SKIP_NETWORK_TESTS") != "" {
		t.Skip("skipping manifest test requiring network in offline mode")
	}

	manifest := parseAndValidateScriptOutput(t, "../../examples/terminal-bench/.evals/list")
	if len(manifest.Tasks) != 178 {
		t.Fatalf("expected 178 tasks in terminal-bench manifest, got %d", len(manifest.Tasks))
	}
	var count4G, count8G int
	var countOracle, countAgent int
	for _, task := range manifest.Tasks {
		if strings.HasSuffix(task.ID, "/oracle") {
			countOracle++
			taskName := strings.TrimSuffix(strings.TrimPrefix(task.ID, "terminal-bench/"), "/oracle")
			assertTerminalBenchOracleTask(t, &task, taskName)
		} else {
			countAgent++
			taskName := strings.TrimPrefix(task.ID, "terminal-bench/")
			assertTerminalBenchAgentTask(t, &task, taskName)
		}
		if task.Spec == "terminal-bench/regex-log" {
			cpuLimit := task.Resources[0].Limits[corev1.ResourceCPU]
			memoryLimit := task.Resources[0].Limits[corev1.ResourceMemory]
			if cpuLimit.String() != "1" || memoryLimit.String() != "2G" {
				t.Errorf("regex-log expected cpu=1, memory=2G; got cpu=%s, memory=%s", cpuLimit.String(), memoryLimit.String())
			}
		}
		memoryLimitForCheck := task.Resources[0].Limits[corev1.ResourceMemory]
		if memoryLimitForCheck.String() == "4G" {
			count4G++
		}
		if memoryLimitForCheck.String() == "8G" {
			count8G++
		}
	}
	if countOracle != 89 {
		t.Errorf("expected 89 oracle tasks, got %d", countOracle)
	}
	if countAgent != 89 {
		t.Errorf("expected 89 agent tasks, got %d", countAgent)
	}
	if count4G == 0 {
		t.Errorf("expected at least one 4G task in terminal-bench manifest, got 0")
	}
	if count8G == 0 {
		t.Errorf("expected at least one 8G task in terminal-bench manifest, got 0")
	}

	rootManifest := parseAndValidateScriptOutput(t, "../../.evals/list")
	foundTerminalBenchCount := 0
	for i := range rootManifest.Tasks {
		task := &rootManifest.Tasks[i]
		if strings.HasPrefix(task.ID, "terminal-bench/") {
			foundTerminalBenchCount++
			var taskName string
			if strings.HasSuffix(task.ID, "/oracle") {
				taskName = strings.TrimSuffix(strings.TrimPrefix(task.ID, "terminal-bench/"), "/oracle")
				assertTerminalBenchOracleTask(t, task, taskName)
			} else {
				taskName = strings.TrimPrefix(task.ID, "terminal-bench/")
				assertTerminalBenchAgentTask(t, task, taskName)
			}
			if taskName == "regex-log" {
				cpuLimit := task.Resources[0].Limits[corev1.ResourceCPU]
				memoryLimit := task.Resources[0].Limits[corev1.ResourceMemory]
				if cpuLimit.String() != "1" || memoryLimit.String() != "2G" {
					t.Errorf("root %s expected cpu=1, memory=2G; got cpu=%s, memory=%s", task.ID, cpuLimit.String(), memoryLimit.String())
				}
			}
			if len(task.Paths) != 1 || task.Paths[0] != "examples/terminal-bench/**" {
				t.Errorf("root %s Paths = %v, want ['examples/terminal-bench/**']", task.ID, task.Paths)
			}
		}
	}
	if foundTerminalBenchCount == 0 {
		t.Fatalf("no terminal-bench tasks found in root .evals/list")
	}
}

func TestParseManifest_InvalidResourceLimits(t *testing.T) {
	tests := []struct {
		name         string
		manifestJSON string
	}{
		{
			name: "bad cpu quantity in sandbox resource limits",
			manifestJSON: `{
				"tasks": [
					{
						"id": "task-bad-cpu",
						"spec": "eval.py@task",
						"resources": [{"type": "sandbox", "limits": {"cpu": "not-a-number"}}]
					}
				]
			}`,
		},
		{
			name: "bad memory quantity in sandbox resource limits",
			manifestJSON: `{
				"tasks": [
					{
						"id": "task-bad-mem",
						"spec": "eval.py@task",
						"resources": [{"type": "sandbox", "limits": {"memory": "invalid-mem"}}]
					}
				]
			}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseManifest([]byte(tc.manifestJSON))
			if err == nil {
				t.Fatalf("expected ParseManifest to fail for invalid resource quantity, got nil")
			}
		})
	}
}

func TestValidateTaskResources_RejectionPaths(t *testing.T) {
	tests := []struct {
		name          string
		task          v1alpha1.ManifestTask
		expectedTask  string
		expectedField string
		expectedValue string
	}{
		{
			name: "unsupported resource key",
			task: v1alpha1.ManifestTask{
				ID:        "task-bad-key",
				Spec:      "eval.py@task",
				Image:     "image:v1",
				Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}},
			},
			expectedTask:  "task-bad-key",
			expectedField: "nvidia.com/gpu",
			expectedValue: "1",
		},
		{
			name: "non-sandbox resource with limits",
			task: v1alpha1.ManifestTask{
				ID:        "task-non-sb",
				Spec:      "eval.py@task",
				Resources: []v1alpha1.ManifestResource{{Type: "docker", Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}},
			},
			expectedTask:  "task-non-sb",
			expectedField: "limits",
			expectedValue: "1",
		},
		{
			name: "gcp-project resource with limits",
			task: v1alpha1.ManifestTask{
				ID:        "task-gcp-limits",
				Spec:      "eval.py@task",
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeGCPProject, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}},
			},
			expectedTask:  "task-gcp-limits",
			expectedField: "limits",
			expectedValue: "1",
		},
		{
			name: "sandbox and gcp-project with limits on gcp-project",
			task: v1alpha1.ManifestTask{
				ID:    "task-sandbox-gcp-limits",
				Spec:  "eval.py@task",
				Image: "image:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}},
					{Type: v1alpha1.ResourceTypeGCPProject, Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}},
				},
			},
			expectedTask:  "task-sandbox-gcp-limits",
			expectedField: "limits",
			expectedValue: "1Gi",
		},
		{
			name: "kind-cluster resource with unsupported key",
			task: v1alpha1.ManifestTask{
				ID:        "task-kind-bad-key",
				Spec:      "eval.py@task",
				Image:     "image:v1",
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeKindCluster, Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}},
			},
			expectedTask:  "task-kind-bad-key",
			expectedField: "nvidia.com/gpu",
			expectedValue: "1",
		},
		{
			name: "kind-cluster resource with cpu over cap 16",
			task: v1alpha1.ManifestTask{
				ID:    "task-kind-over-cpu",
				Spec:  "eval.py@task",
				Image: "image:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox},
					{Type: v1alpha1.ResourceTypeKindCluster, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("17")}},
				},
			},
			expectedTask:  "task-kind-over-cpu",
			expectedField: "cpu",
			expectedValue: "17",
		},
		{
			name: "kind-cluster resource with memory over cap 64Gi",
			task: v1alpha1.ManifestTask{
				ID:    "task-kind-over-memory",
				Spec:  "eval.py@task",
				Image: "image:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox},
					{Type: v1alpha1.ResourceTypeKindCluster, Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("65Gi")}},
				},
			},
			expectedTask:  "task-kind-over-memory",
			expectedField: "memory",
			expectedValue: "65Gi",
		},
		{
			name: "kind-cluster resource with zero cpu",
			task: v1alpha1.ManifestTask{
				ID:    "task-kind-zero-cpu",
				Spec:  "eval.py@task",
				Image: "image:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox},
					{Type: v1alpha1.ResourceTypeKindCluster, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}},
				},
			},
			expectedTask:  "task-kind-zero-cpu",
			expectedField: "cpu",
			expectedValue: "0",
		},
		{
			name: "zero cpu",
			task: v1alpha1.ManifestTask{
				ID:        "task-zero-cpu",
				Spec:      "eval.py@task",
				Image:     "image:v1",
				Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}}},
			},
			expectedTask:  "task-zero-cpu",
			expectedField: "cpu",
			expectedValue: "0",
		},
		{
			name: "negative cpu",
			task: v1alpha1.ManifestTask{
				ID:        "task-neg-cpu",
				Spec:      "eval.py@task",
				Image:     "image:v1",
				Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("-1")}}},
			},
			expectedTask:  "task-neg-cpu",
			expectedField: "cpu",
			expectedValue: "-1",
		},
		{
			name: "zero memory",
			task: v1alpha1.ManifestTask{
				ID:        "task-zero-mem",
				Spec:      "eval.py@task",
				Image:     "image:v1",
				Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("0")}}},
			},
			expectedTask:  "task-zero-mem",
			expectedField: "memory",
			expectedValue: "0",
		},
		{
			name: "negative memory",
			task: v1alpha1.ManifestTask{
				ID:        "task-neg-mem",
				Spec:      "eval.py@task",
				Image:     "image:v1",
				Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("-512Mi")}}},
			},
			expectedTask:  "task-neg-mem",
			expectedField: "memory",
			expectedValue: "-512Mi",
		},
		{
			name: "cpu over cap 16",
			task: v1alpha1.ManifestTask{
				ID:        "task-over-cpu",
				Spec:      "eval.py@task",
				Image:     "image:v1",
				Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("17")}}},
			},
			expectedTask:  "task-over-cpu",
			expectedField: "cpu",
			expectedValue: "17",
		},
		{
			name: "memory over cap 64Gi",
			task: v1alpha1.ManifestTask{
				ID:        "task-over-mem",
				Spec:      "eval.py@task",
				Image:     "image:v1",
				Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("65Gi")}}},
			},
			expectedTask:  "task-over-mem",
			expectedField: "memory",
			expectedValue: "65Gi",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTaskResources(tc.task)
			if err == nil {
				t.Fatalf("expected validation error, got nil")
			}
			errMsg := err.Error()
			if !strings.Contains(errMsg, tc.expectedTask) {
				t.Errorf("error %q should name task id %q", errMsg, tc.expectedTask)
			}
			if !strings.Contains(errMsg, tc.expectedField) {
				t.Errorf("error %q should name field %q", errMsg, tc.expectedField)
			}
			if !strings.Contains(errMsg, tc.expectedValue) {
				t.Errorf("error %q should name value %q", errMsg, tc.expectedValue)
			}
		})
	}
}

func TestValidateTaskResources_Valid(t *testing.T) {
	tests := []struct {
		name string
		task v1alpha1.ManifestTask
	}{
		{
			name: "typical sandbox task",
			task: v1alpha1.ManifestTask{
				ID:        "task-1",
				Spec:      "eval.py@task",
				Image:     "image:v1",
				Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2G")}}},
			},
		},
		{
			name: "at upper bounds (16 cpu, 64Gi memory)",
			task: v1alpha1.ManifestTask{
				ID:        "task-cap",
				Spec:      "eval.py@task",
				Image:     "image:v1",
				Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("64Gi")}}},
			},
		},
		{
			name: "milli-cpu and mebibytes",
			task: v1alpha1.ManifestTask{
				ID:        "task-milli",
				Spec:      "eval.py@task",
				Image:     "image:v1",
				Resources: []v1alpha1.ManifestResource{{Type: "sandbox", Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("512Mi")}}},
			},
		},
		{
			name: "absent cpu and memory (keeps defaults)",
			task: v1alpha1.ManifestTask{
				ID:        "task-def",
				Spec:      "eval.py@task",
				Image:     "image:v1",
				Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
			},
		},
		{
			name: "no resources declared",
			task: v1alpha1.ManifestTask{
				ID:   "task-no-resources",
				Spec: "eval.py@task",
			},
		},
		{
			name: "standalone gcp-project resource",
			task: v1alpha1.ManifestTask{
				ID:        "task-gcp-only",
				Spec:      "eval.py@task",
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeGCPProject}},
			},
		},
		{
			name: "sandbox with limits combined with gcp-project",
			task: v1alpha1.ManifestTask{
				ID:    "task-sandbox-and-gcp",
				Spec:  "eval.py@task",
				Image: "image:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}},
					{Type: v1alpha1.ResourceTypeGCPProject},
				},
			},
		},
		{
			name: "standalone kind-cluster resource",
			task: v1alpha1.ManifestTask{
				ID:        "task-kind-only",
				Spec:      "eval.py@task",
				Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeKindCluster}},
			},
		},
		{
			name: "sandbox with limits combined with kind-cluster",
			task: v1alpha1.ManifestTask{
				ID:    "task-sandbox-and-kind",
				Spec:  "eval.py@task",
				Image: "image:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}},
					{Type: v1alpha1.ResourceTypeKindCluster},
				},
			},
		},
		{
			name: "sandbox with limits combined with kind-cluster and gcp-project",
			task: v1alpha1.ManifestTask{
				ID:    "task-sandbox-kind-gcp",
				Spec:  "eval.py@task",
				Image: "image:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}},
					{Type: v1alpha1.ResourceTypeKindCluster},
					{Type: v1alpha1.ResourceTypeGCPProject},
				},
			},
		},
		{
			name: "sandbox with limits combined with kind-cluster with limits",
			task: v1alpha1.ManifestTask{
				ID:    "task-sandbox-and-kind-limits",
				Spec:  "eval.py@task",
				Image: "image:v1",
				Resources: []v1alpha1.ManifestResource{
					{Type: v1alpha1.ResourceTypeSandbox, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}},
					{Type: v1alpha1.ResourceTypeKindCluster, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("6"), corev1.ResourceMemory: resource.MustParse("12Gi")}},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateTaskResources(tc.task); err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestKindClusterResourceLimits(t *testing.T) {
	t.Run("parsed from manifest JSON", func(t *testing.T) {
		manifestJSON := []byte(`{
			"tasks": [
				{
					"id": "kube-agents/kind-task",
					"spec": "eval.py@kind",
					"image": "kube-agents-eval:v1",
					"command": ["./run.sh"],
					"resources": [
						{ "type": "sandbox", "limits": { "cpu": "2", "memory": "4Gi" } },
						{ "type": "kind-cluster", "limits": { "cpu": "6", "memory": "12Gi" } }
					]
				}
			]
		}`)

		manifest, err := ParseManifest(manifestJSON)
		if err != nil {
			t.Fatalf("ParseManifest failed: %v", err)
		}
		if err := ValidateManifest(manifest); err != nil {
			t.Fatalf("ValidateManifest failed: %v", err)
		}
		if err := ValidateTaskResources(manifest.Tasks[0]); err != nil {
			t.Fatalf("ValidateTaskResources failed: %v", err)
		}

		limits := KindClusterResourceLimits(manifest.Tasks[0])
		cpuLimit := limits[corev1.ResourceCPU]
		memoryLimit := limits[corev1.ResourceMemory]
		if cpuLimit.Cmp(resource.MustParse("6")) != 0 {
			t.Errorf("got cpuLimit=%v, want 6", cpuLimit)
		}
		if memoryLimit.Cmp(resource.MustParse("12Gi")) != 0 {
			t.Errorf("got memoryLimit=%v, want 12Gi", memoryLimit)
		}
	})

	t.Run("explicit limits", func(t *testing.T) {
		task := v1alpha1.ManifestTask{
			ID: "task-kind-explicit",
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
						corev1.ResourceCPU:    resource.MustParse("8"),
						corev1.ResourceMemory: resource.MustParse("16Gi"),
					},
				},
			},
		}
		limits := KindClusterResourceLimits(task)
		cpuLimit := limits[corev1.ResourceCPU]
		memoryLimit := limits[corev1.ResourceMemory]
		if cpuLimit.Cmp(resource.MustParse("8")) != 0 {
			t.Errorf("got cpuLimit=%v, want 8", cpuLimit)
		}
		if memoryLimit.Cmp(resource.MustParse("16Gi")) != 0 {
			t.Errorf("got memoryLimit=%v, want 16Gi", memoryLimit)
		}
	})

	t.Run("defaults when absent", func(t *testing.T) {
		task := v1alpha1.ManifestTask{
			ID:        "task-kind-defaults",
			Resources: []v1alpha1.ManifestResource{{Type: v1alpha1.ResourceTypeKindCluster}},
		}
		limits := KindClusterResourceLimits(task)
		cpuLimit := limits[corev1.ResourceCPU]
		memoryLimit := limits[corev1.ResourceMemory]
		if cpuLimit.Cmp(resource.MustParse("4")) != 0 {
			t.Errorf("got cpuLimit=%v, want 4", cpuLimit)
		}
		if memoryLimit.Cmp(resource.MustParse("8Gi")) != 0 {
			t.Errorf("got memoryLimit=%v, want 8Gi", memoryLimit)
		}
	})

	t.Run("omitted keys filled with defaults", func(t *testing.T) {
		taskCPUOnly := v1alpha1.ManifestTask{
			ID: "task-kind-cpu-only",
			Resources: []v1alpha1.ManifestResource{{
				Type: v1alpha1.ResourceTypeKindCluster,
				Limits: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("6"),
				},
			}},
		}
		limitsCPUOnly := KindClusterResourceLimits(taskCPUOnly)
		cpuLimitFromCPUOnly := limitsCPUOnly[corev1.ResourceCPU]
		memoryLimitFromCPUOnly := limitsCPUOnly[corev1.ResourceMemory]
		if cpuLimitFromCPUOnly.Cmp(resource.MustParse("6")) != 0 {
			t.Errorf("got cpuLimit=%v, want 6", cpuLimitFromCPUOnly)
		}
		if memoryLimitFromCPUOnly.Cmp(resource.MustParse(DefaultDockerInDockerMemoryLimit)) != 0 {
			t.Errorf("got memoryLimit=%v, want %s", memoryLimitFromCPUOnly, DefaultDockerInDockerMemoryLimit)
		}

		taskMemoryOnly := v1alpha1.ManifestTask{
			ID: "task-kind-memory-only",
			Resources: []v1alpha1.ManifestResource{{
				Type: v1alpha1.ResourceTypeKindCluster,
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("16Gi"),
				},
			}},
		}
		limitsMemoryOnly := KindClusterResourceLimits(taskMemoryOnly)
		cpuLimitFromMemoryOnly := limitsMemoryOnly[corev1.ResourceCPU]
		memoryLimitFromMemoryOnly := limitsMemoryOnly[corev1.ResourceMemory]
		if cpuLimitFromMemoryOnly.Cmp(resource.MustParse(DefaultDockerInDockerCPULimit)) != 0 {
			t.Errorf("got cpuLimit=%v, want %s", cpuLimitFromMemoryOnly, DefaultDockerInDockerCPULimit)
		}
		if memoryLimitFromMemoryOnly.Cmp(resource.MustParse("16Gi")) != 0 {
			t.Errorf("got memoryLimit=%v, want 16Gi", memoryLimitFromMemoryOnly)
		}
	})
}

func TestTaskResourceLimits(t *testing.T) {
	t.Run("explicit limits", func(t *testing.T) {
		task := v1alpha1.ManifestTask{
			ID: "task-explicit",
			Resources: []v1alpha1.ManifestResource{{
				Type: "sandbox",
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2"),
					corev1.ResourceMemory: resource.MustParse("4G"),
				},
			}},
		}
		limits := TaskResourceLimits(task)
		cpuLimit := limits[corev1.ResourceCPU]
		memoryLimit := limits[corev1.ResourceMemory]
		if cpuLimit.Cmp(resource.MustParse("2")) != 0 {
			t.Errorf("got cpuLimit=%v, want 2", cpuLimit)
		}
		if memoryLimit.Cmp(resource.MustParse("4G")) != 0 {
			t.Errorf("got memoryLimit=%v, want 4G", memoryLimit)
		}
	})

	t.Run("defaults when absent", func(t *testing.T) {
		task := v1alpha1.ManifestTask{
			ID:        "task-defaults",
			Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
		}
		limits := TaskResourceLimits(task)
		cpuLimit := limits[corev1.ResourceCPU]
		memoryLimit := limits[corev1.ResourceMemory]
		if cpuLimit.Cmp(resource.MustParse(v1alpha1.DefaultTaskCPULimit)) != 0 {
			t.Errorf("got cpuLimit=%v, want %s", cpuLimit, v1alpha1.DefaultTaskCPULimit)
		}
		if memoryLimit.Cmp(resource.MustParse(v1alpha1.DefaultTaskMemoryLimit)) != 0 {
			t.Errorf("got memoryLimit=%v, want %s", memoryLimit, v1alpha1.DefaultTaskMemoryLimit)
		}
	})

	t.Run("omitted keys filled with defaults", func(t *testing.T) {
		taskCPUOnly := v1alpha1.ManifestTask{
			ID: "task-cpu-only",
			Resources: []v1alpha1.ManifestResource{{
				Type: "sandbox",
				Limits: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("2"),
				},
			}},
		}
		limitsCPUOnly := TaskResourceLimits(taskCPUOnly)
		cpuLimitFromCPUOnly := limitsCPUOnly[corev1.ResourceCPU]
		memoryLimitFromCPUOnly := limitsCPUOnly[corev1.ResourceMemory]
		if cpuLimitFromCPUOnly.Cmp(resource.MustParse("2")) != 0 {
			t.Errorf("got cpuLimit=%v, want 2", cpuLimitFromCPUOnly)
		}
		if memoryLimitFromCPUOnly.Cmp(resource.MustParse(v1alpha1.DefaultTaskMemoryLimit)) != 0 {
			t.Errorf("got memoryLimit=%v, want %s", memoryLimitFromCPUOnly, v1alpha1.DefaultTaskMemoryLimit)
		}

		taskMemoryOnly := v1alpha1.ManifestTask{
			ID: "task-mem-only",
			Resources: []v1alpha1.ManifestResource{{
				Type: "sandbox",
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("4Gi"),
				},
			}},
		}
		limitsMemoryOnly := TaskResourceLimits(taskMemoryOnly)
		cpuLimitFromMemoryOnly := limitsMemoryOnly[corev1.ResourceCPU]
		memoryLimitFromMemoryOnly := limitsMemoryOnly[corev1.ResourceMemory]
		if cpuLimitFromMemoryOnly.Cmp(resource.MustParse(v1alpha1.DefaultTaskCPULimit)) != 0 {
			t.Errorf("got cpuLimit=%v, want %s", cpuLimitFromMemoryOnly, v1alpha1.DefaultTaskCPULimit)
		}
		if memoryLimitFromMemoryOnly.Cmp(resource.MustParse("4Gi")) != 0 {
			t.Errorf("got memoryLimit=%v, want 4Gi", memoryLimitFromMemoryOnly)
		}
	})
}

func TestValidateTaskAgent(t *testing.T) {
	timeoutZero, timeoutExcess := 0, 7201
	tests := []struct {
		name          string
		task          v1alpha1.ManifestTask
		wantErr       bool
		expectedField string
	}{
		{
			name: "valid agent block",
			task: v1alpha1.ManifestTask{
				ID: "task-ok", Command: []string{"test"}, Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				Agent: &v1alpha1.ManifestTaskAgent{
					PrepareCommand: []string{"sh", "-c", "echo", ""}, InstructionPath: "/out/inst.md",
				},
			},
		},
		{
			name: "rejection without sandbox resource",
			task: v1alpha1.ManifestTask{
				ID: "task-no-sb", Command: []string{"test"},
				Agent: &v1alpha1.ManifestTaskAgent{InstructionPath: "/out/inst.md"},
			},
			wantErr: true, expectedField: "invalid agent",
		},
		{
			name: "rejection without command",
			task: v1alpha1.ManifestTask{
				ID: "task-no-cmd", Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				Agent: &v1alpha1.ManifestTaskAgent{InstructionPath: "/out/inst.md"},
			},
			wantErr: true, expectedField: "invalid command",
		},
		{
			name: "rejection empty prepareCommand",
			task: v1alpha1.ManifestTask{
				ID: "task-empty-prep", Command: []string{"test"}, Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				Agent: &v1alpha1.ManifestTaskAgent{PrepareCommand: []string{}, InstructionPath: "/out/inst.md"},
			},
			wantErr: true, expectedField: "invalid agent.prepareCommand",
		},
		{
			name: "rejection missing instructionPath",
			task: v1alpha1.ManifestTask{
				ID: "task-no-path", Command: []string{"test"}, Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				Agent: &v1alpha1.ManifestTaskAgent{InstructionPath: ""},
			},
			wantErr: true, expectedField: "invalid agent.instructionPath",
		},
		{
			name: "rejection relative instructionPath",
			task: v1alpha1.ManifestTask{
				ID: "task-rel-path", Command: []string{"test"}, Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				Agent: &v1alpha1.ManifestTaskAgent{InstructionPath: "out/inst.md"},
			},
			wantErr: true, expectedField: "invalid agent.instructionPath",
		},
		{
			name: "rejection timeout below min",
			task: v1alpha1.ManifestTask{
				ID: "task-zero-to", Command: []string{"test"}, Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				Agent: &v1alpha1.ManifestTaskAgent{InstructionPath: "/out/inst.md", TimeoutSeconds: &timeoutZero},
			},
			wantErr: true, expectedField: "invalid agent.timeoutSeconds",
		},
		{
			name: "rejection timeout above max",
			task: v1alpha1.ManifestTask{
				ID: "task-excess-to", Command: []string{"test"}, Resources: []v1alpha1.ManifestResource{{Type: "sandbox"}},
				Agent: &v1alpha1.ManifestTaskAgent{InstructionPath: "/out/inst.md", TimeoutSeconds: &timeoutExcess},
			},
			wantErr: true, expectedField: "invalid agent.timeoutSeconds",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTaskAgent(tc.task)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.task.ID) || !strings.Contains(err.Error(), tc.expectedField) {
				t.Errorf("error %q should name task %q and field %q", err.Error(), tc.task.ID, tc.expectedField)
			}
		})
	}
}

func TestParseManifest_Prebuilds(t *testing.T) {
	manifestJSON := []byte(`{
		"prebuilds": {
			"kind-images": {
				"image": "kube-agents-builder:v1",
				"command": ["bash", "hack/prebuild-images.sh"],
				"timeoutSeconds": 900,
				"paths": ["cmd/**", "pkg/**", "Dockerfile"],
				"resources": [
					{ "type": "sandbox", "limits": { "cpu": "4", "memory": "8Gi" } },
					{ "type": "kind-cluster", "limits": { "cpu": "6", "memory": "12Gi" } }
				]
			},
			"unit-fixtures": {
				"image": "golang:1.27",
				"command": ["go", "build", "./..."]
			}
		},
		"tasks": [
			{
				"id": "kube-agents/task-with-prebuild",
				"spec": "eval.py@kind_task",
				"image": "kube-agents-eval:v1",
				"command": ["./run.sh"],
				"resources": [
					{ "type": "sandbox" },
					{ "type": "kind-cluster" }
				],
				"prebuild": "kind-images"
			},
			{
				"id": "kube-agents/task-without-prebuild",
				"spec": "eval.py@simple_task"
			}
		]
	}`)

	manifest, err := ParseManifest(manifestJSON)
	if err != nil {
		t.Fatalf("ParseManifest failed: %v", err)
	}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("ValidateManifest failed: %v", err)
	}

	if len(manifest.Prebuilds) != 2 {
		t.Fatalf("len(manifest.Prebuilds) = %d, want 2", len(manifest.Prebuilds))
	}

	kindPrebuild, exists := manifest.Prebuilds["kind-images"]
	if !exists {
		t.Fatalf("expected prebuild %q in manifest.Prebuilds", "kind-images")
	}
	if kindPrebuild.Image != "kube-agents-builder:v1" {
		t.Errorf("kindPrebuild.Image = %q, want %q", kindPrebuild.Image, "kube-agents-builder:v1")
	}
	if len(kindPrebuild.Command) != 2 || kindPrebuild.Command[0] != "bash" || kindPrebuild.Command[1] != "hack/prebuild-images.sh" {
		t.Errorf("kindPrebuild.Command = %v, want [bash hack/prebuild-images.sh]", kindPrebuild.Command)
	}
	if kindPrebuild.TimeoutSeconds != 900 {
		t.Errorf("kindPrebuild.TimeoutSeconds = %d, want 900", kindPrebuild.TimeoutSeconds)
	}
	if len(kindPrebuild.Paths) != 3 || kindPrebuild.Paths[0] != "cmd/**" || kindPrebuild.Paths[2] != "Dockerfile" {
		t.Errorf("kindPrebuild.Paths = %v, want [cmd/** pkg/** Dockerfile]", kindPrebuild.Paths)
	}
	if len(kindPrebuild.Resources) != 2 {
		t.Fatalf("len(kindPrebuild.Resources) = %d, want 2", len(kindPrebuild.Resources))
	}
	if kindPrebuild.Resources[0].Type != v1alpha1.ResourceTypeSandbox {
		t.Errorf("kindPrebuild.Resources[0].Type = %q, want %q", kindPrebuild.Resources[0].Type, v1alpha1.ResourceTypeSandbox)
	}
	if kindPrebuild.Resources[1].Type != v1alpha1.ResourceTypeKindCluster {
		t.Errorf("kindPrebuild.Resources[1].Type = %q, want %q", kindPrebuild.Resources[1].Type, v1alpha1.ResourceTypeKindCluster)
	}

	if manifest.Tasks[0].Prebuild != "kind-images" {
		t.Errorf("manifest.Tasks[0].Prebuild = %q, want %q", manifest.Tasks[0].Prebuild, "kind-images")
	}
	if manifest.Tasks[1].Prebuild != "" {
		t.Errorf("manifest.Tasks[1].Prebuild = %q, want empty string", manifest.Tasks[1].Prebuild)
	}

	copied := manifest.DeepCopy()
	copiedPrebuild := copied.Prebuilds["kind-images"]
	copiedPrebuild.Command[0] = "sh"
	copiedPrebuild.Paths[0] = "other/**"
	copiedPrebuild.Resources[0].Limits[corev1.ResourceCPU] = resource.MustParse("1")
	if manifest.Prebuilds["kind-images"].Command[0] != "bash" {
		t.Errorf("DeepCopy shared Command slice with original manifest")
	}
	if manifest.Prebuilds["kind-images"].Paths[0] != "cmd/**" {
		t.Errorf("DeepCopy shared Paths slice with original manifest")
	}
	originalCPU := manifest.Prebuilds["kind-images"].Resources[0].Limits[corev1.ResourceCPU]
	if originalCPU.Cmp(resource.MustParse("4")) != 0 {
		t.Errorf("DeepCopy shared Resources Limits with original manifest: got %s, want 4", originalCPU.String())
	}
}

func TestValidateManifest_Prebuilds(t *testing.T) {
	validPrebuild := v1alpha1.ManifestPrebuild{
		Image:   "builder:v1",
		Command: []string{"./scripts/prebuild.sh"},
	}

	tests := []struct {
		name               string
		manifest           *v1alpha1.Manifest
		expectError        bool
		expectedSubstrings []string
	}{
		{
			name: "backward compatible manifest without prebuilds",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@task1"},
				},
			},
			expectError: false,
		},
		{
			name: "backward compatible manifest with empty prebuilds map",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@task1"},
				},
			},
			expectError: false,
		},
		{
			name: "valid manifest with prebuilds and tasks with and without prebuild",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"a": {
						Image:          "builder:v1",
						Command:        []string{"make", "images"},
						TimeoutSeconds: 0,
					},
					"kind-images": {
						Image:          "kube-agents-builder:v1",
						Command:        []string{"bash", "-c", "echo build"},
						TimeoutSeconds: 1800,
						Paths:          []string{"cmd/**", "pkg/**", "go.mod"},
						Resources: []v1alpha1.ManifestResource{
							{
								Type: v1alpha1.ResourceTypeSandbox,
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("16"),
									corev1.ResourceMemory: resource.MustParse("64Gi"),
								},
							},
							{
								Type: v1alpha1.ResourceTypeKindCluster,
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("6"),
									corev1.ResourceMemory: resource.MustParse("12Gi"),
								},
							},
						},
					},
					strings.Repeat("b", 63): validPrebuild,
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-with-kind-prebuild", Spec: "eval.py@t1", Prebuild: "kind-images"},
					{ID: "task-with-single-char-prebuild", Spec: "eval.py@t2", Prebuild: "a"},
					{ID: "task-without-prebuild", Spec: "eval.py@t3"},
				},
			},
			expectError: false,
		},
		{
			name: "undefined task prebuild with nil prebuilds map",
			manifest: &v1alpha1.Manifest{
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-missing-prebuild", Spec: "eval.py@t1", Prebuild: "kind-images"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"task-missing-prebuild", "undefined prebuild", "kind-images"},
		},
		{
			name: "undefined task prebuild when other prebuilds exist",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"existing-prebuild": validPrebuild,
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-wrong-prebuild", Spec: "eval.py@t1", Prebuild: "other-prebuild"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"task-wrong-prebuild", "undefined prebuild", "other-prebuild"},
		},
		{
			name: "whitespace task prebuild fails validation",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"existing-prebuild": validPrebuild,
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-whitespace-prebuild", Spec: "eval.py@t1", Prebuild: "   "},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"task-whitespace-prebuild", "undefined prebuild"},
		},
		{
			name: "empty prebuild image",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"no-image": {
						Image:   "",
						Command: []string{"./build.sh"},
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1", Prebuild: "no-image"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"no-image", "image"},
		},
		{
			name: "whitespace prebuild image",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"blank-image": {
						Image:   "   ",
						Command: []string{"./build.sh"},
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"blank-image", "image"},
		},
		{
			name: "nil prebuild command",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"nil-command": {
						Image: "builder:v1",
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"nil-command", "command"},
		},
		{
			name: "empty prebuild command slice",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"empty-command": {
						Image:   "builder:v1",
						Command: []string{},
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"empty-command", "command"},
		},
		{
			name: "blank executable in prebuild command",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"blank-executable": {
						Image:   "builder:v1",
						Command: []string{"   "},
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"blank-executable", "command"},
		},
		{
			name: "negative prebuild timeoutSeconds",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"neg-timeout": {
						Image:          "builder:v1",
						Command:        []string{"./build.sh"},
						TimeoutSeconds: -1,
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"neg-timeout", "timeoutSeconds", "-1"},
		},
		{
			name: "invalid prebuild name empty",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"": validPrebuild,
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"invalid name"},
		},
		{
			name: "invalid prebuild name uppercase",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"Kind-Images": validPrebuild,
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"Kind-Images", "invalid name"},
		},
		{
			name: "invalid prebuild name underscore",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"kind_images": validPrebuild,
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"kind_images", "invalid name"},
		},
		{
			name: "invalid prebuild name leading dash",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"-kind": validPrebuild,
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"-kind", "invalid name"},
		},
		{
			name: "invalid prebuild name trailing dash",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"kind-": validPrebuild,
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"kind-", "invalid name"},
		},
		{
			name: "invalid prebuild name slash",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"kind/images": validPrebuild,
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"kind/images", "invalid name"},
		},
		{
			name: "invalid prebuild name exceeding 63 chars",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					strings.Repeat("a", 64): validPrebuild,
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"invalid name"},
		},
		{
			name: "invalid prebuild resource empty type",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"bad-resource-type": {
						Image:     "builder:v1",
						Command:   []string{"./build.sh"},
						Resources: []v1alpha1.ManifestResource{{Type: ""}},
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"bad-resource-type", "empty type"},
		},
		{
			name: "invalid prebuild resource limits on non-sandbox resource",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"bad-gcp-limits": {
						Image:   "builder:v1",
						Command: []string{"./build.sh"},
						Resources: []v1alpha1.ManifestResource{
							{
								Type:   v1alpha1.ResourceTypeGCPProject,
								Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
							},
						},
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"bad-gcp-limits", "limits"},
		},
		{
			name: "invalid prebuild resource unsupported limit key",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"bad-limit-key": {
						Image:   "builder:v1",
						Command: []string{"./build.sh"},
						Resources: []v1alpha1.ManifestResource{
							{
								Type:   v1alpha1.ResourceTypeSandbox,
								Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
							},
						},
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"bad-limit-key", "nvidia.com/gpu"},
		},
		{
			name: "invalid prebuild resource cpu over cap",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"over-cpu": {
						Image:   "builder:v1",
						Command: []string{"./build.sh"},
						Resources: []v1alpha1.ManifestResource{
							{
								Type:   v1alpha1.ResourceTypeKindCluster,
								Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("17")},
							},
						},
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"over-cpu", "cpu", "17"},
		},
		{
			name: "invalid prebuild resource memory over cap",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"over-memory": {
						Image:   "builder:v1",
						Command: []string{"./build.sh"},
						Resources: []v1alpha1.ManifestResource{
							{
								Type:   v1alpha1.ResourceTypeSandbox,
								Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("65Gi")},
							},
						},
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"over-memory", "memory", "65Gi"},
		},
		{
			name: "invalid prebuild resource zero cpu",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"zero-cpu": {
						Image:   "builder:v1",
						Command: []string{"./build.sh"},
						Resources: []v1alpha1.ManifestResource{
							{
								Type:   v1alpha1.ResourceTypeSandbox,
								Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")},
							},
						},
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"zero-cpu", "cpu", "0"},
		},
		{
			name: "invalid prebuild path pattern",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"bad-path": {
						Image:   "builder:v1",
						Command: []string{"./build.sh"},
						Paths:   []string{"cmd/["},
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"bad-path", "path pattern"},
		},
		{
			name: "empty prebuild path pattern",
			manifest: &v1alpha1.Manifest{
				Prebuilds: map[string]v1alpha1.ManifestPrebuild{
					"empty-path": {
						Image:   "builder:v1",
						Command: []string{"./build.sh"},
						Paths:   []string{""},
					},
				},
				Tasks: []v1alpha1.ManifestTask{
					{ID: "task-1", Spec: "eval.py@t1"},
				},
			},
			expectError:        true,
			expectedSubstrings: []string{"empty-path", "empty path pattern"},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := ValidateManifest(testCase.manifest)
			if !testCase.expectError {
				if err != nil {
					t.Fatalf("ValidateManifest unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateManifest expected error, got nil")
			}
			errorMessage := err.Error()
			for _, expectedSubstring := range testCase.expectedSubstrings {
				if !strings.Contains(errorMessage, expectedSubstring) {
					t.Errorf("error %q should contain %q", errorMessage, expectedSubstring)
				}
			}
		})
	}
}
