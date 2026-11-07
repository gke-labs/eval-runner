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

package e2e

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/forge"
	"github.com/gke-labs/eval-runner/pkg/forge/gitremote"
)

func TestRunDiscovery(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("skipping e2e test: RUN_E2E is not set")
	}

	clusterName := os.Getenv("KIND_CLUSTER_NAME")
	if clusterName == "" {
		clusterName = "kind"
	}

	harness := NewHarness(t, clusterName)
	harness.Setup()

	// Deploy eval-runner controller from k8s manifests
	harness.DeployEvalRunner()

	// Deploy git server in eval-runner-sandbox namespace
	harness.DeployGitServer("eval-runner-sandbox")

	commit := harness.GetGitServerCommit("eval-runner-sandbox", "support-agent.git")
	t.Logf("Fixture repository commit: %s", commit)

	t.Run("DiscoveredSuccess", func(t *testing.T) {
		runName := "support-agent-success"
		manifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/support-agent.git
    commit: %s
`, runName, commit)

		harness.KubectlApplyContent(manifest)

		// Assert sandbox was created in eval-runner-sandbox
		sandboxName := discovery.SandboxNameForRun(runName)
		harness.WaitForSandboxExists("eval-runner-sandbox", sandboxName, 30*time.Second)

		run := harness.WaitForRunPhase("eval-runner", runName, "Discovered", 90*time.Second)

		if run.Status.DiscoveryDeadline == nil {
			t.Errorf("Expected status.discoveryDeadline to be populated")
		}

		if run.Status.Discovery == nil {
			t.Fatalf("Expected status.discovery to be populated")
		}
		if run.Status.Discovery.TaskCount != 5 {
			t.Fatalf("Expected 5 tasks in status.discovery.taskCount, got %d", run.Status.Discovery.TaskCount)
		}
		if run.Status.Discovery.EligibleTaskCount != 3 {
			t.Fatalf("Expected 3 eligible tasks, got %d", run.Status.Discovery.EligibleTaskCount)
		}
		if len(run.Status.Discovery.TaskIDs) != 5 {
			t.Fatalf("Expected 5 taskIDs in status.discovery, got %d", len(run.Status.Discovery.TaskIDs))
		}
		if run.Status.Discovery.ManifestPath == "" || run.Status.Discovery.ManifestHash == "" {
			t.Fatalf("Expected ManifestPath and ManifestHash populated, got: %+v", run.Status.Discovery)
		}
		if !strings.HasPrefix(run.Status.Discovery.ManifestHash, "sha256:") {
			t.Errorf("Expected ManifestHash to start with sha256:, got %s", run.Status.Discovery.ManifestHash)
		}

		expectedTasks := map[string]bool{
			"triage/runbook_lookup":    false,
			"triage/escalation_policy": false,
			"triage/summary_quality":   false,
			"triage/sandbox_eval":      false,
			"triage/sandbox_root_eval": false,
		}
		for _, id := range run.Status.Discovery.TaskIDs {
			if _, ok := expectedTasks[id]; !ok {
				t.Errorf("Unexpected task id: %s", id)
			} else {
				expectedTasks[id] = true
			}
		}
		for taskID, found := range expectedTasks {
			if !found {
				t.Errorf("Expected task %q was not discovered", taskID)
			}
		}

		// Assert /tmp/artifacts/runs/<commit>/manifest.json exists in kind
		manifestPathSuffix := fmt.Sprintf("artifacts/runs/%s/manifest.json", commit)
		findCmd := fmt.Sprintf("find /var/lib/kubelet/pods -path '*/%s' -type f", manifestPathSuffix)
		nodeContainer := fmt.Sprintf("%s-control-plane", harness.clusterName)
		foundFiles := harness.runCmd("docker", "exec", nodeContainer, "sh", "-c", findCmd)
		if strings.TrimSpace(foundFiles) == "" {
			t.Fatalf("Expected %s to exist in kind node, but find returned empty", manifestPathSuffix)
		}
		firstFile := strings.Split(strings.TrimSpace(foundFiles), "\n")[0]
		manifestContent := harness.runCmd("docker", "exec", nodeContainer, "cat", firstFile)
		if strings.TrimSpace(manifestContent) == "" {
			t.Fatalf("Expected %s to have content in kind node, got empty", firstFile)
		}
		var storedManifest v1alpha1.Manifest
		if err := json.Unmarshal([]byte(manifestContent), &storedManifest); err != nil {
			t.Fatalf("Failed to parse stored manifest JSON from kind node: %v", err)
		}
		if len(storedManifest.Tasks) != 5 {
			t.Errorf("Expected 5 tasks in stored manifest, got %d", len(storedManifest.Tasks))
		}

		// Assert sandbox was deleted from eval-runner-sandbox
		out := harness.runCmd("kubectl", "get", "sandbox", sandboxName, "-n", "eval-runner-sandbox", "--ignore-not-found")
		if strings.TrimSpace(out) != "" {
			t.Errorf("Expected sandbox %s to be deleted, but found:\n%s", sandboxName, out)
		}

		// Assert CA Secret exists in eval-runner
		caSec := harness.runCmd("kubectl", "get", "secret", "sandbox-ca", "-n", "eval-runner", "--ignore-not-found")
		if strings.TrimSpace(caSec) == "" {
			t.Errorf("Expected sandbox-ca Secret in eval-runner, got empty")
		}

		// Assert CA ConfigMap exists in eval-runner-sandbox
		caCM := harness.runCmd("kubectl", "get", "configmap", "sandbox-ca", "-n", "eval-runner-sandbox", "--ignore-not-found")
		if strings.TrimSpace(caCM) == "" {
			t.Errorf("Expected sandbox-ca ConfigMap in eval-runner-sandbox, got empty")
		}

		// Assert server cert secret is deleted from eval-runner-sandbox
		tlsSec := harness.runCmd("kubectl", "get", "secret", "sb-tls-"+sandboxName, "-n", "eval-runner-sandbox", "--ignore-not-found")
		if strings.TrimSpace(tlsSec) != "" {
			t.Errorf("Expected server cert secret sb-tls-%s to be deleted, but found:\n%s", sandboxName, tlsSec)
		}

		// Delete Run and assert no sandbox remains
		harness.runCmd("kubectl", "delete", "run", runName, "-n", "eval-runner")
		harness.WaitForRunDeleted("eval-runner", runName, 30*time.Second)

		out = harness.runCmd("kubectl", "get", "sandbox", sandboxName, "-n", "eval-runner-sandbox", "--ignore-not-found")
		if strings.TrimSpace(out) != "" {
			t.Errorf("Expected no sandbox %s in eval-runner-sandbox after Run deletion, but found:\n%s", sandboxName, out)
		}
	})

	t.Run("BadCommitFails", func(t *testing.T) {
		runName := "support-agent-bad-commit"
		manifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/support-agent.git
    commit: "0000000000000000000000000000000000000000"
`, runName)

		harness.KubectlApplyContent(manifest)

		// Assert sandbox was created in eval-runner-sandbox
		sandboxName := discovery.SandboxNameForRun(runName)
		harness.WaitForSandboxExists("eval-runner-sandbox", sandboxName, 30*time.Second)

		run := harness.WaitForRunPhase("eval-runner", runName, "Failed", 60*time.Second)
		if run.Status.DiscoveryDeadline == nil {
			t.Errorf("Expected status.discoveryDeadline to be populated")
		}
		if strings.TrimSpace(run.Status.Message) == "" {
			t.Errorf("Expected non-empty failure message for bad commit")
		}
		if !strings.Contains(run.Status.Message, "checkout") {
			t.Errorf("Expected failure message to name the checkout step, got: %s", run.Status.Message)
		}
		if strings.Contains(run.Status.Message, "attempts") {
			t.Errorf("Expected bad commit to fail promptly without retries, got: %s", run.Status.Message)
		}

		// Assert sandbox was deleted from eval-runner-sandbox
		out := harness.runCmd("kubectl", "get", "sandbox", sandboxName, "-n", "eval-runner-sandbox", "--ignore-not-found")
		if strings.TrimSpace(out) != "" {
			t.Errorf("Expected sandbox %s to be deleted on failure, but found:\n%s", sandboxName, out)
		}

		// Delete Run and assert no sandbox remains
		harness.runCmd("kubectl", "delete", "run", runName, "-n", "eval-runner")
		harness.WaitForRunDeleted("eval-runner", runName, 30*time.Second)

		out = harness.runCmd("kubectl", "get", "sandbox", sandboxName, "-n", "eval-runner-sandbox", "--ignore-not-found")
		if strings.TrimSpace(out) != "" {
			t.Errorf("Expected no sandbox %s in eval-runner-sandbox after Run deletion, but found:\n%s", sandboxName, out)
		}
	})

	t.Run("MissingEvalsListFails", func(t *testing.T) {
		noEvalsCommit := harness.GetGitServerCommit("eval-runner-sandbox", "no-evals.git")
		t.Logf("Fixture repository without .evals/list commit: %s", noEvalsCommit)

		runName := "support-agent-missing-evals"
		manifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/no-evals.git
    commit: %s
`, runName, noEvalsCommit)

		harness.KubectlApplyContent(manifest)

		// Assert sandbox was created in eval-runner-sandbox
		sandboxName := discovery.SandboxNameForRun(runName)
		harness.WaitForSandboxExists("eval-runner-sandbox", sandboxName, 30*time.Second)

		run := harness.WaitForRunPhase("eval-runner", runName, "Failed", 60*time.Second)
		if run.Status.DiscoveryDeadline == nil {
			t.Errorf("Expected status.discoveryDeadline to be populated")
		}
		if !strings.Contains(run.Status.Message, ".evals/list not found or not executable; see docs/concept/functionality.md §2.3") {
			t.Errorf("Expected failure message to report .evals/list not found or not executable, got: %s", run.Status.Message)
		}
		if strings.Contains(run.Status.Message, "attempts") {
			t.Errorf("Expected missing .evals/list to fail promptly without retries, got: %s", run.Status.Message)
		}

		// Assert sandbox was deleted from eval-runner-sandbox
		out := harness.runCmd("kubectl", "get", "sandbox", sandboxName, "-n", "eval-runner-sandbox", "--ignore-not-found")
		if strings.TrimSpace(out) != "" {
			t.Errorf("Expected sandbox %s to be deleted on failure, but found:\n%s", sandboxName, out)
		}

		// Delete Run and assert no sandbox remains
		harness.runCmd("kubectl", "delete", "run", runName, "-n", "eval-runner")
		harness.WaitForRunDeleted("eval-runner", runName, 30*time.Second)

		out = harness.runCmd("kubectl", "get", "sandbox", sandboxName, "-n", "eval-runner-sandbox", "--ignore-not-found")
		if strings.TrimSpace(out) != "" {
			t.Errorf("Expected no sandbox %s in eval-runner-sandbox after Run deletion, but found:\n%s", sandboxName, out)
		}
	})

	t.Run("DocsNoManifestFallback", func(t *testing.T) {
		docsNoManifestCommit := harness.GetGitServerCommit("eval-runner-sandbox", "docs-no-manifest.git")
		t.Logf("Fixture repository docs-no-manifest commit: %s", docsNoManifestCommit)

		runName := "docs-no-manifest-fails"
		manifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/docs-no-manifest.git
    commit: %s
`, runName, docsNoManifestCommit)

		harness.KubectlApplyContent(manifest)

		// Assert sandbox was created in eval-runner-sandbox
		sandboxName := discovery.SandboxNameForRun(runName)
		harness.WaitForSandboxExists("eval-runner-sandbox", sandboxName, 30*time.Second)

		run := harness.WaitForRunPhase("eval-runner", runName, "Failed", 60*time.Second)

		if run.Status.DiscoveryDeadline == nil {
			t.Errorf("Expected status.discoveryDeadline to be populated")
		}
		if !strings.Contains(run.Status.Message, ".evals/list not found or not executable; see docs/concept/functionality.md §2.3") {
			t.Errorf("Expected failure message to report .evals/list not found or not executable, got: %s", run.Status.Message)
		}
		if strings.Contains(run.Status.Message, "attempts") {
			t.Errorf("Expected missing .evals/list to fail promptly without retries, got: %s", run.Status.Message)
		}

		// Delete Run and assert cleanup
		harness.runCmd("kubectl", "delete", "run", runName, "-n", "eval-runner")
		harness.WaitForRunDeleted("eval-runner", runName, 30*time.Second)
	})

	t.Run("DeleteMidDiscovery", func(t *testing.T) {
		runName := "support-agent-delete-mid-discovery"
		manifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/support-agent.git
    commit: %s
`, runName, commit)

		harness.KubectlApplyContent(manifest)

		// Assert sandbox was created in eval-runner-sandbox
		sandboxName := discovery.SandboxNameForRun(runName)
		harness.WaitForSandboxExists("eval-runner-sandbox", sandboxName, 30*time.Second)

		// Delete the Run while it is mid-discovery
		harness.runCmd("kubectl", "delete", "run", runName, "-n", "eval-runner")

		// Assert Run is deleted (finalizer cleanup completes)
		harness.WaitForRunDeleted("eval-runner", runName, 30*time.Second)

		// Assert sandbox was deleted from eval-runner-sandbox
		harness.WaitForSandboxDeleted("eval-runner-sandbox", sandboxName, 30*time.Second)
	})

	t.Run("UISmoke", func(t *testing.T) {
		runName := "smoke-ui-run"
		manifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: https://github.com/gke-labs/eval-runner.git
    commit: a81d0c4
  model: test-model
`, runName)
		harness.KubectlApplyContent(manifest)
		t.Cleanup(func() {
			harness.runCmd("kubectl", "delete", "run", runName, "-n", "eval-runner", "--ignore-not-found")
		})

		runUISmoke(t, harness, runName, false)
	})

	t.Run("ConfigMapReload", func(t *testing.T) {
		t.Cleanup(func() {
			restoreJSON := fmt.Sprintf(`{"data":{"artifactDir":%q}}`, "/tmp/artifacts")
			harness.runCmd("kubectl", "patch", "configmap", "eval-runner-config", "-n", "eval-runner", "--type=merge", "-p", restoreJSON)
		})

		restartsBefore := harness.runCmd("kubectl", "get", "pods", "-n", "eval-runner", "-l", "app=eval-runner-controller", "-o", "jsonpath={.items[0].status.containerStatuses[0].restartCount}")

		newDir := "/tmp/artifacts-reloaded-e2e"
		patchJSON := fmt.Sprintf(`{"data":{"artifactDir":%q}}`, newDir)
		harness.runCmd("kubectl", "patch", "configmap", "eval-runner-config", "-n", "eval-runner", "--type=merge", "-p", patchJSON)

		deadline := time.Now().Add(30 * time.Second)
		var found bool
		for time.Now().Before(deadline) {
			logs := harness.runCmd("kubectl", "logs", "-n", "eval-runner", "deployment/eval-runner-controller", "--since=60s")
			if strings.Contains(logs, "effective cluster settings") && strings.Contains(logs, newDir) {
				found = true
				break
			}
			time.Sleep(1 * time.Second)
		}
		if !found {
			t.Fatalf("expected controller logs to show effective cluster settings with artifactDir=%q within 30s", newDir)
		}

		restartsAfter := harness.runCmd("kubectl", "get", "pods", "-n", "eval-runner", "-l", "app=eval-runner-controller", "-o", "jsonpath={.items[0].status.containerStatuses[0].restartCount}")
		if restartsBefore != restartsAfter {
			t.Fatalf("expected 0 pod restarts, restartCount changed from %s to %s", restartsBefore, restartsAfter)
		}
	})

	t.Run("SupportAgentSandboxTaskCompleted", func(t *testing.T) {
		supportAgentCommit := harness.GetGitServerCommit("eval-runner-sandbox", "support-agent.git")
		runName := "support-agent-sandbox-run"
		manifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/support-agent.git
    commit: %s
  model: mockllm/model
  tasks:
  - triage/sandbox_eval
  - triage/sandbox_root_eval
`, runName, supportAgentCommit)

		harness.KubectlApplyContent(manifest)

		// Wait for Run to complete
		run := harness.WaitForRunPhase("eval-runner", runName, string(v1alpha1.RunPhaseCompleted), 180*time.Second)

		if run.Status.Discovery == nil {
			t.Fatalf("Expected status.discovery to be populated")
		}
		if run.Status.Discovery.TaskCount != 5 {
			t.Fatalf("Expected 5 tasks in discovery, got %d", run.Status.Discovery.TaskCount)
		}

		if len(run.Status.Tasks) != 2 {
			t.Fatalf("Expected 2 tasks in status.tasks, got %d: %+v", len(run.Status.Tasks), run.Status.Tasks)
		}

		tasksByID := make(map[string]v1alpha1.TaskStatus)
		for _, task := range run.Status.Tasks {
			tasksByID[task.ID] = task
		}
		for _, expectedTaskID := range []string{"triage/sandbox_eval", "triage/sandbox_root_eval"} {
			task, ok := tasksByID[expectedTaskID]
			if !ok {
				t.Fatalf("Expected task %s in status.tasks", expectedTaskID)
			}
			if task.Phase != v1alpha1.TaskPhaseCompleted {
				t.Errorf("Expected task %s Completed, got %s (msg: %s)", expectedTaskID, task.Phase, task.Message)
			}
			if task.Samples != 1 {
				t.Errorf("Expected 1 sample for %s, got %d", expectedTaskID, task.Samples)
			}
			if task.Successes != 1 {
				t.Errorf("Expected 1 success for %s, got %d", expectedTaskID, task.Successes)
			}
			if task.SamplesDone != 1 {
				t.Errorf("Expected samplesDone 1 for %s, got %d", expectedTaskID, task.SamplesDone)
			}
			if task.MeanScore == nil || *task.MeanScore != 1.0 {
				t.Errorf("Expected meanScore 1.0 for %s, got %v", expectedTaskID, task.MeanScore)
			}
		}

		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", runName, 30*time.Second)
	})

	t.Run("TerminalBenchSandboxTaskCompleted", func(t *testing.T) {
		terminalBenchCommit := harness.GetGitServerCommit("eval-runner-sandbox", "terminal-bench.git")
		runName := "terminal-bench-sandbox-run"
		manifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/terminal-bench.git
    commit: %s
  model: mockllm/model
  tasks:
  - terminal-bench/regex-log
`, runName, terminalBenchCommit)

		harness.KubectlApplyContent(manifest)

		// Wait for Run to complete
		run := harness.WaitForRunPhase("eval-runner", runName, string(v1alpha1.RunPhaseCompleted), 600*time.Second)

		if run.Status.Discovery == nil {
			t.Fatalf("Expected status.discovery to be populated")
		}
		if run.Status.Discovery.TaskCount != 89 {
			t.Fatalf("Expected 89 tasks in discovery, got %d", run.Status.Discovery.TaskCount)
		}

		if len(run.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task in status.tasks, got %d: %+v", len(run.Status.Tasks), run.Status.Tasks)
		}

		task := run.Status.Tasks[0]
		if task.ID != "terminal-bench/regex-log" {
			t.Errorf("Expected task ID terminal-bench/regex-log, got %s", task.ID)
		}
		if task.Phase != v1alpha1.TaskPhaseCompleted {
			t.Errorf("Expected task %s Completed, got %s (msg: %s)", task.ID, task.Phase, task.Message)
		}
		if task.Samples != 1 {
			t.Errorf("Expected 1 sample for %s, got %d", task.ID, task.Samples)
		}
		if task.Successes != 1 {
			t.Errorf("Expected 1 success for %s, got %d", task.ID, task.Successes)
		}
		if task.SamplesDone != 1 {
			t.Errorf("Expected samplesDone 1 for %s, got %d", task.ID, task.SamplesDone)
		}
		if task.MeanScore == nil || *task.MeanScore != 1.0 {
			t.Errorf("Expected meanScore 1.0 for %s, got %v", task.ID, task.MeanScore)
		}

		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", runName, 30*time.Second)
	})

	t.Run("WebhookSmoke", func(t *testing.T) {
		runWebhookSmoke(t, harness)
	})

	t.Run("GitRemoteSmoke", func(t *testing.T) {
		runGitRemoteSmoke(t, harness, "eval-runner-sandbox")
	})
}

func runGitRemoteSmoke(t *testing.T, harness *Harness, sandboxNamespace string) {
	// Pick an available local port for port-forwarding
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("failed to close listener: %v", err)
	}

	cmd := exec.Command("kubectl", "port-forward", "-n", sandboxNamespace, "svc/git-server", fmt.Sprintf("%d:9418", port))
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start git-server port-forward: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	// Wait for port-forward to be ready
	ready := false
	for i := 0; i < 30; i++ {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			ready = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("port-forward to git-server failed to become ready on port %d", port)
	}

	ref := forge.RepoRef{
		Host:  "git-server.eval-runner-sandbox.svc",
		Owner: "-",
		Name:  "docs-eval",
	}

	f := gitremote.New(gitremote.WithURLResolver(func(r forge.RepoRef) string {
		return fmt.Sprintf("git://127.0.0.1:%d/%s.git", port, r.Name)
	}))

	branches, err := f.Branches(t.Context(), ref)
	if err != nil {
		t.Fatalf("f.Branches() failed: %v", err)
	}

	foundMain := false
	for _, b := range branches {
		if b.Name == "main" {
			foundMain = true
			if b.SHA == "" {
				t.Errorf("expected non-empty SHA for branch main, got %q", b.SHA)
			}
			break
		}
	}
	if !foundMain {
		t.Errorf("expected branches to return 'main', got %+v", branches)
	}
}
