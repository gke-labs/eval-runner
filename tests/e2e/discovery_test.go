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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/pkg/discovery"
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

		if run.Status.Manifest == nil {
			t.Fatalf("Expected status.manifest to be populated")
		}
		if len(run.Status.Manifest.Tasks) != 3 {
			t.Fatalf("Expected 3 tasks in manifest, got %d", len(run.Status.Manifest.Tasks))
		}

		expectedTasks := map[string]bool{
			"triage/runbook_lookup":    false,
			"triage/escalation_policy": false,
			"triage/summary_quality":   false,
		}
		for _, task := range run.Status.Manifest.Tasks {
			if _, ok := expectedTasks[task.ID]; !ok {
				t.Errorf("Unexpected task id: %s", task.ID)
			} else {
				expectedTasks[task.ID] = true
			}
		}
		for taskID, found := range expectedTasks {
			if !found {
				t.Errorf("Expected task %q was not discovered", taskID)
			}
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
		if !strings.Contains(run.Status.Message, ".evals/list") {
			t.Errorf("Expected failure message to name .evals/list step, got: %s", run.Status.Message)
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
}
