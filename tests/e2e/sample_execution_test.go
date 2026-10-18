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
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/discovery"
)

func TestRunSampleExecution(t *testing.T) {
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

	commit := harness.GetGitServerCommit("eval-runner-sandbox", "docs-eval.git")
	t.Logf("docs-eval fixture repository commit: %s", commit)

	t.Run("DocsComprehensionCompleted", func(t *testing.T) {
		runName := "docs-comprehension-sample-run"
		manifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/docs-eval.git
    commit: %s
  model: mockllm/model
  limit: 2
`, runName, commit)

		harness.KubectlApplyContent(manifest)

		// Wait for Run to reach Completed
		run := harness.WaitForRunPhase("eval-runner", runName, string(v1alpha1.RunPhaseCompleted), 180*time.Second)

		if run.Status.Discovery == nil {
			t.Fatalf("Expected status.discovery to be populated")
		}
		if run.Status.Discovery.TaskCount != 2 {
			t.Fatalf("Expected 2 tasks in discovery, got %d", run.Status.Discovery.TaskCount)
		}

		if len(run.Status.Tasks) != 2 {
			t.Fatalf("Expected 2 tasks in status.tasks, got %d: %+v", len(run.Status.Tasks), run.Status.Tasks)
		}

		expectedTasks := map[string]bool{
			"docs/readme_comprehension":       false,
			"docs/readme_comprehension_blind": false,
		}

		for _, task := range run.Status.Tasks {
			if _, ok := expectedTasks[task.ID]; !ok {
				t.Errorf("Unexpected task ID in status.tasks: %s", task.ID)
			} else {
				expectedTasks[task.ID] = true
			}

			if task.Phase != v1alpha1.TaskPhaseCompleted {
				t.Errorf("Expected task %s to be Completed, got %s (message: %s)", task.ID, task.Phase, task.Message)
			}
			if task.Samples <= 0 {
				t.Errorf("Expected task %s to have samples > 0, got %d", task.ID, task.Samples)
			}
		}

		for taskID, found := range expectedTasks {
			if !found {
				t.Errorf("Expected task %q was not found in status.tasks", taskID)
			}
		}

		// Assert task sandboxes are gone from eval-runner-sandbox
		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", runName, 30*time.Second)

		// Assert that per-sandbox server cert secrets were deleted
		for _, task := range run.Status.Tasks {
			sbName := discovery.TaskSandboxNameForRun(runName, task.ID)
			secName := "sb-tls-" + sbName
			tlsSec := harness.runCmd("kubectl", "get", "secret", secName, "-n", "eval-runner-sandbox", "--ignore-not-found")
			if strings.TrimSpace(tlsSec) != "" {
				t.Errorf("Expected server cert secret %s to be deleted, but found:\n%s", secName, tlsSec)
			}
		}

		// Assert that no model Secret was mounted
		secretsOut := harness.runCmd("kubectl", "get", "secrets", "-n", "eval-runner-sandbox", "-o", "jsonpath={.items[*].metadata.name}")
		if strings.Contains(secretsOut, "model") || strings.Contains(secretsOut, "gemini") {
			t.Errorf("Unexpected model secret found in eval-runner-sandbox: %s", secretsOut)
		}
	})

	t.Run("ComparisonMockLLMExecution", func(t *testing.T) {
		commit := harness.GetGitServerCommit("eval-runner-sandbox", "docs-eval.git")
		baseRunName := "docs-comp-base"
		candRunName := "docs-comp-cand"
		compName := "docs-mockllm-comparison"

		// Clean up any existing resources from prior runs
		harness.runCmd("kubectl", "delete", "comparison", compName, "-n", "eval-runner", "--ignore-not-found")
		harness.runCmd("kubectl", "delete", "run", baseRunName, candRunName, "-n", "eval-runner", "--ignore-not-found")

		baseManifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/docs-eval.git
    commit: %s
  model: mockllm/model
  limit: 10
`, baseRunName, commit)

		candManifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/docs-eval.git
    commit: %s
  model: mockllm/model
  limit: 10
`, candRunName, commit)

		compManifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Comparison
metadata:
  name: %s
  namespace: eval-runner
spec:
  baseline:
    run: %s
  candidate:
    run: %s
  equivalenceMargin: 0.05
`, compName, baseRunName, candRunName)

		harness.KubectlApplyContent(baseManifest)
		harness.KubectlApplyContent(candManifest)
		harness.KubectlApplyContent(compManifest)

		// Wait for both Runs to reach Completed
		harness.WaitForRunPhase("eval-runner", baseRunName, string(v1alpha1.RunPhaseCompleted), 180*time.Second)
		harness.WaitForRunPhase("eval-runner", candRunName, string(v1alpha1.RunPhaseCompleted), 180*time.Second)

		// Wait for Comparison to reach terminal UNCHANGED verdict
		comp := harness.WaitForComparisonVerdict("eval-runner", compName, string(v1alpha1.ComparisonVerdictUnchanged), 60*time.Second)

		if comp.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("Expected comparison phase Completed, got %s", comp.Status.Phase)
		}
		if comp.Status.Verdict != v1alpha1.ComparisonVerdictUnchanged {
			t.Errorf("Expected comparison verdict UNCHANGED, got %s", comp.Status.Verdict)
		}
		if comp.Status.PImprovement == nil || math.Abs(*comp.Status.PImprovement-0.50) > 1e-4 {
			t.Errorf("Expected pImprovement 0.50, got %v", comp.Status.PImprovement)
		}
		if len(comp.Status.Tasks) != 2 {
			t.Fatalf("Expected 2 tasks in comparison status, got %d", len(comp.Status.Tasks))
		}
		for _, task := range comp.Status.Tasks {
			if task.Label != v1alpha1.ComparisonTaskLabelUnchanged {
				t.Errorf("Expected task %s label unchanged, got %s", task.ID, task.Label)
			}
			if task.Pairs != 10 {
				t.Errorf("Expected task %s pairs 10, got %d", task.ID, task.Pairs)
			}
			if task.Wins != 0 || task.Losses != 0 || task.Ties != 10 {
				t.Errorf("Expected task %s wins/losses/ties 0/0/10, got %d/%d/%d", task.ID, task.Wins, task.Losses, task.Ties)
			}
			if task.Delta == nil || math.Abs(*task.Delta) > 1e-4 {
				t.Errorf("Expected task %s delta 0.0, got %v", task.ID, task.Delta)
			}
		}

		if comp.Status.SummaryPath == "" {
			t.Errorf("Expected non-empty summaryPath in comparison status")
		}

		// Verify evalstats -from-comparison output
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatalf("Failed to get home dir: %v", err)
		}
		kubeconfigPath := filepath.Join(home, ".kube", "config")
		statsOut := harness.runCmd("go", "run", "github.com/gke-labs/eval-runner/cmd/evalstats", "-kubeconfig", kubeconfigPath, "-from-comparison", compName, "-namespace", "eval-runner")
		if !strings.Contains(statsOut, "UNCHANGED") {
			t.Errorf("Expected evalstats output to contain UNCHANGED, got:\n%s", statsOut)
		}
		if !strings.Contains(statsOut, "eval") || !strings.Contains(statsOut, "base") || !strings.Contains(statsOut, "this PR") || !strings.Contains(statsOut, "wins/losses/ties") {
			t.Errorf("Expected evalstats output to contain table header with wins/losses/ties, got:\n%s", statsOut)
		}

		// Clean up
		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", baseRunName, 30*time.Second)
		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", candRunName, 30*time.Second)
	})

	t.Run("SampleRangeExecution", func(t *testing.T) {
		run1Name := "sample-range-run-1"
		run2Name := "sample-range-run-2"
		candRunName := "sample-range-run-cand"
		compName := "sample-range-comparison"

		// Clean up any previous runs or comparisons
		harness.runCmd("kubectl", "delete", "run", run1Name, run2Name, candRunName, "-n", "eval-runner", "--ignore-not-found")
		harness.runCmd("kubectl", "delete", "comparison", compName, "-n", "eval-runner", "--ignore-not-found")

		run1Manifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/docs-eval.git
    commit: %s
  model: mockllm/model
  sampleRange:
    start: 1
    end: 5
`, run1Name, commit)

		run2Manifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/docs-eval.git
    commit: %s
  model: mockllm/model
  sampleRange:
    start: 6
    end: 10
`, run2Name, commit)

		runCandManifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/docs-eval.git
    commit: %s
  model: mockllm/model
  sampleRange:
    start: 1
    end: 5
`, candRunName, commit)

		compManifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Comparison
metadata:
  name: %s
  namespace: eval-runner
spec:
  baseline:
    run: %s
  candidate:
    run: %s
  equivalenceMargin: 0.05
`, compName, run1Name, candRunName)

		harness.KubectlApplyContent(run1Manifest)
		harness.KubectlApplyContent(run2Manifest)
		harness.KubectlApplyContent(runCandManifest)
		harness.KubectlApplyContent(compManifest)

		// Wait for all Runs to reach Completed
		run1 := harness.WaitForRunPhase("eval-runner", run1Name, string(v1alpha1.RunPhaseCompleted), 180*time.Second)
		run2 := harness.WaitForRunPhase("eval-runner", run2Name, string(v1alpha1.RunPhaseCompleted), 180*time.Second)
		harness.WaitForRunPhase("eval-runner", candRunName, string(v1alpha1.RunPhaseCompleted), 180*time.Second)

		// Assert status.tasks[].samples is 5 each
		if len(run1.Status.Tasks) == 0 {
			t.Fatalf("Expected tasks in run1 status.tasks, got 0")
		}
		for _, task := range run1.Status.Tasks {
			if task.Phase != v1alpha1.TaskPhaseCompleted {
				t.Errorf("Expected run1 task %s to be Completed, got %s", task.ID, task.Phase)
			}
			if task.Samples != 5 {
				t.Errorf("Expected run1 task %s to have samples=5, got %d", task.ID, task.Samples)
			}
			if task.SampleRange == nil || task.SampleRange.Start != 1 || task.SampleRange.End != 5 {
				t.Errorf("Expected run1 task %s to have sampleRange {1, 5}, got %+v", task.ID, task.SampleRange)
			}
		}

		if len(run2.Status.Tasks) == 0 {
			t.Fatalf("Expected tasks in run2 status.tasks, got 0")
		}
		for _, task := range run2.Status.Tasks {
			if task.Phase != v1alpha1.TaskPhaseCompleted {
				t.Errorf("Expected run2 task %s to be Completed, got %s", task.ID, task.Phase)
			}
			if task.Samples != 5 {
				t.Errorf("Expected run2 task %s to have samples=5, got %d", task.ID, task.Samples)
			}
			if task.SampleRange == nil || task.SampleRange.Start != 6 || task.SampleRange.End != 10 {
				t.Errorf("Expected run2 task %s to have sampleRange {6, 10}, got %+v", task.ID, task.SampleRange)
			}
		}

		// Read stored Inspect logs for run1 and run2 to assert disjoint sample IDs
		nodeContainer := fmt.Sprintf("%s-control-plane", harness.clusterName)
		loadInspectSampleIDs := func(task v1alpha1.TaskStatus) map[string]bool {
			logSuffix := strings.TrimPrefix(task.LogPath, "/tmp/")
			findCmd := fmt.Sprintf("find /var/lib/kubelet/pods -path '*/%s' -type f", logSuffix)
			foundFile := strings.TrimSpace(harness.runCmd("docker", "exec", nodeContainer, "sh", "-c", findCmd))
			if foundFile == "" {
				t.Fatalf("Log file not found on node for %s", task.LogPath)
			}
			firstFile := strings.Split(foundFile, "\n")[0]
			logContent := harness.runCmd("docker", "exec", nodeContainer, "cat", firstFile)
			scores, err := artifacts.ParseInspectSampleScores([]byte(logContent))
			if err != nil {
				t.Fatalf("Failed to parse sample scores for %s: %v", task.ID, err)
			}
			ids := make(map[string]bool)
			for _, s := range scores {
				ids[s.ID] = true
			}
			return ids
		}

		idsRun1 := loadInspectSampleIDs(run1.Status.Tasks[0])
		idsRun2 := loadInspectSampleIDs(run2.Status.Tasks[0])

		if len(idsRun1) != 5 {
			t.Errorf("Expected 5 sample IDs in run1, got %d: %+v", len(idsRun1), idsRun1)
		}
		if len(idsRun2) != 5 {
			t.Errorf("Expected 5 sample IDs in run2, got %d: %+v", len(idsRun2), idsRun2)
		}

		for id := range idsRun1 {
			if idsRun2[id] {
				t.Errorf("Expected disjoint sample IDs, but ID %s appears in both run1 and run2", id)
			}
		}

		// Wait for Comparison to reach Completed phase
		comp := harness.WaitForComparisonPhase("eval-runner", compName, string(v1alpha1.ComparisonPhaseCompleted), 60*time.Second)
		if len(comp.Status.Tasks) == 0 {
			t.Fatalf("Expected comparison tasks, got 0")
		}
		for _, task := range comp.Status.Tasks {
			if task.Pairs != 5 {
				t.Errorf("Expected task %s comparison pairs=5, got %d", task.ID, task.Pairs)
			}
		}

		// Clean up
		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", run1Name, 30*time.Second)
		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", run2Name, 30*time.Second)
		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", candRunName, 30*time.Second)
	})

	t.Run("GCSEmulatorDiscoveryAndExecution", func(t *testing.T) {
		bucket := "test-eval-bucket"
		harness.DeployFakeGCSServer("eval-runner", bucket)
		harness.ConfigureGCSSink(bucket)
		t.Cleanup(func() {
			harness.ConfigureLocalDirSink()
		})

		commit := harness.GetGitServerCommit("eval-runner-sandbox", "docs-eval.git")
		runName := "gcs-emulator-sample-run"

		// Clean up any existing run
		harness.runCmd("kubectl", "delete", "run", runName, "-n", "eval-runner", "--ignore-not-found")

		manifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/docs-eval.git
    commit: %s
  model: mockllm/model
  limit: 1
`, runName, commit)

		harness.KubectlApplyContent(manifest)

		// Wait for Run to reach Completed
		run := harness.WaitForRunPhase("eval-runner", runName, string(v1alpha1.RunPhaseCompleted), 180*time.Second)

		// Verify discovery populated GCS manifest path and hash
		if run.Status.Discovery == nil {
			t.Fatalf("Expected status.discovery to be populated")
		}
		expectedManifestPath := fmt.Sprintf("gs://%s/runs/%s/manifest.json", bucket, commit)
		if run.Status.Discovery.ManifestPath != expectedManifestPath {
			t.Errorf("Expected manifestPath %q, got %q", expectedManifestPath, run.Status.Discovery.ManifestPath)
		}
		if !strings.HasPrefix(run.Status.Discovery.ManifestHash, "sha256:") {
			t.Errorf("Expected ManifestHash to start with sha256:, got %s", run.Status.Discovery.ManifestHash)
		}

		// Verify sample task completed and stored log in GCS
		if len(run.Status.Tasks) == 0 {
			t.Fatalf("Expected tasks in status.tasks, got 0")
		}
		for _, task := range run.Status.Tasks {
			if task.Phase != v1alpha1.TaskPhaseCompleted {
				t.Errorf("Expected task %s to be Completed, got %s (message: %s)", task.ID, task.Phase, task.Message)
			}
			expectedLogPrefix := fmt.Sprintf("gs://%s/runs/%s/%s/", bucket, commit, task.ID)
			if !strings.HasPrefix(task.LogPath, expectedLogPrefix) {
				t.Errorf("Expected task logPath to start with %q, got %q", expectedLogPrefix, task.LogPath)
			}
		}

		// Verify objects actually exist in fake-gcs-server
		objectsJSON := harness.GetGCSObjects("eval-runner", bucket)
		t.Logf("Objects in fake-gcs-server:\n%s", objectsJSON)
		if !strings.Contains(objectsJSON, fmt.Sprintf("runs/%s/manifest.json", commit)) {
			t.Errorf("Expected manifest.json to be stored in fake-gcs-server, but object list was:\n%s", objectsJSON)
		}
		for _, task := range run.Status.Tasks {
			if !strings.Contains(objectsJSON, fmt.Sprintf("runs/%s/%s/summary.json", commit, task.ID)) {
				t.Errorf("Expected summary.json for task %s to be stored in fake-gcs-server", task.ID)
			}
		}

		// Clean up
		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", runName, 30*time.Second)
	})

	t.Run("SamplingPairedBatchLoop", func(t *testing.T) {
		commit := harness.GetGitServerCommit("eval-runner-sandbox", "docs-eval.git")
		candRunName := "batch-cand-run"
		baseRunName := "batch-base-run"
		compName := "batch-loop-comp"

		// Clean up any previous runs or comparisons
		harness.runCmd("kubectl", "delete", "run", candRunName, baseRunName, "-n", "eval-runner", "--ignore-not-found")
		harness.runCmd("kubectl", "delete", "comparison", compName, "-n", "eval-runner", "--ignore-not-found")

		candManifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/docs-eval.git
    commit: %s
  model: mockllm/model
  sampleRange:
    start: 1
    end: 4
`, candRunName, commit)

		baseManifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: %s
  namespace: eval-runner
spec:
  repo:
    url: git://git-server.eval-runner-sandbox.svc/docs-eval.git
    commit: %s
  model: mockllm/model
  sampleRange:
    start: 1
    end: 4
`, baseRunName, commit)

		compManifest := fmt.Sprintf(`
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Comparison
metadata:
  name: %s
  namespace: eval-runner
spec:
  candidate:
    run: %s
  baseline:
    run: %s
  sampling:
    batchSize: 4
    maxSamplesPerTask: 12
  equivalenceMargin: 0.05
`, compName, candRunName, baseRunName)

		harness.KubectlApplyContent(candManifest)
		harness.KubectlApplyContent(baseManifest)
		harness.KubectlApplyContent(compManifest)

		// Wait for Comparison to reach terminal Completed phase (having executed multiple batches)
		comp := harness.WaitForComparisonPhase("eval-runner", compName, string(v1alpha1.ComparisonPhaseCompleted), 180*time.Second)

		if len(comp.Status.Batches) < 2 {
			t.Fatalf("Expected at least 2 batches created, got %d (batches: %+v)", len(comp.Status.Batches), comp.Status.Batches)
		}

		b1 := comp.Status.Batches[0]
		if b1.Index != 1 || b1.CandidateRun != candRunName || b1.BaselineRun != baseRunName {
			t.Errorf("Unexpected batch 1 status: %+v", b1)
		}

		b2 := comp.Status.Batches[1]
		if b2.Index != 2 {
			t.Errorf("Expected batch 2 index 2, got %d", b2.Index)
		}
		if b2.SampleRange == nil || b2.SampleRange.Start != 5 || b2.SampleRange.End != 8 {
			t.Errorf("Expected batch 2 range [5, 8], got %v", b2.SampleRange)
		}

		if len(comp.Status.Tasks) == 0 {
			t.Fatalf("Expected status.tasks to be populated, got 0")
		}
		for _, task := range comp.Status.Tasks {
			if task.SamplesRun < 8 {
				t.Errorf("Expected task %s samplesRun >= 8 across batches, got %d", task.ID, task.SamplesRun)
			}
			if task.Decision == "" {
				t.Errorf("Expected task %s decision to be recorded, got empty", task.ID)
			}
		}

		// Clean up
		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", candRunName, 30*time.Second)
		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", baseRunName, 30*time.Second)
		b2CandName := fmt.Sprintf("%s-b2-cand", compName)
		b2BaseName := fmt.Sprintf("%s-b2-base", compName)
		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", b2CandName, 30*time.Second)
		harness.WaitForAllSandboxesDeletedForRun("eval-runner-sandbox", b2BaseName, 30*time.Second)
	})
}
