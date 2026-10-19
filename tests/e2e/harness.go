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
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
)

const (
	AgentSandboxVersion = "v1.0.7"
	AgentSandboxURL     = "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/" + AgentSandboxVersion + "/sandbox.yaml"
)

type Harness struct {
	t           *testing.T
	clusterName string
}

func NewHarness(t *testing.T, clusterName string) *Harness {
	return &Harness{
		t:           t,
		clusterName: clusterName,
	}
}

func (h *Harness) Setup() {
	h.t.Logf("Setting up harness for cluster %s", h.clusterName)

	gitRoot := h.GetGitRoot()
	buildBin := filepath.Join(gitRoot, ".build", "bin")
	currentPath := os.Getenv("PATH")
	if !strings.Contains(currentPath, buildBin) {
		if info, err := os.Stat(buildBin); err == nil && info.IsDir() {
			if err := os.Setenv("PATH", buildBin+string(os.PathListSeparator)+currentPath); err != nil {
				h.t.Fatalf("Failed to update PATH: %v", err)
			}
		}
	}

	if _, err := exec.LookPath("kind"); err != nil {
		h.t.Fatalf("kind not found on PATH: %v (run ./dev/tasks/test-e2e to install)", err)
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		h.t.Fatalf("kubectl not found on PATH: %v (run ./dev/tasks/test-e2e to install)", err)
	}

	clusters := h.runCmd("kind", "get", "clusters")
	exists := false
	for _, cluster := range strings.Split(clusters, "\n") {
		if strings.TrimSpace(cluster) == h.clusterName {
			exists = true
			break
		}
	}

	if !exists {
		h.t.Logf("Creating kind cluster %s", h.clusterName)
		h.runCmd("kind", "create", "cluster", "--name", h.clusterName)
		h.t.Cleanup(func() {
			if os.Getenv("SKIP_CLEANUP") == "" {
				h.t.Logf("Deleting kind cluster %s", h.clusterName)
				h.runCmd("kind", "delete", "cluster", "--name", h.clusterName)
			} else {
				h.t.Logf("Skipping deletion of kind cluster %s (SKIP_CLEANUP set)", h.clusterName)
			}
		})
	} else {
		h.t.Logf("Reusing existing kind cluster %s", h.clusterName)
	}

	contextName := "kind-" + h.clusterName
	h.runCmd("kubectl", "config", "use-context", contextName)

	h.InstallAgentSandbox()
}

func (h *Harness) InstallAgentSandbox() {
	h.t.Logf("Installing agent-sandbox %s", AgentSandboxVersion)
	h.KubectlApplyFileWithRetry(AgentSandboxURL, 60*time.Second)
	h.runCmd("kubectl", "wait", "--namespace", "agent-sandbox-system", "--for=condition=available", "deployment/agent-sandbox-controller", "--timeout=120s")
}

func (h *Harness) DeployEvalRunner() {
	h.t.Log("Building and deploying eval-runner controller from k8s manifests")
	gitRoot := h.GetGitRoot()

	// Build and load controller image
	h.DockerBuild("eval-runner:e2e", filepath.Join(gitRoot, "images/eval-runner/Dockerfile"), gitRoot)
	h.KindLoad("eval-runner:e2e")
	h.DockerBuild("eval-runner-ui:e2e", filepath.Join(gitRoot, "images/eval-runner-ui/Dockerfile"), gitRoot)
	h.KindLoad("eval-runner-ui:e2e")
	h.DockerBuild("eval-runner-webhook:e2e", filepath.Join(gitRoot, "images/eval-runner-webhook/Dockerfile"), gitRoot)
	h.KindLoad("eval-runner-webhook:e2e")
	h.DockerBuild("sample-runner:e2e", filepath.Join(gitRoot, "images/sample-runner/Dockerfile"), gitRoot)
	h.KindLoad("sample-runner:e2e")

	// Build and load sandboxd image from fork (clones out of tree if sibling is absent)
	h.runCmd(filepath.Join(gitRoot, "dev/tasks/build-sandboxd"))
	agentSandboxDir := os.Getenv("AGENT_SANDBOX_DIR")
	if agentSandboxDir == "" {
		sibling := filepath.Join(gitRoot, "../agent-sandbox")
		if _, err := os.Stat(sibling); err == nil {
			agentSandboxDir = sibling
		} else {
			cacheDir := os.Getenv("XDG_CACHE_HOME")
			if cacheDir == "" {
				home, err := os.UserHomeDir()
				if err != nil || home == "" {
					home = os.TempDir()
				}
				cacheDir = filepath.Join(home, ".cache")
			}
			agentSandboxDir = filepath.Join(cacheDir, "eval-runner", "agent-sandbox")
		}
	}
	h.DockerBuild("sandboxd:e2e", filepath.Join(agentSandboxDir, "packages/sandboxd/Dockerfile"), agentSandboxDir)
	h.KindLoad("sandboxd:e2e")

	// Apply namespaces
	h.KubectlApplyFile(filepath.Join(gitRoot, "k8s/00-namespaces.yaml"))

	// Apply Run and Comparison CRDs and wait for them to be established
	h.KubectlApplyFile(filepath.Join(gitRoot, "k8s/evalrunner.gke-labs.dev_runs.yaml"))
	h.runCmd("kubectl", "wait", "--for=condition=established", "--timeout=60s", "crd/runs.evalrunner.gke-labs.dev")

	h.KubectlApplyFile(filepath.Join(gitRoot, "k8s/evalrunner.gke-labs.dev_comparisons.yaml"))
	h.runCmd("kubectl", "wait", "--for=condition=established", "--timeout=60s", "crd/comparisons.evalrunner.gke-labs.dev")

	// Apply RBAC
	h.KubectlApplyFile(filepath.Join(gitRoot, "k8s/rbac.yaml"))

	// Apply NetworkPolicy
	h.KubectlApplyFile(filepath.Join(gitRoot, "k8s/sandbox-networkpolicy.yaml"))

	// Apply eval-runner-config ConfigMap with artifactDir for local sink support
	cmManifest := `
apiVersion: v1
kind: ConfigMap
metadata:
  name: eval-runner-config
  namespace: eval-runner
data:
  artifactDir: /tmp/artifacts
  defaultModel: mockllm/model
  defaultBatchSize: "8"
  defaultMaxSamplesPerTask: "64"
  commandUsers: "reviewer"
  webhookRepositories: "*"
`
	h.KubectlApplyContent(cmManifest)

	// Apply Secret for eval-runner-webhook HMAC signature verification
	secManifest := `
apiVersion: v1
kind: Secret
metadata:
  name: github-webhook
  namespace: eval-runner
type: Opaque
stringData:
  secret: e2e-webhook-secret
`
	h.KubectlApplyContent(secManifest)

	// Apply Deployment with image rewritten for kind local image
	deployContent, err := os.ReadFile(filepath.Join(gitRoot, "k8s/runner-deployment.yaml"))
	if err != nil {
		h.t.Fatalf("Failed to read runner-deployment.yaml: %v", err)
	}
	modifiedDeploy := strings.Replace(
		string(deployContent),
		"image: eval-runner",
		"image: eval-runner:e2e\n        imagePullPolicy: Never",
		1,
	)
	modifiedDeploy = strings.Replace(
		modifiedDeploy,
		"args:",
		"args:\n        - --sandboxd-image=sandboxd:e2e",
		1,
	)
	h.KubectlApplyContent(modifiedDeploy)

	h.runCmd("kubectl", "wait", "--namespace", "eval-runner", "--for=condition=available", "deployment/eval-runner-controller", "--timeout=90s")

	// Apply UI Deployment with image rewritten for kind local image
	uiDeployContent, err := os.ReadFile(filepath.Join(gitRoot, "k8s/ui-deployment.yaml"))
	if err != nil {
		h.t.Fatalf("Failed to read ui-deployment.yaml: %v", err)
	}
	modifiedUIDeploy := strings.Replace(
		string(uiDeployContent),
		"image: eval-runner-ui",
		"image: eval-runner-ui:e2e\n        imagePullPolicy: Never",
		1,
	)
	h.KubectlApplyContent(modifiedUIDeploy)

	h.runCmd("kubectl", "wait", "--namespace", "eval-runner", "--for=condition=available", "deployment/eval-runner-ui", "--timeout=60s")

	// Apply Webhook Deployment with image rewritten for kind local image
	webhookDeployContent, err := os.ReadFile(filepath.Join(gitRoot, "k8s/webhook-deployment.yaml"))
	if err != nil {
		h.t.Fatalf("Failed to read webhook-deployment.yaml: %v", err)
	}
	modifiedWebhookDeploy := strings.Replace(
		string(webhookDeployContent),
		"image: eval-runner-webhook",
		"image: eval-runner-webhook:e2e\n        imagePullPolicy: Never",
		1,
	)
	h.KubectlApplyContent(modifiedWebhookDeploy)

	h.runCmd("kubectl", "wait", "--namespace", "eval-runner", "--for=condition=available", "deployment/eval-runner-webhook", "--timeout=60s")
}

func (h *Harness) DeployGitServer(namespace string) {
	h.t.Logf("Building and deploying git server in namespace %s", namespace)
	gitRoot := h.GetGitRoot()
	h.DockerBuild("git-server:e2e", filepath.Join(gitRoot, "images/git-server/Dockerfile"), gitRoot)
	h.KindLoad("git-server:e2e")

	manifest := fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: git-server
  namespace: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: git-server
  template:
    metadata:
      labels:
        app: git-server
    spec:
      securityContext:
        runAsNonRoot: true
        seccompProfile:
          type: RuntimeDefault
      containers:
      - name: git-server
        image: git-server:e2e
        imagePullPolicy: Never
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop:
            - ALL
          readOnlyRootFilesystem: true
          runAsNonRoot: true
          seccompProfile:
            type: RuntimeDefault
        ports:
        - containerPort: 9418
---
apiVersion: v1
kind: Service
metadata:
  name: git-server
  namespace: %s
spec:
  selector:
    app: git-server
  ports:
  - port: 9418
    targetPort: 9418
`, namespace, namespace)

	h.KubectlApplyContent(manifest)
	h.runCmd("kubectl", "wait", "--namespace", namespace, "--for=condition=available", "deployment/git-server", "--timeout=90s")
}

func (h *Harness) GetGitServerCommit(namespace, repoName string) string {
	gitDir := fmt.Sprintf("/git/%s", repoName)
	out := h.runCmd("kubectl", "exec", "-n", namespace, "deployment/git-server", "--", "git", "--git-dir="+gitDir, "rev-parse", "HEAD")
	return strings.TrimSpace(out)
}

func (h *Harness) DeployFakeGCSServer(namespace, bucket string) {
	h.t.Logf("Deploying fake-gcs-server in namespace %s with bucket %s", namespace, bucket)

	manifest := fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fake-gcs-server
  namespace: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: fake-gcs-server
  template:
    metadata:
      labels:
        app: fake-gcs-server
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 1000
        seccompProfile:
          type: RuntimeDefault
      containers:
      - name: fake-gcs-server
        image: fsouza/fake-gcs-server:latest
        imagePullPolicy: IfNotPresent
        args:
        - -scheme
        - http
        - -port
        - "4443"
        - -backend
        - memory
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop:
            - ALL
          readOnlyRootFilesystem: true
          runAsNonRoot: true
          runAsUser: 1000
          runAsGroup: 1000
          seccompProfile:
            type: RuntimeDefault
        ports:
        - containerPort: 4443
---
apiVersion: v1
kind: Service
metadata:
  name: fake-gcs-server
  namespace: %s
spec:
  selector:
    app: fake-gcs-server
  ports:
  - port: 4443
    targetPort: 4443
`, namespace, namespace)

	h.KubectlApplyContent(manifest)
	h.runCmd("kubectl", "wait", "--namespace", namespace, "--for=condition=available", "deployment/fake-gcs-server", "--timeout=90s")

	postData := fmt.Sprintf(`{"name":"%s"}`, bucket)
	h.runCmd("kubectl", "exec", "-n", namespace, "deployment/fake-gcs-server", "--", "wget", "-q", "-O-", "--post-data="+postData, "--header=Content-Type: application/json", "http://localhost:4443/storage/v1/b?project=test")
}

func (h *Harness) ConfigureGCSSink(bucket string) {
	h.t.Logf("Configuring controller with GCS sink pointing to bucket %s on fake-gcs-server", bucket)
	cmManifest := fmt.Sprintf(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: eval-runner-config
  namespace: eval-runner
data:
  artifactBucket: %s
`, bucket)
	h.KubectlApplyContent(cmManifest)

	h.runCmd("kubectl", "set", "env", "deployment/eval-runner-controller", "-n", "eval-runner", "STORAGE_EMULATOR_HOST=fake-gcs-server.eval-runner.svc:4443")
	h.runCmd("kubectl", "rollout", "status", "-n", "eval-runner", "deployment/eval-runner-controller", "--timeout=90s")
}

func (h *Harness) ConfigureLocalDirSink() {
	h.t.Log("Restoring controller to LocalDir artifact sink")
	cmManifest := `
apiVersion: v1
kind: ConfigMap
metadata:
  name: eval-runner-config
  namespace: eval-runner
data:
  artifactDir: /tmp/artifacts
`
	h.KubectlApplyContent(cmManifest)

	h.runCmd("kubectl", "set", "env", "deployment/eval-runner-controller", "-n", "eval-runner", "STORAGE_EMULATOR_HOST-")
	h.runCmd("kubectl", "rollout", "status", "-n", "eval-runner", "deployment/eval-runner-controller", "--timeout=90s")
}

func (h *Harness) GetGCSObjects(namespace, bucket string) string {
	url := fmt.Sprintf("http://localhost:4443/storage/v1/b/%s/o", bucket)
	return h.runCmd("kubectl", "exec", "-n", namespace, "deployment/fake-gcs-server", "--", "wget", "-q", "-O-", url)
}

func (h *Harness) WaitForSandboxExists(namespace, sandboxName string, timeout time.Duration) {
	h.t.Logf("Waiting for Sandbox %s/%s to be created (timeout %v)", namespace, sandboxName, timeout)
	start := time.Now()
	for {
		cmd := exec.Command("kubectl", "get", "sandbox", sandboxName, "-n", namespace, "--ignore-not-found")
		out, err := cmd.Output()
		if err == nil && strings.TrimSpace(string(out)) != "" {
			return
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for Sandbox %s/%s to be created in namespace %s", namespace, sandboxName, namespace)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (h *Harness) WaitForRunExists(namespace, runName string, timeout time.Duration) *v1alpha1.Run {
	h.t.Logf("Waiting for Run %s/%s to exist (timeout %v)", namespace, runName, timeout)
	start := time.Now()
	var lastErr error
	for {
		cmd := exec.Command("kubectl", "get", "run", runName, "-n", namespace, "-o", "json")
		out, err := cmd.Output()
		if err != nil {
			lastErr = err
		} else {
			var r v1alpha1.Run
			if err := json.Unmarshal(out, &r); err != nil {
				lastErr = err
			} else if r.Name == runName {
				return &r
			}
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for Run %s/%s to exist (last error: %v)", namespace, runName, lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (h *Harness) WaitForComparisonExists(namespace, compName string, timeout time.Duration) *v1alpha1.Comparison {
	h.t.Logf("Waiting for Comparison %s/%s to exist (timeout %v)", namespace, compName, timeout)
	start := time.Now()
	var lastErr error
	for {
		cmd := exec.Command("kubectl", "get", "comparison", compName, "-n", namespace, "-o", "json")
		out, err := cmd.Output()
		if err != nil {
			lastErr = err
		} else {
			var c v1alpha1.Comparison
			if err := json.Unmarshal(out, &c); err != nil {
				lastErr = err
			} else if c.Name == compName {
				return &c
			}
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for Comparison %s/%s to exist (last error: %v)", namespace, compName, lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (h *Harness) WaitForRunPhase(namespace, runName, expectedPhase string, timeout time.Duration) *v1alpha1.Run {
	h.t.Logf("Waiting for Run %s/%s to reach phase %s (timeout %v)", namespace, runName, expectedPhase, timeout)
	start := time.Now()
	var lastRun v1alpha1.Run
	var lastErr error
	for {
		cmd := exec.Command("kubectl", "get", "run", runName, "-n", namespace, "-o", "json")
		out, err := cmd.Output()
		if err != nil {
			lastErr = err
		} else {
			var r v1alpha1.Run
			if err := json.Unmarshal(out, &r); err != nil {
				lastErr = err
			} else {
				lastRun = r
				if string(r.Status.Phase) == expectedPhase {
					return &r
				}
			}
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for Run %s/%s to reach %s; last phase: %s, message: %s, lastErr: %v",
				namespace, runName, expectedPhase, lastRun.Status.Phase, lastRun.Status.Message, lastErr)
		}
		time.Sleep(2 * time.Second)
	}
}

func (h *Harness) WaitForRunDeleted(namespace, runName string, timeout time.Duration) {
	h.t.Logf("Waiting for Run %s/%s to be deleted (timeout %v)", namespace, runName, timeout)
	start := time.Now()
	for {
		cmd := exec.Command("kubectl", "get", "run", runName, "-n", namespace, "--ignore-not-found")
		out, err := cmd.Output()
		if err == nil && strings.TrimSpace(string(out)) == "" {
			return
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for Run %s/%s to be deleted; current: %s", namespace, runName, strings.TrimSpace(string(out)))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (h *Harness) WaitForComparisonDeleted(namespace, compName string, timeout time.Duration) {
	h.t.Logf("Waiting for Comparison %s/%s to be deleted (timeout %v)", namespace, compName, timeout)
	start := time.Now()
	for {
		cmd := exec.Command("kubectl", "get", "comparison", compName, "-n", namespace, "--ignore-not-found")
		out, err := cmd.Output()
		if err == nil && strings.TrimSpace(string(out)) == "" {
			return
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for Comparison %s/%s to be deleted; current: %s", namespace, compName, strings.TrimSpace(string(out)))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (h *Harness) WaitForComparisonVerdict(namespace, compName, expectedVerdict string, timeout time.Duration) *v1alpha1.Comparison {
	h.t.Logf("Waiting for Comparison %s/%s to reach verdict %s (timeout %v)", namespace, compName, expectedVerdict, timeout)
	start := time.Now()
	var lastComp v1alpha1.Comparison
	var lastErr error
	for {
		cmd := exec.Command("kubectl", "get", "comparison", compName, "-n", namespace, "-o", "json")
		out, err := cmd.Output()
		if err != nil {
			lastErr = err
		} else {
			var c v1alpha1.Comparison
			if err := json.Unmarshal(out, &c); err != nil {
				lastErr = err
			} else {
				lastComp = c
				if string(c.Status.Verdict) == expectedVerdict {
					return &c
				}
			}
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for Comparison %s/%s to reach verdict %s; last verdict: %s, phase: %s, message: %s, lastErr: %v",
				namespace, compName, expectedVerdict, lastComp.Status.Verdict, lastComp.Status.Phase, lastComp.Status.Message, lastErr)
		}
		time.Sleep(2 * time.Second)
	}
}

func (h *Harness) WaitForComparisonPhase(namespace, compName, expectedPhase string, timeout time.Duration) *v1alpha1.Comparison {
	h.t.Logf("Waiting for Comparison %s/%s to reach phase %s (timeout %v)", namespace, compName, expectedPhase, timeout)
	start := time.Now()
	var lastComp v1alpha1.Comparison
	var lastErr error
	for {
		cmd := exec.Command("kubectl", "get", "comparison", compName, "-n", namespace, "-o", "json")
		out, err := cmd.Output()
		if err != nil {
			lastErr = err
		} else {
			var c v1alpha1.Comparison
			if err := json.Unmarshal(out, &c); err != nil {
				lastErr = err
			} else {
				lastComp = c
				if string(c.Status.Phase) == expectedPhase {
					return &c
				}
			}
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for Comparison %s/%s to reach phase %s; last verdict: %s, phase: %s, message: %s, lastErr: %v",
				namespace, compName, expectedPhase, lastComp.Status.Verdict, lastComp.Status.Phase, lastComp.Status.Message, lastErr)
		}
		time.Sleep(2 * time.Second)
	}
}

func (h *Harness) WaitForSandboxDeleted(namespace, sandboxName string, timeout time.Duration) {
	h.t.Logf("Waiting for Sandbox %s/%s to be deleted (timeout %v)", namespace, sandboxName, timeout)
	start := time.Now()
	for {
		cmd := exec.Command("kubectl", "get", "sandbox", sandboxName, "-n", namespace, "--ignore-not-found")
		out, err := cmd.Output()
		if err == nil && strings.TrimSpace(string(out)) == "" {
			return
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for Sandbox %s/%s to be deleted; current: %s", namespace, sandboxName, strings.TrimSpace(string(out)))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (h *Harness) WaitForAllSandboxesDeletedForRun(namespace, runName string, timeout time.Duration) {
	h.t.Logf("Waiting for all sandboxes for Run %s in namespace %s to be deleted (timeout %v)", runName, namespace, timeout)
	start := time.Now()
	for {
		cmd := exec.Command("kubectl", "get", "sandboxes", "-n", namespace, "-l", fmt.Sprintf("evalrunner.gke-labs.dev/run-name=%s", runName), "--no-headers")
		out, err := cmd.Output()
		if err == nil && strings.TrimSpace(string(out)) == "" {
			return
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for sandboxes for run %s to be deleted; current: %s", runName, strings.TrimSpace(string(out)))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (h *Harness) CreateNamespace() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		h.t.Fatalf("Failed to generate random namespace suffix: %v", err)
	}
	ns := fmt.Sprintf("test-eval-%x", b)
	h.t.Logf("Creating test namespace %s", ns)
	h.runCmd("kubectl", "create", "namespace", ns)
	h.t.Cleanup(func() {
		if os.Getenv("SKIP_CLEANUP") == "" {
			h.t.Logf("Cleaning up test namespace %s", ns)
			h.runCmd("kubectl", "delete", "namespace", ns, "--ignore-not-found")
		} else {
			h.t.Logf("Skipping cleanup of test namespace %s (SKIP_CLEANUP set)", ns)
		}
	})
	return ns
}

func (h *Harness) GetGitRoot() string {
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	if err != nil {
		h.t.Fatalf("Failed to get git root: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func (h *Harness) DockerBuild(tag, dockerfile, context string) {
	h.t.Logf("Building docker image %s", tag)
	h.runCmd("docker", "build", "-t", tag, "-f", dockerfile, context)
}

func (h *Harness) KindLoad(tag string) {
	h.t.Logf("Loading image %s into kind cluster %s", tag, h.clusterName)
	h.runCmd("kind", "load", "docker-image", tag, "--name", h.clusterName)
}

func (h *Harness) KubectlApplyContent(content string) {
	h.t.Logf("Applying kubectl content:\n%s", content)
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(content)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		h.t.Fatalf("kubectl apply failed: %v\nStderr: %s", err, stderr.String())
	}
}

func (h *Harness) KubectlApplyFile(path string) {
	h.t.Logf("Applying kubectl file: %s", path)
	h.runCmd("kubectl", "apply", "-f", path)
}

func (h *Harness) KubectlApplyFileWithRetry(path string, timeout time.Duration) {
	h.t.Logf("Applying kubectl file (with retry): %s", path)
	start := time.Now()
	var lastErr error
	var lastStderr string
	for {
		cmd := exec.Command("kubectl", "apply", "-f", path)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err == nil {
			return
		} else {
			lastErr = err
			lastStderr = stderr.String()
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for kubectl apply %s to succeed: %v\nStderr: %s", path, lastErr, lastStderr)
		}
		time.Sleep(2 * time.Second)
	}
}

func (h *Harness) runCmd(name string, args ...string) string {
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		h.t.Fatalf("Command %s %v failed: %v\nStdout: %s\nStderr: %s", name, args, err, stdout.String(), stderr.String())
	}
	return stdout.String()
}
