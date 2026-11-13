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
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
)

const (
	AgentSandboxVersion = "v1.0.7"
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
	if err := os.Setenv("DOCKER_BUILDKIT", "1"); err != nil {
		h.t.Fatalf("Failed to set DOCKER_BUILDKIT: %v", err)
	}

	if _, err := exec.LookPath("kind"); err != nil {
		h.t.Fatalf("kind not found on PATH: %v", err)
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		h.t.Fatalf("kubectl not found on PATH: %v", err)
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
}

func (h *Harness) DeployEvalRunner() {
	h.t.Log("Installing eval-runner through dev/tasks/deploy-to-kube")
	gitRoot := h.GetGitRoot()

	deployScript := filepath.Join(gitRoot, "dev/tasks/deploy-to-kube")
	h.runCmd(deployScript,
		"--set", "artifactDir=/tmp/artifacts",
		"--set", "defaultModel=mockllm/model",
		"--set", "defaultBatchSize=8",
		"--set", "defaultMaxSamplesPerTask=64",
		"--set", "commandUsers=reviewer",
		"--set", "webhookRepositories=gke-labs/eval-runner",
		"--set", "pollProgressInterval=2s",
		"--set", "allowedCloneHosts=github.com,git-server.eval-runner-sandbox.svc",
		"--set", "sandboxPortalPolicy=*",
	)

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

	// Assert controller startup logs report stamped 40-hex VCS revision rather than 'devel'
	controllerLogs := h.runCmd("kubectl", "logs", "--namespace", "eval-runner", "deployment/eval-runner-controller")
	reVersion := regexp.MustCompile(`"version": "[0-9a-f]{40}(-dirty)?"`)
	if !reVersion.MatchString(controllerLogs) {
		h.t.Fatalf("expected controller logs to report stamped 40-hex VCS revision, got:\n%s", controllerLogs)
	}

	// Verify controller-owned sandbox egress NetworkPolicy reconciliation
	out := h.runCmd("kubectl", "get", "networkpolicy", "eval-runner-sandbox-egress", "-n", "eval-runner-sandbox", "-o", "json")
	var egressPolicy networkingv1.NetworkPolicy
	if err := json.Unmarshal([]byte(out), &egressPolicy); err != nil {
		h.t.Fatalf("failed to decode eval-runner-sandbox-egress into NetworkPolicy: %v", err)
	}
	if len(egressPolicy.Spec.Egress) != 3 {
		h.t.Fatalf("expected 3 egress rules in eval-runner-sandbox-egress, got %d", len(egressPolicy.Spec.Egress))
	}
	var internetRule *networkingv1.NetworkPolicyEgressRule
	for i := range egressPolicy.Spec.Egress {
		r := &egressPolicy.Spec.Egress[i]
		for _, to := range r.To {
			if to.IPBlock != nil && to.IPBlock.CIDR == "0.0.0.0/0" {
				internetRule = r
				break
			}
		}
	}
	if internetRule == nil {
		h.t.Fatalf("eval-runner-sandbox-egress missing 0.0.0.0/0 internet rule")
	}
	expectedPorts := map[int32]bool{80: false, 443: false, 9418: false}
	for _, port := range internetRule.Ports {
		if port.Protocol != nil && *port.Protocol == corev1.ProtocolTCP && port.Port != nil {
			expectedPorts[port.Port.IntVal] = true
		}
	}
	for p, found := range expectedPorts {
		if !found {
			h.t.Fatalf("0.0.0.0/0 internet rule missing expected TCP port %d", p)
		}
	}
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
      initContainers:
      - name: init-git
        image: git-server:e2e
        imagePullPolicy: Never
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop:
            - ALL
          readOnlyRootFilesystem: true
          runAsNonRoot: true
          runAsUser: 65534
          runAsGroup: 65534
          seccompProfile:
            type: RuntimeDefault
        command:
        - sh
        - -c
        - "cp -a /git-baked/. /git/"
        volumeMounts:
        - name: git-data
          mountPath: /git
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
          runAsUser: 65534
          runAsGroup: 65534
          seccompProfile:
            type: RuntimeDefault
        ports:
        - containerPort: 9418
        volumeMounts:
        - name: git-data
          mountPath: /git
      volumes:
      - name: git-data
        emptyDir: {}
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
        - -public-host
        - fake-gcs-server.eval-runner.svc:4443
        - -external-url
        - http://fake-gcs-server.eval-runner.svc:4443
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
	patch := fmt.Sprintf(`{"data":{"artifactBucket":%q,"artifactDir":null}}`, bucket)
	h.runCmd("kubectl", "patch", "configmap", "eval-runner-config", "-n", "eval-runner", "--type=merge", "-p", patch)

	h.runCmd("kubectl", "set", "env", "deployment/eval-runner-controller", "-n", "eval-runner", "STORAGE_EMULATOR_HOST=fake-gcs-server.eval-runner.svc:4443")
	h.runCmd("kubectl", "rollout", "status", "-n", "eval-runner", "deployment/eval-runner-controller", "--timeout=90s")

	h.runCmd("kubectl", "set", "env", "deployment/eval-runner-ui", "-n", "eval-runner", "STORAGE_EMULATOR_HOST=fake-gcs-server.eval-runner.svc:4443")
	h.runCmd("kubectl", "rollout", "status", "-n", "eval-runner", "deployment/eval-runner-ui", "--timeout=90s")
}

func (h *Harness) ConfigureLocalDirSink() {
	h.t.Log("Restoring controller to LocalDir artifact sink")
	patch := `{"data":{"artifactDir":"/tmp/artifacts","artifactBucket":null}}`
	h.runCmd("kubectl", "patch", "configmap", "eval-runner-config", "-n", "eval-runner", "--type=merge", "-p", patch)

	h.runCmd("kubectl", "set", "env", "deployment/eval-runner-controller", "-n", "eval-runner", "STORAGE_EMULATOR_HOST-")
	h.runCmd("kubectl", "rollout", "status", "-n", "eval-runner", "deployment/eval-runner-controller", "--timeout=90s")

	h.runCmd("kubectl", "set", "env", "deployment/eval-runner-ui", "-n", "eval-runner", "STORAGE_EMULATOR_HOST-")
	h.runCmd("kubectl", "rollout", "status", "-n", "eval-runner", "deployment/eval-runner-ui", "--timeout=90s")
}

func (h *Harness) GetGCSObjects(namespace, bucket string) string {
	url := fmt.Sprintf("http://localhost:4443/storage/v1/b/%s/o", bucket)
	return h.runCmd("kubectl", "exec", "-n", namespace, "deployment/fake-gcs-server", "--", "wget", "-q", "-O-", url)
}

func (h *Harness) GetGCSObject(namespace, bucket, objectPath string) []byte {
	url := fmt.Sprintf("http://localhost:4443/storage/v1/b/%s/o/%s?alt=media", bucket, url.PathEscape(objectPath))
	out := h.runCmd("kubectl", "exec", "-n", namespace, "deployment/fake-gcs-server", "--", "wget", "-q", "-O-", url)
	return []byte(out)
}

func (h *Harness) PortForwardUI(port int) (baseURL string, cleanup func()) {
	cmd := exec.Command("kubectl", "port-forward", "-n", "eval-runner", "svc/eval-runner-ui", fmt.Sprintf("%d:80", port))
	if err := cmd.Start(); err != nil {
		h.t.Fatalf("failed to start eval-runner-ui port-forward: %v", err)
	}
	cleanup = func() {
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				h.t.Logf("warning: failed to kill port-forward process: %v", err)
			}
			if err := cmd.Wait(); err != nil {
				// The process was killed, so an exit error is expected.
				h.t.Logf("port-forward process terminated: %v", err)
			}
		}
	}

	baseURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	ready := false
	for i := 0; i < 30; i++ {
		resp, err := http.Get(baseURL + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			ready = true
			if err := resp.Body.Close(); err != nil {
				h.t.Logf("warning: failed to close response body: %v", err)
			}
			break
		}
		if resp != nil {
			if err := resp.Body.Close(); err != nil {
				h.t.Logf("warning: failed to close response body: %v", err)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !ready {
		cleanup()
		h.t.Fatalf("eval-runner-ui port-forward failed to become ready on port %d", port)
	}
	return baseURL, cleanup
}

func (h *Harness) PortForwardWebhook(port int) (baseURL string, cleanup func()) {
	cmd := exec.Command("kubectl", "port-forward", "-n", "eval-runner", "svc/eval-runner-webhook", fmt.Sprintf("%d:80", port))
	if err := cmd.Start(); err != nil {
		h.t.Fatalf("failed to start eval-runner-webhook port-forward: %v", err)
	}
	cleanup = func() {
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				h.t.Logf("warning: failed to kill port-forward process: %v", err)
			}
			if err := cmd.Wait(); err != nil {
				h.t.Logf("port-forward process terminated: %v", err)
			}
		}
	}

	baseURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	ready := false
	for i := 0; i < 30; i++ {
		resp, err := http.Get(baseURL + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			ready = true
			if err := resp.Body.Close(); err != nil {
				h.t.Logf("warning: failed to close response body: %v", err)
			}
			break
		}
		if resp != nil {
			if err := resp.Body.Close(); err != nil {
				h.t.Logf("warning: failed to close response body: %v", err)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !ready {
		cleanup()
		h.t.Fatalf("eval-runner-webhook port-forward failed to become ready on port %d", port)
	}
	return baseURL, cleanup
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
			h.logTimeoutDiagnostics(namespace, runName)
			h.t.Fatalf("Timeout waiting for Run %s/%s to reach %s; last phase: %s, message: %s, lastErr: %v",
				namespace, runName, expectedPhase, lastRun.Status.Phase, lastRun.Status.Message, lastErr)
		}
		time.Sleep(2 * time.Second)
	}
}

func (h *Harness) logTimeoutDiagnostics(namespace, runName string) {
	h.t.Logf("=== TIMEOUT DIAGNOSTICS FOR RUN %s/%s ===", namespace, runName)

	// 1. Run as YAML
	if out, err := exec.Command("kubectl", "get", "run", runName, "-n", namespace, "-o", "yaml").CombinedOutput(); err == nil {
		h.t.Logf("Run %s/%s YAML:\n%s", namespace, runName, string(out))
	} else {
		h.t.Logf("Failed to get Run %s/%s YAML: %v (output: %s)", namespace, runName, err, string(out))
	}

	// 2. kubectl get pods -n eval-runner-sandbox -o wide
	if out, err := exec.Command("kubectl", "get", "pods", "-n", "eval-runner-sandbox", "-o", "wide").CombinedOutput(); err == nil {
		h.t.Logf("Pods in eval-runner-sandbox (wide):\n%s", string(out))
	} else {
		h.t.Logf("Failed to list pods in eval-runner-sandbox: %v (output: %s)", err, string(out))
	}

	// 3 & 4. For each sandbox pod of the Run: container statuses and last ~100 lines of logs
	out, err := exec.Command("kubectl", "get", "pods", "-n", "eval-runner-sandbox", "-o", "json").CombinedOutput()
	if err != nil {
		h.t.Logf("Failed to get pods in eval-runner-sandbox: %v (output: %s)", err, string(out))
	} else {
		var podList corev1.PodList
		if err := json.Unmarshal(out, &podList); err != nil {
			h.t.Logf("Failed to unmarshal pods in eval-runner-sandbox: %v", err)
		} else {
			for _, pod := range podList.Items {
				if pod.Labels["evalrunner.gke-labs.dev/run-name"] != runName {
					continue
				}
				h.t.Logf("--- Sandbox Pod: %s (Phase: %s) ---", pod.Name, pod.Status.Phase)
				logContainerStatus := func(containerType string, statuses []corev1.ContainerStatus) {
					for _, cs := range statuses {
						var stateStr, lastStateStr, reason string
						var exitCode int32
						var oomKilled bool

						if cs.State.Waiting != nil {
							stateStr = fmt.Sprintf("Waiting(reason=%s, message=%s)", cs.State.Waiting.Reason, cs.State.Waiting.Message)
							reason = cs.State.Waiting.Reason
						} else if cs.State.Running != nil {
							stateStr = fmt.Sprintf("Running(startedAt=%s)", cs.State.Running.StartedAt)
						} else if cs.State.Terminated != nil {
							stateStr = fmt.Sprintf("Terminated(exitCode=%d, reason=%s, message=%s)", cs.State.Terminated.ExitCode, cs.State.Terminated.Reason, cs.State.Terminated.Message)
							reason = cs.State.Terminated.Reason
							exitCode = cs.State.Terminated.ExitCode
							oomKilled = (cs.State.Terminated.Reason == "OOMKilled" || cs.State.Terminated.ExitCode == 137)
						}

						if cs.LastTerminationState.Waiting != nil {
							lastStateStr = fmt.Sprintf("Waiting(reason=%s, message=%s)", cs.LastTerminationState.Waiting.Reason, cs.LastTerminationState.Waiting.Message)
						} else if cs.LastTerminationState.Running != nil {
							lastStateStr = fmt.Sprintf("Running(startedAt=%s)", cs.LastTerminationState.Running.StartedAt)
						} else if cs.LastTerminationState.Terminated != nil {
							lastStateStr = fmt.Sprintf("Terminated(exitCode=%d, reason=%s, message=%s)", cs.LastTerminationState.Terminated.ExitCode, cs.LastTerminationState.Terminated.Reason, cs.LastTerminationState.Terminated.Message)
							if !oomKilled {
								oomKilled = (cs.LastTerminationState.Terminated.Reason == "OOMKilled" || cs.LastTerminationState.Terminated.ExitCode == 137)
							}
						}

						h.t.Logf("Pod %s container [%s] %s: state=%s, lastState=%s, reason=%s, exitCode=%d, OOMKilled=%v",
							pod.Name, containerType, cs.Name, stateStr, lastStateStr, reason, exitCode, oomKilled)

						// 4. Last ~100 lines of each container's logs
						if logOut, err := exec.Command("kubectl", "logs", pod.Name, "-n", "eval-runner-sandbox", "-c", cs.Name, "--tail=100").CombinedOutput(); err == nil {
							h.t.Logf("Logs for %s/%s (container %s, tail 100):\n%s", pod.Name, cs.Name, cs.Name, string(logOut))
						} else {
							h.t.Logf("Failed to get logs for %s container %s: %v", pod.Name, cs.Name, err)
						}
					}
				}

				logContainerStatus("init", pod.Status.InitContainerStatuses)
				logContainerStatus("main", pod.Status.ContainerStatuses)
			}
		}
	}

	// 5. Last ~100 lines of the controller log
	if ctrlLogs, err := exec.Command("kubectl", "logs", "-n", namespace, "-l", "app=eval-runner-controller", "--tail=100").CombinedOutput(); err == nil {
		h.t.Logf("Controller logs (tail 100):\n%s", string(ctrlLogs))
	} else {
		h.t.Logf("Failed to get controller logs: %v (output: %s)", err, string(ctrlLogs))
	}

	h.t.Logf("=== END TIMEOUT DIAGNOSTICS ===")
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

// WaitForRunsWithLabelDeleted waits until no Runs matching the label selector remain in the namespace.
func (h *Harness) WaitForRunsWithLabelDeleted(namespace, selector string, timeout time.Duration) {
	h.t.Logf("Waiting for Runs in %s matching %q to be deleted (timeout %v)", namespace, selector, timeout)
	start := time.Now()
	for {
		cmd := exec.Command("kubectl", "get", "run", "-n", namespace, "-l", selector, "--no-headers", "--ignore-not-found")
		out, err := cmd.Output()
		if err == nil && strings.TrimSpace(string(out)) == "" {
			return
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for Runs in %s matching %q to be deleted; current: %s", namespace, selector, strings.TrimSpace(string(out)))
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

func (h *Harness) WaitForComparisonLabel(namespace, compName, key, expectedValue string, timeout time.Duration) *v1alpha1.Comparison {
	h.t.Logf("Waiting for Comparison %s/%s to have label %s=%s (timeout %v)", namespace, compName, key, expectedValue, timeout)
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
				if c.Labels != nil && c.Labels[key] == expectedValue {
					return &c
				}
			}
		}

		if time.Since(start) > timeout {
			h.t.Fatalf("Timeout waiting for Comparison %s/%s to have label %s=%s; current labels: %v, lastErr: %v",
				namespace, compName, key, expectedValue, lastComp.Labels, lastErr)
		}
		time.Sleep(200 * time.Millisecond)
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
