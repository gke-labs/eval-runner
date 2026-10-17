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
	"strings"
	"testing"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestSandboxNameForRun(t *testing.T) {
	tests := []struct {
		runName  string
		expected string
	}{
		{
			runName:  "my-run",
			expected: "discovery-my-run",
		},
		{
			runName:  "support-agent-abc123",
			expected: "discovery-support-agent-abc123",
		},
	}

	for _, tc := range tests {
		got := SandboxNameForRun(tc.runName)
		if got != tc.expected {
			t.Errorf("SandboxNameForRun(%q) = %q; want %q", tc.runName, got, tc.expected)
		}
		if len(got) > 63 {
			t.Errorf("SandboxNameForRun(%q) length %d > 63", tc.runName, len(got))
		}
	}

	// Long run name test
	longRunName := strings.Repeat("long-run-name-test-part", 5) // > 100 chars
	gotLong := SandboxNameForRun(longRunName)
	if len(gotLong) > 63 {
		t.Errorf("SandboxNameForRun for long name exceeded 63 chars: length = %d", len(gotLong))
	}
	if !strings.HasPrefix(gotLong, "discovery-") {
		t.Errorf("SandboxNameForRun for long name missing prefix: %s", gotLong)
	}
}

func TestBuildSandbox(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-123",
			Namespace: "eval-runner",
			UID:       types.UID("1111-2222-3333"),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abc1234",
			},
		},
	}

	sb := BuildSandbox(run, "eval-runner-sandbox", AuthTokenDigest("test"))

	if sb.Name != "discovery-test-run-123" {
		t.Errorf("Unexpected sandbox name: %s", sb.Name)
	}
	if sb.Namespace != "eval-runner-sandbox" {
		t.Errorf("Unexpected sandbox namespace: %s", sb.Namespace)
	}

	// Labels
	if sb.Labels[LabelRunName] != run.Name {
		t.Errorf("Missing/incorrect %s label: %s", LabelRunName, sb.Labels[LabelRunName])
	}
	if sb.Labels[LabelRunNamespace] != run.Namespace {
		t.Errorf("Missing/incorrect %s label: %s", LabelRunNamespace, sb.Labels[LabelRunNamespace])
	}
	if sb.Labels[LabelRunUID] != string(run.UID) {
		t.Errorf("Missing/incorrect %s label: %s", LabelRunUID, sb.Labels[LabelRunUID])
	}
	if sb.Labels[LabelRole] != "discovery" {
		t.Errorf("Missing/incorrect %s label: %s", LabelRole, sb.Labels[LabelRole])
	}
	if sb.Labels[LabelSandbox] != "true" {
		t.Errorf("Missing/incorrect %s label: %s", LabelSandbox, sb.Labels[LabelSandbox])
	}
	if sb.Spec.PodTemplate.ObjectMeta.Labels[LabelSandbox] != "true" {
		t.Errorf("Missing/incorrect pod template %s label: %s", LabelSandbox, sb.Spec.PodTemplate.ObjectMeta.Labels[LabelSandbox])
	}

	// Zero-privilege checks (concept §3.2)
	podSpec := sb.Spec.PodTemplate.Spec
	if podSpec.AutomountServiceAccountToken == nil || *podSpec.AutomountServiceAccountToken != false {
		t.Errorf("AutomountServiceAccountToken must be false, got: %v", podSpec.AutomountServiceAccountToken)
	}

	if podSpec.SecurityContext == nil {
		t.Fatalf("Pod SecurityContext is nil")
	}
	if podSpec.SecurityContext.RunAsNonRoot == nil || !*podSpec.SecurityContext.RunAsNonRoot {
		t.Errorf("RunAsNonRoot must be true")
	}
	if podSpec.SecurityContext.RunAsUser == nil || *podSpec.SecurityContext.RunAsUser == 0 {
		t.Errorf("RunAsUser must be non-zero (non-root)")
	}
	if podSpec.SecurityContext.SeccompProfile == nil || podSpec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("Pod SeccompProfile must be RuntimeDefault, got: %v", podSpec.SecurityContext.SeccompProfile)
	}

	// InitContainers check (topology B injection)
	if len(podSpec.InitContainers) != 1 {
		t.Fatalf("Expected 1 initContainer, got %d", len(podSpec.InitContainers))
	}
	initC := podSpec.InitContainers[0]
	if initC.Name != SandboxdInitContainerName {
		t.Errorf("Expected initContainer name %s, got %s", SandboxdInitContainerName, initC.Name)
	}
	if initC.Image != SandboxdImage {
		t.Errorf("Expected sandboxd image %s, got %s", SandboxdImage, initC.Image)
	}
	if initC.SecurityContext == nil || initC.SecurityContext.RunAsNonRoot == nil || !*initC.SecurityContext.RunAsNonRoot {
		t.Errorf("InitContainer must run as non-root")
	}

	// App container check
	if len(podSpec.Containers) != 1 {
		t.Fatalf("Expected 1 container, got %d", len(podSpec.Containers))
	}
	c := podSpec.Containers[0]
	if c.Image != DiscoveryImage {
		t.Errorf("Expected pinned image %s, got %s", DiscoveryImage, c.Image)
	}

	if c.SecurityContext == nil {
		t.Fatalf("Container SecurityContext is nil")
	}
	if c.SecurityContext.AllowPrivilegeEscalation == nil || *c.SecurityContext.AllowPrivilegeEscalation {
		t.Errorf("AllowPrivilegeEscalation must be false")
	}
	if c.SecurityContext.RunAsNonRoot == nil || !*c.SecurityContext.RunAsNonRoot {
		t.Errorf("Container RunAsNonRoot must be true")
	}
	if c.SecurityContext.Capabilities == nil || len(c.SecurityContext.Capabilities.Drop) != 1 || c.SecurityContext.Capabilities.Drop[0] != "ALL" {
		t.Errorf("Expected Drop ALL capabilities, got: %v", c.SecurityContext.Capabilities)
	}
	if c.SecurityContext.SeccompProfile == nil || c.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("Container SeccompProfile must be RuntimeDefault, got: %v", c.SecurityContext.SeccompProfile)
	}

	// Command should run sandboxd
	if len(c.Command) == 0 || !strings.Contains(c.Command[0], "sandboxd") {
		t.Errorf("Expected container command to run sandboxd, got: %v", c.Command)
	}

	// Ports: 8080 (REST), 9090 (gRPC), and 8081 (health)
	if len(c.Ports) != 3 {
		t.Fatalf("Expected 3 container ports, got %d", len(c.Ports))
	}
	var has8080, has9090, has8081 bool
	for _, p := range c.Ports {
		if p.ContainerPort == 8080 {
			has8080 = true
		}
		if p.ContainerPort == 9090 {
			has9090 = true
		}
		if p.ContainerPort == 8081 {
			has8081 = true
		}
	}
	if !has8080 || !has9090 || !has8081 {
		t.Errorf("Expected ports 8080, 9090, and 8081, got: %v", c.Ports)
	}

	// Readiness probe
	if c.ReadinessProbe == nil || c.ReadinessProbe.HTTPGet == nil {
		t.Fatalf("Expected HTTPGet readiness probe")
	}
	if c.ReadinessProbe.HTTPGet.Path != "/v1/health" || c.ReadinessProbe.HTTPGet.Port.IntValue() != 8081 {
		t.Errorf("Expected readiness probe GET /v1/health on 8081, got: %+v", c.ReadinessProbe.HTTPGet)
	}

	// Resources checks
	if c.Resources.Requests.Cpu().IsZero() || c.Resources.Requests.Memory().IsZero() {
		t.Errorf("Resource requests must be set, got: %+v", c.Resources.Requests)
	}
	if c.Resources.Limits.Cpu().IsZero() || c.Resources.Limits.Memory().IsZero() {
		t.Errorf("Resource limits must be set, got: %+v", c.Resources.Limits)
	}

	// Volumes
	if len(podSpec.Volumes) != 4 {
		t.Fatalf("Expected 4 volumes, got %d", len(podSpec.Volumes))
	}

	// Negative assertion: discovery sandbox container has no EnvFrom
	if len(c.EnvFrom) != 0 {
		t.Errorf("Discovery container must not have EnvFrom, got: %+v", c.EnvFrom)
	}
}

func TestBuildSandbox_WithAuthToken(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-auth",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "git://git-server/repo.git",
			},
		},
	}

	token := "test-secret-token-12345"
	digest := AuthTokenDigest(token)
	if digest == "" || len(digest) != 64 {
		t.Fatalf("Expected 64-char hex digest, got %q", digest)
	}

	sb := BuildSandbox(run, "eval-runner-sandbox", digest)
	containers := sb.Spec.PodTemplate.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("Expected 1 container, got %d", len(containers))
	}

	cmd := containers[0].Command
	expectedFlag := "--auth-token-sha256=" + digest
	var found bool
	for _, arg := range cmd {
		if arg == expectedFlag {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected command to contain %q, got: %v", expectedFlag, cmd)
	}
}

func TestAuthSecretNameForRun(t *testing.T) {
	name := AuthSecretNameForRun("short-name")
	if name != "discovery-auth-short-name" {
		t.Errorf("Unexpected secret name: %s", name)
	}

	longName := strings.Repeat("a", 100)
	gotLong := AuthSecretNameForRun(longName)
	if len(gotLong) > 63 {
		t.Errorf("Secret name exceeds 63 characters: %d (%s)", len(gotLong), gotLong)
	}
	if !strings.HasPrefix(gotLong, "discovery-auth-") {
		t.Errorf("Secret name missing prefix: %s", gotLong)
	}
}

func TestTaskAuthSecretNameForRun(t *testing.T) {
	name := TaskAuthSecretNameForRun("short-run", "docs/readme_comprehension")
	if len(name) > 63 {
		t.Errorf("Task auth secret name exceeds 63 characters: %d (%s)", len(name), name)
	}
	if !strings.HasPrefix(name, "task-auth-") {
		t.Errorf("Task auth secret name missing prefix: %s", name)
	}
}

func TestBuildSandbox_EmptyAuthTokenPanics(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-empty-token",
			Namespace: "eval-runner",
		},
	}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected BuildSandbox to panic on empty authTokenDigest")
		}
	}()
	BuildSandbox(run, "eval-runner-sandbox", "")
}

func TestTaskSandboxNameForRun(t *testing.T) {
	name1 := TaskSandboxNameForRun("my-run", "docs/readme_comprehension")
	if len(name1) > 63 {
		t.Errorf("TaskSandboxNameForRun exceeded 63 chars: %d", len(name1))
	}
	if !strings.HasPrefix(name1, "task-my-run-") {
		t.Errorf("Expected prefix 'task-my-run-', got %s", name1)
	}

	// Long run name and task ID
	longRun := strings.Repeat("a", 50)
	longTask := strings.Repeat("b", 50)
	name2 := TaskSandboxNameForRun(longRun, longTask)
	if len(name2) > 63 {
		t.Errorf("TaskSandboxNameForRun for long names exceeded 63 chars: %d", len(name2))
	}
}

func TestBuildTaskSandbox_WithAndWithoutModelSecret(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
			UID:       types.UID("run-uid-123"),
		},
	}

	token := "sample-task-auth-token-12345"
	digest := AuthTokenDigest(token)

	// Case 1: Without model secret
	sbNoSecret := BuildTaskSandbox(run, "docs/readme_comprehension", TaskSandboxConfig{
		Namespace:         "eval-runner-sandbox",
		SampleRunnerImage: "sample-runner:e2e",
		ModelSecret:       "",
		AuthTokenDigest:   digest,
	})

	if sbNoSecret.Labels[LabelRole] != RoleSample {
		t.Errorf("Expected role %s, got %s", RoleSample, sbNoSecret.Labels[LabelRole])
	}
	if sbNoSecret.Labels[LabelSandbox] != "true" {
		t.Errorf("Expected sandbox label 'true', got %s", sbNoSecret.Labels[LabelSandbox])
	}
	if sbNoSecret.Annotations[AnnotationTaskID] != "docs/readme_comprehension" {
		t.Errorf("Expected task ID annotation, got %s", sbNoSecret.Annotations[AnnotationTaskID])
	}

	cNoSecret := sbNoSecret.Spec.PodTemplate.Spec.Containers[0]
	if cNoSecret.Image != "sample-runner:e2e" {
		t.Errorf("Expected image sample-runner:e2e, got %s", cNoSecret.Image)
	}
	if len(cNoSecret.EnvFrom) != 0 {
		t.Errorf("Expected no EnvFrom when modelSecret is empty, got: %v", cNoSecret.EnvFrom)
	}

	expectedFlag := "--auth-token-sha256=" + digest
	var foundFlag bool
	for _, arg := range cNoSecret.Command {
		if arg == expectedFlag {
			foundFlag = true
			break
		}
	}
	if !foundFlag {
		t.Errorf("Expected command to contain %q, got: %v", expectedFlag, cNoSecret.Command)
	}

	// Case 2: With model secret
	sbWithSecret := BuildTaskSandbox(run, "docs/readme_comprehension", TaskSandboxConfig{
		Namespace:         "eval-runner-sandbox",
		SampleRunnerImage: "sample-runner:e2e",
		ModelSecret:       "gemini-credentials",
		AuthTokenDigest:   digest,
	})

	cWithSecret := sbWithSecret.Spec.PodTemplate.Spec.Containers[0]
	if len(cWithSecret.EnvFrom) != 1 {
		t.Fatalf("Expected 1 EnvFrom source, got %d", len(cWithSecret.EnvFrom))
	}
	if cWithSecret.EnvFrom[0].SecretRef == nil || cWithSecret.EnvFrom[0].SecretRef.Name != "gemini-credentials" {
		t.Errorf("Expected SecretRef 'gemini-credentials', got: %+v", cWithSecret.EnvFrom[0].SecretRef)
	}
}

func TestBuildTaskSandbox_EmptyAuthTokenPanics(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
	}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected BuildTaskSandbox to panic on empty authTokenDigest")
		}
	}()
	BuildTaskSandbox(run, "docs/readme_comprehension", TaskSandboxConfig{
		AuthTokenDigest: "",
	})
}

func TestBuildTaskSandbox_NoGCSCredentialsOrVolumes(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "abcdef123456",
			},
			Model: "mockllm/model",
		},
	}

	sb := BuildTaskSandbox(run, "docs/readme_comprehension", TaskSandboxConfig{
		Namespace:         "eval-runner-sandbox",
		SampleRunnerImage: "sample-runner:latest",
		ModelSecret:       "model-secret",
		AuthTokenDigest:   AuthTokenDigest("dummy-token"),
	})

	podSpec := sb.Spec.PodTemplate.Spec

	// 1. Verify AutomountServiceAccountToken is explicitly false
	if podSpec.AutomountServiceAccountToken == nil || *podSpec.AutomountServiceAccountToken {
		t.Errorf("Expected AutomountServiceAccountToken to be false")
	}

	// 2. Verify ServiceAccountName is empty
	if podSpec.ServiceAccountName != "" {
		t.Errorf("Expected ServiceAccountName to be empty, got %q", podSpec.ServiceAccountName)
	}

	// 3. Verify no GCS or cloud credential volumes exist
	allowedVolumes := map[string]bool{
		"sandboxd-bin": true,
		"workspace":    true,
		"sandbox-tls":  true,
		"sandbox-ca":   true,
	}
	for _, v := range podSpec.Volumes {
		if !allowedVolumes[v.Name] {
			t.Errorf("Unexpected volume %q in sandbox pod spec; only sandboxd-bin, workspace, sandbox-tls, and sandbox-ca should exist", v.Name)
		}
		if v.Projected != nil {
			t.Errorf("Volume %q projects projected credentials into untrusted sandbox", v.Name)
		}
	}

	// 4. Verify no GCS credential env vars exist in any container
	allContainers := append(podSpec.InitContainers, podSpec.Containers...)
	for _, c := range allContainers {
		for _, env := range c.Env {
			name := strings.ToUpper(env.Name)
			if strings.Contains(name, "GOOGLE") || strings.Contains(name, "GCS") || strings.Contains(name, "BUCKET") || strings.Contains(name, "CREDENTIAL") {
				t.Errorf("Sandbox container %q contains prohibited cloud credential env var %q", c.Name, env.Name)
			}
		}
		for _, vm := range c.VolumeMounts {
			if !allowedVolumes[vm.Name] {
				t.Errorf("Sandbox container %q has unexpected volume mount %q", c.Name, vm.Name)
			}
		}
	}
}
