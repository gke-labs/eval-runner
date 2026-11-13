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
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

	// Case 3: With sandbox portal policy (suppresses model Secret, adds label and annotations, sets environment variables)
	runWithRepo := run.DeepCopy()
	runWithRepo.Spec.Repo.URL = "https://github.com/example/repo.git"
	sbWithPortal := BuildTaskSandbox(runWithRepo, "docs/readme_comprehension", TaskSandboxConfig{
		Namespace:           "eval-runner-sandbox",
		SampleRunnerImage:   "sample-runner:e2e",
		ModelSecret:         "gemini-credentials",
		SandboxPortalPolicy: "*",
		CloneRepo:           true,
		AuthTokenDigest:     digest,
	})

	if sbWithPortal.Labels[LabelServicePortalIntercept] != "true" {
		t.Errorf("Expected intercept label 'true', got %s", sbWithPortal.Labels[LabelServicePortalIntercept])
	}
	if sbWithPortal.Annotations[AnnotationServicePortalPolicy] != "*" {
		t.Errorf("Expected policy annotation '*', got %s", sbWithPortal.Annotations[AnnotationServicePortalPolicy])
	}
	expectedIdentity := "test-run/docs/readme_comprehension"
	if sbWithPortal.Annotations[AnnotationServicePortalIdentity] != expectedIdentity {
		t.Errorf("Expected identity annotation %q, got %s", expectedIdentity, sbWithPortal.Annotations[AnnotationServicePortalIdentity])
	}

	cWithPortal := sbWithPortal.Spec.PodTemplate.Spec.Containers[0]
	if len(cWithPortal.EnvFrom) != 0 {
		t.Fatalf("Expected 0 EnvFrom sources when SandboxPortalPolicy is set, got %d", len(cWithPortal.EnvFrom))
	}
	actualEnv := make(map[string]string, len(cWithPortal.Env))
	for _, envVar := range cWithPortal.Env {
		actualEnv[envVar.Name] = envVar.Value
	}
	for envName, expectedValue := range map[string]string{
		EnvGeminiAPIKey:     "placeholder",
		EnvGoogleAPIKey:     "placeholder",
		EnvRequestsCABundle: ServicePortalCACertPath,
		EnvCurlCABundle:     ServicePortalCACertPath,
		EnvGitSSLCAInfo:     ServicePortalCACertPath,
		EnvPipCert:          ServicePortalCACertPath,
		EnvNodeExtraCACerts: ServicePortalCACertPath,
	} {
		if actualValue, ok := actualEnv[envName]; !ok || actualValue != expectedValue {
			t.Errorf("Expected env %s=%q, got %q (present=%v)", envName, expectedValue, actualValue, ok)
		}
	}

	initCheckout := sbWithPortal.Spec.PodTemplate.Spec.InitContainers[1]
	actualInitEnv := make(map[string]string, len(initCheckout.Env))
	for _, envVar := range initCheckout.Env {
		actualInitEnv[envVar.Name] = envVar.Value
	}
	for _, caEnv := range ServicePortalCAEnvVars() {
		if actualInitEnv[caEnv.Name] != caEnv.Value {
			t.Errorf("Expected init container env %s=%q, got %q", caEnv.Name, caEnv.Value, actualInitEnv[caEnv.Name])
		}
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

func TestBuildTaskSandbox_ImageSelection(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
	}
	digest := AuthTokenDigest("dummy-token")

	// Case 1: Task image specified
	sandboxCustom := BuildTaskSandbox(run, "task-custom", TaskSandboxConfig{
		Namespace:         "eval-runner-sandbox",
		SampleRunnerImage: "sample-runner:default",
		Image:             "my-task-image:v1",
		AuthTokenDigest:   digest,
	})
	if image := sandboxCustom.Spec.PodTemplate.Spec.Containers[0].Image; image != "my-task-image:v1" {
		t.Errorf("Expected task image 'my-task-image:v1', got %q", image)
	}
	if len(sandboxCustom.Spec.PodTemplate.Spec.InitContainers) != 1 {
		t.Fatalf("Expected 1 init container, got %d", len(sandboxCustom.Spec.PodTemplate.Spec.InitContainers))
	}
	if initContainerName := sandboxCustom.Spec.PodTemplate.Spec.InitContainers[0].Name; initContainerName != SandboxdInitContainerName {
		t.Errorf("Expected init container name %q, got %q", SandboxdInitContainerName, initContainerName)
	}

	// Case 2: Task image empty, uses SampleRunnerImage
	sandboxDefaultSample := BuildTaskSandbox(run, "task-default", TaskSandboxConfig{
		Namespace:         "eval-runner-sandbox",
		SampleRunnerImage: "sample-runner:custom",
		AuthTokenDigest:   digest,
	})
	if image := sandboxDefaultSample.Spec.PodTemplate.Spec.Containers[0].Image; image != "sample-runner:custom" {
		t.Errorf("Expected sample runner image 'sample-runner:custom', got %q", image)
	}
	if initContainerName := sandboxDefaultSample.Spec.PodTemplate.Spec.InitContainers[0].Name; initContainerName != SandboxdInitContainerName {
		t.Errorf("Expected init container name %q, got %q", SandboxdInitContainerName, initContainerName)
	}

	// Case 3: Both empty, falls back to DefaultSampleRunnerImage
	sandboxBuiltinDefault := BuildTaskSandbox(run, "task-fallback", TaskSandboxConfig{
		Namespace:       "eval-runner-sandbox",
		AuthTokenDigest: digest,
	})
	if image := sandboxBuiltinDefault.Spec.PodTemplate.Spec.Containers[0].Image; image != DefaultSampleRunnerImage {
		t.Errorf("Expected fallback image %q, got %q", DefaultSampleRunnerImage, image)
	}
}

func TestBuildTaskSandbox_RepoCheckoutInitContainer(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abcdef123456",
			},
		},
	}
	digest := AuthTokenDigest("dummy-token")

	// Case 1: CloneRepo is true, init container is present
	sandboxWithCheckout := BuildTaskSandbox(run, "task-sandbox", TaskSandboxConfig{
		Namespace:          "eval-runner-sandbox",
		Image:              "custom-task-image:v1",
		AuthTokenDigest:    digest,
		CloneRepo:          true,
		StepTimeoutSeconds: 60,
	})

	if len(sandboxWithCheckout.Spec.PodTemplate.Spec.InitContainers) != 2 {
		t.Fatalf("Expected 2 init containers, got %d", len(sandboxWithCheckout.Spec.PodTemplate.Spec.InitContainers))
	}

	sandboxdInitContainer := sandboxWithCheckout.Spec.PodTemplate.Spec.InitContainers[0]
	if sandboxdInitContainer.Name != SandboxdInitContainerName {
		t.Errorf("Expected first init container name %q, got %q", SandboxdInitContainerName, sandboxdInitContainer.Name)
	}
	if sandboxdInitContainer.Image != SandboxdImage {
		t.Errorf("Expected first init container image %q, got %q", SandboxdImage, sandboxdInitContainer.Image)
	}
	if len(sandboxdInitContainer.VolumeMounts) != 1 || sandboxdInitContainer.VolumeMounts[0].Name != "sandboxd-bin" {
		t.Errorf("Expected sandboxd-bin volume mount on install-sandboxd, got %+v", sandboxdInitContainer.VolumeMounts)
	}

	checkoutInitContainer := sandboxWithCheckout.Spec.PodTemplate.Spec.InitContainers[1]
	if checkoutInitContainer.Name != RepoCheckoutInitContainerName {
		t.Errorf("Expected second init container name %q, got %q", RepoCheckoutInitContainerName, checkoutInitContainer.Name)
	}
	if checkoutInitContainer.Image != GitCheckoutImage {
		t.Errorf("Expected second init container image %q, got %q", GitCheckoutImage, checkoutInitContainer.Image)
	}

	// Assert mount: workspace volume mounted at /workspace
	foundWorkspaceMount := false
	for _, volumeMount := range checkoutInitContainer.VolumeMounts {
		if volumeMount.Name == "workspace" && volumeMount.MountPath == "/workspace" {
			foundWorkspaceMount = true
			break
		}
	}
	if !foundWorkspaceMount {
		t.Errorf("Expected workspace volume mounted at /workspace, got %+v", checkoutInitContainer.VolumeMounts)
	}

	// Assert security context is unchanged and matches install-sandboxd / pod security context
	securityContext := checkoutInitContainer.SecurityContext
	if securityContext == nil {
		t.Fatalf("Expected security context on checkout init container")
	}
	if securityContext.AllowPrivilegeEscalation == nil || *securityContext.AllowPrivilegeEscalation {
		t.Errorf("Expected allowPrivilegeEscalation false")
	}
	if securityContext.RunAsNonRoot == nil || !*securityContext.RunAsNonRoot {
		t.Errorf("Expected runAsNonRoot true")
	}
	if securityContext.RunAsUser == nil || *securityContext.RunAsUser != 1000 {
		t.Errorf("Expected runAsUser 1000")
	}
	if securityContext.RunAsGroup == nil || *securityContext.RunAsGroup != 1000 {
		t.Errorf("Expected runAsGroup 1000")
	}
	if securityContext.SeccompProfile == nil || securityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("Expected RuntimeDefault seccomp profile")
	}
	if securityContext.Capabilities == nil || len(securityContext.Capabilities.Drop) != 1 || securityContext.Capabilities.Drop[0] != "ALL" {
		t.Errorf("Expected drop ALL capabilities")
	}

	// Case 2: CloneRepo is false, checkout init container is not present
	sandboxWithoutCheckout := BuildTaskSandbox(run, "task-inspect", TaskSandboxConfig{
		Namespace:       "eval-runner-sandbox",
		Image:           "custom-task-image:v1",
		AuthTokenDigest: digest,
		CloneRepo:       false,
	})
	if len(sandboxWithoutCheckout.Spec.PodTemplate.Spec.InitContainers) != 1 {
		t.Fatalf("Expected 1 init container when CloneRepo is false, got %d", len(sandboxWithoutCheckout.Spec.PodTemplate.Spec.InitContainers))
	}
	if sandboxWithoutCheckout.Spec.PodTemplate.Spec.InitContainers[0].Name != SandboxdInitContainerName {
		t.Errorf("Expected only install-sandboxd init container, got %q", sandboxWithoutCheckout.Spec.PodTemplate.Spec.InitContainers[0].Name)
	}
}

func TestBuildTaskSandbox_SecurityContext_SandboxVsInspect(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://git-server/repo.git",
				Commit: "abcdef123456",
			},
		},
	}
	digest := AuthTokenDigest("dummy-token")

	// Case 1: Sandbox task (SandboxTask = true, CloneRepo = true)
	sandboxTask := BuildTaskSandbox(run, "task-sandbox", TaskSandboxConfig{
		Namespace:          "eval-runner-sandbox",
		Image:              "alpine:3.21",
		AuthTokenDigest:    digest,
		CloneRepo:          true,
		StepTimeoutSeconds: 60,
		SandboxTask:        true,
	})

	// Pod security context on sandbox task:
	// - FSGroup stays
	// - RuntimeDefault seccomp profile stays
	// - runAsUser, runAsGroup, runAsNonRoot are NOT set
	podSecurityContext := sandboxTask.Spec.PodTemplate.Spec.SecurityContext
	if podSecurityContext == nil {
		t.Fatalf("Expected pod security context on sandbox task")
	}
	if podSecurityContext.RunAsUser != nil {
		t.Errorf("Expected pod runAsUser nil on sandbox task, got %v", *podSecurityContext.RunAsUser)
	}
	if podSecurityContext.RunAsGroup != nil {
		t.Errorf("Expected pod runAsGroup nil on sandbox task, got %v", *podSecurityContext.RunAsGroup)
	}
	if podSecurityContext.RunAsNonRoot != nil {
		t.Errorf("Expected pod runAsNonRoot nil on sandbox task, got %v", *podSecurityContext.RunAsNonRoot)
	}
	if podSecurityContext.FSGroup == nil || *podSecurityContext.FSGroup != 1000 {
		t.Errorf("Expected pod FSGroup 1000 on sandbox task, got %v", podSecurityContext.FSGroup)
	}
	if podSecurityContext.SeccompProfile == nil || podSecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("Expected pod RuntimeDefault seccomp profile on sandbox task")
	}

	// Task container security context on sandbox task:
	// - no runAsUser, runAsGroup, runAsNonRoot
	// - no capability drop
	// - no allowPrivilegeEscalation setting
	// - keeps RuntimeDefault seccomp profile
	taskContainer := sandboxTask.Spec.PodTemplate.Spec.Containers[0]
	taskSecurityContext := taskContainer.SecurityContext
	if taskSecurityContext == nil {
		t.Fatalf("Expected task container security context on sandbox task")
	}
	if taskSecurityContext.RunAsUser != nil {
		t.Errorf("Expected task container runAsUser nil on sandbox task, got %v", *taskSecurityContext.RunAsUser)
	}
	if taskSecurityContext.RunAsGroup != nil {
		t.Errorf("Expected task container runAsGroup nil on sandbox task, got %v", *taskSecurityContext.RunAsGroup)
	}
	if taskSecurityContext.RunAsNonRoot != nil {
		t.Errorf("Expected task container runAsNonRoot nil on sandbox task, got %v", *taskSecurityContext.RunAsNonRoot)
	}
	if taskSecurityContext.Capabilities != nil {
		t.Errorf("Expected task container capabilities nil on sandbox task, got %+v", taskSecurityContext.Capabilities)
	}
	if taskSecurityContext.AllowPrivilegeEscalation != nil {
		t.Errorf("Expected task container allowPrivilegeEscalation nil on sandbox task, got %v", *taskSecurityContext.AllowPrivilegeEscalation)
	}
	if taskSecurityContext.SeccompProfile == nil || taskSecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("Expected task container RuntimeDefault seccomp profile on sandbox task")
	}

	// Both init containers keep their non-root security context
	if len(sandboxTask.Spec.PodTemplate.Spec.InitContainers) != 2 {
		t.Fatalf("Expected 2 init containers on sandbox task with CloneRepo, got %d", len(sandboxTask.Spec.PodTemplate.Spec.InitContainers))
	}
	for _, initContainer := range sandboxTask.Spec.PodTemplate.Spec.InitContainers {
		initSecurityContext := initContainer.SecurityContext
		if initSecurityContext == nil {
			t.Fatalf("Expected security context on init container %s", initContainer.Name)
		}
		if initSecurityContext.RunAsNonRoot == nil || !*initSecurityContext.RunAsNonRoot {
			t.Errorf("Expected runAsNonRoot true on init container %s", initContainer.Name)
		}
		if initSecurityContext.RunAsUser == nil || *initSecurityContext.RunAsUser != 1000 {
			t.Errorf("Expected runAsUser 1000 on init container %s", initContainer.Name)
		}
		if initSecurityContext.RunAsGroup == nil || *initSecurityContext.RunAsGroup != 1000 {
			t.Errorf("Expected runAsGroup 1000 on init container %s", initContainer.Name)
		}
		if initSecurityContext.AllowPrivilegeEscalation == nil || *initSecurityContext.AllowPrivilegeEscalation {
			t.Errorf("Expected allowPrivilegeEscalation false on init container %s", initContainer.Name)
		}
		if initSecurityContext.Capabilities == nil || len(initSecurityContext.Capabilities.Drop) != 1 || initSecurityContext.Capabilities.Drop[0] != "ALL" {
			t.Errorf("Expected drop ALL capabilities on init container %s", initContainer.Name)
		}
		if initSecurityContext.SeccompProfile == nil || initSecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
			t.Errorf("Expected RuntimeDefault seccomp profile on init container %s", initContainer.Name)
		}
	}

	// Case 2: Inspect task (SandboxTask = false)
	inspectTask := BuildTaskSandbox(run, "task-inspect", TaskSandboxConfig{
		Namespace:       "eval-runner-sandbox",
		Image:           "custom-task-image:v1",
		AuthTokenDigest: digest,
		CloneRepo:       false,
		SandboxTask:     false,
	})

	// Pod security context on inspect task: unchanged
	inspectPodSecurityContext := inspectTask.Spec.PodTemplate.Spec.SecurityContext
	if inspectPodSecurityContext == nil {
		t.Fatalf("Expected pod security context on inspect task")
	}
	if inspectPodSecurityContext.RunAsUser == nil || *inspectPodSecurityContext.RunAsUser != 1000 {
		t.Errorf("Expected pod runAsUser 1000 on inspect task")
	}
	if inspectPodSecurityContext.RunAsGroup == nil || *inspectPodSecurityContext.RunAsGroup != 1000 {
		t.Errorf("Expected pod runAsGroup 1000 on inspect task")
	}
	if inspectPodSecurityContext.RunAsNonRoot == nil || !*inspectPodSecurityContext.RunAsNonRoot {
		t.Errorf("Expected pod runAsNonRoot true on inspect task")
	}
	if inspectPodSecurityContext.FSGroup == nil || *inspectPodSecurityContext.FSGroup != 1000 {
		t.Errorf("Expected pod FSGroup 1000 on inspect task")
	}
	if inspectPodSecurityContext.SeccompProfile == nil || inspectPodSecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("Expected pod RuntimeDefault seccomp profile on inspect task")
	}

	// Task container security context on inspect task: unchanged
	inspectContainer := inspectTask.Spec.PodTemplate.Spec.Containers[0]
	inspectSecurityContext := inspectContainer.SecurityContext
	if inspectSecurityContext == nil {
		t.Fatalf("Expected container security context on inspect task")
	}
	if inspectSecurityContext.RunAsUser == nil || *inspectSecurityContext.RunAsUser != 1000 {
		t.Errorf("Expected runAsUser 1000 on inspect task container")
	}
	if inspectSecurityContext.RunAsGroup == nil || *inspectSecurityContext.RunAsGroup != 1000 {
		t.Errorf("Expected runAsGroup 1000 on inspect task container")
	}
	if inspectSecurityContext.RunAsNonRoot == nil || !*inspectSecurityContext.RunAsNonRoot {
		t.Errorf("Expected runAsNonRoot true on inspect task container")
	}
	if inspectSecurityContext.AllowPrivilegeEscalation == nil || *inspectSecurityContext.AllowPrivilegeEscalation {
		t.Errorf("Expected allowPrivilegeEscalation false on inspect task container")
	}
	if inspectSecurityContext.Capabilities == nil || len(inspectSecurityContext.Capabilities.Drop) != 1 || inspectSecurityContext.Capabilities.Drop[0] != "ALL" {
		t.Errorf("Expected drop ALL capabilities on inspect task container")
	}
	if inspectSecurityContext.SeccompProfile == nil || inspectSecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("Expected RuntimeDefault seccomp profile on inspect task container")
	}

	// install-sandboxd init container is byte-for-byte identical between sandbox and inspect tasks
	if !reflect.DeepEqual(sandboxTask.Spec.PodTemplate.Spec.InitContainers[0], inspectTask.Spec.PodTemplate.Spec.InitContainers[0]) {
		t.Errorf("Expected install-sandboxd init container to be byte-for-byte identical between sandbox and inspect tasks")
	}
}

func TestRepoCheckoutScript(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not found on PATH")
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "git://example.com/repo.git",
				Commit: "test-commit",
			},
		},
	}
	sandbox := BuildTaskSandbox(run, "task-test", TaskSandboxConfig{
		Namespace:       "eval-runner-sandbox",
		AuthTokenDigest: AuthTokenDigest("dummy-token"),
		CloneRepo:       true,
	})
	if len(sandbox.Spec.PodTemplate.Spec.InitContainers) != 2 {
		t.Fatalf("Expected 2 init containers, got %d", len(sandbox.Spec.PodTemplate.Spec.InitContainers))
	}
	checkoutScript := sandbox.Spec.PodTemplate.Spec.InitContainers[1].Command[2]

	temporaryDirectory := t.TempDir()
	binDirectory := filepath.Join(temporaryDirectory, "bin")
	if err := os.MkdirAll(binDirectory, 0755); err != nil {
		t.Fatalf("failed to create bin directory: %v", err)
	}

	fakeGitPath := filepath.Join(binDirectory, "git")
	fakeGitScript := `#!/bin/sh
if [ "$1" = "clone" ]; then
    url="$3"
    target="$4"
    if [ "$url" = "fail-clone" ]; then
        echo "fatal: repository 'fail-clone' not found" >&2
        exit 128
    fi
    if [ "$url" = "timeout-clone" ]; then
        exit 124
    fi
    mkdir -p "$target"
    exit 0
fi
if [ "$1" = "-C" ] && [ "$3" = "checkout" ]; then
    commit="$4"
    if [ "$commit" = "fail-checkout" ]; then
        echo "error: pathspec 'fail-checkout' did not match any file(s) known to git" >&2
        exit 1
    fi
    if [ "$commit" = "timeout-checkout" ]; then
        exit 124
    fi
    exit 0
fi
exit 0
`
	if err := os.WriteFile(fakeGitPath, []byte(fakeGitScript), 0755); err != nil {
		t.Fatalf("failed to write fake git: %v", err)
	}

	testCases := []struct {
		name                 string
		repositoryURL        string
		repositoryCommit     string
		expectedExitCode     int
		expectedMessagePart  string
		expectRepoDirCreated bool
	}{
		{
			name:                 "SuccessPath",
			repositoryURL:        "https://example.com/valid/repo.git",
			repositoryCommit:     "v1.0.0",
			expectedExitCode:     0,
			expectRepoDirCreated: true,
		},
		{
			name:                "PlainCloneFailure",
			repositoryURL:       "fail-clone",
			repositoryCommit:    "main",
			expectedExitCode:    128,
			expectedMessagePart: "step git clone failed with exit code 128",
		},
		{
			name:                "CloneTimeout",
			repositoryURL:       "timeout-clone",
			repositoryCommit:    "main",
			expectedExitCode:    124,
			expectedMessagePart: "step git clone failed: sandbox did not report completion",
		},
		{
			name:                "PlainCheckoutFailure",
			repositoryURL:       "https://example.com/valid/repo.git",
			repositoryCommit:    "fail-checkout",
			expectedExitCode:    1,
			expectedMessagePart: "step git checkout failed with exit code 1",
		},
		{
			name:                "CheckoutTimeout",
			repositoryURL:       "https://example.com/valid/repo.git",
			repositoryCommit:    "timeout-checkout",
			expectedExitCode:    124,
			expectedMessagePart: "step git checkout failed: sandbox did not report completion",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			caseDirectory := t.TempDir()
			workspaceDirectory := filepath.Join(caseDirectory, "workspace")
			if err := os.MkdirAll(workspaceDirectory, 0755); err != nil {
				t.Fatalf("failed to create workspace: %v", err)
			}
			terminationLogPath := filepath.Join(caseDirectory, "termination-log")

			command := exec.Command("sh", "-c", checkoutScript)
			command.Env = append(os.Environ(),
				"PATH="+binDirectory+":"+os.Getenv("PATH"),
				"WORKSPACE_ROOT="+workspaceDirectory,
				"TERMINATION_LOG="+terminationLogPath,
				"REPO_URL="+tc.repositoryURL,
				"REPO_COMMIT="+tc.repositoryCommit,
			)

			output, err := command.CombinedOutput()
			exitCode := 0
			if err != nil {
				if exitError, ok := err.(*exec.ExitError); ok {
					exitCode = exitError.ExitCode()
				} else {
					t.Fatalf("unexpected execution error: %v", err)
				}
			}

			if exitCode != tc.expectedExitCode {
				t.Errorf("expected exit code %d, got %d (output: %s)", tc.expectedExitCode, exitCode, string(output))
			}

			if tc.expectedMessagePart != "" {
				logBytes, readErr := os.ReadFile(terminationLogPath)
				if readErr != nil {
					t.Fatalf("failed to read termination log: %v", readErr)
				}
				logContent := string(logBytes)
				if !strings.Contains(logContent, tc.expectedMessagePart) {
					t.Errorf("expected termination log to contain %q, got %q", tc.expectedMessagePart, logContent)
				}
			}

			if tc.expectRepoDirCreated {
				repoDirectory := filepath.Join(workspaceDirectory, "repo")
				if info, statErr := os.Stat(repoDirectory); statErr != nil || !info.IsDir() {
					t.Errorf("expected repo directory to be created at %q, err: %v", repoDirectory, statErr)
				}
			}
		})
	}
}

func TestBuildTaskSandbox_ResourceLimits_Explicit(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-resources",
			Namespace: "eval-runner",
		},
	}
	token := "auth-token-12345"
	digest := AuthTokenDigest(token)

	cpuLimit := resource.MustParse("2")
	memoryLimit := resource.MustParse("4Gi")

	sb := BuildTaskSandbox(run, "custom-task", TaskSandboxConfig{
		Namespace:       "eval-runner-sandbox",
		AuthTokenDigest: digest,
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    cpuLimit,
			corev1.ResourceMemory: memoryLimit,
		},
	})

	podSpec := sb.Spec.PodTemplate.Spec
	if len(podSpec.Containers) == 0 {
		t.Fatalf("Expected at least 1 container in pod spec")
	}

	taskContainer := podSpec.Containers[0]
	// Assert limits match explicit values
	if taskContainer.Resources.Limits.Cpu().Cmp(cpuLimit) != 0 {
		t.Errorf("Expected task container CPU limit %v, got %v", cpuLimit, taskContainer.Resources.Limits.Cpu())
	}
	if taskContainer.Resources.Limits.Memory().Cmp(memoryLimit) != 0 {
		t.Errorf("Expected task container Memory limit %v, got %v", memoryLimit, taskContainer.Resources.Limits.Memory())
	}

	// Assert requests remain the 100m / 128Mi default
	expectedCPURequest := resource.MustParse("100m")
	expectedMemoryRequest := resource.MustParse("128Mi")
	if taskContainer.Resources.Requests.Cpu().Cmp(expectedCPURequest) != 0 {
		t.Errorf("Expected task container CPU request %v, got %v", expectedCPURequest, taskContainer.Resources.Requests.Cpu())
	}
	if taskContainer.Resources.Requests.Memory().Cmp(expectedMemoryRequest) != 0 {
		t.Errorf("Expected task container Memory request %v, got %v", expectedMemoryRequest, taskContainer.Resources.Requests.Memory())
	}

	// Assert init containers keep their default limits (500m / 512Mi)
	defaultCPULimit := resource.MustParse("500m")
	defaultMemoryLimit := resource.MustParse("512Mi")
	for _, initContainer := range podSpec.InitContainers {
		if initContainer.Resources.Limits.Cpu().Cmp(defaultCPULimit) != 0 {
			t.Errorf("Init container %s CPU limit should be %v, got %v", initContainer.Name, defaultCPULimit, initContainer.Resources.Limits.Cpu())
		}
		if initContainer.Resources.Limits.Memory().Cmp(defaultMemoryLimit) != 0 {
			t.Errorf("Init container %s Memory limit should be %v, got %v", initContainer.Name, defaultMemoryLimit, initContainer.Resources.Limits.Memory())
		}
	}
}

func TestBuildTaskSandbox_ResourceLimits_DefaultsWhenAbsent(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run-default-resources",
			Namespace: "eval-runner",
		},
	}
	token := "auth-token-12345"
	digest := AuthTokenDigest(token)

	sandbox := BuildTaskSandbox(run, "default-task", TaskSandboxConfig{
		Namespace:       "eval-runner-sandbox",
		AuthTokenDigest: digest,
	})

	podSpec := sandbox.Spec.PodTemplate.Spec
	if len(podSpec.Containers) == 0 {
		t.Fatalf("Expected at least 1 container in pod spec")
	}

	taskContainer := podSpec.Containers[0]
	defaultCPULimit := resource.MustParse("500m")
	defaultMemoryLimit := resource.MustParse("512Mi")

	if taskContainer.Resources.Limits.Cpu().Cmp(defaultCPULimit) != 0 {
		t.Errorf("Expected default task container CPU limit %v, got %v", defaultCPULimit, taskContainer.Resources.Limits.Cpu())
	}
	if taskContainer.Resources.Limits.Memory().Cmp(defaultMemoryLimit) != 0 {
		t.Errorf("Expected default task container Memory limit %v, got %v", defaultMemoryLimit, taskContainer.Resources.Limits.Memory())
	}
}
