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
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/pki"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

const (
	// DefaultSandboxNamespace is the namespace where sandboxes run.
	DefaultSandboxNamespace = "eval-runner-sandbox"

	// DefaultControllerNamespace is the namespace where eval-runner controller runs.
	DefaultControllerNamespace = "eval-runner"

	// SandboxdImage is the pinned image containing the sandboxd binary.
	SandboxdImage = "gcr.io/justinsb-knotai-dev/sandboxd:0d9e0d2@sha256:01efbe0c568ec76e73e0c1c9337a77f61f8b45e2b06efb39ec256c568d782609"

	// GitCheckoutImage is the pinned image used to checkout repositories in sandbox tasks.
	GitCheckoutImage = "alpine/git:v2.47.2"

	// DiscoveryImage is the pinned image used for discovery sandboxes.
	DiscoveryImage = "alpine/git:v2.47.2"

	// DiscoveryContainerName is the container name in the discovery sandbox.
	DiscoveryContainerName = "agent"

	// TaskContainerName is the container name for the evaluated task in the task sandbox.
	TaskContainerName = DiscoveryContainerName

	// SandboxdInitContainerName is the init container name that injects sandboxd.
	SandboxdInitContainerName = "install-sandboxd"

	// RepoCheckoutInitContainerName is the init container name that checks out the repository.
	RepoCheckoutInitContainerName = "checkout-repo"

	// DefaultRepositoryCloneRetrySeconds is the default retry window in seconds for repository clone attempts in task sandboxes.
	DefaultRepositoryCloneRetrySeconds = 60

	// LabelRunName tracks the name of the Run that owns the sandbox.
	LabelRunName = "evalrunner.gke-labs.dev/run-name"

	// LabelRunNamespace tracks the namespace of the Run.
	LabelRunNamespace = "evalrunner.gke-labs.dev/run-namespace"

	// LabelRunUID tracks the UID of the Run for garbage collection.
	LabelRunUID = "evalrunner.gke-labs.dev/run-uid"

	// LabelRole indicates the role of the sandbox.
	LabelRole = "evalrunner.gke-labs.dev/role"

	// LabelSandbox indicates whether the pod is a sandbox managed by eval-runner.
	LabelSandbox = "evalrunner.gke-labs.dev/sandbox"

	// RoleDiscovery is the role value for discovery sandboxes.
	RoleDiscovery = "discovery"

	// RoleSample is the role value for sample execution sandboxes.
	RoleSample = "sample"

	// RolePrebuild is the role value for manifest prebuild sandboxes.
	RolePrebuild = "prebuild"

	// DefaultSampleRunnerImage is the default image used for sample sandboxes.
	DefaultSampleRunnerImage = "sample-runner:latest"

	// AnnotationTaskID tracks the original task ID on the sandbox.
	AnnotationTaskID = "evalrunner.gke-labs.dev/task-id"

	// AnnotationPrebuildName tracks the manifest prebuild name on a prebuild sandbox.
	AnnotationPrebuildName = "evalrunner.gke-labs.dev/prebuild-name"

	// AnnotationControllerInstance tracks the controller instance that created the sandbox.
	AnnotationControllerInstance = "evalrunner.gke-labs.dev/controller-instance"

	// AnnotationTaskAttempt tracks the attempt number (restarts count) of the task when the sandbox was created.
	AnnotationTaskAttempt = "evalrunner.gke-labs.dev/task-attempt"

	// LabelServicePortalIntercept is the pod label opting the sandbox into NRI service portal interception.
	LabelServicePortalIntercept = "service-portal.gke.io/intercept"

	// ServicePortalCACertificatePath is the container path where service-portal-nri-plugin
	// mounts the node's interception CA certificate inside intercepted containers.
	ServicePortalCACertificatePath = "/etc/service-portal/ca/tls.crt"

	// PlaceholderModelCredential is a recognisable, non-functional placeholder string injected
	// as a plain literal value into sample-runner pods so evaluation SDKs construct and send model
	// requests normally without hanging. Authentic credentials belong exclusively to the controller
	// and service portals, and are never mounted into task pods. When a service portal is deployed,
	// it swaps the placeholder for authentic credentials on egress. Without a service portal, the
	// upstream provider promptly rejects the placeholder with an authentication error.
	PlaceholderModelCredential = "placeholder-model-credential"

	// KindNodeSelectorLabel is the node label selecting dedicated nodes for kind-cluster tasks.
	KindNodeSelectorLabel = "eval-runner.gke-labs.dev/kind"

	// DockerInDockerContainerName is the sidecar container name providing Docker for kind-cluster tasks.
	DockerInDockerContainerName = "dind"

	// DockerInDockerImage is the pinned Docker-in-Docker sidecar image for kind-cluster tasks.
	DockerInDockerImage = "docker:27-dind"

	// DockerInDockerPort is the plaintext Docker daemon TCP port exposed by the dind sidecar on loopback.
	DockerInDockerPort int32 = 2375

	// DockerInDockerHost is the DOCKER_HOST value injected into kind-cluster task containers.
	DockerInDockerHost = "tcp://127.0.0.1:2375"

	// DefaultDockerInDockerCPURequest is the default CPU request for the dind sidecar container.
	DefaultDockerInDockerCPURequest = "500m"

	// DefaultDockerInDockerMemoryRequest is the default memory request for the dind sidecar container.
	DefaultDockerInDockerMemoryRequest = "1Gi"

	// DefaultDockerInDockerCPULimit is the default CPU limit for the dind sidecar container.
	DefaultDockerInDockerCPULimit = "4"

	// DefaultDockerInDockerMemoryLimit is the default memory limit for the dind sidecar container.
	DefaultDockerInDockerMemoryLimit = "8Gi"
)

// PlaceholderModelCredentialEnvironmentVariables is the table of model credential environment
// variable names injected into sample-runner pods with PlaceholderModelCredential.
// It includes the variable(s) read by the Gemini SDK used by Inspect evals, and is easily
// extensible for other providers.
var PlaceholderModelCredentialEnvironmentVariables = []string{
	"GEMINI_API_KEY",
	"OPENAI_API_KEY",
	"ANTHROPIC_API_KEY",
}

// SampleRunnerPlaceholderEnvironmentVariables returns the placeholder model credential environment
// variables injected into sample-runner pods.
func SampleRunnerPlaceholderEnvironmentVariables() []corev1.EnvVar {
	environmentVariables := make([]corev1.EnvVar, len(PlaceholderModelCredentialEnvironmentVariables))
	for i, name := range PlaceholderModelCredentialEnvironmentVariables {
		environmentVariables[i] = corev1.EnvVar{
			Name:  name,
			Value: PlaceholderModelCredential,
		}
	}
	return environmentVariables
}

// ServicePortalCAEnvironmentVariables returns client-library CA bundle environment variables
// pointing at ServicePortalCACertificatePath. service-portal-nri-plugin injects SSL_CERT_FILE
// automatically, while tools such as git (GnuTLS libcurl), curl, Python requests (tiktoken),
// and Node.js read their own CA bundle variables.
func ServicePortalCAEnvironmentVariables() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "GIT_SSL_CAINFO", Value: ServicePortalCACertificatePath},
		{Name: "CURL_CA_BUNDLE", Value: ServicePortalCACertificatePath},
		{Name: "REQUESTS_CA_BUNDLE", Value: ServicePortalCACertificatePath},
		{Name: "NODE_EXTRA_CA_CERTS", Value: ServicePortalCACertificatePath},
	}
}

// AuthTokenDigest calculates the hex SHA-256 digest of the given bearer token.
func AuthTokenDigest(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// AuthSecretNameForRun deterministically generates a valid Kubernetes resource name
// for a Run's discovery auth Secret (DNS-1123 label, max 63 characters).
func AuthSecretNameForRun(runName string) string {
	prefix := "discovery-auth-"
	fullName := prefix + runName
	if len(fullName) <= 63 {
		return fullName
	}

	h := sha256.Sum256([]byte(runName))
	hashSuffix := "-" + hex.EncodeToString(h[:4]) // 9 chars: -12345678
	maxPrefixLen := 63 - len(hashSuffix)
	return fullName[:maxPrefixLen] + hashSuffix
}

// TaskAuthSecretNameForRun deterministically generates a valid Kubernetes resource name
// for a Run's task auth Secret (DNS-1123 label, max 63 characters).
func TaskAuthSecretNameForRun(runName, taskID string) string {
	prefix := "task-auth-"
	sanitized := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return '-'
	}, taskID)
	sanitized = strings.Trim(sanitized, "-")

	h := sha256.Sum256([]byte(runName + "/" + taskID))
	hashSuffix := "-" + hex.EncodeToString(h[:4])

	fullName := prefix + runName + "-" + sanitized + hashSuffix
	if len(fullName) <= 63 {
		return fullName
	}

	maxBaseLen := 63 - len(hashSuffix)
	base := prefix + runName + "-" + sanitized
	if len(base) > maxBaseLen {
		base = base[:maxBaseLen]
		base = strings.TrimRight(base, "-")
	}
	return base + hashSuffix
}

// TaskSandboxNameForRun deterministically generates a valid Kubernetes resource name
// for a Run's task execution sandbox (DNS-1123 label, max 63 characters).
func TaskSandboxNameForRun(runName, taskID string) string {
	prefix := "task-"
	sanitized := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return '-'
	}, taskID)
	sanitized = strings.Trim(sanitized, "-")

	h := sha256.Sum256([]byte(taskID))
	hashSuffix := "-" + hex.EncodeToString(h[:4]) // -12345678 (9 chars)

	fullName := prefix + runName + "-" + sanitized + hashSuffix
	if len(fullName) <= 63 {
		return fullName
	}

	maxBaseLen := 63 - len(hashSuffix)
	base := prefix + runName + "-" + sanitized
	if len(base) > maxBaseLen {
		base = base[:maxBaseLen]
		base = strings.TrimRight(base, "-")
	}
	return base + hashSuffix
}

// PrebuildAuthSecretNameForRun deterministically generates a valid Kubernetes resource name
// for a Run's prebuild auth Secret (DNS-1123 label, max 63 characters).
func PrebuildAuthSecretNameForRun(runName, prebuildName string) string {
	return namedResourceForRun("prebuild-auth-", runName, prebuildName, runName+"/"+prebuildName)
}

// PrebuildSandboxNameForRun deterministically generates a valid Kubernetes resource name
// for a Run's prebuild execution sandbox (DNS-1123 label, max 63 characters).
func PrebuildSandboxNameForRun(runName, prebuildName string) string {
	return namedResourceForRun("prebuild-", runName, prebuildName, prebuildName)
}

func namedResourceForRun(prefix, runName, identifier, hashInput string) string {
	sanitized := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return '-'
	}, identifier)
	sanitized = strings.Trim(sanitized, "-")

	sum := sha256.Sum256([]byte(hashInput))
	hashSuffix := "-" + hex.EncodeToString(sum[:4])

	fullName := prefix + runName + "-" + sanitized + hashSuffix
	if len(fullName) <= 63 {
		return fullName
	}

	maxBaseLen := 63 - len(hashSuffix)
	base := prefix + runName + "-" + sanitized
	if len(base) > maxBaseLen {
		base = base[:maxBaseLen]
		base = strings.TrimRight(base, "-")
	}
	return base + hashSuffix
}

// TaskSandboxConfig configures task and prebuild sandbox creation.
type TaskSandboxConfig struct {
	Namespace              string
	ControllerInstance     string
	TaskAttempt            int
	SampleRunnerImage      string
	Image                  string
	AuthTokenDigest        string
	ServerCertSecret       string
	CAConfigMap            string
	SandboxdImage          string
	CloneRepo              bool
	StepTimeoutSeconds     int
	SandboxTask            bool
	KindCluster            bool
	KindClusterLimits      corev1.ResourceList
	Limits                 corev1.ResourceList
	ServicePortalIntercept bool
}

// BuildTaskSandbox constructs the Sandbox CR for executing an eval task for a Run.
func BuildTaskSandbox(run *v1alpha1.Run, taskID string, cfg TaskSandboxConfig) *sandboxv1beta1.Sandbox {
	annotations := map[string]string{
		AnnotationTaskID:      taskID,
		AnnotationTaskAttempt: strconv.Itoa(cfg.TaskAttempt),
	}
	return buildWorkloadSandbox(run, TaskSandboxNameForRun(run.Name, taskID), RoleSample, annotations, cfg)
}

// BuildPrebuildSandbox constructs the Sandbox CR for executing a manifest prebuild for a Run.
func BuildPrebuildSandbox(run *v1alpha1.Run, prebuildName string, cfg TaskSandboxConfig) *sandboxv1beta1.Sandbox {
	annotations := map[string]string{
		AnnotationPrebuildName: prebuildName,
	}
	cfg.SandboxTask = true
	return buildWorkloadSandbox(run, PrebuildSandboxNameForRun(run.Name, prebuildName), RolePrebuild, annotations, cfg)
}

func buildWorkloadSandbox(
	run *v1alpha1.Run,
	sandboxName string,
	role string,
	baseAnnotations map[string]string,
	cfg TaskSandboxConfig,
) *sandboxv1beta1.Sandbox {
	if cfg.AuthTokenDigest == "" {
		panic("authTokenDigest is required")
	}
	sandboxNamespace := cfg.Namespace
	if sandboxNamespace == "" {
		sandboxNamespace = DefaultSandboxNamespace
	}
	image := cfg.Image
	if image == "" {
		image = cfg.SampleRunnerImage
	}
	if image == "" {
		image = DefaultSampleRunnerImage
	}
	limits := cfg.Limits
	if limits == nil {
		limits = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(v1alpha1.DefaultTaskCPULimit),
			corev1.ResourceMemory: resource.MustParse(v1alpha1.DefaultTaskMemoryLimit),
		}
	}
	sbName := sandboxName
	serverCertSecret := cfg.ServerCertSecret
	if serverCertSecret == "" {
		serverCertSecret = pki.ServerSecretName(sbName)
	}
	caConfigMap := cfg.CAConfigMap
	if caConfigMap == "" {
		caConfigMap = pki.DefaultCAConfigMapName
	}
	sandboxdImg := cfg.SandboxdImage
	if sandboxdImg == "" {
		sandboxdImg = SandboxdImage
	}

	automountToken := false
	runAsNonRoot := true
	runAsUser := int64(1000)
	runAsGroup := int64(1000)
	allowPrivilegeEscalation := false
	seccompProfile := corev1.SeccompProfile{
		Type: corev1.SeccompProfileTypeRuntimeDefault,
	}

	podSecurityContext := &corev1.PodSecurityContext{
		FSGroup:        &runAsGroup,
		SeccompProfile: &seccompProfile,
	}
	var taskSecurityContext *corev1.SecurityContext
	if cfg.SandboxTask {
		taskSecurityContext = &corev1.SecurityContext{
			SeccompProfile: &seccompProfile,
		}
	} else {
		podSecurityContext.RunAsNonRoot = &runAsNonRoot
		podSecurityContext.RunAsUser = &runAsUser
		podSecurityContext.RunAsGroup = &runAsGroup
		taskSecurityContext = &corev1.SecurityContext{
			AllowPrivilegeEscalation: &allowPrivilegeEscalation,
			RunAsNonRoot:             &runAsNonRoot,
			RunAsUser:                &runAsUser,
			RunAsGroup:               &runAsGroup,
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
			},
			SeccompProfile: &seccompProfile,
		}
	}

	labels := map[string]string{
		LabelRunName:      run.Name,
		LabelRunNamespace: run.Namespace,
		LabelRole:         role,
		LabelSandbox:      "true",
	}
	if run.UID != "" {
		labels[LabelRunUID] = string(run.UID)
	}
	if cfg.ServicePortalIntercept {
		labels[LabelServicePortalIntercept] = "true"
	}

	annotations := make(map[string]string, len(baseAnnotations)+1)
	for key, value := range baseAnnotations {
		annotations[key] = value
	}
	if cfg.ControllerInstance != "" {
		annotations[AnnotationControllerInstance] = cfg.ControllerInstance
	}

	cmd := []string{
		"/sandboxd-bin/sandboxd",
		"--root-dir=/workspace",
		"--rest-port=8080",
		"--grpc-port=9090",
		"--health-port=8081",
		"--auth-token-sha256=" + cfg.AuthTokenDigest,
		"--tls-cert-file=/sandbox-tls/tls.crt",
		"--tls-key-file=/sandbox-tls/tls.key",
		"--tls-client-ca-file=/sandbox-ca/ca.crt",
		"--require-client-cert",
	}

	initContainers := []corev1.Container{
		{
			Name:  SandboxdInitContainerName,
			Image: sandboxdImg,
			Command: []string{
				"/bin/sh",
				"-c",
				"cp /usr/local/bin/sandboxd /sandboxd-bin/sandboxd",
			},
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: &allowPrivilegeEscalation,
				RunAsNonRoot:             &runAsNonRoot,
				RunAsUser:                &runAsUser,
				RunAsGroup:               &runAsGroup,
				Capabilities: &corev1.Capabilities{
					Drop: []corev1.Capability{"ALL"},
				},
				SeccompProfile: &seccompProfile,
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("512Mi"),
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      "sandboxd-bin",
					MountPath: "/sandboxd-bin",
				},
			},
		},
	}

	if cfg.CloneRepo && run.Spec.Repo.URL != "" {
		var checkoutEnv []corev1.EnvVar
		checkoutEnv = append(checkoutEnv, corev1.EnvVar{
			Name:  "REPO_URL",
			Value: run.Spec.Repo.URL,
		})
		if run.Spec.Repo.Commit != "" {
			checkoutEnv = append(checkoutEnv, corev1.EnvVar{
				Name:  "REPO_COMMIT",
				Value: run.Spec.Repo.Commit,
			})
		}
		if cfg.StepTimeoutSeconds > 0 {
			checkoutEnv = append(checkoutEnv, corev1.EnvVar{
				Name:  "TIMEOUT_SECONDS",
				Value: strconv.Itoa(cfg.StepTimeoutSeconds),
			})
		}
		if cfg.ServicePortalIntercept {
			checkoutEnv = append(checkoutEnv, corev1.EnvVar{
				Name:  "GIT_SSL_CAINFO",
				Value: ServicePortalCACertificatePath,
			})
		}

		checkoutScript := `status=0
workspace_root="${WORKSPACE_ROOT:-/workspace}"
repo_dir="$workspace_root/repo"
termination_log="${TERMINATION_LOG:-/dev/termination-log}"
clone_retry_seconds="${CLONE_RETRY_SECONDS:-` + strconv.Itoa(DefaultRepositoryCloneRetrySeconds) + `}"

if [ -n "$TIMEOUT_SECONDS" ] && [ "$TIMEOUT_SECONDS" -gt 0 ] 2>/dev/null; then
    timeout_prefix="timeout $TIMEOUT_SECONDS"
    if [ "$TIMEOUT_SECONDS" -lt "$clone_retry_seconds" ]; then
        clone_retry_seconds="$TIMEOUT_SECONDS"
    fi
else
    timeout_prefix=""
fi

start_time=$(date +%s)
deadline=$((start_time + clone_retry_seconds))

seconds_remaining() {
    current_time=$(date +%s)
    echo $((deadline - current_time))
}

shallow_fetched=0
while :; do
    rm -rf "$repo_dir"
    if [ -n "$REPO_COMMIT" ]; then
        output=$($timeout_prefix git init "$repo_dir" 2>&1)
        status=$?
        if [ "$status" -eq 0 ]; then
            output=$($timeout_prefix git -C "$repo_dir" fetch --depth 1 -- "$REPO_URL" "$REPO_COMMIT" 2>&1)
            status=$?
            if [ "$status" -eq 0 ]; then
                shallow_fetched=1
                break
            fi
            if [ "$status" -eq 124 ] || [ "$status" -eq 143 ]; then
                break
            fi
        elif [ "$status" -eq 124 ] || [ "$status" -eq 143 ]; then
            break
        fi
        rm -rf "$repo_dir"
    fi

    output=$($timeout_prefix git clone -- "$REPO_URL" "$repo_dir" 2>&1)
    status=$?
    if [ "$status" -eq 0 ] || [ "$status" -eq 124 ] || [ "$status" -eq 143 ]; then
        break
    fi

    if [ $(seconds_remaining) -le 0 ]; then
        break
    fi

    rm -rf "$repo_dir"
    sleep 1
done

if [ "$status" -ne 0 ]; then
    rm -rf "$repo_dir"
    if [ "$status" -eq 124 ] || [ "$status" -eq 143 ]; then
        echo "step git clone failed: sandbox did not report completion" > "$termination_log" 2>/dev/null || true
    else
        tail_output=$(echo "$output" | tail -n 20)
        echo "step git clone failed with exit code $status: $tail_output" > "$termination_log" 2>/dev/null || true
    fi
    echo "$output" >&2
    exit "$status"
fi

if [ "$shallow_fetched" -eq 1 ] || [ -n "$REPO_COMMIT" ]; then
    if [ "$shallow_fetched" -eq 1 ]; then
        output=$($timeout_prefix git -C "$repo_dir" checkout --detach FETCH_HEAD 2>&1)
        status=$?
    else
        output=$($timeout_prefix git -C "$repo_dir" checkout "$REPO_COMMIT" 2>&1)
        status=$?
    fi
    if [ "$status" -ne 0 ]; then
        if [ "$status" -eq 124 ] || [ "$status" -eq 143 ]; then
            echo "step git checkout failed: sandbox did not report completion" > "$termination_log" 2>/dev/null || true
        else
            tail_output=$(echo "$output" | tail -n 20)
            echo "step git checkout failed with exit code $status: $tail_output" > "$termination_log" 2>/dev/null || true
        fi
        echo "$output" >&2
        exit "$status"
    fi
fi
`

		initContainers = append(initContainers, corev1.Container{
			Name:    RepoCheckoutInitContainerName,
			Image:   GitCheckoutImage,
			Command: []string{"/bin/sh", "-c", checkoutScript},
			Env:     checkoutEnv,
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: &allowPrivilegeEscalation,
				RunAsNonRoot:             &runAsNonRoot,
				RunAsUser:                &runAsUser,
				RunAsGroup:               &runAsGroup,
				Capabilities: &corev1.Capabilities{
					Drop: []corev1.Capability{"ALL"},
				},
				SeccompProfile: &seccompProfile,
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2"),
					corev1.ResourceMemory: resource.MustParse("512Mi"),
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      "workspace",
					MountPath: "/workspace",
				},
			},
		})
	}

	var taskEnvironmentVariables []corev1.EnvVar
	if !cfg.SandboxTask {
		taskEnvironmentVariables = SampleRunnerPlaceholderEnvironmentVariables()
	}
	if cfg.ServicePortalIntercept {
		taskEnvironmentVariables = append(taskEnvironmentVariables, ServicePortalCAEnvironmentVariables()...)
	}
	if cfg.KindCluster {
		taskEnvironmentVariables = append(taskEnvironmentVariables, corev1.EnvVar{
			Name:  "DOCKER_HOST",
			Value: DockerInDockerHost,
		})
	}

	sandbox := &sandboxv1beta1.Sandbox{
		TypeMeta: metav1.TypeMeta{
			APIVersion: sandboxv1beta1.GroupVersion.String(),
			Kind:       "Sandbox",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        sbName,
			Namespace:   sandboxNamespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: sandboxv1beta1.SandboxSpec{
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				PodTemplate: sandboxv1beta1.PodTemplate{
					ObjectMeta: sandboxv1beta1.PodMetadata{
						Labels:      labels,
						Annotations: annotations,
					},
					Spec: corev1.PodSpec{
						AutomountServiceAccountToken: &automountToken,
						SecurityContext:              podSecurityContext,
						RestartPolicy:                corev1.RestartPolicyNever,
						InitContainers:               initContainers,
						Containers: []corev1.Container{
							{
								Name:    TaskContainerName,
								Image:   image,
								Env:     taskEnvironmentVariables,
								Command: cmd,
								Ports: []corev1.ContainerPort{
									{
										Name:          "rest",
										ContainerPort: 8080,
										Protocol:      corev1.ProtocolTCP,
									},
									{
										Name:          "grpc",
										ContainerPort: 9090,
										Protocol:      corev1.ProtocolTCP,
									},
									{
										Name:          "health",
										ContainerPort: 8081,
										Protocol:      corev1.ProtocolTCP,
									},
								},
								ReadinessProbe: &corev1.Probe{
									ProbeHandler: corev1.ProbeHandler{
										HTTPGet: &corev1.HTTPGetAction{
											Path: "/v1/health",
											Port: intstr.FromInt(8081),
										},
									},
									InitialDelaySeconds: 1,
									PeriodSeconds:       1,
									TimeoutSeconds:      2,
									SuccessThreshold:    1,
									FailureThreshold:    3,
								},
								SecurityContext: taskSecurityContext,
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("128Mi"),
									},
									Limits: limits.DeepCopy(),
								},
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      "sandboxd-bin",
										MountPath: "/sandboxd-bin",
										ReadOnly:  true,
									},
									{
										Name:      "workspace",
										MountPath: "/workspace",
									},
									{
										Name:      "workspace",
										MountPath: "/out",
										SubPath:   "out",
									},
									{
										Name:      "sandbox-tls",
										MountPath: "/sandbox-tls",
										ReadOnly:  true,
									},
									{
										Name:      "sandbox-ca",
										MountPath: "/sandbox-ca",
										ReadOnly:  true,
									},
								},
							},
						},
						Volumes: []corev1.Volume{
							{
								Name: "sandboxd-bin",
								VolumeSource: corev1.VolumeSource{
									EmptyDir: &corev1.EmptyDirVolumeSource{},
								},
							},
							{
								Name: "workspace",
								VolumeSource: corev1.VolumeSource{
									EmptyDir: &corev1.EmptyDirVolumeSource{},
								},
							},
							{
								Name: "sandbox-tls",
								VolumeSource: corev1.VolumeSource{
									Secret: &corev1.SecretVolumeSource{
										SecretName:  serverCertSecret,
										DefaultMode: ptr.To(int32(0400)),
									},
								},
							},
							{
								Name: "sandbox-ca",
								VolumeSource: corev1.VolumeSource{
									ConfigMap: &corev1.ConfigMapVolumeSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: caConfigMap,
										},
										DefaultMode: ptr.To(int32(0444)),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if cfg.KindCluster {
		sandbox.Spec.PodTemplate.Spec.NodeSelector = map[string]string{
			KindNodeSelectorLabel: "true",
		}
		sandbox.Spec.PodTemplate.Spec.Containers = append(
			sandbox.Spec.PodTemplate.Spec.Containers,
			buildDockerInDockerSidecarContainer(cfg.KindClusterLimits),
		)
	}

	return sandbox
}

// buildDockerInDockerSidecarContainer returns the privileged Docker-in-Docker sidecar
// container attached to task sandboxes declaring the kind-cluster resource.
func buildDockerInDockerSidecarContainer(customLimits corev1.ResourceList) corev1.Container {
	limits := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(DefaultDockerInDockerCPULimit),
		corev1.ResourceMemory: resource.MustParse(DefaultDockerInDockerMemoryLimit),
	}
	if quantity, ok := customLimits[corev1.ResourceCPU]; ok && !quantity.IsZero() {
		limits[corev1.ResourceCPU] = quantity.DeepCopy()
	}
	if quantity, ok := customLimits[corev1.ResourceMemory]; ok && !quantity.IsZero() {
		limits[corev1.ResourceMemory] = quantity.DeepCopy()
	}

	return corev1.Container{
		Name:  DockerInDockerContainerName,
		Image: DockerInDockerImage,
		Env: []corev1.EnvVar{
			{
				Name:  "DOCKER_TLS_CERTDIR",
				Value: "",
			},
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          "docker",
				ContainerPort: DockerInDockerPort,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromInt32(DockerInDockerPort),
				},
			},
			InitialDelaySeconds: 1,
			PeriodSeconds:       1,
			TimeoutSeconds:      2,
			SuccessThreshold:    1,
			FailureThreshold:    3,
		},
		SecurityContext: &corev1.SecurityContext{
			Privileged: ptr.To(true),
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(DefaultDockerInDockerCPURequest),
				corev1.ResourceMemory: resource.MustParse(DefaultDockerInDockerMemoryRequest),
			},
			Limits: limits,
		},
	}
}

// SandboxNameForRun deterministically generates a valid Kubernetes resource name
// for a Run's discovery sandbox (DNS-1123 label, max 63 characters).
func SandboxNameForRun(runName string) string {
	prefix := "discovery-"
	fullName := prefix + runName
	if len(fullName) <= 63 {
		return fullName
	}

	h := sha256.Sum256([]byte(runName))
	hashSuffix := "-" + hex.EncodeToString(h[:4]) // 9 chars: -12345678
	maxPrefixLen := 63 - len(hashSuffix)
	return fullName[:maxPrefixLen] + hashSuffix
}

// SandboxOptions configures optional settings for BuildSandbox.
type SandboxOptions struct {
	ServerCertSecret string
	CAConfigMap      string
	SandboxdImage    string
}

// BuildSandbox constructs the Sandbox CR for discovering tasks for a Run.
// The pod spec implements deployment topology B (injecting sandboxd binary into rootfs)
// and concept §3.2 "empty resources = zero privilege":
// - automountServiceAccountToken: false
// - non-root user (UID 1000)
// - no extra capabilities (drop ALL)
// - RuntimeDefault seccomp profile
// - pinned images
// - resource requests and limits
// - sandboxd gRPC (9090) and REST (8080) ports exposed
// - readiness probe targeting sandboxd /v1/health on plaintext port 8081
// - mutual TLS (mTLS) with controller CA and per-sandbox server cert Secret
func BuildSandbox(run *v1alpha1.Run, sandboxNamespace string, authTokenDigest string, opts ...SandboxOptions) *sandboxv1beta1.Sandbox {
	if authTokenDigest == "" {
		panic("authTokenDigest is required")
	}
	if sandboxNamespace == "" {
		sandboxNamespace = DefaultSandboxNamespace
	}

	var opt SandboxOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	sbName := SandboxNameForRun(run.Name)
	serverCertSecret := opt.ServerCertSecret
	if serverCertSecret == "" {
		serverCertSecret = pki.ServerSecretName(sbName)
	}
	caConfigMap := opt.CAConfigMap
	if caConfigMap == "" {
		caConfigMap = pki.DefaultCAConfigMapName
	}
	sandboxdImg := opt.SandboxdImage
	if sandboxdImg == "" {
		sandboxdImg = SandboxdImage
	}

	automountToken := false
	runAsNonRoot := true
	runAsUser := int64(1000)
	runAsGroup := int64(1000)
	allowPrivilegeEscalation := false
	seccompProfile := corev1.SeccompProfile{
		Type: corev1.SeccompProfileTypeRuntimeDefault,
	}

	labels := map[string]string{
		LabelRunName:      run.Name,
		LabelRunNamespace: run.Namespace,
		LabelRole:         "discovery",
		LabelSandbox:      "true",
	}
	if run.UID != "" {
		labels[LabelRunUID] = string(run.UID)
	}

	cmd := []string{
		"/sandboxd-bin/sandboxd",
		"--root-dir=/workspace",
		"--rest-port=8080",
		"--grpc-port=9090",
		"--health-port=8081",
		"--auth-token-sha256=" + authTokenDigest,
		"--tls-cert-file=/sandbox-tls/tls.crt",
		"--tls-key-file=/sandbox-tls/tls.key",
		"--tls-client-ca-file=/sandbox-ca/ca.crt",
		"--require-client-cert",
	}

	return &sandboxv1beta1.Sandbox{
		TypeMeta: metav1.TypeMeta{
			APIVersion: sandboxv1beta1.GroupVersion.String(),
			Kind:       "Sandbox",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      SandboxNameForRun(run.Name),
			Namespace: sandboxNamespace,
			Labels:    labels,
		},
		Spec: sandboxv1beta1.SandboxSpec{
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				PodTemplate: sandboxv1beta1.PodTemplate{
					ObjectMeta: sandboxv1beta1.PodMetadata{
						Labels: labels,
					},
					Spec: corev1.PodSpec{
						AutomountServiceAccountToken: &automountToken,
						SecurityContext: &corev1.PodSecurityContext{
							RunAsNonRoot:   &runAsNonRoot,
							RunAsUser:      &runAsUser,
							RunAsGroup:     &runAsGroup,
							FSGroup:        &runAsGroup,
							SeccompProfile: &seccompProfile,
						},
						RestartPolicy: corev1.RestartPolicyNever,
						InitContainers: []corev1.Container{
							{
								Name:  SandboxdInitContainerName,
								Image: sandboxdImg,
								Command: []string{
									"/bin/sh",
									"-c",
									"cp /usr/local/bin/sandboxd /sandboxd-bin/sandboxd",
								},
								SecurityContext: &corev1.SecurityContext{
									AllowPrivilegeEscalation: &allowPrivilegeEscalation,
									RunAsNonRoot:             &runAsNonRoot,
									RunAsUser:                &runAsUser,
									RunAsGroup:               &runAsGroup,
									Capabilities: &corev1.Capabilities{
										Drop: []corev1.Capability{"ALL"},
									},
									SeccompProfile: &seccompProfile,
								},
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("128Mi"),
									},
									Limits: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("500m"),
										corev1.ResourceMemory: resource.MustParse("512Mi"),
									},
								},
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      "sandboxd-bin",
										MountPath: "/sandboxd-bin",
									},
								},
							},
						},
						Containers: []corev1.Container{
							{
								Name:    DiscoveryContainerName,
								Image:   DiscoveryImage,
								Command: cmd,
								Ports: []corev1.ContainerPort{
									{
										Name:          "rest",
										ContainerPort: 8080,
										Protocol:      corev1.ProtocolTCP,
									},
									{
										Name:          "grpc",
										ContainerPort: 9090,
										Protocol:      corev1.ProtocolTCP,
									},
									{
										Name:          "health",
										ContainerPort: 8081,
										Protocol:      corev1.ProtocolTCP,
									},
								},
								ReadinessProbe: &corev1.Probe{
									ProbeHandler: corev1.ProbeHandler{
										HTTPGet: &corev1.HTTPGetAction{
											Path: "/v1/health",
											Port: intstr.FromInt(8081),
										},
									},
									InitialDelaySeconds: 1,
									PeriodSeconds:       1,
									TimeoutSeconds:      2,
									SuccessThreshold:    1,
									FailureThreshold:    3,
								},
								SecurityContext: &corev1.SecurityContext{
									AllowPrivilegeEscalation: &allowPrivilegeEscalation,
									RunAsNonRoot:             &runAsNonRoot,
									RunAsUser:                &runAsUser,
									RunAsGroup:               &runAsGroup,
									Capabilities: &corev1.Capabilities{
										Drop: []corev1.Capability{"ALL"},
									},
									SeccompProfile: &seccompProfile,
								},
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("128Mi"),
									},
									Limits: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("2"),
										corev1.ResourceMemory: resource.MustParse("512Mi"),
									},
								},
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      "sandboxd-bin",
										MountPath: "/sandboxd-bin",
										ReadOnly:  true,
									},
									{
										Name:      "workspace",
										MountPath: "/workspace",
									},
									{
										Name:      "sandbox-tls",
										MountPath: "/sandbox-tls",
										ReadOnly:  true,
									},
									{
										Name:      "sandbox-ca",
										MountPath: "/sandbox-ca",
										ReadOnly:  true,
									},
								},
							},
						},
						Volumes: []corev1.Volume{
							{
								Name: "sandboxd-bin",
								VolumeSource: corev1.VolumeSource{
									EmptyDir: &corev1.EmptyDirVolumeSource{},
								},
							},
							{
								Name: "workspace",
								VolumeSource: corev1.VolumeSource{
									EmptyDir: &corev1.EmptyDirVolumeSource{},
								},
							},
							{
								Name: "sandbox-tls",
								VolumeSource: corev1.VolumeSource{
									Secret: &corev1.SecretVolumeSource{
										SecretName:  serverCertSecret,
										DefaultMode: ptr.To(int32(0400)),
									},
								},
							},
							{
								Name: "sandbox-ca",
								VolumeSource: corev1.VolumeSource{
									ConfigMap: &corev1.ConfigMapVolumeSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: caConfigMap,
										},
										DefaultMode: ptr.To(int32(0444)),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
