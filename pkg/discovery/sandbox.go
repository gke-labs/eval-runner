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

	// DiscoveryImage is the pinned image used for discovery sandboxes.
	DiscoveryImage = "alpine/git:v2.47.2"

	// DiscoveryContainerName is the container name in the discovery sandbox.
	DiscoveryContainerName = "agent"

	// SandboxdInitContainerName is the init container name that injects sandboxd.
	SandboxdInitContainerName = "install-sandboxd"

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

	// DefaultSampleRunnerImage is the default image used for sample sandboxes.
	DefaultSampleRunnerImage = "sample-runner:latest"

	// AnnotationTaskID tracks the original task ID on the sandbox.
	AnnotationTaskID = "evalrunner.gke-labs.dev/task-id"
)

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

// TaskSandboxConfig configures task sandbox creation.
type TaskSandboxConfig struct {
	Namespace         string
	SampleRunnerImage string
	ModelSecret       string
	AuthTokenDigest   string
	ServerCertSecret  string
	CAConfigMap       string
	SandboxdImage     string
}

// BuildTaskSandbox constructs the Sandbox CR for executing an eval task for a Run.
func BuildTaskSandbox(run *v1alpha1.Run, taskID string, cfg TaskSandboxConfig) *sandboxv1beta1.Sandbox {
	if cfg.AuthTokenDigest == "" {
		panic("authTokenDigest is required")
	}
	sandboxNamespace := cfg.Namespace
	if sandboxNamespace == "" {
		sandboxNamespace = DefaultSandboxNamespace
	}
	image := cfg.SampleRunnerImage
	if image == "" {
		image = DefaultSampleRunnerImage
	}
	sbName := TaskSandboxNameForRun(run.Name, taskID)
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

	labels := map[string]string{
		LabelRunName:      run.Name,
		LabelRunNamespace: run.Namespace,
		LabelRole:         RoleSample,
		LabelSandbox:      "true",
	}
	if run.UID != "" {
		labels[LabelRunUID] = string(run.UID)
	}

	annotations := map[string]string{
		AnnotationTaskID: taskID,
	}

	var envFrom []corev1.EnvFromSource
	if cfg.ModelSecret != "" {
		envFrom = append(envFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: cfg.ModelSecret,
				},
			},
		})
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

	return &sandboxv1beta1.Sandbox{
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
								Image:   image,
								EnvFrom: envFrom,
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
										corev1.ResourceCPU:    resource.MustParse("500m"),
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
										corev1.ResourceCPU:    resource.MustParse("500m"),
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
