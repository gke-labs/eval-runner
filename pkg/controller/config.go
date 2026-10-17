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

package controller

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gke-labs/eval-runner/pkg/artifacts/gcs"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// DefaultConfigMapName is the fixed name of the per-cluster configuration ConfigMap.
	DefaultConfigMapName = "eval-runner-config"

	// ConfigMapKeyArtifactBucket is the ConfigMap key for the GCS artifact bucket.
	ConfigMapKeyArtifactBucket = "artifactBucket"
	// ConfigMapKeyArtifactDir is the ConfigMap key for the local artifact directory.
	ConfigMapKeyArtifactDir = "artifactDir"
	// ConfigMapKeyDiscoveryDeadline is the ConfigMap key for discovery deadline.
	ConfigMapKeyDiscoveryDeadline = "discoveryDeadline"
	// ConfigMapKeyExecutionDeadline is the ConfigMap key for sample execution deadline.
	ConfigMapKeyExecutionDeadline = "executionDeadline"
	// ConfigMapKeyMaxParallelTasks is the ConfigMap key for maximum parallel task sandboxes.
	ConfigMapKeyMaxParallelTasks = "maxParallelTasks"
	// ConfigMapKeyModelSecret is the ConfigMap key for default model credentials Secret name.
	ConfigMapKeyModelSecret = "modelSecret"
	// ConfigMapKeySandboxdImage is the ConfigMap key for sandboxd init container image.
	ConfigMapKeySandboxdImage = "sandboxdImage"
	// ConfigMapKeyTaskGracePeriod is the ConfigMap key for task sandbox cache grace period.
	ConfigMapKeyTaskGracePeriod = "taskGracePeriod"
)

// ClusterConfig holds the effective cluster-wide settings resolved with precedence:
// explicit flag > ConfigMap key > built-in default.
type ClusterConfig struct {
	ArtifactBucket    string
	ArtifactDir       string
	DiscoveryDeadline time.Duration
	ExecutionDeadline time.Duration
	MaxParallelTasks  int
	ModelSecret       string
	SandboxdImage     string
	TaskGracePeriod   time.Duration
}

// RawFlags contains the flag values passed to the controller and tracks which flags were explicitly set.
type RawFlags struct {
	ArtifactBucket    string
	ArtifactDir       string
	DiscoveryDeadline time.Duration
	ExecutionDeadline time.Duration
	MaxParallelTasks  int
	ModelSecret       string
	SandboxdImage     string
	TaskGracePeriod   time.Duration
	Explicit          map[string]bool
}

// IsExplicit reports whether a flag by name was explicitly set on the command line.
func (f RawFlags) IsExplicit(name string) bool {
	if f.Explicit == nil {
		return false
	}
	return f.Explicit[name]
}

// DefaultClusterConfig returns a ClusterConfig populated with built-in defaults.
func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		ArtifactBucket:    "",
		ArtifactDir:       "",
		DiscoveryDeadline: DefaultDiscoveryDeadline,
		ExecutionDeadline: DefaultExecutionDeadline,
		MaxParallelTasks:  DefaultMaxParallelTasks,
		ModelSecret:       DefaultModelSecret,
		SandboxdImage:     discovery.SandboxdImage,
		TaskGracePeriod:   DefaultTaskGracePeriod,
	}
}

// FlagsFromFlagSet extracts RawFlags from a parsed flag.FlagSet.
func FlagsFromFlagSet(
	fs *flag.FlagSet,
	artifactBucket, artifactDir, modelSecret, sandboxdImage string,
	discoveryDeadline, executionDeadline, taskGracePeriod time.Duration,
	maxParallelTasks int,
) RawFlags {
	explicit := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		explicit[f.Name] = true
	})
	return RawFlags{
		ArtifactBucket:    artifactBucket,
		ArtifactDir:       artifactDir,
		DiscoveryDeadline: discoveryDeadline,
		ExecutionDeadline: executionDeadline,
		MaxParallelTasks:  maxParallelTasks,
		ModelSecret:       modelSecret,
		SandboxdImage:     sandboxdImage,
		TaskGracePeriod:   taskGracePeriod,
		Explicit:          explicit,
	}
}

// ResolveConfig applies precedence: explicit flag > ConfigMap key > built-in default.
// If the ConfigMap is nil or empty, built-in defaults are used (or explicit flags if set).
// Returns an error if any value in the ConfigMap is malformed.
func ResolveConfig(cm *corev1.ConfigMap, flags RawFlags) (ClusterConfig, error) {
	cfg := DefaultClusterConfig()

	ns := ""
	name := ""
	if cm != nil {
		ns = cm.Namespace
		name = cm.Name
	}

	if cm != nil && cm.Data != nil {
		// 1. artifactBucket
		if val, ok := cm.Data[ConfigMapKeyArtifactBucket]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if err := gcs.ValidateBucket(trimmed); err != nil {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %w", ConfigMapKeyArtifactBucket, val, ns, name, err)
				}
				cfg.ArtifactBucket = trimmed
			} else {
				cfg.ArtifactBucket = ""
			}
		}

		// 1b. artifactDir
		if val, ok := cm.Data[ConfigMapKeyArtifactDir]; ok {
			trimmed := strings.TrimSpace(val)
			cfg.ArtifactDir = trimmed
		}

		// 2. discoveryDeadline
		if val, ok := cm.Data[ConfigMapKeyDiscoveryDeadline]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				d, err := time.ParseDuration(trimmed)
				if err != nil {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %w", ConfigMapKeyDiscoveryDeadline, val, ns, name, err)
				}
				if d <= 0 {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: duration must be positive", ConfigMapKeyDiscoveryDeadline, val, ns, name)
				}
				cfg.DiscoveryDeadline = d
			}
		}

		// 3. executionDeadline
		if val, ok := cm.Data[ConfigMapKeyExecutionDeadline]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				d, err := time.ParseDuration(trimmed)
				if err != nil {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %w", ConfigMapKeyExecutionDeadline, val, ns, name, err)
				}
				if d <= 0 {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: duration must be positive", ConfigMapKeyExecutionDeadline, val, ns, name)
				}
				cfg.ExecutionDeadline = d
			}
		}

		// 4. maxParallelTasks
		if val, ok := cm.Data[ConfigMapKeyMaxParallelTasks]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				n, err := strconv.Atoi(trimmed)
				if err != nil {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %w", ConfigMapKeyMaxParallelTasks, val, ns, name, err)
				}
				if n <= 0 {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must be positive", ConfigMapKeyMaxParallelTasks, val, ns, name)
				}
				cfg.MaxParallelTasks = n
			}
		}

		// 5. modelSecret
		if val, ok := cm.Data[ConfigMapKeyModelSecret]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if errs := validation.IsDNS1123Subdomain(trimmed); len(errs) > 0 {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %s", ConfigMapKeyModelSecret, val, ns, name, strings.Join(errs, "; "))
				}
				cfg.ModelSecret = trimmed
			} else {
				cfg.ModelSecret = ""
			}
		}

		// 6. taskGracePeriod
		if val, ok := cm.Data[ConfigMapKeyTaskGracePeriod]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				d, err := time.ParseDuration(trimmed)
				if err != nil {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %w", ConfigMapKeyTaskGracePeriod, val, ns, name, err)
				}
				if d <= 0 {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: duration must be positive", ConfigMapKeyTaskGracePeriod, val, ns, name)
				}
				cfg.TaskGracePeriod = d
			}
		}

		// 7. sandboxdImage
		if val, ok := cm.Data[ConfigMapKeySandboxdImage]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				cfg.SandboxdImage = trimmed
			}
		}
	}

	// Apply explicit flag overrides
	if flags.IsExplicit("artifact-bucket") {
		cfg.ArtifactBucket = flags.ArtifactBucket
	}
	if flags.IsExplicit("artifact-dir") {
		cfg.ArtifactDir = flags.ArtifactDir
	}
	if flags.IsExplicit("discovery-deadline") {
		cfg.DiscoveryDeadline = flags.DiscoveryDeadline
	}
	if flags.IsExplicit("execution-deadline") {
		cfg.ExecutionDeadline = flags.ExecutionDeadline
	}
	if flags.IsExplicit("max-parallel-tasks") {
		cfg.MaxParallelTasks = flags.MaxParallelTasks
	}
	if flags.IsExplicit("model-secret") {
		cfg.ModelSecret = flags.ModelSecret
	}
	if flags.IsExplicit("sandboxd-image") {
		cfg.SandboxdImage = flags.SandboxdImage
	}
	if flags.IsExplicit("task-grace-period") {
		cfg.TaskGracePeriod = flags.TaskGracePeriod
	}

	return cfg, nil
}

// LoadClusterConfig retrieves the ConfigMap using the uncached reader.
// Returns (nil, nil) if the ConfigMap does not exist.
func LoadClusterConfig(ctx context.Context, reader client.Reader, namespace, name string) (*corev1.ConfigMap, error) {
	if name == "" {
		name = DefaultConfigMapName
	}
	if namespace == "" {
		namespace = discovery.DefaultControllerNamespace
	}

	var cm corev1.ConfigMap
	err := reader.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, &cm)
	if err == nil {
		return &cm, nil
	}
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return nil, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, name, err)
}

// LoadAndResolveClusterConfig reads the configuration ConfigMap using reader (uncached),
// applies precedence (explicit flag > ConfigMap key > built-in default), and logs effective settings.
// Missing ConfigMap is handled gracefully by falling back to built-in defaults.
func LoadAndResolveClusterConfig(
	ctx context.Context,
	reader client.Reader,
	namespace, name string,
	flags RawFlags,
	logger logr.Logger,
) (ClusterConfig, error) {
	cm, err := LoadClusterConfig(ctx, reader, namespace, name)
	if err != nil {
		return ClusterConfig{}, err
	}
	if cm != nil {
		logger.Info("loaded per-cluster configmap", "namespace", cm.Namespace, "name", cm.Name)
	} else {
		logger.Info("per-cluster configmap not found; using defaults", "namespace", namespace, "name", name)
	}

	resolved, err := ResolveConfig(cm, flags)
	if err != nil {
		return ClusterConfig{}, err
	}

	logger.Info("effective cluster settings",
		"artifactBucket", resolved.ArtifactBucket,
		"discoveryDeadline", resolved.DiscoveryDeadline,
		"executionDeadline", resolved.ExecutionDeadline,
		"maxParallelTasks", resolved.MaxParallelTasks,
		"modelSecret", resolved.ModelSecret,
		"sandboxdImage", resolved.SandboxdImage,
		"taskGracePeriod", resolved.TaskGracePeriod,
	)

	return resolved, nil
}
