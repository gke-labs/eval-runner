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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gke-labs/eval-runner/pkg/artifacts/gcs"
	"github.com/gke-labs/eval-runner/pkg/configkeys"
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
	DefaultConfigMapName = configkeys.DefaultConfigMapName

	// ConfigMapKeyArtifactBucket is the ConfigMap key for the GCS artifact bucket.
	ConfigMapKeyArtifactBucket = configkeys.ConfigMapKeyArtifactBucket
	// ConfigMapKeyArtifactDir is the ConfigMap key for the local artifact directory.
	ConfigMapKeyArtifactDir = configkeys.ConfigMapKeyArtifactDir
	// ConfigMapKeyArtifactMaxUploadBytes is the ConfigMap key for the maximum artifact upload byte limit.
	ConfigMapKeyArtifactMaxUploadBytes = configkeys.ConfigMapKeyArtifactMaxUploadBytes
	// ConfigMapKeyDiscoveryDeadline is the ConfigMap key for discovery deadline.
	ConfigMapKeyDiscoveryDeadline = configkeys.ConfigMapKeyDiscoveryDeadline
	// ConfigMapKeyExecutionDeadline is the ConfigMap key for sample execution deadline.
	ConfigMapKeyExecutionDeadline = configkeys.ConfigMapKeyExecutionDeadline
	// ConfigMapKeyMaxParallelTasks is the ConfigMap key for maximum parallel task sandboxes.
	ConfigMapKeyMaxParallelTasks = configkeys.ConfigMapKeyMaxParallelTasks
	// ConfigMapKeyModelSecret is the ConfigMap key for default model credentials Secret name.
	ConfigMapKeyModelSecret = configkeys.ConfigMapKeyModelSecret
	// ConfigMapKeySandboxdImage is the ConfigMap key for sandboxd init container image.
	ConfigMapKeySandboxdImage = configkeys.ConfigMapKeySandboxdImage
	// ConfigMapKeyTaskGracePeriod is the ConfigMap key for task sandbox cache grace period.
	ConfigMapKeyTaskGracePeriod = configkeys.ConfigMapKeyTaskGracePeriod
	// ConfigMapKeyGitHubTokenSecret is the ConfigMap key for GitHub token Secret name.
	ConfigMapKeyGitHubTokenSecret = configkeys.ConfigMapKeyGitHubTokenSecret
	// ConfigMapKeyGitHubAppSecret is the ConfigMap key for GitHub App credentials Secret name.
	ConfigMapKeyGitHubAppSecret = configkeys.ConfigMapKeyGitHubAppSecret
	// ConfigMapKeyUIBaseURL is the ConfigMap key for the eval-runner UI base URL.
	ConfigMapKeyUIBaseURL = configkeys.ConfigMapKeyUIBaseURL
	// ConfigMapKeyInconclusivePolicy is the ConfigMap key for check run inconclusive verdict policy ("neutral" or "failure").
	ConfigMapKeyInconclusivePolicy = configkeys.ConfigMapKeyInconclusivePolicy
	// ConfigMapKeyComparisonTTL is the ConfigMap key for completed comparison TTL duration.
	ConfigMapKeyComparisonTTL = configkeys.ConfigMapKeyComparisonTTL
	// ConfigMapKeyDefaultBatchSize is the ConfigMap key for default batch size.
	ConfigMapKeyDefaultBatchSize = configkeys.ConfigMapKeyDefaultBatchSize
	// ConfigMapKeyDefaultMaxSamplesPerTask is the ConfigMap key for default maximum samples per task.
	ConfigMapKeyDefaultMaxSamplesPerTask = configkeys.ConfigMapKeyDefaultMaxSamplesPerTask
	// ConfigMapKeyCommandUsers is the ConfigMap key for allowed comment command users.
	ConfigMapKeyCommandUsers = configkeys.ConfigMapKeyCommandUsers

	// ConfigSentinelDisabled is the sentinel value used in ConfigMap or CLI flags to explicitly disable
	// a credential or feature that otherwise has a non-empty built-in default.
	ConfigSentinelDisabled = configkeys.ConfigSentinelDisabled

	// DefaultGitHubTokenSecret is the default Secret name for GitHub credentials in controller namespace.
	DefaultGitHubTokenSecret = "github-token"

	// DefaultInconclusivePolicy is the default check-run conclusion policy for inconclusive comparisons.
	DefaultInconclusivePolicy = configkeys.InconclusivePolicyNeutral
)

// KnownConfigMapKeys is the set of all recognized keys in the eval-runner-config ConfigMap,
// covering both eval-runner controller and eval-runner-webhook keys.
var KnownConfigMapKeys = map[string]struct{}{
	configkeys.ConfigMapKeyArtifactBucket:           {},
	configkeys.ConfigMapKeyArtifactDir:              {},
	configkeys.ConfigMapKeyArtifactMaxUploadBytes:   {},
	configkeys.ConfigMapKeyDiscoveryDeadline:        {},
	configkeys.ConfigMapKeyExecutionDeadline:        {},
	configkeys.ConfigMapKeyMaxParallelTasks:         {},
	configkeys.ConfigMapKeyModelSecret:              {},
	configkeys.ConfigMapKeySandboxdImage:            {},
	configkeys.ConfigMapKeyTaskGracePeriod:          {},
	configkeys.ConfigMapKeyGitHubTokenSecret:        {},
	configkeys.ConfigMapKeyGitHubAppSecret:          {},
	configkeys.ConfigMapKeyUIBaseURL:                {},
	configkeys.ConfigMapKeyInconclusivePolicy:       {},
	configkeys.ConfigMapKeyComparisonTTL:            {},
	configkeys.ConfigMapKeyGitHubWebhookSecret:      {},
	configkeys.ConfigMapKeyWebhookRepositories:      {},
	configkeys.ConfigMapKeyDefaultModel:             {},
	configkeys.ConfigMapKeyDefaultLimit:             {},
	configkeys.ConfigMapKeyDefaultBatchSize:         {},
	configkeys.ConfigMapKeyDefaultMaxSamplesPerTask: {},
	configkeys.ConfigMapKeyCommandUsers:             {},
}

// UnknownConfigMapKeys returns a sorted list of unrecognised keys present in cm.Data.
// Returns nil if cm or cm.Data is nil, or if all keys are recognized.
func UnknownConfigMapKeys(cm *corev1.ConfigMap) []string {
	if cm == nil || cm.Data == nil {
		return nil
	}
	var unknown []string
	for k := range cm.Data {
		if _, ok := KnownConfigMapKeys[k]; !ok {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return unknown
}

// ClusterConfig holds the effective cluster-wide settings resolved with precedence:
// explicit flag > ConfigMap key > built-in default.
type ClusterConfig struct {
	ArtifactBucket         string
	ArtifactDir            string
	ArtifactMaxUploadBytes int64
	DiscoveryDeadline      time.Duration
	ExecutionDeadline      time.Duration
	GitHubTokenSecret      string
	GitHubAppSecret        string
	InconclusivePolicy     string
	MaxParallelTasks       int
	ModelSecret            string
	SandboxdImage          string
	TaskGracePeriod        time.Duration
	UIBaseURL              string
	ComparisonTTL          time.Duration
}

// RawFlags contains the flag values passed to the controller and tracks which flags were explicitly set.
type RawFlags struct {
	ArtifactBucket         string
	ArtifactDir            string
	ArtifactMaxUploadBytes int64
	DiscoveryDeadline      time.Duration
	ExecutionDeadline      time.Duration
	GitHubTokenSecret      string
	GitHubAppSecret        string
	InconclusivePolicy     string
	MaxParallelTasks       int
	ModelSecret            string
	SandboxdImage          string
	TaskGracePeriod        time.Duration
	UIBaseURL              string
	ComparisonTTL          time.Duration
	StrictConfig           bool
	Explicit               map[string]bool
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
		ArtifactBucket:         "",
		ArtifactDir:            "",
		ArtifactMaxUploadBytes: gcs.DefaultMaxUploadBytes,
		DiscoveryDeadline:      DefaultDiscoveryDeadline,
		ExecutionDeadline:      DefaultExecutionDeadline,
		GitHubTokenSecret:      DefaultGitHubTokenSecret,
		GitHubAppSecret:        "",
		InconclusivePolicy:     DefaultInconclusivePolicy,
		MaxParallelTasks:       DefaultMaxParallelTasks,
		ModelSecret:            DefaultModelSecret,
		SandboxdImage:          discovery.SandboxdImage,
		TaskGracePeriod:        DefaultTaskGracePeriod,
		UIBaseURL:              "",
		ComparisonTTL:          0,
	}
}

// FlagsFromFlagSet extracts RawFlags from a parsed flag.FlagSet.
func FlagsFromFlagSet(
	fs *flag.FlagSet,
	artifactBucket, artifactDir, modelSecret, sandboxdImage, gitHubTokenSecret, gitHubAppSecret, uiBaseURL string,
	discoveryDeadline, executionDeadline, taskGracePeriod time.Duration,
	maxParallelTasks int,
	strictConfig bool,
	artifactMaxUploadBytes int64,
	inconclusivePolicy string,
	comparisonTTL time.Duration,
) RawFlags {
	explicit := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		explicit[f.Name] = true
	})
	return RawFlags{
		ArtifactBucket:         artifactBucket,
		ArtifactDir:            artifactDir,
		ArtifactMaxUploadBytes: artifactMaxUploadBytes,
		DiscoveryDeadline:      discoveryDeadline,
		ExecutionDeadline:      executionDeadline,
		GitHubTokenSecret:      gitHubTokenSecret,
		GitHubAppSecret:        gitHubAppSecret,
		InconclusivePolicy:     inconclusivePolicy,
		MaxParallelTasks:       maxParallelTasks,
		ModelSecret:            modelSecret,
		SandboxdImage:          sandboxdImage,
		TaskGracePeriod:        taskGracePeriod,
		UIBaseURL:              uiBaseURL,
		ComparisonTTL:          comparisonTTL,
		StrictConfig:           strictConfig,
		Explicit:               explicit,
	}
}

// ResolveConfig applies precedence: explicit flag > ConfigMap key > built-in default.
// If the ConfigMap is nil or empty, built-in defaults are used (or explicit flags if set).
// Returns an error if any value in the ConfigMap is malformed, or if StrictConfig is true
// and unrecognised keys are present in the ConfigMap.
func ResolveConfig(cm *corev1.ConfigMap, flags RawFlags) (ClusterConfig, error) {
	cfg := DefaultClusterConfig()

	ns := ""
	name := ""
	if cm != nil {
		ns = cm.Namespace
		name = cm.Name
	}

	if flags.StrictConfig {
		unknown := UnknownConfigMapKeys(cm)
		if len(unknown) > 0 {
			return cfg, fmt.Errorf("unrecognised keys in ConfigMap %s/%s: %s", ns, name, strings.Join(unknown, ", "))
		}
	}

	if cm != nil && cm.Data != nil {
		// 1. artifactBucket
		if val, ok := cm.Data[ConfigMapKeyArtifactBucket]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.ArtifactBucket = ""
				} else {
					if err := gcs.ValidateBucket(trimmed); err != nil {
						return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %w", ConfigMapKeyArtifactBucket, val, ns, name, err)
					}
					cfg.ArtifactBucket = trimmed
				}
			}
		}

		// 1b. artifactDir
		if val, ok := cm.Data[ConfigMapKeyArtifactDir]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.ArtifactDir = ""
				} else {
					cfg.ArtifactDir = trimmed
				}
			}
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
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.ModelSecret = ""
				} else {
					if errs := validation.IsDNS1123Subdomain(trimmed); len(errs) > 0 {
						return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %s", ConfigMapKeyModelSecret, val, ns, name, strings.Join(errs, "; "))
					}
					cfg.ModelSecret = trimmed
				}
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
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %s cannot be disabled", ConfigMapKeySandboxdImage, val, ns, name, ConfigMapKeySandboxdImage)
				}
				cfg.SandboxdImage = trimmed
			}
		}

		// 8. githubTokenSecret
		if val, ok := cm.Data[ConfigMapKeyGitHubTokenSecret]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.GitHubTokenSecret = ""
				} else {
					if errs := validation.IsDNS1123Subdomain(trimmed); len(errs) > 0 {
						return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %s", ConfigMapKeyGitHubTokenSecret, val, ns, name, strings.Join(errs, ", "))
					}
					cfg.GitHubTokenSecret = trimmed
				}
			}
		}

		// 9. githubAppSecret
		if val, ok := cm.Data[ConfigMapKeyGitHubAppSecret]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.GitHubAppSecret = ""
				} else {
					if errs := validation.IsDNS1123Subdomain(trimmed); len(errs) > 0 {
						return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %s", ConfigMapKeyGitHubAppSecret, val, ns, name, strings.Join(errs, ", "))
					}
					cfg.GitHubAppSecret = trimmed
				}
			}
		}

		// 10. uiBaseURL
		if val, ok := cm.Data[ConfigMapKeyUIBaseURL]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.UIBaseURL = ""
				} else {
					trimmed = strings.TrimRight(trimmed, "/")
					if !strings.HasPrefix(trimmed, "http://") && !strings.HasPrefix(trimmed, "https://") {
						return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must begin with http:// or https://", ConfigMapKeyUIBaseURL, val, ns, name)
					}
					cfg.UIBaseURL = trimmed
				}
			}
		}

		// 11. artifactMaxUploadBytes
		if val, ok := cm.Data[ConfigMapKeyArtifactMaxUploadBytes]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				parsed, err := strconv.ParseInt(trimmed, 10, 64)
				if err != nil || parsed <= 0 {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must be a positive integer", ConfigMapKeyArtifactMaxUploadBytes, val, ns, name)
				}
				cfg.ArtifactMaxUploadBytes = parsed
			}
		}

		// 12. inconclusivePolicy
		if val, ok := cm.Data[ConfigMapKeyInconclusivePolicy]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				lower := strings.ToLower(trimmed)
				if lower != configkeys.InconclusivePolicyNeutral && lower != configkeys.InconclusivePolicyFailure {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must be %q or %q",
						ConfigMapKeyInconclusivePolicy, val, ns, name,
						configkeys.InconclusivePolicyNeutral, configkeys.InconclusivePolicyFailure)
				}
				cfg.InconclusivePolicy = lower
			}
		}

		// 13. comparisonTTL
		if val, ok := cm.Data[ConfigMapKeyComparisonTTL]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.ComparisonTTL = 0
				} else {
					d, err := time.ParseDuration(trimmed)
					if err != nil {
						return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %w", ConfigMapKeyComparisonTTL, val, ns, name, err)
					}
					if d < 0 {
						return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: duration cannot be negative", ConfigMapKeyComparisonTTL, val, ns, name)
					}
					cfg.ComparisonTTL = d
				}
			}
		}
	}

	// Apply explicit flag overrides
	if flags.IsExplicit("artifact-bucket") {
		if strings.EqualFold(flags.ArtifactBucket, ConfigSentinelDisabled) {
			cfg.ArtifactBucket = ""
		} else {
			cfg.ArtifactBucket = flags.ArtifactBucket
		}
	}
	if flags.IsExplicit("artifact-dir") {
		if strings.EqualFold(flags.ArtifactDir, ConfigSentinelDisabled) {
			cfg.ArtifactDir = ""
		} else {
			cfg.ArtifactDir = flags.ArtifactDir
		}
	}
	if flags.IsExplicit("artifact-max-upload-bytes") {
		cfg.ArtifactMaxUploadBytes = flags.ArtifactMaxUploadBytes
	}
	if flags.IsExplicit("comparison-ttl") {
		if flags.ComparisonTTL < 0 {
			return cfg, fmt.Errorf("invalid flag --comparison-ttl: duration cannot be negative")
		}
		cfg.ComparisonTTL = flags.ComparisonTTL
	}
	if flags.IsExplicit("discovery-deadline") {
		cfg.DiscoveryDeadline = flags.DiscoveryDeadline
	}
	if flags.IsExplicit("execution-deadline") {
		cfg.ExecutionDeadline = flags.ExecutionDeadline
	}
	if flags.IsExplicit("github-token-secret") {
		if strings.EqualFold(flags.GitHubTokenSecret, ConfigSentinelDisabled) {
			cfg.GitHubTokenSecret = ""
		} else {
			cfg.GitHubTokenSecret = flags.GitHubTokenSecret
		}
	}
	if flags.IsExplicit("github-app-secret") {
		if strings.EqualFold(flags.GitHubAppSecret, ConfigSentinelDisabled) {
			cfg.GitHubAppSecret = ""
		} else {
			cfg.GitHubAppSecret = flags.GitHubAppSecret
		}
	}
	if flags.IsExplicit("ui-base-url") {
		trimmed := strings.TrimRight(strings.TrimSpace(flags.UIBaseURL), "/")
		if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
			cfg.UIBaseURL = ""
		} else {
			cfg.UIBaseURL = trimmed
		}
	}
	if flags.IsExplicit("max-parallel-tasks") {
		cfg.MaxParallelTasks = flags.MaxParallelTasks
	}
	if flags.IsExplicit("model-secret") {
		if strings.EqualFold(flags.ModelSecret, ConfigSentinelDisabled) {
			cfg.ModelSecret = ""
		} else {
			cfg.ModelSecret = flags.ModelSecret
		}
	}
	if flags.IsExplicit("sandboxd-image") {
		if strings.EqualFold(flags.SandboxdImage, ConfigSentinelDisabled) {
			return cfg, fmt.Errorf("invalid sandboxd-image %q: sandboxd-image cannot be disabled", flags.SandboxdImage)
		}
		cfg.SandboxdImage = flags.SandboxdImage
	}
	if flags.IsExplicit("task-grace-period") {
		cfg.TaskGracePeriod = flags.TaskGracePeriod
	}
	if flags.IsExplicit("inconclusive-policy") {
		lower := strings.ToLower(strings.TrimSpace(flags.InconclusivePolicy))
		if lower != configkeys.InconclusivePolicyNeutral && lower != configkeys.InconclusivePolicyFailure {
			return cfg, fmt.Errorf("invalid --inconclusive-policy %q: must be %q or %q",
				flags.InconclusivePolicy, configkeys.InconclusivePolicyNeutral, configkeys.InconclusivePolicyFailure)
		}
		cfg.InconclusivePolicy = lower
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
// applies precedence (explicit flag > ConfigMap key > built-in default), logs warnings for
// any unrecognised ConfigMap keys, and logs effective settings.
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
		if unknown := UnknownConfigMapKeys(cm); len(unknown) > 0 {
			logger.Info("warning: unrecognised keys in ConfigMap", "unrecognisedKeys", unknown, "namespace", cm.Namespace, "name", cm.Name)
		}
	} else {
		logger.Info("per-cluster configmap not found; using defaults", "namespace", namespace, "name", name)
	}

	resolved, err := ResolveConfig(cm, flags)
	if err != nil {
		return ClusterConfig{}, err
	}

	logger.Info("effective cluster settings",
		"artifactBucket", resolved.ArtifactBucket,
		"comparisonTTL", resolved.ComparisonTTL,
		"discoveryDeadline", resolved.DiscoveryDeadline,
		"executionDeadline", resolved.ExecutionDeadline,
		"githubTokenSecret", resolved.GitHubTokenSecret,
		"maxParallelTasks", resolved.MaxParallelTasks,
		"modelSecret", resolved.ModelSecret,
		"sandboxdImage", resolved.SandboxdImage,
		"taskGracePeriod", resolved.TaskGracePeriod,
	)

	return resolved, nil
}
