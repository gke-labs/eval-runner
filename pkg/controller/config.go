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
	"encoding/json"
	"flag"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/artifacts/gcs"
	"github.com/gke-labs/eval-runner/pkg/artifacts/sinkconfig"
	"github.com/gke-labs/eval-runner/pkg/configkeys"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/forge/consumer"
	"github.com/gke-labs/eval-runner/pkg/forge/syncer"
	"github.com/gke-labs/eval-runner/pkg/github"
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
	// ConfigMapKeyGCPWorkloadIdentityProvider is the ConfigMap key for the GCP Workload Identity Federation provider resource name.
	ConfigMapKeyGCPWorkloadIdentityProvider = configkeys.ConfigMapKeyGCPWorkloadIdentityProvider
	// ConfigMapKeyGCPProjectParentFolderID is the ConfigMap key for the GCP parent folder ID under which per-task projects are created.
	ConfigMapKeyGCPProjectParentFolderID = configkeys.ConfigMapKeyGCPProjectParentFolderID
	// ConfigMapKeyGCPProjectBillingAccountID is the ConfigMap key for the GCP billing account ID linked to per-task projects.
	ConfigMapKeyGCPProjectBillingAccountID = configkeys.ConfigMapKeyGCPProjectBillingAccountID
	// ConfigMapKeyGCPProjectPool is the ConfigMap key for comma-separated pre-created GCP project IDs leased to gcp-project tasks.
	ConfigMapKeyGCPProjectPool = configkeys.ConfigMapKeyGCPProjectPool
	// ConfigMapKeyGCPProjectTaskRoles is the ConfigMap key for comma-separated project-level IAM roles granted to per-project task service accounts.
	ConfigMapKeyGCPProjectTaskRoles = configkeys.ConfigMapKeyGCPProjectTaskRoles
	// ConfigMapKeyDiscoveryDeadline is the ConfigMap key for discovery deadline.
	ConfigMapKeyDiscoveryDeadline = configkeys.ConfigMapKeyDiscoveryDeadline
	// ConfigMapKeyExecutionDeadline is the ConfigMap key for sample execution deadline.
	ConfigMapKeyExecutionDeadline = configkeys.ConfigMapKeyExecutionDeadline
	// ConfigMapKeyMaxParallelTasks is the ConfigMap key for maximum parallel task sandboxes.
	ConfigMapKeyMaxParallelTasks = configkeys.ConfigMapKeyMaxParallelTasks
	// ConfigMapKeyPollProgressInterval is the ConfigMap key for task progress polling interval.
	ConfigMapKeyPollProgressInterval = configkeys.ConfigMapKeyPollProgressInterval
	// ConfigMapKeyModelSecret is the legacy ConfigMap key for default model credentials Secret name.
	// Deprecated: modelSecret is no longer used; the model credential is never attached to pods.
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
	// ConfigMapKeyModelPrices is the ConfigMap key for model pricing table (JSON map model -> {inputPer1M, outputPer1M} in USD).
	ConfigMapKeyModelPrices = configkeys.ConfigMapKeyModelPrices
	// ConfigMapKeyDefaultBatchSize is the ConfigMap key for default batch size.
	ConfigMapKeyDefaultBatchSize = configkeys.ConfigMapKeyDefaultBatchSize
	// ConfigMapKeyDefaultMaxSamplesPerTask is the ConfigMap key for default maximum samples per task.
	ConfigMapKeyDefaultMaxSamplesPerTask = configkeys.ConfigMapKeyDefaultMaxSamplesPerTask
	// ConfigMapKeyCommandUsers is the ConfigMap key for allowed comment command users.
	ConfigMapKeyCommandUsers = configkeys.ConfigMapKeyCommandUsers
	// ConfigMapKeyAffinityMode is the ConfigMap key for changed-file task affinity mode ("order" or "prune").
	ConfigMapKeyAffinityMode = configkeys.ConfigMapKeyAffinityMode
	// ConfigMapKeySandboxEgressHosts is the ConfigMap key for comma-separated allowed FQDN egress hostnames for sandboxes.
	ConfigMapKeySandboxEgressHosts = configkeys.ConfigMapKeySandboxEgressHosts
	// ConfigMapKeyRepositories is the ConfigMap key for tracked repositories.
	ConfigMapKeyRepositories = configkeys.ConfigMapKeyRepositories

	// ConfigMapKeyServicePortalControllerAddress is the ConfigMap key for the Service Portal Controller address.
	ConfigMapKeyServicePortalControllerAddress = configkeys.ConfigMapKeyServicePortalControllerAddress
	// ConfigMapKeyServicePortalControllerCAFile is the ConfigMap key for the Service Portal Controller CA certificate file path.
	ConfigMapKeyServicePortalControllerCAFile = configkeys.ConfigMapKeyServicePortalControllerCAFile
	// ConfigMapKeyServicePortalTokenFile is the ConfigMap key for the Service Portal Controller token file path.
	ConfigMapKeyServicePortalTokenFile = configkeys.ConfigMapKeyServicePortalTokenFile
	// ConfigMapKeyServicePortalAllowedHosts is the ConfigMap key for comma-separated allowed egress hosts for the service portal.
	ConfigMapKeyServicePortalAllowedHosts = configkeys.ConfigMapKeyServicePortalAllowedHosts
	// ConfigMapKeyServicePortalAllowedHTTPHosts is the ConfigMap key for comma-separated allowed cleartext HTTP egress hosts for the service portal.
	ConfigMapKeyServicePortalAllowedHTTPHosts = configkeys.ConfigMapKeyServicePortalAllowedHTTPHosts
	// ConfigMapKeyServicePortalModelBaseURL is the ConfigMap key for an optional custom model API base URL routed through the service portal.
	ConfigMapKeyServicePortalModelBaseURL = configkeys.ConfigMapKeyServicePortalModelBaseURL
	// ConfigMapKeyGoProxyServicePortalEnabled is the ConfigMap key enabling forwarding proxy.golang.org HTTPS traffic to the in-cluster Go proxy service portal.
	ConfigMapKeyGoProxyServicePortalEnabled = configkeys.ConfigMapKeyGoProxyServicePortalEnabled
	// ConfigMapKeyRegistryServicePortalEnabled is the ConfigMap key enabling the in-cluster OCI registry service portal for task sandboxes.
	ConfigMapKeyRegistryServicePortalEnabled = configkeys.ConfigMapKeyRegistryServicePortalEnabled
	// ConfigMapKeyServicePortalsNamespace is the ConfigMap key for the namespace where ecosystem caching service portals run.
	ConfigMapKeyServicePortalsNamespace = configkeys.ConfigMapKeyServicePortalsNamespace
)

const (
	// ConfigSentinelDisabled is the sentinel value used in ConfigMap or CLI flags to explicitly disable
	// a credential or feature that otherwise has a non-empty built-in default.
	ConfigSentinelDisabled = configkeys.ConfigSentinelDisabled

	// DefaultGitHubTokenSecret is the default Secret name for GitHub credentials in controller namespace.
	DefaultGitHubTokenSecret = "github-token"

	// DefaultInconclusivePolicy is the default check-run conclusion policy for inconclusive comparisons.
	DefaultInconclusivePolicy = configkeys.InconclusivePolicyNeutral

	// DefaultHeadTargetCIHalfWidth is the default target 95% CI half-width for head top-ups (0.05).
	DefaultHeadTargetCIHalfWidth = 0.05
)

// DefaultGCPProjectTaskRoles is the default list of project-level IAM roles granted to each
// dynamically created project's eval-task service account.
var DefaultGCPProjectTaskRoles = []string{"roles/editor"}

// KnownConfigMapKeys is the set of all recognized keys in the eval-runner-config ConfigMap,
// covering both eval-runner controller and eval-runner-webhook keys.
var KnownConfigMapKeys = map[string]struct{}{
	configkeys.ConfigMapKeyArtifactBucket:           {},
	configkeys.ConfigMapKeyArtifactDir:              {},
	configkeys.ConfigMapKeyArtifactMaxUploadBytes:   {},
	configkeys.ConfigMapKeyDiscoveryDeadline:        {},
	configkeys.ConfigMapKeyExecutionDeadline:        {},
	configkeys.ConfigMapKeyMaxParallelTasks:         {},
	configkeys.ConfigMapKeyPollProgressInterval:     {},
	configkeys.ConfigMapKeyModelSecret:              {},
	configkeys.ConfigMapKeySandboxdImage:            {},
	configkeys.ConfigMapKeyTaskGracePeriod:          {},
	configkeys.ConfigMapKeyGitHubTokenSecret:        {},
	configkeys.ConfigMapKeyGitHubAppSecret:          {},
	configkeys.ConfigMapKeyUIBaseURL:                {},
	configkeys.ConfigMapKeyInconclusivePolicy:       {},
	configkeys.ConfigMapKeyComparisonTTL:            {},
	configkeys.ConfigMapKeyModelPrices:              {},
	configkeys.ConfigMapKeyGitHubWebhookSecret:      {},
	configkeys.ConfigMapKeyWebhookRepositories:      {},
	configkeys.ConfigMapKeyAllowedCloneHosts:        {},
	configkeys.ConfigMapKeyDefaultModel:             {},
	configkeys.ConfigMapKeyDefaultLimit:             {},
	configkeys.ConfigMapKeyDefaultBatchSize:         {},
	configkeys.ConfigMapKeyDefaultMaxSamplesPerTask: {},
	configkeys.ConfigMapKeyDefaultBudgetUSD:         {},
	configkeys.ConfigMapKeyDefaultCohort:            {},
	configkeys.ConfigMapKeyCommandUsers:             {},
	configkeys.ConfigMapKeyAffinityMode:             {},
	configkeys.ConfigMapKeySandboxEgressHosts:       {},
	configkeys.ConfigMapKeyRepositories:             {},
	configkeys.ConfigMapKeyLsRemoteInterval:         {},
	configkeys.ConfigMapKeyFullReconcileInterval:    {},
	configkeys.ConfigMapKeyRateLimitFloorPercent:    {},
	configkeys.ConfigMapKeyHeadTopUpInterval:        {},
	configkeys.ConfigMapKeyHeadTargetCIHalfWidth:    {},
	configkeys.ConfigMapKeyHeadMaxSamplesPerTask:    {},

	// Service Portal Controller
	configkeys.ConfigMapKeyServicePortalControllerAddress: {},
	configkeys.ConfigMapKeyServicePortalControllerCAFile:  {},
	configkeys.ConfigMapKeyServicePortalTokenFile:         {},
	configkeys.ConfigMapKeyServicePortalAllowedHosts:      {},
	configkeys.ConfigMapKeyServicePortalAllowedHTTPHosts:  {},
	configkeys.ConfigMapKeyServicePortalModelBaseURL:      {},
	configkeys.ConfigMapKeyGoProxyServicePortalEnabled:    {},
	configkeys.ConfigMapKeyRegistryServicePortalEnabled:   {},
	configkeys.ConfigMapKeyServicePortalsNamespace:        {},

	// Workload Identity Federation and ephemeral GCP project creation
	configkeys.ConfigMapKeyGCPWorkloadIdentityProvider: {},
	configkeys.ConfigMapKeyGCPProjectParentFolderID:    {},
	configkeys.ConfigMapKeyGCPProjectBillingAccountID:  {},
	configkeys.ConfigMapKeyGCPProjectPool:              {},
	configkeys.ConfigMapKeyGCPProjectTaskRoles:         {},
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

// ModelPrice specifies token pricing in USD per 1M tokens.
type ModelPrice struct {
	InputPer1M  float64 `json:"inputPer1M"`
	OutputPer1M float64 `json:"outputPer1M"`
}

// ModelConfig holds configuration for model pricing and default model selection.
type ModelConfig struct {
	// Prices maps model identifier to token pricing in USD per 1M tokens.
	Prices map[string]ModelPrice

	// PriceSources maps model or prefix key to pricing provenance ("configmap" or "default").
	PriceSources map[string]v1alpha1.CostSource

	// PricesDisabled reports whether pricing has been explicitly disabled via ConfigMap sentinel.
	PricesDisabled bool

	// DefaultModel is the default model identifier for runs.
	DefaultModel string
}

// DefaultModelConfig returns a ModelConfig populated with built-in defaults.
func DefaultModelConfig() ModelConfig {
	return ModelConfig{
		Prices:         DefaultModelPrices(),
		PriceSources:   DefaultModelPriceSources(),
		PricesDisabled: false,
		DefaultModel:   "",
	}
}

// GitHubConfig holds configuration for GitHub authentication, rate limiting, and PR reporting.
type GitHubConfig struct {
	// TokenSecret is the name of a Secret containing a personal access token for GitHub reporting.
	// Defaults to DefaultGitHubTokenSecret ("github-token").
	TokenSecret string

	// AppSecret is the name of a Secret containing GitHub App credentials for check run reporting.
	AppSecret string

	// InconclusivePolicy is the check run inconclusive verdict policy ("neutral" or "failure").
	// Defaults to DefaultInconclusivePolicy ("neutral").
	InconclusivePolicy string

	// UIBaseURL is the base URL of the eval-runner UI for links in check runs.
	UIBaseURL string

	// CommandUsers is the allow-list of GitHub usernames permitted to execute comment commands without checking GitHub permissions.
	CommandUsers []string

	// RateLimitFloorPercent is the minimum percentage of GitHub rate limit quota remaining before REST API calls are suppressed (default 10).
	RateLimitFloorPercent int
}

// DefaultGitHubConfig returns a GitHubConfig populated with built-in defaults.
func DefaultGitHubConfig() GitHubConfig {
	return GitHubConfig{
		TokenSecret:           DefaultGitHubTokenSecret,
		AppSecret:             "",
		InconclusivePolicy:    DefaultInconclusivePolicy,
		UIBaseURL:             "",
		CommandUsers:          nil,
		RateLimitFloorPercent: syncer.DefaultRateFloorPercent,
	}
}

// ClusterConfig holds the effective cluster-wide settings resolved with precedence:
// explicit flag > ConfigMap key > built-in default.
type ClusterConfig struct {
	ArtifactBucket         string
	ArtifactDir            string
	ArtifactCacheDirectory string
	ArtifactMaxUploadBytes int64

	// DiscoveryDeadline is the overall deadline duration for discovery to complete.
	// Defaults to DefaultDiscoveryDeadline (10m) if <= 0.
	DiscoveryDeadline time.Duration

	// ExecutionDeadline is the overall deadline duration for sample execution.
	// Defaults to DefaultExecutionDeadline (30m) if <= 0.
	ExecutionDeadline time.Duration

	// GitHub holds configuration for GitHub authentication, rate limiting, and PR reporting.
	GitHub GitHubConfig

	// MaxParallelTasks is the maximum number of task sandboxes to run concurrently.
	// Defaults to DefaultMaxParallelTasks (4) if <= 0.
	MaxParallelTasks int

	// Model holds configuration for model pricing and default model selection.
	Model ModelConfig

	// PollProgressInterval overrides the interval between task progress polling attempts (default: 10s).
	PollProgressInterval time.Duration

	// SandboxdImage is the container image used for the sandboxd initContainer.
	SandboxdImage string

	// TaskGracePeriod is the grace period before treating absence of a running task's sandbox as a failure.
	// Defaults to DefaultTaskGracePeriod (30s) if <= 0.
	TaskGracePeriod time.Duration

	ComparisonTTL            time.Duration
	AffinityMode             string
	SandboxEgressHosts       []string
	Repositories             []syncer.RepoConfig
	DefaultCohort            string
	DefaultBatchSize         int
	DefaultMaxSamplesPerTask int
	DefaultBudgetUSD         *float64

	// HeadTopUpInterval is the interval between periodic head top-up Runs.
	HeadTopUpInterval time.Duration

	HeadTargetCIHalfWidth float64
	HeadMaxSamplesPerTask int
	LsRemoteInterval      time.Duration
	FullReconcileInterval time.Duration

	// GCPWorkloadIdentityProvider is the GCP Workload Identity Federation provider resource name.
	GCPWorkloadIdentityProvider string

	// GCPProjectParentFolderID is the GCP parent folder ID under which per-task ephemeral projects are created.
	GCPProjectParentFolderID string

	// GCPProjectBillingAccountID is the GCP billing account ID linked to per-task ephemeral projects.
	GCPProjectBillingAccountID string

	// GCPProjectPool is an optional list of pre-created GCP project IDs leased exclusively to gcp-project tasks.
	GCPProjectPool []string

	// GCPProjectTaskRoles lists project-level IAM roles granted to each project's eval-task service account.
	GCPProjectTaskRoles []string

	// ServicePortal configures settings for the Service Portal Controller and allowed egress hosts.
	ServicePortal ServicePortalOptions
}

// ArtifactConfig returns the sinkconfig.Config representation of the artifact settings.
func (c ClusterConfig) ArtifactConfig() sinkconfig.Config {
	return sinkconfig.Config{
		ArtifactBucket:              c.ArtifactBucket,
		ArtifactDir:                 c.ArtifactDir,
		ArtifactCacheDirectory:      c.ArtifactCacheDirectory,
		GCPWorkloadIdentityProvider: c.GCPWorkloadIdentityProvider,
		ArtifactMaxUploadBytes:      c.ArtifactMaxUploadBytes,
	}
}

// RawFlags contains the flag values passed to the controller and tracks which flags were explicitly set.
type RawFlags struct {
	ArtifactBucket                 string
	ArtifactDir                    string
	ArtifactCacheDirectory         string
	ArtifactMaxUploadBytes         int64
	DiscoveryDeadline              time.Duration
	ExecutionDeadline              time.Duration
	GitHubTokenSecret              string
	GitHubAppSecret                string
	InconclusivePolicy             string
	MaxParallelTasks               int
	PollProgressInterval           time.Duration
	SandboxdImage                  string
	TaskGracePeriod                time.Duration
	UIBaseURL                      string
	ComparisonTTL                  time.Duration
	CommandUsers                   string
	AffinityMode                   string
	SandboxEgressHosts             string
	GCPProjectParentFolderID       string
	GCPProjectBillingAccountID     string
	GCPProjectPool                 string
	GCPProjectTaskRoles            string
	ServicePortalControllerAddress string
	ServicePortalCAFile            string
	ServicePortalTokenFile         string
	StrictConfig                   bool
	Explicit                       map[string]bool
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
		ArtifactBucket:           "",
		ArtifactDir:              "",
		ArtifactMaxUploadBytes:   gcs.DefaultMaxUploadBytes,
		DiscoveryDeadline:        DefaultDiscoveryDeadline,
		ExecutionDeadline:        DefaultExecutionDeadline,
		GitHub:                   DefaultGitHubConfig(),
		MaxParallelTasks:         DefaultMaxParallelTasks,
		Model:                    DefaultModelConfig(),
		PollProgressInterval:     DefaultPollProgressInterval,
		SandboxdImage:            discovery.SandboxdImage,
		TaskGracePeriod:          DefaultTaskGracePeriod,
		ComparisonTTL:            0,
		AffinityMode:             configkeys.AffinityModeOrder,
		SandboxEgressHosts:       discovery.CloneSandboxEgressHosts(discovery.DefaultSandboxEgressHosts),
		DefaultCohort:            "default",
		DefaultBatchSize:         8,
		DefaultMaxSamplesPerTask: 64,
		HeadTopUpInterval:        0,
		HeadTargetCIHalfWidth:    DefaultHeadTargetCIHalfWidth,
		HeadMaxSamplesPerTask:    64,
		LsRemoteInterval:         syncer.DefaultLsRemoteInterval,
		FullReconcileInterval:    syncer.DefaultFullReconcileInterval,

		GCPWorkloadIdentityProvider: "",
		GCPProjectTaskRoles:         append([]string(nil), DefaultGCPProjectTaskRoles...),
		ServicePortal: ServicePortalOptions{
			ControllerAddress:       "",
			CAFile:                  "",
			TokenFile:               "",
			AllowedHosts:            append([]string(nil), DefaultServicePortalAllowedHosts...),
			AllowedHTTPHosts:        append([]string(nil), DefaultServicePortalAllowedHTTPHosts...),
			ServicePortalsNamespace: DefaultServicePortalsNamespace,
		},
	}
}

// FlagsFromFlagSet extracts RawFlags from a parsed flag.FlagSet.
func FlagsFromFlagSet(
	fs *flag.FlagSet,
	artifactBucket, artifactDir, artifactCacheDirectory, sandboxdImage, gitHubTokenSecret, gitHubAppSecret, uiBaseURL string,
	discoveryDeadline, executionDeadline, taskGracePeriod time.Duration,
	maxParallelTasks int,
	strictConfig bool,
	artifactMaxUploadBytes int64,
	inconclusivePolicy string,
	comparisonTTL time.Duration,
	commandUsers string,
	pollProgressInterval time.Duration,
	affinityMode string,
	sandboxEgressHosts string,
	gcpProjectParentFolderID string,
	gcpProjectBillingAccountID string,
	gcpProjectPool string,
	gcpProjectTaskRoles string,
	servicePortalControllerAddress, servicePortalCAFile, servicePortalTokenFile string,
) RawFlags {
	explicit := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		explicit[f.Name] = true
	})
	return RawFlags{
		ArtifactBucket:                 artifactBucket,
		ArtifactDir:                    artifactDir,
		ArtifactCacheDirectory:         artifactCacheDirectory,
		ArtifactMaxUploadBytes:         artifactMaxUploadBytes,
		DiscoveryDeadline:              discoveryDeadline,
		ExecutionDeadline:              executionDeadline,
		GitHubTokenSecret:              gitHubTokenSecret,
		GitHubAppSecret:                gitHubAppSecret,
		InconclusivePolicy:             inconclusivePolicy,
		MaxParallelTasks:               maxParallelTasks,
		PollProgressInterval:           pollProgressInterval,
		SandboxdImage:                  sandboxdImage,
		TaskGracePeriod:                taskGracePeriod,
		UIBaseURL:                      uiBaseURL,
		ComparisonTTL:                  comparisonTTL,
		CommandUsers:                   commandUsers,
		AffinityMode:                   affinityMode,
		SandboxEgressHosts:             sandboxEgressHosts,
		GCPProjectParentFolderID:       gcpProjectParentFolderID,
		GCPProjectBillingAccountID:     gcpProjectBillingAccountID,
		GCPProjectPool:                 gcpProjectPool,
		GCPProjectTaskRoles:            gcpProjectTaskRoles,
		ServicePortalControllerAddress: servicePortalControllerAddress,
		ServicePortalCAFile:            servicePortalCAFile,
		ServicePortalTokenFile:         servicePortalTokenFile,
		StrictConfig:                   strictConfig,
		Explicit:                       explicit,
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

		// 1c. gcpWorkloadIdentityProvider
		if val, ok := cm.Data[ConfigMapKeyGCPWorkloadIdentityProvider]; ok {
			cfg.GCPWorkloadIdentityProvider = strings.TrimSpace(val)
		}

		// 1d. gcpProjectParentFolderID
		if value, ok := cm.Data[ConfigMapKeyGCPProjectParentFolderID]; ok {
			trimmed := strings.TrimSpace(value)
			if trimmed == "" || strings.EqualFold(trimmed, ConfigSentinelDisabled) {
				cfg.GCPProjectParentFolderID = ""
			} else {
				cfg.GCPProjectParentFolderID = trimmed
			}
		}

		// 1e. gcpProjectBillingAccountID
		if value, ok := cm.Data[ConfigMapKeyGCPProjectBillingAccountID]; ok {
			trimmed := strings.TrimSpace(value)
			if trimmed == "" || strings.EqualFold(trimmed, ConfigSentinelDisabled) {
				cfg.GCPProjectBillingAccountID = ""
			} else {
				cfg.GCPProjectBillingAccountID = trimmed
			}
		}

		// 1f. gcpProjectPool
		if value, ok := cm.Data[ConfigMapKeyGCPProjectPool]; ok {
			cfg.GCPProjectPool = parseCommaSeparatedList(value)
		}

		// 1g. gcpProjectTaskRoles
		if value, ok := cm.Data[ConfigMapKeyGCPProjectTaskRoles]; ok {
			trimmed := strings.TrimSpace(value)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.GCPProjectTaskRoles = nil
				} else if roles := parseCommaSeparatedList(value); len(roles) > 0 {
					cfg.GCPProjectTaskRoles = roles
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

		// pollProgressInterval
		if val, ok := cm.Data[ConfigMapKeyPollProgressInterval]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				d, err := time.ParseDuration(trimmed)
				if err != nil {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %w", ConfigMapKeyPollProgressInterval, val, ns, name, err)
				}
				if d < time.Second {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must be at least 1s", ConfigMapKeyPollProgressInterval, val, ns, name)
				}
				cfg.PollProgressInterval = d
			}
		}

		// 5. modelSecret is deprecated and ignored; model credentials are never attached to pods.

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
					cfg.GitHub.TokenSecret = ""
				} else {
					if errs := validation.IsDNS1123Subdomain(trimmed); len(errs) > 0 {
						return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %s", ConfigMapKeyGitHubTokenSecret, val, ns, name, strings.Join(errs, ", "))
					}
					cfg.GitHub.TokenSecret = trimmed
				}
			}
		}

		// 9. githubAppSecret
		if val, ok := cm.Data[ConfigMapKeyGitHubAppSecret]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.GitHub.AppSecret = ""
				} else {
					if errs := validation.IsDNS1123Subdomain(trimmed); len(errs) > 0 {
						return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %s", ConfigMapKeyGitHubAppSecret, val, ns, name, strings.Join(errs, ", "))
					}
					cfg.GitHub.AppSecret = trimmed
				}
			}
		}

		// 10. uiBaseURL
		if val, ok := cm.Data[ConfigMapKeyUIBaseURL]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.GitHub.UIBaseURL = ""
				} else {
					trimmed = strings.TrimRight(trimmed, "/")
					if !strings.HasPrefix(trimmed, "http://") && !strings.HasPrefix(trimmed, "https://") {
						return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must begin with http:// or https://", ConfigMapKeyUIBaseURL, val, ns, name)
					}
					cfg.GitHub.UIBaseURL = trimmed
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
				cfg.GitHub.InconclusivePolicy = lower
			}
		}

		// 13. modelPrices
		if val, ok := cm.Data[ConfigMapKeyModelPrices]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.Model.Prices = nil
					cfg.Model.PriceSources = nil
					cfg.Model.PricesDisabled = true
				} else {
					var prices map[string]ModelPrice
					dec := json.NewDecoder(strings.NewReader(trimmed))
					dec.DisallowUnknownFields()
					if err := dec.Decode(&prices); err != nil {
						return cfg, fmt.Errorf("invalid %s in ConfigMap %s/%s: %w", ConfigMapKeyModelPrices, ns, name, err)
					}
					for model, p := range prices {
						if p.InputPer1M < 0 || p.OutputPer1M < 0 {
							return cfg, fmt.Errorf("invalid %s in ConfigMap %s/%s: price for %q cannot be negative", ConfigMapKeyModelPrices, ns, name, model)
						}
						if strings.HasPrefix(model, "prefix:") && strings.TrimPrefix(model, "prefix:") == "" {
							return cfg, fmt.Errorf("invalid %s in ConfigMap %s/%s: prefix for %q cannot be empty", ConfigMapKeyModelPrices, ns, name, model)
						}
					}
					if cfg.Model.Prices == nil {
						cfg.Model.Prices = DefaultModelPrices()
						cfg.Model.PriceSources = DefaultModelPriceSources()
					}
					for model, p := range prices {
						cfg.Model.Prices[model] = p
						cfg.Model.PriceSources[model] = v1alpha1.CostSourceConfigMap
					}
					cfg.Model.PricesDisabled = false
				}
			}
		}

		// 14. comparisonTTL
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

		// 14. commandUsers
		if val, ok := cm.Data[ConfigMapKeyCommandUsers]; ok {
			cfg.GitHub.CommandUsers = ParseUserList(val)
		}

		// 15. affinityMode
		if val, ok := cm.Data[ConfigMapKeyAffinityMode]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				lower := strings.ToLower(trimmed)
				if lower != configkeys.AffinityModeOrder && lower != configkeys.AffinityModePrune {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must be %q or %q",
						ConfigMapKeyAffinityMode, val, ns, name,
						configkeys.AffinityModeOrder, configkeys.AffinityModePrune)
				}
				cfg.AffinityMode = lower
			}
		}

		// 16. sandboxEgressHosts
		if val, ok := cm.Data[ConfigMapKeySandboxEgressHosts]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.SandboxEgressHosts = nil
				} else {
					cfg.SandboxEgressHosts = discovery.ParseHostList(val)
				}
			}
		}

		// 17. repositories
		if val, ok := cm.Data[ConfigMapKeyRepositories]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				repos, err := syncer.ParseRepoConfigList(val)
				if err != nil {
					return cfg, fmt.Errorf("invalid %s in ConfigMap %s/%s: %w", ConfigMapKeyRepositories, ns, name, err)
				}
				cfg.Repositories = repos
			}
		}

		// Defaults for syncer/consumer
		if val, ok := cm.Data[configkeys.ConfigMapKeyDefaultModel]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				cfg.Model.DefaultModel = trimmed
			}
		}
		if val, ok := cm.Data[configkeys.ConfigMapKeyDefaultCohort]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				cfg.DefaultCohort = trimmed
			}
		}
		if val, ok := cm.Data[configkeys.ConfigMapKeyDefaultBatchSize]; ok {
			if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && n > 0 {
				cfg.DefaultBatchSize = n
			}
		}
		if val, ok := cm.Data[configkeys.ConfigMapKeyDefaultMaxSamplesPerTask]; ok {
			if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && n > 0 {
				cfg.DefaultMaxSamplesPerTask = n
			}
		}
		if val, ok := cm.Data[configkeys.ConfigMapKeyDefaultBudgetUSD]; ok {
			if f, err := strconv.ParseFloat(strings.TrimSpace(val), 64); err == nil && f >= 0 {
				cfg.DefaultBudgetUSD = &f
			}
		}

		// 18. lsRemoteInterval
		if val, ok := cm.Data[configkeys.ConfigMapKeyLsRemoteInterval]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				d, err := time.ParseDuration(trimmed)
				if err != nil {
					return cfg, fmt.Errorf("invalid %s in ConfigMap %s/%s: %w", configkeys.ConfigMapKeyLsRemoteInterval, ns, name, err)
				}
				cfg.LsRemoteInterval = d
			}
		}

		// 19. fullReconcileInterval
		if val, ok := cm.Data[configkeys.ConfigMapKeyFullReconcileInterval]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				d, err := time.ParseDuration(trimmed)
				if err != nil {
					return cfg, fmt.Errorf("invalid %s in ConfigMap %s/%s: %w", configkeys.ConfigMapKeyFullReconcileInterval, ns, name, err)
				}
				cfg.FullReconcileInterval = d
			}
		}

		// 21. rateLimitFloorPercent
		if val, ok := cm.Data[configkeys.ConfigMapKeyRateLimitFloorPercent]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				pct, err := strconv.Atoi(trimmed)
				if err != nil || pct <= 0 {
					return cfg, fmt.Errorf("invalid %s in ConfigMap %s/%s: must be a positive integer", configkeys.ConfigMapKeyRateLimitFloorPercent, ns, name)
				}
				cfg.GitHub.RateLimitFloorPercent = pct
			}
		}

		// 22. headTopUpInterval
		if val, ok := cm.Data[configkeys.ConfigMapKeyHeadTopUpInterval]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) || trimmed == "0" || trimmed == "0s" {
					cfg.HeadTopUpInterval = 0
				} else {
					d, err := time.ParseDuration(trimmed)
					if err != nil {
						return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %w", configkeys.ConfigMapKeyHeadTopUpInterval, val, ns, name, err)
					}
					if d < 0 {
						return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: duration cannot be negative", configkeys.ConfigMapKeyHeadTopUpInterval, val, ns, name)
					}
					cfg.HeadTopUpInterval = d
				}
			}
		}

		// 23. headTargetCIHalfWidth
		if val, ok := cm.Data[configkeys.ConfigMapKeyHeadTargetCIHalfWidth]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				f, err := strconv.ParseFloat(trimmed, 64)
				if err != nil || f <= 0 || f >= 1 {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must be a float between 0 and 1", configkeys.ConfigMapKeyHeadTargetCIHalfWidth, val, ns, name)
				}
				cfg.HeadTargetCIHalfWidth = f
			}
		}

		// 24. headMaxSamplesPerTask
		if val, ok := cm.Data[configkeys.ConfigMapKeyHeadMaxSamplesPerTask]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				n, err := strconv.Atoi(trimmed)
				if err != nil || n <= 0 {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must be a positive integer", configkeys.ConfigMapKeyHeadMaxSamplesPerTask, val, ns, name)
				}
				cfg.HeadMaxSamplesPerTask = n
			}
		} else if cfg.DefaultMaxSamplesPerTask > 0 {
			cfg.HeadMaxSamplesPerTask = cfg.DefaultMaxSamplesPerTask
		}

		// Service Portal Controller settings
		if val, ok := cm.Data[ConfigMapKeyServicePortalControllerAddress]; ok {
			cfg.ServicePortal.ControllerAddress = strings.TrimSpace(val)
		}
		if val, ok := cm.Data[ConfigMapKeyServicePortalControllerCAFile]; ok {
			cfg.ServicePortal.CAFile = strings.TrimSpace(val)
		}
		if val, ok := cm.Data[ConfigMapKeyServicePortalTokenFile]; ok {
			cfg.ServicePortal.TokenFile = strings.TrimSpace(val)
		}
		if val, ok := cm.Data[ConfigMapKeyServicePortalAllowedHosts]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, "disabled") || strings.EqualFold(trimmed, ConfigSentinelDisabled) || strings.EqualFold(trimmed, "none") {
					cfg.ServicePortal.AllowedHosts = nil
				} else {
					cfg.ServicePortal.AllowedHosts = discovery.ParseHostList(val)
				}
			}
		}
		if val, ok := cm.Data[ConfigMapKeyServicePortalAllowedHTTPHosts]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, "disabled") || strings.EqualFold(trimmed, ConfigSentinelDisabled) || strings.EqualFold(trimmed, "none") {
					cfg.ServicePortal.AllowedHTTPHosts = nil
				} else {
					cfg.ServicePortal.AllowedHTTPHosts = discovery.ParseHostList(val)
				}
			}
		}
		if val, ok := cm.Data[ConfigMapKeyServicePortalModelBaseURL]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed == "" || strings.EqualFold(trimmed, "disabled") || strings.EqualFold(trimmed, ConfigSentinelDisabled) || strings.EqualFold(trimmed, "none") {
				cfg.ServicePortal.ModelBaseURL = ""
			} else {
				cfg.ServicePortal.ModelBaseURL = trimmed
			}
		}
		if value, ok := cm.Data[ConfigMapKeyGoProxyServicePortalEnabled]; ok {
			trimmed := strings.TrimSpace(value)
			if trimmed != "" {
				enabled, err := strconv.ParseBool(trimmed)
				if err != nil {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %w", ConfigMapKeyGoProxyServicePortalEnabled, value, ns, name, err)
				}
				cfg.ServicePortal.GoProxyServicePortalEnabled = enabled
			}
		}
		if value, ok := cm.Data[ConfigMapKeyRegistryServicePortalEnabled]; ok {
			trimmed := strings.TrimSpace(value)
			if trimmed != "" {
				enabled, err := strconv.ParseBool(trimmed)
				if err != nil {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %w", ConfigMapKeyRegistryServicePortalEnabled, value, ns, name, err)
				}
				cfg.ServicePortal.RegistryServicePortalEnabled = enabled
			}
		}
		if value, ok := cm.Data[ConfigMapKeyServicePortalsNamespace]; ok {
			trimmed := strings.TrimSpace(value)
			if trimmed != "" {
				if errs := validation.IsDNS1123Label(trimmed); len(errs) > 0 {
					return cfg, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: %s", ConfigMapKeyServicePortalsNamespace, value, ns, name, strings.Join(errs, ", "))
				}
				cfg.ServicePortal.ServicePortalsNamespace = trimmed
			}
		}
	}

	// Apply explicit flag overrides
	if flags.IsExplicit("affinity-mode") {
		trimmed := strings.TrimSpace(flags.AffinityMode)
		lower := strings.ToLower(trimmed)
		if lower != configkeys.AffinityModeOrder && lower != configkeys.AffinityModePrune {
			return cfg, fmt.Errorf("invalid flag --affinity-mode %q: must be %q or %q",
				flags.AffinityMode, configkeys.AffinityModeOrder, configkeys.AffinityModePrune)
		}
		cfg.AffinityMode = lower
	}
	if flags.IsExplicit("sandbox-egress-hosts") {
		trimmed := strings.TrimSpace(flags.SandboxEgressHosts)
		if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
			cfg.SandboxEgressHosts = nil
		} else {
			cfg.SandboxEgressHosts = discovery.ParseHostList(flags.SandboxEgressHosts)
		}
	}
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
	cfg.ArtifactCacheDirectory = flags.ArtifactCacheDirectory
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
			cfg.GitHub.TokenSecret = ""
		} else {
			cfg.GitHub.TokenSecret = flags.GitHubTokenSecret
		}
	}
	if flags.IsExplicit("github-app-secret") {
		if strings.EqualFold(flags.GitHubAppSecret, ConfigSentinelDisabled) {
			cfg.GitHub.AppSecret = ""
		} else {
			cfg.GitHub.AppSecret = flags.GitHubAppSecret
		}
	}
	if flags.IsExplicit("ui-base-url") {
		trimmed := strings.TrimRight(strings.TrimSpace(flags.UIBaseURL), "/")
		if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
			cfg.GitHub.UIBaseURL = ""
		} else {
			cfg.GitHub.UIBaseURL = trimmed
		}
	}
	if flags.IsExplicit("max-parallel-tasks") {
		cfg.MaxParallelTasks = flags.MaxParallelTasks
	}
	if flags.IsExplicit("poll-progress-interval") {
		if flags.PollProgressInterval < time.Second {
			return cfg, fmt.Errorf("invalid flag --poll-progress-interval: must be at least 1s")
		}
		cfg.PollProgressInterval = flags.PollProgressInterval
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
		cfg.GitHub.InconclusivePolicy = lower
	}
	if flags.IsExplicit("command-users") {
		cfg.GitHub.CommandUsers = ParseUserList(flags.CommandUsers)
	}
	if flags.IsExplicit("gcp-project-parent-folder-id") {
		trimmed := strings.TrimSpace(flags.GCPProjectParentFolderID)
		if trimmed == "" || strings.EqualFold(trimmed, ConfigSentinelDisabled) {
			cfg.GCPProjectParentFolderID = ""
		} else {
			cfg.GCPProjectParentFolderID = trimmed
		}
	}
	if flags.IsExplicit("gcp-project-billing-account-id") {
		trimmed := strings.TrimSpace(flags.GCPProjectBillingAccountID)
		if trimmed == "" || strings.EqualFold(trimmed, ConfigSentinelDisabled) {
			cfg.GCPProjectBillingAccountID = ""
		} else {
			cfg.GCPProjectBillingAccountID = trimmed
		}
	}
	if flags.IsExplicit("gcp-project-pool") {
		cfg.GCPProjectPool = parseCommaSeparatedList(flags.GCPProjectPool)
	}
	if flags.IsExplicit("gcp-project-task-roles") {
		cfg.GCPProjectTaskRoles = parseCommaSeparatedList(flags.GCPProjectTaskRoles)
	}
	if flags.IsExplicit("service-portal-controller-address") {
		cfg.ServicePortal.ControllerAddress = strings.TrimSpace(flags.ServicePortalControllerAddress)
	}
	if flags.IsExplicit("service-portal-controller-ca-file") {
		cfg.ServicePortal.CAFile = strings.TrimSpace(flags.ServicePortalCAFile)
	}
	if flags.IsExplicit("service-portal-token-file") {
		cfg.ServicePortal.TokenFile = strings.TrimSpace(flags.ServicePortalTokenFile)
	}

	if err := cfg.ServicePortal.Validate(); err != nil {
		return cfg, err
	}

	return cfg, nil
}

// parseCommaSeparatedList parses a comma-separated list, trimming whitespace,
// dropping empty entries, returning nil on empty/sentinel values, and deduplicating in order.
func parseCommaSeparatedList(rawValue string) []string {
	trimmed := strings.TrimSpace(rawValue)
	if trimmed == "" || strings.EqualFold(trimmed, ConfigSentinelDisabled) {
		return nil
	}
	var items []string
	seen := make(map[string]struct{})
	for _, part := range strings.Split(trimmed, ",") {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		if _, duplicate := seen[item]; duplicate {
			continue
		}
		seen[item] = struct{}{}
		items = append(items, item)
	}
	return items
}

// ParseUserList parses a comma-separated list of GitHub usernames.
func ParseUserList(s string) []string {
	var users []string
	for _, part := range strings.Split(s, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			users = append(users, trimmed)
		}
	}
	return users
}

// IsUserAllowed checks whether user is in allowList (case-insensitive exact match).
// Returns false if allowList is empty or user is empty. Wildcards are not supported.
func IsUserAllowed(user string, allowList []string) bool {
	if len(allowList) == 0 {
		return false
	}
	normalized := strings.ToLower(strings.TrimSpace(user))
	if normalized == "" {
		return false
	}
	for _, allowed := range allowList {
		trimmed := strings.ToLower(strings.TrimSpace(allowed))
		if trimmed == normalized {
			return true
		}
	}
	return false
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
		if _, ok := cm.Data[ConfigMapKeyModelSecret]; ok {
			logger.Info("modelSecret is no longer used; the model credential is never attached to pods")
		}
	} else {
		logger.Info("per-cluster configmap not found; using defaults", "namespace", namespace, "name", name)
	}

	resolved, err := ResolveConfig(cm, flags)
	if err != nil {
		return ClusterConfig{}, err
	}

	LogEffectiveClusterSettings(logger, resolved)

	return resolved, nil
}

// LogEffectiveClusterSettings logs the effective cluster settings in a standardized one-line format.
func LogEffectiveClusterSettings(logger logr.Logger, resolved ClusterConfig) {
	logger.Info("effective cluster settings",
		"artifactBucket", resolved.ArtifactBucket,
		"artifactDir", resolved.ArtifactDir,
		"comparisonTTL", resolved.ComparisonTTL,
		"discoveryDeadline", resolved.DiscoveryDeadline,
		"executionDeadline", resolved.ExecutionDeadline,
		"githubTokenSecret", resolved.GitHub.TokenSecret,
		"maxParallelTasks", resolved.MaxParallelTasks,
		"pollProgressInterval", resolved.PollProgressInterval,
		"sandboxdImage", resolved.SandboxdImage,
		"taskGracePeriod", resolved.TaskGracePeriod,
		"sandboxEgressHosts", strings.Join(resolved.SandboxEgressHosts, ","),
		"gcpProjectParentFolderID", resolved.GCPProjectParentFolderID,
		"gcpProjectBillingAccountID", resolved.GCPProjectBillingAccountID,
		"gcpProjectPool", strings.Join(resolved.GCPProjectPool, ","),
		"gcpProjectTaskRoles", strings.Join(resolved.GCPProjectTaskRoles, ","),
		"servicePortalModelBaseURL", resolved.ServicePortal.ModelBaseURL,
		"goproxyServicePortalEnabled", resolved.ServicePortal.GoProxyServicePortalEnabled,
		"registryServicePortalEnabled", resolved.ServicePortal.RegistryServicePortalEnabled,
		"servicePortalsNamespace", resolved.ServicePortal.ServicePortalsNamespace,
		"headTopUpInterval", resolved.HeadTopUpInterval,
		"headTargetCIHalfWidth", resolved.HeadTargetCIHalfWidth,
		"headMaxSamplesPerTask", resolved.HeadMaxSamplesPerTask,
	)
}

// ConfigReloaderOptions configures a ConfigReloader for runtime cluster configuration updates.
type ConfigReloaderOptions struct {
	RawFlags                      RawFlags
	Logger                        logr.Logger
	SinkFactory                   sinkconfig.SinkFactory
	ReloadableSink                *artifacts.ReloadableSink
	RunReconciler                 *RunReconciler
	CompReconciler                *ComparisonReconciler
	Syncer                        *syncer.Syncer
	Consumer                      *consumer.Consumer
	TopUpRunner                   *HeadTopUpRunner
	InitialConfig                 *ClusterConfig
	FQDNSyncFunc                  func(ctx context.Context, hosts []string) error
	RegistryServicePortalSyncFunc func(ctx context.Context, enabled bool, servicePortalsNamespace string) error
	GitHubClient                  *github.HTTPClient
}

// ConfigReloader dynamically applies eval-runner-config updates at runtime.
type ConfigReloader struct {
	mu                            sync.Mutex
	rawFlags                      RawFlags
	logger                        logr.Logger
	sinkFactory                   sinkconfig.SinkFactory
	reloadableSink                *artifacts.ReloadableSink
	runReconciler                 *RunReconciler
	compReconciler                *ComparisonReconciler
	syncer                        *syncer.Syncer
	consumer                      *consumer.Consumer
	topUpRunner                   *HeadTopUpRunner
	fqdnSyncFunc                  func(ctx context.Context, hosts []string) error
	registryServicePortalSyncFunc func(ctx context.Context, enabled bool, servicePortalsNamespace string) error
	gitHubClient                  *github.HTTPClient
	lastConfig                    *ClusterConfig
}

// NewConfigReloader constructs a new ConfigReloader.
func NewConfigReloader(opts ConfigReloaderOptions) *ConfigReloader {
	sf := opts.SinkFactory
	if sf == nil {
		sf = sinkconfig.NewArtifactSink
	}
	r := &ConfigReloader{
		rawFlags:                      opts.RawFlags,
		logger:                        opts.Logger,
		sinkFactory:                   sf,
		reloadableSink:                opts.ReloadableSink,
		runReconciler:                 opts.RunReconciler,
		compReconciler:                opts.CompReconciler,
		syncer:                        opts.Syncer,
		consumer:                      opts.Consumer,
		topUpRunner:                   opts.TopUpRunner,
		fqdnSyncFunc:                  opts.FQDNSyncFunc,
		registryServicePortalSyncFunc: opts.RegistryServicePortalSyncFunc,
		gitHubClient:                  opts.GitHubClient,
	}
	if opts.InitialConfig != nil {
		copied := *opts.InitialConfig
		r.lastConfig = &copied
	}
	return r
}

// Reload re-resolves and applies configuration from the provided ConfigMap (or nil on deletion).
// Swaps the artifact sink (closing the old sink after in-flight uploads finish) and updates reconcilers.
func (r *ConfigReloader) Reload(ctx context.Context, cm *corev1.ConfigMap) error {
	return r.reloadInternal(ctx, cm, false)
}

// ReloadWait performs Reload and synchronously waits for any retired artifact sink to finish closing.
// This is primarily useful in unit tests.
func (r *ConfigReloader) ReloadWait(ctx context.Context, cm *corev1.ConfigMap) error {
	return r.reloadInternal(ctx, cm, true)
}

func (r *ConfigReloader) reloadInternal(ctx context.Context, cm *corev1.ConfigMap, wait bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if cm != nil {
		if unknown := UnknownConfigMapKeys(cm); len(unknown) > 0 {
			r.logger.Info("warning: unrecognised keys in ConfigMap", "unrecognisedKeys", unknown, "namespace", cm.Namespace, "name", cm.Name)
		}
	}

	resolved, err := ResolveConfig(cm, r.rawFlags)
	if err != nil {
		r.logger.Error(err, "failed to resolve cluster configuration; retaining active settings")
		return err
	}

	if r.lastConfig != nil && reflect.DeepEqual(*r.lastConfig, resolved) {
		return nil
	}

	sinkChanged := r.lastConfig == nil ||
		r.lastConfig.ArtifactBucket != resolved.ArtifactBucket ||
		r.lastConfig.ArtifactDir != resolved.ArtifactDir ||
		r.lastConfig.ArtifactMaxUploadBytes != resolved.ArtifactMaxUploadBytes ||
		r.lastConfig.GCPWorkloadIdentityProvider != resolved.GCPWorkloadIdentityProvider

	if sinkChanged && r.reloadableSink != nil {
		newSink, err := r.sinkFactory(ctx, resolved.ArtifactConfig())
		if err != nil {
			r.logger.Error(err, "unable to configure artifact sink; retaining active sink", "bucket", resolved.ArtifactBucket, "dir", resolved.ArtifactDir)
			if r.lastConfig != nil {
				resolved.ArtifactBucket = r.lastConfig.ArtifactBucket
				resolved.ArtifactDir = r.lastConfig.ArtifactDir
				resolved.ArtifactMaxUploadBytes = r.lastConfig.ArtifactMaxUploadBytes
				resolved.GCPWorkloadIdentityProvider = r.lastConfig.GCPWorkloadIdentityProvider
			} else {
				resolved.ArtifactBucket = ""
				resolved.ArtifactDir = ""
				resolved.ArtifactMaxUploadBytes = 0
				resolved.GCPWorkloadIdentityProvider = ""
			}
		} else {
			if resolved.ArtifactBucket != "" && newSink != nil {
				probeCtx, probeCancel := context.WithTimeout(ctx, 10*time.Second)
				if probeErr := sinkconfig.ProbeArtifactSink(probeCtx, newSink); probeErr != nil {
					r.logger.Info(fmt.Sprintf("WARN: artifactBucket is set but artifact storage is unavailable: %v; ensure workload identity or pod credentials are configured, or set artifactBucket: %q in eval-runner-config to disable", probeErr, configkeys.ConfigSentinelDisabled))
					if closeErr := newSink.Close(); closeErr != nil {
						r.logger.Error(closeErr, "closing replaced artifact sink")
					}
					newSink = artifacts.NewUnavailableSink(resolved.ArtifactBucket, probeErr)
				}
				probeCancel()
			}
			if newSink == nil {
				r.logger.Info("neither artifactBucket nor artifactDir configured; using default in-memory sink (non-durable: controller restarts reset Runs to Pending to re-discover)")
				newSink = artifacts.NewInMemorySink()
			}
			if wait {
				if err := r.reloadableSink.SwapWait(newSink); err != nil {
					r.logger.Error(err, "error closing retired artifact sink")
				}
			} else {
				r.reloadableSink.Swap(newSink)
			}
		}
	}

	if r.runReconciler != nil {
		r.runReconciler.UpdateConfig(resolved)
	}
	if r.compReconciler != nil {
		r.compReconciler.UpdateConfig(resolved)
	}
	if r.consumer != nil {
		var repoNames []string
		for _, repo := range resolved.Repositories {
			repoNames = append(repoNames, repo.Name)
		}
		r.consumer.UpdateConfig(consumer.Config{
			DefaultModel:             resolved.Model.DefaultModel,
			DefaultCohort:            resolved.DefaultCohort,
			DefaultBatchSize:         resolved.DefaultBatchSize,
			DefaultMaxSamplesPerTask: resolved.DefaultMaxSamplesPerTask,
			DefaultBudgetUSD:         resolved.DefaultBudgetUSD,
			TrackedRepos:             resolved.Repositories,
			IsMultiRepo:              consumer.IsMultiRepo(repoNames),
		})
	}
	if r.gitHubClient != nil && resolved.GitHub.RateLimitFloorPercent > 0 {
		r.gitHubClient.SetRateFloorPercent(resolved.GitHub.RateLimitFloorPercent)
	}
	if r.syncer != nil {
		oldRepos := make(map[string]bool)
		if r.lastConfig != nil {
			for _, repo := range r.lastConfig.Repositories {
				oldRepos[repo.Ref.String()] = true
			}
		}
		for _, repo := range resolved.Repositories {
			if !oldRepos[repo.Ref.String()] && !repo.Ref.IsZero() {
				r.logger.Info("syncing newly added repository", "repo", repo.Ref.String())
			}
		}
		r.syncer.UpdateConfig(resolved.Repositories, resolved.LsRemoteInterval, resolved.FullReconcileInterval, resolved.GitHub.RateLimitFloorPercent)
	}

	if r.topUpRunner != nil {
		r.topUpRunner.UpdateConfig(resolved)
	}

	fqdnChanged := r.lastConfig == nil || !reflect.DeepEqual(r.lastConfig.SandboxEgressHosts, resolved.SandboxEgressHosts)
	if fqdnChanged && r.fqdnSyncFunc != nil {
		if fqdnErr := r.fqdnSyncFunc(ctx, resolved.SandboxEgressHosts); fqdnErr != nil {
			r.logger.Error(fqdnErr, "error updating sandbox FQDN network policy")
		}
	}

	registryServicePortalChanged := r.lastConfig == nil ||
		r.lastConfig.ServicePortal.RegistryServicePortalEnabled != resolved.ServicePortal.RegistryServicePortalEnabled ||
		r.lastConfig.ServicePortal.EffectiveServicePortalsNamespace() != resolved.ServicePortal.EffectiveServicePortalsNamespace()
	if registryServicePortalChanged && r.registryServicePortalSyncFunc != nil {
		if syncErr := r.registryServicePortalSyncFunc(
			ctx,
			resolved.ServicePortal.RegistryServicePortalEnabled,
			resolved.ServicePortal.EffectiveServicePortalsNamespace(),
		); syncErr != nil {
			r.logger.Error(syncErr, "error updating registry service portal egress network policy")
		}
	}

	r.lastConfig = &resolved

	LogEffectiveClusterSettings(r.logger, resolved)
	return nil
}
