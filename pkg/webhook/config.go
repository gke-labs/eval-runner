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

package webhook

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/configkeys"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/forge"
	"github.com/gke-labs/eval-runner/pkg/forge/consumer"
	"github.com/gke-labs/eval-runner/pkg/github"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// DefaultNamespace is the default namespace where webhook and runs reside.
	DefaultNamespace = "eval-runner"

	// DefaultConfigMapName is the cluster ConfigMap name.
	DefaultConfigMapName = configkeys.DefaultConfigMapName

	// DefaultGitHubWebhookSecret is the default Secret name containing the webhook HMAC secret.
	DefaultGitHubWebhookSecret = "github-webhook"

	// DefaultListenAddr is the default HTTP listen address.
	DefaultListenAddr = ":8080"

	// DefaultWorkers is the default number of worker goroutines.
	DefaultWorkers = 2

	// DefaultQueueSize is the default buffer size for the async work queue.
	DefaultQueueSize = 100

	// DefaultBatchSize is the default batch size for paired adaptive sampling.
	DefaultBatchSize = 8

	// DefaultMaxSamplesPerTask is the default maximum samples per task across batches.
	DefaultMaxSamplesPerTask = v1alpha1.DefaultMaxSamplesPerTask

	// DefaultAllowedCloneHosts is the default comma-separated allow-list of clone URL hosts.
	DefaultAllowedCloneHosts = "github.com"

	// ConfigMapKeyDefaultModel is the ConfigMap key for the default model.
	ConfigMapKeyDefaultModel = configkeys.ConfigMapKeyDefaultModel

	// ConfigMapKeyDefaultLimit is the ConfigMap key for the default sample limit.
	ConfigMapKeyDefaultLimit = configkeys.ConfigMapKeyDefaultLimit

	// ConfigMapKeyDefaultBatchSize is the ConfigMap key for the default batch size.
	ConfigMapKeyDefaultBatchSize = configkeys.ConfigMapKeyDefaultBatchSize

	// ConfigMapKeyDefaultMaxSamplesPerTask is the ConfigMap key for the default maximum samples per task.
	ConfigMapKeyDefaultMaxSamplesPerTask = configkeys.ConfigMapKeyDefaultMaxSamplesPerTask

	// ConfigMapKeyDefaultBudgetUSD is the ConfigMap key for the default budget in USD.
	ConfigMapKeyDefaultBudgetUSD = configkeys.ConfigMapKeyDefaultBudgetUSD

	// ConfigMapKeyDefaultCohort is the ConfigMap key for the default cohort.
	ConfigMapKeyDefaultCohort = configkeys.ConfigMapKeyDefaultCohort

	// ConfigMapKeyGitHubWebhookSecret is the ConfigMap key for the webhook secret name.
	ConfigMapKeyGitHubWebhookSecret = configkeys.ConfigMapKeyGitHubWebhookSecret

	// ConfigMapKeyWebhookRepositories is the ConfigMap key for repository allow-list.
	ConfigMapKeyWebhookRepositories = configkeys.ConfigMapKeyWebhookRepositories

	// ConfigMapKeyAllowedCloneHosts is the ConfigMap key for clone URL host allow-list.
	// An empty list defaults to "github.com".
	ConfigMapKeyAllowedCloneHosts = configkeys.ConfigMapKeyAllowedCloneHosts

	// ConfigSentinelDisabled is the sentinel value used in ConfigMap or CLI flags to explicitly disable
	// a credential or feature that otherwise has a non-empty built-in default.
	ConfigSentinelDisabled = configkeys.ConfigSentinelDisabled
)

// Config holds settings for the eval-runner-webhook service.
type Config struct {
	Namespace                string
	ConfigMapName            string
	ListenAddr               string
	GitHubWebhookSecret      string
	DefaultModel             string
	DefaultCohort            string
	DefaultLimit             *int
	DefaultBatchSize         int
	DefaultMaxSamplesPerTask int
	DefaultBudgetUSD         *float64
	WebhookRepositories      []string
	AllowedCloneHosts        []string
	Workers                  int
	QueueSize                int

	// StaticWebhookSecret optionally provides the HMAC secret directly (useful for testing or env vars).
	StaticWebhookSecret []byte

	// Client is the Kubernetes client used for reading Secrets/ConfigMaps and managing Runs/Comparisons.
	Client client.Client

	// Forge optionally provides a direct Forge abstraction for tests and repository queries.
	Forge forge.Forge

	// ForgeRegistry optionally provides a multi-host forge registry.
	ForgeRegistry *forge.Registry

	// Consumer optionally provides an explicit Consumer.
	Consumer *consumer.Consumer

	// GitHubAppSecret optionally specifies the Secret name containing GitHub App credentials.
	GitHubAppSecret string

	// GitHubClient optionally provides a direct GitHub client for tests and API calls.
	GitHubClient github.Client
}

// RawWebhookFlags captures explicit flags passed on the command line.
type RawWebhookFlags struct {
	Namespace                string
	ConfigMapName            string
	ListenAddr               string
	GitHubWebhookSecret      string
	GitHubAppSecret          string
	DefaultModel             string
	DefaultCohort            string
	DefaultLimit             *int
	DefaultBatchSize         *int
	DefaultMaxSamplesPerTask *int
	DefaultBudgetUSD         *float64
	WebhookRepositories      string
	AllowedCloneHosts        string
	Workers                  int
	QueueSize                int
	Explicit                 map[string]bool
}

// IsExplicit reports whether a flag was explicitly set on the command line.
func (f RawWebhookFlags) IsExplicit(name string) bool {
	if f.Explicit == nil {
		return false
	}
	return f.Explicit[name]
}

// DefaultConfig returns a Config initialized with built-in default values.
func DefaultConfig() Config {
	return Config{
		Namespace:                DefaultNamespace,
		ConfigMapName:            DefaultConfigMapName,
		ListenAddr:               DefaultListenAddr,
		GitHubWebhookSecret:      DefaultGitHubWebhookSecret,
		DefaultModel:             "",
		DefaultCohort:            "",
		DefaultLimit:             nil,
		DefaultBatchSize:         DefaultBatchSize,
		DefaultMaxSamplesPerTask: DefaultMaxSamplesPerTask,
		WebhookRepositories:      nil,
		AllowedCloneHosts:        []string{DefaultAllowedCloneHosts},
		Workers:                  DefaultWorkers,
		QueueSize:                DefaultQueueSize,
	}
}

// LoadClusterConfig loads the specified ConfigMap from the Kubernetes API.
func LoadClusterConfig(ctx context.Context, cl client.Reader, namespace, configMapName string) (*corev1.ConfigMap, error) {
	if cl == nil {
		return nil, nil
	}
	var cm corev1.ConfigMap
	key := types.NamespacedName{
		Namespace: namespace,
		Name:      configMapName,
	}
	if err := cl.Get(ctx, key, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get cluster ConfigMap %s/%s: %w", namespace, configMapName, err)
	}
	return &cm, nil
}

// ResolveWebhookConfig resolves configuration using precedence:
// explicit flag > ConfigMap value > built-in default.
func ResolveWebhookConfig(cm *corev1.ConfigMap, flags RawWebhookFlags, cl client.Client) (Config, error) {
	cfg := DefaultConfig()
	cfg.Client = cl

	if flags.IsExplicit("namespace") && flags.Namespace != "" {
		cfg.Namespace = flags.Namespace
	}
	if flags.IsExplicit("configmap-name") && flags.ConfigMapName != "" {
		cfg.ConfigMapName = flags.ConfigMapName
	}
	if flags.IsExplicit("listen-addr") && flags.ListenAddr != "" {
		cfg.ListenAddr = flags.ListenAddr
	}
	if flags.IsExplicit("workers") && flags.Workers > 0 {
		cfg.Workers = flags.Workers
	}
	if flags.IsExplicit("queue-size") && flags.QueueSize > 0 {
		cfg.QueueSize = flags.QueueSize
	}

	// 1. Apply from ConfigMap if present
	if cm != nil && cm.Data != nil {
		if val, ok := cm.Data[ConfigMapKeyGitHubWebhookSecret]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				cfg.GitHubWebhookSecret = trimmed
			}
		}
		if val, ok := cm.Data[configkeys.ConfigMapKeyGitHubAppSecret]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				cfg.GitHubAppSecret = trimmed
			}
		}
		if val, ok := cm.Data[ConfigMapKeyDefaultModel]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.DefaultModel = ""
				} else {
					cfg.DefaultModel = trimmed
				}
			}
		}
		if val, ok := cm.Data[ConfigMapKeyDefaultCohort]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.DefaultCohort = ""
				} else {
					cfg.DefaultCohort = trimmed
				}
			}
		}
		if val, ok := cm.Data[ConfigMapKeyDefaultLimit]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				limit, err := strconv.Atoi(trimmed)
				if err != nil || limit <= 0 {
					return Config{}, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must be a positive integer",
						ConfigMapKeyDefaultLimit, val, cm.Namespace, cm.Name)
				}
				cfg.DefaultLimit = &limit
			}
		}
		if val, ok := cm.Data[ConfigMapKeyDefaultBatchSize]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				bs, err := strconv.Atoi(trimmed)
				if err != nil || bs <= 0 {
					return Config{}, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must be a positive integer",
						ConfigMapKeyDefaultBatchSize, val, cm.Namespace, cm.Name)
				}
				cfg.DefaultBatchSize = bs
			}
		}
		if val, ok := cm.Data[ConfigMapKeyDefaultMaxSamplesPerTask]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				ms, err := strconv.Atoi(trimmed)
				if err != nil || ms <= 0 {
					return Config{}, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must be a positive integer",
						ConfigMapKeyDefaultMaxSamplesPerTask, val, cm.Namespace, cm.Name)
				}
				cfg.DefaultMaxSamplesPerTask = ms
			}
		}
		if val, ok := cm.Data[ConfigMapKeyDefaultBudgetUSD]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				if strings.EqualFold(trimmed, ConfigSentinelDisabled) {
					cfg.DefaultBudgetUSD = nil
				} else {
					budget, err := strconv.ParseFloat(trimmed, 64)
					if err != nil || budget < 0 {
						return Config{}, fmt.Errorf("invalid %s %q in ConfigMap %s/%s: must be a non-negative number",
							ConfigMapKeyDefaultBudgetUSD, val, cm.Namespace, cm.Name)
					}
					if budget > 0 {
						cfg.DefaultBudgetUSD = &budget
					} else {
						cfg.DefaultBudgetUSD = nil
					}
				}
			}
		}
		if val, ok := cm.Data[ConfigMapKeyWebhookRepositories]; ok {
			cfg.WebhookRepositories = ParseRepositoryList(val)
		}
		if val, ok := cm.Data[ConfigMapKeyAllowedCloneHosts]; ok {
			hosts := discovery.ParseHostList(val)
			if len(hosts) > 0 {
				cfg.AllowedCloneHosts = hosts
			} else {
				cfg.AllowedCloneHosts = []string{DefaultAllowedCloneHosts}
			}
		}
	}

	// 2. Apply explicit flags (highest precedence)
	if flags.IsExplicit("github-webhook-secret") && flags.GitHubWebhookSecret != "" {
		cfg.GitHubWebhookSecret = flags.GitHubWebhookSecret
	}
	if flags.IsExplicit("default-model") {
		if strings.EqualFold(flags.DefaultModel, ConfigSentinelDisabled) {
			cfg.DefaultModel = ""
		} else {
			cfg.DefaultModel = flags.DefaultModel
		}
	}
	if flags.IsExplicit("default-cohort") {
		if strings.EqualFold(flags.DefaultCohort, ConfigSentinelDisabled) {
			cfg.DefaultCohort = ""
		} else {
			cfg.DefaultCohort = flags.DefaultCohort
		}
	}
	if flags.IsExplicit("default-limit") && flags.DefaultLimit != nil {
		cfg.DefaultLimit = flags.DefaultLimit
	}
	if flags.IsExplicit("default-batch-size") && flags.DefaultBatchSize != nil {
		cfg.DefaultBatchSize = *flags.DefaultBatchSize
	}
	if flags.IsExplicit("default-max-samples-per-task") && flags.DefaultMaxSamplesPerTask != nil {
		cfg.DefaultMaxSamplesPerTask = *flags.DefaultMaxSamplesPerTask
	}
	if flags.IsExplicit("default-budget-usd") && flags.DefaultBudgetUSD != nil {
		if *flags.DefaultBudgetUSD > 0 {
			cfg.DefaultBudgetUSD = flags.DefaultBudgetUSD
		} else {
			cfg.DefaultBudgetUSD = nil
		}
	}
	if flags.IsExplicit("github-app-secret") && flags.GitHubAppSecret != "" {
		cfg.GitHubAppSecret = flags.GitHubAppSecret
	}
	if cfg.GitHubAppSecret == "" {
		cfg.GitHubAppSecret = "github-app"
	}
	if flags.IsExplicit("webhook-repositories") {
		cfg.WebhookRepositories = ParseRepositoryList(flags.WebhookRepositories)
	}
	if flags.IsExplicit("allowed-clone-hosts") {
		hosts := discovery.ParseHostList(flags.AllowedCloneHosts)
		if len(hosts) > 0 {
			cfg.AllowedCloneHosts = hosts
		} else {
			cfg.AllowedCloneHosts = []string{DefaultAllowedCloneHosts}
		}
	}

	return cfg, nil
}

// IsHostAllowed checks whether host (e.g. "github.com") is in allowList using exact,
// case-insensitive comparison. Wildcards are not supported.
// If allowList is empty, all hosts are rejected (fail-closed).
func IsHostAllowed(host string, allowList []string) bool {
	if len(allowList) == 0 {
		return false
	}
	normalized := strings.ToLower(strings.TrimSpace(host))
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

// ParseRepositoryList parses a comma-separated list of repository names ("owner/repo").
func ParseRepositoryList(s string) []string {
	var repos []string
	for _, part := range strings.Split(s, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			repos = append(repos, trimmed)
		}
	}
	return repos
}

// IsRepositoryAllowed checks whether repo (e.g. "gke-labs/eval-runner") is allowed.
// If allowList is empty, all repositories are rejected (fail-closed).
// An entry of "*" allows all repositories.
func IsRepositoryAllowed(repo string, allowList []string) bool {
	if len(allowList) == 0 {
		return false
	}
	normalized := strings.ToLower(strings.TrimSpace(repo))
	if normalized == "" {
		return false
	}
	for _, allowed := range allowList {
		trimmed := strings.TrimSpace(allowed)
		if trimmed == "*" || strings.ToLower(trimmed) == normalized {
			return true
		}
	}
	return false
}

// IsMultiRepo reports whether the configuration allows multiple repositories.
func (cfg Config) IsMultiRepo() bool {
	return consumer.IsMultiRepo(cfg.WebhookRepositories)
}
