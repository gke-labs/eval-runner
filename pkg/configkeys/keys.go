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

// Package configkeys defines the recognized ConfigMap keys and configuration sentinels
// used across eval-runner components (controller and webhook).
//
// It is a leaf package with no imports beyond the standard library so that components
// can reference configuration keys without coupling their dependency graphs.
package configkeys

const (
	// DefaultConfigMapName is the fixed name of the per-cluster configuration ConfigMap.
	DefaultConfigMapName = "eval-runner-config"

	// ConfigSentinelDisabled is the sentinel value used in ConfigMap or CLI flags to explicitly disable
	// a credential or feature that otherwise has a non-empty built-in default.
	ConfigSentinelDisabled = "none"

	// Controller keys

	// ConfigMapKeyArtifactBucket is the ConfigMap key for the GCS artifact bucket.
	ConfigMapKeyArtifactBucket = "artifactBucket"
	// ConfigMapKeyArtifactDir is the ConfigMap key for the local artifact directory.
	ConfigMapKeyArtifactDir = "artifactDir"
	// ConfigMapKeyArtifactMaxUploadBytes is the ConfigMap key for the maximum artifact upload byte limit.
	ConfigMapKeyArtifactMaxUploadBytes = "artifactMaxUploadBytes"
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
	// ConfigMapKeyGitHubTokenSecret is the ConfigMap key for GitHub token Secret name.
	ConfigMapKeyGitHubTokenSecret = "githubTokenSecret"
	// ConfigMapKeyGitHubAppSecret is the ConfigMap key for GitHub App credentials Secret name.
	ConfigMapKeyGitHubAppSecret = "githubAppSecret"
	// ConfigMapKeyUIBaseURL is the ConfigMap key for the eval-runner UI base URL.
	ConfigMapKeyUIBaseURL = "uiBaseURL"
	// ConfigMapKeyInconclusivePolicy is the ConfigMap key for check run inconclusive verdict policy ("neutral" or "failure").
	ConfigMapKeyInconclusivePolicy = "inconclusivePolicy"
	// ConfigMapKeyComparisonTTL is the ConfigMap key for completed comparison TTL duration.
	ConfigMapKeyComparisonTTL = "comparisonTTL"

	// Webhook keys

	// ConfigMapKeyGitHubWebhookSecret is the ConfigMap key for the webhook secret name (used by eval-runner-webhook).
	ConfigMapKeyGitHubWebhookSecret = "githubWebhookSecret"
	// ConfigMapKeyWebhookRepositories is the ConfigMap key for repository allow-list (used by eval-runner-webhook).
	ConfigMapKeyWebhookRepositories = "webhookRepositories"
	// ConfigMapKeyDefaultModel is the ConfigMap key for the default model (used by eval-runner-webhook).
	ConfigMapKeyDefaultModel = "defaultModel"
	// ConfigMapKeyDefaultLimit is the ConfigMap key for the default sample limit (used by eval-runner-webhook).
	ConfigMapKeyDefaultLimit = "defaultLimit"
	// ConfigMapKeyDefaultBatchSize is the ConfigMap key for the default batch size (used by eval-runner-webhook).
	ConfigMapKeyDefaultBatchSize = "defaultBatchSize"
	// ConfigMapKeyDefaultMaxSamplesPerTask is the ConfigMap key for the default maximum samples per task (used by eval-runner-webhook).
	ConfigMapKeyDefaultMaxSamplesPerTask = "defaultMaxSamplesPerTask"
	// ConfigMapKeyCommandUsers is the ConfigMap key for allowed comment command users (used by eval-runner-webhook).
	ConfigMapKeyCommandUsers = "commandUsers"
)

const (
	// InconclusivePolicyNeutral maps an INCONCLUSIVE verdict to a "neutral" check-run conclusion (default).
	InconclusivePolicyNeutral = "neutral"
	// InconclusivePolicyFailure maps an INCONCLUSIVE verdict to a "failure" check-run conclusion.
	InconclusivePolicyFailure = "failure"
)
