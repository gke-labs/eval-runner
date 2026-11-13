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
	// ConfigMapKeyGCPWorkloadIdentityProvider is the ConfigMap key for the GCP Workload Identity Federation provider resource name.
	ConfigMapKeyGCPWorkloadIdentityProvider = "gcpWorkloadIdentityProvider"
	// DefaultWorkloadIdentityTokenFile is the contract path for the projected Kubernetes ServiceAccount token.
	DefaultWorkloadIdentityTokenFile = "/var/run/secrets/eval-runner/gcp/token"
	// ConfigMapKeyDiscoveryDeadline is the ConfigMap key for discovery deadline.
	ConfigMapKeyDiscoveryDeadline = "discoveryDeadline"
	// ConfigMapKeyExecutionDeadline is the ConfigMap key for sample execution deadline.
	ConfigMapKeyExecutionDeadline = "executionDeadline"
	// ConfigMapKeyMaxParallelTasks is the ConfigMap key for maximum parallel task sandboxes.
	ConfigMapKeyMaxParallelTasks = "maxParallelTasks"
	// ConfigMapKeyPollProgressInterval is the ConfigMap key for task progress polling interval.
	ConfigMapKeyPollProgressInterval = "pollProgressInterval"
	// ConfigMapKeyModelSecret is the ConfigMap key for default model credentials Secret name.
	ConfigMapKeyModelSecret = "modelSecret"
	// ConfigMapKeySandboxPortalPolicy is the ConfigMap key for opting task sandboxes into service-portals interception.
	ConfigMapKeySandboxPortalPolicy = "sandboxPortalPolicy"
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
	// ConfigMapKeyModelPrices is the ConfigMap key for model pricing table (JSON map model -> {inputPer1M, outputPer1M} in USD).
	ConfigMapKeyModelPrices = "modelPrices"

	// Webhook keys

	// ConfigMapKeyGitHubWebhookSecret is the ConfigMap key for the webhook secret name (used by eval-runner-webhook).
	ConfigMapKeyGitHubWebhookSecret = "githubWebhookSecret"
	// ConfigMapKeyWebhookRepositories is the ConfigMap key for repository allow-list (used by eval-runner-webhook).
	ConfigMapKeyWebhookRepositories = "webhookRepositories"
	// ConfigMapKeyAllowedCloneHosts is the ConfigMap key for clone URL host allow-list (used by eval-runner-webhook; empty defaults to github.com).
	ConfigMapKeyAllowedCloneHosts = "allowedCloneHosts"
	// ConfigMapKeyDefaultModel is the ConfigMap key for the default model (used by eval-runner-webhook).
	ConfigMapKeyDefaultModel = "defaultModel"
	// ConfigMapKeyDefaultLimit is the ConfigMap key for the default sample limit (used by eval-runner-webhook).
	ConfigMapKeyDefaultLimit = "defaultLimit"
	// ConfigMapKeyDefaultBatchSize is the ConfigMap key for the default batch size (used by eval-runner-webhook).
	ConfigMapKeyDefaultBatchSize = "defaultBatchSize"
	// ConfigMapKeyDefaultMaxSamplesPerTask is the ConfigMap key for the default maximum samples per task (used by eval-runner-webhook).
	ConfigMapKeyDefaultMaxSamplesPerTask = "defaultMaxSamplesPerTask"
	// ConfigMapKeyDefaultBudgetUSD is the ConfigMap key for the default budget in USD across all tasks and batches (used by eval-runner-webhook).
	ConfigMapKeyDefaultBudgetUSD = "defaultBudgetUSD"
	// ConfigMapKeyDefaultCohort is the ConfigMap key for the default cohort identifier (used by eval-runner-webhook).
	ConfigMapKeyDefaultCohort = "defaultCohort"
	// ConfigMapKeyCommandUsers is the ConfigMap key for allowed comment command users (used by eval-runner controller).
	ConfigMapKeyCommandUsers = "commandUsers"
	// ConfigMapKeyAffinityMode is the ConfigMap key for changed-file task affinity mode ("order" or "prune").
	ConfigMapKeyAffinityMode = "affinityMode"
	// ConfigMapKeySandboxEgressHosts is the ConfigMap key for comma-separated allowed FQDN egress hostnames for sandboxes.
	ConfigMapKeySandboxEgressHosts = "sandboxEgressHosts"
	// ConfigMapKeyRepositories is the ConfigMap key for tracked repositories.
	ConfigMapKeyRepositories = "repositories"
	// ConfigMapKeyLsRemoteInterval is the ConfigMap key for forge ls-remote polling interval (default "60s").
	ConfigMapKeyLsRemoteInterval = "lsRemoteInterval"
	// ConfigMapKeyFullReconcileInterval is the ConfigMap key for forge full reconcile interval (default "1h").
	ConfigMapKeyFullReconcileInterval = "fullReconcileInterval"
	// ConfigMapKeyRateLimitFloorPercent is the ConfigMap key for GitHub rate limit floor percentage (default "10").
	ConfigMapKeyRateLimitFloorPercent = "rateLimitFloorPercent"
	// ConfigMapKeyHeadTopUpInterval is the ConfigMap key for periodic head top-up interval (default "0" = disabled).
	ConfigMapKeyHeadTopUpInterval = "headTopUpInterval"
	// ConfigMapKeyHeadTargetCIHalfWidth is the ConfigMap key for target 95% CI half-width for head top-ups (default "0.05").
	ConfigMapKeyHeadTargetCIHalfWidth = "headTargetCIHalfWidth"
	// ConfigMapKeyHeadMaxSamplesPerTask is the ConfigMap key for maximum samples per task for head top-ups (defaults to defaultMaxSamplesPerTask).
	ConfigMapKeyHeadMaxSamplesPerTask = "headMaxSamplesPerTask"
)

const (
	// InconclusivePolicyNeutral maps an INCONCLUSIVE verdict to a "neutral" check-run conclusion (default).
	InconclusivePolicyNeutral = "neutral"
	// InconclusivePolicyFailure maps an INCONCLUSIVE verdict to a "failure" check-run conclusion.
	InconclusivePolicyFailure = "failure"
)

const (
	// AffinityModeOrder sorts each batch's spec.tasks matched/always first then unmatched (default).
	AffinityModeOrder = "order"
	// AffinityModePrune drops unmatched tasks from evaluation.
	AffinityModePrune = "prune"
)

const (
	// LabelBranch tracks the branch name on created baseline Runs and Comparisons.
	LabelBranch = "evalrunner.gke-labs.dev/branch"
)
