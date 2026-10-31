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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/affinity"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/configkeys"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/forge"
	"github.com/gke-labs/eval-runner/pkg/forge/consumer"
	forgegithub "github.com/gke-labs/eval-runner/pkg/forge/github"
	"github.com/gke-labs/eval-runner/pkg/forge/gitremote"
	"github.com/gke-labs/eval-runner/pkg/github"
	"github.com/gke-labs/eval-runner/pkg/stats"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// LabelBatchKey identifies a baseline batch evaluation by its deterministic hash.
	LabelBatchKey = "evalrunner.gke-labs.dev/batch-key"
	// LabelComparison references the parent Comparison resource.
	LabelComparison = "evalrunner.gke-labs.dev/comparison"
	// LabelBatch is the 1-based index of the evaluation batch.
	LabelBatch = "evalrunner.gke-labs.dev/batch"
	// LabelRole indicates whether a batch run is candidate or baseline.
	LabelRole = "evalrunner.gke-labs.dev/role"
	// LabelTargetRepo tracks the target repository identifier on created Runs and Comparisons.
	LabelTargetRepo = consumer.LabelTargetRepo
	// LabelRepo tracks the repository identifier on created Runs and Comparisons.
	LabelRepo = consumer.LabelRepo
	// LabelPR tracks the pull request number on created Runs and Comparisons.
	LabelPR = consumer.LabelPR
	// LabelHeadSHA tracks the git commit SHA on created Runs and Comparisons.
	LabelHeadSHA = consumer.LabelHeadSHA

	// RoleCandidate identifies the candidate run in a batch pair.
	RoleCandidate = "candidate"
	// RoleBaseline identifies the baseline run in a batch pair.
	RoleBaseline = "baseline"
)

// errBatchRunTerminating indicates that a batch Run is terminating and waiting for sandbox cleanup.
var errBatchRunTerminating = errors.New("batch run is terminating: waiting for cleanup")

func isBatchRunTerminating(err error) bool {
	return errors.Is(err, errBatchRunTerminating)
}

// PairedSampleScore records paired scores for a single sample ID.
type PairedSampleScore = stats.PairedSampleScore

// ComparisonTaskArtifact records paired comparison details for a single task in the artifact JSON.
type ComparisonTaskArtifact = artifacts.ComparisonTaskArtifact

// ComparisonArtifact is the schema stored as comparisons/<name>.json in the artifact sink.
type ComparisonArtifact = artifacts.ComparisonArtifact

// GitHubCredentials holds credentials extracted from a Secret for factory injection.
type GitHubCredentials struct {
	Token          string
	AppID          string
	InstallationID string
	PrivateKey     []byte
	AppSlug        string
	AppAuth        *github.AppTokenProvider
}

// ComparisonReconciler reconciles a Comparison object.
type ComparisonReconciler struct {
	client.Client
	Scheme                 *runtime.Scheme
	ArtifactSink           artifacts.ArtifactSink
	ArtifactSinkTimeout    time.Duration
	ControllerNamespace    string
	GitHubTokenSecret      string
	GitHubAppSecret        string
	UIBaseURL              string
	InconclusivePolicy     string
	GitHubBaseURL          string
	GitHubClientFactory    func(creds GitHubCredentials) github.Client
	ComparisonTTL          time.Duration
	CommandRetryInterval   time.Duration
	ReportingRetryInterval time.Duration
	CommandUsers           []string
	AffinityMode           string
	Now                    func() time.Time
	ModelPrices            map[string]ModelPrice
	ModelPriceSources      map[string]v1alpha1.CostSource
	ModelPricesDisabled    bool
	ForgeRegistry          *forge.Registry
	GitHubClient           github.Client

	cachedAppMu   sync.Mutex
	cachedAppKey  string
	cachedAppAuth *github.AppTokenProvider

	affinityMu     sync.Mutex
	cachedAffinity map[string]cachedPRFiles

	cfgMu sync.RWMutex
}

const maxCachedAffinityEntries = 100

type cachedPRFiles struct {
	files          []string
	truncated      bool
	fallbackReason string
}

func (r *ComparisonReconciler) getCachedAffinity(key string) (cachedPRFiles, bool) {
	r.affinityMu.Lock()
	defer r.affinityMu.Unlock()
	if r.cachedAffinity == nil {
		return cachedPRFiles{}, false
	}
	entry, ok := r.cachedAffinity[key]
	return entry, ok
}

func (r *ComparisonReconciler) recordCachedAffinity(key string, entry cachedPRFiles) {
	r.affinityMu.Lock()
	defer r.affinityMu.Unlock()
	if r.cachedAffinity == nil {
		r.cachedAffinity = make(map[string]cachedPRFiles)
	}
	if len(r.cachedAffinity) >= maxCachedAffinityEntries {
		for k := range r.cachedAffinity {
			delete(r.cachedAffinity, k)
			break
		}
	}
	r.cachedAffinity[key] = entry
}

func (r *ComparisonReconciler) evictCachedAffinity(key string) {
	r.affinityMu.Lock()
	defer r.affinityMu.Unlock()
	if r.cachedAffinity != nil {
		delete(r.cachedAffinity, key)
	}
}

func (r *ComparisonReconciler) commandBackoff(attempts int, err error) time.Duration {
	if r.CommandRetryInterval > 0 {
		return r.CommandRetryInterval
	}
	var rle *github.RateLimitError
	if errors.As(err, &rle) && rle.RetryAfter > 0 {
		return rle.RetryAfter
	}
	switch attempts {
	case 1:
		return 10 * time.Second
	case 2:
		return 1 * time.Minute
	default:
		return 5 * time.Minute
	}
}

func (r *ComparisonReconciler) earliestPendingCommandWait(comp *v1alpha1.Comparison) (time.Duration, bool) {
	if comp == nil || len(comp.Status.PendingCommands) == 0 {
		return 0, false
	}
	now := r.now()
	var minWait time.Duration
	found := false
	for _, cmd := range comp.Status.PendingCommands {
		var wait time.Duration
		if cmd.NextAttemptAt == nil || !cmd.NextAttemptAt.After(now) {
			wait = 1 * time.Second
		} else {
			wait = cmd.NextAttemptAt.Sub(now)
			if wait <= 0 {
				wait = 1 * time.Second
			}
		}
		if !found || wait < minWait {
			minWait = wait
			found = true
		}
	}
	return minWait, found
}

func (r *ComparisonReconciler) reportingBackoff(attempts int, err error) time.Duration {
	var rle *github.RateLimitError
	if errors.As(err, &rle) && rle.RetryAfter > 0 {
		return rle.RetryAfter
	}
	if r.ReportingRetryInterval > 0 {
		return r.ReportingRetryInterval
	}
	switch attempts {
	case 1:
		return 10 * time.Second
	case 2:
		return 1 * time.Minute
	default:
		return 5 * time.Minute
	}
}

func (r *ComparisonReconciler) earliestReportingWait(comp *v1alpha1.Comparison) (time.Duration, bool) {
	if comp == nil || comp.Spec.PullRequest == nil || comp.Status.Reporting == nil || comp.Status.Reporting.NextAttemptAt == nil {
		return 0, false
	}
	if comp.Status.Reporting.Message != "" {
		return 0, false
	}
	now := r.now()
	if !comp.Status.Reporting.NextAttemptAt.After(now) {
		return 0, false
	}
	return comp.Status.Reporting.NextAttemptAt.Sub(now), true
}

func (r *ComparisonReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *ComparisonReconciler) artifactSink() artifacts.ArtifactSink {
	if r.ArtifactSink != nil {
		return r.ArtifactSink
	}
	return &artifacts.NoopSink{}
}

func (r *ComparisonReconciler) artifactSinkTimeout() time.Duration {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.ArtifactSinkTimeout > 0 {
		return r.ArtifactSinkTimeout
	}
	return DefaultArtifactSinkTimeout
}

func (r *ComparisonReconciler) controllerNamespace() string {
	if r.ControllerNamespace != "" {
		return r.ControllerNamespace
	}
	return discovery.DefaultControllerNamespace
}

func (r *ComparisonReconciler) gitHubTokenSecret() string {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	return r.GitHubTokenSecret
}

func (r *ComparisonReconciler) gitHubAppSecret() string {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	return r.GitHubAppSecret
}

func (r *ComparisonReconciler) uiBaseURL() string {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	return r.UIBaseURL
}

func (r *ComparisonReconciler) inconclusivePolicy() string {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.InconclusivePolicy != "" {
		return r.InconclusivePolicy
	}
	return DefaultInconclusivePolicy
}

func (r *ComparisonReconciler) comparisonTTL() time.Duration {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	return r.ComparisonTTL
}

func (r *ComparisonReconciler) lookupModelPrice(model string) (ModelPrice, v1alpha1.CostSource, bool) {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.ModelPricesDisabled {
		return ModelPrice{}, "", false
	}
	prices := r.ModelPrices
	sources := r.ModelPriceSources
	if prices == nil {
		prices = DefaultModelPrices()
		sources = DefaultModelPriceSources()
	}
	return LookupModelPrice(model, prices, sources)
}

func (r *ComparisonReconciler) commandUsers() []string {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	return r.CommandUsers
}

func (r *ComparisonReconciler) affinityMode() string {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.AffinityMode != "" {
		return r.AffinityMode
	}
	return configkeys.AffinityModeOrder
}

// UpdateConfig updates the reloadable settings on ComparisonReconciler in a thread-safe manner.
func (r *ComparisonReconciler) UpdateConfig(cfg ClusterConfig) {
	r.cfgMu.Lock()
	defer r.cfgMu.Unlock()
	r.GitHubTokenSecret = cfg.GitHubTokenSecret
	r.GitHubAppSecret = cfg.GitHubAppSecret
	r.UIBaseURL = cfg.UIBaseURL
	r.InconclusivePolicy = cfg.InconclusivePolicy
	r.ComparisonTTL = cfg.ComparisonTTL
	r.ModelPrices = cfg.ModelPrices
	r.ModelPriceSources = cfg.ModelPriceSources
	r.ModelPricesDisabled = cfg.ModelPricesDisabled
	r.CommandUsers = cfg.CommandUsers
	r.AffinityMode = cfg.AffinityMode
}

func (r *ComparisonReconciler) getOrCreateAppAuth(secret *corev1.Secret, appID, installationID, appSlug string, privKey []byte) (*github.AppTokenProvider, error) {
	cacheKey := fmt.Sprintf("%s/%s:%s", secret.Namespace, secret.Name, secret.ResourceVersion)
	if secret.ResourceVersion == "" {
		h := sha256.Sum256(append([]byte(appID+":"+installationID+":"), privKey...))
		cacheKey = fmt.Sprintf("%s/%s:%x", secret.Namespace, secret.Name, h)
	}

	r.cachedAppMu.Lock()
	defer r.cachedAppMu.Unlock()

	if r.cachedAppKey == cacheKey && r.cachedAppAuth != nil {
		return r.cachedAppAuth, nil
	}

	appAuth, err := github.NewAppTokenProvider(github.AppConfig{
		AppID:          appID,
		InstallationID: installationID,
		PrivateKeyPEM:  privKey,
		AppSlug:        appSlug,
		BaseURL:        r.GitHubBaseURL,
	})
	if err != nil {
		return nil, err
	}

	r.cachedAppKey = cacheKey
	r.cachedAppAuth = appAuth
	return appAuth, nil
}

func (r *ComparisonReconciler) getGitHubClient(ctx context.Context) (github.Client, error) {
	if r.GitHubClient != nil {
		return r.GitHubClient, nil
	}

	secretNames := []string{}
	appSec := r.gitHubAppSecret()
	tokSec := r.gitHubTokenSecret()
	if appSec != "" {
		secretNames = append(secretNames, appSec)
	}
	if tokSec != "" && tokSec != appSec {
		secretNames = append(secretNames, tokSec)
	}

	if len(secretNames) == 0 {
		return nil, nil
	}

	for _, secretName := range secretNames {
		var secret corev1.Secret
		key := types.NamespacedName{
			Namespace: r.controllerNamespace(),
			Name:      secretName,
		}
		if err := r.Get(ctx, key, &secret); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("failed to get github credentials secret %q: %w", secretName, err)
		}

		appID := strings.TrimSpace(string(secret.Data["app-id"]))
		if appID == "" {
			appID = strings.TrimSpace(string(secret.Data["app_id"]))
		}
		installationID := strings.TrimSpace(string(secret.Data["installation-id"]))
		if installationID == "" {
			installationID = strings.TrimSpace(string(secret.Data["installation_id"]))
		}
		privKey := secret.Data["private-key"]
		if len(privKey) == 0 {
			privKey = secret.Data["private_key"]
		}
		appSlug := strings.TrimSpace(string(secret.Data["app-slug"]))
		if appSlug == "" {
			appSlug = strings.TrimSpace(string(secret.Data["app_slug"]))
		}
		token := strings.TrimSpace(string(secret.Data["token"]))

		// App credentials are preferred over token
		if appID != "" && installationID != "" && len(privKey) > 0 {
			appAuth, err := r.getOrCreateAppAuth(&secret, appID, installationID, appSlug, privKey)
			if err != nil {
				return nil, &github.PermanentError{
					StatusCode: 0,
					Message:    fmt.Sprintf("invalid app credentials: %v", err),
				}
			}

			creds := GitHubCredentials{
				AppID:          appID,
				InstallationID: installationID,
				PrivateKey:     privKey,
				AppSlug:        appSlug,
				AppAuth:        appAuth,
			}
			if r.GitHubClientFactory != nil {
				return r.GitHubClientFactory(creds), nil
			}
			return github.NewHTTPClient(github.HTTPClientConfig{
				AppAuth: appAuth,
				BaseURL: r.GitHubBaseURL,
			}), nil
		} else if token != "" {
			creds := GitHubCredentials{
				Token: token,
			}
			if r.GitHubClientFactory != nil {
				return r.GitHubClientFactory(creds), nil
			}
			return github.NewHTTPClient(github.HTTPClientConfig{
				Token:   token,
				BaseURL: r.GitHubBaseURL,
			}), nil
		}
	}

	return nil, nil
}

func isTransientPermissionError(err error) bool {
	if err == nil {
		return false
	}
	if github.IsPermanent(err) {
		return false
	}
	return true
}

func (r *ComparisonReconciler) reactToDeniedOrErroredCommand(
	ctx context.Context,
	ghClient github.Client,
	prSpec *v1alpha1.PullRequestSpec,
	commentID int64,
) {
	if ghClient == nil || prSpec == nil {
		return
	}
	parts := strings.Split(prSpec.Repository, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return
	}
	owner, repo := parts[0], parts[1]
	logger := log.FromContext(ctx)
	if err := ghClient.CreateCommentReaction(ctx, owner, repo, commentID, "confused"); err != nil {
		logger.Info("failed to react with confused to denied or errored comment", "commentId", commentID, "err", err)
	}
}

func (r *ComparisonReconciler) processPendingCommands(ctx context.Context, comp *v1alpha1.Comparison) error {
	if len(comp.Status.PendingCommands) == 0 {
		return nil
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	now := r.now()
	logger := log.FromContext(ctx)
	ghClient, ghErr := r.getGitHubClient(ctx)
	if ghErr != nil {
		logger.Info("warning: failed to get github client while processing commands", "err", ghErr)
	}

	var newHandled []v1alpha1.ComparisonHandledCommand
	updatedPending := make(map[int64]v1alpha1.ComparisonPendingCommand)
	totalExtraBump := 0

	for _, cmd := range comp.Status.PendingCommands {
		if cmd.NextAttemptAt != nil && cmd.NextAttemptAt.After(now) {
			// Command retry is not yet due; skip and preserve untouched.
			continue
		}

		authorized := false
		var terminalOutcome string
		isTransient := false
		var transientErr error

		// Reject bot logins (e.g. renovate[bot], dependabot[bot], etc.)
		if strings.HasSuffix(strings.ToLower(strings.TrimSpace(cmd.Login)), "[bot]") {
			terminalOutcome = "denied: bot"
		} else if IsUserAllowed(cmd.Login, r.commandUsers()) {
			// 1. Check commandUsers allow-list (exact case-insensitive match)
			authorized = true
		} else {
			// 2. Check GitHub collaborator permissions API
			if comp.Spec.PullRequest == nil {
				terminalOutcome = "denied: no pull request configured"
			} else if ghClient == nil {
				if ghErr != nil {
					isTransient = true
					transientErr = ghErr
				} else {
					terminalOutcome = "error: github credentials unavailable"
				}
			} else {
				parts := strings.Split(comp.Spec.PullRequest.Repository, "/")
				if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
					terminalOutcome = fmt.Sprintf("denied: invalid pullRequest.repository %q", comp.Spec.PullRequest.Repository)
				} else {
					owner, repo := parts[0], parts[1]
					perm, err := ghClient.GetRepoPermission(ctx, owner, repo, cmd.Login)
					if err != nil {
						logger.Info("github collaborator permission check failed", "user", cmd.Login, "repo", comp.Spec.PullRequest.Repository, "err", err)
						if isTransientPermissionError(err) {
							isTransient = true
							transientErr = err
						} else {
							terminalOutcome = fmt.Sprintf("error: failed to verify permissions: %v", err)
						}
					} else if !github.HasWriteAccess(perm) {
						terminalOutcome = "denied: unauthorized"
					} else {
						authorized = true
					}
				}
			}
		}

		if authorized {
			samplesToBump := cmd.Requested
			if comp.Spec.Sampling != nil {
				maxPerTask := comp.Spec.Sampling.EffectiveMaxSamplesPerTask()
				if maxPerTask > 0 && samplesToBump > maxPerTask {
					samplesToBump = maxPerTask
				}
			} else if samplesToBump > v1alpha1.DefaultMaxSamplesPerTask {
				samplesToBump = v1alpha1.DefaultMaxSamplesPerTask
			}
			totalExtraBump += samplesToBump
			terminalOutcome = "applied"

			// Acknowledge via GitHub reaction (eyes) or fallback comment
			if ghClient != nil && comp.Spec.PullRequest != nil {
				parts := strings.Split(comp.Spec.PullRequest.Repository, "/")
				if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
					owner, repo := parts[0], parts[1]
					prNum := comp.Spec.PullRequest.Number
					if reactErr := ghClient.CreateCommentReaction(ctx, owner, repo, cmd.CommentID, "eyes"); reactErr != nil {
						logger.Info("failed to react to comment, falling back to acknowledgement comment", "commentId", cmd.CommentID, "err", reactErr)
						msg := fmt.Sprintf("Evaluating %d more sample(s) for PR #%d (extraSamples=%d).", cmd.Requested, prNum, comp.Spec.ExtraSamples+totalExtraBump)
						if _, replyErr := ghClient.CreateIssueComment(ctx, owner, repo, prNum, msg); replyErr != nil {
							logger.Info("warning: failed to post acknowledgement comment", "pr", prNum, "err", replyErr)
						}
					}
				}
			}

			handled := v1alpha1.ComparisonHandledCommand{
				CommentID:  cmd.CommentID,
				Login:      cmd.Login,
				Requested:  cmd.Requested,
				ReceivedAt: cmd.ReceivedAt,
				HandledAt:  &metav1.Time{Time: r.now()},
				Outcome:    terminalOutcome,
			}
			newHandled = append(newHandled, handled)
		} else if isTransient {
			attempts := cmd.Attempts + 1
			if attempts < v1alpha1.MaxCommandAttempts {
				delay := r.commandBackoff(attempts, transientErr)
				nextTime := metav1.NewTime(now.Add(delay))
				cmd.Attempts = attempts
				cmd.NextAttemptAt = &nextTime
				updatedPending[cmd.CommentID] = cmd
				logger.Info("transient error verifying permissions, retrying", "commentId", cmd.CommentID, "user", cmd.Login, "attempt", attempts, "nextAttemptAt", nextTime.Time, "maxAttempts", v1alpha1.MaxCommandAttempts, "err", transientErr)
			} else {
				var outcome string
				if ghClient == nil && ghErr != nil {
					outcome = fmt.Sprintf("error: github credentials unavailable: %v", ghErr)
				} else {
					outcome = fmt.Sprintf("error: failed to verify permissions: %v", transientErr)
				}
				logger.Info("transient error retries exhausted, marking terminal", "commentId", cmd.CommentID, "user", cmd.Login, "attempts", attempts, "outcome", outcome)

				r.reactToDeniedOrErroredCommand(ctx, ghClient, comp.Spec.PullRequest, cmd.CommentID)

				handled := v1alpha1.ComparisonHandledCommand{
					CommentID:  cmd.CommentID,
					Login:      cmd.Login,
					Requested:  cmd.Requested,
					ReceivedAt: cmd.ReceivedAt,
					HandledAt:  &metav1.Time{Time: r.now()},
					Outcome:    outcome,
				}
				newHandled = append(newHandled, handled)
			}
		} else {
			logger.Info("command denied or terminal error", "commentId", cmd.CommentID, "user", cmd.Login, "outcome", terminalOutcome)

			r.reactToDeniedOrErroredCommand(ctx, ghClient, comp.Spec.PullRequest, cmd.CommentID)

			handled := v1alpha1.ComparisonHandledCommand{
				CommentID:  cmd.CommentID,
				Login:      cmd.Login,
				Requested:  cmd.Requested,
				ReceivedAt: cmd.ReceivedAt,
				HandledAt:  &metav1.Time{Time: r.now()},
				Outcome:    terminalOutcome,
			}
			newHandled = append(newHandled, handled)
		}
	}

	key := types.NamespacedName{Namespace: comp.Namespace, Name: comp.Name}
	specBumped := false

	handledIDs := make(map[int64]bool, len(newHandled))
	for _, h := range newHandled {
		handledIDs[h.CommentID] = true
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest v1alpha1.Comparison
		if err := r.Get(ctx, key, &latest); err != nil {
			return err
		}

		if totalExtraBump > 0 && !specBumped {
			latest.Spec.ExtraSamples += totalExtraBump
			if err := r.Update(ctx, &latest); err != nil {
				return err
			}
			specBumped = true
		}

		if len(newHandled) > 0 {
			latest.Status.HandledCommands = append(latest.Status.HandledCommands, newHandled...)
			if len(latest.Status.HandledCommands) > v1alpha1.MaxHandledCommands {
				latest.Status.HandledCommands = latest.Status.HandledCommands[len(latest.Status.HandledCommands)-v1alpha1.MaxHandledCommands:]
			}
		}

		var remainingPending []v1alpha1.ComparisonPendingCommand
		for _, p := range latest.Status.PendingCommands {
			if handledIDs[p.CommentID] {
				continue
			}
			if up, ok := updatedPending[p.CommentID]; ok {
				remainingPending = append(remainingPending, up)
			} else {
				remainingPending = append(remainingPending, p)
			}
		}
		latest.Status.PendingCommands = remainingPending

		if err := r.Status().Update(ctx, &latest); err != nil {
			return err
		}

		comp.Spec = latest.Spec
		comp.Status = latest.Status
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed persisting pending command outcome: %w", err)
	}

	return nil
}

func (r *ComparisonReconciler) handleTransientReportingError(
	comp *v1alpha1.Comparison,
	err error,
) error {
	now := r.now()
	if comp.Status.Reporting == nil {
		comp.Status.Reporting = &v1alpha1.ComparisonReportingStatus{}
	}
	attempts := comp.Status.Reporting.Attempts + 1
	if attempts < v1alpha1.MaxReportingAttempts {
		delay := r.reportingBackoff(attempts, err)
		nextTime := metav1.NewTime(now.Add(delay))
		comp.Status.Reporting.Attempts = attempts
		comp.Status.Reporting.NextAttemptAt = &nextTime
		comp.Status.Reporting.Message = ""
		return err
	}

	// Max attempts reached: record permanent message and stop retrying
	comp.Status.Reporting.Attempts = attempts
	comp.Status.Reporting.NextAttemptAt = nil
	comp.Status.Reporting.Message = fmt.Sprintf("reporting failed after %d attempts: %v", attempts, err)
	return nil
}

func pullRequestRepoRef(repo string) (forge.RepoRef, bool) {
	if ref, ok := forge.RepoRefFromOwnerName(repo); ok {
		return ref, true
	}
	return forge.ParseRepoURL(repo)
}

// reportToGitHub handles PR comment and check run reporting via the forge Reporter interface.
// Errors not matching github.PermanentError (5xx, token endpoint errors, rate limits, transport errors, or secret Get errors)
// are treated as transient.
func (r *ComparisonReconciler) reportToGitHub(
	ctx context.Context,
	comp *v1alpha1.Comparison,
	baseRun, candRun *v1alpha1.Run,
) error {
	if comp == nil || comp.Spec.PullRequest == nil {
		return nil
	}

	// When nextAttemptAt is in the future, skip reporting without consuming attempts.
	if comp.Status.Reporting != nil && comp.Status.Reporting.NextAttemptAt != nil && comp.Status.Reporting.NextAttemptAt.After(r.now()) {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	repoRef, ok := pullRequestRepoRef(comp.Spec.PullRequest.Repository)
	if !ok {
		permErr := &github.PermanentError{
			StatusCode: 400,
			Message:    fmt.Sprintf("invalid pullRequest.repository %q: expected owner/name", comp.Spec.PullRequest.Repository),
		}
		if comp.Status.Reporting == nil {
			comp.Status.Reporting = &v1alpha1.ComparisonReportingStatus{}
		}
		comp.Status.Reporting.Message = permErr.Error()
		comp.Status.Reporting.NextAttemptAt = nil
		return nil
	}

	if r.ForgeRegistry == nil {
		r.ForgeRegistry = forge.NewDefaultRegistry(forgegithub.New(), gitremote.New())
	}
	f := r.ForgeRegistry.ForRepo(repoRef)
	if f == nil {
		if comp.Status.Reporting == nil {
			comp.Status.Reporting = &v1alpha1.ComparisonReportingStatus{}
		}
		comp.Status.Reporting.Message = "disabled"
		comp.Status.Reporting.NextAttemptAt = nil
		return nil
	}

	// If using the github forge, verify client has credentials
	if ghForge, ok := f.(*forgegithub.Forge); ok {
		client := ghForge.Client()
		if client == nil || !client.HasCredentials() {
			ghClient, err := r.getGitHubClient(ctx)
			if err != nil {
				if github.IsPermanent(err) {
					if comp.Status.Reporting == nil {
						comp.Status.Reporting = &v1alpha1.ComparisonReportingStatus{}
					}
					comp.Status.Reporting.Message = err.Error()
					comp.Status.Reporting.NextAttemptAt = nil
					return nil
				}
				if !isTerminalComparisonPhase(comp.Status.Phase) {
					return err
				}
				return r.handleTransientReportingError(comp, err)
			}
			if ghClient == nil {
				if comp.Status.Reporting == nil {
					comp.Status.Reporting = &v1alpha1.ComparisonReportingStatus{}
				}
				comp.Status.Reporting.Message = "disabled"
				comp.Status.Reporting.NextAttemptAt = nil
				return nil
			}
			if hc, ok := ghClient.(*github.HTTPClient); ok {
				if !hc.HasCredentials() {
					if comp.Status.Reporting == nil {
						comp.Status.Reporting = &v1alpha1.ComparisonReportingStatus{}
					}
					comp.Status.Reporting.Message = "disabled"
					comp.Status.Reporting.NextAttemptAt = nil
					return nil
				}
				f = forgegithub.New(forgegithub.WithClient(hc))
			}
		}
	}

	rep, ok := f.(forge.Reporter)
	if !ok {
		log.FromContext(ctx).Info("reporting disabled: forge does not implement Reporter", "kind", f.Kind())
		if comp.Status.Reporting == nil {
			comp.Status.Reporting = &v1alpha1.ComparisonReportingStatus{}
		}
		comp.Status.Reporting.Message = "disabled"
		comp.Status.Reporting.NextAttemptAt = nil
		return nil
	}

	markdown := github.RenderComparisonMarkdown(comp, baseRun, candRun, r.uiBaseURL())
	checkState := github.CheckRunStateForComparison(comp, baseRun, candRun, r.inconclusivePolicy(), r.uiBaseURL())
	baseHash := github.HashContent(markdown, checkState)

	commentID := int64(0)
	checkRunID := int64(0)
	if comp.Status.Reporting != nil {
		commentID = comp.Status.Reporting.CommentID
		checkRunID = comp.Status.Reporting.CheckRunID
		if comp.Status.Reporting.LastPostedHash == baseHash && commentID != 0 {
			// If not in a terminal phase, or if annotations have already been posted, short-circuit immediately.
			if !isTerminalComparisonPhase(comp.Status.Phase) || comp.Status.Reporting.AnnotationsPosted {
				return nil
			}
		}
	}

	// Annotations: GitHub appends annotations on each check run update.
	// Emit annotations only once, upon reaching a terminal comparison phase.
	if isTerminalComparisonPhase(comp.Status.Phase) && (comp.Status.Reporting == nil || !comp.Status.Reporting.AnnotationsPosted) {
		annotations := r.buildTerminalAnnotations(ctx, comp, baseRun, candRun)
		if len(annotations) > 0 {
			checkState.Annotations = annotations
		}
	}

	crID := forge.ChangeRequestID(strconv.Itoa(comp.Spec.PullRequest.Number))
	markerKey := github.ComparisonMarker(comp.Namespace, comp.Name)
	newCommentID, err := rep.UpsertComment(ctx, repoRef, crID, markerKey, markdown)
	if err != nil {
		if github.IsPermanent(err) {
			if comp.Status.Reporting == nil {
				comp.Status.Reporting = &v1alpha1.ComparisonReportingStatus{}
			}
			comp.Status.Reporting.Message = err.Error()
			comp.Status.Reporting.NextAttemptAt = nil
			return nil
		}
		if !isTerminalComparisonPhase(comp.Status.Phase) {
			return err
		}
		return r.handleTransientReportingError(comp, err)
	}
	commentID = newCommentID

	if comp.Status.Reporting == nil {
		comp.Status.Reporting = &v1alpha1.ComparisonReportingStatus{}
	}
	comp.Status.Reporting.CommentID = commentID

	headSHA := comp.Spec.PullRequest.HeadSHA
	if headSHA == "" && candRun != nil {
		headSHA = candRun.Spec.Repo.Commit
	}
	if headSHA != "" {
		st := forge.Status{
			Status:      checkState.Status,
			Conclusion:  checkState.Conclusion,
			State:       checkState.Conclusion,
			Context:     checkState.Name,
			Description: checkState.Summary,
			Text:        checkState.Text,
			TargetURL:   checkState.DetailsURL,
			CheckRunID:  checkRunID,
			Images:      checkState.Images,
			Annotations: checkState.Annotations,
		}
		newCheckRunID, err := rep.SetStatus(ctx, repoRef, headSHA, st)
		if err != nil {
			if errors.Is(err, forgegithub.ErrCheckRunsUnavailable) {
				comp.Status.Reporting.Message = "check runs unavailable with token auth; comment posted"
				comp.Status.Reporting.CheckRunID = 0
			} else if github.IsPermanent(err) {
				comp.Status.Reporting.Message = err.Error()
				comp.Status.Reporting.NextAttemptAt = nil
				return nil
			} else {
				if !isTerminalComparisonPhase(comp.Status.Phase) {
					return err
				}
				return r.handleTransientReportingError(comp, err)
			}
		} else {
			comp.Status.Reporting.CheckRunID = newCheckRunID
			comp.Status.Reporting.Message = ""
			if isTerminalComparisonPhase(comp.Status.Phase) {
				comp.Status.Reporting.AnnotationsPosted = true
				if len(checkState.Annotations) > 0 {
					comp.Status.Reporting.AnnotationsHash = computeAnnotationsHash(checkState.Annotations)
				}
			}
		}
	}

	comp.Status.Reporting.LastPostedHash = baseHash
	comp.Status.Reporting.Attempts = 0
	comp.Status.Reporting.NextAttemptAt = nil
	return nil
}

func (r *ComparisonReconciler) buildTerminalAnnotations(
	ctx context.Context,
	comp *v1alpha1.Comparison,
	baseRun, candRun *v1alpha1.Run,
) []forge.CheckRunAnnotation {
	if candRun == nil || r.artifactSink() == nil {
		return nil
	}

	manifest, err := r.loadManifest(ctx, candRun, baseRun)
	if err != nil {
		log.FromContext(ctx).V(1).Info("unable to load manifest for check run annotations", "error", err)
		return nil
	}
	if manifest == nil || len(manifest.Tasks) == 0 {
		return nil
	}

	// Get changed files for the PR to resolve task path globs
	var changedFiles []string
	if comp.Spec.PullRequest != nil && comp.Spec.PullRequest.Repository != "" && comp.Spec.PullRequest.Number > 0 {
		cacheKey := fmt.Sprintf("%s#%d", comp.Spec.PullRequest.Repository, comp.Spec.PullRequest.Number)
		if cached, ok := r.getCachedAffinity(cacheKey); ok && len(cached.files) > 0 {
			changedFiles = cached.files
		} else {
			ghClient, clientErr := r.getGitHubClient(ctx)
			if clientErr != nil {
				log.FromContext(ctx).V(1).Info("unable to get GitHub client for annotations", "error", clientErr)
			} else if ghClient != nil {
				parts := strings.Split(comp.Spec.PullRequest.Repository, "/")
				if len(parts) == 2 {
					files, truncated, listErr := ghClient.ListPullRequestFiles(ctx, parts[0], parts[1], comp.Spec.PullRequest.Number)
					if listErr != nil {
						log.FromContext(ctx).V(1).Info("unable to list PR files for annotations", "error", listErr)
					} else {
						changedFiles = files
						r.recordCachedAffinity(cacheKey, cachedPRFiles{files: files, truncated: truncated})
					}
				}
			}
		}
	}

	// Filter tasks: must declare paths that resolve to concrete changed files,
	// and candidate task must have recorded failures.
	taskLogs := make(map[string][]byte)
	for _, mt := range manifest.Tasks {
		if len(mt.Paths) == 0 {
			continue
		}
		resolved := github.ResolveTaskFilePaths(mt.Paths, changedFiles)
		if len(resolved) == 0 {
			continue
		}
		for _, ct := range candRun.Status.Tasks {
			if ct.ID == mt.ID && ct.LogPath != "" && ct.Samples > ct.Successes {
				loadCtx, loadCancel := context.WithTimeout(ctx, r.artifactSinkTimeout())
				data, err := artifacts.LoadWithTimeout(loadCtx, r.artifactSink(), ct.LogPath)
				loadCancel()
				if err == nil && len(data) > 0 {
					taskLogs[mt.ID] = data
				}
				break
			}
		}
	}

	if len(taskLogs) == 0 {
		return nil
	}

	return github.BuildCheckRunAnnotations(manifest.Tasks, changedFiles, taskLogs)
}

func computeAnnotationsHash(anns []forge.CheckRunAnnotation) string {
	h := sha256.New()
	for _, a := range anns {
		h.Write([]byte(a.Path))
		h.Write([]byte{0})
		h.Write([]byte(strconv.Itoa(a.StartLine)))
		h.Write([]byte{0})
		h.Write([]byte(a.Message))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func isTerminalRunPhase(phase v1alpha1.RunPhase) bool {
	return phase == v1alpha1.RunPhaseCompleted || phase == v1alpha1.RunPhaseFailed
}

func isTerminalComparisonPhase(phase v1alpha1.ComparisonPhase) bool {
	return phase == v1alpha1.ComparisonPhaseCompleted
}

func (r *ComparisonReconciler) comparisonAge(comp *v1alpha1.Comparison) (time.Duration, bool) {
	referenceTime := comp.CreationTimestamp.Time
	if comp.Status.CompletionTime != nil && !comp.Status.CompletionTime.IsZero() {
		referenceTime = comp.Status.CompletionTime.Time
	}
	if referenceTime.IsZero() {
		return 0, false
	}
	return r.now().Sub(referenceTime), true
}

func isUnservicedHandledCommand(h *v1alpha1.ComparisonHandledCommand, completionTime *metav1.Time) bool {
	if h.Outcome != "applied" || h.HandledAt == nil {
		return false
	}
	if completionTime == nil {
		return true
	}
	return !h.HandledAt.Before(completionTime)
}

func (r *ComparisonReconciler) postUnserviceableCommandNote(ctx context.Context, comp *v1alpha1.Comparison, reason string) error {
	if comp == nil || comp.Spec.PullRequest == nil {
		return nil
	}
	repoRef, ok := pullRequestRepoRef(comp.Spec.PullRequest.Repository)
	if !ok {
		return fmt.Errorf("invalid pullRequest.repository: %q", comp.Spec.PullRequest.Repository)
	}
	if r.ForgeRegistry == nil {
		r.ForgeRegistry = forge.NewDefaultRegistry(forgegithub.New(), gitremote.New())
	}
	f := r.ForgeRegistry.ForRepo(repoRef)
	if f == nil {
		return nil
	}
	if ghForge, ok := f.(*forgegithub.Forge); ok {
		client := ghForge.Client()
		if client == nil || !client.HasCredentials() {
			ghClient, err := r.getGitHubClient(ctx)
			if err != nil {
				return err
			}
			if ghClient == nil {
				return nil
			}
			if hc, ok := ghClient.(*github.HTTPClient); ok {
				if !hc.HasCredentials() {
					return nil
				}
				f = forgegithub.New(forgegithub.WithClient(hc))
			}
		}
	}
	rep, ok := f.(forge.Reporter)
	if !ok {
		log.FromContext(ctx).Info("cannot post unserviceable note: forge does not implement Reporter", "kind", f.Kind())
		return nil
	}
	crID := forge.ChangeRequestID(strconv.Itoa(comp.Spec.PullRequest.Number))
	body := fmt.Sprintf("Extra samples cannot be run: %s.", reason)
	_, err := rep.UpsertComment(ctx, repoRef, crID, "", body)
	return err
}

// SetupWithManager sets up the controller with the Manager and watches Runs.
func (r *ComparisonReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Comparison{}).
		Watches(
			&v1alpha1.Run{},
			handler.EnqueueRequestsFromMapFunc(r.findComparisonsForRun),
		).
		Complete(r)
}

func (r *ComparisonReconciler) findComparisonsForRun(ctx context.Context, obj client.Object) []reconcile.Request {
	run, ok := obj.(*v1alpha1.Run)
	if !ok {
		return nil
	}

	var compList v1alpha1.ComparisonList
	if err := r.List(ctx, &compList, client.InNamespace(run.Namespace)); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for _, comp := range compList.Items {
		matched := false
		if comp.Spec.Baseline.Run == run.Name || comp.Spec.Candidate.Run == run.Name {
			matched = true
		}
		if !matched {
			for _, b := range comp.Status.Batches {
				if b.CandidateRun == run.Name || b.BaselineRun == run.Name {
					matched = true
					break
				}
			}
		}
		if !matched {
			for _, ref := range run.OwnerReferences {
				if ref.Kind == "Comparison" && ref.Name == comp.Name {
					matched = true
					break
				}
			}
		}
		if matched {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: comp.Namespace,
					Name:      comp.Name,
				},
			})
		}
	}
	return requests
}

// ComputeBatchKey calculates a deterministic SHA-256 hash identifying a baseline batch evaluation:
// repo URL, commit, model, sorted task IDs, and sample window.
func ComputeBatchKey(repoURL, commit, model string, tasks []string, window v1alpha1.SampleRange) string {
	sortedTasks := make([]string, len(tasks))
	copy(sortedTasks, tasks)
	sort.Strings(sortedTasks)
	raw := fmt.Sprintf("%s\n%s\n%s\n%s\n%d-%d", repoURL, commit, model, strings.Join(sortedTasks, ","), window.Start, window.End)
	h := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", h)[:32]
}

func (r *ComparisonReconciler) findCompletedBaselineRun(ctx context.Context, namespace, repoURL, commit, model string, tasks []string, window v1alpha1.SampleRange) (*v1alpha1.Run, error) {
	logger := log.FromContext(ctx)
	batchKey := ComputeBatchKey(repoURL, commit, model, tasks, window)

	var runList v1alpha1.RunList
	if err := r.List(ctx, &runList, client.InNamespace(namespace), client.MatchingLabels{
		LabelBatchKey: batchKey,
	}); err != nil {
		return nil, fmt.Errorf("failed to list runs by batch-key: %w", err)
	}

	expectedPrefix := r.artifactSink().ArtifactPrefix(commit)
	for i := range runList.Items {
		run := &runList.Items[i]
		if run.Status.Phase == v1alpha1.RunPhaseCompleted && run.DeletionTimestamp == nil {
			taskMap := make(map[string]v1alpha1.TaskStatus, len(run.Status.Tasks))
			for _, t := range run.Status.Tasks {
				taskMap[t.ID] = t
			}

			allMatch := len(tasks) > 0
			for _, taskID := range tasks {
				task, ok := taskMap[taskID]
				if !ok || task.Phase != v1alpha1.TaskPhaseCompleted || task.LogPath == "" {
					allMatch = false
					break
				}
				if expectedPrefix != "" && !strings.HasPrefix(task.LogPath, expectedPrefix) {
					allMatch = false
					break
				}
			}
			if !allMatch {
				logger.V(1).Info("skipping baseline run whose task log paths do not match sink artifact prefix", "run", run.Name, "expectedPrefix", expectedPrefix)
				continue
			}
			return run, nil
		}
	}

	return nil, nil
}

func (r *ComparisonReconciler) addOwnerReferenceToRun(ctx context.Context, run *v1alpha1.Run, comp *v1alpha1.Comparison) error {
	for _, ref := range run.OwnerReferences {
		if ref.UID == comp.UID {
			return nil
		}
	}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var latest v1alpha1.Run
		if err := r.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Name}, &latest); err != nil {
			return err
		}
		for _, ref := range latest.OwnerReferences {
			if ref.UID == comp.UID {
				return nil
			}
		}
		latest.OwnerReferences = append(latest.OwnerReferences, metav1.OwnerReference{
			APIVersion:         v1alpha1.GroupVersion.String(),
			Kind:               "Comparison",
			Name:               comp.Name,
			UID:                comp.UID,
			Controller:         ptr.To(false),
			BlockOwnerDeletion: ptr.To(true),
		})
		return r.Update(ctx, &latest)
	})
}

// ensureBatchRun gets or creates a batch Run (candidate or baseline), performing baseline reuse
// lookup when role is RoleBaseline. Returns the name of the batch Run.
func (r *ComparisonReconciler) ensureBatchRun(
	ctx context.Context,
	comp *v1alpha1.Comparison,
	templateRun *v1alpha1.Run,
	role string,
	tasks []string,
	window v1alpha1.SampleRange,
	batchIndex int,
) (string, error) {
	logger := log.FromContext(ctx)

	if role == RoleBaseline {
		batchKey := ComputeBatchKey(templateRun.Spec.Repo.URL, templateRun.Spec.Repo.Commit, templateRun.Spec.Model, tasks, window)
		existingBase, err := r.findCompletedBaselineRun(ctx, comp.Namespace, templateRun.Spec.Repo.URL, templateRun.Spec.Repo.Commit, templateRun.Spec.Model, tasks, window)
		if err != nil {
			return "", fmt.Errorf("failed looking up baseline run for reuse: %w", err)
		}
		if existingBase != nil {
			if addErr := r.addOwnerReferenceToRun(ctx, existingBase, comp); addErr != nil {
				return "", fmt.Errorf("failed to add owner reference to reused baseline run %q: %w", existingBase.Name, addErr)
			}
			logger.Info("reusing completed baseline run for batch", "comparison", comp.Name, "batch", batchIndex, "run", existingBase.Name, "batchKey", batchKey)
			return existingBase.Name, nil
		}
	}

	roleSuffix := "cand"
	isController := true
	if role == RoleBaseline {
		roleSuffix = "base"
		isController = false
	}
	runName := fmt.Sprintf("%s-b%d-%s", comp.Name, batchIndex, roleSuffix)

	var existingRun v1alpha1.Run
	if err := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: runName}, &existingRun); apierrors.IsNotFound(err) {
		spec := templateRun.Spec.DeepCopy()
		spec.Tasks = tasks
		spec.SampleRange = &window
		spec.Limit = nil

		labels := map[string]string{
			LabelComparison: comp.Name,
			LabelBatch:      strconv.Itoa(batchIndex),
			LabelRole:       role,
		}
		if role == RoleBaseline {
			labels[LabelBatchKey] = ComputeBatchKey(templateRun.Spec.Repo.URL, templateRun.Spec.Repo.Commit, templateRun.Spec.Model, tasks, window)
		}
		// Copy target-repo, repo, pr, and head-sha labels from templateRun if present
		for _, k := range []string{LabelTargetRepo, LabelRepo, LabelPR, LabelHeadSHA} {
			if v, ok := templateRun.Labels[k]; ok && v != "" {
				labels[k] = v
			}
		}
		if _, ok := labels[LabelTargetRepo]; !ok && comp.Spec.PullRequest != nil && comp.Spec.PullRequest.Repository != "" {
			labels[LabelTargetRepo] = consumer.SanitizeLabelValue(comp.Spec.PullRequest.Repository)
		}

		clone := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      runName,
				Namespace: comp.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         v1alpha1.GroupVersion.String(),
						Kind:               "Comparison",
						Name:               comp.Name,
						UID:                comp.UID,
						Controller:         ptr.To(isController),
						BlockOwnerDeletion: ptr.To(true),
					},
				},
				Labels: labels,
			},
			Spec: *spec,
		}
		if err := r.Create(ctx, clone); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("failed to create %s batch run: %w", role, err)
		}
	} else if err != nil {
		return "", fmt.Errorf("failed looking up %s run %q: %w", role, runName, err)
	} else {
		if existingRun.DeletionTimestamp != nil {
			return "", fmt.Errorf("batch run %q is terminating: waiting for cleanup: %w", runName, errBatchRunTerminating)
		}
		if role == RoleCandidate && templateRun.Spec.Repo.Commit != "" && existingRun.Spec.Repo.Commit != templateRun.Spec.Repo.Commit {
			return "", fmt.Errorf("stale batch run %q exists for commit %s (want %s); cleanup pending", runName, existingRun.Spec.Repo.Commit, templateRun.Spec.Repo.Commit)
		}
	}

	return runName, nil
}

func toComparisonTaskLabel(label stats.TaskLabel) v1alpha1.ComparisonTaskLabel {
	if label == stats.TaskLabelContinue {
		return v1alpha1.ComparisonTaskLabelRunning
	}
	return v1alpha1.ComparisonTaskLabel(label)
}

func effectiveSampleRange(spec v1alpha1.RunSpec, maxObserved int) *v1alpha1.SampleRange {
	if spec.SampleRange != nil {
		return spec.SampleRange
	}
	if spec.Limit != nil && *spec.Limit > 0 {
		return &v1alpha1.SampleRange{Start: 1, End: *spec.Limit}
	}
	if maxObserved > 0 {
		return &v1alpha1.SampleRange{Start: 1, End: maxObserved}
	}
	return nil
}

func runIncludesTask(run *v1alpha1.Run, taskID string) bool {
	if run == nil {
		return false
	}
	if len(run.Spec.Tasks) > 0 {
		for _, t := range run.Spec.Tasks {
			if t == taskID {
				return true
			}
		}
		return false
	}
	return true
}

func determineRunningTaskIDs(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run) []string {
	candTasks := getRunPlannedTaskIDs(candRun)
	baseTasks := getRunPlannedTaskIDs(baseRun)

	if len(comp.Spec.Tasks) > 0 {
		if candTasks == nil || baseTasks == nil {
			return comp.Spec.Tasks
		}
		var taskIDs []string
		for _, id := range comp.Spec.Tasks {
			if candTasks[id] && baseTasks[id] {
				taskIDs = append(taskIDs, id)
			}
		}
		return taskIDs
	}

	if candTasks == nil || baseTasks == nil {
		var known []string
		var source map[string]bool
		if candTasks != nil {
			source = candTasks
		} else if baseTasks != nil {
			source = baseTasks
		}
		for id := range source {
			known = append(known, id)
		}
		sort.Strings(known)
		return known
	}

	var taskIDs []string
	for id := range candTasks {
		if baseTasks[id] {
			taskIDs = append(taskIDs, id)
		}
	}
	sort.Strings(taskIDs)
	return taskIDs
}

func getRunPlannedTaskIDs(run *v1alpha1.Run) map[string]bool {
	if len(run.Spec.Tasks) > 0 {
		m := make(map[string]bool, len(run.Spec.Tasks))
		for _, id := range run.Spec.Tasks {
			m[id] = true
		}
		return m
	}
	if len(run.Status.Tasks) > 0 {
		m := make(map[string]bool, len(run.Status.Tasks))
		for _, t := range run.Status.Tasks {
			m[t.ID] = true
		}
		return m
	}
	return nil
}

// determineCompletedTaskIDs returns:
// - taskIDs: only tasks executed by both Runs with phase Completed, filtered by comp.Spec.Tasks if specified.
// - unexecuted: tasks in scope that were not executed by both runs.
// - candFailed: tasks that completed in baselineRun but failed in candRun.
func determineCompletedTaskIDs(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run) (taskIDs []string, unexecuted []string, candFailed []string) {
	candCompleted := make(map[string]bool)
	for i := range candRun.Status.Tasks {
		if candRun.Status.Tasks[i].Phase == v1alpha1.TaskPhaseCompleted {
			candCompleted[candRun.Status.Tasks[i].ID] = true
		}
	}

	baseCompleted := make(map[string]bool)
	for i := range baseRun.Status.Tasks {
		if baseRun.Status.Tasks[i].Phase == v1alpha1.TaskPhaseCompleted {
			baseCompleted[baseRun.Status.Tasks[i].ID] = true
		}
	}

	var scopeTasks []string
	if len(comp.Spec.Tasks) > 0 {
		scopeTasks = comp.Spec.Tasks
	} else {
		scopeSet := make(map[string]bool)
		for _, t := range candRun.Status.Tasks {
			scopeSet[t.ID] = true
		}
		for _, t := range baseRun.Status.Tasks {
			scopeSet[t.ID] = true
		}
		for _, id := range candRun.Spec.Tasks {
			scopeSet[id] = true
		}
		for _, id := range baseRun.Spec.Tasks {
			scopeSet[id] = true
		}
		for id := range scopeSet {
			scopeTasks = append(scopeTasks, id)
		}
		sort.Strings(scopeTasks)
	}

	for _, id := range scopeTasks {
		if candCompleted[id] && baseCompleted[id] {
			taskIDs = append(taskIDs, id)
		} else {
			unexecuted = append(unexecuted, id)
			candTask := findTaskStatus(candRun.Status.Tasks, id)
			if candTask != nil && candTask.Phase == v1alpha1.TaskPhaseFailed && baseCompleted[id] {
				candFailed = append(candFailed, id)
			}
		}
	}
	return taskIDs, unexecuted, candFailed
}

func determineSkippedTaskIDs(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run) []string {
	discovered := make(map[string]bool)
	if candRun.Status.Discovery != nil {
		for _, id := range candRun.Status.Discovery.TaskIDs {
			discovered[id] = true
		}
	}
	if baseRun.Status.Discovery != nil {
		for _, id := range baseRun.Status.Discovery.TaskIDs {
			discovered[id] = true
		}
	}

	var skipped []string
	for id := range discovered {
		if taskInRunSpecOrStatus(candRun, id) || taskInRunSpecOrStatus(baseRun, id) {
			continue
		}
		if len(comp.Spec.Tasks) > 0 && containsTask(comp.Spec.Tasks, id) {
			continue
		}
		skipped = append(skipped, id)
	}
	sort.Strings(skipped)
	return skipped
}

func taskInRunSpecOrStatus(run *v1alpha1.Run, taskID string) bool {
	if run == nil {
		return false
	}
	for _, id := range run.Spec.Tasks {
		if id == taskID {
			return true
		}
	}
	for _, t := range run.Status.Tasks {
		if t.ID == taskID {
			return true
		}
	}
	return false
}

func containsTask(tasks []string, taskID string) bool {
	for _, t := range tasks {
		if t == taskID {
			return true
		}
	}
	return false
}

func findTaskStatus(tasks []v1alpha1.TaskStatus, taskID string) *v1alpha1.TaskStatus {
	for i := range tasks {
		if tasks[i].ID == taskID {
			return &tasks[i]
		}
	}
	return nil
}

type resolvedAffinity struct {
	active       bool
	status       *v1alpha1.ComparisonAffinityStatus
	taskAffinity map[string]string
	matchedCount int
	mode         string
}

func (r *ComparisonReconciler) loadManifest(ctx context.Context, candRun, baseRun *v1alpha1.Run) (*v1alpha1.Manifest, error) {
	sink := r.artifactSink()
	if sink == nil {
		return nil, nil
	}
	manifestPath := ""
	if candRun != nil && candRun.Status.Discovery != nil && candRun.Status.Discovery.ManifestPath != "" {
		manifestPath = candRun.Status.Discovery.ManifestPath
	} else if baseRun != nil && baseRun.Status.Discovery != nil && baseRun.Status.Discovery.ManifestPath != "" {
		manifestPath = baseRun.Status.Discovery.ManifestPath
	}
	if manifestPath == "" {
		return nil, nil
	}
	loadCtx, loadCancel := context.WithTimeout(ctx, r.artifactSinkTimeout())
	data, err := artifacts.LoadWithTimeout(loadCtx, sink, manifestPath)
	loadCancel()
	if err != nil {
		return nil, fmt.Errorf("load manifest from %s: %w", manifestPath, err)
	}
	return discovery.ParseManifest(data)
}

func (r *ComparisonReconciler) effectiveAffinityMode() string {
	if strings.ToLower(strings.TrimSpace(r.affinityMode())) == configkeys.AffinityModePrune {
		return configkeys.AffinityModePrune
	}
	return configkeys.AffinityModeOrder
}

func (r *ComparisonReconciler) sortTasksByAffinity(tasks []string, taskAffinity map[string]string) []string {
	if len(taskAffinity) == 0 {
		return tasks
	}
	var matchedAlways []string
	var unmatched []string
	for _, t := range tasks {
		if taskAffinity[t] == v1alpha1.ComparisonTaskAffinityUnmatched {
			unmatched = append(unmatched, t)
		} else {
			matchedAlways = append(matchedAlways, t)
		}
	}
	return append(matchedAlways, unmatched...)
}

func (r *ComparisonReconciler) applyAffinityToBatchTasks(tasks []string, taskAffinity map[string]string, mode string) []string {
	if len(taskAffinity) == 0 {
		return tasks
	}
	if mode == configkeys.AffinityModePrune {
		var kept []string
		for _, t := range tasks {
			if taskAffinity[t] != v1alpha1.ComparisonTaskAffinityUnmatched {
				kept = append(kept, t)
			}
		}
		return kept
	}
	return r.sortTasksByAffinity(tasks, taskAffinity)
}

func (r *ComparisonReconciler) fallbackAffinity(manifest *v1alpha1.Manifest, comp *v1alpha1.Comparison, reason string) resolvedAffinity {
	taskAff := make(map[string]string)
	if manifest != nil {
		for _, t := range manifest.Tasks {
			taskAff[t.ID] = v1alpha1.ComparisonTaskAffinityAlways
		}
	} else if comp != nil && len(comp.Status.Tasks) > 0 {
		for _, t := range comp.Status.Tasks {
			taskAff[t.ID] = v1alpha1.ComparisonTaskAffinityAlways
		}
	}
	boundedReason := reason
	if len(boundedReason) > 256 {
		boundedReason = boundedReason[:256]
	}
	var statusAff *v1alpha1.ComparisonAffinityStatus
	if boundedReason != "" {
		statusAff = &v1alpha1.ComparisonAffinityStatus{
			FallbackReason: boundedReason,
		}
	}
	return resolvedAffinity{
		active:       true,
		status:       statusAff,
		taskAffinity: taskAff,
		matchedCount: 0,
		mode:         configkeys.AffinityModeOrder,
	}
}

func (r *ComparisonReconciler) resolveAffinity(
	ctx context.Context,
	comp *v1alpha1.Comparison,
	candRun, baseRun *v1alpha1.Run,
) resolvedAffinity {
	logger := log.FromContext(ctx)

	if comp.Spec.PullRequest == nil {
		return resolvedAffinity{mode: r.effectiveAffinityMode()}
	}

	mode := r.effectiveAffinityMode()
	cacheKey := fmt.Sprintf("%s#%d:%s:%s", comp.Spec.PullRequest.Repository, comp.Spec.PullRequest.Number, comp.Spec.PullRequest.HeadSHA, comp.Spec.PullRequest.BaseSHA)

	// 1. If affinity status is already recorded, reuse it directly WITHOUT loading manifest from sink
	if comp.Status.Affinity != nil {
		r.evictCachedAffinity(cacheKey)
		if comp.Status.Affinity.FallbackReason != "" {
			return r.fallbackAffinity(nil, comp, comp.Status.Affinity.FallbackReason)
		}
		if len(comp.Status.Tasks) > 0 {
			taskAff := make(map[string]string, len(comp.Status.Tasks))
			matched := 0
			for _, t := range comp.Status.Tasks {
				if t.Affinity != "" {
					taskAff[t.ID] = t.Affinity
					if t.Affinity == v1alpha1.ComparisonTaskAffinityMatched {
						matched++
					}
				}
			}
			if len(taskAff) > 0 {
				return resolvedAffinity{
					active:       true,
					status:       comp.Status.Affinity,
					taskAffinity: taskAff,
					matchedCount: matched,
					mode:         mode,
				}
			}
		}
	}

	// 2. Check in-memory cache
	cached, found := r.getCachedAffinity(cacheKey)
	if found && cached.fallbackReason != "" {
		return r.fallbackAffinity(nil, comp, cached.fallbackReason)
	}

	// 3. Load manifest to check if any task declares paths
	manifest, err := r.loadManifest(ctx, candRun, baseRun)
	if err != nil {
		logger.Info("unable to load manifest for affinity check", "error", err)
	}

	hasDeclaredPaths := false
	if manifest != nil {
		for _, t := range manifest.Tasks {
			if len(t.Paths) > 0 {
				hasDeclaredPaths = true
				break
			}
		}
	}

	if !hasDeclaredPaths && comp.Status.Affinity == nil {
		return resolvedAffinity{mode: r.effectiveAffinityMode()}
	}

	// 4. Fetch changed files from GitHub if not already in cache
	if !found {
		ghClient, clientErr := r.getGitHubClient(ctx)
		if clientErr != nil || ghClient == nil {
			reason := "github client unavailable"
			if clientErr != nil {
				reason = clientErr.Error()
			}
			logger.Info("failed to get GitHub client for affinity; falling back to order mode with all tasks always", "error", clientErr)
			r.recordCachedAffinity(cacheKey, cachedPRFiles{fallbackReason: reason})
			return r.fallbackAffinity(manifest, comp, reason)
		}

		parts := strings.Split(comp.Spec.PullRequest.Repository, "/")
		if len(parts) != 2 {
			reason := fmt.Sprintf("invalid repository %q in pullRequest spec", comp.Spec.PullRequest.Repository)
			logger.Info(reason)
			r.recordCachedAffinity(cacheKey, cachedPRFiles{fallbackReason: reason})
			return r.fallbackAffinity(manifest, comp, reason)
		}
		owner, repo := parts[0], parts[1]

		files, truncated, listErr := ghClient.ListPullRequestFiles(ctx, owner, repo, comp.Spec.PullRequest.Number)
		if listErr != nil {
			logger.Info("failed to list pull request files from GitHub; falling back to order mode with all tasks always", "error", listErr)
			r.recordCachedAffinity(cacheKey, cachedPRFiles{fallbackReason: listErr.Error()})
			return r.fallbackAffinity(manifest, comp, listErr.Error())
		}

		cached = cachedPRFiles{files: files, truncated: truncated}
		r.recordCachedAffinity(cacheKey, cached)
	}

	filesHash := affinity.ComputeFilesHash(cached.files)
	statusAff := &v1alpha1.ComparisonAffinityStatus{
		FilesHash:  filesHash,
		FilesCount: len(cached.files),
		Truncated:  cached.truncated,
	}

	taskAff := make(map[string]string)
	matched := 0
	if manifest != nil {
		for _, t := range manifest.Tasks {
			aff := affinity.DetermineAffinity(t.Paths, cached.files, cached.truncated)
			taskAff[t.ID] = aff
			if aff == v1alpha1.ComparisonTaskAffinityMatched {
				matched++
			}
		}
	} else if len(comp.Status.Tasks) > 0 {
		for _, t := range comp.Status.Tasks {
			taskAff[t.ID] = v1alpha1.ComparisonTaskAffinityAlways
		}
	}

	return resolvedAffinity{
		active:       true,
		status:       statusAff,
		taskAffinity: taskAff,
		matchedCount: matched,
		mode:         mode,
	}
}

// Reconcile handles a reconciliation request for a Comparison.
func (r *ComparisonReconciler) Reconcile(ctx context.Context, req reconcile.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var comp v1alpha1.Comparison
	if err := r.Get(ctx, req.NamespacedName, &comp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if comp.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	// Detect if candidate or baseline Run changed compared to what status was computed from.
	// If so, reset scoring state and start over for the new candidate/baseline.
	observedCandidate := comp.Status.ObservedCandidateRun
	if observedCandidate == "" && len(comp.Status.Batches) > 0 {
		observedCandidate = comp.Status.Batches[0].CandidateRun
	}
	observedBaseline := comp.Status.ObservedBaselineRun
	if observedBaseline == "" && len(comp.Status.Batches) > 0 {
		observedBaseline = comp.Status.Batches[0].BaselineRun
	}

	candChanged := observedCandidate != "" && comp.Spec.Candidate.Run != "" && comp.Spec.Candidate.Run != observedCandidate
	baseChanged := observedBaseline != "" && comp.Spec.Baseline.Run != "" && comp.Spec.Baseline.Run != observedBaseline

	if candChanged || baseChanged {
		logger.Info("candidate or baseline Run changed; resetting comparison scoring state",
			"comparison", comp.Name,
			"oldCandidate", observedCandidate, "newCandidate", comp.Spec.Candidate.Run,
			"oldBaseline", observedBaseline, "newBaseline", comp.Spec.Baseline.Run,
		)

		var bRun, cRun *v1alpha1.Run
		var bRunObj, cRunObj v1alpha1.Run
		if err := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: comp.Spec.Baseline.Run}, &bRunObj); err == nil {
			bRun = &bRunObj
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("failed looking up baseline run %s: %w", comp.Spec.Baseline.Run, err)
		}

		if err := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: comp.Spec.Candidate.Run}, &cRunObj); err == nil {
			cRun = &cRunObj
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("failed looking up candidate run %s: %w", comp.Spec.Candidate.Run, err)
		}

		// Delete stale candidate extension Runs from previous candidate commits before persisting status reset.
		candCommit := ""
		if cRun != nil {
			candCommit = cRun.Spec.Repo.Commit
		} else if comp.Spec.PullRequest != nil {
			candCommit = comp.Spec.PullRequest.HeadSHA
		}

		if candCommit != "" {
			var runList v1alpha1.RunList
			if err := r.List(ctx, &runList, client.InNamespace(comp.Namespace), client.MatchingLabels{LabelComparison: comp.Name}); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed listing runs for comparison %s: %w", comp.Name, err)
			}
			for i := range runList.Items {
				run := &runList.Items[i]
				if run.Labels[LabelRole] != RoleCandidate {
					continue
				}
				if run.Spec.Repo.Commit == candCommit {
					continue
				}
				if run.DeletionTimestamp != nil {
					continue
				}
				if err := r.Delete(ctx, run); err != nil {
					if !apierrors.IsNotFound(err) {
						return ctrl.Result{}, fmt.Errorf("failed deleting stale run %s: %w", run.Name, err)
					}
					continue
				}
				logger.Info("deleted stale batch run",
					"comparison", comp.Name,
					"run", run.Name,
					"oldCommit", run.Spec.Repo.Commit,
				)
			}
		}

		msg := ""
		if cRun == nil {
			msg = "waiting for candidate run"
		}

		if _, err := r.updateComparisonStatus(ctx, &comp, bRun, cRun, func(status *v1alpha1.ComparisonStatus) {
			// Reset all scoring state for the new head commit while preserving reporting.commentID to edit in place.
			status.Phase = v1alpha1.ComparisonPhaseRunning
			status.CompletionTime = nil
			status.Verdict = v1alpha1.ComparisonVerdictRunning
			status.Summary = nil
			status.Batches = nil
			status.Tasks = nil
			status.ExtraSamplesApplied = 0
			status.CostUSD = nil
			status.PossibleSamples = 0
			status.PImprovement = nil
			status.SummaryPath = ""
			status.Conditions = nil
			status.Message = msg
			status.ObservedCandidateRun = comp.Spec.Candidate.Run
			status.ObservedBaselineRun = comp.Spec.Baseline.Run
			if status.Reporting != nil {
				status.Reporting.LastPostedHash = ""
			}
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed resetting comparison status for %s: %w", comp.Name, err)
		}

		if cRun == nil {
			return ctrl.Result{}, nil
		}

		if err := r.Get(ctx, req.NamespacedName, &comp); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Process pending review comment commands (e.g. /eval more <n>)
	if len(comp.Status.PendingCommands) > 0 {
		if err := r.processPendingCommands(ctx, &comp); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed processing pending commands on comparison %s: %w", comp.Name, err)
		}
	}

	// Check TTL expiration for completed comparisons
	ttl := r.comparisonTTL()
	if ttl > 0 && comp.Status.Phase == v1alpha1.ComparisonPhaseCompleted && comp.Spec.ExtraSamples <= comp.Status.ExtraSamplesApplied && len(comp.Status.PendingCommands) == 0 {
		if age, ok := r.comparisonAge(&comp); ok && age >= ttl {
			logger.Info("deleting completed comparison past TTL", "comparison", comp.Name, "age", age, "ttl", ttl)
			if err := r.Delete(ctx, &comp); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("failed to delete expired comparison %s: %w", comp.Name, err)
			}
			return ctrl.Result{}, nil
		}
	}

	if isTerminalComparisonPhase(comp.Status.Phase) {
		// If extra samples were requested (e.g. via /eval more), the comparison has work
		// to evaluate and is no longer terminal; proceed to batch execution.
		if comp.Spec.ExtraSamples <= comp.Status.ExtraSamplesApplied {
			// Check if GitHub reporting is still pending for this terminal comparison.
			// Only skip if reporting is disabled or a permanent failure message was recorded.
			needsReporting := comp.Spec.PullRequest != nil &&
				(comp.Status.Reporting == nil || comp.Status.Reporting.Message == "")
			if needsReporting {
				if comp.Status.Reporting != nil && comp.Status.Reporting.NextAttemptAt != nil && comp.Status.Reporting.NextAttemptAt.After(r.now()) {
					// Time-gated: not yet eligible for retry; do not consume an attempt.
				} else {
					var bRun, cRun v1alpha1.Run
					baseErr := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: comp.Spec.Baseline.Run}, &bRun)
					candErr := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: comp.Spec.Candidate.Run}, &cRun)
					if baseErr == nil && candErr == nil {
						prevReporting := comp.Status.Reporting.DeepCopy()
						reportErr := r.reportToGitHub(ctx, &comp, &bRun, &cRun)
						if !equality.Semantic.DeepEqual(prevReporting, comp.Status.Reporting) {
							reportingStatus := comp.Status.Reporting.DeepCopy()
							key := types.NamespacedName{Namespace: comp.Namespace, Name: comp.Name}
							if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
								var cur v1alpha1.Comparison
								if err := r.Get(ctx, key, &cur); err != nil {
									return err
								}
								cur.Status.Reporting = reportingStatus
								return r.Status().Update(ctx, &cur)
							}); err != nil {
								return ctrl.Result{}, fmt.Errorf("failed to update reporting status for comparison %s: %w", comp.Name, err)
							}
						}
						if reportErr != nil {
							return ctrl.Result{}, reportErr
						}
					} else {
						if baseErr != nil && !apierrors.IsNotFound(baseErr) {
							return ctrl.Result{}, fmt.Errorf("failed to get baseline run %s: %w", comp.Spec.Baseline.Run, baseErr)
						}
						if candErr != nil && !apierrors.IsNotFound(candErr) {
							return ctrl.Result{}, fmt.Errorf("failed to get candidate run %s: %w", comp.Spec.Candidate.Run, candErr)
						}
					}
				}
			}

			var result ctrl.Result
			if ttl := r.comparisonTTL(); ttl > 0 {
				if age, ok := r.comparisonAge(&comp); ok && age < ttl {
					result.RequeueAfter = ttl - age
				}
			}
			if wait, hasPending := r.earliestPendingCommandWait(&comp); hasPending {
				if result.RequeueAfter == 0 || wait < result.RequeueAfter {
					result.RequeueAfter = wait
				}
			}
			if wait, hasReporting := r.earliestReportingWait(&comp); hasReporting {
				if result.RequeueAfter == 0 || wait < result.RequeueAfter {
					result.RequeueAfter = wait
				}
			}
			return result, nil
		}
	}

	baselineRunKey := types.NamespacedName{
		Namespace: comp.Namespace,
		Name:      comp.Spec.Baseline.Run,
	}
	candidateRunKey := types.NamespacedName{
		Namespace: comp.Namespace,
		Name:      comp.Spec.Candidate.Run,
	}

	var baselineRun v1alpha1.Run
	errBase := r.Get(ctx, baselineRunKey, &baselineRun)

	var candidateRun v1alpha1.Run
	errCand := r.Get(ctx, candidateRunKey, &candidateRun)

	if apierrors.IsNotFound(errBase) || apierrors.IsNotFound(errCand) {
		var missing []string
		if apierrors.IsNotFound(errBase) {
			missing = append(missing, fmt.Sprintf("baseline run %q", comp.Spec.Baseline.Run))
		}
		if apierrors.IsNotFound(errCand) {
			missing = append(missing, fmt.Sprintf("candidate run %q", comp.Spec.Candidate.Run))
		}
		msg := fmt.Sprintf("Waiting for %s to be created", strings.Join(missing, " and "))
		if apierrors.IsNotFound(errCand) && !apierrors.IsNotFound(errBase) {
			msg = "waiting for candidate run"
		}
		var bRun *v1alpha1.Run
		if errBase == nil {
			bRun = &baselineRun
		}
		var cRun *v1alpha1.Run
		if errCand == nil {
			cRun = &candidateRun
		}
		return r.updateComparisonStatus(ctx, &comp, bRun, cRun, func(status *v1alpha1.ComparisonStatus) {
			status.Phase = v1alpha1.ComparisonPhaseRunning
			status.CompletionTime = nil
			status.Verdict = v1alpha1.ComparisonVerdictRunning
			status.Summary = &v1alpha1.ComparisonSummary{Tasks: "0/0"}
			status.Message = msg
		})
	}
	if errBase != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get baseline run: %w", errBase)
	}
	if errCand != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get candidate run: %w", errCand)
	}

	seedHasWindow := candidateRun.Spec.SampleRange != nil || (candidateRun.Spec.Limit != nil && *candidateRun.Spec.Limit > 0) ||
		baselineRun.Spec.SampleRange != nil || (baselineRun.Spec.Limit != nil && *baselineRun.Spec.Limit > 0)

	// Build or restore batches list
	var batches []v1alpha1.ComparisonBatchStatus
	if len(comp.Status.Batches) > 0 {
		batches = append([]v1alpha1.ComparisonBatchStatus(nil), comp.Status.Batches...)
	} else {
		batches = []v1alpha1.ComparisonBatchStatus{
			{
				Index:        1,
				CandidateRun: comp.Spec.Candidate.Run,
				BaselineRun:  comp.Spec.Baseline.Run,
			},
		}
	}

	// Check if any subsequent batch Runs already exist on the cluster (for idempotency and state recovery)
	for k := len(batches) + 1; ; k++ {
		candName := fmt.Sprintf("%s-b%d-cand", comp.Name, k)
		baseName := fmt.Sprintf("%s-b%d-base", comp.Name, k)
		var cRun, bRun v1alpha1.Run
		if err := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: candName}, &cRun); err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("failed looking up batch run %q: %w", candName, err)
			}
			break
		}
		if candidateRun.Spec.Repo.Commit != "" && cRun.Spec.Repo.Commit != candidateRun.Spec.Repo.Commit {
			break
		}
		if cRun.DeletionTimestamp != nil {
			break
		}
		if err := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: baseName}, &bRun); err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("failed looking up batch run %q: %w", baseName, err)
			}
			// Baseline may have been reused from another Comparison (batch-key label).
			if cRun.Spec.SampleRange != nil {
				reused, lerr := r.findCompletedBaselineRun(ctx, comp.Namespace, baselineRun.Spec.Repo.URL, baselineRun.Spec.Repo.Commit, baselineRun.Spec.Model, cRun.Spec.Tasks, *cRun.Spec.SampleRange)
				if lerr != nil {
					return ctrl.Result{}, lerr
				}
				if reused != nil {
					baseName = reused.Name
					if addErr := r.addOwnerReferenceToRun(ctx, reused, &comp); addErr != nil {
						return ctrl.Result{}, fmt.Errorf("failed to add owner reference to reused baseline run %q: %w", reused.Name, addErr)
					}
				}
			}
		} else if bRun.DeletionTimestamp != nil {
			break
		}
		batches = append(batches, v1alpha1.ComparisonBatchStatus{
			Index:        k,
			SampleRange:  cRun.Spec.SampleRange,
			CandidateRun: candName,
			BaselineRun:  baseName,
		})
	}

	type batchRunPair struct {
		candRun *v1alpha1.Run
		baseRun *v1alpha1.Run
	}
	batchRuns := make([]batchRunPair, len(batches))
	allBatchesTerminal := true
	var missingBatchRuns []string
	var failedBatchRuns []string

	for i := range batches {
		var bCand, bBase v1alpha1.Run
		batchFailed := false
		if i == 0 {
			bCand = candidateRun
			bBase = baselineRun
		} else {
			cErr := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: batches[i].CandidateRun}, &bCand)
			if cErr != nil {
				if apierrors.IsNotFound(cErr) {
					// Missing batch Run is treated as Failed (terminal)
					bCand = v1alpha1.Run{
						ObjectMeta: metav1.ObjectMeta{Name: batches[i].CandidateRun, Namespace: comp.Namespace},
						Status:     v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseFailed},
					}
					batchFailed = true
					missingBatchRuns = append(missingBatchRuns, batches[i].CandidateRun)
				} else {
					return ctrl.Result{}, fmt.Errorf("failed to get candidate batch run %q: %w", batches[i].CandidateRun, cErr)
				}
			}

			bErr := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: batches[i].BaselineRun}, &bBase)
			if bErr != nil {
				if apierrors.IsNotFound(bErr) {
					// Missing batch Run is treated as Failed (terminal)
					bBase = v1alpha1.Run{
						ObjectMeta: metav1.ObjectMeta{Name: batches[i].BaselineRun, Namespace: comp.Namespace},
						Status:     v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseFailed},
					}
					batchFailed = true
					missingBatchRuns = append(missingBatchRuns, batches[i].BaselineRun)
				} else {
					return ctrl.Result{}, fmt.Errorf("failed to get baseline batch run %q: %w", batches[i].BaselineRun, bErr)
				}
			}

			if bCand.Status.Phase == v1alpha1.RunPhaseFailed && !containsTask(missingBatchRuns, batches[i].CandidateRun) && !containsTask(failedBatchRuns, batches[i].CandidateRun) {
				failedBatchRuns = append(failedBatchRuns, batches[i].CandidateRun)
			}
			if bBase.Status.Phase == v1alpha1.RunPhaseFailed && !containsTask(missingBatchRuns, batches[i].BaselineRun) && !containsTask(failedBatchRuns, batches[i].BaselineRun) {
				failedBatchRuns = append(failedBatchRuns, batches[i].BaselineRun)
			}
		}

		batchRuns[i] = batchRunPair{candRun: &bCand, baseRun: &bBase}

		if batches[i].SampleRange == nil {
			if sr := effectiveSampleRange(bCand.Spec, 0); sr != nil {
				batches[i].SampleRange = sr
			} else if sr := effectiveSampleRange(bBase.Spec, 0); sr != nil {
				batches[i].SampleRange = sr
			}
		}

		termC := isTerminalRunPhase(bCand.Status.Phase)
		termB := isTerminalRunPhase(bBase.Status.Phase)
		if batchFailed || (termC && termB) {
			batches[i].Phase = v1alpha1.ComparisonPhaseCompleted
		} else {
			batches[i].Phase = v1alpha1.ComparisonPhaseRunning
			allBatchesTerminal = false
		}
	}

	// If any batch Run is not terminal, the Comparison is RUNNING and re-evaluates on Run updates
	if !allBatchesTerminal {
		aff := r.resolveAffinity(ctx, &comp, &candidateRun, &baselineRun)
		allTaskIDs := determineRunningTaskIDs(&comp, &baselineRun, &candidateRun)
		taskIDs := allTaskIDs
		if aff.active {
			if aff.mode == configkeys.AffinityModePrune {
				var kept []string
				for _, id := range allTaskIDs {
					if aff.taskAffinity[id] != v1alpha1.ComparisonTaskAffinityUnmatched {
						kept = append(kept, id)
					}
				}
				taskIDs = kept
			} else {
				taskIDs = r.sortTasksByAffinity(allTaskIDs, aff.taskAffinity)
			}
		}

		runningTasks := make(map[string]bool)
		var runningBatchIndex int
		for i, b := range batches {
			if b.Phase == v1alpha1.ComparisonPhaseRunning {
				runningBatchIndex = b.Index
				bCand := batchRuns[i].candRun
				if bCand != nil && len(bCand.Spec.Tasks) > 0 {
					for _, t := range bCand.Spec.Tasks {
						runningTasks[t] = true
					}
				} else {
					for _, id := range taskIDs {
						runningTasks[id] = true
					}
				}
			}
		}

		var tasks []v1alpha1.ComparisonTaskStatus
		if len(comp.Status.Tasks) > 0 {
			tasks = make([]v1alpha1.ComparisonTaskStatus, len(comp.Status.Tasks))
			copy(tasks, comp.Status.Tasks)
			for i := range tasks {
				if runningTasks[tasks[i].ID] {
					tasks[i].Label = v1alpha1.ComparisonTaskLabelRunning
				} else if tasks[i].Decision != "" && tasks[i].Decision != v1alpha1.ComparisonTaskLabelContinue {
					tasks[i].Label = tasks[i].Decision
				}
				if aff.active {
					if a, ok := aff.taskAffinity[tasks[i].ID]; ok {
						tasks[i].Affinity = a
					} else if tasks[i].Affinity == "" {
						tasks[i].Affinity = v1alpha1.ComparisonTaskAffinityAlways
					}
					if aff.mode == configkeys.AffinityModePrune && tasks[i].Affinity == v1alpha1.ComparisonTaskAffinityUnmatched {
						tasks[i].Label = v1alpha1.ComparisonTaskLabelSkipped
						tasks[i].Decision = v1alpha1.ComparisonTaskLabelSkipped
					}
				}
			}
			if aff.active {
				var matchedAlways []v1alpha1.ComparisonTaskStatus
				var unmatched []v1alpha1.ComparisonTaskStatus
				for _, t := range tasks {
					if t.Affinity == v1alpha1.ComparisonTaskAffinityUnmatched {
						unmatched = append(unmatched, t)
					} else {
						matchedAlways = append(matchedAlways, t)
					}
				}
				tasks = append(matchedAlways, unmatched...)
			}
		} else {
			for _, id := range allTaskIDs {
				taskStatus := v1alpha1.ComparisonTaskStatus{
					ID:       id,
					Label:    v1alpha1.ComparisonTaskLabelRunning,
					Decision: v1alpha1.ComparisonTaskLabelContinue,
				}
				if aff.active {
					if a, ok := aff.taskAffinity[id]; ok {
						taskStatus.Affinity = a
					} else {
						taskStatus.Affinity = v1alpha1.ComparisonTaskAffinityAlways
					}
					if aff.mode == configkeys.AffinityModePrune && taskStatus.Affinity == v1alpha1.ComparisonTaskAffinityUnmatched {
						taskStatus.Label = v1alpha1.ComparisonTaskLabelSkipped
						taskStatus.Decision = v1alpha1.ComparisonTaskLabelSkipped
					}
				}
				tasks = append(tasks, taskStatus)
			}
			if aff.active {
				var matchedAlways []v1alpha1.ComparisonTaskStatus
				var unmatched []v1alpha1.ComparisonTaskStatus
				for _, t := range tasks {
					if t.Affinity == v1alpha1.ComparisonTaskAffinityUnmatched {
						unmatched = append(unmatched, t)
					} else {
						matchedAlways = append(matchedAlways, t)
					}
				}
				tasks = append(matchedAlways, unmatched...)
			}
		}

		completedTasks := 0
		prunedTasksCount := 0
		for _, t := range tasks {
			if t.Label == v1alpha1.ComparisonTaskLabelSkipped && t.Affinity == v1alpha1.ComparisonTaskAffinityUnmatched {
				prunedTasksCount++
			} else if t.Label != v1alpha1.ComparisonTaskLabelRunning {
				completedTasks++
			}
		}

		msg := "Runs are still executing"
		if comp.Status.Message != "" && (strings.HasPrefix(comp.Status.Message, "Running batch") || strings.HasPrefix(comp.Status.Message, "Running extra batch")) {
			msg = comp.Status.Message
		} else if len(batches) > 1 && runningBatchIndex > 0 {
			msg = fmt.Sprintf("Running batch %d (%d tasks continuing)", runningBatchIndex, len(runningTasks))
		}

		return r.updateComparisonStatus(ctx, &comp, &baselineRun, &candidateRun, func(status *v1alpha1.ComparisonStatus) {
			status.Phase = v1alpha1.ComparisonPhaseRunning
			status.CompletionTime = nil
			status.Verdict = v1alpha1.ComparisonVerdictRunning
			status.Batches = batches
			status.ObservedCandidateRun = comp.Spec.Candidate.Run
			status.ObservedBaselineRun = comp.Spec.Baseline.Run
			status.Tasks = tasks
			status.Summary = &v1alpha1.ComparisonSummary{
				Tasks:   fmt.Sprintf("%d/%d", completedTasks, len(tasks)-prunedTasksCount),
				Skipped: prunedTasksCount,
			}
			if aff.active {
				status.Affinity = aff.status
				status.Summary.AffinityMatched = aff.matchedCount
			}
			status.Message = msg
		})
	}

	// All current batch Runs are terminal; perform pairing and comparison
	aff := r.resolveAffinity(ctx, &comp, &candidateRun, &baselineRun)

	taskIDs, unexecuted, candFailed := determineCompletedTaskIDs(&comp, &baselineRun, &candidateRun)
	skippedNotPlanned := determineSkippedTaskIDs(&comp, &baselineRun, &candidateRun)

	var prunedTasks []string
	if aff.active {
		if aff.mode == configkeys.AffinityModePrune {
			var kept []string
			for _, id := range taskIDs {
				if aff.taskAffinity[id] == v1alpha1.ComparisonTaskAffinityUnmatched {
					prunedTasks = append(prunedTasks, id)
				} else {
					kept = append(kept, id)
				}
			}
			taskIDs = kept
		} else {
			taskIDs = r.sortTasksByAffinity(taskIDs, aff.taskAffinity)
		}
	}

	if batches[0].SampleRange == nil {
		maxObs := 0
		for _, t := range candidateRun.Status.Tasks {
			if t.Samples > maxObs {
				maxObs = t.Samples
			}
		}
		for _, t := range baselineRun.Status.Tasks {
			if t.Samples > maxObs {
				maxObs = t.Samples
			}
		}
		if maxObs > 0 {
			batches[0].SampleRange = &v1alpha1.SampleRange{Start: 1, End: maxObs}
		}
	}

	margin := stats.DefaultEquivalenceMargin
	if comp.Spec.EquivalenceMargin != nil {
		margin = *comp.Spec.EquivalenceMargin
	}

	maxSamples := 64
	if comp.Spec.Sampling != nil {
		maxSamples = comp.Spec.Sampling.EffectiveMaxSamplesPerTask()
	}

	type taskEval struct {
		taskID          string
		candScores      []stats.SampleScore
		baseScores      []stats.SampleScore
		candTokens      *v1alpha1.TaskTokens
		baseTokens      *v1alpha1.TaskTokens
		tokens          *v1alpha1.TaskTokens
		latency         *v1alpha1.TaskLatency
		candLatency     *v1alpha1.TaskLatency
		baseLatency     *v1alpha1.TaskLatency
		costUSD         *float64
		costSource      v1alpha1.CostSource
		candCostUSD     *float64
		baseCostUSD     *float64
		possibleSamples int
		exhausted       bool
		failed          bool
		budgetStopped   bool
		est             stats.PairedEstimate
		dec             stats.Decision
		suggestedNext   int
		samplesRun      int
	}

	var taskEvals []taskEval

	for _, taskID := range taskIDs {
		var allCandScores []stats.SampleScore
		var allBaseScores []stats.SampleScore
		var allCandLatencies []float64
		var allBaseLatencies []float64
		var allTaskLatencies []float64
		taskPossibleSamples := 0
		candInputSum, candOutputSum := 0, 0
		candReasoningSum, candCacheReadSum, candCacheWriteSum := 0, 0, 0
		hasCandTokens := false
		baseInputSum, baseOutputSum := 0, 0
		baseReasoningSum, baseCacheReadSum, baseCacheWriteSum := 0, 0, 0
		hasBaseTokens := false
		taskExhausted := false
		taskFailed := false

		for batchIdx, pair := range batchRuns {
			if pair.candRun == nil || pair.baseRun == nil {
				continue
			}

			// Check if this batch intended to run taskID
			taskInBatch := runIncludesTask(pair.candRun, taskID) || runIncludesTask(pair.baseRun, taskID)
			if !taskInBatch {
				// Task was not part of this batch (e.g. stopped in an earlier batch)
				continue
			}

			// If the task was part of this batch, check if either Run failed or task status is missing
			candTask := findTaskStatus(pair.candRun.Status.Tasks, taskID)
			baseTask := findTaskStatus(pair.baseRun.Status.Tasks, taskID)

			if pair.candRun.Status.Phase == v1alpha1.RunPhaseFailed ||
				pair.baseRun.Status.Phase == v1alpha1.RunPhaseFailed ||
				candTask == nil || baseTask == nil ||
				candTask.Phase != v1alpha1.TaskPhaseCompleted ||
				baseTask.Phase != v1alpha1.TaskPhaseCompleted ||
				candTask.LogPath == "" || baseTask.LogPath == "" {
				// A candidate-side task failure in any batch caps the final verdict (#141),
				// exactly as a seed-Run failure does via determineCompletedTaskIDs.
				if candTask != nil && candTask.Phase == v1alpha1.TaskPhaseFailed &&
					baseTask != nil && baseTask.Phase == v1alpha1.TaskPhaseCompleted &&
					!containsTask(candFailed, taskID) {
					candFailed = append(candFailed, taskID)
				}
				if (candTask != nil && candTask.Message == "no scored samples") ||
					(baseTask != nil && baseTask.Message == "no scored samples") {
					taskExhausted = true
				}
				taskFailed = true
				continue
			}

			// Verify that log paths match the commit prefixes of the respective runs
			candPrefix := r.artifactSink().ArtifactPrefix(pair.candRun.Spec.Repo.Commit)
			basePrefix := r.artifactSink().ArtifactPrefix(pair.baseRun.Spec.Repo.Commit)

			candMismatch := candPrefix != "" && !strings.HasPrefix(candTask.LogPath, candPrefix)
			baseMismatch := basePrefix != "" && !strings.HasPrefix(baseTask.LogPath, basePrefix)

			if candMismatch || baseMismatch {
				if batchIdx > 0 {
					if candMismatch {
						logger.V(1).Info("candidate batch run log path does not match sink artifact prefix; treating batch as failed", "run", pair.candRun.Name, "task", taskID, "logPath", candTask.LogPath, "expectedPrefix", candPrefix)
						if !containsTask(failedBatchRuns, pair.candRun.Name) {
							failedBatchRuns = append(failedBatchRuns, pair.candRun.Name)
						}
						pair.candRun.Status.Phase = v1alpha1.RunPhaseFailed
					}
					if baseMismatch {
						logger.V(1).Info("baseline batch run log path does not match sink artifact prefix; treating batch as failed", "run", pair.baseRun.Name, "task", taskID, "logPath", baseTask.LogPath, "expectedPrefix", basePrefix)
						if !containsTask(failedBatchRuns, pair.baseRun.Name) {
							failedBatchRuns = append(failedBatchRuns, pair.baseRun.Name)
						}
						pair.baseRun.Status.Phase = v1alpha1.RunPhaseFailed
					}
					batches[batchIdx].Phase = v1alpha1.ComparisonPhaseCompleted
					taskFailed = true
					continue
				}

				var mismatchMsg string
				if candMismatch {
					mismatchMsg = fmt.Sprintf("candidate log path %q does not match commit prefix %q", candTask.LogPath, candPrefix)
				} else {
					mismatchMsg = fmt.Sprintf("baseline log path %q does not match commit prefix %q", baseTask.LogPath, basePrefix)
				}
				logger.Error(nil, mismatchMsg, "task", taskID)

				if isTerminalComparisonPhase(comp.Status.Phase) {
					return r.recordUnserviceableExtraSamples(ctx, &comp, &baselineRun, &candidateRun, "artifact prefix mismatch", v1alpha1.ConditionReasonExtraSamplesArtifactMismatch, mismatchMsg)
				}
				return r.completeInconclusiveArtifactsUnreadable(ctx, &comp, &baselineRun, &candidateRun, batches, v1alpha1.ConditionReasonPrefixMismatch, mismatchMsg)
			}

			// Load Inspect log for candidate
			candCtx, candCancel := context.WithTimeout(ctx, r.artifactSinkTimeout())
			candData, err := artifacts.LoadWithTimeout(candCtx, r.artifactSink(), candTask.LogPath)
			candCancel()
			if err != nil {
				if artifacts.IsNotFound(err) {
					if batchIdx > 0 {
						logger.V(1).Info("candidate batch run log not found in artifact sink; treating batch as failed", "run", pair.candRun.Name, "task", taskID, "logPath", candTask.LogPath, "error", err)
						if !containsTask(failedBatchRuns, pair.candRun.Name) {
							failedBatchRuns = append(failedBatchRuns, pair.candRun.Name)
						}
						pair.candRun.Status.Phase = v1alpha1.RunPhaseFailed
						batches[batchIdx].Phase = v1alpha1.ComparisonPhaseCompleted
						taskFailed = true
						continue
					}
					msg := fmt.Sprintf("failed to load candidate log for task %q (%s): %v", taskID, candTask.LogPath, err)
					logger.Error(err, "failed to load candidate log", "task", taskID, "path", candTask.LogPath)
					if isTerminalComparisonPhase(comp.Status.Phase) {
						return r.recordUnserviceableExtraSamples(ctx, &comp, &baselineRun, &candidateRun, "artifact not found", v1alpha1.ConditionReasonExtraSamplesArtifactNotFound, msg)
					}
					return r.completeInconclusiveArtifactsUnreadable(ctx, &comp, &baselineRun, &candidateRun, batches, v1alpha1.ConditionReasonNotFound, msg)
				}
				msg := fmt.Sprintf("failed to load candidate log for task %q (%s): %v", taskID, candTask.LogPath, err)
				logger.Error(err, "failed to load candidate log", "task", taskID, "path", candTask.LogPath)
				if _, statusErr := r.updateComparisonStatus(ctx, &comp, &baselineRun, &candidateRun, func(status *v1alpha1.ComparisonStatus) {
					status.Message = msg
				}); statusErr != nil {
					return ctrl.Result{}, errors.Join(err, statusErr)
				}
				return ctrl.Result{}, err
			}

			// Load Inspect log for baseline
			baseCtx, baseCancel := context.WithTimeout(ctx, r.artifactSinkTimeout())
			baseData, err := artifacts.LoadWithTimeout(baseCtx, r.artifactSink(), baseTask.LogPath)
			baseCancel()
			if err != nil {
				if artifacts.IsNotFound(err) {
					if batchIdx > 0 {
						logger.V(1).Info("baseline batch run log not found in artifact sink; treating batch as failed", "run", pair.baseRun.Name, "task", taskID, "logPath", baseTask.LogPath, "error", err)
						if !containsTask(failedBatchRuns, pair.baseRun.Name) {
							failedBatchRuns = append(failedBatchRuns, pair.baseRun.Name)
						}
						pair.baseRun.Status.Phase = v1alpha1.RunPhaseFailed
						batches[batchIdx].Phase = v1alpha1.ComparisonPhaseCompleted
						taskFailed = true
						continue
					}
					msg := fmt.Sprintf("failed to load baseline log for task %q (%s): %v", taskID, baseTask.LogPath, err)
					logger.Error(err, "failed to load baseline log", "task", taskID, "path", baseTask.LogPath)
					if isTerminalComparisonPhase(comp.Status.Phase) {
						return r.recordUnserviceableExtraSamples(ctx, &comp, &baselineRun, &candidateRun, "artifact not found", v1alpha1.ConditionReasonExtraSamplesArtifactNotFound, msg)
					}
					return r.completeInconclusiveArtifactsUnreadable(ctx, &comp, &baselineRun, &candidateRun, batches, v1alpha1.ConditionReasonNotFound, msg)
				}
				msg := fmt.Sprintf("failed to load baseline log for task %q (%s): %v", taskID, baseTask.LogPath, err)
				logger.Error(err, "failed to load baseline log", "task", taskID, "path", baseTask.LogPath)
				if _, statusErr := r.updateComparisonStatus(ctx, &comp, &baselineRun, &candidateRun, func(status *v1alpha1.ComparisonStatus) {
					status.Message = msg
				}); statusErr != nil {
					return ctrl.Result{}, errors.Join(err, statusErr)
				}
				return ctrl.Result{}, err
			}

			candScores, err := artifacts.ParseInspectSampleScores(candData)
			if err != nil {
				msg := fmt.Sprintf("failed to parse candidate sample scores for task %q: %v", taskID, err)
				logger.Error(err, "failed to parse candidate sample scores", "task", taskID)
				if _, statusErr := r.updateComparisonStatus(ctx, &comp, &baselineRun, &candidateRun, func(status *v1alpha1.ComparisonStatus) {
					status.Message = msg
				}); statusErr != nil {
					return ctrl.Result{}, errors.Join(err, statusErr)
				}
				return ctrl.Result{}, err
			}
			baseScores, err := artifacts.ParseInspectSampleScores(baseData)
			if err != nil {
				msg := fmt.Sprintf("failed to parse baseline sample scores for task %q: %v", taskID, err)
				logger.Error(err, "failed to parse baseline sample scores", "task", taskID)
				if _, statusErr := r.updateComparisonStatus(ctx, &comp, &baselineRun, &candidateRun, func(status *v1alpha1.ComparisonStatus) {
					status.Message = msg
				}); statusErr != nil {
					return ctrl.Result{}, errors.Join(err, statusErr)
				}
				return ctrl.Result{}, err
			}

			if candTok, err := artifacts.ExtractInspectTokens(candData); err == nil && candTok != nil {
				candInputSum += candTok.Input
				candOutputSum += candTok.Output
				candReasoningSum += candTok.Reasoning
				candCacheReadSum += candTok.CacheRead
				candCacheWriteSum += candTok.CacheWrite
				hasCandTokens = true
			}
			if baseTok, err := artifacts.ExtractInspectTokens(baseData); err == nil && baseTok != nil {
				baseInputSum += baseTok.Input
				baseOutputSum += baseTok.Output
				baseReasoningSum += baseTok.Reasoning
				baseCacheReadSum += baseTok.CacheRead
				baseCacheWriteSum += baseTok.CacheWrite
				hasBaseTokens = true
			}

			if candLats, err := artifacts.ExtractInspectSampleLatencies(candData); err == nil && len(candLats) > 0 {
				allCandLatencies = append(allCandLatencies, candLats...)
				allTaskLatencies = append(allTaskLatencies, candLats...)
			}
			if baseLats, err := artifacts.ExtractInspectSampleLatencies(baseData); err == nil && len(baseLats) > 0 {
				allBaseLatencies = append(allBaseLatencies, baseLats...)
				allTaskLatencies = append(allTaskLatencies, baseLats...)
			}

			if ds, err := artifacts.ExtractInspectDatasetSamples(candData); err == nil && ds > taskPossibleSamples {
				taskPossibleSamples = ds
			}
			if ds, err := artifacts.ExtractInspectDatasetSamples(baseData); err == nil && ds > taskPossibleSamples {
				taskPossibleSamples = ds
			}

			// Check window length for dataset exhaustion:
			// If seeds had no window, batch 1 is the whole dataset: treat as exhausted and do not batch past end.
			if batchIdx == 0 && !seedHasWindow {
				taskExhausted = true
			} else {
				batchRange := batches[batchIdx].SampleRange
				if batchRange != nil {
					winLen := batchRange.End - batchRange.Start + 1
					if len(candScores) < winLen || len(baseScores) < winLen {
						taskExhausted = true
					}
				} else {
					taskExhausted = true
				}
			}

			allCandScores = append(allCandScores, candScores...)
			allBaseScores = append(allBaseScores, baseScores...)
		}

		if taskPossibleSamples > 0 && (len(allCandScores) >= taskPossibleSamples || len(allBaseScores) >= taskPossibleSamples) {
			taskExhausted = true
		}

		var totalCandTokens *v1alpha1.TaskTokens
		if hasCandTokens {
			totalCandTokens = &v1alpha1.TaskTokens{
				Input:      candInputSum,
				Output:     candOutputSum,
				Total:      candInputSum + candOutputSum,
				Reasoning:  candReasoningSum,
				CacheRead:  candCacheReadSum,
				CacheWrite: candCacheWriteSum,
			}
		}
		var totalBaseTokens *v1alpha1.TaskTokens
		if hasBaseTokens {
			totalBaseTokens = &v1alpha1.TaskTokens{
				Input:      baseInputSum,
				Output:     baseOutputSum,
				Total:      baseInputSum + baseOutputSum,
				Reasoning:  baseReasoningSum,
				CacheRead:  baseCacheReadSum,
				CacheWrite: baseCacheWriteSum,
			}
		}
		var totalTokens *v1alpha1.TaskTokens
		if hasCandTokens || hasBaseTokens {
			totalTokens = &v1alpha1.TaskTokens{
				Input:      candInputSum + baseInputSum,
				Output:     candOutputSum + baseOutputSum,
				Total:      (candInputSum + baseInputSum) + (candOutputSum + baseOutputSum),
				Reasoning:  candReasoningSum + baseReasoningSum,
				CacheRead:  candCacheReadSum + baseCacheReadSum,
				CacheWrite: candCacheWriteSum + baseCacheWriteSum,
			}
		}

		var taskLatency *v1alpha1.TaskLatency
		if lat := artifacts.CalculateLatencyPercentiles(allTaskLatencies); lat != nil {
			taskLatency = &v1alpha1.TaskLatency{P50: lat.P50, P95: lat.P95}
		}
		var candLatency *v1alpha1.TaskLatency
		if lat := artifacts.CalculateLatencyPercentiles(allCandLatencies); lat != nil {
			candLatency = &v1alpha1.TaskLatency{P50: lat.P50, P95: lat.P95}
		}
		var baseLatency *v1alpha1.TaskLatency
		if lat := artifacts.CalculateLatencyPercentiles(allBaseLatencies); lat != nil {
			baseLatency = &v1alpha1.TaskLatency{P50: lat.P50, P95: lat.P95}
		}

		candPrice, candSrc, candPriceKnown := r.lookupModelPrice(candidateRun.Spec.Model)
		basePrice, baseSrc, basePriceKnown := r.lookupModelPrice(baselineRun.Spec.Model)

		var candCost *float64
		if hasCandTokens && candPriceKnown {
			c := (float64(candInputSum)*candPrice.InputPer1M + float64(candOutputSum)*candPrice.OutputPer1M) / 1_000_000.0
			candCost = &c
		}
		var baseCost *float64
		if hasBaseTokens && basePriceKnown {
			b := (float64(baseInputSum)*basePrice.InputPer1M + float64(baseOutputSum)*basePrice.OutputPer1M) / 1_000_000.0
			baseCost = &b
		}
		var taskCost *float64
		var costSource v1alpha1.CostSource
		if (hasCandTokens || hasBaseTokens) &&
			(!hasCandTokens || candCost != nil) &&
			(!hasBaseTokens || baseCost != nil) {
			tot := 0.0
			if candCost != nil {
				tot += *candCost
			}
			if baseCost != nil {
				tot += *baseCost
			}
			taskCost = &tot
			if candCost != nil && baseCost != nil {
				if candSrc == v1alpha1.CostSourceDefault || baseSrc == v1alpha1.CostSourceDefault {
					costSource = v1alpha1.CostSourceDefault
				} else {
					costSource = v1alpha1.CostSourceConfigMap
				}
			} else if candCost != nil {
				costSource = candSrc
			} else if baseCost != nil {
				costSource = baseSrc
			}
		}

		est := stats.ComparePaired(allCandScores, allBaseScores, 0.95)
		samplesRun := est.NumPairs
		if samplesRun == 0 && len(allCandScores) > 0 {
			samplesRun = len(allCandScores)
		}

		taskEvals = append(taskEvals, taskEval{
			taskID:          taskID,
			candScores:      allCandScores,
			baseScores:      allBaseScores,
			candTokens:      totalCandTokens,
			baseTokens:      totalBaseTokens,
			tokens:          totalTokens,
			latency:         taskLatency,
			candLatency:     candLatency,
			baseLatency:     baseLatency,
			costUSD:         taskCost,
			costSource:      costSource,
			candCostUSD:     candCost,
			baseCostUSD:     baseCost,
			possibleSamples: taskPossibleSamples,
			exhausted:       taskExhausted,
			failed:          taskFailed,
			est:             est,
			samplesRun:      samplesRun,
		})
	}

	spentSoFar := 0.0
	for _, te := range taskEvals {
		if te.costUSD != nil {
			spentSoFar += *te.costUSD
		}
	}

	budgetLimit := 0.0
	if comp.Spec.BudgetUSD != nil && *comp.Spec.BudgetUSD > 0 {
		budgetLimit = *comp.Spec.BudgetUSD
	}

	budgetEnforced := true
	budgetNote := ""
	if budgetLimit > 0 {
		_, _, candPriceKnown := r.lookupModelPrice(candidateRun.Spec.Model)
		_, _, basePriceKnown := r.lookupModelPrice(baselineRun.Spec.Model)
		if candidateRun.Spec.Model != "" && !candPriceKnown {
			budgetEnforced = false
			budgetNote = fmt.Sprintf("cost unknown for %s; budget not enforced", candidateRun.Spec.Model)
		} else if baselineRun.Spec.Model != "" && !basePriceKnown {
			budgetEnforced = false
			budgetNote = fmt.Sprintf("cost unknown for %s; budget not enforced", baselineRun.Spec.Model)
		}
	}

	budgetVal := 0.0
	spentVal := 0.0
	if budgetEnforced && budgetLimit > 0 {
		budgetVal = budgetLimit
		spentVal = spentSoFar
	}

	// Stopping rule evaluation for all tasks:
	for i := range taskEvals {
		te := &taskEvals[i]
		var dec stats.Decision
		if te.est.NumPairs == 0 {
			dec = stats.Decision{
				Label:  stats.TaskLabelInconclusive,
				Stop:   true,
				Reason: "no matching sample IDs between candidate and baseline",
			}
			te.est.PImprovement = 0.5
		} else if comp.Spec.Sampling == nil {
			// Single comparison (today's behavior): sampleCap = samplesRun
			dec = stats.Decide(te.est, margin, te.samplesRun, te.samplesRun, spentVal, budgetVal)
		} else if te.exhausted {
			dec = stats.Decide(te.est, margin, te.samplesRun, maxSamples, spentVal, budgetVal)
			if dec.Label == stats.TaskLabelContinue {
				dec.Label = stats.TaskLabelInconclusive
				dec.Stop = true
				dec.Reason = "dataset exhausted"
				dec.EstimatedSamples = 0
				dec.NextBatchSize = 0
			}
		} else {
			dec = stats.Decide(te.est, margin, te.samplesRun, maxSamples, spentVal, budgetVal)
			if dec.Label == stats.TaskLabelContinue {
				rem := maxSamples - te.samplesRun
				if rem <= 0 {
					dec.Label = stats.TaskLabelInconclusive
					dec.Stop = true
					dec.Reason = fmt.Sprintf("sample cap (%d) reached without resolving", maxSamples)
				} else if te.failed {
					dec.Label = stats.TaskLabelInconclusive
					dec.Stop = true
					dec.Reason = "task failed"
				}
			}
		}

		suggestedNext := 0
		if !te.exhausted && !te.failed {
			if dec.EstimatedSamples > 0 {
				suggestedNext = dec.EstimatedSamples
			} else if dec.NextBatchSize > 0 {
				suggestedNext = dec.NextBatchSize
			}
		}
		te.dec = dec
		te.suggestedNext = suggestedNext
		if dec.Stop && budgetVal > 0 && spentVal >= budgetVal {
			te.budgetStopped = true
		}
	}

	// Check if any tasks need to continue sampling
	var continueTasks []string
	maxRemaining := 0
	if comp.Spec.Sampling != nil {
		for _, te := range taskEvals {
			if te.dec.Label == stats.TaskLabelContinue && !te.exhausted && !te.failed {
				rem := maxSamples - te.samplesRun
				if rem > 0 {
					continueTasks = append(continueTasks, te.taskID)
					if rem > maxRemaining {
						maxRemaining = rem
					}
				}
			}
		}
	}

	// Schedule next batch if any tasks continue
	if len(continueTasks) > 0 {
		batchSize := comp.Spec.Sampling.EffectiveBatchSize()
		if batchSize > maxRemaining {
			batchSize = maxRemaining
		}

		stopReason := fmt.Sprintf("sample cap (%d) reached without resolving", maxSamples)
		if budgetEnforced && budgetLimit > 0 {
			totalSamplesRun := 0
			for _, te := range taskEvals {
				totalSamplesRun += len(te.candScores) + len(te.baseScores)
			}
			if totalSamplesRun > 0 && spentSoFar > 0 {
				avgCostPerSample := spentSoFar / float64(totalSamplesRun)
				costPerPair := avgCostPerSample * 2.0
				projected := spentSoFar + float64(batchSize)*costPerPair
				if projected > budgetLimit {
					remaining := budgetLimit - spentSoFar
					fit := 0
					if costPerPair > 0 && remaining > 0 {
						fit = int(math.Floor(remaining/costPerPair + 1e-9))
					}
					if fit >= 1 {
						if fit < batchSize {
							batchSize = fit
						}
					} else {
						// Stop! Zero batchSize so the guard below fires and doesn't spawn an unfiltered batch
						batchSize = 0
						stopReason = fmt.Sprintf("budget ($%.2f) reached (spent $%.2f)", budgetLimit, spentSoFar)
						for i := range taskEvals {
							if taskEvals[i].dec.Label == stats.TaskLabelContinue {
								taskEvals[i].budgetStopped = true
							}
						}
						continueTasks = nil
					}
				}
			}
		}

		nextBatchIndex := len(batches) + 1
		lastBatch := batches[len(batches)-1]
		nextStart := 1
		if lastBatch.SampleRange != nil {
			nextStart = lastBatch.SampleRange.End + 1
		}
		nextEnd := nextStart + batchSize - 1

		if batchSize <= 0 || nextStart > nextEnd {
			// No window can be issued; stop continuing tasks as inconclusive
			for i := range taskEvals {
				if taskEvals[i].dec.Label == stats.TaskLabelContinue {
					taskEvals[i].dec.Label = stats.TaskLabelInconclusive
					taskEvals[i].dec.Stop = true
					taskEvals[i].dec.Reason = stopReason
				}
			}
			continueTasks = nil
		} else {
			batchRange := v1alpha1.SampleRange{Start: nextStart, End: nextEnd}

			tasksToRun := continueTasks
			if aff.active {
				tasksToRun = r.applyAffinityToBatchTasks(continueTasks, aff.taskAffinity, aff.mode)
			}

			baseRunName, err := r.ensureBatchRun(ctx, &comp, &baselineRun, RoleBaseline, tasksToRun, batchRange, nextBatchIndex)
			if err != nil {
				if isBatchRunTerminating(err) {
					return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
				}
				return ctrl.Result{}, err
			}

			candCloneName, err := r.ensureBatchRun(ctx, &comp, &candidateRun, RoleCandidate, tasksToRun, batchRange, nextBatchIndex)
			if err != nil {
				if isBatchRunTerminating(err) {
					return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
				}
				return ctrl.Result{}, err
			}

			batches = append(batches, v1alpha1.ComparisonBatchStatus{
				Index:        nextBatchIndex,
				SampleRange:  &batchRange,
				CandidateRun: candCloneName,
				BaselineRun:  baseRunName,
				Phase:        v1alpha1.ComparisonPhaseRunning,
			})

			var partialStatuses []v1alpha1.ComparisonTaskStatus
			for _, te := range taskEvals {
				var pImp *float64
				if te.est.NumPairs > 0 {
					v := te.est.PImprovement
					pImp = &v
				}
				ts := v1alpha1.ComparisonTaskStatus{
					ID:            te.taskID,
					BaseRate:      &te.est.BaseMean,
					CandidateRate: &te.est.CandidateMean,
					Delta:         &te.est.MeanDelta,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: te.est.CI.Lower,
						Upper: te.est.CI.Upper,
					},
					PImprovement: pImp,
					Pairs:        te.est.NumPairs,
					Wins:         te.est.Wins,
					Losses:       te.est.Losses,
					Ties:         te.est.Ties,
					Dropped: v1alpha1.ComparisonDropped{
						Candidate: te.est.DroppedCandidate,
						Baseline:  te.est.DroppedBaseline,
					},
					Label:           toComparisonTaskLabel(te.dec.Label),
					SamplesRun:      te.samplesRun,
					Decision:        v1alpha1.ComparisonTaskLabel(te.dec.Label),
					SuggestedNext:   te.suggestedNext,
					Tokens:          te.tokens,
					Latency:         te.latency,
					CostUSD:         te.costUSD,
					CostSource:      te.costSource,
					PossibleSamples: te.possibleSamples,
				}
				if aff.active {
					if a, ok := aff.taskAffinity[te.taskID]; ok {
						ts.Affinity = a
					} else {
						ts.Affinity = v1alpha1.ComparisonTaskAffinityAlways
					}
				}
				partialStatuses = append(partialStatuses, ts)
			}

			for _, id := range prunedTasks {
				ts := v1alpha1.ComparisonTaskStatus{
					ID:       id,
					Affinity: v1alpha1.ComparisonTaskAffinityUnmatched,
					Label:    v1alpha1.ComparisonTaskLabelSkipped,
					Decision: v1alpha1.ComparisonTaskLabelSkipped,
				}
				partialStatuses = append(partialStatuses, ts)
			}

			var partialCompCost *float64
			partialTotalCompCost := 0.0
			hasAnyPartialCost := false
			allPartialTasksWithTokensHaveCost := true
			partialPossibleSamples := 0
			for _, te := range taskEvals {
				if te.tokens != nil {
					if te.costUSD != nil {
						partialTotalCompCost += *te.costUSD
						hasAnyPartialCost = true
					} else {
						allPartialTasksWithTokensHaveCost = false
					}
				}
				partialPossibleSamples += te.possibleSamples
			}
			if hasAnyPartialCost && allPartialTasksWithTokensHaveCost {
				partialCompCost = &partialTotalCompCost
			}

			batchMsg := fmt.Sprintf("Running batch %d (%d tasks continuing)", nextBatchIndex, len(continueTasks))
			if len(prunedTasks) > 0 {
				sort.Strings(prunedTasks)
				batchMsg = fmt.Sprintf("%s; skipped (unmatched): %s", batchMsg, github.FormatIDList(prunedTasks, github.MaxIDList))
			}

			return r.updateComparisonStatus(ctx, &comp, &baselineRun, &candidateRun, func(status *v1alpha1.ComparisonStatus) {
				status.Phase = v1alpha1.ComparisonPhaseRunning
				status.CompletionTime = nil
				status.Verdict = v1alpha1.ComparisonVerdictRunning
				status.Batches = batches
				status.ObservedCandidateRun = comp.Spec.Candidate.Run
				status.ObservedBaselineRun = comp.Spec.Baseline.Run
				status.Tasks = partialStatuses
				status.CostUSD = partialCompCost
				if partialPossibleSamples > 0 {
					status.PossibleSamples = partialPossibleSamples
				}
				status.Summary = &v1alpha1.ComparisonSummary{
					Tasks:   fmt.Sprintf("%d/%d", len(taskIDs)-len(continueTasks), len(taskIDs)),
					Skipped: len(skippedNotPlanned) + len(prunedTasks),
				}
				if aff.active {
					status.Affinity = aff.status
					status.Summary.AffinityMatched = aff.matchedCount
				}
				status.Message = batchMsg
				if budgetLimit > 0 {
					var spentUSD *float64
					if budgetEnforced {
						spentUSD = &spentSoFar
					}
					status.Budget = &v1alpha1.ComparisonBudgetStatus{
						LimitUSD: comp.Spec.BudgetUSD,
						SpentUSD: spentUSD,
						Enforced: budgetEnforced,
						Note:     budgetNote,
					}
				}
			})
		}
	}

	// Check extraSamples if all tasks have stopped
	extraRequested := comp.Spec.ExtraSamples > comp.Status.ExtraSamplesApplied
	noEligibleExtraTasks := false
	var noEligibleDetailMsg string
	if extraRequested {
		var undecidedTasks []string
		var decidedTasks []string
		for _, te := range taskEvals {
			if te.exhausted || te.failed {
				continue
			}
			if te.dec.Label == stats.TaskLabelInconclusive {
				undecidedTasks = append(undecidedTasks, te.taskID)
			} else {
				decidedTasks = append(decidedTasks, te.taskID)
			}
		}

		var extraTasks []string
		if len(undecidedTasks) > 0 {
			extraTasks = undecidedTasks
		} else {
			extraTasks = decidedTasks
		}

		if len(extraTasks) > 0 {
			nextBatchIndex := len(batches) + 1
			lastBatch := batches[len(batches)-1]
			nextStart := 1
			if lastBatch.SampleRange != nil {
				nextStart = lastBatch.SampleRange.End + 1
			}
			batchSize := 8
			if comp.Spec.Sampling != nil {
				batchSize = comp.Spec.Sampling.EffectiveBatchSize()
			}
			extraRemaining := comp.Spec.ExtraSamples - comp.Status.ExtraSamplesApplied
			if extraRemaining < batchSize {
				batchSize = extraRemaining
			}
			nextEnd := nextStart + batchSize - 1

			batchRange := v1alpha1.SampleRange{Start: nextStart, End: nextEnd}

			tasksToRun := extraTasks
			if aff.active {
				tasksToRun = r.applyAffinityToBatchTasks(extraTasks, aff.taskAffinity, aff.mode)
			}

			baseRunName, err := r.ensureBatchRun(ctx, &comp, &baselineRun, RoleBaseline, tasksToRun, batchRange, nextBatchIndex)
			if err != nil {
				if isBatchRunTerminating(err) {
					return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
				}
				return ctrl.Result{}, err
			}

			candCloneName, err := r.ensureBatchRun(ctx, &comp, &candidateRun, RoleCandidate, tasksToRun, batchRange, nextBatchIndex)
			if err != nil {
				if isBatchRunTerminating(err) {
					return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
				}
				return ctrl.Result{}, err
			}

			batches = append(batches, v1alpha1.ComparisonBatchStatus{
				Index:        nextBatchIndex,
				SampleRange:  &batchRange,
				CandidateRun: candCloneName,
				BaselineRun:  baseRunName,
				Phase:        v1alpha1.ComparisonPhaseRunning,
			})

			newExtraApplied := comp.Status.ExtraSamplesApplied + batchSize

			var partialStatuses []v1alpha1.ComparisonTaskStatus
			for _, te := range taskEvals {
				var pImp *float64
				if te.est.NumPairs > 0 {
					v := te.est.PImprovement
					pImp = &v
				}
				ts := v1alpha1.ComparisonTaskStatus{
					ID:            te.taskID,
					BaseRate:      &te.est.BaseMean,
					CandidateRate: &te.est.CandidateMean,
					Delta:         &te.est.MeanDelta,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: te.est.CI.Lower,
						Upper: te.est.CI.Upper,
					},
					PImprovement: pImp,
					Pairs:        te.est.NumPairs,
					Wins:         te.est.Wins,
					Losses:       te.est.Losses,
					Ties:         te.est.Ties,
					Dropped: v1alpha1.ComparisonDropped{
						Candidate: te.est.DroppedCandidate,
						Baseline:  te.est.DroppedBaseline,
					},
					Label:           toComparisonTaskLabel(te.dec.Label),
					SamplesRun:      te.samplesRun,
					Decision:        v1alpha1.ComparisonTaskLabel(te.dec.Label),
					SuggestedNext:   te.suggestedNext,
					Tokens:          te.tokens,
					Latency:         te.latency,
					CostUSD:         te.costUSD,
					CostSource:      te.costSource,
					PossibleSamples: te.possibleSamples,
				}
				if aff.active {
					if a, ok := aff.taskAffinity[te.taskID]; ok {
						ts.Affinity = a
					} else {
						ts.Affinity = v1alpha1.ComparisonTaskAffinityAlways
					}
				}
				partialStatuses = append(partialStatuses, ts)
			}

			for _, id := range prunedTasks {
				ts := v1alpha1.ComparisonTaskStatus{
					ID:       id,
					Affinity: v1alpha1.ComparisonTaskAffinityUnmatched,
					Label:    v1alpha1.ComparisonTaskLabelSkipped,
					Decision: v1alpha1.ComparisonTaskLabelSkipped,
				}
				partialStatuses = append(partialStatuses, ts)
			}

			var partialCompCost *float64
			partialTotalCompCost := 0.0
			hasAnyPartialCost := false
			allPartialTasksWithTokensHaveCost := true
			partialPossibleSamples := 0
			for _, te := range taskEvals {
				if te.tokens != nil {
					if te.costUSD != nil {
						partialTotalCompCost += *te.costUSD
						hasAnyPartialCost = true
					} else {
						allPartialTasksWithTokensHaveCost = false
					}
				}
				partialPossibleSamples += te.possibleSamples
			}
			if hasAnyPartialCost && allPartialTasksWithTokensHaveCost {
				partialCompCost = &partialTotalCompCost
			}

			extraMsg := fmt.Sprintf("Running extra batch %d (%d tasks)", nextBatchIndex, len(extraTasks))
			if len(prunedTasks) > 0 {
				sort.Strings(prunedTasks)
				extraMsg = fmt.Sprintf("%s; skipped (unmatched): %s", extraMsg, github.FormatIDList(prunedTasks, github.MaxIDList))
			}

			return r.updateComparisonStatus(ctx, &comp, &baselineRun, &candidateRun, func(status *v1alpha1.ComparisonStatus) {
				status.Phase = v1alpha1.ComparisonPhaseRunning
				status.CompletionTime = nil
				status.Verdict = v1alpha1.ComparisonVerdictRunning
				status.Batches = batches
				status.ObservedCandidateRun = comp.Spec.Candidate.Run
				status.ObservedBaselineRun = comp.Spec.Baseline.Run
				status.Tasks = partialStatuses
				status.ExtraSamplesApplied = newExtraApplied
				status.CostUSD = partialCompCost
				if partialPossibleSamples > 0 {
					status.PossibleSamples = partialPossibleSamples
				}
				status.Summary = &v1alpha1.ComparisonSummary{
					Tasks:   fmt.Sprintf("%d/%d", len(taskIDs)-len(extraTasks), len(taskIDs)),
					Skipped: len(skippedNotPlanned) + len(prunedTasks),
				}
				if aff.active {
					status.Affinity = aff.status
					status.Summary.AffinityMatched = aff.matchedCount
				}
				status.Message = extraMsg
				if budgetLimit > 0 {
					var spentUSD *float64
					if budgetEnforced {
						spentUSD = &spentSoFar
					}
					status.Budget = &v1alpha1.ComparisonBudgetStatus{
						LimitUSD: comp.Spec.BudgetUSD,
						SpentUSD: spentUSD,
						Enforced: budgetEnforced,
						Note:     budgetNote,
					}
				}
				meta.SetStatusCondition(&status.Conditions, metav1.Condition{
					Type:               v1alpha1.ConditionTypeExtraSamples,
					Status:             metav1.ConditionTrue,
					Reason:             v1alpha1.ConditionReasonExtraSamplesScheduled,
					Message:            extraMsg,
					ObservedGeneration: comp.Generation,
				})
			})
		}

		var exhaustedTasks []string
		var failedTasks []string
		for _, te := range taskEvals {
			if te.failed {
				failedTasks = append(failedTasks, te.taskID)
			} else if te.exhausted {
				exhaustedTasks = append(exhaustedTasks, te.taskID)
			}
		}
		var reasonClauses []string
		if len(exhaustedTasks) > 0 {
			sort.Strings(exhaustedTasks)
			reasonClauses = append(reasonClauses, fmt.Sprintf("dataset exhausted: %s", github.FormatIDList(exhaustedTasks, github.MaxIDList)))
		}
		if len(failedTasks) > 0 {
			sort.Strings(failedTasks)
			reasonClauses = append(reasonClauses, fmt.Sprintf("task failed: %s", github.FormatIDList(failedTasks, github.MaxIDList)))
		}
		noEligibleDetailMsg = strings.Join(reasonClauses, "; ")

		// When extra samples were requested but no task is eligible (exhausted or failed)
		// on a terminal Comparison, record unserviceable extra samples, settle
		// extraSamplesApplied to spec.extraSamples, and post a PR note.
		if isTerminalComparisonPhase(comp.Status.Phase) {
			return r.recordUnserviceableExtraSamples(ctx, &comp, &baselineRun, &candidateRun, "no eligible tasks", v1alpha1.ConditionReasonExtraSamplesNoEligibleTasks, noEligibleDetailMsg)
		}
		noEligibleExtraTasks = true
	}

	// All tasks are decided or stopped; finalize comparison
	var taskStatuses []v1alpha1.ComparisonTaskStatus
	var reports []stats.TaskReport
	var taskArtifacts []ComparisonTaskArtifact

	for _, te := range taskEvals {
		var pImp *float64
		if te.est.NumPairs > 0 {
			v := te.est.PImprovement
			pImp = &v
		}
		taskStatus := v1alpha1.ComparisonTaskStatus{
			ID:            te.taskID,
			BaseRate:      &te.est.BaseMean,
			CandidateRate: &te.est.CandidateMean,
			Delta:         &te.est.MeanDelta,
			DeltaCI: &v1alpha1.DeltaConfidenceInterval{
				Lower: te.est.CI.Lower,
				Upper: te.est.CI.Upper,
			},
			PImprovement: pImp,
			Pairs:        te.est.NumPairs,
			Wins:         te.est.Wins,
			Losses:       te.est.Losses,
			Ties:         te.est.Ties,
			Dropped: v1alpha1.ComparisonDropped{
				Candidate: te.est.DroppedCandidate,
				Baseline:  te.est.DroppedBaseline,
			},
			Label:           toComparisonTaskLabel(te.dec.Label),
			SamplesRun:      te.samplesRun,
			Decision:        v1alpha1.ComparisonTaskLabel(te.dec.Label),
			SuggestedNext:   te.suggestedNext,
			Tokens:          te.tokens,
			Latency:         te.latency,
			CostUSD:         te.costUSD,
			CostSource:      te.costSource,
			PossibleSamples: te.possibleSamples,
		}
		if aff.active {
			if a, ok := aff.taskAffinity[te.taskID]; ok {
				taskStatus.Affinity = a
			} else {
				taskStatus.Affinity = v1alpha1.ComparisonTaskAffinityAlways
			}
		}
		taskStatuses = append(taskStatuses, taskStatus)

		reports = append(reports, stats.TaskReport{
			ID:           te.taskID,
			Label:        te.dec.Label,
			Estimate:     te.est,
			PImprovement: te.est.PImprovement,
		})

		// Build paired table for artifact
		candMap := make(map[string]float64)
		for _, s := range te.candScores {
			candMap[s.ID] = s.Score
		}
		baseMap := make(map[string]float64, len(te.baseScores))
		for _, s := range te.baseScores {
			baseMap[s.ID] = s.Score
		}

		var pairsTable []PairedSampleScore
		seen := make(map[string]bool, len(te.candScores))
		for _, s := range te.candScores {
			if seen[s.ID] {
				continue
			}
			seen[s.ID] = true
			if bScore, ok := baseMap[s.ID]; ok {
				pairsTable = append(pairsTable, PairedSampleScore{
					ID:             s.ID,
					BaseScore:      bScore,
					CandidateScore: s.Score,
					Delta:          s.Score - bScore,
				})
			}
		}

		taskArt := ComparisonTaskArtifact{
			ID:            te.taskID,
			BaseRate:      te.est.BaseMean,
			CandidateRate: te.est.CandidateMean,
			Delta:         te.est.MeanDelta,
			DeltaCI: v1alpha1.DeltaConfidenceInterval{
				Lower: te.est.CI.Lower,
				Upper: te.est.CI.Upper,
			},
			Pairs:  te.est.NumPairs,
			Wins:   te.est.Wins,
			Losses: te.est.Losses,
			Ties:   te.est.Ties,
			Dropped: v1alpha1.ComparisonDropped{
				Candidate: te.est.DroppedCandidate,
				Baseline:  te.est.DroppedBaseline,
			},
			Label:            string(te.dec.Label),
			PImprovement:     te.est.PImprovement,
			SamplesRun:       te.samplesRun,
			Decision:         string(te.dec.Label),
			SuggestedNext:    te.suggestedNext,
			Tokens:           te.tokens,
			CandidateTokens:  te.candTokens,
			BaseTokens:       te.baseTokens,
			Latency:          te.latency,
			CandidateLatency: te.candLatency,
			BaseLatency:      te.baseLatency,
			CostUSD:          te.costUSD,
			CostSource:       te.costSource,
			CandidateCostUSD: te.candCostUSD,
			BaseCostUSD:      te.baseCostUSD,
			PossibleSamples:  te.possibleSamples,
			PairsTable:       pairsTable,
		}
		if aff.active {
			taskArt.Affinity = aff.taskAffinity[te.taskID]
		}
		taskArtifacts = append(taskArtifacts, taskArt)
	}

	for _, id := range prunedTasks {
		ts := v1alpha1.ComparisonTaskStatus{
			ID:       id,
			Affinity: v1alpha1.ComparisonTaskAffinityUnmatched,
			Label:    v1alpha1.ComparisonTaskLabelSkipped,
			Decision: v1alpha1.ComparisonTaskLabelSkipped,
		}
		taskStatuses = append(taskStatuses, ts)

		taskArt := ComparisonTaskArtifact{
			ID:       id,
			Affinity: v1alpha1.ComparisonTaskAffinityUnmatched,
			Label:    string(v1alpha1.ComparisonTaskLabelSkipped),
			Decision: string(v1alpha1.ComparisonTaskLabelSkipped),
		}
		taskArtifacts = append(taskArtifacts, taskArt)
	}

	var compCost *float64
	totalCompCost := 0.0
	hasAnyTaskCost := false
	allTasksWithTokensHaveCost := true
	totalPossibleSamples := 0

	for _, te := range taskEvals {
		if te.tokens != nil {
			if te.costUSD != nil {
				totalCompCost += *te.costUSD
				hasAnyTaskCost = true
			} else {
				allTasksWithTokensHaveCost = false
			}
		}
		totalPossibleSamples += te.possibleSamples
	}
	if hasAnyTaskCost && allTasksWithTokensHaveCost {
		compCost = &totalCompCost
	}

	runVerdict := stats.AggregateVerdict(reports)
	sort.Strings(candFailed)
	runVerdict, capped := stats.CapForCandidateFailures(runVerdict, candFailed)
	pImp := runVerdict.PImprovement

	var unexecutedNotes []string
	if len(missingBatchRuns) > 0 {
		sort.Strings(missingBatchRuns)
		runWord := "run"
		if len(missingBatchRuns) > 1 {
			runWord = "runs"
		}
		unexecutedNotes = append(unexecutedNotes, fmt.Sprintf("missing batch %s: %s", runWord, github.FormatIDList(missingBatchRuns, github.MaxIDList)))
	}
	if len(failedBatchRuns) > 0 {
		sort.Strings(failedBatchRuns)
		runWord := "run"
		if len(failedBatchRuns) > 1 {
			runWord = "runs"
		}
		unexecutedNotes = append(unexecutedNotes, fmt.Sprintf("failed batch %s: %s", runWord, github.FormatIDList(failedBatchRuns, github.MaxIDList)))
	}
	if len(candFailed) > 0 && !capped {
		taskWord := "task"
		if len(candFailed) > 1 {
			taskWord = "tasks"
		}
		unexecutedNotes = append(unexecutedNotes, fmt.Sprintf("candidate %s failed: %s", taskWord, github.FormatIDList(candFailed, github.MaxIDList)))
	}
	var failedBoth []string
	var unattempted []string
	var notBothExecuted []string
	if len(unexecuted) > 0 {
		for _, id := range unexecuted {
			if containsTask(candFailed, id) {
				continue
			}
			candTask := findTaskStatus(candidateRun.Status.Tasks, id)
			baseTask := findTaskStatus(baselineRun.Status.Tasks, id)
			candRan := candTask != nil && candTask.Phase == v1alpha1.TaskPhaseCompleted
			baseRan := baseTask != nil && baseTask.Phase == v1alpha1.TaskPhaseCompleted
			candFailedBoth := candTask != nil && candTask.Phase == v1alpha1.TaskPhaseFailed
			baseFailedBoth := baseTask != nil && baseTask.Phase == v1alpha1.TaskPhaseFailed
			if !candRan && !baseRan {
				if candFailedBoth && baseFailedBoth {
					failedBoth = append(failedBoth, id)
				} else {
					unattempted = append(unattempted, id)
				}
			} else {
				notBothExecuted = append(notBothExecuted, id)
			}
		}
		sort.Strings(failedBoth)
		sort.Strings(unattempted)
		sort.Strings(notBothExecuted)
		if len(failedBoth) > 0 {
			unexecutedNotes = append(unexecutedNotes, fmt.Sprintf("failed in both runs: %s", github.FormatIDList(failedBoth, github.MaxIDList)))
		}
		if len(unattempted) > 0 {
			unexecutedNotes = append(unexecutedNotes, fmt.Sprintf("tasks not executed: %s", github.FormatIDList(unattempted, github.MaxIDList)))
		}
		if len(notBothExecuted) > 0 {
			unexecutedNotes = append(unexecutedNotes, fmt.Sprintf("tasks not executed by both runs: %s", github.FormatIDList(notBothExecuted, github.MaxIDList)))
		}
	}
	sort.Strings(skippedNotPlanned)
	if len(skippedNotPlanned) > 0 {
		unexecutedNotes = append(unexecutedNotes, fmt.Sprintf("skipped (not planned): %s", github.FormatIDList(skippedNotPlanned, github.MaxIDList)))
	}
	if len(prunedTasks) > 0 {
		sort.Strings(prunedTasks)
		unexecutedNotes = append(unexecutedNotes, fmt.Sprintf("skipped (unmatched): %s", github.FormatIDList(prunedTasks, github.MaxIDList)))
	}

	message := runVerdict.Summary
	if len(unexecutedNotes) > 0 {
		noteStr := strings.Join(unexecutedNotes, "; ")
		if message != "" {
			message = fmt.Sprintf("%s; %s", message, noteStr)
		} else {
			message = noteStr
		}
	}

	budgetReached := false
	if budgetEnforced && budgetLimit > 0 {
		if spentSoFar >= budgetLimit {
			budgetReached = true
		}
		for _, te := range taskEvals {
			if te.budgetStopped {
				budgetReached = true
				break
			}
		}
	}

	if budgetReached && budgetLimit > 0 {
		budgetReason := fmt.Sprintf("budget ($%.2f) reached (spent $%.2f)", budgetLimit, spentSoFar)
		if strings.Contains(message, "more samples recommended") {
			message = strings.Replace(message, "more samples recommended", budgetReason, 1)
		} else if runVerdict.Verdict == stats.VerdictInconclusive {
			if message == "" || message == "INCONCLUSIVE" {
				message = fmt.Sprintf("INCONCLUSIVE: %s", budgetReason)
			} else if !strings.Contains(message, "budget") {
				message = fmt.Sprintf("%s; %s", message, budgetReason)
			}
		}
	}

	var compBudget *v1alpha1.ComparisonBudgetStatus
	if budgetLimit > 0 {
		var spentUSD *float64
		if budgetEnforced {
			spentUSD = &spentSoFar
		}
		compBudget = &v1alpha1.ComparisonBudgetStatus{
			LimitUSD: comp.Spec.BudgetUSD,
			SpentUSD: spentUSD,
			Enforced: budgetEnforced,
			Note:     budgetNote,
		}
	}

	// Store paired table as comparisons/<name>.json in the artifact sink
	artifact := ComparisonArtifact{
		Comparison:            comp.Name,
		Namespace:             comp.Namespace,
		Baseline:              comp.Spec.Baseline.Run,
		Candidate:             comp.Spec.Candidate.Run,
		Verdict:               string(runVerdict.Verdict),
		PImprovement:          runVerdict.PImprovement,
		CostUSD:               compCost,
		PossibleSamples:       totalPossibleSamples,
		Summary:               message,
		Tasks:                 taskArtifacts,
		Batches:               batches,
		CreatedAt:             time.Now().UTC(),
		SkippedNotPlanned:     skippedNotPlanned,
		SkippedUnmatched:      prunedTasks,
		NotExecuted:           unattempted,
		NotExecutedByBothRuns: notBothExecuted,
		FailedInBothRuns:      failedBoth,
		CandidateFailed:       candFailed,
		Affinity:              aff.status,
		AffinityMatched:       aff.matchedCount,
		Budget:                compBudget,
	}

	var summaryPath string
	artJSON, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		logger.Error(err, "failed to marshal comparison artifact", "comparison", comp.Name)
	} else {
		storeCtx, storeCancel := context.WithTimeout(ctx, r.artifactSinkTimeout())
		storedPath, storeErr := artifacts.StoreComparisonWithTimeout(storeCtx, r.artifactSink(), comp.Name, artJSON)
		storeCancel()
		if storeErr != nil {
			logger.Error(storeErr, "failed to store comparison artifact in sink", "comparison", comp.Name)
		} else {
			summaryPath = storedPath
		}
	}

	completedTasks := 0
	for _, t := range taskStatuses {
		if t.Label != v1alpha1.ComparisonTaskLabelRunning && t.Label != v1alpha1.ComparisonTaskLabelSkipped {
			completedTasks++
		}
	}

	phase := v1alpha1.ComparisonPhaseCompleted
	if runVerdict.Verdict == stats.VerdictRunning {
		phase = v1alpha1.ComparisonPhaseRunning
	}

	var compTime *metav1.Time
	if phase == v1alpha1.ComparisonPhaseCompleted {
		if comp.Status.CompletionTime != nil {
			compTime = comp.Status.CompletionTime
		} else if comp.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			nowTime := metav1.NewTime(r.now())
			compTime = &nowTime
		}
	}
	comp.Status.CompletionTime = compTime

	result, err := r.updateComparisonStatus(ctx, &comp, &baselineRun, &candidateRun, func(status *v1alpha1.ComparisonStatus) {
		status.Phase = phase
		status.CompletionTime = compTime
		status.Verdict = v1alpha1.ComparisonVerdict(runVerdict.Verdict)
		status.PImprovement = &pImp
		status.Batches = batches
		status.ObservedCandidateRun = comp.Spec.Candidate.Run
		status.ObservedBaselineRun = comp.Spec.Baseline.Run
		status.Tasks = taskStatuses
		status.CostUSD = compCost
		if totalPossibleSamples > 0 {
			status.PossibleSamples = totalPossibleSamples
		}
		if noEligibleExtraTasks {
			status.ExtraSamplesApplied = comp.Spec.ExtraSamples
			condMsg := fmt.Sprintf("extra samples cannot be run: no eligible tasks (%s)", noEligibleDetailMsg)
			if noEligibleDetailMsg == "" {
				condMsg = "extra samples cannot be run: no eligible tasks"
			}
			meta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               v1alpha1.ConditionTypeExtraSamples,
				Status:             metav1.ConditionFalse,
				Reason:             v1alpha1.ConditionReasonExtraSamplesNoEligibleTasks,
				Message:            condMsg,
				ObservedGeneration: comp.Generation,
			})
		} else {
			status.ExtraSamplesApplied = comp.Status.ExtraSamplesApplied
		}
		status.Summary = &v1alpha1.ComparisonSummary{
			Tasks:   fmt.Sprintf("%d/%d", completedTasks, len(taskStatuses)-len(prunedTasks)),
			Skipped: len(skippedNotPlanned) + len(prunedTasks),
		}
		if aff.active {
			status.Affinity = aff.status
			status.Summary.AffinityMatched = aff.matchedCount
		}
		status.SummaryPath = summaryPath
		status.Message = message
		if compBudget != nil {
			status.Budget = compBudget
		}
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	if noEligibleExtraTasks {
		comp.Status.ExtraSamplesApplied = comp.Spec.ExtraSamples
	}

	if ttl := r.comparisonTTL(); ttl > 0 && phase == v1alpha1.ComparisonPhaseCompleted && comp.Spec.ExtraSamples <= comp.Status.ExtraSamplesApplied && len(comp.Status.PendingCommands) == 0 {
		if age, ok := r.comparisonAge(&comp); ok {
			if age >= ttl {
				logger.Info("deleting completed comparison past TTL", "comparison", comp.Name, "age", age, "ttl", ttl)
				if err := r.Delete(ctx, &comp); err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("failed to delete expired comparison %s: %w", comp.Name, err)
				}
				return ctrl.Result{}, nil
			}
			remaining := ttl - age
			if result.RequeueAfter == 0 || remaining < result.RequeueAfter {
				result.RequeueAfter = remaining
			}
		}
	}

	if wait, hasPending := r.earliestPendingCommandWait(&comp); hasPending {
		if result.RequeueAfter == 0 || wait < result.RequeueAfter {
			result.RequeueAfter = wait
		}
	}

	return result, nil
}

func (r *ComparisonReconciler) recordUnserviceableExtraSamples(
	ctx context.Context,
	comp *v1alpha1.Comparison,
	baselineRun, candidateRun *v1alpha1.Run,
	noteReason, conditionReason, detailMsg string,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	delta := comp.Spec.ExtraSamples - comp.Status.ExtraSamplesApplied
	hasPRCommentSource := false
	for i := range comp.Status.HandledCommands {
		h := &comp.Status.HandledCommands[i]
		// Commands handled while the comparison was already terminal (handledAt >= completionTime)
		// are unserviced. A command handled earlier while still Running gets the condition upon
		// subsequent completion without eligible tasks, but no PR note.
		if isUnservicedHandledCommand(h, comp.Status.CompletionTime) {
			if h.CommentID != 0 {
				hasPRCommentSource = true
			}
		}
	}

	conditionMissing := meta.FindStatusCondition(comp.Status.Conditions, v1alpha1.ConditionTypeExtraSamples) == nil
	if delta > 0 || conditionMissing {
		condMsg := fmt.Sprintf("extra samples cannot be run: %s (%s)", noteReason, detailMsg)
		if detailMsg == "" {
			condMsg = fmt.Sprintf("extra samples cannot be run: %s", noteReason)
		}
		if _, statusErr := r.updateComparisonStatus(ctx, comp, baselineRun, candidateRun, func(status *v1alpha1.ComparisonStatus) {
			if status.ExtraSamplesApplied < comp.Spec.ExtraSamples {
				status.ExtraSamplesApplied = comp.Spec.ExtraSamples
			}
			for i := range status.HandledCommands {
				h := &status.HandledCommands[i]
				if isUnservicedHandledCommand(h, status.CompletionTime) {
					h.Outcome = fmt.Sprintf("unserviceable: %s", noteReason)
				}
			}
			meta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               v1alpha1.ConditionTypeExtraSamples,
				Status:             metav1.ConditionFalse,
				Reason:             conditionReason,
				Message:            condMsg,
				ObservedGeneration: comp.Generation,
			})
		}); statusErr != nil {
			logger.Error(statusErr, "failed to update terminal comparison status for unserviceable extra samples", "comparison", comp.Name)
			return ctrl.Result{}, statusErr
		}
	}

	if hasPRCommentSource {
		if postErr := r.postUnserviceableCommandNote(ctx, comp, noteReason); postErr != nil {
			logger.Info("warning: failed to post unserviceable command note to PR", "pr", comp.Spec.PullRequest.Number, "err", postErr)
		}
	}

	var result ctrl.Result
	if ttl := r.comparisonTTL(); ttl > 0 {
		if age, ok := r.comparisonAge(comp); ok {
			if age < ttl {
				result.RequeueAfter = ttl - age
			} else {
				result.RequeueAfter = time.Second
			}
		}
	}
	return result, nil
}

func (r *ComparisonReconciler) completeInconclusiveArtifactsUnreadable(
	ctx context.Context,
	comp *v1alpha1.Comparison,
	baselineRun, candidateRun *v1alpha1.Run,
	batches []v1alpha1.ComparisonBatchStatus,
	reason, message string,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("completing comparison as inconclusive due to unreadable artifacts", "comparison", comp.Name, "reason", reason, "message", message)

	var compTime *metav1.Time
	if comp.Status.CompletionTime != nil {
		compTime = comp.Status.CompletionTime
	} else {
		nowTime := metav1.NewTime(r.now())
		compTime = &nowTime
	}
	comp.Status.CompletionTime = compTime
	comp.Status.Phase = v1alpha1.ComparisonPhaseCompleted
	comp.Status.Verdict = v1alpha1.ComparisonVerdictInconclusive
	if comp.Spec.ExtraSamples > comp.Status.ExtraSamplesApplied {
		comp.Status.ExtraSamplesApplied = comp.Spec.ExtraSamples
	}

	result, err := r.updateComparisonStatus(ctx, comp, baselineRun, candidateRun, func(status *v1alpha1.ComparisonStatus) {
		status.Phase = v1alpha1.ComparisonPhaseCompleted
		status.CompletionTime = compTime
		status.Verdict = v1alpha1.ComparisonVerdictInconclusive
		status.ObservedCandidateRun = comp.Spec.Candidate.Run
		status.ObservedBaselineRun = comp.Spec.Baseline.Run
		if comp.Spec.ExtraSamples > status.ExtraSamplesApplied {
			status.ExtraSamplesApplied = comp.Spec.ExtraSamples
		}
		if len(status.Batches) == 0 && len(batches) > 0 {
			for i := range batches {
				batches[i].Phase = v1alpha1.ComparisonPhaseCompleted
			}
			status.Batches = batches
		}
		status.Message = message
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionTypeArtifactsReadable,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: comp.Generation,
		})
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	if ttl := r.comparisonTTL(); ttl > 0 && comp.Spec.ExtraSamples <= comp.Status.ExtraSamplesApplied && len(comp.Status.PendingCommands) == 0 {
		if age, ok := r.comparisonAge(comp); ok {
			if age >= ttl {
				logger.Info("deleting completed comparison past TTL", "comparison", comp.Name, "age", age, "ttl", ttl)
				if err := r.Delete(ctx, comp); err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("failed to delete expired comparison %s: %w", comp.Name, err)
				}
				return ctrl.Result{}, nil
			}
			remaining := ttl - age
			if result.RequeueAfter == 0 || remaining < result.RequeueAfter {
				result.RequeueAfter = remaining
			}
		}
	}

	if wait, hasPending := r.earliestPendingCommandWait(comp); hasPending {
		if result.RequeueAfter == 0 || wait < result.RequeueAfter {
			result.RequeueAfter = wait
		}
	}

	return result, nil
}

func (r *ComparisonReconciler) updateComparisonStatus(
	ctx context.Context,
	comp *v1alpha1.Comparison,
	baseRun, candRun *v1alpha1.Run,
	mutate func(*v1alpha1.ComparisonStatus),
) (ctrl.Result, error) {
	key := types.NamespacedName{Namespace: comp.Namespace, Name: comp.Name}

	// 1. Fetch latest object and apply in-memory status mutations
	var latest v1alpha1.Comparison
	if err := r.Get(ctx, key, &latest); err != nil {
		return ctrl.Result{}, err
	}
	mutate(&latest.Status)
	if latest.Status.Phase == v1alpha1.ComparisonPhaseRunning {
		latest.Status.CompletionTime = nil
	}
	if len(latest.Status.Batches) > 0 {
		if latest.Status.ObservedCandidateRun == "" && comp.Spec.Candidate.Run != "" {
			latest.Status.ObservedCandidateRun = comp.Spec.Candidate.Run
		}
		if latest.Status.ObservedBaselineRun == "" && comp.Spec.Baseline.Run != "" {
			latest.Status.ObservedBaselineRun = comp.Spec.Baseline.Run
		}
	}

	// 2. Perform GitHub reporting ONCE outside the RetryOnConflict loop
	var reportErr error
	if latest.Spec.PullRequest != nil {
		if err := r.reportToGitHub(ctx, &latest, baseRun, candRun); err != nil {
			reportErr = err
		}
	}
	reportingStatus := latest.Status.Reporting.DeepCopy()

	// 3. Persist status with conflict retry
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current v1alpha1.Comparison
		if err := r.Get(ctx, key, &current); err != nil {
			return err
		}
		mutate(&current.Status)
		if current.Status.Phase == v1alpha1.ComparisonPhaseRunning {
			current.Status.CompletionTime = nil
		}
		if len(current.Status.Batches) > 0 {
			if current.Status.ObservedCandidateRun == "" && comp.Spec.Candidate.Run != "" {
				current.Status.ObservedCandidateRun = comp.Spec.Candidate.Run
			}
			if current.Status.ObservedBaselineRun == "" && comp.Spec.Baseline.Run != "" {
				current.Status.ObservedBaselineRun = comp.Spec.Baseline.Run
			}
		}
		if latest.Spec.PullRequest != nil {
			current.Status.Reporting = reportingStatus.DeepCopy()
		}
		return r.Status().Update(ctx, &current)
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update Comparison status: %w", err)
	}
	if reportErr != nil {
		return ctrl.Result{}, reportErr
	}
	if wait, hasPending := r.earliestPendingCommandWait(&latest); hasPending {
		return ctrl.Result{RequeueAfter: wait}, nil
	}
	if wait, hasReporting := r.earliestReportingWait(&latest); hasReporting {
		return ctrl.Result{RequeueAfter: wait}, nil
	}
	return ctrl.Result{}, nil
}
