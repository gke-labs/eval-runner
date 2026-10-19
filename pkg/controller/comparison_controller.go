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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/github"
	"github.com/gke-labs/eval-runner/pkg/stats"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
)

// PairedSampleScore records paired scores for a single sample ID.
type PairedSampleScore struct {
	ID             string  `json:"id"`
	BaseScore      float64 `json:"baseScore"`
	CandidateScore float64 `json:"candidateScore"`
	Delta          float64 `json:"delta"`
}

// ComparisonTaskArtifact records paired comparison details for a single task in the artifact JSON.
type ComparisonTaskArtifact struct {
	ID               string                           `json:"id"`
	BaseRate         float64                          `json:"baseRate"`
	CandidateRate    float64                          `json:"candidateRate"`
	Delta            float64                          `json:"delta"`
	DeltaCI          v1alpha1.DeltaConfidenceInterval `json:"deltaCI"`
	Pairs            int                              `json:"pairs"`
	Wins             int                              `json:"wins"`
	Losses           int                              `json:"losses"`
	Ties             int                              `json:"ties"`
	Dropped          v1alpha1.ComparisonDropped       `json:"dropped"`
	Label            string                           `json:"label"`
	PImprovement     float64                          `json:"pImprovement"`
	SamplesRun       int                              `json:"samplesRun,omitempty"`
	Decision         string                           `json:"decision,omitempty"`
	SuggestedNext    int                              `json:"suggestedNext,omitempty"`
	Tokens           *v1alpha1.TaskTokens             `json:"tokens,omitempty"`
	CandidateTokens  *v1alpha1.TaskTokens             `json:"candidateTokens,omitempty"`
	BaseTokens       *v1alpha1.TaskTokens             `json:"baseTokens,omitempty"`
	Latency          *v1alpha1.TaskLatency            `json:"latency,omitempty"`
	CandidateLatency *v1alpha1.TaskLatency            `json:"candidateLatency,omitempty"`
	BaseLatency      *v1alpha1.TaskLatency            `json:"baseLatency,omitempty"`
	CostUSD          *float64                         `json:"costUSD,omitempty"`
	CandidateCostUSD *float64                         `json:"candidateCostUSD,omitempty"`
	BaseCostUSD      *float64                         `json:"baseCostUSD,omitempty"`
	PossibleSamples  int                              `json:"possibleSamples,omitempty"`
	PairsTable       []PairedSampleScore              `json:"pairsTable,omitempty"`
}

// ComparisonArtifact is the schema stored as comparisons/<name>.json in the artifact sink.
type ComparisonArtifact struct {
	Comparison            string                           `json:"comparison"`
	Namespace             string                           `json:"namespace"`
	Baseline              string                           `json:"baseline"`
	Candidate             string                           `json:"candidate"`
	Verdict               string                           `json:"verdict"`
	PImprovement          float64                          `json:"pImprovement"`
	CostUSD               *float64                         `json:"costUSD,omitempty"`
	PossibleSamples       int                              `json:"possibleSamples,omitempty"`
	Summary               string                           `json:"summary,omitempty"`
	Tasks                 []ComparisonTaskArtifact         `json:"tasks"`
	Batches               []v1alpha1.ComparisonBatchStatus `json:"batches,omitempty"`
	CreatedAt             time.Time                        `json:"createdAt"`
	SkippedNotPlanned     []string                         `json:"skippedNotPlanned,omitempty"`
	NotExecuted           []string                         `json:"notExecuted,omitempty"`
	NotExecutedByBothRuns []string                         `json:"notExecutedByBothRuns,omitempty"`
	CandidateFailed       []string                         `json:"candidateFailed,omitempty"`
}

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
	Scheme               *runtime.Scheme
	ArtifactSink         artifacts.ArtifactSink
	ControllerNamespace  string
	GitHubTokenSecret    string
	GitHubAppSecret      string
	UIBaseURL            string
	InconclusivePolicy   string
	GitHubBaseURL        string
	GitHubClientFactory  func(creds GitHubCredentials) github.Client
	ComparisonTTL        time.Duration
	CommandRetryInterval time.Duration
	CommandUsers         []string
	Now                  func() time.Time
	ModelPrices          map[string]ModelPrice

	cachedAppMu   sync.Mutex
	cachedAppKey  string
	cachedAppAuth *github.AppTokenProvider
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

func (r *ComparisonReconciler) controllerNamespace() string {
	if r.ControllerNamespace != "" {
		return r.ControllerNamespace
	}
	return discovery.DefaultControllerNamespace
}

func (r *ComparisonReconciler) gitHubTokenSecret() string {
	return r.GitHubTokenSecret
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
	secretNames := []string{}
	if r.GitHubAppSecret != "" {
		secretNames = append(secretNames, r.GitHubAppSecret)
	}
	if r.gitHubTokenSecret() != "" && r.gitHubTokenSecret() != r.GitHubAppSecret {
		secretNames = append(secretNames, r.gitHubTokenSecret())
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
				return nil, fmt.Errorf("invalid app credentials: %w", err)
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
		} else if IsUserAllowed(cmd.Login, r.CommandUsers) {
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

func (r *ComparisonReconciler) reportToGitHub(
	ctx context.Context,
	comp *v1alpha1.Comparison,
	baseRun, candRun *v1alpha1.Run,
) error {
	if comp == nil || comp.Spec.PullRequest == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ghClient, err := r.getGitHubClient(ctx)
	if err != nil {
		comp.Status.Reporting = &v1alpha1.ComparisonReportingStatus{
			Message: err.Error(),
		}
		return nil
	}
	if ghClient == nil {
		comp.Status.Reporting = &v1alpha1.ComparisonReportingStatus{
			Message: "disabled",
		}
		return nil
	}

	reporter := github.NewReporter(ghClient, github.ReporterOptions{
		UIBaseURL:          r.UIBaseURL,
		InconclusivePolicy: r.InconclusivePolicy,
	})
	repStatus, err := reporter.Report(ctx, comp, baseRun, candRun)
	if repStatus != nil {
		comp.Status.Reporting = repStatus
	}
	if err != nil {
		if github.IsPermanent(err) {
			if comp.Status.Reporting == nil {
				comp.Status.Reporting = &v1alpha1.ComparisonReportingStatus{}
			}
			comp.Status.Reporting.Message = err.Error()
			return nil
		}
		return err
	}

	return nil
}

func isTerminalRunPhase(phase v1alpha1.RunPhase) bool {
	return phase == v1alpha1.RunPhaseCompleted || phase == v1alpha1.RunPhaseFailed
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

func sameTaskSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sa := make([]string, len(a))
	copy(sa, a)
	sort.Strings(sa)
	sb := make([]string, len(b))
	copy(sb, b)
	sort.Strings(sb)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}

func (r *ComparisonReconciler) findCompletedBaselineRun(ctx context.Context, namespace, repoURL, commit, model string, tasks []string, window v1alpha1.SampleRange) (*v1alpha1.Run, error) {
	batchKey := ComputeBatchKey(repoURL, commit, model, tasks, window)

	// 1. Label selector search
	var runList v1alpha1.RunList
	if err := r.List(ctx, &runList, client.InNamespace(namespace), client.MatchingLabels{
		LabelBatchKey: batchKey,
	}); err != nil {
		return nil, fmt.Errorf("failed to list runs by batch-key: %w", err)
	}
	for _, run := range runList.Items {
		if run.Status.Phase == v1alpha1.RunPhaseCompleted && run.DeletionTimestamp == nil {
			return &run, nil
		}
	}

	// 2. Spec-matching fallback across completed runs in namespace
	if err := r.List(ctx, &runList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("failed to list runs for baseline matching: %w", err)
	}
	for _, run := range runList.Items {
		if run.Status.Phase != v1alpha1.RunPhaseCompleted || run.DeletionTimestamp != nil {
			continue
		}
		if run.Spec.Repo.URL == repoURL && run.Spec.Repo.Commit == commit && run.Spec.Model == model {
			effRange := EffectiveSampleRange(run.Spec)
			if effRange != nil && effRange.Start == window.Start && effRange.End == window.End {
				if sameTaskSet(run.Spec.Tasks, tasks) {
					return &run, nil
				}
			}
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

	// Process pending review comment commands (e.g. /eval more <n>)
	if len(comp.Status.PendingCommands) > 0 {
		if err := r.processPendingCommands(ctx, &comp); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed processing pending commands on comparison %s: %w", comp.Name, err)
		}
	}

	// Check TTL expiration for completed comparisons
	if r.ComparisonTTL > 0 && comp.Status.Phase == v1alpha1.ComparisonPhaseCompleted && comp.Spec.ExtraSamples <= comp.Status.ExtraSamplesApplied && len(comp.Status.PendingCommands) == 0 {
		referenceTime := comp.CreationTimestamp.Time
		if comp.Status.CompletionTime != nil && !comp.Status.CompletionTime.IsZero() {
			referenceTime = comp.Status.CompletionTime.Time
		}
		if !referenceTime.IsZero() {
			age := r.now().Sub(referenceTime)
			if age >= r.ComparisonTTL {
				logger.Info("deleting completed comparison past TTL", "comparison", comp.Name, "age", age, "ttl", r.ComparisonTTL)
				if err := r.Delete(ctx, &comp); err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("failed to delete expired comparison %s: %w", comp.Name, err)
				}
				return ctrl.Result{}, nil
			}
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
		return r.updateComparisonStatus(ctx, &comp, nil, nil, func(status *v1alpha1.ComparisonStatus) {
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
			break
		}
		if err := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: baseName}, &bRun); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("failed looking up batch run %q: %w", baseName, err)
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
				} else {
					return ctrl.Result{}, fmt.Errorf("failed to get baseline batch run %q: %w", batches[i].BaselineRun, bErr)
				}
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
		taskIDs := determineRunningTaskIDs(&comp, &baselineRun, &candidateRun)
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
			}
		} else {
			for _, id := range taskIDs {
				tasks = append(tasks, v1alpha1.ComparisonTaskStatus{
					ID:       id,
					Label:    v1alpha1.ComparisonTaskLabelRunning,
					Decision: v1alpha1.ComparisonTaskLabelContinue,
				})
			}
		}

		completedTasks := 0
		for _, t := range tasks {
			if t.Label != v1alpha1.ComparisonTaskLabelRunning {
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
			status.Tasks = tasks
			status.Summary = &v1alpha1.ComparisonSummary{
				Tasks: fmt.Sprintf("%d/%d", completedTasks, len(tasks)),
			}
			status.Message = msg
		})
	}

	// All current batch Runs are terminal; perform pairing and comparison
	taskIDs, unexecuted, candFailed := determineCompletedTaskIDs(&comp, &baselineRun, &candidateRun)
	skippedNotPlanned := determineSkippedTaskIDs(&comp, &baselineRun, &candidateRun)

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
		candCostUSD     *float64
		baseCostUSD     *float64
		possibleSamples int
		exhausted       bool
		failed          bool
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
				taskExhausted = true
				taskFailed = true
				continue
			}

			// Verify that log paths match the commit prefixes of the respective runs
			candPrefix := r.artifactSink().ArtifactPrefix(pair.candRun.Spec.Repo.Commit)
			if candPrefix != "" && !strings.HasPrefix(candTask.LogPath, candPrefix) {
				msg := fmt.Sprintf("candidate log path %q does not match commit prefix %q", candTask.LogPath, candPrefix)
				logger.Error(nil, msg, "task", taskID)
				err := fmt.Errorf("%s", msg)
				if _, statusErr := r.updateComparisonStatus(ctx, &comp, &baselineRun, &candidateRun, func(status *v1alpha1.ComparisonStatus) {
					status.Message = msg
				}); statusErr != nil {
					return ctrl.Result{}, errors.Join(err, statusErr)
				}
				return ctrl.Result{}, err
			}

			basePrefix := r.artifactSink().ArtifactPrefix(pair.baseRun.Spec.Repo.Commit)
			if basePrefix != "" && !strings.HasPrefix(baseTask.LogPath, basePrefix) {
				msg := fmt.Sprintf("baseline log path %q does not match commit prefix %q", baseTask.LogPath, basePrefix)
				logger.Error(nil, msg, "task", taskID)
				err := fmt.Errorf("%s", msg)
				if _, statusErr := r.updateComparisonStatus(ctx, &comp, &baselineRun, &candidateRun, func(status *v1alpha1.ComparisonStatus) {
					status.Message = msg
				}); statusErr != nil {
					return ctrl.Result{}, errors.Join(err, statusErr)
				}
				return ctrl.Result{}, err
			}

			// Load Inspect log for candidate
			candData, err := r.artifactSink().Load(ctx, candTask.LogPath)
			if err != nil {
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
			baseData, err := r.artifactSink().Load(ctx, baseTask.LogPath)
			if err != nil {
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

		prices := r.ModelPrices
		candPrice, candPriceKnown := prices[candidateRun.Spec.Model]
		basePrice, basePriceKnown := prices[baselineRun.Spec.Model]

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
		}

		est := stats.ComparePaired(allCandScores, allBaseScores, 0.95)
		samplesRun := est.NumPairs
		if samplesRun == 0 && len(allCandScores) > 0 {
			samplesRun = len(allCandScores)
		}

		// Stopping rule evaluation:
		var dec stats.Decision
		if est.NumPairs == 0 {
			dec = stats.Decision{
				Label:  stats.TaskLabelInconclusive,
				Stop:   true,
				Reason: "no matching sample IDs between candidate and baseline",
			}
			est.PImprovement = 0.5
		} else if comp.Spec.Sampling == nil {
			// Single comparison (today's behavior): sampleCap = samplesRun
			dec = stats.Decide(est, margin, samplesRun, samplesRun, 0, 0)
		} else if taskExhausted {
			dec = stats.Decide(est, margin, samplesRun, maxSamples, 0, 0)
			if dec.Label == stats.TaskLabelContinue {
				dec.Label = stats.TaskLabelInconclusive
				dec.Stop = true
				dec.Reason = "dataset exhausted"
			}
		} else {
			dec = stats.Decide(est, margin, samplesRun, maxSamples, 0, 0)
			if dec.Label == stats.TaskLabelContinue {
				rem := maxSamples - samplesRun
				if rem <= 0 {
					dec.Label = stats.TaskLabelInconclusive
					dec.Stop = true
					dec.Reason = fmt.Sprintf("sample cap (%d) reached without resolving", maxSamples)
				}
			}
		}

		suggestedNext := 0
		if !taskExhausted {
			if dec.EstimatedSamples > 0 {
				suggestedNext = dec.EstimatedSamples
			} else if dec.NextBatchSize > 0 {
				suggestedNext = dec.NextBatchSize
			}
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
			candCostUSD:     candCost,
			baseCostUSD:     baseCost,
			possibleSamples: taskPossibleSamples,
			exhausted:       taskExhausted,
			failed:          taskFailed,
			est:             est,
			dec:             dec,
			suggestedNext:   suggestedNext,
			samplesRun:      samplesRun,
		})
	}

	// Check if any tasks need to continue sampling
	var continueTasks []string
	maxRemaining := 0
	if comp.Spec.Sampling != nil {
		for _, te := range taskEvals {
			if te.dec.Label == stats.TaskLabelContinue && !te.exhausted {
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
					taskEvals[i].dec.Reason = fmt.Sprintf("sample cap (%d) reached without resolving", maxSamples)
				}
			}
			continueTasks = nil
		} else {
			candCloneName := fmt.Sprintf("%s-b%d-cand", comp.Name, nextBatchIndex)
			baseRunName := fmt.Sprintf("%s-b%d-base", comp.Name, nextBatchIndex)

			batchRange := v1alpha1.SampleRange{Start: nextStart, End: nextEnd}
			batchKey := ComputeBatchKey(baselineRun.Spec.Repo.URL, baselineRun.Spec.Repo.Commit, baselineRun.Spec.Model, continueTasks, batchRange)

			// Baseline reuse lookup
			existingBaseRun, err := r.findCompletedBaselineRun(ctx, comp.Namespace, baselineRun.Spec.Repo.URL, baselineRun.Spec.Repo.Commit, baselineRun.Spec.Model, continueTasks, batchRange)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("failed looking up baseline run for reuse: %w", err)
			}
			if existingBaseRun != nil {
				baseRunName = existingBaseRun.Name
				if addErr := r.addOwnerReferenceToRun(ctx, existingBaseRun, &comp); addErr != nil {
					return ctrl.Result{}, fmt.Errorf("failed to add owner reference to reused baseline run %q: %w", baseRunName, addErr)
				}
				logger.Info("reusing completed baseline run for batch", "comparison", comp.Name, "batch", nextBatchIndex, "run", baseRunName, "batchKey", batchKey)
			} else {
				var existingBase v1alpha1.Run
				if err := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: baseRunName}, &existingBase); apierrors.IsNotFound(err) {
					baseSpec := baselineRun.Spec.DeepCopy()
					baseSpec.Tasks = continueTasks
					baseSpec.SampleRange = &v1alpha1.SampleRange{Start: nextStart, End: nextEnd}
					baseSpec.Limit = nil

					baseClone := &v1alpha1.Run{
						ObjectMeta: metav1.ObjectMeta{
							Name:      baseRunName,
							Namespace: comp.Namespace,
							OwnerReferences: []metav1.OwnerReference{
								{
									APIVersion:         v1alpha1.GroupVersion.String(),
									Kind:               "Comparison",
									Name:               comp.Name,
									UID:                comp.UID,
									Controller:         ptr.To(false),
									BlockOwnerDeletion: ptr.To(true),
								},
							},
							Labels: map[string]string{
								LabelBatchKey:   batchKey,
								LabelComparison: comp.Name,
								LabelBatch:      strconv.Itoa(nextBatchIndex),
								LabelRole:       "baseline",
							},
						},
						Spec: *baseSpec,
					}
					if err := r.Create(ctx, baseClone); err != nil && !apierrors.IsAlreadyExists(err) {
						return ctrl.Result{}, fmt.Errorf("failed to create baseline batch run: %w", err)
					}
				} else if err != nil {
					return ctrl.Result{}, fmt.Errorf("failed looking up baseline run %q: %w", baseRunName, err)
				}
			}

			var existingCand v1alpha1.Run
			if err := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: candCloneName}, &existingCand); apierrors.IsNotFound(err) {
				candSpec := candidateRun.Spec.DeepCopy()
				candSpec.Tasks = continueTasks
				candSpec.SampleRange = &v1alpha1.SampleRange{Start: nextStart, End: nextEnd}
				candSpec.Limit = nil

				candClone := &v1alpha1.Run{
					ObjectMeta: metav1.ObjectMeta{
						Name:      candCloneName,
						Namespace: comp.Namespace,
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion:         v1alpha1.GroupVersion.String(),
								Kind:               "Comparison",
								Name:               comp.Name,
								UID:                comp.UID,
								Controller:         ptr.To(true),
								BlockOwnerDeletion: ptr.To(true),
							},
						},
						Labels: map[string]string{
							"evalrunner.gke-labs.dev/comparison": comp.Name,
							"evalrunner.gke-labs.dev/batch":      strconv.Itoa(nextBatchIndex),
							"evalrunner.gke-labs.dev/role":       "candidate",
						},
					},
					Spec: *candSpec,
				}
				if err := r.Create(ctx, candClone); err != nil && !apierrors.IsAlreadyExists(err) {
					return ctrl.Result{}, fmt.Errorf("failed to create candidate batch run: %w", err)
				}
			} else if err != nil {
				return ctrl.Result{}, fmt.Errorf("failed looking up candidate run %q: %w", candCloneName, err)
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
				partialStatuses = append(partialStatuses, v1alpha1.ComparisonTaskStatus{
					ID:            te.taskID,
					BaseRate:      &te.est.BaseMean,
					CandidateRate: &te.est.CandidateMean,
					Delta:         &te.est.MeanDelta,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
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
					Label:           toComparisonTaskLabel(te.dec.Label),
					SamplesRun:      te.samplesRun,
					Decision:        v1alpha1.ComparisonTaskLabel(te.dec.Label),
					SuggestedNext:   te.suggestedNext,
					Tokens:          te.tokens,
					Latency:         te.latency,
					CostUSD:         te.costUSD,
					PossibleSamples: te.possibleSamples,
				})
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

			return r.updateComparisonStatus(ctx, &comp, &baselineRun, &candidateRun, func(status *v1alpha1.ComparisonStatus) {
				status.Phase = v1alpha1.ComparisonPhaseRunning
				status.CompletionTime = nil
				status.Verdict = v1alpha1.ComparisonVerdictRunning
				status.Batches = batches
				status.Tasks = partialStatuses
				status.CostUSD = partialCompCost
				if partialPossibleSamples > 0 {
					status.PossibleSamples = partialPossibleSamples
				}
				status.Summary = &v1alpha1.ComparisonSummary{
					Tasks: fmt.Sprintf("%d/%d", len(taskIDs)-len(continueTasks), len(taskIDs)),
				}
				status.Message = fmt.Sprintf("Running batch %d (%d tasks continuing)", nextBatchIndex, len(continueTasks))
			})
		}
	}

	// Check extraSamples if all tasks have stopped
	extraRequested := comp.Spec.ExtraSamples > comp.Status.ExtraSamplesApplied
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

			candCloneName := fmt.Sprintf("%s-b%d-cand", comp.Name, nextBatchIndex)
			baseRunName := fmt.Sprintf("%s-b%d-base", comp.Name, nextBatchIndex)

			batchRange := v1alpha1.SampleRange{Start: nextStart, End: nextEnd}
			batchKey := ComputeBatchKey(baselineRun.Spec.Repo.URL, baselineRun.Spec.Repo.Commit, baselineRun.Spec.Model, extraTasks, batchRange)

			existingBase, err := r.findCompletedBaselineRun(ctx, comp.Namespace, baselineRun.Spec.Repo.URL, baselineRun.Spec.Repo.Commit, baselineRun.Spec.Model, extraTasks, batchRange)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("failed looking up baseline run for extra batch reuse: %w", err)
			}
			if existingBase != nil {
				baseRunName = existingBase.Name
				if addErr := r.addOwnerReferenceToRun(ctx, existingBase, &comp); addErr != nil {
					return ctrl.Result{}, fmt.Errorf("failed to add owner reference to reused baseline run %q: %w", baseRunName, addErr)
				}
				logger.Info("reusing completed baseline run for extra batch", "comparison", comp.Name, "batch", nextBatchIndex, "run", baseRunName, "batchKey", batchKey)
			} else {
				var existingBaseRun v1alpha1.Run
				if err := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: baseRunName}, &existingBaseRun); apierrors.IsNotFound(err) {
					baseSpec := baselineRun.Spec.DeepCopy()
					baseSpec.Tasks = extraTasks
					baseSpec.SampleRange = &batchRange
					baseSpec.Limit = nil

					baseClone := &v1alpha1.Run{
						ObjectMeta: metav1.ObjectMeta{
							Name:      baseRunName,
							Namespace: comp.Namespace,
							OwnerReferences: []metav1.OwnerReference{
								{
									APIVersion:         v1alpha1.GroupVersion.String(),
									Kind:               "Comparison",
									Name:               comp.Name,
									UID:                comp.UID,
									Controller:         ptr.To(false),
									BlockOwnerDeletion: ptr.To(true),
								},
							},
							Labels: map[string]string{
								LabelBatchKey:   batchKey,
								LabelComparison: comp.Name,
								LabelBatch:      strconv.Itoa(nextBatchIndex),
								LabelRole:       "baseline",
							},
						},
						Spec: *baseSpec,
					}
					if err := r.Create(ctx, baseClone); err != nil && !apierrors.IsAlreadyExists(err) {
						return ctrl.Result{}, fmt.Errorf("failed to create baseline batch run: %w", err)
					}
				} else if err != nil {
					return ctrl.Result{}, fmt.Errorf("failed looking up baseline run %q: %w", baseRunName, err)
				}
			}

			var existingCand v1alpha1.Run
			if err := r.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: candCloneName}, &existingCand); apierrors.IsNotFound(err) {
				candSpec := candidateRun.Spec.DeepCopy()
				candSpec.Tasks = extraTasks
				candSpec.SampleRange = &batchRange
				candSpec.Limit = nil

				candClone := &v1alpha1.Run{
					ObjectMeta: metav1.ObjectMeta{
						Name:      candCloneName,
						Namespace: comp.Namespace,
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion:         v1alpha1.GroupVersion.String(),
								Kind:               "Comparison",
								Name:               comp.Name,
								UID:                comp.UID,
								Controller:         ptr.To(true),
								BlockOwnerDeletion: ptr.To(true),
							},
						},
						Labels: map[string]string{
							"evalrunner.gke-labs.dev/comparison": comp.Name,
							"evalrunner.gke-labs.dev/batch":      strconv.Itoa(nextBatchIndex),
							"evalrunner.gke-labs.dev/role":       "candidate",
						},
					},
					Spec: *candSpec,
				}
				if err := r.Create(ctx, candClone); err != nil && !apierrors.IsAlreadyExists(err) {
					return ctrl.Result{}, fmt.Errorf("failed to create candidate batch run: %w", err)
				}
			} else if err != nil {
				return ctrl.Result{}, fmt.Errorf("failed looking up candidate run %q: %w", candCloneName, err)
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
				partialStatuses = append(partialStatuses, v1alpha1.ComparisonTaskStatus{
					ID:            te.taskID,
					BaseRate:      &te.est.BaseMean,
					CandidateRate: &te.est.CandidateMean,
					Delta:         &te.est.MeanDelta,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
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
					Label:           toComparisonTaskLabel(te.dec.Label),
					SamplesRun:      te.samplesRun,
					Decision:        v1alpha1.ComparisonTaskLabel(te.dec.Label),
					SuggestedNext:   te.suggestedNext,
					Tokens:          te.tokens,
					Latency:         te.latency,
					CostUSD:         te.costUSD,
					PossibleSamples: te.possibleSamples,
				})
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

			return r.updateComparisonStatus(ctx, &comp, &baselineRun, &candidateRun, func(status *v1alpha1.ComparisonStatus) {
				status.Phase = v1alpha1.ComparisonPhaseRunning
				status.CompletionTime = nil
				status.Verdict = v1alpha1.ComparisonVerdictRunning
				status.Batches = batches
				status.Tasks = partialStatuses
				status.ExtraSamplesApplied = newExtraApplied
				status.CostUSD = partialCompCost
				if partialPossibleSamples > 0 {
					status.PossibleSamples = partialPossibleSamples
				}
				status.Summary = &v1alpha1.ComparisonSummary{
					Tasks: fmt.Sprintf("%d/%d", len(taskIDs)-len(extraTasks), len(taskIDs)),
				}
				status.Message = fmt.Sprintf("Running extra batch %d (%d tasks)", nextBatchIndex, len(extraTasks))
			})
		}
	}

	// All tasks are decided or stopped; finalize comparison
	var taskStatuses []v1alpha1.ComparisonTaskStatus
	var reports []stats.TaskReport
	var taskArtifacts []ComparisonTaskArtifact

	for _, te := range taskEvals {
		taskStatus := v1alpha1.ComparisonTaskStatus{
			ID:            te.taskID,
			BaseRate:      &te.est.BaseMean,
			CandidateRate: &te.est.CandidateMean,
			Delta:         &te.est.MeanDelta,
			DeltaCI: &v1alpha1.DeltaConfidenceInterval{
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
			Label:           toComparisonTaskLabel(te.dec.Label),
			SamplesRun:      te.samplesRun,
			Decision:        v1alpha1.ComparisonTaskLabel(te.dec.Label),
			SuggestedNext:   te.suggestedNext,
			Tokens:          te.tokens,
			Latency:         te.latency,
			CostUSD:         te.costUSD,
			PossibleSamples: te.possibleSamples,
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
		baseMap := make(map[string]float64)
		for _, s := range te.baseScores {
			baseMap[s.ID] = s.Score
		}
		var commonIDs []string
		for id := range candMap {
			if _, ok := baseMap[id]; ok {
				commonIDs = append(commonIDs, id)
			}
		}
		sort.Strings(commonIDs)

		var pairsTable []PairedSampleScore
		for _, id := range commonIDs {
			cScore := candMap[id]
			bScore := baseMap[id]
			pairsTable = append(pairsTable, PairedSampleScore{
				ID:             id,
				BaseScore:      bScore,
				CandidateScore: cScore,
				Delta:          cScore - bScore,
			})
		}

		taskArtifacts = append(taskArtifacts, ComparisonTaskArtifact{
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
			CandidateCostUSD: te.candCostUSD,
			BaseCostUSD:      te.baseCostUSD,
			PossibleSamples:  te.possibleSamples,
			PairsTable:       pairsTable,
		})
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
	if len(candFailed) > 0 && !capped {
		taskWord := "task"
		if len(candFailed) > 1 {
			taskWord = "tasks"
		}
		unexecutedNotes = append(unexecutedNotes, fmt.Sprintf("candidate %s failed: %s", taskWord, github.FormatIDList(candFailed, github.MaxIDList)))
	}
	var neitherExecuted []string
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
			if !candRan && !baseRan {
				neitherExecuted = append(neitherExecuted, id)
			} else {
				notBothExecuted = append(notBothExecuted, id)
			}
		}
		sort.Strings(neitherExecuted)
		sort.Strings(notBothExecuted)
		if len(neitherExecuted) > 0 {
			unexecutedNotes = append(unexecutedNotes, fmt.Sprintf("tasks not executed: %s", github.FormatIDList(neitherExecuted, github.MaxIDList)))
		}
		if len(notBothExecuted) > 0 {
			unexecutedNotes = append(unexecutedNotes, fmt.Sprintf("tasks not executed by both runs: %s", github.FormatIDList(notBothExecuted, github.MaxIDList)))
		}
	}
	sort.Strings(skippedNotPlanned)
	if len(skippedNotPlanned) > 0 {
		unexecutedNotes = append(unexecutedNotes, fmt.Sprintf("skipped (not planned): %s", github.FormatIDList(skippedNotPlanned, github.MaxIDList)))
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
		NotExecuted:           neitherExecuted,
		NotExecutedByBothRuns: notBothExecuted,
		CandidateFailed:       candFailed,
	}

	var summaryPath string
	artJSON, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		logger.Error(err, "failed to marshal comparison artifact", "comparison", comp.Name)
	} else {
		storedPath, storeErr := r.artifactSink().StoreComparison(ctx, comp.Name, artJSON)
		if storeErr != nil {
			logger.Error(storeErr, "failed to store comparison artifact in sink", "comparison", comp.Name)
		} else {
			summaryPath = storedPath
		}
	}

	completedTasks := 0
	for _, t := range taskStatuses {
		if t.Label != v1alpha1.ComparisonTaskLabelRunning {
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
		status.Tasks = taskStatuses
		status.CostUSD = compCost
		if totalPossibleSamples > 0 {
			status.PossibleSamples = totalPossibleSamples
		}
		status.ExtraSamplesApplied = comp.Status.ExtraSamplesApplied
		status.Summary = &v1alpha1.ComparisonSummary{
			Tasks:   fmt.Sprintf("%d/%d", completedTasks, len(taskStatuses)),
			Skipped: len(skippedNotPlanned),
		}
		status.SummaryPath = summaryPath
		status.Message = message
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	if r.ComparisonTTL > 0 && phase == v1alpha1.ComparisonPhaseCompleted && comp.Spec.ExtraSamples <= comp.Status.ExtraSamplesApplied && len(comp.Status.PendingCommands) == 0 {
		referenceTime := comp.CreationTimestamp.Time
		if comp.Status.CompletionTime != nil && !comp.Status.CompletionTime.IsZero() {
			referenceTime = comp.Status.CompletionTime.Time
		}
		if !referenceTime.IsZero() {
			age := r.now().Sub(referenceTime)
			if age >= r.ComparisonTTL {
				logger.Info("deleting completed comparison past TTL", "comparison", comp.Name, "age", age, "ttl", r.ComparisonTTL)
				if err := r.Delete(ctx, &comp); err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("failed to delete expired comparison %s: %w", comp.Name, err)
				}
				return ctrl.Result{}, nil
			}
			remaining := r.ComparisonTTL - age
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
	return ctrl.Result{}, nil
}
