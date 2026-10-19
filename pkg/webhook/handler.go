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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/github"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

const (
	// MaxPayloadBytes limits the maximum accepted request body size (10 MiB).
	MaxPayloadBytes = 10 * 1024 * 1024
)

// Server implements http.Handler for receiving GitHub webhooks.
type Server struct {
	cfg    Config
	mux    *http.ServeMux
	worker *WorkerPool

	mu              sync.RWMutex
	cachedSecret    []byte
	lastSecretFetch time.Time

	ghClientMu     sync.Mutex
	cachedGHClient github.Client
}

// NewServer creates a new webhook Server and initializes endpoints.
func NewServer(cfg Config) *Server {
	s := &Server{
		cfg:    cfg,
		mux:    http.NewServeMux(),
		worker: NewWorkerPool(cfg),
	}
	s.routes()
	return s
}

// Worker returns the underlying WorkerPool.
func (s *Server) Worker() *WorkerPool {
	return s.worker
}

// Start launches the worker pool.
func (s *Server) Start() {
	s.worker.Start()
}

// Stop stops the worker pool and waits for queued items to complete.
func (s *Server) Stop() {
	s.worker.Stop()
}

// ServeHTTP delegates to internal mux.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/readyz", s.handleReadyz)
	s.mux.HandleFunc("/", s.handleWebhook)
	s.mux.HandleFunc("/webhook", s.handleWebhook)
}

func writeResponse(w http.ResponseWriter, status int, body string) {
	w.WriteHeader(status)
	if _, err := w.Write([]byte(body)); err != nil {
		_ = err
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	writeResponse(w, http.StatusOK, "ok\n")
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	writeResponse(w, http.StatusOK, "ok\n")
}

// getWebhookSecret loads the webhook secret either from static config or from Secret in Kubernetes.
func (s *Server) getWebhookSecret(ctx context.Context) ([]byte, error) {
	if len(s.cfg.StaticWebhookSecret) > 0 {
		return s.cfg.StaticWebhookSecret, nil
	}

	s.mu.RLock()
	if len(s.cachedSecret) > 0 && time.Since(s.lastSecretFetch) < 30*time.Second {
		defer s.mu.RUnlock()
		return s.cachedSecret, nil
	}
	s.mu.RUnlock()

	if s.cfg.Client == nil {
		return nil, fmt.Errorf("kubernetes client not configured and no static secret provided")
	}

	secretName := s.cfg.GitHubWebhookSecret
	if secretName == "" {
		secretName = DefaultGitHubWebhookSecret
	}

	var secret corev1.Secret
	key := types.NamespacedName{
		Namespace: s.cfg.Namespace,
		Name:      secretName,
	}

	if err := s.cfg.Client.Get(ctx, key, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("webhook secret %s/%s not found", s.cfg.Namespace, secretName)
		}
		return nil, fmt.Errorf("failed to get webhook secret %s/%s: %w", s.cfg.Namespace, secretName, err)
	}

	secBytes := secret.Data["secret"]
	if len(secBytes) == 0 {
		return nil, fmt.Errorf("key 'secret' not found or empty in Secret %s/%s", s.cfg.Namespace, secretName)
	}

	s.mu.Lock()
	s.cachedSecret = secBytes
	s.lastSecretFetch = time.Now()
	s.mu.Unlock()

	return secBytes, nil
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	// Health probes and other non-root paths should not trigger webhook on root path
	if r.URL.Path != "/" && r.URL.Path != "/webhook" {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1. Read request body capped at MaxPayloadBytes
	r.Body = http.MaxBytesReader(w, r.Body, MaxPayloadBytes)
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read payload: %v", err), http.StatusBadRequest)
		return
	}

	// 2. Load webhook secret
	secret, err := s.getWebhookSecret(r.Context())
	if err != nil {
		log.Printf("webhook secret error: %v", err)
		http.Error(w, "Unauthorized: webhook secret unavailable", http.StatusUnauthorized)
		return
	}

	// 3. Verify X-Hub-Signature-256 signature
	sigHeader := r.Header.Get(HeaderHubSignature256)
	if sigHeader == "" || !VerifySignature(payload, secret, sigHeader) {
		http.Error(w, "Unauthorized: invalid signature", http.StatusUnauthorized)
		return
	}

	eventType := r.Header.Get(HeaderGitHubEvent)
	deliveryID := r.Header.Get(HeaderGitHubDelivery)

	switch eventType {
	case EventPing:
		w.Header().Set("Content-Type", "application/json")
		writeResponse(w, http.StatusOK, `{"message":"pong"}`+"\n")
		return

	case EventPullRequest:
		var event GitHubPullRequestEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			http.Error(w, fmt.Sprintf("Malformed pull_request payload: %v", err), http.StatusBadRequest)
			return
		}

		prNum := event.Number
		if prNum == 0 && event.PullRequest.Number != 0 {
			prNum = event.PullRequest.Number
		}
		if prNum <= 0 {
			http.Error(w, fmt.Sprintf("Invalid pull request number: %d", prNum), http.StatusBadRequest)
			return
		}

		// Parse and validate target base repository (hosting the PR)
		targetOwner, targetName, err := ParseOwnerAndName(&event.Repository)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid repository in payload: %v", err), http.StatusBadRequest)
			return
		}
		targetRepoFullName := fmt.Sprintf("%s/%s", targetOwner, targetName)

		// Check allow-list against target repository hosting the PR
		if !IsRepositoryAllowed(targetRepoFullName, s.cfg.WebhookRepositories) {
			log.Printf("ignoring event for repository %s: not in allow-list", targetRepoFullName)
			writeResponse(w, http.StatusOK, "ignored: repository not in allow-list\n")
			return
		}

		// Filter actions: opened, synchronize, reopened, closed
		action := event.Action
		if action != ActionOpened && action != ActionSynchronize && action != ActionReopened && action != ActionClosed {
			log.Printf("ignoring pull_request action %q for %s #%d", action, targetRepoFullName, prNum)
			writeResponse(w, http.StatusOK, "ignored: action not actionable\n")
			return
		}

		if action == ActionClosed {
			candRepoFullName := ""
			if candRepoObj := event.PullRequest.Head.Repo; candRepoObj != nil {
				candOwner, candName, err := ParseOwnerAndName(candRepoObj)
				if err != nil {
					log.Printf("ignoring invalid head repository in closed pull_request: %v", err)
				} else {
					candRepoFullName = fmt.Sprintf("%s/%s", candOwner, candName)
				}
			}
			item := WorkItem{
				Event: EventPullRequest,
				PullRequest: &PullRequestWorkItem{
					Action:        ActionClosed,
					PRNumber:      prNum,
					CandidateRepo: candRepoFullName,
					TargetRepo:    targetRepoFullName,
				},
			}
			if !s.worker.Enqueue(item) {
				log.Printf("warning: worker queue full, dropping pull_request event delivery %s", deliveryID)
				http.Error(w, "worker queue full", http.StatusServiceUnavailable)
				return
			}
			writeResponse(w, http.StatusOK, "ok: enqueued pull_request\n")
			return
		}

		// Validate head and base commit SHAs (Blocking 2)
		headSHA := strings.ToLower(strings.TrimSpace(event.PullRequest.Head.SHA))
		baseSHA := strings.ToLower(strings.TrimSpace(event.PullRequest.Base.SHA))
		if err := ValidateCommitSHA(headSHA); err != nil {
			http.Error(w, fmt.Sprintf("Invalid head commit SHA: %v", err), http.StatusBadRequest)
			return
		}
		if err := ValidateCommitSHA(baseSHA); err != nil {
			http.Error(w, fmt.Sprintf("Invalid base commit SHA: %v", err), http.StatusBadRequest)
			return
		}

		// Candidate repo from pull_request.head.repo (for forks) or fallback to event.Repository (Blocking 1)
		candRepoObj := event.PullRequest.Head.Repo
		if candRepoObj == nil {
			candRepoObj = &event.Repository
		}
		candOwner, candName, err := ParseOwnerAndName(candRepoObj)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid head repository in payload: %v", err), http.StatusBadRequest)
			return
		}

		// Baseline repo from pull_request.base.repo or fallback to event.Repository
		baseRepoObj := event.PullRequest.Base.Repo
		if baseRepoObj == nil {
			baseRepoObj = &event.Repository
		}
		baseOwner, baseName, err := ParseOwnerAndName(baseRepoObj)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid base repository in payload: %v", err), http.StatusBadRequest)
			return
		}

		candCloneURL := BuildCloneURL(candOwner, candName)
		baseCloneURL := BuildCloneURL(baseOwner, baseName)
		candRepoFullName := fmt.Sprintf("%s/%s", candOwner, candName)
		baseRepoFullName := fmt.Sprintf("%s/%s", baseOwner, baseName)

		item := WorkItem{
			Event: EventPullRequest,
			PullRequest: &PullRequestWorkItem{
				Action:            action,
				PRNumber:          prNum,
				HeadSHA:           headSHA,
				BaseSHA:           baseSHA,
				CandidateCloneURL: candCloneURL,
				CandidateRepo:     candRepoFullName,
				BaselineCloneURL:  baseCloneURL,
				BaselineRepo:      baseRepoFullName,
				TargetRepo:        targetRepoFullName,
			},
		}
		if !s.worker.Enqueue(item) {
			log.Printf("warning: worker queue full, dropping pull_request event delivery %s", deliveryID)
			http.Error(w, "worker queue full", http.StatusServiceUnavailable)
			return
		}

		writeResponse(w, http.StatusOK, "ok: enqueued pull_request\n")

	case EventPush:
		var event GitHubPushEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			http.Error(w, fmt.Sprintf("Malformed push payload: %v", err), http.StatusBadRequest)
			return
		}

		repoOwner, repoName, err := ParseOwnerAndName(&event.Repository)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid repository in payload: %v", err), http.StatusBadRequest)
			return
		}
		repoFullName := fmt.Sprintf("%s/%s", repoOwner, repoName)

		// Check allow-list
		if !IsRepositoryAllowed(repoFullName, s.cfg.WebhookRepositories) {
			log.Printf("ignoring event for repository %s: not in allow-list", repoFullName)
			writeResponse(w, http.StatusOK, "ignored: repository not in allow-list\n")
			return
		}

		// Filter: push must be to default branch
		defaultBranch := event.Repository.DefaultBranch
		if defaultBranch == "" {
			defaultBranch = "main"
		}
		expectedRef := fmt.Sprintf("refs/heads/%s", defaultBranch)
		if event.Ref != expectedRef && event.Ref != defaultBranch {
			log.Printf("ignoring push to non-default branch %q (default: %q)", event.Ref, expectedRef)
			writeResponse(w, http.StatusOK, "ignored: push to non-default branch\n")
			return
		}

		// Ignore deleted branches
		if event.Deleted || event.After == "0000000000000000000000000000000000000000" {
			writeResponse(w, http.StatusOK, "ignored: branch deleted\n")
			return
		}

		commitSHA := strings.ToLower(strings.TrimSpace(event.After))
		if commitSHA == "" && event.HeadCommit != nil {
			commitSHA = strings.ToLower(strings.TrimSpace(event.HeadCommit.ID))
		}
		if err := ValidateCommitSHA(commitSHA); err != nil {
			http.Error(w, fmt.Sprintf("Invalid commit SHA in push payload: %v", err), http.StatusBadRequest)
			return
		}

		cloneURL := BuildCloneURL(repoOwner, repoName)

		item := WorkItem{
			Event: EventPush,
			Push: &PushWorkItem{
				CommitSHA: commitSHA,
				CloneURL:  cloneURL,
				Repo:      repoFullName,
			},
		}
		if !s.worker.Enqueue(item) {
			log.Printf("warning: worker queue full, dropping push event delivery %s", deliveryID)
			http.Error(w, "worker queue full", http.StatusServiceUnavailable)
			return
		}

		writeResponse(w, http.StatusOK, "ok: enqueued push\n")

	case EventIssueComment:
		s.handleIssueCommentEvent(w, r, payload, deliveryID)

	default:
		log.Printf("ignoring unhandled event type %q", eventType)
		writeResponse(w, http.StatusOK, "ignored: event type not handled\n")
	}
}

func (s *Server) handleIssueCommentEvent(w http.ResponseWriter, r *http.Request, payload []byte, deliveryID string) {
	var event GitHubIssueCommentEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		http.Error(w, fmt.Sprintf("Malformed JSON payload: %v", err), http.StatusBadRequest)
		return
	}

	if event.Action != ActionCreated {
		log.Printf("ignoring issue_comment action %q for delivery %s", event.Action, deliveryID)
		writeResponse(w, http.StatusOK, "ignored: action not actionable\n")
		return
	}

	if event.Issue.PullRequest == nil {
		log.Printf("ignoring issue_comment on issue #%d for delivery %s: not a pull request", event.Issue.Number, deliveryID)
		writeResponse(w, http.StatusOK, "ignored: not a pull request\n")
		return
	}

	repoOwner, repoName, err := ParseOwnerAndName(&event.Repository)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid repository metadata: %v", err), http.StatusBadRequest)
		return
	}
	repoFullName := fmt.Sprintf("%s/%s", repoOwner, repoName)
	if !IsRepositoryAllowed(repoFullName, s.cfg.WebhookRepositories) {
		log.Printf("ignoring issue_comment event for repository %s (delivery %s): not in allow-list", repoFullName, deliveryID)
		writeResponse(w, http.StatusOK, "ignored: repository not in allow-list\n")
		return
	}

	n, ok := ParseEvalMoreCommand(event.Comment.Body, s.cfg.DefaultMaxSamplesPerTask)
	if !ok {
		log.Printf("ignoring issue_comment on PR #%d (delivery %s): no supported command", event.Issue.Number, deliveryID)
		writeResponse(w, http.StatusOK, "ignored: no supported command\n")
		return
	}

	author := event.Comment.User.Login
	if author == "" {
		author = event.Sender.Login
	}

	if strings.EqualFold(event.Comment.User.Type, "Bot") || strings.EqualFold(event.Sender.Type, "Bot") {
		log.Printf("ignoring issue_comment from bot %q on PR #%d (delivery %s)", author, event.Issue.Number, deliveryID)
		writeResponse(w, http.StatusOK, "ignored: bot comment\n")
		return
	}

	authorized, err := s.isAuthorAuthorized(r.Context(), repoOwner, repoName, author)
	if err != nil {
		log.Printf("error checking authorization for user %q on %s: %v", author, repoFullName, err)
		http.Error(w, fmt.Sprintf("Authorization check failed: %v", err), http.StatusInternalServerError)
		return
	}
	if !authorized {
		log.Printf("unauthorized user %q attempted /eval more command on %s#%d", author, repoFullName, event.Issue.Number)
		http.Error(w, "Forbidden: user not authorized to execute command", http.StatusForbidden)
		return
	}

	if s.cfg.Client == nil {
		writeResponse(w, http.StatusOK, "ok: no kubernetes client\n")
		return
	}

	prNum := event.Issue.Number
	compName := fmt.Sprintf("pr-%d", prNum)
	key := types.NamespacedName{
		Namespace: s.cfg.Namespace,
		Name:      compName,
	}

	var comp v1alpha1.Comparison
	alreadyHandled := false
	repoMismatch := false
	err = retry.OnError(retry.DefaultBackoff, apierrors.IsConflict, func() error {
		if getErr := s.cfg.Client.Get(r.Context(), key, &comp); getErr != nil {
			return getErr
		}

		if comp.Spec.PullRequest == nil || !strings.EqualFold(comp.Spec.PullRequest.Repository, repoFullName) {
			repoMismatch = true
			return nil
		}

		if isCommentHandled(comp.Annotations, event.Comment.ID) {
			alreadyHandled = true
			return nil
		}

		samplesToBump := n
		if comp.Spec.Sampling != nil {
			maxPerTask := comp.Spec.Sampling.EffectiveMaxSamplesPerTask()
			if maxPerTask > 0 && samplesToBump > maxPerTask {
				samplesToBump = maxPerTask
			}
		}

		comp.Spec.ExtraSamples += samplesToBump
		recordHandledComment(&comp, event.Comment.ID)

		return s.cfg.Client.Update(r.Context(), &comp)
	})

	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Printf("comparison %s/%s not found for PR #%d", s.cfg.Namespace, compName, prNum)
			writeResponse(w, http.StatusOK, "ignored: comparison not found\n")
			return
		}
		log.Printf("error updating comparison %s/%s: %v", s.cfg.Namespace, compName, err)
		http.Error(w, fmt.Sprintf("Failed to update comparison: %v", err), http.StatusInternalServerError)
		return
	}

	if repoMismatch {
		log.Printf("comparison %s/%s repository does not match %s for PR #%d (delivery %s)", s.cfg.Namespace, compName, repoFullName, prNum, deliveryID)
		writeResponse(w, http.StatusOK, "ignored: comparison not found\n")
		return
	}

	if alreadyHandled {
		log.Printf("comment %d on PR #%d already handled (delivery %s)", event.Comment.ID, prNum, deliveryID)
		writeResponse(w, http.StatusOK, "ok: comment already handled\n")
		return
	}

	// React to the comment (eyes or +1) or reply with an acknowledgement
	ghClient, err := s.getGitHubClient(r.Context())
	if err != nil {
		log.Printf("warning: error obtaining github client for reaction: %v", err)
	}
	if ghClient != nil {
		if reactErr := ghClient.CreateCommentReaction(r.Context(), repoOwner, repoName, event.Comment.ID, "eyes"); reactErr != nil {
			log.Printf("warning: failed to react to comment %d: %v; falling back to acknowledgement comment", event.Comment.ID, reactErr)
			msg := fmt.Sprintf("Evaluating %d more sample(s) for PR #%d (extraSamples=%d).", n, prNum, comp.Spec.ExtraSamples)
			if _, replyErr := ghClient.CreateIssueComment(r.Context(), repoOwner, repoName, prNum, msg); replyErr != nil {
				log.Printf("warning: failed to reply to PR #%d: %v", prNum, replyErr)
			}
		}
	}

	log.Printf("successfully processed /eval more %d on PR #%d for delivery %s", n, prNum, deliveryID)
	writeResponse(w, http.StatusOK, "ok: extraSamples updated\n")
}

func (s *Server) isAuthorAuthorized(ctx context.Context, owner, repo, username string) (bool, error) {
	if username == "" {
		return false, nil
	}
	// 1. Allow-list key commandUsers
	if IsUserAllowed(username, s.cfg.CommandUsers) {
		return true, nil
	}

	// 2. GitHub collaborator permission API
	ghClient, err := s.getGitHubClient(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get GitHub client: %w", err)
	}
	if ghClient == nil {
		return false, nil
	}

	perm, err := ghClient.GetRepoPermission(ctx, owner, repo, username)
	if err != nil {
		return false, fmt.Errorf("failed to get repository permission: %w", err)
	}

	return github.HasWriteAccess(perm), nil
}

func (s *Server) getGitHubClient(ctx context.Context) (github.Client, error) {
	if s.cfg.GitHubClient != nil {
		return s.cfg.GitHubClient, nil
	}

	s.ghClientMu.Lock()
	defer s.ghClientMu.Unlock()
	if s.cachedGHClient != nil {
		return s.cachedGHClient, nil
	}

	if s.cfg.Client == nil {
		return nil, nil
	}

	var secretNames []string
	if s.cfg.GitHubAppSecret != "" {
		secretNames = append(secretNames, s.cfg.GitHubAppSecret)
	}
	if s.cfg.GitHubTokenSecret != "" {
		secretNames = append(secretNames, s.cfg.GitHubTokenSecret)
	}

	for _, secretName := range secretNames {
		var secret corev1.Secret
		key := types.NamespacedName{
			Namespace: s.cfg.Namespace,
			Name:      secretName,
		}
		if err := s.cfg.Client.Get(ctx, key, &secret); err != nil {
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
		token := strings.TrimSpace(string(secret.Data["token"]))

		if appID != "" && installationID != "" && len(privKey) > 0 {
			appAuth, err := github.NewAppTokenProvider(github.AppConfig{
				AppID:          appID,
				InstallationID: installationID,
				PrivateKeyPEM:  privKey,
				BaseURL:        s.cfg.GitHubBaseURL,
			})
			if err != nil {
				return nil, fmt.Errorf("invalid app credentials in %s: %w", secretName, err)
			}
			s.cachedGHClient = github.NewHTTPClient(github.HTTPClientConfig{
				AppAuth: appAuth,
				BaseURL: s.cfg.GitHubBaseURL,
			})
			return s.cachedGHClient, nil
		} else if token != "" {
			s.cachedGHClient = github.NewHTTPClient(github.HTTPClientConfig{
				Token:   token,
				BaseURL: s.cfg.GitHubBaseURL,
			})
			return s.cachedGHClient, nil
		}
	}

	return nil, nil
}

func isCommentHandled(annotations map[string]string, commentID int64) bool {
	if annotations == nil {
		return false
	}
	raw, ok := annotations[AnnotationHandledComments]
	if !ok || strings.TrimSpace(raw) == "" {
		return false
	}
	idStr := strconv.FormatInt(commentID, 10)
	for _, part := range strings.Split(raw, ",") {
		if strings.TrimSpace(part) == idStr {
			return true
		}
	}
	return false
}

func recordHandledComment(comp *v1alpha1.Comparison, commentID int64) {
	if comp.Annotations == nil {
		comp.Annotations = make(map[string]string)
	}
	idStr := strconv.FormatInt(commentID, 10)
	raw := comp.Annotations[AnnotationHandledComments]
	var ids []string
	if strings.TrimSpace(raw) != "" {
		for _, part := range strings.Split(raw, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" && trimmed != idStr {
				ids = append(ids, trimmed)
			}
		}
	}
	ids = append(ids, idStr)
	if len(ids) > 100 {
		ids = ids[len(ids)-100:]
	}
	comp.Annotations[AnnotationHandledComments] = strings.Join(ids, ",")
}
