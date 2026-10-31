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
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/forge"
	"github.com/gke-labs/eval-runner/pkg/github"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// MaxPayloadBytes limits the maximum accepted request body size (10 MiB).
	MaxPayloadBytes = 10 * 1024 * 1024
)

type cachedPermission struct {
	hasWrite bool
	cachedAt time.Time
}

// Server implements http.Handler for receiving GitHub webhooks.
type Server struct {
	cfg    Config
	mux    *http.ServeMux
	worker *WorkerPool

	mu              sync.RWMutex
	cachedSecret    []byte
	lastSecretFetch time.Time

	// deliveryMu protects deliveryCache for 24h idempotency
	deliveryMu    sync.Mutex
	deliveryCache map[string]time.Time

	// permMu protects collaborator permission cache
	permMu    sync.RWMutex
	permCache map[string]cachedPermission

	ghClientMu sync.Mutex
	ghClient   github.Client
}

// NewServer creates a new webhook Server and initializes endpoints.
func NewServer(cfg Config) *Server {
	if len(cfg.AllowedCloneHosts) == 0 {
		cfg.AllowedCloneHosts = []string{DefaultAllowedCloneHosts}
	}
	s := &Server{
		cfg:           cfg,
		mux:           http.NewServeMux(),
		worker:        NewWorkerPool(cfg),
		deliveryCache: make(map[string]time.Time),
		permCache:     make(map[string]cachedPermission),
		ghClient:      cfg.GitHubClient,
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
		log.Printf("error writing response: %v", err)
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

	key := types.NamespacedName{
		Namespace: s.cfg.Namespace,
		Name:      secretName,
	}

	secBytes, err := s.extractWebhookSecret(ctx, key)
	if err == nil && len(secBytes) > 0 {
		s.mu.Lock()
		s.cachedSecret = secBytes
		s.lastSecretFetch = time.Now()
		s.mu.Unlock()
		return secBytes, nil
	}

	// If secretName was default and not found, also check github-app Secret under webhook-secret or secret
	if secretName == DefaultGitHubWebhookSecret {
		appKey := types.NamespacedName{
			Namespace: s.cfg.Namespace,
			Name:      "github-app",
		}
		if appBytes, appErr := s.extractWebhookSecret(ctx, appKey); appErr == nil && len(appBytes) > 0 {
			s.mu.Lock()
			s.cachedSecret = appBytes
			s.lastSecretFetch = time.Now()
			s.mu.Unlock()
			return appBytes, nil
		}
	}

	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("webhook secret %s/%s not found", s.cfg.Namespace, secretName)
		}
		return nil, fmt.Errorf("failed to get webhook secret %s/%s: %w", s.cfg.Namespace, secretName, err)
	}

	return nil, fmt.Errorf("webhook secret key 'webhook-secret' or 'secret' not found in %s/%s", s.cfg.Namespace, secretName)
}

func (s *Server) extractWebhookSecret(ctx context.Context, key types.NamespacedName) ([]byte, error) {
	var secret corev1.Secret
	if err := s.cfg.Client.Get(ctx, key, &secret); err != nil {
		return nil, err
	}
	if b, ok := secret.Data["webhook-secret"]; ok && len(b) > 0 {
		return b, nil
	}
	if b, ok := secret.Data["secret"]; ok && len(b) > 0 {
		return b, nil
	}
	return nil, nil
}

func (s *Server) isDeliverySeen(id string) bool {
	if id == "" {
		return false
	}
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	if s.deliveryCache == nil {
		return false
	}
	t, ok := s.deliveryCache[id]
	if !ok {
		return false
	}
	if time.Since(t) > 24*time.Hour {
		delete(s.deliveryCache, id)
		return false
	}
	return true
}

func (s *Server) rememberDelivery(id string) {
	if id == "" {
		return
	}
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	if s.deliveryCache == nil {
		s.deliveryCache = make(map[string]time.Time)
	}
	now := time.Now()
	s.deliveryCache[id] = now
	for k, t := range s.deliveryCache {
		if now.Sub(t) > 24*time.Hour {
			delete(s.deliveryCache, k)
		}
	}
}

func (s *Server) getGitHubClient(ctx context.Context) (github.Client, error) {
	s.ghClientMu.Lock()
	defer s.ghClientMu.Unlock()
	if s.ghClient != nil {
		return s.ghClient, nil
	}

	if s.cfg.Client == nil {
		return nil, fmt.Errorf("kubernetes client not configured")
	}

	// Load github-app secret
	appName := s.cfg.GitHubAppSecret
	if appName == "" {
		appName = "github-app"
	}

	var appSec corev1.Secret
	if err := s.cfg.Client.Get(ctx, types.NamespacedName{Namespace: s.cfg.Namespace, Name: appName}, &appSec); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Printf("warning: error reading secret %s: %v", appName, err)
		}
		return nil, fmt.Errorf("github-app secret %s/%s not found: %w", s.cfg.Namespace, appName, err)
	}

	appID := string(appSec.Data["app-id"])
	installationID := string(appSec.Data["installation-id"])
	privateKey := appSec.Data["private-key"]
	appSlug := string(appSec.Data["app-slug"])

	if appID == "" || installationID == "" || len(privateKey) == 0 {
		return nil, fmt.Errorf("missing app-id, installation-id, or private-key in secret %s/%s", s.cfg.Namespace, appName)
	}

	provider, err := github.NewAppTokenProvider(github.AppConfig{
		AppID:          appID,
		InstallationID: installationID,
		PrivateKeyPEM:  privateKey,
		AppSlug:        appSlug,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating App token provider from %s/%s: %w", s.cfg.Namespace, appName, err)
	}

	client := github.NewHTTPClient(github.HTTPClientConfig{
		AppAuth: provider,
	})
	s.ghClient = client
	return client, nil
}

func (s *Server) checkCollaboratorWriteAccess(ctx context.Context, owner, repo, username string) (bool, error) {
	cacheKey := fmt.Sprintf("%s/%s/%s", strings.ToLower(owner), strings.ToLower(repo), strings.ToLower(username))
	s.permMu.RLock()
	if entry, ok := s.permCache[cacheKey]; ok {
		if time.Since(entry.cachedAt) < 10*time.Minute {
			s.permMu.RUnlock()
			return entry.hasWrite, nil
		}
	}
	s.permMu.RUnlock()

	ghClient, err := s.getGitHubClient(ctx)
	if err != nil {
		return false, fmt.Errorf("github client unavailable: %w", err)
	}

	perm, err := ghClient.GetRepoPermission(ctx, owner, repo, username)
	if err != nil {
		return false, err
	}

	hasWrite := github.HasWriteAccess(perm)

	s.permMu.Lock()
	if s.permCache == nil {
		s.permCache = make(map[string]cachedPermission)
	}
	now := time.Now()
	for k, v := range s.permCache {
		if now.Sub(v.cachedAt) > 10*time.Minute {
			delete(s.permCache, k)
		}
	}
	s.permCache[cacheKey] = cachedPermission{
		hasWrite: hasWrite,
		cachedAt: now,
	}
	s.permMu.Unlock()

	return hasWrite, nil
}

func (s *Server) findComparisonForHeadSHA(ctx context.Context, targetRepo, headSHA string) (*v1alpha1.Comparison, error) {
	if s.cfg.Client == nil || headSHA == "" {
		return nil, nil
	}

	var compList v1alpha1.ComparisonList
	var listOpts []client.ListOption
	if s.cfg.Namespace != "" {
		listOpts = append(listOpts, client.InNamespace(s.cfg.Namespace))
	}
	listOpts = append(listOpts, client.MatchingLabels{LabelHeadSHA: headSHA})

	if err := s.cfg.Client.List(ctx, &compList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list comparisons: %w", err)
	}

	targetRepoNoHost := targetRepo
	if parts := strings.Split(targetRepo, "/"); len(parts) == 3 {
		targetRepoNoHost = parts[1] + "/" + parts[2]
	}
	targetRepoWithHost := targetRepo
	if !strings.Contains(targetRepo, ".") {
		targetRepoWithHost = "github.com/" + targetRepo
	}

	sanitizedRepo := SanitizeLabelValue(targetRepo)
	sanitizedNoHost := SanitizeLabelValue(targetRepoNoHost)
	sanitizedWithHost := SanitizeLabelValue(targetRepoWithHost)

	var best *v1alpha1.Comparison
	for i := range compList.Items {
		comp := &compList.Items[i]

		// 1. Check head SHA exact match on labels or spec
		compHeadSHA := comp.Labels[LabelHeadSHA]
		if compHeadSHA == "" && comp.Spec.PullRequest != nil {
			compHeadSHA = comp.Spec.PullRequest.HeadSHA
		}
		if compHeadSHA == "" || !strings.EqualFold(compHeadSHA, headSHA) {
			continue
		}

		// 2. Check target repo match on labels or spec
		matchedRepo := false
		if comp.Labels != nil {
			for _, lKey := range []string{LabelTargetRepo, LabelRepo} {
				lVal := comp.Labels[lKey]
				if lVal != "" && (lVal == sanitizedRepo || lVal == sanitizedNoHost || lVal == sanitizedWithHost ||
					strings.EqualFold(lVal, targetRepo) || strings.EqualFold(lVal, targetRepoNoHost)) {
					matchedRepo = true
					break
				}
			}
		}
		if !matchedRepo && comp.Spec.PullRequest != nil {
			r := comp.Spec.PullRequest.Repository
			if strings.EqualFold(r, targetRepo) || strings.EqualFold(r, targetRepoNoHost) || strings.EqualFold(r, targetRepoWithHost) {
				matchedRepo = true
			}
		}

		if !matchedRepo {
			continue
		}

		if best == nil || comp.CreationTimestamp.After(best.CreationTimestamp.Time) {
			best = comp
		}
	}

	return best, nil
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
					Merged:        event.PullRequest.Merged,
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

		candCloneURL := candRepoObj.CloneURL
		if candCloneURL != "" {
			if err := s.validateCloneURL(candCloneURL, candOwner, candName); err != nil {
				http.Error(w, fmt.Sprintf("Invalid candidate clone URL %q: %v", candCloneURL, err), http.StatusBadRequest)
				return
			}
		} else {
			candCloneURL = BuildCloneURL(candOwner, candName)
		}
		baseCloneURL := baseRepoObj.CloneURL
		if baseCloneURL != "" {
			if err := s.validateCloneURL(baseCloneURL, baseOwner, baseName); err != nil {
				http.Error(w, fmt.Sprintf("Invalid base clone URL %q: %v", baseCloneURL, err), http.StatusBadRequest)
				return
			}
		} else {
			baseCloneURL = BuildCloneURL(baseOwner, baseName)
		}
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

		var commitTime string
		if event.HeadCommit != nil && event.HeadCommit.Timestamp != "" {
			t, err := time.Parse(time.RFC3339, event.HeadCommit.Timestamp)
			if err != nil {
				log.Printf("ignoring unparseable head_commit timestamp %q: %v", event.HeadCommit.Timestamp, err)
			} else {
				commitTime = t.UTC().Format(time.RFC3339)
			}
		}

		item := WorkItem{
			Event: EventPush,
			Push: &PushWorkItem{
				CommitSHA:  commitSHA,
				CloneURL:   cloneURL,
				Repo:       repoFullName,
				Branch:     defaultBranch,
				CommitTime: commitTime,
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

	case EventCheckRun:
		s.handleCheckRunEvent(w, r, payload, deliveryID)

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

	if !IsValidGitHubLogin(author) {
		log.Printf("ignoring issue_comment from invalid author login %q on PR #%d (delivery %s)", author, event.Issue.Number, deliveryID)
		writeResponse(w, http.StatusOK, "ignored: invalid author login\n")
		return
	}

	if strings.EqualFold(event.Comment.User.Type, "Bot") || strings.EqualFold(event.Sender.Type, "Bot") {
		log.Printf("ignoring issue_comment from bot %q on PR #%d (delivery %s)", author, event.Issue.Number, deliveryID)
		writeResponse(w, http.StatusOK, "ignored: bot comment\n")
		return
	}

	if s.cfg.Client == nil {
		writeResponse(w, http.StatusOK, "ok: no kubernetes client\n")
		return
	}

	prNum := event.Issue.Number

	var comp *v1alpha1.Comparison
	alreadyHandled := false
	queueFull := false

	backoff := wait.Backoff{
		Steps:    10,
		Duration: 10 * time.Millisecond,
		Factor:   1.5,
		Jitter:   0.1,
	}

	err = retry.OnError(backoff, apierrors.IsConflict, func() error {
		var getErr error
		comp, getErr = FindComparisonForPR(r.Context(), s.cfg.Client, s.cfg.Namespace, repoFullName, prNum)
		if getErr != nil {
			return getErr
		}
		if comp == nil {
			return apierrors.NewNotFound(v1alpha1.GroupVersion.WithResource("comparisons").GroupResource(), fmt.Sprintf("pr-%d for %s", prNum, repoFullName))
		}

		for _, cmd := range comp.Status.PendingCommands {
			if cmd.CommentID == event.Comment.ID {
				alreadyHandled = true
				return nil
			}
		}
		for _, cmd := range comp.Status.HandledCommands {
			if cmd.CommentID == event.Comment.ID {
				alreadyHandled = true
				return nil
			}
		}

		if len(comp.Status.PendingCommands) >= v1alpha1.MaxPendingCommands {
			queueFull = true
			return nil
		}

		newCmd := v1alpha1.ComparisonPendingCommand{
			CommentID:   event.Comment.ID,
			DeliveryID:  deliveryID,
			Login:       author,
			RequestedBy: author,
			Command:     fmt.Sprintf("more %d", n),
			Requested:   n,
			ReceivedAt:  metav1.Now(),
		}

		comp.Status.PendingCommands = append(comp.Status.PendingCommands, newCmd)

		return s.cfg.Client.Status().Update(r.Context(), comp)
	})

	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Printf("comparison for %s PR #%d not found", repoFullName, prNum)
			writeResponse(w, http.StatusOK, "ignored: comparison not found\n")
			return
		}
		log.Printf("error updating comparison for %s PR #%d: %v", repoFullName, prNum, err)
		http.Error(w, fmt.Sprintf("Failed to update comparison: %v", err), http.StatusInternalServerError)
		return
	}

	if alreadyHandled {
		log.Printf("comment %d on PR #%d already handled (delivery %s)", event.Comment.ID, prNum, deliveryID)
		writeResponse(w, http.StatusOK, "ok: comment already handled\n")
		return
	}

	if queueFull {
		compName := ""
		if comp != nil {
			compName = comp.Name
		}
		log.Printf("pending commands queue full on Comparison %s/%s for PR #%d; dropping command %d (delivery %s)", s.cfg.Namespace, compName, prNum, event.Comment.ID, deliveryID)
		writeResponse(w, http.StatusOK, "ignored: pending commands queue full\n")
		return
	}

	log.Printf("successfully recorded /eval more %d on PR #%d for delivery %s", n, prNum, deliveryID)
	writeResponse(w, http.StatusOK, "ok: pending command recorded\n")
}

func (s *Server) handleCheckRunEvent(w http.ResponseWriter, r *http.Request, payload []byte, deliveryID string) {
	ctx := r.Context()

	// 1. Idempotency on GitHub redelivery (X-GitHub-Delivery id remembered for 24h)
	if s.isDeliverySeen(deliveryID) {
		log.Printf("redelivery %s already processed; ignoring", deliveryID)
		writeResponse(w, http.StatusOK, "ok: delivery already processed\n")
		return
	}

	// 2. Unmarshal payload
	var event GitHubCheckRunEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		http.Error(w, fmt.Sprintf("Malformed JSON payload: %v", err), http.StatusBadRequest)
		return
	}

	// 3. Action filter: only "requested_action"
	if event.Action != ActionRequestedAction {
		log.Printf("ignoring check_run action %q for delivery %s", event.Action, deliveryID)
		writeResponse(w, http.StatusOK, "ignored: action not actionable\n")
		return
	}

	// 4. Validate requested action identifier
	if event.RequestedAction == nil || event.RequestedAction.Identifier == "" {
		http.Error(w, "missing requested_action identifier", http.StatusBadRequest)
		return
	}

	actionID := event.RequestedAction.Identifier
	var cmdString string
	var requestedSamples int
	switch actionID {
	case github.ActionIdentifierMore32:
		cmdString = "more 32"
		requestedSamples = 32
	case github.ActionIdentifierRerunBase:
		cmdString = "rerun baseline"
		requestedSamples = 0
	case github.ActionIdentifierRerunCand:
		cmdString = "rerun candidate"
		requestedSamples = 0
	default:
		log.Printf("unknown action identifier %q for delivery %s", actionID, deliveryID)
		http.Error(w, fmt.Sprintf("unknown action identifier: %s", actionID), http.StatusBadRequest)
		return
	}

	// 5. Validate sender (not empty or bot)
	sender := strings.TrimSpace(event.Sender.Login)
	if sender == "" || strings.HasSuffix(strings.ToLower(sender), "[bot]") || strings.EqualFold(event.Sender.Type, "Bot") || !IsValidGitHubLogin(sender) {
		log.Printf("rejecting check_run action from invalid/bot sender %q (type %q) for delivery %s", sender, event.Sender.Type, deliveryID)
		http.Error(w, "Forbidden: bot or empty sender", http.StatusForbidden)
		return
	}

	// 6. Check repository metadata and allow-list
	owner, repo, err := ParseOwnerAndName(&event.Repository)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid repository metadata: %v", err), http.StatusBadRequest)
		return
	}
	repoFullName := fmt.Sprintf("%s/%s", owner, repo)
	if !IsRepositoryAllowed(repoFullName, s.cfg.WebhookRepositories) {
		log.Printf("ignoring check_run event for repository %s (delivery %s): not in allow-list", repoFullName, deliveryID)
		writeResponse(w, http.StatusOK, "ignored: repository not in allow-list\n")
		return
	}

	// 7. Validate head commit SHA
	headSHA := strings.ToLower(strings.TrimSpace(event.CheckRun.HeadSHA))
	if headSHA == "" {
		http.Error(w, "missing head_sha in check_run", http.StatusBadRequest)
		return
	}
	if err := ValidateCommitSHA(headSHA); err != nil {
		log.Printf("invalid commit SHA %q in check_run: %v", headSHA, err)
		http.Error(w, fmt.Sprintf("invalid commit SHA in check_run: %v", err), http.StatusBadRequest)
		return
	}

	// 8. Resolve Comparison from check_run.head_sha + repository before checking permissions
	comp, err := s.findComparisonForHeadSHA(ctx, repoFullName, headSHA)
	if err != nil {
		log.Printf("failed to find comparison for head SHA %s on repo %s: %v", headSHA, repoFullName, err)
		http.Error(w, fmt.Sprintf("failed to resolve comparison: %v", err), http.StatusInternalServerError)
		return
	}
	if comp == nil {
		log.Printf("no comparison found for head SHA %s on repo %s (delivery %s)", headSHA, repoFullName, deliveryID)
		http.Error(w, "Comparison not found", http.StatusNotFound)
		return
	}

	// 9. Check sender has write access to repository (collaborators/{user}/permission >= write, cached)
	hasWrite, err := s.checkCollaboratorWriteAccess(ctx, owner, repo, sender)
	if err != nil {
		log.Printf("failed to check collaborator permission for %s on %s: %v", sender, repoFullName, err)
		http.Error(w, fmt.Sprintf("failed to check collaborator permission: %v", err), http.StatusInternalServerError)
		return
	}
	if !hasWrite {
		log.Printf("sender %s lacks write access to %s; rejecting check_run action %s (delivery %s)", sender, repoFullName, actionID, deliveryID)
		http.Error(w, "Forbidden: sender lacks write access to repository", http.StatusForbidden)
		return
	}

	// 10. Append pending command to Comparison status
	newCmd := v1alpha1.ComparisonPendingCommand{
		CheckRunID:  event.CheckRun.ID,
		DeliveryID:  deliveryID,
		Login:       sender,
		RequestedBy: sender,
		Command:     cmdString,
		Requested:   requestedSamples,
		ReceivedAt:  metav1.Now(),
	}

	var alreadyRecorded bool
	var queueFull bool
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var latest v1alpha1.Comparison
		if err := s.cfg.Client.Get(ctx, types.NamespacedName{Namespace: comp.Namespace, Name: comp.Name}, &latest); err != nil {
			return err
		}

		for _, cmd := range latest.Status.PendingCommands {
			if (deliveryID != "" && cmd.DeliveryID == deliveryID) ||
				(cmd.CheckRunID == event.CheckRun.ID && cmd.Command == cmdString && cmd.CheckRunID != 0) {
				alreadyRecorded = true
				return nil
			}
		}
		for _, cmd := range latest.Status.HandledCommands {
			if (deliveryID != "" && cmd.DeliveryID == deliveryID) ||
				(cmd.CheckRunID == event.CheckRun.ID && cmd.Command == cmdString && cmd.CheckRunID != 0) {
				alreadyRecorded = true
				return nil
			}
		}

		if len(latest.Status.PendingCommands) >= v1alpha1.MaxPendingCommands {
			queueFull = true
			return nil
		}

		latest.Status.PendingCommands = append(latest.Status.PendingCommands, newCmd)
		return s.cfg.Client.Status().Update(ctx, &latest)
	})

	if err != nil {
		log.Printf("failed to update pendingCommands on comparison %s/%s: %v", comp.Namespace, comp.Name, err)
		http.Error(w, fmt.Sprintf("failed to update comparison: %v", err), http.StatusInternalServerError)
		return
	}

	if alreadyRecorded {
		s.rememberDelivery(deliveryID)
		log.Printf("check_run action %s already recorded on Comparison %s/%s (delivery %s)", actionID, comp.Namespace, comp.Name, deliveryID)
		writeResponse(w, http.StatusOK, "ok: already recorded\n")
		return
	}

	if queueFull {
		log.Printf("pendingCommands queue is full on Comparison %s/%s; dropping check_run action %s (delivery %s)", comp.Namespace, comp.Name, actionID, deliveryID)
		writeResponse(w, http.StatusOK, "ok: queue full\n")
		return
	}

	// 10. Remember delivery for 24h
	s.rememberDelivery(deliveryID)

	// 11. Reply by updating check run to in_progress with "requested by @user"
	ghClient, err := s.getGitHubClient(ctx)
	if err != nil {
		log.Printf("warning: could not get github client to update check run: %v", err)
	} else if event.CheckRun.ID != 0 {
		updateState := github.CheckRunState{
			Name:       event.CheckRun.Name,
			Status:     "in_progress",
			Conclusion: "",
			Title:      fmt.Sprintf("in progress · requested by @%s", sender),
			Summary:    fmt.Sprintf("requested by @%s", sender),
		}
		if updateErr := ghClient.UpdateCheckRun(ctx, owner, repo, event.CheckRun.ID, updateState); updateErr != nil {
			log.Printf("warning: failed to update check run %d to in_progress: %v", event.CheckRun.ID, updateErr)
		}
	}

	log.Printf("successfully recorded check_run action %s (%q) on Comparison %s/%s for delivery %s", actionID, cmdString, comp.Namespace, comp.Name, deliveryID)
	writeResponse(w, http.StatusOK, "ok\n")
}

// validateCloneURL validates that a repository clone URL parses, that its host
// is within the allow-list, and that its owner and name match unconditionally.
func (s *Server) validateCloneURL(raw, owner, name string) error {
	parsed, ok := forge.ParseRepoURL(raw)
	if !ok {
		return fmt.Errorf("failed to parse clone URL")
	}
	if !IsHostAllowed(parsed.Host, s.cfg.AllowedCloneHosts) {
		return fmt.Errorf("host %q is not in the allowed-clone-hosts list", parsed.Host)
	}
	if !strings.EqualFold(parsed.Owner, owner) || !strings.EqualFold(parsed.Name, name) {
		return fmt.Errorf("does not match repository %s/%s", owner, name)
	}
	return nil
}
