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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/github"
	"github.com/gke-labs/eval-runner/pkg/pki"
	"github.com/gke-labs/eval-runner/pkg/sandboxclient"
	"github.com/gke-labs/eval-runner/pkg/stats"
	"github.com/gke-labs/eval-runner/pkg/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

const (
	// DefaultMaxDiscoveryAttempts is the default maximum number of discovery attempts for clone retries.
	DefaultMaxDiscoveryAttempts = 5

	// DefaultMaxUncountedRequeuesPerAttempt is the default maximum number of uncounted requeues
	// for an unchanged endpoint before counting one attempt against MaxDiscoveryAttempts.
	DefaultMaxUncountedRequeuesPerAttempt = 6

	// DefaultDiscoveryDeadline is the default overall deadline for discovery to complete.
	DefaultDiscoveryDeadline = 10 * time.Minute

	// DefaultDiscoveryStepTimeout is the maximum duration for a single discovery Execute call.
	DefaultDiscoveryStepTimeout = 60 * time.Second

	// DefaultExecutionDeadline is the default overall deadline for sample execution.
	DefaultExecutionDeadline = 30 * time.Minute

	// DefaultExecutionStepTimeout is the maximum duration for non-inspect execution Execute calls.
	DefaultExecutionStepTimeout = 2 * time.Minute

	// DefaultWatchdogInterval is the interval between watchdog checks for expired discovery deadlines.
	DefaultWatchdogInterval = 5 * time.Second

	// DefaultArtifactSinkTimeout is the default maximum duration for an artifact sink operation (uploads/loads of small JSON: 60s).
	DefaultArtifactSinkTimeout = 60 * time.Second

	// DefaultMaxParallelTasks is the default maximum number of concurrent task sandboxes.
	DefaultMaxParallelTasks = 4

	// DefaultTaskGracePeriod is the default grace period before treating absence of a running task's sandbox as a failure.
	DefaultTaskGracePeriod = 30 * time.Second

	// DefaultPollProgressInterval is the default interval between task progress polling attempts (10s).
	DefaultPollProgressInterval = 10 * time.Second

	// DefaultMinProgressWriteInterval is the minimum duration between live progress status writes for a task (10s).
	DefaultMinProgressWriteInterval = 10 * time.Second

	// evalsListNotFoundOrNotExecutableMessage is the failure message reported when .evals/list is absent or not executable.
	evalsListNotFoundOrNotExecutableMessage = ".evals/list not found or not executable; see docs/concept/functionality.md §2.3"

	// logMsgRecreatingSandbox is the log message recorded when discovery sandbox is missing and recreated.
	logMsgRecreatingSandbox = "Discovery sandbox does not exist; recreating sandbox and restarting discovery"
)

var (
	// ErrHashDrift indicates the loaded manifest hash does not match status.discovery.manifestHash.
	ErrHashDrift = errors.New("manifest hash drift detected")

	// ErrTransient indicates a transient failure loading manifest (timeout, 5xx).
	ErrTransient = errors.New("transient error loading manifest")

	// ErrNeedsRediscovery indicates the manifest is missing from storage or non-durable sink lost it.
	ErrNeedsRediscovery = errors.New("manifest missing or non-durable sink; needs rediscovery")

	// logFileRegex matches valid Inspect JSON log filenames.
	logFileRegex = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*\.json$`)
)

// DefaultDiscoveryRetryBackoff defines the default backoff intervals between clone retry attempts.
var DefaultDiscoveryRetryBackoff = []time.Duration{
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
	40 * time.Second,
}

func backoffForAttempt(backoffs []time.Duration, attempt int) time.Duration {
	if len(backoffs) == 0 {
		return 5 * time.Second
	}
	idx := attempt - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(backoffs) {
		idx = len(backoffs) - 1
	}
	return backoffs[idx]
}

func formatAttemptCount(n int) string {
	if n == 1 {
		return "1 attempt"
	}
	return fmt.Sprintf("%d attempts", n)
}

func isNotFoundError(err error) bool {
	return status.Code(err) == codes.NotFound
}

func isPermissionError(err error) bool {
	return status.Code(err) == codes.PermissionDenied
}

func isUnavailableOrDialTimeout(err error) bool {
	if err == nil {
		return false
	}
	return status.Code(err) == codes.Unavailable
}

func defaultSleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// RunReconciler reconciles a Run object
type RunReconciler struct {
	client.Client
	APIReader     client.Reader
	Scheme        *runtime.Scheme
	ClientFactory sandboxclient.Factory
	// SandboxClient can be set directly to override ClientFactory (e.g. for unit testing).
	SandboxClient    sandboxclient.Client
	SandboxNamespace string
	MaxMessageBytes  int

	// DiscoveryDeadline is the overall deadline duration for discovery to complete.
	// Defaults to DefaultDiscoveryDeadline (10m) if <= 0.
	DiscoveryDeadline time.Duration

	// DiscoveryStepTimeout is the maximum duration for a single discovery Execute call.
	// Defaults to DefaultDiscoveryStepTimeout (60s) if <= 0.
	DiscoveryStepTimeout time.Duration

	// ExecutionDeadline is the overall deadline duration for sample execution.
	// Defaults to DefaultExecutionDeadline (30m) if <= 0.
	ExecutionDeadline time.Duration

	// ExecutionStepTimeout is the maximum duration for non-inspect execution Execute calls (e.g. git clone, checkout).
	// Defaults to DefaultExecutionStepTimeout (2m) if <= 0.
	ExecutionStepTimeout time.Duration

	// WatchdogInterval is the interval between watchdog checks for expired discovery deadlines.
	// Defaults to DefaultWatchdogInterval (5s) if <= 0.
	WatchdogInterval time.Duration

	// TaskGracePeriod is the grace period before treating absence of a running task's sandbox as a failure.
	// Defaults to DefaultTaskGracePeriod (30s) if <= 0.
	TaskGracePeriod time.Duration

	// MaxParallelTasks is the maximum number of task sandboxes to run concurrently.
	// Defaults to DefaultMaxParallelTasks (4) if <= 0.
	MaxParallelTasks int

	// PollProgressInterval overrides the interval between task progress polling attempts (default: 10s).
	PollProgressInterval time.Duration

	// MinProgressWriteInterval overrides the minimum interval between progress status writes (default: 10s).
	MinProgressWriteInterval time.Duration

	// ModelSecret is the name of a Secret in SandboxNamespace containing model credentials.
	ModelSecret string

	// SampleRunnerImage is the image used for sample execution sandboxes.
	SampleRunnerImage string

	// ArtifactSink stores raw Inspect logs and returns their storage path.
	ArtifactSink artifacts.ArtifactSink

	// ArtifactSinkTimeout is the maximum duration for a single artifact sink operation (uploads/loads of small JSON: 60s).
	// Defaults to DefaultArtifactSinkTimeout (60s) if <= 0.
	ArtifactSinkTimeout time.Duration

	// Now returns the current time. In tests, this can be overridden.
	Now func() time.Time

	// MaxDiscoveryAttempts is the maximum number of attempts for the git clone step.
	// Defaults to DefaultMaxDiscoveryAttempts (5) if <= 0.
	MaxDiscoveryAttempts int

	// MaxUncountedRequeuesPerAttempt is the cap on uncounted requeues for an unchanged endpoint
	// before counting one attempt against MaxDiscoveryAttempts.
	// Defaults to DefaultMaxUncountedRequeuesPerAttempt (6) if <= 0.
	MaxUncountedRequeuesPerAttempt int

	// DiscoveryRetryBackoff defines the backoff schedule between clone retry attempts.
	// Defaults to DefaultDiscoveryRetryBackoff if empty.
	DiscoveryRetryBackoff []time.Duration

	// Sleep pauses execution between retries. In tests, this can be overridden to return immediately.
	Sleep func(ctx context.Context, d time.Duration) error

	// ControllerNamespace is the namespace where the controller runs (defaults to eval-runner).
	ControllerNamespace string

	// PKIManager coordinates controller CA and per-sandbox server cert Secrets.
	PKIManager *pki.Manager

	// SandboxdImage is the container image used for the sandboxd initContainer.
	SandboxdImage string

	// ModelPrices maps model identifier to token pricing in USD per 1M tokens.
	ModelPrices map[string]ModelPrice

	// ModelPriceSources maps model or prefix key to pricing provenance ("configmap" or "default").
	ModelPriceSources map[string]v1alpha1.CostSource

	// ModelPricesDisabled reports whether pricing has been explicitly disabled via ConfigMap sentinel.
	ModelPricesDisabled bool

	// ControllerVersion is the version or revision of the eval-runner controller.
	// If empty, falls back to version.String().
	ControllerVersion string

	// GitHubTokenSecret is the name of a Secret containing a personal access token for GitHub reporting.
	GitHubTokenSecret string

	// GitHubAppSecret is the name of a Secret containing GitHub App credentials for check run reporting.
	GitHubAppSecret string

	// UIBaseURL is the base URL of the eval-runner UI for links in check runs.
	UIBaseURL string

	// GitHubBaseURL is an optional base URL override for the GitHub API (used in tests).
	GitHubBaseURL string

	// GitHubClientFactory creates a github.Client from credentials. If nil, uses default HTTP client.
	GitHubClientFactory func(creds GitHubCredentials) github.Client

	// GitHubClient overrides GitHub client creation (useful for testing).
	GitHubClient github.Client

	// HeadTopUpInterval is the interval between periodic head top-up Runs.
	HeadTopUpInterval time.Duration

	noCredsLoggedMu sync.Mutex
	noCredsLogged   bool

	cachedAppMu   sync.Mutex
	cachedAppKey  string
	cachedAppAuth *github.AppTokenProvider

	pkiMu sync.Mutex

	manifestCacheLock sync.RWMutex
	manifestCache     map[string]*v1alpha1.Manifest

	discoveryTrackerMu sync.Mutex
	discoveryTrackers  map[types.NamespacedName]*discoveryAttemptTracker

	branchReportMu    sync.Mutex
	branchReportLocks map[string]*sync.Mutex

	mu     sync.RWMutex
	tokens map[string]string // key: namespace/sandboxName -> bearer token

	cfgMu sync.RWMutex
}

func (r *RunReconciler) controllerVersion() string {
	if r.ControllerVersion != "" {
		return r.ControllerVersion
	}
	return version.String()
}

func (r *RunReconciler) controllerNamespace() string {
	if r.ControllerNamespace != "" {
		return r.ControllerNamespace
	}
	return discovery.DefaultControllerNamespace
}

func (r *RunReconciler) sandboxdImage() string {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.SandboxdImage != "" {
		return r.SandboxdImage
	}
	return discovery.SandboxdImage
}

func (r *RunReconciler) pki() *pki.Manager {
	r.pkiMu.Lock()
	defer r.pkiMu.Unlock()
	if r.PKIManager == nil {
		r.PKIManager = pki.NewManager(r.Client, r.controllerNamespace(), r.sandboxNamespace())
	}
	return r.PKIManager
}

func (r *RunReconciler) getAuthToken(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.tokens == nil {
		return "", false
	}
	t, ok := r.tokens[key]
	return t, ok
}

func (r *RunReconciler) setAuthToken(key, token string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tokens == nil {
		r.tokens = make(map[string]string)
	}
	r.tokens[key] = token
}

func (r *RunReconciler) deleteAuthToken(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tokens != nil {
		delete(r.tokens, key)
	}
}

func generateAuthToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate auth token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func (r *RunReconciler) ensureAuthToken(ctx context.Context, run *v1alpha1.Run, sandboxKey types.NamespacedName) (string, error) {
	if token, ok := r.getAuthToken(sandboxKey.String()); ok && token != "" {
		return token, nil
	}

	secretNamespace := run.Namespace
	if secretNamespace == "" {
		secretNamespace = discovery.DefaultControllerNamespace
	}
	secretName := discovery.AuthSecretNameForRun(run.Name)
	secretKey := types.NamespacedName{Namespace: secretNamespace, Name: secretName}

	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}

	var secret corev1.Secret
	if err := reader.Get(ctx, secretKey, &secret); err == nil {
		if tokenBytes, ok := secret.Data["token"]; ok && len(tokenBytes) > 0 {
			token := string(tokenBytes)
			r.setAuthToken(sandboxKey.String(), token)
			return token, nil
		}
	} else if !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("failed to get auth secret: %w", err)
	}

	token, err := generateAuthToken()
	if err != nil {
		return "", err
	}

	secret = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: secretNamespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"token": []byte(token),
		},
	}
	if r.Scheme != nil {
		if err := controllerutil.SetControllerReference(run, &secret, r.Scheme); err != nil {
			return "", fmt.Errorf("failed to set controller reference on auth secret: %w", err)
		}
	}

	if err := r.Create(ctx, &secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("failed to create auth secret: %w", err)
		}
		if getErr := reader.Get(ctx, secretKey, &secret); getErr != nil {
			return "", fmt.Errorf("failed to get existing auth secret: %w", getErr)
		}
		tokenBytes, ok := secret.Data["token"]
		if !ok || len(tokenBytes) == 0 {
			return "", fmt.Errorf("existing auth secret %s lacks a token", secretKey)
		}
		token = string(tokenBytes)
	}

	r.setAuthToken(sandboxKey.String(), token)
	return token, nil
}

func (r *RunReconciler) cleanupAuthToken(ctx context.Context, runNamespace, runName string, sandboxKey types.NamespacedName) {
	r.deleteAuthToken(sandboxKey.String())
	if err := r.pki().DeleteServerCertSecret(ctx, sandboxKey.Name); err != nil {
		logger := log.FromContext(ctx)
		logger.V(1).Info("Failed to delete server cert secret", "sandbox", sandboxKey.Name, "error", err)
	}
	if runNamespace == "" {
		runNamespace = discovery.DefaultControllerNamespace
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.AuthSecretNameForRun(runName),
			Namespace: runNamespace,
		},
	}
	if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
		logger := log.FromContext(ctx)
		logger.V(1).Info("Failed to delete auth secret", "secret", secret.Name, "error", err)
	}
}

func (r *RunReconciler) ensureTaskAuthToken(ctx context.Context, run *v1alpha1.Run, taskID string, sandboxKey types.NamespacedName) (string, error) {
	if token, ok := r.getAuthToken(sandboxKey.String()); ok && token != "" {
		return token, nil
	}

	secretNamespace := run.Namespace
	if secretNamespace == "" {
		secretNamespace = discovery.DefaultControllerNamespace
	}
	secretName := discovery.TaskAuthSecretNameForRun(run.Name, taskID)
	secretKey := types.NamespacedName{Namespace: secretNamespace, Name: secretName}

	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}

	var secret corev1.Secret
	if err := reader.Get(ctx, secretKey, &secret); err == nil {
		if tokenBytes, ok := secret.Data["token"]; ok && len(tokenBytes) > 0 {
			token := string(tokenBytes)
			r.setAuthToken(sandboxKey.String(), token)
			return token, nil
		}
	} else if !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("failed to get task auth secret: %w", err)
	}

	token, err := generateAuthToken()
	if err != nil {
		return "", err
	}

	secret = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: secretNamespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"token": []byte(token),
		},
	}
	if r.Scheme != nil {
		if err := controllerutil.SetControllerReference(run, &secret, r.Scheme); err != nil {
			return "", fmt.Errorf("failed to set controller reference on task auth secret: %w", err)
		}
	}

	if err := r.Create(ctx, &secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("failed to create task auth secret: %w", err)
		}
		if getErr := reader.Get(ctx, secretKey, &secret); getErr != nil {
			return "", fmt.Errorf("failed to get existing task auth secret: %w", getErr)
		}
		tokenBytes, ok := secret.Data["token"]
		if !ok || len(tokenBytes) == 0 {
			return "", fmt.Errorf("existing task auth secret %s lacks a token", secretKey)
		}
		token = string(tokenBytes)
	}

	r.setAuthToken(sandboxKey.String(), token)
	return token, nil
}

func (r *RunReconciler) cleanupTaskAuthToken(ctx context.Context, runNamespace, runName, taskID string, sandboxKey types.NamespacedName) {
	r.deleteAuthToken(sandboxKey.String())
	if err := r.pki().DeleteServerCertSecret(ctx, sandboxKey.Name); err != nil {
		logger := log.FromContext(ctx)
		logger.V(1).Info("Failed to delete task server cert secret", "sandbox", sandboxKey.Name, "error", err)
	}
	if runNamespace == "" {
		runNamespace = discovery.DefaultControllerNamespace
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      discovery.TaskAuthSecretNameForRun(runName, taskID),
			Namespace: runNamespace,
		},
	}
	if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
		logger := log.FromContext(ctx)
		logger.V(1).Info("Failed to delete task auth secret", "secret", secret.Name, "error", err)
	}
}

func (r *RunReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *RunReconciler) discoveryDeadline() time.Duration {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.DiscoveryDeadline > 0 {
		return r.DiscoveryDeadline
	}
	return DefaultDiscoveryDeadline
}

func (r *RunReconciler) discoveryStepTimeout() time.Duration {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.DiscoveryStepTimeout > 0 {
		return r.DiscoveryStepTimeout
	}
	return DefaultDiscoveryStepTimeout
}

func (r *RunReconciler) executionDeadline() time.Duration {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.ExecutionDeadline > 0 {
		return r.ExecutionDeadline
	}
	return DefaultExecutionDeadline
}

func (r *RunReconciler) executionStepTimeout() time.Duration {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.ExecutionStepTimeout > 0 {
		return r.ExecutionStepTimeout
	}
	return DefaultExecutionStepTimeout
}

func (r *RunReconciler) watchdogInterval() time.Duration {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.WatchdogInterval > 0 {
		return r.WatchdogInterval
	}
	return DefaultWatchdogInterval
}

func (r *RunReconciler) artifactSinkTimeout() time.Duration {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.ArtifactSinkTimeout > 0 {
		return r.ArtifactSinkTimeout
	}
	return DefaultArtifactSinkTimeout
}

func (r *RunReconciler) stepContext(parent context.Context, deadline *metav1.Time, maxStepTimeout time.Duration) (context.Context, context.CancelFunc) {
	if deadline != nil {
		remaining := deadline.Sub(r.now())
		if remaining <= 0 {
			return context.WithDeadline(parent, r.now().Add(-1*time.Millisecond))
		}
		if maxStepTimeout > 0 && maxStepTimeout < remaining {
			return context.WithTimeout(parent, maxStepTimeout)
		}
		return context.WithTimeout(parent, remaining)
	}
	if maxStepTimeout > 0 {
		return context.WithTimeout(parent, maxStepTimeout)
	}
	return context.WithCancel(parent)
}

func isStepTimeout(stepCtx context.Context, err error) bool {
	if stepCtx != nil && errors.Is(stepCtx.Err(), context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if status.Code(err) == codes.DeadlineExceeded {
		return true
	}
	return false
}

func executeSandboxWithTimeout(ctx context.Context, client sandboxclient.Client, cfg sandboxclient.ProcessConfig) (*sandboxclient.ExecuteResult, error) {
	type result struct {
		res *sandboxclient.ExecuteResult
		err error
	}
	ch := make(chan result, 1)
	go func() {
		res, err := client.Execute(ctx, cfg)
		ch <- result{res: res, err: err}
	}()

	select {
	case r := <-ch:
		return r.res, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func readFileWithTimeout(ctx context.Context, client sandboxclient.Client, path string) ([]byte, error) {
	type result struct {
		data []byte
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		data, err := client.ReadFile(ctx, path)
		ch <- result{data: data, err: err}
	}()

	select {
	case r := <-ch:
		return r.data, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *RunReconciler) taskGracePeriod() time.Duration {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.TaskGracePeriod > 0 {
		return r.TaskGracePeriod
	}
	return DefaultTaskGracePeriod
}

func (r *RunReconciler) maxParallelTasks() int {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.MaxParallelTasks > 0 {
		return r.MaxParallelTasks
	}
	return DefaultMaxParallelTasks
}

func (r *RunReconciler) pollProgressInterval() time.Duration {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.PollProgressInterval > 0 {
		return r.PollProgressInterval
	}
	return DefaultPollProgressInterval
}

func (r *RunReconciler) minProgressWriteInterval() time.Duration {
	if r.MinProgressWriteInterval > 0 {
		return r.MinProgressWriteInterval
	}
	return r.pollProgressInterval()
}

func (r *RunReconciler) modelSecret() string {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	return r.ModelSecret
}

func (r *RunReconciler) lookupModelPrice(model string) (ModelPrice, v1alpha1.CostSource, bool) {
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

// UpdateConfig updates the reloadable settings on RunReconciler in a thread-safe manner.
func (r *RunReconciler) UpdateConfig(cfg ClusterConfig) {
	r.cfgMu.Lock()
	defer r.cfgMu.Unlock()
	r.SandboxdImage = cfg.SandboxdImage
	r.DiscoveryDeadline = cfg.DiscoveryDeadline
	r.ExecutionDeadline = cfg.ExecutionDeadline
	r.TaskGracePeriod = cfg.TaskGracePeriod
	r.MaxParallelTasks = cfg.MaxParallelTasks
	r.PollProgressInterval = cfg.PollProgressInterval
	r.ModelSecret = cfg.ModelSecret
	r.ModelPrices = cfg.ModelPrices
	r.ModelPriceSources = cfg.ModelPriceSources
	r.ModelPricesDisabled = cfg.ModelPricesDisabled
	r.GitHubTokenSecret = cfg.GitHubTokenSecret
	r.GitHubAppSecret = cfg.GitHubAppSecret
	r.UIBaseURL = cfg.UIBaseURL
	r.HeadTopUpInterval = cfg.HeadTopUpInterval
}

func (r *RunReconciler) sampleRunnerImage() string {
	if r.SampleRunnerImage != "" {
		return r.SampleRunnerImage
	}
	return discovery.DefaultSampleRunnerImage
}

func (r *RunReconciler) artifactSink() artifacts.ArtifactSink {
	if r.ArtifactSink != nil {
		return r.ArtifactSink
	}
	return artifacts.NewInMemorySink()
}

func (r *RunReconciler) getCachedManifest(key string) *v1alpha1.Manifest {
	r.manifestCacheLock.RLock()
	defer r.manifestCacheLock.RUnlock()
	if r.manifestCache == nil {
		return nil
	}
	return r.manifestCache[key]
}

func (r *RunReconciler) setCachedManifest(key string, m *v1alpha1.Manifest) {
	r.manifestCacheLock.Lock()
	defer r.manifestCacheLock.Unlock()
	if r.manifestCache == nil {
		r.manifestCache = make(map[string]*v1alpha1.Manifest)
	}
	r.manifestCache[key] = m
}

func (r *RunReconciler) evictCachedManifest(key string) {
	r.manifestCacheLock.Lock()
	defer r.manifestCacheLock.Unlock()
	if r.manifestCache != nil {
		delete(r.manifestCache, key)
	}
}

// SetCachedManifest pre-seeds the in-memory manifest cache for a Run (e.g. in tests).
func (r *RunReconciler) SetCachedManifest(namespace, name string, m *v1alpha1.Manifest) {
	r.setCachedManifest(namespace+"/"+name, m)
}

func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "eof") ||
		strings.Contains(msg, "500") ||
		strings.Contains(msg, "502") ||
		strings.Contains(msg, "503") ||
		strings.Contains(msg, "504") ||
		strings.Contains(msg, "internal server error") ||
		strings.Contains(msg, "bad gateway") ||
		strings.Contains(msg, "service unavailable") ||
		strings.Contains(msg, "gateway timeout")
}

// getOrLoadManifest retrieves the Run's manifest from the per-Run in-memory cache,
// or loads it from the artifact sink by URI, verifying prefix confinement and hash.
func (r *RunReconciler) getOrLoadManifest(ctx context.Context, run *v1alpha1.Run) (*v1alpha1.Manifest, error) {
	key := run.Namespace + "/" + run.Name
	if cached := r.getCachedManifest(key); cached != nil {
		return cached, nil
	}

	if run.Status.Discovery == nil || run.Status.Discovery.ManifestPath == "" {
		return nil, fmt.Errorf("%w: discovery status or manifest path missing", ErrNeedsRediscovery)
	}

	manifestPath := run.Status.Discovery.ManifestPath
	prefix := r.artifactSink().ArtifactPrefix(run.Spec.Repo.Commit)
	if prefix != "" && !strings.HasPrefix(manifestPath, prefix) {
		return nil, fmt.Errorf("security violation: manifest path %q is not confined to commit prefix %q", manifestPath, prefix)
	}

	sink := r.artifactSink()
	var deadline *metav1.Time
	if run.Status.ExecutionDeadline != nil {
		deadline = run.Status.ExecutionDeadline
	} else if run.Status.DiscoveryDeadline != nil {
		deadline = run.Status.DiscoveryDeadline
	}
	loadCtx, loadCancel := r.stepContext(ctx, deadline, r.artifactSinkTimeout())
	data, err := artifacts.LoadWithTimeout(loadCtx, sink, manifestPath)
	loadCancel()
	if err != nil {
		if !sink.IsDurable() {
			return nil, fmt.Errorf("%w: non-durable sink lost manifest (%v)", ErrNeedsRediscovery, err)
		}
		if artifacts.IsNotFound(err) {
			return nil, fmt.Errorf("%w: manifest not found at %s: %v", ErrNeedsRediscovery, manifestPath, err)
		}
		if isStepTimeout(loadCtx, err) {
			return nil, fmt.Errorf("%w: load manifest from %s timed out: %v", ErrTransient, manifestPath, err)
		}
		if isTransientError(err) {
			return nil, fmt.Errorf("%w: %v", ErrTransient, err)
		}
		// Treat unclassified storage read failures on durable sink as transient to avoid deleting task sandboxes prematurely
		return nil, fmt.Errorf("%w: %v", ErrTransient, err)
	}

	h := sha256.Sum256(data)
	actualHash := "sha256:" + hex.EncodeToString(h[:])
	if run.Status.Discovery.ManifestHash != "" && actualHash != run.Status.Discovery.ManifestHash {
		return nil, fmt.Errorf("%w: expected %s, got %s", ErrHashDrift, run.Status.Discovery.ManifestHash, actualHash)
	}

	if int64(len(data)) > discovery.MaxManifestBytes {
		return nil, fmt.Errorf("manifest size %d exceeds 1 MiB limit", len(data))
	}

	manifest, parseErr := discovery.ParseManifest(data)
	if parseErr != nil {
		return nil, fmt.Errorf("invalid manifest JSON: %w", parseErr)
	}
	if valErr := discovery.ValidateManifest(manifest); valErr != nil {
		return nil, fmt.Errorf("manifest validation failed: %w", valErr)
	}

	r.setCachedManifest(key, manifest)
	return manifest, nil
}

type discoveryAttemptTracker struct {
	uncountedRequeues int
	countedAttempts   int
}

func (r *RunReconciler) maxDiscoveryAttempts() int {
	if r.MaxDiscoveryAttempts > 0 {
		return r.MaxDiscoveryAttempts
	}
	return DefaultMaxDiscoveryAttempts
}

func (r *RunReconciler) maxUncountedRequeuesPerAttempt() int {
	if r.MaxUncountedRequeuesPerAttempt > 0 {
		return r.MaxUncountedRequeuesPerAttempt
	}
	return DefaultMaxUncountedRequeuesPerAttempt
}

func (r *RunReconciler) getDiscoveryTracker(key types.NamespacedName) *discoveryAttemptTracker {
	r.discoveryTrackerMu.Lock()
	defer r.discoveryTrackerMu.Unlock()
	if r.discoveryTrackers == nil {
		r.discoveryTrackers = make(map[types.NamespacedName]*discoveryAttemptTracker)
	}
	t, ok := r.discoveryTrackers[key]
	if !ok {
		t = &discoveryAttemptTracker{}
		r.discoveryTrackers[key] = t
	}
	return t
}

func (r *RunReconciler) clearDiscoveryTracker(key types.NamespacedName) {
	r.discoveryTrackerMu.Lock()
	defer r.discoveryTrackerMu.Unlock()
	if r.discoveryTrackers != nil {
		delete(r.discoveryTrackers, key)
	}
}

func (r *RunReconciler) requeueDelay(deadline *metav1.Time) time.Duration {
	defaultDelay := 2 * time.Second
	if deadline == nil {
		return defaultDelay
	}
	remaining := deadline.Sub(r.now())
	if remaining <= 0 {
		return 0
	}
	if remaining < defaultDelay {
		return remaining
	}
	return defaultDelay
}

func (r *RunReconciler) maxMessageBytes() int {
	if r.MaxMessageBytes > 0 {
		return r.MaxMessageBytes
	}
	return discovery.MaxMessageBytes
}

func (r *RunReconciler) maxStderrLines() int {
	return discovery.MaxStderrLines
}

// Reconcile handles the Run state machine:
//  1. Pending -> ensure finalizer, create Sandbox in eval-runner-sandbox, set Discovering.
//  2. Wait for sandbox pod Ready, drive step-based discovery via sandboxd (gRPC ProcessService).
//     - Step 1: git clone -- <url> repo (retries up to N times on failure)
//     - Step 2: git checkout <commit> (fails immediately on error)
//     - Step 3: ./.evals/list (fails immediately on error, typed outcomes)
//  3. Parse and validate manifest, write status.manifest, set Discovered.
//     On failure, set Failed with message naming the failed step.
//  4. Delete sandbox when done, idempotent across restarts.
//  5. On Run deletion, GC discovery sandbox and remove finalizer.
func (r *RunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	sandboxNamespace := r.SandboxNamespace
	if sandboxNamespace == "" {
		sandboxNamespace = discovery.DefaultSandboxNamespace
	}

	sandboxName := discovery.SandboxNameForRun(req.Name)
	sandboxKey := types.NamespacedName{Namespace: sandboxNamespace, Name: sandboxName}

	var run v1alpha1.Run
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		if apierrors.IsNotFound(err) {
			// Run was deleted. Clean up any leftover sandbox matching this Run.
			if err := r.deleteSandboxIfMatching(ctx, sandboxKey, req.Namespace, req.Name); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Deletion lifecycle: Run is marked for deletion. Clean up all sandboxes and remove finalizer.
	if !run.DeletionTimestamp.IsZero() {
		r.clearDiscoveryTracker(req.NamespacedName)
		r.evictCachedManifest(run.Namespace + "/" + run.Name)
		if controllerutil.ContainsFinalizer(&run, v1alpha1.FinalizerDiscoverySandbox) {
			logger.Info("Run is being deleted; cleaning up sandboxes", "run", run.Name)
			if err := r.deleteAllSandboxesForRun(ctx, run.Namespace, run.Name); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&run, v1alpha1.FinalizerDiscoverySandbox)
			if err := r.Update(ctx, &run); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Terminal states: already finished, do nothing (clean up any leftover sandboxes).
	isTerminal := run.Status.Phase == v1alpha1.RunPhaseCompleted ||
		run.Status.Phase == v1alpha1.RunPhaseFailed ||
		(run.Status.Phase == v1alpha1.RunPhaseDiscovered && run.Spec.Model == "")

	if isTerminal {
		r.evictCachedManifest(run.Namespace + "/" + run.Name)
		if err := r.deleteAllSandboxesForRun(ctx, run.Namespace, run.Name); err != nil {
			logger.Error(err, "failed to delete leftover sandboxes for terminal run", "sandbox", sandboxName)
		}
		if err := r.reportBranchHeadTerminal(ctx, &run); err != nil {
			logger.Error(err, "failed to report branch-head check run for terminal run", "run", run.Name)
		}
		return ctrl.Result{}, nil
	}

	if err := r.reportBranchHeadInProgress(ctx, &run); err != nil {
		logger.Error(err, "failed to report branch head in-progress check run", "run", run.Name)
	}

	// Validate RunSpec constraints (mutual exclusivity of limit and sampleRange, valid range bounds).
	if err := ValidateRunSpec(run.Spec); err != nil {
		logger.Error(err, "invalid run spec", "run", run.Name)
		return r.failRun(ctx, &run, nil, fmt.Sprintf("invalid spec: %v", err))
	}

	// Ensure finalizer is attached before creating or processing sandbox.
	if !controllerutil.ContainsFinalizer(&run, v1alpha1.FinalizerDiscoverySandbox) {
		controllerutil.AddFinalizer(&run, v1alpha1.FinalizerDiscoverySandbox)
		if err := r.Update(ctx, &run); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Fail fast if artifact storage is configured but unavailable (e.g. invalid credentials).
	if unavailErr, isUnavail := artifacts.IsUnavailable(r.artifactSink()); isUnavail {
		msg := fmt.Sprintf("artifactBucket is set but credentials are unavailable: %v", unavailErr)
		logger.Error(unavailErr, "artifact storage is unavailable; failing Run", "run", run.Name)
		return r.failRun(ctx, &run, nil, msg)
	}

	// If Run is already in Running phase, proceed to task sample execution
	if run.Status.Phase == v1alpha1.RunPhaseRunning {
		return r.reconcileRunning(ctx, req, &run)
	}

	// If Run is in Discovered phase and Spec.Model is set, transition to Running
	if run.Status.Phase == v1alpha1.RunPhaseDiscovered && run.Spec.Model != "" {
		manifest, loadErr := r.getOrLoadManifest(ctx, &run)
		if loadErr != nil {
			if errors.Is(loadErr, ErrTransient) {
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
			if errors.Is(loadErr, ErrNeedsRediscovery) {
				updateErr := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
					status.Phase = v1alpha1.RunPhasePending
					status.DiscoveryDeadline = nil
					status.Discovery = nil
					status.Tasks = nil
					status.Summary = nil
					status.Message = "Manifest missing; re-discovering"
					meta.SetStatusCondition(&status.Conditions, metav1.Condition{
						Type:               v1alpha1.ConditionTypeDiscovered,
						Status:             metav1.ConditionFalse,
						Reason:             "Rediscovering",
						Message:            "Manifest missing; re-discovering",
						ObservedGeneration: gen,
					})
				})
				return ctrl.Result{Requeue: true}, updateErr
			}
			if errors.Is(loadErr, ErrHashDrift) {
				if updateErr := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
					status.Message = loadErr.Error()
				}); updateErr != nil {
					return ctrl.Result{}, updateErr
				}
				return ctrl.Result{}, nil
			}
			return r.failRun(ctx, &run, nil, fmt.Sprintf("failed to load manifest: %v", loadErr))
		}

		initialTasks := r.initializeTasks(run.Spec, manifest)
		dl := metav1.NewTime(r.now().Add(r.executionDeadline()))
		err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
			status.Phase = v1alpha1.RunPhaseRunning
			status.Tasks = initialTasks
			status.ExecutionDeadline = &dl
			if prefix := r.artifactSink().ArtifactPrefix(run.Spec.Repo.Commit); prefix != "" {
				status.Artifacts = prefix
			}
			status.Summary = ComputeRunSummary(status.Tasks)
		})
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	var sandbox sandboxv1beta1.Sandbox
	err := r.Get(ctx, sandboxKey, &sandbox)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	sandboxExists := (err == nil)
	if !sandboxExists && run.Status.Phase == v1alpha1.RunPhaseDiscovering {
		if foundSB, findErr := r.findDiscoverySandbox(ctx, &run); findErr == nil && foundSB != nil {
			sandbox = *foundSB
			sandboxExists = true
		}
	}

	// Step 1: Create sandbox if it does not exist
	if !sandboxExists {
		if run.Status.Phase == v1alpha1.RunPhaseDiscovering {
			logger.Info(logMsgRecreatingSandbox, "run", run.Name, "sandbox", sandboxName)
		}
		if err := r.recreateDiscoverySandbox(ctx, &run); err != nil {
			return ctrl.Result{}, err
		}

		deadline := metav1.NewTime(r.now().Add(r.discoveryDeadline()))
		err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
			status.Phase = v1alpha1.RunPhaseDiscovering
			status.Message = ""
			if status.DiscoveryDeadline == nil {
				status.DiscoveryDeadline = &deadline
			}
			meta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               v1alpha1.ConditionTypeDiscovered,
				Status:             metav1.ConditionFalse,
				Reason:             "Discovering",
				Message:            "Running discovery in sandbox",
				ObservedGeneration: gen,
			})
		})
		if err != nil {
			return ctrl.Result{}, err
		}
		run.Status.Phase = v1alpha1.RunPhaseDiscovering
		run.Status.DiscoveryDeadline = &deadline

		return ctrl.Result{RequeueAfter: r.requeueDelay(run.Status.DiscoveryDeadline)}, nil
	}

	// Sandbox exists. Ensure phase reflects Discovering and deadline is recorded (back-filled for in-flight Runs)
	if run.Status.Phase != v1alpha1.RunPhaseDiscovering || run.Status.DiscoveryDeadline == nil {
		var setDeadline *metav1.Time
		err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
			status.Phase = v1alpha1.RunPhaseDiscovering
			if status.DiscoveryDeadline == nil {
				dl := metav1.NewTime(r.now().Add(r.discoveryDeadline()))
				status.DiscoveryDeadline = &dl
			}
			setDeadline = status.DiscoveryDeadline
			meta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               v1alpha1.ConditionTypeDiscovered,
				Status:             metav1.ConditionFalse,
				Reason:             "Discovering",
				Message:            "Running discovery in sandbox",
				ObservedGeneration: gen,
			})
		})
		if err != nil {
			return ctrl.Result{}, err
		}
		run.Status.Phase = v1alpha1.RunPhaseDiscovering
		run.Status.DiscoveryDeadline = setDeadline
	}

	// Step 2: Check backing pod and conditions
	podName := sandbox.Name
	if name, ok := sandbox.Annotations[sandboxv1beta1.SandboxPodNameAnnotation]; ok && name != "" {
		podName = name
	}

	var pod corev1.Pod
	podKey := types.NamespacedName{Namespace: sandboxNamespace, Name: podName}
	podErr := r.Get(ctx, podKey, &pod)
	if podErr != nil && !apierrors.IsNotFound(podErr) {
		return ctrl.Result{}, podErr
	}
	podExists := (podErr == nil)

	// Check if overall discovery deadline has been exceeded.
	if run.Status.DiscoveryDeadline != nil && !r.now().Before(run.Status.DiscoveryDeadline.Time) {
		var podPtr *corev1.Pod
		if podExists {
			podPtr = &pod
		}
		msg := formatPodDeadlineMessage(podPtr)
		logger.Info("Discovery deadline exceeded; failing Run", "run", run.Name, "message", msg)
		return r.failRun(ctx, &run, &sandbox, msg)
	}

	if !podExists {
		logger.Info("Backing pod not yet created", "pod", podKey)
		return ctrl.Result{RequeueAfter: r.requeueDelay(run.Status.DiscoveryDeadline)}, nil
	}

	if pod.Status.Phase == corev1.PodFailed {
		msg := "Sandbox pod failed"
		if pod.Status.Message != "" {
			msg = fmt.Sprintf("%s: %s", msg, pod.Status.Message)
		}
		return r.failRun(ctx, &run, &sandbox, msg)
	}

	if pod.Status.Phase == corev1.PodSucceeded {
		msg := "Sandbox pod succeeded unexpectedly before discovery completed"
		if pod.Status.Message != "" {
			msg = fmt.Sprintf("%s: %s", msg, pod.Status.Message)
		}
		return r.failRun(ctx, &run, &sandbox, msg)
	}

	finishedCond := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionFinished))
	if finishedCond != nil && finishedCond.Status == metav1.ConditionTrue {
		if finishedCond.Reason == sandboxv1beta1.SandboxReasonPodFailed {
			msg := "Sandbox pod failed"
			if finishedCond.Message != "" {
				msg = fmt.Sprintf("%s: %s", msg, finishedCond.Message)
			}
			return r.failRun(ctx, &run, &sandbox, msg)
		}
		if finishedCond.Reason == sandboxv1beta1.SandboxReasonPodSucceeded {
			msg := "Sandbox pod succeeded unexpectedly before discovery completed"
			if finishedCond.Message != "" {
				msg = fmt.Sprintf("%s: %s", msg, finishedCond.Message)
			}
			return r.failRun(ctx, &run, &sandbox, msg)
		}
	}

	sandboxReady := meta.IsStatusConditionTrue(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady))
	if !sandboxReady && !isPodReady(&pod) {
		logger.Info("Backing pod not yet ready", "pod", podKey, "phase", pod.Status.Phase)
		return ctrl.Result{RequeueAfter: r.requeueDelay(run.Status.DiscoveryDeadline)}, nil
	}

	// Obtain sandbox client (via direct instance or factory from pod IP)
	var sbClient sandboxclient.Client
	currentEndpoint := pod.Status.PodIP
	if currentEndpoint == "" {
		currentEndpoint = r.resolveSandboxEndpoint(ctx, &sandbox)
	}
	if currentEndpoint == "" && r.SandboxClient == nil {
		logger.Info("Pod IP not yet assigned", "pod", podKey)
		return ctrl.Result{RequeueAfter: r.requeueDelay(run.Status.DiscoveryDeadline)}, nil
	}

	var clientErr error
	sbClient, clientErr = r.createSandboxClient(ctx, &run, sandbox.Name, sandbox.Namespace, currentEndpoint)
	if clientErr != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create sandbox client for %s: %w", currentEndpoint, clientErr)
	}
	defer func() {
		if sbClient != nil && sbClient != r.SandboxClient {
			if closeErr := sbClient.Close(); closeErr != nil {
				logger.Error(closeErr, "failed to close sandbox client")
			}
		}
	}()

	// Set execution deadline context
	if run.Status.DiscoveryDeadline != nil {
		remaining := run.Status.DiscoveryDeadline.Sub(r.now())
		if remaining <= 0 {
			return r.failRun(ctx, &run, &sandbox, formatPodDeadlineMessage(&pod))
		}
	}

	maxAttempts := r.maxDiscoveryAttempts()
	backoffs := r.DiscoveryRetryBackoff
	if len(backoffs) == 0 {
		backoffs = DefaultDiscoveryRetryBackoff
	}

	sleepFn := r.Sleep
	if sleepFn == nil {
		sleepFn = defaultSleep
	}

	// -------------------------------------------------------------------------
	// Step 1: git clone -- <url> repo (retry up to N times on failure)
	// -------------------------------------------------------------------------
	logger.Info("Sandbox pod is ready; starting step 1: git clone", "pod", podKey)
	var cloneRes *sandboxclient.ExecuteResult
	var cloneErr error
	var cloneAttempts int
	var uncountedRetried bool
	var lastCloneStepCtx context.Context

	handleUnavailable := func(dialErr error) (retried bool, handled bool, res ctrl.Result, err error) {
		logger.Info("Discovery step failed with Unavailable; checking sandbox status", "error", dialErr, "run", run.Name, "endpoint", currentEndpoint)

		foundSB, sbErr := r.findDiscoverySandbox(ctx, &run)
		if sbErr != nil {
			return false, true, ctrl.Result{}, sbErr
		}
		if foundSB == nil {
			logger.Info(logMsgRecreatingSandbox, "run", run.Name, "sandbox", sandboxName)
			if err := r.recreateDiscoverySandbox(ctx, &run); err != nil {
				return false, true, ctrl.Result{}, err
			}
			if updateErr := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
				status.Phase = v1alpha1.RunPhaseDiscovering
				status.Message = ""
				meta.SetStatusCondition(&status.Conditions, metav1.Condition{
					Type:               v1alpha1.ConditionTypeDiscovered,
					Status:             metav1.ConditionFalse,
					Reason:             "Discovering",
					Message:            "Running discovery in sandbox",
					ObservedGeneration: gen,
				})
			}); updateErr != nil {
				logger.Error(updateErr, "failed to update run status after recreating sandbox")
			}
			return false, true, ctrl.Result{RequeueAfter: r.requeueDelay(run.Status.DiscoveryDeadline)}, nil
		}

		newEndpoint := r.resolveFreshSandboxEndpoint(ctx, foundSB)
		if newEndpoint == "" || newEndpoint == currentEndpoint {
			tracker := r.getDiscoveryTracker(req.NamespacedName)
			tracker.uncountedRequeues++

			// Carry the last dial error into status.message on each requeue
			boundedErr := truncateMessage(dialErr.Error(), r.maxMessageBytes())
			if updateErr := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
				status.Message = boundedErr
			}); updateErr != nil {
				logger.Error(updateErr, "failed to update run status message on unavailable requeue")
			}

			if tracker.uncountedRequeues >= r.maxUncountedRequeuesPerAttempt() {
				tracker.uncountedRequeues = 0
				tracker.countedAttempts++
				maxAttempts := r.maxDiscoveryAttempts()
				logger.Info("Uncounted requeue cap reached; counting attempt",
					"run", run.Name, "countedAttempts", tracker.countedAttempts, "maxAttempts", maxAttempts)

				if tracker.countedAttempts >= maxAttempts {
					r.clearDiscoveryTracker(req.NamespacedName)
					msg := fmt.Sprintf("step git clone failed after %s: %v", formatAttemptCount(maxAttempts), dialErr)
					failRes, failErr := r.failRun(ctx, &run, foundSB, msg)
					return false, true, failRes, failErr
				}
			}

			logger.Info("Sandbox endpoint unchanged or not yet ready after Unavailable failure; requeuing without counting attempt",
				"run", run.Name, "endpoint", currentEndpoint, "resolved", newEndpoint,
				"uncountedRequeues", tracker.uncountedRequeues, "countedAttempts", tracker.countedAttempts)
			return false, true, ctrl.Result{RequeueAfter: r.requeueDelay(run.Status.DiscoveryDeadline)}, nil
		}

		// Reset uncounted requeues on endpoint change
		tracker := r.getDiscoveryTracker(req.NamespacedName)
		tracker.uncountedRequeues = 0

		newClient, clientErr := r.createSandboxClient(ctx, &run, foundSB.Name, foundSB.Namespace, newEndpoint)
		if clientErr != nil {
			return false, true, ctrl.Result{}, fmt.Errorf("failed to create sandbox client for recovered endpoint %s: %w", newEndpoint, clientErr)
		}
		if sbClient != nil && sbClient != r.SandboxClient {
			if closeErr := sbClient.Close(); closeErr != nil {
				logger.Error(closeErr, "failed to close sandbox client during endpoint recovery")
			}
		}
		sbClient = newClient
		currentEndpoint = newEndpoint
		return true, false, ctrl.Result{}, nil
	}

	executeCleanAndClone := func() (*sandboxclient.ExecuteResult, error) {
		cleanCtx, cleanCancel := r.stepContext(ctx, run.Status.DiscoveryDeadline, r.discoveryStepTimeout())
		defer cleanCancel()
		lastCloneStepCtx = cleanCtx
		if _, cleanErr := executeSandboxWithTimeout(cleanCtx, sbClient, sandboxclient.ProcessConfig{
			Command: []string{"rm", "-rf", "repo"},
		}); cleanErr != nil {
			logger.V(1).Info("Failed to clean repo directory before clone attempt", "error", cleanErr)
			if isStepTimeout(cleanCtx, cleanErr) {
				return nil, cleanErr
			}
			if isUnavailableOrDialTimeout(cleanErr) {
				return nil, cleanErr
			}
		}

		cloneCtx, cloneCancel := r.stepContext(ctx, run.Status.DiscoveryDeadline, r.discoveryStepTimeout())
		defer cloneCancel()
		lastCloneStepCtx = cloneCtx
		return executeSandboxWithTimeout(cloneCtx, sbClient, sandboxclient.ProcessConfig{
			Command: []string{"git", "clone", "--", run.Spec.Repo.URL, "repo"},
		})
	}

	for cloneAttempts = 1; cloneAttempts <= maxAttempts; cloneAttempts++ {
		if run.Status.DiscoveryDeadline != nil && !r.now().Before(run.Status.DiscoveryDeadline.Time) {
			break
		}

		// Re-resolve the sandbox pod IP (informer Get, falling back to Sandbox.status) before every attempt
		latestEndpoint := r.resolveSandboxEndpoint(ctx, &sandbox)
		if latestEndpoint != "" && latestEndpoint != currentEndpoint {
			newClient, clientErr := r.createSandboxClient(ctx, &run, sandbox.Name, sandbox.Namespace, latestEndpoint)
			if clientErr != nil {
				logger.Error(clientErr, "failed to recreate sandbox client on endpoint change", "endpoint", latestEndpoint)
			} else {
				if sbClient != nil && sbClient != r.SandboxClient {
					if closeErr := sbClient.Close(); closeErr != nil {
						logger.Error(closeErr, "failed to close old sandbox client on endpoint change")
					}
				}
				sbClient = newClient
				currentEndpoint = latestEndpoint
			}
		}

		cloneRes, cloneErr = executeCleanAndClone()

		if isStepTimeout(lastCloneStepCtx, cloneErr) {
			return r.failRun(ctx, &run, &sandbox, "step git clone failed: sandbox did not report completion")
		}

		if isUnavailableOrDialTimeout(cloneErr) && !uncountedRetried {
			uncountedRetried = true
			recovered, handled, res, err := handleUnavailable(cloneErr)
			if handled {
				return res, err
			}
			if recovered {
				cloneRes, cloneErr = executeCleanAndClone()
				if isStepTimeout(lastCloneStepCtx, cloneErr) {
					return r.failRun(ctx, &run, &sandbox, "step git clone failed: sandbox did not report completion")
				}
			}
		} else if isUnavailableOrDialTimeout(cloneErr) {
			_, handled, res, err := handleUnavailable(cloneErr)
			if handled {
				return res, err
			}
		}

		if cloneErr == nil && cloneRes != nil && cloneRes.ExitCode == 0 {
			break
		}

		if status.Code(cloneErr) == codes.Unauthenticated {
			break
		}

		if cloneAttempts >= maxAttempts {
			break
		}

		if run.Status.DiscoveryDeadline != nil && !r.now().Before(run.Status.DiscoveryDeadline.Time) {
			break
		}

		backoffDuration := backoffForAttempt(backoffs, cloneAttempts)
		if run.Status.DiscoveryDeadline != nil {
			rem := run.Status.DiscoveryDeadline.Sub(r.now())
			if rem <= 0 {
				break
			}
			if rem < backoffDuration {
				backoffDuration = rem
			}
		}

		logger.Info("Clone step failed; retrying with backoff",
			"attempt", cloneAttempts,
			"maxAttempts", maxAttempts,
			"error", cloneErr,
			"backoff", backoffDuration,
		)

		attemptMsg := fmt.Sprintf("Running discovery in sandbox (step git clone attempt %d of %d failed; retrying)", cloneAttempts, maxAttempts)
		retryMsg := fmt.Sprintf("step git clone attempt %d of %d failed; retrying", cloneAttempts, maxAttempts)
		if updateErr := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
			meta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               v1alpha1.ConditionTypeDiscovered,
				Status:             metav1.ConditionFalse,
				Reason:             "Discovering",
				Message:            attemptMsg,
				ObservedGeneration: gen,
			})
			status.Message = retryMsg
		}); updateErr != nil {
			logger.Error(updateErr, "failed to update run status during retry", "attempt", cloneAttempts)
		}

		sleepCtx, sleepCancel := r.stepContext(ctx, run.Status.DiscoveryDeadline, backoffDuration)
		sleepErr := sleepFn(sleepCtx, backoffDuration)
		sleepCancel()
		if sleepErr != nil {
			logger.Info("Context done while waiting to retry clone", "error", sleepErr)
			break
		}
	}

	// Evaluate git clone outcome
	if cloneErr != nil || cloneRes == nil || cloneRes.ExitCode != 0 {
		if isStepTimeout(lastCloneStepCtx, cloneErr) {
			return r.failRun(ctx, &run, &sandbox, "step git clone failed: sandbox did not report completion")
		}
		var cloneStderr string
		var exitCode int
		if cloneRes != nil {
			cloneStderr = cloneRes.Stderr
			exitCode = cloneRes.ExitCode
		}
		tail := extractStderrTail(cloneStderr, r.maxStderrLines(), discovery.MaxStderrTailBytes)
		isDeadlineExceeded := (run.Status.DiscoveryDeadline != nil && !r.now().Before(run.Status.DiscoveryDeadline.Time))
		if isDeadlineExceeded {
			msg := formatPodDeadlineMessage(&pod)
			if tail != "" {
				msg += "\n" + tail
			}
			return r.failRun(ctx, &run, &sandbox, msg)
		}

		var msg string
		if status.Code(cloneErr) == codes.Unauthenticated {
			msg = fmt.Sprintf("step git clone failed: unauthenticated: %v", cloneErr)
		} else if tail != "" {
			msg = fmt.Sprintf("step git clone failed after %s: exit code %d: %s", formatAttemptCount(cloneAttempts), exitCode, tail)
		} else if cloneErr != nil {
			msg = fmt.Sprintf("step git clone failed after %s: %v", formatAttemptCount(cloneAttempts), cloneErr)
		} else {
			msg = fmt.Sprintf("step git clone failed after %s: exit code %d", formatAttemptCount(cloneAttempts), exitCode)
		}
		return r.failRun(ctx, &run, &sandbox, msg)
	}

	// -------------------------------------------------------------------------
	// Step 2: git checkout <commit> (no retry: commit not found is permanent)
	// -------------------------------------------------------------------------
	logger.Info("Step 1 succeeded; starting step 2: git checkout", "commit", run.Spec.Repo.Commit)
	checkoutCtx, checkoutCancel := r.stepContext(ctx, run.Status.DiscoveryDeadline, r.discoveryStepTimeout())
	checkoutRes, checkoutErr := executeSandboxWithTimeout(checkoutCtx, sbClient, sandboxclient.ProcessConfig{
		Command: []string{"git", "checkout", run.Spec.Repo.Commit},
		Cwd:     "repo",
	})
	checkoutCancel()
	if checkoutErr != nil || checkoutRes == nil || checkoutRes.ExitCode != 0 {
		if isStepTimeout(checkoutCtx, checkoutErr) {
			return r.failRun(ctx, &run, &sandbox, "step git checkout failed: sandbox did not report completion")
		}
		if status.Code(checkoutErr) == codes.Unauthenticated {
			return r.failRun(ctx, &run, &sandbox, fmt.Sprintf("step git checkout failed: unauthenticated: %v", checkoutErr))
		}
		var checkoutStderr string
		var exitCode int
		if checkoutRes != nil {
			checkoutStderr = checkoutRes.Stderr
			exitCode = checkoutRes.ExitCode
		}
		tail := extractStderrTail(checkoutStderr, r.maxStderrLines(), discovery.MaxStderrTailBytes)
		isDeadlineExceeded := (run.Status.DiscoveryDeadline != nil && !r.now().Before(run.Status.DiscoveryDeadline.Time))
		if isDeadlineExceeded {
			msg := formatPodDeadlineMessage(&pod)
			if tail != "" {
				msg += "\n" + tail
			}
			return r.failRun(ctx, &run, &sandbox, msg)
		}

		var msg string
		if tail != "" {
			msg = fmt.Sprintf("step git checkout failed: exit code %d: %s", exitCode, tail)
		} else if checkoutErr != nil {
			msg = fmt.Sprintf("step git checkout failed: %v", checkoutErr)
		} else {
			msg = fmt.Sprintf("step git checkout failed: exit code %d", exitCode)
		}
		return r.failRun(ctx, &run, &sandbox, msg)
	}

	// -------------------------------------------------------------------------
	// Step 3: ./.evals/list (writes /workspace/out/manifest.json)
	// -------------------------------------------------------------------------
	logger.Info("Step 2 succeeded; starting step 3: discover evals to file", "command", discovery.DiscoveryCommand)
	listCtx, listCancel := r.stepContext(ctx, run.Status.DiscoveryDeadline, 0)
	listRes, listErr := executeSandboxWithTimeout(listCtx, sbClient, sandboxclient.ProcessConfig{
		Command: []string{"sh", "-c", discovery.DiscoveryCommand},
		Cwd:     "repo",
	})
	listCancel()

	if listErr != nil {
		if isStepTimeout(listCtx, listErr) {
			return r.failRun(ctx, &run, &sandbox, "step ./.evals/list failed: sandbox did not report completion")
		}
		var limitErr *discovery.LimitError
		if errors.As(listErr, &limitErr) {
			var tail string
			if listRes != nil {
				tail = extractStderrTail(listRes.Stderr, r.maxStderrLines(), discovery.MaxStderrTailBytes)
			}
			msg := fmt.Sprintf("step ./.evals/list failed: %v", limitErr)
			if tail != "" {
				msg += "\n" + tail
			}
			return r.failRun(ctx, &run, &sandbox, msg)
		}

		if status.Code(listErr) == codes.Unauthenticated {
			return r.failRun(ctx, &run, &sandbox, fmt.Sprintf("step ./.evals/list failed: unauthenticated: %v", listErr))
		}
		if isNotFoundError(listErr) || isPermissionError(listErr) {
			var tail string
			if listRes != nil {
				tail = extractStderrTail(listRes.Stderr, r.maxStderrLines(), discovery.MaxStderrTailBytes)
			}
			msg := evalsListNotFoundOrNotExecutableMessage
			if tail != "" {
				msg += "\n" + tail
			}
			return r.failRun(ctx, &run, &sandbox, msg)
		}
		var tail string
		if listRes != nil {
			tail = extractStderrTail(listRes.Stderr, r.maxStderrLines(), discovery.MaxStderrTailBytes)
		}
		msg := fmt.Sprintf("step ./.evals/list failed: %v", listErr)
		if tail != "" {
			msg += "\n" + tail
		}
		return r.failRun(ctx, &run, &sandbox, msg)
	}

	if listRes == nil {
		return r.failRun(ctx, &run, &sandbox, "step ./.evals/list failed: nil response")
	}

	if listRes.ExitCode != 0 {
		tail := extractStderrTail(listRes.Stderr, r.maxStderrLines(), discovery.MaxStderrTailBytes)
		if listRes.ExitCode == 126 || listRes.ExitCode == 127 {
			msg := evalsListNotFoundOrNotExecutableMessage
			if tail != "" {
				msg += "\n" + tail
			}
			return r.failRun(ctx, &run, &sandbox, msg)
		}
		var msg string
		if tail != "" {
			msg = fmt.Sprintf("step ./.evals/list failed with exit code %d: %s", listRes.ExitCode, tail)
		} else {
			msg = fmt.Sprintf("step ./.evals/list failed with exit code %d", listRes.ExitCode)
		}
		return r.failRun(ctx, &run, &sandbox, msg)
	}

	// Step 4: Pull manifest via bounded sandboxd ReadFile (resolves under /workspace)
	manifestCtx, manifestCancel := r.stepContext(ctx, run.Status.DiscoveryDeadline, r.discoveryStepTimeout())
	manifestBytes, readErr := readFileWithTimeout(manifestCtx, sbClient, discovery.DiscoveryManifestReadPath)
	manifestCancel()
	if readErr != nil {
		if isStepTimeout(manifestCtx, readErr) {
			return r.failRun(ctx, &run, &sandbox, "step ./.evals/list failed: sandbox did not report completion")
		}
		var limitErr *discovery.LimitError
		if errors.As(readErr, &limitErr) {
			return r.failRun(ctx, &run, &sandbox, fmt.Sprintf("step ./.evals/list failed: %v", limitErr))
		}
		if status.Code(readErr) == codes.Unauthenticated {
			return r.failRun(ctx, &run, &sandbox, fmt.Sprintf("step ./.evals/list failed: unauthenticated: %v", readErr))
		}
		return r.failRun(ctx, &run, &sandbox, fmt.Sprintf("step ./.evals/list failed: %v", readErr))
	}

	// Controller-side 1 MiB check before parse
	if int64(len(manifestBytes)) > discovery.MaxManifestBytes {
		return r.failRun(ctx, &run, &sandbox, fmt.Sprintf("step ./.evals/list failed: manifest size %d exceeds 1 MiB limit", len(manifestBytes)))
	}

	manifest, err := discovery.ParseManifest(manifestBytes)
	if err != nil {
		tail := extractStderrTail(listRes.Stderr, r.maxStderrLines(), discovery.MaxStderrTailBytes)
		msg := fmt.Sprintf("step ./.evals/list failed: invalid JSON: %v", err)
		if tail != "" {
			msg += "\n" + tail
		}
		return r.failRun(ctx, &run, &sandbox, msg)
	}

	if err := discovery.ValidateManifest(manifest); err != nil {
		return r.failRun(ctx, &run, &sandbox, fmt.Sprintf("Manifest validation failed: %v", err))
	}

	// Store manifest in artifact store
	sinkCtx, sinkCancel := r.stepContext(ctx, run.Status.DiscoveryDeadline, r.artifactSinkTimeout())
	manifestURI, storeErr := artifacts.StoreManifestWithTimeout(sinkCtx, r.artifactSink(), artifacts.RunMetadata{
		RunName:      run.Name,
		RunNamespace: run.Namespace,
		Commit:       run.Spec.Repo.Commit,
		TaskCount:    len(manifest.Tasks),
	}, manifestBytes)
	sinkCancel()
	if storeErr != nil {
		msg := fmt.Sprintf("step store manifest failed: %v", storeErr)
		if isStepTimeout(sinkCtx, storeErr) {
			logger.Error(storeErr, "Step store manifest timed out", "run", run.Name)
		} else {
			logger.Error(storeErr, "Step store manifest failed", "run", run.Name)
		}
		return r.failRun(ctx, &run, &sandbox, msg)
	}

	// Compute checksum, counts, and capped task IDs (up to 50)
	h := sha256.Sum256(manifestBytes)
	manifestHash := "sha256:" + hex.EncodeToString(h[:])

	taskCount := len(manifest.Tasks)
	eligibleCount := 0
	for _, t := range manifest.Tasks {
		if len(t.Resources) == 0 {
			eligibleCount++
		}
	}

	const maxCappedIDs = 50
	cappedCount := len(manifest.Tasks)
	if cappedCount > maxCappedIDs {
		cappedCount = maxCappedIDs
	}
	taskIDs := make([]string, 0, cappedCount)
	for i := 0; i < cappedCount; i++ {
		taskIDs = append(taskIDs, manifest.Tasks[i].ID)
	}

	discoveryStatus := &v1alpha1.RunDiscoveryStatus{
		ManifestPath:      manifestURI,
		ManifestHash:      manifestHash,
		TaskCount:         taskCount,
		EligibleTaskCount: eligibleCount,
		TaskIDs:           taskIDs,
	}

	// Populate in-memory cache
	r.setCachedManifest(run.Namespace+"/"+run.Name, manifest)

	// Step 5: Success -> update Run to Discovered (or Running if spec.model is set), then cleanup sandbox
	totalCloneAttempts := cloneAttempts
	if tracker := r.getDiscoveryTracker(req.NamespacedName); tracker.countedAttempts > 0 {
		totalCloneAttempts += tracker.countedAttempts
	}
	r.clearDiscoveryTracker(req.NamespacedName)

	successMsg := ""
	condMsg := fmt.Sprintf("Discovered %d tasks", len(manifest.Tasks))
	if totalCloneAttempts > 1 {
		successMsg = fmt.Sprintf("Discovered after %s", formatAttemptCount(totalCloneAttempts))
		condMsg = fmt.Sprintf("%s after %s", condMsg, formatAttemptCount(totalCloneAttempts))
	}

	logger.Info("Discovery successful; recording discovery status and cleaning up sandbox",
		"tasks", len(manifest.Tasks),
		"cloneAttempts", totalCloneAttempts,
		"manifestURI", manifestURI,
	)

	if run.Spec.Model == "" {
		err = r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
			status.Phase = v1alpha1.RunPhaseDiscovered
			status.Message = successMsg
			status.Discovery = discoveryStatus
			if prefix := r.artifactSink().ArtifactPrefix(run.Spec.Repo.Commit); prefix != "" {
				status.Artifacts = prefix
			}
			meta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               v1alpha1.ConditionTypeDiscovered,
				Status:             metav1.ConditionTrue,
				Reason:             "Discovered",
				Message:            condMsg,
				ObservedGeneration: gen,
			})
		})
		if err != nil {
			return ctrl.Result{}, err
		}

		if err := r.Delete(ctx, &sandbox); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to delete discovery sandbox", "sandbox", sandbox.Name)
		}
		r.cleanupAuthToken(ctx, run.Namespace, run.Name, sandboxKey)

		return ctrl.Result{}, nil
	}

	initialTasks := r.initializeTasks(run.Spec, manifest)
	dl := metav1.NewTime(r.now().Add(r.executionDeadline()))
	err = r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
		status.Phase = v1alpha1.RunPhaseRunning
		status.Message = successMsg
		status.Discovery = discoveryStatus
		status.Tasks = initialTasks
		status.ExecutionDeadline = &dl
		if prefix := r.artifactSink().ArtifactPrefix(run.Spec.Repo.Commit); prefix != "" {
			status.Artifacts = prefix
		}
		status.Summary = ComputeRunSummary(status.Tasks)
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionTypeDiscovered,
			Status:             metav1.ConditionTrue,
			Reason:             "Discovered",
			Message:            condMsg,
			ObservedGeneration: gen,
		})
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.Delete(ctx, &sandbox); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to delete discovery sandbox", "sandbox", sandbox.Name)
	}
	r.cleanupAuthToken(ctx, run.Namespace, run.Name, sandboxKey)

	return ctrl.Result{Requeue: true}, nil
}

func (r *RunReconciler) sandboxNamespace() string {
	if r.SandboxNamespace != "" {
		return r.SandboxNamespace
	}
	return discovery.DefaultSandboxNamespace
}

func (r *RunReconciler) recreateDiscoverySandbox(ctx context.Context, run *v1alpha1.Run) error {
	logger := log.FromContext(ctx)
	sandboxNamespace := r.sandboxNamespace()
	sandboxName := discovery.SandboxNameForRun(run.Name)
	sandboxKey := types.NamespacedName{Namespace: sandboxNamespace, Name: sandboxName}

	authToken, tokenErr := r.ensureAuthToken(ctx, run, sandboxKey)
	if tokenErr != nil {
		return fmt.Errorf("failed to ensure auth token: %w", tokenErr)
	}
	tokenDigest := discovery.AuthTokenDigest(authToken)

	// Create server cert Secret before Sandbox so it exists when pod starts
	if _, err := r.pki().EnsureServerCertSecret(ctx, sandboxName, r.discoveryDeadline()+time.Hour); err != nil {
		return fmt.Errorf("failed to ensure server cert secret: %w", err)
	}

	logger.Info("Creating discovery sandbox", "sandbox", sandboxName, "namespace", sandboxNamespace)
	desiredSandbox := discovery.BuildSandbox(run, sandboxNamespace, tokenDigest, discovery.SandboxOptions{
		ServerCertSecret: pki.ServerSecretName(sandboxName),
		CAConfigMap:      pki.DefaultCAConfigMapName,
		SandboxdImage:    r.sandboxdImage(),
	})
	if err := r.Create(ctx, desiredSandbox); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create sandbox: %w", err)
		}
	}
	return nil
}

func (r *RunReconciler) findDiscoverySandbox(ctx context.Context, run *v1alpha1.Run) (*sandboxv1beta1.Sandbox, error) {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}

	runNamespace := run.Namespace
	if runNamespace == "" {
		runNamespace = discovery.DefaultControllerNamespace
	}

	matchingLabels := client.MatchingLabels{
		discovery.LabelRunName:      run.Name,
		discovery.LabelRunNamespace: runNamespace,
		discovery.LabelRole:         discovery.RoleDiscovery,
	}
	var sbList sandboxv1beta1.SandboxList
	if err := reader.List(ctx, &sbList, client.InNamespace(r.sandboxNamespace()), matchingLabels); err != nil {
		return nil, err
	}
	for i := range sbList.Items {
		sb := &sbList.Items[i]
		if sb.DeletionTimestamp.IsZero() {
			return sb, nil
		}
	}

	// Fallback to direct name lookup if labels were not matched
	sbName := discovery.SandboxNameForRun(run.Name)
	var directSB sandboxv1beta1.Sandbox
	if err := reader.Get(ctx, types.NamespacedName{Namespace: r.sandboxNamespace(), Name: sbName}, &directSB); err == nil {
		if directSB.DeletionTimestamp.IsZero() {
			return &directSB, nil
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}

	return nil, nil
}

func (r *RunReconciler) resolveSandboxEndpoint(ctx context.Context, sb *sandboxv1beta1.Sandbox) string {
	return r.resolveEndpointWithReader(ctx, r.Client, sb)
}

func (r *RunReconciler) resolveFreshSandboxEndpoint(ctx context.Context, sb *sandboxv1beta1.Sandbox) string {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	return r.resolveEndpointWithReader(ctx, reader, sb)
}

func (r *RunReconciler) resolveEndpointWithReader(ctx context.Context, reader client.Reader, sb *sandboxv1beta1.Sandbox) string {
	podName := sb.Name
	if name, ok := sb.Annotations[sandboxv1beta1.SandboxPodNameAnnotation]; ok && name != "" {
		podName = name
	}
	var pod corev1.Pod
	podKey := types.NamespacedName{Namespace: sb.Namespace, Name: podName}
	err := reader.Get(ctx, podKey, &pod)
	if err == nil {
		return pod.Status.PodIP
	}
	if !apierrors.IsNotFound(err) {
		log.FromContext(ctx).V(1).Info("failed to get pod for endpoint resolution", "pod", podKey, "error", err)
	}
	if len(sb.Status.PodIPs) > 0 {
		return sb.Status.PodIPs[0]
	}
	return ""
}

func (r *RunReconciler) createSandboxClient(ctx context.Context, run *v1alpha1.Run, sbName, sbNamespace, endpoint string) (sandboxclient.Client, error) {
	if r.SandboxClient != nil {
		return r.SandboxClient, nil
	}

	factory := r.ClientFactory
	if factory == nil {
		factory = sandboxclient.DefaultFactory()
	}
	authToken, tokenErr := r.ensureAuthToken(ctx, run, types.NamespacedName{Namespace: sbNamespace, Name: sbName})
	if tokenErr != nil {
		return nil, fmt.Errorf("failed to retrieve auth token for sandbox: %w", tokenErr)
	}
	var opts []sandboxclient.Option
	if authToken != "" {
		opts = append(opts, sandboxclient.WithAuthToken(authToken))
	}
	if tlsConfig := r.pki().ClientTLSConfig(sbName, sbNamespace); tlsConfig != nil {
		opts = append(opts, sandboxclient.WithTLSConfig(tlsConfig))
	}
	return factory.NewClient(endpoint, opts...)
}

func (r *RunReconciler) deleteAllSandboxesForRun(ctx context.Context, runNamespace, runName string) error {
	logger := log.FromContext(ctx)
	if runNamespace == "" {
		runNamespace = discovery.DefaultControllerNamespace
	}

	discoverySBKey := types.NamespacedName{
		Namespace: r.sandboxNamespace(),
		Name:      discovery.SandboxNameForRun(runName),
	}
	r.cleanupAuthToken(ctx, runNamespace, runName, discoverySBKey)

	var sandboxList sandboxv1beta1.SandboxList
	if err := r.List(ctx, &sandboxList, client.InNamespace(r.sandboxNamespace()), client.MatchingLabels{
		discovery.LabelRunName:      runName,
		discovery.LabelRunNamespace: runNamespace,
	}); err != nil {
		return err
	}
	for i := range sandboxList.Items {
		sb := &sandboxList.Items[i]
		sbKey := types.NamespacedName{Namespace: sb.Namespace, Name: sb.Name}
		taskID := sb.Annotations[discovery.AnnotationTaskID]
		if taskID != "" {
			r.cleanupTaskAuthToken(ctx, runNamespace, runName, taskID, sbKey)
		}
		if err := r.Delete(ctx, sb); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to delete sandbox for run", "sandbox", sb.Name, "run", runName)
		}
	}
	return nil
}

func (r *RunReconciler) initializeTasks(spec v1alpha1.RunSpec, manifest *v1alpha1.Manifest) []v1alpha1.TaskStatus {
	var statuses []v1alpha1.TaskStatus
	manifestMap := make(map[string]v1alpha1.ManifestTask)
	if manifest != nil {
		for _, t := range manifest.Tasks {
			manifestMap[t.ID] = t
		}
	}
	effRange := EffectiveSampleRange(spec)
	planned := computeSamplesPlanned(effRange, spec.Limit, 0)

	if len(spec.Tasks) > 0 {
		for _, taskID := range spec.Tasks {
			task, exists := manifestMap[taskID]
			cohort := task.Cohort
			if cohort == "" {
				cohort = v1alpha1.DefaultCohort
			}
			if !exists {
				statuses = append(statuses, v1alpha1.TaskStatus{
					ID:             taskID,
					Cohort:         cohort,
					Phase:          v1alpha1.TaskPhaseFailed,
					Message:        "task not found in manifest",
					SampleRange:    effRange,
					SamplesPlanned: planned,
				})
				continue
			}

			if len(task.Resources) > 0 {
				statuses = append(statuses, v1alpha1.TaskStatus{
					ID:             taskID,
					Cohort:         cohort,
					Phase:          v1alpha1.TaskPhaseFailed,
					Message:        "resources not supported yet",
					SampleRange:    effRange,
					SamplesPlanned: planned,
				})
				continue
			}

			statuses = append(statuses, v1alpha1.TaskStatus{
				ID:             taskID,
				Cohort:         cohort,
				Phase:          v1alpha1.TaskPhasePending,
				SampleRange:    effRange,
				SamplesPlanned: planned,
			})
		}
	} else if manifest != nil {
		for _, task := range manifest.Tasks {
			cohort := task.Cohort
			if cohort == "" {
				cohort = v1alpha1.DefaultCohort
			}
			if len(task.Resources) > 0 {
				statuses = append(statuses, v1alpha1.TaskStatus{
					ID:             task.ID,
					Cohort:         cohort,
					Phase:          v1alpha1.TaskPhaseFailed,
					Message:        "resources not supported yet",
					SampleRange:    effRange,
					SamplesPlanned: planned,
				})
				continue
			}

			statuses = append(statuses, v1alpha1.TaskStatus{
				ID:             task.ID,
				Cohort:         cohort,
				Phase:          v1alpha1.TaskPhasePending,
				SampleRange:    effRange,
				SamplesPlanned: planned,
			})
		}
	}
	return statuses
}

func (r *RunReconciler) reconcileRunning(ctx context.Context, req reconcile.Request, run *v1alpha1.Run) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Check execution deadline
	if run.Status.ExecutionDeadline != nil && !r.now().Before(run.Status.ExecutionDeadline.Time) {
		r.evictCachedManifest(run.Namespace + "/" + run.Name)
		msg := "Execution deadline exceeded"
		logger.Info(msg, "run", run.Name)
		if err := r.deleteAllSandboxesForRun(ctx, run.Namespace, run.Name); err != nil {
			logger.Error(err, "failed to delete sandboxes on execution deadline", "run", run.Name)
		}
		err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
			status.Phase = v1alpha1.RunPhaseFailed
			status.Message = msg
			meta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               "Completed",
				Status:             metav1.ConditionFalse,
				Reason:             "ExecutionDeadlineExceeded",
				Message:            msg,
				ObservedGeneration: gen,
			})
		})
		return ctrl.Result{}, err
	}

	// 2. Check if all tasks have reached a terminal phase
	allFinished := len(run.Status.Tasks) > 0
	completedCount := 0
	for _, t := range run.Status.Tasks {
		if t.Phase == v1alpha1.TaskPhaseCompleted {
			completedCount++
		} else if t.Phase == v1alpha1.TaskPhaseFailed {
			// failed
		} else {
			allFinished = false
			break
		}
	}
	if allFinished {
		r.evictCachedManifest(run.Namespace + "/" + run.Name)
		if err := r.deleteAllSandboxesForRun(ctx, run.Namespace, run.Name); err != nil {
			logger.Error(err, "failed to delete sandboxes on run completion", "run", run.Name)
		}
		if completedCount > 0 {
			logger.Info("Tasks finished with at least one completed; marking Run as Completed", "run", run.Name, "completed", completedCount)
			err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
				status.Phase = v1alpha1.RunPhaseCompleted
				if status.Artifacts == "" {
					if prefix := r.artifactSink().ArtifactPrefix(run.Spec.Repo.Commit); prefix != "" {
						status.Artifacts = prefix
					}
				}
				status.Summary = ComputeRunSummary(status.Tasks)
			})
			return ctrl.Result{}, err
		}
		msg := fmt.Sprintf("all %d tasks failed", len(run.Status.Tasks))
		logger.Info("All tasks failed; marking Run as Failed", "run", run.Name, "message", msg)
		err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
			status.Phase = v1alpha1.RunPhaseFailed
			status.Message = msg
			status.Summary = ComputeRunSummary(status.Tasks)
		})
		return ctrl.Result{}, err
	}

	// Load manifest via in-memory cache or sink URI
	manifest, manifestErr := r.getOrLoadManifest(ctx, run)
	if manifestErr != nil {
		if errors.Is(manifestErr, ErrTransient) {
			logger.Info("Transient error loading manifest; requeuing and keeping task sandboxes", "run", run.Name, "error", manifestErr)
			if updateErr := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
				status.Message = fmt.Sprintf("reloading manifest: %v", manifestErr)
			}); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		if errors.Is(manifestErr, ErrNeedsRediscovery) {
			logger.Info("Manifest missing or non-durable sink; resetting to Pending to re-discover", "run", run.Name, "error", manifestErr)
			r.evictCachedManifest(run.Namespace + "/" + run.Name)
			if err := r.deleteAllSandboxesForRun(ctx, run.Namespace, run.Name); err != nil {
				logger.Error(err, "failed to delete sandboxes on rediscovery reset", "run", run.Name)
			}
			// Reset deadline and phase to Pending
			updateErr := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
				status.Phase = v1alpha1.RunPhasePending
				status.DiscoveryDeadline = nil
				status.Discovery = nil
				status.Tasks = nil
				status.Summary = nil
				status.Message = "Manifest missing; re-discovering"
				meta.SetStatusCondition(&status.Conditions, metav1.Condition{
					Type:               v1alpha1.ConditionTypeDiscovered,
					Status:             metav1.ConditionFalse,
					Reason:             "Rediscovering",
					Message:            "Manifest missing; re-discovering",
					ObservedGeneration: gen,
				})
			})
			if updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{Requeue: true}, nil
		}
		if errors.Is(manifestErr, ErrHashDrift) {
			logger.Error(manifestErr, "Manifest hash drift detected; keeping phase", "run", run.Name)
			if updateErr := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
				status.Message = manifestErr.Error()
			}); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, nil
		}
		return r.failRun(ctx, run, nil, fmt.Sprintf("failed to load manifest: %v", manifestErr))
	}

	if len(run.Status.Tasks) == 0 && manifest != nil {
		initialTasks := r.initializeTasks(run.Spec, manifest)
		err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
			status.Tasks = initialTasks
			status.Summary = ComputeRunSummary(status.Tasks)
		})
		if err != nil {
			return ctrl.Result{}, err
		}
		run.Status.Tasks = initialTasks
		run.Status.Summary = ComputeRunSummary(initialTasks)
	}

	// List current task sandboxes in sandbox namespace
	var sandboxList sandboxv1beta1.SandboxList
	if err := r.List(ctx, &sandboxList, client.InNamespace(r.sandboxNamespace()), client.MatchingLabels{
		discovery.LabelRunName:      run.Name,
		discovery.LabelRunNamespace: run.Namespace,
		discovery.LabelRole:         discovery.RoleSample,
	}); err != nil {
		return ctrl.Result{}, err
	}

	sandboxesByTask := make(map[string]sandboxv1beta1.Sandbox)
	for _, sb := range sandboxList.Items {
		taskID := sb.Annotations[discovery.AnnotationTaskID]
		if taskID != "" {
			sandboxesByTask[taskID] = sb
		}
	}

	// Clean up lingering sandboxes for tasks already in a terminal phase (e.g. from a crash before sandbox deletion)
	for _, t := range run.Status.Tasks {
		if t.Phase == v1alpha1.TaskPhaseCompleted || t.Phase == v1alpha1.TaskPhaseFailed {
			if sb, ok := sandboxesByTask[t.ID]; ok {
				sbKey := types.NamespacedName{Namespace: sb.Namespace, Name: sb.Name}
				if err := r.Delete(ctx, &sb); err != nil && !apierrors.IsNotFound(err) {
					logger.Error(err, "failed to delete lingering task sandbox", "task", t.ID)
				}
				r.cleanupTaskAuthToken(ctx, run.Namespace, run.Name, t.ID, sbKey)
				delete(sandboxesByTask, t.ID)
			}
		}
	}

	// Count active running tasks
	runningCount := 0
	for _, t := range run.Status.Tasks {
		if t.Phase == v1alpha1.TaskPhaseRunning {
			runningCount++
		}
	}

	// Start pending tasks up to MaxParallelTasks
	availableSlots := r.maxParallelTasks() - runningCount
	if availableSlots > 0 {
		var tasksToStart []string
		for _, t := range run.Status.Tasks {
			if t.Phase == v1alpha1.TaskPhasePending && availableSlots > 0 {
				tasksToStart = append(tasksToStart, t.ID)
				availableSlots--
			}
		}

		if len(tasksToStart) > 0 {
			reader := client.Reader(r.Client)
			if r.APIReader != nil {
				reader = r.APIReader
			}
			effModelSecret := r.modelSecret()
			secretExists, err := CheckModelSecretExists(ctx, reader, r.sandboxNamespace(), effModelSecret)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to check model secret %s/%s: %w", r.sandboxNamespace(), effModelSecret, err)
			}

			var started []string
			failedTasks := make(map[string]string)
			for _, taskID := range tasksToStart {
				effectiveModelSecret := ""
				if secretExists {
					effectiveModelSecret = effModelSecret
				} else if !IsMockModel(run.Spec.Model) {
					msg := ModelCredentialsMissingMessage(r.sandboxNamespace(), effModelSecret)
					logger.Info("Rejecting task launch due to missing model credentials", "task", taskID, "model", run.Spec.Model, "message", msg)
					failedTasks[taskID] = msg
					continue
				}

				sbName := discovery.TaskSandboxNameForRun(run.Name, taskID)
				sbKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: sbName}
				token, tokenErr := r.ensureTaskAuthToken(ctx, run, taskID, sbKey)
				if tokenErr != nil {
					logger.Error(tokenErr, "Failed to ensure task auth token", "task", taskID)
					failedTasks[taskID] = fmt.Sprintf("failed to ensure task auth token: %v", tokenErr)
					continue
				}

				// Create server cert Secret before Sandbox so it exists when pod starts
				if _, err := r.pki().EnsureServerCertSecret(ctx, sbName, r.executionDeadline()+time.Hour); err != nil {
					logger.Error(err, "Failed to ensure task server cert secret", "task", taskID)
					failedTasks[taskID] = fmt.Sprintf("failed to ensure server cert secret: %v", err)
					continue
				}

				desiredSandbox := discovery.BuildTaskSandbox(run, taskID, discovery.TaskSandboxConfig{
					Namespace:         r.sandboxNamespace(),
					SampleRunnerImage: r.sampleRunnerImage(),
					ModelSecret:       effectiveModelSecret,
					AuthTokenDigest:   discovery.AuthTokenDigest(token),
					ServerCertSecret:  pki.ServerSecretName(sbName),
					CAConfigMap:       pki.DefaultCAConfigMapName,
					SandboxdImage:     r.sandboxdImage(),
				})
				if err := r.Create(ctx, desiredSandbox); err != nil && !apierrors.IsAlreadyExists(err) {
					logger.Error(err, "Failed to create task sandbox", "task", taskID)
					failedTasks[taskID] = fmt.Sprintf("failed to create sandbox: %v", err)
				} else {
					started = append(started, taskID)
					sandboxesByTask[taskID] = *desiredSandbox
				}
			}

			if len(started) > 0 || len(failedTasks) > 0 {
				now := metav1.NewTime(r.now())
				err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
					for i := range status.Tasks {
						tid := status.Tasks[i].ID
						if failMsg, failed := failedTasks[tid]; failed {
							status.Tasks[i].Phase = v1alpha1.TaskPhaseFailed
							status.Tasks[i].Message = failMsg
						} else {
							for _, s := range started {
								if s == tid {
									status.Tasks[i].Phase = v1alpha1.TaskPhaseRunning
									status.Tasks[i].StartedAt = &now
									break
								}
							}
						}
					}
					status.Summary = ComputeRunSummary(status.Tasks)
				})
				if err != nil {
					return ctrl.Result{}, err
				}
				for i := range run.Status.Tasks {
					tid := run.Status.Tasks[i].ID
					if failMsg, failed := failedTasks[tid]; failed {
						run.Status.Tasks[i].Phase = v1alpha1.TaskPhaseFailed
						run.Status.Tasks[i].Message = failMsg
					} else {
						for _, s := range started {
							if s == tid {
								run.Status.Tasks[i].Phase = v1alpha1.TaskPhaseRunning
								run.Status.Tasks[i].StartedAt = &now
								break
							}
						}
					}
				}
				if len(started) == 0 && len(failedTasks) > 0 {
					return ctrl.Result{Requeue: true}, nil
				}
			}
		}
	}

	// Check running tasks for ready pods
	type readyTaskItem struct {
		task    v1alpha1.TaskStatus
		sandbox sandboxv1beta1.Sandbox
		pod     corev1.Pod
	}
	var readyTasks []readyTaskItem
	terminalFailures := make(map[string]string)
	type rescuedTaskInfo struct {
		agg     *artifacts.TaskAggregates
		summary *artifacts.TaskSummary
		logPath string
	}
	rescuedTasks := make(map[string]rescuedTaskInfo)
	waitingInGracePeriod := false
	var minGraceRemaining time.Duration

	for _, t := range run.Status.Tasks {
		if t.Phase != v1alpha1.TaskPhaseRunning {
			continue
		}

		sb, hasSB := sandboxesByTask[t.ID]
		if !hasSB {
			// Prefer reading back the created object with the API reader (uncached)
			sbName := discovery.TaskSandboxNameForRun(run.Name, t.ID)
			sbKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: sbName}
			reader := client.Reader(r.Client)
			if r.APIReader != nil {
				reader = r.APIReader
			}
			var directSB sandboxv1beta1.Sandbox
			if err := reader.Get(ctx, sbKey, &directSB); err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
			} else {
				sb = directSB
				hasSB = true
				sandboxesByTask[t.ID] = directSB
			}
		}

		if !hasSB {
			var startedAt time.Time
			if t.StartedAt != nil {
				startedAt = t.StartedAt.Time
			} else {
				secretName := discovery.TaskAuthSecretNameForRun(run.Name, t.ID)
				secretNamespace := run.Namespace
				if secretNamespace == "" {
					secretNamespace = discovery.DefaultControllerNamespace
				}
				reader := client.Reader(r.Client)
				if r.APIReader != nil {
					reader = r.APIReader
				}
				var secret corev1.Secret
				if err := reader.Get(ctx, types.NamespacedName{Namespace: secretNamespace, Name: secretName}, &secret); err != nil {
					if !apierrors.IsNotFound(err) {
						return ctrl.Result{}, err
					}
				} else if !secret.CreationTimestamp.IsZero() {
					startedAt = secret.CreationTimestamp.Time
				}
			}

			if !startedAt.IsZero() {
				elapsed := r.now().Sub(startedAt)
				if elapsed < r.taskGracePeriod() {
					logger.V(1).Info("Task sandbox not yet visible in cache; waiting within grace period", "task", t.ID)
					waitingInGracePeriod = true
					remaining := r.taskGracePeriod() - elapsed
					if minGraceRemaining == 0 || remaining < minGraceRemaining {
						minGraceRemaining = remaining
					}
					continue
				}
			}

			reqWindow := taskRequestedWindowSize(run.Spec, t)
			rescued := false
			var rescuedAgg *artifacts.TaskAggregates
			var rescuedSummary *artifacts.TaskSummary
			var rescuedLogPath string

			if t.Samples > 0 && (reqWindow <= 0 || t.Samples >= reqWindow) {
				rescued = true
			} else if t.LogPath != "" {
				loadCtx, loadCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.artifactSinkTimeout())
				data, loadErr := artifacts.LoadWithTimeout(loadCtx, r.artifactSink(), t.LogPath)
				loadCancel()
				if loadErr == nil && len(data) > 0 {
					if agg, parseErr := artifacts.ParseInspectLog(data); parseErr == nil && agg.Samples > 0 {
						if reqWindow <= 0 || agg.Samples >= reqWindow || (t.PossibleSamples > 0 && agg.Samples >= t.PossibleSamples) {
							rescued = true
							rescuedAgg = agg
							rescuedLogPath = t.LogPath
						}
					}
				}
			} else if run.Spec.Repo.Commit != "" {
				summaryPath := artifacts.ArtifactTarget(r.artifactSink(), run.Spec.Repo.Commit, t.ID, "summary.json")
				loadCtx, loadCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.artifactSinkTimeout())
				data, loadErr := artifacts.LoadWithTimeout(loadCtx, r.artifactSink(), summaryPath)
				loadCancel()
				if loadErr != nil {
					if !artifacts.IsNotFound(loadErr) {
						logger.V(1).Info("probed artifact sink for restart rescue", "task", t.ID, "path", summaryPath, "outcome", "error", "error", loadErr)
						return ctrl.Result{}, fmt.Errorf("probe artifact sink for task %s summary: %w", t.ID, loadErr)
					}
					logger.V(1).Info("probed artifact sink for restart rescue", "task", t.ID, "path", summaryPath, "outcome", "not_found")
				} else if len(data) > 0 {
					var sum artifacts.TaskSummary
					if parseErr := json.Unmarshal(data, &sum); parseErr != nil {
						logger.V(1).Info("probed artifact sink for restart rescue", "task", t.ID, "path", summaryPath, "outcome", "unparseable", "error", parseErr)
					} else if reqWindow <= 0 || sum.SampleCount >= reqWindow || (sum.PossibleSamples > 0 && sum.SampleCount >= sum.PossibleSamples) {
						if sum.SampleCount > 0 {
							logger.V(1).Info("probed artifact sink for restart rescue", "task", t.ID, "path", summaryPath, "outcome", "complete", "samples", sum.SampleCount)
							rescued = true
							rescuedSummary = &sum
							if sum.LogFilename != "" {
								rescuedLogPath = artifacts.ArtifactTarget(r.artifactSink(), run.Spec.Repo.Commit, t.ID, sum.LogFilename)
							}
						} else {
							logger.V(1).Info("probed artifact sink for restart rescue", "task", t.ID, "path", summaryPath, "outcome", "empty", "samples", 0)
						}
					} else {
						logger.V(1).Info("probed artifact sink for restart rescue", "task", t.ID, "path", summaryPath, "outcome", "partial", "samples", sum.SampleCount, "requested", reqWindow)
					}
				}
			}

			if rescued {
				sampleCount := t.Samples
				if sampleCount == 0 {
					if rescuedSummary != nil {
						sampleCount = rescuedSummary.SampleCount
					} else if rescuedAgg != nil {
						sampleCount = rescuedAgg.Samples
					}
				}
				logger.Info("restart rescue", "task", t.ID, "run", run.Name, "samples", sampleCount)
				rescuedTasks[t.ID] = rescuedTaskInfo{agg: rescuedAgg, summary: rescuedSummary, logPath: rescuedLogPath}
				continue
			}

			terminalFailures[t.ID] = "task sandbox missing while in Running phase"
			continue
		}

		podName := sb.Name
		if name, ok := sb.Annotations[sandboxv1beta1.SandboxPodNameAnnotation]; ok && name != "" {
			podName = name
		}
		podKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: podName}
		var pod corev1.Pod
		if err := r.Get(ctx, podKey, &pod); err != nil {
			if apierrors.IsNotFound(err) {
				logger.V(1).Info("Task pod not yet created", "task", t.ID)
			}
			continue
		}

		if pod.Status.Phase == corev1.PodFailed {
			terminalFailures[t.ID] = "task pod failed"
			continue
		}
		if pod.Status.Phase == corev1.PodSucceeded {
			terminalFailures[t.ID] = "task pod succeeded unexpectedly before execution"
			continue
		}

		finishedCond := meta.FindStatusCondition(sb.Status.Conditions, string(sandboxv1beta1.SandboxConditionFinished))
		if finishedCond != nil && finishedCond.Status == metav1.ConditionTrue {
			terminalFailures[t.ID] = fmt.Sprintf("task sandbox finished: %s", finishedCond.Message)
			continue
		}

		sandboxReady := meta.IsStatusConditionTrue(sb.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady))
		if (sandboxReady || isPodReady(&pod)) && pod.Status.PodIP != "" {
			readyTasks = append(readyTasks, readyTaskItem{
				task:    t,
				sandbox: sb,
				pod:     pod,
			})
		}
	}

	if len(terminalFailures) > 0 || len(rescuedTasks) > 0 {
		err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
			for i := range status.Tasks {
				tid := status.Tasks[i].ID
				if msg, failed := terminalFailures[tid]; failed {
					status.Tasks[i].Phase = v1alpha1.TaskPhaseFailed
					status.Tasks[i].Message = msg
				} else if info, ok := rescuedTasks[tid]; ok {
					status.Tasks[i].Phase = v1alpha1.TaskPhaseCompleted
					if status.Tasks[i].Message == "" {
						status.Tasks[i].Message = "restart rescue"
					} else if !strings.Contains(status.Tasks[i].Message, "restart rescue") {
						status.Tasks[i].Message = status.Tasks[i].Message + " (restart rescue)"
					}
					if status.Tasks[i].LogPath == "" && info.logPath != "" {
						status.Tasks[i].LogPath = info.logPath
					}
					if info.summary != nil {
						status.Tasks[i].Samples = info.summary.SampleCount
						status.Tasks[i].Successes = info.summary.Successes
						status.Tasks[i].MeanScore = info.summary.MeanScore
						status.Tasks[i].SuccessRate = info.summary.SuccessRate
						status.Tasks[i].SuccessRateCI = info.summary.SuccessRateCI
						status.Tasks[i].SuccessRateSD = info.summary.SuccessRateSD
						status.Tasks[i].PossibleSamples = info.summary.PossibleSamples
						if info.summary.SampleRange != nil {
							status.Tasks[i].SampleRange = &v1alpha1.SampleRange{
								Start: info.summary.SampleRange.Start,
								End:   info.summary.SampleRange.End,
							}
						}
					} else if info.agg != nil {
						if status.Tasks[i].Samples == 0 {
							status.Tasks[i].Samples = info.agg.Samples
							status.Tasks[i].Successes = info.agg.Successes
							status.Tasks[i].MeanScore = info.agg.MeanScore
						}
						if status.Tasks[i].SuccessRate == nil && info.agg.Samples > 0 {
							rate, ci, sd := computeTaskAggregateStats(info.agg)
							status.Tasks[i].SuccessRate = rate
							status.Tasks[i].SuccessRateCI = ci
							status.Tasks[i].SuccessRateSD = sd
						}
					}
					if status.Tasks[i].MeanScore == nil && status.Tasks[i].Samples > 0 {
						mean := float64(status.Tasks[i].Successes) / float64(status.Tasks[i].Samples)
						status.Tasks[i].MeanScore = &mean
					}
					if status.Tasks[i].SuccessRate == nil && status.Tasks[i].Samples > 0 {
						rate := float64(status.Tasks[i].Successes) / float64(status.Tasks[i].Samples)
						status.Tasks[i].SuccessRate = &rate
					}
					if status.Tasks[i].SuccessRateCI == nil && status.Tasks[i].Samples > 0 {
						est := stats.Proportion(status.Tasks[i].Successes, status.Tasks[i].Samples, 0.95)
						status.Tasks[i].SuccessRateCI = &v1alpha1.ConfidenceInterval{
							Lower:      est.CI.Lower,
							Upper:      est.CI.Upper,
							Confidence: 0.95,
							Method:     "wilson",
						}
					}
					if status.Tasks[i].SamplesDone < status.Tasks[i].Samples {
						status.Tasks[i].SamplesDone = status.Tasks[i].Samples
					}
				}
			}
			status.Summary = ComputeRunSummary(status.Tasks)
		})
		if err != nil {
			return ctrl.Result{}, err
		}

		for i := range run.Status.Tasks {
			tid := run.Status.Tasks[i].ID
			if msg, failed := terminalFailures[tid]; failed {
				run.Status.Tasks[i].Phase = v1alpha1.TaskPhaseFailed
				run.Status.Tasks[i].Message = msg
			} else if info, ok := rescuedTasks[tid]; ok {
				run.Status.Tasks[i].Phase = v1alpha1.TaskPhaseCompleted
				if run.Status.Tasks[i].Message == "" {
					run.Status.Tasks[i].Message = "restart rescue"
				}
				if run.Status.Tasks[i].LogPath == "" && info.logPath != "" {
					run.Status.Tasks[i].LogPath = info.logPath
				}
			}
		}

		for tid := range terminalFailures {
			sbName := discovery.TaskSandboxNameForRun(run.Name, tid)
			sbKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: sbName}
			if sb, ok := sandboxesByTask[tid]; ok {
				if err := r.Delete(ctx, &sb); err != nil && !apierrors.IsNotFound(err) {
					logger.Error(err, "failed to delete task sandbox on terminal failure", "task", tid)
				}
			}
			r.cleanupTaskAuthToken(ctx, run.Namespace, run.Name, tid, sbKey)
		}
		for tid := range rescuedTasks {
			sbName := discovery.TaskSandboxNameForRun(run.Name, tid)
			sbKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: sbName}
			if sb, ok := sandboxesByTask[tid]; ok {
				if err := r.Delete(ctx, &sb); err != nil && !apierrors.IsNotFound(err) {
					logger.Error(err, "failed to delete task sandbox on restart rescue", "task", tid)
				}
			}
			r.cleanupTaskAuthToken(ctx, run.Namespace, run.Name, tid, sbKey)
		}
	}

	if len(readyTasks) == 0 {
		if len(terminalFailures) > 0 || len(rescuedTasks) > 0 {
			return ctrl.Result{Requeue: true}, nil
		}
		requeueAfter := 1 * time.Second
		if waitingInGracePeriod && minGraceRemaining > 0 && minGraceRemaining < requeueAfter {
			requeueAfter = minGraceRemaining
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	resultsChan := make(chan taskExecutionResult, len(readyTasks))
	var wg sync.WaitGroup

	for _, item := range readyTasks {
		wg.Add(1)
		go func(task v1alpha1.TaskStatus, sb sandboxv1beta1.Sandbox, pod corev1.Pod) {
			defer wg.Done()
			res := r.executeTask(ctx, run, task.ID, &sb, &pod)
			resultsChan <- res
		}(item.task, item.sandbox, item.pod)
	}

	wg.Wait()
	close(resultsChan)

	taskResults := make(map[string]taskExecutionResult)
	for res := range resultsChan {
		taskResults[res.taskID] = res
	}

	// Update task statuses FIRST before deleting sandboxes
	err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
		for i := range status.Tasks {
			if res, ok := taskResults[status.Tasks[i].ID]; ok {
				status.Tasks[i].Phase = res.phase
				if res.cohort != "" {
					status.Tasks[i].Cohort = res.cohort
				} else if status.Tasks[i].Cohort == "" {
					status.Tasks[i].Cohort = v1alpha1.DefaultCohort
				}
				status.Tasks[i].Samples = res.samples
				status.Tasks[i].Successes = res.successes
				status.Tasks[i].MeanScore = res.meanScore
				status.Tasks[i].SuccessRate = res.successRate
				status.Tasks[i].SuccessRateCI = res.successRateCI
				status.Tasks[i].SuccessRateSD = res.successRateSD
				status.Tasks[i].Message = res.message
				status.Tasks[i].LogPath = res.logPath
				status.Tasks[i].Tokens = res.tokens
				status.Tasks[i].Latency = res.latency
				status.Tasks[i].CostUSD = res.costUSD
				status.Tasks[i].CostSource = res.costSource
				status.Tasks[i].PossibleSamples = res.possibleSamples
				finalSamplesDone := res.samples
				if res.phase != v1alpha1.TaskPhaseCompleted && status.Tasks[i].SamplesDone > finalSamplesDone {
					finalSamplesDone = status.Tasks[i].SamplesDone
				}
				if finalSamplesDone > status.Tasks[i].SamplesDone {
					nowMeta := metav1.NewTime(r.now())
					status.Tasks[i].LastProgressAt = &nowMeta
				}
				status.Tasks[i].SamplesDone = finalSamplesDone

				if res.samplesPlanned > 0 {
					status.Tasks[i].SamplesPlanned = res.samplesPlanned
				} else if status.Tasks[i].SamplesPlanned == 0 {
					status.Tasks[i].SamplesPlanned = computeSamplesPlanned(res.sampleRange, run.Spec.Limit, res.possibleSamples)
				}
				if res.sampleRange != nil {
					status.Tasks[i].SampleRange = res.sampleRange
				} else if status.Tasks[i].SampleRange == nil {
					status.Tasks[i].SampleRange = EffectiveSampleRange(run.Spec)
				}
			}
		}
		status.Summary = ComputeRunSummary(status.Tasks)

		var totalCost float64
		hasCost := false
		for i := range status.Tasks {
			if status.Tasks[i].CostUSD != nil {
				totalCost += *status.Tasks[i].CostUSD
				hasCost = true
			}
		}
		if hasCost {
			status.CostUSD = &totalCost
		}
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	// Delete task sandboxes and clean up auth tokens only after status update succeeds
	for taskID := range taskResults {
		if sb, ok := sandboxesByTask[taskID]; ok {
			sbKey := types.NamespacedName{Namespace: sb.Namespace, Name: sb.Name}
			if err := r.Delete(ctx, &sb); err != nil && !apierrors.IsNotFound(err) {
				logger.Error(err, "failed to delete task sandbox after execution", "task", taskID)
			}
			r.cleanupTaskAuthToken(ctx, run.Namespace, run.Name, taskID, sbKey)
		}
	}

	return ctrl.Result{Requeue: true}, nil
}

type taskExecutionResult struct {
	taskID          string
	cohort          string
	phase           v1alpha1.TaskPhase
	samples         int
	successes       int
	meanScore       *float64
	successRate     *float64
	successRateCI   *v1alpha1.ConfidenceInterval
	successRateSD   *float64
	tokens          *v1alpha1.TaskTokens
	latency         *v1alpha1.TaskLatency
	costUSD         *float64
	costSource      v1alpha1.CostSource
	possibleSamples int
	samplesDone     int
	samplesPlanned  int
	message         string
	logPath         string
	sampleRange     *v1alpha1.SampleRange
}

func (r *RunReconciler) executeTask(ctx context.Context, run *v1alpha1.Run, taskID string, sb *sandboxv1beta1.Sandbox, pod *corev1.Pod) (res taskExecutionResult) {
	startTime := r.now()
	logger := log.FromContext(ctx).WithValues("task", taskID, "run", run.Name)
	effRange := EffectiveSampleRange(run.Spec)

	cohort := v1alpha1.DefaultCohort
	// Ensure every return (including early error returns) carries the task cohort to status.tasks[].cohort.
	defer func() {
		if res.cohort == "" {
			res.cohort = cohort
		}
	}()

	var targetTask *v1alpha1.ManifestTask
	manifest, manifestErr := r.getOrLoadManifest(ctx, run)
	if manifestErr != nil {
		logger.Error(manifestErr, "Failed to load manifest for task execution", "task", taskID)
	} else if manifest != nil {
		for i := range manifest.Tasks {
			if manifest.Tasks[i].ID == taskID {
				targetTask = &manifest.Tasks[i]
				if targetTask.Cohort != "" {
					cohort = targetTask.Cohort
				}
				break
			}
		}
	}
	if targetTask == nil {
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     "task definition not found in manifest",
			sampleRange: effRange,
		}
	}

	execCtx := ctx
	if run.Status.ExecutionDeadline != nil {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithDeadline(ctx, run.Status.ExecutionDeadline.Time)
		defer cancel()
	}

	sbKey := types.NamespacedName{Namespace: sb.Namespace, Name: sb.Name}
	authToken, tokenErr := r.ensureTaskAuthToken(execCtx, run, taskID, sbKey)
	if tokenErr != nil {
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     fmt.Sprintf("failed to get task auth token: %v", tokenErr),
			sampleRange: effRange,
		}
	}

	var sbClient sandboxclient.Client
	var err error
	if r.SandboxClient != nil {
		sbClient = r.SandboxClient
	} else {
		var opts []sandboxclient.Option
		if authToken != "" {
			opts = append(opts, sandboxclient.WithAuthToken(authToken))
		}
		if tlsConfig := r.pki().ClientTLSConfig(sb.Name, sb.Namespace); tlsConfig != nil {
			opts = append(opts, sandboxclient.WithTLSConfig(tlsConfig))
		}
		factory := r.ClientFactory
		if factory == nil {
			factory = sandboxclient.DefaultFactory()
		}
		sbClient, err = factory.NewClient(pod.Status.PodIP, opts...)
	}
	if err != nil {
		return taskExecutionResult{
			taskID:  taskID,
			phase:   v1alpha1.TaskPhaseFailed,
			message: fmt.Sprintf("failed to create sandbox client: %v", err),
		}
	}
	defer func() {
		if closeErr := sbClient.Close(); closeErr != nil {
			logger.Error(closeErr, "failed to close sandbox client")
		}
	}()

	// Ensure /workspace/out exists
	mkdirCtx, mkdirCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	if _, mkdirErr := executeSandboxWithTimeout(mkdirCtx, sbClient, sandboxclient.ProcessConfig{
		Command: []string{"mkdir", "-p", "/workspace/out"},
	}); mkdirErr != nil {
		logger.V(1).Info("mkdir /workspace/out returned error", "error", mkdirErr)
	}
	mkdirCancel()

	// Step 1: Clone repo
	logger.Info("Starting task step 1: git clone", "url", run.Spec.Repo.URL)
	cloneCtx, cloneCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	cloneRes, cloneErr := executeSandboxWithTimeout(cloneCtx, sbClient, sandboxclient.ProcessConfig{
		Command: []string{"git", "clone", "--", run.Spec.Repo.URL, "repo"},
	})
	cloneCancel()
	if cloneErr != nil || cloneRes == nil || cloneRes.ExitCode != 0 {
		if isStepTimeout(cloneCtx, cloneErr) {
			msg := "step git clone failed: sandbox did not report completion"
			logger.Error(errors.New(msg), "Task step git clone timed out", "task", taskID)
			return taskExecutionResult{
				taskID:      taskID,
				phase:       v1alpha1.TaskPhaseFailed,
				message:     msg,
				sampleRange: effRange,
			}
		}
		if status.Code(cloneErr) == codes.Unauthenticated {
			msg := fmt.Sprintf("step git clone failed: unauthenticated: %v", cloneErr)
			logger.Error(cloneErr, "Task step git clone failed", "task", taskID)
			return taskExecutionResult{
				taskID:  taskID,
				phase:   v1alpha1.TaskPhaseFailed,
				message: msg,
			}
		}
		var stderr string
		var exitCode int
		if cloneRes != nil {
			stderr = cloneRes.Stderr
			exitCode = cloneRes.ExitCode
		}
		tail := extractStderrTail(stderr, r.maxStderrLines(), discovery.MaxStderrTailBytes)
		var msg string
		if cloneRes != nil && cloneRes.ExitCode != 0 {
			if tail != "" {
				msg = fmt.Sprintf("step git clone failed with exit code %d: %s", cloneRes.ExitCode, tail)
			} else {
				msg = fmt.Sprintf("step git clone failed with exit code %d", cloneRes.ExitCode)
			}
		} else if tail != "" {
			if cloneErr != nil {
				msg = fmt.Sprintf("step git clone failed: %v: %s", cloneErr, tail)
			} else {
				msg = fmt.Sprintf("step git clone failed: %s", tail)
			}
		} else if cloneErr != nil {
			msg = fmt.Sprintf("step git clone failed: %v", cloneErr)
		} else {
			msg = "step git clone failed"
		}
		logger.Error(errors.New(msg), "Task step git clone failed", "task", taskID, "exitCode", exitCode, "stderr", tail)
		return taskExecutionResult{
			taskID:  taskID,
			phase:   v1alpha1.TaskPhaseFailed,
			message: msg,
		}
	}

	// Step 2: Checkout commit
	logger.Info("Starting task step 2: git checkout", "commit", run.Spec.Repo.Commit)
	checkoutCtx, checkoutCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	checkoutRes, checkoutErr := executeSandboxWithTimeout(checkoutCtx, sbClient, sandboxclient.ProcessConfig{
		Command: []string{"git", "checkout", run.Spec.Repo.Commit},
		Cwd:     "repo",
	})
	checkoutCancel()
	if checkoutErr != nil || checkoutRes == nil || checkoutRes.ExitCode != 0 {
		if isStepTimeout(checkoutCtx, checkoutErr) {
			msg := "step git checkout failed: sandbox did not report completion"
			logger.Error(errors.New(msg), "Task step git checkout timed out", "task", taskID)
			return taskExecutionResult{
				taskID:      taskID,
				phase:       v1alpha1.TaskPhaseFailed,
				message:     msg,
				sampleRange: effRange,
			}
		}
		if status.Code(checkoutErr) == codes.Unauthenticated {
			msg := fmt.Sprintf("step git checkout failed: unauthenticated: %v", checkoutErr)
			logger.Error(checkoutErr, "Task step git checkout failed", "task", taskID)
			return taskExecutionResult{
				taskID:  taskID,
				phase:   v1alpha1.TaskPhaseFailed,
				message: msg,
			}
		}
		var stderr string
		var exitCode int
		if checkoutRes != nil {
			stderr = checkoutRes.Stderr
			exitCode = checkoutRes.ExitCode
		}
		tail := extractStderrTail(stderr, r.maxStderrLines(), discovery.MaxStderrTailBytes)
		var msg string
		if checkoutRes != nil && checkoutRes.ExitCode != 0 {
			if tail != "" {
				msg = fmt.Sprintf("step git checkout failed with exit code %d: %s", checkoutRes.ExitCode, tail)
			} else {
				msg = fmt.Sprintf("step git checkout failed with exit code %d", checkoutRes.ExitCode)
			}
		} else if tail != "" {
			if checkoutErr != nil {
				msg = fmt.Sprintf("step git checkout failed: %v: %s", checkoutErr, tail)
			} else {
				msg = fmt.Sprintf("step git checkout failed: %s", tail)
			}
		} else if checkoutErr != nil {
			msg = fmt.Sprintf("step git checkout failed: %v", checkoutErr)
		} else {
			msg = "step git checkout failed"
		}
		logger.Error(errors.New(msg), "Task step git checkout failed", "task", taskID, "exitCode", exitCode, "stderr", tail)
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     msg,
			sampleRange: effRange,
		}
	}

	// Step 3: inspect eval
	cwd := "repo"
	if targetTask.Workdir != "" {
		cwd = filepath.Join("repo", targetTask.Workdir)
	}

	inspectCmd := InspectEvalArgs(*targetTask, run.Spec)

	logger.Info("Starting task step 3: inspect eval", "command", inspectCmd, "cwd", cwd)

	pollInterval := r.pollProgressInterval()
	stopPoller := make(chan struct{})
	var pollerWg sync.WaitGroup
	pollerWg.Add(1)

	go func() {
		defer pollerWg.Done()
		r.pollTaskProgress(execCtx, sbClient, run, taskID, effRange, pollInterval, startTime, stopPoller)
	}()

	inspectCtx, inspectCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, 0)
	inspectRes, inspectErr := executeSandboxWithTimeout(inspectCtx, sbClient, sandboxclient.ProcessConfig{
		Command: inspectCmd,
		Cwd:     cwd,
	})
	inspectCancel()

	close(stopPoller)
	pollerWg.Wait()
	if inspectErr != nil || inspectRes == nil || inspectRes.ExitCode != 0 {
		if isStepTimeout(inspectCtx, inspectErr) {
			msg := "step inspect eval failed: sandbox did not report completion"
			logger.Error(errors.New(msg), "Task step inspect eval timed out", "task", taskID)
			return taskExecutionResult{
				taskID:      taskID,
				phase:       v1alpha1.TaskPhaseFailed,
				message:     msg,
				sampleRange: effRange,
			}
		}
		if status.Code(inspectErr) == codes.Unauthenticated {
			msg := fmt.Sprintf("step inspect eval failed: unauthenticated: %v", inspectErr)
			logger.Error(inspectErr, "Task step inspect eval failed", "task", taskID)
			return taskExecutionResult{
				taskID:      taskID,
				phase:       v1alpha1.TaskPhaseFailed,
				message:     msg,
				sampleRange: effRange,
			}
		}
		var stderr string
		var exitCode int
		if inspectRes != nil {
			stderr = inspectRes.Stderr
			exitCode = inspectRes.ExitCode
		}
		tail := extractStderrTail(stderr, r.maxStderrLines(), discovery.MaxStderrTailBytes)
		var msg string
		if inspectRes != nil && inspectRes.ExitCode != 0 {
			if tail != "" {
				msg = fmt.Sprintf("step inspect eval failed with exit code %d: %s", inspectRes.ExitCode, tail)
			} else {
				msg = fmt.Sprintf("step inspect eval failed with exit code %d", inspectRes.ExitCode)
			}
		} else if tail != "" {
			if inspectErr != nil {
				msg = fmt.Sprintf("step inspect eval failed: %v: %s", inspectErr, tail)
			} else {
				msg = fmt.Sprintf("step inspect eval failed: %s", tail)
			}
		} else if inspectErr != nil {
			msg = fmt.Sprintf("step inspect eval failed: %v", inspectErr)
		} else {
			msg = "step inspect eval failed"
		}
		logger.Error(errors.New(msg), "Task step inspect eval failed", "task", taskID, "exitCode", exitCode, "stderr", tail)
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     msg,
			sampleRange: effRange,
		}
	}

	// Step 4: List /out for json log
	lsCtx, lsCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	lsRes, lsErr := executeSandboxWithTimeout(lsCtx, sbClient, sandboxclient.ProcessConfig{
		Command: []string{"ls", "/out"},
	})
	lsCancel()
	if lsErr != nil || lsRes == nil {
		if isStepTimeout(lsCtx, lsErr) {
			msg := "step list /out failed: sandbox did not report completion"
			logger.Error(errors.New(msg), "Task step list /out timed out", "task", taskID)
			return taskExecutionResult{
				taskID:      taskID,
				phase:       v1alpha1.TaskPhaseFailed,
				message:     msg,
				sampleRange: effRange,
			}
		}
		msg := fmt.Sprintf("failed to list /out: %v", lsErr)
		logger.Error(lsErr, "Task step list /out failed", "task", taskID)
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     msg,
			sampleRange: effRange,
		}
	}

	var validFiles []string
	for _, raw := range strings.Fields(lsRes.Stdout) {
		base := filepath.Base(raw)
		if logFileRegex.MatchString(base) && !strings.Contains(base, "..") && !strings.Contains(base, "/") && !strings.Contains(base, "\\") {
			validFiles = append(validFiles, base)
		}
	}
	if len(validFiles) == 0 {
		msg := "no valid JSON log found in /out"
		logger.Error(errors.New(msg), "Task step find JSON log failed", "task", taskID)
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     msg,
			sampleRange: effRange,
		}
	}

	sort.Sort(sort.Reverse(sort.StringSlice(validFiles)))
	logFilename := validFiles[0]

	readCtx, readCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	logBytes, readErr := readFileWithTimeout(readCtx, sbClient, filepath.Join("/out", logFilename))
	readCancel()
	if readErr != nil {
		if isStepTimeout(readCtx, readErr) {
			msg := "step read inspect log failed: sandbox did not report completion"
			logger.Error(errors.New(msg), "Task step read inspect log timed out", "task", taskID)
			return taskExecutionResult{
				taskID:      taskID,
				phase:       v1alpha1.TaskPhaseFailed,
				message:     msg,
				sampleRange: effRange,
			}
		}
		msg := fmt.Sprintf("failed to read inspect log: %v", readErr)
		logger.Error(readErr, "Task step read inspect log failed", "task", taskID)
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     msg,
			sampleRange: effRange,
		}
	}

	// Step 5: Parse Inspect log
	agg, parseErr := artifacts.ParseInspectLog(logBytes)
	if parseErr != nil {
		msg := fmt.Sprintf("failed to parse inspect log: %v", parseErr)
		logger.Error(parseErr, "Task step parse inspect log failed", "task", taskID)
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     msg,
			sampleRange: effRange,
		}
	}

	// Step 6: Store artifact
	tokens, err := artifacts.ExtractInspectTokens(logBytes)
	if err != nil {
		logger.V(1).Info("failed to extract tokens from inspect log", "task", taskID, "error", err)
	}
	latency, err := artifacts.ExtractInspectLatency(logBytes)
	if err != nil {
		logger.V(1).Info("failed to extract latency from inspect log", "task", taskID, "error", err)
	}
	datasetSamples, err := artifacts.ExtractInspectDatasetSamples(logBytes)
	if err != nil {
		logger.V(1).Info("failed to extract dataset samples from inspect log", "task", taskID, "error", err)
	}

	var costUSD *float64
	var costSource v1alpha1.CostSource
	if tokens != nil {
		if price, src, ok := r.lookupModelPrice(run.Spec.Model); ok {
			cost := (float64(tokens.Input)*price.InputPer1M + float64(tokens.Output)*price.OutputPer1M) / 1_000_000.0
			costUSD = &cost
			costSource = src
		}
	}

	finishTime := r.now()
	var artifactSampleRange *artifacts.SampleRange
	if effRange != nil {
		artifactSampleRange = &artifacts.SampleRange{Start: effRange.Start, End: effRange.End}
	}

	sampleSummaries, primaryScorer, err := artifacts.ExtractTaskSampleSummaries(logBytes)
	if err != nil {
		logger.V(1).Info("failed to extract task sample summaries from inspect log", "task", taskID, "error", err)
	}

	successRate, successRateCI, successRateSD := computeTaskAggregateStats(agg)

	meta := artifacts.RunMetadata{
		RunName:           run.Name,
		RunNamespace:      run.Namespace,
		ControllerVersion: r.controllerVersion(),
		TaskID:            taskID,
		Model:             run.Spec.Model,
		Commit:            run.Spec.Repo.Commit,
		Scorer:            primaryScorer,
		SampleCount:       agg.Samples,
		Samples:           sampleSummaries,
		Successes:         agg.Successes,
		MeanScore:         agg.MeanScore,
		SuccessRate:       successRate,
		SuccessRateCI:     successRateCI,
		SuccessRateSD:     successRateSD,
		StartedAt:         &startTime,
		FinishedAt:        &finishTime,
		SampleRange:       artifactSampleRange,
		Tokens:            tokens,
		Latency:           latency,
		CostUSD:           costUSD,
		CostSource:        costSource,
		PossibleSamples:   datasetSamples,
	}
	sinkCtx, sinkCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.artifactSinkTimeout())
	logPath, storeErr := artifacts.StoreArtifactWithTimeout(sinkCtx, r.artifactSink(), meta, logFilename, logBytes)
	sinkCancel()
	if storeErr != nil {
		logger.Error(storeErr, "Failed to store artifact", "task", taskID)
		msg := fmt.Sprintf("failed to store artifact: %v", storeErr)
		return taskExecutionResult{
			taskID:          taskID,
			phase:           v1alpha1.TaskPhaseFailed,
			samples:         agg.Samples,
			successes:       agg.Successes,
			meanScore:       agg.MeanScore,
			message:         msg,
			sampleRange:     effRange,
			possibleSamples: datasetSamples,
		}
	}

	if agg.Samples == 0 {
		msg := "no scored samples"
		logger.Error(errors.New(msg), "Task completed without scored samples", "task", taskID)
		return taskExecutionResult{
			taskID:          taskID,
			phase:           v1alpha1.TaskPhaseFailed,
			message:         msg,
			logPath:         logPath,
			sampleRange:     effRange,
			possibleSamples: datasetSamples,
		}
	}

	var v1Tokens *v1alpha1.TaskTokens
	if tokens != nil {
		v1Tokens = &v1alpha1.TaskTokens{
			Input:      tokens.Input,
			Output:     tokens.Output,
			Total:      tokens.Input + tokens.Output,
			Reasoning:  tokens.Reasoning,
			CacheRead:  tokens.CacheRead,
			CacheWrite: tokens.CacheWrite,
		}
	}

	var v1Latency *v1alpha1.TaskLatency
	if latency != nil {
		v1Latency = &v1alpha1.TaskLatency{
			P50: latency.P50,
			P95: latency.P95,
		}
	}

	planned := computeSamplesPlanned(effRange, run.Spec.Limit, datasetSamples)

	return taskExecutionResult{
		taskID:          taskID,
		phase:           v1alpha1.TaskPhaseCompleted,
		samples:         agg.Samples,
		successes:       agg.Successes,
		meanScore:       agg.MeanScore,
		successRate:     successRate,
		successRateCI:   successRateCI,
		successRateSD:   successRateSD,
		tokens:          v1Tokens,
		latency:         v1Latency,
		costUSD:         costUSD,
		costSource:      costSource,
		possibleSamples: datasetSamples,
		samplesDone:     agg.Samples,
		samplesPlanned:  planned,
		logPath:         logPath,
		sampleRange:     effRange,
	}
}

func (r *RunReconciler) pollTaskProgress(
	ctx context.Context,
	sbClient sandboxclient.Client,
	run *v1alpha1.Run,
	taskID string,
	effRange *v1alpha1.SampleRange,
	pollInterval time.Duration,
	startTime time.Time,
	stopCh <-chan struct{},
) {
	logger := log.FromContext(ctx).WithValues("task", taskID, "run", run.Name)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var lastWrittenSamplesDone int
	lastWriteTime := startTime
	for _, t := range run.Status.Tasks {
		if t.ID == taskID {
			lastWrittenSamplesDone = t.SamplesDone
			if t.LastProgressAt != nil {
				lastWriteTime = t.LastProgressAt.Time
			}
			break
		}
	}

	for {
		select {
		case <-stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Step 1: ls /out via Execute
			pollStepCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
			lsRes, lsErr := executeSandboxWithTimeout(pollStepCtx, sbClient, sandboxclient.ProcessConfig{
				Command: []string{"ls", "/out"},
			})
			pollCancel()
			if lsErr != nil || lsRes == nil || lsRes.ExitCode != 0 {
				logger.V(1).Info("poller ls /out returned error or non-zero", "error", lsErr)
				continue
			}

			// Filter for valid json log files
			var validFiles []string
			for _, raw := range strings.Fields(lsRes.Stdout) {
				base := filepath.Base(raw)
				if logFileRegex.MatchString(base) && !strings.Contains(base, "..") && !strings.Contains(base, "/") && !strings.Contains(base, "\\") {
					validFiles = append(validFiles, base)
				}
			}
			if len(validFiles) == 0 {
				// 404 before first flush: skip
				continue
			}
			sort.Sort(sort.Reverse(sort.StringSlice(validFiles)))
			logFilename := validFiles[0]

			// Step 2: ReadFile newest json log
			readStepCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
			logBytes, readErr := readFileWithTimeout(readStepCtx, sbClient, filepath.Join("/out", logFilename))
			readCancel()
			if readErr != nil {
				var limitErr *discovery.LimitError
				if errors.As(readErr, &limitErr) {
					// LimitError (over 10 MiB): stop polling and keep the last value
					logger.V(1).Info("poller log exceeded ReadFile limit; stopping poller", "limit", limitErr.Limit)
					return
				}
				// 404 or transient error: skip
				logger.V(1).Info("poller ReadFile failed", "error", readErr)
				continue
			}

			// Step 3: decode only samples length and eval.dataset.samples
			samplesCount, datasetSamples, parseErr := artifacts.ExtractInspectProgress(logBytes)
			if parseErr != nil {
				// parse error: keep the last value
				logger.V(1).Info("poller failed to parse log progress", "error", parseErr)
				continue
			}

			// Monotonicity check
			if samplesCount < lastWrittenSamplesDone {
				continue
			}

			samplesDone := samplesCount
			samplesPlanned := computeSamplesPlanned(effRange, run.Spec.Limit, datasetSamples)

			now := r.now()
			timeSinceLastWrite := now.Sub(lastWriteTime)
			writeFloor := r.minProgressWriteInterval()
			if samplesDone != lastWrittenSamplesDone && timeSinceLastWrite >= writeFloor {
				nowMeta := metav1.NewTime(now)
				err := r.updateRunStatus(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Name}, func(status *v1alpha1.RunStatus, gen int64) {
					for i := range status.Tasks {
						if status.Tasks[i].ID == taskID {
							status.Tasks[i].SamplesDone = samplesDone
							if samplesPlanned > 0 {
								status.Tasks[i].SamplesPlanned = samplesPlanned
							}
							status.Tasks[i].LastProgressAt = &nowMeta
							break
						}
					}
				})
				if err != nil {
					logger.V(1).Info("poller updateRunStatus failed", "error", err)
				} else {
					lastWrittenSamplesDone = samplesDone
					lastWriteTime = now
				}
			}
		}
	}
}

func (r *RunReconciler) deleteSandboxIfMatching(ctx context.Context, sandboxKey types.NamespacedName, runNamespace, runName string) error {
	logger := log.FromContext(ctx)
	var sb sandboxv1beta1.Sandbox
	if err := r.Get(ctx, sandboxKey, &sb); err != nil {
		if apierrors.IsNotFound(err) {
			if _, ok := r.getAuthToken(sandboxKey.String()); ok {
				r.cleanupAuthToken(ctx, runNamespace, runName, sandboxKey)
			}
			return nil
		}
		return client.IgnoreNotFound(err)
	}
	if sb.Labels[discovery.LabelRunName] == runName && sb.Labels[discovery.LabelRunNamespace] == runNamespace {
		r.cleanupAuthToken(ctx, runNamespace, runName, sandboxKey)
		if err := r.Delete(ctx, &sb); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to delete sandbox", "sandbox", sandboxKey.Name)
			return err
		}
		logger.Info("Deleted sandbox for run", "sandbox", sandboxKey.Name, "run", runName)
	}
	return nil
}

func (r *RunReconciler) updateRunStatus(ctx context.Context, key types.NamespacedName, mutate func(*v1alpha1.RunStatus, int64)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest v1alpha1.Run
		if err := r.Get(ctx, key, &latest); err != nil {
			return err
		}
		mutate(&latest.Status, latest.Generation)
		return r.Status().Update(ctx, &latest)
	})
}

func (r *RunReconciler) failRun(ctx context.Context, run *v1alpha1.Run, sandbox *sandboxv1beta1.Sandbox, message string) (ctrl.Result, error) {
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Name}
	r.clearDiscoveryTracker(key)
	r.evictCachedManifest(run.Namespace + "/" + run.Name)
	boundedMessage := truncateMessage(message, r.maxMessageBytes())
	err := r.updateRunStatus(ctx, key, func(status *v1alpha1.RunStatus, gen int64) {
		status.Phase = v1alpha1.RunPhaseFailed
		status.Message = boundedMessage
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionTypeDiscovered,
			Status:             metav1.ConditionFalse,
			Reason:             "DiscoveryFailed",
			Message:            boundedMessage,
			ObservedGeneration: gen,
		})
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	if sandbox != nil {
		if err := r.Delete(ctx, sandbox); err != nil && !apierrors.IsNotFound(err) {
			logger := log.FromContext(ctx)
			logger.Error(err, "failed to delete sandbox on failure", "sandbox", sandbox.Name)
		}
		sandboxKey := types.NamespacedName{Namespace: sandbox.Namespace, Name: sandbox.Name}
		r.cleanupAuthToken(ctx, run.Namespace, run.Name, sandboxKey)
	} else {
		sbNamespace := r.SandboxNamespace
		if sbNamespace == "" {
			sbNamespace = discovery.DefaultSandboxNamespace
		}
		sandboxKey := types.NamespacedName{Namespace: sbNamespace, Name: discovery.SandboxNameForRun(run.Name)}
		r.cleanupAuthToken(ctx, run.Namespace, run.Name, sandboxKey)
	}

	return ctrl.Result{}, nil
}

func isPodReady(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func formatPodDeadlineMessage(pod *corev1.Pod) string {
	if pod == nil {
		return "Discovery deadline exceeded: sandbox pod not found"
	}

	var details []string
	allStatuses := append(append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...)
	for _, cs := range allStatuses {
		if cs.State.Waiting != nil {
			reason := cs.State.Waiting.Reason
			msg := cs.State.Waiting.Message
			if reason != "" && msg != "" {
				details = append(details, fmt.Sprintf("container %s waiting: %s: %s", cs.Name, reason, msg))
			} else if reason != "" {
				details = append(details, fmt.Sprintf("container %s waiting: %s", cs.Name, reason))
			} else if msg != "" {
				details = append(details, fmt.Sprintf("container %s waiting: %s", cs.Name, msg))
			}
		}
	}

	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse {
			if cond.Reason != "" && cond.Message != "" {
				details = append(details, fmt.Sprintf("PodScheduled=%s: %s", cond.Reason, cond.Message))
			} else if cond.Reason != "" {
				details = append(details, fmt.Sprintf("PodScheduled=%s", cond.Reason))
			} else if cond.Message != "" {
				details = append(details, fmt.Sprintf("PodScheduled: %s", cond.Message))
			}
		}
	}

	if len(details) == 0 {
		for _, cond := range pod.Status.Conditions {
			if cond.Status == corev1.ConditionFalse && cond.Type != corev1.PodScheduled && cond.Message != "" {
				details = append(details, fmt.Sprintf("%s=%s: %s", cond.Type, cond.Reason, cond.Message))
			}
		}
	}

	msg := fmt.Sprintf("Discovery deadline exceeded (pod phase: %s)", pod.Status.Phase)
	if len(details) > 0 {
		msg = fmt.Sprintf("%s: %s", msg, strings.Join(details, "; "))
	}
	return msg
}

func extractStderrTail(stderr string, maxLines int, maxBytes int) string {
	trimmed := strings.TrimSpace(stderr)
	if trimmed == "" {
		return ""
	}
	lines := strings.Split(trimmed, "\n")
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	res := strings.Join(lines, "\n")
	if maxBytes > 0 && len(res) > maxBytes {
		res = res[len(res)-maxBytes:]
		if idx := strings.Index(res, "\n"); idx != -1 && idx < len(res)-1 {
			res = res[idx+1:]
		}
		for len(res) > 0 && !utf8.RuneStart(res[0]) {
			res = res[1:]
		}
	}
	return strings.TrimSpace(res)
}

func truncateMessage(msg string, maxBytes int) string {
	if maxBytes <= 0 || len(msg) <= maxBytes {
		return msg
	}
	const suffix = "\n... [truncated]"
	if maxBytes <= len(suffix) {
		return msg[:maxBytes]
	}
	cutoff := maxBytes - len(suffix)
	for cutoff > 0 && !utf8.RuneStart(msg[cutoff]) {
		cutoff--
	}
	return msg[:cutoff] + suffix
}

// ValidateRunSpec validates mutual exclusivity of limit and sampleRange, and range constraints.
func ValidateRunSpec(spec v1alpha1.RunSpec) error {
	if spec.Limit != nil && spec.SampleRange != nil {
		return errors.New("limit and sampleRange are mutually exclusive")
	}
	if spec.Limit != nil && *spec.Limit < 1 {
		return fmt.Errorf("limit must be >= 1, got %d", *spec.Limit)
	}
	if spec.SampleRange != nil {
		if spec.SampleRange.Start < 1 {
			return fmt.Errorf("sampleRange.start must be >= 1, got %d", spec.SampleRange.Start)
		}
		if spec.SampleRange.End < spec.SampleRange.Start {
			return fmt.Errorf("sampleRange.end (%d) must be >= start (%d)", spec.SampleRange.End, spec.SampleRange.Start)
		}
	}
	return nil
}

// EffectiveSampleRange returns the effective sample window for a RunSpec.
// If SampleRange is set, it is returned.
// If Limit is set, a 1-based range {1, Limit} is returned.
// If neither is set, nil is returned.
func EffectiveSampleRange(spec v1alpha1.RunSpec) *v1alpha1.SampleRange {
	if spec.SampleRange != nil {
		return &v1alpha1.SampleRange{
			Start: spec.SampleRange.Start,
			End:   spec.SampleRange.End,
		}
	}
	if spec.Limit != nil {
		return &v1alpha1.SampleRange{
			Start: 1,
			End:   *spec.Limit,
		}
	}
	return nil
}

// computeSamplesPlanned calculates the expected number of samples to be evaluated for a task,
// taking into account sampleRange width, limit, and clamping to datasetSamples when known.
func computeSamplesPlanned(sampleRange *v1alpha1.SampleRange, limit *int, datasetSamples int) int {
	planned := 0
	if sampleRange != nil {
		planned = sampleRange.End - sampleRange.Start + 1
		if datasetSamples > 0 && datasetSamples < planned {
			planned = datasetSamples
		}
	} else if limit != nil {
		planned = *limit
		if datasetSamples > 0 && datasetSamples < planned {
			planned = datasetSamples
		}
	} else if datasetSamples > 0 {
		planned = datasetSamples
	}
	return planned
}

// taskRequestedWindowSize returns the number of samples requested for a task based on
// task or run SampleRange, spec Limit, task SamplesPlanned, or task PossibleSamples.
func taskRequestedWindowSize(spec v1alpha1.RunSpec, t v1alpha1.TaskStatus) int {
	effRange := t.SampleRange
	if effRange == nil {
		effRange = EffectiveSampleRange(spec)
	}
	if effRange != nil {
		planned := effRange.End - effRange.Start + 1
		if t.PossibleSamples > 0 && t.PossibleSamples < planned {
			planned = t.PossibleSamples
		}
		return planned
	}
	if spec.Limit != nil && *spec.Limit > 0 {
		planned := *spec.Limit
		if t.PossibleSamples > 0 && t.PossibleSamples < planned {
			planned = t.PossibleSamples
		}
		return planned
	}
	if t.SamplesPlanned > 0 {
		return t.SamplesPlanned
	}
	if t.PossibleSamples > 0 {
		return t.PossibleSamples
	}
	return 0
}

// computeTaskAggregateStats computes success rate point estimate, confidence interval, and SD from task aggregates.
func computeTaskAggregateStats(agg *artifacts.TaskAggregates) (*float64, *v1alpha1.ConfidenceInterval, *float64) {
	if agg == nil || agg.Samples <= 0 {
		return nil, nil, nil
	}
	if agg.IsBinary() {
		est := stats.Proportion(agg.Successes, agg.Samples, 0.95)
		rate := est.Point
		return &rate, &v1alpha1.ConfidenceInterval{
			Lower:      est.CI.Lower,
			Upper:      est.CI.Upper,
			Confidence: 0.95,
			Method:     "wilson",
		}, nil
	}
	est := stats.Mean(agg.Scores, 0.95)
	rate := est.Point
	return &rate, &v1alpha1.ConfidenceInterval{
		Lower:      est.CI.Lower,
		Upper:      est.CI.Upper,
		Confidence: 0.95,
		Method:     "t",
	}, est.SD
}

// InspectEvalArgs builds the CLI arguments for inspect eval.
func InspectEvalArgs(targetTask v1alpha1.ManifestTask, spec v1alpha1.RunSpec) []string {
	cmd := []string{"inspect", "eval", targetTask.Spec, "--model", spec.Model, "--log-dir", "/out", "--log-format", "json", "--log-buffer", "1"}
	if spec.SampleRange != nil {
		cmd = append(cmd, "--limit", fmt.Sprintf("%d-%d", spec.SampleRange.Start, spec.SampleRange.End))
	} else if spec.Limit != nil {
		cmd = append(cmd, "--limit", strconv.Itoa(*spec.Limit))
	}
	return cmd
}

// DefaultMaxRatesSummaryLength is the maximum length of the compact rates printer column string before truncation.
const DefaultMaxRatesSummaryLength = 120

// ComputeRunSummary calculates aggregate statistics across tasks for status.summary.
func ComputeRunSummary(tasks []v1alpha1.TaskStatus) *v1alpha1.RunSummary {
	if len(tasks) == 0 {
		return nil
	}

	completed := 0
	failed := 0
	totalSamples := 0
	var rates []float64

	for _, t := range tasks {
		totalSamples += t.Samples
		switch t.Phase {
		case v1alpha1.TaskPhaseCompleted:
			completed++
			if t.SuccessRate != nil {
				rates = append(rates, *t.SuccessRate)
			}
		case v1alpha1.TaskPhaseFailed:
			failed++
		}
	}

	summary := &v1alpha1.RunSummary{
		Tasks:          fmt.Sprintf("%d/%d", completed, len(tasks)),
		TasksCompleted: completed,
		TasksFailed:    failed,
		TasksTotal:     len(tasks),
		TotalSamples:   totalSamples,
		Rates:          FormatRatesSummary(tasks, DefaultMaxRatesSummaryLength),
	}

	if len(rates) > 0 {
		var sum float64
		minRate := rates[0]
		maxRate := rates[0]
		for _, r := range rates {
			sum += r
			if r < minRate {
				minRate = r
			}
			if r > maxRate {
				maxRate = r
			}
		}
		mean := sum / float64(len(rates))
		summary.MeanSuccessRate = &mean
		summary.SuccessRateRange = &v1alpha1.ScoreRange{
			Min: minRate,
			Max: maxRate,
		}
	}

	return summary
}

// FormatRatesSummary produces a compact string of per-task rates and confidence intervals
// in the format "task1=rate [lo,hi], task2=..." capped at maxLen characters.
func FormatRatesSummary(tasks []v1alpha1.TaskStatus, maxLen int) string {
	var parts []string
	for _, t := range tasks {
		if t.Phase == v1alpha1.TaskPhaseCompleted && t.SuccessRate != nil {
			if t.SuccessRateCI != nil {
				parts = append(parts, fmt.Sprintf("%s=%.2f [%.2f, %.2f]", t.ID, *t.SuccessRate, t.SuccessRateCI.Lower, t.SuccessRateCI.Upper))
			} else {
				parts = append(parts, fmt.Sprintf("%s=%.2f", t.ID, *t.SuccessRate))
			}
		} else if t.Phase == v1alpha1.TaskPhaseFailed {
			parts = append(parts, fmt.Sprintf("%s=failed", t.ID))
		} else if t.Phase == v1alpha1.TaskPhaseRunning {
			parts = append(parts, fmt.Sprintf("%s=running", t.ID))
		} else if t.Phase == v1alpha1.TaskPhasePending {
			parts = append(parts, fmt.Sprintf("%s=pending", t.ID))
		}
	}
	res := strings.Join(parts, ", ")
	if maxLen > 0 && len(res) > maxLen {
		if maxLen <= 3 {
			return res[:maxLen]
		}
		cutoff := maxLen - 3
		for cutoff > 0 && !utf8.RuneStart(res[cutoff]) {
			cutoff--
		}
		return res[:cutoff] + "..."
	}
	return res
}

// CheckDiscoveryDeadlines inspects all Runs in the Discovering phase and fails any that have exceeded their DiscoveryDeadline.
// It returns the number of expired runs that were failed.
func (r *RunReconciler) CheckDiscoveryDeadlines(ctx context.Context) (int, error) {
	var runList v1alpha1.RunList
	if err := r.List(ctx, &runList); err != nil {
		return 0, fmt.Errorf("failed to list runs for watchdog: %w", err)
	}

	logger := log.FromContext(ctx)
	failedCount := 0

	for i := range runList.Items {
		run := &runList.Items[i]
		if run.Status.Phase != v1alpha1.RunPhaseDiscovering {
			continue
		}
		if run.Status.DiscoveryDeadline == nil {
			continue
		}
		if r.now().Before(run.Status.DiscoveryDeadline.Time) {
			continue
		}

		logger.Info("Watchdog: discovery deadline exceeded; failing Run", "run", run.Name, "namespace", run.Namespace)

		sbName := discovery.SandboxNameForRun(run.Name)
		sbNamespace := r.sandboxNamespace()
		var pod corev1.Pod
		var podPtr *corev1.Pod
		if err := r.Get(ctx, types.NamespacedName{Namespace: sbNamespace, Name: sbName}, &pod); err != nil {
			if !apierrors.IsNotFound(err) {
				logger.V(1).Info("Watchdog: failed to get backing pod", "pod", sbName, "error", err)
			}
		} else {
			podPtr = &pod
		}

		msg := formatPodDeadlineMessage(podPtr)
		foundSB, sbErr := r.findDiscoverySandbox(ctx, run)
		if sbErr != nil {
			logger.Error(sbErr, "Watchdog: failed to find discovery sandbox for expired run", "run", run.Name)
		}

		if _, err := r.failRun(ctx, run, foundSB, msg); err != nil {
			logger.Error(err, "Watchdog: failed to fail expired run", "run", run.Name)
		} else {
			failedCount++
		}
	}

	return failedCount, nil
}

// StartDiscoveryWatchdog runs a periodic loop checking for Runs whose discovery deadline has expired.
func (r *RunReconciler) StartDiscoveryWatchdog(ctx context.Context) error {
	ticker := time.NewTicker(r.watchdogInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := r.CheckDiscoveryDeadlines(ctx); err != nil {
				log.FromContext(ctx).V(1).Info("Watchdog check encountered error", "error", err)
			}
		}
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *RunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.Add(manager.RunnableFunc(r.StartDiscoveryWatchdog)); err != nil {
		return fmt.Errorf("failed to add discovery watchdog runnable: %w", err)
	}
	if err := mgr.Add(&BranchHeadBackfillRunnable{
		Reconciler: r,
	}); err != nil {
		return fmt.Errorf("failed to add branch-head backfill runnable: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 4}).
		For(&v1alpha1.Run{}).
		Watches(
			&sandboxv1beta1.Sandbox{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				runName := obj.GetLabels()[discovery.LabelRunName]
				runNS := obj.GetLabels()[discovery.LabelRunNamespace]
				if runName != "" && runNS != "" {
					return []reconcile.Request{
						{NamespacedName: types.NamespacedName{Namespace: runNS, Name: runName}},
					}
				}
				return nil
			}),
		).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				runName := obj.GetLabels()[discovery.LabelRunName]
				runNS := obj.GetLabels()[discovery.LabelRunNamespace]
				if runName != "" && runNS != "" {
					return []reconcile.Request{
						{NamespacedName: types.NamespacedName{Namespace: runNS, Name: runName}},
					}
				}
				return nil
			}),
		).
		Complete(r)
}
