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
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/agent"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/github"
	"github.com/gke-labs/eval-runner/pkg/modelclient/gemini"
	"github.com/gke-labs/eval-runner/pkg/pki"
	"github.com/gke-labs/eval-runner/pkg/sandboxclient"
	"github.com/gke-labs/eval-runner/pkg/stats"
	"github.com/gke-labs/eval-runner/pkg/version"
	"github.com/go-logr/logr"
	"golang.org/x/oauth2"
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

	// maxTaskAttempts is the maximum number of attempts allowed for an infrastructure failure before marking a task failed.
	// Three attempts (the initial run plus up to two retries) tolerates transient disruptions such as
	// pod evictions, node preemptions, or network partitions without consuming unbounded cluster resources.
	maxTaskAttempts = 3

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

	// Config holds the cluster configuration for this reconciler.
	Config ClusterConfig

	// NewAgentModel constructs an agent.Model for a Run.
	NewAgentModel func(run *v1alpha1.Run) (agent.Model, error)

	// AgentModel is an optional fixed model client used directly instead of NewAgentModel (for testing).
	AgentModel agent.Model

	// DiscoveryStepTimeout is the maximum duration for a single discovery Execute call.
	// Defaults to DefaultDiscoveryStepTimeout (60s) if <= 0.
	DiscoveryStepTimeout time.Duration

	// ExecutionStepTimeout is the maximum duration for non-inspect execution Execute calls (e.g. git clone, checkout).
	// Defaults to DefaultExecutionStepTimeout (2m) if <= 0.
	ExecutionStepTimeout time.Duration

	// WatchdogInterval is the interval between watchdog checks for expired discovery deadlines.
	// Defaults to DefaultWatchdogInterval (5s) if <= 0.
	WatchdogInterval time.Duration

	// MinProgressWriteInterval overrides the minimum interval between progress status writes (default: 10s).
	MinProgressWriteInterval time.Duration

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

	// ControllerVersion is the version or revision of the eval-runner controller.
	// If empty, falls back to version.String().
	ControllerVersion string

	// GitHubBaseURL is an optional base URL override for the GitHub API (used in tests).
	GitHubBaseURL string

	// GitHubClientFactory creates a github.Client from credentials. If nil, uses default HTTP client.
	GitHubClientFactory func(creds GitHubCredentials) github.Client

	// GitHubClient overrides GitHub client creation (useful for testing).
	GitHubClient github.Client

	// ServicePortalClient binds egress policies to sandbox pods via the Service Portal Controller.
	ServicePortalClient ServicePortalClient

	// GCPProjectProvisioner provisions and deletes ephemeral per-task GCP projects.
	// If nil, defaults to RESTGCPProjectProvisioner using GCPTokenSource or controller ADC/WIF credentials.
	GCPProjectProvisioner GCPProjectProvisioner

	// GCPAccessTokenMinter mints short-lived project-scoped OAuth2 access tokens by impersonating
	// each project's eval-task service account. If nil, defaults to RESTGCPProjectProvisioner
	// using GCPTokenSource or controller ADC/WIF credentials.
	GCPAccessTokenMinter GCPAccessTokenMinter

	// GCPTokenSource supplies OAuth2 tokens for GCP project provisioning and service account impersonation.
	// If nil, defaults to NewGCPTokenSource using Config.GCPWorkloadIdentityProvider or ADC.
	GCPTokenSource oauth2.TokenSource

	// InstanceID uniquely identifies this controller process/instance.
	InstanceID string

	gcpTokenSourceMu     sync.Mutex
	cachedGCPTokenSource oauth2.TokenSource
	cachedGCPWIFProvider string

	gcpProjectPoolMu         sync.Mutex
	inFlightGCPProjectLeases map[string]struct{}

	boundPodsMu            sync.RWMutex
	boundServicePortalPods map[types.UID]bool

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

// NewProcessInstanceID generates a unique identifier for the current controller process instance.
func NewProcessInstanceID() string {
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		return fmt.Sprintf("instance-%d", time.Now().UnixNano())
	}
	podName := os.Getenv("POD_NAME")
	if podName != "" {
		return fmt.Sprintf("%s-%s", podName, hex.EncodeToString(randomBytes))
	}
	return hex.EncodeToString(randomBytes)
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
	if r.Config.SandboxdImage != "" {
		return r.Config.SandboxdImage
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
	if r.Config.DiscoveryDeadline > 0 {
		return r.Config.DiscoveryDeadline
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
	if r.Config.ExecutionDeadline > 0 {
		return r.Config.ExecutionDeadline
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

func (r *RunReconciler) effectiveTaskCommandTimeoutSeconds(task v1alpha1.ManifestTask, executionDeadline *metav1.Time) int {
	stepTimeout := r.executionStepTimeout()
	if task.TimeoutSeconds != nil {
		stepTimeout = time.Duration(*task.TimeoutSeconds) * time.Second
	}
	if executionDeadline != nil {
		remaining := executionDeadline.Sub(r.now())
		if remaining < stepTimeout {
			if remaining <= 0 {
				return 0
			}
			return int(remaining.Round(time.Second).Seconds())
		}
	}
	return int(stepTimeout.Round(time.Second).Seconds())
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

func checkoutRepositoryProcessConfig(commit string, shallowFetched bool) sandboxclient.ProcessConfig {
	if shallowFetched {
		return sandboxclient.ProcessConfig{
			Command: []string{"git", "-C", "repo", "checkout", "--detach", "FETCH_HEAD"},
		}
	}
	return sandboxclient.ProcessConfig{
		Command: []string{"git", "checkout", commit},
		Cwd:     "repo",
	}
}

type repositoryFetchResult struct {
	shallowFetched bool
	executeResult  *sandboxclient.ExecuteResult
	timedOut       bool
}

func (r *RunReconciler) executeSandboxStep(
	ctx context.Context,
	sandboxClient sandboxclient.Client,
	deadline *metav1.Time,
	stepTimeout time.Duration,
	processConfig sandboxclient.ProcessConfig,
) (*sandboxclient.ExecuteResult, bool, error) {
	stepContext, stepCancel := r.stepContext(ctx, deadline, stepTimeout)
	stepResult, stepErr := executeSandboxWithTimeout(stepContext, sandboxClient, processConfig)
	stepCancel()
	return stepResult, isStepTimeout(stepContext, stepErr), stepErr
}

func (r *RunReconciler) cleanRepositoryDirectory(
	ctx context.Context,
	sandboxClient sandboxclient.Client,
	deadline *metav1.Time,
	stepTimeout time.Duration,
) (bool, error) {
	_, timedOut, cleanErr := r.executeSandboxStep(ctx, sandboxClient, deadline, stepTimeout, sandboxclient.ProcessConfig{
		Command: []string{"rm", "-rf", "repo"},
	})
	if cleanErr != nil {
		log.FromContext(ctx).V(1).Info("Failed to clean repo directory before git operation", "error", cleanErr)
		if timedOut || isUnavailableOrDialTimeout(cleanErr) || status.Code(cleanErr) == codes.Unauthenticated {
			return timedOut, cleanErr
		}
	}
	return false, nil
}

// fetchOrCloneRepository populates the "repo" directory inside the sandbox.
// When commit is non-empty, it prefers a shallow single-commit fetch
// (git init repo + git -C repo fetch --depth 1 -- <url> <commit>). If the git
// server rejects fetching by commit SHA (non-zero git exit), it cleans up
// "repo" and falls back to a full git clone -- <url> repo.
func (r *RunReconciler) fetchOrCloneRepository(
	ctx context.Context,
	sandboxClient sandboxclient.Client,
	deadline *metav1.Time,
	stepTimeout time.Duration,
	repositoryURL string,
	commit string,
) (repositoryFetchResult, error) {
	logger := log.FromContext(ctx)

	if commit != "" {
		if timedOut, cleanErr := r.cleanRepositoryDirectory(ctx, sandboxClient, deadline, stepTimeout); cleanErr != nil {
			return repositoryFetchResult{timedOut: timedOut}, cleanErr
		}

		gitInitResult, gitInitTimedOut, gitInitErr := r.executeSandboxStep(ctx, sandboxClient, deadline, stepTimeout, sandboxclient.ProcessConfig{
			Command: []string{"git", "init", "repo"},
		})
		if gitInitErr != nil {
			return repositoryFetchResult{executeResult: gitInitResult, timedOut: gitInitTimedOut}, gitInitErr
		}

		if gitInitResult != nil && gitInitResult.ExitCode == 0 {
			fetchResult, fetchTimedOut, fetchErr := r.executeSandboxStep(ctx, sandboxClient, deadline, stepTimeout, sandboxclient.ProcessConfig{
				Command: []string{"git", "-C", "repo", "fetch", "--depth", "1", "--", repositoryURL, commit},
			})
			if fetchErr != nil {
				return repositoryFetchResult{executeResult: fetchResult, timedOut: fetchTimedOut}, fetchErr
			}
			if fetchResult != nil && fetchResult.ExitCode == 0 {
				return repositoryFetchResult{shallowFetched: true, executeResult: fetchResult}, nil
			}
			if deadline != nil && !r.now().Before(deadline.Time) {
				return repositoryFetchResult{executeResult: fetchResult}, nil
			}
			var fetchExitCode int
			if fetchResult != nil {
				fetchExitCode = fetchResult.ExitCode
			}
			logger.Info("Shallow git fetch failed; falling back to full git clone", "exitCode", fetchExitCode)
		} else {
			if deadline != nil && !r.now().Before(deadline.Time) {
				return repositoryFetchResult{executeResult: gitInitResult}, nil
			}
			logger.Info("git init failed before shallow fetch; falling back to full git clone")
		}
	}

	if timedOut, cleanErr := r.cleanRepositoryDirectory(ctx, sandboxClient, deadline, stepTimeout); cleanErr != nil {
		return repositoryFetchResult{timedOut: timedOut}, cleanErr
	}

	cloneResult, cloneTimedOut, cloneErr := r.executeSandboxStep(ctx, sandboxClient, deadline, stepTimeout, sandboxclient.ProcessConfig{
		Command: []string{"git", "clone", "--", repositoryURL, "repo"},
	})
	return repositoryFetchResult{executeResult: cloneResult, timedOut: cloneTimedOut}, cloneErr
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

type sandboxAgentExecutor struct {
	client sandboxclient.Client
	cwd    string
	env    map[string]string
}

func (executor *sandboxAgentExecutor) Execute(ctx context.Context, command string) (agent.ExecuteResult, error) {
	result, err := executeSandboxWithTimeout(ctx, executor.client, sandboxclient.ProcessConfig{
		Command: []string{"bash", "-c", command},
		Cwd:     executor.cwd,
		Env:     executor.env,
	})
	if err != nil {
		if result != nil {
			return agent.ExecuteResult{ExitCode: result.ExitCode, Stdout: result.Stdout, Stderr: result.Stderr}, err
		}
		return agent.ExecuteResult{ExitCode: 1}, err
	}
	return agent.ExecuteResult{ExitCode: result.ExitCode, Stdout: result.Stdout, Stderr: result.Stderr}, nil
}

func (r *RunReconciler) taskGracePeriod() time.Duration {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.Config.TaskGracePeriod > 0 {
		return r.Config.TaskGracePeriod
	}
	return DefaultTaskGracePeriod
}

func (r *RunReconciler) maxParallelTasks() int {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.Config.MaxParallelTasks > 0 {
		return r.Config.MaxParallelTasks
	}
	return DefaultMaxParallelTasks
}

func (r *RunReconciler) pollProgressInterval() time.Duration {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	if r.Config.PollProgressInterval > 0 {
		return r.Config.PollProgressInterval
	}
	return DefaultPollProgressInterval
}

func (r *RunReconciler) minProgressWriteInterval() time.Duration {
	if r.MinProgressWriteInterval > 0 {
		return r.MinProgressWriteInterval
	}
	return r.pollProgressInterval()
}

func (r *RunReconciler) lookupModelPrice(model string) (ModelPrice, v1alpha1.CostSource, bool) {
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	return r.Config.Model.LookupPrice(model)
}

// UpdateConfig updates the configuration on RunReconciler in a thread-safe manner.
func (r *RunReconciler) UpdateConfig(cfg ClusterConfig) {
	r.cfgMu.Lock()
	defer r.cfgMu.Unlock()
	r.Config = cfg
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
			if err := r.deleteAllGCPProjectsForRun(ctx, &run); err != nil {
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
		if err := r.deleteAllGCPProjectsForRun(ctx, &run); err != nil {
			logger.Error(err, "failed to delete leftover gcp projects for terminal run", "run", run.Name)
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

	// Fail fast if artifact storage is configured but unavailable.
	if unavailErr, isUnavail := artifacts.IsUnavailable(r.artifactSink()); isUnavail {
		msg := fmt.Sprintf("artifactBucket is set but artifact storage is unavailable: %v", unavailErr)
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
		if podExists && isUsableSandboxPod(&pod, sandbox.UID) {
			podPtr = &pod
		}
		msg := formatPodDeadlineMessage(podPtr)
		logger.Info("Discovery deadline exceeded; failing Run", "run", run.Name, "message", msg)
		return r.failRun(ctx, &run, &sandbox, msg)
	}

	if !podExists || !isUsableSandboxPod(&pod, sandbox.UID) {
		logger.Info("Backing pod not yet created or not controlled by current sandbox", "pod", podKey, "sandboxUID", sandbox.UID)
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
	var fetchResult repositoryFetchResult
	var cloneErr error
	var cloneAttempts int
	var uncountedRetried bool

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

	executeCleanAndClone := func() (repositoryFetchResult, error) {
		return r.fetchOrCloneRepository(
			ctx,
			sbClient,
			run.Status.DiscoveryDeadline,
			r.discoveryStepTimeout(),
			run.Spec.Repo.URL,
			run.Spec.Repo.Commit,
		)
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

		fetchResult, cloneErr = executeCleanAndClone()

		if fetchResult.timedOut {
			return r.failRun(ctx, &run, &sandbox, "step git clone failed: sandbox did not report completion")
		}

		if isUnavailableOrDialTimeout(cloneErr) && !uncountedRetried {
			uncountedRetried = true
			recovered, handled, res, err := handleUnavailable(cloneErr)
			if handled {
				return res, err
			}
			if recovered {
				fetchResult, cloneErr = executeCleanAndClone()
				if fetchResult.timedOut {
					return r.failRun(ctx, &run, &sandbox, "step git clone failed: sandbox did not report completion")
				}
			}
		} else if isUnavailableOrDialTimeout(cloneErr) {
			_, handled, res, err := handleUnavailable(cloneErr)
			if handled {
				return res, err
			}
		}

		if cloneErr == nil && fetchResult.executeResult != nil && fetchResult.executeResult.ExitCode == 0 {
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
	if cloneErr != nil || fetchResult.executeResult == nil || fetchResult.executeResult.ExitCode != 0 {
		if fetchResult.timedOut {
			return r.failRun(ctx, &run, &sandbox, "step git clone failed: sandbox did not report completion")
		}
		var cloneStderr string
		var exitCode int
		if fetchResult.executeResult != nil {
			cloneStderr = fetchResult.executeResult.Stderr
			exitCode = fetchResult.executeResult.ExitCode
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
	// Step 2: git checkout <commit> (or FETCH_HEAD after shallow fetch; no retry)
	// -------------------------------------------------------------------------
	logger.Info("Step 1 succeeded; starting step 2: git checkout", "commit", run.Spec.Repo.Commit, "shallowFetched", fetchResult.shallowFetched)
	checkoutCtx, checkoutCancel := r.stepContext(ctx, run.Status.DiscoveryDeadline, r.discoveryStepTimeout())
	checkoutRes, checkoutErr := executeSandboxWithTimeout(checkoutCtx, sbClient, checkoutRepositoryProcessConfig(run.Spec.Repo.Commit, fetchResult.shallowFetched))
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
	initialPrebuilds := initializePrebuilds(initialTasks, manifest)
	dl := metav1.NewTime(r.now().Add(r.executionDeadline()))
	err = r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
		status.Phase = v1alpha1.RunPhaseRunning
		status.Message = successMsg
		status.Discovery = discoveryStatus
		status.Prebuilds = initialPrebuilds
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
		if isUsableSandboxPod(&pod, sb.UID) {
			return pod.Status.PodIP
		}
		return ""
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
		prebuildName := sb.Annotations[discovery.AnnotationPrebuildName]
		if prebuildName != "" {
			r.cleanupPrebuildAuthToken(ctx, runNamespace, runName, prebuildName, sbKey)
		}
		if err := r.Delete(ctx, sb); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to delete sandbox for run", "sandbox", sb.Name, "run", runName)
		}
	}
	return nil
}

// isSandboxTask returns true if the task declares sandbox and optionally gcp-project
// and/or kind-cluster (and no other resource types).
func isSandboxTask(task v1alpha1.ManifestTask) bool {
	hasSandbox := false
	for _, resource := range task.Resources {
		switch resource.Type {
		case v1alpha1.ResourceTypeSandbox:
			hasSandbox = true
		case v1alpha1.ResourceTypeGCPProject, v1alpha1.ResourceTypeKindCluster:
		default:
			return false
		}
	}
	return hasSandbox
}

func taskRequiresGCPProject(task v1alpha1.ManifestTask) bool {
	for _, resource := range task.Resources {
		if resource.Type == v1alpha1.ResourceTypeGCPProject {
			return true
		}
	}
	return false
}

func taskRequiresKindCluster(task v1alpha1.ManifestTask) bool {
	for _, manifestResource := range task.Resources {
		if manifestResource.Type == v1alpha1.ResourceTypeKindCluster {
			return true
		}
	}
	return false
}

type gcpProjectConfig struct {
	pool             []string
	parentFolderID   string
	billingAccountID string
	taskRoles        []string
}

func (r *RunReconciler) validateGCPProjectConfig() (gcpProjectConfig, error) {
	if r.ServicePortalClient == nil {
		return gcpProjectConfig{}, errors.New("gcp-project resource requires service portal to be configured")
	}
	r.cfgMu.RLock()
	pool := append([]string(nil), r.Config.GCPProjectPool...)
	parentFolderID := strings.TrimSpace(r.Config.GCPProjectParentFolderID)
	billingAccountID := strings.TrimSpace(r.Config.GCPProjectBillingAccountID)
	taskRoles := append([]string(nil), r.Config.GCPProjectTaskRoles...)
	r.cfgMu.RUnlock()
	if len(pool) > 0 {
		return gcpProjectConfig{pool: pool}, nil
	}
	if parentFolderID == "" || billingAccountID == "" {
		return gcpProjectConfig{}, errors.New("gcp-project resource requires gcpProjectPool or both gcpProjectParentFolderID and gcpProjectBillingAccountID to be configured")
	}
	return gcpProjectConfig{
		parentFolderID:   parentFolderID,
		billingAccountID: billingAccountID,
		taskRoles:        taskRoles,
	}, nil
}

func (r *RunReconciler) isPooledGCPProject(projectID string) bool {
	if projectID == "" {
		return false
	}
	r.cfgMu.RLock()
	defer r.cfgMu.RUnlock()
	for _, pooledProject := range r.Config.GCPProjectPool {
		if pooledProject == projectID {
			return true
		}
	}
	return false
}

func (r *RunReconciler) leaseGCPProject(ctx context.Context, namespace string, pool []string) (string, bool, error) {
	r.gcpProjectPoolMu.Lock()
	defer r.gcpProjectPoolMu.Unlock()

	var runList v1alpha1.RunList
	var listOptions []client.ListOption
	if namespace != "" {
		listOptions = append(listOptions, client.InNamespace(namespace))
	}
	if err := r.uncachedReader().List(ctx, &runList, listOptions...); err != nil {
		return "", false, fmt.Errorf("failed to list runs for gcp project pool leasing: %w", err)
	}

	inUseProjects := make(map[string]struct{}, len(r.inFlightGCPProjectLeases))
	for projectID := range r.inFlightGCPProjectLeases {
		inUseProjects[projectID] = struct{}{}
	}
	for i := range runList.Items {
		existingRun := &runList.Items[i]
		if existingRun.Status.Phase == v1alpha1.RunPhaseCompleted || existingRun.Status.Phase == v1alpha1.RunPhaseFailed {
			continue
		}
		for _, existingTask := range existingRun.Status.Tasks {
			if existingTask.Phase == v1alpha1.TaskPhaseRunning && existingTask.GCPProject != "" {
				inUseProjects[existingTask.GCPProject] = struct{}{}
			}
		}
	}

	for _, candidateProject := range pool {
		if _, busy := inUseProjects[candidateProject]; busy {
			continue
		}
		if r.inFlightGCPProjectLeases == nil {
			r.inFlightGCPProjectLeases = make(map[string]struct{})
		}
		r.inFlightGCPProjectLeases[candidateProject] = struct{}{}
		return candidateProject, true, nil
	}
	return "", false, nil
}

func (r *RunReconciler) releaseInFlightGCPProjectLease(projectID string) {
	if projectID == "" {
		return
	}
	r.gcpProjectPoolMu.Lock()
	defer r.gcpProjectPoolMu.Unlock()
	delete(r.inFlightGCPProjectLeases, projectID)
}

func (r *RunReconciler) releaseInFlightGCPProjectLeases(outcomes map[string]pendingTaskStartOutcome) {
	r.gcpProjectPoolMu.Lock()
	defer r.gcpProjectPoolMu.Unlock()
	for _, outcome := range outcomes {
		if outcome.leasedGCPProject && outcome.gcpProject != "" {
			delete(r.inFlightGCPProjectLeases, outcome.gcpProject)
		}
	}
}

func (r *RunReconciler) gcpTokenSource(ctx context.Context) (oauth2.TokenSource, error) {
	if r.GCPTokenSource != nil {
		return r.GCPTokenSource, nil
	}
	r.cfgMu.RLock()
	provider := strings.TrimSpace(r.Config.GCPWorkloadIdentityProvider)
	r.cfgMu.RUnlock()

	r.gcpTokenSourceMu.Lock()
	defer r.gcpTokenSourceMu.Unlock()
	if r.cachedGCPTokenSource != nil && r.cachedGCPWIFProvider == provider {
		return r.cachedGCPTokenSource, nil
	}
	tokenSource, err := NewGCPTokenSource(ctx, provider)
	if err != nil {
		return nil, err
	}
	r.cachedGCPTokenSource = tokenSource
	r.cachedGCPWIFProvider = provider
	return tokenSource, nil
}

func (r *RunReconciler) gcpProjectProvisioner(ctx context.Context) (GCPProjectProvisioner, error) {
	if r.GCPProjectProvisioner != nil {
		return r.GCPProjectProvisioner, nil
	}
	tokenSource, err := r.gcpTokenSource(ctx)
	if err != nil {
		return nil, err
	}
	return &RESTGCPProjectProvisioner{TokenSource: tokenSource}, nil
}

func (r *RunReconciler) gcpAccessTokenMinter(ctx context.Context) (GCPAccessTokenMinter, error) {
	if r.GCPAccessTokenMinter != nil {
		return r.GCPAccessTokenMinter, nil
	}
	tokenSource, err := r.gcpTokenSource(ctx)
	if err != nil {
		return nil, err
	}
	return &RESTGCPProjectProvisioner{TokenSource: tokenSource}, nil
}

func (r *RunReconciler) cleanupTaskGCPProject(ctx context.Context, runName, taskID, projectID string) {
	if err := r.deleteTaskGCPProject(ctx, runName, taskID, projectID); err != nil {
		log.FromContext(ctx).V(1).Info("best-effort gcp project cleanup failed", "run", runName, "task", taskID, "gcpProject", projectID, "error", err)
	}
}

func (r *RunReconciler) deleteTaskGCPProject(ctx context.Context, runName, taskID, projectID string) error {
	if projectID == "" || r.isPooledGCPProject(projectID) {
		return nil
	}
	provisioner, err := r.gcpProjectProvisioner(ctx)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to create gcp project provisioner for cleanup", "run", runName, "task", taskID, "gcpProject", projectID)
		return err
	}
	if err := provisioner.DeleteProject(ctx, projectID); err != nil {
		log.FromContext(ctx).Error(err, "failed to delete ephemeral gcp project", "run", runName, "task", taskID, "gcpProject", projectID)
		return err
	}
	return nil
}

func (r *RunReconciler) deleteAllGCPProjectsForRun(ctx context.Context, run *v1alpha1.Run) error {
	var firstErr error
	for _, task := range run.Status.Tasks {
		if task.GCPProject == "" {
			continue
		}
		if err := r.deleteTaskGCPProject(ctx, run.Name, task.ID, task.GCPProject); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func collectTaskGCPProjects(tasks []v1alpha1.TaskStatus) map[string]string {
	var projects map[string]string
	for _, task := range tasks {
		if task.GCPProject == "" {
			continue
		}
		if projects == nil {
			projects = make(map[string]string)
		}
		projects[task.ID] = task.GCPProject
	}
	return projects
}

func taskGCPProject(run *v1alpha1.Run, taskID string) string {
	if run == nil {
		return ""
	}
	for _, task := range run.Status.Tasks {
		if task.ID == taskID {
			return task.GCPProject
		}
	}
	return ""
}

// validateTaskTimeoutSeconds validates that the task's timeoutSeconds is within the allowed range [1, 7200].
func validateTaskTimeoutSeconds(task v1alpha1.ManifestTask) error {
	if task.TimeoutSeconds != nil {
		if *task.TimeoutSeconds < v1alpha1.MinTaskTimeoutSeconds || *task.TimeoutSeconds > v1alpha1.MaxTaskTimeoutSeconds {
			return fmt.Errorf("invalid timeoutSeconds %d: must be between %d and %d", *task.TimeoutSeconds, v1alpha1.MinTaskTimeoutSeconds, v1alpha1.MaxTaskTimeoutSeconds)
		}
	}
	return nil
}

func (r *RunReconciler) initializeTaskStatus(task v1alpha1.ManifestTask, effRange *v1alpha1.SampleRange, planned int) v1alpha1.TaskStatus {
	cohort := task.Cohort
	if cohort == "" {
		cohort = v1alpha1.DefaultCohort
	}
	failTask := func(message string) v1alpha1.TaskStatus {
		return v1alpha1.TaskStatus{
			ID:             task.ID,
			Cohort:         cohort,
			Phase:          v1alpha1.TaskPhaseFailed,
			Message:        message,
			SampleRange:    effRange,
			SamplesPlanned: planned,
		}
	}
	if err := discovery.ValidateTaskAgent(task); err != nil {
		return failTask(err.Error())
	}
	if len(task.Resources) > 0 && !isSandboxTask(task) {
		return failTask("resources not supported yet")
	}
	var taskResources *v1alpha1.TaskResources
	if isSandboxTask(task) {
		if err := validateTaskTimeoutSeconds(task); err != nil {
			return failTask(err.Error())
		}
		if err := discovery.ValidateTaskResources(task); err != nil {
			return failTask(err.Error())
		}
		if taskRequiresGCPProject(task) {
			if _, err := r.validateGCPProjectConfig(); err != nil {
				return failTask(err.Error())
			}
		}
		taskResources = &v1alpha1.TaskResources{
			Limits: discovery.TaskResourceLimits(task),
		}
	}
	return v1alpha1.TaskStatus{
		ID:             task.ID,
		Cohort:         cohort,
		Phase:          v1alpha1.TaskPhasePending,
		SampleRange:    effRange,
		SamplesPlanned: planned,
		Resources:      taskResources,
	}
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
			if !exists {
				statuses = append(statuses, v1alpha1.TaskStatus{
					ID:             taskID,
					Cohort:         v1alpha1.DefaultCohort,
					Phase:          v1alpha1.TaskPhaseFailed,
					Message:        "task not found in manifest",
					SampleRange:    effRange,
					SamplesPlanned: planned,
				})
				continue
			}
			statuses = append(statuses, r.initializeTaskStatus(task, effRange, planned))
		}
	} else if manifest != nil {
		for _, task := range manifest.Tasks {
			statuses = append(statuses, r.initializeTaskStatus(task, effRange, planned))
		}
	}
	return statuses
}

func (r *RunReconciler) reconcileRunning(ctx context.Context, req reconcile.Request, run *v1alpha1.Run) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Check execution deadline
	if r.isExecutionDeadlineExceeded(run.Status.ExecutionDeadline) {
		r.cleanupExecutionDeadlineExceeded(ctx, run)
		err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, generation int64) {
			r.markDeadlineCutOff(status, generation, nil)
		})
		return ctrl.Result{}, err
	}

	// 2. Check if all tasks have reached a terminal phase
	allFinished := len(run.Status.Tasks) > 0
	completedCount := 0
	skippedCount := 0
	for _, t := range run.Status.Tasks {
		switch t.Phase {
		case v1alpha1.TaskPhaseCompleted:
			completedCount++
		case v1alpha1.TaskPhaseSkipped:
			skippedCount++
		case v1alpha1.TaskPhaseFailed:
			// failed
		default:
			allFinished = false
		}
		if !allFinished {
			break
		}
	}
	if allFinished {
		r.evictCachedManifest(run.Namespace + "/" + run.Name)
		if err := r.deleteAllSandboxesForRun(ctx, run.Namespace, run.Name); err != nil {
			logger.Error(err, "failed to delete sandboxes on run completion", "run", run.Name)
		}
		if err := r.deleteAllGCPProjectsForRun(ctx, run); err != nil {
			logger.Error(err, "failed to delete gcp projects on run completion", "run", run.Name)
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
		if skippedCount == len(run.Status.Tasks) {
			msg := fmt.Sprintf("all %d tasks skipped", len(run.Status.Tasks))
			logger.Info("All tasks skipped; marking Run as Completed", "run", run.Name, "message", msg)
			err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
				status.Phase = v1alpha1.RunPhaseCompleted
				status.Message = msg
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
				status.Prebuilds = nil
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
		initialPrebuilds := initializePrebuilds(initialTasks, manifest)
		err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
			status.Prebuilds = initialPrebuilds
			status.Tasks = initialTasks
			status.Summary = ComputeRunSummary(status.Tasks)
		})
		if err != nil {
			return ctrl.Result{}, err
		}
		run.Status.Prebuilds = initialPrebuilds
		run.Status.Tasks = initialTasks
		run.Status.Summary = ComputeRunSummary(initialTasks)
	} else if len(run.Status.Prebuilds) == 0 && manifest != nil {
		if initialPrebuilds := initializePrebuilds(run.Status.Tasks, manifest); len(initialPrebuilds) > 0 {
			err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, gen int64) {
				status.Prebuilds = initialPrebuilds
			})
			if err != nil {
				return ctrl.Result{}, err
			}
			run.Status.Prebuilds = initialPrebuilds
		}
	}

	prebuildResult, err := r.reconcilePrebuilds(ctx, req, run, manifest)
	if err != nil {
		return ctrl.Result{}, err
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
		if t.Phase == v1alpha1.TaskPhaseCompleted || t.Phase == v1alpha1.TaskPhaseFailed || t.Phase == v1alpha1.TaskPhaseSkipped {
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

	// Start pending tasks up to MaxParallelTasks: observe, decide, apply.
	availableSlots := r.maxParallelTasks() - runningCount
	waitingForOldSandbox := prebuildResult.waitingForOldSandbox
	if availableSlots > 0 {
		pendingObservations, err := r.observePendingTasks(ctx, run, sandboxesByTask)
		if err != nil {
			return ctrl.Result{}, err
		}
		pendingDecisions := decidePendingTasks(
			run.Status.Tasks,
			pendingObservations,
			availableSlots,
			r.InstanceID,
			taskPrebuildByID(manifest),
			prebuildPhaseByName(run.Status.Prebuilds),
		)

		var startOutcomes map[string]pendingTaskStartOutcome
		waitingForOldTaskSandbox := false
		waitingForOldTaskSandbox, startOutcomes = r.executePendingTaskDecisions(ctx, run, manifest, pendingDecisions, sandboxesByTask)
		waitingForOldSandbox = waitingForOldSandbox || waitingForOldTaskSandbox
		if len(startOutcomes) > 0 {
			statusNow := metav1.NewTime(r.now())
			err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, generation int64) {
				applyPendingTaskStartOutcomes(status, startOutcomes, statusNow)
			})
			r.releaseInFlightGCPProjectLeases(startOutcomes)
			if err != nil {
				for taskID, outcome := range startOutcomes {
					if outcome.createdGCPProject {
						r.cleanupTaskGCPProject(ctx, run.Name, taskID, outcome.gcpProject)
					}
				}
				return ctrl.Result{}, err
			}
			applyPendingTaskStartOutcomes(&run.Status, startOutcomes, statusNow)
			if !hasStartedPendingTask(startOutcomes) {
				return ctrl.Result{Requeue: true}, nil
			}
		}
	}

	// Check running tasks for ready pods: observe, decide, apply.
	now := r.now()
	gracePeriod := r.taskGracePeriod()
	maxMessageBytes := r.maxMessageBytes()
	observations := make(map[string]runningTaskObservation)
	for _, task := range run.Status.Tasks {
		if task.Phase != v1alpha1.TaskPhaseRunning {
			continue
		}
		observation, err := r.observeRunningTask(ctx, run, task, sandboxesByTask, now, gracePeriod)
		if err != nil {
			return ctrl.Result{}, err
		}
		observations[task.ID] = observation
	}

	decisions := make(map[string]runningTaskDecision, len(observations))
	for _, task := range run.Status.Tasks {
		observation, ok := observations[task.ID]
		if !ok {
			continue
		}
		decision := decideRunningTask(task, observation, now, gracePeriod, maxMessageBytes)
		if decision.action == runningTaskActionBindServicePortal {
			decision = r.bindAndDecideRunningTask(ctx, run, manifest, task, observation, now, gracePeriod, maxMessageBytes)
		}
		decisions[task.ID] = decision
	}

	var readyTasks []readyTaskItem
	var asyncTaskResults map[string]taskExecutionResult
	hasStatusUpdates, waitingInGracePeriod, waitingForPod, pollingAsync := false, false, false, false
	var minGraceRemaining time.Duration
	for _, task := range run.Status.Tasks {
		decision, ok := decisions[task.ID]
		if !ok {
			continue
		}
		if decision.action == runningTaskActionExecute {
			if manifestTask := findManifestTask(manifest, task.ID); isCommandOnlySandboxTask(manifestTask) {
				asyncDecision, asyncResult := r.reconcileAsyncSandboxTask(ctx, run, task, *manifestTask, decision.sandbox, decision.pod, maxMessageBytes)
				if asyncResult != nil {
					if asyncTaskResults == nil {
						asyncTaskResults = make(map[string]taskExecutionResult)
					}
					asyncTaskResults[task.ID] = *asyncResult
					delete(decisions, task.ID)
					continue
				}
				if asyncDecision == nil {
					pollingAsync = true
					delete(decisions, task.ID)
					continue
				}
				decision = *asyncDecision
				decisions[task.ID] = decision
			}
		}
		switch decision.action {
		case runningTaskActionWait:
			if decision.graceRemaining > 0 {
				logger.V(1).Info("Task sandbox not yet visible in cache; waiting within grace period", "task", task.ID)
				waitingInGracePeriod = true
				if minGraceRemaining == 0 || decision.graceRemaining < minGraceRemaining {
					minGraceRemaining = decision.graceRemaining
				}
			} else {
				waitingForPod = true
			}
		case runningTaskActionExecute:
			readyTasks = append(readyTasks, readyTaskItem{
				task:    task,
				sandbox: *decision.sandbox,
				pod:     *decision.pod,
			})
		case runningTaskActionRecordExecution:
			hasStatusUpdates = true
		case runningTaskActionRescue:
			logger.Info("restart rescue", "task", task.ID, "run", run.Name, "samples", rescuedSampleCount(task, decision.rescued))
			hasStatusUpdates = true
		case runningTaskActionRestart:
			oldSandbox := ""
			if decision.sandbox != nil {
				oldSandbox = decision.sandbox.Name
			}
			logger.Info("restarting task in fresh sandbox", "task", task.ID, "run", run.Name, "oldSandbox", oldSandbox, "reason", decision.restartReason)
			hasStatusUpdates = true
		case runningTaskActionFail:
			hasStatusUpdates = true
		}
	}

	if hasStatusUpdates {
		gcpProjectsByTask := collectTaskGCPProjects(run.Status.Tasks)
		statusNow := metav1.NewTime(r.now())
		err := r.updateRunStatus(ctx, req.NamespacedName, func(status *v1alpha1.RunStatus, generation int64) {
			applyRunningTaskDecisions(status, decisions, statusNow)
		})
		if err != nil {
			return ctrl.Result{}, err
		}
		applyRunningTaskDecisions(&run.Status, decisions, statusNow)
		r.cleanupDecidedTaskSandboxes(ctx, run, decisions, sandboxesByTask, gcpProjectsByTask)
	}

	if len(readyTasks) == 0 && len(asyncTaskResults) == 0 {
		if hasStatusUpdates || prebuildResult.hasStatusUpdates {
			return ctrl.Result{Requeue: true}, nil
		}
		if waitingForOldSandbox {
			return ctrl.Result{RequeueAfter: 500 * time.Millisecond}, nil
		}
		combinedWaitingInGracePeriod := waitingInGracePeriod || prebuildResult.waitingInGracePeriod
		combinedWaitingForPod := waitingForPod || prebuildResult.waitingForPod
		combinedPollingAsync := pollingAsync || prebuildResult.pollingAsync
		combinedMinGraceRemaining := minGraceRemaining
		if prebuildResult.minGraceRemaining > 0 && (combinedMinGraceRemaining == 0 || prebuildResult.minGraceRemaining < combinedMinGraceRemaining) {
			combinedMinGraceRemaining = prebuildResult.minGraceRemaining
		}
		if combinedPollingAsync && !combinedWaitingInGracePeriod && !combinedWaitingForPod {
			return ctrl.Result{RequeueAfter: r.pollProgressInterval()}, nil
		}
		requeueAfter := 1 * time.Second
		if combinedWaitingInGracePeriod && combinedMinGraceRemaining > 0 && combinedMinGraceRemaining < requeueAfter {
			requeueAfter = combinedMinGraceRemaining
		}
		if combinedPollingAsync && r.pollProgressInterval() < requeueAfter {
			requeueAfter = r.pollProgressInterval()
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	// Execute ready tasks, decide outcomes, and apply status updates before cleanup.
	taskResults := asyncTaskResults
	if len(readyTasks) > 0 {
		syncResults := r.executeReadyTasks(ctx, run, readyTasks)
		for taskID, result := range taskResults {
			syncResults[taskID] = result
		}
		taskResults = syncResults
	}
	executionDecisions := decideExecutionResults(run.Status.Tasks, taskResults)
	deadlineExceeded := r.isExecutionDeadlineExceeded(run.Status.ExecutionDeadline)

	if err := r.applyExecutionResultStatus(ctx, req.NamespacedName, run.Spec, executionDecisions, taskResults, deadlineExceeded); err != nil {
		return ctrl.Result{}, err
	}
	if deadlineExceeded {
		r.cleanupExecutionDeadlineExceeded(ctx, run)
		return ctrl.Result{}, nil
	}
	r.cleanupExecutedTaskSandboxes(ctx, run, executionDecisions, sandboxesByTask)

	return ctrl.Result{Requeue: true}, nil
}

type pendingTaskAction string

const (
	pendingTaskActionWait             pendingTaskAction = "wait"
	pendingTaskActionDeleteOldSandbox pendingTaskAction = "delete-old-sandbox"
	pendingTaskActionStart            pendingTaskAction = "start"
)

type pendingTaskObservation struct {
	sandbox     *sandboxv1beta1.Sandbox
	existingPod bool
}

type pendingTaskDecision struct {
	task    v1alpha1.TaskStatus
	action  pendingTaskAction
	sandbox *sandboxv1beta1.Sandbox
}

type pendingTaskStartOutcome struct {
	started           bool
	waiting           bool
	expectedRestarts  int
	failureMessage    string
	sandboxOnly       bool
	timeoutSeconds    int
	resources         *v1alpha1.TaskResources
	gcpProject        string
	createdGCPProject bool
	leasedGCPProject  bool
}

func (r *RunReconciler) observePendingTasks(
	ctx context.Context,
	run *v1alpha1.Run,
	sandboxesByTask map[string]sandboxv1beta1.Sandbox,
) (map[string]pendingTaskObservation, error) {
	observations := make(map[string]pendingTaskObservation)
	for _, task := range run.Status.Tasks {
		if task.Phase != v1alpha1.TaskPhasePending {
			continue
		}
		observation, err := r.observePendingTask(ctx, run, task, sandboxesByTask)
		if err != nil {
			return nil, err
		}
		observations[task.ID] = observation
	}
	return observations, nil
}

func (r *RunReconciler) observePendingTask(
	ctx context.Context,
	run *v1alpha1.Run,
	task v1alpha1.TaskStatus,
	sandboxesByTask map[string]sandboxv1beta1.Sandbox,
) (pendingTaskObservation, error) {
	if existingSandbox, exists := sandboxesByTask[task.ID]; exists {
		sandboxCopy := existingSandbox
		return pendingTaskObservation{sandbox: &sandboxCopy}, nil
	}
	// Check if a terminating pod with the same name still exists from an earlier attempt.
	sandboxName := discovery.TaskSandboxNameForRun(run.Name, task.ID)
	podKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: sandboxName}
	var existingPod corev1.Pod
	if err := r.Get(ctx, podKey, &existingPod); err != nil {
		if apierrors.IsNotFound(err) {
			return pendingTaskObservation{}, nil
		}
		return pendingTaskObservation{}, err
	}
	return pendingTaskObservation{existingPod: true}, nil
}

func decidePendingTasks(
	tasks []v1alpha1.TaskStatus,
	observations map[string]pendingTaskObservation,
	availableSlots int,
	controllerInstance string,
	prebuildsByTask map[string]string,
	prebuildPhases map[string]v1alpha1.PrebuildPhase,
) []pendingTaskDecision {
	if availableSlots <= 0 {
		return nil
	}
	remainingSlots := availableSlots
	var decisions []pendingTaskDecision
	for _, task := range tasks {
		if task.Phase != v1alpha1.TaskPhasePending {
			continue
		}
		if prebuildName := prebuildsByTask[task.ID]; prebuildName != "" {
			if prebuildPhases[prebuildName] != v1alpha1.PrebuildPhaseCompleted {
				continue
			}
		}
		if remainingSlots <= 0 {
			break
		}
		observation := observations[task.ID]
		if observation.sandbox != nil {
			if isOldTaskSandbox(observation.sandbox, controllerInstance, task.Restarts) {
				decisions = append(decisions, pendingTaskDecision{
					task:    task,
					action:  pendingTaskActionDeleteOldSandbox,
					sandbox: observation.sandbox,
				})
				continue
			}
			// Sandbox belongs to this process instance for the current attempt (e.g. status write after Create failed on previous reconcile).
			// Allow start to proceed; Create will tolerate AlreadyExists and status write will record Running.
		} else if observation.existingPod {
			decisions = append(decisions, pendingTaskDecision{
				task:   task,
				action: pendingTaskActionWait,
			})
			continue
		}
		decisions = append(decisions, pendingTaskDecision{
			task:   task,
			action: pendingTaskActionStart,
		})
		remainingSlots--
	}
	return decisions
}

func (r *RunReconciler) executePendingTaskDecisions(
	ctx context.Context,
	run *v1alpha1.Run,
	manifest *v1alpha1.Manifest,
	decisions []pendingTaskDecision,
	sandboxesByTask map[string]sandboxv1beta1.Sandbox,
) (bool, map[string]pendingTaskStartOutcome) {
	logger := log.FromContext(ctx)
	waitingForOldSandbox := false
	for _, decision := range decisions {
		switch decision.action {
		case pendingTaskActionDeleteOldSandbox:
			existingSandbox := decision.sandbox
			sandboxKey := types.NamespacedName{Namespace: existingSandbox.Namespace, Name: existingSandbox.Name}
			if err := r.Delete(ctx, existingSandbox); err != nil && !apierrors.IsNotFound(err) {
				logger.Error(err, "failed to delete old task sandbox for pending task", "task", decision.task.ID)
			}
			r.cleanupTaskAuthToken(ctx, run.Namespace, run.Name, decision.task.ID, sandboxKey)
			delete(sandboxesByTask, decision.task.ID)
			waitingForOldSandbox = true
		case pendingTaskActionWait:
			waitingForOldSandbox = true
		}
	}

	outcomes := make(map[string]pendingTaskStartOutcome)
	for _, decision := range decisions {
		if decision.action != pendingTaskActionStart {
			continue
		}
		outcome := r.startPendingTask(ctx, run, manifest, decision.task, sandboxesByTask)
		if outcome.waiting {
			continue
		}
		outcomes[decision.task.ID] = outcome
	}
	return waitingForOldSandbox, outcomes
}

func findManifestTask(manifest *v1alpha1.Manifest, taskID string) *v1alpha1.ManifestTask {
	if manifest == nil {
		return nil
	}
	for i := range manifest.Tasks {
		if manifest.Tasks[i].ID == taskID {
			return &manifest.Tasks[i]
		}
	}
	return nil
}

func (r *RunReconciler) startPendingTask(
	ctx context.Context,
	run *v1alpha1.Run,
	manifest *v1alpha1.Manifest,
	task v1alpha1.TaskStatus,
	sandboxesByTask map[string]sandboxv1beta1.Sandbox,
) pendingTaskStartOutcome {
	logger := log.FromContext(ctx)
	taskID := task.ID
	manifestTask := findManifestTask(manifest, taskID)
	if manifestTask != nil && manifestTask.Agent != nil {
		if _, err := r.agentModelForRun(run); err != nil {
			message := err.Error()
			logger.Info("Rejecting agent task launch", "task", taskID, "model", run.Spec.Model, "message", message)
			return pendingTaskStartOutcome{failureMessage: message}
		}
	}

	var gcpProject string
	createdGCPProject := false
	leasedGCPProject := false
	if manifestTask != nil && taskRequiresGCPProject(*manifestTask) {
		gcpConfig, err := r.validateGCPProjectConfig()
		if err != nil {
			logger.Info("Rejecting gcp-project task launch", "task", taskID, "message", err.Error())
			return pendingTaskStartOutcome{failureMessage: err.Error()}
		}
		if task.GCPProject != "" {
			gcpProject = task.GCPProject
		} else if len(gcpConfig.pool) > 0 {
			projectID, leased, err := r.leaseGCPProject(ctx, run.Namespace, gcpConfig.pool)
			if err != nil {
				logger.Error(err, "Failed to lease pooled gcp project; leaving task pending", "task", taskID)
				return pendingTaskStartOutcome{waiting: true}
			}
			if !leased {
				logger.V(1).Info("All pooled gcp projects are busy; leaving task pending", "task", taskID)
				return pendingTaskStartOutcome{waiting: true}
			}
			gcpProject = projectID
			leasedGCPProject = true
		} else {
			provisioner, err := r.gcpProjectProvisioner(ctx)
			if err != nil {
				logger.Error(err, "Failed to create gcp project provisioner", "task", taskID)
				return pendingTaskStartOutcome{failureMessage: fmt.Sprintf("failed to initialize gcp project provisioner: %v", err)}
			}
			projectID, err := generateGCPProjectID()
			if err != nil {
				return pendingTaskStartOutcome{failureMessage: fmt.Sprintf("failed to generate gcp project id: %v", err)}
			}
			createContext, createCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
			err = provisioner.CreateProject(createContext, gcpConfig.parentFolderID, gcpConfig.billingAccountID, projectID, gcpConfig.taskRoles)
			createCancel()
			if err != nil {
				logger.Error(err, "Failed to provision ephemeral gcp project", "task", taskID, "gcpProject", projectID)
				return pendingTaskStartOutcome{failureMessage: fmt.Sprintf("failed to provision gcp project: %v", err)}
			}
			gcpProject = projectID
			createdGCPProject = true
		}
	}
	cleanupCreatedProject := func() {
		if createdGCPProject {
			r.cleanupTaskGCPProject(ctx, run.Name, taskID, gcpProject)
		}
		if leasedGCPProject {
			r.releaseInFlightGCPProjectLease(gcpProject)
		}
	}

	sandboxName := discovery.TaskSandboxNameForRun(run.Name, taskID)
	sandboxKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: sandboxName}
	token, err := r.ensureTaskAuthToken(ctx, run, taskID, sandboxKey)
	if err != nil {
		cleanupCreatedProject()
		logger.Error(err, "Failed to ensure task auth token", "task", taskID)
		return pendingTaskStartOutcome{failureMessage: fmt.Sprintf("failed to ensure task auth token: %v", err)}
	}

	// Create server cert Secret before Sandbox so it exists when pod starts.
	if _, err := r.pki().EnsureServerCertSecret(ctx, sandboxName, r.executionDeadline()+time.Hour); err != nil {
		cleanupCreatedProject()
		logger.Error(err, "Failed to ensure task server cert secret", "task", taskID)
		return pendingTaskStartOutcome{failureMessage: fmt.Sprintf("failed to ensure server cert secret: %v", err)}
	}

	desiredSandbox := r.buildPendingTaskSandbox(run, task, manifestTask, sandboxName, token)
	if err := r.Create(ctx, desiredSandbox); err != nil && !apierrors.IsAlreadyExists(err) {
		cleanupCreatedProject()
		logger.Error(err, "Failed to create task sandbox", "task", taskID)
		return pendingTaskStartOutcome{failureMessage: fmt.Sprintf("failed to create sandbox: %v", err)}
	}

	sandboxesByTask[taskID] = *desiredSandbox
	outcome := pendingTaskStartOutcome{
		started:           true,
		expectedRestarts:  task.Restarts,
		gcpProject:        gcpProject,
		createdGCPProject: createdGCPProject,
		leasedGCPProject:  leasedGCPProject,
	}
	if manifestTask != nil && isSandboxTask(*manifestTask) {
		outcome.sandboxOnly = true
		outcome.timeoutSeconds = r.effectiveTaskCommandTimeoutSeconds(*manifestTask, run.Status.ExecutionDeadline)
		outcome.resources = &v1alpha1.TaskResources{
			Limits: discovery.TaskResourceLimits(*manifestTask),
		}
	}
	return outcome
}

func (r *RunReconciler) buildPendingTaskSandbox(
	run *v1alpha1.Run,
	task v1alpha1.TaskStatus,
	manifestTask *v1alpha1.ManifestTask,
	sandboxName string,
	token string,
) *sandboxv1beta1.Sandbox {
	taskImage := ""
	sandboxTask := false
	kindCluster := false
	var taskLimits corev1.ResourceList
	var kindClusterLimits corev1.ResourceList
	if manifestTask != nil {
		taskImage = manifestTask.Image
		sandboxTask = isSandboxTask(*manifestTask)
		if sandboxTask {
			taskLimits = discovery.TaskResourceLimits(*manifestTask)
			kindCluster = taskRequiresKindCluster(*manifestTask)
			if kindCluster {
				kindClusterLimits = discovery.KindClusterResourceLimits(*manifestTask)
			}
		}
	}
	return discovery.BuildTaskSandbox(run, task.ID, discovery.TaskSandboxConfig{
		Namespace:              r.sandboxNamespace(),
		ControllerInstance:     r.InstanceID,
		TaskAttempt:            task.Restarts,
		SampleRunnerImage:      r.sampleRunnerImage(),
		Image:                  taskImage,
		AuthTokenDigest:        discovery.AuthTokenDigest(token),
		ServerCertSecret:       pki.ServerSecretName(sandboxName),
		CAConfigMap:            pki.DefaultCAConfigMapName,
		SandboxdImage:          r.sandboxdImage(),
		CloneRepo:              sandboxTask && run.Spec.Repo.URL != "",
		StepTimeoutSeconds:     int(r.executionStepTimeout().Seconds()),
		SandboxTask:            sandboxTask,
		KindCluster:            kindCluster,
		KindClusterLimits:      kindClusterLimits,
		Limits:                 taskLimits,
		ServicePortalIntercept: r.ServicePortalClient != nil,
	})
}

func applyPendingTaskStartOutcomes(
	status *v1alpha1.RunStatus,
	outcomes map[string]pendingTaskStartOutcome,
	now metav1.Time,
) {
	for i := range status.Tasks {
		task := &status.Tasks[i]
		outcome, ok := outcomes[task.ID]
		if !ok {
			continue
		}
		if !outcome.started {
			task.Phase = v1alpha1.TaskPhaseFailed
			task.Message = outcome.failureMessage
			task.FinishedAt = &now
			task.GCPProject = ""
			continue
		}
		if task.Phase == v1alpha1.TaskPhasePending && task.Restarts == outcome.expectedRestarts {
			task.Phase = v1alpha1.TaskPhaseRunning
			task.StartedAt = &now
			if outcome.sandboxOnly {
				task.TimeoutSeconds = outcome.timeoutSeconds
				task.Resources = outcome.resources.DeepCopy()
			}
			if outcome.gcpProject != "" {
				task.GCPProject = outcome.gcpProject
			}
		}
	}
	status.Summary = ComputeRunSummary(status.Tasks)
}

func hasStartedPendingTask(outcomes map[string]pendingTaskStartOutcome) bool {
	for _, outcome := range outcomes {
		if outcome.started {
			return true
		}
	}
	return false
}

type runningTaskAction string

const (
	runningTaskActionWait              runningTaskAction = "wait"
	runningTaskActionBindServicePortal runningTaskAction = "bind-service-portal"
	runningTaskActionExecute           runningTaskAction = "execute"
	runningTaskActionRecordExecution   runningTaskAction = "record-execution"
	runningTaskActionRestart           runningTaskAction = "restart"
	runningTaskActionRescue            runningTaskAction = "rescue"
	runningTaskActionFail              runningTaskAction = "fail"
)

type runningTaskObservation struct {
	sandbox              *sandboxv1beta1.Sandbox
	sandboxDirty         bool
	startedAt            time.Time
	rescued              *rescuedTaskInfo
	pod                  *corev1.Pod
	podUsable            bool
	servicePortalEnabled bool
	servicePortalBound   bool
}

type runningTaskDecision struct {
	action                 runningTaskAction
	executionID            string
	message                string
	restartReason          string
	countsAsFailure        bool
	graceRemaining         time.Duration
	rescued                *rescuedTaskInfo
	sandbox                *sandboxv1beta1.Sandbox
	pod                    *corev1.Pod
	unmarkServicePortalPod bool
}

// isOldTaskSandbox reports whether sandbox belongs to an earlier attempt or controller process
// or is terminating, so it cannot be reused for the task's current Pending attempt.
func isOldTaskSandbox(sandbox *sandboxv1beta1.Sandbox, controllerInstance string, taskRestarts int) bool {
	if sandbox.DeletionTimestamp != nil {
		return true
	}
	if sandbox.Annotations[discovery.AnnotationControllerInstance] != controllerInstance {
		return true
	}
	if rawAttempt, ok := sandbox.Annotations[discovery.AnnotationTaskAttempt]; ok {
		if attempt, err := strconv.Atoi(rawAttempt); err == nil && attempt < taskRestarts {
			return true
		}
	}
	return false
}

func (decision runningTaskDecision) updatesStatus() bool {
	return decision.cleansUpSandbox() || decision.action == runningTaskActionRecordExecution
}

func (decision runningTaskDecision) cleansUpSandbox() bool {
	switch decision.action {
	case runningTaskActionFail, runningTaskActionRescue, runningTaskActionRestart:
		return true
	default:
		return false
	}
}

func (r *RunReconciler) uncachedReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *RunReconciler) observeRunningTask(
	ctx context.Context,
	run *v1alpha1.Run,
	task v1alpha1.TaskStatus,
	sandboxesByTask map[string]sandboxv1beta1.Sandbox,
	now time.Time,
	gracePeriod time.Duration,
) (runningTaskObservation, error) {
	var observation runningTaskObservation

	sandbox, err := r.observeTaskSandbox(ctx, run, task.ID, sandboxesByTask)
	if err != nil {
		return observation, err
	}

	if sandbox == nil {
		startedAt, err := r.observeTaskStartedAt(ctx, run, task)
		if err != nil {
			return observation, err
		}
		observation.startedAt = startedAt
		if !startedAt.IsZero() && now.Sub(startedAt) < gracePeriod {
			return observation, nil
		}
		rescued, probeErr := r.probeTaskRescue(ctx, run, task)
		if probeErr != nil {
			return observation, probeErr
		}
		observation.rescued = rescued
		return observation, nil
	}

	observation.sandbox = sandbox
	if task.ExecutionID == "" && sandbox.Annotations[discovery.AnnotationControllerInstance] != r.InstanceID {
		observation.sandboxDirty = true
		rescued, probeErr := r.probeTaskRescue(ctx, run, task)
		if probeErr != nil {
			return observation, probeErr
		}
		observation.rescued = rescued
		return observation, nil
	}

	pod, usable := r.observeTaskPod(ctx, task.ID, sandbox)
	observation.pod = pod
	observation.podUsable = usable
	if pod != nil && usable {
		observation.servicePortalEnabled = r.ServicePortalClient != nil
		observation.servicePortalBound = r.isServicePortalPodBound(pod.UID)
	}
	return observation, nil
}

func (r *RunReconciler) observeTaskSandbox(
	ctx context.Context,
	run *v1alpha1.Run,
	taskID string,
	sandboxesByTask map[string]sandboxv1beta1.Sandbox,
) (*sandboxv1beta1.Sandbox, error) {
	if sandbox, ok := sandboxesByTask[taskID]; ok {
		return &sandbox, nil
	}
	// Prefer reading back the created object with the API reader (uncached)
	sandboxName := discovery.TaskSandboxNameForRun(run.Name, taskID)
	sandboxKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: sandboxName}
	var directSandbox sandboxv1beta1.Sandbox
	if err := r.uncachedReader().Get(ctx, sandboxKey, &directSandbox); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
		return nil, nil
	}
	sandboxesByTask[taskID] = directSandbox
	return &directSandbox, nil
}

func (r *RunReconciler) observeTaskStartedAt(ctx context.Context, run *v1alpha1.Run, task v1alpha1.TaskStatus) (time.Time, error) {
	if task.StartedAt != nil {
		return task.StartedAt.Time, nil
	}
	secretName := discovery.TaskAuthSecretNameForRun(run.Name, task.ID)
	secretNamespace := run.Namespace
	if secretNamespace == "" {
		secretNamespace = discovery.DefaultControllerNamespace
	}
	var secret corev1.Secret
	if err := r.uncachedReader().Get(ctx, types.NamespacedName{Namespace: secretNamespace, Name: secretName}, &secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return time.Time{}, err
		}
		return time.Time{}, nil
	}
	if !secret.CreationTimestamp.IsZero() {
		return secret.CreationTimestamp.Time, nil
	}
	return time.Time{}, nil
}

func (r *RunReconciler) observeTaskPod(ctx context.Context, taskID string, sandbox *sandboxv1beta1.Sandbox) (*corev1.Pod, bool) {
	logger := log.FromContext(ctx)
	podName := sandbox.Name
	if name, ok := sandbox.Annotations[sandboxv1beta1.SandboxPodNameAnnotation]; ok && name != "" {
		podName = name
	}
	podKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: podName}
	var pod corev1.Pod
	if err := r.Get(ctx, podKey, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("Task pod not yet created", "task", taskID)
		}
		return nil, false
	}
	// Only execute in a pod that is controlled by the current Sandbox and is not terminating.
	if !isUsableSandboxPod(&pod, sandbox.UID) {
		logger.V(1).Info("Task pod is terminating or not controlled by current sandbox", "task", taskID, "sandboxUID", sandbox.UID)
		return &pod, false
	}
	return &pod, true
}

// decideInfrastructureFailure decides whether an infrastructure failure retries in a fresh sandbox
// or fails the task terminally once maxTaskAttempts have been exhausted.
func decideInfrastructureFailure(
	task v1alpha1.TaskStatus,
	reason string,
	sandbox *sandboxv1beta1.Sandbox,
	pod *corev1.Pod,
	unmarkServicePortalPod bool,
	maxMessageBytes int,
) runningTaskDecision {
	reason = truncateMessage(reason, maxMessageBytes)
	attempts := task.FailureAttempts + 1
	if attempts >= maxTaskAttempts {
		return runningTaskDecision{
			action:                 runningTaskActionFail,
			message:                truncateMessage(fmt.Sprintf("failed after %d attempts: %s", attempts, reason), maxMessageBytes),
			countsAsFailure:        true,
			sandbox:                sandbox,
			pod:                    pod,
			unmarkServicePortalPod: unmarkServicePortalPod,
		}
	}
	return runningTaskDecision{
		action:                 runningTaskActionRestart,
		restartReason:          reason,
		countsAsFailure:        true,
		sandbox:                sandbox,
		pod:                    pod,
		unmarkServicePortalPod: unmarkServicePortalPod,
	}
}

func decideRunningTask(
	task v1alpha1.TaskStatus,
	observation runningTaskObservation,
	now time.Time,
	gracePeriod time.Duration,
	maxMessageBytes int,
) runningTaskDecision {
	if observation.sandbox == nil {
		startedAt := observation.startedAt
		if task.StartedAt != nil {
			startedAt = task.StartedAt.Time
		}
		if !startedAt.IsZero() {
			if elapsed := now.Sub(startedAt); elapsed < gracePeriod {
				return runningTaskDecision{action: runningTaskActionWait, graceRemaining: gracePeriod - elapsed}
			}
		}
		if observation.rescued != nil {
			return runningTaskDecision{action: runningTaskActionRescue, rescued: observation.rescued}
		}
		return decideInfrastructureFailure(task, "task sandbox missing while in Running phase", nil, nil, false, maxMessageBytes)
	}

	if observation.sandboxDirty {
		if observation.rescued != nil {
			return runningTaskDecision{action: runningTaskActionRescue, rescued: observation.rescued}
		}
		return runningTaskDecision{
			action:          runningTaskActionRestart,
			restartReason:   "controller restart",
			countsAsFailure: false,
			sandbox:         observation.sandbox,
		}
	}

	if observation.pod == nil || !observation.podUsable {
		return runningTaskDecision{action: runningTaskActionWait}
	}

	if observation.servicePortalEnabled && !observation.servicePortalBound {
		if observation.pod.Spec.NodeName == "" {
			return runningTaskDecision{action: runningTaskActionWait}
		}
		return runningTaskDecision{action: runningTaskActionBindServicePortal, pod: observation.pod}
	}

	// Setup-step failures (init containers: git clone, git checkout) stay terminal in this slice.
	if failureMessage := taskPodInitFailureMessage(observation.pod); failureMessage != "" {
		return runningTaskDecision{
			action:                 runningTaskActionFail,
			message:                truncateMessage(failureMessage, maxMessageBytes),
			sandbox:                observation.sandbox,
			pod:                    observation.pod,
			unmarkServicePortalPod: true,
		}
	}

	if observation.pod.Status.Phase == corev1.PodFailed {
		return decideInfrastructureFailure(task, taskPodFailureMessage(observation.pod), observation.sandbox, observation.pod, true, maxMessageBytes)
	}
	if observation.pod.Status.Phase == corev1.PodSucceeded {
		return decideInfrastructureFailure(task, "task pod succeeded unexpectedly before execution", observation.sandbox, observation.pod, true, maxMessageBytes)
	}

	finishedCondition := meta.FindStatusCondition(observation.sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionFinished))
	if finishedCondition != nil && finishedCondition.Status == metav1.ConditionTrue {
		return decideInfrastructureFailure(task, fmt.Sprintf("task sandbox finished: %s", finishedCondition.Message), observation.sandbox, observation.pod, false, maxMessageBytes)
	}

	sandboxReady := meta.IsStatusConditionTrue(observation.sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady))
	if (sandboxReady || isPodReady(observation.pod)) && observation.pod.Status.PodIP != "" {
		return runningTaskDecision{action: runningTaskActionExecute, sandbox: observation.sandbox, pod: observation.pod}
	}

	return runningTaskDecision{action: runningTaskActionWait}
}

func (r *RunReconciler) bindAndDecideRunningTask(
	ctx context.Context,
	run *v1alpha1.Run,
	manifest *v1alpha1.Manifest,
	task v1alpha1.TaskStatus,
	observation runningTaskObservation,
	now time.Time,
	gracePeriod time.Duration,
	maxMessageBytes int,
) runningTaskDecision {
	logger := log.FromContext(ctx)
	r.cfgMu.RLock()
	servicePortalOptions := r.Config.ServicePortal
	r.cfgMu.RUnlock()
	bindContext, bindCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	defer bindCancel()

	policy, secrets, modelCredential, prepareError := r.prepareServicePortalBinding(bindContext, run, manifest, task.ID, servicePortalOptions)
	if prepareError != nil {
		logger.Error(prepareError, "Failed to prepare service portal binding", "task", task.ID, "podUID", observation.pod.UID)
		return decideInfrastructureFailure(task, prepareError.Error(), observation.sandbox, observation.pod, false, maxMessageBytes)
	}
	logFields := []any{
		"task", task.ID,
		"podUID", observation.pod.UID,
		"node", observation.pod.Spec.NodeName,
		"httpsHostsCount", len(servicePortalOptions.AllowedHosts),
		"httpHostsCount", len(servicePortalOptions.AllowedHTTPHosts),
		"httpsHosts", truncateHostList(servicePortalOptions.AllowedHosts, 5),
		"httpHosts", truncateHostList(servicePortalOptions.AllowedHTTPHosts, 5),
		"modelCredentialInjected", modelCredential != nil,
	}
	if modelCredential != nil {
		logFields = append(logFields, "modelHost", modelCredential.Host, "modelHeader", modelCredential.Header)
	}
	logger.Info("Binding service portal policy", logFields...)
	if bindError := r.bindServicePortalPolicy(bindContext, string(observation.pod.UID), policy, secrets, modelCredential); bindError != nil {
		logger.Error(bindError, "Failed to bind service portal policy", "task", task.ID, "podUID", observation.pod.UID)
		return decideInfrastructureFailure(task, bindError.Error(), observation.sandbox, observation.pod, false, maxMessageBytes)
	}
	logger.Info("Successfully bound service portal policy", "task", task.ID, "podUID", observation.pod.UID, "modelCredentialInjected", modelCredential != nil)
	r.markServicePortalPodBound(observation.pod.UID)
	observation.servicePortalBound = true
	return decideRunningTask(task, observation, now, gracePeriod, maxMessageBytes)
}

func applyRunningTaskDecisions(status *v1alpha1.RunStatus, decisions map[string]runningTaskDecision, now metav1.Time) {
	for i := range status.Tasks {
		task := &status.Tasks[i]
		if task.Phase != v1alpha1.TaskPhaseRunning {
			continue
		}
		decision, ok := decisions[task.ID]
		if !ok || !decision.updatesStatus() {
			continue
		}
		applyRunningTaskDecision(task, decision, now)
	}
	status.Summary = ComputeRunSummary(status.Tasks)
}

func applyRunningTaskDecision(task *v1alpha1.TaskStatus, decision runningTaskDecision, now metav1.Time) {
	switch decision.action {
	case runningTaskActionRecordExecution:
		task.ExecutionID = decision.executionID
	case runningTaskActionFail:
		task.Phase = v1alpha1.TaskPhaseFailed
		task.Message = decision.message
		task.FinishedAt = &now
		task.ExecutionID = ""
		task.GCPProject = ""
		if decision.countsAsFailure {
			task.FailureAttempts++
		}
	case runningTaskActionRescue:
		applyRescuedTaskStatus(task, decision.rescued)
	case runningTaskActionRestart:
		task.Phase = v1alpha1.TaskPhasePending
		task.Restarts++
		if decision.countsAsFailure {
			task.FailureAttempts++
		}
		task.RestartReason = decision.restartReason
		task.Message = ""
		task.StartedAt = nil
		task.ExecutionID = ""
		task.SamplesDone = 0
		task.LastProgressAt = nil
		task.GCPProject = ""
	}
}

func applyRescuedTaskStatus(task *v1alpha1.TaskStatus, rescued *rescuedTaskInfo) {
	task.Phase = v1alpha1.TaskPhaseCompleted
	task.ExecutionID = ""
	task.GCPProject = ""
	if rescued.summary != nil && rescued.summary.FinishedAt != nil {
		finishedAt := metav1.NewTime(*rescued.summary.FinishedAt)
		task.FinishedAt = &finishedAt
	}
	if task.Message == "" {
		task.Message = "restart rescue"
	} else if !strings.Contains(task.Message, "restart rescue") {
		task.Message = task.Message + " (restart rescue)"
	}
	if task.LogPath == "" && rescued.logPath != "" {
		task.LogPath = rescued.logPath
	}
	if task.LogHash == "" && rescued.logHash != "" {
		task.LogHash = rescued.logHash
	}
	if rescued.summary != nil {
		task.Samples = rescued.summary.SampleCount
		task.Successes = rescued.summary.Successes
		task.MeanScore = rescued.summary.MeanScore
		task.SuccessRate = rescued.summary.SuccessRate
		task.SuccessRateCI = rescued.summary.SuccessRateCI
		task.SuccessRateSD = rescued.summary.SuccessRateSD
		task.PossibleSamples = rescued.summary.PossibleSamples
		if rescued.summary.SampleRange != nil {
			task.SampleRange = &v1alpha1.SampleRange{
				Start: rescued.summary.SampleRange.Start,
				End:   rescued.summary.SampleRange.End,
			}
		}
	} else if rescued.agg != nil {
		if task.Samples == 0 {
			task.Samples = rescued.agg.Samples
			task.Successes = rescued.agg.Successes
			task.MeanScore = rescued.agg.MeanScore
		}
		if task.SuccessRate == nil && rescued.agg.Samples > 0 {
			successRate, successRateCI, successRateSD := computeTaskAggregateStats(rescued.agg)
			task.SuccessRate = successRate
			task.SuccessRateCI = successRateCI
			task.SuccessRateSD = successRateSD
		}
	}
	fillRescuedTaskDefaults(task)
}

func fillRescuedTaskDefaults(task *v1alpha1.TaskStatus) {
	if task.MeanScore == nil && task.Samples > 0 {
		mean := float64(task.Successes) / float64(task.Samples)
		task.MeanScore = &mean
	}
	if task.SuccessRate == nil && task.Samples > 0 {
		successRate := float64(task.Successes) / float64(task.Samples)
		task.SuccessRate = &successRate
	}
	if task.SuccessRateCI == nil && task.Samples > 0 {
		estimate := stats.Proportion(task.Successes, task.Samples, 0.95)
		task.SuccessRateCI = &v1alpha1.ConfidenceInterval{
			Lower:      estimate.CI.Lower,
			Upper:      estimate.CI.Upper,
			Confidence: 0.95,
			Method:     "wilson",
		}
	}
	if task.SamplesDone < task.Samples {
		task.SamplesDone = task.Samples
	}
}

func rescuedSampleCount(task v1alpha1.TaskStatus, rescued *rescuedTaskInfo) int {
	if task.Samples != 0 {
		return task.Samples
	}
	if rescued.summary != nil {
		return rescued.summary.SampleCount
	}
	if rescued.agg != nil {
		return rescued.agg.Samples
	}
	return 0
}

func (r *RunReconciler) cleanupDecidedTaskSandboxes(
	ctx context.Context,
	run *v1alpha1.Run,
	decisions map[string]runningTaskDecision,
	sandboxesByTask map[string]sandboxv1beta1.Sandbox,
	gcpProjectsByTask map[string]string,
) {
	logger := log.FromContext(ctx)
	for _, task := range run.Status.Tasks {
		decision, ok := decisions[task.ID]
		if !ok || !decision.cleansUpSandbox() {
			continue
		}
		if decision.unmarkServicePortalPod && decision.pod != nil {
			r.unmarkServicePortalPodBound(decision.pod.UID)
		}
		sandboxName := discovery.TaskSandboxNameForRun(run.Name, task.ID)
		sandboxKey := types.NamespacedName{Namespace: r.sandboxNamespace(), Name: sandboxName}
		if sandbox, ok := sandboxesByTask[task.ID]; ok {
			if err := r.Delete(ctx, &sandbox); err != nil && !apierrors.IsNotFound(err) {
				logger.Error(err, deleteTaskSandboxErrorMessage(decision.action), "task", task.ID)
			}
		}
		r.cleanupTaskAuthToken(ctx, run.Namespace, run.Name, task.ID, sandboxKey)
		if projectID := gcpProjectsByTask[task.ID]; projectID != "" {
			r.cleanupTaskGCPProject(ctx, run.Name, task.ID, projectID)
		}
	}
}

func deleteTaskSandboxErrorMessage(action runningTaskAction) string {
	switch action {
	case runningTaskActionFail:
		return "failed to delete task sandbox on terminal failure"
	case runningTaskActionRescue:
		return "failed to delete task sandbox on restart rescue"
	case runningTaskActionRestart:
		return "failed to delete task sandbox on restart"
	default:
		return "failed to delete task sandbox"
	}
}

type readyTaskItem struct {
	task    v1alpha1.TaskStatus
	sandbox sandboxv1beta1.Sandbox
	pod     corev1.Pod
}

type executionResultAction string

const (
	executionResultActionApply executionResultAction = "apply"
)

type executionResultDecision struct {
	action executionResultAction
	result taskExecutionResult
}

func (r *RunReconciler) executeReadyTasks(
	ctx context.Context,
	run *v1alpha1.Run,
	readyTasks []readyTaskItem,
) map[string]taskExecutionResult {
	resultsChannel := make(chan taskExecutionResult, len(readyTasks))
	var waitGroup sync.WaitGroup

	for _, item := range readyTasks {
		waitGroup.Add(1)
		go func(task v1alpha1.TaskStatus, sandbox sandboxv1beta1.Sandbox, pod corev1.Pod) {
			defer waitGroup.Done()
			defer r.unmarkServicePortalPodBound(pod.UID)
			result := r.executeTask(ctx, run, task.ID, &sandbox, &pod)
			resultsChannel <- result
		}(item.task, item.sandbox, item.pod)
	}

	waitGroup.Wait()
	close(resultsChannel)

	taskResults := make(map[string]taskExecutionResult, len(readyTasks))
	for result := range resultsChannel {
		taskResults[result.taskID] = result
	}
	return taskResults
}

func decideExecutionResults(
	tasks []v1alpha1.TaskStatus,
	taskResults map[string]taskExecutionResult,
) map[string]executionResultDecision {
	decisions := make(map[string]executionResultDecision, len(taskResults))
	for _, task := range tasks {
		result, ok := taskResults[task.ID]
		if !ok {
			continue
		}
		decisions[task.ID] = decideExecutionResult(task, result)
	}
	return decisions
}

func decideExecutionResult(task v1alpha1.TaskStatus, result taskExecutionResult) executionResultDecision {
	return executionResultDecision{
		action: executionResultActionApply,
		result: result,
	}
}

func (r *RunReconciler) applyExecutionResultStatus(
	ctx context.Context,
	runKey types.NamespacedName,
	runSpec v1alpha1.RunSpec,
	decisions map[string]executionResultDecision,
	taskResults map[string]taskExecutionResult,
	deadlineExceeded bool,
) error {
	// Update task statuses FIRST before deleting sandboxes.
	return r.updateRunStatus(ctx, runKey, func(status *v1alpha1.RunStatus, generation int64) {
		applyExecutionResultDecisions(status, runSpec, decisions, metav1.NewTime(r.now()))
		if deadlineExceeded {
			r.markDeadlineCutOff(status, generation, taskResults)
		}
	})
}

func applyExecutionResultDecisions(
	status *v1alpha1.RunStatus,
	runSpec v1alpha1.RunSpec,
	decisions map[string]executionResultDecision,
	now metav1.Time,
) {
	for i := range status.Tasks {
		task := &status.Tasks[i]
		decision, ok := decisions[task.ID]
		if !ok {
			continue
		}
		applyExecutionResultDecision(task, runSpec, decision, now)
	}
	status.Summary = ComputeRunSummary(status.Tasks)
	if totalCost, hasCost := computeRunTotalCost(status.Tasks); hasCost {
		status.CostUSD = &totalCost
	}
}

func applyExecutionResultDecision(
	task *v1alpha1.TaskStatus,
	runSpec v1alpha1.RunSpec,
	decision executionResultDecision,
	now metav1.Time,
) {
	switch decision.action {
	case executionResultActionApply:
		applyTaskExecutionResult(task, runSpec, decision.result, now)
	}
}

func applyTaskExecutionResult(
	task *v1alpha1.TaskStatus,
	runSpec v1alpha1.RunSpec,
	result taskExecutionResult,
	now metav1.Time,
) {
	task.Phase = result.phase
	if result.phase == v1alpha1.TaskPhaseCompleted || result.phase == v1alpha1.TaskPhaseFailed || result.phase == v1alpha1.TaskPhaseSkipped {
		task.ExecutionID = ""
		task.GCPProject = ""
	}
	if result.cohort != "" {
		task.Cohort = result.cohort
	} else if task.Cohort == "" {
		task.Cohort = v1alpha1.DefaultCohort
	}
	task.Samples = result.samples
	task.Successes = result.successes
	task.MeanScore = result.meanScore
	task.SuccessRate = result.successRate
	task.SuccessRateCI = result.successRateCI
	task.SuccessRateSD = result.successRateSD
	task.Message = result.message
	task.LogPath = result.logPath
	task.LogHash = result.logHash
	task.Tokens = result.tokens
	task.Latency = result.latency
	task.CostUSD = result.costUSD
	task.CostSource = result.costSource
	task.PossibleSamples = result.possibleSamples
	task.FinishedAt = result.finishedAt

	finalSamplesDone := result.samples
	if result.phase != v1alpha1.TaskPhaseCompleted && task.SamplesDone > finalSamplesDone {
		finalSamplesDone = task.SamplesDone
	}
	if finalSamplesDone > task.SamplesDone {
		progressTime := now
		task.LastProgressAt = &progressTime
	}
	task.SamplesDone = finalSamplesDone

	if result.samplesPlanned > 0 {
		task.SamplesPlanned = result.samplesPlanned
	} else if task.SamplesPlanned == 0 {
		task.SamplesPlanned = computeSamplesPlanned(result.sampleRange, runSpec.Limit, result.possibleSamples)
	}
	if result.sampleRange != nil {
		task.SampleRange = result.sampleRange
	} else if task.SampleRange == nil {
		task.SampleRange = EffectiveSampleRange(runSpec)
	}
	if result.timeoutSeconds > 0 {
		task.TimeoutSeconds = result.timeoutSeconds
	}
	if result.resources != nil {
		task.Resources = result.resources
	}
}

func computeRunTotalCost(tasks []v1alpha1.TaskStatus) (float64, bool) {
	var totalCost float64
	hasCost := false
	for i := range tasks {
		if tasks[i].CostUSD != nil {
			totalCost += *tasks[i].CostUSD
			hasCost = true
		}
	}
	return totalCost, hasCost
}

func (r *RunReconciler) cleanupExecutionDeadlineExceeded(ctx context.Context, run *v1alpha1.Run) {
	logger := log.FromContext(ctx)
	r.evictCachedManifest(run.Namespace + "/" + run.Name)
	if err := r.deleteAllSandboxesForRun(ctx, run.Namespace, run.Name); err != nil {
		logger.Error(err, "failed to delete sandboxes on execution deadline", "run", run.Name)
	}
	if err := r.deleteAllGCPProjectsForRun(ctx, run); err != nil {
		logger.Error(err, "failed to delete gcp projects on execution deadline", "run", run.Name)
	}
	logger.Info("Execution deadline exceeded", "run", run.Name)
}

func (r *RunReconciler) cleanupExecutedTaskSandboxes(
	ctx context.Context,
	run *v1alpha1.Run,
	decisions map[string]executionResultDecision,
	sandboxesByTask map[string]sandboxv1beta1.Sandbox,
) {
	logger := log.FromContext(ctx)
	for _, task := range run.Status.Tasks {
		if _, ok := decisions[task.ID]; !ok {
			continue
		}
		if sandbox, ok := sandboxesByTask[task.ID]; ok {
			sandboxKey := types.NamespacedName{Namespace: sandbox.Namespace, Name: sandbox.Name}
			if err := r.Delete(ctx, &sandbox); err != nil && !apierrors.IsNotFound(err) {
				logger.Error(err, "failed to delete task sandbox after execution", "task", task.ID)
			}
			r.cleanupTaskAuthToken(ctx, run.Namespace, run.Name, task.ID, sandboxKey)
		}
		if task.GCPProject != "" {
			r.cleanupTaskGCPProject(ctx, run.Name, task.ID, task.GCPProject)
		}
	}
}

type rescuedTaskInfo struct {
	agg     *artifacts.TaskAggregates
	summary *artifacts.TaskSummary
	logPath string
	logHash string
}

func (r *RunReconciler) probeTaskRescue(ctx context.Context, run *v1alpha1.Run, task v1alpha1.TaskStatus) (*rescuedTaskInfo, error) {
	logger := log.FromContext(ctx)
	requestedWindow := taskRequestedWindowSize(run.Spec, task)
	var rescuedLogPath string
	var rescuedLogHash string

	if task.Samples > 0 && (requestedWindow <= 0 || task.Samples >= requestedWindow) {
		return &rescuedTaskInfo{logPath: task.LogPath, logHash: task.LogHash}, nil
	} else if task.LogPath != "" {
		loadCtx, loadCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.artifactSinkTimeout())
		data, loadErr := artifacts.LoadWithTimeout(loadCtx, r.artifactSink(), task.LogPath)
		loadCancel()
		if loadErr == nil && len(data) > 0 {
			if agg, parseErr := artifacts.ParseInspectLog(data); parseErr == nil && agg.Samples > 0 {
				if requestedWindow <= 0 || agg.Samples >= requestedWindow || (task.PossibleSamples > 0 && agg.Samples >= task.PossibleSamples) {
					return &rescuedTaskInfo{agg: agg, logPath: task.LogPath, logHash: task.LogHash}, nil
				}
			}
		}
	} else if run.Spec.Repo.Commit != "" {
		summaryPath := artifacts.ArtifactTarget(r.artifactSink(), run.Spec.Repo.Commit, task.ID, "summary.json")
		loadCtx, loadCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.artifactSinkTimeout())
		data, loadErr := artifacts.LoadWithTimeout(loadCtx, r.artifactSink(), summaryPath)
		loadCancel()
		if loadErr != nil {
			if !artifacts.IsNotFound(loadErr) {
				logger.V(1).Info("probed artifact sink for restart rescue", "task", task.ID, "path", summaryPath, "outcome", "error", "error", loadErr)
				return nil, fmt.Errorf("probe artifact sink for task %s summary: %w", task.ID, loadErr)
			}
			logger.V(1).Info("probed artifact sink for restart rescue", "task", task.ID, "path", summaryPath, "outcome", "not_found")
		} else if len(data) > 0 {
			var sum artifacts.TaskSummary
			if parseErr := json.Unmarshal(data, &sum); parseErr != nil {
				logger.V(1).Info("probed artifact sink for restart rescue", "task", task.ID, "path", summaryPath, "outcome", "unparseable", "error", parseErr)
			} else if sum.RunName != run.Name || (run.Namespace != "" && sum.RunNamespace != run.Namespace) {
				logger.V(1).Info("probed artifact sink for restart rescue", "task", task.ID, "path", summaryPath, "outcome", "mismatched_run", "summaryRun", sum.RunName, "summaryNamespace", sum.RunNamespace, "run", run.Name, "namespace", run.Namespace)
			} else if requestedWindow <= 0 || sum.SampleCount >= requestedWindow || (sum.PossibleSamples > 0 && sum.SampleCount >= sum.PossibleSamples) {
				if sum.SampleCount > 0 {
					logger.V(1).Info("probed artifact sink for restart rescue", "task", task.ID, "path", summaryPath, "outcome", "complete", "samples", sum.SampleCount)
					if sum.LogFilename != "" {
						rescuedLogPath = artifacts.ArtifactTarget(r.artifactSink(), run.Spec.Repo.Commit, task.ID, sum.LogFilename)
						rescuedLogHash = sum.LogHash
					}
					return &rescuedTaskInfo{summary: &sum, logPath: rescuedLogPath, logHash: rescuedLogHash}, nil
				}
				logger.V(1).Info("probed artifact sink for restart rescue", "task", task.ID, "path", summaryPath, "outcome", "empty", "samples", 0)
			} else {
				logger.V(1).Info("probed artifact sink for restart rescue", "task", task.ID, "path", summaryPath, "outcome", "partial", "samples", sum.SampleCount, "requested", requestedWindow)
			}
		}
	}
	return nil, nil
}

func (r *RunReconciler) isExecutionDeadlineExceeded(deadline *metav1.Time) bool {
	return deadline != nil && !r.now().Before(deadline.Time)
}

func formatTaskDuration(duration time.Duration) string {
	if duration < 0 {
		duration = 0
	}
	return duration.Round(time.Second).String()
}

func (r *RunReconciler) markDeadlineCutOff(status *v1alpha1.RunStatus, generation int64, taskResults map[string]taskExecutionResult) {
	now := metav1.NewTime(r.now())
	runningCutOffCount := 0
	pendingCutOffCount := 0

	for i := range status.Prebuilds {
		prebuild := &status.Prebuilds[i]
		if prebuild.Phase == v1alpha1.PrebuildPhasePending || prebuild.Phase == v1alpha1.PrebuildPhaseRunning {
			prebuild.Phase = v1alpha1.PrebuildPhaseFailed
			prebuild.ExecutionID = ""
			if prebuild.FinishedAt == nil {
				prebuild.FinishedAt = &now
			}
			prebuild.Message = "cut off by Run execution deadline"
		}
	}

	for i := range status.Tasks {
		task := &status.Tasks[i]
		if res, ok := taskResults[task.ID]; ok && res.cutOff {
			task.Phase = v1alpha1.TaskPhaseFailed
			task.ExecutionID = ""
			task.GCPProject = ""
			if task.FinishedAt == nil {
				task.FinishedAt = &now
			}
			var elapsed time.Duration
			if task.StartedAt != nil {
				elapsed = task.FinishedAt.Time.Sub(task.StartedAt.Time)
			}
			task.Message = fmt.Sprintf("cut off by Run execution deadline after running for %s", formatTaskDuration(elapsed))
			runningCutOffCount++
		} else if task.Phase == v1alpha1.TaskPhaseRunning {
			task.Phase = v1alpha1.TaskPhaseFailed
			task.ExecutionID = ""
			task.GCPProject = ""
			task.FinishedAt = &now
			var elapsed time.Duration
			if task.StartedAt != nil {
				elapsed = now.Time.Sub(task.StartedAt.Time)
			}
			task.Message = fmt.Sprintf("cut off by Run execution deadline after running for %s", formatTaskDuration(elapsed))
			runningCutOffCount++
		} else if task.Phase == v1alpha1.TaskPhasePending {
			task.Phase = v1alpha1.TaskPhaseFailed
			task.ExecutionID = ""
			task.GCPProject = ""
			task.FinishedAt = &now
			task.Message = "cut off by Run execution deadline (not started)"
			pendingCutOffCount++
		}
	}

	var parts []string
	if runningCutOffCount > 0 {
		parts = append(parts, fmt.Sprintf("%d running", runningCutOffCount))
	}
	if pendingCutOffCount > 0 {
		parts = append(parts, fmt.Sprintf("%d not started", pendingCutOffCount))
	}
	message := "Execution deadline exceeded"
	if len(parts) > 0 {
		message = fmt.Sprintf("Execution deadline exceeded (%s cut off)", strings.Join(parts, ", "))
	}

	status.Phase = v1alpha1.RunPhaseFailed
	status.Message = message
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               "Completed",
		Status:             metav1.ConditionFalse,
		Reason:             "ExecutionDeadlineExceeded",
		Message:            message,
		ObservedGeneration: generation,
	})
	status.Summary = ComputeRunSummary(status.Tasks)
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
	logHash         string
	sampleRange     *v1alpha1.SampleRange
	timeoutSeconds  int
	resources       *v1alpha1.TaskResources
	finishedAt      *metav1.Time
	cutOff          bool
}

func (r *RunReconciler) handleStepExecutionFailure(
	ctx context.Context,
	runName string,
	stepName string,
	timedOut bool,
	stepResult *sandboxclient.ExecuteResult,
	stepError error,
	taskID string,
	sampleRange *v1alpha1.SampleRange,
) (taskExecutionResult, bool) {
	if stepError == nil && stepResult != nil && stepResult.ExitCode == 0 {
		return taskExecutionResult{}, false
	}
	logger := log.FromContext(ctx).WithValues("task", taskID, "run", runName)
	if timedOut || isStepTimeout(nil, stepError) {
		message := fmt.Sprintf("step %s failed: sandbox did not report completion", stepName)
		logger.Error(errors.New(message), fmt.Sprintf("Task step %s timed out", stepName), "task", taskID)
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     message,
			sampleRange: sampleRange,
		}, true
	}
	if status.Code(stepError) == codes.Unauthenticated {
		message := fmt.Sprintf("step %s failed: unauthenticated: %v", stepName, stepError)
		logger.Error(stepError, fmt.Sprintf("Task step %s failed", stepName), "task", taskID)
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     message,
			sampleRange: sampleRange,
		}, true
	}
	var stderr string
	var exitCode int
	if stepResult != nil {
		stderr = stepResult.Stderr
		exitCode = stepResult.ExitCode
	}
	tail := extractStderrTail(stderr, r.maxStderrLines(), discovery.MaxStderrTailBytes)
	prefix := fmt.Sprintf("step %s failed", stepName)
	var message string
	if stepResult != nil && stepResult.ExitCode != 0 {
		if tail != "" {
			message = fmt.Sprintf("%s with exit code %d: %s", prefix, stepResult.ExitCode, tail)
		} else {
			message = fmt.Sprintf("%s with exit code %d", prefix, stepResult.ExitCode)
		}
	} else if tail != "" {
		if stepError != nil {
			message = fmt.Sprintf("%s: %v: %s", prefix, stepError, tail)
		} else {
			message = fmt.Sprintf("%s: %s", prefix, tail)
		}
	} else if stepError != nil {
		message = fmt.Sprintf("%s: %v", prefix, stepError)
	} else {
		message = prefix
	}
	logger.Error(errors.New(message), fmt.Sprintf("Task step %s failed", stepName), "task", taskID, "exitCode", exitCode, "stderr", tail)
	return taskExecutionResult{
		taskID:      taskID,
		phase:       v1alpha1.TaskPhaseFailed,
		message:     message,
		sampleRange: sampleRange,
	}, true
}

func (r *RunReconciler) handleStepReadFailure(
	ctx context.Context,
	runName string,
	stepName string,
	readContext context.Context,
	readError error,
	taskID string,
	sampleRange *v1alpha1.SampleRange,
) (taskExecutionResult, bool) {
	if readError == nil {
		return taskExecutionResult{}, false
	}
	logger := log.FromContext(ctx).WithValues("task", taskID, "run", runName)
	if isStepTimeout(readContext, readError) {
		message := fmt.Sprintf("step read %s failed: sandbox did not report completion", stepName)
		logger.Error(errors.New(message), fmt.Sprintf("Task step read %s timed out", stepName), "task", taskID)
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     message,
			sampleRange: sampleRange,
		}, true
	}
	message := fmt.Sprintf("failed to read %s: %v", stepName, readError)
	logger.Error(readError, fmt.Sprintf("Task step read %s failed", stepName), "task", taskID)
	return taskExecutionResult{
		taskID:      taskID,
		phase:       v1alpha1.TaskPhaseFailed,
		message:     message,
		sampleRange: sampleRange,
	}, true
}

func (r *RunReconciler) executeTask(ctx context.Context, run *v1alpha1.Run, taskID string, sb *sandboxv1beta1.Sandbox, pod *corev1.Pod) (res taskExecutionResult) {
	startTime := r.now()
	logger := log.FromContext(ctx).WithValues("task", taskID, "run", run.Name)
	effRange := EffectiveSampleRange(run.Spec)

	cohort := v1alpha1.DefaultCohort
	// Ensure every return (including early error returns) carries the task cohort and finishedAt.
	defer func() {
		if res.cohort == "" {
			res.cohort = cohort
		}
		if res.finishedAt == nil {
			now := metav1.NewTime(r.now())
			res.finishedAt = &now
		}
		if res.phase == v1alpha1.TaskPhaseFailed && r.isExecutionDeadlineExceeded(run.Status.ExecutionDeadline) {
			res.cutOff = true
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

	sbClient, err := r.newTaskSandboxClient(execCtx, run, taskID, sb, pod)
	if err != nil {
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     err.Error(),
			sampleRange: effRange,
		}
	}
	defer func() {
		if closeErr := sbClient.Close(); closeErr != nil {
			logger.Error(closeErr, "failed to close sandbox client")
		}
	}()

	if isSandboxTask(*targetTask) {
		return r.executeSandboxTask(ctx, run, taskID, sbClient, *targetTask, effRange, startTime)
	}

	// Ensure /workspace/out exists
	mkdirCtx, mkdirCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	if _, mkdirErr := executeSandboxWithTimeout(mkdirCtx, sbClient, sandboxclient.ProcessConfig{
		Command: []string{"mkdir", "-p", "/workspace/out"},
	}); mkdirErr != nil {
		logger.V(1).Info("mkdir /workspace/out returned error", "error", mkdirErr)
	}
	mkdirCancel()

	// Step 1: Shallow fetch commit (falling back to full git clone)
	logger.Info("Starting task step 1: git clone", "url", run.Spec.Repo.URL)
	fetchResult, cloneErr := r.fetchOrCloneRepository(
		ctx,
		sbClient,
		run.Status.ExecutionDeadline,
		r.executionStepTimeout(),
		run.Spec.Repo.URL,
		run.Spec.Repo.Commit,
	)
	if failedResult, failed := r.handleStepExecutionFailure(ctx, run.Name, "git clone", fetchResult.timedOut, fetchResult.executeResult, cloneErr, taskID, effRange); failed {
		return failedResult
	}

	// Step 2: Checkout commit (or FETCH_HEAD after shallow fetch)
	logger.Info("Starting task step 2: git checkout", "commit", run.Spec.Repo.Commit, "shallowFetched", fetchResult.shallowFetched)
	checkoutCtx, checkoutCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	checkoutRes, checkoutErr := executeSandboxWithTimeout(checkoutCtx, sbClient, checkoutRepositoryProcessConfig(run.Spec.Repo.Commit, fetchResult.shallowFetched))
	checkoutCancel()
	if failedResult, failed := r.handleStepExecutionFailure(ctx, run.Name, "git checkout", isStepTimeout(checkoutCtx, checkoutErr), checkoutRes, checkoutErr, taskID, effRange); failed {
		return failedResult
	}

	// Step 3: inspect eval
	cwd := "repo"
	if targetTask.Workdir != "" {
		cwd = filepath.Join("repo", targetTask.Workdir)
	}

	inspectCmd := InspectEvalArgs(*targetTask, run.Spec)
	if modelBaseURL := r.servicePortalModelBaseURL(); modelBaseURL != "" && !isMockModel(run.Spec.Model) {
		inspectCmd = append(inspectCmd, "--model-base-url", modelBaseURL)
	}

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
	if failedResult, failed := r.handleStepExecutionFailure(ctx, run.Name, "inspect eval", isStepTimeout(inspectCtx, inspectErr), inspectRes, inspectErr, taskID, effRange); failed {
		return failedResult
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
	if failedResult, failed := r.handleStepReadFailure(ctx, run.Name, "inspect log", readCtx, readErr, taskID, effRange); failed {
		return failedResult
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
	storedArtifact, storeErr := artifacts.StoreArtifactWithTimeout(sinkCtx, r.artifactSink(), meta, logFilename, logBytes)
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
	logPath := storedArtifact.Location
	logHash := storedArtifact.Hash

	if agg.Samples == 0 {
		msg := "no scored samples"
		logger.Error(errors.New(msg), "Task completed without scored samples", "task", taskID)
		return taskExecutionResult{
			taskID:          taskID,
			phase:           v1alpha1.TaskPhaseFailed,
			message:         msg,
			logPath:         logPath,
			logHash:         logHash,
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
		logHash:         logHash,
		sampleRange:     effRange,
	}
}

func isMockModel(model string) bool {
	return model == "mockllm" || strings.HasPrefix(model, "mockllm/")
}

// MaxInstructionBytes is the maximum allowed size of an agent instruction file (64 KiB).
const MaxInstructionBytes = 64 * 1024

type taskTokensAndCost struct {
	artifactTokens *artifacts.TaskTokens
	v1Tokens       *v1alpha1.TaskTokens
	costUSD        *float64
	costSource     v1alpha1.CostSource
}

func (r *RunReconciler) computeAgentTokensAndCost(model string, agentResult *agent.Result) *taskTokensAndCost {
	if agentResult == nil || (agentResult.InputTokens == 0 && agentResult.OutputTokens == 0) {
		return nil
	}
	totalTokens := agentResult.InputTokens + agentResult.OutputTokens
	tc := &taskTokensAndCost{
		artifactTokens: &artifacts.TaskTokens{
			Input:  agentResult.InputTokens,
			Output: agentResult.OutputTokens,
			Total:  totalTokens,
		},
		v1Tokens: &v1alpha1.TaskTokens{
			Input:  agentResult.InputTokens,
			Output: agentResult.OutputTokens,
			Total:  totalTokens,
		},
	}
	if price, source, ok := r.lookupModelPrice(model); ok {
		cost := (float64(agentResult.InputTokens)*price.InputPer1M + float64(agentResult.OutputTokens)*price.OutputPer1M) / 1_000_000.0
		tc.costUSD = &cost
		tc.costSource = source
	}
	return tc
}

func applyTokensAndCost(res *taskExecutionResult, tc *taskTokensAndCost) {
	if tc != nil && res != nil {
		res.tokens = tc.v1Tokens
		res.costUSD = tc.costUSD
		res.costSource = tc.costSource
	}
}

// DefaultAgentModelConstructor returns a constructor that creates a scripted model
// for mock models, a Gemini client for Gemini models using the provided API key,
// and rejects unsupported models or missing credentials.
func DefaultAgentModelConstructor(geminiAPIKey string) func(run *v1alpha1.Run) (agent.Model, error) {
	return func(run *v1alpha1.Run) (agent.Model, error) {
		if run == nil {
			return nil, errors.New("run is required")
		}
		if isMockModel(run.Spec.Model) {
			return agent.NewScriptedModel(), nil
		}
		if !gemini.IsGeminiModel(run.Spec.Model) {
			return nil, fmt.Errorf("unsupported model %q for agent task: only Gemini models are supported", run.Spec.Model)
		}
		if geminiAPIKey == "" {
			return nil, errors.New("controller has no model credential")
		}
		return gemini.New(gemini.Options{
			Model:  run.Spec.Model,
			APIKey: geminiAPIKey,
		})
	}
}

func (r *RunReconciler) agentModelForRun(run *v1alpha1.Run) (agent.Model, error) {
	if r.AgentModel != nil {
		return r.AgentModel, nil
	}
	if r.NewAgentModel == nil {
		return nil, errors.New("no agent model configured")
	}
	return r.NewAgentModel(run)
}

type agentPhaseOutcome struct {
	loopEndingMessage string
	instruction       string
	duration          time.Duration
	result            *agent.Result
}

func (r *RunReconciler) executeAgentPhase(
	ctx context.Context,
	run *v1alpha1.Run,
	taskID string,
	sbClient sandboxclient.Client,
	targetTask v1alpha1.ManifestTask,
	cwd string,
	effectiveTimeoutSeconds int,
	effectiveResources *v1alpha1.TaskResources,
	effRange *v1alpha1.SampleRange,
) (agentPhaseOutcome, *taskExecutionResult) {
	fail := func(message string) *taskExecutionResult {
		return &taskExecutionResult{
			taskID:         taskID,
			phase:          v1alpha1.TaskPhaseFailed,
			message:        message,
			timeoutSeconds: effectiveTimeoutSeconds,
			resources:      effectiveResources,
			sampleRange:    effRange,
		}
	}

	agentModel, err := r.agentModelForRun(run)
	if err != nil {
		return agentPhaseOutcome{}, fail(err.Error())
	}

	taskEnv := gcpProjectTaskEnv(taskGCPProject(run, taskID))
	if len(targetTask.Agent.PrepareCommand) > 0 {
		prepareTimeout := r.executionStepTimeout()
		if targetTask.TimeoutSeconds != nil && *targetTask.TimeoutSeconds > 0 {
			prepareTimeout = time.Duration(*targetTask.TimeoutSeconds) * time.Second
		}
		prepareContext, prepareCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, prepareTimeout)
		prepareResult, prepareError := executeSandboxWithTimeout(prepareContext, sbClient, sandboxclient.ProcessConfig{
			Command: targetTask.Agent.PrepareCommand,
			Cwd:     cwd,
			Env:     taskEnv,
		})
		prepareCancel()
		if failedResult, failed := r.handleStepExecutionFailure(ctx, run.Name, "agent prepare command", isStepTimeout(prepareContext, prepareError), prepareResult, prepareError, taskID, effRange); failed {
			failedResult.timeoutSeconds = effectiveTimeoutSeconds
			failedResult.resources = effectiveResources
			return agentPhaseOutcome{}, &failedResult
		}
	}

	cleanedPath := path.Clean(targetTask.Agent.InstructionPath)
	if !strings.HasPrefix(cleanedPath, "/workspace/") && cleanedPath != "/workspace" {
		return agentPhaseOutcome{}, fail(fmt.Sprintf("instructionPath %q must be an absolute path inside /workspace", targetTask.Agent.InstructionPath))
	}
	cleanPath := strings.TrimPrefix(cleanedPath, "/workspace")
	readInstructionContext, readInstructionCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	instructionBytes, readInstructionError := readFileWithTimeout(readInstructionContext, sbClient, cleanPath)
	readInstructionCancel()
	if failedResult, failed := r.handleStepReadFailure(ctx, run.Name, "agent instruction file", readInstructionContext, readInstructionError, taskID, effRange); failed {
		failedResult.timeoutSeconds = effectiveTimeoutSeconds
		failedResult.resources = effectiveResources
		return agentPhaseOutcome{}, &failedResult
	}
	if len(instructionBytes) > MaxInstructionBytes {
		return agentPhaseOutcome{}, fail(fmt.Sprintf("agent instruction file %s exceeds %d bytes (%d bytes)", targetTask.Agent.InstructionPath, MaxInstructionBytes, len(instructionBytes)))
	}

	instruction := string(instructionBytes)
	agentTimeout := r.executionStepTimeout()
	if targetTask.Agent.TimeoutSeconds != nil && *targetTask.Agent.TimeoutSeconds > 0 {
		agentTimeout = time.Duration(*targetTask.Agent.TimeoutSeconds) * time.Second
	}
	agentContext, agentCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, agentTimeout)
	executor := &sandboxAgentExecutor{client: sbClient, cwd: cwd, env: taskEnv}
	agentStartTime := r.now()
	agentResult, agentError := agent.Run(agentContext, agentModel, executor, agent.Options{
		Instruction: instruction,
	})
	agentDuration := r.now().Sub(agentStartTime)
	if agentDuration < 0 {
		agentDuration = 0
	}
	agentCancel()

	outcome := agentPhaseOutcome{
		instruction: instruction,
		duration:    agentDuration,
		result:      agentResult,
	}

	if agentResult == nil || agentResult.StopReason == agent.StopReasonModelError {
		errorMessage := "model error"
		if agentError != nil {
			errorMessage = agentError.Error()
		}
		failedResult := fail(fmt.Sprintf("agent model error: %s", truncateMessage(errorMessage, r.maxMessageBytes())))
		return outcome, failedResult
	}

	switch agentResult.StopReason {
	case agent.StopReasonMaxSteps:
		outcome.loopEndingMessage = "agent loop stopped: step bound reached"
	case agent.StopReasonContextDone:
		outcome.loopEndingMessage = "agent loop stopped: timeout"
	}
	return outcome, nil
}

func enrichResultsWithAgent(resultsBytes []byte, instruction string, agentDuration time.Duration, agentResult *agent.Result) ([]byte, error) {
	if agentResult == nil {
		return resultsBytes, nil
	}

	var resultsFile artifacts.ResultsFile
	if err := json.Unmarshal(resultsBytes, &resultsFile); err != nil {
		return nil, err
	}

	if len(resultsFile.Samples) != 1 {
		return resultsBytes, nil
	}

	sample := &resultsFile.Samples[0]
	if sample.Input == "" {
		sample.Input = instruction
	}
	if sample.Output == "" {
		for i := len(agentResult.Steps) - 1; i >= 0; i-- {
			if agentResult.Steps[i].Text != "" {
				sample.Output = agentResult.Steps[i].Text
				break
			}
		}
	}
	if sample.LatencySeconds == nil {
		latencySeconds := agentDuration.Seconds()
		sample.LatencySeconds = &latencySeconds
	}
	if sample.InputTokens == 0 {
		sample.InputTokens = agentResult.InputTokens
	}
	if sample.OutputTokens == 0 {
		sample.OutputTokens = agentResult.OutputTokens
	}
	if len(sample.Transcript) == 0 {
		sample.Transcript = buildAgentTranscript(instruction, agentResult.Steps)
	}

	return json.Marshal(resultsFile)
}

func buildAgentTranscript(instruction string, steps []agent.Step) []artifacts.SampleTranscriptRow {
	var transcript []artifacts.SampleTranscriptRow
	if instruction != "" {
		transcript = append(transcript, artifacts.SampleTranscriptRow{
			Kind:    "user",
			Content: instruction,
		})
	}
	for _, step := range steps {
		if step.Text != "" {
			transcript = append(transcript, artifacts.SampleTranscriptRow{
				Kind:    "assistant",
				Content: step.Text,
			})
		}
		for _, toolCall := range step.ToolCalls {
			transcript = append(transcript, artifacts.SampleTranscriptRow{
				Kind:    "tool_call",
				Content: toolCall.Command,
			})
		}
		for _, toolResult := range step.ToolResults {
			transcript = append(transcript, artifacts.SampleTranscriptRow{
				Kind:    "tool",
				Content: formatAgentToolResult(toolResult),
			})
		}
	}
	return transcript
}

func formatAgentToolResult(toolResult agent.ToolResult) string {
	if toolResult.ExitCode == 0 {
		return toolResult.Output
	}
	if toolResult.Output == "" {
		return fmt.Sprintf("exit %d", toolResult.ExitCode)
	}
	return fmt.Sprintf("exit %d\n%s", toolResult.ExitCode, toolResult.Output)
}

func (r *RunReconciler) storeAgentArtifact(
	ctx context.Context,
	logger logr.Logger,
	run *v1alpha1.Run,
	taskID string,
	agentResult *agent.Result,
) {
	data, err := json.Marshal(agentResult)
	if err != nil {
		logger.Error(err, "failed to marshal agent record")
		return
	}

	meta := artifacts.RunMetadata{
		RunName:      run.Name,
		RunNamespace: run.Namespace,
		TaskID:       taskID,
		Commit:       run.Spec.Repo.Commit,
	}

	sinkContext, sinkCancel := context.WithTimeout(context.WithoutCancel(ctx), r.artifactSinkTimeout())
	defer sinkCancel()
	if _, storeErr := artifacts.StoreTaskFileWithTimeout(sinkContext, r.artifactSink(), meta, "agent.json", data); storeErr != nil {
		logger.Error(storeErr, "failed to store agent artifact in sink")
	}
}

func (r *RunReconciler) executeSandboxTask(
	ctx context.Context,
	run *v1alpha1.Run,
	taskID string,
	sbClient sandboxclient.Client,
	targetTask v1alpha1.ManifestTask,
	effRange *v1alpha1.SampleRange,
	startTime time.Time,
) (result taskExecutionResult) {
	logger := log.FromContext(ctx).WithValues("task", taskID, "run", run.Name)

	var agentOutcome agentPhaseOutcome
	var tokensAndCost *taskTokensAndCost
	defer func() {
		if agentOutcome.loopEndingMessage != "" {
			if result.message == "" {
				result.message = agentOutcome.loopEndingMessage
			} else {
				result.message = fmt.Sprintf("%s; %s", agentOutcome.loopEndingMessage, result.message)
			}
		}
	}()

	if err := validateTaskTimeoutSeconds(targetTask); err != nil {
		logger.Error(err, "Task invalid timeoutSeconds", "task", taskID)
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     err.Error(),
			sampleRange: effRange,
		}
	}
	if err := discovery.ValidateTaskResources(targetTask); err != nil {
		logger.Error(err, "Task invalid resources", "task", taskID)
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     err.Error(),
			sampleRange: effRange,
		}
	}
	if err := discovery.ValidateTaskAgent(targetTask); err != nil {
		logger.Error(err, "Task invalid agent", "task", taskID)
		return taskExecutionResult{
			taskID:      taskID,
			phase:       v1alpha1.TaskPhaseFailed,
			message:     err.Error(),
			sampleRange: effRange,
		}
	}
	effectiveResources := &v1alpha1.TaskResources{
		Limits: discovery.TaskResourceLimits(targetTask),
	}

	// Ensure /workspace/out exists
	mkdirContext, mkdirCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	if _, mkdirErr := executeSandboxWithTimeout(mkdirContext, sbClient, sandboxclient.ProcessConfig{
		Command: []string{"mkdir", "-p", "/workspace/out"},
	}); mkdirErr != nil {
		logger.V(1).Info("mkdir /workspace/out returned error", "error", mkdirErr)
	}
	mkdirCancel()

	// Step 1: Execute task command in sandbox with working directory repo
	cwd := "repo"
	if run.Spec.Repo.URL == "" {
		cwd = "/workspace"
	}

	if targetTask.Agent != nil {
		effectiveTimeoutSeconds := r.effectiveTaskCommandTimeoutSeconds(targetTask, run.Status.ExecutionDeadline)
		var failedResult *taskExecutionResult
		agentOutcome, failedResult = r.executeAgentPhase(ctx, run, taskID, sbClient, targetTask, cwd, effectiveTimeoutSeconds, effectiveResources, effRange)
		if agentOutcome.result != nil {
			r.storeAgentArtifact(ctx, logger, run, taskID, agentOutcome.result)
		}
		tokensAndCost = r.computeAgentTokensAndCost(run.Spec.Model, agentOutcome.result)
		if failedResult != nil {
			applyTokensAndCost(failedResult, tokensAndCost)
			return *failedResult
		}
	}

	taskCommandTimeout := r.executionStepTimeout()
	if targetTask.TimeoutSeconds != nil {
		taskCommandTimeout = time.Duration(*targetTask.TimeoutSeconds) * time.Second
	}
	effectiveTimeoutSeconds := r.effectiveTaskCommandTimeoutSeconds(targetTask, run.Status.ExecutionDeadline)
	logger.Info("Starting task step 1: task command", "command", targetTask.Command, "cwd", cwd, "timeoutSeconds", effectiveTimeoutSeconds)

	commandContext, commandCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, taskCommandTimeout)
	commandResult, commandErr := executeSandboxWithTimeout(commandContext, sbClient, sandboxclient.ProcessConfig{
		Command: targetTask.Command,
		Cwd:     cwd,
		Env:     gcpProjectTaskEnv(taskGCPProject(run, taskID)),
	})
	commandCancel()
	if failedResult, failed := r.handleStepExecutionFailure(ctx, run.Name, "task command", isStepTimeout(commandContext, commandErr), commandResult, commandErr, taskID, effRange); failed {
		failedResult.timeoutSeconds = effectiveTimeoutSeconds
		failedResult.resources = effectiveResources
		applyTokensAndCost(&failedResult, tokensAndCost)
		return failedResult
	}

	return r.finalizeSandboxTaskResult(ctx, run, taskID, sbClient, effRange, startTime, effectiveTimeoutSeconds, effectiveResources, agentOutcome, tokensAndCost)
}

func (r *RunReconciler) finalizeSandboxTaskResult(ctx context.Context, run *v1alpha1.Run, taskID string, sbClient sandboxclient.Client, effRange *v1alpha1.SampleRange, startTime time.Time, effectiveTimeoutSeconds int, effectiveResources *v1alpha1.TaskResources, agentOutcome agentPhaseOutcome, tokensAndCost *taskTokensAndCost) taskExecutionResult {
	logger := log.FromContext(ctx).WithValues("task", taskID, "run", run.Name)

	// Step 2: Read /out/results.json via sandboxd ReadFile
	readContext, readCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	resultsBytes, readErr := readFileWithTimeout(readContext, sbClient, "/out/results.json")
	readCancel()
	if failedResult, failed := r.handleStepReadFailure(ctx, run.Name, "results file", readContext, readErr, taskID, effRange); failed {
		failedResult.timeoutSeconds = effectiveTimeoutSeconds
		failedResult.resources = effectiveResources
		applyTokensAndCost(&failedResult, tokensAndCost)
		return failedResult
	}

	// Step 3: Parse and validate results
	aggregates, sampleSummaries, parseErr := artifacts.ParseSandboxResults(resultsBytes)
	if parseErr != nil {
		message := fmt.Sprintf("failed to parse results file: %v", parseErr)
		logger.Error(parseErr, "Task step parse results failed", "task", taskID)
		failedResult := taskExecutionResult{
			taskID:         taskID,
			phase:          v1alpha1.TaskPhaseFailed,
			message:        message,
			sampleRange:    effRange,
			timeoutSeconds: effectiveTimeoutSeconds,
			resources:      effectiveResources,
		}
		applyTokensAndCost(&failedResult, tokensAndCost)
		return failedResult
	}
	if aggregates.Samples == 0 {
		message := "no scored samples"
		logger.Error(errors.New(message), "Task completed without scored samples", "task", taskID)
		failedResult := taskExecutionResult{
			taskID:         taskID,
			phase:          v1alpha1.TaskPhaseFailed,
			message:        message,
			sampleRange:    effRange,
			timeoutSeconds: effectiveTimeoutSeconds,
			resources:      effectiveResources,
		}
		applyTokensAndCost(&failedResult, tokensAndCost)
		return failedResult
	}

	// Step 4: Compute statistics
	successRate, successRateCI, successRateSD := computeTaskAggregateStats(aggregates)
	finishTime := r.now()
	planned := computeSamplesPlanned(effRange, run.Spec.Limit, aggregates.Samples)

	// Step 5: Store artifact
	var artifactSampleRange *artifacts.SampleRange
	if effRange != nil {
		artifactSampleRange = &artifacts.SampleRange{
			Start: effRange.Start,
			End:   effRange.End,
		}
	}
	var metaTokens *artifacts.TaskTokens
	var metaCostUSD *float64
	var metaCostSource v1alpha1.CostSource
	if tokensAndCost != nil {
		metaTokens = tokensAndCost.artifactTokens
		metaCostUSD = tokensAndCost.costUSD
		metaCostSource = tokensAndCost.costSource
	}
	meta := artifacts.RunMetadata{
		RunName:           run.Name,
		RunNamespace:      run.Namespace,
		ControllerVersion: r.controllerVersion(),
		TaskID:            taskID,
		Model:             run.Spec.Model,
		Commit:            run.Spec.Repo.Commit,
		Scorer:            "sandbox",
		SampleCount:       aggregates.Samples,
		Samples:           sampleSummaries,
		Successes:         aggregates.Successes,
		MeanScore:         aggregates.MeanScore,
		SuccessRate:       successRate,
		SuccessRateCI:     successRateCI,
		SuccessRateSD:     successRateSD,
		StartedAt:         &startTime,
		FinishedAt:        &finishTime,
		SampleRange:       artifactSampleRange,
		Tokens:            metaTokens,
		CostUSD:           metaCostUSD,
		CostSource:        metaCostSource,
		PossibleSamples:   aggregates.Samples,
	}
	resultsToStore := resultsBytes
	if agentOutcome.result != nil {
		enrichedResults, enrichErr := enrichResultsWithAgent(resultsBytes, agentOutcome.instruction, agentOutcome.duration, agentOutcome.result)
		if enrichErr != nil {
			logger.Error(enrichErr, "Failed to enrich results with agent trajectory, storing original results", "task", taskID)
		} else {
			resultsToStore = enrichedResults
		}
	}
	sinkContext, sinkCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.artifactSinkTimeout())
	storedArtifact, storeErr := artifacts.StoreArtifactWithTimeout(sinkContext, r.artifactSink(), meta, "results.json", resultsToStore)
	sinkCancel()
	if storeErr != nil {
		message := fmt.Sprintf("failed to store artifact: %v", storeErr)
		logger.Error(storeErr, "Failed to store artifact in sink", "task", taskID)
		failedResult := taskExecutionResult{
			taskID:         taskID,
			phase:          v1alpha1.TaskPhaseFailed,
			message:        message,
			sampleRange:    effRange,
			timeoutSeconds: effectiveTimeoutSeconds,
			resources:      effectiveResources,
		}
		applyTokensAndCost(&failedResult, tokensAndCost)
		return failedResult
	}
	logPath := storedArtifact.Location
	logHash := storedArtifact.Hash

	completedResult := taskExecutionResult{
		taskID:          taskID,
		phase:           v1alpha1.TaskPhaseCompleted,
		timeoutSeconds:  effectiveTimeoutSeconds,
		resources:       effectiveResources,
		samples:         aggregates.Samples,
		successes:       aggregates.Successes,
		meanScore:       aggregates.MeanScore,
		successRate:     successRate,
		successRateCI:   successRateCI,
		successRateSD:   successRateSD,
		possibleSamples: aggregates.Samples,
		samplesDone:     aggregates.Samples,
		samplesPlanned:  planned,
		logPath:         logPath,
		logHash:         logHash,
		sampleRange:     effRange,
	}
	applyTokensAndCost(&completedResult, tokensAndCost)
	return completedResult
}

func isCommandOnlySandboxTask(task *v1alpha1.ManifestTask) bool {
	return task != nil && isSandboxTask(*task) && len(task.Command) > 0 && task.Agent == nil
}

func asyncTaskExecutionID(runName string, task v1alpha1.TaskStatus) string {
	return fmt.Sprintf("%s-attempt-%d", discovery.TaskSandboxNameForRun(runName, task.ID), task.Restarts)
}

func (r *RunReconciler) newTaskSandboxClient(ctx context.Context, run *v1alpha1.Run, taskID string, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) (sandboxclient.Client, error) {
	authToken, err := r.ensureTaskAuthToken(ctx, run, taskID, types.NamespacedName{Namespace: sandbox.Namespace, Name: sandbox.Name})
	if err != nil {
		return nil, fmt.Errorf("failed to get task auth token: %w", err)
	}
	if r.SandboxClient != nil {
		return r.SandboxClient, nil
	}
	var options []sandboxclient.Option
	if authToken != "" {
		options = append(options, sandboxclient.WithAuthToken(authToken))
	}
	if tlsConfig := r.pki().ClientTLSConfig(sandbox.Name, sandbox.Namespace); tlsConfig != nil {
		options = append(options, sandboxclient.WithTLSConfig(tlsConfig))
	}
	factory := r.ClientFactory
	if factory == nil {
		factory = sandboxclient.DefaultFactory()
	}
	sandboxClient, err := factory.NewClient(pod.Status.PodIP, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create sandbox client: %w", err)
	}
	return sandboxClient, nil
}

func (r *RunReconciler) reconcileAsyncSandboxTask(ctx context.Context, run *v1alpha1.Run, task v1alpha1.TaskStatus, targetTask v1alpha1.ManifestTask, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod, maxMessageBytes int) (*runningTaskDecision, *taskExecutionResult) {
	logger := log.FromContext(ctx).WithValues("task", task.ID, "run", run.Name)
	effectiveRange := EffectiveSampleRange(run.Spec)
	cohort := targetTask.Cohort
	if cohort == "" {
		cohort = v1alpha1.DefaultCohort
	}
	effectiveTimeoutSeconds := task.TimeoutSeconds
	if effectiveTimeoutSeconds <= 0 {
		effectiveTimeoutSeconds = r.effectiveTaskCommandTimeoutSeconds(targetTask, run.Status.ExecutionDeadline)
	}
	var effectiveResources *v1alpha1.TaskResources

	finishResult := func(result taskExecutionResult) (*runningTaskDecision, *taskExecutionResult) {
		result.taskID, result.cohort = task.ID, cohort
		if result.timeoutSeconds == 0 {
			result.timeoutSeconds = effectiveTimeoutSeconds
		}
		if result.resources == nil {
			result.resources = effectiveResources
		}
		if result.sampleRange == nil {
			result.sampleRange = effectiveRange
		}
		if result.finishedAt == nil {
			finishedAt := metav1.NewTime(r.now())
			result.finishedAt = &finishedAt
		}
		if result.phase == v1alpha1.TaskPhaseFailed && r.isExecutionDeadlineExceeded(run.Status.ExecutionDeadline) {
			result.cutOff = true
		}
		r.unmarkServicePortalPodBound(pod.UID)
		return nil, &result
	}
	infraFailure := func(reason string) (*runningTaskDecision, *taskExecutionResult) {
		decision := decideInfrastructureFailure(task, reason, sandbox, pod, true, maxMessageBytes)
		return &decision, nil
	}
	handleRPCError := func(reason string, rpcError error) (*runningTaskDecision, *taskExecutionResult) {
		if r.isExecutionDeadlineExceeded(run.Status.ExecutionDeadline) {
			return finishResult(taskExecutionResult{phase: v1alpha1.TaskPhaseFailed, cutOff: true})
		}
		if status.Code(rpcError) == codes.Unauthenticated {
			return finishResult(taskExecutionResult{phase: v1alpha1.TaskPhaseFailed, message: fmt.Sprintf("step task command failed: unauthenticated: %v", rpcError)})
		}
		logger.Error(rpcError, reason)
		return infraFailure(fmt.Sprintf("%s: %v", reason, rpcError))
	}

	if task.ExecutionID == "" {
		if err := validateTaskTimeoutSeconds(targetTask); err != nil {
			return finishResult(taskExecutionResult{phase: v1alpha1.TaskPhaseFailed, message: err.Error()})
		}
		if err := discovery.ValidateTaskResources(targetTask); err != nil {
			return finishResult(taskExecutionResult{phase: v1alpha1.TaskPhaseFailed, message: err.Error()})
		}
	}
	effectiveResources = &v1alpha1.TaskResources{Limits: discovery.TaskResourceLimits(targetTask)}

	if task.ExecutionID != "" {
		if r.isExecutionDeadlineExceeded(run.Status.ExecutionDeadline) {
			return finishResult(taskExecutionResult{phase: v1alpha1.TaskPhaseFailed, cutOff: true})
		}
		taskTimeout := r.executionStepTimeout()
		if task.TimeoutSeconds > 0 {
			taskTimeout = time.Duration(task.TimeoutSeconds) * time.Second
		} else if targetTask.TimeoutSeconds != nil {
			taskTimeout = time.Duration(*targetTask.TimeoutSeconds) * time.Second
		}
		if task.StartedAt != nil && !r.now().Before(task.StartedAt.Time.Add(taskTimeout)) {
			return finishResult(taskExecutionResult{phase: v1alpha1.TaskPhaseFailed, message: "step task command failed: sandbox did not report completion"})
		}
	}

	clientContext, clientCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	sandboxClient, err := r.newTaskSandboxClient(clientContext, run, task.ID, sandbox, pod)
	clientCancel()
	if err != nil {
		return handleRPCError("failed to connect to task sandbox", err)
	}
	defer func() {
		if closeErr := sandboxClient.Close(); closeErr != nil {
			logger.Error(closeErr, "failed to close sandbox client")
		}
	}()

	if task.ExecutionID == "" {
		mkdirContext, mkdirCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
		if _, mkdirErr := executeSandboxWithTimeout(mkdirContext, sandboxClient, sandboxclient.ProcessConfig{Command: []string{"mkdir", "-p", "/workspace/out"}}); mkdirErr != nil {
			logger.V(1).Info("mkdir /workspace/out returned error", "error", mkdirErr)
		}
		mkdirCancel()

		cwd := "repo"
		if run.Spec.Repo.URL == "" {
			cwd = "/workspace"
		}
		executionID := asyncTaskExecutionID(run.Name, task)
		startContext, startCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
		startErr := sandboxClient.StartAsync(startContext, executionID, sandboxclient.ProcessConfig{
			Command: targetTask.Command,
			Cwd:     cwd,
			Env:     gcpProjectTaskEnv(task.GCPProject),
		})
		startCancel()
		if startErr != nil && !errors.Is(startErr, sandboxclient.ErrAsyncTaskAlreadyExists) {
			return handleRPCError(fmt.Sprintf("failed to start async task %q", executionID), startErr)
		}
		logger.Info("Started async task command", "executionID", executionID, "command", targetTask.Command, "cwd", cwd, "timeoutSeconds", effectiveTimeoutSeconds)
		return &runningTaskDecision{action: runningTaskActionRecordExecution, executionID: executionID, sandbox: sandbox, pod: pod}, nil
	}

	pollContext, pollCancel := r.stepContext(ctx, run.Status.ExecutionDeadline, r.executionStepTimeout())
	pollStatus, pollErr := sandboxClient.PollAsync(pollContext, task.ExecutionID)
	pollCancel()
	if pollErr != nil {
		return handleRPCError(fmt.Sprintf("failed to poll async task %q", task.ExecutionID), pollErr)
	}
	if pollStatus == nil {
		return infraFailure(fmt.Sprintf("empty poll status for async task %q", task.ExecutionID))
	}

	switch pollStatus.State {
	case sandboxclient.AsyncTaskRunning:
		return nil, nil
	case sandboxclient.AsyncTaskCompleted:
		if pollStatus.ExitCode != 0 {
			execResult := &sandboxclient.ExecuteResult{ExitCode: pollStatus.ExitCode, Stdout: pollStatus.Stdout, Stderr: pollStatus.Stderr}
			failedResult, _ := r.handleStepExecutionFailure(ctx, run.Name, "task command", false, execResult, nil, task.ID, effectiveRange)
			return finishResult(failedResult)
		}
		startTime := r.now()
		if task.StartedAt != nil {
			startTime = task.StartedAt.Time
		}
		return finishResult(r.finalizeSandboxTaskResult(ctx, run, task.ID, sandboxClient, effectiveRange, startTime, effectiveTimeoutSeconds, effectiveResources, agentPhaseOutcome{}, nil))
	case sandboxclient.AsyncTaskNotFound:
		return infraFailure(fmt.Sprintf("async task %q not found in sandbox", task.ExecutionID))
	default:
		return infraFailure(fmt.Sprintf("unexpected async task state %q for %q", pollStatus.State, task.ExecutionID))
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

// isUsableSandboxPod checks that a pod is not terminating and is controlled by the given sandbox UID.
func isUsableSandboxPod(pod *corev1.Pod, sandboxUID types.UID) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	controllerRef := metav1.GetControllerOf(pod)
	if sandboxUID != "" {
		return controllerRef != nil && controllerRef.UID == sandboxUID
	}
	return controllerRef == nil
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

func taskPodFailureMessage(pod *corev1.Pod) string {
	if pod == nil {
		return "task pod failed"
	}
	if initFailureMessage := taskPodInitFailureMessage(pod); initFailureMessage != "" {
		return initFailureMessage
	}
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode != 0 {
			if message := strings.TrimSpace(containerStatus.State.Terminated.Message); message != "" {
				return message
			}
			return fmt.Sprintf("container %s failed with exit code %d", containerStatus.Name, containerStatus.State.Terminated.ExitCode)
		}
	}
	return "task pod failed"
}

func taskPodInitFailureMessage(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	for _, containerStatus := range pod.Status.InitContainerStatuses {
		if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode != 0 {
			if message := strings.TrimSpace(containerStatus.State.Terminated.Message); message != "" {
				return message
			}
			return fmt.Sprintf("container %s failed with exit code %d", containerStatus.Name, containerStatus.State.Terminated.ExitCode)
		}
	}
	return ""
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
		} else if t.Phase == v1alpha1.TaskPhaseSkipped {
			parts = append(parts, fmt.Sprintf("%s=skipped", t.ID))
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

func truncateHostList(hosts []string, limit int) []string {
	if len(hosts) <= limit {
		return hosts
	}
	res := append([]string(nil), hosts[:limit]...)
	return append(res, fmt.Sprintf("... (%d total)", len(hosts)))
}
