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
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/github"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	return scheme
}

func computeSignature(payload, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func pollUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %v", timeout)
	}
}

func TestSignatureVerification(t *testing.T) {
	secret := []byte("super-secret-key-12345")
	payload := []byte(`{"action":"opened","number":42}`)

	validSig := computeSignature(payload, secret)

	tests := []struct {
		name      string
		payload   []byte
		secret    []byte
		sigHeader string
		expected  bool
	}{
		{
			name:      "valid signature",
			payload:   payload,
			secret:    secret,
			sigHeader: validSig,
			expected:  true,
		},
		{
			name:      "tampered signature",
			payload:   payload,
			secret:    secret,
			sigHeader: validSig + "bad",
			expected:  false,
		},
		{
			name:      "tampered body",
			payload:   []byte(`{"action":"opened","number":43}`),
			secret:    secret,
			sigHeader: validSig,
			expected:  false,
		},
		{
			name:      "wrong secret",
			payload:   payload,
			secret:    []byte("different-secret"),
			sigHeader: validSig,
			expected:  false,
		},
		{
			name:      "missing signature header",
			payload:   payload,
			secret:    secret,
			sigHeader: "",
			expected:  false,
		},
		{
			name:      "empty secret",
			payload:   payload,
			secret:    nil,
			sigHeader: validSig,
			expected:  false,
		},
		{
			name:      "invalid header prefix (not sha256=)",
			payload:   payload,
			secret:    secret,
			sigHeader: "sha1=" + hex.EncodeToString(secret),
			expected:  false,
		},
		{
			name:      "malformed hex digest",
			payload:   payload,
			secret:    secret,
			sigHeader: "sha256=not-valid-hex-zzz",
			expected:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := VerifySignature(tc.payload, tc.secret, tc.sigHeader)
			if actual != tc.expected {
				t.Errorf("VerifySignature() = %v, expected %v", actual, tc.expected)
			}
		})
	}
}

func TestSanitizeLabelValue(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"gke-labs/eval-runner", "gke-labs_eval-runner"},
		{"eval-runner", "eval-runner"},
		{"--test..value--", "test..value"},
		{"org/sub/repo", "org_sub_repo"},
		{"", ""},
		{"a" + string(bytes.Repeat([]byte("x"), 100)) + "z", "a" + string(bytes.Repeat([]byte("x"), 61)) + "x"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			actual := SanitizeLabelValue(tc.input)
			if actual != tc.expected {
				t.Errorf("SanitizeLabelValue(%q) = %q, expected %q", tc.input, actual, tc.expected)
			}
		})
	}
}

func TestValidationHelpers(t *testing.T) {
	// 1. Repo component validation
	if err := ValidateRepoComponent("gke-labs"); err != nil {
		t.Errorf("expected gke-labs to be valid: %v", err)
	}
	if err := ValidateRepoComponent("eval-runner.git?x=../../evil#"); err == nil {
		t.Errorf("expected evil component to be rejected")
	}

	// 2. Commit SHA validation
	if err := ValidateCommitSHA("1234567"); err != nil {
		t.Errorf("expected 7-char sha to be valid: %v", err)
	}
	if err := ValidateCommitSHA("1234567890abcdef1234567890abcdef12345678"); err != nil {
		t.Errorf("expected 40-char sha to be valid: %v", err)
	}
	if err := ValidateCommitSHA("not-a-hex-sha;"); err == nil {
		t.Errorf("expected non-hex sha to be rejected")
	}
	if err := ValidateCommitSHA("123456"); err == nil {
		t.Errorf("expected <7 char sha to be rejected")
	}
}

func TestRepositoryAllowList(t *testing.T) {
	allowList := []string{"gke-labs/eval-runner", "org/repo"}

	if !IsRepositoryAllowed("gke-labs/eval-runner", allowList) {
		t.Errorf("expected gke-labs/eval-runner to be allowed")
	}
	if !IsRepositoryAllowed("GKE-LABS/EVAL-RUNNER", allowList) {
		t.Errorf("expected case-insensitive match to be allowed")
	}
	if !IsRepositoryAllowed("org/repo", allowList) {
		t.Errorf("expected org/repo to be allowed")
	}
	if IsRepositoryAllowed("untrusted/repo", allowList) {
		t.Errorf("expected untrusted/repo to be denied")
	}

	// Fail-closed: empty allow-list denies all
	if IsRepositoryAllowed("anyone/anyrepo", nil) {
		t.Errorf("nil allow-list should deny all (fail-closed)")
	}
	if IsRepositoryAllowed("anyone/anyrepo", []string{}) {
		t.Errorf("empty slice allow-list should deny all (fail-closed)")
	}

	// Wildcard '*' allows any repository
	if !IsRepositoryAllowed("anyone/anyrepo", []string{"*"}) {
		t.Errorf("wildcard allow-list should allow any repository")
	}
	if !IsRepositoryAllowed("anyone/anyrepo", []string{" * "}) {
		t.Errorf("wildcard with spaces should allow any repository")
	}
	if !IsRepositoryAllowed("org/repo", []string{"other/repo", "*"}) {
		t.Errorf("allow-list with wildcard should allow repository")
	}
	if IsRepositoryAllowed("", []string{"*"}) {
		t.Errorf("empty repo string should not be allowed")
	}
}

func TestConfigPrecedence(t *testing.T) {
	limit10 := 10
	limit20 := 20

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "eval-runner-config",
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeyGitHubWebhookSecret: "custom-webhook-secret",
			ConfigMapKeyDefaultModel:        "google/gemini-flash",
			ConfigMapKeyDefaultLimit:        "10",
			ConfigMapKeyWebhookRepositories: "gke-labs/eval-runner,foo/bar",
		},
	}

	// 1. ConfigMap applied
	cfg, err := ResolveWebhookConfig(cm, RawWebhookFlags{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GitHubWebhookSecret != "custom-webhook-secret" {
		t.Errorf("expected GitHubWebhookSecret %q, got %q", "custom-webhook-secret", cfg.GitHubWebhookSecret)
	}
	if cfg.DefaultModel != "google/gemini-flash" {
		t.Errorf("expected DefaultModel %q, got %q", "google/gemini-flash", cfg.DefaultModel)
	}
	if cfg.DefaultLimit == nil || *cfg.DefaultLimit != 10 {
		t.Errorf("expected DefaultLimit 10, got %v", cfg.DefaultLimit)
	}
	if len(cfg.WebhookRepositories) != 2 || cfg.WebhookRepositories[0] != "gke-labs/eval-runner" {
		t.Errorf("expected WebhookRepositories %v", cfg.WebhookRepositories)
	}

	// 2. Explicit flag overrides ConfigMap
	flags := RawWebhookFlags{
		DefaultModel: "mockllm/model",
		DefaultLimit: &limit20,
		Explicit: map[string]bool{
			"default-model": true,
			"default-limit": true,
		},
	}
	cfgWithFlags, err := ResolveWebhookConfig(cm, flags, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgWithFlags.DefaultModel != "mockllm/model" {
		t.Errorf("expected explicit flag to override DefaultModel, got %q", cfgWithFlags.DefaultModel)
	}
	if cfgWithFlags.DefaultLimit == nil || *cfgWithFlags.DefaultLimit != limit20 {
		t.Errorf("expected explicit flag to override DefaultLimit, got %v", cfgWithFlags.DefaultLimit)
	}

	// 3. Malformed limit in ConfigMap returns error
	badCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "eval-runner-config", Namespace: "eval-runner"},
		Data: map[string]string{
			ConfigMapKeyDefaultLimit: "not-a-number",
		},
	}
	_, err = ResolveWebhookConfig(badCM, RawWebhookFlags{}, nil)
	if err == nil {
		t.Errorf("expected error for malformed defaultLimit")
	}
	_ = limit10
}

func TestResolveWebhookConfig_EmptyAndSentinelSemantics(t *testing.T) {
	defaults := DefaultConfig()

	t.Run("empty string in ConfigMap retains default model", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "eval-runner-config",
				Namespace: "eval-runner",
			},
			Data: map[string]string{
				ConfigMapKeyDefaultModel:        "",
				ConfigMapKeyGitHubWebhookSecret: "",
				ConfigMapKeyDefaultLimit:        "",
			},
		}
		cfg, err := ResolveWebhookConfig(cm, RawWebhookFlags{}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.DefaultModel != defaults.DefaultModel {
			t.Errorf("expected DefaultModel to retain default %q, got %q", defaults.DefaultModel, cfg.DefaultModel)
		}
		if cfg.GitHubWebhookSecret != defaults.GitHubWebhookSecret {
			t.Errorf("expected GitHubWebhookSecret to retain default %q, got %q", defaults.GitHubWebhookSecret, cfg.GitHubWebhookSecret)
		}
		if cfg.DefaultLimit != defaults.DefaultLimit {
			t.Errorf("expected DefaultLimit to retain default %v, got %v", defaults.DefaultLimit, cfg.DefaultLimit)
		}
	})

	t.Run("whitespace string in ConfigMap retains default model", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "eval-runner-config",
				Namespace: "eval-runner",
			},
			Data: map[string]string{
				ConfigMapKeyDefaultModel:        "   ",
				ConfigMapKeyGitHubWebhookSecret: "\t ",
				ConfigMapKeyDefaultLimit:        "  ",
			},
		}
		cfg, err := ResolveWebhookConfig(cm, RawWebhookFlags{}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.DefaultModel != defaults.DefaultModel {
			t.Errorf("expected whitespace to retain default DefaultModel %q, got %q", defaults.DefaultModel, cfg.DefaultModel)
		}
		if cfg.GitHubWebhookSecret != defaults.GitHubWebhookSecret {
			t.Errorf("expected whitespace to retain default GitHubWebhookSecret %q, got %q", defaults.GitHubWebhookSecret, cfg.GitHubWebhookSecret)
		}
	})

	t.Run("sentinel none in ConfigMap disables default model", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "eval-runner-config",
				Namespace: "eval-runner",
			},
			Data: map[string]string{
				ConfigMapKeyDefaultModel: "none",
			},
		}
		cfg, err := ResolveWebhookConfig(cm, RawWebhookFlags{}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.DefaultModel != "" {
			t.Errorf("expected DefaultModel disabled (empty), got %q", cfg.DefaultModel)
		}

		// Case-insensitive NONE
		cmUpper := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "eval-runner-config",
				Namespace: "eval-runner",
			},
			Data: map[string]string{
				ConfigMapKeyDefaultModel: "NONE",
			},
		}
		cfgUpper, err := ResolveWebhookConfig(cmUpper, RawWebhookFlags{}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfgUpper.DefaultModel != "" {
			t.Errorf("expected DefaultModel disabled (empty) with NONE, got %q", cfgUpper.DefaultModel)
		}
	})

	t.Run("explicit empty flag disables default model even when ConfigMap is set", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "eval-runner-config",
				Namespace: "eval-runner",
			},
			Data: map[string]string{
				ConfigMapKeyDefaultModel: "custom-model",
			},
		}
		flags := RawWebhookFlags{
			DefaultModel: "",
			Explicit: map[string]bool{
				"default-model": true,
			},
		}
		cfg, err := ResolveWebhookConfig(cm, flags, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.DefaultModel != "" {
			t.Errorf("expected explicit empty flag to override ConfigMap, got %q", cfg.DefaultModel)
		}
	})

	t.Run("explicit none flag disables default model even when ConfigMap is set", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "eval-runner-config",
				Namespace: "eval-runner",
			},
			Data: map[string]string{
				ConfigMapKeyDefaultModel: "custom-model",
			},
		}
		flags := RawWebhookFlags{
			DefaultModel: "none",
			Explicit: map[string]bool{
				"default-model": true,
			},
		}
		cfg, err := ResolveWebhookConfig(cm, flags, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.DefaultModel != "" {
			t.Errorf("expected explicit none flag to override ConfigMap, got %q", cfg.DefaultModel)
		}
	})
}

func TestProcessPullRequest_CreateAndIdempotency(t *testing.T) {
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	cfg := Config{
		Namespace:                "eval-runner",
		DefaultModel:             "mockllm/model",
		DefaultBatchSize:         8,
		DefaultMaxSamplesPerTask: 64,
		Client:                   cl,
	}
	worker := NewWorkerPool(cfg)

	item := &PullRequestWorkItem{
		PRNumber:          105,
		HeadSHA:           "1234567890abcdef1234567890abcdef12345678",
		BaseSHA:           "abcdef1234567890abcdef1234567890abcdef12",
		CandidateCloneURL: "https://github.com/contributor-fork/eval-runner.git",
		CandidateRepo:     "contributor-fork/eval-runner",
		BaselineCloneURL:  "https://github.com/gke-labs/eval-runner.git",
		BaselineRepo:      "gke-labs/eval-runner",
		TargetRepo:        "gke-labs/eval-runner",
	}

	ctx := t.Context()

	// 1. Process pull_request
	if err := worker.ProcessPullRequest(ctx, item); err != nil {
		t.Fatalf("ProcessPullRequest failed: %v", err)
	}

	// Verify Candidate Run has fork clone URL (Blocking 1)
	candRunName := "pr-105-1234567"
	var candRun v1alpha1.Run
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: candRunName}, &candRun); err != nil {
		t.Fatalf("candidate Run %s not found: %v", candRunName, err)
	}
	if candRun.Spec.Repo.URL != "https://github.com/contributor-fork/eval-runner.git" {
		t.Errorf("expected fork candidate clone URL, got %s", candRun.Spec.Repo.URL)
	}
	if candRun.Spec.Repo.Commit != item.HeadSHA {
		t.Errorf("expected commit %s, got %s", item.HeadSHA, candRun.Spec.Repo.Commit)
	}
	if candRun.Spec.Model != "mockllm/model" {
		t.Errorf("expected model mockllm/model, got %s", candRun.Spec.Model)
	}
	if candRun.Spec.SampleRange == nil || candRun.Spec.SampleRange.Start != 1 || candRun.Spec.SampleRange.End != 8 {
		t.Errorf("expected candidate sampleRange {1, 8}, got %+v", candRun.Spec.SampleRange)
	}
	if candRun.Spec.Limit != nil {
		t.Errorf("expected candidate limit nil, got %v", candRun.Spec.Limit)
	}
	if candRun.Labels[LabelPR] != "105" {
		t.Errorf("expected label pr=105, got %s", candRun.Labels[LabelPR])
	}
	if candRun.Labels[LabelRepo] != "contributor-fork_eval-runner" {
		t.Errorf("expected sanitized repo label, got %s", candRun.Labels[LabelRepo])
	}
	if candRun.Labels[LabelHeadSHA] != item.HeadSHA {
		t.Errorf("expected head-sha label, got %s", candRun.Labels[LabelHeadSHA])
	}

	// Verify Baseline Run has base clone URL
	baseRunName := "base-abcdef1"
	var baseRun v1alpha1.Run
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: baseRunName}, &baseRun); err != nil {
		t.Fatalf("baseline Run %s not found: %v", baseRunName, err)
	}
	if baseRun.Spec.Repo.URL != "https://github.com/gke-labs/eval-runner.git" {
		t.Errorf("expected base clone URL, got %s", baseRun.Spec.Repo.URL)
	}
	if baseRun.Spec.Repo.Commit != item.BaseSHA {
		t.Errorf("expected commit %s, got %s", item.BaseSHA, baseRun.Spec.Repo.Commit)
	}
	if baseRun.Spec.SampleRange == nil || baseRun.Spec.SampleRange.Start != 1 || baseRun.Spec.SampleRange.End != 8 {
		t.Errorf("expected baseline sampleRange {1, 8}, got %+v", baseRun.Spec.SampleRange)
	}
	if baseRun.Spec.Limit != nil {
		t.Errorf("expected baseline limit nil, got %v", baseRun.Spec.Limit)
	}

	// Verify Comparison
	compName := "pr-105"
	var comp v1alpha1.Comparison
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: compName}, &comp); err != nil {
		t.Fatalf("Comparison %s not found: %v", compName, err)
	}
	if comp.Spec.Sampling == nil || comp.Spec.Sampling.BatchSize != 8 || comp.Spec.Sampling.MaxSamplesPerTask != 64 {
		t.Errorf("expected Comparison sampling {8, 64}, got %+v", comp.Spec.Sampling)
	}
	if comp.Spec.Candidate.Run != candRunName {
		t.Errorf("expected Candidate.Run %s, got %s", candRunName, comp.Spec.Candidate.Run)
	}
	if comp.Spec.Baseline.Run != baseRunName {
		t.Errorf("expected Baseline.Run %s, got %s", baseRunName, comp.Spec.Baseline.Run)
	}
	if comp.Spec.PullRequest == nil || comp.Spec.PullRequest.Number != 105 {
		t.Errorf("expected PullRequest.Number 105, got %v", comp.Spec.PullRequest)
	}
	if comp.Spec.PullRequest.Repository != "gke-labs/eval-runner" {
		t.Errorf("expected TargetRepo gke-labs/eval-runner, got %s", comp.Spec.PullRequest.Repository)
	}
	if comp.Spec.PullRequest.HeadSHA != item.HeadSHA {
		t.Errorf("expected HeadSHA %s, got %s", item.HeadSHA, comp.Spec.PullRequest.HeadSHA)
	}
	if comp.Labels[LabelPR] != "105" {
		t.Errorf("expected label pr=105, got %s", comp.Labels[LabelPR])
	}

	// 2. Redelivery of the exact same event: must be idempotent and succeed
	if err := worker.ProcessPullRequest(ctx, item); err != nil {
		t.Fatalf("Redelivery ProcessPullRequest failed: %v", err)
	}

	var runList v1alpha1.RunList
	if err := cl.List(ctx, &runList, client.InNamespace("eval-runner")); err != nil {
		t.Fatalf("failed to list runs: %v", err)
	}
	if len(runList.Items) != 2 {
		t.Errorf("expected exactly 2 Runs after redelivery, got %d", len(runList.Items))
	}
}

func TestProcessPullRequest_Synchronize(t *testing.T) {
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	cfg := Config{
		Namespace:    "eval-runner",
		DefaultModel: "mockllm/model",
		Client:       cl,
	}
	worker := NewWorkerPool(cfg)
	ctx := t.Context()

	// Initial opened event
	initialItem := &PullRequestWorkItem{
		PRNumber:          105,
		HeadSHA:           "1111111111111111111111111111111111111111",
		BaseSHA:           "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		CandidateCloneURL: "https://github.com/codebot-robot/eval-runner.git",
		CandidateRepo:     "codebot-robot/eval-runner",
		BaselineCloneURL:  "https://github.com/gke-labs/eval-runner.git",
		BaselineRepo:      "gke-labs/eval-runner",
		TargetRepo:        "gke-labs/eval-runner",
	}
	if err := worker.ProcessPullRequest(ctx, initialItem); err != nil {
		t.Fatalf("initial event failed: %v", err)
	}

	// New synchronize event with new head commit
	syncItem := &PullRequestWorkItem{
		PRNumber:          105,
		HeadSHA:           "2222222222222222222222222222222222222222",
		BaseSHA:           "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		CandidateCloneURL: "https://github.com/codebot-robot/eval-runner.git",
		CandidateRepo:     "codebot-robot/eval-runner",
		BaselineCloneURL:  "https://github.com/gke-labs/eval-runner.git",
		BaselineRepo:      "gke-labs/eval-runner",
		TargetRepo:        "gke-labs/eval-runner",
	}
	if err := worker.ProcessPullRequest(ctx, syncItem); err != nil {
		t.Fatalf("synchronize event failed: %v", err)
	}

	// Verify new candidate Run created
	var newCandRun v1alpha1.Run
	newCandName := "pr-105-2222222"
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: newCandName}, &newCandRun); err != nil {
		t.Fatalf("new candidate Run %s not found: %v", newCandName, err)
	}

	// Verify old candidate Run is kept (F6)
	var oldCandRun v1alpha1.Run
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "pr-105-1111111"}, &oldCandRun); err != nil {
		t.Fatalf("expected old candidate Run to be kept: %v", err)
	}

	// Verify Comparison updated to point to new candidate Run
	var comp v1alpha1.Comparison
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "pr-105"}, &comp); err != nil {
		t.Fatalf("Comparison pr-105 not found: %v", err)
	}
	if comp.Spec.Candidate.Run != newCandName {
		t.Errorf("expected Comparison candidate to point to %s, got %s", newCandName, comp.Spec.Candidate.Run)
	}
	if comp.Spec.PullRequest.HeadSHA != "2222222222222222222222222222222222222222" {
		t.Errorf("expected Comparison HeadSHA to update to %s, got %s", "2222222222222222222222222222222222222222", comp.Spec.PullRequest.HeadSHA)
	}
	if comp.Labels[LabelHeadSHA] != "2222222222222222222222222222222222222222" {
		t.Errorf("expected Comparison label %s to update", LabelHeadSHA)
	}
}

func TestProcessPullRequest_BaselineCacheReuse(t *testing.T) {
	scheme := newTestScheme()

	baseSHA := "abcdef1234567890abcdef1234567890abcdef12"
	baseCloneURL := "https://github.com/gke-labs/eval-runner.git"

	// Pre-create an already Completed Run for basecommit with matching repo URL and model
	preExistingCompletedRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-abcdef1",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelHeadSHA: baseSHA,
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    baseCloneURL,
				Commit: baseSHA,
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(preExistingCompletedRun).Build()

	cfg := Config{
		Namespace:    "eval-runner",
		DefaultModel: "mockllm/model",
		Client:       cl,
	}
	worker := NewWorkerPool(cfg)
	ctx := t.Context()

	item := &PullRequestWorkItem{
		PRNumber:          200,
		HeadSHA:           "1111111111111111111111111111111111111111",
		BaseSHA:           baseSHA,
		CandidateCloneURL: "https://github.com/contributor/eval-runner.git",
		CandidateRepo:     "contributor/eval-runner",
		BaselineCloneURL:  baseCloneURL,
		BaselineRepo:      "gke-labs/eval-runner",
		TargetRepo:        "gke-labs/eval-runner",
	}

	if err := worker.ProcessPullRequest(ctx, item); err != nil {
		t.Fatalf("ProcessPullRequest failed: %v", err)
	}

	// Verify Comparison points to preExistingCompletedRun
	var comp v1alpha1.Comparison
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "pr-200"}, &comp); err != nil {
		t.Fatalf("Comparison pr-200 not found: %v", err)
	}
	if comp.Spec.Baseline.Run != "base-abcdef1" {
		t.Errorf("expected Baseline.Run to reuse 'base-abcdef1', got %s", comp.Spec.Baseline.Run)
	}

	// Verify different URL is NOT reused (F1)
	differentURLItem := &PullRequestWorkItem{
		PRNumber:          201,
		HeadSHA:           "3333333333333333333333333333333333333333",
		BaseSHA:           baseSHA,
		CandidateCloneURL: "https://github.com/other/other-repo.git",
		CandidateRepo:     "other/other-repo",
		BaselineCloneURL:  "https://github.com/other/other-repo.git",
		BaselineRepo:      "other/other-repo",
		TargetRepo:        "other/other-repo",
	}
	found, err := worker.findCompletedBaselineRun(ctx, "eval-runner", baseSHA, "https://github.com/other/other-repo.git", "mockllm/model")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found != "" {
		t.Errorf("expected different URL to NOT match baseline cache, got %q", found)
	}
	_ = differentURLItem
}

func TestProcessPullRequest_ExistingRunSpecNotRewritten(t *testing.T) {
	scheme := newTestScheme()

	// Pre-create a candidate Run with custom model
	existingRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-105-1234567",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR: "105",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/contributor/eval-runner.git",
				Commit: "1234567890abcdef1234567890abcdef12345678",
			},
			Model: "original-model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingRun).Build()

	// New webhook config with different defaultModel
	cfg := Config{
		Namespace:    "eval-runner",
		DefaultModel: "different-default-model",
		Client:       cl,
	}
	worker := NewWorkerPool(cfg)
	ctx := t.Context()

	item := &PullRequestWorkItem{
		PRNumber:          105,
		HeadSHA:           "1234567890abcdef1234567890abcdef12345678",
		BaseSHA:           "abcdef1234567890abcdef1234567890abcdef12",
		CandidateCloneURL: "https://github.com/contributor/eval-runner.git",
		CandidateRepo:     "contributor/eval-runner",
		BaselineCloneURL:  "https://github.com/gke-labs/eval-runner.git",
		BaselineRepo:      "gke-labs/eval-runner",
		TargetRepo:        "gke-labs/eval-runner",
	}

	if err := worker.ProcessPullRequest(ctx, item); err != nil {
		t.Fatalf("ProcessPullRequest failed: %v", err)
	}

	// Verify existing Run spec was NOT rewritten (F2)
	var candRun v1alpha1.Run
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "pr-105-1234567"}, &candRun); err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if candRun.Spec.Model != "original-model" {
		t.Errorf("expected spec.model to remain 'original-model', got %q", candRun.Spec.Model)
	}
	// Verify labels were reconciled
	if candRun.Labels[LabelHeadSHA] != item.HeadSHA {
		t.Errorf("expected label head-sha to be reconciled")
	}
}

func TestProcessPush_DefaultBranch(t *testing.T) {
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	cfg := Config{
		Namespace:    "eval-runner",
		DefaultModel: "google/gemini-flash",
		Client:       cl,
	}
	worker := NewWorkerPool(cfg)
	ctx := t.Context()

	item := &PushWorkItem{
		CommitSHA: "9876543210abcdef9876543210abcdef98765432",
		CloneURL:  "https://github.com/gke-labs/eval-runner.git",
		Repo:      "gke-labs/eval-runner",
	}

	if err := worker.ProcessPush(ctx, item); err != nil {
		t.Fatalf("ProcessPush failed: %v", err)
	}

	var baseRun v1alpha1.Run
	baseRunName := "base-9876543"
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: baseRunName}, &baseRun); err != nil {
		t.Fatalf("base Run %s not found: %v", baseRunName, err)
	}
	if baseRun.Spec.Repo.Commit != item.CommitSHA {
		t.Errorf("expected commit %s, got %s", item.CommitSHA, baseRun.Spec.Repo.Commit)
	}
	if baseRun.Spec.Model != "google/gemini-flash" {
		t.Errorf("expected model google/gemini-flash, got %s", baseRun.Spec.Model)
	}

	// Redelivery should be idempotent
	if err := worker.ProcessPush(ctx, item); err != nil {
		t.Fatalf("ProcessPush redelivery failed: %v", err)
	}
}

func TestHandler_HTTPEndpointsAndSignature(t *testing.T) {
	secretKey := []byte("my-test-secret-key")
	scheme := newTestScheme()

	secretObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "github-webhook",
			Namespace: "eval-runner",
		},
		Data: map[string][]byte{
			"secret": secretKey,
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secretObj).Build()

	cfg := Config{
		Namespace:           "eval-runner",
		GitHubWebhookSecret: "github-webhook",
		DefaultModel:        "mockllm/model",
		WebhookRepositories: []string{"gke-labs/eval-runner"},
		Client:              cl,
	}
	server := NewServer(cfg)
	server.Start()
	defer server.Stop()

	// 1. Healthz probe
	reqHealth := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rrHealth := httptest.NewRecorder()
	server.ServeHTTP(rrHealth, reqHealth)
	if rrHealth.Code != http.StatusOK {
		t.Errorf("expected healthz 200, got %d", rrHealth.Code)
	}

	// 2. Readyz probe
	reqReady := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rrReady := httptest.NewRecorder()
	server.ServeHTTP(rrReady, reqReady)
	if rrReady.Code != http.StatusOK {
		t.Errorf("expected readyz 200, got %d", rrReady.Code)
	}

	// 3. Unsigned request -> 401
	body := []byte(`{"action":"opened","number":101}`)
	reqUnsigned := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	reqUnsigned.Header.Set(HeaderGitHubEvent, EventPullRequest)
	rrUnsigned := httptest.NewRecorder()
	server.ServeHTTP(rrUnsigned, reqUnsigned)
	if rrUnsigned.Code != http.StatusUnauthorized {
		t.Errorf("expected unsigned request 401, got %d", rrUnsigned.Code)
	}

	// 4. Invalid signature -> 401
	reqBadSig := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	reqBadSig.Header.Set(HeaderGitHubEvent, EventPullRequest)
	reqBadSig.Header.Set(HeaderHubSignature256, "sha256=1111222233334444")
	rrBadSig := httptest.NewRecorder()
	server.ServeHTTP(rrBadSig, reqBadSig)
	if rrBadSig.Code != http.StatusUnauthorized {
		t.Errorf("expected bad signature 401, got %d", rrBadSig.Code)
	}

	// 5. Valid signature with real recorded pull_request fixture from testdata/
	recordedPRPayload, err := os.ReadFile("testdata/pull_request.json")
	if err != nil {
		t.Fatalf("failed to read testdata/pull_request.json: %v", err)
	}
	validSig := computeSignature(recordedPRPayload, secretKey)

	reqValid := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(recordedPRPayload))
	reqValid.Header.Set(HeaderGitHubEvent, EventPullRequest)
	reqValid.Header.Set(HeaderHubSignature256, validSig)
	rrValid := httptest.NewRecorder()

	server.ServeHTTP(rrValid, reqValid)
	if rrValid.Code != http.StatusOK {
		t.Fatalf("expected valid signed PR request 200, got %d: %s", rrValid.Code, rrValid.Body.String())
	}

	// Poll until worker queue creates candidate Run
	var candRun v1alpha1.Run
	pollUntil(t, 2*time.Second, func() bool {
		return cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-305-aaaaaaa"}, &candRun) == nil
	})

	// Verify candidate clone URL points to the contributor fork (Blocking 1)
	if candRun.Spec.Repo.URL != "https://github.com/contributor-fork/eval-runner.git" {
		t.Errorf("expected fork clone URL on candidate Run, got %s", candRun.Spec.Repo.URL)
	}

	var baseRun v1alpha1.Run
	pollUntil(t, 2*time.Second, func() bool {
		return cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "base-bbbbbbb"}, &baseRun) == nil
	})
	if baseRun.Spec.Repo.URL != "https://github.com/gke-labs/eval-runner.git" {
		t.Errorf("expected base clone URL on baseline Run, got %s", baseRun.Spec.Repo.URL)
	}

	var comp v1alpha1.Comparison
	pollUntil(t, 2*time.Second, func() bool {
		return cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-305"}, &comp) == nil
	})
	if comp.Spec.PullRequest.Repository != "gke-labs/eval-runner" {
		t.Errorf("expected PullRequest.Repository gke-labs/eval-runner, got %s", comp.Spec.PullRequest.Repository)
	}
}

func TestHandler_Validation_BadRequest(t *testing.T) {
	secretKey := []byte("secret")
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	cfg := Config{
		Namespace:           "eval-runner",
		StaticWebhookSecret: secretKey,
		WebhookRepositories: []string{"*"},
		Client:              cl,
	}
	server := NewServer(cfg)
	server.Start()
	defer server.Stop()

	tests := []struct {
		name    string
		event   string
		payload []byte
	}{
		{
			name:  "malicious repo name injection (Blocking 2)",
			event: EventPullRequest,
			payload: []byte(`{
				"action": "opened",
				"number": 1,
				"pull_request": {
					"number": 1,
					"head": {"sha": "1111111111111111111111111111111111111111"},
					"base": {"sha": "2222222222222222222222222222222222222222"}
				},
				"repository": {
					"full_name": "gke-labs/eval-runner.git?x=../../evil#",
					"name": "eval-runner.git?x=../../evil#",
					"owner": {"login": "gke-labs"}
				}
			}`),
		},
		{
			name:  "invalid head commit sha",
			event: EventPullRequest,
			payload: []byte(`{
				"action": "opened",
				"number": 1,
				"pull_request": {
					"number": 1,
					"head": {"sha": "not-a-valid-hex-sha;rm -rf /"},
					"base": {"sha": "2222222222222222222222222222222222222222"}
				},
				"repository": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
					"owner": {"login": "gke-labs"}
				}
			}`),
		},
		{
			name:  "invalid pull request number 0",
			event: EventPullRequest,
			payload: []byte(`{
				"action": "opened",
				"number": 0,
				"pull_request": {
					"number": 0,
					"head": {"sha": "1111111111111111111111111111111111111111"},
					"base": {"sha": "2222222222222222222222222222222222222222"}
				},
				"repository": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
					"owner": {"login": "gke-labs"}
				}
			}`),
		},
		{
			name:  "push invalid commit sha",
			event: EventPush,
			payload: []byte(`{
				"ref": "refs/heads/main",
				"after": "not-valid-sha",
				"repository": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
					"default_branch": "main",
					"owner": {"login": "gke-labs"}
				}
			}`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sig := computeSignature(tc.payload, secretKey)
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(tc.payload))
			req.Header.Set(HeaderGitHubEvent, tc.event)
			req.Header.Set(HeaderHubSignature256, sig)
			rr := httptest.NewRecorder()

			server.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Errorf("expected 400 Bad Request, got %d: %s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestHandler_AllowListIgnored(t *testing.T) {
	secretKey := []byte("secret")
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	cfg := Config{
		Namespace:           "eval-runner",
		StaticWebhookSecret: secretKey,
		WebhookRepositories: []string{"gke-labs/eval-runner"},
		Client:              cl,
	}
	server := NewServer(cfg)
	server.Start()
	defer server.Stop()

	// PR for a repo not in the allow-list
	payload := []byte(`{
		"action": "opened",
		"number": 1,
		"pull_request": {
			"number": 1,
			"head": {"sha": "1111111111111111111111111111111111111111"},
			"base": {"sha": "2222222222222222222222222222222222222222"}
		},
		"repository": {
			"full_name": "other-org/other-repo",
			"name": "other-repo",
			"owner": {"login": "other-org"}
		}
	}`)
	sig := computeSignature(payload, secretKey)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
	req.Header.Set(HeaderGitHubEvent, EventPullRequest)
	req.Header.Set(HeaderHubSignature256, sig)
	rr := httptest.NewRecorder()

	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK for ignored repository, got %d", rr.Code)
	}

	time.Sleep(50 * time.Millisecond)

	var runList v1alpha1.RunList
	_ = cl.List(t.Context(), &runList, client.InNamespace("eval-runner"))
	if len(runList.Items) != 0 {
		t.Errorf("expected 0 runs created for disallowed repo, got %d", len(runList.Items))
	}
}

func TestHandler_FailClosedEmptyAllowList(t *testing.T) {
	secretKey := []byte("secret")
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	// 1. Server configured with empty allow-list (fail-closed)
	cfgEmpty := Config{
		Namespace:           "eval-runner",
		StaticWebhookSecret: secretKey,
		WebhookRepositories: nil, // empty/nil: fail-closed
		Client:              cl,
	}
	serverEmpty := NewServer(cfgEmpty)
	serverEmpty.Start()
	defer serverEmpty.Stop()

	payload := []byte(`{
		"action": "opened",
		"number": 1,
		"pull_request": {
			"number": 1,
			"head": {"sha": "1111111111111111111111111111111111111111"},
			"base": {"sha": "2222222222222222222222222222222222222222"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sig := ComputeSignature(payload, secretKey)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
	req.Header.Set(HeaderGitHubEvent, EventPullRequest)
	req.Header.Set(HeaderHubSignature256, sig)
	rr := httptest.NewRecorder()

	serverEmpty.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK for ignored repository, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "ignored: repository not in allow-list") {
		t.Errorf("expected response body to contain 'ignored: repository not in allow-list', got: %s", rr.Body.String())
	}

	time.Sleep(50 * time.Millisecond)

	var runList v1alpha1.RunList
	_ = cl.List(t.Context(), &runList, client.InNamespace("eval-runner"))
	if len(runList.Items) != 0 {
		t.Errorf("expected 0 runs created when allow-list is empty (fail-closed), got %d", len(runList.Items))
	}

	// 2. Server configured with wildcard '*' allow-list
	cfgWildcard := Config{
		Namespace:           "eval-runner",
		StaticWebhookSecret: secretKey,
		WebhookRepositories: []string{"*"},
		Client:              cl,
	}
	serverWildcard := NewServer(cfgWildcard)
	serverWildcard.Start()
	defer serverWildcard.Stop()

	reqWildcard := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
	reqWildcard.Header.Set(HeaderGitHubEvent, EventPullRequest)
	reqWildcard.Header.Set(HeaderHubSignature256, sig)
	rrWildcard := httptest.NewRecorder()

	serverWildcard.ServeHTTP(rrWildcard, reqWildcard)
	if rrWildcard.Code != http.StatusOK {
		t.Errorf("expected 200 OK with wildcard allow-list, got %d", rrWildcard.Code)
	}
	if !strings.Contains(rrWildcard.Body.String(), "ok: enqueued pull_request") {
		t.Errorf("expected response body to contain 'ok: enqueued pull_request', got: %s", rrWildcard.Body.String())
	}
}

func TestResolveWebhookConfig_AdaptiveSamplingAndKeys(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "eval-runner-config",
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeyDefaultBatchSize:         "16",
			ConfigMapKeyDefaultMaxSamplesPerTask: "48",
			ConfigMapKeyCommandUsers:             "alice, bob, CHARLIE",
		},
	}

	cfg, err := ResolveWebhookConfig(cm, RawWebhookFlags{}, nil)
	if err != nil {
		t.Fatalf("unexpected error resolving config: %v", err)
	}
	if cfg.DefaultBatchSize != 16 {
		t.Errorf("expected DefaultBatchSize 16, got %d", cfg.DefaultBatchSize)
	}
	if cfg.DefaultMaxSamplesPerTask != 48 {
		t.Errorf("expected DefaultMaxSamplesPerTask 48, got %d", cfg.DefaultMaxSamplesPerTask)
	}
	if len(cfg.CommandUsers) != 3 || cfg.CommandUsers[0] != "alice" || cfg.CommandUsers[1] != "bob" || cfg.CommandUsers[2] != "CHARLIE" {
		t.Errorf("expected CommandUsers [alice bob CHARLIE], got %v", cfg.CommandUsers)
	}

	// Verify case-insensitive user check and no wildcard support
	if !IsUserAllowed("Alice", cfg.CommandUsers) {
		t.Errorf("expected Alice to be allowed")
	}
	if !IsUserAllowed("charlie", cfg.CommandUsers) {
		t.Errorf("expected charlie to be allowed")
	}
	if IsUserAllowed("eve", cfg.CommandUsers) {
		t.Errorf("expected eve to not be allowed")
	}
	// Wildcard "*" in allow-list must NOT match arbitrary users
	if IsUserAllowed("eve", []string{"*"}) {
		t.Errorf("expected '*' to NOT act as a wildcard in commandUsers")
	}

	// Malformed batch size
	badCM := &corev1.ConfigMap{
		Data: map[string]string{
			ConfigMapKeyDefaultBatchSize: "-5",
		},
	}
	if _, err := ResolveWebhookConfig(badCM, RawWebhookFlags{}, nil); err == nil {
		t.Errorf("expected error for negative defaultBatchSize")
	}

	// Malformed max samples
	badCM2 := &corev1.ConfigMap{
		Data: map[string]string{
			ConfigMapKeyDefaultMaxSamplesPerTask: "not-int",
		},
	}
	if _, err := ResolveWebhookConfig(badCM2, RawWebhookFlags{}, nil); err == nil {
		t.Errorf("expected error for non-integer defaultMaxSamplesPerTask")
	}

	// Explicit flags override
	bs := 32
	ms := 128
	flags := RawWebhookFlags{
		DefaultBatchSize:         &bs,
		DefaultMaxSamplesPerTask: &ms,
		CommandUsers:             "david",
		Explicit: map[string]bool{
			"default-batch-size":           true,
			"default-max-samples-per-task": true,
			"command-users":                true,
		},
	}
	cfgFlags, err := ResolveWebhookConfig(cm, flags, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgFlags.DefaultBatchSize != 32 {
		t.Errorf("expected explicit flag DefaultBatchSize 32, got %d", cfgFlags.DefaultBatchSize)
	}
	if cfgFlags.DefaultMaxSamplesPerTask != 128 {
		t.Errorf("expected explicit flag DefaultMaxSamplesPerTask 128, got %d", cfgFlags.DefaultMaxSamplesPerTask)
	}
	if len(cfgFlags.CommandUsers) != 1 || cfgFlags.CommandUsers[0] != "david" {
		t.Errorf("expected CommandUsers [david], got %v", cfgFlags.CommandUsers)
	}
}

func TestWorker_SetsSamplingFromConfigMap(t *testing.T) {
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	cfg := Config{
		Namespace:                "eval-runner",
		DefaultModel:             "mockllm/model",
		DefaultBatchSize:         12,
		DefaultMaxSamplesPerTask: 48,
		Client:                   cl,
	}
	worker := NewWorkerPool(cfg)
	ctx := t.Context()

	item := &PullRequestWorkItem{
		PRNumber:          200,
		HeadSHA:           "1111111111111111111111111111111111111111",
		BaseSHA:           "2222222222222222222222222222222222222222",
		CandidateCloneURL: "https://github.com/contributor/eval-runner.git",
		CandidateRepo:     "contributor/eval-runner",
		BaselineCloneURL:  "https://github.com/gke-labs/eval-runner.git",
		BaselineRepo:      "gke-labs/eval-runner",
		TargetRepo:        "gke-labs/eval-runner",
	}

	if err := worker.ProcessPullRequest(ctx, item); err != nil {
		t.Fatalf("ProcessPullRequest failed: %v", err)
	}

	var candRun v1alpha1.Run
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "pr-200-1111111"}, &candRun); err != nil {
		t.Fatalf("candidate Run not found: %v", err)
	}
	if candRun.Spec.SampleRange == nil || candRun.Spec.SampleRange.Start != 1 || candRun.Spec.SampleRange.End != 12 {
		t.Errorf("expected candidate SampleRange {1, 12}, got %+v", candRun.Spec.SampleRange)
	}
	if candRun.Spec.Limit != nil {
		t.Errorf("expected candidate Limit nil, got %v", candRun.Spec.Limit)
	}

	var baseRun v1alpha1.Run
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "base-2222222"}, &baseRun); err != nil {
		t.Fatalf("base Run not found: %v", err)
	}
	if baseRun.Spec.SampleRange == nil || baseRun.Spec.SampleRange.Start != 1 || baseRun.Spec.SampleRange.End != 12 {
		t.Errorf("expected base SampleRange {1, 12}, got %+v", baseRun.Spec.SampleRange)
	}
	if baseRun.Spec.Limit != nil {
		t.Errorf("expected base Limit nil, got %v", baseRun.Spec.Limit)
	}

	var comp v1alpha1.Comparison
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "pr-200"}, &comp); err != nil {
		t.Fatalf("Comparison not found: %v", err)
	}
	if comp.Spec.Sampling == nil {
		t.Fatalf("expected comp.Spec.Sampling to be non-nil")
	}
	if comp.Spec.Sampling.BatchSize != 12 || comp.Spec.Sampling.MaxSamplesPerTask != 48 {
		t.Errorf("expected Sampling {12, 48}, got %+v", comp.Spec.Sampling)
	}

	// Test push warm baseline run gets sampleRange {1, defaultBatchSize}
	pushItem := &PushWorkItem{
		CommitSHA: "3333333333333333333333333333333333333333",
		CloneURL:  "https://github.com/gke-labs/eval-runner.git",
		Repo:      "gke-labs/eval-runner",
	}
	if err := worker.ProcessPush(ctx, pushItem); err != nil {
		t.Fatalf("ProcessPush failed: %v", err)
	}
	var pushBaseRun v1alpha1.Run
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "base-3333333"}, &pushBaseRun); err != nil {
		t.Fatalf("push base Run not found: %v", err)
	}
	if pushBaseRun.Spec.SampleRange == nil || pushBaseRun.Spec.SampleRange.Start != 1 || pushBaseRun.Spec.SampleRange.End != 12 {
		t.Errorf("expected push base SampleRange {1, 12}, got %+v", pushBaseRun.Spec.SampleRange)
	}
}

func TestParseEvalMoreCommand(t *testing.T) {
	tests := []struct {
		name              string
		body              string
		maxSamplesPerTask int
		wantN             int
		wantOK            bool
	}{
		{
			name:              "simple /eval more 8",
			body:              "/eval more 8",
			maxSamplesPerTask: 64,
			wantN:             8,
			wantOK:            true,
		},
		{
			name:              "bounded by maxSamplesPerTask",
			body:              "/eval more 100",
			maxSamplesPerTask: 64,
			wantN:             64,
			wantOK:            true,
		},
		{
			name:              "case insensitive and extra whitespace",
			body:              "   /EVAL   MORE   16   ",
			maxSamplesPerTask: 64,
			wantN:             16,
			wantOK:            true,
		},
		{
			name:              "multiline comment containing command",
			body:              "Looks promising, let's get higher statistical confidence:\n/eval more 24\nThanks!",
			maxSamplesPerTask: 64,
			wantN:             24,
			wantOK:            true,
		},
		{
			name:              "zero samples is rejected",
			body:              "/eval more 0",
			maxSamplesPerTask: 64,
			wantN:             0,
			wantOK:            false,
		},
		{
			name:              "/eval rerun is out of scope",
			body:              "/eval rerun",
			maxSamplesPerTask: 64,
			wantN:             0,
			wantOK:            false,
		},
		{
			name:              "missing number",
			body:              "/eval more",
			maxSamplesPerTask: 64,
			wantN:             0,
			wantOK:            false,
		},
		{
			name:              "non-numeric",
			body:              "/eval more abc",
			maxSamplesPerTask: 64,
			wantN:             0,
			wantOK:            false,
		},
		{
			name:              "general comment",
			body:              "LGTM!",
			maxSamplesPerTask: 64,
			wantN:             0,
			wantOK:            false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotN, gotOK := ParseEvalMoreCommand(tc.body, tc.maxSamplesPerTask)
			if gotOK != tc.wantOK || gotN != tc.wantN {
				t.Errorf("ParseEvalMoreCommand(%q, %d) = (%d, %v), want (%d, %v)",
					tc.body, tc.maxSamplesPerTask, gotN, gotOK, tc.wantN, tc.wantOK)
			}
		})
	}
}

func TestHandler_IssueComment_ParseAndBound(t *testing.T) {
	secretKey := []byte("comment-secret")
	scheme := newTestScheme()

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-42",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     42,
			},
			ExtraSamples: 4,
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp).Build()

	cfg := Config{
		Namespace:                "eval-runner",
		StaticWebhookSecret:      secretKey,
		WebhookRepositories:      []string{"gke-labs/eval-runner"},
		CommandUsers:             []string{"allowed-user"},
		DefaultMaxSamplesPerTask: 64,
		Client:                   cl,
	}
	server := NewServer(cfg)
	server.Start()
	defer server.Stop()

	// 1. Post /eval more 8
	payload1 := []byte(`{
		"action": "created",
		"issue": {
			"number": 42,
			"pull_request": {"url": "https://api.github.com/repos/gke-labs/eval-runner/pulls/42"}
		},
		"comment": {
			"id": 1001,
			"body": "/eval more 8",
			"user": {"login": "allowed-user"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sig1 := ComputeSignature(payload1, secretKey)
	req1 := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload1))
	req1.Header.Set(HeaderGitHubEvent, EventIssueComment)
	req1.Header.Set(HeaderHubSignature256, sig1)
	rr1 := httptest.NewRecorder()

	server.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr1.Code, rr1.Body.String())
	}

	var updatedComp v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-42"}, &updatedComp); err != nil {
		t.Fatalf("failed to get comparison: %v", err)
	}
	// Initial was 4, added 8 -> 12
	if updatedComp.Spec.ExtraSamples != 12 {
		t.Errorf("expected ExtraSamples 12, got %d", updatedComp.Spec.ExtraSamples)
	}
	if !isCommentHandled(updatedComp.Annotations, 1001) {
		t.Errorf("expected comment 1001 to be recorded in annotations")
	}

	// 2. Post /eval more 100 (should be bounded by defaultMaxSamplesPerTask=64)
	payload2 := []byte(`{
		"action": "created",
		"issue": {
			"number": 42,
			"pull_request": {"url": "https://api.github.com/repos/gke-labs/eval-runner/pulls/42"}
		},
		"comment": {
			"id": 1002,
			"body": "/eval more 100",
			"user": {"login": "allowed-user"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sig2 := ComputeSignature(payload2, secretKey)
	req2 := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload2))
	req2.Header.Set(HeaderGitHubEvent, EventIssueComment)
	req2.Header.Set(HeaderHubSignature256, sig2)
	rr2 := httptest.NewRecorder()

	server.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr2.Code, rr2.Body.String())
	}

	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-42"}, &updatedComp); err != nil {
		t.Fatalf("failed to get comparison: %v", err)
	}
	// Previous was 12, added bounded 64 -> 76
	if updatedComp.Spec.ExtraSamples != 76 {
		t.Errorf("expected ExtraSamples 76 (12 + 64 bounded), got %d", updatedComp.Spec.ExtraSamples)
	}

	// 3. /eval rerun is ignored
	payloadRerun := []byte(`{
		"action": "created",
		"issue": {
			"number": 42,
			"pull_request": {"url": "https://api.github.com/repos/gke-labs/eval-runner/pulls/42"}
		},
		"comment": {
			"id": 1003,
			"body": "/eval rerun",
			"user": {"login": "allowed-user"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sigRerun := ComputeSignature(payloadRerun, secretKey)
	reqRerun := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payloadRerun))
	reqRerun.Header.Set(HeaderGitHubEvent, EventIssueComment)
	reqRerun.Header.Set(HeaderHubSignature256, sigRerun)
	rrRerun := httptest.NewRecorder()

	server.ServeHTTP(rrRerun, reqRerun)
	if rrRerun.Code != http.StatusOK {
		t.Errorf("expected 200 OK for ignored command, got %d", rrRerun.Code)
	}
	if !strings.Contains(rrRerun.Body.String(), "ignored: no supported command") {
		t.Errorf("expected ignored response, got: %s", rrRerun.Body.String())
	}

	// 4. Issue comment on non-PR issue is ignored
	payloadNonPR := []byte(`{
		"action": "created",
		"issue": {
			"number": 42
		},
		"comment": {
			"id": 1004,
			"body": "/eval more 8",
			"user": {"login": "allowed-user"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sigNonPR := ComputeSignature(payloadNonPR, secretKey)
	reqNonPR := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payloadNonPR))
	reqNonPR.Header.Set(HeaderGitHubEvent, EventIssueComment)
	reqNonPR.Header.Set(HeaderHubSignature256, sigNonPR)
	rrNonPR := httptest.NewRecorder()

	server.ServeHTTP(rrNonPR, reqNonPR)
	if rrNonPR.Code != http.StatusOK {
		t.Errorf("expected 200 OK for non-PR issue, got %d", rrNonPR.Code)
	}
	if !strings.Contains(rrNonPR.Body.String(), "ignored: not a pull request") {
		t.Errorf("expected ignored: not a pull request, got: %s", rrNonPR.Body.String())
	}

	// 5. Bot comment is ignored
	payloadBot := []byte(`{
		"action": "created",
		"issue": {
			"number": 42,
			"pull_request": {"url": "https://api.github.com/repos/gke-labs/eval-runner/pulls/42"}
		},
		"comment": {
			"id": 1005,
			"body": "/eval more 8",
			"user": {"login": "bot-user", "type": "Bot"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sigBot := ComputeSignature(payloadBot, secretKey)
	reqBot := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payloadBot))
	reqBot.Header.Set(HeaderGitHubEvent, EventIssueComment)
	reqBot.Header.Set(HeaderHubSignature256, sigBot)
	rrBot := httptest.NewRecorder()

	server.ServeHTTP(rrBot, reqBot)
	if rrBot.Code != http.StatusOK {
		t.Errorf("expected 200 OK for bot comment, got %d", rrBot.Code)
	}
	if !strings.Contains(rrBot.Body.String(), "ignored: bot comment") {
		t.Errorf("expected ignored: bot comment, got: %s", rrBot.Body.String())
	}

	// 6. Repository mismatch is ignored like not found (Blocking 3)
	// Comparison pr-42 belongs to gke-labs/eval-runner; incoming comment is for other-org/other-repo#42
	cfgWithTwoRepos := cfg
	cfgWithTwoRepos.WebhookRepositories = []string{"gke-labs/eval-runner", "other-org/other-repo"}
	serverMultiRepo := NewServer(cfgWithTwoRepos)
	serverMultiRepo.Start()
	defer serverMultiRepo.Stop()

	payloadRepoMismatch := []byte(`{
		"action": "created",
		"issue": {
			"number": 42,
			"pull_request": {"url": "https://api.github.com/repos/other-org/other-repo/pulls/42"}
		},
		"comment": {
			"id": 1006,
			"body": "/eval more 8",
			"user": {"login": "allowed-user"}
		},
		"repository": {
			"full_name": "other-org/other-repo",
			"name": "other-repo",
			"owner": {"login": "other-org"}
		}
	}`)
	sigMismatch := ComputeSignature(payloadRepoMismatch, secretKey)
	reqMismatch := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payloadRepoMismatch))
	reqMismatch.Header.Set(HeaderGitHubEvent, EventIssueComment)
	reqMismatch.Header.Set(HeaderHubSignature256, sigMismatch)
	rrMismatch := httptest.NewRecorder()

	serverMultiRepo.ServeHTTP(rrMismatch, reqMismatch)
	if rrMismatch.Code != http.StatusOK {
		t.Errorf("expected 200 OK for mismatched repo, got %d", rrMismatch.Code)
	}
	if !strings.Contains(rrMismatch.Body.String(), "ignored: comparison not found") {
		t.Errorf("expected ignored: comparison not found, got: %s", rrMismatch.Body.String())
	}
	// Verify pr-42 ExtraSamples was NOT bumped
	var compAfterMismatch v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-42"}, &compAfterMismatch); err != nil {
		t.Fatalf("failed to get comparison: %v", err)
	}
	if compAfterMismatch.Spec.ExtraSamples != 76 {
		t.Errorf("expected ExtraSamples to remain 76 after mismatched repo attempt, got %d", compAfterMismatch.Spec.ExtraSamples)
	}

	// 7. Comparison with custom spec.sampling.maxSamplesPerTask bounds n
	compCustomCap := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-70",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     70,
			},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         4,
				MaxSamplesPerTask: 16,
			},
			ExtraSamples: 0,
		},
	}
	if err := cl.Create(t.Context(), compCustomCap); err != nil {
		t.Fatalf("failed to create custom cap comparison: %v", err)
	}

	payloadCustomCap := []byte(`{
		"action": "created",
		"issue": {
			"number": 70,
			"pull_request": {"url": "https://api.github.com/repos/gke-labs/eval-runner/pulls/70"}
		},
		"comment": {
			"id": 1007,
			"body": "/eval more 64",
			"user": {"login": "allowed-user"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sigCustomCap := ComputeSignature(payloadCustomCap, secretKey)
	reqCustomCap := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payloadCustomCap))
	reqCustomCap.Header.Set(HeaderGitHubEvent, EventIssueComment)
	reqCustomCap.Header.Set(HeaderHubSignature256, sigCustomCap)
	rrCustomCap := httptest.NewRecorder()

	server.ServeHTTP(rrCustomCap, reqCustomCap)
	if rrCustomCap.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for custom cap comparison, got %d", rrCustomCap.Code)
	}
	var compAfterCustomCap v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-70"}, &compAfterCustomCap); err != nil {
		t.Fatalf("failed to get comparison: %v", err)
	}
	// Bounded by comp.Spec.Sampling.EffectiveMaxSamplesPerTask() = 16
	if compAfterCustomCap.Spec.ExtraSamples != 16 {
		t.Errorf("expected ExtraSamples 16 (bounded by Comparison custom cap), got %d", compAfterCustomCap.Spec.ExtraSamples)
	}
}

func TestHandler_IssueComment_RejectsUnauthorized(t *testing.T) {
	secretKey := []byte("comment-auth-secret")
	scheme := newTestScheme()

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-50",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     50,
			},
			ExtraSamples: 0,
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp).Build()

	fakeGH := github.NewFakeGitHubServer()
	defer fakeGH.Close()

	// Configure collaborator permissions in fake GitHub
	fakeGH.SetUserPermission("gke-labs", "eval-runner", "writer", "write")
	fakeGH.SetUserPermission("gke-labs", "eval-runner", "reader", "read")
	// "outsider" is not added, will return 404 from GitHub API

	ghClient := fakeGH.Client("test-token")

	cfg := Config{
		Namespace:                "eval-runner",
		StaticWebhookSecret:      secretKey,
		WebhookRepositories:      []string{"gke-labs/eval-runner"},
		CommandUsers:             []string{"allowlisted-user"},
		DefaultMaxSamplesPerTask: 64,
		GitHubClient:             ghClient,
		Client:                   cl,
	}
	server := NewServer(cfg)
	server.Start()
	defer server.Stop()

	// 1. Outsider author (not in allow-list, not a collaborator) -> 403 Forbidden
	payloadOutsider := []byte(`{
		"action": "created",
		"issue": {
			"number": 50,
			"pull_request": {"url": "https://api.github.com/repos/gke-labs/eval-runner/pulls/50"}
		},
		"comment": {
			"id": 2001,
			"body": "/eval more 8",
			"user": {"login": "outsider"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sigOutsider := ComputeSignature(payloadOutsider, secretKey)
	reqOutsider := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payloadOutsider))
	reqOutsider.Header.Set(HeaderGitHubEvent, EventIssueComment)
	reqOutsider.Header.Set(HeaderHubSignature256, sigOutsider)
	rrOutsider := httptest.NewRecorder()

	server.ServeHTTP(rrOutsider, reqOutsider)
	if rrOutsider.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for outsider, got %d: %s", rrOutsider.Code, rrOutsider.Body.String())
	}

	// 2. Read-only collaborator -> 403 Forbidden
	payloadReader := []byte(`{
		"action": "created",
		"issue": {
			"number": 50,
			"pull_request": {"url": "https://api.github.com/repos/gke-labs/eval-runner/pulls/50"}
		},
		"comment": {
			"id": 2002,
			"body": "/eval more 8",
			"user": {"login": "reader"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sigReader := ComputeSignature(payloadReader, secretKey)
	reqReader := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payloadReader))
	reqReader.Header.Set(HeaderGitHubEvent, EventIssueComment)
	reqReader.Header.Set(HeaderHubSignature256, sigReader)
	rrReader := httptest.NewRecorder()

	server.ServeHTTP(rrReader, reqReader)
	if rrReader.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for reader, got %d: %s", rrReader.Code, rrReader.Body.String())
	}

	// 3. Allow-listed user without write permission on GitHub -> 200 OK
	payloadAllowlisted := []byte(`{
		"action": "created",
		"issue": {
			"number": 50,
			"pull_request": {"url": "https://api.github.com/repos/gke-labs/eval-runner/pulls/50"}
		},
		"comment": {
			"id": 2003,
			"body": "/eval more 8",
			"user": {"login": "allowlisted-user"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sigAllowlisted := ComputeSignature(payloadAllowlisted, secretKey)
	reqAllowlisted := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payloadAllowlisted))
	reqAllowlisted.Header.Set(HeaderGitHubEvent, EventIssueComment)
	reqAllowlisted.Header.Set(HeaderHubSignature256, sigAllowlisted)
	rrAllowlisted := httptest.NewRecorder()

	server.ServeHTTP(rrAllowlisted, reqAllowlisted)
	if rrAllowlisted.Code != http.StatusOK {
		t.Errorf("expected 200 OK for allowlisted user, got %d: %s", rrAllowlisted.Code, rrAllowlisted.Body.String())
	}

	// 4. Collaborator with write access -> 200 OK and reacts with "eyes"
	payloadWriter := []byte(`{
		"action": "created",
		"issue": {
			"number": 50,
			"pull_request": {"url": "https://api.github.com/repos/gke-labs/eval-runner/pulls/50"}
		},
		"comment": {
			"id": 2004,
			"body": "/eval more 16",
			"user": {"login": "writer"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sigWriter := ComputeSignature(payloadWriter, secretKey)
	reqWriter := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payloadWriter))
	reqWriter.Header.Set(HeaderGitHubEvent, EventIssueComment)
	reqWriter.Header.Set(HeaderHubSignature256, sigWriter)
	rrWriter := httptest.NewRecorder()

	server.ServeHTTP(rrWriter, reqWriter)
	if rrWriter.Code != http.StatusOK {
		t.Errorf("expected 200 OK for writer, got %d: %s", rrWriter.Code, rrWriter.Body.String())
	}

	// Verify reaction "eyes" recorded on fake GitHub server
	reactions := fakeGH.CommentReactions(2004)
	if len(reactions) != 1 || reactions[0] != "eyes" {
		t.Errorf("expected comment 2004 to have reaction ['eyes'], got %v", reactions)
	}

	// Verify ExtraSamples was incremented (0 + 8 from allowlisted + 16 from writer = 24)
	var finalComp v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-50"}, &finalComp); err != nil {
		t.Fatalf("failed to get comparison: %v", err)
	}
	if finalComp.Spec.ExtraSamples != 24 {
		t.Errorf("expected ExtraSamples 24, got %d", finalComp.Spec.ExtraSamples)
	}
}

func TestHandler_IssueComment_IdempotentOnRedelivery(t *testing.T) {
	secretKey := []byte("comment-idempotent-secret")
	scheme := newTestScheme()

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-60",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     60,
			},
			ExtraSamples: 0,
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp).Build()

	fakeGH := github.NewFakeGitHubServer()
	defer fakeGH.Close()
	fakeGH.SetUserPermission("gke-labs", "eval-runner", "reviewer", "write")
	ghClient := fakeGH.Client("test-token")

	cfg := Config{
		Namespace:                "eval-runner",
		StaticWebhookSecret:      secretKey,
		WebhookRepositories:      []string{"gke-labs/eval-runner"},
		DefaultMaxSamplesPerTask: 64,
		GitHubClient:             ghClient,
		Client:                   cl,
	}
	server := NewServer(cfg)
	server.Start()
	defer server.Stop()

	payload := []byte(`{
		"action": "created",
		"issue": {
			"number": 60,
			"pull_request": {"url": "https://api.github.com/repos/gke-labs/eval-runner/pulls/60"}
		},
		"comment": {
			"id": 3001,
			"body": "/eval more 8",
			"user": {"login": "reviewer"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sig := ComputeSignature(payload, secretKey)

	// Delivery 1
	req1 := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
	req1.Header.Set(HeaderGitHubEvent, EventIssueComment)
	req1.Header.Set(HeaderHubSignature256, sig)
	rr1 := httptest.NewRecorder()

	server.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("first delivery failed with status %d: %s", rr1.Code, rr1.Body.String())
	}

	var compAfter1 v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-60"}, &compAfter1); err != nil {
		t.Fatalf("failed to get comparison: %v", err)
	}
	if compAfter1.Spec.ExtraSamples != 8 {
		t.Errorf("expected ExtraSamples 8 after first delivery, got %d", compAfter1.Spec.ExtraSamples)
	}
	if len(fakeGH.CommentReactions(3001)) != 1 {
		t.Errorf("expected 1 reaction after first delivery, got %d", len(fakeGH.CommentReactions(3001)))
	}

	// Delivery 2 (redelivery of identical comment payload)
	req2 := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
	req2.Header.Set(HeaderGitHubEvent, EventIssueComment)
	req2.Header.Set(HeaderHubSignature256, sig)
	rr2 := httptest.NewRecorder()

	server.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("redelivery failed with status %d: %s", rr2.Code, rr2.Body.String())
	}
	if !strings.Contains(rr2.Body.String(), "ok: comment already handled") {
		t.Errorf("expected body to contain 'ok: comment already handled', got %s", rr2.Body.String())
	}

	var compAfter2 v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-60"}, &compAfter2); err != nil {
		t.Fatalf("failed to get comparison: %v", err)
	}
	// ExtraSamples must remain 8 (NOT bumped again)
	if compAfter2.Spec.ExtraSamples != 8 {
		t.Errorf("expected ExtraSamples to remain 8 on redelivery, got %d", compAfter2.Spec.ExtraSamples)
	}
	// Reactions must remain 1 (NOT reacted again)
	if len(fakeGH.CommentReactions(3001)) != 1 {
		t.Errorf("expected reactions count to remain 1 on redelivery, got %d", len(fakeGH.CommentReactions(3001)))
	}
}

func TestHandler_PullRequest_Closed(t *testing.T) {
	secretKey := []byte("test-secret-key-12345")
	scheme := newTestScheme()

	// 1. Seed objects for PR 105 on gke-labs/eval-runner
	candRun105 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-105-aaaaaaa",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:      "105",
				LabelRepo:    "gke-labs_eval-runner",
				LabelHeadSHA: "aaaaaaa111111122222223333333444444455555",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "aaaaaaa111111122222223333333444444455555",
			},
			Model: "mockllm/model",
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-bbbbbbb",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelRepo:    "gke-labs_eval-runner",
				LabelHeadSHA: "bbbbbbb111111122222223333333444444455555",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "bbbbbbb111111122222223333333444444455555",
			},
			Model: "mockllm/model",
		},
	}

	comp105 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-105",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:      "105",
				LabelRepo:    "gke-labs_eval-runner",
				LabelHeadSHA: "aaaaaaa111111122222223333333444444455555",
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-bbbbbbb"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "pr-105-aaaaaaa"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     105,
				HeadSHA:    "aaaaaaa111111122222223333333444444455555",
				BaseSHA:    "bbbbbbb111111122222223333333444444455555",
			},
		},
	}

	// 2. Objects for a different PR (PR 200) on the same repo
	candRun200 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-200-xxxxxxx",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:   "200",
				LabelRepo: "gke-labs_eval-runner",
			},
		},
	}
	comp200 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-200",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:   "200",
				LabelRepo: "gke-labs_eval-runner",
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-bbbbbbb"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "pr-200-xxxxxxx"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     200,
			},
		},
	}

	// 3. Objects for PR 105 on a different repo (testing #163 isolation)
	candRunOther := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-105-other-cand",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:   "105",
				LabelRepo: "other-org_other-repo",
			},
		},
	}
	compOther := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-105-other",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:   "105",
				LabelRepo: "other-org_other-repo",
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-bbbbbbb"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "pr-105-other-cand"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "other-org/other-repo",
				Number:     105,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun105, baseRun, comp105, candRun200, comp200, candRunOther, compOther).
		WithStatusSubresource(candRun105, baseRun, comp105, candRun200, comp200, candRunOther, compOther).
		Build()

	cfg := Config{
		Namespace:           "eval-runner",
		StaticWebhookSecret: secretKey,
		WebhookRepositories: []string{"gke-labs/eval-runner", "other-org/other-repo"},
		Client:              cl,
	}

	server := NewServer(cfg)
	server.Start()
	defer server.Stop()

	closedPayload := []byte(`{
		"action": "closed",
		"number": 105,
		"pull_request": {
			"number": 105,
			"head": {
				"sha": "aaaaaaa111111122222223333333444444455555",
				"ref": "feature",
				"repo": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
					"owner": {"login": "gke-labs"}
				}
			},
			"base": {
				"sha": "bbbbbbb111111122222223333333444444455555",
				"ref": "main",
				"repo": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
					"owner": {"login": "gke-labs"}
				}
			}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)

	sig := ComputeSignature(closedPayload, secretKey)
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(closedPayload))
	req.Header.Set(HeaderGitHubEvent, EventPullRequest)
	req.Header.Set(HeaderHubSignature256, sig)
	rr := httptest.NewRecorder()

	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for closed PR, got %d: %s", rr.Code, rr.Body.String())
	}

	// Poll until Comparison pr-105 is deleted
	pollUntil(t, 2*time.Second, func() bool {
		var c v1alpha1.Comparison
		return apierrors.IsNotFound(cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-105"}, &c))
	})

	// Verify candidate run pr-105-aaaaaaa is deleted
	var cand v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-105-aaaaaaa"}, &cand); !apierrors.IsNotFound(err) {
		t.Errorf("expected candidate run pr-105-aaaaaaa to be deleted, got err: %v", err)
	}

	// Verify baseline run base-bbbbbbb is preserved
	var bRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "base-bbbbbbb"}, &bRun); err != nil {
		t.Errorf("expected baseline run base-bbbbbbb to be preserved, got err: %v", err)
	}

	// Verify PR 200 objects are preserved
	var c200 v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-200"}, &c200); err != nil {
		t.Errorf("expected PR 200 comparison to be preserved, got err: %v", err)
	}
	var cand200 v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-200-xxxxxxx"}, &cand200); err != nil {
		t.Errorf("expected PR 200 candidate run to be preserved, got err: %v", err)
	}

	// Verify other repo's PR 105 objects are preserved
	var cOther v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-105-other"}, &cOther); err != nil {
		t.Errorf("expected other repo PR 105 comparison to be preserved, got err: %v", err)
	}
	var candOther v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-105-other-cand"}, &candOther); err != nil {
		t.Errorf("expected other repo PR 105 candidate run to be preserved, got err: %v", err)
	}
}

func TestProcessPullRequestClosed_ForkAndIdempotent(t *testing.T) {
	scheme := newTestScheme()

	// Fork PR seed run with LabelRepo = sanitized fork repo
	forkRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-55-fork123",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:   "55",
				LabelRepo: "contributor-fork_eval-runner",
			},
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-shared",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelRepo: "gke-labs_eval-runner",
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-55",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:   "55",
				LabelRepo: "gke-labs_eval-runner",
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-shared"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "pr-55-fork123"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     55,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(forkRun, baseRun, comp).
		WithStatusSubresource(forkRun, baseRun, comp).
		Build()

	cfg := Config{
		Namespace: "eval-runner",
		Client:    cl,
	}
	wp := NewWorkerPool(cfg)

	item := &PullRequestWorkItem{
		Action:        ActionClosed,
		PRNumber:      55,
		CandidateRepo: "contributor-fork/eval-runner",
		TargetRepo:    "gke-labs/eval-runner",
	}

	// First call deletes comparison and fork seed run
	if err := wp.ProcessPullRequestClosed(t.Context(), item); err != nil {
		t.Fatalf("ProcessPullRequestClosed failed: %v", err)
	}

	var checkComp v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-55"}, &checkComp); !apierrors.IsNotFound(err) {
		t.Errorf("expected comparison to be deleted, got err: %v", err)
	}

	var checkForkRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-55-fork123"}, &checkForkRun); !apierrors.IsNotFound(err) {
		t.Errorf("expected fork candidate run to be deleted, got err: %v", err)
	}

	var checkBaseRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "base-shared"}, &checkBaseRun); err != nil {
		t.Errorf("expected baseline run to be preserved, got err: %v", err)
	}

	// Second call (idempotency / redelivery) must succeed without error
	if err := wp.ProcessPullRequestClosed(t.Context(), item); err != nil {
		t.Errorf("second call to ProcessPullRequestClosed should succeed idempotently, got: %v", err)
	}
}

func TestHandler_PullRequest_Closed_NotAllowed(t *testing.T) {
	secretKey := []byte("secret")
	scheme := newTestScheme()

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-99",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:   "99",
				LabelRepo: "unauthorized_repo",
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "unauthorized/repo",
				Number:     99,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(comp).
		WithStatusSubresource(comp).
		Build()

	cfg := Config{
		Namespace:           "eval-runner",
		StaticWebhookSecret: secretKey,
		WebhookRepositories: []string{"allowed/repo"},
		Client:              cl,
	}

	server := NewServer(cfg)

	closedPayload := []byte(`{
		"action": "closed",
		"number": 99,
		"pull_request": {
			"number": 99
		},
		"repository": {
			"full_name": "unauthorized/repo",
			"name": "repo",
			"owner": {"login": "unauthorized"}
		}
	}`)

	sig := ComputeSignature(closedPayload, secretKey)
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(closedPayload))
	req.Header.Set(HeaderGitHubEvent, EventPullRequest)
	req.Header.Set(HeaderHubSignature256, sig)
	rr := httptest.NewRecorder()

	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "ignored: repository not in allow-list") {
		t.Errorf("expected ignored response, got %s", rr.Body.String())
	}

	var checkComp v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-99"}, &checkComp); err != nil {
		t.Errorf("expected comparison to still exist, got err: %v", err)
	}
}
