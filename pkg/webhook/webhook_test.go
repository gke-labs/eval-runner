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
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/forge"
	"github.com/gke-labs/eval-runner/pkg/forge/consumer"
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
	found, err := worker.FindCompletedBaselineRun(ctx, "eval-runner", baseSHA, "https://github.com/other/other-repo.git", "mockllm/model")
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
		Namespace:     "eval-runner",
		DefaultModel:  "google/gemini-flash",
		DefaultCohort: "test-cohort",
		Client:        cl,
	}
	worker := NewWorkerPool(cfg)
	ctx := t.Context()

	item := &PushWorkItem{
		CommitSHA:  "9876543210abcdef9876543210abcdef98765432",
		CloneURL:   "https://github.com/gke-labs/eval-runner.git",
		Repo:       "gke-labs/eval-runner",
		Branch:     "main",
		CommitTime: "2026-10-20T04:12:00Z",
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
	if baseRun.Labels[LabelBranch] != "main" {
		t.Errorf("expected branch label 'main', got %q", baseRun.Labels[LabelBranch])
	}
	if baseRun.Annotations[AnnotationCommitTime] != "2026-10-20T04:12:00Z" {
		t.Errorf("expected commit-time annotation '2026-10-20T04:12:00Z', got %q", baseRun.Annotations[AnnotationCommitTime])
	}
	if baseRun.Spec.Cohort != "test-cohort" {
		t.Errorf("expected spec.cohort 'test-cohort', got %q", baseRun.Spec.Cohort)
	}

	// Redelivery should be idempotent
	if err := worker.ProcessPush(ctx, item); err != nil {
		t.Fatalf("ProcessPush redelivery failed: %v", err)
	}
}

func TestHandler_PushEvent_RecordedFixture(t *testing.T) {
	secretKey := []byte("push-secret-key")
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	cfg := Config{
		Namespace:           "eval-runner",
		DefaultModel:        "google/gemini-flash",
		DefaultCohort:       "gold-cohort",
		StaticWebhookSecret: secretKey,
		WebhookRepositories: []string{"gke-labs/eval-runner"},
		Client:              cl,
	}
	server := NewServer(cfg)
	server.Start()
	defer server.Stop()

	payload, err := os.ReadFile("testdata/push.json")
	if err != nil {
		t.Fatalf("failed to read testdata/push.json: %v", err)
	}

	sig := computeSignature(payload, secretKey)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
	req.Header.Set(HeaderGitHubEvent, EventPush)
	req.Header.Set(HeaderHubSignature256, sig)
	rr := httptest.NewRecorder()

	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	// Wait for worker processing
	var baseRun v1alpha1.Run
	baseRunName := "base-c0ffee1"
	ctx := t.Context()
	for i := 0; i < 50; i++ {
		err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: baseRunName}, &baseRun)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if baseRun.Name == "" {
		t.Fatalf("timed out waiting for warm baseline run %s", baseRunName)
	}
	if baseRun.Labels[LabelBranch] != "main" {
		t.Errorf("expected branch label 'main', got %q", baseRun.Labels[LabelBranch])
	}
	if baseRun.Annotations[AnnotationCommitTime] != "2026-10-20T04:12:00Z" {
		t.Errorf("expected commit-time annotation '2026-10-20T04:12:00Z', got %q", baseRun.Annotations[AnnotationCommitTime])
	}
	if baseRun.Spec.Cohort != "gold-cohort" {
		t.Errorf("expected spec.cohort 'gold-cohort', got %q", baseRun.Spec.Cohort)
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
		{
			name:  "pull_request mismatched candidate clone_url",
			event: EventPullRequest,
			payload: []byte(`{
				"action": "opened",
				"number": 1,
				"pull_request": {
					"number": 1,
					"head": {
						"sha": "1111111111111111111111111111111111111111",
						"ref": "feature",
						"repo": {
							"name": "eval-runner",
							"full_name": "fork/eval-runner",
							"clone_url": "https://github.com/attacker/malicious.git",
							"owner": {"login": "fork"}
						}
					},
					"base": {
						"sha": "2222222222222222222222222222222222222222",
						"ref": "main",
						"repo": {
							"name": "eval-runner",
							"full_name": "gke-labs/eval-runner",
							"owner": {"login": "gke-labs"}
						}
					}
				},
				"repository": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
					"owner": {"login": "gke-labs"}
				}
			}`),
		},
		{
			name:  "pull_request unparseable candidate clone_url",
			event: EventPullRequest,
			payload: []byte(`{
				"action": "opened",
				"number": 1,
				"pull_request": {
					"number": 1,
					"head": {
						"sha": "1111111111111111111111111111111111111111",
						"ref": "feature",
						"repo": {
							"name": "eval-runner",
							"full_name": "fork/eval-runner",
							"clone_url": "::invalid-url::",
							"owner": {"login": "fork"}
						}
					},
					"base": {
						"sha": "2222222222222222222222222222222222222222",
						"ref": "main",
						"repo": {
							"name": "eval-runner",
							"full_name": "gke-labs/eval-runner",
							"owner": {"login": "gke-labs"}
						}
					}
				},
				"repository": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
					"owner": {"login": "gke-labs"}
				}
			}`),
		},
		{
			name:  "pull_request mismatched base clone_url",
			event: EventPullRequest,
			payload: []byte(`{
				"action": "opened",
				"number": 1,
				"pull_request": {
					"number": 1,
					"head": {
						"sha": "1111111111111111111111111111111111111111",
						"ref": "feature",
						"repo": {
							"name": "eval-runner",
							"full_name": "fork/eval-runner",
							"owner": {"login": "fork"}
						}
					},
					"base": {
						"sha": "2222222222222222222222222222222222222222",
						"ref": "main",
						"repo": {
							"name": "eval-runner",
							"full_name": "gke-labs/eval-runner",
							"clone_url": "https://github.com/attacker/eval-runner.git",
							"owner": {"login": "gke-labs"}
						}
					}
				},
				"repository": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
					"owner": {"login": "gke-labs"}
				}
			}`),
		},
		{
			name:  "pull_request candidate clone_url foreign host with matching owner and name",
			event: EventPullRequest,
			payload: []byte(`{
				"action": "opened",
				"number": 1,
				"pull_request": {
					"number": 1,
					"head": {
						"sha": "1111111111111111111111111111111111111111",
						"ref": "feature",
						"repo": {
							"name": "eval-runner",
							"full_name": "fork/eval-runner",
							"clone_url": "https://evil.example.com/fork/eval-runner.git",
							"owner": {"login": "fork"}
						}
					},
					"base": {
						"sha": "2222222222222222222222222222222222222222",
						"ref": "main",
						"repo": {
							"name": "eval-runner",
							"full_name": "gke-labs/eval-runner",
							"owner": {"login": "gke-labs"}
						}
					}
				},
				"repository": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
					"owner": {"login": "gke-labs"}
				}
			}`),
		},
		{
			name:  "pull_request candidate clone_url single-segment path",
			event: EventPullRequest,
			payload: []byte(`{
				"action": "opened",
				"number": 1,
				"pull_request": {
					"number": 1,
					"head": {
						"sha": "1111111111111111111111111111111111111111",
						"ref": "feature",
						"repo": {
							"name": "eval-runner",
							"full_name": "fork/eval-runner",
							"clone_url": "https://evil.example.com/eval-runner.git",
							"owner": {"login": "fork"}
						}
					},
					"base": {
						"sha": "2222222222222222222222222222222222222222",
						"ref": "main",
						"repo": {
							"name": "eval-runner",
							"full_name": "gke-labs/eval-runner",
							"owner": {"login": "gke-labs"}
						}
					}
				},
				"repository": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
					"owner": {"login": "gke-labs"}
				}
			}`),
		},
		{
			name:  "pull_request candidate clone_url single-segment path on allowed host",
			event: EventPullRequest,
			payload: []byte(`{
				"action": "opened",
				"number": 1,
				"pull_request": {
					"number": 1,
					"head": {
						"sha": "1111111111111111111111111111111111111111",
						"ref": "feature",
						"repo": {
							"name": "eval-runner",
							"full_name": "fork/eval-runner",
							"clone_url": "https://github.com/eval-runner.git",
							"owner": {"login": "fork"}
						}
					},
					"base": {
						"sha": "2222222222222222222222222222222222222222",
						"ref": "main",
						"repo": {
							"name": "eval-runner",
							"full_name": "gke-labs/eval-runner",
							"owner": {"login": "gke-labs"}
						}
					}
				},
				"repository": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
					"owner": {"login": "gke-labs"}
				}
			}`),
		},
		{
			name:  "pull_request candidate clone_url scp-style form foreign host",
			event: EventPullRequest,
			payload: []byte(`{
				"action": "opened",
				"number": 1,
				"pull_request": {
					"number": 1,
					"head": {
						"sha": "1111111111111111111111111111111111111111",
						"ref": "feature",
						"repo": {
							"name": "eval-runner",
							"full_name": "fork/eval-runner",
							"clone_url": "git@evil.example.com:fork/eval-runner.git",
							"owner": {"login": "fork"}
						}
					},
					"base": {
						"sha": "2222222222222222222222222222222222222222",
						"ref": "main",
						"repo": {
							"name": "eval-runner",
							"full_name": "gke-labs/eval-runner",
							"owner": {"login": "gke-labs"}
						}
					}
				},
				"repository": {
					"full_name": "gke-labs/eval-runner",
					"name": "eval-runner",
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
			ConfigMapKeyDefaultCohort:            "suite-2026",
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
	if cfg.DefaultCohort != "suite-2026" {
		t.Errorf("expected DefaultCohort 'suite-2026', got %q", cfg.DefaultCohort)
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
		Explicit: map[string]bool{
			"default-batch-size":           true,
			"default-max-samples-per-task": true,
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
}

func TestResolveWebhookConfig_DefaultBudgetUSD(t *testing.T) {
	// 1. ConfigMap parses defaultBudgetUSD
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			ConfigMapKeyDefaultBudgetUSD: "0.50",
		},
	}
	cfg, err := ResolveWebhookConfig(cm, RawWebhookFlags{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.DefaultBudgetUSD == nil || *cfg.DefaultBudgetUSD != 0.50 {
		t.Errorf("expected DefaultBudgetUSD 0.50, got %v", cfg.DefaultBudgetUSD)
	}

	// 2. ConfigMap sentinel "none" or "0" disables default budget (unlimited)
	cmDisabled := &corev1.ConfigMap{
		Data: map[string]string{
			ConfigMapKeyDefaultBudgetUSD: "none",
		},
	}
	cfgDisabled, err := ResolveWebhookConfig(cmDisabled, RawWebhookFlags{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgDisabled.DefaultBudgetUSD != nil {
		t.Errorf("expected DefaultBudgetUSD nil for 'none', got %v", cfgDisabled.DefaultBudgetUSD)
	}

	cmZero := &corev1.ConfigMap{
		Data: map[string]string{
			ConfigMapKeyDefaultBudgetUSD: "0",
		},
	}
	cfgZero, err := ResolveWebhookConfig(cmZero, RawWebhookFlags{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgZero.DefaultBudgetUSD != nil {
		t.Errorf("expected DefaultBudgetUSD nil for '0', got %v", cfgZero.DefaultBudgetUSD)
	}

	// 3. Negative budget in ConfigMap returns error
	cmNeg := &corev1.ConfigMap{
		Data: map[string]string{
			ConfigMapKeyDefaultBudgetUSD: "-1.0",
		},
	}
	if _, err := ResolveWebhookConfig(cmNeg, RawWebhookFlags{}, nil); err == nil {
		t.Errorf("expected error for negative defaultBudgetUSD")
	}

	// 4. Non-numeric budget in ConfigMap returns error
	cmBad := &corev1.ConfigMap{
		Data: map[string]string{
			ConfigMapKeyDefaultBudgetUSD: "not-a-number",
		},
	}
	if _, err := ResolveWebhookConfig(cmBad, RawWebhookFlags{}, nil); err == nil {
		t.Errorf("expected error for non-numeric defaultBudgetUSD")
	}

	// 5. Explicit flag overrides ConfigMap
	bFlag := 1.25
	flags := RawWebhookFlags{
		DefaultBudgetUSD: &bFlag,
		Explicit: map[string]bool{
			"default-budget-usd": true,
		},
	}
	cfgFlag, err := ResolveWebhookConfig(cm, flags, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgFlag.DefaultBudgetUSD == nil || *cfgFlag.DefaultBudgetUSD != 1.25 {
		t.Errorf("expected explicit flag to override defaultBudgetUSD, got %v", cfgFlag.DefaultBudgetUSD)
	}

	zeroFlag := 0.0
	flagsZero := RawWebhookFlags{
		DefaultBudgetUSD: &zeroFlag,
		Explicit: map[string]bool{
			"default-budget-usd": true,
		},
	}
	cfgZeroFlag, err := ResolveWebhookConfig(cm, flagsZero, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgZeroFlag.DefaultBudgetUSD != nil {
		t.Errorf("expected explicit flag 0 to unset defaultBudgetUSD, got %v", cfgZeroFlag.DefaultBudgetUSD)
	}
}

func TestIsHostAllowed(t *testing.T) {
	tests := []struct {
		name      string
		host      string
		allowList []string
		expected  bool
	}{
		{
			name:      "exact match",
			host:      "github.com",
			allowList: []string{"github.com"},
			expected:  true,
		},
		{
			name:      "case-insensitive match host uppercase",
			host:      "GITHUB.COM",
			allowList: []string{"github.com"},
			expected:  true,
		},
		{
			name:      "case-insensitive match allow-list uppercase",
			host:      "github.com",
			allowList: []string{"GITHUB.COM"},
			expected:  true,
		},
		{
			name:      "whitespace trimming",
			host:      "  github.com  ",
			allowList: []string{" github.com "},
			expected:  true,
		},
		{
			name:      "unmatched host",
			host:      "evil.example.com",
			allowList: []string{"github.com"},
			expected:  false,
		},
		{
			name:      "empty allow-list fails closed",
			host:      "github.com",
			allowList: nil,
			expected:  false,
		},
		{
			name:      "empty allow-list slice fails closed",
			host:      "github.com",
			allowList: []string{},
			expected:  false,
		},
		{
			name:      "empty host fails closed",
			host:      "",
			allowList: []string{"github.com"},
			expected:  false,
		},
		{
			name:      "whitespace host fails closed",
			host:      "   ",
			allowList: []string{"github.com"},
			expected:  false,
		},
		{
			name:      "wildcard * entry does not match host",
			host:      "github.com",
			allowList: []string{"*"},
			expected:  false,
		},
		{
			name:      "wildcard * entry does not match arbitrary host",
			host:      "evil.example.com",
			allowList: []string{"*"},
			expected:  false,
		},
		{
			name:      "wildcard domain *.github.com does not match apex domain",
			host:      "github.com",
			allowList: []string{"*.github.com"},
			expected:  false,
		},
		{
			name:      "wildcard domain *.github.com does not match subdomain",
			host:      "sub.github.com",
			allowList: []string{"*.github.com"},
			expected:  false,
		},
		{
			name:      "wildcard domain *.github.com does not match foreign host",
			host:      "evil.example.com",
			allowList: []string{"*.github.com"},
			expected:  false,
		},
		{
			name:      "mixed allow-list matches exact entry only",
			host:      "github.com",
			allowList: []string{"*", "*.gitlab.com", "github.com"},
			expected:  true,
		},
		{
			name:      "mixed allow-list does not match wildcard entry",
			host:      "sub.gitlab.com",
			allowList: []string{"*", "*.gitlab.com", "github.com"},
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsHostAllowed(tt.host, tt.allowList)
			if got != tt.expected {
				t.Errorf("IsHostAllowed(%q, %v) = %v, expected %v", tt.host, tt.allowList, got, tt.expected)
			}
		})
	}
}

func TestResolveWebhookConfig_AllowedCloneHosts(t *testing.T) {
	// 1. Default when unset
	cfgDef, err := ResolveWebhookConfig(nil, RawWebhookFlags{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(cfgDef.AllowedCloneHosts, []string{"github.com"}) {
		t.Errorf("expected default AllowedCloneHosts [github.com], got %v", cfgDef.AllowedCloneHosts)
	}

	// 2. ConfigMap parses allowedCloneHosts
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			ConfigMapKeyAllowedCloneHosts: "github.com, git-server.eval-runner-sandbox.svc",
		},
	}
	cfgCM, err := ResolveWebhookConfig(cm, RawWebhookFlags{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectedCM := []string{"github.com", "git-server.eval-runner-sandbox.svc"}
	if !reflect.DeepEqual(cfgCM.AllowedCloneHosts, expectedCM) {
		t.Errorf("expected AllowedCloneHosts %v, got %v", expectedCM, cfgCM.AllowedCloneHosts)
	}

	// 3. Empty ConfigMap value defaults to github.com
	cmEmpty := &corev1.ConfigMap{
		Data: map[string]string{
			ConfigMapKeyAllowedCloneHosts: "",
		},
	}
	cfgEmpty, err := ResolveWebhookConfig(cmEmpty, RawWebhookFlags{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(cfgEmpty.AllowedCloneHosts, []string{"github.com"}) {
		t.Errorf("expected default AllowedCloneHosts [github.com] for empty ConfigMap value, got %v", cfgEmpty.AllowedCloneHosts)
	}

	// 4. Explicit flag overrides ConfigMap
	flags := RawWebhookFlags{
		AllowedCloneHosts: "git.mycorp.internal, github.com",
		Explicit: map[string]bool{
			"allowed-clone-hosts": true,
		},
	}
	cfgFlag, err := ResolveWebhookConfig(cm, flags, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectedFlag := []string{"git.mycorp.internal", "github.com"}
	if !reflect.DeepEqual(cfgFlag.AllowedCloneHosts, expectedFlag) {
		t.Errorf("expected explicit flag to override AllowedCloneHosts, got %v", cfgFlag.AllowedCloneHosts)
	}

	// 5. Explicit empty flag defaults to github.com even when ConfigMap is set
	flagsEmpty := RawWebhookFlags{
		AllowedCloneHosts: "",
		Explicit: map[string]bool{
			"allowed-clone-hosts": true,
		},
	}
	cfgFlagEmpty, err := ResolveWebhookConfig(cm, flagsEmpty, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(cfgFlagEmpty.AllowedCloneHosts, []string{"github.com"}) {
		t.Errorf("expected default AllowedCloneHosts [github.com] for empty flag value, got %v", cfgFlagEmpty.AllowedCloneHosts)
	}
}

func TestValidateCloneURL(t *testing.T) {
	s := &Server{
		cfg: Config{
			AllowedCloneHosts: []string{"github.com"},
		},
	}

	// 1. Foreign host with matching owner and name returns error
	foreignHostURL := "https://evil.example.com/fork/eval-runner.git"
	if err := s.validateCloneURL(foreignHostURL, "fork", "eval-runner"); err == nil {
		t.Errorf("expected validateCloneURL(%q) to fail for foreign host, but succeeded", foreignHostURL)
	} else if !strings.Contains(err.Error(), "evil.example.com") {
		t.Errorf("expected error to mention foreign host evil.example.com, got: %v", err)
	}

	// Fail if host check is removed: if foreign host IS allowed, it succeeds!
	sForeignAllowed := &Server{
		cfg: Config{
			AllowedCloneHosts: []string{"github.com", "evil.example.com"},
		},
	}
	if err := sForeignAllowed.validateCloneURL(foreignHostURL, "fork", "eval-runner"); err != nil {
		t.Errorf("expected validateCloneURL(%q) to succeed when evil.example.com is allowed, got: %v", foreignHostURL, err)
	}

	// 2. Single-segment path returns error unconditionally (even on allowed host)
	singleSegmentAllowed := "https://github.com/eval-runner.git"
	if err := s.validateCloneURL(singleSegmentAllowed, "fork", "eval-runner"); err == nil {
		t.Errorf("expected validateCloneURL(%q) to fail for single-segment path, but succeeded", singleSegmentAllowed)
	}
	singleSegmentForeign := "https://evil.example.com/eval-runner.git"
	if err := s.validateCloneURL(singleSegmentForeign, "fork", "eval-runner"); err == nil {
		t.Errorf("expected validateCloneURL(%q) to fail for single-segment foreign path, but succeeded", singleSegmentForeign)
	}

	// 3. SCP-style form with foreign host returns error
	scpForeignURL := "git@evil.example.com:fork/eval-runner.git"
	if err := s.validateCloneURL(scpForeignURL, "fork", "eval-runner"); err == nil {
		t.Errorf("expected validateCloneURL(%q) to fail for foreign scp-style host, but succeeded", scpForeignURL)
	} else if !strings.Contains(err.Error(), "evil.example.com") {
		t.Errorf("expected error to mention foreign host evil.example.com, got: %v", err)
	}

	// Fail if host check is removed: if foreign host IS allowed for scp, it succeeds!
	if err := sForeignAllowed.validateCloneURL(scpForeignURL, "fork", "eval-runner"); err != nil {
		t.Errorf("expected validateCloneURL(%q) to succeed when evil.example.com is allowed, got: %v", scpForeignURL, err)
	}

	// 4. Valid github.com URLs with matching owner/name succeed
	validHTTPS := "https://github.com/fork/eval-runner.git"
	if err := s.validateCloneURL(validHTTPS, "fork", "eval-runner"); err != nil {
		t.Errorf("expected validateCloneURL(%q) to succeed, got: %v", validHTTPS, err)
	}
	validSCP := "git@github.com:fork/eval-runner.git"
	if err := s.validateCloneURL(validSCP, "fork", "eval-runner"); err != nil {
		t.Errorf("expected validateCloneURL(%q) to succeed, got: %v", validSCP, err)
	}

	// 5. Mismatched owner or name on allowed host returns error
	mismatchedOwner := "https://github.com/other/eval-runner.git"
	if err := s.validateCloneURL(mismatchedOwner, "fork", "eval-runner"); err == nil {
		t.Errorf("expected validateCloneURL(%q) to fail on owner mismatch, but succeeded", mismatchedOwner)
	}
	mismatchedName := "https://github.com/fork/other-repo.git"
	if err := s.validateCloneURL(mismatchedName, "fork", "eval-runner"); err == nil {
		t.Errorf("expected validateCloneURL(%q) to fail on repo name mismatch, but succeeded", mismatchedName)
	}

	// 6. Unparseable clone URL returns error
	unparseableURL := "::invalid-url::"
	if err := s.validateCloneURL(unparseableURL, "fork", "eval-runner"); err == nil {
		t.Errorf("expected validateCloneURL(%q) to fail on unparseable URL, but succeeded", unparseableURL)
	}

	// 7. Server initialized with empty config uses default allow-list (github.com)
	sDefault := NewServer(Config{})
	if err := sDefault.validateCloneURL(validHTTPS, "fork", "eval-runner"); err != nil {
		t.Errorf("expected default server validateCloneURL to succeed, got: %v", err)
	}
	if err := sDefault.validateCloneURL(foreignHostURL, "fork", "eval-runner"); err == nil {
		t.Errorf("expected default server validateCloneURL to fail for foreign host, but succeeded")
	}

	// 8. Wildcard entries in Server allow-list do not match anything
	sWildcardAll := &Server{
		cfg: Config{
			AllowedCloneHosts: []string{"*"},
		},
	}
	if err := sWildcardAll.validateCloneURL(validHTTPS, "fork", "eval-runner"); err == nil {
		t.Errorf("expected server with '*' allow-list to reject %q, but succeeded", validHTTPS)
	}
	sWildcardDomain := &Server{
		cfg: Config{
			AllowedCloneHosts: []string{"*.github.com"},
		},
	}
	subdomainHTTPS := "https://sub.github.com/fork/eval-runner.git"
	if err := sWildcardDomain.validateCloneURL(subdomainHTTPS, "fork", "eval-runner"); err == nil {
		t.Errorf("expected server with '*.github.com' allow-list to reject %q, but succeeded", subdomainHTTPS)
	}
	if err := sWildcardDomain.validateCloneURL(validHTTPS, "fork", "eval-runner"); err == nil {
		t.Errorf("expected server with '*.github.com' allow-list to reject apex %q, but succeeded", validHTTPS)
	}
}

func TestWorker_SetsSamplingFromConfigMap(t *testing.T) {
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	bVal := 0.75
	cfg := Config{
		Namespace:                "eval-runner",
		DefaultModel:             "mockllm/model",
		DefaultBatchSize:         12,
		DefaultMaxSamplesPerTask: 48,
		DefaultBudgetUSD:         &bVal,
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
	if comp.Spec.BudgetUSD == nil || *comp.Spec.BudgetUSD != 0.75 {
		t.Errorf("expected BudgetUSD 0.75, got %v", comp.Spec.BudgetUSD)
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

type secretTrackingClient struct {
	client.Client
	mu         sync.Mutex
	secretGets []string
}

func (c *secretTrackingClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok {
		c.mu.Lock()
		c.secretGets = append(c.secretGets, key.Name)
		c.mu.Unlock()
	}
	return c.Client.Get(ctx, key, obj, opts...)
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
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(comp).WithObjects(comp).Build()
	trackCl := &secretTrackingClient{Client: cl}

	cfg := Config{
		Namespace:                "eval-runner",
		StaticWebhookSecret:      secretKey,
		WebhookRepositories:      []string{"gke-labs/eval-runner"},
		DefaultMaxSamplesPerTask: 64,
		Client:                   trackCl,
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
	// Webhook must NOT bump Spec.ExtraSamples; that is reserved for the controller
	if updatedComp.Spec.ExtraSamples != 4 {
		t.Errorf("expected ExtraSamples to remain 4, got %d", updatedComp.Spec.ExtraSamples)
	}
	if len(updatedComp.Status.PendingCommands) != 1 {
		t.Fatalf("expected 1 pendingCommand, got %d", len(updatedComp.Status.PendingCommands))
	}
	cmd1 := updatedComp.Status.PendingCommands[0]
	if cmd1.CommentID != 1001 || cmd1.Login != "allowed-user" || cmd1.Requested != 8 || cmd1.ReceivedAt.IsZero() {
		t.Errorf("unexpected pending command: %+v", cmd1)
	}

	// 2. Post /eval more 100 (should be bounded to defaultMaxSamplesPerTask=64)
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
	if len(updatedComp.Status.PendingCommands) != 2 {
		t.Fatalf("expected 2 pendingCommands, got %d", len(updatedComp.Status.PendingCommands))
	}
	cmd2 := updatedComp.Status.PendingCommands[1]
	if cmd2.CommentID != 1002 || cmd2.Requested != 64 {
		t.Errorf("expected requested 64 (bounded by defaultMaxSamplesPerTask), got %+v", cmd2)
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

	// 6. Repository mismatch is ignored like not found
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

	// 7. Invalid author login shape is rejected with 200
	payloadBadLogin := []byte(`{
		"action": "created",
		"issue": {
			"number": 42,
			"pull_request": {"url": "https://api.github.com/repos/gke-labs/eval-runner/pulls/42"}
		},
		"comment": {
			"id": 1008,
			"body": "/eval more 8",
			"user": {"login": "<script>alert(1)</script>"}
		},
		"repository": {
			"full_name": "gke-labs/eval-runner",
			"name": "eval-runner",
			"owner": {"login": "gke-labs"}
		}
	}`)
	sigBadLogin := ComputeSignature(payloadBadLogin, secretKey)
	reqBadLogin := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payloadBadLogin))
	reqBadLogin.Header.Set(HeaderGitHubEvent, EventIssueComment)
	reqBadLogin.Header.Set(HeaderHubSignature256, sigBadLogin)
	rrBadLogin := httptest.NewRecorder()

	server.ServeHTTP(rrBadLogin, reqBadLogin)
	if rrBadLogin.Code != http.StatusOK {
		t.Errorf("expected 200 OK for invalid login, got %d", rrBadLogin.Code)
	}
	if !strings.Contains(rrBadLogin.Body.String(), "ignored: invalid author login") {
		t.Errorf("expected ignored: invalid author login, got: %s", rrBadLogin.Body.String())
	}

	// 8. Bounding: when status.pendingCommands reaches MaxPendingCommands, new commands are dropped and oldest preserved
	var lastRR *httptest.ResponseRecorder
	for i := 2000; i < 2070; i++ {
		p := []byte(fmt.Sprintf(`{
			"action": "created",
			"issue": {
				"number": 42,
				"pull_request": {"url": "https://api.github.com/repos/gke-labs/eval-runner/pulls/42"}
			},
			"comment": {
				"id": %d,
				"body": "/eval more 2",
				"user": {"login": "user-%d"}
			},
			"repository": {
				"full_name": "gke-labs/eval-runner",
				"name": "eval-runner",
				"owner": {"login": "gke-labs"}
			}
		}`, i, i))
		s := ComputeSignature(p, secretKey)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(p))
		req.Header.Set(HeaderGitHubEvent, EventIssueComment)
		req.Header.Set(HeaderHubSignature256, s)
		lastRR = httptest.NewRecorder()
		server.ServeHTTP(lastRR, req)
	}

	if !strings.Contains(lastRR.Body.String(), "ignored: pending commands queue full") {
		t.Errorf("expected queue full response for overflow, got: %s", lastRR.Body.String())
	}

	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-42"}, &updatedComp); err != nil {
		t.Fatalf("failed to get comparison: %v", err)
	}
	if len(updatedComp.Status.PendingCommands) != v1alpha1.MaxPendingCommands {
		t.Errorf("expected pendingCommands count exactly %d, got %d", v1alpha1.MaxPendingCommands, len(updatedComp.Status.PendingCommands))
	}
	// Oldest entries must be preserved
	if updatedComp.Status.PendingCommands[0].CommentID != 1001 {
		t.Errorf("expected oldest comment 1001 to be preserved, got %d", updatedComp.Status.PendingCommands[0].CommentID)
	}

	// 9. Assert NO Secret reads occurred (especially github-token or github-app)
	trackCl.mu.Lock()
	defer trackCl.mu.Unlock()
	for _, s := range trackCl.secretGets {
		if s == "github-token" || s == "github-app" {
			t.Errorf("webhook unexpectedly read secret %s", s)
		}
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
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(comp).WithObjects(comp).Build()
	trackCl := &secretTrackingClient{Client: cl}

	cfg := Config{
		Namespace:                "eval-runner",
		StaticWebhookSecret:      secretKey,
		WebhookRepositories:      []string{"gke-labs/eval-runner"},
		DefaultMaxSamplesPerTask: 64,
		Client:                   trackCl,
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
	if len(compAfter1.Status.PendingCommands) != 1 {
		t.Fatalf("expected 1 pending command after first delivery, got %d", len(compAfter1.Status.PendingCommands))
	}
	if compAfter1.Status.PendingCommands[0].CommentID != 3001 {
		t.Errorf("expected comment ID 3001, got %d", compAfter1.Status.PendingCommands[0].CommentID)
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
	// PendingCommands must still have only 1 entry
	if len(compAfter2.Status.PendingCommands) != 1 {
		t.Errorf("expected pendingCommands count to remain 1 on redelivery, got %d", len(compAfter2.Status.PendingCommands))
	}

	// Delivery 3 (when comment is already recorded in status.handledCommands)
	compAfter2.Status.HandledCommands = []v1alpha1.ComparisonHandledCommand{
		{
			CommentID: 3001,
			Login:     "reviewer",
			Requested: 8,
			Outcome:   "applied",
		},
	}
	compAfter2.Status.PendingCommands = nil
	if err := cl.Status().Update(t.Context(), &compAfter2); err != nil {
		t.Fatalf("failed to update status: %v", err)
	}

	req3 := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
	req3.Header.Set(HeaderGitHubEvent, EventIssueComment)
	req3.Header.Set(HeaderHubSignature256, sig)
	rr3 := httptest.NewRecorder()

	server.ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusOK {
		t.Fatalf("delivery 3 failed with status %d: %s", rr3.Code, rr3.Body.String())
	}
	if !strings.Contains(rr3.Body.String(), "ok: comment already handled") {
		t.Errorf("expected body to contain 'ok: comment already handled', got %s", rr3.Body.String())
	}

	var compAfter3 v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-60"}, &compAfter3); err != nil {
		t.Fatalf("failed to get comparison: %v", err)
	}
	if len(compAfter3.Status.PendingCommands) != 0 {
		t.Errorf("expected 0 pendingCommands after already handled redelivery, got %d", len(compAfter3.Status.PendingCommands))
	}

	// Assert NO Secret reads occurred
	trackCl.mu.Lock()
	defer trackCl.mu.Unlock()
	for _, s := range trackCl.secretGets {
		if s == "github-token" || s == "github-app" {
			t.Errorf("webhook unexpectedly read secret %s", s)
		}
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

	// Poll until Comparison pr-105 is labeled closed
	pollUntil(t, 2*time.Second, func() bool {
		var c v1alpha1.Comparison
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-105"}, &c); err == nil {
			return c.Labels[consumer.LabelPRState] == consumer.StateClosed
		}
		return false
	})

	// Verify candidate run pr-105-aaaaaaa is preserved (referenced by comparison)
	var cand v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-105-aaaaaaa"}, &cand); err != nil {
		t.Errorf("expected candidate run pr-105-aaaaaaa to be preserved, got err: %v", err)
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

func TestHandler_PullRequest_Closed_Merged(t *testing.T) {
	secretKey := []byte("secret")
	scheme := newTestScheme()

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-106",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:               "106",
				LabelRepo:             "gke-labs_eval-runner",
				consumer.LabelPRState: consumer.StateOpen,
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "pr-106-aaaaaaa"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-bbbbbbb"},
		},
	}
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-106-aaaaaaa",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:   "106",
				LabelRepo: "gke-labs_eval-runner",
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(comp, candRun).
		Build()

	cfg := Config{
		Namespace:           "eval-runner",
		StaticWebhookSecret: secretKey,
		WebhookRepositories: []string{"gke-labs/eval-runner"},
		Client:              cl,
	}

	server := NewServer(cfg)
	server.Start()
	defer server.Stop()

	closedPayload := []byte(`{
		"action": "closed",
		"number": 106,
		"pull_request": {
			"number": 106,
			"state": "closed",
			"merged": true,
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

	// Poll until Comparison pr-106 is labeled merged
	pollUntil(t, 2*time.Second, func() bool {
		var c v1alpha1.Comparison
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-106"}, &c); err == nil {
			return c.Labels[consumer.LabelPRState] == consumer.StateMerged
		}
		return false
	})

	// Candidate run pr-106-aaaaaaa preserved
	var cand v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-106-aaaaaaa"}, &cand); err != nil {
		t.Errorf("expected candidate run pr-106-aaaaaaa to be preserved, got err: %v", err)
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

	// First call labels comparison as closed and preserves referenced fork seed run
	if err := wp.ProcessPullRequestClosed(t.Context(), item); err != nil {
		t.Fatalf("ProcessPullRequestClosed failed: %v", err)
	}

	var checkComp v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-55"}, &checkComp); err != nil {
		t.Errorf("expected comparison to be retained, got err: %v", err)
	} else if checkComp.Labels[consumer.LabelPRState] != consumer.StateClosed {
		t.Errorf("expected comparison pr-state=%s, got %q", consumer.StateClosed, checkComp.Labels[consumer.LabelPRState])
	}

	var checkForkRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-55-fork123"}, &checkForkRun); err != nil {
		t.Errorf("expected fork candidate run to be preserved, got err: %v", err)
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

func TestProcessPullRequestClosed_PreservesSeedRunReusedBySurvivingComparison(t *testing.T) {
	scheme := newTestScheme()

	// 1. Seed Run for PR 7 that was chosen as baseline for live PR 8
	reusedRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-7-aaaaaaa",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:      "7",
				LabelRepo:    "gke-labs_eval-runner",
				LabelHeadSHA: "aaaaaaa111111122222223333333444444455555",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "evalrunner.gke-labs.dev/v1alpha1",
					Kind:       "Comparison",
					Name:       "pr-8",
					UID:        "pr-8-uid",
				},
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

	// 2. An older seed Run for PR 7 that is NOT reused anywhere
	unreusedRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-7-oldsha",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:      "7",
				LabelRepo:    "gke-labs_eval-runner",
				LabelHeadSHA: "old1111111111122222223333333444444455555",
			},
		},
	}

	// 3. A seed Run for PR 7 that carries an owner reference to live PR 8 (e.g. batch reuse)
	ownedOnlyRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-7-ownedonly",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:   "7",
				LabelRepo: "gke-labs_eval-runner",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "evalrunner.gke-labs.dev/v1alpha1",
					Kind:       "Comparison",
					Name:       "pr-8",
					UID:        "pr-8-uid",
				},
			},
		},
	}

	// 4. Comparison for PR 7
	comp7 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-7",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:   "7",
				LabelRepo: "gke-labs_eval-runner",
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-init"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "pr-7-aaaaaaa"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     7,
			},
		},
	}

	// 5. Live Comparison for PR 8 that uses pr-7-aaaaaaa as its baseline
	comp8 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-8",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:   "8",
				LabelRepo: "gke-labs_eval-runner",
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "pr-7-aaaaaaa"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "pr-8-bbbbbbb"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     8,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(reusedRun, unreusedRun, ownedOnlyRun, comp7, comp8).
		WithStatusSubresource(reusedRun, unreusedRun, ownedOnlyRun, comp7, comp8).
		Build()

	cfg := Config{
		Namespace: "eval-runner",
		Client:    cl,
	}
	wp := NewWorkerPool(cfg)

	item := &PullRequestWorkItem{
		Action:     ActionClosed,
		PRNumber:   7,
		TargetRepo: "gke-labs/eval-runner",
	}

	if err := wp.ProcessPullRequestClosed(t.Context(), item); err != nil {
		t.Fatalf("ProcessPullRequestClosed failed: %v", err)
	}

	// comp7 must be retained with pr-state=closed
	var checkComp7 v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-7"}, &checkComp7); err != nil {
		t.Errorf("expected comparison pr-7 to be retained, got err: %v", err)
	} else if checkComp7.Labels[consumer.LabelPRState] != consumer.StateClosed {
		t.Errorf("expected comparison pr-7 pr-state=%s, got %q", consumer.StateClosed, checkComp7.Labels[consumer.LabelPRState])
	}

	// unreusedRun must be deleted
	var checkOldRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-7-oldsha"}, &checkOldRun); !apierrors.IsNotFound(err) {
		t.Errorf("expected unreused seed run pr-7-oldsha to be deleted, got err: %v", err)
	}

	// reusedRun (pr-7-aaaaaaa) MUST BE PRESERVED because pr-8 is a live comparison referencing it as baseline
	var checkReusedRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-7-aaaaaaa"}, &checkReusedRun); err != nil {
		t.Fatalf("expected reused seed run pr-7-aaaaaaa to be preserved for surviving comparison pr-8, got err: %v", err)
	}

	// ownedOnlyRun (pr-7-ownedonly) MUST BE PRESERVED because it carries an owner reference to surviving comparison pr-8
	var checkOwnedOnlyRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-7-ownedonly"}, &checkOwnedOnlyRun); err != nil {
		t.Fatalf("expected seed run pr-7-ownedonly with owner reference to pr-8 to be preserved, got err: %v", err)
	}

	// comp8 must be preserved
	var checkComp8 v1alpha1.Comparison
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pr-8"}, &checkComp8); err != nil {
		t.Errorf("expected comparison pr-8 to be preserved, got err: %v", err)
	}
}

func TestRepoHashAndNaming(t *testing.T) {
	// 1. RepoHash
	h1 := RepoHash("gke-labs/eval-runner")
	h2 := RepoHash("GKE-LABS/eval-runner")
	h3 := RepoHash(" other-org/other-repo  ")
	if len(h1) != 7 {
		t.Errorf("expected RepoHash length 7, got %d (%s)", len(h1), h1)
	}
	if h1 != h2 {
		t.Errorf("expected case-insensitive hash equality: %s vs %s", h1, h2)
	}
	if h1 == h3 {
		t.Errorf("expected different hashes for different repos: %s vs %s", h1, h3)
	}

	// 2. IsMultiRepo
	if consumer.IsMultiRepo(nil) {
		t.Errorf("expected IsMultiRepo(nil) to be false")
	}
	if consumer.IsMultiRepo([]string{"gke-labs/eval-runner"}) {
		t.Errorf("expected single-repo allow-list to not be multi-repo")
	}
	if consumer.IsMultiRepo([]string{"gke-labs/eval-runner", "gke-labs/eval-runner"}) {
		t.Errorf("expected duplicate single-repo to not be multi-repo")
	}
	if !consumer.IsMultiRepo([]string{"gke-labs/eval-runner", "other/repo"}) {
		t.Errorf("expected multiple repos to be multi-repo")
	}
	if !consumer.IsMultiRepo([]string{"*"}) {
		t.Errorf("expected wildcard '*' to be multi-repo")
	}

	// 3. ComparisonName
	singleComp := ComparisonName("gke-labs/eval-runner", 50, false)
	if singleComp != "pr-50" {
		t.Errorf("expected single-repo Comparison name 'pr-50', got %q", singleComp)
	}
	multiComp := ComparisonName("gke-labs/eval-runner", 50, true)
	expectedMultiComp := fmt.Sprintf("pr-%s-50", h1)
	if multiComp != expectedMultiComp {
		t.Errorf("expected multi-repo Comparison name %q, got %q", expectedMultiComp, multiComp)
	}

	// 4. CandidateRunName
	singleCand := CandidateRunName("gke-labs/eval-runner", 50, "1234567890abcdef", false)
	if singleCand != "pr-50-1234567" {
		t.Errorf("expected single-repo CandidateRun name 'pr-50-1234567', got %q", singleCand)
	}
	multiCand := CandidateRunName("gke-labs/eval-runner", 50, "1234567890abcdef", true)
	expectedMultiCand := fmt.Sprintf("pr-%s-50-1234567", h1)
	if multiCand != expectedMultiCand {
		t.Errorf("expected multi-repo CandidateRun name %q, got %q", expectedMultiCand, multiCand)
	}

	// 5. BaselineRunName
	singleBase := BaselineRunName("gke-labs/eval-runner", "1234567890abcdef", false)
	if singleBase != "base-1234567" {
		t.Errorf("expected single-repo BaselineRun name 'base-1234567', got %q", singleBase)
	}
	multiBase := BaselineRunName("gke-labs/eval-runner", "1234567890abcdef", true)
	expectedMultiBase := fmt.Sprintf("base-%s-1234567", h1)
	if multiBase != expectedMultiBase {
		t.Errorf("expected multi-repo BaselineRun name %q, got %q", expectedMultiBase, multiBase)
	}
	emptyRepoBase := BaselineRunName("", "1234567890abcdef", true)
	if emptyRepoBase != "base-1234567" {
		t.Errorf("expected empty-repo BaselineRun name 'base-1234567', got %q", emptyRepoBase)
	}
}

func TestMultiRepo_SamePRNumber_DistinctObjectsAndCommentIsolation(t *testing.T) {
	secretKey := []byte("multi-repo-secret")
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.Comparison{}).Build()

	cfg := Config{
		Namespace:           "eval-runner",
		StaticWebhookSecret: secretKey,
		WebhookRepositories: []string{"org1/repo1", "org2/repo2"},
		DefaultModel:        "mockllm/model",
		Client:              cl,
	}

	server := NewServer(cfg)
	server.Start()
	defer server.Stop()

	sha1 := "aaaaaaa111111122222223333333444444455555"
	sha2 := "bbbbbbb111111122222223333333444444455555"
	baseSHA := "ccccccc111111122222223333333444444455555"

	// 1. PR #50 on org1/repo1 opened
	pr1Payload := fmt.Sprintf(`{
		"action": "opened",
		"number": 50,
		"pull_request": {
			"number": 50,
			"head": {
				"sha": %q,
				"ref": "feature1",
				"repo": {"full_name": "org1/repo1", "name": "repo1", "clone_url": "https://github.com/org1/repo1.git", "owner": {"login": "org1"}}
			},
			"base": {
				"sha": %q,
				"ref": "main",
				"repo": {"full_name": "org1/repo1", "name": "repo1", "clone_url": "https://github.com/org1/repo1.git", "owner": {"login": "org1"}}
			}
		},
		"repository": {"full_name": "org1/repo1", "name": "repo1", "clone_url": "https://github.com/org1/repo1.git", "owner": {"login": "org1"}}
	}`, sha1, baseSHA)

	sig1 := ComputeSignature([]byte(pr1Payload), secretKey)
	req1 := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader([]byte(pr1Payload)))
	req1.Header.Set(HeaderGitHubEvent, EventPullRequest)
	req1.Header.Set(HeaderHubSignature256, sig1)
	rr1 := httptest.NewRecorder()
	server.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for PR1, got %d: %s", rr1.Code, rr1.Body.String())
	}

	hash1 := RepoHash("org1/repo1")
	expectedComp1Name := fmt.Sprintf("pr-%s-50", hash1)
	expectedCand1Name := fmt.Sprintf("pr-%s-50-aaaaaaa", hash1)
	expectedBase1Name := fmt.Sprintf("base-%s-ccccccc", hash1)

	var comp1 v1alpha1.Comparison
	pollUntil(t, 2*time.Second, func() bool {
		return cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedComp1Name}, &comp1) == nil
	})

	var cand1 v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedCand1Name}, &cand1); err != nil {
		t.Fatalf("expected candidate run %s to exist: %v", expectedCand1Name, err)
	}
	var base1 v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedBase1Name}, &base1); err != nil {
		t.Fatalf("expected baseline run %s to exist: %v", expectedBase1Name, err)
	}
	if comp1.Spec.Baseline.Run != expectedBase1Name {
		t.Errorf("expected comp1 Baseline.Run to be %s, got %s", expectedBase1Name, comp1.Spec.Baseline.Run)
	}

	// 2. PR #50 on org2/repo2 opened (same PR number 50!)
	pr2Payload := fmt.Sprintf(`{
		"action": "opened",
		"number": 50,
		"pull_request": {
			"number": 50,
			"head": {
				"sha": %q,
				"ref": "feature2",
				"repo": {"full_name": "org2/repo2", "name": "repo2", "clone_url": "https://github.com/org2/repo2.git", "owner": {"login": "org2"}}
			},
			"base": {
				"sha": %q,
				"ref": "main",
				"repo": {"full_name": "org2/repo2", "name": "repo2", "clone_url": "https://github.com/org2/repo2.git", "owner": {"login": "org2"}}
			}
		},
		"repository": {"full_name": "org2/repo2", "name": "repo2", "clone_url": "https://github.com/org2/repo2.git", "owner": {"login": "org2"}}
	}`, sha2, baseSHA)

	sig2 := ComputeSignature([]byte(pr2Payload), secretKey)
	req2 := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader([]byte(pr2Payload)))
	req2.Header.Set(HeaderGitHubEvent, EventPullRequest)
	req2.Header.Set(HeaderHubSignature256, sig2)
	rr2 := httptest.NewRecorder()
	server.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for PR2, got %d: %s", rr2.Code, rr2.Body.String())
	}

	hash2 := RepoHash("org2/repo2")
	expectedComp2Name := fmt.Sprintf("pr-%s-50", hash2)
	expectedCand2Name := fmt.Sprintf("pr-%s-50-bbbbbbb", hash2)
	expectedBase2Name := fmt.Sprintf("base-%s-ccccccc", hash2)

	if expectedComp1Name == expectedComp2Name {
		t.Fatalf("expected distinct Comparison names, but both were %s", expectedComp1Name)
	}
	if expectedCand1Name == expectedCand2Name {
		t.Fatalf("expected distinct CandidateRun names, but both were %s", expectedCand1Name)
	}
	if expectedBase1Name == expectedBase2Name {
		t.Fatalf("expected distinct BaselineRun names, but both were %s", expectedBase1Name)
	}

	var comp2 v1alpha1.Comparison
	pollUntil(t, 2*time.Second, func() bool {
		return cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedComp2Name}, &comp2) == nil
	})

	var cand2 v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedCand2Name}, &cand2); err != nil {
		t.Fatalf("expected candidate run %s to exist: %v", expectedCand2Name, err)
	}
	var base2 v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedBase2Name}, &base2); err != nil {
		t.Fatalf("expected baseline run %s to exist: %v", expectedBase2Name, err)
	}
	if comp2.Spec.Baseline.Run != expectedBase2Name {
		t.Errorf("expected comp2 Baseline.Run to be %s, got %s", expectedBase2Name, comp2.Spec.Baseline.Run)
	}

	// Verify comp1 was NOT overwritten or re-pointed
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedComp1Name}, &comp1); err != nil {
		t.Fatalf("expected comp1 %s to still exist: %v", expectedComp1Name, err)
	}
	if comp1.Spec.Candidate.Run != expectedCand1Name {
		t.Errorf("comp1 candidate run was corrupted: expected %s, got %s", expectedCand1Name, comp1.Spec.Candidate.Run)
	}

	// 3. Post /eval more 4 on org1/repo1 PR #50
	comment1 := []byte(`{
		"action": "created",
		"issue": {
			"number": 50,
			"pull_request": {"url": "https://api.github.com/repos/org1/repo1/pulls/50"}
		},
		"comment": {
			"id": 1001,
			"body": "/eval more 4",
			"user": {"login": "alice"}
		},
		"repository": {
			"full_name": "org1/repo1",
			"name": "repo1",
			"owner": {"login": "org1"}
		}
	}`)
	sigComment1 := ComputeSignature(comment1, secretKey)
	reqComment1 := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(comment1))
	reqComment1.Header.Set(HeaderGitHubEvent, EventIssueComment)
	reqComment1.Header.Set(HeaderHubSignature256, sigComment1)
	rrComment1 := httptest.NewRecorder()
	server.ServeHTTP(rrComment1, reqComment1)
	if rrComment1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for comment1, got %d: %s", rrComment1.Code, rrComment1.Body.String())
	}

	// Verify comp1 received the pending command, and comp2 did NOT
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedComp1Name}, &comp1); err != nil {
		t.Fatalf("failed to get comp1: %v", err)
	}
	if len(comp1.Status.PendingCommands) != 1 || comp1.Status.PendingCommands[0].CommentID != 1001 {
		t.Errorf("expected comp1 to have pending command 1001, got %v", comp1.Status.PendingCommands)
	}

	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedComp2Name}, &comp2); err != nil {
		t.Fatalf("failed to get comp2: %v", err)
	}
	if len(comp2.Status.PendingCommands) != 0 {
		t.Errorf("expected comp2 to have 0 pending commands, got %v", comp2.Status.PendingCommands)
	}

	// 4. Post /eval more 6 on org2/repo2 PR #50
	comment2 := []byte(`{
		"action": "created",
		"issue": {
			"number": 50,
			"pull_request": {"url": "https://api.github.com/repos/org2/repo2/pulls/50"}
		},
		"comment": {
			"id": 1002,
			"body": "/eval more 6",
			"user": {"login": "bob"}
		},
		"repository": {
			"full_name": "org2/repo2",
			"name": "repo2",
			"owner": {"login": "org2"}
		}
	}`)
	sigComment2 := ComputeSignature(comment2, secretKey)
	reqComment2 := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(comment2))
	reqComment2.Header.Set(HeaderGitHubEvent, EventIssueComment)
	reqComment2.Header.Set(HeaderHubSignature256, sigComment2)
	rrComment2 := httptest.NewRecorder()
	server.ServeHTTP(rrComment2, reqComment2)
	if rrComment2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for comment2, got %d: %s", rrComment2.Code, rrComment2.Body.String())
	}

	// Verify comp2 received pending command 1002, and comp1 remains untouched
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedComp2Name}, &comp2); err != nil {
		t.Fatalf("failed to get comp2: %v", err)
	}
	if len(comp2.Status.PendingCommands) != 1 || comp2.Status.PendingCommands[0].CommentID != 1002 {
		t.Errorf("expected comp2 to have pending command 1002, got %v", comp2.Status.PendingCommands)
	}

	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedComp1Name}, &comp1); err != nil {
		t.Fatalf("failed to get comp1: %v", err)
	}
	if len(comp1.Status.PendingCommands) != 1 || comp1.Status.PendingCommands[0].CommentID != 1001 {
		t.Errorf("expected comp1 to still have only pending command 1001, got %v", comp1.Status.PendingCommands)
	}

	// 5. Close PR #50 on org1/repo1
	closePayload := fmt.Sprintf(`{
		"action": "closed",
		"number": 50,
		"pull_request": {
			"number": 50,
			"head": {"sha": %q, "ref": "feature1", "repo": {"full_name": "org1/repo1", "name": "repo1", "owner": {"login": "org1"}}},
			"base": {"sha": %q, "ref": "main", "repo": {"full_name": "org1/repo1", "name": "repo1", "owner": {"login": "org1"}}}
		},
		"repository": {"full_name": "org1/repo1", "name": "repo1", "owner": {"login": "org1"}}
	}`, sha1, baseSHA)
	sigClose := ComputeSignature([]byte(closePayload), secretKey)
	reqClose := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader([]byte(closePayload)))
	reqClose.Header.Set(HeaderGitHubEvent, EventPullRequest)
	reqClose.Header.Set(HeaderHubSignature256, sigClose)
	rrClose := httptest.NewRecorder()
	server.ServeHTTP(rrClose, reqClose)
	if rrClose.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for close, got %d: %s", rrClose.Code, rrClose.Body.String())
	}

	// comp1 is retained with pr-state=closed, cand1 is preserved
	pollUntil(t, 2*time.Second, func() bool {
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedComp1Name}, &comp1); err == nil {
			return comp1.Labels[consumer.LabelPRState] == consumer.StateClosed
		}
		return false
	})
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedCand1Name}, &cand1); err != nil {
		t.Errorf("expected cand1 %s to be preserved, got err: %v", expectedCand1Name, err)
	}

	// comp2 and cand2 must be preserved!
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedComp2Name}, &comp2); err != nil {
		t.Errorf("expected comp2 %s to be preserved, got err: %v", expectedComp2Name, err)
	}
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedCand2Name}, &cand2); err != nil {
		t.Errorf("expected cand2 %s to be preserved, got err: %v", expectedCand2Name, err)
	}

	// Baseline runs base1 and base2 must both be preserved!
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedBase1Name}, &base1); err != nil {
		t.Errorf("expected baseline run base1 %s to be preserved, got err: %v", expectedBase1Name, err)
	}
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: expectedBase2Name}, &base2); err != nil {
		t.Errorf("expected baseline run base2 %s to be preserved, got err: %v", expectedBase2Name, err)
	}
}

func TestMultiRepo_BackwardsCompatibility_ExistingLegacyNames(t *testing.T) {
	scheme := newTestScheme()

	// Pre-create a legacy Comparison named "pr-50" without hash
	legacyComp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-50",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelPR:      "50",
				LabelRepo:    "org1_repo1",
				LabelHeadSHA: "1111111111111111111111111111111111111111",
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-abcdef1"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "pr-50-1111111"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "org1/repo1",
				Number:     50,
				HeadSHA:    "1111111111111111111111111111111111111111",
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(legacyComp).
		WithStatusSubresource(legacyComp).
		Build()

	cfg := Config{
		Namespace:           "eval-runner",
		DefaultModel:        "mockllm/model",
		WebhookRepositories: []string{"org1/repo1", "org2/repo2"}, // multi-repo enabled!
		Client:              cl,
	}
	worker := NewWorkerPool(cfg)
	ctx := t.Context()

	// 1. Synchronize event for org1/repo1 PR #50 with new commit
	syncItem := &PullRequestWorkItem{
		PRNumber:          50,
		HeadSHA:           "2222222222222222222222222222222222222222",
		BaseSHA:           "abcdef1234567890abcdef1234567890abcdef12",
		CandidateCloneURL: "https://github.com/org1/repo1.git",
		CandidateRepo:     "org1/repo1",
		BaselineCloneURL:  "https://github.com/org1/repo1.git",
		BaselineRepo:      "org1/repo1",
		TargetRepo:        "org1/repo1",
	}

	if err := worker.ProcessPullRequest(ctx, syncItem); err != nil {
		t.Fatalf("ProcessPullRequest failed: %v", err)
	}

	// Verify legacy comparison "pr-50" was updated in-place (not duplicated)
	var updatedComp v1alpha1.Comparison
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "pr-50"}, &updatedComp); err != nil {
		t.Fatalf("expected legacy comparison pr-50 to exist: %v", err)
	}
	if updatedComp.Spec.PullRequest.HeadSHA != "2222222222222222222222222222222222222222" {
		t.Errorf("expected HeadSHA updated to 2222222222222222222222222222222222222222, got %s", updatedComp.Spec.PullRequest.HeadSHA)
	}

	// Verify no new Comparison was created
	var compList v1alpha1.ComparisonList
	if err := cl.List(ctx, &compList, client.InNamespace("eval-runner")); err != nil {
		t.Fatalf("failed to list comparisons: %v", err)
	}
	if len(compList.Items) != 1 {
		t.Errorf("expected exactly 1 comparison (reusing legacy pr-50), got %d", len(compList.Items))
	}
}

func TestBaselineRuns_CollisionAndReuse(t *testing.T) {
	scheme := newTestScheme()
	ctx := t.Context()

	sharedBaseSHA := "abcdef1234567890abcdef1234567890abcdef12"
	pushSHA := "fedcba9876543210fedcba9876543210fedcba98"

	t.Run("SingleRepo_NamesUnchanged_AndReuseHits", func(t *testing.T) {
		cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.Comparison{}, &v1alpha1.Run{}).Build()
		cfg := Config{
			Namespace:           "eval-runner",
			DefaultModel:        "mockllm/model",
			DefaultBatchSize:    4,
			WebhookRepositories: []string{"org1/repo1"},
			Client:              cl,
		}
		worker := NewWorkerPool(cfg)

		// 1. First PR in single-repo mode
		pr1 := &PullRequestWorkItem{
			PRNumber:          10,
			HeadSHA:           "1111111222222233333334444444555556666666",
			BaseSHA:           sharedBaseSHA,
			CandidateCloneURL: "https://github.com/org1/repo1.git",
			CandidateRepo:     "org1/repo1",
			BaselineCloneURL:  "https://github.com/org1/repo1.git",
			BaselineRepo:      "org1/repo1",
			TargetRepo:        "org1/repo1",
		}
		if err := worker.ProcessPullRequest(ctx, pr1); err != nil {
			t.Fatalf("ProcessPullRequest failed: %v", err)
		}

		// Single-repo baseline Run must be named base-<sha7>
		expectedBaseName := "base-abcdef1"
		var baseRun v1alpha1.Run
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: expectedBaseName}, &baseRun); err != nil {
			t.Fatalf("expected single-repo baseline run %s to exist: %v", expectedBaseName, err)
		}
		if baseRun.Spec.Repo.URL != "https://github.com/org1/repo1.git" {
			t.Errorf("expected base repo URL https://github.com/org1/repo1.git, got %s", baseRun.Spec.Repo.URL)
		}

		var comp v1alpha1.Comparison
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "pr-10"}, &comp); err != nil {
			t.Fatalf("expected comparison pr-10 to exist: %v", err)
		}
		if comp.Spec.Baseline.Run != expectedBaseName {
			t.Errorf("expected comparison to reference %s, got %s", expectedBaseName, comp.Spec.Baseline.Run)
		}

		// Push event in single-repo mode warms base-<sha7>
		pushItem := &PushWorkItem{
			CommitSHA: pushSHA,
			CloneURL:  "https://github.com/org1/repo1.git",
			Repo:      "org1/repo1",
		}
		if err := worker.ProcessPush(ctx, pushItem); err != nil {
			t.Fatalf("ProcessPush failed: %v", err)
		}
		var pushBaseRun v1alpha1.Run
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "base-fedcba9"}, &pushBaseRun); err != nil {
			t.Fatalf("expected warm push baseline run base-fedcba9 to exist: %v", err)
		}

		// Complete the baseline run
		baseRun.Status.Phase = v1alpha1.RunPhaseCompleted
		if err := cl.Status().Update(ctx, &baseRun); err != nil {
			t.Fatalf("failed to mark baseline run completed: %v", err)
		}

		// 2. Second PR in single-repo mode with same base commit reuses base-<sha7>
		pr2 := &PullRequestWorkItem{
			PRNumber:          11,
			HeadSHA:           "2222222333333344444445555566666667777777",
			BaseSHA:           sharedBaseSHA,
			CandidateCloneURL: "https://github.com/org1/repo1.git",
			CandidateRepo:     "org1/repo1",
			BaselineCloneURL:  "https://github.com/org1/repo1.git",
			BaselineRepo:      "org1/repo1",
			TargetRepo:        "org1/repo1",
		}
		if err := worker.ProcessPullRequest(ctx, pr2); err != nil {
			t.Fatalf("ProcessPullRequest pr2 failed: %v", err)
		}

		var comp2 v1alpha1.Comparison
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "pr-11"}, &comp2); err != nil {
			t.Fatalf("expected comparison pr-11 to exist: %v", err)
		}
		if comp2.Spec.Baseline.Run != expectedBaseName {
			t.Errorf("expected pr2 to reuse baseline %s, got %s", expectedBaseName, comp2.Spec.Baseline.Run)
		}
	})

	t.Run("MultiRepo_CollisionProducesTwoRuns_AndReuseIsolated", func(t *testing.T) {
		cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.Comparison{}, &v1alpha1.Run{}).Build()
		cfg := Config{
			Namespace:           "eval-runner",
			DefaultModel:        "mockllm/model",
			DefaultBatchSize:    4,
			WebhookRepositories: []string{"org1/repo1", "org2/repo2"},
			Client:              cl,
		}
		worker := NewWorkerPool(cfg)

		hash1 := RepoHash("org1/repo1")
		hash2 := RepoHash("org2/repo2")
		expectedBase1Name := fmt.Sprintf("base-%s-abcdef1", hash1)
		expectedBase2Name := fmt.Sprintf("base-%s-abcdef1", hash2)
		if expectedBase1Name == expectedBase2Name {
			t.Fatalf("expected different baseline names for different repos, got %s", expectedBase1Name)
		}

		// 1. PR in org1/repo1 with shared base commit
		pr1 := &PullRequestWorkItem{
			PRNumber:          10,
			HeadSHA:           "1111111222222233333334444444555556666666",
			BaseSHA:           sharedBaseSHA,
			CandidateCloneURL: "https://github.com/org1/repo1.git",
			CandidateRepo:     "org1/repo1",
			BaselineCloneURL:  "https://github.com/org1/repo1.git",
			BaselineRepo:      "org1/repo1",
			TargetRepo:        "org1/repo1",
		}
		if err := worker.ProcessPullRequest(ctx, pr1); err != nil {
			t.Fatalf("ProcessPullRequest pr1 failed: %v", err)
		}

		var base1 v1alpha1.Run
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: expectedBase1Name}, &base1); err != nil {
			t.Fatalf("expected baseline run %s to exist: %v", expectedBase1Name, err)
		}
		if base1.Spec.Repo.URL != "https://github.com/org1/repo1.git" {
			t.Errorf("expected URL https://github.com/org1/repo1.git, got %s", base1.Spec.Repo.URL)
		}

		// 2. PR in org2/repo2 with SAME shared base commit (fork collision scenario)
		pr2 := &PullRequestWorkItem{
			PRNumber:          20,
			HeadSHA:           "2222222333333344444445555566666667777777",
			BaseSHA:           sharedBaseSHA,
			CandidateCloneURL: "https://github.com/org2/repo2.git",
			CandidateRepo:     "org2/repo2",
			BaselineCloneURL:  "https://github.com/org2/repo2.git",
			BaselineRepo:      "org2/repo2",
			TargetRepo:        "org2/repo2",
		}
		if err := worker.ProcessPullRequest(ctx, pr2); err != nil {
			t.Fatalf("ProcessPullRequest pr2 failed: %v", err)
		}

		var base2 v1alpha1.Run
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: expectedBase2Name}, &base2); err != nil {
			t.Fatalf("expected baseline run %s to exist: %v", expectedBase2Name, err)
		}
		if base2.Spec.Repo.URL != "https://github.com/org2/repo2.git" {
			t.Errorf("expected URL https://github.com/org2/repo2.git, got %s", base2.Spec.Repo.URL)
		}

		// Confirm BOTH baseline Runs exist simultaneously with their respective repo URLs
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: expectedBase1Name}, &base1); err != nil {
			t.Fatalf("expected baseline run %s to still exist: %v", expectedBase1Name, err)
		}
		if base1.Spec.Repo.URL != "https://github.com/org1/repo1.git" {
			t.Errorf("base1 URL corrupted: %s", base1.Spec.Repo.URL)
		}

		// 3. Complete org1's baseline run
		base1.Status.Phase = v1alpha1.RunPhaseCompleted
		if err := cl.Status().Update(ctx, &base1); err != nil {
			t.Fatalf("failed to update base1 status: %v", err)
		}

		// PR in org1/repo1 now reuses base1
		pr1Again := &PullRequestWorkItem{
			PRNumber:          12,
			HeadSHA:           "3333333444444455555666666677777778888888",
			BaseSHA:           sharedBaseSHA,
			CandidateCloneURL: "https://github.com/org1/repo1.git",
			CandidateRepo:     "org1/repo1",
			BaselineCloneURL:  "https://github.com/org1/repo1.git",
			BaselineRepo:      "org1/repo1",
			TargetRepo:        "org1/repo1",
		}
		if err := worker.ProcessPullRequest(ctx, pr1Again); err != nil {
			t.Fatalf("ProcessPullRequest pr1Again failed: %v", err)
		}
		var comp1Again v1alpha1.Comparison
		comp1AgainName := fmt.Sprintf("pr-%s-12", hash1)
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: comp1AgainName}, &comp1Again); err != nil {
			t.Fatalf("failed to get comp1Again: %v", err)
		}
		if comp1Again.Spec.Baseline.Run != expectedBase1Name {
			t.Errorf("expected pr1Again to reuse %s, got %s", expectedBase1Name, comp1Again.Spec.Baseline.Run)
		}

		// New PR in org2/repo2 must NOT reuse org1's completed baseline because repo URL differs!
		pr2Again := &PullRequestWorkItem{
			PRNumber:          22,
			HeadSHA:           "4444444555555566666667777777888888899999",
			BaseSHA:           sharedBaseSHA,
			CandidateCloneURL: "https://github.com/org2/repo2.git",
			CandidateRepo:     "org2/repo2",
			BaselineCloneURL:  "https://github.com/org2/repo2.git",
			BaselineRepo:      "org2/repo2",
			TargetRepo:        "org2/repo2",
		}
		if err := worker.ProcessPullRequest(ctx, pr2Again); err != nil {
			t.Fatalf("ProcessPullRequest pr2Again failed: %v", err)
		}
		var comp2Again v1alpha1.Comparison
		comp2AgainName := fmt.Sprintf("pr-%s-22", hash2)
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: comp2AgainName}, &comp2Again); err != nil {
			t.Fatalf("failed to get comp2Again: %v", err)
		}
		if comp2Again.Spec.Baseline.Run != expectedBase2Name {
			t.Errorf("expected pr2Again to use org2 baseline %s, but got %s", expectedBase2Name, comp2Again.Spec.Baseline.Run)
		}

		// Complete org2's baseline run and verify org2 PR now reuses base2
		base2.Status.Phase = v1alpha1.RunPhaseCompleted
		if err := cl.Status().Update(ctx, &base2); err != nil {
			t.Fatalf("failed to update base2 status: %v", err)
		}

		pr2Third := &PullRequestWorkItem{
			PRNumber:          23,
			HeadSHA:           "5555555666666677777778888888999999900000",
			BaseSHA:           sharedBaseSHA,
			CandidateCloneURL: "https://github.com/org2/repo2.git",
			CandidateRepo:     "org2/repo2",
			BaselineCloneURL:  "https://github.com/org2/repo2.git",
			BaselineRepo:      "org2/repo2",
			TargetRepo:        "org2/repo2",
		}
		if err := worker.ProcessPullRequest(ctx, pr2Third); err != nil {
			t.Fatalf("ProcessPullRequest pr2Third failed: %v", err)
		}
		var comp2Third v1alpha1.Comparison
		comp2ThirdName := fmt.Sprintf("pr-%s-23", hash2)
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: comp2ThirdName}, &comp2Third); err != nil {
			t.Fatalf("failed to get comp2Third: %v", err)
		}
		if comp2Third.Spec.Baseline.Run != expectedBase2Name {
			t.Errorf("expected pr2Third to reuse %s, got %s", expectedBase2Name, comp2Third.Spec.Baseline.Run)
		}

		// 4. Test push events in multi-repo mode produce distinct warm baselines
		push1 := &PushWorkItem{
			CommitSHA: pushSHA,
			CloneURL:  "https://github.com/org1/repo1.git",
			Repo:      "org1/repo1",
		}
		if err := worker.ProcessPush(ctx, push1); err != nil {
			t.Fatalf("ProcessPush push1 failed: %v", err)
		}
		expectedPushBase1 := fmt.Sprintf("base-%s-fedcba9", hash1)
		var pushRun1 v1alpha1.Run
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: expectedPushBase1}, &pushRun1); err != nil {
			t.Fatalf("expected push baseline %s to exist: %v", expectedPushBase1, err)
		}

		push2 := &PushWorkItem{
			CommitSHA: pushSHA,
			CloneURL:  "https://github.com/org2/repo2.git",
			Repo:      "org2/repo2",
		}
		if err := worker.ProcessPush(ctx, push2); err != nil {
			t.Fatalf("ProcessPush push2 failed: %v", err)
		}
		expectedPushBase2 := fmt.Sprintf("base-%s-fedcba9", hash2)
		var pushRun2 v1alpha1.Run
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: expectedPushBase2}, &pushRun2); err != nil {
			t.Fatalf("expected push baseline %s to exist: %v", expectedPushBase2, err)
		}
		if expectedPushBase1 == expectedPushBase2 {
			t.Errorf("expected distinct push baseline names, both were %s", expectedPushBase1)
		}

		// 5. Cleanup on PR close preserves baseline Runs
		closeItem := &PullRequestWorkItem{
			Action:        ActionClosed,
			PRNumber:      10,
			CandidateRepo: "org1/repo1",
			TargetRepo:    "org1/repo1",
		}
		if err := worker.ProcessPullRequestClosed(ctx, closeItem); err != nil {
			t.Fatalf("ProcessPullRequestClosed failed: %v", err)
		}
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: expectedBase1Name}, &base1); err != nil {
			t.Errorf("expected baseline run %s to be preserved after PR close, got %v", expectedBase1Name, err)
		}
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: expectedBase2Name}, &base2); err != nil {
			t.Errorf("expected baseline run %s to be preserved after PR close, got %v", expectedBase2Name, err)
		}
	})

	t.Run("WildcardMultiRepo_NamesAndCollisionPrevention", func(t *testing.T) {
		cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.Comparison{}, &v1alpha1.Run{}).Build()
		cfg := Config{
			Namespace:           "eval-runner",
			DefaultModel:        "mockllm/model",
			DefaultBatchSize:    4,
			WebhookRepositories: []string{"*"},
			Client:              cl,
		}
		worker := NewWorkerPool(cfg)

		hashWild := RepoHash("any-org/any-repo")
		expectedBaseName := fmt.Sprintf("base-%s-abcdef1", hashWild)

		pr := &PullRequestWorkItem{
			PRNumber:          99,
			HeadSHA:           "1111111222222233333334444444555556666666",
			BaseSHA:           sharedBaseSHA,
			CandidateCloneURL: "https://github.com/any-org/any-repo.git",
			CandidateRepo:     "any-org/any-repo",
			BaselineCloneURL:  "https://github.com/any-org/any-repo.git",
			BaselineRepo:      "any-org/any-repo",
			TargetRepo:        "any-org/any-repo",
		}
		if err := worker.ProcessPullRequest(ctx, pr); err != nil {
			t.Fatalf("ProcessPullRequest failed: %v", err)
		}

		var baseRun v1alpha1.Run
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: expectedBaseName}, &baseRun); err != nil {
			t.Fatalf("expected wildcard multi-repo baseline %s to exist: %v", expectedBaseName, err)
		}
	})
}

// TestWebhookAndSyncer_ByteIdenticalObjects verifies requirement 1:
// the same event sequence through both webhook direct path and syncer->consumer path
// produces byte-identical Runs and Comparisons (spec, labels, annotations).
func TestWebhookAndSyncer_ByteIdenticalObjects(t *testing.T) {
	ctx := t.Context()
	scheme := newTestScheme()

	repoFullName := "gke-labs/eval-runner"
	ref := forge.RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"}
	baseSHA := "abcdef1234567890abcdef1234567890abcdef12"
	headSHA1 := "1111111111111111111111111111111111111111"
	headSHA2 := "2222222222222222222222222222222222222222"
	commitTime := "2026-10-22T08:00:00Z"
	cloneURL := "https://github.com/gke-labs/eval-runner.git"
	prNum := 7

	// 1. Webhook worker path
	clWebhook := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.Comparison{}, &v1alpha1.Run{}).Build()
	cfgWebhook := Config{
		Namespace:                "eval-runner",
		DefaultModel:             "mockllm/model",
		DefaultBatchSize:         8,
		DefaultMaxSamplesPerTask: 64,
		WebhookRepositories:      []string{repoFullName},
		Client:                   clWebhook,
	}
	worker := NewWorkerPool(cfgWebhook)

	// Webhook: push
	if err := worker.ProcessPush(ctx, &PushWorkItem{
		Repo:       repoFullName,
		Branch:     "main",
		CommitSHA:  baseSHA,
		CommitTime: commitTime,
		CloneURL:   cloneURL,
	}); err != nil {
		t.Fatalf("webhook ProcessPush failed: %v", err)
	}

	// Webhook: PR #7 opened
	if err := worker.ProcessPullRequest(ctx, &PullRequestWorkItem{
		PRNumber:          prNum,
		HeadSHA:           headSHA1,
		BaseSHA:           baseSHA,
		CandidateCloneURL: cloneURL,
		CandidateRepo:     repoFullName,
		BaselineCloneURL:  cloneURL,
		BaselineRepo:      repoFullName,
		TargetRepo:        repoFullName,
	}); err != nil {
		t.Fatalf("webhook ProcessPullRequest opened failed: %v", err)
	}

	// Webhook: PR #7 synchronize
	if err := worker.ProcessPullRequest(ctx, &PullRequestWorkItem{
		PRNumber:          prNum,
		HeadSHA:           headSHA2,
		BaseSHA:           baseSHA,
		CandidateCloneURL: cloneURL,
		CandidateRepo:     repoFullName,
		BaselineCloneURL:  cloneURL,
		BaselineRepo:      repoFullName,
		TargetRepo:        repoFullName,
	}); err != nil {
		t.Fatalf("webhook ProcessPullRequest synchronize failed: %v", err)
	}

	// 2. Syncer Consumer path
	clConsumer := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.Comparison{}, &v1alpha1.Run{}).Build()
	c := consumer.New(clConsumer, consumer.Config{
		Namespace:                "eval-runner",
		DefaultModel:             "mockllm/model",
		DefaultBatchSize:         8,
		DefaultMaxSamplesPerTask: 64,
		TrackedRepos: []consumer.RepoConfig{
			{Name: repoFullName, Ref: ref, Branches: []string{"main"}},
		},
		IsMultiRepo: false,
	})

	// Consumer: push (BranchAdvanced)
	if err := c.ConsumeEvent(ctx, consumer.BranchAdvancedEvent{
		Repo:       ref,
		Branch:     "main",
		NewSHA:     baseSHA,
		CommitTime: commitTime,
		CloneURL:   cloneURL,
	}); err != nil {
		t.Fatalf("consumer BranchAdvanced failed: %v", err)
	}

	// Consumer: PR #7 opened (ChangeOpened)
	prChange := forge.ChangeRequest{
		ID:     forge.ChangeRequestID(strconv.Itoa(prNum)),
		Number: prNum,
		Title:  fmt.Sprintf("PR #%d", prNum),
		State:  "open",
		Head:   forge.ChangeRequestHead{Ref: repoFullName, SHA: headSHA1, RepoRef: ref},
		Base:   forge.ChangeRequestBase{Ref: "main", SHA: baseSHA},
	}
	if err := c.ConsumeEvent(ctx, consumer.ChangeOpenedEvent{
		Repo:              ref,
		Change:            prChange,
		CandidateCloneURL: cloneURL,
		BaselineCloneURL:  cloneURL,
	}); err != nil {
		t.Fatalf("consumer ChangeOpened failed: %v", err)
	}

	// Consumer: PR #7 synchronize (ChangeUpdated)
	prChange.Head.SHA = headSHA2
	if err := c.ConsumeEvent(ctx, consumer.ChangeUpdatedEvent{
		Repo:              ref,
		OldHeadSHA:        headSHA1,
		Change:            prChange,
		CandidateCloneURL: cloneURL,
		BaselineCloneURL:  cloneURL,
	}); err != nil {
		t.Fatalf("consumer ChangeUpdated failed: %v", err)
	}

	// 3. Compare created Runs
	var runsWebhook, runsConsumer v1alpha1.RunList
	if err := clWebhook.List(ctx, &runsWebhook, client.InNamespace("eval-runner")); err != nil {
		t.Fatalf("failed to list webhook runs: %v", err)
	}
	if err := clConsumer.List(ctx, &runsConsumer, client.InNamespace("eval-runner")); err != nil {
		t.Fatalf("failed to list consumer runs: %v", err)
	}

	if len(runsWebhook.Items) != len(runsConsumer.Items) {
		t.Fatalf("runs count mismatch: webhook has %d, consumer has %d", len(runsWebhook.Items), len(runsConsumer.Items))
	}

	runWebhookMap := make(map[string]v1alpha1.Run)
	for _, r := range runsWebhook.Items {
		runWebhookMap[r.Name] = r
	}

	for _, rConsumer := range runsConsumer.Items {
		rWebhook, ok := runWebhookMap[rConsumer.Name]
		if !ok {
			t.Fatalf("run %q exists in consumer but not in webhook", rConsumer.Name)
		}

		// Normalize TypeMeta and ResourceVersion for JSON comparison
		rWebhook.TypeMeta = metav1.TypeMeta{}
		rWebhook.ResourceVersion = ""
		rConsumer.TypeMeta = metav1.TypeMeta{}
		rConsumer.ResourceVersion = ""

		wJSON, _ := json.Marshal(rWebhook)
		cJSON, _ := json.Marshal(rConsumer)

		if string(wJSON) != string(cJSON) {
			t.Errorf("Run %s JSON mismatch:\nWebhook:  %s\nConsumer: %s", rConsumer.Name, string(wJSON), string(cJSON))
		}
	}

	// 4. Compare created Comparisons
	var compsWebhook, compsConsumer v1alpha1.ComparisonList
	if err := clWebhook.List(ctx, &compsWebhook, client.InNamespace("eval-runner")); err != nil {
		t.Fatalf("failed to list webhook comps: %v", err)
	}
	if err := clConsumer.List(ctx, &compsConsumer, client.InNamespace("eval-runner")); err != nil {
		t.Fatalf("failed to list consumer comps: %v", err)
	}

	if len(compsWebhook.Items) != len(compsConsumer.Items) {
		t.Fatalf("comparisons count mismatch: webhook has %d, consumer has %d", len(compsWebhook.Items), len(compsConsumer.Items))
	}

	compWebhookMap := make(map[string]v1alpha1.Comparison)
	for _, c := range compsWebhook.Items {
		compWebhookMap[c.Name] = c
	}

	for _, cConsumer := range compsConsumer.Items {
		cWebhook, ok := compWebhookMap[cConsumer.Name]
		if !ok {
			t.Fatalf("comparison %q exists in consumer but not in webhook", cConsumer.Name)
		}

		cWebhook.TypeMeta = metav1.TypeMeta{}
		cWebhook.ResourceVersion = ""
		cConsumer.TypeMeta = metav1.TypeMeta{}
		cConsumer.ResourceVersion = ""

		wJSON, _ := json.Marshal(cWebhook)
		cJSON, _ := json.Marshal(cConsumer)

		if string(wJSON) != string(cJSON) {
			t.Errorf("Comparison %s JSON mismatch:\nWebhook:  %s\nConsumer: %s", cConsumer.Name, string(wJSON), string(cJSON))
		}
	}
}

func TestHandler_CheckRun_RequestedAction(t *testing.T) {
	secretKey := []byte("checkrun-secret")
	scheme := newTestScheme()

	owner := "gke-labs"
	repo := "eval-runner"
	repoFullName := fmt.Sprintf("%s/%s", owner, repo)
	headSHA := "1234567890abcdef1234567890abcdef12345678"

	// Create a fake GitHub server
	fakeGH := github.NewFakeGitHubServer()
	defer fakeGH.Close()

	// Set permissions
	fakeGH.SetUserPermission(owner, repo, "write-user", "write")
	fakeGH.SetUserPermission(owner, repo, "admin-user", "admin")
	fakeGH.SetUserPermission(owner, repo, "read-user", "read")

	// Pre-create check run on fake server
	checkRunID, err := fakeGH.Client("test-token").CreateCheckRun(t.Context(), owner, repo, headSHA, github.CheckRunState{
		Name:   github.CheckRunName,
		Status: "completed",
	})
	if err != nil {
		t.Fatalf("failed to create check run: %v", err)
	}

	// Comparison labeled with target-repo and head-sha (deliberately not named by head or PR)
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "arbitrary-comp-name-999",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelTargetRepo: SanitizeLabelValue(repoFullName),
				LabelHeadSHA:    headSHA,
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: repoFullName,
				Number:     433,
				HeadSHA:    headSHA,
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(comp).WithObjects(comp).Build()

	cfg := Config{
		Namespace:           "eval-runner",
		StaticWebhookSecret: secretKey,
		WebhookRepositories: []string{repoFullName},
		Client:              cl,
		GitHubClient:        fakeGH.Client("test-token"),
	}

	server := NewServer(cfg)
	server.Start()
	defer server.Stop()

	makePayload := func(action, identifier, senderLogin string) []byte {
		var reqAction string
		if identifier != "" {
			reqAction = fmt.Sprintf(`"requested_action": {"identifier": %q},`, identifier)
		}
		return []byte(fmt.Sprintf(`{
			"action": %q,
			%s
			"check_run": {
				"id": %d,
				"name": %q,
				"head_sha": %q
			},
			"repository": {
				"name": %q,
				"full_name": %q,
				"owner": {"login": %q}
			},
			"sender": {
				"login": %q
			}
		}`, action, reqAction, checkRunID, github.CheckRunName, headSHA, repo, repoFullName, owner, senderLogin))
	}

	t.Run("RejectsBadSignature", func(t *testing.T) {
		payload := makePayload(ActionRequestedAction, github.ActionIdentifierMore32, "write-user")
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		req.Header.Set(HeaderGitHubEvent, EventCheckRun)
		req.Header.Set(HeaderGitHubDelivery, "deliv-sig-fail")
		req.Header.Set(HeaderHubSignature256, "sha256=badsignaturebadsignaturebadsignaturebadsignaturebadsignature")
		rr := httptest.NewRecorder()

		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized, got %d", rr.Code)
		}
	})

	t.Run("RejectsUnknownIdentifier", func(t *testing.T) {
		payload := makePayload(ActionRequestedAction, "unknown-action", "write-user")
		sig := ComputeSignature(payload, secretKey)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		req.Header.Set(HeaderGitHubEvent, EventCheckRun)
		req.Header.Set(HeaderGitHubDelivery, "deliv-unknown-id")
		req.Header.Set(HeaderHubSignature256, sig)
		rr := httptest.NewRecorder()

		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("expected 400 Bad Request, got %d", rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "unknown action identifier") {
			t.Errorf("expected body to mention unknown action identifier, got: %s", rr.Body.String())
		}
	})

	t.Run("RejectsSenderWithoutWriteAccess", func(t *testing.T) {
		payload := makePayload(ActionRequestedAction, github.ActionIdentifierMore32, "read-user")
		sig := ComputeSignature(payload, secretKey)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		req.Header.Set(HeaderGitHubEvent, EventCheckRun)
		req.Header.Set(HeaderGitHubDelivery, "deliv-read-only")
		req.Header.Set(HeaderHubSignature256, sig)
		rr := httptest.NewRecorder()

		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusForbidden {
			t.Errorf("expected 403 Forbidden, got %d", rr.Code)
		}
	})

	t.Run("RejectsBotSender", func(t *testing.T) {
		payload := makePayload(ActionRequestedAction, github.ActionIdentifierMore32, "renovate[bot]")
		sig := ComputeSignature(payload, secretKey)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		req.Header.Set(HeaderGitHubEvent, EventCheckRun)
		req.Header.Set(HeaderGitHubDelivery, "deliv-bot")
		req.Header.Set(HeaderHubSignature256, sig)
		rr := httptest.NewRecorder()

		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusForbidden {
			t.Errorf("expected 403 Forbidden, got %d", rr.Code)
		}
	})

	t.Run("RejectsBotType", func(t *testing.T) {
		payload := []byte(fmt.Sprintf(`{
			"action": %q,
			"requested_action": {"identifier": %q},
			"check_run": {
				"id": %d,
				"name": %q,
				"head_sha": %q
			},
			"repository": {
				"name": %q,
				"full_name": %q,
				"owner": {"login": %q}
			},
			"sender": {
				"login": "some-bot",
				"type": "Bot"
			}
		}`, ActionRequestedAction, github.ActionIdentifierMore32, checkRunID, github.CheckRunName, headSHA, repo, repoFullName, owner))
		sig := ComputeSignature(payload, secretKey)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		req.Header.Set(HeaderGitHubEvent, EventCheckRun)
		req.Header.Set(HeaderGitHubDelivery, "deliv-bot-type")
		req.Header.Set(HeaderHubSignature256, sig)
		rr := httptest.NewRecorder()

		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusForbidden {
			t.Errorf("expected 403 Forbidden, got %d", rr.Code)
		}
	})

	t.Run("RejectsInvalidCommitSHA", func(t *testing.T) {
		payload := []byte(fmt.Sprintf(`{
			"action": %q,
			"requested_action": {"identifier": %q},
			"check_run": {
				"id": %d,
				"name": %q,
				"head_sha": "not-a-valid-sha"
			},
			"repository": {
				"name": %q,
				"full_name": %q,
				"owner": {"login": %q}
			},
			"sender": {
				"login": "write-user"
			}
		}`, ActionRequestedAction, github.ActionIdentifierMore32, checkRunID, github.CheckRunName, repo, repoFullName, owner))
		sig := ComputeSignature(payload, secretKey)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		req.Header.Set(HeaderGitHubEvent, EventCheckRun)
		req.Header.Set(HeaderGitHubDelivery, "deliv-invalid-sha")
		req.Header.Set(HeaderHubSignature256, sig)
		rr := httptest.NewRecorder()

		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("expected 400 Bad Request for invalid commit SHA, got %d", rr.Code)
		}
	})

	t.Run("ComparisonNotFoundReturns404BeforePermissionCheck", func(t *testing.T) {
		unknownSHA := "ffffffffffffffffffffffffffffffffffffffff"
		payload := []byte(fmt.Sprintf(`{
			"action": %q,
			"requested_action": {"identifier": %q},
			"check_run": {
				"id": %d,
				"name": %q,
				"head_sha": %q
			},
			"repository": {
				"name": %q,
				"full_name": %q,
				"owner": {"login": %q}
			},
			"sender": {
				"login": "write-user"
			}
		}`, ActionRequestedAction, github.ActionIdentifierMore32, checkRunID, github.CheckRunName, unknownSHA, repo, repoFullName, owner))
		sig := ComputeSignature(payload, secretKey)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		req.Header.Set(HeaderGitHubEvent, EventCheckRun)
		req.Header.Set(HeaderGitHubDelivery, "deliv-unknown-comp")
		req.Header.Set(HeaderHubSignature256, sig)
		rr := httptest.NewRecorder()

		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Errorf("expected 404 Not Found for unknown comparison, got %d", rr.Code)
		}
	})

	t.Run("AcceptsMore32_AppendsPendingCommand_UpdatesCheckRun", func(t *testing.T) {
		payload := makePayload(ActionRequestedAction, github.ActionIdentifierMore32, "write-user")
		sig := ComputeSignature(payload, secretKey)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		req.Header.Set(HeaderGitHubEvent, EventCheckRun)
		req.Header.Set(HeaderGitHubDelivery, "deliv-more-32")
		req.Header.Set(HeaderHubSignature256, sig)
		rr := httptest.NewRecorder()

		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}, &updatedComp); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if len(updatedComp.Status.PendingCommands) != 1 {
			t.Fatalf("expected 1 pending command, got %d", len(updatedComp.Status.PendingCommands))
		}
		cmd := updatedComp.Status.PendingCommands[0]
		if cmd.Command != "more 32" {
			t.Errorf("expected command 'more 32', got %q", cmd.Command)
		}
		if cmd.Requested != 32 {
			t.Errorf("expected requested samples 32, got %d", cmd.Requested)
		}
		if cmd.RequestedBy != "write-user" || cmd.Login != "write-user" {
			t.Errorf("expected requestedBy/login 'write-user', got requestedBy=%q, login=%q", cmd.RequestedBy, cmd.Login)
		}
		if cmd.CheckRunID != checkRunID {
			t.Errorf("expected checkRunId %d, got %d", checkRunID, cmd.CheckRunID)
		}
		if cmd.DeliveryID != "deliv-more-32" {
			t.Errorf("expected deliveryId 'deliv-more-32', got %q", cmd.DeliveryID)
		}

		// Verify Check Run updated to in_progress with "requested by @user"
		cr := fakeGH.GetCheckRun(checkRunID)
		if cr == nil {
			t.Fatalf("check run %d not found on fake server", checkRunID)
		}
		if cr.Status != "in_progress" {
			t.Errorf("expected check run status 'in_progress', got %q", cr.Status)
		}
		if !strings.Contains(cr.Summary, "requested by @write-user") && !strings.Contains(cr.Title, "requested by @write-user") {
			t.Errorf("expected check run title/summary to contain 'requested by @write-user', title=%q summary=%q", cr.Title, cr.Summary)
		}
	})

	t.Run("IdempotentOnRedelivery", func(t *testing.T) {
		// Re-send same delivery ID
		payload := makePayload(ActionRequestedAction, github.ActionIdentifierMore32, "write-user")
		sig := ComputeSignature(payload, secretKey)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		req.Header.Set(HeaderGitHubEvent, EventCheckRun)
		req.Header.Set(HeaderGitHubDelivery, "deliv-more-32")
		req.Header.Set(HeaderHubSignature256, sig)
		rr := httptest.NewRecorder()

		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 OK on redelivery, got %d", rr.Code)
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}, &updatedComp); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		// Still only 1 pending command
		if len(updatedComp.Status.PendingCommands) != 1 {
			t.Errorf("expected pending commands to remain 1 after redelivery, got %d", len(updatedComp.Status.PendingCommands))
		}
	})

	t.Run("AcceptsRerunBase", func(t *testing.T) {
		payload := makePayload(ActionRequestedAction, github.ActionIdentifierRerunBase, "admin-user")
		sig := ComputeSignature(payload, secretKey)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		req.Header.Set(HeaderGitHubEvent, EventCheckRun)
		req.Header.Set(HeaderGitHubDelivery, "deliv-rerun-base")
		req.Header.Set(HeaderHubSignature256, sig)
		rr := httptest.NewRecorder()

		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}, &updatedComp); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if len(updatedComp.Status.PendingCommands) != 2 {
			t.Fatalf("expected 2 pending commands, got %d", len(updatedComp.Status.PendingCommands))
		}
		cmd := updatedComp.Status.PendingCommands[1]
		if cmd.Command != "rerun baseline" {
			t.Errorf("expected command 'rerun baseline', got %q", cmd.Command)
		}
		if cmd.RequestedBy != "admin-user" {
			t.Errorf("expected requestedBy 'admin-user', got %q", cmd.RequestedBy)
		}
	})

	t.Run("AcceptsRerunCand", func(t *testing.T) {
		payload := makePayload(ActionRequestedAction, github.ActionIdentifierRerunCand, "admin-user")
		sig := ComputeSignature(payload, secretKey)
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		req.Header.Set(HeaderGitHubEvent, EventCheckRun)
		req.Header.Set(HeaderGitHubDelivery, "deliv-rerun-cand")
		req.Header.Set(HeaderHubSignature256, sig)
		rr := httptest.NewRecorder()

		server.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}, &updatedComp); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if len(updatedComp.Status.PendingCommands) != 3 {
			t.Fatalf("expected 3 pending commands, got %d", len(updatedComp.Status.PendingCommands))
		}
		cmd := updatedComp.Status.PendingCommands[2]
		if cmd.Command != "rerun candidate" {
			t.Errorf("expected command 'rerun candidate', got %q", cmd.Command)
		}
		if cmd.RequestedBy != "admin-user" {
			t.Errorf("expected requestedBy 'admin-user', got %q", cmd.RequestedBy)
		}
	})
}
