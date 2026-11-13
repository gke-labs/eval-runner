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
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/internal/fakes"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/artifacts/gcs"
	"github.com/gke-labs/eval-runner/pkg/artifacts/sinkconfig"
	"github.com/gke-labs/eval-runner/pkg/configkeys"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/gke-labs/eval-runner/pkg/forge"
	"github.com/gke-labs/eval-runner/pkg/forge/consumer"
	"github.com/gke-labs/eval-runner/pkg/forge/store"
	"github.com/gke-labs/eval-runner/pkg/forge/syncer"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestResolveConfig_Precedence(t *testing.T) {
	builtInDefaults := DefaultClusterConfig()

	// 1. All defaults: nil ConfigMap, no explicit flags
	t.Run("nil ConfigMap uses defaults", func(t *testing.T) {
		cfg, err := ResolveConfig(nil, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(cfg, builtInDefaults) {
			t.Errorf("expected %+v, got %+v", builtInDefaults, cfg)
		}
	})

	// 2. Empty ConfigMap uses defaults
	t.Run("empty ConfigMap uses defaults", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data:       map[string]string{},
		}
		cfg, err := ResolveConfig(cm, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(cfg, builtInDefaults) {
			t.Errorf("expected %+v, got %+v", builtInDefaults, cfg)
		}
	})

	// 3. ConfigMap overrides built-in defaults when flags are not set
	t.Run("ConfigMap overrides built-in defaults", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data: map[string]string{
				"artifactBucket":    "gs://cluster-bucket",
				"discoveryDeadline": "25m",
				"executionDeadline": "45m",
				"maxParallelTasks":  "8",
				"modelSecret":       "cluster-secret",
				"taskGracePeriod":   "1m",
			},
		}
		cfg, err := ResolveConfig(cm, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ArtifactBucket != "cluster-bucket" && cfg.ArtifactBucket != "gs://cluster-bucket" {
			// normalized or as-is: trimmed "gs://cluster-bucket" is stored
			if cfg.ArtifactBucket != "gs://cluster-bucket" {
				t.Errorf("expected ArtifactBucket gs://cluster-bucket, got %q", cfg.ArtifactBucket)
			}
		}
		if cfg.DiscoveryDeadline != 25*time.Minute {
			t.Errorf("expected DiscoveryDeadline 25m, got %v", cfg.DiscoveryDeadline)
		}
		if cfg.ExecutionDeadline != 45*time.Minute {
			t.Errorf("expected ExecutionDeadline 45m, got %v", cfg.ExecutionDeadline)
		}
		if cfg.MaxParallelTasks != 8 {
			t.Errorf("expected MaxParallelTasks 8, got %d", cfg.MaxParallelTasks)
		}
		if cfg.ModelSecret != "cluster-secret" {
			t.Errorf("expected ModelSecret cluster-secret, got %q", cfg.ModelSecret)
		}
		if cfg.TaskGracePeriod != time.Minute {
			t.Errorf("expected TaskGracePeriod 1m, got %v", cfg.TaskGracePeriod)
		}
	})

	// 4. Explicit flag set to its default value overrides ConfigMap
	t.Run("explicit flag set to default value overrides ConfigMap", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data: map[string]string{
				"maxParallelTasks":  "8",
				"discoveryDeadline": "25m",
				"modelSecret":       "custom-secret",
			},
		}
		// Explicitly pass flags with values identical to built-in defaults:
		flags := RawFlags{
			MaxParallelTasks:  DefaultMaxParallelTasks,  // 4
			DiscoveryDeadline: DefaultDiscoveryDeadline, // 10m
			ModelSecret:       DefaultModelSecret,       // "model-credentials"
			Explicit: map[string]bool{
				"max-parallel-tasks": true,
				"discovery-deadline": true,
				"model-secret":       true,
			},
		}
		cfg, err := ResolveConfig(cm, flags)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.MaxParallelTasks != DefaultMaxParallelTasks {
			t.Errorf("expected MaxParallelTasks %d from explicit flag, got %d", DefaultMaxParallelTasks, cfg.MaxParallelTasks)
		}
		if cfg.DiscoveryDeadline != DefaultDiscoveryDeadline {
			t.Errorf("expected DiscoveryDeadline %v from explicit flag, got %v", DefaultDiscoveryDeadline, cfg.DiscoveryDeadline)
		}
		if cfg.ModelSecret != DefaultModelSecret {
			t.Errorf("expected ModelSecret %q from explicit flag, got %q", DefaultModelSecret, cfg.ModelSecret)
		}
	})

	// 5. Explicit flags override ConfigMap
	t.Run("explicit flags override ConfigMap", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data: map[string]string{
				"artifactBucket":    "gs://cluster-bucket",
				"discoveryDeadline": "25m",
				"executionDeadline": "45m",
				"maxParallelTasks":  "8",
				"modelSecret":       "cluster-secret",
				"taskGracePeriod":   "1m",
			},
		}
		flags := RawFlags{
			ArtifactBucket:    "gs://flag-bucket",
			DiscoveryDeadline: 5 * time.Minute,
			ExecutionDeadline: 15 * time.Minute,
			MaxParallelTasks:  2,
			ModelSecret:       "flag-secret",
			TaskGracePeriod:   10 * time.Second,
			Explicit: map[string]bool{
				"artifact-bucket":    true,
				"discovery-deadline": true,
				"execution-deadline": true,
				"max-parallel-tasks": true,
				"model-secret":       true,
				"task-grace-period":  true,
			},
		}
		cfg, err := ResolveConfig(cm, flags)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ArtifactBucket != "gs://flag-bucket" {
			t.Errorf("expected ArtifactBucket gs://flag-bucket, got %q", cfg.ArtifactBucket)
		}
		if cfg.DiscoveryDeadline != 5*time.Minute {
			t.Errorf("expected DiscoveryDeadline 5m, got %v", cfg.DiscoveryDeadline)
		}
		if cfg.ExecutionDeadline != 15*time.Minute {
			t.Errorf("expected ExecutionDeadline 15m, got %v", cfg.ExecutionDeadline)
		}
		if cfg.MaxParallelTasks != 2 {
			t.Errorf("expected MaxParallelTasks 2, got %d", cfg.MaxParallelTasks)
		}
		if cfg.ModelSecret != "flag-secret" {
			t.Errorf("expected ModelSecret flag-secret, got %q", cfg.ModelSecret)
		}
		if cfg.TaskGracePeriod != 10*time.Second {
			t.Errorf("expected TaskGracePeriod 10s, got %v", cfg.TaskGracePeriod)
		}
	})

	// 6. Explicitly empty flag overrides ConfigMap
	t.Run("explicitly empty flag overrides ConfigMap", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data: map[string]string{
				"artifactBucket": "gs://cluster-bucket",
				"modelSecret":    "cluster-secret",
			},
		}
		flags := RawFlags{
			ArtifactBucket: "",
			ModelSecret:    "",
			Explicit: map[string]bool{
				"artifact-bucket": true,
				"model-secret":    true,
			},
		}
		cfg, err := ResolveConfig(cm, flags)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ArtifactBucket != "" {
			t.Errorf("expected empty ArtifactBucket from explicit flag, got %q", cfg.ArtifactBucket)
		}
		if cfg.ModelSecret != "" {
			t.Errorf("expected empty ModelSecret from explicit flag, got %q", cfg.ModelSecret)
		}
	})

	// 7. Partial mixing: flag overrides one, ConfigMap sets another, third uses default
	t.Run("partial mixing of flags, ConfigMap, and defaults", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data: map[string]string{
				"artifactBucket":   "gs://cm-bucket",
				"maxParallelTasks": "12",
			},
		}
		flags := RawFlags{
			DiscoveryDeadline: 7 * time.Minute,
			Explicit: map[string]bool{
				"discovery-deadline": true,
			},
		}
		cfg, err := ResolveConfig(cm, flags)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// From flag:
		if cfg.DiscoveryDeadline != 7*time.Minute {
			t.Errorf("expected DiscoveryDeadline from flag 7m, got %v", cfg.DiscoveryDeadline)
		}
		// From ConfigMap:
		if cfg.ArtifactBucket != "gs://cm-bucket" {
			t.Errorf("expected ArtifactBucket from ConfigMap gs://cm-bucket, got %q", cfg.ArtifactBucket)
		}
		if cfg.MaxParallelTasks != 12 {
			t.Errorf("expected MaxParallelTasks from ConfigMap 12, got %d", cfg.MaxParallelTasks)
		}
		// From defaults:
		if cfg.ExecutionDeadline != DefaultExecutionDeadline {
			t.Errorf("expected ExecutionDeadline default %v, got %v", DefaultExecutionDeadline, cfg.ExecutionDeadline)
		}
		if cfg.ModelSecret != DefaultModelSecret {
			t.Errorf("expected ModelSecret default %q, got %q", DefaultModelSecret, cfg.ModelSecret)
		}
		if cfg.TaskGracePeriod != DefaultTaskGracePeriod {
			t.Errorf("expected TaskGracePeriod default %v, got %v", DefaultTaskGracePeriod, cfg.TaskGracePeriod)
		}
	})
}

func TestResolveConfig_MalformedValue(t *testing.T) {
	testCases := []struct {
		name        string
		key         string
		value       string
		errContains string
	}{
		{
			name:        "invalid discoveryDeadline duration format",
			key:         "discoveryDeadline",
			value:       "not-a-duration",
			errContains: "invalid discoveryDeadline",
		},
		{
			name:        "negative discoveryDeadline",
			key:         "discoveryDeadline",
			value:       "-5m",
			errContains: "duration must be positive",
		},
		{
			name:        "zero discoveryDeadline",
			key:         "discoveryDeadline",
			value:       "0s",
			errContains: "duration must be positive",
		},
		{
			name:        "invalid executionDeadline duration format",
			key:         "executionDeadline",
			value:       "bad-time",
			errContains: "invalid executionDeadline",
		},
		{
			name:        "negative executionDeadline",
			key:         "executionDeadline",
			value:       "-10s",
			errContains: "duration must be positive",
		},
		{
			name:        "invalid maxParallelTasks non-numeric",
			key:         "maxParallelTasks",
			value:       "four",
			errContains: "invalid maxParallelTasks",
		},
		{
			name:        "zero maxParallelTasks",
			key:         "maxParallelTasks",
			value:       "0",
			errContains: "must be positive",
		},
		{
			name:        "negative maxParallelTasks",
			key:         "maxParallelTasks",
			value:       "-3",
			errContains: "must be positive",
		},
		{
			name:        "invalid pollProgressInterval duration format",
			key:         "pollProgressInterval",
			value:       "not-a-duration",
			errContains: "invalid pollProgressInterval",
		},
		{
			name:        "sub-second pollProgressInterval",
			key:         "pollProgressInterval",
			value:       "500ms",
			errContains: "must be at least 1s",
		},
		{
			name:        "zero pollProgressInterval",
			key:         "pollProgressInterval",
			value:       "0s",
			errContains: "must be at least 1s",
		},
		{
			name:        "negative pollProgressInterval",
			key:         "pollProgressInterval",
			value:       "-5s",
			errContains: "must be at least 1s",
		},
		{
			name:        "invalid artifactBucket with subpath",
			key:         "artifactBucket",
			value:       "gs://bucket/with/subpath",
			errContains: "invalid artifactBucket",
		},
		{
			name:        "invalid artifactBucket with invalid chars",
			key:         "artifactBucket",
			value:       "bucket..invalid",
			errContains: "invalid artifactBucket",
		},
		{
			name:        "invalid artifactBucket with whitespace in name",
			key:         "artifactBucket",
			value:       "bucket with space",
			errContains: "invalid artifactBucket",
		},
		{
			name:        "invalid modelSecret with uppercase",
			key:         "modelSecret",
			value:       "INVALID_SECRET_NAME",
			errContains: "invalid modelSecret",
		},
		{
			name:        "invalid modelSecret with special chars",
			key:         "modelSecret",
			value:       "secret/name!",
			errContains: "invalid modelSecret",
		},
		{
			name:        "invalid taskGracePeriod duration format",
			key:         "taskGracePeriod",
			value:       "xyz",
			errContains: "invalid taskGracePeriod",
		},
		{
			name:        "zero taskGracePeriod",
			key:         "taskGracePeriod",
			value:       "0s",
			errContains: "duration must be positive",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
				Data: map[string]string{
					tc.key: tc.value,
				},
			}
			_, err := ResolveConfig(cm, RawFlags{})
			if err == nil {
				t.Fatalf("expected error for %s=%q, got nil", tc.key, tc.value)
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Errorf("expected error containing %q, got %q", tc.errContains, err.Error())
			}
		})
	}
}

func TestLoadAndResolveClusterConfig_MissingConfigMap(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}

	// Empty client - no ConfigMap exists
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	cfg, err := LoadAndResolveClusterConfig(
		t.Context(),
		fakeClient,
		"eval-runner",
		DefaultConfigMapName,
		RawFlags{},
		logr.Discard(),
	)
	if err != nil {
		t.Fatalf("missing ConfigMap should not produce error: %v", err)
	}

	expected := DefaultClusterConfig()
	if !reflect.DeepEqual(cfg, expected) {
		t.Errorf("expected default config %+v, got %+v", expected, cfg)
	}
}

func TestLoadAndResolveClusterConfig_PresentConfigMap(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			"artifactBucket":    "gs://my-cluster-bucket",
			"discoveryDeadline": "15m",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()

	cfg, err := LoadAndResolveClusterConfig(
		t.Context(),
		fakeClient,
		"eval-runner",
		DefaultConfigMapName,
		RawFlags{},
		logr.Discard(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ArtifactBucket != "gs://my-cluster-bucket" {
		t.Errorf("expected ArtifactBucket gs://my-cluster-bucket, got %q", cfg.ArtifactBucket)
	}
	if cfg.DiscoveryDeadline != 15*time.Minute {
		t.Errorf("expected DiscoveryDeadline 15m, got %v", cfg.DiscoveryDeadline)
	}
	if cfg.ExecutionDeadline != DefaultExecutionDeadline {
		t.Errorf("expected default ExecutionDeadline, got %v", cfg.ExecutionDeadline)
	}
}

type errorReader struct {
	err error
}

func (e *errorReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return e.err
}

func (e *errorReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return e.err
}

func TestLoadClusterConfig_ReaderError(t *testing.T) {
	injectedErr := errors.New("connection refused")
	errReader := &errorReader{err: injectedErr}

	_, err := LoadClusterConfig(t.Context(), errReader, "eval-runner", DefaultConfigMapName)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("expected error containing connection refused, got %v", err)
	}
}

func TestFlagsFromFlagSet(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	bucket := fs.String("artifact-bucket", "", "artifact bucket")
	dir := fs.String("artifact-dir", "", "artifact dir")
	discovery := fs.Duration("discovery-deadline", 10*time.Minute, "discovery deadline")
	execution := fs.Duration("execution-deadline", 30*time.Minute, "execution deadline")
	parallel := fs.Int("max-parallel-tasks", 4, "max parallel tasks")
	secret := fs.String("model-secret", "model-credentials", "model secret")
	sandboxd := fs.String("sandboxd-image", "", "sandboxd image")
	grace := fs.Duration("task-grace-period", 30*time.Second, "task grace period")
	githubToken := fs.String("github-token-secret", "github-token", "github token secret")
	githubApp := fs.String("github-app-secret", "", "github app secret")
	uiURL := fs.String("ui-base-url", "", "ui base url")
	strictConfig := fs.Bool("strict-config", false, "strict config")
	uploadBytes := fs.Int64("artifact-max-upload-bytes", 10*1024*1024, "artifact max upload bytes")
	inconclusivePolicy := fs.String("inconclusive-policy", DefaultInconclusivePolicy, "inconclusive policy")
	compTTL := fs.Duration("comparison-ttl", 0, "comparison ttl")
	commandUsers := fs.String("command-users", "", "command users")
	pollInterval := fs.Duration("poll-progress-interval", 10*time.Second, "poll progress interval")
	affinityMode := fs.String("affinity-mode", configkeys.AffinityModeOrder, "affinity mode")
	sandboxEgressHosts := fs.String("sandbox-egress-hosts", "", "sandbox egress hosts")

	// Parse with only artifact-bucket, max-parallel-tasks, ui-base-url, strict-config, artifact-max-upload-bytes, inconclusive-policy, comparison-ttl set
	err := fs.Parse([]string{"--artifact-bucket=gs://custom", "--max-parallel-tasks=10", "--sandboxd-image=my-sandboxd:tag", "--github-token-secret=my-gh-secret", "--ui-base-url=https://eval.example.com", "--strict-config", "--artifact-max-upload-bytes=5242880", "--inconclusive-policy=failure", "--comparison-ttl=24h", "--poll-progress-interval=2s", "--sandbox-egress-hosts=github.com,*.googleapis.com"})
	if err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	rawFlags := FlagsFromFlagSet(fs, *bucket, *dir, *secret, *sandboxd, *githubToken, *githubApp, *uiURL, *discovery, *execution, *grace, *parallel, *strictConfig, *uploadBytes, *inconclusivePolicy, *compTTL, *commandUsers, *pollInterval, *affinityMode, *sandboxEgressHosts)
	if !rawFlags.IsExplicit("artifact-bucket") {
		t.Errorf("expected artifact-bucket to be explicit")
	}
	if !rawFlags.IsExplicit("sandbox-egress-hosts") {
		t.Errorf("expected sandbox-egress-hosts to be explicit")
	}
	if !rawFlags.IsExplicit("inconclusive-policy") {
		t.Errorf("expected inconclusive-policy to be explicit")
	}
	if !rawFlags.IsExplicit("max-parallel-tasks") {
		t.Errorf("expected max-parallel-tasks to be explicit")
	}
	if !rawFlags.IsExplicit("sandboxd-image") {
		t.Errorf("expected sandboxd-image to be explicit")
	}
	if !rawFlags.IsExplicit("github-token-secret") {
		t.Errorf("expected github-token-secret to be explicit")
	}
	if !rawFlags.IsExplicit("ui-base-url") {
		t.Errorf("expected ui-base-url to be explicit")
	}
	if !rawFlags.IsExplicit("strict-config") {
		t.Errorf("expected strict-config to be explicit")
	}
	if !rawFlags.StrictConfig {
		t.Errorf("expected StrictConfig to be true")
	}
	if !rawFlags.IsExplicit("artifact-max-upload-bytes") {
		t.Errorf("expected artifact-max-upload-bytes to be explicit")
	}
	if rawFlags.ArtifactMaxUploadBytes != 5242880 {
		t.Errorf("expected ArtifactMaxUploadBytes 5242880, got %d", rawFlags.ArtifactMaxUploadBytes)
	}
	if !rawFlags.IsExplicit("comparison-ttl") {
		t.Errorf("expected comparison-ttl to be explicit")
	}
	if rawFlags.ComparisonTTL != 24*time.Hour {
		t.Errorf("expected ComparisonTTL 24h, got %v", rawFlags.ComparisonTTL)
	}
	if !rawFlags.IsExplicit("poll-progress-interval") {
		t.Errorf("expected poll-progress-interval to be explicit")
	}
	if rawFlags.PollProgressInterval != 2*time.Second {
		t.Errorf("expected PollProgressInterval 2s, got %v", rawFlags.PollProgressInterval)
	}
	if rawFlags.IsExplicit("github-app-secret") {
		t.Errorf("expected github-app-secret NOT to be explicit")
	}
	if rawFlags.IsExplicit("discovery-deadline") {
		t.Errorf("expected discovery-deadline NOT to be explicit")
	}
	if rawFlags.IsExplicit("model-secret") {
		t.Errorf("expected model-secret NOT to be explicit")
	}
}

func TestResolveConfig_UIBaseURL(t *testing.T) {
	// 1. Built-in default
	defCfg := DefaultClusterConfig()
	if defCfg.UIBaseURL != "" {
		t.Errorf("expected default UIBaseURL empty, got %q", defCfg.UIBaseURL)
	}

	// 2. ConfigMap override with trailing slash normalization
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			ConfigMapKeyUIBaseURL: "https://eval.example.com/",
		},
	}
	cfg, err := ResolveConfig(cm, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.UIBaseURL != "https://eval.example.com" {
		t.Errorf("expected UIBaseURL %q, got %q", "https://eval.example.com", cfg.UIBaseURL)
	}

	// 3. Flag override takes precedence
	flags := RawFlags{
		UIBaseURL: "https://override.example.com",
		Explicit:  map[string]bool{"ui-base-url": true},
	}
	cfgWithFlag, err := ResolveConfig(cm, flags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgWithFlag.UIBaseURL != "https://override.example.com" {
		t.Errorf("expected UIBaseURL %q, got %q", "https://override.example.com", cfgWithFlag.UIBaseURL)
	}

	// 4. Invalid protocol returns error
	invalidCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			ConfigMapKeyUIBaseURL: "ftp://eval.example.com",
		},
	}
	_, err = ResolveConfig(invalidCM, RawFlags{})
	if err == nil {
		t.Fatalf("expected error for non-http(s) UIBaseURL, got nil")
	}
}

func TestResolveConfig_SandboxPortalPolicy(t *testing.T) {
	if def := DefaultClusterConfig().SandboxPortalPolicy; def != "" {
		t.Errorf("expected default SandboxPortalPolicy empty, got %q", def)
	}
	cm := &corev1.ConfigMap{Data: map[string]string{ConfigMapKeySandboxPortalPolicy: "*"}}
	if cfg, err := ResolveConfig(cm, RawFlags{}); err != nil || cfg.SandboxPortalPolicy != "*" {
		t.Errorf("expected SandboxPortalPolicy '*', got %q (err: %v)", cfg.SandboxPortalPolicy, err)
	}
	cmNone := &corev1.ConfigMap{Data: map[string]string{ConfigMapKeySandboxPortalPolicy: "none"}}
	if cfg, err := ResolveConfig(cmNone, RawFlags{}); err != nil || cfg.SandboxPortalPolicy != "" {
		t.Errorf("expected empty SandboxPortalPolicy for 'none', got %q (err: %v)", cfg.SandboxPortalPolicy, err)
	}
}

func TestResolveConfig_InconclusivePolicy(t *testing.T) {
	// 1. Built-in default is "neutral"
	defCfg := DefaultClusterConfig()
	if defCfg.InconclusivePolicy != configkeys.InconclusivePolicyNeutral {
		t.Errorf("expected default InconclusivePolicy %q, got %q", configkeys.InconclusivePolicyNeutral, defCfg.InconclusivePolicy)
	}

	// 2. ConfigMap override with "failure"
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			ConfigMapKeyInconclusivePolicy: "failure",
		},
	}
	cfg, err := ResolveConfig(cm, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.InconclusivePolicy != configkeys.InconclusivePolicyFailure {
		t.Errorf("expected InconclusivePolicy %q, got %q", configkeys.InconclusivePolicyFailure, cfg.InconclusivePolicy)
	}

	// 3. Case insensitivity ("FAILURE")
	cmUpper := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			ConfigMapKeyInconclusivePolicy: "FAILURE",
		},
	}
	cfgUpper, err := ResolveConfig(cmUpper, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgUpper.InconclusivePolicy != configkeys.InconclusivePolicyFailure {
		t.Errorf("expected InconclusivePolicy %q, got %q", configkeys.InconclusivePolicyFailure, cfgUpper.InconclusivePolicy)
	}

	// 4. Explicit flag override takes precedence over ConfigMap
	flags := RawFlags{
		InconclusivePolicy: "neutral",
		Explicit:           map[string]bool{"inconclusive-policy": true},
	}
	cfgWithFlag, err := ResolveConfig(cm, flags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgWithFlag.InconclusivePolicy != configkeys.InconclusivePolicyNeutral {
		t.Errorf("expected InconclusivePolicy %q, got %q", configkeys.InconclusivePolicyNeutral, cfgWithFlag.InconclusivePolicy)
	}

	// 5. Empty or whitespace value in ConfigMap retains default
	cmEmpty := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			ConfigMapKeyInconclusivePolicy: "   ",
		},
	}
	cfgEmpty, err := ResolveConfig(cmEmpty, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgEmpty.InconclusivePolicy != configkeys.InconclusivePolicyNeutral {
		t.Errorf("expected default InconclusivePolicy %q for whitespace, got %q", configkeys.InconclusivePolicyNeutral, cfgEmpty.InconclusivePolicy)
	}

	// 6. Unknown value in ConfigMap rejected with clear error
	cmInvalid := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			ConfigMapKeyInconclusivePolicy: "unknown",
		},
	}
	_, err = ResolveConfig(cmInvalid, RawFlags{})
	if err == nil {
		t.Fatalf("expected error for unknown inconclusivePolicy in ConfigMap, got nil")
	}
	expectedSubstr := "invalid inconclusivePolicy \"unknown\" in ConfigMap eval-runner/eval-runner-config: must be \"neutral\" or \"failure\""
	if !strings.Contains(err.Error(), expectedSubstr) {
		t.Errorf("expected error containing %q, got %q", expectedSubstr, err.Error())
	}

	// 7. Unknown value in explicit flag rejected with clear error
	invalidFlags := RawFlags{
		InconclusivePolicy: "invalid",
		Explicit:           map[string]bool{"inconclusive-policy": true},
	}
	_, err = ResolveConfig(nil, invalidFlags)
	if err == nil {
		t.Fatalf("expected error for unknown inconclusivePolicy flag, got nil")
	}
	expectedFlagSubstr := "invalid --inconclusive-policy \"invalid\": must be \"neutral\" or \"failure\""
	if !strings.Contains(err.Error(), expectedFlagSubstr) {
		t.Errorf("expected error containing %q, got %q", expectedFlagSubstr, err.Error())
	}
}

func TestResolveConfig_GCPWorkloadIdentityProvider(t *testing.T) {
	// 1. Built-in default is empty
	defCfg := DefaultClusterConfig()
	if defCfg.GCPWorkloadIdentityProvider != "" {
		t.Errorf("expected default GCPWorkloadIdentityProvider empty, got %q", defCfg.GCPWorkloadIdentityProvider)
	}

	const provider = "//iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/my-pool/providers/my-provider"

	// 2. ConfigMap override (trimmed)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			ConfigMapKeyGCPWorkloadIdentityProvider: "  " + provider + "  ",
		},
	}
	cfg, err := ResolveConfig(cm, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GCPWorkloadIdentityProvider != provider {
		t.Errorf("expected GCPWorkloadIdentityProvider %q, got %q", provider, cfg.GCPWorkloadIdentityProvider)
	}
}

func TestResolveConfig_GitHubAppSecret(t *testing.T) {
	// 1. Built-in default
	defCfg := DefaultClusterConfig()
	if defCfg.GitHubAppSecret != "" {
		t.Errorf("expected default GitHubAppSecret empty, got %q", defCfg.GitHubAppSecret)
	}

	// 2. ConfigMap override
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			ConfigMapKeyGitHubAppSecret: "custom-app-secret",
		},
	}
	cfg, err := ResolveConfig(cm, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GitHubAppSecret != "custom-app-secret" {
		t.Errorf("expected GitHubAppSecret %q, got %q", "custom-app-secret", cfg.GitHubAppSecret)
	}

	// 3. Explicit flag override takes precedence
	flags := RawFlags{
		GitHubAppSecret: "flag-app-secret",
		Explicit:        map[string]bool{"github-app-secret": true},
	}
	cfgWithFlag, err := ResolveConfig(cm, flags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgWithFlag.GitHubAppSecret != "flag-app-secret" {
		t.Errorf("expected GitHubAppSecret %q, got %q", "flag-app-secret", cfgWithFlag.GitHubAppSecret)
	}
}

func TestResolveConfig_GitHubTokenSecret(t *testing.T) {
	// 1. Built-in default
	defCfg := DefaultClusterConfig()
	if defCfg.GitHubTokenSecret != DefaultGitHubTokenSecret {
		t.Errorf("expected default GitHubTokenSecret %q, got %q", DefaultGitHubTokenSecret, defCfg.GitHubTokenSecret)
	}

	// 2. ConfigMap override
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			ConfigMapKeyGitHubTokenSecret: "custom-token-secret",
		},
	}
	cfg, err := ResolveConfig(cm, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GitHubTokenSecret != "custom-token-secret" {
		t.Errorf("expected GitHubTokenSecret %q, got %q", "custom-token-secret", cfg.GitHubTokenSecret)
	}

	// 3. Explicit flag override takes precedence over ConfigMap
	flags := RawFlags{
		GitHubTokenSecret: "flag-token-secret",
		Explicit:          map[string]bool{"github-token-secret": true},
	}
	cfgWithFlag, err := ResolveConfig(cm, flags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgWithFlag.GitHubTokenSecret != "flag-token-secret" {
		t.Errorf("expected GitHubTokenSecret %q, got %q", "flag-token-secret", cfgWithFlag.GitHubTokenSecret)
	}

	// 4. Invalid secret name in ConfigMap returns error
	invalidCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			ConfigMapKeyGitHubTokenSecret: "INVALID_SECRET_NAME!",
		},
	}
	_, err = ResolveConfig(invalidCM, RawFlags{})
	if err == nil {
		t.Fatalf("expected error for invalid secret name, got nil")
	}
}

func TestResolveConfig_SandboxdImage(t *testing.T) {
	// Default
	defCfg := DefaultClusterConfig()
	if defCfg.SandboxdImage != discovery.SandboxdImage {
		t.Errorf("expected default SandboxdImage %q, got %q", discovery.SandboxdImage, defCfg.SandboxdImage)
	}

	// ConfigMap override
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeySandboxdImage: "gcr.io/test/sandboxd:cm-tag",
		},
	}
	flags := RawFlags{
		Explicit: map[string]bool{},
	}
	cfg, err := ResolveConfig(cm, flags)
	if err != nil {
		t.Fatalf("ResolveConfig failed: %v", err)
	}
	if cfg.SandboxdImage != "gcr.io/test/sandboxd:cm-tag" {
		t.Errorf("expected SandboxdImage gcr.io/test/sandboxd:cm-tag, got %q", cfg.SandboxdImage)
	}

	// Flag override takes precedence
	flags.SandboxdImage = "gcr.io/test/sandboxd:flag-tag"
	flags.Explicit["sandboxd-image"] = true

	cfgOverride, err := ResolveConfig(cm, flags)
	if err != nil {
		t.Fatalf("ResolveConfig failed: %v", err)
	}
	if cfgOverride.SandboxdImage != "gcr.io/test/sandboxd:flag-tag" {
		t.Errorf("expected SandboxdImage gcr.io/test/sandboxd:flag-tag, got %q", cfgOverride.SandboxdImage)
	}

	// ConfigMap with "none" is rejected with clear error
	cmNone := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeySandboxdImage: "none",
		},
	}
	_, err = ResolveConfig(cmNone, RawFlags{})
	if err == nil {
		t.Fatal("expected error for sandboxdImage: none, got nil")
	}
	if !strings.Contains(err.Error(), "sandboxdImage cannot be disabled") {
		t.Errorf("expected error message to mention sandboxdImage cannot be disabled, got %v", err)
	}

	// Case-insensitive "NONE" also rejected
	cmNoneUpper := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeySandboxdImage: "NONE",
		},
	}
	_, err = ResolveConfig(cmNoneUpper, RawFlags{})
	if err == nil {
		t.Fatal("expected error for sandboxdImage: NONE, got nil")
	}
	if !strings.Contains(err.Error(), "sandboxdImage cannot be disabled") {
		t.Errorf("expected error message to mention sandboxdImage cannot be disabled, got %v", err)
	}

	// Explicit flag with "none" is rejected with clear error
	flagsNone := RawFlags{
		SandboxdImage: "none",
		Explicit: map[string]bool{
			"sandboxd-image": true,
		},
	}
	_, err = ResolveConfig(nil, flagsNone)
	if err == nil {
		t.Fatal("expected error for --sandboxd-image=none, got nil")
	}
	if !strings.Contains(err.Error(), "sandboxd-image cannot be disabled") {
		t.Errorf("expected error message to mention sandboxd-image cannot be disabled, got %v", err)
	}
}

func TestResolveConfig_ArtifactDir(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeyArtifactDir: "/tmp/artifacts",
		},
	}

	flags := RawFlags{
		Explicit: map[string]bool{},
	}

	cfg, err := ResolveConfig(cm, flags)
	if err != nil {
		t.Fatalf("ResolveConfig failed: %v", err)
	}
	if cfg.ArtifactDir != "/tmp/artifacts" {
		t.Errorf("expected ArtifactDir /tmp/artifacts, got %q", cfg.ArtifactDir)
	}

	// Flag override takes precedence
	flags.ArtifactDir = "/custom/dir"
	flags.Explicit["artifact-dir"] = true

	cfgOverride, err := ResolveConfig(cm, flags)
	if err != nil {
		t.Fatalf("ResolveConfig failed: %v", err)
	}
	if cfgOverride.ArtifactDir != "/custom/dir" {
		t.Errorf("expected ArtifactDir /custom/dir, got %q", cfgOverride.ArtifactDir)
	}
}

type testLogSink struct {
	entries []string
}

func (s *testLogSink) Init(info logr.RuntimeInfo) {}
func (s *testLogSink) Enabled(level int) bool     { return true }
func (s *testLogSink) Info(level int, msg string, keysAndValues ...any) {
	s.entries = append(s.entries, fmt.Sprintf("%s: %v", msg, keysAndValues))
}
func (s *testLogSink) Error(err error, msg string, keysAndValues ...any) {
	s.entries = append(s.entries, fmt.Sprintf("ERROR: %s: %v (%v)", msg, keysAndValues, err))
}
func (s *testLogSink) WithValues(keysAndValues ...any) logr.LogSink { return s }
func (s *testLogSink) WithName(name string) logr.LogSink            { return s }

func TestUnknownConfigMapKeys(t *testing.T) {
	// 1. nil ConfigMap
	if unknown := UnknownConfigMapKeys(nil); unknown != nil {
		t.Errorf("expected nil for nil ConfigMap, got %v", unknown)
	}

	// 2. nil Data
	cmEmpty := &corev1.ConfigMap{}
	if unknown := UnknownConfigMapKeys(cmEmpty); unknown != nil {
		t.Errorf("expected nil for nil Data, got %v", unknown)
	}

	// 3. all recognized keys
	cmRecognized := &corev1.ConfigMap{
		Data: map[string]string{
			ConfigMapKeyArtifactBucket:                      "gs://b",
			ConfigMapKeyArtifactDir:                         "/tmp",
			ConfigMapKeyArtifactMaxUploadBytes:              "10485760",
			ConfigMapKeyDiscoveryDeadline:                   "10m",
			ConfigMapKeyExecutionDeadline:                   "30m",
			ConfigMapKeyMaxParallelTasks:                    "4",
			ConfigMapKeyModelSecret:                         "sec",
			ConfigMapKeySandboxPortalPolicy:                 "*",
			ConfigMapKeySandboxdImage:                       "img:tag",
			ConfigMapKeyTaskGracePeriod:                     "30s",
			ConfigMapKeyGitHubTokenSecret:                   "gh-token",
			ConfigMapKeyGitHubAppSecret:                     "gh-app",
			ConfigMapKeyUIBaseURL:                           "https://ui",
			ConfigMapKeyInconclusivePolicy:                  "neutral",
			configkeys.ConfigMapKeyGitHubWebhookSecret:      "gh-wh",
			configkeys.ConfigMapKeyWebhookRepositories:      "org/repo",
			configkeys.ConfigMapKeyAllowedCloneHosts:        "github.com",
			configkeys.ConfigMapKeyDefaultModel:             "model",
			configkeys.ConfigMapKeyDefaultLimit:             "5",
			configkeys.ConfigMapKeyDefaultBatchSize:         "8",
			configkeys.ConfigMapKeyDefaultMaxSamplesPerTask: "64",
			configkeys.ConfigMapKeyDefaultBudgetUSD:         "0.50",
			configkeys.ConfigMapKeyCommandUsers:             "user1,user2",
			ConfigMapKeySandboxEgressHosts:                  "*.googleapis.com,github.com",
			configkeys.ConfigMapKeyHeadTopUpInterval:        "6h",
			configkeys.ConfigMapKeyHeadTargetCIHalfWidth:    "0.05",
			configkeys.ConfigMapKeyHeadMaxSamplesPerTask:    "64",
		},
	}
	if unknown := UnknownConfigMapKeys(cmRecognized); unknown != nil {
		t.Errorf("expected nil for recognized keys, got %v", unknown)
	}

	// 4. unrecognized keys are returned sorted
	cmWithUnknown := &corev1.ConfigMap{
		Data: map[string]string{
			"artifactBuckets": "gs://typo",
			"unknownKey2":     "val2",
			"anotherUnknown":  "val1",
			"artifactBucket":  "gs://good",
		},
	}
	expected := []string{"anotherUnknown", "artifactBuckets", "unknownKey2"}
	unknown := UnknownConfigMapKeys(cmWithUnknown)
	if !reflect.DeepEqual(unknown, expected) {
		t.Errorf("expected %v, got %v", expected, unknown)
	}
}

func TestResolveConfig_StrictMode(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			"artifactBuckets": "gs://typo",
			"artifactBucket":  "gs://valid-bucket",
		},
	}

	// When strict-config is disabled (default), unknown keys are ignored by ResolveConfig
	cfg, err := ResolveConfig(cm, RawFlags{StrictConfig: false})
	if err != nil {
		t.Fatalf("unexpected error when strict-config is false: %v", err)
	}
	if cfg.ArtifactBucket != "gs://valid-bucket" {
		t.Errorf("expected ArtifactBucket gs://valid-bucket, got %q", cfg.ArtifactBucket)
	}

	// When strict-config is enabled, ResolveConfig fails fast with descriptive error
	_, err = ResolveConfig(cm, RawFlags{StrictConfig: true})
	if err == nil {
		t.Fatal("expected error when strict-config is true, got nil")
	}
	if !strings.Contains(err.Error(), "unrecognised keys in ConfigMap") {
		t.Errorf("expected error mentioning unrecognised keys, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "artifactBuckets") {
		t.Errorf("expected error mentioning artifactBuckets, got %q", err.Error())
	}

	// When strict-config is enabled but all keys are valid, ResolveConfig succeeds
	cmValid := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			"artifactBucket": "gs://valid-bucket",
		},
	}
	cfgValid, err := ResolveConfig(cmValid, RawFlags{StrictConfig: true})
	if err != nil {
		t.Fatalf("unexpected error when strict-config is true with valid keys: %v", err)
	}
	if cfgValid.ArtifactBucket != "gs://valid-bucket" {
		t.Errorf("expected ArtifactBucket gs://valid-bucket, got %q", cfgValid.ArtifactBucket)
	}
}

func TestLoadAndResolveClusterConfig_UnknownKeyWarning(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}

	cmUnknown := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			"artifactBuckets": "gs://typo",
			"extraField":      "value",
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cmUnknown).Build()

	sink := &testLogSink{}
	logger := logr.New(sink)

	cfg, err := LoadAndResolveClusterConfig(
		t.Context(),
		fakeClient,
		"eval-runner",
		DefaultConfigMapName,
		RawFlags{},
		logger,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Verify built-in defaults applied
	if !reflect.DeepEqual(cfg, DefaultClusterConfig()) {
		t.Errorf("expected default cluster config, got %+v", cfg)
	}

	// Verify warning logged
	warningFound := false
	for _, entry := range sink.entries {
		if strings.Contains(entry, "warning: unrecognised keys in ConfigMap") &&
			strings.Contains(entry, "artifactBuckets") &&
			strings.Contains(entry, "extraField") {
			warningFound = true
			break
		}
	}
	if !warningFound {
		t.Errorf("expected warning logged for unrecognised keys, got entries: %v", sink.entries)
	}

	// Verify fail-fast when StrictConfig is enabled
	sinkStrict := &testLogSink{}
	loggerStrict := logr.New(sinkStrict)
	_, err = LoadAndResolveClusterConfig(
		t.Context(),
		fakeClient,
		"eval-runner",
		DefaultConfigMapName,
		RawFlags{StrictConfig: true},
		loggerStrict,
	)
	if err == nil {
		t.Fatal("expected error under strict config, got nil")
	}
	if !strings.Contains(err.Error(), "unrecognised keys in ConfigMap") {
		t.Errorf("expected unrecognised keys error, got %v", err)
	}

	// Verify no warning logged when only recognized keys are present
	cmKnown := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			"artifactBucket": "gs://valid-bucket",
		},
	}
	fakeClientKnown := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cmKnown).Build()
	sinkKnown := &testLogSink{}
	loggerKnown := logr.New(sinkKnown)
	_, err = LoadAndResolveClusterConfig(
		t.Context(),
		fakeClientKnown,
		"eval-runner",
		DefaultConfigMapName,
		RawFlags{},
		loggerKnown,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, entry := range sinkKnown.entries {
		if strings.Contains(entry, "unrecognised keys") {
			t.Errorf("unexpected warning logged for recognized keys: %s", entry)
		}
	}
}

func TestResolveConfig_EmptyValueSemantics(t *testing.T) {
	defaults := DefaultClusterConfig()

	// 1. Empty string for all keys retains built-in defaults (does not disable)
	t.Run("empty strings retain built-in defaults", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eval-runner",
				Name:      DefaultConfigMapName,
			},
			Data: map[string]string{
				ConfigMapKeyArtifactBucket:      "",
				ConfigMapKeyArtifactDir:         "",
				ConfigMapKeyDiscoveryDeadline:   "",
				ConfigMapKeyExecutionDeadline:   "",
				ConfigMapKeyMaxParallelTasks:    "",
				ConfigMapKeyModelSecret:         "",
				ConfigMapKeySandboxPortalPolicy: "",
				ConfigMapKeySandboxdImage:       "",
				ConfigMapKeyTaskGracePeriod:     "",
				ConfigMapKeyGitHubTokenSecret:   "",
				ConfigMapKeyGitHubAppSecret:     "",
				ConfigMapKeyUIBaseURL:           "",
			},
		}

		cfg, err := ResolveConfig(cm, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(cfg, defaults) {
			t.Errorf("expected empty strings to retain defaults %+v, got %+v", defaults, cfg)
		}
	})

	// 2. Whitespace-only strings retain built-in defaults
	t.Run("whitespace strings retain built-in defaults", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eval-runner",
				Name:      DefaultConfigMapName,
			},
			Data: map[string]string{
				ConfigMapKeyModelSecret:         "   ",
				ConfigMapKeySandboxPortalPolicy: "   ",
				ConfigMapKeyGitHubTokenSecret:   " \t ",
				ConfigMapKeyDiscoveryDeadline:   " ",
			},
		}

		cfg, err := ResolveConfig(cm, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ModelSecret != defaults.ModelSecret {
			t.Errorf("expected ModelSecret default %q, got %q", defaults.ModelSecret, cfg.ModelSecret)
		}
		if cfg.SandboxPortalPolicy != defaults.SandboxPortalPolicy {
			t.Errorf("expected SandboxPortalPolicy default %q, got %q", defaults.SandboxPortalPolicy, cfg.SandboxPortalPolicy)
		}
		if cfg.GitHubTokenSecret != defaults.GitHubTokenSecret {
			t.Errorf("expected GitHubTokenSecret default %q, got %q", defaults.GitHubTokenSecret, cfg.GitHubTokenSecret)
		}
		if cfg.DiscoveryDeadline != defaults.DiscoveryDeadline {
			t.Errorf("expected DiscoveryDeadline default %v, got %v", defaults.DiscoveryDeadline, cfg.DiscoveryDeadline)
		}
	})

	// 3. Sentinel "none" (case-insensitive) explicitly disables features/credentials
	t.Run("sentinel none disables features in ConfigMap", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eval-runner",
				Name:      DefaultConfigMapName,
			},
			Data: map[string]string{
				ConfigMapKeyModelSecret:       "none",
				ConfigMapKeyGitHubTokenSecret: "None",
				ConfigMapKeyArtifactBucket:    "NONE",
				ConfigMapKeyArtifactDir:       "none",
				ConfigMapKeyGitHubAppSecret:   "none",
				ConfigMapKeyUIBaseURL:         "none",
			},
		}

		cfg, err := ResolveConfig(cm, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ModelSecret != "" {
			t.Errorf("expected ModelSecret disabled (empty), got %q", cfg.ModelSecret)
		}
		if cfg.GitHubTokenSecret != "" {
			t.Errorf("expected GitHubTokenSecret disabled (empty), got %q", cfg.GitHubTokenSecret)
		}
		if cfg.ArtifactBucket != "" {
			t.Errorf("expected ArtifactBucket disabled (empty), got %q", cfg.ArtifactBucket)
		}
		if cfg.ArtifactDir != "" {
			t.Errorf("expected ArtifactDir disabled (empty), got %q", cfg.ArtifactDir)
		}
		if cfg.GitHubAppSecret != "" {
			t.Errorf("expected GitHubAppSecret disabled (empty), got %q", cfg.GitHubAppSecret)
		}
		if cfg.UIBaseURL != "" {
			t.Errorf("expected UIBaseURL disabled (empty), got %q", cfg.UIBaseURL)
		}
	})

	// 4. Explicit flags with sentinel "none" or empty string disable features
	t.Run("explicit flag with none disables features", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eval-runner",
				Name:      DefaultConfigMapName,
			},
			Data: map[string]string{
				ConfigMapKeyModelSecret:       "custom-model-secret",
				ConfigMapKeyGitHubTokenSecret: "custom-token-secret",
			},
		}
		flags := RawFlags{
			ModelSecret:       "none",
			GitHubTokenSecret: "None",
			Explicit: map[string]bool{
				"model-secret":        true,
				"github-token-secret": true,
			},
		}

		cfg, err := ResolveConfig(cm, flags)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ModelSecret != "" {
			t.Errorf("expected ModelSecret disabled via flag, got %q", cfg.ModelSecret)
		}
		if cfg.GitHubTokenSecret != "" {
			t.Errorf("expected GitHubTokenSecret disabled via flag, got %q", cfg.GitHubTokenSecret)
		}
	})
}

func TestResolveConfig_ArtifactMaxUploadBytes(t *testing.T) {
	// 1. Built-in default
	defCfg := DefaultClusterConfig()
	if defCfg.ArtifactMaxUploadBytes != gcs.DefaultMaxUploadBytes {
		t.Errorf("expected default ArtifactMaxUploadBytes %d, got %d", gcs.DefaultMaxUploadBytes, defCfg.ArtifactMaxUploadBytes)
	}

	// 2. ConfigMap override with artifactMaxUploadBytes
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeyArtifactMaxUploadBytes: "5242880",
		},
	}
	cfg, err := ResolveConfig(cm, RawFlags{})
	if err != nil {
		t.Fatalf("ResolveConfig failed: %v", err)
	}
	if cfg.ArtifactMaxUploadBytes != 5242880 {
		t.Errorf("expected ArtifactMaxUploadBytes 5242880, got %d", cfg.ArtifactMaxUploadBytes)
	}

	// 3. Flag override with artifact-max-upload-bytes
	flags := RawFlags{
		ArtifactMaxUploadBytes: 1048576,
		Explicit:               map[string]bool{"artifact-max-upload-bytes": true},
	}
	cfgFlag, err := ResolveConfig(cm, flags)
	if err != nil {
		t.Fatalf("ResolveConfig failed: %v", err)
	}
	if cfgFlag.ArtifactMaxUploadBytes != 1048576 {
		t.Errorf("expected ArtifactMaxUploadBytes 1048576, got %d", cfgFlag.ArtifactMaxUploadBytes)
	}

	// 4. Invalid non-numeric or <= 0
	invalidCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeyArtifactMaxUploadBytes: "-10",
		},
	}
	if _, err := ResolveConfig(invalidCM, RawFlags{}); err == nil {
		t.Errorf("expected error for negative artifactMaxUploadBytes, got nil")
	}
}

func TestResolveConfig_ComparisonTTL(t *testing.T) {
	// 1. Default when unset is 0 (disabled)
	cfg, err := ResolveConfig(nil, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ComparisonTTL != 0 {
		t.Errorf("expected default ComparisonTTL 0, got %v", cfg.ComparisonTTL)
	}

	// 2. Valid duration from ConfigMap
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeyComparisonTTL: "24h",
		},
	}
	cfg, err = ResolveConfig(cm, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ComparisonTTL != 24*time.Hour {
		t.Errorf("expected ComparisonTTL 24h, got %v", cfg.ComparisonTTL)
	}

	// 3. Disabled via "0", "0s", or "none"
	for _, val := range []string{"0", "0s", "none", "NONE"} {
		cmDisabled := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      DefaultConfigMapName,
				Namespace: "eval-runner",
			},
			Data: map[string]string{
				ConfigMapKeyComparisonTTL: val,
			},
		}
		cfg, err = ResolveConfig(cmDisabled, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", val, err)
		}
		if cfg.ComparisonTTL != 0 {
			t.Errorf("expected ComparisonTTL 0 for %q, got %v", val, cfg.ComparisonTTL)
		}
	}

	// 4. Invalid duration string
	cmInvalid := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeyComparisonTTL: "not-a-duration",
		},
	}
	if _, err := ResolveConfig(cmInvalid, RawFlags{}); err == nil {
		t.Errorf("expected error for invalid duration string, got nil")
	}

	// 5. Negative duration in ConfigMap
	cmNegative := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeyComparisonTTL: "-1h",
		},
	}
	if _, err := ResolveConfig(cmNegative, RawFlags{}); err == nil {
		t.Errorf("expected error for negative duration, got nil")
	}

	// 6. Explicit flag overrides ConfigMap
	flags := RawFlags{
		ComparisonTTL: 48 * time.Hour,
		Explicit: map[string]bool{
			"comparison-ttl": true,
		},
	}
	cfg, err = ResolveConfig(cm, flags)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ComparisonTTL != 48*time.Hour {
		t.Errorf("expected flag override 48h, got %v", cfg.ComparisonTTL)
	}

	// 7. Explicit negative flag returns error
	flagsNeg := RawFlags{
		ComparisonTTL: -1 * time.Minute,
		Explicit: map[string]bool{
			"comparison-ttl": true,
		},
	}
	if _, err := ResolveConfig(nil, flagsNeg); err == nil {
		t.Errorf("expected error for negative flag --comparison-ttl, got nil")
	}
}

// TestKnownConfigMapKeys_AllConstantsRegistered ensures that every constant whose identifier
// starts with "ConfigMapKey" in any package in the repository appears in KnownConfigMapKeys.
// This prevents adding a new ConfigMapKey constant without registering it in KnownConfigMapKeys.
// It resolves both string literals and identifier / selector RHS values (e.g. ConfigMapKeyX = otherConst or pkg.Const).
func TestKnownConfigMapKeys_AllConstantsRegistered(t *testing.T) {
	// Find repository root
	repoRoot := ""
	dir := "."
	for i := 0; i < 5; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			repoRoot = dir
			break
		}
		dir = filepath.Join("..", dir)
	}
	if repoRoot == "" {
		t.Fatal("could not find repository root containing go.mod")
	}

	fset := token.NewFileSet()

	type constDecl struct {
		pkgName string
		name    string
		expr    ast.Expr
		path    string
	}
	var allDecls []constDecl
	resolved := make(map[string]string) // "pkgName.Name" -> value AND "Name" -> value

	err := filepath.Walk(repoRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if path == repoRoot {
				return nil
			}
			name := info.Name()
			if strings.HasPrefix(name, ".") || name == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		node, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return fmt.Errorf("failed to parse %s: %w", path, err)
		}

		pkgName := node.Name.Name
		for _, decl := range node.Decls {
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.CONST {
				continue
			}
			var lastExpr ast.Expr
			for _, spec := range genDecl.Specs {
				valSpec, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range valSpec.Names {
					if i < len(valSpec.Values) {
						lastExpr = valSpec.Values[i]
					}
					expr := lastExpr
					if expr == nil {
						continue
					}
					allDecls = append(allDecls, constDecl{
						pkgName: pkgName,
						name:    name.Name,
						expr:    expr,
						path:    path,
					})
					if lit, ok := expr.(*ast.BasicLit); ok && lit.Kind == token.STRING {
						val, err := strconv.Unquote(lit.Value)
						if err == nil {
							resolved[pkgName+"."+name.Name] = val
							if _, exists := resolved[name.Name]; !exists {
								resolved[name.Name] = val
							}
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed walking repository files: %v", err)
	}

	// Iteratively resolve identifier and selector RHS expressions (e.g. ConfigMapKeyX = otherConst or pkg.Const)
	for pass := 0; pass < 10; pass++ {
		progress := false
		for _, d := range allDecls {
			if _, ok := resolved[d.pkgName+"."+d.name]; ok {
				continue
			}
			var val string
			var found bool
			switch e := d.expr.(type) {
			case *ast.Ident:
				if v, ok := resolved[d.pkgName+"."+e.Name]; ok {
					val, found = v, true
				} else if v, ok := resolved[e.Name]; ok {
					val, found = v, true
				}
			case *ast.SelectorExpr:
				if xIdent, ok := e.X.(*ast.Ident); ok {
					if v, ok := resolved[xIdent.Name+"."+e.Sel.Name]; ok {
						val, found = v, true
					}
				}
			}
			if found {
				resolved[d.pkgName+"."+d.name] = val
				if _, ok := resolved[d.name]; !ok {
					resolved[d.name] = val
				}
				progress = true
			}
		}
		if !progress {
			break
		}
	}

	foundConstants := make(map[string]string) // constName -> constValue
	for _, d := range allDecls {
		if strings.HasPrefix(d.name, "ConfigMapKey") {
			val, ok := resolved[d.pkgName+"."+d.name]
			if !ok {
				t.Errorf("could not resolve string value for constant %s in %s", d.name, d.path)
				continue
			}
			if existing, exists := foundConstants[d.name]; exists && existing != val {
				t.Errorf("conflicting values for constant %s: %q in %s vs %q", d.name, val, d.path, existing)
			}
			foundConstants[d.name] = val
		}
	}

	if len(foundConstants) == 0 {
		t.Fatal("expected to find ConfigMapKey constants, found none")
	}
	if len(foundConstants) != len(KnownConfigMapKeys) {
		t.Errorf("expected %d ConfigMapKey constants matching KnownConfigMapKeys, found %d", len(KnownConfigMapKeys), len(foundConstants))
	}
	t.Logf("verified %d ConfigMapKey constants across packages", len(foundConstants))

	// Verify that every found constant value is present in KnownConfigMapKeys
	for constName, val := range foundConstants {
		if _, ok := KnownConfigMapKeys[val]; !ok {
			t.Errorf("constant %s with value %q is not registered in KnownConfigMapKeys", constName, val)
		}
	}

	// Verify that KnownConfigMapKeys does not have extra untracked keys
	knownValues := make(map[string]bool)
	for _, val := range foundConstants {
		knownValues[val] = true
	}
	for key := range KnownConfigMapKeys {
		if !knownValues[key] {
			t.Errorf("KnownConfigMapKeys contains key %q which has no corresponding ConfigMapKey* constant", key)
		}
	}
}

func TestResolveConfig_ModelPrices(t *testing.T) {
	t.Run("defaults present when key is absent", func(t *testing.T) {
		// When cm is nil
		cfgNil, err := ResolveConfig(nil, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error with nil ConfigMap: %v", err)
		}
		if len(cfgNil.ModelPrices) != 4 {
			t.Fatalf("expected 4 default model prices, got %d", len(cfgNil.ModelPrices))
		}
		defaults := DefaultModelPrices()
		for m, wantPrice := range defaults {
			gotPrice, ok := cfgNil.ModelPrices[m]
			if !ok {
				t.Errorf("missing default model %q", m)
			} else if gotPrice != wantPrice {
				t.Errorf("model %q: got price %+v, want %+v", m, gotPrice, wantPrice)
			}
			if cfgNil.ModelPriceSources[m] != v1alpha1.CostSourceDefault {
				t.Errorf("model %q: got source %q, want %q", m, cfgNil.ModelPriceSources[m], v1alpha1.CostSourceDefault)
			}
		}

		// When ConfigMap exists but modelPrices key is absent
		cmEmpty := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data:       map[string]string{},
		}
		cfgEmpty, err := ResolveConfig(cmEmpty, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error with empty ConfigMap: %v", err)
		}
		if len(cfgEmpty.ModelPrices) != 4 {
			t.Fatalf("expected 4 default model prices, got %d", len(cfgEmpty.ModelPrices))
		}
	})

	t.Run("merge and override with configmap", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data: map[string]string{
				ConfigMapKeyModelPrices: `{"google/gemini-3.5-flash-lite":{"inputPer1M":0.20,"outputPer1M":0.80},"mockllm/model":{"inputPer1M":1.0,"outputPer1M":2.0},"custom/model":{"inputPer1M":5.0,"outputPer1M":10.0}}`,
			},
		}
		cfg, err := ResolveConfig(cm, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// 4 defaults - 2 overridden + 1 new = 5 total
		if len(cfg.ModelPrices) != 5 {
			t.Fatalf("expected 5 model prices after merge, got %d", len(cfg.ModelPrices))
		}

		// Overridden gemini
		p1, ok := cfg.ModelPrices["google/gemini-3.5-flash-lite"]
		if !ok || p1.InputPer1M != 0.20 || p1.OutputPer1M != 0.80 {
			t.Errorf("unexpected price for overridden gemini: %+v", p1)
		}
		if cfg.ModelPriceSources["google/gemini-3.5-flash-lite"] != v1alpha1.CostSourceConfigMap {
			t.Errorf("expected source configmap for overridden gemini, got %q", cfg.ModelPriceSources["google/gemini-3.5-flash-lite"])
		}

		// Overridden mockllm
		p2, ok := cfg.ModelPrices["mockllm/model"]
		if !ok || p2.InputPer1M != 1.0 || p2.OutputPer1M != 2.0 {
			t.Errorf("unexpected price for mockllm: %+v", p2)
		}
		if cfg.ModelPriceSources["mockllm/model"] != v1alpha1.CostSourceConfigMap {
			t.Errorf("expected source configmap for overridden mockllm, got %q", cfg.ModelPriceSources["mockllm/model"])
		}

		// New custom model
		p3, ok := cfg.ModelPrices["custom/model"]
		if !ok || p3.InputPer1M != 5.0 || p3.OutputPer1M != 10.0 {
			t.Errorf("unexpected price for custom/model: %+v", p3)
		}
		if cfg.ModelPriceSources["custom/model"] != v1alpha1.CostSourceConfigMap {
			t.Errorf("expected source configmap for custom/model, got %q", cfg.ModelPriceSources["custom/model"])
		}

		// Non-overridden default retained
		pFlash, ok := cfg.ModelPrices["google/gemini-2.5-flash"]
		if !ok || pFlash.InputPer1M != 0.30 || pFlash.OutputPer1M != 2.50 {
			t.Errorf("unexpected price for non-overridden gemini-2.5-flash: %+v", pFlash)
		}
		if cfg.ModelPriceSources["google/gemini-2.5-flash"] != v1alpha1.CostSourceDefault {
			t.Errorf("expected source default for non-overridden gemini-2.5-flash, got %q", cfg.ModelPriceSources["google/gemini-2.5-flash"])
		}
	})

	t.Run("prefix keys and lookup resolution", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data: map[string]string{
				ConfigMapKeyModelPrices: `{"prefix:google/gemini-2.5-":{"inputPer1M":0.25,"outputPer1M":1.50},"prefix:openrouter/":{"inputPer1M":2.00,"outputPer1M":4.00},"openrouter/special":{"inputPer1M":1.00,"outputPer1M":2.00}}`,
			},
		}
		cfg, err := ResolveConfig(cm, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Prefix match overrides defaults for gemini-2.5 models
		p, src, ok := LookupModelPrice("google/gemini-2.5-flash", cfg.ModelPrices, cfg.ModelPriceSources)
		if !ok || p.InputPer1M != 0.25 || p.OutputPer1M != 1.50 || src != v1alpha1.CostSourceConfigMap {
			t.Errorf("expected prefix match for gemini-2.5-flash, got price=%+v, src=%q, ok=%v", p, src, ok)
		}

		// Default exact match for gemini-3.5-flash-lite untouched by gemini-2.5- prefix
		p35, src35, ok35 := LookupModelPrice("google/gemini-3.5-flash-lite", cfg.ModelPrices, cfg.ModelPriceSources)
		if !ok35 || p35.InputPer1M != 0.30 || p35.OutputPer1M != 2.50 || src35 != v1alpha1.CostSourceDefault {
			t.Errorf("expected default price for gemini-3.5-flash-lite, got price=%+v, src=%q, ok=%v", p35, src35, ok35)
		}

		// Exact match in configmap takes precedence over prefix match in configmap
		pSpecial, srcSpecial, okSpecial := LookupModelPrice("openrouter/special", cfg.ModelPrices, cfg.ModelPriceSources)
		if !okSpecial || pSpecial.InputPer1M != 1.00 || pSpecial.OutputPer1M != 2.00 || srcSpecial != v1alpha1.CostSourceConfigMap {
			t.Errorf("expected exact match for openrouter/special, got price=%+v, src=%q, ok=%v", pSpecial, srcSpecial, okSpecial)
		}

		// Other openrouter models match prefix
		pOther, srcOther, okOther := LookupModelPrice("openrouter/meta-llama/llama-3", cfg.ModelPrices, cfg.ModelPriceSources)
		if !okOther || pOther.InputPer1M != 2.00 || pOther.OutputPer1M != 4.00 || srcOther != v1alpha1.CostSourceConfigMap {
			t.Errorf("expected prefix match for openrouter model, got price=%+v, src=%q, ok=%v", pOther, srcOther, okOther)
		}

		// Unknown model has no price
		_, _, okUnknown := LookupModelPrice("anthropic/claude-3-opus", cfg.ModelPrices, cfg.ModelPriceSources)
		if okUnknown {
			t.Errorf("expected false for unknown model price")
		}
	})

	t.Run("empty prefix returns error", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data: map[string]string{
				ConfigMapKeyModelPrices: `{"prefix:":{"inputPer1M":0.15,"outputPer1M":0.60}}`,
			},
		}
		_, err := ResolveConfig(cm, RawFlags{})
		if err == nil {
			t.Fatalf("expected error for empty prefix:, got nil")
		}
	})

	t.Run("malformed json returns error", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data: map[string]string{
				ConfigMapKeyModelPrices: `not-valid-json`,
			},
		}
		_, err := ResolveConfig(cm, RawFlags{})
		if err == nil {
			t.Fatalf("expected error for malformed json, got nil")
		}
	})

	t.Run("negative price returns error", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data: map[string]string{
				ConfigMapKeyModelPrices: `{"model":{"inputPer1M":-0.5,"outputPer1M":1.0}}`,
			},
		}
		_, err := ResolveConfig(cm, RawFlags{})
		if err == nil {
			t.Fatalf("expected error for negative price, got nil")
		}
	})

	t.Run("unknown field returns error", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data: map[string]string{
				ConfigMapKeyModelPrices: `{"model":{"inputPerM":0.15,"outputPer1M":0.60}}`,
			},
		}
		_, err := ResolveConfig(cm, RawFlags{})
		if err == nil {
			t.Fatalf("expected error for unknown field in modelPrices, got nil")
		}
	})

	t.Run("disabled sentinel clears prices", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
			Data: map[string]string{
				ConfigMapKeyModelPrices: "none",
			},
		}
		cfg, err := ResolveConfig(cm, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ModelPrices != nil {
			t.Errorf("expected nil ModelPrices with sentinel none, got %+v", cfg.ModelPrices)
		}
		if !cfg.ModelPricesDisabled {
			t.Errorf("expected ModelPricesDisabled to be true")
		}
	})
}

func TestResolveConfig_CommandUsers(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "eval-runner-config",
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeyCommandUsers: "alice, bob, CHARLIE",
		},
	}

	cfg, err := ResolveConfig(cm, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error resolving config: %v", err)
	}
	expectedUsers := []string{"alice", "bob", "CHARLIE"}
	if !reflect.DeepEqual(cfg.CommandUsers, expectedUsers) {
		t.Errorf("expected CommandUsers %v, got %v", expectedUsers, cfg.CommandUsers)
	}

	// Verify case-insensitive check and no wildcard
	if !IsUserAllowed("Alice", cfg.CommandUsers) {
		t.Errorf("expected Alice to be allowed")
	}
	if !IsUserAllowed("charlie", cfg.CommandUsers) {
		t.Errorf("expected charlie to be allowed")
	}
	if IsUserAllowed("eve", cfg.CommandUsers) {
		t.Errorf("expected eve to not be allowed")
	}
	if IsUserAllowed("eve", []string{"*"}) {
		t.Errorf("expected '*' to NOT act as a wildcard in commandUsers")
	}

	// Explicit flag overrides ConfigMap
	flags := RawFlags{
		CommandUsers: "david, emily",
		Explicit: map[string]bool{
			"command-users": true,
		},
	}
	cfgFlags, err := ResolveConfig(cm, flags)
	if err != nil {
		t.Fatalf("unexpected error resolving config with flags: %v", err)
	}
	expectedFlagUsers := []string{"david", "emily"}
	if !reflect.DeepEqual(cfgFlags.CommandUsers, expectedFlagUsers) {
		t.Errorf("expected flag CommandUsers %v, got %v", expectedFlagUsers, cfgFlags.CommandUsers)
	}
}

func TestResolveConfig_PollProgressInterval(t *testing.T) {
	// 1. Default when unset is DefaultPollProgressInterval (10s)
	cfg, err := ResolveConfig(nil, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.PollProgressInterval != DefaultPollProgressInterval {
		t.Errorf("expected default PollProgressInterval %v, got %v", DefaultPollProgressInterval, cfg.PollProgressInterval)
	}

	// 2. Valid duration from ConfigMap
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeyPollProgressInterval: "2s",
		},
	}
	cfg, err = ResolveConfig(cm, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.PollProgressInterval != 2*time.Second {
		t.Errorf("expected PollProgressInterval 2s, got %v", cfg.PollProgressInterval)
	}

	// 3. Minimum 1s validation on ConfigMap
	for _, val := range []string{"500ms", "0s", "-1s"} {
		cmSubSec := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      DefaultConfigMapName,
				Namespace: "eval-runner",
			},
			Data: map[string]string{
				ConfigMapKeyPollProgressInterval: val,
			},
		}
		if _, err := ResolveConfig(cmSubSec, RawFlags{}); err == nil {
			t.Errorf("expected error for sub-second pollProgressInterval %q, got nil", val)
		}
	}

	// 4. Invalid duration string
	cmInvalid := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeyPollProgressInterval: "not-a-duration",
		},
	}
	if _, err := ResolveConfig(cmInvalid, RawFlags{}); err == nil {
		t.Errorf("expected error for invalid duration string, got nil")
	}

	// 5. Flag overrides ConfigMap
	flags := RawFlags{
		PollProgressInterval: 5 * time.Second,
		Explicit: map[string]bool{
			"poll-progress-interval": true,
		},
	}
	cfgFlags, err := ResolveConfig(cm, flags)
	if err != nil {
		t.Fatalf("unexpected error resolving config with flags: %v", err)
	}
	if cfgFlags.PollProgressInterval != 5*time.Second {
		t.Errorf("expected flag PollProgressInterval 5s, got %v", cfgFlags.PollProgressInterval)
	}

	// 6. Minimum 1s validation on flag
	invalidFlag := RawFlags{
		PollProgressInterval: 500 * time.Millisecond,
		Explicit: map[string]bool{
			"poll-progress-interval": true,
		},
	}
	if _, err := ResolveConfig(cm, invalidFlag); err == nil {
		t.Errorf("expected error for sub-second flag --poll-progress-interval, got nil")
	}

	// 7. Empty or whitespace value in ConfigMap retains default
	cmEmpty := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultConfigMapName,
			Namespace: "eval-runner",
		},
		Data: map[string]string{
			ConfigMapKeyPollProgressInterval: "   ",
		},
	}
	cfgEmpty, err := ResolveConfig(cmEmpty, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error for empty string: %v", err)
	}
	if cfgEmpty.PollProgressInterval != DefaultPollProgressInterval {
		t.Errorf("expected default PollProgressInterval %v for empty string, got %v", DefaultPollProgressInterval, cfgEmpty.PollProgressInterval)
	}
}

func TestResolveConfig_AffinityMode(t *testing.T) {
	// 1. Built-in default is "order"
	cfgDef, err := ResolveConfig(nil, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error for nil ConfigMap: %v", err)
	}
	if cfgDef.AffinityMode != configkeys.AffinityModeOrder {
		t.Errorf("expected default AffinityMode %q, got %q", configkeys.AffinityModeOrder, cfgDef.AffinityMode)
	}

	// 2. ConfigMap "order"
	cmOrder := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultConfigMapName, Namespace: "eval-runner"},
		Data:       map[string]string{ConfigMapKeyAffinityMode: "order"},
	}
	cfgOrder, err := ResolveConfig(cmOrder, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgOrder.AffinityMode != configkeys.AffinityModeOrder {
		t.Errorf("expected AffinityMode %q, got %q", configkeys.AffinityModeOrder, cfgOrder.AffinityMode)
	}

	// 3. ConfigMap "prune" (case-insensitive)
	cmPrune := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultConfigMapName, Namespace: "eval-runner"},
		Data:       map[string]string{ConfigMapKeyAffinityMode: "  Prune  "},
	}
	cfgPrune, err := ResolveConfig(cmPrune, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgPrune.AffinityMode != configkeys.AffinityModePrune {
		t.Errorf("expected AffinityMode %q, got %q", configkeys.AffinityModePrune, cfgPrune.AffinityMode)
	}

	// 4. ConfigMap invalid value returns error
	cmInvalid := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultConfigMapName, Namespace: "eval-runner"},
		Data:       map[string]string{ConfigMapKeyAffinityMode: "bogus"},
	}
	if _, err := ResolveConfig(cmInvalid, RawFlags{}); err == nil {
		t.Errorf("expected error for invalid affinityMode, got nil")
	}

	// 5. Flag override
	flagsOverride := RawFlags{
		AffinityMode: configkeys.AffinityModePrune,
		Explicit: map[string]bool{
			"affinity-mode": true,
		},
	}
	cfgFlag, err := ResolveConfig(cmOrder, flagsOverride)
	if err != nil {
		t.Fatalf("unexpected error with flag override: %v", err)
	}
	if cfgFlag.AffinityMode != configkeys.AffinityModePrune {
		t.Errorf("expected flag override %q, got %q", configkeys.AffinityModePrune, cfgFlag.AffinityMode)
	}

	// 6. Invalid flag returns error
	flagsBad := RawFlags{
		AffinityMode: "invalid-mode",
		Explicit: map[string]bool{
			"affinity-mode": true,
		},
	}
	if _, err := ResolveConfig(nil, flagsBad); err == nil {
		t.Errorf("expected error for invalid --affinity-mode flag, got nil")
	}
}

func TestResolveConfig_SandboxEgressHosts(t *testing.T) {
	// 1. Built-in default when nil or key omitted
	cfgDef, err := ResolveConfig(nil, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error for nil ConfigMap: %v", err)
	}
	if !reflect.DeepEqual(cfgDef.SandboxEgressHosts, discovery.DefaultSandboxEgressHosts) {
		t.Errorf("expected default SandboxEgressHosts %v, got %v", discovery.DefaultSandboxEgressHosts, cfgDef.SandboxEgressHosts)
	}

	// 2. ConfigMap specifies comma-separated hosts
	cmHosts := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultConfigMapName, Namespace: "eval-runner"},
		Data:       map[string]string{ConfigMapKeySandboxEgressHosts: "github.com, *.googleapis.com, api.openai.com, github.com"},
	}
	cfgHosts, err := ResolveConfig(cmHosts, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectedHosts := []string{"github.com", "*.googleapis.com", "api.openai.com"}
	if !reflect.DeepEqual(cfgHosts.SandboxEgressHosts, expectedHosts) {
		t.Errorf("expected SandboxEgressHosts %v, got %v", expectedHosts, cfgHosts.SandboxEgressHosts)
	}

	// 3. ConfigMap specifies "none" (sentinel disables hosts)
	cmNone := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultConfigMapName, Namespace: "eval-runner"},
		Data:       map[string]string{ConfigMapKeySandboxEgressHosts: "  None  "},
	}
	cfgNone, err := ResolveConfig(cmNone, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error for none sentinel: %v", err)
	}
	if cfgNone.SandboxEgressHosts != nil {
		t.Errorf("expected nil SandboxEgressHosts for none sentinel, got %v", cfgNone.SandboxEgressHosts)
	}

	// 4. Empty string in ConfigMap retains built-in defaults
	cmEmpty := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: DefaultConfigMapName, Namespace: "eval-runner"},
		Data:       map[string]string{ConfigMapKeySandboxEgressHosts: "   "},
	}
	cfgEmpty, err := ResolveConfig(cmEmpty, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error for empty string: %v", err)
	}
	if !reflect.DeepEqual(cfgEmpty.SandboxEgressHosts, discovery.DefaultSandboxEgressHosts) {
		t.Errorf("expected default SandboxEgressHosts for empty string, got %v", cfgEmpty.SandboxEgressHosts)
	}

	// 5. Flag override
	flagsOverride := RawFlags{
		SandboxEgressHosts: "api.anthropic.com, *.openai.com",
		Explicit: map[string]bool{
			"sandbox-egress-hosts": true,
		},
	}
	cfgFlag, err := ResolveConfig(cmHosts, flagsOverride)
	if err != nil {
		t.Fatalf("unexpected error with flag override: %v", err)
	}
	expectedFlagHosts := []string{"api.anthropic.com", "*.openai.com"}
	if !reflect.DeepEqual(cfgFlag.SandboxEgressHosts, expectedFlagHosts) {
		t.Errorf("expected flag override %v, got %v", expectedFlagHosts, cfgFlag.SandboxEgressHosts)
	}

	// 6. Flag override with "none"
	flagsNone := RawFlags{
		SandboxEgressHosts: "none",
		Explicit: map[string]bool{
			"sandbox-egress-hosts": true,
		},
	}
	cfgFlagNone, err := ResolveConfig(cmHosts, flagsNone)
	if err != nil {
		t.Fatalf("unexpected error with flag override none: %v", err)
	}
	if cfgFlagNone.SandboxEgressHosts != nil {
		t.Errorf("expected nil SandboxEgressHosts with flag none, got %v", cfgFlagNone.SandboxEgressHosts)
	}
}

type trackingTestSink struct {
	artifacts.ArtifactSink
	bucket        string
	dir           string
	closed        atomic.Bool
	blockUpload   chan struct{}
	uploadStarted chan struct{}
}

func (s *trackingTestSink) Close() error {
	s.closed.Store(true)
	return nil
}

func (s *trackingTestSink) Bucket() string {
	return s.bucket
}

func (s *trackingTestSink) StoreArtifact(ctx context.Context, meta artifacts.RunMetadata, filename string, data []byte) (string, error) {
	if s.uploadStarted != nil {
		close(s.uploadStarted)
	}
	if s.blockUpload != nil {
		<-s.blockUpload
	}
	return "stored", nil
}

func (s *trackingTestSink) Load(ctx context.Context, path string) ([]byte, error) {
	return []byte("data"), nil
}

func (s *trackingTestSink) ArtifactPrefix(commit string) string {
	if s.bucket != "" {
		return "gs://" + s.bucket + "/" + commit
	}
	return ""
}

func TestConfigReloader_UpdatesConfigMap_SinkFactoryCalledAndOldSinkClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const ns = "eval-runner"
	const cmName = DefaultConfigMapName

	initialCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            cmName,
			ResourceVersion: "100",
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactBucket:    "initial-bucket",
			configkeys.ConfigMapKeyDiscoveryDeadline: "5m",
			configkeys.ConfigMapKeyMaxParallelTasks:  "6",
		},
	}

	clientset := k8sfake.NewSimpleClientset(initialCM)

	watchReady := make(chan struct{})
	var watchOnce sync.Once
	clientset.PrependWatchReactor("configmaps", func(action k8stesting.Action) (bool, watch.Interface, error) {
		w, err := clientset.Tracker().Watch(action.GetResource(), action.GetNamespace())
		watchOnce.Do(func() { close(watchReady) })
		return true, w, err
	})

	var factoryCalls []string
	var createdSinks []*trackingTestSink

	mockFactory := func(ctx context.Context, bucket, dir, workloadIdentityProvider string, maxUploadBytes int64) (artifacts.ArtifactSink, error) {
		factoryCalls = append(factoryCalls, bucket)
		sink := &trackingTestSink{bucket: bucket, dir: dir}
		createdSinks = append(createdSinks, sink)
		return sink, nil
	}

	rawFlags := RawFlags{}
	initialCfg, err := ResolveConfig(initialCM, rawFlags)
	if err != nil {
		t.Fatalf("failed to resolve initial config: %v", err)
	}

	initialSink, err := mockFactory(ctx, initialCfg.ArtifactBucket, initialCfg.ArtifactDir, initialCfg.GCPWorkloadIdentityProvider, initialCfg.ArtifactMaxUploadBytes)
	if err != nil {
		t.Fatalf("failed to construct initial sink: %v", err)
	}
	reloadableSink := artifacts.NewReloadableSink(initialSink)
	defer reloadableSink.Close()

	runReconciler := &RunReconciler{
		ArtifactSink:      reloadableSink,
		DiscoveryDeadline: initialCfg.DiscoveryDeadline,
		MaxParallelTasks:  initialCfg.MaxParallelTasks,
	}
	compReconciler := &ComparisonReconciler{
		ArtifactSink: reloadableSink,
	}

	reloader := NewConfigReloader(ConfigReloaderOptions{
		RawFlags:       rawFlags,
		Logger:         logr.Discard(),
		SinkFactory:    mockFactory,
		ReloadableSink: reloadableSink,
		RunReconciler:  runReconciler,
		CompReconciler: compReconciler,
		InitialConfig:  &initialCfg,
	})

	reloadDone := make(chan struct{}, 10)
	go func() {
		_ = sinkconfig.WatchConfigMap(ctx, clientset, ns, cmName, func(cm *corev1.ConfigMap) {
			_ = reloader.Reload(ctx, cm)
			reloadDone <- struct{}{}
		})
	}()

	// Wait for initial watch event
	select {
	case <-reloadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial watch callback")
	}

	select {
	case <-watchReady:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch registration")
	}

	if len(factoryCalls) != 1 || factoryCalls[0] != "initial-bucket" {
		t.Fatalf("expected 1 factory call for initial-bucket, got %v", factoryCalls)
	}
	if createdSinks[0].closed.Load() {
		t.Fatal("initial sink should not be closed")
	}

	// Update ConfigMap to new bucket and new discoveryDeadline
	updatedCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            cmName,
			ResourceVersion: "101",
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactBucket:    "new-bucket",
			configkeys.ConfigMapKeyDiscoveryDeadline: "15m",
			configkeys.ConfigMapKeyMaxParallelTasks:  "8",
		},
	}
	_, err = clientset.CoreV1().ConfigMaps(ns).Update(ctx, updatedCM, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("failed to update ConfigMap: %v", err)
	}

	select {
	case <-reloadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update reload callback")
	}

	// Assert factory was called with the new bucket
	if len(factoryCalls) != 2 || factoryCalls[1] != "new-bucket" {
		t.Fatalf("expected sink factory called with new-bucket, got %v", factoryCalls)
	}

	// Assert the old sink is closed
	deadline := time.Now().Add(2 * time.Second)
	for !createdSinks[0].closed.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !createdSinks[0].closed.Load() {
		t.Fatal("expected old sink (createdSinks[0]) to be closed after ConfigMap update")
	}

	// Assert new sink is not closed
	if createdSinks[1].closed.Load() {
		t.Fatal("new sink (createdSinks[1]) should not be closed")
	}

	// Assert reloadable keys updated on reconcilers
	if runReconciler.discoveryDeadline() != 15*time.Minute {
		t.Errorf("expected discoveryDeadline 15m, got %v", runReconciler.discoveryDeadline())
	}
	if runReconciler.maxParallelTasks() != 8 {
		t.Errorf("expected maxParallelTasks 8, got %d", runReconciler.maxParallelTasks())
	}

	// Verify in-flight safety on next swap:
	// Start an in-flight upload on createdSinks[1]
	createdSinks[1].blockUpload = make(chan struct{})
	createdSinks[1].uploadStarted = make(chan struct{})

	uploadDone := make(chan struct{})
	go func() {
		_, _ = reloadableSink.StoreArtifact(ctx, artifacts.RunMetadata{}, "test.json", []byte("in-flight"))
		close(uploadDone)
	}()

	<-createdSinks[1].uploadStarted
	time.Sleep(20 * time.Millisecond)

	// Update ConfigMap again to third-bucket
	thirdCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            cmName,
			ResourceVersion: "102",
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactBucket: "third-bucket",
		},
	}
	_, err = clientset.CoreV1().ConfigMaps(ns).Update(ctx, thirdCM, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("failed to update ConfigMap to third-bucket: %v", err)
	}

	select {
	case <-reloadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for third reload callback")
	}

	// Assert createdSinks[1] is NOT closed yet while upload is in-flight
	if createdSinks[1].closed.Load() {
		t.Fatal("expected createdSinks[1] to NOT be closed while in-flight upload is active")
	}

	// Unblock in-flight upload
	close(createdSinks[1].blockUpload)
	<-uploadDone

	deadline = time.Now().Add(2 * time.Second)
	for !createdSinks[1].closed.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !createdSinks[1].closed.Load() {
		t.Fatal("expected createdSinks[1] to be closed after in-flight upload completes")
	}
}

func TestConfigReloader_UpdatesWorkloadIdentityProvider_SinkReloaded(t *testing.T) {
	ctx := t.Context()

	initialCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactBucket: "my-bucket",
		},
	}

	var factoryCalls int
	var capturedProvider string
	mockFactory := func(ctx context.Context, bucket, dir, workloadIdentityProvider string, maxUploadBytes int64) (artifacts.ArtifactSink, error) {
		factoryCalls++
		capturedProvider = workloadIdentityProvider
		return &trackingTestSink{bucket: bucket, dir: dir}, nil
	}

	rawFlags := RawFlags{}
	initialCfg, err := ResolveConfig(initialCM, rawFlags)
	if err != nil {
		t.Fatalf("failed to resolve initial config: %v", err)
	}

	initialSink, err := mockFactory(ctx, initialCfg.ArtifactBucket, initialCfg.ArtifactDir, initialCfg.GCPWorkloadIdentityProvider, initialCfg.ArtifactMaxUploadBytes)
	if err != nil {
		t.Fatalf("failed to construct initial sink: %v", err)
	}
	reloadableSink := artifacts.NewReloadableSink(initialSink)
	defer reloadableSink.Close()

	reloader := NewConfigReloader(ConfigReloaderOptions{
		RawFlags:       rawFlags,
		Logger:         logr.Discard(),
		SinkFactory:    mockFactory,
		ReloadableSink: reloadableSink,
		InitialConfig:  &initialCfg,
	})

	// Reload with same config: no reload
	if err := reloader.Reload(ctx, initialCM); err != nil {
		t.Fatalf("Reload failed: %v", err)
	}
	if factoryCalls != 1 {
		t.Fatalf("expected factoryCalls=1, got %d", factoryCalls)
	}

	// Reload with gcpWorkloadIdentityProvider added: sink should be reloaded
	const provider = "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/p/providers/pr"
	updatedCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      DefaultConfigMapName,
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactBucket:              "my-bucket",
			configkeys.ConfigMapKeyGCPWorkloadIdentityProvider: provider,
		},
	}

	if err := reloader.Reload(ctx, updatedCM); err != nil {
		t.Fatalf("Reload with provider failed: %v", err)
	}
	if factoryCalls != 2 {
		t.Fatalf("expected factoryCalls=2 after adding provider, got %d", factoryCalls)
	}
	if capturedProvider != provider {
		t.Fatalf("expected provider %q passed on reload, got %q", provider, capturedProvider)
	}
}

func TestConfigReloader_UpdatesModelPrices_LookupAndDisabledFlipsDynamically(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const ns = "eval-runner"
	const cmName = DefaultConfigMapName

	// Initial ConfigMap without modelPrices (defaults apply)
	initialCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            cmName,
			ResourceVersion: "100",
		},
		Data: map[string]string{},
	}

	clientset := k8sfake.NewSimpleClientset(initialCM)

	watchReady := make(chan struct{})
	var watchOnce sync.Once
	clientset.PrependWatchReactor("configmaps", func(action k8stesting.Action) (bool, watch.Interface, error) {
		w, err := clientset.Tracker().Watch(action.GetResource(), action.GetNamespace())
		watchOnce.Do(func() { close(watchReady) })
		return true, w, err
	})

	rawFlags := RawFlags{}
	initialCfg, err := ResolveConfig(initialCM, rawFlags)
	if err != nil {
		t.Fatalf("failed to resolve initial config: %v", err)
	}

	runReconciler := &RunReconciler{
		ModelPrices:         initialCfg.ModelPrices,
		ModelPriceSources:   initialCfg.ModelPriceSources,
		ModelPricesDisabled: initialCfg.ModelPricesDisabled,
	}
	compReconciler := &ComparisonReconciler{
		ModelPrices:         initialCfg.ModelPrices,
		ModelPriceSources:   initialCfg.ModelPriceSources,
		ModelPricesDisabled: initialCfg.ModelPricesDisabled,
	}

	reloader := NewConfigReloader(ConfigReloaderOptions{
		RawFlags:       rawFlags,
		Logger:         logr.Discard(),
		RunReconciler:  runReconciler,
		CompReconciler: compReconciler,
		InitialConfig:  &initialCfg,
	})

	reloadDone := make(chan struct{}, 10)
	go func() {
		_ = sinkconfig.WatchConfigMap(ctx, clientset, ns, cmName, func(cm *corev1.ConfigMap) {
			_ = reloader.Reload(ctx, cm)
			reloadDone <- struct{}{}
		})
	}()

	select {
	case <-reloadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial watch callback")
	}

	select {
	case <-watchReady:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch registration")
	}

	// Initial lookup returns default pricing
	p, src, ok := runReconciler.lookupModelPrice("google/gemini-3.5-flash-lite")
	if !ok || p.InputPer1M != 0.30 || p.OutputPer1M != 2.50 || src != v1alpha1.CostSourceDefault {
		t.Fatalf("expected initial default price for gemini-3.5-flash-lite, got price=%+v, src=%q, ok=%v", p, src, ok)
	}
	cp, csrc, cok := compReconciler.lookupModelPrice("google/gemini-3.5-flash-lite")
	if !cok || cp.InputPer1M != 0.30 || cp.OutputPer1M != 2.50 || csrc != v1alpha1.CostSourceDefault {
		t.Fatalf("expected compReconciler initial default price, got price=%+v, src=%q, ok=%v", cp, csrc, cok)
	}

	// 1. Update ConfigMap to override gemini-3.5-flash-lite and add prefix:custom/
	updatedCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            cmName,
			ResourceVersion: "101",
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyModelPrices: `{"google/gemini-3.5-flash-lite":{"inputPer1M":0.50,"outputPer1M":3.00},"prefix:custom/":{"inputPer1M":1.0,"outputPer1M":2.0}}`,
		},
	}
	if _, err := clientset.CoreV1().ConfigMaps(ns).Update(ctx, updatedCM, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to update ConfigMap: %v", err)
	}

	select {
	case <-reloadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update reload callback")
	}

	pOverridden, srcOverridden, okOverridden := runReconciler.lookupModelPrice("google/gemini-3.5-flash-lite")
	if !okOverridden || pOverridden.InputPer1M != 0.50 || pOverridden.OutputPer1M != 3.00 || srcOverridden != v1alpha1.CostSourceConfigMap {
		t.Fatalf("expected overridden price from configmap, got price=%+v, src=%q, ok=%v", pOverridden, srcOverridden, okOverridden)
	}

	pPrefix, srcPrefix, okPrefix := compReconciler.lookupModelPrice("custom/model-x")
	if !okPrefix || pPrefix.InputPer1M != 1.0 || pPrefix.OutputPer1M != 2.0 || srcPrefix != v1alpha1.CostSourceConfigMap {
		t.Fatalf("expected prefix price from configmap, got price=%+v, src=%q, ok=%v", pPrefix, srcPrefix, okPrefix)
	}

	// Non-overridden default remains active
	pDef, srcDef, okDef := runReconciler.lookupModelPrice("google/gemini-2.5-pro")
	if !okDef || pDef.InputPer1M != 1.25 || pDef.OutputPer1M != 10.00 || srcDef != v1alpha1.CostSourceDefault {
		t.Fatalf("expected non-overridden default price, got price=%+v, src=%q, ok=%v", pDef, srcDef, okDef)
	}

	// 2. Update ConfigMap to sentinel "none" (disables model prices entirely)
	disabledCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            cmName,
			ResourceVersion: "102",
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyModelPrices: "none",
		},
	}
	if _, err := clientset.CoreV1().ConfigMaps(ns).Update(ctx, disabledCM, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to update ConfigMap to none: %v", err)
	}

	select {
	case <-reloadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for disable reload callback")
	}

	_, _, okDisabled := runReconciler.lookupModelPrice("google/gemini-3.5-flash-lite")
	if okDisabled {
		t.Fatalf("expected lookup to return false when pricing disabled via none")
	}

	// 3. Invalid JSON update: reload fails and retains previous configuration
	invalidCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            cmName,
			ResourceVersion: "103",
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyModelPrices: "not-valid-json",
		},
	}
	if _, err := clientset.CoreV1().ConfigMaps(ns).Update(ctx, invalidCM, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to update ConfigMap with invalid json: %v", err)
	}

	select {
	case <-reloadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for invalid reload callback")
	}

	// Should still be disabled (previous state retained)
	_, _, okStillDisabled := runReconciler.lookupModelPrice("google/gemini-3.5-flash-lite")
	if okStillDisabled {
		t.Fatalf("expected lookup to still return false after invalid json reload failure")
	}
}

// TestResolveConfig_Repositories_InvalidEntryRejected verifies that invalid entries in the
// repositories ConfigMap key are rejected with a clear error, and ReloadWait retains active settings.
func TestResolveConfig_Repositories_InvalidEntryRejected(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      "eval-runner-config",
		},
		Data: map[string]string{
			ConfigMapKeyRepositories: "- name: nonsense\n",
		},
	}

	_, err := ResolveConfig(cm, RawFlags{})
	if err == nil {
		t.Fatalf("expected ResolveConfig to reject invalid repository nonsense, got nil")
	}

	expectedSubstr := `invalid repository "nonsense": invalid host, owner, or repo name`
	if !strings.Contains(err.Error(), expectedSubstr) {
		t.Errorf("expected error message to contain %q, got %q", expectedSubstr, err.Error())
	}

	// Test with ReloadWait: ensures reload fails and retains previous configuration
	reloader := NewConfigReloader(ConfigReloaderOptions{
		Logger: logr.Discard(),
	})
	reloadErr := reloader.ReloadWait(t.Context(), cm)
	if reloadErr == nil {
		t.Fatalf("expected ReloadWait to return error on invalid repository configuration")
	}
}

// TestConfigReloader_Repositories_ReloadTriggersImmediateSync verifies that adding a repository
// on runtime reload triggers an immediate sync of it.
func TestConfigReloader_Repositories_ReloadTriggersImmediateSync(t *testing.T) {
	ctx := t.Context()
	ref := forge.RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"}

	ff := forge.NewFakeForge("fake")
	ff.SetRepository(ref, &forge.Repository{Ref: ref, DefaultBranch: "main", CloneURL: "https://github.com/gke-labs/eval-runner.git"})
	ff.SetBranches(ref, []forge.Branch{{Name: "main", SHA: "0000000000000000000000000000000000000001"}})
	ff.SetChangeRequests(ref, []forge.ChangeRequest{})
	reg := forge.NewRegistry(ff)

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "controller-syncer-test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer st.Close()

	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	cons := consumer.New(cl, consumer.Config{
		Namespace:    "eval-runner",
		DefaultModel: "mockllm/model",
		Store:        st,
	})

	syncInst := syncer.New(syncer.SyncerOptions{
		Store:    st,
		Registry: reg,
		Consumer: cons,
	})
	go func() {
		_ = syncInst.Start(ctx)
	}()

	initialCfg := DefaultClusterConfig()
	testSink := &testLogSink{}
	reloader := NewConfigReloader(ConfigReloaderOptions{
		Logger:        logr.New(testSink),
		Syncer:        syncInst,
		Consumer:      cons,
		InitialConfig: &initialCfg,
	})

	// Initial state: store does not have main branch for ref
	branchesBefore, _ := st.ListBranches(ref)
	if len(branchesBefore) != 0 {
		t.Fatalf("expected 0 branches before reload")
	}

	// ConfigMap update adding the repository
	updatedCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      "eval-runner-config",
		},
		Data: map[string]string{
			ConfigMapKeyRepositories:            "- name: gke-labs/eval-runner\n  branches: [main]\n",
			configkeys.ConfigMapKeyDefaultModel: "mockllm/model",
		},
	}

	if err := reloader.ReloadWait(ctx, updatedCM); err != nil {
		t.Fatalf("ReloadWait failed: %v", err)
	}

	// Verify that 'syncing newly added repository' was logged
	var logFound bool
	for _, entry := range testSink.entries {
		if strings.Contains(entry, "syncing newly added repository") && strings.Contains(entry, ref.String()) {
			logFound = true
			break
		}
	}
	if !logFound {
		t.Errorf("expected 'syncing newly added repository' log line for %s, got log entries: %v", ref.String(), testSink.entries)
	}

	// Verify immediate sync of newly added repository: store now contains main branch!
	var branchesAfter []forge.Branch
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		branchesAfter, _ = st.ListBranches(ref)
		if len(branchesAfter) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(branchesAfter) != 1 || branchesAfter[0].Name != "main" {
		t.Fatalf("expected immediate sync to commit main branch to store, got %v", branchesAfter)
	}

	// Verify consumer created the baseline run
	var runs v1alpha1.RunList
	if err := cl.List(ctx, &runs, client.InNamespace("eval-runner")); err != nil {
		t.Fatalf("failed to list runs: %v", err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("expected consumer to create 1 baseline run immediately upon reload sync, got %d", len(runs.Items))
	}
}

func TestResolveConfig_CadenceIntervalsAndRateLimitFloor(t *testing.T) {
	// 1. Defaults
	defCfg, err := ResolveConfig(&corev1.ConfigMap{}, RawFlags{})
	if err != nil {
		t.Fatalf("ResolveConfig default failed: %v", err)
	}
	if defCfg.LsRemoteInterval != syncer.DefaultLsRemoteInterval {
		t.Errorf("LsRemoteInterval default = %v, want %v", defCfg.LsRemoteInterval, syncer.DefaultLsRemoteInterval)
	}
	if defCfg.FullReconcileInterval != syncer.DefaultFullReconcileInterval {
		t.Errorf("FullReconcileInterval default = %v, want %v", defCfg.FullReconcileInterval, syncer.DefaultFullReconcileInterval)
	}
	if defCfg.RateLimitFloorPercent != syncer.DefaultRateFloorPercent {
		t.Errorf("RateLimitFloorPercent default = %v, want %v", defCfg.RateLimitFloorPercent, syncer.DefaultRateFloorPercent)
	}

	// 2. Custom ConfigMap values
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      "eval-runner-config",
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyLsRemoteInterval:      "45s",
			configkeys.ConfigMapKeyFullReconcileInterval: "2h",
			configkeys.ConfigMapKeyRateLimitFloorPercent: "20",
		},
	}
	cfg, err := ResolveConfig(cm, RawFlags{})
	if err != nil {
		t.Fatalf("ResolveConfig with custom intervals failed: %v", err)
	}
	if cfg.LsRemoteInterval != 45*time.Second {
		t.Errorf("LsRemoteInterval = %v, want 45s", cfg.LsRemoteInterval)
	}
	if cfg.FullReconcileInterval != 2*time.Hour {
		t.Errorf("FullReconcileInterval = %v, want 2h", cfg.FullReconcileInterval)
	}
	if cfg.RateLimitFloorPercent != 20 {
		t.Errorf("RateLimitFloorPercent = %v, want 20", cfg.RateLimitFloorPercent)
	}

	// 3. Invalid values rejected
	badCM := &corev1.ConfigMap{
		Data: map[string]string{
			configkeys.ConfigMapKeyLsRemoteInterval: "not-a-duration",
		},
	}
	if _, err := ResolveConfig(badCM, RawFlags{}); err == nil {
		t.Errorf("expected error for invalid lsRemoteInterval, got nil")
	}

	badFloorCM := &corev1.ConfigMap{
		Data: map[string]string{
			configkeys.ConfigMapKeyRateLimitFloorPercent: "not-an-int",
		},
	}
	if _, err := ResolveConfig(badFloorCM, RawFlags{}); err == nil {
		t.Errorf("expected error for invalid rateLimitFloorPercent, got nil")
	}
}

func TestConfigReloader_ProbeCredentials_UnavailableOnFailure(t *testing.T) {
	ctx := t.Context()
	const ns = "eval-runner"
	const cmName = DefaultConfigMapName

	initialSink := artifacts.NewInMemorySink()
	reloadableSink := artifacts.NewReloadableSink(initialSink)

	probeErr := errors.New("dial tcp 169.254.169.254:80: connect: connection refused")
	fakeStorage := fakes.NewFakeStorageClient()
	fakeStorage.BucketAccessError = probeErr

	mockFactory := func(ctx context.Context, bucket, dir, workloadIdentityProvider string, maxUploadBytes int64) (artifacts.ArtifactSink, error) {
		if bucket == "failing-bucket" {
			return gcs.New(ctx, bucket, gcs.WithStorageClient(fakeStorage))
		}
		if bucket == "working-bucket" {
			workingStorage := fakes.NewFakeStorageClient()
			return gcs.New(ctx, bucket, gcs.WithStorageClient(workingStorage))
		}
		return artifacts.NewInMemorySink(), nil
	}

	reloader := NewConfigReloader(ConfigReloaderOptions{
		ReloadableSink: reloadableSink,
		SinkFactory:    mockFactory,
		Logger:         logr.Discard(),
	})

	// 1. Reload with failing-bucket: probe fails, sink marked unavailable
	failingCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: cmName, ResourceVersion: "1"},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactBucket: "failing-bucket",
		},
	}
	if err := reloader.ReloadWait(ctx, failingCM); err != nil {
		t.Fatalf("ReloadWait failed: %v", err)
	}

	err, isUnavail := artifacts.IsUnavailable(reloadableSink)
	if !isUnavail {
		t.Fatalf("expected sink to be marked unavailable after failed probe")
	}
	if !errors.Is(err, probeErr) {
		t.Errorf("expected probe error %v, got %v", probeErr, err)
	}

	// 2. Subsequent reload with working-bucket: probe succeeds, sink becomes available
	workingCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: cmName, ResourceVersion: "2"},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactBucket: "working-bucket",
		},
	}
	if err := reloader.ReloadWait(ctx, workingCM); err != nil {
		t.Fatalf("ReloadWait failed: %v", err)
	}

	_, isUnavail = artifacts.IsUnavailable(reloadableSink)
	if isUnavail {
		t.Fatalf("expected sink to be available after working probe")
	}
}

func TestResolveConfig_HeadTopUpKeys(t *testing.T) {
	// 1. Defaults when unset: interval=0 (disabled), ci=0.05, maxSamples=64
	emptyCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
		Data:       map[string]string{},
	}
	cfg, err := ResolveConfig(emptyCM, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HeadTopUpInterval != 0 {
		t.Errorf("expected HeadTopUpInterval=0, got %v", cfg.HeadTopUpInterval)
	}
	if cfg.HeadTargetCIHalfWidth != DefaultHeadTargetCIHalfWidth {
		t.Errorf("expected HeadTargetCIHalfWidth=%v, got %v", DefaultHeadTargetCIHalfWidth, cfg.HeadTargetCIHalfWidth)
	}
	if cfg.HeadMaxSamplesPerTask != 64 {
		t.Errorf("expected HeadMaxSamplesPerTask=64, got %v", cfg.HeadMaxSamplesPerTask)
	}

	// 2. Custom valid values
	customCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
		Data: map[string]string{
			configkeys.ConfigMapKeyHeadTopUpInterval:     "4h",
			configkeys.ConfigMapKeyHeadTargetCIHalfWidth: "0.03",
			configkeys.ConfigMapKeyHeadMaxSamplesPerTask: "32",
		},
	}
	cfg, err = ResolveConfig(customCM, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HeadTopUpInterval != 4*time.Hour {
		t.Errorf("expected HeadTopUpInterval=4h, got %v", cfg.HeadTopUpInterval)
	}
	if cfg.HeadTargetCIHalfWidth != 0.03 {
		t.Errorf("expected HeadTargetCIHalfWidth=0.03, got %v", cfg.HeadTargetCIHalfWidth)
	}
	if cfg.HeadMaxSamplesPerTask != 32 {
		t.Errorf("expected HeadMaxSamplesPerTask=32, got %v", cfg.HeadMaxSamplesPerTask)
	}

	// 3. Disabling interval via "0" or "none"
	disabledCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
		Data: map[string]string{
			configkeys.ConfigMapKeyHeadTopUpInterval: "none",
		},
	}
	cfg, err = ResolveConfig(disabledCM, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HeadTopUpInterval != 0 {
		t.Errorf("expected HeadTopUpInterval=0 for 'none', got %v", cfg.HeadTopUpInterval)
	}

	// 4. headMaxSamplesPerTask inherits defaultMaxSamplesPerTask if not set explicitly
	inheritCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
		Data: map[string]string{
			configkeys.ConfigMapKeyDefaultMaxSamplesPerTask: "128",
		},
	}
	cfg, err = ResolveConfig(inheritCM, RawFlags{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HeadMaxSamplesPerTask != 128 {
		t.Errorf("expected HeadMaxSamplesPerTask=128 (inherited), got %v", cfg.HeadMaxSamplesPerTask)
	}

	// 5. Invalid values
	invalidCases := []struct {
		name string
		data map[string]string
	}{
		{
			name: "negative interval",
			data: map[string]string{configkeys.ConfigMapKeyHeadTopUpInterval: "-1h"},
		},
		{
			name: "invalid interval duration",
			data: map[string]string{configkeys.ConfigMapKeyHeadTopUpInterval: "not-a-duration"},
		},
		{
			name: "zero ci half-width",
			data: map[string]string{configkeys.ConfigMapKeyHeadTargetCIHalfWidth: "0"},
		},
		{
			name: "negative ci half-width",
			data: map[string]string{configkeys.ConfigMapKeyHeadTargetCIHalfWidth: "-0.05"},
		},
		{
			name: "ci half-width >= 1",
			data: map[string]string{configkeys.ConfigMapKeyHeadTargetCIHalfWidth: "1.0"},
		},
		{
			name: "zero max samples per task",
			data: map[string]string{configkeys.ConfigMapKeyHeadMaxSamplesPerTask: "0"},
		},
		{
			name: "negative max samples per task",
			data: map[string]string{configkeys.ConfigMapKeyHeadMaxSamplesPerTask: "-5"},
		},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: "eval-runner", Name: "eval-runner-config"},
				Data:       tc.data,
			}
			_, err := ResolveConfig(cm, RawFlags{})
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}
