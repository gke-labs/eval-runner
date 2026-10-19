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
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/pkg/artifacts/gcs"
	"github.com/gke-labs/eval-runner/pkg/configkeys"
	"github.com/gke-labs/eval-runner/pkg/discovery"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
		if cfg != builtInDefaults {
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
		if cfg != builtInDefaults {
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
	if cfg != expected {
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

	// Parse with only artifact-bucket, max-parallel-tasks, ui-base-url, strict-config, artifact-max-upload-bytes, inconclusive-policy, comparison-ttl set
	err := fs.Parse([]string{"--artifact-bucket=gs://custom", "--max-parallel-tasks=10", "--sandboxd-image=my-sandboxd:tag", "--github-token-secret=my-gh-secret", "--ui-base-url=https://eval.example.com", "--strict-config", "--artifact-max-upload-bytes=5242880", "--inconclusive-policy=failure", "--comparison-ttl=24h"})
	if err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	rawFlags := FlagsFromFlagSet(fs, *bucket, *dir, *secret, *sandboxd, *githubToken, *githubApp, *uiURL, *discovery, *execution, *grace, *parallel, *strictConfig, *uploadBytes, *inconclusivePolicy, *compTTL)
	if !rawFlags.IsExplicit("artifact-bucket") {
		t.Errorf("expected artifact-bucket to be explicit")
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
			ConfigMapKeySandboxdImage:                       "img:tag",
			ConfigMapKeyTaskGracePeriod:                     "30s",
			ConfigMapKeyGitHubTokenSecret:                   "gh-token",
			ConfigMapKeyGitHubAppSecret:                     "gh-app",
			ConfigMapKeyUIBaseURL:                           "https://ui",
			ConfigMapKeyInconclusivePolicy:                  "neutral",
			configkeys.ConfigMapKeyGitHubWebhookSecret:      "gh-wh",
			configkeys.ConfigMapKeyWebhookRepositories:      "org/repo",
			configkeys.ConfigMapKeyDefaultModel:             "model",
			configkeys.ConfigMapKeyDefaultLimit:             "5",
			configkeys.ConfigMapKeyDefaultBatchSize:         "8",
			configkeys.ConfigMapKeyDefaultMaxSamplesPerTask: "64",
			configkeys.ConfigMapKeyCommandUsers:             "user1,user2",
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
	if cfg != DefaultClusterConfig() {
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
				ConfigMapKeyArtifactBucket:    "",
				ConfigMapKeyArtifactDir:       "",
				ConfigMapKeyDiscoveryDeadline: "",
				ConfigMapKeyExecutionDeadline: "",
				ConfigMapKeyMaxParallelTasks:  "",
				ConfigMapKeyModelSecret:       "",
				ConfigMapKeySandboxdImage:     "",
				ConfigMapKeyTaskGracePeriod:   "",
				ConfigMapKeyGitHubTokenSecret: "",
				ConfigMapKeyGitHubAppSecret:   "",
				ConfigMapKeyUIBaseURL:         "",
			},
		}

		cfg, err := ResolveConfig(cm, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != defaults {
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
				ConfigMapKeyModelSecret:       "   ",
				ConfigMapKeyGitHubTokenSecret: " \t ",
				ConfigMapKeyDiscoveryDeadline: " ",
			},
		}

		cfg, err := ResolveConfig(cm, RawFlags{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ModelSecret != defaults.ModelSecret {
			t.Errorf("expected ModelSecret default %q, got %q", defaults.ModelSecret, cfg.ModelSecret)
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
