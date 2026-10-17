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
	"strings"
	"testing"
	"time"

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

	// Parse with only artifact-bucket and max-parallel-tasks set
	err := fs.Parse([]string{"--artifact-bucket=gs://custom", "--max-parallel-tasks=10", "--sandboxd-image=my-sandboxd:tag"})
	if err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	rawFlags := FlagsFromFlagSet(fs, *bucket, *dir, *secret, *sandboxd, *discovery, *execution, *grace, *parallel)
	if !rawFlags.IsExplicit("artifact-bucket") {
		t.Errorf("expected artifact-bucket to be explicit")
	}
	if !rawFlags.IsExplicit("max-parallel-tasks") {
		t.Errorf("expected max-parallel-tasks to be explicit")
	}
	if !rawFlags.IsExplicit("sandboxd-image") {
		t.Errorf("expected sandboxd-image to be explicit")
	}
	if rawFlags.IsExplicit("discovery-deadline") {
		t.Errorf("expected discovery-deadline NOT to be explicit")
	}
	if rawFlags.IsExplicit("model-secret") {
		t.Errorf("expected model-secret NOT to be explicit")
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
