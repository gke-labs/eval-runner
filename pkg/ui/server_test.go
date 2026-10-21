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

package ui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/artifacts/sinkconfig"
	"github.com/gke-labs/eval-runner/pkg/configkeys"
	"github.com/gke-labs/eval-runner/pkg/stats"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add v1alpha1 to scheme: %v", err)
	}
	return s
}

func sampleFixtures() (*v1alpha1.Run, *v1alpha1.Run, *v1alpha1.Comparison) {
	rate084 := 0.84
	rate061 := 0.61
	delta23 := 0.23
	pImp := 0.98

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "docs-eval-base-05ce4d8",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-15 * time.Minute)),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "a81d0c4111111111111111111111111111111111",
			},
			Model: "google/gemini-3.5-flash-lite",
		},
		Status: v1alpha1.RunStatus{
			Phase:     v1alpha1.RunPhaseCompleted,
			Artifacts: "gs://eval-bucket/runs/a81d0c4/",
			Summary: &v1alpha1.RunSummary{
				Tasks:          "1/1",
				TasksCompleted: 1,
				TasksTotal:     1,
				TotalSamples:   48,
				Rates:          "triage/runbook_lookup=0.61[0.46,0.74]",
			},
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          "triage/runbook_lookup",
					Phase:       v1alpha1.TaskPhaseCompleted,
					Samples:     48,
					Successes:   29,
					SuccessRate: &rate061,
					SuccessRateCI: &v1alpha1.ConfidenceInterval{
						Lower:      0.46,
						Upper:      0.74,
						Confidence: 0.95,
						Method:     "wilson",
					},
					LogPath: "gs://eval-bucket/runs/a81d0c4/triage/runbook_lookup/inspect.json",
				},
			},
		},
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "docs-eval-cand-05ce4d8",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "05ce4d8222222222222222222222222222222222",
			},
			Model: "google/gemini-3.5-flash-lite",
		},
		Status: v1alpha1.RunStatus{
			Phase:     v1alpha1.RunPhaseCompleted,
			Artifacts: "gs://eval-bucket/runs/05ce4d8/",
			Summary: &v1alpha1.RunSummary{
				Tasks:          "1/1",
				TasksCompleted: 1,
				TasksTotal:     1,
				TotalSamples:   48,
				Rates:          "triage/runbook_lookup=0.84[0.71,0.92]",
			},
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          "triage/runbook_lookup",
					Phase:       v1alpha1.TaskPhaseCompleted,
					Samples:     48,
					Successes:   40,
					SuccessRate: &rate084,
					SuccessRateCI: &v1alpha1.ConfidenceInterval{
						Lower:      0.71,
						Upper:      0.92,
						Confidence: 0.95,
						Method:     "wilson",
					},
					LogPath: "gs://eval-bucket/runs/05ce4d8/triage/runbook_lookup/inspect.json",
				},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "docs-eval-same-commit",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-5 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "docs-eval-base-05ce4d8",
			},
			Candidate: v1alpha1.ComparisonRunRef{
				Run: "docs-eval-cand-05ce4d8",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:        v1alpha1.ComparisonPhaseCompleted,
			Verdict:      v1alpha1.ComparisonVerdictImproved,
			PImprovement: &pImp,
			SummaryPath:  "gs://eval-bucket/comparisons/docs-eval-same-commit.json",
			Summary: &v1alpha1.ComparisonSummary{
				Tasks: "1/1",
			},
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:            "triage/runbook_lookup",
					BaseRate:      &rate061,
					CandidateRate: &rate084,
					Delta:         &delta23,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: 0.11,
						Upper: 0.35,
					},
					Pairs: 48,
					Dropped: v1alpha1.ComparisonDropped{
						Candidate: 0,
						Baseline:  0,
					},
					Label: v1alpha1.ComparisonTaskLabelImproved,
				},
			},
		},
	}

	return baseRun, candRun, comp
}

func TestServerHealthz(t *testing.T) {
	scheme := newTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "ok") {
		t.Errorf("expected body to contain 'ok', got %q", w.Body.String())
	}
}

func TestServerRootRedirect(t *testing.T) {
	scheme := newTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("expected 302 Found, got %d", w.Code)
	}
	loc := w.Header().Get("Location")
	if loc != "/runs" {
		t.Errorf("expected redirect to /runs, got %q", loc)
	}
}

func TestServerRunsList(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/runs", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "docs-eval-base-05ce4d8") {
		t.Errorf("expected body to contain base run name, got: %s", body)
	}
	if !strings.Contains(body, "docs-eval-cand-05ce4d8") {
		t.Errorf("expected body to contain candidate run name, got: %s", body)
	}
	if !strings.Contains(body, "chip good") {
		t.Errorf("expected Completed phase chip to have 'chip good'")
	}
	if !strings.Contains(body, "/runs/eval-runner/docs-eval-base-05ce4d8") {
		t.Errorf("expected link to run detail")
	}

	// Trailing slash support
	reqSlash := httptest.NewRequest(http.MethodGet, "/runs/", nil)
	wSlash := httptest.NewRecorder()
	server.ServeHTTP(wSlash, reqSlash)
	if wSlash.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /runs/, got %d", wSlash.Code)
	}
}

func TestServerRunDetail(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/docs-eval-cand-05ce4d8", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()

	// Check header
	if !strings.Contains(body, "docs-eval-cand-05ce4d8") {
		t.Errorf("expected run name in header")
	}
	if !strings.Contains(body, "google/gemini-3.5-flash-lite") {
		t.Errorf("expected model name in header")
	}
	if !strings.Contains(body, "05ce4d8") {
		t.Errorf("expected commit in header")
	}
	if !strings.Contains(body, "https://github.com/gke-labs/eval-runner.git") {
		t.Errorf("expected repo URL in header")
	}

	// Check task row
	if !strings.Contains(body, "triage/runbook_lookup") {
		t.Errorf("expected task id in table")
	}
	if !strings.Contains(body, "0.84") {
		t.Errorf("expected rate 0.84 in table")
	}
	if !strings.Contains(body, "wilson") {
		t.Errorf("expected method 'wilson' in table")
	}
	if !strings.Contains(body, "cibar-01") {
		t.Errorf("expected 0-1 CI bar element")
	}
	expectedLink := "/runs/eval-runner/docs-eval-cand-05ce4d8/tasks/triage%2Frunbook_lookup/samples"
	if strings.Contains(body, expectedLink) {
		t.Errorf("expected NO link to task samples list when ArtifactSink is nil")
	}
	if !strings.Contains(body, "artifacts unavailable") {
		t.Errorf("expected 'artifacts unavailable' note when ArtifactSink is nil")
	}
	if !strings.Contains(body, "artifacts: none") {
		t.Errorf("expected 'artifacts: none' in footer when ArtifactSink is nil")
	}
	if !strings.Contains(body, "gs://eval-bucket/runs/05ce4d8/triage/runbook_lookup/inspect.json") {
		t.Errorf("expected log path in table")
	}

	// When ArtifactSink is configured, samples list link is present
	sinkDir := t.TempDir()
	serverWithSink := NewServer(Config{Client: fakeClient, ArtifactSink: &artifacts.LocalDirSink{BaseDir: sinkDir}})
	wWithSink := httptest.NewRecorder()
	serverWithSink.ServeHTTP(wWithSink, req)
	bodyWithSink := wWithSink.Body.String()
	if !strings.Contains(bodyWithSink, expectedLink) {
		t.Errorf("expected link to task samples list %q in table when sink configured", expectedLink)
	}
	if strings.Contains(bodyWithSink, "artifacts unavailable") {
		t.Errorf("expected NO 'artifacts unavailable' note when sink configured")
	}
	if !strings.Contains(bodyWithSink, fmt.Sprintf("artifacts: dir %s", sinkDir)) {
		t.Errorf("expected 'artifacts: dir %s' in footer", sinkDir)
	}

	// Test 404 for missing run
	reqNotFound := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/non-existent-run", nil)
	wNotFound := httptest.NewRecorder()
	server.ServeHTTP(wNotFound, reqNotFound)
	if wNotFound.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing run, got %d", wNotFound.Code)
	}
	if strings.TrimSpace(wNotFound.Body.String()) != "run not found" {
		t.Errorf("expected 'run not found', got %q", wNotFound.Body.String())
	}
	if strings.Contains(wNotFound.Body.String(), "non-existent-run") {
		t.Errorf("expected no reflected input in body, got %q", wNotFound.Body.String())
	}
}

type mockFooterSink struct {
	artifacts.ArtifactSink
	bucket string
}

func (m *mockFooterSink) Bucket() string {
	return m.bucket
}

func (m *mockFooterSink) ArtifactPrefix(commit string) string {
	return "gs://" + m.bucket + "/" + commit
}

func (m *mockFooterSink) Close() error {
	return nil
}

func TestServer_ReloadConfigMap_FooterReflectsNewValueWithoutRestart(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()

	// 1. Initial server without sink
	server := NewServer(Config{Client: fakeClient, DefaultNamespace: "eval-runner"})

	req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/docs-eval-cand-05ce4d8", nil)
	w1 := httptest.NewRecorder()
	server.ServeHTTP(w1, req)

	if w1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w1.Code)
	}
	body1 := w1.Body.String()
	if !strings.Contains(body1, "artifacts: none") {
		t.Errorf("expected 'artifacts: none' in initial footer, got: %s", body1)
	}
	if server.ConfigGeneration() != 1 {
		t.Errorf("expected initial config generation 1, got %d", server.ConfigGeneration())
	}

	// 2. Reload with GCS bucket without restarting server
	cmBucket := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      sinkconfig.DefaultConfigMapName,
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactBucket: "gs://reloaded-eval-bucket",
		},
	}
	mockSinkFactory := func(ctx context.Context, bucket, dir string, maxUploadBytes int64) (artifacts.ArtifactSink, error) {
		if bucket != "" {
			return &mockFooterSink{bucket: strings.TrimPrefix(bucket, "gs://")}, nil
		}
		if dir != "" {
			return &artifacts.LocalDirSink{BaseDir: dir}, nil
		}
		return nil, nil
	}

	if err := server.ReloadConfigMap(t.Context(), cmBucket, "", "", mockSinkFactory); err != nil {
		t.Fatalf("ReloadConfigMap failed: %v", err)
	}

	if server.ConfigGeneration() != 2 {
		t.Errorf("expected config generation 2 after bucket reload, got %d", server.ConfigGeneration())
	}

	// 3. Make HTTP request again without restarting server - footer reflects new value
	w2 := httptest.NewRecorder()
	server.ServeHTTP(w2, req)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w2.Code)
	}
	body2 := w2.Body.String()
	if !strings.Contains(body2, "artifacts: gs://reloaded-eval-bucket") {
		t.Errorf("expected 'artifacts: gs://reloaded-eval-bucket' in footer without restart, got: %s", body2)
	}
	if strings.Contains(body2, "artifacts: none") {
		t.Errorf("expected footer to no longer contain 'artifacts: none', got: %s", body2)
	}

	// 4. Reload with local directory
	cmDir := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      sinkconfig.DefaultConfigMapName,
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactDir: "/var/log/artifacts",
		},
	}
	if err := server.ReloadConfigMap(t.Context(), cmDir, "", "", mockSinkFactory); err != nil {
		t.Fatalf("ReloadConfigMap failed: %v", err)
	}
	if server.ConfigGeneration() != 3 {
		t.Errorf("expected config generation 3 after dir reload, got %d", server.ConfigGeneration())
	}

	w3 := httptest.NewRecorder()
	server.ServeHTTP(w3, req)
	body3 := w3.Body.String()
	if !strings.Contains(body3, "artifacts: dir /var/log/artifacts") {
		t.Errorf("expected 'artifacts: dir /var/log/artifacts' in footer without restart, got: %s", body3)
	}

	// 5. WatchConfigMap integration with fake clientset
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cmWatch := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "eval-runner",
			Name:            sinkconfig.DefaultConfigMapName,
			ResourceVersion: "100",
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactBucket: "gs://watch-eval-bucket",
		},
	}
	clientset := k8sfake.NewSimpleClientset(cmWatch)

	watchReady := make(chan struct{})
	var watchOnce sync.Once
	clientset.PrependWatchReactor("configmaps", func(action k8stesting.Action) (bool, watch.Interface, error) {
		w, err := clientset.Tracker().Watch(action.GetResource(), action.GetNamespace())
		watchOnce.Do(func() { close(watchReady) })
		return true, w, err
	})

	serverWatch := NewServer(Config{Client: fakeClient, DefaultNamespace: "eval-runner"})

	watchDone := make(chan struct{}, 10)
	go func() {
		_ = sinkconfig.WatchConfigMap(ctx, clientset, "eval-runner", sinkconfig.DefaultConfigMapName, func(cm *corev1.ConfigMap) {
			_ = serverWatch.ReloadConfigMap(ctx, cm, "", "", mockSinkFactory)
			watchDone <- struct{}{}
		})
	}()

	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial watch callback")
	}

	wWatch1 := httptest.NewRecorder()
	serverWatch.ServeHTTP(wWatch1, req)
	if !strings.Contains(wWatch1.Body.String(), "artifacts: gs://watch-eval-bucket") {
		t.Errorf("expected footer to reflect gs://watch-eval-bucket, got: %s", wWatch1.Body.String())
	}

	select {
	case <-watchReady:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch registration")
	}

	// Update ConfigMap via fake clientset
	cmWatchUpdated := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "eval-runner",
			Name:            sinkconfig.DefaultConfigMapName,
			ResourceVersion: "101",
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactBucket: "gs://hot-reloaded-bucket",
		},
	}
	_, err := clientset.CoreV1().ConfigMaps("eval-runner").Update(ctx, cmWatchUpdated, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("failed to update configmap: %v", err)
	}

	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update watch callback")
	}

	wWatch2 := httptest.NewRecorder()
	serverWatch.ServeHTTP(wWatch2, req)
	if !strings.Contains(wWatch2.Body.String(), "artifacts: gs://hot-reloaded-bucket") {
		t.Errorf("expected footer to reflect gs://hot-reloaded-bucket without restart, got: %s", wWatch2.Body.String())
	}
}

func TestServer_InitialBucketDir_DoesNotDuplicateOnStartup(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()

	var factoryCalls int
	mockSinkFactory := func(ctx context.Context, bucket, dir string, maxUploadBytes int64) (artifacts.ArtifactSink, error) {
		factoryCalls++
		return &mockFooterSink{bucket: bucket}, nil
	}

	initialSink, _ := mockSinkFactory(t.Context(), "initial-bucket", "", 0)
	server := NewServer(Config{
		Client:           fakeClient,
		DefaultNamespace: "eval-runner",
		ArtifactSink:     initialSink,
		InitialBucket:    "initial-bucket",
	})

	if server.ConfigGeneration() != 1 {
		t.Fatalf("expected initial config generation 1, got %d", server.ConfigGeneration())
	}

	cmInitial := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      sinkconfig.DefaultConfigMapName,
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactBucket: "initial-bucket",
		},
	}

	// First watch callback with matching ConfigMap should be a no-op
	if err := server.ReloadConfigMap(t.Context(), cmInitial, "", "", mockSinkFactory); err != nil {
		t.Fatalf("ReloadConfigMap failed: %v", err)
	}

	if server.ConfigGeneration() != 1 {
		t.Errorf("expected config generation to remain 1 on matching initial watch event, got %d", server.ConfigGeneration())
	}
	if factoryCalls != 1 {
		t.Errorf("expected factory calls to remain 1 (no duplicate creation), got %d", factoryCalls)
	}
}

func TestServer_ReloadConfigMap_FactoryErrorDoesNotAdvanceState(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()

	server := NewServer(Config{Client: fakeClient, DefaultNamespace: "eval-runner"})

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      sinkconfig.DefaultConfigMapName,
		},
		Data: map[string]string{
			configkeys.ConfigMapKeyArtifactBucket: "transient-bucket",
		},
	}

	failNext := true
	mockSinkFactory := func(ctx context.Context, bucket, dir string, maxUploadBytes int64) (artifacts.ArtifactSink, error) {
		if failNext {
			return nil, errors.New("simulated transient network failure")
		}
		return &mockFooterSink{bucket: bucket}, nil
	}

	// First attempt fails
	err := server.ReloadConfigMap(t.Context(), cm, "", "", mockSinkFactory)
	if err == nil {
		t.Fatal("expected error on first attempt, got nil")
	}

	// State should not have advanced, so re-running with the same ConfigMap retries construction
	failNext = false
	err = server.ReloadConfigMap(t.Context(), cm, "", "", mockSinkFactory)
	if err != nil {
		t.Fatalf("expected retry to succeed, got %v", err)
	}
	if server.ConfigGeneration() != 2 {
		t.Errorf("expected config generation 2 after successful retry, got %d", server.ConfigGeneration())
	}
}

func TestServerRunsList_RunningRunTasksRunningString(t *testing.T) {
	scheme := newTestScheme(t)
	runningRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-running-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:    "task-1",
					Phase: v1alpha1.TaskPhaseRunning,
				},
				{
					ID:    "task-2",
					Phase: v1alpha1.TaskPhasePending,
				},
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runningRun).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/runs", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "1/2 tasks running") {
		t.Errorf("expected '1/2 tasks running' in Runs list, got:\n%s", body)
	}
}

func TestServerRunDetail_LiveProgressAndMetaRefresh(t *testing.T) {
	scheme := newTestScheme(t)
	runningRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-running-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
			Limit: func() *int { i := 50; return &i }(),
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:             "docs/readme_comprehension",
					Phase:          v1alpha1.TaskPhaseRunning,
					SamplesDone:    12,
					SamplesPlanned: 50,
				},
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runningRun).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-running-run", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `<meta http-equiv="refresh" content="15">`) {
		t.Errorf("expected meta refresh tag in running run detail page, got:\n%s", body)
	}
	if !strings.Contains(body, "12 of 50 samples") {
		t.Errorf("expected '12 of 50 samples' progress bar in run detail page, got:\n%s", body)
	}
	if !strings.Contains(body, "progress-bar") {
		t.Errorf("expected progress-bar in run detail page, got:\n%s", body)
	}
}

func TestServerRunDetail_LiveProgressUnknownPlanned(t *testing.T) {
	scheme := newTestScheme(t)
	runningRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-running-unknown-planned",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          "task-1",
					Phase:       v1alpha1.TaskPhaseRunning,
					SamplesDone: 3,
				},
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runningRun).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-running-unknown-planned", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "3 samples") {
		t.Errorf("expected '3 samples' progress text, got:\n%s", body)
	}
	if strings.Contains(body, "3 of 0 samples") {
		t.Errorf("expected NOT to contain '3 of 0 samples', got:\n%s", body)
	}
}

func TestServerRunDetail_LiveProgressClampsToPossibleSamples(t *testing.T) {
	scheme := newTestScheme(t)
	limit := 50
	runningRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-running-clamp",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "mockllm/model",
			Limit: &limit,
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:              "task-1",
					Phase:           v1alpha1.TaskPhaseRunning,
					SamplesDone:     10,
					PossibleSamples: 21,
				},
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runningRun).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-running-clamp", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "10 of 21 samples") {
		t.Errorf("expected '10 of 21 samples' progress text clamped to dataset size, got:\n%s", body)
	}
}

func TestServerComparisonsList(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/comparisons", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "docs-eval-same-commit") {
		t.Errorf("expected comparison name in list")
	}
	if !strings.Contains(body, "IMPROVED") {
		t.Errorf("expected verdict IMPROVED in list")
	}
	if !strings.Contains(body, "0.98") {
		t.Errorf("expected P(imp) 0.98 in list")
	}
	if !strings.Contains(body, "/comparisons/eval-runner/docs-eval-same-commit") {
		t.Errorf("expected link to comparison detail")
	}

	// Trailing slash support
	reqSlash := httptest.NewRequest(http.MethodGet, "/comparisons/", nil)
	wSlash := httptest.NewRecorder()
	server.ServeHTTP(wSlash, reqSlash)
	if wSlash.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /comparisons/, got %d", wSlash.Code)
	}
}

func TestServerComparisonDetail(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/docs-eval-same-commit", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()

	// Check verdict chip and P(improvement)
	if !strings.Contains(body, "IMPROVED") {
		t.Errorf("expected verdict IMPROVED in detail")
	}
	if !strings.Contains(body, "P(improvement) = 0.98") {
		t.Errorf("expected P(improvement) in headline")
	}

	// Check PR-comment table headers
	expectedHeaders := []string{"eval", "base", "this PR", "Δ (95% CI)", "label", "samples"}
	for _, h := range expectedHeaders {
		if !strings.Contains(body, h) {
			t.Errorf("expected PR-comment header %q in table", h)
		}
	}

	// Check row content
	if !strings.Contains(body, "triage/runbook_lookup") {
		t.Errorf("expected task id in table")
	}
	if !strings.Contains(body, "0.61") {
		t.Errorf("expected base rate 0.61")
	}
	if !strings.Contains(body, "0.84") {
		t.Errorf("expected pr rate 0.84")
	}
	if !strings.Contains(body, "&#43;0.23") {
		t.Errorf("expected delta &#43;0.23")
	}
	if !strings.Contains(body, "[&#43;0.11, &#43;0.35]") {
		t.Errorf("expected CI label in cell")
	}
	if !strings.Contains(body, "improved") {
		t.Errorf("expected label 'improved'")
	}
	if !strings.Contains(body, "48") {
		t.Errorf("expected runs count '48'")
	}

	// Check CI bar with zero line
	if !strings.Contains(body, `class="zero" style="left:170px"`) {
		t.Errorf("expected CI bar zero line at 170px")
	}
	if !strings.Contains(body, "good-c") {
		t.Errorf("expected good-c class for improved CI bar")
	}

	// Check footer
	if !strings.Contains(body, "a81d0c4") {
		t.Errorf("expected base commit in footer")
	}
	if !strings.Contains(body, "05ce4d8") {
		t.Errorf("expected candidate commit in footer")
	}
	if !strings.Contains(body, "paired") {
		t.Errorf("expected paired samples note in footer")
	}

	// Test 404 for missing comparison
	reqNotFound := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/missing-comp", nil)
	wNotFound := httptest.NewRecorder()
	server.ServeHTTP(wNotFound, reqNotFound)
	if wNotFound.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing comparison, got %d", wNotFound.Code)
	}
	if strings.TrimSpace(wNotFound.Body.String()) != "comparison not found" {
		t.Errorf("expected 'comparison not found', got %q", wNotFound.Body.String())
	}
	if strings.Contains(wNotFound.Body.String(), "missing-comp") {
		t.Errorf("expected no reflected input in body, got %q", wNotFound.Body.String())
	}
}

func TestServerComparisonDetail_BatchesAndTokens(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()

	// Add batches and tokens to comp status
	comp.Name = "comp-batches-tokens"
	comp.Status.Batches = []v1alpha1.ComparisonBatchStatus{
		{
			Index:        1,
			Phase:        v1alpha1.ComparisonPhaseCompleted,
			SampleRange:  &v1alpha1.SampleRange{Start: 1, End: 8},
			CandidateRun: "cand-b1-run",
			BaselineRun:  "base-b1-run",
		},
		{
			Index:        2,
			Phase:        v1alpha1.ComparisonPhaseRunning,
			SampleRange:  &v1alpha1.SampleRange{Start: 9, End: 16},
			CandidateRun: "cand-b2-run",
			BaselineRun:  "base-b2-run",
		},
	}
	comp.Status.Tasks[0].Tokens = &v1alpha1.TaskTokens{
		Input:  600,
		Output: 250,
		Total:  850,
	}
	comp.Status.Tasks[0].Label = v1alpha1.ComparisonTaskLabelInconclusive
	comp.Status.Tasks[0].SuggestedNext = 16

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/comp-batches-tokens", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()

	// Check table headers have tokens and samples
	if !strings.Contains(body, "<th>tokens</th>") {
		t.Errorf("expected <th>tokens</th> header in table")
	}
	if !strings.Contains(body, "<th>samples</th>") {
		t.Errorf("expected <th>samples</th> header in table")
	}
	if !strings.Contains(body, "850") {
		t.Errorf("expected 850 tokens in table")
	}

	// Check Batches section rendered
	if !strings.Contains(body, "Batches</h2>") {
		t.Errorf("expected Batches section in detail view")
	}
	if !strings.Contains(body, "cand-b1-run") || !strings.Contains(body, "base-b1-run") {
		t.Errorf("expected batch run links in detail view")
	}
	if !strings.Contains(body, "<span><b>Batches:</b> 2</span>") {
		t.Errorf("expected Batches: 2 in footer")
	}

	// Check action hint
	if !strings.Contains(body, "<li>run 16 more samples") {
		t.Errorf("expected extraSamples hint in detail view")
	}
}

func TestServerPullsList(t *testing.T) {
	scheme := newTestScheme(t)
	now := time.Now().Truncate(time.Second)
	pImp95 := 0.95
	pImp05 := 0.05
	compTime := metav1.NewTime(now.Add(-8 * time.Minute))

	// comp1: gke-labs/eval-runner PR 10 (head1, older)
	comp1 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pr-10-head1",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-20 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     10,
				HeadSHA:    "1111111222222222222222222222222222222222",
				BaseSHA:    "0000000111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictInconclusive,
		},
	}

	// comp2: gke-labs/eval-runner PR 10 (head2, newer push)
	comp2 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pr-10-head2",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     10,
				HeadSHA:    "2222222222222222222222222222222222222222",
				BaseSHA:    "0000000111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict:        v1alpha1.ComparisonVerdictImproved,
			PImprovement:   &pImp95,
			CompletionTime: &compTime,
			Batches: []v1alpha1.ComparisonBatchStatus{
				{Index: 1}, {Index: 2},
			},
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{ID: "task-1", SamplesRun: 96, Pairs: 48},
			},
		},
	}

	// comp3: gke-labs/eval-runner PR 11 (newest in gke-labs/eval-runner)
	comp3 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pr-11",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     11,
				HeadSHA:    "3333333222222222222222222222222222222222",
				BaseSHA:    "0000000111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict:      v1alpha1.ComparisonVerdictRegressed,
			PImprovement: &pImp05,
		},
	}

	// comp4: other-org/other-repo PR 10 (newest overall)
	comp4 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pr-other-10",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "other-org/other-repo",
				Number:     10,
				HeadSHA:    "4444444222222222222222222222222222222222",
				BaseSHA:    "0000000111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictUnchanged,
		},
	}

	// compNonPR: comparison without spec.pullRequest
	compNonPR := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-non-pr",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now),
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand"},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp1, comp2, comp3, comp4, compNonPR).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/pulls", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	body := w.Body.String()

	// 1. Assert non-PR comparison is excluded
	if strings.Contains(body, "comp-non-pr") {
		t.Errorf("expected comp-non-pr to be excluded from /pulls")
	}

	// 2. Assert grouping: gke-labs/eval-runner PR 10 should have only one row (with latest head2)
	if strings.Contains(body, "pr-10-head1") {
		t.Errorf("did not expect older comp pr-10-head1 in /pulls list view")
	}
	if !strings.Contains(body, "pr-10-head2") {
		t.Errorf("expected latest comparison pr-10-head2 in /pulls list view")
	}

	// 3. Assert ordering: other-org/other-repo (1m ago) before PR 11 (5m ago) before PR 10 (10m ago)
	posOther := strings.Index(body, "pr-other-10")
	posPR11 := strings.Index(body, "pr-11")
	posPR10 := strings.Index(body, "pr-10-head2")
	if posOther == -1 || posPR11 == -1 || posPR10 == -1 {
		t.Fatalf("missing PR comparisons in body: other=%d pr11=%d pr10=%d", posOther, posPR11, posPR10)
	}
	if !(posOther < posPR11 && posPR11 < posPR10) {
		t.Errorf("expected newest head first ordering: posOther=%d < posPR11=%d < posPR10=%d", posOther, posPR11, posPR10)
	}

	// 4. Assert links: GitHub link, comparison link, history link
	if !strings.Contains(body, "https://github.com/gke-labs/eval-runner/pull/10") {
		t.Errorf("expected GitHub link for PR 10")
	}
	if !strings.Contains(body, "/comparisons/eval-runner/pr-10-head2") {
		t.Errorf("expected Comparison link for PR 10")
	}
	hashGke := RepoHash("gke-labs/eval-runner")
	if !strings.Contains(body, fmt.Sprintf("/pulls/%s/10", hashGke)) {
		t.Errorf("expected history link for PR 10: /pulls/%s/10", hashGke)
	}

	// 5. Assert columns: Head 2222222, Verdict IMPROVED, P=0.95, Samples 96, Batches 2
	if !strings.Contains(body, "2222222") {
		t.Errorf("expected short head 2222222")
	}
	if !strings.Contains(body, "IMPROVED") {
		t.Errorf("expected verdict IMPROVED")
	}
	if !strings.Contains(body, "0.95") {
		t.Errorf("expected P(imp) 0.95")
	}
	if !strings.Contains(body, ">96<") {
		t.Errorf("expected samples run 96")
	}
	if !strings.Contains(body, ">2<") {
		t.Errorf("expected batches 2")
	}

	// Trailing slash support
	reqSlash := httptest.NewRequest(http.MethodGet, "/pulls/", nil)
	wSlash := httptest.NewRecorder()
	server.ServeHTTP(wSlash, reqSlash)
	if wSlash.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /pulls/, got %d", wSlash.Code)
	}
}

func TestServerPullDetail(t *testing.T) {
	scheme := newTestScheme(t)
	now := time.Now().Truncate(time.Second)
	pImp98 := 0.98
	compTime := metav1.NewTime(now.Add(-4 * time.Minute))

	comp1 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pr-42-head1",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-20 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     42,
				HeadSHA:    "1111111222222222222222222222222222222222",
				BaseSHA:    "0000000111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictInconclusive,
			Batches: []v1alpha1.ComparisonBatchStatus{{Index: 1}},
			Tasks:   []v1alpha1.ComparisonTaskStatus{{ID: "task-1", SamplesRun: 48}},
		},
	}

	comp2 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pr-42-head2",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     42,
				HeadSHA:    "2222222222222222222222222222222222222222",
				BaseSHA:    "0000000111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict:        v1alpha1.ComparisonVerdictImproved,
			PImprovement:   &pImp98,
			CompletionTime: &compTime,
			Batches:        []v1alpha1.ComparisonBatchStatus{{Index: 1}, {Index: 2}},
			Tasks:          []v1alpha1.ComparisonTaskStatus{{ID: "task-1", SamplesRun: 96}},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp1, comp2).Build()
	server := NewServer(Config{Client: fakeClient})
	hash := RepoHash("gke-labs/eval-runner")

	t.Run("ByRepoHashAndNumberPath", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/pulls/%s/42", hash), nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()

		// Title and links
		if !strings.Contains(body, "gke-labs/eval-runner") || !strings.Contains(body, "#42") {
			t.Errorf("expected repo and PR number in title")
		}
		if !strings.Contains(body, "https://github.com/gke-labs/eval-runner/pull/42") {
			t.Errorf("expected GitHub link")
		}
		if !strings.Contains(body, "← Back to Pull Requests") {
			t.Errorf("expected back link")
		}

		// Both comparisons present
		if !strings.Contains(body, "pr-42-head2") || !strings.Contains(body, "pr-42-head1") {
			t.Errorf("expected both comparisons in history")
		}

		// Ordering: newer comp2 before older comp1
		pos2 := strings.Index(body, "pr-42-head2")
		pos1 := strings.Index(body, "pr-42-head1")
		if !(pos2 < pos1) {
			t.Errorf("expected comp2 (newer) before comp1 (older)")
		}

		// Verdicts
		if !strings.Contains(body, "IMPROVED") || !strings.Contains(body, "INCONCLUSIVE") {
			t.Errorf("expected both verdicts in history")
		}
	})

	t.Run("ByRepoAndNumberQuery", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/pulls?repo=gke-labs/eval-runner&number=42", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "pr-42-head2") || !strings.Contains(body, "pr-42-head1") {
			t.Errorf("expected both comparisons when queried via ?repo=...&number=...")
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/pulls/%s/999", hash), nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404 Not Found, got %d", w.Code)
		}
		if strings.Contains(w.Body.String(), "999") || strings.Contains(w.Body.String(), hash) {
			t.Errorf("expected no reflected parameters in 404 body: %q", w.Body.String())
		}
	})

	t.Run("InvalidRouteParams", func(t *testing.T) {
		for _, urlPath := range []string{
			fmt.Sprintf("/pulls/%s/-1", hash),
			fmt.Sprintf("/pulls/%s/bad", hash),
			"/pulls/bad%20hash/42",
		} {
			req := httptest.NewRequest(http.MethodGet, urlPath, nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("path %s: expected 400 Bad Request, got %d", urlPath, w.Code)
			}
			for _, leak := range []string{"bad", "bad%20hash", "-1"} {
				if strings.Contains(w.Body.String(), leak) {
					t.Errorf("path %s: leaked input %q in body: %s", urlPath, leak, w.Body.String())
				}
			}
		}
	})
}

func TestServerPullsPagination(t *testing.T) {
	scheme := newTestScheme(t)
	now := time.Now().Truncate(time.Second)

	comps := make([]client.Object, 5)
	for i := 0; i < 5; i++ {
		comps[i] = &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              fmt.Sprintf("pr-comp-%d", i+1),
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-time.Duration(5-i) * time.Minute)), // 5 is newest
			},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     i + 1,
					HeadSHA:    fmt.Sprintf("head%d1111111111111111111111111111111", i+1),
				},
			},
			Status: v1alpha1.ComparisonStatus{
				Verdict: v1alpha1.ComparisonVerdictImproved,
			},
		}
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comps...).Build()
	server := NewServer(Config{Client: fakeClient})

	t.Run("Page1", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/pulls?limit=2&offset=0", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()

		// PR 5 and 4 should be present
		if !strings.Contains(body, ">#5<") || !strings.Contains(body, ">#4<") {
			t.Errorf("expected #5 and #4 on page 1")
		}
		if strings.Contains(body, ">#3<") {
			t.Errorf("did not expect #3 on page 1")
		}
		if !strings.Contains(body, "More →") {
			t.Errorf("expected More link on page 1")
		}
		if !strings.Contains(body, "offset=2") {
			t.Errorf("expected offset=2 in Next link")
		}
		if strings.Contains(body, "← Previous") {
			t.Errorf("did not expect Previous link on page 1")
		}
		if !strings.Contains(body, "Showing 1–2 of 5") {
			t.Errorf("expected Showing 1–2 of 5")
		}
	})

	t.Run("Page2", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/pulls?limit=2&offset=2", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()

		if !strings.Contains(body, ">#3<") || !strings.Contains(body, ">#2<") {
			t.Errorf("expected #3 and #2 on page 2")
		}
		if strings.Contains(body, ">#5<") || strings.Contains(body, ">#1<") {
			t.Errorf("unexpected PR on page 2")
		}
		if !strings.Contains(body, "More →") || !strings.Contains(body, "← Previous") {
			t.Errorf("expected both More and Previous links on page 2")
		}
		if !strings.Contains(body, "Showing 3–4 of 5") {
			t.Errorf("expected Showing 3–4 of 5")
		}
	})

	t.Run("Page3", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/pulls?limit=2&offset=4", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()

		if !strings.Contains(body, ">#1<") {
			t.Errorf("expected #1 on page 3")
		}
		if strings.Contains(body, "More →") {
			t.Errorf("did not expect More link on last page")
		}
		if !strings.Contains(body, "← Previous") {
			t.Errorf("expected Previous link on page 3")
		}
		if !strings.Contains(body, "Showing 5–5 of 5") {
			t.Errorf("expected Showing 5–5 of 5")
		}
	})
}

func TestServerPullsNamespaceFilterAndEmptyState(t *testing.T) {
	scheme := newTestScheme(t)

	compDefault := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-default",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     1,
				HeadSHA:    "1111111222222222222222222222222222222222",
			},
		},
		Status: v1alpha1.ComparisonStatus{Verdict: v1alpha1.ComparisonVerdictImproved},
	}

	compCustom := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-custom",
			Namespace: "custom-ns",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "custom-org/custom-repo",
				Number:     2,
				HeadSHA:    "2222222222222222222222222222222222222222",
			},
		},
		Status: v1alpha1.ComparisonStatus{Verdict: v1alpha1.ComparisonVerdictImproved},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(compDefault, compCustom).Build()
	server := NewServer(Config{Client: fakeClient})

	t.Run("DefaultNamespace", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/pulls", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "gke-labs/eval-runner") {
			t.Errorf("expected default ns PR")
		}
		if strings.Contains(body, "custom-org/custom-repo") {
			t.Errorf("did not expect custom ns PR in default ns view")
		}
	})

	t.Run("CustomNamespace", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/pulls?namespace=custom-ns", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "custom-org/custom-repo") {
			t.Errorf("expected custom ns PR")
		}
		if strings.Contains(body, "gke-labs/eval-runner") {
			t.Errorf("did not expect default ns PR in custom ns view")
		}
	})

	t.Run("EmptyNamespace", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/pulls?namespace=empty-ns", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "No pull requests found in namespace empty-ns.") {
			t.Errorf("expected empty state message for empty-ns")
		}
	})

	t.Run("InvalidNamespace", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/pulls?namespace=INVALID_NS", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got %d", w.Code)
		}
		if strings.Contains(w.Body.String(), "INVALID_NS") {
			t.Errorf("expected no reflected namespace in 400 body")
		}
	})
}

func TestServerPullsEscaping(t *testing.T) {
	scheme := newTestScheme(t)
	const hostile = `<script>alert('pwned')</script>`

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-hostile",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "evil/repo" + hostile,
				Number:     13,
				HeadSHA:    "head" + hostile,
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictImproved,
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp).Build()
	server := NewServer(Config{Client: fakeClient})

	t.Run("PullsListEscaping", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/pulls", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if strings.Contains(body, "<script") {
			t.Errorf("unescaped <script found in /pulls HTML")
		}
		if !strings.Contains(body, "&lt;script&gt;") {
			t.Errorf("expected &lt;script&gt; in /pulls HTML")
		}
	})

	t.Run("PullDetailEscaping", func(t *testing.T) {
		hash := RepoHash("evil/repo" + hostile)
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/pulls/%s/13", hash), nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if strings.Contains(body, "<script") {
			t.Errorf("unescaped <script found in /pulls/{repohash}/{number} HTML")
		}
		if !strings.Contains(body, "&lt;script&gt;") {
			t.Errorf("expected &lt;script&gt; in /pulls/{repohash}/{number} HTML")
		}
	})
}

func TestServerWebhookE2EFixture(t *testing.T) {
	scheme := newTestScheme(t)

	// Fixture matching the webhook e2e fixture from pkg/webhook/testdata/pull_request.json
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-305",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "base-bbbbbbb",
			},
			Candidate: v1alpha1.ComparisonRunRef{
				Run: "pr-305-aaaaaaa",
			},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         8,
				MaxSamplesPerTask: 64,
			},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     305,
				HeadSHA:    "aaaaaaa111111122222223333333444444455555",
				BaseSHA:    "bbbbbbb111111122222223333333444444455555",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:   v1alpha1.ComparisonPhaseCompleted,
			Verdict: v1alpha1.ComparisonVerdictImproved,
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp).Build()
	server := NewServer(Config{Client: fakeClient})
	hash := RepoHash("gke-labs/eval-runner")

	// Both pages render for Comparisons created by the webhook e2e fixture
	t.Run("PullsListRendersWebhookFixture", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/pulls", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "gke-labs/eval-runner") {
			t.Errorf("expected repository in body")
		}
		if !strings.Contains(body, "#305") {
			t.Errorf("expected #305 in body")
		}
		if !strings.Contains(body, "aaaaaaa") {
			t.Errorf("expected short head aaaaaaa in body")
		}
		if !strings.Contains(body, "pr-305") {
			t.Errorf("expected pr-305 comparison in body")
		}
	})

	t.Run("PullDetailRendersWebhookFixture", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/pulls/%s/305", hash), nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "gke-labs/eval-runner") || !strings.Contains(body, "#305") {
			t.Errorf("expected repo and #305 in title")
		}
		if !strings.Contains(body, "pr-305") {
			t.Errorf("expected comparison name pr-305")
		}
		if !strings.Contains(body, "aaaaaaa") {
			t.Errorf("expected head SHA aaaaaaa")
		}
		if !strings.Contains(body, "bbbbbbb") {
			t.Errorf("expected base SHA bbbbbbb")
		}
	})
}

func TestServerRouteParamValidation(t *testing.T) {
	scheme := newTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	server := NewServer(Config{Client: fakeClient})

	invalidRoutes := []string{
		// Invalid namespace in path
		"/runs/UPPERCASE/run-1",
		"/runs/eval_runner/run-1",
		"/runs/-leading/run-1",
		"/runs/trailing-/run-1",
		"/runs/with..dots/run-1",
		// Invalid name in path
		"/runs/eval-runner/UPPERCASE",
		"/runs/eval-runner/-leading-hyphen",
		"/runs/eval-runner/trailing-hyphen-",
		"/runs/eval-runner/with..double.dot",
		"/runs/eval-runner/with_underscore",
		"/runs/eval-runner/with%20spaces",
		// Invalid query params on /runs
		"/runs?namespace=UPPERCASE",
		"/runs?namespace=bad_ns",
		"/runs?limit=-1",
		"/runs?limit=bad",
		"/runs?offset=-1",
		"/runs?offset=notanumber",
		// Invalid namespace in path on /comparisons
		"/comparisons/UPPERCASE/comp-1",
		"/comparisons/eval_runner/comp-1",
		// Invalid name in path on /comparisons
		"/comparisons/eval-runner/COMP_UPPERCASE",
		"/comparisons/eval-runner/-leading",
		"/comparisons/eval-runner/with_underscore",
		// Invalid query params on /comparisons
		"/comparisons?namespace=UPPERCASE",
		"/comparisons?namespace=bad_ns",
		"/comparisons?limit=-5",
		"/comparisons?limit=notanumber",
		"/comparisons?offset=-2",
		"/comparisons?offset=invalid",
		// Invalid query params on /pulls
		"/pulls?namespace=UPPERCASE",
		"/pulls?namespace=bad_ns",
		"/pulls?limit=-1",
		"/pulls?limit=bad",
		"/pulls?offset=-1",
		"/pulls?offset=notanumber",
		"/pulls?repo=gke-labs/eval-runner&number=-1",
		"/pulls?repo=gke-labs/eval-runner&number=notanumber",
		// Invalid route params on /pulls/{repohash}/{number}
		"/pulls/bad%20hash/1",
		"/pulls/2cc24c2/-1",
		"/pulls/2cc24c2/notanumber",
		"/pulls/2cc24c2/0",
	}

	for _, route := range invalidRoutes {
		req := httptest.NewRequest(http.MethodGet, route, nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("route %q: expected 400 Bad Request, got %d", route, w.Code)
		}
		for _, leak := range []string{"UPPERCASE", "bad_ns", "notanumber", "with_underscore", "eval_runner", "bad%20hash", "2cc24c2"} {
			if strings.Contains(w.Body.String(), leak) {
				t.Errorf("route %q: expected no reflected input %q in body, got %q", route, leak, w.Body.String())
			}
		}
	}
}

type forbiddenClient struct {
	client.Reader
}

func (f *forbiddenClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	return apierrors.NewForbidden(schema.GroupResource{Group: "evalrunner.gke-labs.dev", Resource: "runs"}, key.Name, fmt.Errorf("access denied"))
}

func (f *forbiddenClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return apierrors.NewForbidden(schema.GroupResource{Group: "evalrunner.gke-labs.dev", Resource: "runs"}, "", fmt.Errorf("access denied"))
}

func TestServerForbidden(t *testing.T) {
	server := NewServer(Config{Client: &forbiddenClient{}})

	forbiddenRequests := []string{
		"/runs?namespace=secret-namespace",
		"/runs/eval-runner/run-1",
		"/comparisons?namespace=secret-namespace",
		"/comparisons/eval-runner/comp-1",
		"/pulls?namespace=secret-namespace",
		"/pulls/2cc24c2/42?namespace=secret-namespace",
	}

	for _, route := range forbiddenRequests {
		req := httptest.NewRequest(http.MethodGet, route, nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusForbidden {
			t.Errorf("route %q: expected 403 Forbidden, got %d", route, w.Code)
		}
		for _, leak := range []string{"secret-namespace", "run-1", "comp-1", "access denied", "2cc24c2"} {
			if strings.Contains(w.Body.String(), leak) {
				t.Errorf("route %q: expected no leaked details %q in body, got %q", route, leak, w.Body.String())
			}
		}
	}
}

func TestServerPagination(t *testing.T) {
	scheme := newTestScheme(t)

	// Create 5 runs with distinct timestamps
	now := time.Now()
	runs := make([]client.Object, 5)
	for i := 0; i < 5; i++ {
		runs[i] = &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:              fmt.Sprintf("run-%d", i+1),
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-time.Duration(5-i) * time.Minute)), // run-5 is newest
			},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{Commit: fmt.Sprintf("commit%d", i+1)},
				Model: "model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
			},
		}
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runs...).Build()
	server := NewServer(Config{Client: fakeClient})

	t.Run("Page1", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs?limit=2&offset=0", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()

		// run-5 and run-4 should be present (newest first)
		if !strings.Contains(body, "run-5") || !strings.Contains(body, "run-4") {
			t.Errorf("expected run-5 and run-4 on page 1")
		}
		if strings.Contains(body, "run-3") {
			t.Errorf("did not expect run-3 on page 1")
		}

		// Pagination controls: More link should be present, Previous should not
		if !strings.Contains(body, "More →") {
			t.Errorf("expected More link on page 1")
		}
		if !strings.Contains(body, "offset=2") {
			t.Errorf("expected More link to contain offset=2")
		}
		if strings.Contains(body, "← Previous") {
			t.Errorf("did not expect Previous link on page 1")
		}
		if !strings.Contains(body, "Showing 1–2 of 5") {
			t.Errorf("expected range 'Showing 1–2 of 5'")
		}
	})

	t.Run("Page2", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs?limit=2&offset=2", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()

		// run-3 and run-2 should be present
		if !strings.Contains(body, "run-3") || !strings.Contains(body, "run-2") {
			t.Errorf("expected run-3 and run-2 on page 2")
		}
		if strings.Contains(body, "run-5") || strings.Contains(body, "run-1") {
			t.Errorf("unexpected run on page 2")
		}

		if !strings.Contains(body, "More →") {
			t.Errorf("expected More link on page 2")
		}
		if !strings.Contains(body, "offset=4") {
			t.Errorf("expected More link to contain offset=4")
		}
		if !strings.Contains(body, "← Previous") {
			t.Errorf("expected Previous link on page 2")
		}
		if !strings.Contains(body, "Showing 3–4 of 5") {
			t.Errorf("expected range 'Showing 3–4 of 5'")
		}
	})

	t.Run("Page3", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs?limit=2&offset=4", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()

		// run-1 should be present
		if !strings.Contains(body, "run-1") {
			t.Errorf("expected run-1 on page 3")
		}
		if strings.Contains(body, "run-2") {
			t.Errorf("did not expect run-2 on page 3")
		}

		// Last page: no More link
		if strings.Contains(body, "More →") {
			t.Errorf("did not expect More link on last page")
		}
		if !strings.Contains(body, "← Previous") {
			t.Errorf("expected Previous link on page 3")
		}
		if !strings.Contains(body, "offset=2") {
			t.Errorf("expected Previous link to contain offset=2")
		}
		if !strings.Contains(body, "Showing 5–5 of 5") {
			t.Errorf("expected range 'Showing 5–5 of 5'")
		}
	})

	t.Run("ComparisonsPagination", func(t *testing.T) {
		comps := make([]client.Object, 3)
		for i := 0; i < 3; i++ {
			comps[i] = &v1alpha1.Comparison{
				ObjectMeta: metav1.ObjectMeta{
					Name:              fmt.Sprintf("comp-%d", i+1),
					Namespace:         "eval-runner",
					CreationTimestamp: metav1.NewTime(now.Add(-time.Duration(3-i) * time.Minute)),
				},
				Spec: v1alpha1.ComparisonSpec{
					Baseline:  v1alpha1.ComparisonRunRef{Run: "base"},
					Candidate: v1alpha1.ComparisonRunRef{Run: "cand"},
				},
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictImproved,
				},
			}
		}

		compClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comps...).Build()
		compServer := NewServer(Config{Client: compClient})

		req := httptest.NewRequest(http.MethodGet, "/comparisons?limit=2&offset=0", nil)
		w := httptest.NewRecorder()
		compServer.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()

		if !strings.Contains(body, "comp-3") || !strings.Contains(body, "comp-2") {
			t.Errorf("expected comp-3 and comp-2 on page 1")
		}
		if strings.Contains(body, "comp-1") {
			t.Errorf("did not expect comp-1 on page 1")
		}
		if !strings.Contains(body, "More →") {
			t.Errorf("expected More link on comparisons page 1")
		}
	})
}

func TestServerSortTiebreaker(t *testing.T) {
	scheme := newTestScheme(t)
	now := time.Now().Truncate(time.Second)

	t.Run("RunsSortTiebreaker", func(t *testing.T) {
		names := []string{"run-charlie", "run-alpha", "run-bravo"}
		runs := make([]client.Object, len(names))
		for i, name := range names {
			runs[i] = &v1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{
					Name:              name,
					Namespace:         "eval-runner",
					CreationTimestamp: metav1.NewTime(now),
				},
				Spec: v1alpha1.RunSpec{
					Repo:  v1alpha1.RepoSpec{Commit: "a81d0c4"},
					Model: "model",
				},
				Status: v1alpha1.RunStatus{
					Phase: v1alpha1.RunPhaseCompleted,
				},
			}
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runs...).Build()
		server := NewServer(Config{Client: fakeClient})

		req := httptest.NewRequest(http.MethodGet, "/runs", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()

		idxAlpha := strings.Index(body, "run-alpha")
		idxBravo := strings.Index(body, "run-bravo")
		idxCharlie := strings.Index(body, "run-charlie")

		if idxAlpha == -1 || idxBravo == -1 || idxCharlie == -1 {
			t.Fatalf("expected all runs to appear in response body")
		}
		if !(idxAlpha < idxBravo && idxBravo < idxCharlie) {
			t.Errorf("expected runs to be sorted alphabetically on equal timestamp: alpha=%d bravo=%d charlie=%d", idxAlpha, idxBravo, idxCharlie)
		}
	})

	t.Run("ComparisonsSortTiebreaker", func(t *testing.T) {
		names := []string{"comp-charlie", "comp-alpha", "comp-bravo"}
		comps := make([]client.Object, len(names))
		for i, name := range names {
			comps[i] = &v1alpha1.Comparison{
				ObjectMeta: metav1.ObjectMeta{
					Name:              name,
					Namespace:         "eval-runner",
					CreationTimestamp: metav1.NewTime(now),
				},
				Spec: v1alpha1.ComparisonSpec{
					Baseline:  v1alpha1.ComparisonRunRef{Run: "base"},
					Candidate: v1alpha1.ComparisonRunRef{Run: "cand"},
				},
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictImproved,
				},
			}
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comps...).Build()
		server := NewServer(Config{Client: fakeClient})

		req := httptest.NewRequest(http.MethodGet, "/comparisons", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()

		idxAlpha := strings.Index(body, "comp-alpha")
		idxBravo := strings.Index(body, "comp-bravo")
		idxCharlie := strings.Index(body, "comp-charlie")

		if idxAlpha == -1 || idxBravo == -1 || idxCharlie == -1 {
			t.Fatalf("expected all comparisons to appear in response body")
		}
		if !(idxAlpha < idxBravo && idxBravo < idxCharlie) {
			t.Errorf("expected comparisons to be sorted alphabetically on equal timestamp: alpha=%d bravo=%d charlie=%d", idxAlpha, idxBravo, idxCharlie)
		}
	})
}

func TestServerRunsList_SampleRange(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "range-run",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.Now(),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/example/repo",
				Commit: "abcdef123456",
			},
			Model: "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{
				Start: 9,
				End:   16,
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Summary: &v1alpha1.RunSummary{
				Tasks:          "1/1",
				TasksCompleted: 1,
				TasksTotal:     1,
				TotalSamples:   8,
			},
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      "triage/task",
					Phase:   v1alpha1.TaskPhaseCompleted,
					Samples: 8,
					SampleRange: &v1alpha1.SampleRange{
						Start: 9,
						End:   16,
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		Build()

	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/runs", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}

	body := w.Body.String()
	if !strings.Contains(body, "samples 9-16") {
		t.Errorf("expected Runs view to contain 'samples 9-16', got body:\n%s", body)
	}
}

func TestServerRunDetail_SampleRange(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "range-run-detail",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.Now(),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/example/repo",
				Commit: "abcdef123456",
			},
			Model: "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{
				Start: 9,
				End:   16,
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Summary: &v1alpha1.RunSummary{
				Tasks:          "1/1",
				TasksCompleted: 1,
				TasksTotal:     1,
				TotalSamples:   8,
			},
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      "triage/task",
					Phase:   v1alpha1.TaskPhaseCompleted,
					Samples: 8,
					SampleRange: &v1alpha1.SampleRange{
						Start: 9,
						End:   16,
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		Build()

	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/range-run-detail", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}

	body := w.Body.String()
	if !strings.Contains(body, "samples 9-16") {
		t.Errorf("expected Run Detail view to contain 'samples 9-16', got body:\n%s", body)
	}
}

type errorSink struct {
	err error
}

func (s *errorSink) StoreArtifact(ctx context.Context, meta artifacts.RunMetadata, filename string, data []byte) (string, error) {
	return "", nil
}
func (s *errorSink) StoreComparison(ctx context.Context, comparisonName string, data []byte) (string, error) {
	return "", nil
}
func (s *errorSink) StoreManifest(ctx context.Context, meta artifacts.RunMetadata, data []byte) (string, error) {
	return "", nil
}
func (s *errorSink) Load(ctx context.Context, path string) ([]byte, error) {
	return nil, s.err
}
func (s *errorSink) ArtifactPrefix(commit string) string {
	return ""
}
func (s *errorSink) IsDurable() bool {
	return false
}
func (s *errorSink) Close() error {
	return nil
}

func TestServerTaskSamplesAndTranscript(t *testing.T) {
	scheme := newTestScheme(t)
	baseDir := t.TempDir()
	sink := &artifacts.LocalDirSink{BaseDir: baseDir}

	fixturePath := filepath.Join("..", "artifacts", "testdata", "trajectory_messages.json")
	fixtureData, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("failed to read trajectory_messages fixture: %v", err)
	}

	commit := "abcdef1234567890"
	taskID := "triage/runbook_lookup"
	meanScore := 0.5
	now := time.Now()
	meta := artifacts.RunMetadata{
		TaskID:     taskID,
		Commit:     commit,
		Model:      "gemini-2.5-pro",
		Successes:  1,
		MeanScore:  &meanScore,
		StartedAt:  &now,
		FinishedAt: &now,
	}

	logURI, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", fixtureData)
	if err != nil {
		t.Fatalf("failed to store artifact: %v", err)
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sample-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: commit,
			},
			Model: "gemini-2.5-pro",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: logURI,
				},
				{
					ID:    "task/no-log",
					Phase: v1alpha1.TaskPhasePending,
				},
				{
					ID:      "task/bad-prefix",
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: filepath.Join(baseDir, "runs", "othercommit12345", "inspect.json"),
				},
				{
					ID:      "task/file-not-found",
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: filepath.Join(baseDir, "runs", commit, "missing", "inspect.json"),
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		Build()

	server := NewServer(Config{
		Client:           fakeClient,
		DefaultNamespace: "eval-runner",
		ArtifactSink:     sink,
	})

	t.Run("SamplesList_Success", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/sample-run/tasks/triage%2Frunbook_lookup/samples", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}

		body := w.Body.String()
		for _, expected := range []string{
			"sample-017",
			"sample-019",
			"1.00",
			"0.00",
			"3.0s",
			"13.0s",
			"1,500",
			"41,200",
			"Cleared CDN cache",
			"Escalated to on-call",
			"<th>pass rule</th>",
			"<th>grader reason</th>",
			`<pre class="preview-text" title="Resolved via runbook">Resolved via runbook</pre>`,
			`<pre class="preview-text" title="failed runbook check">failed runbook check</pre>`,
			`title="pass: the reply names the cache purge and the HTTP 200 recheck"`,
		} {
			if !strings.Contains(body, expected) {
				t.Errorf("expected samples list to contain %q, got body:\n%s", expected, body)
			}
		}

		normalizedBody := strings.ReplaceAll(body, baseDir, "/tmp/eval-runner-sink")
		assertGolden(t, "task_samples.golden.html", normalizedBody)
	})

	t.Run("SampleTranscript_FailSample", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/sample-run/tasks/triage%2Frunbook_lookup/samples/sample-019", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}

		body := w.Body.String()
		for _, expected := range []string{
			"sample-019",
			"FAIL",
			"user",
			"Customer reports checkout returns HTTP 502",
			"tool_call",
			"classify_severity",
			"page_oncall",
			"tool",
			"high",
			"Paged on-call team payments",
			"assistant",
			"Escalated to on-call. Severity high. No further action.",
			"class=\"ev bad\"",
			"<span class=\"k\">pass rule</span>\n          <pre class=\"b\">Resolved via runbook</pre>",
			"<span class=\"k\">grader reason</span>\n          <pre class=\"b\">failed runbook check</pre>",
		} {
			if !strings.Contains(body, expected) {
				t.Errorf("expected sample transcript to contain %q, got body:\n%s", expected, body)
			}
		}

		normalizedBody := strings.ReplaceAll(body, baseDir, "/tmp/eval-runner-sink")
		assertGolden(t, "sample_transcript.golden.html", normalizedBody)
	})

	t.Run("SampleTranscript_PassSample", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/sample-run/tasks/triage%2Frunbook_lookup/samples/sample-017", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}

		body := w.Body.String()
		if !strings.Contains(body, "PASS") {
			t.Errorf("expected sample transcript to contain 'PASS', got body:\n%s", body)
		}
		if !strings.Contains(body, "check_cdn") {
			t.Errorf("expected sample transcript to contain 'check_cdn', got body:\n%s", body)
		}
	})

	t.Run("Negative_MissingLogPath_404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/sample-run/tasks/task%2Fno-log/samples", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected 404 for missing logPath, got %d", w.Code)
		}
	})

	t.Run("Negative_NoSuchTask_404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/sample-run/tasks/nonexistent%2Ftask/samples", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected 404 for nonexistent task, got %d", w.Code)
		}
		if strings.TrimSpace(w.Body.String()) != "log path not found for task" {
			t.Errorf("expected exact 'log path not found for task' message, got %q", w.Body.String())
		}
		if strings.Contains(w.Body.String(), "nonexistent") {
			t.Errorf("expected no reflected task ID in body, got %q", w.Body.String())
		}
	})

	t.Run("Negative_NoSuchRun_404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/no-such-run/tasks/triage%2Frunbook_lookup/samples", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected 404 for nonexistent run, got %d", w.Code)
		}
		if strings.TrimSpace(w.Body.String()) != "run not found" {
			t.Errorf("expected exact 'run not found' message, got %q", w.Body.String())
		}
		if strings.Contains(w.Body.String(), "no-such-run") || strings.Contains(w.Body.String(), "eval-runner") {
			t.Errorf("expected no reflected input in body, got %q", w.Body.String())
		}
	})

	t.Run("NilSink_RendersGuidance", func(t *testing.T) {
		serverNoSink := NewServer(Config{
			Client:           fakeClient,
			DefaultNamespace: "eval-runner",
			ArtifactSink:     nil,
		})
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/sample-run/tasks/triage%2Frunbook_lookup/samples", nil)
		w := httptest.NewRecorder()
		serverNoSink.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200 OK when ArtifactSink is nil, got %d", w.Code)
		}
		expectedMsg := "artifact storage is not configured for the UI; set artifactBucket or artifactDir in eval-runner-config"
		if !strings.Contains(w.Body.String(), expectedMsg) {
			t.Errorf("expected %q in body, got %q", expectedMsg, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "artifacts: none") {
			t.Errorf("expected 'artifacts: none' in body, got %q", w.Body.String())
		}
		normalizedBody := strings.ReplaceAll(w.Body.String(), baseDir, "/tmp/eval-runner-sink")
		assertGolden(t, "task_samples_nil_sink.golden.html", normalizedBody)
	})

	t.Run("Negative_PrefixMismatch_403", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/sample-run/tasks/task%2Fbad-prefix/samples", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusForbidden {
			t.Errorf("expected 403 Forbidden for prefix mismatch, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("Negative_StorageFileNotFound_404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/sample-run/tasks/task%2Ffile-not-found/samples", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected 404 for storage file not found, got %d: %s", w.Code, w.Body.String())
		}
		if strings.TrimSpace(w.Body.String()) != "task log not found on storage" {
			t.Errorf("expected exact 'task log not found on storage' message, got %q", w.Body.String())
		}
		if strings.Contains(w.Body.String(), "missing") || strings.Contains(w.Body.String(), "file-not-found") {
			t.Errorf("expected no leaked storage path in body, got %q", w.Body.String())
		}
	})

	t.Run("Negative_NoSuchSample_404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/sample-run/tasks/triage%2Frunbook_lookup/samples/no-such-sample", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected 404 for nonexistent sample, got %d", w.Code)
		}
		if strings.TrimSpace(w.Body.String()) != "sample not found" {
			t.Errorf("expected exact 'sample not found' message, got %q", w.Body.String())
		}
		if strings.Contains(w.Body.String(), "no-such-sample") {
			t.Errorf("expected no reflected input in body, got %q", w.Body.String())
		}
	})

	t.Run("Negative_InvalidRouteParams_400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/invalid_namespace/sample-run/tasks/triage%2Frunbook_lookup/samples", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for invalid namespace, got %d", w.Code)
		}
		if strings.TrimSpace(w.Body.String()) != "invalid route params" {
			t.Errorf("expected exact 'invalid route params' message, got %q", w.Body.String())
		}
		if strings.Contains(w.Body.String(), "invalid_namespace") {
			t.Errorf("expected no reflected input in body, got %q", w.Body.String())
		}
	})

	t.Run("Negative_StorageError_WrappedGoogleAPI_FixedBodyWithoutLeaks", func(t *testing.T) {
		secretLeak := "googleapi: got HTTP response code 403 with body: <?xml version=\"1.0\"?><Error><Code>AccessDenied</Code><BucketName>super-secret-eval-bucket</BucketName></Error>"
		wrappedErr := fmt.Errorf("failed to read artifact from gs://super-secret-eval-bucket/runs/%s/triage/runbook_lookup/inspect.json: %w", commit, errors.New(secretLeak))

		serverWithErrSink := NewServer(Config{
			Client:           fakeClient,
			DefaultNamespace: "eval-runner",
			ArtifactSink:     &errorSink{err: wrappedErr},
		})

		paths := []string{
			"/runs/eval-runner/sample-run/tasks/triage%2Frunbook_lookup/samples",
			"/runs/eval-runner/sample-run/tasks/triage%2Frunbook_lookup/samples/sample-017",
		}

		for _, path := range paths {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			serverWithErrSink.ServeHTTP(w, req)

			if w.Code != http.StatusBadGateway {
				t.Fatalf("%s: expected 502 Bad Gateway, got %d: %s", path, w.Code, w.Body.String())
			}
			expectedBody := "artifact storage unavailable; see UI logs"
			if strings.TrimSpace(w.Body.String()) != expectedBody {
				t.Errorf("%s: expected fixed body %q, got %q", path, expectedBody, w.Body.String())
			}
			for _, leak := range []string{"googleapi", "AccessDenied", "super-secret-eval-bucket", "<?xml", "gs://"} {
				if strings.Contains(w.Body.String(), leak) {
					t.Errorf("%s: body leaks raw error string %q: %s", path, leak, w.Body.String())
				}
			}
		}
	})

	t.Run("Negative_CappedOrTruncatedLog_FriendlyPage", func(t *testing.T) {
		// Store truncated invalid JSON
		metaTrunc := artifacts.RunMetadata{
			TaskID:     "task/truncated",
			Commit:     commit,
			Model:      "gemini-2.5-pro",
			StartedAt:  &now,
			FinishedAt: &now,
		}
		truncURI, err := sink.StoreArtifact(t.Context(), metaTrunc, "inspect.json", []byte(`{"version": 2, "samples": [{"id": 1, "input": "incomp`))
		if err != nil {
			t.Fatalf("failed to store truncated artifact: %v", err)
		}

		runTrunc := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "run-truncated",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					Commit: commit,
				},
				Model: "gemini-2.5-pro",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task/truncated",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: truncURI,
					},
				},
			},
		}

		clientTrunc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runTrunc).Build()
		serverTrunc := NewServer(Config{Client: clientTrunc, ArtifactSink: sink})

		// List page should return 200 with friendly message, not 500
		reqList := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/run-truncated/tasks/task%2Ftruncated/samples", nil)
		wList := httptest.NewRecorder()
		serverTrunc.ServeHTTP(wList, reqList)

		if wList.Code != http.StatusOK {
			t.Errorf("expected 200 OK for truncated log list page, got %d", wList.Code)
		}
		if !strings.Contains(wList.Body.String(), "Log Truncated") {
			t.Errorf("expected 'Log Truncated' message, got body:\n%s", wList.Body.String())
		}

		// Transcript page should return 200 with friendly message, not 500
		reqTranscript := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/run-truncated/tasks/task%2Ftruncated/samples/1", nil)
		wTranscript := httptest.NewRecorder()
		serverTrunc.ServeHTTP(wTranscript, reqTranscript)

		if wTranscript.Code != http.StatusOK {
			t.Errorf("expected 200 OK for truncated log transcript page, got %d", wTranscript.Code)
		}
		if !strings.Contains(wTranscript.Body.String(), "Log Truncated") {
			t.Errorf("expected 'Log Truncated' message, got body:\n%s", wTranscript.Body.String())
		}
	})

	t.Run("Escaping_HostileContent", func(t *testing.T) {
		hostileJSON := `{
			"version": 2,
			"status": "success",
			"eval": {"task": "task/hostile"},
			"samples": [
				{
					"id": "sample-xss",
					"epoch": 1,
					"input": "<script>alert('input-xss')</script>",
					"output": {"completion": "<img src=x onerror=alert('output-xss')>"},
					"messages": [
						{
							"role": "user",
							"content": "<script>alert('msg-xss')</script>"
						},
						{
							"role": "assistant",
							"tool_calls": [
								{
									"id": "c1",
									"function": "malicious<script>",
									"arguments": {"arg": "<iframe src=bad></iframe>"}
								}
							]
						}
					]
				}
			]
		}`
		metaHostile := artifacts.RunMetadata{
			TaskID:     "task/hostile",
			Commit:     commit,
			Model:      "gemini-2.5-pro",
			StartedAt:  &now,
			FinishedAt: &now,
		}
		hostileURI, err := sink.StoreArtifact(t.Context(), metaHostile, "inspect.json", []byte(hostileJSON))
		if err != nil {
			t.Fatalf("failed to store hostile artifact: %v", err)
		}

		runHostile := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "run-hostile",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{Commit: commit},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task/hostile",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: hostileURI,
					},
				},
			},
		}

		clientHostile := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runHostile).Build()
		serverHostile := NewServer(Config{Client: clientHostile, ArtifactSink: sink})

		// List page
		reqList := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/run-hostile/tasks/task%2Fhostile/samples", nil)
		wList := httptest.NewRecorder()
		serverHostile.ServeHTTP(wList, reqList)

		bodyList := wList.Body.String()
		if strings.Contains(bodyList, "<script>alert") {
			t.Errorf("raw unescaped script tag found in list page HTML:\n%s", bodyList)
		}
		if !strings.Contains(bodyList, "&lt;script&gt;alert") {
			t.Errorf("expected escaped script tag in list page HTML")
		}

		// Transcript page
		reqTranscript := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/run-hostile/tasks/task%2Fhostile/samples/sample-xss", nil)
		wTranscript := httptest.NewRecorder()
		serverHostile.ServeHTTP(wTranscript, reqTranscript)

		bodyTranscript := wTranscript.Body.String()
		if strings.Contains(bodyTranscript, "<script>alert") {
			t.Errorf("raw unescaped script tag found in transcript page HTML:\n%s", bodyTranscript)
		}
		if !strings.Contains(bodyTranscript, "&lt;script&gt;alert") {
			t.Errorf("expected escaped script tag in transcript page HTML")
		}
		if strings.Contains(bodyTranscript, "<iframe src=bad>") {
			t.Errorf("raw unescaped iframe tag found in transcript page HTML:\n%s", bodyTranscript)
		}
	})
}

func TestServerTaskSamples_InMemorySink_Drilldown(t *testing.T) {
	scheme := newTestScheme(t)
	sink := artifacts.NewInMemorySink()

	commit := "05ce4d8222222222222222222222222222222222"
	taskID := "docs/readme_comprehension"
	sampleIDWithEpoch := "sample-017#2"
	normalSampleID := "sample-019"

	logJSON := `{
		"version": 2,
		"status": "success",
		"eval": {
			"task": "docs/readme_comprehension",
			"model": "gemini-2.5-pro",
			"scorers": [{"name": "accuracy"}]
		},
		"results": {
			"total_samples": 2,
			"completed_samples": 2,
			"scores": [{"name": "accuracy", "scorer": "accuracy", "scored_samples": 2, "metrics": {"accuracy": {"name": "accuracy", "value": 1.0}}}]
		},
		"samples": [
			{
				"id": "sample-017",
				"epoch": 2,
				"input": "How do I configure eval runner?",
				"target": "See README",
				"output": {"model": "gemini-2.5-pro", "completion": "Check README instructions."},
				"scores": {"accuracy": {"value": 1.0, "answer": "Check README instructions."}},
				"messages": [
					{"role": "user", "content": "How do I configure eval runner?"},
					{"role": "assistant", "content": "Check README instructions."}
				]
			},
			{
				"id": "sample-019",
				"epoch": 1,
				"input": "Where is the UI hosted?",
				"target": "Port 80",
				"output": {"model": "gemini-2.5-pro", "completion": "Port 80 on cluster."},
				"scores": {"accuracy": {"value": 1.0, "answer": "Port 80 on cluster."}},
				"messages": [
					{"role": "user", "content": "Where is the UI hosted?"},
					{"role": "assistant", "content": "Port 80 on cluster."}
				]
			}
		]
	}`

	now := time.Now()
	meta := artifacts.RunMetadata{
		TaskID:     taskID,
		Commit:     commit,
		Model:      "gemini-2.5-pro",
		StartedAt:  &now,
		FinishedAt: &now,
	}

	uri, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", []byte(logJSON))
	if err != nil {
		t.Fatalf("failed to store artifact: %v", err)
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				Commit: commit,
			},
			Model: "gemini-2.5-pro",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: uri,
				},
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()
	server := NewServer(Config{Client: client, ArtifactSink: sink})

	t.Run("MultiSegment_DecodedAndEncoded_IdenticalBodies", func(t *testing.T) {
		// Encoded %2F
		reqEncoded := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-run/tasks/docs%2Freadme_comprehension/samples", nil)
		wEncoded := httptest.NewRecorder()
		server.ServeHTTP(wEncoded, reqEncoded)
		if wEncoded.Code != http.StatusOK {
			t.Fatalf("expected 200 for encoded path, got %d: %s", wEncoded.Code, wEncoded.Body.String())
		}

		// Decoded multi-segment
		reqDecoded := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-run/tasks/docs/readme_comprehension/samples", nil)
		wDecoded := httptest.NewRecorder()
		server.ServeHTTP(wDecoded, reqDecoded)
		if wDecoded.Code != http.StatusOK {
			t.Fatalf("expected 200 for decoded path, got %d: %s", wDecoded.Code, wDecoded.Body.String())
		}

		if wEncoded.Body.String() != wDecoded.Body.String() {
			t.Errorf("expected identical bodies for encoded and decoded task samples list path")
		}

		// Sample transcript: encoded vs decoded
		reqTranscriptEncoded := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-run/tasks/docs%2Freadme_comprehension/samples/"+normalSampleID, nil)
		wTranscriptEncoded := httptest.NewRecorder()
		server.ServeHTTP(wTranscriptEncoded, reqTranscriptEncoded)
		if wTranscriptEncoded.Code != http.StatusOK {
			t.Fatalf("expected 200 for transcript encoded path, got %d", wTranscriptEncoded.Code)
		}

		reqTranscriptDecoded := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-run/tasks/docs/readme_comprehension/samples/"+normalSampleID, nil)
		wTranscriptDecoded := httptest.NewRecorder()
		server.ServeHTTP(wTranscriptDecoded, reqTranscriptDecoded)
		if wTranscriptDecoded.Code != http.StatusOK {
			t.Fatalf("expected 200 for transcript decoded path, got %d", wTranscriptDecoded.Code)
		}

		if wTranscriptEncoded.Body.String() != wTranscriptDecoded.Body.String() {
			t.Errorf("expected identical bodies for encoded and decoded transcript path")
		}

		// Escape hatch: ?task=&sample=
		reqEscapeHatch := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-run/tasks?task=docs/readme_comprehension&sample="+normalSampleID, nil)
		wEscapeHatch := httptest.NewRecorder()
		server.ServeHTTP(wEscapeHatch, reqEscapeHatch)
		if wEscapeHatch.Code != http.StatusOK {
			t.Fatalf("expected 200 for escape hatch query params, got %d", wEscapeHatch.Code)
		}
		if wEscapeHatch.Body.String() != wTranscriptEncoded.Body.String() {
			t.Errorf("expected identical bodies for query params escape hatch and path")
		}
	})

	t.Run("PoundSuffixedSampleID_RoundTrips", func(t *testing.T) {
		reqList := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-run/tasks/docs/readme_comprehension/samples", nil)
		wList := httptest.NewRecorder()
		server.ServeHTTP(wList, reqList)
		expectedHref := "/runs/eval-runner/test-run/tasks/docs%2Freadme_comprehension/samples/" + url.PathEscape(sampleIDWithEpoch)
		if !strings.Contains(wList.Body.String(), expectedHref) {
			t.Errorf("expected samples list to link to %q, got body:\n%s", expectedHref, wList.Body.String())
		}

		// GET with %23 in sample segment
		reqEscaped := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-run/tasks/docs/readme_comprehension/samples/sample-017%232", nil)
		wEscaped := httptest.NewRecorder()
		server.ServeHTTP(wEscaped, reqEscaped)
		if wEscaped.Code != http.StatusOK {
			t.Fatalf("expected 200 for escaped #-suffixed sample, got %d: %s", wEscaped.Code, wEscaped.Body.String())
		}
		if !strings.Contains(wEscaped.Body.String(), sampleIDWithEpoch) {
			t.Errorf("expected body to contain %q, got:\n%s", sampleIDWithEpoch, wEscaped.Body.String())
		}

		// GET with raw # in URL
		reqRaw := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-run/tasks/docs/readme_comprehension/samples/sample-017#2", nil)
		wRaw := httptest.NewRecorder()
		server.ServeHTTP(wRaw, reqRaw)
		if wRaw.Code != http.StatusOK {
			t.Fatalf("expected 200 for raw #-suffixed sample, got %d: %s", wRaw.Code, wRaw.Body.String())
		}
		if !strings.Contains(wRaw.Body.String(), sampleIDWithEpoch) {
			t.Errorf("expected body to contain %q, got:\n%s", sampleIDWithEpoch, wRaw.Body.String())
		}
	})

	t.Run("MissingSample_Still404WithOwnText", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-run/tasks/docs/readme_comprehension/samples/no-such-sample", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)
		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404 for missing sample, got %d", w.Code)
		}
		if strings.TrimSpace(w.Body.String()) != "sample not found" {
			t.Errorf("expected 'sample not found', got %q", w.Body.String())
		}
	})

	t.Run("NilSink_RendersGuidance", func(t *testing.T) {
		serverNoSink := NewServer(Config{Client: client, ArtifactSink: nil})
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/test-run/tasks/docs/readme_comprehension/samples", nil)
		w := httptest.NewRecorder()
		serverNoSink.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK when sink is nil, got %d: %s", w.Code, w.Body.String())
		}
		expectedMsg := "artifact storage is not configured for the UI; set artifactBucket or artifactDir in eval-runner-config"
		if !strings.Contains(w.Body.String(), expectedMsg) {
			t.Errorf("expected %q in body, got:\n%s", expectedMsg, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "artifacts: none") {
			t.Errorf("expected 'artifacts: none' in body, got:\n%s", w.Body.String())
		}
	})
}

func TestServer_TaskSamplesFastPathWithSummaryJSON(t *testing.T) {
	tempDir := t.TempDir()
	sink := &artifacts.LocalDirSink{BaseDir: tempDir}
	commit := "commitfast1234"
	taskID := "task/fast"

	scoreVal := 1.0
	sampleSummaries := []artifacts.TaskSampleSummary{
		{
			ID:     "sample-fast-1",
			Score:  &scoreVal,
			Status: "pass",
		},
	}

	meta := artifacts.RunMetadata{
		RunName:           "fast-run",
		RunNamespace:      "eval-runner",
		TaskID:            taskID,
		Commit:            commit,
		Model:             "gemini-2.5-pro",
		ControllerVersion: "v1.0.0",
		Scorer:            "choice",
		Samples:           sampleSummaries,
	}

	inspectLog := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "task/fast"},
		"samples": [
			{
				"id": "sample-fast-1",
				"scores": {"choice": {"value": 1.0}},
				"messages": [
					{"role": "user", "content": "Transcript prompt content"}
				]
			}
		]
	}`

	logPath, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", []byte(inspectLog))
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "fast-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Model: "gemini-2.5-pro",
			Repo:  v1alpha1.RepoSpec{Commit: commit},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: logPath,
				},
			},
		},
	}

	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()

	srv := NewServer(Config{Client: cl, ArtifactSink: sink})

	// 1. Request sample list: uses fast path from summary.json
	reqList := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/fast-run/tasks/task%2Ffast/samples", nil)
	wList := httptest.NewRecorder()
	srv.ServeHTTP(wList, reqList)
	if wList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for sample list, got %d: %s", wList.Code, wList.Body.String())
	}
	if !strings.Contains(wList.Body.String(), "sample-fast-1") {
		t.Errorf("expected sample-fast-1 in sample list HTML")
	}

	// 2. Request sample transcript: loads full log to get transcript content
	reqTranscript := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/fast-run/tasks/task%2Ffast/samples/sample-fast-1", nil)
	wTranscript := httptest.NewRecorder()
	srv.ServeHTTP(wTranscript, reqTranscript)
	if wTranscript.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for sample transcript, got %d: %s", wTranscript.Code, wTranscript.Body.String())
	}
	if !strings.Contains(wTranscript.Body.String(), "Transcript prompt content") {
		t.Errorf("expected transcript content in transcript page HTML")
	}
}

func TestLogLRUCache(t *testing.T) {
	t.Run("GetAndSet", func(t *testing.T) {
		cache := newLogLRUCache(1024, time.Hour)
		sample := artifacts.ParsedInspectSample{ID: "s1"}
		cache.set("key1", 50, []artifacts.ParsedInspectSample{sample}, nil)

		got, err, ok := cache.get("key1")
		if !ok || err != nil || len(got) != 1 || got[0].ID != "s1" {
			t.Fatalf("expected cache hit for key1, got ok=%v, err=%v, len=%d", ok, err, len(got))
		}

		_, _, ok = cache.get("nonexistent")
		if ok {
			t.Errorf("expected cache miss for nonexistent key")
		}
	})

	t.Run("LRUEvictionByBytes", func(t *testing.T) {
		cache := newLogLRUCache(100, time.Hour)
		cache.set("k1", 60, []artifacts.ParsedInspectSample{{ID: "s1"}}, nil)
		cache.set("k2", 50, []artifacts.ParsedInspectSample{{ID: "s2"}}, nil)

		// Total bytes would be 110 > 100, so k1 should be evicted
		_, _, ok1 := cache.get("k1")
		if ok1 {
			t.Errorf("expected k1 to be evicted by byte limit")
		}
		_, _, ok2 := cache.get("k2")
		if !ok2 {
			t.Errorf("expected k2 to be present in cache")
		}
	})

	t.Run("TTLExpiration", func(t *testing.T) {
		cache := newLogLRUCache(1024, 10*time.Millisecond)
		cache.set("k1", 10, []artifacts.ParsedInspectSample{{ID: "s1"}}, nil)

		time.Sleep(20 * time.Millisecond)

		_, _, ok := cache.get("k1")
		if ok {
			t.Errorf("expected k1 to be expired after TTL")
		}
	})
}

func TestTruncateMessageBytes_UTF8(t *testing.T) {
	// Japanese text: each rune is 3 bytes in UTF-8
	s := "こんにちは世界"
	// Slicing at 5 bytes falls in the middle of the second rune.
	// TruncateMessageBytes should walk back to the rune boundary at 3 bytes ('こ').
	truncated := TruncateMessageBytes(s, 5)
	expectedPrefix := "こ\n... [content truncated]"
	if truncated != expectedPrefix {
		t.Errorf("expected %q, got %q", expectedPrefix, truncated)
	}

	// Short string under limit should remain unchanged
	if notTruncated := TruncateMessageBytes("hello", 10); notTruncated != "hello" {
		t.Errorf("expected 'hello', got %q", notTruncated)
	}
}

func TestTruncateFieldBytes(t *testing.T) {
	t.Run("UnderOrEqualLimit", func(t *testing.T) {
		if got := TruncateFieldBytes("hello", 10); got != "hello" {
			t.Errorf("expected 'hello', got %q", got)
		}
		if got := TruncateFieldBytes("hello", 5); got != "hello" {
			t.Errorf("expected 'hello', got %q", got)
		}
		if got := TruncateFieldBytes("", 5); got != "" {
			t.Errorf("expected empty string, got %q", got)
		}
	})

	t.Run("OverLimit_ASCII", func(t *testing.T) {
		s := "hello world"
		got := TruncateFieldBytes(s, 5)
		want := "hello\n... [truncated, 11 bytes total]"
		if got != want {
			t.Errorf("expected %q, got %q", want, got)
		}
	})

	t.Run("OverLimit_UTF8RuneBoundary", func(t *testing.T) {
		// "こんにちは世界": each rune is 3 bytes (21 bytes total).
		// Slicing at 5 bytes falls inside 'ん' (bytes 3..5).
		// It should walk back to 3 bytes ('こ').
		s := "こんにちは世界"
		got := TruncateFieldBytes(s, 5)
		want := "こ\n... [truncated, 21 bytes total]"
		if got != want {
			t.Errorf("expected %q, got %q", want, got)
		}
	})

	t.Run("OverLimit_HTMLEntities", func(t *testing.T) {
		// Named entity cut in half: "hello &amp; world" (17 bytes).
		// Slicing at 9 bytes gives "hello &am".
		// It should walk back before '&' (byte 6: "hello ").
		s := "hello &amp; world"
		got := TruncateFieldBytes(s, 9)
		want := "hello \n... [truncated, 17 bytes total]"
		if got != want {
			t.Errorf("expected %q, got %q", want, got)
		}

		// Numeric entity cut in half: "hello &#1234; world" (19 bytes).
		// Slicing at 10 bytes gives "hello &#12".
		gotNum := TruncateFieldBytes("hello &#1234; world", 10)
		wantNum := "hello \n... [truncated, 19 bytes total]"
		if gotNum != wantNum {
			t.Errorf("expected %q, got %q", wantNum, gotNum)
		}

		// Hex entity cut in half: "hello &#x1f600; world" (21 bytes).
		// Slicing at 11 bytes gives "hello &#x1f".
		gotHex := TruncateFieldBytes("hello &#x1f600; world", 11)
		wantHex := "hello \n... [truncated, 21 bytes total]"
		if gotHex != wantHex {
			t.Errorf("expected %q, got %q", wantHex, gotHex)
		}

		// Completed entity: slicing at 11 bytes gives "hello &amp;".
		// The entity is complete, so it must not be walked back.
		gotComplete := TruncateFieldBytes(s, 11)
		wantComplete := "hello &amp;\n... [truncated, 17 bytes total]"
		if gotComplete != wantComplete {
			t.Errorf("expected %q, got %q", wantComplete, gotComplete)
		}

		// Dangling ampersand at the cut: "hello &"
		gotDangling := TruncateFieldBytes("hello & world", 7)
		wantDangling := "hello \n... [truncated, 13 bytes total]"
		if gotDangling != wantDangling {
			t.Errorf("expected %q, got %q", wantDangling, gotDangling)
		}

		// Ampersand in regular text not followed by an entity: "Tom & Jerry" (11 bytes).
		// Slicing at 7 bytes gives "Tom & J". Space follows '&', so it's not an entity.
		gotText := TruncateFieldBytes("Tom & Jerry", 7)
		wantText := "Tom & J\n... [truncated, 11 bytes total]"
		if gotText != wantText {
			t.Errorf("expected %q, got %q", wantText, gotText)
		}
	})

	t.Run("ZeroOrNegativeLimit", func(t *testing.T) {
		s := "hello"
		if got := TruncateFieldBytes(s, 0); got != "... [truncated, 5 bytes total]" {
			t.Errorf("expected '... [truncated, 5 bytes total]', got %q", got)
		}
		if got := TruncateFieldBytes(s, -5); got != "... [truncated, 5 bytes total]" {
			t.Errorf("expected '... [truncated, 5 bytes total]', got %q", got)
		}
	})
}

func TestServerSampleTranscript_Truncation(t *testing.T) {
	scheme := newTestScheme(t)
	sink := artifacts.NewInMemorySink()

	commit := "abcdef1234567890"
	taskID := "triage/runbook_lookup"
	meanScore := 1.0
	now := time.Now()
	meta := artifacts.RunMetadata{
		TaskID:     taskID,
		Commit:     commit,
		Model:      "gemini-2.5-pro",
		Successes:  2,
		MeanScore:  &meanScore,
		StartedAt:  &now,
		FinishedAt: &now,
	}

	// 100 KiB explanation (102,400 bytes)
	explanation100k := strings.Repeat("a", 100*1024)
	// 100 KiB target (102,400 bytes)
	target100k := strings.Repeat("b", 100*1024)

	rawJSON := fmt.Sprintf(`{
		"version": 2,
		"status": "success",
		"eval": {"task": "triage/runbook_lookup", "scorers": [{"name": "accuracy"}]},
		"samples": [
			{
				"id": "sample-oversized-explanation",
				"target": "Expected pass rule",
				"scores": {
					"accuracy": {
						"value": 1.0,
						"explanation": %q
					}
				}
			},
			{
				"id": "sample-oversized-target",
				"target": %q,
				"scores": {
					"accuracy": {
						"value": 1.0,
						"explanation": "Short explanation"
					}
				}
			}
		]
	}`, explanation100k, target100k)

	logURI, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", []byte(rawJSON))
	if err != nil {
		t.Fatalf("failed to store artifact in InMemorySink: %v", err)
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "oversized-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: commit,
			},
			Model: "gemini-2.5-pro",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: logURI,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		Build()

	server := NewServer(Config{
		Client:           fakeClient,
		DefaultNamespace: "eval-runner",
		ArtifactSink:     sink,
	})

	t.Run("OversizedExplanation_100KiB_Truncated", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/oversized-run/tasks/triage%2Frunbook_lookup/samples/sample-oversized-explanation", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}

		body := w.Body.String()
		expectedNote := fmt.Sprintf("truncated, %d bytes total", len(explanation100k))
		if !strings.Contains(body, expectedNote) {
			t.Errorf("expected body to contain truncation note %q, but did not find it in:\n%s", expectedNote, body)
		}

		// Body must be well under 100 KiB (target/explanation capped at 16 KiB + HTML template skeleton)
		if len(body) >= 50*1024 {
			t.Errorf("expected body to be well under 100 KiB, got %d bytes", len(body))
		}
	})

	t.Run("OversizedTarget_100KiB_Truncated", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/oversized-run/tasks/triage%2Frunbook_lookup/samples/sample-oversized-target", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}

		body := w.Body.String()
		expectedNote := fmt.Sprintf("truncated, %d bytes total", len(target100k))
		if !strings.Contains(body, expectedNote) {
			t.Errorf("expected body to contain truncation note %q, but did not find it in:\n%s", expectedNote, body)
		}

		if len(body) >= 50*1024 {
			t.Errorf("expected body to be well under 100 KiB, got %d bytes", len(body))
		}
	})
}

func TestServerTaskSamplesList_Truncation(t *testing.T) {
	scheme := newTestScheme(t)
	sink := artifacts.NewInMemorySink()

	commit := "abcdef1234567890"
	taskID := "triage/runbook_lookup"
	meanScore := 1.0
	now := time.Now()
	meta := artifacts.RunMetadata{
		TaskID:     taskID,
		Commit:     commit,
		Model:      "gemini-2.5-pro",
		Successes:  2,
		MeanScore:  &meanScore,
		StartedAt:  &now,
		FinishedAt: &now,
	}

	// 100 KiB explanation (102,400 bytes)
	explanation100k := strings.Repeat("a", 100*1024)
	// 100 KiB target (102,400 bytes)
	target100k := strings.Repeat("b", 100*1024)

	rawJSON := fmt.Sprintf(`{
		"version": 2,
		"status": "success",
		"eval": {"task": "triage/runbook_lookup", "scorers": [{"name": "accuracy"}]},
		"samples": [
			{
				"id": "sample-oversized-explanation",
				"target": "Expected pass rule",
				"scores": {
					"accuracy": {
						"value": 1.0,
						"explanation": %q
					}
				}
			},
			{
				"id": "sample-oversized-target",
				"target": %q,
				"scores": {
					"accuracy": {
						"value": 1.0,
						"explanation": "Short explanation"
					}
				}
			}
		]
	}`, explanation100k, target100k)

	logURI, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", []byte(rawJSON))
	if err != nil {
		t.Fatalf("failed to store artifact in InMemorySink: %v", err)
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "oversized-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: commit,
			},
			Model: "gemini-2.5-pro",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: logURI,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		Build()

	server := NewServer(Config{
		Client:           fakeClient,
		DefaultNamespace: "eval-runner",
		ArtifactSink:     sink,
	})

	req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/oversized-run/tasks/triage%2Frunbook_lookup/samples", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	body := w.Body.String()

	// Truncation notes should be present for both oversized explanation and target tooltips
	expectedExpNote := fmt.Sprintf("truncated, %d bytes total", len(explanation100k))
	if !strings.Contains(body, expectedExpNote) {
		t.Errorf("expected body to contain explanation truncation note %q", expectedExpNote)
	}

	expectedTargetNote := fmt.Sprintf("truncated, %d bytes total", len(target100k))
	if !strings.Contains(body, expectedTargetNote) {
		t.Errorf("expected body to contain target truncation note %q", expectedTargetNote)
	}

	// Body must be well under 100 KiB (each tooltip capped at 512 bytes + HTML template skeleton)
	if len(body) >= 50*1024 {
		t.Errorf("expected body to be well under 100 KiB, got %d bytes", len(body))
	}

	// The sample rows must still link to the transcripts
	expectedExpLink := "/runs/eval-runner/oversized-run/tasks/triage%2Frunbook_lookup/samples/sample-oversized-explanation"
	if !strings.Contains(body, expectedExpLink) {
		t.Errorf("expected body to link to sample transcript %q", expectedExpLink)
	}

	expectedTargetLink := "/runs/eval-runner/oversized-run/tasks/triage%2Frunbook_lookup/samples/sample-oversized-target"
	if !strings.Contains(body, expectedTargetLink) {
		t.Errorf("expected body to link to sample transcript %q", expectedTargetLink)
	}
}

func TestComparisonCIHistory(t *testing.T) {
	scheme := newTestScheme(t)
	baseDir := t.TempDir()
	sink := &artifacts.LocalDirSink{BaseDir: baseDir}

	// 1. Prepare positive fixture: 3 batches, 12 pairs
	pairs := make([]stats.PairedSampleScore, 12)
	for i := 0; i < 12; i++ {
		cand := 0.85
		base := 0.55
		pairs[i] = stats.PairedSampleScore{
			ID:             fmt.Sprintf("sample-%d", i+1),
			BaseScore:      base,
			CandidateScore: cand,
			Delta:          cand - base,
		}
	}

	taskID := "task/test-eval"
	fixtureArtifact := artifacts.ComparisonArtifact{
		Comparison: "comp-history-fixture",
		Namespace:  "eval-runner",
		Verdict:    "IMPROVED",
		Tasks: []artifacts.ComparisonTaskArtifact{
			{
				ID:         taskID,
				Pairs:      12,
				PairsTable: pairs,
			},
		},
		Batches: []v1alpha1.ComparisonBatchStatus{
			{Index: 1, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
			{Index: 2, SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8}},
			{Index: 3, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 12}},
		},
	}
	artBytes, err := json.Marshal(fixtureArtifact)
	if err != nil {
		t.Fatalf("failed to marshal fixture artifact: %v", err)
	}

	summaryPath, err := sink.StoreComparison(t.Context(), "comp-history-fixture", artBytes)
	if err != nil {
		t.Fatalf("failed to store comparison fixture: %v", err)
	}

	compFixture := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "comp-history-fixture",
			Namespace: "eval-runner",
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:       v1alpha1.ComparisonPhaseCompleted,
			Verdict:     v1alpha1.ComparisonVerdictImproved,
			SummaryPath: summaryPath,
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:         taskID,
					SamplesRun: 12,
					Decision:   v1alpha1.ComparisonTaskLabelImproved,
				},
			},
			Batches: []v1alpha1.ComparisonBatchStatus{
				{Index: 1, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
				{Index: 2, SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8}},
				{Index: 3, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 12}},
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(compFixture).Build()
	server := NewServer(Config{Client: client, ArtifactSink: sink})

	t.Run("Positive_3Batches_12Pairs", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/comp-history-fixture", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}

		body := w.Body.String()
		if !strings.Contains(body, "Confidence Interval History") {
			t.Errorf("expected 'Confidence Interval History' section in HTML, got:\n%s", body)
		}

		// Assert SVG numeric attributes
		circleRegex := regexp.MustCompile(`<circle\s+cx="([^"]+)"\s+cy="([^"]+)"\s+r="([^"]+)"`)
		circleMatches := circleRegex.FindAllStringSubmatch(body, -1)
		if len(circleMatches) != 3 {
			t.Fatalf("expected 3 circle points in SVG, got %d", len(circleMatches))
		}

		for idx, m := range circleMatches {
			cx, err1 := strconv.ParseFloat(m[1], 64)
			cy, err2 := strconv.ParseFloat(m[2], 64)
			r, err3 := strconv.ParseFloat(m[3], 64)
			if err1 != nil || err2 != nil || err3 != nil || math.IsNaN(cx) || math.IsNaN(cy) || math.IsNaN(r) {
				t.Errorf("point %d: invalid numeric SVG circle attribute cx=%q cy=%q r=%q", idx, m[1], m[2], m[3])
			}
		}

		lineRegex := regexp.MustCompile(`<line\s+x1="([^"]+)"\s+y1="([^"]+)"\s+x2="([^"]+)"\s+y2="([^"]+)"`)
		lineMatches := lineRegex.FindAllStringSubmatch(body, -1)
		if len(lineMatches) == 0 {
			t.Fatalf("expected SVG line elements, found none")
		}
		for idx, m := range lineMatches {
			x1, err1 := strconv.ParseFloat(m[1], 64)
			y1, err2 := strconv.ParseFloat(m[2], 64)
			x2, err3 := strconv.ParseFloat(m[3], 64)
			y2, err4 := strconv.ParseFloat(m[4], 64)
			if err1 != nil || err2 != nil || err3 != nil || err4 != nil || math.IsNaN(x1) || math.IsNaN(y1) || math.IsNaN(x2) || math.IsNaN(y2) {
				t.Errorf("line %d: invalid numeric SVG line attribute x1=%q y1=%q x2=%q y2=%q", idx, m[1], m[2], m[3], m[4])
			}
		}

		if !strings.Contains(body, "margin ±0.05") {
			t.Errorf("expected equivalence margin reference line label, got:\n%s", body)
		}
		if !strings.Contains(body, "n=12") {
			t.Errorf("expected final point label containing n=12, got:\n%s", body)
		}

		// Assert table rows and monotone n
		tableRowRegex := regexp.MustCompile(`<td class="num">(\d+)</td>\s*<td class="num">([^<]+)</td>\s*<td class="num">([^<]+)</td>\s*<td><span class="chip [^"]+"><span class="dot"></span>([^<]+)</span></td>`)
		rowMatches := tableRowRegex.FindAllStringSubmatch(body, -1)
		if len(rowMatches) != 3 {
			t.Fatalf("expected 3 history table rows, got %d", len(rowMatches))
		}

		expectedNs := []int{4, 8, 12}
		lastN := 0
		for i, rm := range rowMatches {
			nVal, err := strconv.Atoi(rm[1])
			if err != nil {
				t.Fatalf("row %d: invalid n integer %q", i, rm[1])
			}
			if nVal != expectedNs[i] {
				t.Errorf("row %d: expected n=%d, got %d", i, expectedNs[i], nVal)
			}
			if nVal <= lastN {
				t.Errorf("expected strictly monotone increasing n, got row %d n=%d <= lastN=%d", i, nVal, lastN)
			}
			lastN = nVal
			deltaStr := rm[2]
			ciStr := rm[3]
			decisionStr := rm[4]
			if deltaStr == "" || ciStr == "" || decisionStr == "" {
				t.Errorf("row %d: missing values delta=%q ci=%q decision=%q", i, deltaStr, ciStr, decisionStr)
			}
		}
	})

	t.Run("Negative_MissingArtifact", func(t *testing.T) {
		compMissing := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "comp-missing",
				Namespace: "eval-runner",
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:       v1alpha1.ComparisonPhaseCompleted,
				SummaryPath: filepath.Join(baseDir, "comparisons", "missing.json"),
			},
		}
		clMissing := fake.NewClientBuilder().WithScheme(scheme).WithObjects(compMissing).Build()
		srvMissing := NewServer(Config{Client: clMissing, ArtifactSink: sink})

		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/comp-missing", nil)
		w := httptest.NewRecorder()
		srvMissing.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for missing artifact, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "history not available") {
			t.Errorf("expected 'history not available' note, got:\n%s", body)
		}
		if strings.Contains(body, "history-table") {
			t.Errorf("expected no history table when artifact is missing")
		}
	})

	t.Run("Negative_OversizedArtifact", func(t *testing.T) {
		// Create artifact file that exceeds DefaultMaxLoadBytes (4 MiB)
		oversizedFile := filepath.Join(baseDir, "comparisons", "comp-oversized.json")
		f, err := os.Create(oversizedFile)
		if err != nil {
			t.Fatalf("failed to create oversized file: %v", err)
		}
		oversizedData := make([]byte, artifacts.DefaultMaxLoadBytes+1024)
		_, _ = f.Write(oversizedData)
		_ = f.Close()

		compOversized := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "comp-oversized",
				Namespace: "eval-runner",
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:       v1alpha1.ComparisonPhaseCompleted,
				SummaryPath: oversizedFile,
			},
		}
		clOversized := fake.NewClientBuilder().WithScheme(scheme).WithObjects(compOversized).Build()
		srvOversized := NewServer(Config{Client: clOversized, ArtifactSink: sink})

		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/comp-oversized", nil)
		w := httptest.NewRecorder()
		srvOversized.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for oversized artifact, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "history not available") {
			t.Errorf("expected 'history not available' note for oversized artifact, got:\n%s", body)
		}
		if strings.Contains(body, "history-table") {
			t.Errorf("expected no history table when artifact is oversized")
		}
	})

	t.Run("Negative_PairsLessThan2", func(t *testing.T) {
		singlePairArtifact := artifacts.ComparisonArtifact{
			Comparison: "comp-single-pair",
			Namespace:  "eval-runner",
			Verdict:    "INCONCLUSIVE",
			Tasks: []artifacts.ComparisonTaskArtifact{
				{
					ID:    taskID,
					Pairs: 1,
					PairsTable: []stats.PairedSampleScore{
						{ID: "sample-1", BaseScore: 0.5, CandidateScore: 0.8, Delta: 0.3},
					},
				},
			},
		}
		artBytes, err := json.Marshal(singlePairArtifact)
		if err != nil {
			t.Fatalf("failed to marshal single pair artifact: %v", err)
		}
		p, err := sink.StoreComparison(t.Context(), "comp-single-pair", artBytes)
		if err != nil {
			t.Fatalf("failed to store single pair comparison: %v", err)
		}

		compSingle := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "comp-single-pair",
				Namespace: "eval-runner",
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:       v1alpha1.ComparisonPhaseCompleted,
				Verdict:     v1alpha1.ComparisonVerdictInconclusive,
				SummaryPath: p,
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:         taskID,
						SamplesRun: 1,
						Decision:   v1alpha1.ComparisonTaskLabelInconclusive,
					},
				},
			},
		}
		clSingle := fake.NewClientBuilder().WithScheme(scheme).WithObjects(compSingle).Build()
		srvSingle := NewServer(Config{Client: clSingle, ArtifactSink: sink})

		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/comp-single-pair", nil)
		w := httptest.NewRecorder()
		srvSingle.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for pairs < 2, got %d", w.Code)
		}
		body := w.Body.String()
		if strings.Contains(body, "history-table") {
			t.Errorf("expected no history table for pairs < 2, got body:\n%s", body)
		}
	})

	t.Run("Negative_ArtifactStorageUnavailable", func(t *testing.T) {
		srvNilSink := NewServer(Config{Client: client, ArtifactSink: nil})

		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/comp-history-fixture", nil)
		w := httptest.NewRecorder()
		srvNilSink.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK when storage unavailable, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "artifact storage unavailable") {
			t.Errorf("expected 'artifact storage unavailable' note, got:\n%s", body)
		}
		if strings.Contains(body, "history-table") {
			t.Errorf("expected no history table when artifact storage is unavailable")
		}
	})

	t.Run("Negative_EmptySummaryPath_QuietInProgress", func(t *testing.T) {
		compInProgress := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "comp-in-progress",
				Namespace: "eval-runner",
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:       v1alpha1.ComparisonPhaseRunning,
				SummaryPath: "",
			},
		}
		clInProgress := fake.NewClientBuilder().WithScheme(scheme).WithObjects(compInProgress).Build()
		srvInProgress := NewServer(Config{Client: clInProgress, ArtifactSink: sink})

		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/comp-in-progress", nil)
		w := httptest.NewRecorder()
		srvInProgress.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for in-progress comparison, got %d", w.Code)
		}
		body := w.Body.String()
		if strings.Contains(body, "Confidence Interval History") {
			t.Errorf("expected no Confidence Interval History section for comparison without summaryPath, got:\n%s", body)
		}
	})

	t.Run("Negative_SummaryPathMissingComparisonsPrefix", func(t *testing.T) {
		compBadPrefix := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "comp-bad-prefix",
				Namespace: "eval-runner",
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:       v1alpha1.ComparisonPhaseCompleted,
				SummaryPath: "gs://eval-bucket/runs/abcdef12/manifest.json",
			},
		}
		clBadPrefix := fake.NewClientBuilder().WithScheme(scheme).WithObjects(compBadPrefix).Build()
		srvBadPrefix := NewServer(Config{Client: clBadPrefix, ArtifactSink: sink})

		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/comp-bad-prefix", nil)
		w := httptest.NewRecorder()
		srvBadPrefix.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for bad prefix summaryPath, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "history not available") {
			t.Errorf("expected 'history not available' note when summaryPath lacks comparisons/ prefix, got:\n%s", body)
		}
		if strings.Contains(body, "history-table") {
			t.Errorf("expected no history table when summaryPath lacks comparisons/ prefix")
		}
	})
}

func TestServerSeries_Selection(t *testing.T) {
	scheme := newTestScheme(t)
	r10 := 0.8
	ratePtr := &r10

	runValid := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-1111111",
			Namespace: "eval-runner",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/repo":     "gke-labs_eval-runner",
				"evalrunner.gke-labs.dev/branch":   "main",
				"evalrunner.gke-labs.dev/head-sha": "1111111222222233333334444444555555556666",
			},
			Annotations: map[string]string{
				"evalrunner.gke-labs.dev/commit-time": "2026-10-20T01:00:00Z",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "1111111222222233333334444444555555556666",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          "task-a",
					Phase:       v1alpha1.TaskPhaseCompleted,
					SuccessRate: ratePtr,
					Samples:     10,
				},
			},
		},
	}

	// Excluded: branch is dev, not main
	runOtherBranch := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-2222222",
			Namespace: "eval-runner",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/repo":     "gke-labs_eval-runner",
				"evalrunner.gke-labs.dev/branch":   "dev",
				"evalrunner.gke-labs.dev/head-sha": "2222222333333344444445555555666666667777",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "2222222333333344444445555555666666667777",
			},
		},
	}

	// Excluded: pre-existing run lacking branch label
	runPreExisting := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-3333333",
			Namespace: "eval-runner",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/repo":     "gke-labs_eval-runner",
				"evalrunner.gke-labs.dev/head-sha": "3333333444444455555556666666777777778888",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "3333333444444455555556666666777777778888",
			},
		},
	}

	// Excluded: not starting with base-
	runCandidate := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-42-4444444",
			Namespace: "eval-runner",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/repo":     "gke-labs_eval-runner",
				"evalrunner.gke-labs.dev/branch":   "main",
				"evalrunner.gke-labs.dev/head-sha": "4444444555555566666667777777888888889999",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "4444444555555566666667777777888888889999",
			},
		},
	}

	// Excluded: other repo
	runOtherRepo := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-5555555",
			Namespace: "eval-runner",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/repo":     "other-org_other-repo",
				"evalrunner.gke-labs.dev/branch":   "main",
				"evalrunner.gke-labs.dev/head-sha": "5555555666666677777778888888999999990000",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/other-org/other-repo.git",
				Commit: "5555555666666677777778888888999999990000",
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		runValid, runOtherBranch, runPreExisting, runCandidate, runOtherRepo,
	).Build()
	server := NewServer(Config{Client: cl})

	t.Run("SeriesList", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/series", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		body := w.Body.String()
		if !strings.Contains(body, "gke-labs/eval-runner") {
			t.Errorf("expected gke-labs/eval-runner in series list")
		}
		if !strings.Contains(body, "/series/2cc24c2/main") {
			t.Errorf("expected link to /series/2cc24c2/main in series list")
		}
	})

	t.Run("SeriesDetail_FiltersCorrectly", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/series/2cc24c2/main", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		body := w.Body.String()
		if !strings.Contains(body, "base-1111111") {
			t.Errorf("expected base-1111111 in series detail")
		}
		if strings.Contains(body, "base-2222222") {
			t.Errorf("did not expect base-2222222 (branch dev) in series detail")
		}
		if strings.Contains(body, "base-3333333") {
			t.Errorf("did not expect base-3333333 (pre-existing) in series detail")
		}
		if strings.Contains(body, "pr-42-4444444") {
			t.Errorf("did not expect candidate run in series detail")
		}
		if strings.Contains(body, "base-5555555") {
			t.Errorf("did not expect other repo in series detail")
		}
	})

	t.Run("SeriesDetail_NotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/series/2cc24c2/nonexistent", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected 404 for nonexistent series, got %d", w.Code)
		}
	})
}

func TestServerSeries_Ordering(t *testing.T) {
	scheme := newTestScheme(t)
	rate := 0.9

	t.Run("OrderedByCommitTimeAnnotation", func(t *testing.T) {
		// runA was created at 10:00 but commitTime is 02:00
		runA := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "base-aaaaaaa",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)),
				Labels: map[string]string{
					"evalrunner.gke-labs.dev/repo":   "gke-labs_eval-runner",
					"evalrunner.gke-labs.dev/branch": "main",
				},
				Annotations: map[string]string{
					"evalrunner.gke-labs.dev/commit-time": "2026-10-20T02:00:00Z",
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    "https://github.com/gke-labs/eval-runner.git",
					Commit: "aaaaaaa111111111111111111111111111111111",
				},
			},
			Status: v1alpha1.RunStatus{
				Tasks: []v1alpha1.TaskStatus{{ID: "task-1", Phase: v1alpha1.TaskPhaseCompleted, SuccessRate: &rate}},
			},
		}

		// runB was created at 11:00 but commitTime is 01:00 (earlier commit!)
		runB := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "base-bbbbbbb",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(time.Date(2026, 10, 20, 11, 0, 0, 0, time.UTC)),
				Labels: map[string]string{
					"evalrunner.gke-labs.dev/repo":   "gke-labs_eval-runner",
					"evalrunner.gke-labs.dev/branch": "main",
				},
				Annotations: map[string]string{
					"evalrunner.gke-labs.dev/commit-time": "2026-10-20T01:00:00Z",
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    "https://github.com/gke-labs/eval-runner.git",
					Commit: "bbbbbbb222222222222222222222222222222222",
				},
			},
			Status: v1alpha1.RunStatus{
				Tasks: []v1alpha1.TaskStatus{{ID: "task-1", Phase: v1alpha1.TaskPhaseCompleted, SuccessRate: &rate}},
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runA, runB).Build()
		server := NewServer(Config{Client: cl})

		req := httptest.NewRequest(http.MethodGet, "/series/2cc24c2/main", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		body := w.Body.String()
		if strings.Contains(body, "ordered by creation time") {
			t.Errorf("did not expect 'ordered by creation time' note when commit-time annotations are present")
		}

		idxA := strings.Index(body, "base-aaaaaaa")
		idxB := strings.Index(body, "base-bbbbbbb")
		if idxB == -1 || idxA == -1 {
			t.Fatalf("expected both runs in body")
		}
		if idxB > idxA {
			t.Errorf("expected runB (commitTime 01:00) before runA (commitTime 02:00), got idxB=%d idxA=%d", idxB, idxA)
		}
	})

	t.Run("FallbackToCreationTimeWithVisibleNote", func(t *testing.T) {
		runNoAnnotation := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "base-cccccc",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)),
				Labels: map[string]string{
					"evalrunner.gke-labs.dev/repo":   "gke-labs_eval-runner",
					"evalrunner.gke-labs.dev/branch": "main",
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    "https://github.com/gke-labs/eval-runner.git",
					Commit: "cccccc111111111111111111111111111111111",
				},
			},
			Status: v1alpha1.RunStatus{
				Tasks: []v1alpha1.TaskStatus{{ID: "task-1", Phase: v1alpha1.TaskPhaseCompleted, SuccessRate: &rate}},
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runNoAnnotation).Build()
		server := NewServer(Config{Client: cl})

		req := httptest.NewRequest(http.MethodGet, "/series/2cc24c2/main", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "ordered by creation time") {
			t.Errorf("expected visible note 'ordered by creation time', got body:\n%s", body)
		}
	})
}

func TestServerSeries_NCap(t *testing.T) {
	scheme := newTestScheme(t)
	rate := 0.85

	var runs []client.Object
	baseTime := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= 60; i++ {
		cTime := baseTime.Add(time.Duration(i) * time.Hour).Format(time.RFC3339)
		run := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("base-%07d", i),
				Namespace: "eval-runner",
				Labels: map[string]string{
					"evalrunner.gke-labs.dev/repo":   "gke-labs_eval-runner",
					"evalrunner.gke-labs.dev/branch": "main",
				},
				Annotations: map[string]string{
					"evalrunner.gke-labs.dev/commit-time": cTime,
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    "https://github.com/gke-labs/eval-runner.git",
					Commit: fmt.Sprintf("%040d", i),
				},
			},
			Status: v1alpha1.RunStatus{
				Tasks: []v1alpha1.TaskStatus{{ID: "eval-task", Phase: v1alpha1.TaskPhaseCompleted, SuccessRate: &rate}},
			},
		}
		runs = append(runs, run)
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runs...).Build()
	server := NewServer(Config{Client: cl})

	t.Run("DefaultCap50", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/series/2cc24c2/main", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		body := w.Body.String()
		if strings.Contains(body, "base-0000001") {
			t.Errorf("did not expect oldest commit base-0000001 when capped at 50")
		}
		if !strings.Contains(body, "base-0000060") {
			t.Errorf("expected newest commit base-0000060")
		}
		if !strings.Contains(body, "base-0000011") {
			t.Errorf("expected commit base-0000011")
		}
		if !strings.Contains(body, "showing last 50 of 60 commits") {
			t.Errorf("expected 'showing last 50 of 60 commits' in body")
		}
	})

	t.Run("NCapOver50_ClampedTo50", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/series/2cc24c2/main?n=100", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		body := w.Body.String()
		if strings.Contains(body, "base-0000001") {
			t.Errorf("expected n=100 to be capped at 50, but base-0000001 was included")
		}
	})

	t.Run("ExplicitSmallerN", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/series/2cc24c2/main?n=10", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		body := w.Body.String()
		if strings.Contains(body, "base-0000050") {
			t.Errorf("did not expect base-0000050 when n=10 (commits 51-60)")
		}
		if !strings.Contains(body, "base-0000051") {
			t.Errorf("expected base-0000051 when n=10")
		}
		if !strings.Contains(body, "base-0000060") {
			t.Errorf("expected base-0000060 when n=10")
		}
	})
}

func TestServerSeries_CohortFilter(t *testing.T) {
	scheme := newTestScheme(t)
	rate := 0.7

	runA := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-cohort-a",
			Namespace: "eval-runner",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/repo":   "gke-labs_eval-runner",
				"evalrunner.gke-labs.dev/branch": "main",
			},
		},
		Spec: v1alpha1.RunSpec{
			Cohort: "cohort-alpha",
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "aaaa111111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.RunStatus{
			Tasks: []v1alpha1.TaskStatus{{ID: "task-1", Phase: v1alpha1.TaskPhaseCompleted, SuccessRate: &rate}},
		},
	}

	runB := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-cohort-b",
			Namespace: "eval-runner",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/repo":   "gke-labs_eval-runner",
				"evalrunner.gke-labs.dev/branch": "main",
			},
		},
		Spec: v1alpha1.RunSpec{
			Cohort: "cohort-beta",
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "bbbb222222222222222222222222222222222222",
			},
		},
		Status: v1alpha1.RunStatus{
			Tasks: []v1alpha1.TaskStatus{{ID: "task-1", Phase: v1alpha1.TaskPhaseCompleted, SuccessRate: &rate}},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runA, runB).Build()
	server := NewServer(Config{Client: cl})

	t.Run("ListFilteredByCohort", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/series?cohort=cohort-alpha", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, "1 total") {
			t.Errorf("expected 1 total row for cohort-alpha filter, got body:\n%s", body)
		}
	})

	t.Run("DetailFilteredByCohort", func(t *testing.T) {
		reqAlpha := httptest.NewRequest(http.MethodGet, "/series/2cc24c2/main?cohort=cohort-alpha", nil)
		wAlpha := httptest.NewRecorder()
		server.ServeHTTP(wAlpha, reqAlpha)

		if wAlpha.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", wAlpha.Code)
		}
		bodyAlpha := wAlpha.Body.String()
		if !strings.Contains(bodyAlpha, "base-cohort-a") {
			t.Errorf("expected base-cohort-a in filtered series")
		}
		if strings.Contains(bodyAlpha, "base-cohort-b") {
			t.Errorf("did not expect base-cohort-b in filtered series")
		}
	})
}

func TestServerSeries_HostileEscaping(t *testing.T) {
	scheme := newTestScheme(t)
	rate := 0.5

	hostilePayload := "<script>alert('xss')</script>"
	runHostile := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-hostile",
			Namespace: "eval-runner",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/repo":   "gke-labs_eval-runner",
				"evalrunner.gke-labs.dev/branch": "main",
			},
		},
		Spec: v1alpha1.RunSpec{
			Cohort: hostilePayload,
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "1111111222222233333334444444555555556666",
			},
		},
		Status: v1alpha1.RunStatus{
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          hostilePayload,
					Phase:       v1alpha1.TaskPhaseCompleted,
					SuccessRate: &rate,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runHostile).Build()
	server := NewServer(Config{Client: cl})

	t.Run("ListEscaping", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/series?cohort="+url.QueryEscape(hostilePayload), nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		body := w.Body.String()
		if strings.Contains(body, "<script>") {
			t.Errorf("found unescaped <script> in list response")
		}
	})

	t.Run("DetailEscaping", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/series/2cc24c2/main", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		body := w.Body.String()
		if strings.Contains(body, "<script>") {
			t.Errorf("found unescaped <script> in detail response")
		}
		if !strings.Contains(body, html.EscapeString(hostilePayload)) {
			t.Errorf("expected escaped hostile payload in detail response")
		}
	})
}

func TestServerRunDetail_MetricFilter(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()
	candRun.Status.Tasks[0].Latency = &v1alpha1.TaskLatency{P50: 6.01, P95: 9.78}
	candRun.Status.Tasks[0].Tokens = &v1alpha1.TaskTokens{
		Input:     2868,
		Output:    1531,
		Total:     4399,
		Reasoning: 133,
		CacheRead: 512,
	}
	cost := 0.001349
	candRun.Status.Tasks[0].CostUSD = &cost

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()
	server := NewServer(Config{Client: fakeClient})

	tests := []struct {
		name         string
		url          string
		wantChips    []string
		wantHeaders  []string
		wantContains []string
	}{
		{
			name:         "all_metrics",
			url:          "/runs/eval-runner/docs-eval-cand-05ce4d8?metric=all",
			wantChips:    []string{`href="?metric=all" class="filter-chip active"`},
			wantHeaders:  []string{"<th>latency (p50/p95)</th>", "<th>tokens</th>", "<th>cost</th>"},
			wantContains: []string{"6.01s / 9.78s", "4399", "$0.0013"},
		},
		{
			name:         "bogus_metric_fallback_all",
			url:          "/runs/eval-runner/docs-eval-cand-05ce4d8?metric=bogus",
			wantChips:    []string{`href="?metric=all" class="filter-chip active"`},
			wantHeaders:  []string{"<th>latency (p50/p95)</th>", "<th>tokens</th>", "<th>cost</th>"},
			wantContains: []string{"6.01s / 9.78s", "4399", "$0.0013"},
		},
		{
			name:         "latency_filter",
			url:          "/runs/eval-runner/docs-eval-cand-05ce4d8?metric=latency",
			wantChips:    []string{`href="?metric=latency" class="filter-chip active"`},
			wantHeaders:  []string{"<th>latency (p50/p95)</th>"},
			wantContains: []string{"6.01s / 9.78s"},
		},
		{
			name:         "tokens_filter",
			url:          "/runs/eval-runner/docs-eval-cand-05ce4d8?metric=tokens",
			wantChips:    []string{`href="?metric=tokens" class="filter-chip active"`},
			wantHeaders:  []string{"<th>tokens</th>", "<th>input</th>", "<th>output</th>", "<th>reasoning</th>"},
			wantContains: []string{"4399", "2868", "1531", "133"},
		},
		{
			name:         "cost_filter",
			url:          "/runs/eval-runner/docs-eval-cand-05ce4d8?metric=cost",
			wantChips:    []string{`href="?metric=cost" class="filter-chip active"`},
			wantHeaders:  []string{"<th>cost</th>"},
			wantContains: []string{"$0.0013"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.url, nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 OK, got %d", w.Code)
			}
			body := w.Body.String()
			for _, chip := range tc.wantChips {
				if !strings.Contains(body, chip) {
					t.Errorf("expected chip %q in body", chip)
				}
			}
			for _, hdr := range tc.wantHeaders {
				if !strings.Contains(body, hdr) {
					t.Errorf("expected header %q in body", hdr)
				}
			}
			for _, s := range tc.wantContains {
				if !strings.Contains(body, s) {
					t.Errorf("expected %q in body", s)
				}
			}
		})
	}
}

func TestServerComparisonDetail_MetricFilter(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()

	// Configure base run metrics
	baseRun.Status.Tasks[0].Latency = &v1alpha1.TaskLatency{P50: 8.5, P95: 12.0}
	baseRun.Status.Tasks[0].Tokens = &v1alpha1.TaskTokens{Total: 2000}
	baseCost := 0.0015
	baseRun.Status.Tasks[0].CostUSD = &baseCost

	// Configure candidate run metrics
	candRun.Status.Tasks[0].Latency = &v1alpha1.TaskLatency{P50: 6.01, P95: 9.78}
	candRun.Status.Tasks[0].Tokens = &v1alpha1.TaskTokens{
		Total:     3500,
		Reasoning: 120,
	}
	candCost := 0.0025
	candRun.Status.Tasks[0].CostUSD = &candCost

	// Configure comparison resource
	comp.Status.Tasks[0].Latency = &v1alpha1.TaskLatency{P50: 6.01, P95: 9.78}
	comp.Status.Tasks[0].Tokens = &v1alpha1.TaskTokens{
		Total:     3500,
		Reasoning: 120,
	}
	comp.Status.Tasks[0].CostUSD = &candCost

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()
	server := NewServer(Config{Client: fakeClient})

	t.Run("latency_filter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/docs-eval-same-commit?metric=latency", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, `href="?metric=latency" class="filter-chip active"`) {
			t.Errorf("expected active latency filter chip")
		}
		if !strings.Contains(body, "<th>base latency (p50/p95)</th>") {
			t.Errorf("expected base latency header in table")
		}
		if !strings.Contains(body, "<th>latency (p50/p95)</th>") {
			t.Errorf("expected latency header in table")
		}
		if !strings.Contains(body, "<th>Δ latency (p50)</th>") {
			t.Errorf("expected delta latency header in table")
		}
		if !strings.Contains(body, "8.50s / 12.00s") {
			t.Errorf("expected base latency 8.50s / 12.00s in table")
		}
		if !strings.Contains(body, "6.01s / 9.78s") {
			t.Errorf("expected candidate latency 6.01s / 9.78s in table")
		}
		if !strings.Contains(body, "-2.49s") {
			t.Errorf("expected delta latency -2.49s in table")
		}
		if strings.Contains(body, `class="cibar"`) {
			t.Errorf("expected CI bar to be excluded from latency view")
		}
	})

	t.Run("tokens_filter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/docs-eval-same-commit?metric=tokens", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, `href="?metric=tokens" class="filter-chip active"`) {
			t.Errorf("expected active tokens filter chip")
		}
		if !strings.Contains(body, "<th>base tokens</th>") {
			t.Errorf("expected base tokens header in table")
		}
		if !strings.Contains(body, "<th>tokens</th>") {
			t.Errorf("expected tokens header in table")
		}
		if !strings.Contains(body, "<th>Δ tokens</th>") {
			t.Errorf("expected delta tokens header in table")
		}
		if !strings.Contains(body, "<th>reasoning (+R)</th>") {
			t.Errorf("expected reasoning header in table")
		}
		if !strings.Contains(body, "2000") {
			t.Errorf("expected base tokens 2000 in table")
		}
		if !strings.Contains(body, "3500") {
			t.Errorf("expected candidate tokens 3500 in table")
		}
		if !strings.Contains(body, "&#43;1500") {
			t.Errorf("expected delta tokens &#43;1500 in table")
		}
		if !strings.Contains(body, "+R 120") {
			t.Errorf("expected reasoning tokens +R 120 in table")
		}
		if strings.Contains(body, `class="cibar"`) {
			t.Errorf("expected CI bar to be excluded from tokens view")
		}
	})

	t.Run("cost_filter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/docs-eval-same-commit?metric=cost", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, `href="?metric=cost" class="filter-chip active"`) {
			t.Errorf("expected active cost filter chip")
		}
		if !strings.Contains(body, "<th>base cost ($ USD)</th>") {
			t.Errorf("expected base cost header in table")
		}
		if !strings.Contains(body, "<th>cost</th>") {
			t.Errorf("expected cost header in table")
		}
		if !strings.Contains(body, "<th>Δ cost ($ USD)</th>") {
			t.Errorf("expected delta cost header in table")
		}
		if !strings.Contains(body, "$0.0015") {
			t.Errorf("expected base cost $0.0015 in table")
		}
		if !strings.Contains(body, "$0.0025") {
			t.Errorf("expected candidate cost $0.0025 in table")
		}
		if !strings.Contains(body, "&#43;$0.0010") {
			t.Errorf("expected delta cost &#43;$0.0010 in table")
		}
		if strings.Contains(body, `class="cibar"`) {
			t.Errorf("expected CI bar to be excluded from cost view")
		}
	})

	t.Run("rate_filter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/docs-eval-same-commit?metric=rate", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, `href="?metric=rate" class="filter-chip active"`) {
			t.Errorf("expected active rate filter chip")
		}
		if !strings.Contains(body, "<th>base</th>") {
			t.Errorf("expected base header in table")
		}
		if !strings.Contains(body, "<th>this PR</th>") {
			t.Errorf("expected this PR header in table")
		}
		if !strings.Contains(body, `<th colspan="2">Δ (95% CI)</th>`) {
			t.Errorf("expected delta CI header in table")
		}
		if !strings.Contains(body, `class="cibar"`) {
			t.Errorf("expected CI bar in rate view")
		}
		if strings.Contains(body, "<th>latency (p50/p95)</th>") {
			t.Errorf("expected latency header to be excluded from rate view")
		}
		if strings.Contains(body, "<th>cost</th>") {
			t.Errorf("expected cost header to be excluded from rate view")
		}
	})

	t.Run("all_metrics", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/docs-eval-same-commit?metric=all", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		body := w.Body.String()
		if !strings.Contains(body, `href="?metric=all" class="filter-chip active"`) {
			t.Errorf("expected active all metrics filter chip")
		}
		if !strings.Contains(body, "<th>base</th>") {
			t.Errorf("expected base header in all view")
		}
		if !strings.Contains(body, "<th>this PR</th>") {
			t.Errorf("expected this PR header in all view")
		}
		if !strings.Contains(body, "<th>tokens</th>") {
			t.Errorf("expected tokens header in all view")
		}
		if !strings.Contains(body, "<th>latency (p50/p95)</th>") {
			t.Errorf("expected latency header in all view")
		}
		if !strings.Contains(body, "<th>cost</th>") {
			t.Errorf("expected cost header in all view")
		}
	})
}

func TestServerComparisonsList_CompletedReasonAndDeltas(t *testing.T) {
	scheme := newTestScheme(t)
	comp, baseRun, candRun := regressedComparisonFixture()
	comp.Status.Message = ""
	comp.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	comp.Labels = nil

	long := "The candidate answers the readme questions without the install section, " +
		"and the runbook lookups cite a retired page; both changes trace to the prompt rewrite in this release."
	running := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "eval-running", Namespace: "eval-runner", CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute))},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "eval-run-v1"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "eval-run-missing"},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:   v1alpha1.ComparisonPhaseRunning,
			Verdict: v1alpha1.ComparisonVerdictRunning,
			Message: long,
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, baseRun, candRun, running).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/comparisons", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	body := w.Body.String()

	if !strings.Contains(body, "<th>Completed</th>") || !strings.Contains(body, "<th>Age</th>") {
		t.Errorf("expected Completed and Age columns")
	}
	if !strings.Contains(body, `<td class="ts">2026-10-20 10:35 UTC</td>`) {
		t.Errorf("expected absolute UTC completion time for the completed comparison")
	}
	if !strings.Contains(body, `<td class="ts">—</td>`) {
		t.Errorf("expected an em dash for the running comparison's completion time")
	}

	// REGRESSED without a message: reason derived from status.tasks.
	if !strings.Contains(body, "Regressed on 2 of 4 tasks: docs/readme_comprehension (80% → 40%), triage/runbook_lookup (70% → 54%)") {
		t.Errorf("expected the derived regression reason, got:\n%s", body)
	}
	// A long message is truncated in the cell and kept whole in the title attribute.
	if !strings.Contains(body, `title="`+long+`"`) {
		t.Errorf("expected the full message in a title attribute")
	}
	if !strings.Contains(body, ">The candidate answers the readme questions without the install section, and the runbook lookups cite a retired page; both changes trace to…</div>") {
		t.Errorf("expected the message truncated near 140 characters, got:\n%s", body)
	}

	// Run deltas for the comparison whose Runs exist: four shared tasks, quality 72.5% → 63.5%.
	if !strings.Contains(body, `<span class="delta-k">Quality</span> <span class="delta-v delta-bad" title="Mean success rate 72.5% → 63.5% over 4 shared tasks (Run success rates)">−9.0 pp</span>`) {
		t.Errorf("expected the quality delta from both Runs, got:\n%s", body)
	}
	if !strings.Contains(body, `<span class="delta-v delta-good" title="−3.0K tokens (0.10M → 97.0K) over 4 shared tasks">−3.0%</span>`) {
		t.Errorf("expected the token delta from both Runs")
	}
	if !strings.Contains(body, `<span class="delta-v delta-good" title="$0.0200 → $0.0194 over 4 shared tasks">−$0.0006 (−3.0%)</span>`) {
		t.Errorf("expected the cost delta from both Runs")
	}
	// The running comparison's candidate Run does not exist: deltas are missing.
	if !strings.Contains(body, `title="Token counts are missing from one or both Runs">—</span>`) {
		t.Errorf("expected missing token delta for the comparison without a candidate Run")
	}
	// Unlabeled comparisons: no grouped view and no kind filter.
	if strings.Contains(body, "By candidate run") || strings.Contains(body, `class="filter-chip`) {
		t.Errorf("did not expect grouped view or kind filter without labeled comparisons")
	}
}

func TestServerComparisonsList_StructuredReasonShowsHeadline(t *testing.T) {
	scheme := newTestScheme(t)
	comp, baseRun, candRun := regressedComparisonFixture()
	comp.Labels = nil
	comp.Status.Message = structuredReasonMessage

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, baseRun, candRun).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/comparisons", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	body := w.Body.String()

	headline := "Pass rate fell on 2 of 4 tasks and rose on 1; the largest drop is docs/readme_comprehension, 80% -&gt; 40%."
	if !strings.Contains(body, `">`+headline+`</div>`) {
		t.Errorf("expected only the headline in the reason line, got:\n%s", body)
	}
	if strings.Contains(body, headline+" Every task") {
		t.Errorf("did not expect text after the headline in the reason line")
	}
	if !strings.Contains(body, `title="Pass rate fell on 2 of 4 tasks`) || !strings.Contains(body, `Code changes: https://github.com/example/repo/compare/a81d0c4...05ce4d8">`+headline) {
		t.Errorf("expected the full message in the title attribute")
	}
}

func groupedServerFixtures(now time.Time) []client.Object {
	objs := []client.Object{}
	items, runs := groupingFixtures(now)
	for i := range items {
		item := items[i]
		objs = append(objs, &item)
	}
	for _, run := range runs {
		objs = append(objs, run)
	}
	return objs
}

func TestServerComparisonsList_GroupedByCandidateRun(t *testing.T) {
	scheme := newTestScheme(t)
	now := time.Now().Truncate(time.Second)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(groupedServerFixtures(now)...).Build()
	server := NewServer(Config{Client: fakeClient})

	get := func(t *testing.T, path string) string {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("GET %s: expected 200 OK, got %d", path, w.Code)
		}
		return w.Body.String()
	}

	t.Run("all", func(t *testing.T) {
		body := get(t, "/comparisons")
		for _, want := range []string{
			`<a href="/comparisons" class="filter-chip active">All</a>`,
			`<a href="/comparisons?kind=baseline" class="filter-chip ">vs baseline</a>`,
			`<a href="/comparisons?kind=previous" class="filter-chip ">vs previous</a>`,
			"<th>vs baseline</th>",
			"<th>vs previous</th>",
			`<h2 class="section-title">By candidate run</h2>`,
			`<h2 class="section-title">Other comparisons</h2>`,
			"7 total",
			"Older: ",
			`<a href="/comparisons/eval-runner/v3-vs-v1-old" class="mono">v3-vs-v1-old</a>`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("expected %q in body", want)
			}
		}

		// Groups ordered by candidate Run creation time, newest first; the deleted Run's group
		// uses its newest Comparison's creation time.
		idxV4 := strings.Index(body, `<a href="/runs/eval-runner/run-v4-deleted" class="mono" style="font-weight:600">`)
		idxV3 := strings.Index(body, `<a href="/runs/eval-runner/run-v3" class="mono" style="font-weight:600">`)
		idxV2 := strings.Index(body, `<a href="/runs/eval-runner/run-v2" class="mono" style="font-weight:600">`)
		if idxV4 == -1 || idxV3 == -1 || idxV2 == -1 || !(idxV4 < idxV3 && idxV3 < idxV2) {
			t.Errorf("expected group order v4, v3, v2: v4=%d v3=%d v2=%d", idxV4, idxV3, idxV2)
		}
		// The comparison with both labels appears in both cells of its row.
		if n := strings.Count(body, `<a href="/comparisons/eval-runner/v2-vs-v1" class="mono" style="font-size:13px">v2-vs-v1</a>`); n != 2 {
			t.Errorf("expected v2-vs-v1 in both cells, found %d", n)
		}
		// Unlabeled comparisons keep the flat table, including the one labeled vs-baseline=no.
		otherIdx := strings.Index(body, "Other comparisons")
		if otherIdx == -1 || !strings.Contains(body[otherIdx:], "/comparisons/eval-runner/pr-7") || !strings.Contains(body[otherIdx:], "/comparisons/eval-runner/pr-8") {
			t.Errorf("expected pr-7 and pr-8 in the unlabeled table")
		}
		if strings.Contains(body[otherIdx:], "/comparisons/eval-runner/v3-vs-v2") {
			t.Errorf("did not expect labeled comparisons in the unlabeled table")
		}
	})

	t.Run("baseline filter", func(t *testing.T) {
		body := get(t, "/comparisons?kind=baseline")
		if !strings.Contains(body, `<a href="/comparisons?kind=baseline" class="filter-chip active">vs baseline</a>`) {
			t.Errorf("expected the vs baseline chip to be active")
		}
		if strings.Contains(body, "<th>vs previous</th>") || !strings.Contains(body, "<th>vs baseline</th>") {
			t.Errorf("expected only the vs baseline column")
		}
		if strings.Contains(body, "/comparisons/eval-runner/v3-vs-v2") || strings.Contains(body, "/comparisons/eval-runner/pr-7") {
			t.Errorf("expected vs-previous-only and unlabeled comparisons to be hidden")
		}
		if !strings.Contains(body, "4 total") {
			t.Errorf("expected 4 vs-baseline comparisons in the total")
		}
	})

	t.Run("previous filter", func(t *testing.T) {
		body := get(t, "/comparisons?kind=previous")
		if strings.Contains(body, "<th>vs baseline</th>") || !strings.Contains(body, "<th>vs previous</th>") {
			t.Errorf("expected only the vs previous column")
		}
		if !strings.Contains(body, "/comparisons/eval-runner/v3-vs-v2") || strings.Contains(body, "/comparisons/eval-runner/v3-vs-v1\"") {
			t.Errorf("expected vs-previous comparisons only")
		}
		if !strings.Contains(body, "2 total") {
			t.Errorf("expected 2 vs-previous comparisons in the total")
		}
	})

	t.Run("unknown kind shows all", func(t *testing.T) {
		body := get(t, "/comparisons?kind=bogus")
		if !strings.Contains(body, `<a href="/comparisons" class="filter-chip active">All</a>`) || !strings.Contains(body, "7 total") {
			t.Errorf("expected an unknown kind to fall back to All")
		}
	})

	t.Run("kind links keep the namespace and reset the offset", func(t *testing.T) {
		body := get(t, "/comparisons?namespace=eval-runner&offset=1&limit=2")
		if !strings.Contains(body, `<a href="/comparisons?kind=baseline&amp;limit=2&amp;namespace=eval-runner" class="filter-chip ">vs baseline</a>`) {
			t.Errorf("expected the kind link to keep namespace and limit and drop offset, got:\n%s", body)
		}
	})

	t.Run("pagination counts rows", func(t *testing.T) {
		body := get(t, "/comparisons?limit=2")
		if !strings.Contains(body, "Showing 1–2 of 5") {
			t.Errorf("expected 5 rows (3 groups, 2 unlabeled) in the pagination range")
		}
		if !strings.Contains(body, "run-v4-deleted") || !strings.Contains(body, "/comparisons/eval-runner/pr-7") || strings.Contains(body, `href="/runs/eval-runner/run-v3" class="mono" style="font-weight:600"`) {
			t.Errorf("expected page one to hold the v4 group and pr-7 only")
		}
	})
}

// runListErrorClient fails RunList calls and delegates everything else.
type runListErrorClient struct {
	client.Reader
}

func (c *runListErrorClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1alpha1.RunList); ok {
		return apierrors.NewForbidden(schema.GroupResource{Group: "evalrunner.gke-labs.dev", Resource: "runs"}, "", fmt.Errorf("access denied"))
	}
	return c.Reader.List(ctx, list, opts...)
}

func TestServerComparisonsList_RunListFailureDegrades(t *testing.T) {
	scheme := newTestScheme(t)
	now := time.Now().Truncate(time.Second)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(groupedServerFixtures(now)...).Build()
	server := NewServer(Config{Client: &runListErrorClient{Reader: fakeClient}})

	req := httptest.NewRequest(http.MethodGet, "/comparisons", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK when runs cannot be listed, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "By candidate run") {
		t.Errorf("expected the grouped view without Run data")
	}
	if strings.Contains(body, `class="delta-v delta-good"`) || strings.Contains(body, `class="delta-v delta-bad"`) {
		t.Errorf("expected no directional deltas without Run data")
	}
	// Groups fall back to Comparison creation times: v2's newest Comparison (-5m) is newer than v3's (-20m).
	idxV3 := strings.Index(body, `<a href="/runs/eval-runner/run-v3" class="mono" style="font-weight:600">`)
	idxV2 := strings.Index(body, `<a href="/runs/eval-runner/run-v2" class="mono" style="font-weight:600">`)
	if idxV3 == -1 || idxV2 == -1 || idxV2 > idxV3 {
		t.Errorf("expected v2 before v3 when ordering by Comparison creation time: v2=%d v3=%d", idxV2, idxV3)
	}
}

func TestServerComparisonDetail_ReasonPanelRegressedTasksAndDeltas(t *testing.T) {
	scheme := newTestScheme(t)
	comp, baseRun, candRun := regressedComparisonFixture()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, baseRun, candRun).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/eval-v3-vs-v1", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()

	if strings.Contains(body, `class="msg-alert"`) {
		t.Errorf("expected no error alert for the status message")
	}
	if !strings.Contains(body, `<div class="reason-headline">Two tasks regressed after the prompt rewrite.</div>`) ||
		!strings.Contains(body, `<p class="reason-para">readme comprehension answers omit the install section in 3 of 5 samples.</p>`) {
		t.Errorf("expected the message as a headline and a paragraph in the reason panel")
	}
	if !strings.Contains(body, `Regressed tasks <span style="font-size:13px;font-weight:500;color:var(--muted)">2 of 4</span>`) {
		t.Errorf("expected the regressed tasks heading with counts")
	}
	for _, want := range []string{
		`<td class="num">0.80 → 0.40</td>`,
		`<td class="num">-0.40</td>`,
		`<td class="num" style="font-size:13px;color:var(--muted)">[-0.60, -0.20]</td>`,
		`<td class="num">0.70 → 0.54</td>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expected %q in the regressed tasks table", want)
		}
	}
	if !strings.Contains(body, `<div class="delta-strip-detail"><div class="delta-strip">`) {
		t.Errorf("expected the run delta strip at the top of the detail page")
	}
	if !strings.Contains(body, ">−9.0 pp</span>") || !strings.Contains(body, ">−3.0%</span>") || !strings.Contains(body, ">−$0.0006 (−3.0%)</span>") {
		t.Errorf("expected quality, tokens and cost deltas in the strip")
	}

	t.Run("no regressed tasks list without regressed tasks", func(t *testing.T) {
		_, baseRun, candRun := sampleFixtures()
		_, _, improved := sampleFixtures()
		improved.Name = "improved-comp"
		fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(improved, baseRun, candRun).Build()
		s := NewServer(Config{Client: fc})
		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/improved-comp", nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		if strings.Contains(w.Body.String(), "Regressed tasks") {
			t.Errorf("did not expect a regressed tasks list for an IMPROVED comparison")
		}
		if strings.Contains(w.Body.String(), `class="reason-panel"`) {
			t.Errorf("did not expect a reason panel without a status message")
		}
	})
}

type countingReader struct {
	client.Reader
	calls atomic.Int32
}

func (c *countingReader) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	c.calls.Add(1)
	return c.Reader.Get(ctx, key, obj, opts...)
}

func (c *countingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.calls.Add(1)
	return c.Reader.List(ctx, list, opts...)
}

func TestServerServesListsFromInjectedReaderWithoutDirectClient(t *testing.T) {
	scheme := newTestScheme(t)

	runCached := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "base-cached-run-default",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-1 * time.Hour)),
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/branch": "main",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/org/repo",
				Commit: "abcdef123456",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
		},
	}

	compCached := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "cached-comp-default",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-30 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "base-cached-run-default",
			},
			Candidate: v1alpha1.ComparisonRunRef{
				Run: "base-cached-run-default",
			},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "org/repo",
				Number:     101,
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase: v1alpha1.ComparisonPhaseCompleted,
		},
	}

	runDirect := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "direct-run-custom",
			Namespace:         "custom-ns",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/org/repo",
				Commit: "fedcba654321",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
		},
	}

	cacheFake := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runCached, compCached).Build()
	directFake := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runDirect).Build()
	directSpy := &countingReader{Reader: directFake}

	server := NewServer(Config{
		Client:           directSpy,
		Cache:            cacheFake,
		DefaultNamespace: "eval-runner",
	})

	listEndpoints := []struct {
		url      string
		wantBody string
	}{
		{"/runs", "base-cached-run-default"},
		{"/comparisons", "cached-comp-default"},
		{"/pulls", "cached-comp-default"},
		{"/series", "org/repo"},
	}

	for _, ep := range listEndpoints {
		t.Run("default_ns_"+ep.url, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ep.url, nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 OK for %s, got %d", ep.url, w.Code)
			}
			if !strings.Contains(w.Body.String(), ep.wantBody) {
				t.Errorf("expected response from injected reader to contain %q, body: %s", ep.wantBody, w.Body.String())
			}
		})
	}

	// Verify that direct client was NEVER called for the default namespace list requests
	if calls := directSpy.calls.Load(); calls != 0 {
		t.Fatalf("expected direct client to never be called for default namespace lists, but got %d calls", calls)
	}

	// Verify that detail handlers in default namespace also read from cache without touching direct client
	detailEndpoints := []struct {
		url      string
		wantBody string
	}{
		{"/runs/eval-runner/base-cached-run-default", "base-cached-run-default"},
		{"/comparisons/eval-runner/cached-comp-default", "cached-comp-default"},
	}
	for _, ep := range detailEndpoints {
		t.Run("default_ns_detail_"+ep.url, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ep.url, nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 OK for %s, got %d", ep.url, w.Code)
			}
			if !strings.Contains(w.Body.String(), ep.wantBody) {
				t.Errorf("expected response from injected reader to contain %q", ep.wantBody)
			}
		})
	}
	if calls := directSpy.calls.Load(); calls != 0 {
		t.Fatalf("expected direct client to never be called for default namespace details, but got %d calls", calls)
	}

	// Cross-namespace override request must route to direct client
	t.Run("cross_namespace_calls_direct_client", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs?namespace=custom-ns", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for cross-namespace request, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "direct-run-custom") {
			t.Errorf("expected response to contain direct-run-custom, body: %s", w.Body.String())
		}
		if calls := directSpy.calls.Load(); calls == 0 {
			t.Fatalf("expected direct client to be called for cross-namespace request, got 0 calls")
		}
	})
}

func TestServerHealthz_CacheSync(t *testing.T) {
	scheme := newTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	t.Run("returns 503 until synced then 200", func(t *testing.T) {
		var synced atomic.Bool
		synced.Store(false)

		server := NewServer(Config{
			Client:           fakeClient,
			CacheSyncChecker: synced.Load,
		})

		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected status 503, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "cache not synced") {
			t.Errorf("expected body to contain 'cache not synced', got %q", w.Body.String())
		}

		// Cache sync completes
		synced.Store(true)

		w2 := httptest.NewRecorder()
		server.ServeHTTP(w2, req)
		if w2.Code != http.StatusOK {
			t.Fatalf("expected status 200 after sync, got %d", w2.Code)
		}
		if strings.TrimSpace(w2.Body.String()) != "ok" {
			t.Errorf("expected body 'ok', got %q", w2.Body.String())
		}
	})

	t.Run("returns 200 when checker is nil", func(t *testing.T) {
		server := NewServer(Config{
			Client: fakeClient,
		})

		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
		if strings.TrimSpace(w.Body.String()) != "ok" {
			t.Errorf("expected body 'ok', got %q", w.Body.String())
		}
	})

	t.Run("livez returns 200 while cache is unsynced", func(t *testing.T) {
		server := NewServer(Config{
			Client:           fakeClient,
			CacheSyncChecker: func() bool { return false },
		})

		req := httptest.NewRequest(http.MethodGet, "/livez", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200 for /livez, got %d", w.Code)
		}
		if strings.TrimSpace(w.Body.String()) != "ok" {
			t.Errorf("expected body 'ok', got %q", w.Body.String())
		}
	})
}
