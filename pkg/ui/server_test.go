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
	"github.com/gke-labs/eval-runner/pkg/forge/consumer"
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

func TestServerRepoIndex(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "Repositories") {
		t.Errorf("expected body to contain 'Repositories', got: %s", body)
	}
	if !strings.Contains(body, "github.com/gke-labs/eval-runner") {
		t.Errorf("expected body to contain repository name, got: %s", body)
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

	req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8", nil)
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
	expectedLink := "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8/tasks/triage%2Frunbook_lookup/samples"
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

func TestServer_FooterExposesVersion(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()

	testVersion := "8f53b29f9fd379ab026138139e168780f53948f9-dirty"
	server := NewServer(Config{
		Client:           fakeClient,
		DefaultNamespace: "eval-runner",
		Version:          testVersion,
	})

	// 1. Run detail page footer contains version
	reqRun := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8", nil)
	wRun := httptest.NewRecorder()
	server.ServeHTTP(wRun, reqRun)
	if wRun.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for run detail, got %d", wRun.Code)
	}
	expectedFooter := "<span>eval-runner " + testVersion + "</span>"
	if !strings.Contains(wRun.Body.String(), expectedFooter) {
		t.Errorf("expected footer to contain %q, got:\n%s", expectedFooter, wRun.Body.String())
	}

	// 2. Comparison detail page footer contains version
	reqComp := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/comparisons/docs-eval-same-commit", nil)
	wComp := httptest.NewRecorder()
	server.ServeHTTP(wComp, reqComp)
	if wComp.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for comparison detail, got %d", wComp.Code)
	}
	if !strings.Contains(wComp.Body.String(), expectedFooter) {
		t.Errorf("expected comparison footer to contain %q, got:\n%s", expectedFooter, wComp.Body.String())
	}

	// 3. Unconfigured version does not render version span
	serverNoVer := NewServer(Config{
		Client:           fakeClient,
		DefaultNamespace: "eval-runner",
	})
	wNoVer := httptest.NewRecorder()
	serverNoVer.ServeHTTP(wNoVer, reqRun)
	if strings.Contains(wNoVer.Body.String(), "eval-runner "+testVersion) {
		t.Errorf("expected unconfigured server not to render version in footer")
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

	req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8", nil)
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

	req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/comparisons/docs-eval-same-commit", nil)
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

	req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/comparisons/comp-batches-tokens", nil)
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
	if !strings.Contains(body, "/github.com/gke-labs/eval-runner/comparisons/pr-10-head2") {
		t.Errorf("expected Comparison link for PR 10")
	}
	if !strings.Contains(body, "/github.com/gke-labs/eval-runner/pulls/10") {
		t.Errorf("expected history link for PR 10: /github.com/gke-labs/eval-runner/pulls/10")
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

		if w.Code != http.StatusFound {
			t.Fatalf("expected 302 redirect for /pulls/%s/42, got %d", hash, w.Code)
		}
		loc := w.Header().Get("Location")
		if loc != "/github.com/gke-labs/eval-runner/pulls/42" {
			t.Errorf("expected redirect to /github.com/gke-labs/eval-runner/pulls/42, got %q", loc)
		}

		reqTarget := httptest.NewRequest(http.MethodGet, loc, nil)
		wTarget := httptest.NewRecorder()
		server.ServeHTTP(wTarget, reqTarget)

		if wTarget.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for %s, got %d", loc, wTarget.Code)
		}
		body := wTarget.Body.String()

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
		if !strings.Contains(body, "No pull requests found.") {
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

		if w.Code == http.StatusFound {
			loc := w.Header().Get("Location")
			req = httptest.NewRequest(http.MethodGet, loc, nil)
			w = httptest.NewRecorder()
			server.ServeHTTP(w, req)
		}

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

	req := httptest.NewRequest(http.MethodGet, "/github.com/example/repo/runs/range-run-detail", nil)
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
		TaskID:      taskID,
		Commit:      commit,
		Model:       "gemini-2.5-pro",
		SampleCount: 2,
		Successes:   1,
		MeanScore:   &meanScore,
		StartedAt:   &now,
		FinishedAt:  &now,
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/sample-run/tasks/triage%2Frunbook_lookup/samples", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/sample-run/tasks/triage%2Frunbook_lookup/samples/sample-019", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/sample-run/tasks/triage%2Frunbook_lookup/samples/sample-017", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/sample-run/tasks/task%2Fno-log/samples", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected 404 for missing logPath, got %d", w.Code)
		}
	})

	t.Run("Negative_NoSuchTask_404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/sample-run/tasks/nonexistent%2Ftask/samples", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/no-such-run/tasks/triage%2Frunbook_lookup/samples", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/sample-run/tasks/triage%2Frunbook_lookup/samples", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/sample-run/tasks/task%2Fbad-prefix/samples", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusForbidden {
			t.Errorf("expected 403 Forbidden for prefix mismatch, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("Negative_StorageFileNotFound_404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/sample-run/tasks/task%2Ffile-not-found/samples", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/sample-run/tasks/triage%2Frunbook_lookup/samples/no-such-sample", nil)
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
			"/github.com/gke-labs/eval-runner/runs/sample-run/tasks/triage%2Frunbook_lookup/samples",
			"/github.com/gke-labs/eval-runner/runs/sample-run/tasks/triage%2Frunbook_lookup/samples/sample-017",
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
			TaskID:      "task/truncated",
			Commit:      commit,
			Model:       "gemini-2.5-pro",
			SampleCount: 1,
			StartedAt:   &now,
			FinishedAt:  &now,
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
			TaskID:      "task/hostile",
			Commit:      commit,
			Model:       "gemini-2.5-pro",
			SampleCount: 1,
			StartedAt:   &now,
			FinishedAt:  &now,
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
		TaskID:      taskID,
		Commit:      commit,
		Model:       "gemini-2.5-pro",
		SampleCount: 2,
		StartedAt:   &now,
		FinishedAt:  &now,
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

func TestServer_TaskSamplesFallbackWithSummaryJSON_CorruptLog(t *testing.T) {
	tempDir := t.TempDir()
	sink := &artifacts.LocalDirSink{BaseDir: tempDir}
	commit := "commitcorrupt1234"
	taskID := "task/corrupt"

	scoreVal := 1.0
	sampleSummaries := []artifacts.TaskSampleSummary{
		{
			ID:     "sample-corrupt-1",
			Score:  &scoreVal,
			Status: "pass",
		},
	}

	meta := artifacts.RunMetadata{
		RunName:           "corrupt-run",
		RunNamespace:      "eval-runner",
		TaskID:            taskID,
		Commit:            commit,
		Model:             "gemini-2.5-pro",
		ControllerVersion: "v1.0.0",
		Scorer:            "choice",
		Samples:           sampleSummaries,
	}

	// Full log is unparsable corrupt JSON: slow path cannot satisfy the request
	invalidLog := []byte(`{"version": 2, "corrupt_data": true`)

	logPath, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", invalidLog)
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "corrupt-run",
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

	// Request sample list: falls back to summary.json because full log is unparsable
	reqList := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/corrupt-run/tasks/task%2Fcorrupt/samples", nil)
	wList := httptest.NewRecorder()
	srv.ServeHTTP(wList, reqList)
	if wList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for sample list from summary fallback, got %d: %s", wList.Code, wList.Body.String())
	}
	if !strings.Contains(wList.Body.String(), "sample-corrupt-1") {
		t.Errorf("expected sample-corrupt-1 in sample list HTML")
	}

	bodyList := wList.Body.String()
	// Verify visible fallback banner is present
	if !strings.Contains(bodyList, "msg-alert") || !strings.Contains(bodyList, "summary.json") {
		t.Errorf("expected visible fallback banner in body:\n%s", bodyList)
	}
	if !strings.Contains(bodyList, "could not be parsed") {
		t.Errorf("expected 'could not be parsed' in fallback banner, got:\n%s", bodyList)
	}
	if strings.Contains(bodyList, "load cap") {
		t.Errorf("expected banner NOT to contain 'load cap' for corrupt log, got:\n%s", bodyList)
	}
	if strings.Contains(bodyList, "/samples/") {
		t.Errorf("expected no /samples/ hrefs in fallback mode, got:\n%s", bodyList)
	}

	// Verify direct transcript URL returns 404 with unparsable message, NOT load cap
	reqTrans := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/corrupt-run/tasks/task%2Fcorrupt/samples/sample-corrupt-1", nil)
	wTrans := httptest.NewRecorder()
	srv.ServeHTTP(wTrans, reqTrans)
	if wTrans.Code != http.StatusNotFound {
		t.Errorf("expected 404 Not Found for direct transcript URL, got %d", wTrans.Code)
	}
	if !strings.Contains(wTrans.Body.String(), "transcript unavailable: log could not be parsed") {
		t.Errorf("expected 'transcript unavailable: log could not be parsed' in transcript error body, got:\n%s", wTrans.Body.String())
	}
	if strings.Contains(wTrans.Body.String(), "load cap") {
		t.Errorf("expected transcript error body NOT to contain 'load cap', got:\n%s", wTrans.Body.String())
	}

	// Verify the cache key used is the summary URI, never the logPath
	summaryURI := artifacts.TaskSummaryURI(logPath)
	if _, _, ok := srv.cache.get(summaryURI); !ok {
		t.Errorf("expected summaryURI %q to be cached in srv.cache", summaryURI)
	}

	// Verify truncation banner is rendered if samplesTruncated is true
	metaTrunc := meta
	metaTrunc.TaskID = "task/truncated"
	metaTrunc.SamplesTruncated = true
	truncLogPath, err := sink.StoreArtifact(t.Context(), metaTrunc, "inspect.json", invalidLog)
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}
	runTrunc := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "corrupt-run-trunc",
			Namespace: "eval-runner",
		},
		Spec: run.Spec,
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      "task/truncated",
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: truncLogPath,
				},
			},
		},
	}
	clTrunc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runTrunc).Build()
	srvTrunc := NewServer(Config{Client: clTrunc, ArtifactSink: sink})
	reqTrunc := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/corrupt-run-trunc/tasks/task%2Ftruncated/samples", nil)
	wTrunc := httptest.NewRecorder()
	srvTrunc.ServeHTTP(wTrunc, reqTrunc)
	if wTrunc.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for truncated sample list, got %d", wTrunc.Code)
	}
	if !strings.Contains(wTrunc.Body.String(), "10,000") {
		t.Errorf("expected 10,000 truncation message in banner, got:\n%s", wTrunc.Body.String())
	}

	// Fetch a second time to assert the 10,000 truncation banner survives a cache hit
	reqTrunc2 := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/corrupt-run-trunc/tasks/task%2Ftruncated/samples", nil)
	wTrunc2 := httptest.NewRecorder()
	srvTrunc.ServeHTTP(wTrunc2, reqTrunc2)
	if wTrunc2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on cached truncated sample list, got %d", wTrunc2.Code)
	}
	if !strings.Contains(wTrunc2.Body.String(), "10,000") {
		t.Errorf("expected 10,000 truncation message in banner on cache hit, got:\n%s", wTrunc2.Body.String())
	}
}

func TestServer_TaskSamplesFallbackWithSummaryJSON_OversizedLog_InMemorySink(t *testing.T) {
	sink := artifacts.NewInMemorySink()
	commit := "commitoversized1234"
	taskID := "task/oversized"

	scoreVal := 1.0
	sampleSummaries := []artifacts.TaskSampleSummary{
		{
			ID:     "sample-oversized-1",
			Score:  &scoreVal,
			Status: "pass",
		},
		{
			ID:     "sample-oversized-2",
			Score:  &scoreVal,
			Status: "fail",
		},
	}

	meta := artifacts.RunMetadata{
		RunName:           "oversized-run",
		RunNamespace:      "eval-runner",
		TaskID:            taskID,
		Commit:            commit,
		Model:             "gemini-2.5-pro",
		ControllerVersion: "v1.0.0",
		Scorer:            "choice",
		Samples:           sampleSummaries,
		SampleCount:       len(sampleSummaries),
	}

	// Oversized log exceeding DefaultMaxLoadBytes (10 MiB)
	oversizedLog := make([]byte, artifacts.DefaultMaxLoadBytes+1024)

	logPath, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", oversizedLog)
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "oversized-run",
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

	// 1. Request samples list: returns 200 with no /samples/ hrefs and the extended banner
	reqList := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/oversized-run/tasks/task%2Foversized/samples", nil)
	wList := httptest.NewRecorder()
	srv.ServeHTTP(wList, reqList)
	if wList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for oversized log sample list, got %d: %s", wList.Code, wList.Body.String())
	}

	body := wList.Body.String()

	// Samples rendered as plain text
	if !strings.Contains(body, "sample-oversized-1") || !strings.Contains(body, "sample-oversized-2") {
		t.Errorf("expected sample IDs in body, got:\n%s", body)
	}

	// No /samples/ hrefs
	if strings.Contains(body, "/samples/") {
		t.Errorf("expected no /samples/ hrefs in fallback mode, got:\n%s", body)
	}

	// Extended banner with plain text artifact path
	wantBanner := fmt.Sprintf("transcripts unavailable: the evaluation log exceeds the UI load cap (10 MiB); download it from %s", logPath)
	if !strings.Contains(body, wantBanner) {
		t.Errorf("expected extended banner %q in body, got:\n%s", wantBanner, body)
	}
	if !strings.Contains(body, "Showing sample outcomes from summary.json") {
		t.Errorf("expected 'Showing sample outcomes from summary.json' in body, got:\n%s", body)
	}

	// 2. Direct transcript URL: returns 404 with fixed public error string
	reqTranscript := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/oversized-run/tasks/task%2Foversized/samples/sample-oversized-1", nil)
	wTranscript := httptest.NewRecorder()
	srv.ServeHTTP(wTranscript, reqTranscript)

	if wTranscript.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for direct transcript URL in fallback mode, got %d", wTranscript.Code)
	}
	wantError := "transcript unavailable: log exceeds the UI load cap"
	if !strings.Contains(wTranscript.Body.String(), wantError) {
		t.Errorf("expected error %q, got:\n%s", wantError, wTranscript.Body.String())
	}
	if strings.Contains(wTranscript.Body.String(), "Log Truncated") {
		t.Errorf("expected no generic Log Truncated page, got:\n%s", wTranscript.Body.String())
	}
}

type mockGCSTaskSink struct {
	artifacts.ArtifactSink
	bucket string
}

func (m *mockGCSTaskSink) ArtifactPrefix(commit string) string {
	return fmt.Sprintf("gs://%s/runs/%s/", m.bucket, commit)
}

func (m *mockGCSTaskSink) Load(ctx context.Context, path string) ([]byte, error) {
	memPath := "mem://" + strings.TrimPrefix(path, fmt.Sprintf("gs://%s/", m.bucket))
	return m.ArtifactSink.Load(ctx, memPath)
}

func TestServer_TaskSamplesFallbackWithSummaryJSON_GCSConsoleLink(t *testing.T) {
	inMem := artifacts.NewInMemorySink()
	bucket := "my-eval-bucket"
	commit := "commitgcs1234"
	taskID := "task/gcs"

	scoreVal := 1.0
	sampleSummaries := []artifacts.TaskSampleSummary{
		{
			ID:     "sample-gcs-1",
			Score:  &scoreVal,
			Status: "pass",
		},
	}

	meta := artifacts.RunMetadata{
		RunName:           "gcs-run",
		RunNamespace:      "eval-runner",
		TaskID:            taskID,
		Commit:            commit,
		Model:             "gemini-2.5-pro",
		ControllerVersion: "v1.0.0",
		Scorer:            "choice",
		Samples:           sampleSummaries,
		SampleCount:       len(sampleSummaries),
	}

	oversizedLog := make([]byte, artifacts.DefaultMaxLoadBytes+1024)
	rawPath, err := inMem.StoreArtifact(t.Context(), meta, "inspect.json", oversizedLog)
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	gcsSink := &mockGCSTaskSink{
		ArtifactSink: inMem,
		bucket:       bucket,
	}

	gcsLogPath := fmt.Sprintf("gs://%s/%s", bucket, strings.TrimPrefix(rawPath, "mem://"))

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gcs-run",
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
					LogPath: gcsLogPath,
				},
			},
		},
	}

	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()

	srv := NewServer(Config{Client: cl, ArtifactSink: gcsSink})

	reqList := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/gcs-run/tasks/task%2Fgcs/samples", nil)
	wList := httptest.NewRecorder()
	srv.ServeHTTP(wList, reqList)
	if wList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", wList.Code, wList.Body.String())
	}

	body := wList.Body.String()
	wantConsoleURL := FormatLogURL(gcsLogPath)
	if !strings.Contains(body, wantConsoleURL) {
		t.Errorf("expected Cloud Storage console URL %q in body, got:\n%s", wantConsoleURL, body)
	}
	if !strings.Contains(body, gcsLogPath) {
		t.Errorf("expected GCS log path %q in body, got:\n%s", gcsLogPath, body)
	}
	wantBanner := "transcripts unavailable: the evaluation log exceeds the UI load cap (10 MiB)"
	if !strings.Contains(body, wantBanner) {
		t.Errorf("expected extended banner in body, got:\n%s", body)
	}
	if !strings.Contains(body, "download it from") {
		t.Errorf("expected 'download it from' in body, got:\n%s", body)
	}
	if strings.Contains(body, "/samples/") {
		t.Errorf("expected no /samples/ hrefs, got:\n%s", body)
	}
}

func TestServer_TaskSamplesFallbackWithSummaryJSON_MissingLog(t *testing.T) {
	sink := artifacts.NewInMemorySink()
	commit := "commitmissing1234"
	taskID := "task/missing"

	scoreVal := 1.0
	sampleSummaries := []artifacts.TaskSampleSummary{
		{
			ID:     "sample-missing-1",
			Score:  &scoreVal,
			Status: "pass",
		},
	}

	meta := artifacts.RunMetadata{
		RunName:           "missing-run",
		RunNamespace:      "eval-runner",
		TaskID:            taskID,
		Commit:            commit,
		Model:             "gemini-2.5-pro",
		ControllerVersion: "v1.0.0",
		Scorer:            "choice",
		Samples:           sampleSummaries,
		SampleCount:       len(sampleSummaries),
	}

	// Store summary.json by storing an inspect log first
	storedLogPath, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", []byte(`{}`))
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	// Deliberately point to a non-existent inspect log, while summary.json remains where StoreArtifact wrote it
	missingLogPath := strings.Replace(storedLogPath, "inspect.json", "missing_inspect.json", 1)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "missing-run",
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
					LogPath: missingLogPath,
				},
			},
		},
	}

	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()

	srv := NewServer(Config{Client: cl, ArtifactSink: sink})

	// 1. Samples list returns 200 with "could not be loaded" and NO "load cap"
	reqList := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/missing-run/tasks/task%2Fmissing/samples", nil)
	wList := httptest.NewRecorder()
	srv.ServeHTTP(wList, reqList)
	if wList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for missing log sample list, got %d: %s", wList.Code, wList.Body.String())
	}

	body := wList.Body.String()
	if !strings.Contains(body, "could not be loaded") {
		t.Errorf("expected 'could not be loaded' in banner, got:\n%s", body)
	}
	if strings.Contains(body, "load cap") {
		t.Errorf("expected banner NOT to contain 'load cap' for missing log, got:\n%s", body)
	}
	if strings.Contains(body, "/samples/") {
		t.Errorf("expected no /samples/ hrefs, got:\n%s", body)
	}

	// 2. Direct transcript URL returns 404 with "could not be loaded" and NO "load cap"
	reqTrans := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/missing-run/tasks/task%2Fmissing/samples/sample-missing-1", nil)
	wTrans := httptest.NewRecorder()
	srv.ServeHTTP(wTrans, reqTrans)
	if wTrans.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found, got %d", wTrans.Code)
	}
	if !strings.Contains(wTrans.Body.String(), "transcript unavailable: log could not be loaded") {
		t.Errorf("expected 'transcript unavailable: log could not be loaded' in body, got:\n%s", wTrans.Body.String())
	}
	if strings.Contains(wTrans.Body.String(), "load cap") {
		t.Errorf("expected transcript error body NOT to contain 'load cap', got:\n%s", wTrans.Body.String())
	}
}

func TestServer_TaskSamplesPrimaryPath_PreviewsPopulatedWhenLogValid(t *testing.T) {
	tempDir := t.TempDir()
	sink := &artifacts.LocalDirSink{BaseDir: tempDir}
	commit := "commitpreview1234"
	taskID := "task/preview"

	scoreVal := 1.0
	// Summary only has ID, Score, Status - no preview columns!
	sampleSummaries := []artifacts.TaskSampleSummary{
		{
			ID:     "sample-prev-1",
			Score:  &scoreVal,
			Status: "pass",
		},
	}

	meta := artifacts.RunMetadata{
		RunName:           "preview-run",
		RunNamespace:      "eval-runner",
		TaskID:            taskID,
		Commit:            commit,
		Model:             "gemini-2.5-pro",
		ControllerVersion: "v1.0.0",
		Scorer:            "accuracy",
		Samples:           sampleSummaries,
	}

	// Valid Inspect log JSON with input/output/target/explanation previews
	validLogJSON := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "task/preview", "model": "gemini-2.5-pro"},
		"samples": [
			{
				"id": "sample-prev-1",
				"epoch": 1,
				"input": "Input test prompt preview query",
				"output": "Output test answer preview response",
				"target": "Target test answer",
				"scores": {
					"accuracy": {
						"value": 1.0,
						"explanation": "Explanation test grading details"
					}
				}
			}
		]
	}`

	logPath, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", []byte(validLogJSON))
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "preview-run",
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

	// Request sample list: full-log parse stays primary for rows
	reqList := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/preview-run/tasks/task%2Fpreview/samples", nil)
	wList := httptest.NewRecorder()
	srv.ServeHTTP(wList, reqList)
	if wList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for sample list from primary path, got %d: %s", wList.Code, wList.Body.String())
	}

	body := wList.Body.String()
	// Assert input preview column is populated from full log
	if !strings.Contains(body, "Input test prompt preview query") {
		t.Errorf("expected input preview in table rows, got body:\n%s", body)
	}
	// Assert output preview column is populated from full log
	if !strings.Contains(body, "Output test answer preview response") {
		t.Errorf("expected output preview in table rows, got body:\n%s", body)
	}
	// Assert explanation preview is populated from full log
	if !strings.Contains(body, "Explanation test grading details") {
		t.Errorf("expected explanation preview in table rows, got body:\n%s", body)
	}
	// Assert no fallback banner is displayed
	if strings.Contains(body, "could not be loaded") {
		t.Errorf("expected no fallback banner when primary log is valid")
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
		TaskID:      taskID,
		Commit:      commit,
		Model:       "gemini-2.5-pro",
		SampleCount: 2,
		Successes:   2,
		MeanScore:   &meanScore,
		StartedAt:   &now,
		FinishedAt:  &now,
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/oversized-run/tasks/triage%2Frunbook_lookup/samples/sample-oversized-explanation", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/oversized-run/tasks/triage%2Frunbook_lookup/samples/sample-oversized-target", nil)
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
		TaskID:      taskID,
		Commit:      commit,
		Model:       "gemini-2.5-pro",
		SampleCount: 2,
		Successes:   2,
		MeanScore:   &meanScore,
		StartedAt:   &now,
		FinishedAt:  &now,
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

	req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/oversized-run/tasks/triage%2Frunbook_lookup/samples", nil)
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
	expectedExpLink := "/github.com/gke-labs/eval-runner/runs/oversized-run/tasks/triage%2Frunbook_lookup/samples/sample-oversized-explanation"
	if !strings.Contains(body, expectedExpLink) {
		t.Errorf("expected body to link to sample transcript %q", expectedExpLink)
	}
	expectedTargetLink := "/github.com/gke-labs/eval-runner/runs/oversized-run/tasks/triage%2Frunbook_lookup/samples/sample-oversized-target"
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/main", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/nonexistent", nil)
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

		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/main", nil)
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

		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/main", nil)
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

func TestServer_SeriesDetail_FailedTaskWithSamples(t *testing.T) {
	scheme := newTestScheme(t)
	rate := 0.75

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-failed-run",
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
					ID:          "task-fail-scored",
					Phase:       v1alpha1.TaskPhaseFailed,
					Samples:     8,
					Successes:   6,
					SuccessRate: &rate,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()
	server := NewServer(Config{Client: cl})

	req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/main?task=task-fail-scored", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "failed after 8 samples") {
		t.Errorf("expected body to contain 'failed after 8 samples', got:\n%s", body)
	}
	if !strings.Contains(body, "chip bad") {
		t.Errorf("expected body to contain 'chip bad', got:\n%s", body)
	}
	if !strings.Contains(body, "0.75") {
		t.Errorf("expected body to contain rate 0.75, got:\n%s", body)
	}
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/main", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/main?n=100", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		body := w.Body.String()
		if strings.Contains(body, "base-0000001") {
			t.Errorf("expected n=100 to be capped at 50, but base-0000001 was included")
		}
	})

	t.Run("ExplicitSmallerN", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/main?n=10", nil)
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
		reqAlpha := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/main?cohort=cohort-alpha", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/main", nil)
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

func TestServerSeries_ThreeRunGoldenWithSignificantMoveAndGap(t *testing.T) {
	scheme := newTestScheme(t)

	est1 := stats.Proportion(20, 100, 0.95)
	est2 := stats.Proportion(80, 100, 0.95)

	run1 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-1111111",
			Namespace: "custom-ns",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/repo":   "gke-labs_eval-runner",
				"evalrunner.gke-labs.dev/branch": "main",
			},
			Annotations: map[string]string{
				"evalrunner.gke-labs.dev/commit-time": "2026-10-20T01:00:00Z",
			},
		},
		Spec: v1alpha1.RunSpec{
			Cohort: "cohort-gold",
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "1111111111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          "docs/readme_comprehension",
					Phase:       v1alpha1.TaskPhaseCompleted,
					Samples:     100,
					Successes:   20,
					SuccessRate: &est1.Point,
					SuccessRateCI: &v1alpha1.ConfidenceInterval{
						Lower:      est1.CI.Lower,
						Upper:      est1.CI.Upper,
						Confidence: est1.Confidence,
						Method:     est1.Method,
					},
				},
			},
		},
	}

	run2 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-2222222",
			Namespace: "custom-ns",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/repo":   "gke-labs_eval-runner",
				"evalrunner.gke-labs.dev/branch": "main",
			},
			Annotations: map[string]string{
				"evalrunner.gke-labs.dev/commit-time": "2026-10-20T02:00:00Z",
			},
		},
		Spec: v1alpha1.RunSpec{
			Cohort: "cohort-gold",
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "2222222222222222222222222222222222222222",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          "docs/readme_comprehension",
					Phase:       v1alpha1.TaskPhaseCompleted,
					Samples:     100,
					Successes:   80,
					SuccessRate: &est2.Point,
					SuccessRateCI: &v1alpha1.ConfidenceInterval{
						Lower:      est2.CI.Lower,
						Upper:      est2.CI.Upper,
						Confidence: est2.Confidence,
						Method:     est2.Method,
					},
				},
			},
		},
	}

	run3 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-3333333",
			Namespace: "custom-ns",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/repo":   "gke-labs_eval-runner",
				"evalrunner.gke-labs.dev/branch": "main",
			},
			Annotations: map[string]string{
				"evalrunner.gke-labs.dev/commit-time": "2026-10-20T03:00:00Z",
			},
		},
		Spec: v1alpha1.RunSpec{
			Cohort: "cohort-gold",
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "3333333333333333333333333333333333333333",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseFailed,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      "docs/readme_comprehension",
					Phase:   v1alpha1.TaskPhaseFailed,
					Message: "task deadline exceeded",
				},
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run1, run2, run3).Build()
	server := NewServer(Config{Client: cl})

	t.Run("SeriesDetail", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/series/2cc24c2/main?cohort=cohort-gold&namespace=custom-ns", nil)
		w := httptest.NewRecorder()
		server.renderSeriesDetailWithList(w, req, "custom-ns", RepoRef{}, "2cc24c2", "main", &v1alpha1.RunList{Items: []v1alpha1.Run{*run1, *run2, *run3}})

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		body := w.Body.String()
		unescaped := html.UnescapeString(body)

		// Significant move chip: good class, human-readable Newcombe method tooltip
		newcombeTooltip := FormatMethodTooltip("newcombe")
		if !strings.Contains(body, fmt.Sprintf(`class="chip good" title="%s"`, newcombeTooltip)) {
			t.Errorf("expected chip good with newcombe tooltip in body:\n%s", body)
		}
		// Formatted delta string for significant move (exact Wilson bounds produce [+0.47, +0.69])
		if !strings.Contains(unescaped, `Δ +0.60 [+0.47, +0.69]`) {
			t.Errorf("expected Δ +0.60 [+0.47, +0.69] in unescaped body:\n%s", unescaped)
		}

		// Gap row: delta skipped
		if !strings.Contains(body, "base-3333333") {
			t.Errorf("expected base-3333333 in body")
		}

		// SVG: coloured polyline segment with stroke="var(--good)"
		if !strings.Contains(body, `stroke="var(--good)"`) {
			t.Errorf("expected stroke=\"var(--good)\" in SVG polyline")
		}

		// Query params preserved in links
		if !strings.Contains(body, `cohort=cohort-gold`) || !strings.Contains(body, `namespace=custom-ns`) {
			t.Errorf("expected cohort and namespace params in links")
		}

		assertGolden(t, "series_detail_three_runs.golden.html", body)
	})

	t.Run("SeriesList", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/series?cohort=cohort-gold&namespace=custom-ns", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		body := w.Body.String()
		unescaped := html.UnescapeString(body)
		expectedTooltip := fmt.Sprintf("docs/readme_comprehension · %s", FormatMethodTooltip("newcombe"))
		// Last Move shows the significant move from run2 vs run1 with task name and method tooltip
		if !strings.Contains(body, fmt.Sprintf(`class="chip good" title="%s"`, expectedTooltip)) || !strings.Contains(unescaped, `Δ +0.60 [+0.47, +0.69]`) {
			t.Errorf("expected Last Move with chip good and Δ +0.60 [+0.47, +0.69] in series list, got body:\n%s", body)
		}
		if !strings.Contains(body, `cohort=cohort-gold`) || !strings.Contains(body, `namespace=custom-ns`) {
			t.Errorf("expected cohort and namespace params in series list links")
		}
	})
}

func TestServerSeries_CohortsView(t *testing.T) {
	scheme := newTestScheme(t)

	t.Run("TwoCohortsSecondAppearsMidSeries_Golden", func(t *testing.T) {
		repoURL := "https://github.com/gke-labs/eval-runner"
		repoHash := "2cc24c2"
		branch := "main"
		t0 := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)

		rate100 := 1.00
		ci100 := &v1alpha1.ConfidenceInterval{Lower: 0.72, Upper: 1.00, Confidence: 0.95, Method: "wilson"}

		rate020 := 0.20
		ci020 := &v1alpha1.ConfidenceInterval{Lower: 0.06, Upper: 0.52, Confidence: 0.95, Method: "wilson"}

		rate040 := 0.40
		ci040 := &v1alpha1.ConfidenceInterval{Lower: 0.17, Upper: 0.69, Confidence: 0.95, Method: "wilson"}

		rate060 := 0.60
		ci060 := &v1alpha1.ConfidenceInterval{Lower: 0.31, Upper: 0.83, Confidence: 0.95, Method: "wilson"}

		// Commit 1: only default cohort (2 tasks, 100% success)
		run1 := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "base-1111111",
				Namespace: "eval-runner",
				Labels: map[string]string{
					"evalrunner.gke-labs.dev/branch":   branch,
					"evalrunner.gke-labs.dev/head-sha": "1111111",
				},
				Annotations: map[string]string{
					"evalrunner.gke-labs.dev/commit-time": t0.Format(time.RFC3339),
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    repoURL,
					Commit: "1111111",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:            "triage/runbook_lookup",
						Cohort:        "default",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     10,
						SuccessRate:   &rate100,
						SuccessRateCI: ci100,
					},
					{
						ID:            "triage/escalation_policy",
						Cohort:        "default",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     10,
						SuccessRate:   &rate100,
						SuccessRateCI: ci100,
					},
				},
			},
		}

		// Commit 2: new cohort customer-reported appears mid-series (2 tasks, 20% success)
		// default stays at 100%, customer-reported is 20%, combined dips to 60%!
		run2 := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "base-2222222",
				Namespace: "eval-runner",
				Labels: map[string]string{
					"evalrunner.gke-labs.dev/branch":   branch,
					"evalrunner.gke-labs.dev/head-sha": "2222222",
				},
				Annotations: map[string]string{
					"evalrunner.gke-labs.dev/commit-time": t0.Add(1 * time.Hour).Format(time.RFC3339),
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    repoURL,
					Commit: "2222222",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:            "triage/runbook_lookup",
						Cohort:        "default",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     10,
						SuccessRate:   &rate100,
						SuccessRateCI: ci100,
					},
					{
						ID:            "triage/escalation_policy",
						Cohort:        "default",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     10,
						SuccessRate:   &rate100,
						SuccessRateCI: ci100,
					},
					{
						ID:            "triage/summary_quality",
						Cohort:        "customer-reported",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     2,
						SuccessRate:   &rate020,
						SuccessRateCI: ci020,
					},
					{
						ID:            "triage/edge_case",
						Cohort:        "customer-reported",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     2,
						SuccessRate:   &rate020,
						SuccessRateCI: ci020,
					},
				},
			},
		}

		// Commit 3: customer-reported improves to 40%, default remains 100%, combined recovers to 70%!
		run3 := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "base-3333333",
				Namespace: "eval-runner",
				Labels: map[string]string{
					"evalrunner.gke-labs.dev/branch":   branch,
					"evalrunner.gke-labs.dev/head-sha": "3333333",
				},
				Annotations: map[string]string{
					"evalrunner.gke-labs.dev/commit-time": t0.Add(2 * time.Hour).Format(time.RFC3339),
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    repoURL,
					Commit: "3333333",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:            "triage/runbook_lookup",
						Cohort:        "default",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     10,
						SuccessRate:   &rate100,
						SuccessRateCI: ci100,
					},
					{
						ID:            "triage/escalation_policy",
						Cohort:        "default",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     10,
						SuccessRate:   &rate100,
						SuccessRateCI: ci100,
					},
					{
						ID:            "triage/summary_quality",
						Cohort:        "customer-reported",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     4,
						SuccessRate:   &rate040,
						SuccessRateCI: ci040,
					},
					{
						ID:            "triage/edge_case",
						Cohort:        "customer-reported",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     4,
						SuccessRate:   &rate040,
						SuccessRateCI: ci040,
					},
				},
			},
		}

		// Commit 4: customer-reported reaches 60%, default stays 100%, combined reaches 80%!
		run4 := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "base-4444444",
				Namespace: "eval-runner",
				Labels: map[string]string{
					"evalrunner.gke-labs.dev/branch":   branch,
					"evalrunner.gke-labs.dev/head-sha": "4444444",
				},
				Annotations: map[string]string{
					"evalrunner.gke-labs.dev/commit-time": t0.Add(3 * time.Hour).Format(time.RFC3339),
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    repoURL,
					Commit: "4444444",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:            "triage/runbook_lookup",
						Cohort:        "default",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     10,
						SuccessRate:   &rate100,
						SuccessRateCI: ci100,
					},
					{
						ID:            "triage/escalation_policy",
						Cohort:        "default",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     10,
						SuccessRate:   &rate100,
						SuccessRateCI: ci100,
					},
					{
						ID:            "triage/summary_quality",
						Cohort:        "customer-reported",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     6,
						SuccessRate:   &rate060,
						SuccessRateCI: ci060,
					},
					{
						ID:            "triage/edge_case",
						Cohort:        "customer-reported",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     6,
						SuccessRate:   &rate060,
						SuccessRateCI: ci060,
					},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run1, run2, run3, run4).Build()
		server := NewServer(Config{Client: fakeClient})

		req := httptest.NewRequest(http.MethodGet, "/series/"+repoHash+"/"+branch+"?view=cohorts", nil)
		w := httptest.NewRecorder()
		server.renderSeriesDetailWithList(w, req, "eval-runner", RepoRef{}, repoHash, branch, &v1alpha1.RunList{Items: []v1alpha1.Run{*run1, *run2, *run3, *run4}})

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		body := w.Body.String()

		// Verify 3 lines drawn in SVG: default, customer-reported, and all
		if !strings.Contains(body, `stroke="var(--indigo)"`) {
			t.Errorf("expected default cohort polyline stroke var(--indigo)")
		}
		if !strings.Contains(body, `stroke="var(--teal)"`) {
			t.Errorf("expected customer-reported polyline stroke var(--teal)")
		}
		if !strings.Contains(body, `stroke="var(--muted)"`) {
			t.Errorf("expected all polyline stroke var(--muted)")
		}

		// Vertical annotation placed at commit 2 where customer-reported appears
		if !strings.Contains(body, `2 evals added`) {
			t.Errorf("expected '2 evals added' vertical annotation text in body")
		}
		if !strings.Contains(body, `stroke="var(--teal)" stroke-dasharray="4,4"`) {
			t.Errorf("expected vertical annotation dashed line in body")
		}

		// Shaded CI band polygons
		if !strings.Contains(body, `<polygon points="`) {
			t.Errorf("expected shaded CI band polygon in body")
		}

		// Legend contains all 3 series with counts
		if !strings.Contains(body, `default`) || !strings.Contains(body, `(2 evals)`) {
			t.Errorf("expected default (2 evals) in legend")
		}
		if !strings.Contains(body, `customer-reported`) {
			t.Errorf("expected customer-reported in legend")
		}
		if !strings.Contains(body, `all`) || !strings.Contains(body, `(4 evals)`) {
			t.Errorf("expected all (4 evals) in legend")
		}

		// Table contains cohorts and method tooltip
		expectedTooltip := FormatMethodTooltip("pooled-wilson")
		if !strings.Contains(body, expectedTooltip) {
			t.Errorf("expected pooled-wilson tooltip in cohort table")
		}
		if !strings.Contains(body, "2222222") {
			t.Errorf("expected first seen commit 2222222 for customer-reported in table")
		}

		assertGolden(t, "series_detail_cohorts.golden.html", body)
	})

	t.Run("NoCohortsRendersSingleDefaultLine", func(t *testing.T) {
		repoURL := "https://github.com/gke-labs/eval-runner"
		branch := "main"
		t0 := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)

		rate := 0.80
		ci := &v1alpha1.ConfidenceInterval{Lower: 0.60, Upper: 0.95, Confidence: 0.95, Method: "wilson"}

		// 2 runs where no cohort is declared on tasks (empty, defaults to "default")
		runA := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "base-aaaaaaa",
				Namespace: "eval-runner",
				Labels: map[string]string{
					"evalrunner.gke-labs.dev/branch":   branch,
					"evalrunner.gke-labs.dev/head-sha": "aaaaaaa",
				},
				Annotations: map[string]string{
					"evalrunner.gke-labs.dev/commit-time": t0.Format(time.RFC3339),
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    repoURL,
					Commit: "aaaaaaa",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:            "triage/runbook_lookup",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     8,
						SuccessRate:   &rate,
						SuccessRateCI: ci,
					},
				},
			},
		}

		runB := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "base-bbbbbbb",
				Namespace: "eval-runner",
				Labels: map[string]string{
					"evalrunner.gke-labs.dev/branch":   branch,
					"evalrunner.gke-labs.dev/head-sha": "bbbbbbb",
				},
				Annotations: map[string]string{
					"evalrunner.gke-labs.dev/commit-time": t0.Add(1 * time.Hour).Format(time.RFC3339),
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    repoURL,
					Commit: "bbbbbbb",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:            "triage/runbook_lookup",
						Phase:         v1alpha1.TaskPhaseCompleted,
						Samples:       10,
						Successes:     8,
						SuccessRate:   &rate,
						SuccessRateCI: ci,
					},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runA, runB).Build()
		server := NewServer(Config{Client: fakeClient})

		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/"+branch+"?view=cohorts", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		body := w.Body.String()

		// Acceptance criterion: "?view=cohorts with no cohorts renders the single default line."
		// Exactly one polyline rendered with var(--indigo)
		indigoCount := strings.Count(body, `stroke="var(--indigo)"`)
		if indigoCount != 1 {
			t.Errorf("expected exactly 1 var(--indigo) polyline, got %d", indigoCount)
		}
		// No 'all' line rendered
		if strings.Contains(body, `stroke="var(--muted)"`) {
			t.Errorf("did not expect 'all' polyline stroke var(--muted)")
		}
		// No vertical annotations rendered
		if strings.Contains(body, `evals added`) || strings.Contains(body, `eval added`) {
			t.Errorf("did not expect vertical annotation in single-cohort series")
		}
		// Table only has default row
		if strings.Contains(body, `<td>all</td>`) {
			t.Errorf("did not expect 'all' row in single-cohort table")
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
			url:          "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8?metric=all",
			wantChips:    []string{`href="?metric=all" class="filter-chip active"`},
			wantHeaders:  []string{"<th>latency (p50/p95)</th>", "<th>tokens</th>", "<th>cost</th>"},
			wantContains: []string{"6.01s / 9.78s", "4399", "$0.0013"},
		},
		{
			name:         "bogus_metric_fallback_all",
			url:          "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8?metric=bogus",
			wantChips:    []string{`href="?metric=all" class="filter-chip active"`},
			wantHeaders:  []string{"<th>latency (p50/p95)</th>", "<th>tokens</th>", "<th>cost</th>"},
			wantContains: []string{"6.01s / 9.78s", "4399", "$0.0013"},
		},
		{
			name:         "latency_filter",
			url:          "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8?metric=latency",
			wantChips:    []string{`href="?metric=latency" class="filter-chip active"`},
			wantHeaders:  []string{"<th>latency (p50/p95)</th>"},
			wantContains: []string{"6.01s / 9.78s"},
		},
		{
			name:         "tokens_filter",
			url:          "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8?metric=tokens",
			wantChips:    []string{`href="?metric=tokens" class="filter-chip active"`},
			wantHeaders:  []string{"<th>tokens</th>", "<th>input</th>", "<th>output</th>", "<th>reasoning</th>"},
			wantContains: []string{"4399", "2868", "1531", "133"},
		},
		{
			name:         "cost_filter",
			url:          "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8?metric=cost",
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/comparisons/docs-eval-same-commit?metric=latency", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/comparisons/docs-eval-same-commit?metric=tokens", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/comparisons/docs-eval-same-commit?metric=cost", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/comparisons/docs-eval-same-commit?metric=rate", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/comparisons/docs-eval-same-commit?metric=all", nil)
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
		{"/", "github.com/org/repo"},
		{"/runs", "base-cached-run-default"},
		{"/comparisons", "cached-comp-default"},
		{"/pulls", "cached-comp-default"},
		{"/series", "org/repo"},
		{"/github.com/org/repo/runs", "base-cached-run-default"},
		{"/github.com/org/repo/comparisons", "cached-comp-default"},
		{"/github.com/org/repo/series", "org/repo"},
		{"/github.com/org/repo/series/main", "org/repo"},
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
		{"/github.com/org/repo/runs/base-cached-run-default", "base-cached-run-default"},
		{"/github.com/org/repo/comparisons/cached-comp-default", "cached-comp-default"},
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

	// Verify that legacy redirects also resolve from cache without touching direct client
	legacyRedirectEndpoints := []struct {
		url          string
		wantLocation string
	}{
		{"/runs/eval-runner/base-cached-run-default", "/github.com/org/repo/runs/base-cached-run-default"},
		{"/comparisons/eval-runner/cached-comp-default", "/github.com/org/repo/comparisons/cached-comp-default"},
		{"/series/" + RepoHash("org/repo") + "/main", "/github.com/org/repo/series/main"},
	}
	for _, ep := range legacyRedirectEndpoints {
		t.Run("default_ns_legacy_redirect_"+ep.url, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ep.url, nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)

			if w.Code != http.StatusPermanentRedirect {
				t.Fatalf("expected 308 for %s, got %d", ep.url, w.Code)
			}
			if loc := w.Header().Get("Location"); loc != ep.wantLocation {
				t.Errorf("expected Location %q for %s, got %q", ep.wantLocation, ep.url, loc)
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

func TestServerRepoRoutes(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()
	server := NewServer(Config{Client: fakeClient})

	routes := []struct {
		name       string
		path       string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "repo overview",
			path:       "/github.com/gke-labs/eval-runner",
			wantStatus: http.StatusOK,
			wantBody:   "Overview",
		},
		{
			name:       "repo overview with trailing slash",
			path:       "/github.com/gke-labs/eval-runner/",
			wantStatus: http.StatusOK,
			wantBody:   "Overview",
		},
		{
			name:       "repo runs list",
			path:       "/github.com/gke-labs/eval-runner/runs",
			wantStatus: http.StatusOK,
			wantBody:   "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8",
		},
		{
			name:       "repo runs list trailing slash",
			path:       "/github.com/gke-labs/eval-runner/runs/",
			wantStatus: http.StatusOK,
			wantBody:   "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8",
		},
		{
			name:       "repo run detail",
			path:       "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8",
			wantStatus: http.StatusOK,
			wantBody:   "docs-eval-cand-05ce4d8",
		},
		{
			name:       "repo comparisons list",
			path:       "/github.com/gke-labs/eval-runner/comparisons",
			wantStatus: http.StatusOK,
			wantBody:   "/github.com/gke-labs/eval-runner/comparisons/docs-eval-same-commit",
		},
		{
			name:       "repo comparisons list trailing slash",
			path:       "/github.com/gke-labs/eval-runner/comparisons/",
			wantStatus: http.StatusOK,
			wantBody:   "/github.com/gke-labs/eval-runner/comparisons/docs-eval-same-commit",
		},
		{
			name:       "repo comparison detail",
			path:       "/github.com/gke-labs/eval-runner/comparisons/docs-eval-same-commit",
			wantStatus: http.StatusOK,
			wantBody:   "docs-eval-same-commit",
		},
	}

	for _, tc := range routes {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Fatalf("GET %s got status %d, want %d", tc.path, w.Code, tc.wantStatus)
			}
			if tc.wantBody != "" && !strings.Contains(w.Body.String(), tc.wantBody) {
				t.Errorf("GET %s body does not contain %q", tc.path, tc.wantBody)
			}
		})
	}
}

func TestServerRepoFiltering(t *testing.T) {
	scheme := newTestScheme(t)

	runA := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "run-a",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "https://github.com/orgA/repoA.git",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
		},
	}
	runB := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "run-b",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-5 * time.Minute)),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "https://github.com/orgB/repoB.git",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
		},
	}

	compA := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-a",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-8 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "orgA/repoA",
			},
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "run-a",
			},
		},
	}
	compB := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-b",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-3 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "orgB/repoB",
			},
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "run-b",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runA, runB, compA, compB).Build()
	server := NewServer(Config{Client: fakeClient})

	// Repo index should list both
	reqIndex := httptest.NewRequest(http.MethodGet, "/", nil)
	wIndex := httptest.NewRecorder()
	server.ServeHTTP(wIndex, reqIndex)
	if wIndex.Code != http.StatusOK {
		t.Fatalf("GET / failed: %d", wIndex.Code)
	}
	bodyIndex := wIndex.Body.String()
	if !strings.Contains(bodyIndex, "github.com/orgA/repoA") || !strings.Contains(bodyIndex, "github.com/orgB/repoB") {
		t.Errorf("repo index does not contain both repos: %s", bodyIndex)
	}

	// Repo A runs should contain run-a and not run-b
	reqRunsA := httptest.NewRequest(http.MethodGet, "/github.com/orgA/repoA/runs", nil)
	wRunsA := httptest.NewRecorder()
	server.ServeHTTP(wRunsA, reqRunsA)
	if wRunsA.Code != http.StatusOK {
		t.Fatalf("GET /github.com/orgA/repoA/runs failed: %d", wRunsA.Code)
	}
	bodyRunsA := wRunsA.Body.String()
	if !strings.Contains(bodyRunsA, "run-a") || strings.Contains(bodyRunsA, "run-b") {
		t.Errorf("expected repoA runs to contain run-a and not run-b, got: %s", bodyRunsA)
	}

	// Repo B runs should contain run-b and not run-a
	reqRunsB := httptest.NewRequest(http.MethodGet, "/github.com/orgB/repoB/runs", nil)
	wRunsB := httptest.NewRecorder()
	server.ServeHTTP(wRunsB, reqRunsB)
	if wRunsB.Code != http.StatusOK {
		t.Fatalf("GET /github.com/orgB/repoB/runs failed: %d", wRunsB.Code)
	}
	bodyRunsB := wRunsB.Body.String()
	if !strings.Contains(bodyRunsB, "run-b") || strings.Contains(bodyRunsB, "run-a") {
		t.Errorf("expected repoB runs to contain run-b and not run-a, got: %s", bodyRunsB)
	}

	// Repo A comparisons should contain comp-a and not comp-b
	reqCompsA := httptest.NewRequest(http.MethodGet, "/github.com/orgA/repoA/comparisons", nil)
	wCompsA := httptest.NewRecorder()
	server.ServeHTTP(wCompsA, reqCompsA)
	if wCompsA.Code != http.StatusOK {
		t.Fatalf("GET /github.com/orgA/repoA/comparisons failed: %d", wCompsA.Code)
	}
	bodyCompsA := wCompsA.Body.String()
	if !strings.Contains(bodyCompsA, "comp-a") || strings.Contains(bodyCompsA, "comp-b") {
		t.Errorf("expected repoA comparisons to contain comp-a and not comp-b, got: %s", bodyCompsA)
	}
}

func TestServerRepoCrossRepo404(t *testing.T) {
	scheme := newTestScheme(t)

	runA := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-a",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "https://github.com/orgA/repoA.git",
			},
		},
	}
	compA := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "comp-a",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "orgA/repoA",
			},
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "run-a",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runA, compA).Build()
	server := NewServer(Config{Client: fakeClient})

	tests := []struct {
		name string
		path string
	}{
		{
			name: "run detail from wrong repo",
			path: "/github.com/orgB/repoB/runs/run-a",
		},
		{
			name: "run task from wrong repo",
			path: "/github.com/orgB/repoB/runs/run-a/tasks/test-task/samples",
		},
		{
			name: "comparison detail from wrong repo",
			path: "/github.com/orgB/repoB/comparisons/comp-a",
		},
		{
			name: "host without dot",
			path: "/localhost/orgA/repoA/runs",
		},
		{
			name: "dot-dot in owner",
			path: "/github.com/%2e%2e/repoA/runs",
		},
		{
			name: "dot in repo name",
			path: "/github.com/orgA/%2e/runs",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)

			if w.Code != http.StatusNotFound {
				t.Fatalf("GET %s got status %d, want 404 Not Found", tc.path, w.Code)
			}
		})
	}
}

func TestServerRepoNamespacePropagation(t *testing.T) {
	scheme := newTestScheme(t)

	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-run",
			Namespace: "staging",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "https://github.com/gke-labs/eval-runner.git",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()
	server := NewServer(Config{Client: fakeClient})

	runDefault := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "https://github.com/gke-labs/eval-runner.git",
			},
		},
	}
	serverDefault := NewServer(Config{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(runDefault).Build()})

	pages := []string{
		"/",
		"/runs",
		"/comparisons",
		"/pulls",
		"/series",
		"/github.com/gke-labs/eval-runner/runs",
		"/github.com/gke-labs/eval-runner/comparisons",
		"/github.com/gke-labs/eval-runner/pulls",
	}

	for _, page := range pages {
		reqCustom := httptest.NewRequest(http.MethodGet, page+"?namespace=staging", nil)
		wCustom := httptest.NewRecorder()
		server.ServeHTTP(wCustom, reqCustom)
		if wCustom.Code != http.StatusOK {
			t.Fatalf("%s: GET with custom ns failed: %d", page, wCustom.Code)
		}
		bodyCustom := wCustom.Body.String()
		if n := strings.Count(bodyCustom, "namespace: staging"); n != 1 {
			t.Errorf("%s: expected badge with 'namespace: staging' exactly once, got %d", page, n)
		}
		if n := strings.Count(bodyCustom, "namespace:"); n != 1 {
			t.Errorf("%s: expected 'namespace:' to appear exactly once (in badge), got %d", page, n)
		}
		if strings.Contains(bodyCustom, "Namespace</th>") {
			t.Errorf("%s: expected custom ns page to contain no 'Namespace</th>', got: %s", page, bodyCustom)
		}

		reqDefault := httptest.NewRequest(http.MethodGet, page, nil)
		wDefault := httptest.NewRecorder()
		serverDefault.ServeHTTP(wDefault, reqDefault)
		if wDefault.Code != http.StatusOK {
			t.Fatalf("%s: GET with default ns failed: %d", page, wDefault.Code)
		}
		bodyDefault := wDefault.Body.String()
		if strings.Contains(bodyDefault, "namespace:") {
			t.Errorf("%s: expected default ns page to contain no 'namespace:' text, got: %s", page, bodyDefault)
		}
		if strings.Contains(bodyDefault, "Namespace</th>") {
			t.Errorf("%s: expected default ns page to contain no 'Namespace</th>', got: %s", page, bodyDefault)
		}

		if page == "/github.com/gke-labs/eval-runner/runs" {
			if !strings.Contains(bodyCustom, "?namespace=staging") {
				t.Errorf("%s: expected links in custom ns to contain '?namespace=staging', got: %s", page, bodyCustom)
			}
			if strings.Contains(bodyDefault, "?namespace=") {
				t.Errorf("%s: expected links in default ns to NOT contain '?namespace=', got: %s", page, bodyDefault)
			}
		}
	}

	// Non-default Config.DefaultNamespace test:
	// When server's default namespace is "other", accessing without ?namespace= uses "other",
	// so links must NOT carry ?namespace=. Accessing with ?namespace=eval-runner MUST carry ?namespace=eval-runner.
	runOther := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-run",
			Namespace: "other",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "https://github.com/gke-labs/eval-runner.git",
			},
		},
	}
	serverOther := NewServer(Config{
		Client:           fake.NewClientBuilder().WithScheme(scheme).WithObjects(runOther, runDefault).Build(),
		DefaultNamespace: "other",
	})
	reqOther := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs", nil)
	wOther := httptest.NewRecorder()
	serverOther.ServeHTTP(wOther, reqOther)
	if wOther.Code != http.StatusOK {
		t.Fatalf("GET /runs with Config.DefaultNamespace=other failed: %d", wOther.Code)
	}
	bodyOther := wOther.Body.String()
	if strings.Contains(bodyOther, "?namespace=") {
		t.Errorf("expected links in server default ns (other) to NOT contain '?namespace=', got: %s", bodyOther)
	}

	reqOtherExplicit := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs?namespace=eval-runner", nil)
	wOtherExplicit := httptest.NewRecorder()
	serverOther.ServeHTTP(wOtherExplicit, reqOtherExplicit)
	if wOtherExplicit.Code != http.StatusOK {
		t.Fatalf("GET /runs?namespace=eval-runner with Config.DefaultNamespace=other failed: %d", wOtherExplicit.Code)
	}
	bodyOtherExplicit := wOtherExplicit.Body.String()
	if !strings.Contains(bodyOtherExplicit, "?namespace=eval-runner") {
		t.Errorf("expected links to contain '?namespace=eval-runner' when differing from Config.DefaultNamespace=other, got: %s", bodyOtherExplicit)
	}

	// Metric filter chips on run detail page retain ?namespace=staging
	reqRunDetail := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/custom-run?namespace=staging", nil)
	wRunDetail := httptest.NewRecorder()
	server.ServeHTTP(wRunDetail, reqRunDetail)
	if wRunDetail.Code != http.StatusOK {
		t.Fatalf("GET /runs/custom-run?namespace=staging failed: %d", wRunDetail.Code)
	}
	bodyRunDetail := wRunDetail.Body.String()
	for _, m := range []string{"all", "rate", "latency", "tokens", "cost"} {
		wantSubstr := fmt.Sprintf(`href="?metric=%s&amp;namespace=staging"`, m)
		badSubstr := fmt.Sprintf(`href="?metric=%s"`, m)
		if strings.Contains(bodyRunDetail, badSubstr) {
			t.Errorf("run detail rendered metric chip without namespace: %s", badSubstr)
		}
		if !strings.Contains(bodyRunDetail, wantSubstr) {
			t.Errorf("run detail missing metric chip with namespace: %s", wantSubstr)
		}
	}

	// Metric filter chips on comparison detail page retain ?namespace=staging
	compStaging := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-comp",
			Namespace: "staging",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
			},
		},
	}
	serverStagingComp := NewServer(Config{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(compStaging).Build()})
	reqCompDetail := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/comparisons/custom-comp?namespace=staging", nil)
	wCompDetail := httptest.NewRecorder()
	serverStagingComp.ServeHTTP(wCompDetail, reqCompDetail)
	if wCompDetail.Code != http.StatusOK {
		t.Fatalf("GET /comparisons/custom-comp?namespace=staging failed: %d", wCompDetail.Code)
	}
	bodyCompDetail := wCompDetail.Body.String()
	for _, m := range []string{"all", "rate", "latency", "tokens", "cost"} {
		wantSubstr := fmt.Sprintf(`href="?metric=%s&amp;namespace=staging"`, m)
		badSubstr := fmt.Sprintf(`href="?metric=%s"`, m)
		if strings.Contains(bodyCompDetail, badSubstr) {
			t.Errorf("comparison detail rendered metric chip without namespace: %s", badSubstr)
		}
		if !strings.Contains(bodyCompDetail, wantSubstr) {
			t.Errorf("comparison detail missing metric chip with namespace: %s", wantSubstr)
		}
	}
}

func TestServerRepoComparisonDetailNoRunsHref(t *testing.T) {
	scheme := newTestScheme(t)
	baseRun, candRun, comp := sampleFixtures()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(baseRun, candRun, comp).Build()
	server := NewServer(Config{Client: fakeClient})

	req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/comparisons/docs-eval-same-commit", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET comparison detail failed: %d", w.Code)
	}

	body := w.Body.String()

	// Verify status.tasks are rendered
	if !strings.Contains(body, "triage/runbook_lookup") {
		t.Fatalf("expected task triage/runbook_lookup to be rendered in body")
	}

	// Crucial assertion: Must contain NO href="/runs/
	if strings.Contains(body, `href="/runs/`) {
		t.Errorf("repository-scoped comparison detail page contains legacy href=\"/runs/:\n%s", body)
	}

	// Must contain repository-scoped run hrefs
	if !strings.Contains(body, `href="/github.com/gke-labs/eval-runner/runs/`) {
		t.Errorf("expected repository-scoped comparison detail page to contain href=\"/github.com/gke-labs/eval-runner/runs/:\n%s", body)
	}
}

func TestServerRepoPullsUnit(t *testing.T) {
	scheme := newTestScheme(t)
	now := time.Now().Truncate(time.Second)

	// Repo A objects (with mixed-case repository string in Comparison to test casing normalization)
	runA := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "run-a-101",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Minute)),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "https://github.com/org-a/repo-a.git",
			},
		},
	}
	runABase := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "base-a",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-30 * time.Minute)),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "https://github.com/org-a/repo-a.git",
			},
		},
	}
	compA101 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-a-101",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-8 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-a"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "run-a-101"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "Org-A/Repo-A",
				Number:     101,
				HeadSHA:    "aaaa101",
				BaseSHA:    "aaaa000",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictImproved,
		},
	}

	// Fork PR for Repo A: Candidate Run comes from a fork repository
	runFork := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "run-fork-102",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-6 * time.Minute)),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "https://github.com/fork-user/repo-a.git",
			},
		},
	}
	compAFork102 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-a-102",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-a"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "run-fork-102"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "org-a/repo-a",
				Number:     102,
				HeadSHA:    "fork102",
				BaseSHA:    "aaaa000",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictInconclusive,
		},
	}

	compA103 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-a-103",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-4 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-a"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "run-a-101"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "org-a/repo-a",
				Number:     103,
				HeadSHA:    "aaaa103",
				BaseSHA:    "aaaa000",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictRegressed,
		},
	}

	// Repo B objects (same PR number 101)
	runB := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "run-b-101",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-7 * time.Minute)),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "https://github.com/org-b/repo-b.git",
			},
		},
	}
	compB101 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-b-101",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-7 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-a"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "run-b-101"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "org-b/repo-b",
				Number:     101,
				HeadSHA:    "bbbb101",
				BaseSHA:    "bbbb000",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictInconclusive,
		},
	}

	// Custom namespace comparison
	compCustomNS := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-custom-101",
			Namespace:         "custom-ns",
			CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Minute)),
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-a"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "run-a-101"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "org-a/repo-a",
				Number:     101,
				HeadSHA:    "aaaa101",
				BaseSHA:    "aaaa000",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictImproved,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(runA, runABase, runFork, runB, compA101, compAFork102, compA103, compB101, compCustomNS).
		Build()

	server := NewServer(Config{
		Client:           cl,
		DefaultNamespace: "eval-runner",
	})

	// 1. Two repos with the same PR number -> only the path repo's PRs listed
	t.Run("TwoReposSamePRNumber_ListFilter", func(t *testing.T) {
		reqA := httptest.NewRequest(http.MethodGet, "/github.com/org-a/repo-a/pulls", nil)
		wA := httptest.NewRecorder()
		server.ServeHTTP(wA, reqA)

		if wA.Code != http.StatusOK {
			t.Fatalf("GET org-a/repo-a/pulls returned code %d, want 200", wA.Code)
		}
		bodyA := wA.Body.String()
		if !strings.Contains(bodyA, "comp-a-101") {
			t.Errorf("expected comp-a-101 in org-a pulls")
		}
		if strings.Contains(bodyA, "comp-b-101") {
			t.Errorf("unexpected comp-b-101 in org-a pulls")
		}

		reqB := httptest.NewRequest(http.MethodGet, "/github.com/org-b/repo-b/pulls", nil)
		wB := httptest.NewRecorder()
		server.ServeHTTP(wB, reqB)

		if wB.Code != http.StatusOK {
			t.Fatalf("GET org-b/repo-b/pulls returned code %d, want 200", wB.Code)
		}
		bodyB := wB.Body.String()
		if !strings.Contains(bodyB, "comp-b-101") {
			t.Errorf("expected comp-b-101 in org-b pulls")
		}
		if strings.Contains(bodyB, "comp-a-101") {
			t.Errorf("unexpected comp-a-101 in org-b pulls")
		}
	})

	t.Run("TwoReposSamePRNumber_DetailFilter", func(t *testing.T) {
		reqA := httptest.NewRequest(http.MethodGet, "/github.com/org-a/repo-a/pulls/101", nil)
		wA := httptest.NewRecorder()
		server.ServeHTTP(wA, reqA)

		if wA.Code != http.StatusOK {
			t.Fatalf("GET org-a/repo-a/pulls/101 returned code %d, want 200", wA.Code)
		}
		bodyA := wA.Body.String()
		if !strings.Contains(bodyA, "comp-a-101") {
			t.Errorf("expected comp-a-101 in org-a PR 101 detail")
		}
		if strings.Contains(bodyA, "comp-b-101") {
			t.Errorf("unexpected comp-b-101 in org-a PR 101 detail")
		}

		reqB := httptest.NewRequest(http.MethodGet, "/github.com/org-b/repo-b/pulls/101", nil)
		wB := httptest.NewRecorder()
		server.ServeHTTP(wB, reqB)

		if wB.Code != http.StatusOK {
			t.Fatalf("GET org-b/repo-b/pulls/101 returned code %d, want 200", wB.Code)
		}
		bodyB := wB.Body.String()
		if !strings.Contains(bodyB, "comp-b-101") {
			t.Errorf("expected comp-b-101 in org-b PR 101 detail")
		}
		if strings.Contains(bodyB, "comp-a-101") {
			t.Errorf("unexpected comp-a-101 in org-b PR 101 detail")
		}
	})

	// 2. A fork candidate Run does not create a second repo entry
	t.Run("ForkCandidateRunDoesNotCreateSecondRepoEntry", func(t *testing.T) {
		allComps := []v1alpha1.Comparison{*compA101, *compAFork102, *compA103, *compB101}
		groups := groupPullRequests(allComps, nil)

		for _, grp := range groups {
			if strings.Contains(grp.repository, "fork-user") {
				t.Fatalf("unexpected group with repository %q from candidate run", grp.repository)
			}
		}

		reqPulls := httptest.NewRequest(http.MethodGet, "/pulls", nil)
		wPulls := httptest.NewRecorder()
		server.ServeHTTP(wPulls, reqPulls)
		if strings.Contains(wPulls.Body.String(), "fork-user") {
			t.Errorf("unexpected fork-user in /pulls list view")
		}

		// GET /github.com/fork-user/repo-a/pulls should show empty state
		reqFork := httptest.NewRequest(http.MethodGet, "/github.com/fork-user/repo-a/pulls", nil)
		wFork := httptest.NewRecorder()
		server.ServeHTTP(wFork, reqFork)
		if wFork.Code != http.StatusOK {
			t.Fatalf("GET fork pulls returned code %d, want 200", wFork.Code)
		}
		if !strings.Contains(wFork.Body.String(), "No pull requests found") {
			t.Errorf("expected empty state in fork repo pulls")
		}
	})

	// 3. ?offset/limit pagination
	t.Run("OffsetLimitPagination", func(t *testing.T) {
		// org-a has 3 PRs: 101, 102, 103
		reqPage1 := httptest.NewRequest(http.MethodGet, "/github.com/org-a/repo-a/pulls?limit=2&offset=0", nil)
		wPage1 := httptest.NewRecorder()
		server.ServeHTTP(wPage1, reqPage1)

		if wPage1.Code != http.StatusOK {
			t.Fatalf("GET page 1 returned code %d, want 200", wPage1.Code)
		}
		bodyPage1 := wPage1.Body.String()
		if !strings.Contains(bodyPage1, "Showing 1–2 of 3") {
			t.Errorf("expected 'Showing 1–2 of 3' on page 1, got:\n%s", bodyPage1)
		}
		if !strings.Contains(bodyPage1, "More →") {
			t.Errorf("expected 'More →' link on page 1")
		}

		reqPage2 := httptest.NewRequest(http.MethodGet, "/github.com/org-a/repo-a/pulls?limit=2&offset=2", nil)
		wPage2 := httptest.NewRecorder()
		server.ServeHTTP(wPage2, reqPage2)

		if wPage2.Code != http.StatusOK {
			t.Fatalf("GET page 2 returned code %d, want 200", wPage2.Code)
		}
		bodyPage2 := wPage2.Body.String()
		if !strings.Contains(bodyPage2, "Showing 3–3 of 3") {
			t.Errorf("expected 'Showing 3–3 of 3' on page 2, got:\n%s", bodyPage2)
		}
		if !strings.Contains(bodyPage2, "← Previous") {
			t.Errorf("expected '← Previous' link on page 2")
		}
	})

	// 4. 404 for a PR number absent in that repo and non-github host
	t.Run("PRNumberAbsent404", func(t *testing.T) {
		// Absent PR number in repo-a
		req404 := httptest.NewRequest(http.MethodGet, "/github.com/org-a/repo-a/pulls/999", nil)
		w404 := httptest.NewRecorder()
		server.ServeHTTP(w404, req404)

		if w404.Code != http.StatusNotFound {
			t.Errorf("GET absent PR 999 returned code %d, want 404", w404.Code)
		}

		// Non-github host detail returns 404
		reqNonGitHub := httptest.NewRequest(http.MethodGet, "/gitlab.com/org-a/repo-a/pulls/101", nil)
		wNonGitHub := httptest.NewRecorder()
		server.ServeHTTP(wNonGitHub, reqNonGitHub)

		if wNonGitHub.Code != http.StatusNotFound {
			t.Errorf("GET non-github pulls detail returned code %d, want 404", wNonGitHub.Code)
		}

		// Non-github host list returns 200 with "No pull requests found" (Nit 3)
		reqNonGitHubList := httptest.NewRequest(http.MethodGet, "/gitlab.com/org-a/repo-a/pulls", nil)
		wNonGitHubList := httptest.NewRecorder()
		server.ServeHTTP(wNonGitHubList, reqNonGitHubList)

		if wNonGitHubList.Code != http.StatusOK {
			t.Fatalf("GET non-github pulls list returned code %d, want 200", wNonGitHubList.Code)
		}
		if !strings.Contains(wNonGitHubList.Body.String(), "No pull requests found") {
			t.Errorf("expected 'No pull requests found' on non-github pulls list")
		}
	})

	// 5. ?namespace= propagation
	t.Run("NamespacePropagation", func(t *testing.T) {
		reqList := httptest.NewRequest(http.MethodGet, "/github.com/org-a/repo-a/pulls?namespace=custom-ns", nil)
		wList := httptest.NewRecorder()
		server.ServeHTTP(wList, reqList)

		if wList.Code != http.StatusOK {
			t.Fatalf("GET custom-ns pulls list returned code %d, want 200", wList.Code)
		}
		bodyList := wList.Body.String()
		if !strings.Contains(bodyList, "comp-custom-101") {
			t.Errorf("expected comp-custom-101 in custom-ns pulls list")
		}
		if !strings.Contains(bodyList, "?namespace=custom-ns") {
			t.Errorf("expected ?namespace=custom-ns propagation in pulls list")
		}

		reqDetail := httptest.NewRequest(http.MethodGet, "/github.com/org-a/repo-a/pulls/101?namespace=custom-ns", nil)
		wDetail := httptest.NewRecorder()
		server.ServeHTTP(wDetail, reqDetail)

		if wDetail.Code != http.StatusOK {
			t.Fatalf("GET custom-ns pull detail returned code %d, want 200", wDetail.Code)
		}
		bodyDetail := wDetail.Body.String()
		if !strings.Contains(bodyDetail, "comp-custom-101") {
			t.Errorf("expected comp-custom-101 in custom-ns pull detail")
		}
		if !strings.Contains(bodyDetail, "?namespace=custom-ns") {
			t.Errorf("expected ?namespace=custom-ns propagation in pull detail")
		}
	})

	// 6. Mixed-case repo casing in heading (Nit 4)
	t.Run("MixedCaseHeadingNormalization", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/org-a/repo-a/pulls/101", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("GET /github.com/org-a/repo-a/pulls/101 returned code %d, want 200", w.Code)
		}
		body := w.Body.String()
		// Heading should use the filter's normalized lower-case path, not raw "Org-A/Repo-A"
		if strings.Contains(body, "Org-A/Repo-A") {
			t.Errorf("expected heading to use filter casing org-a/repo-a, but found Org-A/Repo-A")
		}
		if !strings.Contains(body, "org-a/repo-a") {
			t.Errorf("expected heading to contain org-a/repo-a")
		}
	})

	// 7. Non-numeric, zero, and negative pull numbers answer 404 not found
	t.Run("InvalidPullNumber_404", func(t *testing.T) {
		for _, bad := range []string{"abc", "0", "-1"} {
			req := httptest.NewRequest(http.MethodGet, "/github.com/org-a/repo-a/pulls/"+bad, nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)

			if w.Code != http.StatusNotFound {
				t.Fatalf("GET /github.com/org-a/repo-a/pulls/%s returned code %d, want 404", bad, w.Code)
			}
			if strings.TrimSpace(w.Body.String()) != "not found" {
				t.Errorf("expected 'not found' message, got %q", w.Body.String())
			}
		}
	})
}

func TestServerPullsStateFilter(t *testing.T) {
	scheme := newTestScheme(t)
	now := time.Now().Truncate(time.Second)

	compOpen := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-pr-1",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
			Labels: map[string]string{
				consumer.LabelPR:      "1",
				consumer.LabelRepo:    "org-a_repo-a",
				consumer.LabelPRState: consumer.StateOpen,
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "org-a/repo-a",
				Number:     1,
				HeadSHA:    "1111111111111111111111111111111111111111",
			},
		},
	}

	compUnlabeled := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-pr-2",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Minute)),
			Labels: map[string]string{
				consumer.LabelPR:   "2",
				consumer.LabelRepo: "org-a_repo-a",
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "org-a/repo-a",
				Number:     2,
				HeadSHA:    "2222222222222222222222222222222222222222",
			},
		},
	}

	compMerged := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-pr-3",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-3 * time.Minute)),
			Labels: map[string]string{
				consumer.LabelPR:      "3",
				consumer.LabelRepo:    "org-a_repo-a",
				consumer.LabelPRState: consumer.StateMerged,
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "org-a/repo-a",
				Number:     3,
				HeadSHA:    "3333333333333333333333333333333333333333",
			},
		},
	}

	compClosed := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-pr-4",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-4 * time.Minute)),
			Labels: map[string]string{
				consumer.LabelPR:      "4",
				consumer.LabelRepo:    "org-a_repo-a",
				consumer.LabelPRState: consumer.StateClosed,
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "org-a/repo-a",
				Number:     4,
				HeadSHA:    "4444444444444444444444444444444444444444",
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(compOpen, compUnlabeled, compMerged, compClosed).Build()
	server := NewServer(Config{Client: cl, DefaultNamespace: "eval-runner"})

	endpoints := []string{
		"/github.com/org-a/repo-a/pulls",
		"/pulls",
	}

	for _, ep := range endpoints {
		t.Run("Default_"+ep, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ep, nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 OK, got %d", w.Code)
			}
			body := w.Body.String()

			// All 4 PRs present
			for _, pr := range []string{">#1<", ">#2<", ">#3<", ">#4<"} {
				if !strings.Contains(body, pr) {
					t.Errorf("expected %s in body", pr)
				}
			}

			// Filter links: All active, Open 2, Closed 2
			if !strings.Contains(body, `Open 2`) {
				t.Errorf("expected 'Open 2' in header")
			}
			if !strings.Contains(body, `Closed 2`) {
				t.Errorf("expected 'Closed 2' in header")
			}
			if !strings.Contains(body, `state-filter active">All<`) {
				t.Errorf("expected All filter to be active by default")
			}

			// State chips on rows
			if !strings.Contains(body, `<span class="chip open"><span class="dot"></span>Open</span>`) {
				t.Errorf("expected Open state chip")
			}
			if !strings.Contains(body, `<span class="chip merged"><span class="dot"></span>Merged</span>`) {
				t.Errorf("expected Merged state chip")
			}
			if !strings.Contains(body, `<span class="chip closed"><span class="dot"></span>Closed</span>`) {
				t.Errorf("expected Closed state chip")
			}
		})

		t.Run("StateOpen_"+ep, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ep+"?state=open", nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 OK, got %d", w.Code)
			}
			body := w.Body.String()

			// PR 1 and 2 present, 3 and 4 excluded
			if !strings.Contains(body, ">#1<") || !strings.Contains(body, ">#2<") {
				t.Errorf("expected PR 1 and 2 in open filter")
			}
			if strings.Contains(body, ">#3<") || strings.Contains(body, ">#4<") {
				t.Errorf("did not expect PR 3 or 4 in open filter")
			}

			// Open filter active
			if !strings.Contains(body, `state-filter active">Open 2<`) {
				t.Errorf("expected Open 2 filter to be active")
			}
		})

		t.Run("StateClosed_"+ep, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ep+"?state=closed", nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 OK, got %d", w.Code)
			}
			body := w.Body.String()

			// PR 3 (merged) and PR 4 (closed) present, 1 and 2 excluded
			if !strings.Contains(body, ">#3<") || !strings.Contains(body, ">#4<") {
				t.Errorf("expected PR 3 and 4 in closed filter")
			}
			if strings.Contains(body, ">#1<") || strings.Contains(body, ">#2<") {
				t.Errorf("did not expect PR 1 or 2 in closed filter")
			}

			// Closed filter active (includes merged)
			if !strings.Contains(body, `state-filter active">Closed 2<`) {
				t.Errorf("expected Closed 2 filter to be active")
			}
		})

		t.Run("StateAll_"+ep, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ep+"?state=all", nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 OK, got %d", w.Code)
			}
			body := w.Body.String()

			for _, pr := range []string{">#1<", ">#2<", ">#3<", ">#4<"} {
				if !strings.Contains(body, pr) {
					t.Errorf("expected %s in all filter", pr)
				}
			}
			if !strings.Contains(body, `state-filter active">All<`) {
				t.Errorf("expected All filter to be active")
			}
		})

		t.Run("InvalidState_"+ep, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ep+"?state=unknown", nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 OK, got %d", w.Code)
			}
			body := w.Body.String()

			// Invalid values behave as all
			for _, pr := range []string{">#1<", ">#2<", ">#3<", ">#4<"} {
				if !strings.Contains(body, pr) {
					t.Errorf("expected %s when state is invalid", pr)
				}
			}
			if !strings.Contains(body, `state-filter active">All<`) {
				t.Errorf("expected All filter to be active for invalid state")
			}
		})

		t.Run("PaginationPreservesState_"+ep, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, ep+"?state=open&limit=1&offset=0", nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("expected 200 OK, got %d", w.Code)
			}
			body := w.Body.String()

			if !strings.Contains(body, "More →") {
				t.Fatalf("expected More → link on page 1")
			}
			if !strings.Contains(body, "state=open") {
				t.Errorf("expected More link to preserve state=open: %s", body)
			}

			// Follow to page 2
			reqP2 := httptest.NewRequest(http.MethodGet, ep+"?state=open&limit=1&offset=1", nil)
			wP2 := httptest.NewRecorder()
			server.ServeHTTP(wP2, reqP2)
			if wP2.Code != http.StatusOK {
				t.Fatalf("expected 200 OK, got %d", wP2.Code)
			}
			bodyP2 := wP2.Body.String()
			if !strings.Contains(bodyP2, "← Previous") {
				t.Fatalf("expected ← Previous link on page 2")
			}
			if !strings.Contains(bodyP2, "state=open") {
				t.Errorf("expected Previous link to preserve state=open: %s", bodyP2)
			}
		})
	}
}

func TestServerRepoSeries(t *testing.T) {
	scheme := newTestScheme(t)
	rate := 0.8
	ci := &v1alpha1.ConfidenceInterval{Lower: 0.6, Upper: 0.9, Confidence: 0.95, Method: "wilson"}

	runMain := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "base-main-1",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)),
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/branch": "main",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "111111111111",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:            "triage/lookup",
					Phase:         v1alpha1.TaskPhaseCompleted,
					Samples:       10,
					SuccessRate:   &rate,
					SuccessRateCI: ci,
				},
			},
		},
	}

	branchSlash := "feature/branch-1"
	runBranchSlash := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "base-branch-1",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(time.Date(2026, 10, 20, 11, 0, 0, 0, time.UTC)),
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/branch": sanitizeBranchLabel(branchSlash),
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "222222222222",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:            "triage/lookup",
					Phase:         v1alpha1.TaskPhaseCompleted,
					Samples:       10,
					SuccessRate:   &rate,
					SuccessRateCI: ci,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runMain, runBranchSlash).Build()
	server := NewServer(Config{Client: fakeClient})

	t.Run("DefaultMainBranch_NoBranchInPath", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for /series default main, got %d: %s", w.Code, w.Body.String())
		}
		body := w.Body.String()
		if !strings.Contains(body, "(main)") {
			t.Errorf("expected (main) in card title, body: %s", body)
		}
		if !strings.Contains(body, `class="nav-link active">Series</a>`) {
			t.Errorf("expected Series tab active, body: %s", body)
		}
		if !strings.Contains(body, "/github.com/gke-labs/eval-runner/runs/base-main-1") {
			t.Errorf("expected repo-scoped run link in body")
		}
	})

	t.Run("ExplicitMainBranch", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/main", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for /series/main, got %d: %s", w.Code, w.Body.String())
		}
		body := w.Body.String()
		if !strings.Contains(body, "(main)") {
			t.Errorf("expected (main) in card title, body: %s", body)
		}
		if !strings.Contains(body, `class="nav-link active">Series</a>`) {
			t.Errorf("expected Series tab active, body: %s", body)
		}
	})

	t.Run("BranchWithSlash", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/series/feature/branch-1", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for branch with slash, got %d: %s", w.Code, w.Body.String())
		}
		body := w.Body.String()
		if !strings.Contains(body, "(feature/branch-1)") {
			t.Errorf("expected (feature/branch-1) in card title, body: %s", body)
		}
		if !strings.Contains(body, `class="nav-link active">Series</a>`) {
			t.Errorf("expected Series tab active, body: %s", body)
		}
		if !strings.Contains(body, "/github.com/gke-labs/eval-runner/runs/base-branch-1") {
			t.Errorf("expected repo-scoped run link in body")
		}
	})

	t.Run("UnknownRepository_404FixedString", func(t *testing.T) {
		for _, path := range []string{
			"/github.com/nonexistent/repo/series",
			"/github.com/nonexistent/repo/series/main",
		} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)

			if w.Code != http.StatusNotFound {
				t.Fatalf("expected 404 for unknown repository %s, got %d", path, w.Code)
			}
			if strings.TrimSpace(w.Body.String()) != "series not found" {
				t.Errorf("expected fixed string 'series not found', got %q", w.Body.String())
			}
		}
	})
}

func TestServerLegacyRedirects(t *testing.T) {
	scheme := newTestScheme(t)
	rate := 0.75
	ci := &v1alpha1.ConfidenceInterval{Lower: 0.5, Upper: 0.9, Confidence: 0.95, Method: "wilson"}

	runDefault := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "eval-runner",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/branch": "main",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "abcdef123456",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:            "triage/lookup",
					Phase:         v1alpha1.TaskPhaseCompleted,
					Samples:       5,
					SuccessRate:   &rate,
					SuccessRateCI: ci,
				},
			},
		},
	}

	runCustom := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "custom-ns",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/branch": "main",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "abcdef123456",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:            "triage/lookup",
					Phase:         v1alpha1.TaskPhaseCompleted,
					Samples:       5,
					SuccessRate:   &rate,
					SuccessRateCI: ci,
				},
			},
		},
	}

	baseBranchRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-run-branch",
			Namespace: "eval-runner",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/branch": sanitizeBranchLabel("feature/branch-1"),
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "111122223333",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: "triage/lookup", Phase: v1alpha1.TaskPhaseCompleted, SuccessRate: &rate, SuccessRateCI: ci},
			},
		},
	}

	compDefault := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-comp",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     101,
			},
		},
	}

	compCustom := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-comp",
			Namespace: "custom-ns",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     101,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(runDefault, runCustom, baseBranchRun, compDefault, compCustom).
		Build()

	server := NewServer(Config{
		Client:           fakeClient,
		DefaultNamespace: "eval-runner",
	})

	hash := RepoHash("gke-labs/eval-runner")

	tests := []struct {
		name         string
		url          string
		wantLocation string
	}{
		// /runs/{ns}/{name}
		{
			name:         "run_default_no_query",
			url:          "/runs/eval-runner/test-run",
			wantLocation: "/github.com/gke-labs/eval-runner/runs/test-run",
		},
		{
			name:         "run_default_with_metric",
			url:          "/runs/eval-runner/test-run?metric=rate",
			wantLocation: "/github.com/gke-labs/eval-runner/runs/test-run?metric=rate",
		},
		{
			name:         "run_custom_ns",
			url:          "/runs/custom-ns/test-run",
			wantLocation: "/github.com/gke-labs/eval-runner/runs/test-run?namespace=custom-ns",
		},
		{
			name:         "run_custom_ns_with_metric",
			url:          "/runs/custom-ns/test-run?metric=latency",
			wantLocation: "/github.com/gke-labs/eval-runner/runs/test-run?metric=latency&namespace=custom-ns",
		},

		// /runs/{ns}/{name}/tasks
		{
			name:         "run_tasks_default_no_query",
			url:          "/runs/eval-runner/test-run/tasks",
			wantLocation: "/github.com/gke-labs/eval-runner/runs/test-run/tasks",
		},
		{
			name:         "run_tasks_default_with_metric",
			url:          "/runs/eval-runner/test-run/tasks?metric=rate",
			wantLocation: "/github.com/gke-labs/eval-runner/runs/test-run/tasks?metric=rate",
		},
		{
			name:         "run_tasks_custom_ns",
			url:          "/runs/custom-ns/test-run/tasks",
			wantLocation: "/github.com/gke-labs/eval-runner/runs/test-run/tasks?namespace=custom-ns",
		},
		{
			name:         "run_tasks_custom_ns_with_metric",
			url:          "/runs/custom-ns/test-run/tasks?metric=tokens",
			wantLocation: "/github.com/gke-labs/eval-runner/runs/test-run/tasks?metric=tokens&namespace=custom-ns",
		},

		// /runs/{ns}/{name}/tasks/{rest...}
		{
			name:         "run_tasks_rest_default_no_query",
			url:          "/runs/eval-runner/test-run/tasks/triage%2Flookup/samples",
			wantLocation: "/github.com/gke-labs/eval-runner/runs/test-run/tasks/triage%2Flookup/samples",
		},
		{
			name:         "run_tasks_rest_default_with_metric",
			url:          "/runs/eval-runner/test-run/tasks/triage%2Flookup/samples?metric=cost",
			wantLocation: "/github.com/gke-labs/eval-runner/runs/test-run/tasks/triage%2Flookup/samples?metric=cost",
		},
		{
			name:         "run_tasks_rest_custom_ns",
			url:          "/runs/custom-ns/test-run/tasks/triage%2Flookup/samples",
			wantLocation: "/github.com/gke-labs/eval-runner/runs/test-run/tasks/triage%2Flookup/samples?namespace=custom-ns",
		},
		{
			name:         "run_tasks_rest_custom_ns_with_metric",
			url:          "/runs/custom-ns/test-run/tasks/triage%2Flookup/samples?metric=rate",
			wantLocation: "/github.com/gke-labs/eval-runner/runs/test-run/tasks/triage%2Flookup/samples?metric=rate&namespace=custom-ns",
		},

		// /comparisons/{ns}/{name}
		{
			name:         "comp_default_no_query",
			url:          "/comparisons/eval-runner/test-comp",
			wantLocation: "/github.com/gke-labs/eval-runner/comparisons/test-comp",
		},
		{
			name:         "comp_default_with_metric",
			url:          "/comparisons/eval-runner/test-comp?metric=latency",
			wantLocation: "/github.com/gke-labs/eval-runner/comparisons/test-comp?metric=latency",
		},
		{
			name:         "comp_custom_ns",
			url:          "/comparisons/custom-ns/test-comp",
			wantLocation: "/github.com/gke-labs/eval-runner/comparisons/test-comp?namespace=custom-ns",
		},
		{
			name:         "comp_custom_ns_with_metric",
			url:          "/comparisons/custom-ns/test-comp?metric=cost",
			wantLocation: "/github.com/gke-labs/eval-runner/comparisons/test-comp?metric=cost&namespace=custom-ns",
		},

		// /github.com/{owner}/{repo}/pull/{number} (singular alias)
		{
			name:         "pull_alias_default_no_query",
			url:          "/github.com/gke-labs/eval-runner/pull/101",
			wantLocation: "/github.com/gke-labs/eval-runner/pulls/101",
		},
		{
			name:         "pull_alias_with_metric",
			url:          "/github.com/gke-labs/eval-runner/pull/101?metric=rate",
			wantLocation: "/github.com/gke-labs/eval-runner/pulls/101?metric=rate",
		},
		{
			name:         "pull_alias_custom_ns",
			url:          "/github.com/gke-labs/eval-runner/pull/101?namespace=custom-ns",
			wantLocation: "/github.com/gke-labs/eval-runner/pulls/101?namespace=custom-ns",
		},

		// /series/{repohash}/{branch...}
		{
			name:         "series_default_no_query",
			url:          "/series/" + hash + "/main",
			wantLocation: "/github.com/gke-labs/eval-runner/series/main",
		},
		{
			name:         "series_default_with_metric",
			url:          "/series/" + hash + "/main?metric=rate",
			wantLocation: "/github.com/gke-labs/eval-runner/series/main?metric=rate",
		},
		{
			name:         "series_custom_ns",
			url:          "/series/" + hash + "/main?namespace=custom-ns",
			wantLocation: "/github.com/gke-labs/eval-runner/series/main?namespace=custom-ns",
		},
		{
			name:         "series_custom_ns_with_metric",
			url:          "/series/" + hash + "/main?metric=tokens&namespace=custom-ns",
			wantLocation: "/github.com/gke-labs/eval-runner/series/main?metric=tokens&namespace=custom-ns",
		},
		{
			name:         "series_branch_with_slash",
			url:          "/series/" + hash + "/feature/branch-1",
			wantLocation: "/github.com/gke-labs/eval-runner/series/feature/branch-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.url, nil)
			w := httptest.NewRecorder()
			server.ServeHTTP(w, req)

			if w.Code != http.StatusPermanentRedirect {
				t.Fatalf("expected 308 Permanent Redirect for %s, got %d", tc.url, w.Code)
			}
			if loc := w.Header().Get("Location"); loc != tc.wantLocation {
				t.Errorf("for %s expected Location %q, got %q", tc.url, tc.wantLocation, loc)
			}
		})
	}
}

func TestServerLegacyUnresolvableRepo(t *testing.T) {
	scheme := newTestScheme(t)

	runNoURL := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-no-url",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: "task-1", Phase: v1alpha1.TaskPhaseCompleted, LogPath: "/var/log/artifacts/inspect.json"},
			},
		},
	}

	compNoURL := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "comp-no-url",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "run-no-url"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "run-no-url"},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase: v1alpha1.ComparisonPhaseCompleted,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(runNoURL, compNoURL).
		Build()

	server := NewServer(Config{
		Client:           fakeClient,
		DefaultNamespace: "eval-runner",
	})

	t.Run("Run_NoURL_Returns200Legacy", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/run-no-url", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for unresolvable run, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "run-no-url") {
			t.Errorf("expected legacy run detail page body to contain run-no-url")
		}
	})

	t.Run("RunTasks_NoURL_Returns200Legacy", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/run-no-url/tasks/task-1/samples", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for unresolvable run tasks, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "task-1") {
			t.Errorf("expected legacy tasks page body to contain task-1")
		}
	})

	t.Run("Comparison_NoURL_Returns200Legacy", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/comp-no-url", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for unresolvable comparison, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "comp-no-url") {
			t.Errorf("expected legacy comparison page body to contain comp-no-url")
		}
	})

	t.Run("Series_HashCollidesTwoRepositories_Returns200Legacy", func(t *testing.T) {
		// org/repo3403 and org/repo15792 both hash to 6f878fa
		h1 := RepoHash("org/repo3403")
		h2 := RepoHash("org/repo15792")
		if h1 != h2 {
			t.Fatalf("expected hash collision, got %s and %s", h1, h2)
		}
		collidingHash := h1

		runRepo1 := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "base-colliding-1",
				Namespace: "eval-runner",
				Labels: map[string]string{
					"evalrunner.gke-labs.dev/branch": "main",
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    "https://github.com/org/repo3403",
					Commit: "111111111111",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: "task-1", Phase: v1alpha1.TaskPhaseCompleted},
				},
			},
		}

		runRepo2 := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "base-colliding-2",
				Namespace: "eval-runner",
				Labels: map[string]string{
					"evalrunner.gke-labs.dev/branch": "main",
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    "https://github.com/org/repo15792",
					Commit: "222222222222",
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: "task-1", Phase: v1alpha1.TaskPhaseCompleted},
				},
			},
		}

		collisionClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runRepo1, runRepo2).Build()
		collisionServer := NewServer(Config{Client: collisionClient, DefaultNamespace: "eval-runner"})

		req := httptest.NewRequest(http.MethodGet, "/series/"+collidingHash+"/main", nil)
		w := httptest.NewRecorder()
		collisionServer.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for colliding repohash, got %d", w.Code)
		}
		if loc := w.Header().Get("Location"); loc != "" {
			t.Errorf("expected no Location header on un-redirected legacy page, got %q", loc)
		}
		body := w.Body.String()
		if !strings.Contains(body, "base-colliding-1") && !strings.Contains(body, "base-colliding-2") {
			t.Errorf("expected legacy series detail page to render colliding runs, got: %s", body)
		}
	})
}

func TestServerLegacyRedirect_NonDefaultNamespacePropagation(t *testing.T) {
	scheme := newTestScheme(t)

	runCustomDefault := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-1",
			Namespace: "team-staging",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "https://github.com/gke-labs/eval-runner.git",
			},
		},
	}

	runOther := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-1",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL: "https://github.com/gke-labs/eval-runner.git",
			},
		},
	}

	compCustomDefault := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "comp-1",
			Namespace: "team-staging",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
			},
		},
	}

	compOther := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "comp-1",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(runCustomDefault, runOther, compCustomDefault, compOther).
		Build()

	server := NewServer(Config{
		Client:           fakeClient,
		DefaultNamespace: "team-staging", // non-default DefaultNamespace
	})

	// Run in default namespace (team-staging) should NOT carry ?namespace=
	req1 := httptest.NewRequest(http.MethodGet, "/runs/team-staging/run-1", nil)
	w1 := httptest.NewRecorder()
	server.ServeHTTP(w1, req1)
	if w1.Code != http.StatusPermanentRedirect {
		t.Fatalf("expected 308, got %d", w1.Code)
	}
	if loc := w1.Header().Get("Location"); loc != "/github.com/gke-labs/eval-runner/runs/run-1" {
		t.Errorf("expected no namespace param when ns matches DefaultNamespace, got Location %q", loc)
	}

	// Run in different namespace (eval-runner) MUST carry ?namespace=eval-runner
	req2 := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/run-1", nil)
	w2 := httptest.NewRecorder()
	server.ServeHTTP(w2, req2)
	if w2.Code != http.StatusPermanentRedirect {
		t.Fatalf("expected 308, got %d", w2.Code)
	}
	if loc := w2.Header().Get("Location"); loc != "/github.com/gke-labs/eval-runner/runs/run-1?namespace=eval-runner" {
		t.Errorf("expected ?namespace=eval-runner when ns != DefaultNamespace, got Location %q", loc)
	}

	// Comparison in default namespace (team-staging) should NOT carry ?namespace=
	reqComp1 := httptest.NewRequest(http.MethodGet, "/comparisons/team-staging/comp-1", nil)
	wComp1 := httptest.NewRecorder()
	server.ServeHTTP(wComp1, reqComp1)
	if wComp1.Code != http.StatusPermanentRedirect {
		t.Fatalf("expected 308, got %d", wComp1.Code)
	}
	if loc := wComp1.Header().Get("Location"); loc != "/github.com/gke-labs/eval-runner/comparisons/comp-1" {
		t.Errorf("expected no namespace param when ns matches DefaultNamespace, got Location %q", loc)
	}

	// Comparison in different namespace (eval-runner) MUST carry ?namespace=eval-runner
	reqComp2 := httptest.NewRequest(http.MethodGet, "/comparisons/eval-runner/comp-1", nil)
	wComp2 := httptest.NewRecorder()
	server.ServeHTTP(wComp2, reqComp2)
	if wComp2.Code != http.StatusPermanentRedirect {
		t.Fatalf("expected 308, got %d", wComp2.Code)
	}
	if loc := wComp2.Header().Get("Location"); loc != "/github.com/gke-labs/eval-runner/comparisons/comp-1?namespace=eval-runner" {
		t.Errorf("expected ?namespace=eval-runner when ns != DefaultNamespace, got Location %q", loc)
	}
}

func TestServerRepoRunDetail_ForkSourcedRun(t *testing.T) {
	scheme := newTestScheme(t)

	// Fork-sourced candidate run
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-391-184dfbc",
			Namespace: "eval-runner",
			Labels: map[string]string{
				consumer.LabelPR:         "391",
				consumer.LabelRepo:       "codebot-robot_eval-runner",
				consumer.LabelTargetRepo: "gke-labs_eval-runner",
				consumer.LabelHeadSHA:    "184dfbc",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/codebot-robot/eval-runner.git",
				Commit: "184dfbc",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      "docs/readme_comprehension",
					Phase:   v1alpha1.TaskPhaseCompleted,
					Samples: 8,
					LogPath: "gs://eval-bucket/runs/184dfbc/docs/readme_comprehension/inspect.json",
				},
			},
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-a81d0c4",
			Namespace: "eval-runner",
			Labels: map[string]string{
				consumer.LabelRepo:       "gke-labs_eval-runner",
				consumer.LabelTargetRepo: "gke-labs_eval-runner",
				consumer.LabelHeadSHA:    "a81d0c4",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "a81d0c4",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-391",
			Namespace: "eval-runner",
			Labels: map[string]string{
				consumer.LabelPR:      "391",
				consumer.LabelRepo:    "gke-labs_eval-runner",
				consumer.LabelHeadSHA: "184dfbc",
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     391,
				HeadSHA:    "184dfbc",
				BaseSHA:    "a81d0c4",
			},
			Candidate: v1alpha1.ComparisonRunRef{
				Run: "pr-391-184dfbc",
			},
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "base-a81d0c4",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase: v1alpha1.ComparisonPhaseCompleted,
		},
	}

	// Legacy candidate run: no target-repo label, points to fork repo URL
	legacyCandRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-pr-392-cand",
			Namespace: "eval-runner",
			Labels: map[string]string{
				consumer.LabelPR:      "392",
				consumer.LabelRepo:    "codebot-robot_eval-runner",
				consumer.LabelHeadSHA: "abcdef0",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/codebot-robot/eval-runner.git",
				Commit: "abcdef0",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
		},
	}

	legacyComp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-392",
			Namespace: "eval-runner",
			Labels: map[string]string{
				consumer.LabelPR:      "392",
				consumer.LabelRepo:    "gke-labs_eval-runner",
				consumer.LabelHeadSHA: "abcdef0",
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     392,
				HeadSHA:    "abcdef0",
				BaseSHA:    "a81d0c4",
			},
			Candidate: v1alpha1.ComparisonRunRef{
				Run: "legacy-pr-392-cand",
			},
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "base-a81d0c4",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase: v1alpha1.ComparisonPhaseCompleted,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp, legacyCandRun, legacyComp).
		Build()

	server := NewServer(Config{
		Client:           fakeClient,
		DefaultNamespace: "eval-runner",
	})

	// 1. Comparison page links candidate run under target repository path
	t.Run("comparison_page_links_fork_candidate_under_target_repo", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/comparisons/pr-391", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for comparison page, got %d: %s", w.Code, w.Body.String())
		}
		expectedRunLink := "/github.com/gke-labs/eval-runner/runs/pr-391-184dfbc"
		if !strings.Contains(w.Body.String(), expectedRunLink) {
			t.Errorf("expected comparison page to link to %q, body:\n%s", expectedRunLink, w.Body.String())
		}
	})

	// 2. Candidate Run resolves under target repository path
	t.Run("fork_candidate_resolves_under_target_repo", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/pr-391-184dfbc", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for target repo run detail, got %d: %s", w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "pr-391-184dfbc") {
			t.Errorf("expected run detail body to contain run name, body:\n%s", w.Body.String())
		}
	})

	// 3. Candidate Run task page resolves under target repository path
	t.Run("fork_candidate_tasks_resolves_under_target_repo", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/pr-391-184dfbc/tasks/docs%2Freadme_comprehension/samples", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for target repo tasks page, got %d: %s", w.Code, w.Body.String())
		}
	})

	// 4. Candidate Run listed in repo runs list
	t.Run("fork_candidate_in_repo_runs_list", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for repo runs list, got %d: %s", w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "/github.com/gke-labs/eval-runner/runs/pr-391-184dfbc") {
			t.Errorf("expected repo runs list to include pr-391-184dfbc, body:\n%s", w.Body.String())
		}
	})

	// 5. Non-goal: Fork run also renders under fork repository path
	t.Run("fork_candidate_resolves_under_fork_repo", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/codebot-robot/eval-runner/runs/pr-391-184dfbc", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for fork repo run detail, got %d: %s", w.Code, w.Body.String())
		}
	})

	// 6. Unrelated repository path returns 404
	t.Run("fork_candidate_rejects_unrelated_repo", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/other-org/other-repo/runs/pr-391-184dfbc", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404 Not Found for unrelated repo, got %d", w.Code)
		}
	})

	// 7. Legacy candidate without target-repo resolves via Comparison fallback
	t.Run("legacy_fork_candidate_resolves_via_comparison_fallback", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner/runs/legacy-pr-392-cand", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for legacy candidate run via comparison fallback, got %d: %s", w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "legacy-pr-392-cand") {
			t.Errorf("expected run detail body to contain run name, body:\n%s", w.Body.String())
		}
	})

	// 8. Legacy unscoped redirect points to target repository
	t.Run("unscoped_redirect_uses_target_repo", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/runs/eval-runner/pr-391-184dfbc", nil)
		w := httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusPermanentRedirect {
			t.Fatalf("expected 308 for unscoped run, got %d", w.Code)
		}
		if loc := w.Header().Get("Location"); loc != "/github.com/gke-labs/eval-runner/runs/pr-391-184dfbc" {
			t.Errorf("expected Location /github.com/gke-labs/eval-runner/runs/pr-391-184dfbc, got %q", loc)
		}
	})
}
