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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/forge"
	"github.com/gke-labs/eval-runner/pkg/forge/consumer"
	"github.com/gke-labs/eval-runner/pkg/forge/syncer"
	"github.com/gke-labs/eval-runner/pkg/ui"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func makeBaseRun(name, repo, branch, commit string, commitTime time.Time, topUp string, phase v1alpha1.RunPhase, tasks []v1alpha1.TaskStatus, sRange *v1alpha1.SampleRange) *v1alpha1.Run {
	labels := map[string]string{
		consumer.LabelTargetRepo: consumer.SanitizeLabelValue(repo),
		consumer.LabelRepo:       consumer.SanitizeLabelValue(repo),
		consumer.LabelBranch:     consumer.SanitizeLabelValue(branch),
		consumer.LabelHeadSHA:    commit,
	}
	if topUp != "" {
		labels[consumer.LabelTopUp] = topUp
	}
	annotations := map[string]string{
		consumer.AnnotationCommitTime: commitTime.UTC().Format(time.RFC3339),
	}
	return &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "eval-runner",
			Labels:            labels,
			Annotations:       annotations,
			CreationTimestamp: metav1.NewTime(commitTime),
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/" + repo,
				Commit: commit,
			},
			Model:       "mockllm/model",
			Cohort:      "default",
			SampleRange: sRange,
		},
		Status: v1alpha1.RunStatus{
			Phase: phase,
			Tasks: tasks,
		},
	}
}

// TestHeadTopUp_DisabledWhenIntervalZero verifies top-up does nothing if HeadTopUpInterval is 0.
func TestHeadTopUp_DisabledWhenIntervalZero(t *testing.T) {
	s := setupScheme(t)
	now := time.Now()
	run1 := makeBaseRun("base-1111111", "gke-labs/eval-runner", "main", "1111111", now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4}},
		&v1alpha1.SampleRange{Start: 1, End: 8})

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(run1).Build()
	cfg := DefaultClusterConfig()
	cfg.HeadTopUpInterval = 0 // disabled

	runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	created, err := runner.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created) != 0 {
		t.Fatalf("expected 0 runs created when disabled, got %d", len(created))
	}
}

// TestHeadTopUp_StopCondition_TargetCIHalfWidth verifies that top-up stops when the CI half-width is narrow enough.
func TestHeadTopUp_StopCondition_TargetCIHalfWidth(t *testing.T) {
	s := setupScheme(t)
	now := time.Now()

	// High samples with narrow CI (e.g. 500 samples, 450 successes -> CI width is ~0.048, half-width ~0.024 <= 0.05)
	runNarrow := makeBaseRun("base-1111111", "gke-labs/eval-runner", "main", "1111111", now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 500, Successes: 450}},
		&v1alpha1.SampleRange{Start: 1, End: 500})

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(runNarrow).Build()
	cfg := DefaultClusterConfig()
	cfg.HeadTopUpInterval = 6 * time.Hour
	cfg.HeadTargetCIHalfWidth = 0.05
	cfg.HeadMaxSamplesPerTask = 1000

	runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	created, err := runner.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created) != 0 {
		t.Fatalf("expected 0 runs created because CI is already narrower than 0.05, got %d", len(created))
	}

	// Conversely, with few samples (8 samples, 4 successes -> CI is wide, half-width ~0.30 > 0.05)
	runWide := makeBaseRun("base-2222222", "gke-labs/eval-runner", "main", "2222222", now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4}},
		&v1alpha1.SampleRange{Start: 1, End: 8})

	fakeClient2 := fake.NewClientBuilder().WithScheme(s).WithObjects(runWide).Build()
	runner2 := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient2,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	created2, err := runner2.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created2) != 1 {
		t.Fatalf("expected 1 run created because CI half-width is > 0.05, got %d", len(created2))
	}
	if created2[0].Name != "base-2222222-2" {
		t.Errorf("expected run name base-2222222-2, got %s", created2[0].Name)
	}
}

// TestHeadTopUp_StopCondition_MaxSamplesPerTask verifies that top-up stops when any task reaches headMaxSamplesPerTask.
func TestHeadTopUp_StopCondition_MaxSamplesPerTask(t *testing.T) {
	s := setupScheme(t)
	now := time.Now()

	// Task reached 64 samples
	runCapped := makeBaseRun("base-1111111", "gke-labs/eval-runner", "main", "1111111", now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{
			{ID: "taskA", Phase: v1alpha1.TaskPhaseCompleted, Samples: 64, Successes: 32},
			{ID: "taskB", Phase: v1alpha1.TaskPhaseCompleted, Samples: 32, Successes: 16},
		},
		&v1alpha1.SampleRange{Start: 1, End: 64})

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(runCapped).Build()
	cfg := DefaultClusterConfig()
	cfg.HeadTopUpInterval = 6 * time.Hour
	cfg.HeadTargetCIHalfWidth = 0.01 // very small target so CI check doesn't trigger
	cfg.HeadMaxSamplesPerTask = 64

	runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	created, err := runner.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created) != 0 {
		t.Fatalf("expected 0 runs created because taskA reached 64 samples, got %d", len(created))
	}
}

// TestHeadTopUp_StopCondition_USDBudget verifies that top-up stops when USD budget would be exceeded.
func TestHeadTopUp_StopCondition_USDBudget(t *testing.T) {
	s := setupScheme(t)
	now := time.Now()

	// 1. Spend already >= budget
	costSpent := 2.50
	runSpent := makeBaseRun("base-1111111", "gke-labs/eval-runner", "main", "1111111", now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4}},
		&v1alpha1.SampleRange{Start: 1, End: 8})
	runSpent.Status.CostUSD = &costSpent

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(runSpent).Build()
	budget := 2.00
	cfg := DefaultClusterConfig()
	cfg.HeadTopUpInterval = 6 * time.Hour
	cfg.HeadTargetCIHalfWidth = 0.01
	cfg.HeadMaxSamplesPerTask = 100
	cfg.DefaultBudgetUSD = &budget

	runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	created, err := runner.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created) != 0 {
		t.Fatalf("expected 0 runs created because spend ($2.50) >= budget ($2.00), got %d", len(created))
	}

	// 2. Projected spend exceeds budget: spend $1.80 for 8 samples ($0.225/sample). Next batch of 8 samples costs $1.80 -> projected $3.60 > $2.00
	costProjected := 1.80
	runProjected := makeBaseRun("base-2222222", "gke-labs/eval-runner", "main", "2222222", now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4}},
		&v1alpha1.SampleRange{Start: 1, End: 8})
	runProjected.Status.CostUSD = &costProjected

	fakeClient2 := fake.NewClientBuilder().WithScheme(s).WithObjects(runProjected).Build()
	runner2 := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient2,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	created2, err := runner2.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created2) != 0 {
		t.Fatalf("expected 0 runs created because projected spend ($3.60) > budget ($2.00), got %d", len(created2))
	}
}

// TestHeadTopUp_StopCondition_TwoConsecutiveFailedRunsZeroSamples verifies that top-up stops
// if the last two runs at head failed and contributed zero scored samples, logging the existing skip line.
func TestHeadTopUp_StopCondition_TwoConsecutiveFailedRunsZeroSamples(t *testing.T) {
	s := setupScheme(t)
	now := time.Now()

	runSeed := makeBaseRun("base-1111111", "gke-labs/eval-runner", "main", "1111111", now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4}},
		&v1alpha1.SampleRange{Start: 1, End: 8})

	// Top-up run 2 failed with 0 samples
	run2Failed := makeBaseRun("base-1111111-2", "gke-labs/eval-runner", "main", "1111111", now.Add(time.Minute), "2",
		v1alpha1.RunPhaseFailed,
		nil,
		&v1alpha1.SampleRange{Start: 9, End: 16})

	// Top-up run 3 also failed with 0 samples
	run3Failed := makeBaseRun("base-1111111-3", "gke-labs/eval-runner", "main", "1111111", now.Add(2*time.Minute), "3",
		v1alpha1.RunPhaseFailed,
		nil,
		&v1alpha1.SampleRange{Start: 9, End: 16})

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(runSeed, run2Failed, run3Failed).Build()
	cfg := DefaultClusterConfig()
	cfg.HeadTopUpInterval = 6 * time.Hour

	sink := &testLogSink{}
	logger := logr.New(sink)

	runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient,
		Namespace: "eval-runner",
		Logger:    logger,
		Config:    cfg,
	})

	created, err := runner.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created) != 0 {
		t.Fatalf("expected 0 runs created because two consecutive top-ups failed with zero samples, got %d", len(created))
	}

	foundLog := false
	for _, entry := range sink.entries {
		if strings.Contains(entry, "skipping head top-up: latest run failed at commit") {
			foundLog = true
			break
		}
	}
	if !foundLog {
		t.Fatalf("expected log 'skipping head top-up: latest run failed at commit', got logs: %v", sink.entries)
	}
}

// TestHeadTopUp_LatestRunFailed_WithScoredSamples_CreatesNextTopUp verifies that when the latest run
// is Failed but scored samples, top-up is not halted and the next run is created with the next window.
func TestHeadTopUp_LatestRunFailed_WithScoredSamples_CreatesNextTopUp(t *testing.T) {
	s := setupScheme(t)
	now := time.Now()

	runSeed := makeBaseRun("base-1111111", "gke-labs/eval-runner", "main", "1111111", now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4}},
		&v1alpha1.SampleRange{Start: 1, End: 8})

	// Top-up run 2 failed, but scored 8 samples (e.g. restart rescued or one task hit deadline)
	run2Failed := makeBaseRun("base-1111111-2", "gke-labs/eval-runner", "main", "1111111", now.Add(time.Minute), "2",
		v1alpha1.RunPhaseFailed,
		[]v1alpha1.TaskStatus{
			{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 5},
			{ID: "task2", Phase: v1alpha1.TaskPhaseFailed, Samples: 0},
		},
		&v1alpha1.SampleRange{Start: 9, End: 16})

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(runSeed, run2Failed).Build()
	cfg := DefaultClusterConfig()
	cfg.HeadTopUpInterval = 6 * time.Hour
	cfg.DefaultBatchSize = 8

	runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	created, err := runner.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created) != 1 {
		t.Fatalf("expected 1 run created because failed run scored samples, got %d", len(created))
	}
	if created[0].Name != "base-1111111-3" {
		t.Errorf("expected run name base-1111111-3, got %s", created[0].Name)
	}
	if created[0].Labels[consumer.LabelTopUp] != "3" {
		t.Errorf("expected topUp label '3', got %s", created[0].Labels[consumer.LabelTopUp])
	}
	if created[0].Spec.SampleRange == nil || created[0].Spec.SampleRange.Start != 17 || created[0].Spec.SampleRange.End != 24 {
		t.Errorf("expected sample range 17..24, got %v", created[0].Spec.SampleRange)
	}
}

// TestHeadTopUp_LatestRunFailed_WithScoredSamples_SeenByStopConditions verifies that Failed-but-scored
// runs are pooled so stop conditions (CI half-width, sample cap) see all observations.
func TestHeadTopUp_LatestRunFailed_WithScoredSamples_SeenByStopConditions(t *testing.T) {
	s := setupScheme(t)
	now := time.Now()

	runSeed := makeBaseRun("base-1111111", "gke-labs/eval-runner", "main", "1111111", now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4}},
		&v1alpha1.SampleRange{Start: 1, End: 8})

	// Top-up run 2 failed, but scored 8 samples
	run2Failed := makeBaseRun("base-1111111-2", "gke-labs/eval-runner", "main", "1111111", now.Add(time.Minute), "2",
		v1alpha1.RunPhaseFailed,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 5}},
		&v1alpha1.SampleRange{Start: 9, End: 16})

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(runSeed, run2Failed).Build()
	cfg := DefaultClusterConfig()
	cfg.HeadTopUpInterval = 6 * time.Hour
	cfg.HeadMaxSamplesPerTask = 16 // pooled samples (8 + 8 = 16) reaches this cap!

	runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	created, err := runner.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created) != 0 {
		t.Fatalf("expected 0 runs created because sample cap reached (including Failed run's samples), got %d", len(created))
	}
}

// TestHeadTopUp_LatestRunFailed_SingleFailureZeroSamples_RetriesWindow verifies that a single Failed
// run with zero samples does not halt the loop and the un-evaluated window is retried.
func TestHeadTopUp_LatestRunFailed_SingleFailureZeroSamples_RetriesWindow(t *testing.T) {
	s := setupScheme(t)
	now := time.Now()

	runSeed := makeBaseRun("base-1111111", "gke-labs/eval-runner", "main", "1111111", now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4}},
		&v1alpha1.SampleRange{Start: 1, End: 8})

	// Top-up run 2 failed with 0 samples
	run2Failed := makeBaseRun("base-1111111-2", "gke-labs/eval-runner", "main", "1111111", now.Add(time.Minute), "2",
		v1alpha1.RunPhaseFailed,
		nil,
		&v1alpha1.SampleRange{Start: 9, End: 16})

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(runSeed, run2Failed).Build()
	cfg := DefaultClusterConfig()
	cfg.HeadTopUpInterval = 6 * time.Hour
	cfg.DefaultBatchSize = 8

	runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	created, err := runner.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created) != 1 {
		t.Fatalf("expected 1 run created for single failure retry, got %d", len(created))
	}
	if created[0].Name != "base-1111111-3" {
		t.Errorf("expected run name base-1111111-3, got %s", created[0].Name)
	}
	// Window 9..16 should be retried because run2 contributed zero samples
	if created[0].Spec.SampleRange == nil || created[0].Spec.SampleRange.Start != 9 || created[0].Spec.SampleRange.End != 16 {
		t.Errorf("expected retried sample range 9..16, got %v", created[0].Spec.SampleRange)
	}
}

// TestHeadTopUp_WindowProgression_NormalAndDatasetExhaustion verifies the window progression
// and wrapping back to 1..batch when dataset is exhausted.
func TestHeadTopUp_WindowProgression_NormalAndDatasetExhaustion(t *testing.T) {
	s := setupScheme(t)
	now := time.Now()

	// 1. Normal progression: seed run evaluated 8 samples, next window is 9..16
	runSeed := makeBaseRun("base-1111111", "gke-labs/eval-runner", "main", "1111111", now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4}},
		&v1alpha1.SampleRange{Start: 1, End: 8})

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(runSeed).Build()
	cfg := DefaultClusterConfig()
	cfg.HeadTopUpInterval = 6 * time.Hour
	cfg.DefaultBatchSize = 8

	runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	created, err := runner.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created) != 1 {
		t.Fatalf("expected 1 run created, got %d", len(created))
	}
	r2 := created[0]
	if r2.Name != "base-1111111-2" {
		t.Errorf("expected name base-1111111-2, got %s", r2.Name)
	}
	if r2.Labels[consumer.LabelTopUp] != "2" {
		t.Errorf("expected topup label '2', got %q", r2.Labels[consumer.LabelTopUp])
	}
	if r2.Spec.SampleRange == nil || r2.Spec.SampleRange.Start != 9 || r2.Spec.SampleRange.End != 16 {
		t.Errorf("expected sample range 9..16, got %+v", r2.Spec.SampleRange)
	}

	// 2. Dataset exhaustion: run-2 requested 9..16 (8 samples), but only evaluated 3 samples
	run2 := makeBaseRun("base-1111111-2", "gke-labs/eval-runner", "main", "1111111", now.Add(time.Minute), "2",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 3, Successes: 2, PossibleSamples: 10}},
		&v1alpha1.SampleRange{Start: 9, End: 16})

	fakeClient2 := fake.NewClientBuilder().WithScheme(s).WithObjects(runSeed, run2).Build()
	runner2 := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient2,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	created2, err := runner2.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created2) != 1 {
		t.Fatalf("expected 1 run created, got %d", len(created2))
	}
	r3 := created2[0]
	if r3.Name != "base-1111111-3" {
		t.Errorf("expected name base-1111111-3, got %s", r3.Name)
	}
	if r3.Labels[consumer.LabelTopUp] != "3" {
		t.Errorf("expected topup label '3', got %q", r3.Labels[consumer.LabelTopUp])
	}
	// Dataset was exhausted (prevEnd 16 >= PossibleSamples 10), so it should wrap back to 1..8
	if r3.Spec.SampleRange == nil || r3.Spec.SampleRange.Start != 1 || r3.Spec.SampleRange.End != 8 {
		t.Errorf("expected wrapped sample range 1..8, got %+v", r3.Spec.SampleRange)
	}
}

// TestHeadTopUp_NewestHeadOnlySelection verifies that only the newest head commit on a branch gets top-ups.
func TestHeadTopUp_NewestHeadOnlySelection(t *testing.T) {
	s := setupScheme(t)
	tOld := time.Now().Add(-24 * time.Hour)
	tNew := time.Now()

	// Old commit on main
	runOld := makeBaseRun("base-aaaaaaa", "gke-labs/eval-runner", "main", "aaaaaaa", tOld, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4}},
		&v1alpha1.SampleRange{Start: 1, End: 8})

	// Newest commit on main
	runNew := makeBaseRun("base-bbbbbbb", "gke-labs/eval-runner", "main", "bbbbbbb", tNew, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4}},
		&v1alpha1.SampleRange{Start: 1, End: 8})

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(runOld, runNew).Build()
	cfg := DefaultClusterConfig()
	cfg.HeadTopUpInterval = 6 * time.Hour

	runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	created, err := runner.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created) != 1 {
		t.Fatalf("expected exactly 1 run created for newest head only, got %d", len(created))
	}
	if created[0].Name != "base-bbbbbbb-2" {
		t.Errorf("expected top-up for newest head base-bbbbbbb-2, got %s", created[0].Name)
	}
	if created[0].Spec.Repo.Commit != "bbbbbbb" {
		t.Errorf("expected commit bbbbbbb, got %s", created[0].Spec.Repo.Commit)
	}
}

// TestHeadTopUp_InFlightSkip verifies that top-up does not run if a Run at the head commit is still in-flight.
func TestHeadTopUp_InFlightSkip(t *testing.T) {
	s := setupScheme(t)
	now := time.Now()

	phases := []v1alpha1.RunPhase{
		v1alpha1.RunPhasePending,
		v1alpha1.RunPhaseDiscovering,
		v1alpha1.RunPhaseRunning,
	}

	for _, phase := range phases {
		t.Run(string(phase), func(t *testing.T) {
			runInFlight := makeBaseRun("base-1111111", "gke-labs/eval-runner", "main", "1111111", now, "",
				phase, nil, &v1alpha1.SampleRange{Start: 1, End: 8})

			fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(runInFlight).Build()
			cfg := DefaultClusterConfig()
			cfg.HeadTopUpInterval = 6 * time.Hour

			runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
				Client:    fakeClient,
				Namespace: "eval-runner",
				Logger:    logr.Discard(),
				Config:    cfg,
			})

			created, err := runner.RunTopUpPass(t.Context())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(created) != 0 {
				t.Fatalf("expected 0 runs created when run is in phase %s, got %d", phase, len(created))
			}
		})
	}
}

// TestHeadTopUp_ControllerAndSeriesPoolingIntegration verifies that a second base-<sha>-2 Run
// appears after the interval/pass and the series overview pools it with ObsCount 2.
func TestHeadTopUp_ControllerAndSeriesPoolingIntegration(t *testing.T) {
	s := setupScheme(t)
	now := time.Now()
	repo := "gke-labs/eval-runner"
	branch := "main"
	commit := "c0ffee1"

	// 1. Initial completed seed run
	seedRun := makeBaseRun("base-c0ffee1", repo, branch, commit, now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{
			{ID: "task/docs", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 6},
		},
		&v1alpha1.SampleRange{Start: 1, End: 8})

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(seedRun).Build()
	cfg := DefaultClusterConfig()
	cfg.HeadTopUpInterval = 6 * time.Hour
	cfg.DefaultBatchSize = 8
	cfg.HeadTargetCIHalfWidth = 0.01 // keep running top-ups
	cfg.HeadMaxSamplesPerTask = 64
	cfg.Repositories = []syncer.RepoConfig{
		{
			Name:     repo,
			Ref:      forge.RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"},
			Branches: []string{"main"},
		},
	}

	runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:    fakeClient,
		Namespace: "eval-runner",
		Logger:    logr.Discard(),
		Config:    cfg,
	})

	// 2. Execute top-up pass
	created, err := runner.RunTopUpPass(t.Context())
	if err != nil {
		t.Fatalf("RunTopUpPass failed: %v", err)
	}
	if len(created) != 1 {
		t.Fatalf("expected 1 top-up run created, got %d", len(created))
	}
	topUpRun := created[0]
	if topUpRun.Name != "base-c0ffee1-2" {
		t.Fatalf("expected run name base-c0ffee1-2, got %s", topUpRun.Name)
	}

	// 3. Complete the top-up run
	topUpRun.Status.Phase = v1alpha1.RunPhaseCompleted
	topUpRun.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: "task/docs", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 7},
	}
	if err := fakeClient.Update(t.Context(), topUpRun); err != nil {
		t.Fatalf("failed to update topUpRun: %v", err)
	}

	// 4. Verify that the UI pools both runs and displays pooled score 0.81 in overview
	uiServer := ui.NewServer(ui.Config{
		Client:           fakeClient,
		DefaultNamespace: "eval-runner",
	})

	req := httptest.NewRequest(http.MethodGet, "/github.com/gke-labs/eval-runner?branch=main", nil)
	w := httptest.NewRecorder()
	uiServer.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from UI repo overview, got %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "0.81") && !strings.Contains(body, "81%") {
		t.Errorf("expected overview body to contain pooled score 0.81 or 81%%, body:\n%s", body)
	}
}

// TestHeadTopUp_StartAndRearm verifies that the background Start loop runs on ticks and can rearm.
func TestHeadTopUp_StartAndRearm(t *testing.T) {
	s := setupScheme(t)
	now := time.Now()
	repo := "gke-labs/eval-runner"
	branch := "main"
	commit := "feedbac"

	seedRun := makeBaseRun("base-feedbac", repo, branch, commit, now, "",
		v1alpha1.RunPhaseCompleted,
		[]v1alpha1.TaskStatus{
			{ID: "task1", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4},
		},
		&v1alpha1.SampleRange{Start: 1, End: 8})

	fakeClient := fake.NewClientBuilder().WithScheme(s).WithObjects(seedRun).Build()
	cfg := DefaultClusterConfig()
	cfg.HeadTopUpInterval = 0 // start disabled

	runner := NewHeadTopUpRunner(HeadTopUpRunnerOptions{
		Client:       fakeClient,
		Namespace:    "eval-runner",
		Logger:       logr.Discard(),
		Config:       cfg,
		InitialDelay: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Start(ctx)
	}()

	// Since disabled, nothing created
	time.Sleep(20 * time.Millisecond)
	var runList v1alpha1.RunList
	if err := fakeClient.List(t.Context(), &runList); err != nil {
		t.Fatalf("failed to list runs: %v", err)
	}
	if len(runList.Items) != 1 {
		t.Fatalf("expected 1 run while disabled, got %d", len(runList.Items))
	}

	// Update config to 10ms interval
	cfg.HeadTopUpInterval = 10 * time.Millisecond
	runner.UpdateConfig(cfg)

	// Wait for tick using an Eventually loop
	pollCtx, pollCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer pollCancel()
	created := false
	for !created {
		select {
		case <-pollCtx.Done():
			t.Fatalf("timed out waiting for top-up run to be created after rearm")
		case <-time.After(10 * time.Millisecond):
			if err := fakeClient.List(t.Context(), &runList); err == nil && len(runList.Items) >= 2 {
				created = true
			}
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("expected clean exit from Start, got: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Start did not exit within 500ms after context cancellation")
	}
}
