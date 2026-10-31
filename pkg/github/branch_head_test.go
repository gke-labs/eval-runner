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

package github

import (
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/forge"
	"github.com/gke-labs/eval-runner/pkg/pooling"
	"github.com/gke-labs/eval-runner/pkg/stats"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBranchHeadDetailsURL(t *testing.T) {
	ref := forge.RepoRef{
		Host:  "github.com",
		Owner: "gke-labs",
		Name:  "eval-runner",
	}

	url := BranchHeadDetailsURL("https://dotevals.com", ref, "main")
	want := "https://dotevals.com/github.com/gke-labs/eval-runner/series/main"
	if url != want {
		t.Errorf("BranchHeadDetailsURL = %q, want %q", url, want)
	}

	// Empty UI base URL
	urlEmpty := BranchHeadDetailsURL("", ref, "main")
	wantEmpty := "/github.com/gke-labs/eval-runner/series/main"
	if urlEmpty != wantEmpty {
		t.Errorf("BranchHeadDetailsURL empty UI = %q, want %q", urlEmpty, wantEmpty)
	}
}

func TestCheckRunStateForBranchHead_FullCycle(t *testing.T) {
	rate := 10.0 / 16.0
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name: "base-abc1234",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/head-sha": "abc1234",
				"evalrunner.gke-labs.dev/branch":   "main",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          "task-1",
					Phase:       v1alpha1.TaskPhaseCompleted,
					Samples:     16,
					Successes:   10,
					SuccessRate: &rate,
				},
			},
		},
	}

	pool := pooling.PoolRunsForCommit([]*v1alpha1.Run{run})
	ref := forge.RepoRef{
		Host:  "github.com",
		Owner: "gke-labs",
		Name:  "eval-runner",
	}
	lastMove := &stats.DeltaEstimate{
		Delta:      0.02,
		CI:         stats.Interval{Lower: -0.09, Upper: 0.13},
		Confidence: 0.95,
	}

	// 1. in_progress (queued vs running)
	queuedState := CheckRunStateForBranchHead(pool, lastMove, "https://dotevals.com", ref, "main", v1alpha1.RunPhasePending, false, 0)
	if queuedState.Status != "in_progress" || queuedState.Conclusion != "" || queuedState.Title != "queued" {
		t.Errorf("unexpected queued state: %+v", queuedState)
	}

	runningState := CheckRunStateForBranchHead(pool, lastMove, "https://dotevals.com", ref, "main", v1alpha1.RunPhaseRunning, false, 0)
	if runningState.Status != "in_progress" || runningState.Conclusion != "" || runningState.Title != "running" {
		t.Errorf("unexpected running state: %+v", runningState)
	}

	// 2. completed with parent delta
	compState := CheckRunStateForBranchHead(pool, lastMove, "https://dotevals.com", ref, "main", v1alpha1.RunPhaseCompleted, true, 45*time.Minute)
	if compState.Status != "completed" || compState.Conclusion != "success" {
		t.Errorf("unexpected completed state: %+v", compState)
	}
	wantTitle := "63% ± 21 · n=16 · Δ vs parent +2 (95% CI \u22129…+13)"
	if compState.Title != wantTitle {
		t.Errorf("Title = %q, want %q", compState.Title, wantTitle)
	}
	if !strings.Contains(compState.Summary, "next top-up in ~45m") {
		t.Errorf("Summary missing top-up notice: %s", compState.Summary)
	}
	if !strings.Contains(compState.Summary, "task-1") || !strings.Contains(compState.Summary, "16") {
		t.Errorf("Summary missing task row: %s", compState.Summary)
	}

	// 3. failed seed
	failRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name: "base-abc1234",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseFailed,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      "task-1",
					Phase:   v1alpha1.TaskPhaseFailed,
					Message: "syntax error",
				},
			},
		},
	}
	failPool := pooling.PoolRunsForCommit([]*v1alpha1.Run{failRun})
	failState := CheckRunStateForBranchHead(failPool, nil, "https://dotevals.com", ref, "main", v1alpha1.RunPhaseFailed, false, 0)
	if failState.Status != "completed" || failState.Conclusion != "failure" {
		t.Errorf("unexpected failure state: %+v", failState)
	}
	if failState.Title != "evals failed: 1 task failed" {
		t.Errorf("failure Title = %q, want 'evals failed: 1 task failed'", failState.Title)
	}

	// 4. failed seed but scored samples
	rateFailScored := 1.0
	failScoredRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name: "base-failed-scored",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/head-sha": "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseFailed,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          "task-1",
					Phase:       v1alpha1.TaskPhaseFailed,
					Samples:     8,
					Successes:   8,
					SuccessRate: &rateFailScored,
				},
			},
		},
	}
	failScoredPool := pooling.PoolRunsForCommit([]*v1alpha1.Run{failScoredRun})
	failScoredState := CheckRunStateForBranchHead(failScoredPool, nil, "https://dotevals.com", ref, "main", v1alpha1.RunPhaseFailed, false, 0)
	if failScoredState.Status != "completed" || failScoredState.Conclusion != "neutral" {
		t.Errorf("unexpected failure-scored state: %+v (want completed/neutral)", failScoredState)
	}
	if !strings.Contains(failScoredState.Title, "100%") || !strings.Contains(failScoredState.Title, "n=8") {
		t.Errorf("expected Title with score and n=8, got %q", failScoredState.Title)
	}
	if !strings.Contains(failScoredState.Summary, "failed after 8 samples") {
		t.Errorf("expected Summary to contain 'failed after 8 samples', got:\n%s", failScoredState.Summary)
	}
	if !strings.Contains(failScoredState.Text, "failed after 8 samples") {
		t.Errorf("expected Text to contain 'failed after 8 samples', got:\n%s", failScoredState.Text)
	}
}

func TestHashCheckRunState_NoCountdownChurn(t *testing.T) {
	ref := forge.RepoRef{
		Host:  "github.com",
		Owner: "gke-labs",
		Name:  "eval-runner",
	}
	rate := 1.0
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name: "base-abc",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/head-sha": "abc1234",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          "task-1",
					Phase:       v1alpha1.TaskPhaseCompleted,
					Samples:     10,
					Successes:   10,
					SuccessRate: &rate,
				},
			},
		},
	}
	pool := pooling.PoolRunsForCommit([]*v1alpha1.Run{run})

	// Two states at different countdown durations: 45m vs 38m
	state1 := CheckRunStateForBranchHead(pool, nil, "https://dotevals.com", ref, "main", v1alpha1.RunPhaseCompleted, true, 45*time.Minute)
	state2 := CheckRunStateForBranchHead(pool, nil, "https://dotevals.com", ref, "main", v1alpha1.RunPhaseCompleted, true, 38*time.Minute)

	hash1 := HashCheckRunState(state1)
	hash2 := HashCheckRunState(state2)

	if hash1 != hash2 {
		t.Errorf("HashCheckRunState churned on countdown: hash1=%s hash2=%s", hash1, hash2)
	}
}

func TestHashCheckRunState_ImageURLChanges(t *testing.T) {
	state1 := CheckRunState{
		Name:       CheckRunName,
		Status:     "completed",
		Conclusion: "success",
		Title:      "100% ± 0 · n=10",
		Summary:    "all passed",
		Images: []CheckRunImage{
			{
				ImageURL: "https://dotevals.com/github.com/gke-labs/eval-runner/sparkline.svg?branch=main&w=600&h=80",
				Alt:      "main, last 20 commits",
				Caption:  "main, last 20 commits",
			},
		},
	}
	state2 := CheckRunState{
		Name:       CheckRunName,
		Status:     "completed",
		Conclusion: "success",
		Title:      "100% ± 0 · n=10",
		Summary:    "all passed",
		Images: []CheckRunImage{
			{
				ImageURL: "https://dotevals.com/github.com/gke-labs/eval-runner/sparkline.svg?branch=dev&w=600&h=80",
				Alt:      "dev, last 20 commits",
				Caption:  "dev, last 20 commits",
			},
		},
	}

	hash1 := HashCheckRunState(state1)
	hash2 := HashCheckRunState(state2)

	if hash1 == hash2 {
		t.Errorf("expected hash to change when image URL changes, got same hash: %s", hash1)
	}
}

func TestCheckRunStateForBranchHead_RichOutput(t *testing.T) {
	ref := forge.RepoRef{
		Host:  "github.com",
		Owner: "gke-labs",
		Name:  "eval-runner",
	}
	rate := 10.0 / 16.0
	cost := 0.0512
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name: "base-head1",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/head-sha": "head1111",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          "task-1",
					Phase:       v1alpha1.TaskPhaseCompleted,
					Samples:     16,
					Successes:   10,
					SuccessRate: &rate,
					Tokens: &v1alpha1.TaskTokens{
						Total:     500,
						Reasoning: 100,
					},
					CostUSD: &cost,
				},
			},
		},
	}
	pool := pooling.PoolRunsForCommit([]*v1alpha1.Run{run})

	// 1. With uiBaseURL
	state := CheckRunStateForBranchHead(pool, nil, "https://dotevals.com", ref, "main", v1alpha1.RunPhaseCompleted, false, 0)

	// Images assertions
	if len(state.Images) != 1 {
		t.Fatalf("expected 1 image, got %d", len(state.Images))
	}
	wantSparklineURL := "https://dotevals.com/github.com/gke-labs/eval-runner/sparkline.svg?branch=main&w=600&h=80"
	if state.Images[0].ImageURL != wantSparklineURL {
		t.Errorf("ImageURL = %q, want %q", state.Images[0].ImageURL, wantSparklineURL)
	}
	wantCaption := "main, last 20 commits"
	if state.Images[0].Caption != wantCaption {
		t.Errorf("Caption = %q, want %q", state.Images[0].Caption, wantCaption)
	}
	if state.Images[0].Alt != wantCaption {
		t.Errorf("Alt = %q, want %q", state.Images[0].Alt, wantCaption)
	}

	// Text assertions: table columns, cells, and footer link
	if !strings.Contains(state.Text, "| eval | samples | successes | rate (95% CI) | tokens | cost |") {
		t.Errorf("expected table header with all columns in state.Text, got:\n%s", state.Text)
	}
	if !strings.Contains(state.Text, "[task-1](https://dotevals.com/github.com/gke-labs/eval-runner/runs/base-head1/tasks/task-1/samples)") {
		t.Errorf("expected repo-scoped sample link in state.Text, got:\n%s", state.Text)
	}
	if !strings.Contains(state.Text, "500 (+R 100)") {
		t.Errorf("expected reasoning tokens in state.Text, got:\n%s", state.Text)
	}
	if !strings.Contains(state.Text, "$0.0512") {
		t.Errorf("expected cost cell in state.Text, got:\n%s", state.Text)
	}
	if !strings.Contains(state.Text, "[series](https://dotevals.com/github.com/gke-labs/eval-runner/series/main)") {
		t.Errorf("expected footer link to series in state.Text, got:\n%s", state.Text)
	}

	// Safety: no /tmp/, gs://, or namespaced routes
	if strings.Contains(state.Text, "/tmp/") || strings.Contains(state.Text, "gs://") {
		t.Errorf("forbidden artifact paths (/tmp/, gs://) found in state.Text:\n%s", state.Text)
	}
	if strings.Contains(state.Text, "/runs/eval-runner") || strings.Contains(state.Text, "/comparisons/eval-runner") {
		t.Errorf("namespaced routes found in state.Text:\n%s", state.Text)
	}

	// 2. Without uiBaseURL: images omitted, links omitted
	stateNoUI := CheckRunStateForBranchHead(pool, nil, "", ref, "main", v1alpha1.RunPhaseCompleted, false, 0)
	if len(stateNoUI.Images) != 0 {
		t.Errorf("expected images omitted when uiBaseURL is empty, got %+v", stateNoUI.Images)
	}
	if strings.Contains(stateNoUI.Text, "[series]") {
		t.Errorf("expected '[series]' link omitted when uiBaseURL is empty, got:\n%s", stateNoUI.Text)
	}
	if strings.Contains(stateNoUI.Text, "[task-1]") {
		t.Errorf("expected markdown link for task-1 omitted when uiBaseURL is empty, got:\n%s", stateNoUI.Text)
	}
	if !strings.Contains(stateNoUI.Text, "task-1") {
		t.Errorf("expected task-1 plain text in stateNoUI.Text, got:\n%s", stateNoUI.Text)
	}
}

func TestReportBranchHead_RecreateOn404(t *testing.T) {
	server := NewFakeGitHubServer()
	defer server.Close()

	client := server.Client("test-token")
	rep := NewReporter(client, ReporterOptions{UIBaseURL: "https://dotevals.com"})

	ref := forge.RepoRef{
		Host:  "github.com",
		Owner: "gke-labs",
		Name:  "eval-runner",
	}

	state := CheckRunState{
		Name:       CheckRunName,
		Status:     "completed",
		Conclusion: "success",
		Title:      "100% ± 0 · n=10",
		Summary:    "all passed",
	}

	// First create check run
	createdID, _, err := rep.ReportBranchHead(t.Context(), ref, "abc1234", state, 0)
	if err != nil {
		t.Fatalf("ReportBranchHead create error: %v", err)
	}

	// Inject 404 on the next update call (simulating deleted check run)
	server.InjectPathError("PATCH", "/repos/gke-labs/eval-runner/check-runs/", 404, `{"message":"Not Found"}`)

	// Calling with old createdID should handle 404 and create a new check run
	newID, _, err := rep.ReportBranchHead(t.Context(), ref, "abc1234", state, createdID)
	if err != nil {
		t.Fatalf("ReportBranchHead update with 404 error: %v", err)
	}
	if newID == createdID {
		t.Errorf("expected new check run ID after 404 recreate, got same ID %d", newID)
	}
}
