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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
)

func sameCommitComparisonFixture() (*v1alpha1.Comparison, *v1alpha1.Run, *v1alpha1.Run) {
	pImp := 0.26
	rate1 := 1.00
	rate2Cand := 0.15
	rate2Base := 0.20
	delta1 := 0.00
	delta2 := -0.05

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-same-commit",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "docs-eval-base-05ce4d8",
			},
			Candidate: v1alpha1.ComparisonRunRef{
				Run: "docs-eval-cand-05ce4d8",
			},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     98,
				HeadSHA:    "05ce4d8222222222222222222222222222222222",
				BaseSHA:    "05ce4d8111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:        v1alpha1.ComparisonPhaseCompleted,
			Verdict:      v1alpha1.ComparisonVerdictInconclusive,
			PImprovement: &pImp,
			SummaryPath:  "gs://eval-bucket/comparisons/docs-eval-same-commit.json",
			Summary: &v1alpha1.ComparisonSummary{
				Tasks: "2/2",
			},
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:            "docs/readme_comprehension",
					BaseRate:      &rate1,
					CandidateRate: &rate1,
					Delta:         &delta1,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: 0.00,
						Upper: 0.00,
					},
					Label:  v1alpha1.ComparisonTaskLabelUnchanged,
					Pairs:  20,
					Wins:   0,
					Losses: 0,
					Ties:   20,
				},
				{
					ID:            "docs/readme_comprehension_blind",
					BaseRate:      &rate2Base,
					CandidateRate: &rate2Cand,
					Delta:         &delta2,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: -0.15,
						Upper: 0.00,
					},
					Label:  v1alpha1.ComparisonTaskLabelInconclusive,
					Pairs:  20,
					Wins:   0,
					Losses: 1,
					Ties:   19,
				},
			},
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-base-05ce4d8",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				Commit: "05ce4d8111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.RunStatus{
			Summary: &v1alpha1.RunSummary{
				TotalSamples: 40,
			},
		},
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-cand-05ce4d8",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				Commit: "05ce4d8222222222222222222222222222222222",
			},
		},
		Status: v1alpha1.RunStatus{
			Summary: &v1alpha1.RunSummary{
				TotalSamples: 40,
			},
		},
	}
	return comp, baseRun, candRun
}

func candidateFailedComparisonFixture() (*v1alpha1.Comparison, *v1alpha1.Run, *v1alpha1.Run) {
	pImp := 0.50
	rateBase := 0.20
	rateCand := 1.00
	delta := 0.80

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-candidate-failed",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "docs-eval-base-05ce4d8",
			},
			Candidate: v1alpha1.ComparisonRunRef{
				Run: "docs-eval-cand-05ce4d8",
			},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     98,
				HeadSHA:    "05ce4d8222222222222222222222222222222222",
				BaseSHA:    "05ce4d8111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:        v1alpha1.ComparisonPhaseCompleted,
			Verdict:      v1alpha1.ComparisonVerdictInconclusive,
			PImprovement: &pImp,
			SummaryPath:  "gs://eval-bucket/comparisons/docs-eval-candidate-failed.json",
			Summary: &v1alpha1.ComparisonSummary{
				Tasks: "1/1",
			},
			Message: "INCONCLUSIVE: candidate task failed: docs/readme_comprehension_blind; otherwise IMPROVED: 1 task(s) improved, 0 unchanged",
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:            "docs/readme_comprehension",
					BaseRate:      &rateBase,
					CandidateRate: &rateCand,
					Delta:         &delta,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: 0.60,
						Upper: 1.00,
					},
					Label:  v1alpha1.ComparisonTaskLabelImproved,
					Pairs:  20,
					Wins:   16,
					Losses: 0,
					Ties:   4,
				},
			},
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-base-05ce4d8",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				Commit: "05ce4d8111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.RunStatus{
			Summary: &v1alpha1.RunSummary{
				TotalSamples: 40,
			},
		},
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-cand-05ce4d8",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				Commit: "05ce4d8222222222222222222222222222222222",
			},
		},
		Status: v1alpha1.RunStatus{
			Summary: &v1alpha1.RunSummary{
				TotalSamples: 40,
			},
		},
	}
	return comp, baseRun, candRun
}

func skippedTaskComparisonFixture() (*v1alpha1.Comparison, *v1alpha1.Run, *v1alpha1.Run) {
	pImp := 0.98
	rateBase := 0.20
	rateCand := 1.00
	delta := 0.80

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-skipped-task",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "docs-eval-base-05ce4d8",
			},
			Candidate: v1alpha1.ComparisonRunRef{
				Run: "docs-eval-cand-05ce4d8",
			},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     98,
				HeadSHA:    "05ce4d8222222222222222222222222222222222",
				BaseSHA:    "05ce4d8111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:        v1alpha1.ComparisonPhaseCompleted,
			Verdict:      v1alpha1.ComparisonVerdictImproved,
			PImprovement: &pImp,
			SummaryPath:  "gs://eval-bucket/comparisons/docs-eval-skipped-task.json",
			Summary: &v1alpha1.ComparisonSummary{
				Tasks:   "1/1",
				Skipped: 1,
			},
			Message: "IMPROVED: 1 task(s) improved, 0 unchanged; skipped (not planned): docs/readme_comprehension_blind",
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:            "docs/readme_comprehension",
					BaseRate:      &rateBase,
					CandidateRate: &rateCand,
					Delta:         &delta,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: 0.60,
						Upper: 1.00,
					},
					Label:  v1alpha1.ComparisonTaskLabelImproved,
					Pairs:  20,
					Wins:   16,
					Losses: 0,
					Ties:   4,
				},
			},
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-base-05ce4d8",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				Commit: "05ce4d8111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.RunStatus{
			Summary: &v1alpha1.RunSummary{
				TotalSamples: 40,
			},
		},
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-cand-05ce4d8",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				Commit: "05ce4d8222222222222222222222222222222222",
			},
		},
		Status: v1alpha1.RunStatus{
			Summary: &v1alpha1.RunSummary{
				TotalSamples: 40,
			},
		},
	}
	return comp, baseRun, candRun
}

func TestRenderComparisonMarkdown_Golden(t *testing.T) {
	comp, baseRun, candRun := sameCommitComparisonFixture()
	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")

	goldenPath := "testdata/same_commit_comparison.golden.md"
	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file: %v", err)
	}
	want := string(wantBytes)

	if got != want {
		t.Errorf("RenderComparisonMarkdown mismatch (-got +want):\nGOT:\n%s\nWANT:\n%s", got, want)
	}
}

func TestRenderComparisonMarkdown_Golden_CandidateTaskFailed(t *testing.T) {
	comp, baseRun, candRun := candidateFailedComparisonFixture()
	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")

	goldenPath := "testdata/candidate_failed_comparison.golden.md"
	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file: %v", err)
	}
	want := string(wantBytes)

	if got != want {
		t.Errorf("RenderComparisonMarkdown mismatch (-got +want):\nGOT:\n%s\nWANT:\n%s", got, want)
	}
}

func TestRenderComparisonMarkdown_Golden_SkippedTask(t *testing.T) {
	comp, baseRun, candRun := skippedTaskComparisonFixture()
	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")

	goldenPath := "testdata/skipped_task_comparison.golden.md"
	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file: %v", err)
	}
	want := string(wantBytes)

	if got != want {
		t.Errorf("RenderComparisonMarkdown mismatch (-got +want):\nGOT:\n%s\nWANT:\n%s", got, want)
	}
}

func runMoreSamplesComparisonFixture() (*v1alpha1.Comparison, *v1alpha1.Run, *v1alpha1.Run) {
	comp, baseRun, candRun := sameCommitComparisonFixture()
	comp.Name = "docs-eval-run-more-samples"
	comp.Status.SummaryPath = "gs://eval-bucket/comparisons/docs-eval-run-more-samples.json"
	comp.Status.Tasks[1].SuggestedNext = 16
	return comp, baseRun, candRun
}

func TestRenderComparisonMarkdown_Golden_RunMoreSamples(t *testing.T) {
	comp, baseRun, candRun := runMoreSamplesComparisonFixture()
	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")

	goldenPath := "testdata/run_more_samples_comparison.golden.md"
	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file: %v", err)
	}
	want := string(wantBytes)

	if got != want {
		t.Errorf("RenderComparisonMarkdown mismatch (-got +want):\nGOT:\n%s\nWANT:\n%s", got, want)
	}
}

func budgetComparisonFixture() (*v1alpha1.Comparison, *v1alpha1.Run, *v1alpha1.Run) {
	comp, baseRun, candRun := runMoreSamplesComparisonFixture()
	comp.Name = "docs-eval-budget-reached"
	comp.Status.SummaryPath = "gs://eval-bucket/comparisons/docs-eval-budget-reached.json"
	budget := 0.50
	spent := 0.52
	comp.Spec.BudgetUSD = &budget
	comp.Status.Budget = &v1alpha1.ComparisonBudgetStatus{
		LimitUSD: &budget,
		SpentUSD: &spent,
		Enforced: true,
	}
	comp.Status.Message = "INCONCLUSIVE: 1 task(s) inconclusive; budget ($0.50) reached (spent $0.52)"
	return comp, baseRun, candRun
}

func TestRenderComparisonMarkdown_Golden_BudgetReached(t *testing.T) {
	comp, baseRun, candRun := budgetComparisonFixture()
	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")

	goldenPath := "testdata/budget_comparison.golden.md"
	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file: %v", err)
	}
	want := string(wantBytes)

	if got != want {
		t.Errorf("RenderComparisonMarkdown mismatch (-got +want):\nGOT:\n%s\nWANT:\n%s", got, want)
	}
}

func TestCheckRunStateForComparison_BudgetSummaryLine(t *testing.T) {
	comp, baseRun, candRun := budgetComparisonFixture()
	state := CheckRunStateForComparison(comp, baseRun, candRun, "", "")
	bIdx := strings.Index(state.Summary, "budget $0.52 / $0.50")
	hIdx := strings.Index(state.Summary, "budget reached; comment `/eval more 16` to exceed it")
	if bIdx == -1 {
		t.Errorf("expected state.Summary to contain 'budget $0.52 / $0.50', got %q", state.Summary)
	}
	if hIdx == -1 {
		t.Errorf("expected state.Summary to contain budget reached hint, got %q", state.Summary)
	}
	if bIdx >= hIdx {
		t.Errorf("expected budget line to appear before hints in check-run summary, got:\n%s", state.Summary)
	}

	// Test unenforced budget displays "(not enforced)" in check-run and PR footer
	comp.Status.Budget.Enforced = false
	comp.Status.Budget.Note = "cost unknown for custom-model; budget not enforced"
	stateUnenforced := CheckRunStateForComparison(comp, baseRun, candRun, "", "")
	if !strings.Contains(stateUnenforced.Summary, "budget $0.52 / $0.50 (not enforced)") {
		t.Errorf("expected check-run summary to contain 'budget $0.52 / $0.50 (not enforced)', got %q", stateUnenforced.Summary)
	}

	mdUnenforced := RenderComparisonMarkdown(comp, baseRun, candRun, "")
	if !strings.Contains(mdUnenforced, "budget $0.52 / $0.50 (not enforced)") {
		t.Errorf("expected markdown footer to contain 'budget $0.52 / $0.50 (not enforced)', got %q", mdUnenforced)
	}
}

func batchesTokensComparisonFixture() (*v1alpha1.Comparison, *v1alpha1.Run, *v1alpha1.Run) {
	rateImprovedBase := 0.60
	rateImprovedCand := 0.90
	deltaImproved := 0.30

	rateInconclusiveBase := 0.20
	rateInconclusiveCand := 0.15
	deltaInconclusive := -0.05

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-batches-tokens",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "docs-eval-base-05ce4d8",
			},
			Candidate: v1alpha1.ComparisonRunRef{
				Run: "docs-eval-cand-05ce4d8",
			},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     98,
				HeadSHA:    "05ce4d8222222222222222222222222222222222",
				BaseSHA:    "a81d0c4111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:       v1alpha1.ComparisonPhaseRunning,
			Verdict:     v1alpha1.ComparisonVerdictRunning,
			Message:     "Running batch 2 (1 tasks continuing)",
			SummaryPath: "gs://eval-bucket/comparisons/docs-eval-batches-tokens.json",
			Summary: &v1alpha1.ComparisonSummary{
				Tasks: "1/3",
			},
			Batches: []v1alpha1.ComparisonBatchStatus{
				{
					Index:        1,
					Phase:        v1alpha1.ComparisonPhaseCompleted,
					SampleRange:  &v1alpha1.SampleRange{Start: 1, End: 8},
					CandidateRun: "docs-eval-cand-05ce4d8-b1",
					BaselineRun:  "docs-eval-base-05ce4d8-b1",
				},
				{
					Index:        2,
					Phase:        v1alpha1.ComparisonPhaseRunning,
					SampleRange:  &v1alpha1.SampleRange{Start: 9, End: 16},
					CandidateRun: "docs-eval-cand-05ce4d8-b2",
					BaselineRun:  "docs-eval-base-05ce4d8-b2",
				},
			},
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:            "docs/readme_comprehension",
					BaseRate:      &rateImprovedBase,
					CandidateRate: &rateImprovedCand,
					Delta:         &deltaImproved,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: 0.10,
						Upper: 0.50,
					},
					Label:      v1alpha1.ComparisonTaskLabelImproved,
					Decision:   v1alpha1.ComparisonTaskLabelImproved,
					Pairs:      16,
					SamplesRun: 16,
					Wins:       8,
					Losses:     2,
					Ties:       6,
					Tokens: &v1alpha1.TaskTokens{
						Input:     600,
						Output:    200,
						Total:     800,
						Reasoning: 100,
					},
					Latency: &v1alpha1.TaskLatency{
						P50: 0.25,
						P95: 0.50,
					},
					CostUSD:         ptr(0.01),
					CostSource:      v1alpha1.CostSourceDefault,
					PossibleSamples: 50,
				},
				{
					ID:            "docs/readme_comprehension_blind",
					BaseRate:      &rateInconclusiveBase,
					CandidateRate: &rateInconclusiveCand,
					Delta:         &deltaInconclusive,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: -0.15,
						Upper: 0.00,
					},
					Label:           v1alpha1.ComparisonTaskLabelInconclusive,
					Decision:        v1alpha1.ComparisonTaskLabelContinue,
					Pairs:           16,
					SamplesRun:      16,
					Wins:            1,
					Losses:          2,
					Ties:            13,
					SuggestedNext:   16,
					PossibleSamples: 50,
				},
				{
					ID:    "docs/readme_summary",
					Label: v1alpha1.ComparisonTaskLabelRunning,
				},
			},
			CostUSD:         ptr(0.01),
			PossibleSamples: 100,
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-base-05ce4d8",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "a81d0c4111111111111111111111111111111111",
			},
			Model: "google/gemini-3.5-flash-lite",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
		},
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-cand-05ce4d8",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "05ce4d8222222222222222222222222222222222",
			},
			Model: "google/gemini-3.5-flash-lite",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseRunning,
		},
	}

	return comp, baseRun, candRun
}

func TestRenderComparisonMarkdown_Golden_BatchesAndTokens(t *testing.T) {
	comp, baseRun, candRun := batchesTokensComparisonFixture()
	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")

	goldenPath := "testdata/batches_tokens_comparison.golden.md"
	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file: %v", err)
	}
	want := string(wantBytes)

	if got != want {
		t.Errorf("RenderComparisonMarkdown mismatch (-got +want):\nGOT:\n%s\nWANT:\n%s", got, want)
	}
}

func statisticsComparisonFixture() (*v1alpha1.Comparison, *v1alpha1.Run, *v1alpha1.Run) {
	rateLookupBase := 0.61
	rateLookupCand := 0.84
	deltaLookup := 0.23
	pImpLookup := 0.99

	ratePolicyBase := 0.88
	ratePolicyCand := 0.90
	deltaPolicy := 0.02
	pImpPolicy := 0.55

	rateQualityBase := 0.72
	rateQualityCand := 0.75
	deltaQuality := 0.03
	pImpQuality := 0.60

	overallPImp := 0.98

	costBase := 2.976
	costCand := 1.152

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-418-eval",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline: v1alpha1.ComparisonRunRef{
				Run: "run-main-a81d0c4",
			},
			Candidate: v1alpha1.ComparisonRunRef{
				Run: "run-pr-418-3f9c2e1",
			},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     418,
				HeadSHA:    "3f9c2e1222222222222222222222222222222222",
				BaseSHA:    "a81d0c4111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:        v1alpha1.ComparisonPhaseCompleted,
			Verdict:      v1alpha1.ComparisonVerdictImproved,
			PImprovement: &overallPImp,
			SummaryPath:  "gs://eval-bucket/comparisons/pr-418-eval.json",
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:            "triage/runbook_lookup",
					BaseRate:      &rateLookupBase,
					CandidateRate: &rateLookupCand,
					Delta:         &deltaLookup,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: 0.11,
						Upper: 0.35,
					},
					Label:        v1alpha1.ComparisonTaskLabelImproved,
					Pairs:        48,
					Wins:         15,
					Losses:       4,
					Ties:         29,
					PImprovement: &pImpLookup,
				},
				{
					ID:            "triage/escalation_policy",
					BaseRate:      &ratePolicyBase,
					CandidateRate: &ratePolicyCand,
					Delta:         &deltaPolicy,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: -0.05,
						Upper: 0.09,
					},
					Label:        v1alpha1.ComparisonTaskLabelUnchanged,
					Pairs:        32,
					Wins:         2,
					Losses:       1,
					Ties:         29,
					PImprovement: &pImpPolicy,
				},
				{
					ID:            "triage/summary_quality",
					BaseRate:      &rateQualityBase,
					CandidateRate: &rateQualityCand,
					Delta:         &deltaQuality,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: -0.08,
						Upper: 0.14,
					},
					Label:        v1alpha1.ComparisonTaskLabelInconclusive,
					Pairs:        16,
					Wins:         2,
					Losses:       1,
					Ties:         13,
					PImprovement: &pImpQuality,
				},
			},
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-main-a81d0c4",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				Commit: "a81d0c4111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.RunStatus{
			Summary: &v1alpha1.RunSummary{
				TotalSamples: 96,
			},
			CostUSD: &costBase,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID: "triage/runbook_lookup",
					Latency: &v1alpha1.TaskLatency{
						P50: 10.0,
						P95: 15.0,
					},
				},
			},
		},
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-pr-418-3f9c2e1",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				Commit: "3f9c2e1222222222222222222222222222222222",
			},
		},
		Status: v1alpha1.RunStatus{
			Summary: &v1alpha1.RunSummary{
				TotalSamples: 96,
			},
			CostUSD: &costCand,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID: "triage/runbook_lookup",
					Latency: &v1alpha1.TaskLatency{
						P50: 6.2,
						P95: 9.0,
					},
				},
			},
		},
	}

	return comp, baseRun, candRun
}

func TestRenderComparisonMarkdown_Golden_StatisticsParity(t *testing.T) {
	comp, baseRun, candRun := statisticsComparisonFixture()
	got := RenderComparisonMarkdown(comp, baseRun, candRun, "https://eval.example.com")

	goldenPath := "testdata/statistics_parity_comparison.golden.md"
	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file: %v", err)
	}
	want := string(wantBytes)

	if got != want {
		t.Errorf("RenderComparisonMarkdown mismatch (-got +want):\nGOT:\n%s\nWANT:\n%s", got, want)
	}
}

func TestRenderComparisonMarkdown_TrajectoriesAndTaskLinksOnlyWithUIBaseURL(t *testing.T) {
	comp, baseRun, candRun := statisticsComparisonFixture()

	// 1. With uiBaseURL
	gotWith := RenderComparisonMarkdown(comp, baseRun, candRun, "https://eval.example.com")
	if !strings.Contains(gotWith, "[trajectories](https://eval.example.com/github.com/gke-labs/eval-runner/runs/run-pr-418-3f9c2e1/tasks)") {
		t.Errorf("expected trajectories link with uiBaseURL, got:\n%s", gotWith)
	}
	if !strings.Contains(gotWith, "[triage/runbook\\_lookup](https://eval.example.com/github.com/gke-labs/eval-runner/runs/run-pr-418-3f9c2e1/tasks/triage%2Frunbook_lookup/samples)") {
		t.Errorf("expected task link with uiBaseURL, got:\n%s", gotWith)
	}
	if strings.Contains(gotWith, "/runs/eval-runner/") || strings.Contains(gotWith, "/comparisons/eval-runner/") {
		t.Errorf("expected no legacy namespaced routes in output, got:\n%s", gotWith)
	}
	if !strings.Contains(gotWith, "cost per sample −61% · p50 latency −38%") {
		t.Errorf("expected cost and latency footer, got:\n%s", gotWith)
	}

	// 2. Without uiBaseURL
	gotWithout := RenderComparisonMarkdown(comp, baseRun, candRun, "")
	if strings.Contains(gotWithout, "trajectories") {
		t.Errorf("expected no trajectories link without uiBaseURL, got:\n%s", gotWithout)
	}
	if strings.Contains(gotWithout, "[triage/runbook\\_lookup]") {
		t.Errorf("expected task ID to be plain text without uiBaseURL, got:\n%s", gotWithout)
	}
	if !strings.Contains(gotWithout, "| triage/runbook\\_lookup | 0.61 | 0.84 | +0.23 [+0.11, +0.35] | improved | 48 | 15/4/29 | 0.99 |") {
		t.Errorf("expected plain task ID row without uiBaseURL, got:\n%s", gotWithout)
	}
	// Cost and latency footer is still present when cost/latency data exists
	if !strings.Contains(gotWithout, "cost per sample −61% · p50 latency −38%") {
		t.Errorf("expected cost and latency footer even without uiBaseURL, got:\n%s", gotWithout)
	}
}

func TestRenderComparisonMarkdown_ZeroPairsRowShowsDashes(t *testing.T) {
	rate := 0.80
	delta := 0.20
	pImp := 0.95
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "zero-pairs-comp", Namespace: "eval-runner"},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictInconclusive,
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:            "evals/normal_task",
					BaseRate:      &rate,
					CandidateRate: &rate,
					Delta:         &delta,
					Label:         v1alpha1.ComparisonTaskLabelImproved,
					Pairs:         10,
					Wins:          7,
					Losses:        1,
					Ties:          2,
					PImprovement:  &pImp,
				},
				{
					ID:           "evals/zero_pairs_task",
					Label:        v1alpha1.ComparisonTaskLabelInconclusive,
					Pairs:        0,
					PImprovement: nil,
				},
			},
		},
	}

	got := RenderComparisonMarkdown(comp, nil, nil, "")
	// Table should have both W/L/T and P(imp) because normal_task has PImprovement
	if !strings.Contains(got, "| eval | base | this PR | Δ (95% CI) | label | samples | W/L/T | P(imp) |") {
		t.Errorf("expected header with W/L/T and P(imp), got:\n%s", got)
	}
	// Normal task has formatted W/L/T and P(imp)
	if !strings.Contains(got, "| evals/normal\\_task | 0.80 | 0.80 | +0.20 | improved | 10 | 7/1/2 | 0.95 |") {
		t.Errorf("expected normal task row with W/L/T and P(imp), got:\n%s", got)
	}
	// Zero-pairs task has dashes for both W/L/T and P(imp)
	if !strings.Contains(got, "| evals/zero\\_pairs\\_task | — | — | — | inconclusive | — | — | — |") {
		t.Errorf("expected zero pairs task row with '—' for W/L/T and P(imp), got:\n%s", got)
	}
}

func TestComparisonNotes(t *testing.T) {
	tests := []struct {
		name string
		comp *v1alpha1.Comparison
		want string
	}{
		{
			name: "nil comparison",
			comp: nil,
			want: "",
		},
		{
			name: "empty message",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictInconclusive,
					Message: "",
				},
			},
			want: "",
		},
		{
			name: "inconclusive with candidate task failed capped",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictInconclusive,
					Message: "INCONCLUSIVE: candidate task failed: docs/task_two; otherwise IMPROVED: 1 task(s) improved, 0 unchanged",
				},
			},
			want: "INCONCLUSIVE: candidate task failed: docs/task_two; otherwise IMPROVED: 1 task(s) improved, 0 unchanged",
		},
		{
			name: "inconclusive standard summary",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictInconclusive,
					Message: "INCONCLUSIVE: 1 task(s) inconclusive; more samples recommended",
				},
			},
			want: "INCONCLUSIVE: 1 task(s) inconclusive; more samples recommended",
		},
		{
			name: "inconclusive with skipped tasks",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictInconclusive,
					Message: "INCONCLUSIVE: 1 task(s) inconclusive; more samples recommended; skipped (not planned): docs/task_three",
				},
			},
			want: "INCONCLUSIVE: 1 task(s) inconclusive; more samples recommended; skipped (not planned): docs/task_three",
		},
		{
			name: "regressed with candidate task failed",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictRegressed,
					Message: "REGRESSED: 1 task(s) regressed; candidate task failed: docs/task_two",
				},
			},
			want: "REGRESSED: 1 task(s) regressed; candidate task failed: docs/task_two",
		},
		{
			name: "regressed standard summary",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictRegressed,
					Message: "REGRESSED: 1 task(s) regressed",
				},
			},
			want: "REGRESSED: 1 task(s) regressed",
		},
		{
			name: "improved with no notes returns empty",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictImproved,
					Message: "IMPROVED: 1 task(s) improved, 0 unchanged",
				},
			},
			want: "",
		},
		{
			name: "improved with skipped tasks returns suffix",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictImproved,
					Message: "IMPROVED: 1 task(s) improved, 0 unchanged; skipped (not planned): docs/task_three",
				},
			},
			want: "skipped (not planned): docs/task_three",
		},
		{
			name: "unchanged with no notes returns empty",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictUnchanged,
					Message: "UNCHANGED: all 2 task(s) unchanged",
				},
			},
			want: "",
		},
		{
			name: "unchanged with unexecuted notes returns suffix",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictUnchanged,
					Message: "UNCHANGED: all 1 task(s) unchanged; tasks not executed: docs/task_two; skipped (not planned): docs/task_three",
				},
			},
			want: "tasks not executed: docs/task_two; skipped (not planned): docs/task_three",
		},
		{
			name: "unchanged with failed in both runs note returns suffix",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictUnchanged,
					Message: "UNCHANGED: all 1 task(s) unchanged; failed in both runs: docs/task_two",
				},
			},
			want: "failed in both runs: docs/task_two",
		},
		{
			name: "running with standard summary returns empty",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictRunning,
					Message: "RUNNING: 1 task(s) in progress",
				},
			},
			want: "",
		},
		{
			name: "running with batch info returns message",
			comp: &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: v1alpha1.ComparisonVerdictRunning,
					Message: "Running batch 2 (1 tasks continuing)",
				},
			},
			want: "Running batch 2 (1 tasks continuing)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComparisonNotes(tt.comp)
			if got != tt.want {
				t.Errorf("ComparisonNotes() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCheckRunStateForComparison_Matrix(t *testing.T) {
	tests := []struct {
		name               string
		verdict            v1alpha1.ComparisonVerdict
		pImp               *float64
		candSamples        int
		candLimit          *int
		candTasks          []v1alpha1.TaskStatus
		baseSamples        int
		baseLimit          *int
		baseTasks          []v1alpha1.TaskStatus
		expectedStatus     string
		expectedConclusion string
		expectedTitle      string
	}{
		{
			name:    "RunningWithSamples",
			verdict: v1alpha1.ComparisonVerdictRunning,
			candTasks: []v1alpha1.TaskStatus{
				{ID: "task1"},
			},
			candLimit:   ptr(48),
			candSamples: 24,
			baseTasks: []v1alpha1.TaskStatus{
				{ID: "task1"},
			},
			baseLimit:          ptr(48),
			baseSamples:        0,
			expectedStatus:     "in_progress",
			expectedConclusion: "",
			expectedTitle:      "running · 24 of 96 samples",
		},
		{
			name:               "RunningNoSamplesKnown",
			verdict:            v1alpha1.ComparisonVerdictRunning,
			expectedStatus:     "in_progress",
			expectedConclusion: "",
			expectedTitle:      "running",
		},
		{
			name:               "ImprovedWithP",
			verdict:            v1alpha1.ComparisonVerdictImproved,
			pImp:               ptr(0.98),
			expectedStatus:     "completed",
			expectedConclusion: "success",
			expectedTitle:      "IMPROVED · P(improvement) = 0.98",
		},
		{
			name:               "Unchanged",
			verdict:            v1alpha1.ComparisonVerdictUnchanged,
			pImp:               ptr(0.50),
			expectedStatus:     "completed",
			expectedConclusion: "success",
			expectedTitle:      "UNCHANGED",
		},
		{
			name:               "Inconclusive",
			verdict:            v1alpha1.ComparisonVerdictInconclusive,
			pImp:               ptr(0.26),
			expectedStatus:     "completed",
			expectedConclusion: "neutral",
			expectedTitle:      "INCONCLUSIVE · more samples recommended",
		},
		{
			name:               "Regressed",
			verdict:            v1alpha1.ComparisonVerdictRegressed,
			pImp:               ptr(0.04),
			expectedStatus:     "completed",
			expectedConclusion: "failure",
			expectedTitle:      "REGRESSED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			comp := &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict:      tt.verdict,
					PImprovement: tt.pImp,
				},
			}
			var candRun, baseRun *v1alpha1.Run
			if tt.candSamples > 0 || len(tt.candTasks) > 0 || tt.candLimit != nil {
				candRun = &v1alpha1.Run{
					Spec: v1alpha1.RunSpec{
						Limit: tt.candLimit,
					},
					Status: v1alpha1.RunStatus{
						Summary: &v1alpha1.RunSummary{TotalSamples: tt.candSamples},
						Tasks:   tt.candTasks,
					},
				}
			}
			if tt.baseSamples > 0 || len(tt.baseTasks) > 0 || tt.baseLimit != nil {
				baseRun = &v1alpha1.Run{
					Spec: v1alpha1.RunSpec{
						Limit: tt.baseLimit,
					},
					Status: v1alpha1.RunStatus{
						Summary: &v1alpha1.RunSummary{TotalSamples: tt.baseSamples},
						Tasks:   tt.baseTasks,
					},
				}
			}

			state := CheckRunStateForComparison(comp, baseRun, candRun, "", "")
			if state.Name != CheckRunName {
				t.Errorf("Name = %q, want %q", state.Name, CheckRunName)
			}
			if state.Status != tt.expectedStatus {
				t.Errorf("Status = %q, want %q", state.Status, tt.expectedStatus)
			}
			if state.Conclusion != tt.expectedConclusion {
				t.Errorf("Conclusion = %q, want %q", state.Conclusion, tt.expectedConclusion)
			}
			if state.Title != tt.expectedTitle {
				t.Errorf("Title = %q, want %q", state.Title, tt.expectedTitle)
			}
		})
	}
}

func TestCheckRunStateForComparison_TitleCountsMixed(t *testing.T) {
	tests := []struct {
		name          string
		verdict       v1alpha1.ComparisonVerdict
		tasks         []v1alpha1.ComparisonTaskStatus
		expectedTitle string
	}{
		{
			name:    "AllFourBucketsNonZero",
			verdict: v1alpha1.ComparisonVerdictInconclusive,
			tasks: []v1alpha1.ComparisonTaskStatus{
				{ID: "t1", Label: v1alpha1.ComparisonTaskLabelImproved},
				{ID: "t2", Label: v1alpha1.ComparisonTaskLabelImproved},
				{ID: "t3", Label: v1alpha1.ComparisonTaskLabelUnchanged},
				{ID: "t4", Label: v1alpha1.ComparisonTaskLabelRegressed},
				{ID: "t5", Label: v1alpha1.ComparisonTaskLabelInconclusive},
				{ID: "t6", Label: v1alpha1.ComparisonTaskLabelInconclusive},
				{ID: "t7", Label: v1alpha1.ComparisonTaskLabelInconclusive},
			},
			expectedTitle: "INCONCLUSIVE: 2 improved, 1 unchanged, 1 regressed, 3 inconclusive",
		},
		{
			name:    "OmitZeroBuckets_ImprovedAndRegressedOnly",
			verdict: v1alpha1.ComparisonVerdictInconclusive,
			tasks: []v1alpha1.ComparisonTaskStatus{
				{ID: "t1", Label: v1alpha1.ComparisonTaskLabelImproved},
				{ID: "t2", Label: v1alpha1.ComparisonTaskLabelImproved},
				{ID: "t3", Label: v1alpha1.ComparisonTaskLabelRegressed},
			},
			expectedTitle: "INCONCLUSIVE: 2 improved, 1 regressed",
		},
		{
			name:    "SingleBucket_Regressed",
			verdict: v1alpha1.ComparisonVerdictRegressed,
			tasks: []v1alpha1.ComparisonTaskStatus{
				{ID: "t1", Label: v1alpha1.ComparisonTaskLabelRegressed},
			},
			expectedTitle: "REGRESSED: 1 regressed",
		},
		{
			name:    "SingleBucket_Unchanged",
			verdict: v1alpha1.ComparisonVerdictUnchanged,
			tasks: []v1alpha1.ComparisonTaskStatus{
				{ID: "t1", Label: v1alpha1.ComparisonTaskLabelUnchanged},
				{ID: "t2", Label: v1alpha1.ComparisonTaskLabelUnchanged},
			},
			expectedTitle: "UNCHANGED: 2 unchanged",
		},
		{
			name:    "MixedImprovedAndUnchanged",
			verdict: v1alpha1.ComparisonVerdictImproved,
			tasks: []v1alpha1.ComparisonTaskStatus{
				{ID: "t1", Label: v1alpha1.ComparisonTaskLabelImproved},
				{ID: "t2", Label: v1alpha1.ComparisonTaskLabelImproved},
				{ID: "t3", Label: v1alpha1.ComparisonTaskLabelUnchanged},
			},
			expectedTitle: "IMPROVED: 2 improved, 1 unchanged",
		},
		{
			name:    "NoMatchingBuckets_FallsBackToStandardTitle",
			verdict: v1alpha1.ComparisonVerdictUnchanged,
			tasks: []v1alpha1.ComparisonTaskStatus{
				{ID: "t1", Label: "custom_label"},
			},
			expectedTitle: "UNCHANGED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			comp := &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: tt.verdict,
					Tasks:   tt.tasks,
				},
			}
			state := CheckRunStateForComparison(comp, nil, nil, "", "")
			if state.Title != tt.expectedTitle {
				t.Errorf("Title = %q, want %q", state.Title, tt.expectedTitle)
			}
		})
	}
}

func TestCheckRunStateForComparison_InconclusivePolicy(t *testing.T) {
	tests := []struct {
		name               string
		verdict            v1alpha1.ComparisonVerdict
		policy             string
		expectedStatus     string
		expectedConclusion string
	}{
		{
			name:               "NeutralPolicy_Inconclusive",
			verdict:            v1alpha1.ComparisonVerdictInconclusive,
			policy:             "neutral",
			expectedStatus:     "completed",
			expectedConclusion: "neutral",
		},
		{
			name:               "DefaultEmptyPolicy_Inconclusive",
			verdict:            v1alpha1.ComparisonVerdictInconclusive,
			policy:             "",
			expectedStatus:     "completed",
			expectedConclusion: "neutral",
		},
		{
			name:               "FailurePolicy_Inconclusive",
			verdict:            v1alpha1.ComparisonVerdictInconclusive,
			policy:             "failure",
			expectedStatus:     "completed",
			expectedConclusion: "failure",
		},
		{
			name:               "FailurePolicy_RunningStaysInProgress",
			verdict:            v1alpha1.ComparisonVerdictRunning,
			policy:             "failure",
			expectedStatus:     "in_progress",
			expectedConclusion: "",
		},
		{
			name:               "FailurePolicy_RegressedStaysFailure",
			verdict:            v1alpha1.ComparisonVerdictRegressed,
			policy:             "failure",
			expectedStatus:     "completed",
			expectedConclusion: "failure",
		},
		{
			name:               "FailurePolicy_ImprovedStaysSuccess",
			verdict:            v1alpha1.ComparisonVerdictImproved,
			policy:             "failure",
			expectedStatus:     "completed",
			expectedConclusion: "success",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			comp := &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Verdict: tt.verdict,
				},
			}
			state := CheckRunStateForComparison(comp, nil, nil, tt.policy, "")
			if state.Status != tt.expectedStatus {
				t.Errorf("Status = %q, want %q", state.Status, tt.expectedStatus)
			}
			if state.Conclusion != tt.expectedConclusion {
				t.Errorf("Conclusion = %q, want %q", state.Conclusion, tt.expectedConclusion)
			}
		})
	}
}

func TestCheckRunStateForComparison_SummaryHints(t *testing.T) {
	t.Run("SingleInconclusiveTaskWithoutTaskID", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Verdict: v1alpha1.ComparisonVerdictInconclusive,
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:            "task-1",
						Label:         v1alpha1.ComparisonTaskLabelInconclusive,
						SuggestedNext: 12,
					},
				},
			},
		}
		state := CheckRunStateForComparison(comp, nil, nil, "", "")
		expectedSummary := "INCONCLUSIVE: 1 inconclusive\n\n- comment `/eval more 12` to run 12 more samples"
		if state.Summary != expectedSummary {
			t.Errorf("Summary = %q, want %q", state.Summary, expectedSummary)
		}
	})

	t.Run("MultipleInconclusiveTasksWithTaskID", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Verdict: v1alpha1.ComparisonVerdictInconclusive,
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:            "task-a",
						Label:         v1alpha1.ComparisonTaskLabelInconclusive,
						SuggestedNext: 8,
					},
					{
						ID:            "task-b",
						Label:         v1alpha1.ComparisonTaskLabelInconclusive,
						SuggestedNext: 16,
					},
				},
			},
		}
		state := CheckRunStateForComparison(comp, nil, nil, "", "")
		expectedSummary := "INCONCLUSIVE: 2 inconclusive\n\n- comment `/eval more 8` to run 8 more samples (task-a)\n- comment `/eval more 16` to run 16 more samples (task-b)"
		if state.Summary != expectedSummary {
			t.Errorf("Summary = %q, want %q", state.Summary, expectedSummary)
		}
	})

	t.Run("MessageOverrideWithHints", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Verdict: v1alpha1.ComparisonVerdictInconclusive,
				Message: "candidate task failed: task-a",
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:            "task-b",
						Label:         v1alpha1.ComparisonTaskLabelInconclusive,
						SuggestedNext: 10,
					},
					{
						ID:    "task-a",
						Label: v1alpha1.ComparisonTaskLabelRegressed,
					},
				},
			},
		}
		state := CheckRunStateForComparison(comp, nil, nil, "", "")
		expectedSummary := "candidate task failed: task-a\n\n- comment `/eval more 10` to run 10 more samples (task-b)"
		if state.Summary != expectedSummary {
			t.Errorf("Summary = %q, want %q", state.Summary, expectedSummary)
		}
	})

	t.Run("Over20InconclusiveTasksBounded", func(t *testing.T) {
		var tasks []v1alpha1.ComparisonTaskStatus
		for i := 1; i <= 25; i++ {
			tasks = append(tasks, v1alpha1.ComparisonTaskStatus{
				ID:            fmt.Sprintf("task-%02d", i),
				Label:         v1alpha1.ComparisonTaskLabelInconclusive,
				SuggestedNext: 8,
			})
		}
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Verdict: v1alpha1.ComparisonVerdictInconclusive,
				Tasks:   tasks,
			},
		}
		state := CheckRunStateForComparison(comp, nil, nil, "", "")
		if !strings.Contains(state.Summary, "- ... and 5 more inconclusive tasks (see the Comparison view)") {
			t.Errorf("expected overflow notice in Summary, got:\n%s", state.Summary)
		}
	})
}

func TestReporter_InconclusivePolicyEndToEnd(t *testing.T) {
	ctx := t.Context()
	comp, baseRun, candRun := sameCommitComparisonFixture()
	// sameCommitComparisonFixture has verdict INCONCLUSIVE

	// 1. Reporter with failure policy
	serverFailure := NewFakeGitHubServer()
	defer serverFailure.Close()

	repFailure := NewReporter(serverFailure.Client("test-token"), ReporterOptions{
		InconclusivePolicy: "failure",
	})
	statusFailure, err := repFailure.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	if statusFailure.CheckRunID == 0 {
		t.Fatalf("expected non-zero CheckRunID")
	}
	crsFailure := serverFailure.CheckRuns()
	if len(crsFailure) != 1 {
		t.Fatalf("expected 1 check run, got %d", len(crsFailure))
	}
	if crsFailure[0].Conclusion != "failure" {
		t.Errorf("expected conclusion 'failure', got %q", crsFailure[0].Conclusion)
	}
	if crsFailure[0].Title != "INCONCLUSIVE: 1 unchanged, 1 inconclusive" {
		t.Errorf("expected Title 'INCONCLUSIVE: 1 unchanged, 1 inconclusive', got %q", crsFailure[0].Title)
	}

	// 2. Reporter with neutral policy (default)
	serverNeutral := NewFakeGitHubServer()
	defer serverNeutral.Close()

	repNeutral := NewReporter(serverNeutral.Client("test-token"), ReporterOptions{
		InconclusivePolicy: "neutral",
	})
	statusNeutral, err := repNeutral.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	if statusNeutral.CheckRunID == 0 {
		t.Fatalf("expected non-zero CheckRunID")
	}
	crsNeutral := serverNeutral.CheckRuns()
	if len(crsNeutral) != 1 {
		t.Fatalf("expected 1 check run, got %d", len(crsNeutral))
	}
	if crsNeutral[0].Conclusion != "neutral" {
		t.Errorf("expected conclusion 'neutral', got %q", crsNeutral[0].Conclusion)
	}
}

func TestReporter_LifecycleAndErrors(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	client := server.Client("test-token")
	reporter := NewReporter(client, ReporterOptions{})

	comp, baseRun, candRun := sameCommitComparisonFixture()

	// 1. First post creates comment and check run
	status1, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("first Report failed: %v", err)
	}
	if status1.CommentID == 0 {
		t.Errorf("expected non-zero CommentID, got %d", status1.CommentID)
	}
	if status1.CheckRunID == 0 {
		t.Errorf("expected non-zero CheckRunID, got %d", status1.CheckRunID)
	}
	if status1.LastPostedHash == "" {
		t.Errorf("expected non-empty LastPostedHash")
	}

	comments := server.Comments()
	if len(comments) != 1 {
		t.Fatalf("expected 1 comment, got %d", len(comments))
	}
	if !HasComparisonMarker(comments[0].Body, comp.Namespace, comp.Name) {
		t.Errorf("expected comment to contain marker")
	}

	checkRuns := server.CheckRuns()
	if len(checkRuns) != 1 {
		t.Fatalf("expected 1 check run, got %d", len(checkRuns))
	}
	if checkRuns[0].Conclusion != "neutral" {
		t.Errorf("expected conclusion 'neutral', got %q", checkRuns[0].Conclusion)
	}

	// 2. Idempotency: second call with same status makes 0 HTTP calls
	reqCountBefore := len(server.RequestLog())
	comp.Status.Reporting = status1
	status2, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("idempotent Report failed: %v", err)
	}
	if status2.LastPostedHash != status1.LastPostedHash {
		t.Errorf("expected same hash, got %s vs %s", status2.LastPostedHash, status1.LastPostedHash)
	}
	if len(server.RequestLog()) != reqCountBefore {
		t.Errorf("expected 0 HTTP requests on idempotent Report, got %d new requests", len(server.RequestLog())-reqCountBefore)
	}

	// 3. Status change updates existing comment and check run
	pImpImproved := 0.98
	comp.Status.Verdict = v1alpha1.ComparisonVerdictImproved
	comp.Status.PImprovement = &pImpImproved

	status3, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("updating Report failed: %v", err)
	}
	if status3.CommentID != status1.CommentID {
		t.Errorf("expected same CommentID %d, got %d", status1.CommentID, status3.CommentID)
	}
	if status3.CheckRunID != status1.CheckRunID {
		t.Errorf("expected same CheckRunID %d, got %d", status1.CheckRunID, status3.CheckRunID)
	}
	if status3.LastPostedHash == status1.LastPostedHash {
		t.Errorf("expected new LastPostedHash after verdict change")
	}

	commentsAfter := server.Comments()
	if len(commentsAfter) != 1 {
		t.Errorf("expected still only 1 comment (updated in-place), got %d", len(commentsAfter))
	}
	if !strings.Contains(commentsAfter[0].Body, "IMPROVED · P(improvement) = 0.98") {
		t.Errorf("expected updated comment body to reflect IMPROVED verdict")
	}

	checkRunsAfter := server.CheckRuns()
	if len(checkRunsAfter) != 1 {
		t.Errorf("expected still only 1 check run (updated in-place), got %d", len(checkRunsAfter))
	}
	if checkRunsAfter[0].Conclusion != "success" {
		t.Errorf("expected conclusion 'success' after update, got %q", checkRunsAfter[0].Conclusion)
	}

	// 4. Marker search: when CommentID is lost/zero in status, searches PR comments by marker and updates in place
	compNoID := comp.DeepCopy()
	compNoID.Status.Reporting = nil // lost status

	status4, err := reporter.Report(ctx, compNoID, baseRun, candRun)
	if err != nil {
		t.Fatalf("marker search Report failed: %v", err)
	}
	if status4.CommentID != status1.CommentID {
		t.Errorf("expected recovered CommentID %d via marker, got %d", status1.CommentID, status4.CommentID)
	}
	if len(server.Comments()) != 1 {
		t.Errorf("expected still only 1 comment, marker search should not create duplicate, got %d", len(server.Comments()))
	}

	// 5. Permanent errors (403, 404): returns PermanentError
	server.InjectError(404, `{"message":"Not Found"}`)
	compFresh := comp.DeepCopy()
	compFresh.Status.Reporting = nil
	_, err404 := reporter.Report(ctx, compFresh, baseRun, candRun)
	if err404 == nil || !IsPermanent(err404) {
		t.Errorf("expected PermanentError for 404, got %v", err404)
	}

	server.InjectError(403, `{"message":"Bad credentials"}`)
	_, err403 := reporter.Report(ctx, compFresh, baseRun, candRun)
	if err403 == nil || !IsPermanent(err403) {
		t.Errorf("expected PermanentError for 403, got %v", err403)
	}

	// 6. Retryable errors: 429 rate limit and 5xx server error
	server.InjectError(429, `{"message":"Too Many Requests"}`)
	_, err429 := reporter.Report(ctx, compFresh, baseRun, candRun)
	if err429 == nil || !IsRetryable(err429) {
		t.Errorf("expected Retryable error for 429, got %v", err429)
	}

	server.InjectError(500, `{"message":"Internal Server Error"}`)
	_, err500 := reporter.Report(ctx, compFresh, baseRun, candRun)
	if err500 == nil || !IsRetryable(err500) {
		t.Errorf("expected Retryable error for 500, got %v", err500)
	}
}

func TestRenderComparisonMarkdown_HostileID(t *testing.T) {
	hostileID := "docs/a<img src=x onerror=alert(1)> `b` [c](http://evil)\nnewline <!-- eval-runner:comparison:eval-runner/other -->"
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-comp",
			Namespace: "eval-runner",
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictRunning,
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:    hostileID,
					Label: v1alpha1.ComparisonTaskLabelRunning,
				},
			},
		},
	}

	got := RenderComparisonMarkdown(comp, nil, nil, "")

	// 1. Genuine marker for this comparison must be present
	if !HasComparisonMarker(got, "eval-runner", "my-comp") {
		t.Errorf("expected genuine marker for my-comp to match")
	}

	// 2. Foreign marker injected in task ID must NOT match
	if HasComparisonMarker(got, "eval-runner", "other") {
		t.Errorf("expected foreign marker for 'other' NOT to match")
	}

	// 3. No raw '<' or '>' from the hostile input must survive (except the genuine marker)
	marker := ComparisonMarker("eval-runner", "my-comp")
	bodyWithoutMarker := strings.Replace(got, marker, "", 1)
	if strings.Contains(bodyWithoutMarker, "<") || strings.Contains(bodyWithoutMarker, ">") {
		t.Errorf("found raw '<' or '>' in escaped body: %q", bodyWithoutMarker)
	}

	// 4. Raw link, HTML tags, and backticks must be escaped
	if strings.Contains(got, "<img") {
		t.Errorf("raw <img tag should not be present in output")
	}
	if strings.Contains(got, "[c](http://evil)") {
		t.Errorf("raw markdown link should not be present in output")
	}

	// 5. Table rows must not be split by newlines
	lines := strings.Split(got, "\n")
	tableLines := 0
	for _, l := range lines {
		if strings.HasPrefix(l, "|") {
			tableLines++
		}
	}
	// Header + separator + 1 row = 3 table lines
	if tableLines != 3 {
		t.Errorf("expected 3 table lines (header, sep, row), got %d; newlines were not collapsed", tableLines)
	}
}

func TestReporter_CheckRun403_PartialStatus(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	client := server.Client("pat-token")
	reporter := NewReporter(client, ReporterOptions{})

	comp, baseRun, candRun := sameCommitComparisonFixture()

	// Injected error on POST /check-runs (403 forbidden with PAT)
	// Comments will succeed, but check-runs will fail with 403
	server.InjectPathError("POST", "/repos/gke-labs/eval-runner/check-runs", 403, `{"message":"Resource not accessible by personal access token"}`)

	status, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("expected nil error on check-run 403 (handled gracefully), got: %v", err)
	}
	if status == nil {
		t.Fatalf("expected non-nil partial status")
	}
	if status.CommentID == 0 {
		t.Errorf("expected commentID to be preserved in partial status")
	}
	if status.LastPostedHash == "" {
		t.Errorf("expected lastPostedHash to be preserved in partial status")
	}
	if status.CheckRunID != 0 {
		t.Errorf("expected checkRunID 0, got %d", status.CheckRunID)
	}
	if status.Message != "check runs unavailable with token auth; comment posted" {
		t.Errorf("expected message 'check runs unavailable with token auth; comment posted', got %q", status.Message)
	}

	// Verify comment was created on server
	if len(server.Comments()) != 1 {
		t.Errorf("expected 1 comment created, got %d", len(server.Comments()))
	}

	// Verify idempotency: subsequent reconcile with same status makes 0 requests (gated on hash)
	reqCountBefore := len(server.RequestLog())
	comp.Status.Reporting = status
	status2, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("idempotent report failed: %v", err)
	}
	if status2.LastPostedHash != status.LastPostedHash {
		t.Errorf("expected same hash")
	}
	if len(server.RequestLog()) != reqCountBefore {
		t.Errorf("expected 0 new requests on idempotent call, got %d", len(server.RequestLog())-reqCountBefore)
	}
}

func TestAppAuthentication(t *testing.T) {
	ctx := t.Context()
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate test RSA key: %v", err)
	}

	pkcs1PEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privKey),
	})

	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		t.Fatalf("failed to marshal PKCS8 key: %v", err)
	}
	pkcs8PEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Bytes,
	})

	// 1. Verify key parsing for PKCS#1 and PKCS#8
	if _, err := ParseRSAPrivateKey(pkcs1PEM); err != nil {
		t.Errorf("failed to parse PKCS#1 private key: %v", err)
	}
	if _, err := ParseRSAPrivateKey(pkcs8PEM); err != nil {
		t.Errorf("failed to parse PKCS#8 private key: %v", err)
	}
	if _, err := ParseRSAPrivateKey([]byte("not a pem key")); err == nil {
		t.Errorf("expected error parsing invalid PEM key")
	}

	// 2. Verify JWT generation
	jwt, err := GenerateAppJWT("123456", privKey, time.Now())
	if err != nil {
		t.Fatalf("failed to generate app JWT: %v", err)
	}
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts in JWT, got %d", len(parts))
	}

	// 3. Test AppTokenProvider with fake server
	server := NewFakeGitHubServer()
	defer server.Close()

	server.SetAppSlug("custom-eval-app")

	provider, err := NewAppTokenProvider(AppConfig{
		AppID:          "123456",
		InstallationID: "987654",
		PrivateKeyPEM:  pkcs1PEM,
		BaseURL:        server.URL(),
		HTTPClient:     server.server.Client(),
	})
	if err != nil {
		t.Fatalf("failed to create AppTokenProvider: %v", err)
	}

	// First call exchanges JWT for token
	tok1, err := provider.Token(ctx)
	if err != nil {
		t.Fatalf("Token failed: %v", err)
	}
	if tok1 != "ghs_fake_installation_token" {
		t.Errorf("expected 'ghs_fake_installation_token', got %q", tok1)
	}

	// Second call reuses cached token
	tok2, err := provider.Token(ctx)
	if err != nil {
		t.Fatalf("Token second call failed: %v", err)
	}
	if tok2 != tok1 {
		t.Errorf("expected cached token %q, got %q", tok1, tok2)
	}

	// Test Identity fetches app slug
	identity, err := provider.Identity(ctx)
	if err != nil {
		t.Fatalf("Identity failed: %v", err)
	}
	if identity != "custom-eval-app[bot]" {
		t.Errorf("expected 'custom-eval-app[bot]', got %q", identity)
	}

	// 4. Test Report with AppClient creates check run (not 403)
	appClient, err := server.AppClient("123456", "987654", pkcs1PEM)
	if err != nil {
		t.Fatalf("failed to create AppClient: %v", err)
	}
	reporter := NewReporter(appClient, ReporterOptions{})
	comp, baseRun, candRun := sameCommitComparisonFixture()

	status, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("Report with app auth failed: %v", err)
	}
	if status.CommentID == 0 {
		t.Errorf("expected non-zero CommentID")
	}
	if status.CheckRunID == 0 {
		t.Errorf("expected non-zero CheckRunID with App auth")
	}
	if status.Message != "" {
		t.Errorf("expected empty message on success, got %q", status.Message)
	}
}

func TestListIssueComments_Pagination(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	// Create 5 comments on issue 42
	for i := 1; i <= 5; i++ {
		server.AddCommentWithAuthor("gke-labs", "eval-runner", 42, fmt.Sprintf("comment %d", i), "user1")
	}

	client := server.Client("test-token")
	comments, err := client.ListIssueComments(ctx, "gke-labs", "eval-runner", 42)
	if err != nil {
		t.Fatalf("ListIssueComments failed: %v", err)
	}
	if len(comments) != 5 {
		t.Fatalf("expected 5 comments across pages, got %d", len(comments))
	}
	for i, c := range comments {
		expectedBody := fmt.Sprintf("comment %d", i+1)
		if c.Body != expectedBody {
			t.Errorf("comment %d body = %q, want %q", i, c.Body, expectedBody)
		}
	}

	// Now add a 6th comment that contains our comparison marker
	markerBody := ComparisonMarker("eval-runner", "docs-eval-same-commit") + "\n\nExisting report"
	server.AddCommentWithAuthor("gke-labs", "eval-runner", 42, markerBody, "test-user")

	comp, baseRun, candRun := sameCommitComparisonFixture()
	comp.Spec.PullRequest.Number = 42

	reporter := NewReporter(client, ReporterOptions{})
	status, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	// The reporter should find the comment on later pages rather than creating a duplicate
	if status.CommentID == 0 {
		t.Errorf("expected to find existing comment ID from pagination")
	}
	// Check total comments on server is still 6 (no new comment added)
	if len(server.Comments()) != 6 {
		t.Errorf("expected 6 comments on server (marker found, updated in place), got %d", len(server.Comments()))
	}
}

func TestReporter_AuthorCheck(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	server.SetCurrentUser("eval-runner[bot]")

	comp, baseRun, candRun := sameCommitComparisonFixture()
	comp.Spec.PullRequest.Number = 10

	// 1. Another user (e.g. malicious-user) creates a comment containing the comparison marker
	marker := ComparisonMarker(comp.Namespace, comp.Name)
	foreignCommentID := server.AddCommentWithAuthor("gke-labs", "eval-runner", 10, marker+"\nForeign hijack comment", "malicious-user")

	client := server.Client("bot-token")
	reporter := NewReporter(client, ReporterOptions{})

	// Report must NOT match malicious-user's comment and must create its own comment
	status, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	if status.CommentID == foreignCommentID {
		t.Errorf("reporter hijacked foreign comment authored by malicious-user")
	}

	// Verify that the malicious comment is unchanged
	for _, c := range server.Comments() {
		if c.ID == foreignCommentID {
			if !strings.Contains(c.Body, "Foreign hijack comment") {
				t.Errorf("foreign comment was overwritten: %q", c.Body)
			}
		}
	}

	// 2. A subsequent report matches our own comment and updates it
	comp.Status.Reporting = nil // reset to test marker search with our own comment now present
	status2, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("second Report failed: %v", err)
	}
	if status2.CommentID != status.CommentID {
		t.Errorf("expected to match our own comment ID %d, got %d", status.CommentID, status2.CommentID)
	}
}

func TestRenderComparisonMarkdown_UIBaseURL(t *testing.T) {
	comp, baseRun, candRun := sameCommitComparisonFixture()

	// 1. Without uiBaseURL: link is rendered as code text
	gotDefault := RenderComparisonMarkdown(comp, baseRun, candRun, "")
	if !strings.Contains(gotDefault, "`/comparisons/eval-runner/docs-eval-same-commit`") {
		t.Errorf("expected code-text path when uiBaseURL is empty, got:\n%s", gotDefault)
	}
	if strings.Contains(gotDefault, "[web view]") {
		t.Errorf("unexpected '[web view]' when uiBaseURL is empty")
	}

	// 2. With uiBaseURL: link is rendered as absolute markdown link with repo-scoped pull page
	uiURL := "https://eval.example.com"
	gotURL := RenderComparisonMarkdown(comp, baseRun, candRun, uiURL)
	expectedLink := "[web view](https://eval.example.com/github.com/gke-labs/eval-runner/pulls/98)"
	if !strings.Contains(gotURL, expectedLink) {
		t.Errorf("expected %q when uiBaseURL is set, got:\n%s", expectedLink, gotURL)
	}

	// 3. Trailing slash is normalized
	gotTrailing := RenderComparisonMarkdown(comp, baseRun, candRun, "https://eval.example.com/")
	if !strings.Contains(gotTrailing, expectedLink) {
		t.Errorf("expected trailing slash to be normalized to %q, got:\n%s", expectedLink, gotTrailing)
	}

	// 4. Reporter passes UIBaseURL through to markdown and check run details_url
	server := NewFakeGitHubServer()
	defer server.Close()
	client := server.Client("token")
	reporter := NewReporter(client, ReporterOptions{
		UIBaseURL: "https://eval.example.com",
	})

	status, err := reporter.Report(t.Context(), comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	comments := server.Comments()
	if len(comments) != 1 {
		t.Fatalf("expected 1 comment, got %d", len(comments))
	}
	if !strings.Contains(comments[0].Body, expectedLink) {
		t.Errorf("expected comment on server to contain %q, got:\n%s", expectedLink, comments[0].Body)
	}
	checkRuns := server.CheckRuns()
	if len(checkRuns) != 1 {
		t.Fatalf("expected 1 check run, got %d", len(checkRuns))
	}
	if checkRuns[0].DetailsURL != "https://eval.example.com/github.com/gke-labs/eval-runner/pulls/98" {
		t.Errorf("expected check run DetailsURL to be repo-scoped pull URL, got %q", checkRuns[0].DetailsURL)
	}
	_ = status
}

func TestRenderComparisonMarkdown_SamplesRun(t *testing.T) {
	comp, baseRun, candRun := sameCommitComparisonFixture()
	// Set explicit SamplesRun on the tasks
	comp.Status.Tasks[0].SamplesRun = 32
	comp.Status.Tasks[1].SamplesRun = 16

	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")
	if !strings.Contains(got, "| docs/readme\\_comprehension | 1.00 | 1.00 | +0.00 [+0.00, +0.00] | unchanged | 32 |") {
		t.Errorf("expected markdown to display 32 for task 0 runs, got:\n%s", got)
	}
	if !strings.Contains(got, "| docs/readme\\_comprehension\\_blind | 0.20 | 0.15 | -0.05 [-0.15, +0.00] | inconclusive | 16 |") {
		t.Errorf("expected markdown to display 16 for task 1 runs, got:\n%s", got)
	}
}

func TestRenderComparisonMarkdown_RunMoreSamples(t *testing.T) {
	comp, baseRun, candRun := sameCommitComparisonFixture()
	comp.Status.Tasks[1].Label = v1alpha1.ComparisonTaskLabelInconclusive
	comp.Status.Tasks[1].SuggestedNext = 32

	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")
	if !strings.Contains(got, "- comment `/eval more 32` to run 32 more samples (docs/readme\\_comprehension\\_blind)") {
		t.Errorf("expected markdown to contain action for inconclusive task, got:\n%s", got)
	}

	// Single-task comparison should omit task ID in the hint
	singleComp := comp.DeepCopy()
	singleComp.Status.Tasks = []v1alpha1.ComparisonTaskStatus{comp.Status.Tasks[1]}
	gotSingle := RenderComparisonMarkdown(singleComp, baseRun, candRun, "")
	if !strings.Contains(gotSingle, "- comment `/eval more 32` to run 32 more samples\n") {
		t.Errorf("expected single-task markdown to contain action without task ID, got:\n%s", gotSingle)
	}
}

func TestRenderComparisonMarkdown_SingleTaskAction(t *testing.T) {
	comp, baseRun, candRun := sameCommitComparisonFixture()
	comp.Status.Tasks = []v1alpha1.ComparisonTaskStatus{
		{
			ID:            "single_task",
			Label:         v1alpha1.ComparisonTaskLabelInconclusive,
			SuggestedNext: 20,
			Pairs:         10,
		},
	}
	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")
	if !strings.Contains(got, "- comment `/eval more 20` to run 20 more samples\n") {
		t.Errorf("expected markdown to contain '- comment `/eval more 20` to run 20 more samples', got:\n%s", got)
	}
	if strings.Contains(got, "(single_task)") {
		t.Errorf("expected single-task action to omit task ID, got:\n%s", got)
	}
}

func TestRenderComparisonMarkdown_Tokens(t *testing.T) {
	comp, baseRun, candRun := sameCommitComparisonFixture()
	// Set explicit tokens on task 0 with reasoning, nil on task 1
	comp.Status.Tasks[0].Tokens = &v1alpha1.TaskTokens{
		Input:     600,
		Output:    200,
		Total:     800,
		Reasoning: 150,
	}
	comp.Status.Tasks[1].Tokens = nil

	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")
	if !strings.Contains(got, "| eval | base | this PR | Δ (95% CI) | label | samples | tokens |") {
		t.Errorf("expected markdown table to include tokens header when tokens non-nil, got:\n%s", got)
	}
	if !strings.Contains(got, "| docs/readme\\_comprehension | 1.00 | 1.00 | +0.00 [+0.00, +0.00] | unchanged | 20 | 800 (+R 150) |") {
		t.Errorf("expected markdown to display 800 (+R 150) tokens for task 0, got:\n%s", got)
	}
	if !strings.Contains(got, "| docs/readme\\_comprehension\\_blind | 0.20 | 0.15 | -0.05 [-0.15, +0.00] | inconclusive | 20 | — |") {
		t.Errorf("expected markdown to display '—' tokens for task 1, got:\n%s", got)
	}

	// Task 0 without reasoning displays plain total
	comp.Status.Tasks[0].Tokens.Reasoning = 0
	gotNoReasoning := RenderComparisonMarkdown(comp, baseRun, candRun, "")
	if !strings.Contains(gotNoReasoning, "| docs/readme\\_comprehension | 1.00 | 1.00 | +0.00 [+0.00, +0.00] | unchanged | 20 | 800 |") {
		t.Errorf("expected markdown to display 800 tokens for task 0 when reasoning=0, got:\n%s", gotNoReasoning)
	}

	// Without tokens, tokens column is omitted
	compNoTokens, baseRun2, candRun2 := sameCommitComparisonFixture()
	gotNoTokens := RenderComparisonMarkdown(compNoTokens, baseRun2, candRun2, "")
	if strings.Contains(gotNoTokens, "| tokens |") {
		t.Errorf("expected markdown table to omit tokens column when tokens nil, got:\n%s", gotNoTokens)
	}
}

func TestReporter_CheckRunRetryableError_HashGateNotSwallowed(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	client := server.Client("token")
	reporter := NewReporter(client, ReporterOptions{})

	comp, baseRun, candRun := sameCommitComparisonFixture()

	// Inject 502 Bad Gateway on check-runs
	server.InjectPathError("POST", "/repos/gke-labs/eval-runner/check-runs", http.StatusBadGateway, `{"message":"Server Error"}`)

	// First call: comment succeeds, check-run fails with 502
	status1, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err == nil || !IsRetryable(err) {
		t.Fatalf("expected retryable error on 502, got %v", err)
	}
	if status1 == nil {
		t.Fatalf("expected non-nil partial status")
	}
	if status1.CommentID == 0 {
		t.Errorf("expected commentID to be preserved on retryable check-run failure")
	}
	if status1.LastPostedHash != "" {
		t.Errorf("expected LastPostedHash to be empty on retryable check-run failure, got %q", status1.LastPostedHash)
	}
	if status1.CheckRunID != 0 {
		t.Errorf("expected checkRunID 0, got %d", status1.CheckRunID)
	}

	// Simulate controller persisting partial status on reconcile requeue
	comp.Status.Reporting = status1

	// Second call: check-run error is gone; Report must NOT be swallowed by hash gate,
	// and must retry CreateCheckRun!
	checkRunsBefore := len(server.CheckRuns())
	status2, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}
	if status2.CheckRunID == 0 {
		t.Errorf("expected CheckRunID to be populated on retry")
	}
	if status2.LastPostedHash == "" {
		t.Errorf("expected LastPostedHash to be populated after successful check-run")
	}
	if len(server.CheckRuns()) != checkRunsBefore+1 {
		t.Errorf("expected CreateCheckRun to be executed on retry")
	}

	// Third call: content unchanged and CheckRun succeeded -> hash gate blocks duplicate requests
	reqCountBefore := len(server.RequestLog())
	comp.Status.Reporting = status2
	status3, err := reporter.Report(ctx, comp, baseRun, candRun)
	if err != nil {
		t.Fatalf("third call failed: %v", err)
	}
	if status3.LastPostedHash != status2.LastPostedHash {
		t.Errorf("expected same hash")
	}
	if len(server.RequestLog()) != reqCountBefore {
		t.Errorf("expected 0 new requests on idempotent call, got %d", len(server.RequestLog())-reqCountBefore)
	}
}

func TestAppAuthentication_NoSlugError(t *testing.T) {
	ctx := t.Context()
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate test RSA key: %v", err)
	}

	pkcs1PEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privKey),
	})

	server := NewFakeGitHubServer()
	defer server.Close()
	server.SetAppSlug("") // empty slug

	provider, err := NewAppTokenProvider(AppConfig{
		AppID:          "123456",
		InstallationID: "987654",
		PrivateKeyPEM:  pkcs1PEM,
		BaseURL:        server.URL(),
		HTTPClient:     server.server.Client(),
	})
	if err != nil {
		t.Fatalf("failed to create AppTokenProvider: %v", err)
	}

	_, err = provider.Identity(ctx)
	if err == nil {
		t.Fatalf("expected error when /app returns empty slug, got nil")
	}
	if !IsPermanent(err) {
		t.Errorf("expected PermanentError when /app returns empty slug, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "did not contain an app slug") {
		t.Errorf("expected error message mentioning 'did not contain an app slug', got: %v", err)
	}
}

func TestListIssueComments_RejectsExternalLink(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	// Inject external Link header pointing outside server.URL
	server.InjectPathError("GET", "/repos/gke-labs/eval-runner/issues/1/comments", http.StatusOK, `[]`, map[string]string{
		"Link": `<https://evil.example.com/comments?page=2>; rel="next"`,
	})

	client := server.Client("test-token")
	_, err := client.ListIssueComments(ctx, "gke-labs", "eval-runner", 1)
	if err == nil {
		t.Fatalf("expected error following external link, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to follow link header outside base URL") {
		t.Errorf("expected error message to mention 'refusing to follow link header outside base URL', got: %v", err)
	}
}

func ptr[T any](v T) *T {
	return &v
}

func TestFormatIDList(t *testing.T) {
	if got := FormatIDList(nil, 20); got != "" {
		t.Errorf("expected empty string for nil ids, got %q", got)
	}
	if got := FormatIDList([]string{"a", "b", "c"}, 20); got != "a, b, c" {
		t.Errorf("expected 'a, b, c', got %q", got)
	}

	// Exactly 20 items: no marker
	twenty := make([]string, 20)
	for i := range twenty {
		twenty[i] = fmt.Sprintf("task%02d", i)
	}
	got20 := FormatIDList(twenty, 20)
	if strings.Contains(got20, "more") {
		t.Errorf("expected exactly 20 items to not have marker, got %q", got20)
	}
	if !strings.HasPrefix(got20, "task00, ") || !strings.HasSuffix(got20, ", task19") {
		t.Errorf("unexpected 20 items formatting: %q", got20)
	}

	// 21 items: ... and 1 more
	twentyOne := append(twenty, "task20")
	got21 := FormatIDList(twentyOne, 20)
	if !strings.HasSuffix(got21, ", ... and 1 more") {
		t.Errorf("expected suffix ', ... and 1 more', got %q", got21)
	}

	// 25 items: ... and 5 more
	twentyFive := make([]string, 25)
	for i := range twentyFive {
		twentyFive[i] = fmt.Sprintf("task%02d", i)
	}
	got25 := FormatIDList(twentyFive, 20)
	if !strings.HasSuffix(got25, ", ... and 5 more") {
		t.Errorf("expected suffix ', ... and 5 more', got %q", got25)
	}
	parts := strings.Split(got25, ", ")
	if len(parts) != 21 {
		t.Errorf("expected 21 comma-separated parts (20 items + marker), got %d: %q", len(parts), got25)
	}
}

func TestTruncateNoteIDLists(t *testing.T) {
	if got := TruncateNoteIDLists(""); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}

	// Short list: unchanged
	short := "skipped (not planned): task1, task2"
	if got := TruncateNoteIDLists(short); got != short {
		t.Errorf("expected %q, got %q", short, got)
	}

	// 25 skipped tasks
	tasks25 := make([]string, 25)
	for i := range tasks25 {
		tasks25[i] = fmt.Sprintf("evals/task_%02d", i)
	}
	longSkipped := "skipped (not planned): " + strings.Join(tasks25, ", ")
	gotSkipped := TruncateNoteIDLists(longSkipped)
	if !strings.HasSuffix(gotSkipped, ", ... and 5 more") {
		t.Errorf("expected suffix ', ... and 5 more', got %q", gotSkipped)
	}
	if !strings.HasPrefix(gotSkipped, "skipped (not planned): evals/task_00, ") {
		t.Errorf("unexpected prefix: %q", gotSkipped)
	}

	// Candidate task failed with 25 items changes to candidate tasks failed
	longFailed := "candidate task failed: " + strings.Join(tasks25, ", ")
	gotFailed := TruncateNoteIDLists(longFailed)
	if !strings.HasPrefix(gotFailed, "candidate tasks failed: evals/task_00, ") {
		t.Errorf("expected 'candidate tasks failed' plural prefix, got %q", gotFailed)
	}
	if !strings.HasSuffix(gotFailed, ", ... and 5 more") {
		t.Errorf("expected suffix ', ... and 5 more', got %q", gotFailed)
	}

	// Inconclusive note with candidate task failed
	longInconclusive := "INCONCLUSIVE: candidate task failed: " + strings.Join(tasks25, ", ") + "; otherwise IMPROVED: 1 task(s) improved, 0 unchanged"
	gotInconclusive := TruncateNoteIDLists(longInconclusive)
	if !strings.HasPrefix(gotInconclusive, "INCONCLUSIVE: candidate tasks failed: evals/task_00, ") {
		t.Errorf("expected 'INCONCLUSIVE: candidate tasks failed', got %q", gotInconclusive)
	}
	if !strings.Contains(gotInconclusive, ", ... and 5 more; otherwise IMPROVED: 1 task(s) improved, 0 unchanged") {
		t.Errorf("expected bounded candidate list with remaining clause preserved, got %q", gotInconclusive)
	}

	// Already truncated note is not truncated again
	alreadyTruncated := "skipped (not planned): task00, ... and 5 more"
	if got := TruncateNoteIDLists(alreadyTruncated); got != alreadyTruncated {
		t.Errorf("expected %q, got %q", alreadyTruncated, got)
	}

	// Failed in both runs with 25 items
	longFailedBoth := "failed in both runs: " + strings.Join(tasks25, ", ")
	gotFailedBoth := TruncateNoteIDLists(longFailedBoth)
	if !strings.HasPrefix(gotFailedBoth, "failed in both runs: evals/task_00, ") {
		t.Errorf("expected 'failed in both runs: evals/task_00, ', got %q", gotFailedBoth)
	}
	if !strings.HasSuffix(gotFailedBoth, ", ... and 5 more") {
		t.Errorf("expected suffix ', ... and 5 more', got %q", gotFailedBoth)
	}
}

func TestRenderComparisonMarkdown_500Tasks_Bounded(t *testing.T) {
	const totalTasks = 500
	tasks := make([]v1alpha1.ComparisonTaskStatus, totalTasks)
	rate := 1.0
	delta := 0.0
	for i := 0; i < totalTasks; i++ {
		tasks[i] = v1alpha1.ComparisonTaskStatus{
			ID:            fmt.Sprintf("evals/task_%04d/long_benchmark_scenario_for_evalrunner_%s", i, strings.Repeat("x", 80)),
			BaseRate:      &rate,
			CandidateRate: &rate,
			Delta:         &delta,
			Label:         v1alpha1.ComparisonTaskLabelUnchanged,
			Pairs:         20,
		}
	}

	pImp := 0.50
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-500-tasks",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:        v1alpha1.ComparisonPhaseCompleted,
			Verdict:      v1alpha1.ComparisonVerdictUnchanged,
			PImprovement: &pImp,
			Summary: &v1alpha1.ComparisonSummary{
				Tasks: fmt.Sprintf("%d/%d", totalTasks, totalTasks),
			},
			Tasks: tasks,
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{Commit: "0123456789abcdef"}},
	}
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{Commit: "fedcba9876543210"}},
	}

	got := RenderComparisonMarkdown(comp, baseRun, candRun, "https://eval.example.com")

	if len(got) > MaxCommentBytes {
		t.Fatalf("rendered markdown exceeds limit %d: got %d bytes", MaxCommentBytes, len(got))
	}

	expectedMarkerSuffix := "more tasks (see the Comparison view)"
	if !strings.Contains(got, expectedMarkerSuffix) {
		t.Fatalf("expected rendered markdown to contain marker %q, got:\n%s", expectedMarkerSuffix, got)
	}

	// Verify the table ends with the marker line separated by empty lines
	lines := strings.Split(got, "\n")
	markerLineIndex := -1
	for idx, line := range lines {
		if strings.HasPrefix(line, "... ") && strings.HasSuffix(line, expectedMarkerSuffix) {
			markerLineIndex = idx
			break
		}
	}
	if markerLineIndex == -1 {
		t.Fatalf("marker line not found in rendered markdown lines")
	}

	// The line before the marker line should be empty (separating table from marker)
	if markerLineIndex-1 < 0 || lines[markerLineIndex-1] != "" {
		t.Errorf("expected empty line before marker line, got %q", lines[markerLineIndex-1])
	}
	// The line following the marker line should be empty (separating marker from footer)
	if markerLineIndex+1 >= len(lines) || lines[markerLineIndex+1] != "" {
		t.Errorf("expected empty line after marker line, got %q", lines[markerLineIndex+1])
	}
}

func TestRenderComparisonMarkdown_500InconclusiveTasks_Bounded(t *testing.T) {
	const totalTasks = 500
	tasks := make([]v1alpha1.ComparisonTaskStatus, totalTasks)
	rate := 0.20
	delta := 0.0
	for i := 0; i < totalTasks; i++ {
		tasks[i] = v1alpha1.ComparisonTaskStatus{
			ID:            fmt.Sprintf("evals/task_%04d/long_benchmark_scenario_for_evalrunner_%s", i, strings.Repeat("x", 60)),
			BaseRate:      &rate,
			CandidateRate: &rate,
			Delta:         &delta,
			Label:         v1alpha1.ComparisonTaskLabelInconclusive,
			Pairs:         16,
			SuggestedNext: 16,
		}
	}

	pImp := 0.50
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-500-inconclusive",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:        v1alpha1.ComparisonPhaseCompleted,
			Verdict:      v1alpha1.ComparisonVerdictInconclusive,
			PImprovement: &pImp,
			Message:      "INCONCLUSIVE: 500 task(s) inconclusive; more samples recommended",
			Summary: &v1alpha1.ComparisonSummary{
				Tasks: fmt.Sprintf("%d/%d", totalTasks, totalTasks),
			},
			Tasks: tasks,
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{Commit: "0123456789abcdef"}},
	}
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{Commit: "fedcba9876543210"}},
	}

	got := RenderComparisonMarkdown(comp, baseRun, candRun, "https://eval.example.com")

	if len(got) > MaxCommentBytes {
		t.Fatalf("rendered markdown exceeds limit %d: got %d bytes", MaxCommentBytes, len(got))
	}

	// Verify actions block is capped with the inconclusive tasks marker
	expectedActionMarker := fmt.Sprintf("- ... and %d more inconclusive tasks (see the Comparison view)", totalTasks-MaxIDList)
	if !strings.Contains(got, expectedActionMarker) {
		t.Errorf("expected rendered markdown to contain actions marker %q, got:\n%s", expectedActionMarker, got)
	}

	// Verify table marker is also present and bounded
	expectedTableMarker := "more tasks (see the Comparison view)"
	if !strings.Contains(got, expectedTableMarker) {
		t.Errorf("expected rendered markdown to contain table marker %q, got:\n%s", expectedTableMarker, got)
	}
}

func TestRenderComparisonMarkdown_WiderTableTruncation(t *testing.T) {
	const totalTasks = 500
	tasks := make([]v1alpha1.ComparisonTaskStatus, totalTasks)
	rate := 0.85
	delta := 0.20
	pImpTask := 0.95
	for i := 0; i < totalTasks; i++ {
		tasks[i] = v1alpha1.ComparisonTaskStatus{
			ID:            fmt.Sprintf("evals/task_%04d/long_scenario_%s", i, strings.Repeat("y", 70)),
			BaseRate:      &rate,
			CandidateRate: &rate,
			Delta:         &delta,
			DeltaCI: &v1alpha1.DeltaConfidenceInterval{
				Lower: 0.10,
				Upper: 0.30,
			},
			Label:        v1alpha1.ComparisonTaskLabelImproved,
			Pairs:        20,
			Wins:         12,
			Losses:       3,
			Ties:         5,
			PImprovement: &pImpTask,
			Tokens: &v1alpha1.TaskTokens{
				Input:     500,
				Output:    200,
				Total:     700,
				Reasoning: 50,
			},
		}
	}

	overallPImp := 0.99
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "docs-eval-wider-table",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:        v1alpha1.ComparisonPhaseCompleted,
			Verdict:      v1alpha1.ComparisonVerdictImproved,
			PImprovement: &overallPImp,
			Summary: &v1alpha1.ComparisonSummary{
				Tasks: fmt.Sprintf("%d/%d", totalTasks, totalTasks),
			},
			Tasks: tasks,
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{Commit: "0123456789abcdef"}},
	}
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{Commit: "fedcba9876543210"}},
	}

	got := RenderComparisonMarkdown(comp, baseRun, candRun, "https://eval.example.com")

	if len(got) > MaxCommentBytes {
		t.Fatalf("rendered markdown exceeds limit %d: got %d bytes", MaxCommentBytes, len(got))
	}

	// Verify wider table headers are present
	if !strings.Contains(got, "| eval | base | this PR | Δ (95% CI) | label | samples | tokens | W/L/T | P(imp) |") {
		t.Errorf("expected wider table header with W/L/T and P(imp), got:\n%s", got)
	}

	// Verify truncation marker
	expectedMarkerSuffix := "more tasks (see the Comparison view)"
	if !strings.Contains(got, expectedMarkerSuffix) {
		t.Fatalf("expected rendered markdown to contain marker %q, got:\n%s", expectedMarkerSuffix, got)
	}
}

func TestRenderComparisonMarkdown_InconclusiveActions_SingularVsPlural(t *testing.T) {
	// Exactly 21 inconclusive tasks -> 20 rendered + 1 more inconclusive task
	const totalTasks = 21
	tasks := make([]v1alpha1.ComparisonTaskStatus, totalTasks)
	rate := 0.20
	delta := 0.0
	for i := 0; i < totalTasks; i++ {
		tasks[i] = v1alpha1.ComparisonTaskStatus{
			ID:            fmt.Sprintf("task_%02d", i),
			BaseRate:      &rate,
			CandidateRate: &rate,
			Delta:         &delta,
			Label:         v1alpha1.ComparisonTaskLabelInconclusive,
			Pairs:         16,
			SuggestedNext: 16,
		}
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "inconclusive-singular", Namespace: "eval-runner"},
		Status: v1alpha1.ComparisonStatus{
			Phase:   v1alpha1.ComparisonPhaseCompleted,
			Verdict: v1alpha1.ComparisonVerdictInconclusive,
			Tasks:   tasks,
		},
	}
	baseRun := &v1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "base", Namespace: "eval-runner"}}
	candRun := &v1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "cand", Namespace: "eval-runner"}}

	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")
	expectedSingular := "- ... and 1 more inconclusive task (see the Comparison view)"
	if !strings.Contains(got, expectedSingular) {
		t.Errorf("expected singular marker %q, got:\n%s", expectedSingular, got)
	}
}

func TestRenderComparisonMarkdown_3Tasks_ByteIdenticalToGolden(t *testing.T) {
	comp, baseRun, candRun := batchesTokensComparisonFixture()
	if len(comp.Status.Tasks) != 3 {
		t.Fatalf("expected 3 tasks in fixture, got %d", len(comp.Status.Tasks))
	}

	got := RenderComparisonMarkdown(comp, baseRun, candRun, "")

	goldenPath := "testdata/batches_tokens_comparison.golden.md"
	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file: %v", err)
	}
	want := string(wantBytes)

	if got != want {
		t.Errorf("RenderComparisonMarkdown mismatch (-got +want):\nGOT:\n%s\nWANT:\n%s", got, want)
	}
}

func TestCreateCommentReaction(t *testing.T) {
	server := NewFakeGitHubServer()
	defer server.Close()

	client := server.Client("test-token")
	ctx := t.Context()

	// Create reactions on a comment
	if err := client.CreateCommentReaction(ctx, "gke-labs", "eval-runner", 101, "eyes"); err != nil {
		t.Fatalf("failed to create eyes reaction: %v", err)
	}
	if err := client.CreateCommentReaction(ctx, "gke-labs", "eval-runner", 102, "confused"); err != nil {
		t.Fatalf("failed to create confused reaction: %v", err)
	}

	reactions101 := server.CommentReactions(101)
	if len(reactions101) != 1 || reactions101[0] != "eyes" {
		t.Errorf("expected [eyes] on comment 101, got %v", reactions101)
	}

	reactions102 := server.CommentReactions(102)
	if len(reactions102) != 1 || reactions102[0] != "confused" {
		t.Errorf("expected [confused] on comment 102, got %v", reactions102)
	}
}

func TestListPullRequestFiles_LinkPaging(t *testing.T) {
	server := NewFakeGitHubServer()
	defer server.Close()

	// 150 files across two pages (100 per page)
	files := make([]string, 150)
	for i := range files {
		files[i] = fmt.Sprintf("docs/file_%03d.md", i)
	}
	server.SetPullRequestFiles("gke-labs", "eval-runner", 42, files)

	client := server.Client("test-token")
	ctx := t.Context()

	got, truncated, err := client.ListPullRequestFiles(ctx, "gke-labs", "eval-runner", 42)
	if err != nil {
		t.Fatalf("ListPullRequestFiles error: %v", err)
	}
	if truncated {
		t.Errorf("expected truncated == false for 150 files, got true")
	}
	if len(got) != 150 {
		t.Fatalf("expected 150 files, got %d", len(got))
	}
	for i, f := range got {
		if f != files[i] {
			t.Errorf("file[%d] mismatch: got %q, want %q", i, f, files[i])
		}
	}
}

func TestListPullRequestFiles_Cap(t *testing.T) {
	server := NewFakeGitHubServer()
	defer server.Close()

	// 350 files (exceeding MaxPullRequestFiles = 300)
	files := make([]string, 350)
	for i := range files {
		files[i] = fmt.Sprintf("src/file_%03d.go", i)
	}
	server.SetPullRequestFiles("gke-labs", "eval-runner", 43, files)

	client := server.Client("test-token")
	ctx := t.Context()

	got, truncated, err := client.ListPullRequestFiles(ctx, "gke-labs", "eval-runner", 43)
	if err != nil {
		t.Fatalf("ListPullRequestFiles error: %v", err)
	}
	if !truncated {
		t.Errorf("expected truncated == true for 350 files, got false")
	}
	if len(got) != MaxPullRequestFiles {
		t.Fatalf("expected capped at %d files, got %d", MaxPullRequestFiles, len(got))
	}
	for i, f := range got {
		if f != files[i] {
			t.Errorf("file[%d] mismatch: got %q, want %q", i, f, files[i])
		}
	}
}

func TestListPullRequestFiles_InjectableError(t *testing.T) {
	server := NewFakeGitHubServer()
	defer server.Close()

	server.InjectPathError(http.MethodGet, "/repos/gke-labs/eval-runner/pulls/44/files", http.StatusInternalServerError, `{"message": "internal error"}`)

	client := server.Client("test-token")
	ctx := t.Context()

	_, _, err := client.ListPullRequestFiles(ctx, "gke-labs", "eval-runner", 44)
	if err == nil {
		t.Fatal("expected error for injected 500, got nil")
	}
}

func TestRenderComparisonMarkdown_WithAffinity(t *testing.T) {
	delta := 0.20
	candMean := 0.80
	baseMean := 0.60
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-affinity-render", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     99,
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictImproved,
			Phase:   v1alpha1.ComparisonPhaseCompleted,
			Affinity: &v1alpha1.ComparisonAffinityStatus{
				FilesCount: 2,
				FilesHash:  "sha256:123456",
			},
			Summary: &v1alpha1.ComparisonSummary{
				Tasks:           "2/2",
				AffinityMatched: 1,
			},
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:            "docs/readme",
					Affinity:      "matched",
					BaseRate:      &baseMean,
					CandidateRate: &candMean,
					Delta:         &delta,
					DeltaCI:       &v1alpha1.DeltaConfidenceInterval{Lower: 0.05, Upper: 0.35},
					Label:         v1alpha1.ComparisonTaskLabelImproved,
					SamplesRun:    16,
				},
				{
					ID:         "src/logic",
					Affinity:   "unmatched",
					Label:      v1alpha1.ComparisonTaskLabelUnchanged,
					SamplesRun: 16,
				},
			},
		},
	}

	markdown := RenderComparisonMarkdown(comp, nil, nil, "https://eval-ui.example.com")

	if !strings.Contains(markdown, "| eval | affinity | base | this PR | Δ (95% CI) | label | samples | W/L/T |") {
		t.Errorf("expected affinity column in markdown table header, got:\n%s", markdown)
	}
	if !strings.Contains(markdown, "[docs/readme](https://eval-ui.example.com/github.com/gke-labs/eval-runner/runs/cand-run/tasks/docs%2Freadme/samples) | matched |") {
		t.Errorf("expected row with matched affinity, got:\n%s", markdown)
	}
	if !strings.Contains(markdown, "[src/logic](https://eval-ui.example.com/github.com/gke-labs/eval-runner/runs/cand-run/tasks/src%2Flogic/samples) | unmatched |") {
		t.Errorf("expected row with unmatched affinity, got:\n%s", markdown)
	}
	if strings.Contains(markdown, "/runs/eval-runner/") || strings.Contains(markdown, "/comparisons/eval-runner/") {
		t.Errorf("expected no legacy namespaced routes in markdown, got:\n%s", markdown)
	}
	if !strings.Contains(markdown, "1 matched") {
		t.Errorf("expected footer to mention '1 matched', got:\n%s", markdown)
	}
}

func TestReporter_PostUnserviceableNote(t *testing.T) {
	ctx := t.Context()
	server := NewFakeGitHubServer()
	defer server.Close()

	client := server.Client("test-token")
	reporter := NewReporter(client, ReporterOptions{})

	// 1. Nil comparison or nil pull request is no-op
	if err := reporter.PostUnserviceableNote(ctx, nil, "artifact prefix mismatch"); err != nil {
		t.Errorf("expected nil error for nil comparison, got: %v", err)
	}
	compNoPR := &v1alpha1.Comparison{}
	if err := reporter.PostUnserviceableNote(ctx, compNoPR, "artifact prefix mismatch"); err != nil {
		t.Errorf("expected nil error for nil pull request, got: %v", err)
	}

	// 2. Invalid repository format returns error
	compInvalidRepo := &v1alpha1.Comparison{
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "invalid-format",
				Number:     42,
			},
		},
	}
	if err := reporter.PostUnserviceableNote(ctx, compInvalidRepo, "artifact prefix mismatch"); err == nil {
		t.Errorf("expected error for invalid repository format, got nil")
	}

	// 3. Valid comparison posts note to PR
	comp := &v1alpha1.Comparison{
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     42,
			},
		},
	}
	if err := reporter.PostUnserviceableNote(ctx, comp, "artifact prefix mismatch"); err != nil {
		t.Fatalf("unexpected error posting note: %v", err)
	}

	comments := server.Comments()
	if len(comments) != 1 {
		t.Fatalf("expected 1 comment created, got %d", len(comments))
	}
	expected := "Extra samples cannot be run: artifact prefix mismatch."
	if comments[0].Body != expected {
		t.Errorf("expected comment body %q, got %q", expected, comments[0].Body)
	}
}

func TestHTTPClient_ETagRoundTrip_304Parsed(t *testing.T) {
	requestCount := 0
	var receivedIfNoneMatch string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		receivedIfNoneMatch = r.Header.Get("If-None-Match")

		if r.Header.Get("If-None-Match") == `"v1-etag"` {
			w.Header().Set("ETag", `"v1-etag"`)
			w.Header().Set("X-RateLimit-Limit", "60")
			w.Header().Set("X-RateLimit-Remaining", "58")
			w.Header().Set("X-RateLimit-Reset", "1761138000")
			w.WriteHeader(http.StatusNotModified)
			return
		}

		w.Header().Set("ETag", `"v1-etag"`)
		w.Header().Set("X-RateLimit-Limit", "60")
		w.Header().Set("X-RateLimit-Remaining", "59")
		w.Header().Set("X-RateLimit-Reset", "1761138000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"message":"hello"}`))
	}))
	defer ts.Close()

	client := NewHTTPClient(HTTPClientConfig{
		BaseURL: ts.URL,
	})

	ctx := t.Context()

	// 1. Initial request without ETag
	var out map[string]string
	headers, notModified, err := client.DoRequestConditional(ctx, http.MethodGet, "/test", nil, &out, "", "")
	if err != nil {
		t.Fatalf("initial request failed: %v", err)
	}
	if notModified {
		t.Errorf("expected notModified to be false on 200 OK")
	}
	etag := headers.Get("ETag")
	if etag != `"v1-etag"` {
		t.Errorf("expected ETag %q, got %q", `"v1-etag"`, etag)
	}
	if out["message"] != "hello" {
		t.Errorf("expected message 'hello', got %q", out["message"])
	}
	rl := client.RateLimit()
	if rl.Remaining != 59 {
		t.Errorf("expected rate limit remaining 59, got %d", rl.Remaining)
	}

	// 2. Second request with If-None-Match ETag
	headers2, notModified2, err := client.DoRequestConditional(ctx, http.MethodGet, "/test", nil, nil, etag, "")
	if err != nil {
		t.Fatalf("conditional request failed: %v", err)
	}
	if !notModified2 {
		t.Errorf("expected notModified to be true on 304 Not Modified")
	}
	if receivedIfNoneMatch != `"v1-etag"` {
		t.Errorf("server did not receive If-None-Match header: got %q", receivedIfNoneMatch)
	}
	if headers2.Get("ETag") != `"v1-etag"` {
		t.Errorf("expected response ETag in 304, got %q", headers2.Get("ETag"))
	}

	// 3. Assert rate limits updated from 304 response
	rl2 := client.RateLimit()
	if rl2.Remaining != 58 {
		t.Errorf("expected rate limit remaining 58 from 304, got %d", rl2.Remaining)
	}
}

func TestHTTPClient_RateLimitBudget_BackoffBelowFloor(t *testing.T) {
	serverHits := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverHits++
		w.Header().Set("X-RateLimit-Limit", "100")
		w.Header().Set("X-RateLimit-Remaining", "10") // 10% floor is 10
		w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(10*time.Minute).Unix()))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	client := NewHTTPClient(HTTPClientConfig{
		BaseURL:               ts.URL,
		RateLimitFloorPercent: 10,
	})

	ctx := t.Context()

	// 1. Initial request sets rate limit: limit 100, remaining 10, reset +10m
	if err := client.DoRequest(ctx, http.MethodGet, "/test", nil, nil); err != nil {
		t.Fatalf("initial request failed: %v", err)
	}
	if serverHits != 1 {
		t.Fatalf("expected 1 server hit, got %d", serverHits)
	}

	// 2. Next request: remaining 10 is <= 10% floor (10), reset in future
	// Must back off without hitting the server
	err := client.DoRequest(ctx, http.MethodGet, "/test", nil, nil)
	if err == nil {
		t.Fatalf("expected rate limit budget floor error, got nil")
	}
	var rle *RateLimitError
	if !errors.As(err, &rle) {
		t.Fatalf("expected *RateLimitError, got %T: %v", err, err)
	}
	if serverHits != 1 {
		t.Errorf("server was hit during backoff! hits=%d", serverHits)
	}
	if rle.RetryAfter <= 0 {
		t.Errorf("expected positive RetryAfter, got %v", rle.RetryAfter)
	}

	// 3. If reset is in the past, request is allowed through
	client.rateMu.Lock()
	client.rateLimit.Reset = time.Now().Add(-1 * time.Second)
	client.rateMu.Unlock()

	if err := client.DoRequest(ctx, http.MethodGet, "/test", nil, nil); err != nil {
		t.Fatalf("request after reset failed: %v", err)
	}
	if serverHits != 2 {
		t.Errorf("expected 2 server hits after reset, got %d", serverHits)
	}
}

func TestHTTPClient_RateLimitLoggingOneLinePerStateTransition(t *testing.T) {
	logMessages := []string{}
	client := NewHTTPClient(HTTPClientConfig{
		BaseURL: "http://example.com",
		RateLimitLogger: func(format string, args ...any) {
			logMessages = append(logMessages, fmt.Sprintf(format, args...))
		},
	})

	// 1. Initial response: remaining 59 (> floor 6) -> no state transition (normal state)
	h1 := http.Header{}
	h1.Set("X-RateLimit-Limit", "60")
	h1.Set("X-RateLimit-Remaining", "59")
	h1.Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()))
	client.updateRateLimits(h1)

	// 2. Decrements above floor (58, 57, 10) -> no state transition
	for _, rem := range []string{"58", "57", "10", "7"} {
		h := http.Header{}
		h.Set("X-RateLimit-Limit", "60")
		h.Set("X-RateLimit-Remaining", rem)
		h.Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()))
		client.updateRateLimits(h)
	}
	if len(logMessages) != 0 {
		t.Fatalf("expected 0 log messages while above floor, got %d: %v", len(logMessages), logMessages)
	}

	// 3. Drops below floor (5 remaining <= 6 floor) -> transition to suppressed: exactly 1 log line
	hLow := http.Header{}
	hLow.Set("X-RateLimit-Limit", "60")
	hLow.Set("X-RateLimit-Remaining", "5")
	hLow.Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()))
	client.updateRateLimits(hLow)
	if len(logMessages) != 1 {
		t.Fatalf("expected 1 log message upon entering suppressed state, got %d: %v", len(logMessages), logMessages)
	}
	if !strings.Contains(logMessages[0], "budget floor reached") {
		t.Errorf("expected log message to mention budget floor reached, got %q", logMessages[0])
	}

	// 4. Stays below floor (4, 3 remaining) -> no state transition, 0 additional logs
	for _, rem := range []string{"4", "3", "2"} {
		h := http.Header{}
		h.Set("X-RateLimit-Limit", "60")
		h.Set("X-RateLimit-Remaining", rem)
		h.Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()))
		client.updateRateLimits(h)
	}
	if len(logMessages) != 1 {
		t.Fatalf("expected still exactly 1 log message while staying suppressed, got %d: %v", len(logMessages), logMessages)
	}

	// 5. Window reset occurs, remaining recovered (60 remaining > 6 floor) -> transition to normal: exactly 1 log line
	hReset := http.Header{}
	hReset.Set("X-RateLimit-Limit", "60")
	hReset.Set("X-RateLimit-Remaining", "60")
	hReset.Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(2*time.Hour).Unix()))
	client.updateRateLimits(hReset)

	if len(logMessages) != 2 {
		t.Fatalf("expected exactly 2 log messages across both state transitions, got %d: %v", len(logMessages), logMessages)
	}
	if !strings.Contains(logMessages[1], "budget recovered") {
		t.Errorf("expected second log message to mention budget recovered, got %q", logMessages[1])
	}
}

type errorCloseBody struct {
	io.Reader
	closeErr error
}

func (b *errorCloseBody) Close() error {
	return b.closeErr
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestHTTPClient_CloseBodyError(t *testing.T) {
	ctx := t.Context()

	// 1. When main path succeeds, close error is returned wrapped
	client := NewHTTPClient(HTTPClientConfig{
		BaseURL: "http://example.com",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body: &errorCloseBody{
						Reader:   strings.NewReader(`{}`),
						closeErr: fmt.Errorf("simulated close error"),
					},
				}, nil
			}),
		},
	})

	err := client.DoRequest(ctx, http.MethodGet, "/test", nil, nil)
	if err == nil {
		t.Fatalf("expected close error, got nil")
	}
	if !strings.Contains(err.Error(), "close response body: simulated close error") {
		t.Errorf("expected wrapped close error, got %v", err)
	}

	// 2. When main path has HTTP error, HTTP error is preserved and not overwritten by close error
	clientErr := NewHTTPClient(HTTPClientConfig{
		BaseURL: "http://example.com",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body: &errorCloseBody{
						Reader:   strings.NewReader(`{"message": "not found"}`),
						closeErr: fmt.Errorf("simulated close error"),
					},
				}, nil
			}),
		},
	})

	err2 := clientErr.DoRequest(ctx, http.MethodGet, "/test", nil, nil)
	if err2 == nil {
		t.Fatalf("expected error, got nil")
	}
	var permErr *PermanentError
	if !errors.As(err2, &permErr) || permErr.StatusCode != http.StatusNotFound {
		t.Errorf("expected PermanentError with 404, got %v", err2)
	}
}

func TestCheckRunDetailsURL(t *testing.T) {
	const uiBase = "https://eval.example.com"

	// 1. Comparison with PullRequest spec
	compWithPR := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-pr", Namespace: "eval-ns"},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     416,
			},
		},
	}
	got := CheckRunDetailsURL(compWithPR, uiBase)
	want := "https://eval.example.com/github.com/gke-labs/eval-runner/pulls/416"
	if got != want {
		t.Errorf("CheckRunDetailsURL(compWithPR) = %q, want %q", got, want)
	}
	if strings.Contains(got, "/comparisons/eval-ns") || strings.Contains(got, "/runs/eval-ns") {
		t.Errorf("expected no namespaced paths in DetailsURL, got %q", got)
	}

	// 2. Comparison with no PR spec, but target-repo label
	compWithTargetRepo := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "comp-no-pr",
			Namespace: "eval-ns",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/target-repo": "gke-labs_eval-runner",
			},
		},
	}
	gotNoPR := CheckRunDetailsURL(compWithTargetRepo, uiBase)
	wantNoPR := "https://eval.example.com/github.com/gke-labs/eval-runner/comparisons/comp-no-pr"
	if gotNoPR != wantNoPR {
		t.Errorf("CheckRunDetailsURL(compWithTargetRepo) = %q, want %q", gotNoPR, wantNoPR)
	}

	// 3. Comparison with no PR and no repo label falls back to namespaced comparison route
	compBare := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-bare", Namespace: "eval-ns"},
	}
	gotBare := CheckRunDetailsURL(compBare, uiBase)
	wantBare := "https://eval.example.com/comparisons/eval-ns/comp-bare"
	if gotBare != wantBare {
		t.Errorf("CheckRunDetailsURL(compBare) = %q, want %q", gotBare, wantBare)
	}

	// 4. Empty uiBaseURL produces empty string
	if gotEmpty := CheckRunDetailsURL(compWithPR, ""); gotEmpty != "" {
		t.Errorf("expected empty string for empty uiBaseURL, got %q", gotEmpty)
	}
}

func TestSanitizeArtifactPaths(t *testing.T) {
	tests := []struct {
		name string
		in   string
		runs []*v1alpha1.Run
		want string
	}{
		{
			name: "local on-disk path with /tmp/",
			in:   `failed to load baseline log for task "task1" (/tmp/artifacts/runs/dd24bfd/inspect.json): artifact not found: open /tmp/artifacts/runs/dd24bfd/inspect.json: no such file or directory`,
			want: `failed to load baseline log for task "task1" (inspect.json): artifact not found: open inspect.json: no such file or directory`,
		},
		{
			name: "GCS gs:// URI",
			in:   `failed to load baseline log for task "task1" (gs://eval-bucket/runs/dd24bfd/inspect.json): artifact not found: storage: object doesn't exist`,
			want: `failed to load baseline log for task "task1" (inspect.json): artifact not found: storage: object doesn't exist`,
		},
		{
			name: "prefix mismatch message with /tmp/",
			in:   `baseline log path "/tmp/artifacts/runs/other/log.json" does not match commit prefix "dd24bfd"`,
			want: `baseline log path "log.json" does not match commit prefix "dd24bfd"`,
		},
		{
			name: "prefix mismatch message with gs://",
			in:   `candidate log path "gs://eval-bucket/runs/other/log.json" does not match commit prefix "dd24bfd"`,
			want: `candidate log path "log.json" does not match commit prefix "dd24bfd"`,
		},
		{
			name: "task ID with multiple slashes is not mangled",
			in:   `failed to load baseline log for task "a/b/c" (/tmp/artifacts/runs/dd24bfd/x.json): artifact not found`,
			want: `failed to load baseline log for task "a/b/c" (x.json): artifact not found`,
		},
		{
			name: "arbitrary HTTPS URLs in error are not corrupted",
			in:   `failed to fetch: Get "https://storage.googleapis.com/bucket/runs/abc1234/inspect.json": dial tcp: i/o timeout`,
			want: `failed to fetch: Get "https://storage.googleapis.com/bucket/runs/abc1234/inspect.json": dial tcp: i/o timeout`,
		},
		{
			name: "known run task LogPath replaced",
			in:   `failed to load baseline log for task "evals/task1" (https://custom.storage.com/runs/123/eval.json): not found`,
			runs: []*v1alpha1.Run{
				{
					Status: v1alpha1.RunStatus{
						Tasks: []v1alpha1.TaskStatus{
							{ID: "evals/task1", LogPath: "https://custom.storage.com/runs/123/eval.json"},
						},
					},
				},
			},
			want: `failed to load baseline log for task "evals/task1" (eval.json): not found`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SanitizeArtifactPaths(tc.in, tc.runs...)
			if got != tc.want {
				t.Errorf("SanitizeArtifactPaths() = %q, want %q", got, tc.want)
			}
			if strings.Contains(got, "/tmp/") {
				t.Errorf("SanitizeArtifactPaths() output contains '/tmp/': %q", got)
			}
			if strings.Contains(got, "gs://") {
				t.Errorf("SanitizeArtifactPaths() output contains 'gs://': %q", got)
			}
		})
	}
}

func TestCheckRunStateForComparison_ScoringFailure(t *testing.T) {
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-fail", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline: v1alpha1.ComparisonRunRef{Run: "base-run-dd24bfd"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     416,
				BaseSHA:    "dd24bfd11111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictInconclusive,
			Message: `failed to load baseline log for task "triage/lookup" (/tmp/artifacts/runs/dd24bfd/inspect.json): artifact not found: open /tmp/artifacts/runs/dd24bfd/inspect.json: no such file or directory`,
			Conditions: []metav1.Condition{
				{
					Type:   v1alpha1.ConditionTypeArtifactsReadable,
					Status: metav1.ConditionFalse,
					Reason: v1alpha1.ConditionReasonNotFound,
				},
			},
		},
	}

	state := CheckRunStateForComparison(comp, nil, nil, "", "https://eval.example.com")

	wantHeadline := "Baseline results for `dd24bfd` are unavailable; eval-runner will need to re-run the baseline."
	if state.Title != wantHeadline {
		t.Errorf("state.Title = %q, want %q", state.Title, wantHeadline)
	}
	if state.Summary != wantHeadline {
		t.Errorf("state.Summary = %q, want %q", state.Summary, wantHeadline)
	}
	wantText := `failed to load baseline log for task "triage/lookup" (inspect.json): artifact not found: open inspect.json: no such file or directory`
	if !strings.Contains(state.Text, wantText) {
		t.Errorf("state.Text expected to contain %q, got %q", wantText, state.Text)
	}
	if !strings.Contains(state.Text, "<details>") {
		t.Errorf("state.Text expected to contain collapsed details, got %q", state.Text)
	}
	if !strings.Contains(state.Text, "| eval |") {
		t.Errorf("state.Text expected to contain table, got %q", state.Text)
	}
	if strings.Contains(state.Text, "/comparisons/eval-runner") || strings.Contains(state.Text, "/runs/eval-runner") {
		t.Errorf("expected no namespaced paths in state.Text, got %q", state.Text)
	}
	if strings.Contains(state.Title, "/tmp/") || strings.Contains(state.Summary, "/tmp/") || strings.Contains(state.Text, "/tmp/") {
		t.Errorf("expected no '/tmp/' in check run state: %+v", state)
	}
	if strings.Contains(state.Title, "gs://") || strings.Contains(state.Summary, "gs://") || strings.Contains(state.Text, "gs://") {
		t.Errorf("expected no 'gs://' in check run state: %+v", state)
	}
	wantDetails := "https://eval.example.com/github.com/gke-labs/eval-runner/pulls/416"
	if state.DetailsURL != wantDetails {
		t.Errorf("state.DetailsURL = %q, want %q", state.DetailsURL, wantDetails)
	}
	if strings.Contains(state.DetailsURL, "/comparisons/eval-runner") || strings.Contains(state.DetailsURL, "/runs/eval-runner") {
		t.Errorf("expected no namespaced paths in DetailsURL, got %q", state.DetailsURL)
	}
}

func TestCheckRunStateForComparison_ActionsPolicy(t *testing.T) {
	// Verify action button constraints
	allActions := []CheckRunAction{ActionMore32, ActionRerunBase, ActionRerunCand}
	for _, a := range allActions {
		if len(a.Label) > 20 {
			t.Errorf("Action %s label %q exceeds 20 characters (got %d)", a.Identifier, a.Label, len(a.Label))
		}
		if len(a.Description) > 40 {
			t.Errorf("Action %s description %q exceeds 40 characters (got %d)", a.Identifier, a.Description, len(a.Description))
		}
		if len(a.Identifier) > 20 {
			t.Errorf("Action identifier %q exceeds 20 characters (got %d)", a.Identifier, len(a.Identifier))
		}
	}

	t.Run("ConclusiveVerdictsOfferOnlyMore32", func(t *testing.T) {
		for _, v := range []v1alpha1.ComparisonVerdict{
			v1alpha1.ComparisonVerdictImproved,
			v1alpha1.ComparisonVerdictRegressed,
			v1alpha1.ComparisonVerdictUnchanged,
		} {
			comp := &v1alpha1.Comparison{
				Status: v1alpha1.ComparisonStatus{
					Phase:   v1alpha1.ComparisonPhaseCompleted,
					Verdict: v,
				},
			}
			state := CheckRunStateForComparison(comp, nil, nil, "", "")
			if len(state.Actions) != 1 || state.Actions[0].Identifier != ActionIdentifierMore32 {
				t.Errorf("Verdict %s: expected [more-32], got %+v", v, state.Actions)
			}
		}
	})

	t.Run("InconclusiveVerdictOffersMore32AndRerunBase", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Phase:   v1alpha1.ComparisonPhaseCompleted,
				Verdict: v1alpha1.ComparisonVerdictInconclusive,
			},
		}
		state := CheckRunStateForComparison(comp, nil, nil, "", "")
		if len(state.Actions) != 2 ||
			state.Actions[0].Identifier != ActionIdentifierMore32 ||
			state.Actions[1].Identifier != ActionIdentifierRerunBase {
			t.Errorf("Inconclusive: expected [more-32, rerun-base], got %+v", state.Actions)
		}
	})

	t.Run("CandidateRunFailedOffersRerunCand", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Phase:   v1alpha1.ComparisonPhaseCompleted,
				Verdict: v1alpha1.ComparisonVerdictInconclusive,
			},
		}
		candRun := &v1alpha1.Run{
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseFailed,
			},
		}
		baseRun := &v1alpha1.Run{
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
			},
		}
		state := CheckRunStateForComparison(comp, baseRun, candRun, "", "")
		if len(state.Actions) != 1 || state.Actions[0].Identifier != ActionIdentifierRerunCand {
			t.Errorf("Candidate failed: expected [rerun-cand], got %+v", state.Actions)
		}
	})

	t.Run("BaselineRunFailedOffersRerunBase", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Phase:   v1alpha1.ComparisonPhaseCompleted,
				Verdict: v1alpha1.ComparisonVerdictInconclusive,
			},
		}
		candRun := &v1alpha1.Run{
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
			},
		}
		baseRun := &v1alpha1.Run{
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseFailed,
			},
		}
		state := CheckRunStateForComparison(comp, baseRun, candRun, "", "")
		if len(state.Actions) != 1 || state.Actions[0].Identifier != ActionIdentifierRerunBase {
			t.Errorf("Baseline failed: expected [rerun-base], got %+v", state.Actions)
		}
	})

	t.Run("BothRunsFailedOffersRerunCandAndRerunBase", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Phase:   v1alpha1.ComparisonPhaseCompleted,
				Verdict: v1alpha1.ComparisonVerdictInconclusive,
			},
		}
		candRun := &v1alpha1.Run{
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseFailed,
			},
		}
		baseRun := &v1alpha1.Run{
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseFailed,
			},
		}
		state := CheckRunStateForComparison(comp, baseRun, candRun, "", "")
		if len(state.Actions) != 2 ||
			state.Actions[0].Identifier != ActionIdentifierRerunCand ||
			state.Actions[1].Identifier != ActionIdentifierRerunBase {
			t.Errorf("Both failed: expected [rerun-cand, rerun-base], got %+v", state.Actions)
		}
	})

	t.Run("ScoringFailureOffersRerunCandAndRerunBase", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "scoring-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:   v1alpha1.ComparisonPhaseCompleted,
				Verdict: v1alpha1.ComparisonVerdictInconclusive,
				Conditions: []metav1.Condition{
					{
						Type:    v1alpha1.ConditionTypeArtifactsReadable,
						Status:  metav1.ConditionFalse,
						Reason:  v1alpha1.ConditionReasonNotFound,
						Message: "Inspect log not found in sink",
					},
				},
			},
		}
		state := CheckRunStateForComparison(comp, nil, nil, "", "")
		if len(state.Actions) != 2 ||
			state.Actions[0].Identifier != ActionIdentifierRerunCand ||
			state.Actions[1].Identifier != ActionIdentifierRerunBase {
			t.Errorf("Scoring failure: expected [rerun-cand, rerun-base], got %+v", state.Actions)
		}
	})

	t.Run("RunningVerdictOffersNoActions", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Phase:   v1alpha1.ComparisonPhaseRunning,
				Verdict: v1alpha1.ComparisonVerdictRunning,
			},
		}
		state := CheckRunStateForComparison(comp, nil, nil, "", "")
		if len(state.Actions) != 0 {
			t.Errorf("Running: expected empty actions, got %+v", state.Actions)
		}
	})

	t.Run("SanitizeCheckRunActionsLimits", func(t *testing.T) {
		oversized := []CheckRunAction{
			{Label: "1234567890123456789012345", Description: "123456789012345678901234567890123456789012345", Identifier: "1234567890123456789012345"},
			{Label: "b2", Description: "desc2", Identifier: "id2"},
			{Label: "b3", Description: "desc3", Identifier: "id3"},
			{Label: "b4", Description: "desc4", Identifier: "id4"},
		}
		sanitized := SanitizeCheckRunActions(oversized)
		if len(sanitized) != 3 {
			t.Fatalf("expected clamped to 3 actions, got %d", len(sanitized))
		}
		if len(sanitized[0].Label) != 20 {
			t.Errorf("expected label clamped to 20, got %d (%q)", len(sanitized[0].Label), sanitized[0].Label)
		}
		if len(sanitized[0].Description) != 40 {
			t.Errorf("expected description clamped to 40, got %d (%q)", len(sanitized[0].Description), sanitized[0].Description)
		}
		if len(sanitized[0].Identifier) != 20 {
			t.Errorf("expected identifier clamped to 20, got %d (%q)", len(sanitized[0].Identifier), sanitized[0].Identifier)
		}
	})
}

func TestRenderComparisonMarkdown_MultipleRequestsByUserCount(t *testing.T) {
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-multi-audit", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     433,
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictInconclusive,
			HandledCommands: []v1alpha1.ComparisonHandledCommand{
				{
					Command:     "more 32",
					Requested:   32,
					RequestedBy: "alice",
					Outcome:     "applied",
				},
				{
					Command:     "more 32",
					Requested:   32,
					RequestedBy: "alice",
					Outcome:     "applied",
				},
			},
		},
	}

	md := RenderComparisonMarkdown(comp, nil, nil, "")
	if !strings.Contains(md, "+32 samples requested by @alice (x2)") {
		t.Errorf("expected markdown to contain '+32 samples requested by @alice (x2)', got:\n%s", md)
	}
}

func TestCheckRunStateForComparison_TransientErrorDoesNotTriggerScoringFailure(t *testing.T) {
	// A comparison in phase Running with a temporary error message (e.g. transient network timeout)
	// must not be treated as a scoring failure when ConditionTypeArtifactsReadable is not ConditionFalse.
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-running", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline: v1alpha1.ComparisonRunRef{Run: "base-run"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     416,
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:   v1alpha1.ComparisonPhaseRunning,
			Verdict: v1alpha1.ComparisonVerdictRunning,
			Message: "failed to load candidate log: connection timeout",
		},
	}

	state := CheckRunStateForComparison(comp, nil, nil, "", "https://eval.example.com")
	if state.Status != "in_progress" {
		t.Errorf("expected status 'in_progress' for transient error, got %q", state.Status)
	}
	if strings.Contains(state.Title, "unavailable") {
		t.Errorf("unexpected failure headline for running comparison with transient error: %q", state.Title)
	}
}

func TestScoringFailureDetails_TaskNamedCandidateNotMisclassified(t *testing.T) {
	comp := &v1alpha1.Comparison{
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     416,
				BaseSHA:    "dd24bfd11111111111111111111111111111111",
				HeadSHA:    "3f9c2e1111111111111111111111111111111111",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictInconclusive,
			Message: `failed to load baseline log for task "candidate/feature_test" (/tmp/artifacts/runs/dd24bfd/inspect.json): artifact not found`,
			Conditions: []metav1.Condition{
				{
					Type:   v1alpha1.ConditionTypeArtifactsReadable,
					Status: metav1.ConditionFalse,
					Reason: v1alpha1.ConditionReasonNotFound,
				},
			},
		},
	}

	headline, _ := ScoringFailureDetails(comp, nil, nil)
	wantHeadline := "Baseline results for `dd24bfd` are unavailable; eval-runner will need to re-run the baseline."
	if headline != wantHeadline {
		t.Errorf("headline = %q, want %q", headline, wantHeadline)
	}
}

func TestRenderComparisonMarkdown_ScoringFailure(t *testing.T) {
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-scoring-fail", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-dd24bfd"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-3f9c2e1"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     416,
				BaseSHA:    "dd24bfd22222222222222222222222222222222",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:   v1alpha1.ComparisonPhaseCompleted,
			Verdict: v1alpha1.ComparisonVerdictInconclusive,
			Message: `failed to load baseline log for task "docs/readme" (/tmp/artifacts/runs/dd24bfd/inspect.json): artifact not found: open /tmp/artifacts/runs/dd24bfd/inspect.json: no such file or directory`,
			Conditions: []metav1.Condition{
				{
					Type:   v1alpha1.ConditionTypeArtifactsReadable,
					Status: metav1.ConditionFalse,
					Reason: v1alpha1.ConditionReasonNotFound,
				},
			},
		},
	}

	// 1. With uiBaseURL
	gotWith := RenderComparisonMarkdown(comp, nil, nil, "https://eval.example.com")
	wantHeadline := "Baseline results for `dd24bfd` are unavailable; eval-runner will need to re-run the baseline."
	if !strings.Contains(gotWith, wantHeadline) {
		t.Errorf("expected comment to contain user-facing headline %q, got:\n%s", wantHeadline, gotWith)
	}
	if !strings.Contains(gotWith, "<details>") || !strings.Contains(gotWith, "</details>") {
		t.Errorf("expected comment to contain collapsed <details> block, got:\n%s", gotWith)
	}
	if !strings.Contains(gotWith, `failed to load baseline log for task "docs/readme" (inspect.json): artifact not found: open inspect.json: no such file or directory`) {
		t.Errorf("expected sanitized error inside details block, got:\n%s", gotWith)
	}
	if strings.Contains(gotWith, "/tmp/") {
		t.Errorf("expected no '/tmp/' in comment output, got:\n%s", gotWith)
	}
	if strings.Contains(gotWith, "gs://") {
		t.Errorf("expected no 'gs://' in comment output, got:\n%s", gotWith)
	}
	if strings.Contains(gotWith, "/comparisons/eval-runner/") || strings.Contains(gotWith, "/runs/eval-runner/") {
		t.Errorf("expected no legacy namespaced routes in comment output, got:\n%s", gotWith)
	}
	wantWebLink := "[web view](https://eval.example.com/github.com/gke-labs/eval-runner/pulls/416)"
	if !strings.Contains(gotWith, wantWebLink) {
		t.Errorf("expected web view link %q, got:\n%s", wantWebLink, gotWith)
	}

	// 2. Without uiBaseURL (existing behavior stays code-formatted plain path)
	gotWithout := RenderComparisonMarkdown(comp, nil, nil, "")
	if !strings.Contains(gotWithout, "`/comparisons/eval-runner/comp-scoring-fail`") {
		t.Errorf("expected plain code-formatted path when uiBaseURL is empty, got:\n%s", gotWithout)
	}
	if strings.Contains(gotWithout, "[web view]") {
		t.Errorf("unexpected '[web view]' when uiBaseURL is empty, got:\n%s", gotWithout)
	}
	if strings.Contains(gotWithout, "/tmp/") || strings.Contains(gotWithout, "gs://") {
		t.Errorf("expected no '/tmp/' or 'gs://' even without uiBaseURL, got:\n%s", gotWithout)
	}
}

func TestReporter_ScoringFailureAndDetailsURL(t *testing.T) {
	server := NewFakeGitHubServer()
	defer server.Close()
	client := server.Client("token")

	reporter := NewReporter(client, ReporterOptions{
		UIBaseURL: "https://eval.example.com",
	})

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-scoring-fail-rep", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-dd24bfd"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-3f9c2e1"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     416,
				HeadSHA:    "3f9c2e1111111111111111111111111111111111",
				BaseSHA:    "dd24bfd22222222222222222222222222222222",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:   v1alpha1.ComparisonPhaseCompleted,
			Verdict: v1alpha1.ComparisonVerdictInconclusive,
			Message: `failed to load baseline log for task "triage/lookup" (gs://eval-bucket/runs/dd24bfd/inspect.json): artifact not found: storage: object doesn't exist`,
			Conditions: []metav1.Condition{
				{
					Type:   v1alpha1.ConditionTypeArtifactsReadable,
					Status: metav1.ConditionFalse,
					Reason: v1alpha1.ConditionReasonNotFound,
				},
			},
		},
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-3f9c2e1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{Commit: "3f9c2e1111111111111111111111111111111111"},
		},
	}

	status, err := reporter.Report(t.Context(), comp, nil, candRun)
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	if status == nil || status.CheckRunID == 0 {
		t.Fatalf("expected non-zero check run ID, got %+v", status)
	}

	checkRuns := server.CheckRuns()
	if len(checkRuns) != 1 {
		t.Fatalf("expected 1 check run, got %d", len(checkRuns))
	}
	cr := checkRuns[0]

	wantDetailsURL := "https://eval.example.com/github.com/gke-labs/eval-runner/pulls/416"
	if cr.DetailsURL != wantDetailsURL {
		t.Errorf("cr.DetailsURL = %q, want %q", cr.DetailsURL, wantDetailsURL)
	}
	wantHeadline := "Baseline results for `dd24bfd` are unavailable; eval-runner will need to re-run the baseline."
	if cr.Title != wantHeadline {
		t.Errorf("cr.Title = %q, want %q", cr.Title, wantHeadline)
	}
	if cr.Summary != wantHeadline {
		t.Errorf("cr.Summary = %q, want %q", cr.Summary, wantHeadline)
	}
	wantText := `failed to load baseline log for task "triage/lookup" (inspect.json): artifact not found: storage: object doesn't exist`
	if !strings.Contains(cr.Text, wantText) {
		t.Errorf("cr.Text expected to contain %q, got %q", wantText, cr.Text)
	}
	if !strings.Contains(cr.Text, "<details>") {
		t.Errorf("cr.Text expected to contain collapsed details, got %q", cr.Text)
	}
	if !strings.Contains(cr.Text, "| eval |") {
		t.Errorf("cr.Text expected to contain table, got %q", cr.Text)
	}
	if strings.Contains(cr.Title, "gs://") || strings.Contains(cr.Summary, "gs://") || strings.Contains(cr.Text, "gs://") {
		t.Errorf("expected no 'gs://' in check run output, got %+v", cr)
	}
	if strings.Contains(cr.DetailsURL, "/comparisons/eval-runner") || strings.Contains(cr.DetailsURL, "/runs/eval-runner") {
		t.Errorf("expected no namespaced paths in DetailsURL, got %q", cr.DetailsURL)
	}

	comments := server.Comments()
	if len(comments) != 1 {
		t.Fatalf("expected 1 comment, got %d", len(comments))
	}
	c := comments[0]
	if !strings.Contains(c.Body, wantHeadline) {
		t.Errorf("expected comment to contain headline %q, got:\n%s", wantHeadline, c.Body)
	}
	if !strings.Contains(c.Body, "<details>") {
		t.Errorf("expected comment to contain <details> block, got:\n%s", c.Body)
	}
	if !strings.Contains(c.Body, wantText) {
		t.Errorf("expected comment to contain sanitized text %q, got:\n%s", wantText, c.Body)
	}
	if strings.Contains(c.Body, "gs://") || strings.Contains(c.Body, "/tmp/") {
		t.Errorf("expected no 'gs://' or '/tmp/' in comment body, got:\n%s", c.Body)
	}
	if strings.Contains(c.Body, "/comparisons/eval-runner") || strings.Contains(c.Body, "/runs/eval-runner") {
		t.Errorf("expected no namespaced paths in comment body, got:\n%s", c.Body)
	}
}

func TestCheckRunState_RichOutput(t *testing.T) {
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      "comp-rich-test",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     425,
				HeadSHA:    "3f9c2e1b",
				BaseSHA:    "a81d0c4e",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictImproved,
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:     "docs/readme",
					Label:  v1alpha1.ComparisonTaskLabelImproved,
					Pairs:  10,
					Wins:   8,
					Losses: 1,
					Ties:   1,
				},
				{
					ID:     "triage/lookup",
					Label:  v1alpha1.ComparisonTaskLabelUnchanged,
					Pairs:  10,
					Wins:   5,
					Losses: 5,
					Ties:   0,
				},
			},
		},
	}
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      "cand-run-3f9c2e1",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "3f9c2e1b",
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      "base-run-a81d0c4",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "a81d0c4e",
			},
		},
	}

	// 1. With uiBaseURL
	stateWithUI := CheckRunStateForComparison(comp, baseRun, candRun, "", "https://eval.example.com")

	// Table assertions
	if !strings.Contains(stateWithUI.Text, "| eval | base | this PR |") {
		t.Errorf("expected state.Text to contain table header, got:\n%s", stateWithUI.Text)
	}
	if !strings.Contains(stateWithUI.Text, "docs/readme") || !strings.Contains(stateWithUI.Text, "triage/lookup") {
		t.Errorf("expected state.Text to contain task rows, got:\n%s", stateWithUI.Text)
	}
	if !strings.Contains(stateWithUI.Text, "[web view](https://eval.example.com/github.com/gke-labs/eval-runner/pulls/425)") {
		t.Errorf("expected state.Text to contain web view link, got:\n%s", stateWithUI.Text)
	}
	if !strings.Contains(stateWithUI.Text, "[trajectories](https://eval.example.com/github.com/gke-labs/eval-runner/runs/cand-run-3f9c2e1/tasks)") {
		t.Errorf("expected state.Text to contain trajectories link, got:\n%s", stateWithUI.Text)
	}
	if strings.Contains(stateWithUI.Text, "/comparisons/eval-runner") || strings.Contains(stateWithUI.Text, "/runs/eval-runner") {
		t.Errorf("expected no legacy namespaced routes in state.Text, got:\n%s", stateWithUI.Text)
	}
	if strings.Contains(stateWithUI.Text, "/tmp/") || strings.Contains(stateWithUI.Text, "gs://") {
		t.Errorf("expected no internal paths in state.Text, got:\n%s", stateWithUI.Text)
	}

	// Images assertions: 2 tasks (<= 3 tasks) -> overall sparkline + one image per task with ?eval=
	if len(stateWithUI.Images) != 3 {
		t.Fatalf("expected 3 images (overall + 2 tasks), got %d", len(stateWithUI.Images))
	}
	wantOverallURL := "https://eval.example.com/github.com/gke-labs/eval-runner/sparkline.svg?branch=main&w=600&h=80"
	wantImg0URL := "https://eval.example.com/github.com/gke-labs/eval-runner/sparkline.svg?branch=main&eval=docs%2Freadme&w=600&h=80"
	wantImg1URL := "https://eval.example.com/github.com/gke-labs/eval-runner/sparkline.svg?branch=main&eval=triage%2Flookup&w=600&h=80"
	if stateWithUI.Images[0].ImageURL != wantOverallURL {
		t.Errorf("Images[0].ImageURL = %q, want %q", stateWithUI.Images[0].ImageURL, wantOverallURL)
	}
	if stateWithUI.Images[0].Caption != "main, last 20 commits" {
		t.Errorf("Images[0].Caption = %q, want 'main, last 20 commits'", stateWithUI.Images[0].Caption)
	}
	if stateWithUI.Images[1].ImageURL != wantImg0URL {
		t.Errorf("Images[1].ImageURL = %q, want %q", stateWithUI.Images[1].ImageURL, wantImg0URL)
	}
	if stateWithUI.Images[1].Caption != "docs/readme (main, last 20 commits)" {
		t.Errorf("Images[1].Caption = %q, want 'docs/readme (main, last 20 commits)'", stateWithUI.Images[1].Caption)
	}
	if stateWithUI.Images[2].ImageURL != wantImg1URL {
		t.Errorf("Images[2].ImageURL = %q, want %q", stateWithUI.Images[2].ImageURL, wantImg1URL)
	}
	if stateWithUI.Images[2].Caption != "triage/lookup (main, last 20 commits)" {
		t.Errorf("Images[2].Caption = %q, want 'triage/lookup (main, last 20 commits)'", stateWithUI.Images[2].Caption)
	}

	// 2. Without uiBaseURL: images omitted
	stateWithoutUI := CheckRunStateForComparison(comp, baseRun, candRun, "", "")
	if len(stateWithoutUI.Images) != 0 {
		t.Errorf("expected Images to be omitted when uiBaseURL is empty, got %+v", stateWithoutUI.Images)
	}
	if strings.Contains(stateWithoutUI.Text, "[web view]") {
		t.Errorf("unexpected '[web view]' link when uiBaseURL is empty in state.Text")
	}
}

func TestCheckRunState_SparklineOverallFourTasks(t *testing.T) {
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      "comp-4tasks",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     425,
				HeadSHA:    "3f9c2e1b",
				BaseSHA:    "a81d0c4e",
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictUnchanged,
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{ID: "task1", Label: v1alpha1.ComparisonTaskLabelUnchanged},
				{ID: "task2", Label: v1alpha1.ComparisonTaskLabelUnchanged},
				{ID: "task3", Label: v1alpha1.ComparisonTaskLabelUnchanged},
				{ID: "task4", Label: v1alpha1.ComparisonTaskLabelUnchanged},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      "base-run",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/branch": "release-2.0",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "a81d0c4e",
			},
		},
	}

	state := CheckRunStateForComparison(comp, baseRun, nil, "", "https://eval.example.com")
	if len(state.Images) != 1 {
		t.Fatalf("expected 1 overall image for >3 tasks, got %d", len(state.Images))
	}
	wantURL := "https://eval.example.com/github.com/gke-labs/eval-runner/sparkline.svg?branch=release-2.0&w=600&h=80"
	if state.Images[0].ImageURL != wantURL {
		t.Errorf("Images[0].ImageURL = %q, want %q", state.Images[0].ImageURL, wantURL)
	}
	wantCaption := "release-2.0, last 20 commits"
	if state.Images[0].Caption != wantCaption {
		t.Errorf("Images[0].Caption = %q, want %q", state.Images[0].Caption, wantCaption)
	}
}

func TestCheckRunText_TruncationAtRowBoundary(t *testing.T) {
	var tasks []v1alpha1.ComparisonTaskStatus
	for i := 0; i < 1500; i++ {
		tasks = append(tasks, v1alpha1.ComparisonTaskStatus{
			ID:     fmt.Sprintf("suite/subsystem_evaluation_task_long_name_%04d", i),
			Label:  v1alpha1.ComparisonTaskLabelUnchanged,
			Pairs:  32,
			Wins:   16,
			Losses: 16,
			Ties:   0,
		})
	}
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "eval-runner",
			Name:      "comp-huge",
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     425,
			},
		},
		Status: v1alpha1.ComparisonStatus{
			Verdict: v1alpha1.ComparisonVerdictUnchanged,
			Tasks:   tasks,
		},
	}

	text := RenderCheckRunText(comp, nil, nil, "https://eval.example.com")
	if len(text) > MaxCheckRunTextBytes {
		t.Errorf("len(text) = %d, expected <= %d", len(text), MaxCheckRunTextBytes)
	}
	if !strings.Contains(text, "more rows (see web view)") {
		t.Errorf("expected text to contain truncation marker 'more rows (see web view)', got:\n%s", text)
	}
	if !strings.Contains(text, "… ") {
		t.Errorf("expected truncation marker to use ellipsis character '…', got:\n%s", text)
	}
	if !strings.Contains(text, "[web view](https://eval.example.com/github.com/gke-labs/eval-runner/pulls/425)") {
		t.Errorf("expected footer with web view link to be retained after truncation")
	}
}

func TestBuildCheckRunAnnotations(t *testing.T) {
	manifestTasks := []v1alpha1.ManifestTask{
		{
			ID:    "docs/readme",
			Spec:  "eval.py@docs",
			Paths: []string{"docs/readme.md", "README.md"},
		},
		{
			ID:    "triage/lookup",
			Spec:  "eval.py@triage",
			Paths: []string{}, // Empty paths should be skipped
		},
	}

	failures := map[string][]TaskSampleFailure{
		"docs/readme": {
			{SampleID: "sample-1", Score: 0.0},
			{SampleID: "sample-2", Score: 0.5},
			{SampleID: "sample-3", Score: 1.0}, // Success, should not be annotated
		},
		"triage/lookup": {
			{SampleID: "sample-a", Score: 0.0}, // Task has empty paths, skipped
		},
	}

	anns := BuildCheckRunAnnotationsFromFailures(manifestTasks, nil, failures)
	// 2 failed samples * 2 paths = 4 annotations
	if len(anns) != 4 {
		t.Fatalf("expected 4 annotations, got %d: %+v", len(anns), anns)
	}

	for _, ann := range anns {
		if ann.StartLine != 1 || ann.EndLine != 1 {
			t.Errorf("expected line 1, got start %d end %d", ann.StartLine, ann.EndLine)
		}
		if ann.AnnotationLevel != "notice" {
			t.Errorf("expected level 'notice', got %q", ann.AnnotationLevel)
		}
		if ann.Title != "docs/readme" {
			t.Errorf("expected title 'docs/readme', got %q", ann.Title)
		}
	}
	if anns[0].Path != "docs/readme.md" || anns[0].Message != "sample sample-1: score 0.00" {
		t.Errorf("unexpected ann[0]: %+v", anns[0])
	}
	if anns[1].Path != "README.md" || anns[1].Message != "sample sample-1: score 0.00" {
		t.Errorf("unexpected ann[1]: %+v", anns[1])
	}
	if anns[2].Path != "docs/readme.md" || anns[2].Message != "sample sample-2: score 0.50" {
		t.Errorf("unexpected ann[2]: %+v", anns[2])
	}
	if anns[3].Path != "README.md" || anns[3].Message != "sample sample-2: score 0.50" {
		t.Errorf("unexpected ann[3]: %+v", anns[3])
	}

	// Test cap at 50 annotations
	var manyFailures []TaskSampleFailure
	for i := 0; i < 60; i++ {
		manyFailures = append(manyFailures, TaskSampleFailure{
			SampleID: fmt.Sprintf("sample-%d", i),
			Score:    0.0,
		})
	}
	cappedAnns := BuildCheckRunAnnotationsFromFailures([]v1alpha1.ManifestTask{
		{
			ID:    "task-many",
			Paths: []string{"file.go"},
		},
	}, nil, map[string][]TaskSampleFailure{"task-many": manyFailures})
	if len(cappedAnns) != 50 {
		t.Errorf("expected capped annotations to be 50, got %d", len(cappedAnns))
	}
}

func TestBuildCheckRunAnnotations_ResolvesGlobsAgainstChangedFiles(t *testing.T) {
	manifestTasks := []v1alpha1.ManifestTask{
		{
			ID:    "docs/readme",
			Spec:  "eval.py@docs",
			Paths: []string{"docs/**", "evals/**"},
		},
		{
			ID:    "src/core",
			Spec:  "eval.py@src",
			Paths: []string{"pkg/core/**"},
		},
	}

	failures := map[string][]TaskSampleFailure{
		"docs/readme": {
			{SampleID: "s1", Score: 0.0},
		},
		"src/core": {
			{SampleID: "s2", Score: 0.0},
		},
	}

	changedFiles := []string{"docs/index.md", "docs/api.md", "cmd/main.go"}

	anns := BuildCheckRunAnnotationsFromFailures(manifestTasks, changedFiles, failures)
	// docs/** matches docs/index.md and docs/api.md; pkg/core/** matches nothing -> skipped
	if len(anns) != 2 {
		t.Fatalf("expected 2 annotations on matched changed files, got %d: %+v", len(anns), anns)
	}
	if anns[0].Path != "docs/index.md" {
		t.Errorf("anns[0].Path = %q, want 'docs/index.md'", anns[0].Path)
	}
	if anns[1].Path != "docs/api.md" {
		t.Errorf("anns[1].Path = %q, want 'docs/api.md'", anns[1].Path)
	}
	for _, ann := range anns {
		if strings.ContainsAny(ann.Path, "*?[{") {
			t.Errorf("annotation path must not contain glob metacharacters: %q", ann.Path)
		}
	}

	// Without changed files: globs are skipped entirely
	annsNoFiles := BuildCheckRunAnnotationsFromFailures(manifestTasks, nil, failures)
	if len(annsNoFiles) != 0 {
		t.Errorf("expected 0 annotations when globs cannot be resolved to files, got %+v", annsNoFiles)
	}
}

func TestCapDetailsBlock(t *testing.T) {
	shortBlock := "<details>\n<summary>Error details</summary>\n\n```\nshort error\n```\n</details>"
	if got := CapDetailsBlock(shortBlock, 8192); got != shortBlock {
		t.Errorf("CapDetailsBlock modified short block: %s", got)
	}

	hugeErr := strings.Repeat("error stack trace line\n", 1000)
	hugeBlock := fmt.Sprintf("<details>\n<summary>Error details</summary>\n\n```\n%s\n```\n</details>", hugeErr)
	capped := CapDetailsBlock(hugeBlock, 1024)
	if len(capped) > 1024 {
		t.Errorf("len(capped) = %d, want <= 1024", len(capped))
	}
	if !strings.Contains(capped, "... (truncated)") {
		t.Errorf("expected truncation notice in capped block: %s", capped)
	}
	if !strings.HasPrefix(capped, "<details>") || !strings.HasSuffix(capped, "</details>") {
		t.Errorf("expected valid details tag wrapping: %s", capped)
	}
}

func TestBuildCheckRunAnnotations_FromTaskLogs(t *testing.T) {
	manifestTasks := []v1alpha1.ManifestTask{
		{
			ID:    "docs/readme",
			Spec:  "eval.py@docs",
			Paths: []string{"README.md"},
		},
	}
	logJSON := `{
		"version": 2,
		"status": "success",
		"eval": {"scorers": [{"name": "exact"}]},
		"samples": [
			{"id": "sample-1", "scores": {"exact": {"value": 0.0}}},
			{"id": "sample-2", "scores": {"exact": {"value": 1.0}}},
			{"id": "sample-3", "scores": {"exact": {"value": 0.25}}}
		]
	}`

	anns := BuildCheckRunAnnotations(manifestTasks, nil, map[string][]byte{
		"docs/readme": []byte(logJSON),
	})
	if len(anns) != 2 {
		t.Fatalf("expected 2 annotations (sample-1 and sample-3), got %d", len(anns))
	}
	if anns[0].Message != "sample sample-1: score 0.00" {
		t.Errorf("anns[0].Message = %q, want sample sample-1: score 0.00", anns[0].Message)
	}
	if anns[1].Message != "sample sample-3: score 0.25" {
		t.Errorf("anns[1].Message = %q, want sample sample-3: score 0.25", anns[1].Message)
	}
}

func TestFakeGitHubServer_RecordsTextImagesAnnotations(t *testing.T) {
	server := NewFakeGitHubServer()
	defer server.Close()

	client := server.Client("token")

	initialState := CheckRunState{
		Name:       CheckRunName,
		Status:     "in_progress",
		Title:      "running",
		Summary:    "running",
		Text:       "Initial markdown body",
		DetailsURL: "https://eval.example.com/details",
		Images: []CheckRunImage{
			{Alt: "sparkline", ImageURL: "https://eval.example.com/sparkline1.svg", Caption: "main, last 20 commits"},
		},
		Annotations: []CheckRunAnnotation{
			{Path: "README.md", StartLine: 1, EndLine: 1, AnnotationLevel: "notice", Message: "sample 1: score 0.00"},
		},
	}

	crID, err := client.CreateCheckRun(t.Context(), "gke-labs", "eval-runner", "abcdef1234", initialState)
	if err != nil {
		t.Fatalf("CreateCheckRun failed: %v", err)
	}

	runs := server.CheckRuns()
	if len(runs) != 1 {
		t.Fatalf("expected 1 check run, got %d", len(runs))
	}
	r := runs[0]
	if r.Text != "Initial markdown body" {
		t.Errorf("r.Text = %q, want 'Initial markdown body'", r.Text)
	}
	if len(r.Images) != 1 || r.Images[0].ImageURL != "https://eval.example.com/sparkline1.svg" {
		t.Errorf("r.Images = %+v", r.Images)
	}
	if len(r.Annotations) != 1 || r.Annotations[0].Message != "sample 1: score 0.00" {
		t.Errorf("r.Annotations = %+v", r.Annotations)
	}

	// Update check run: GitHub's API appends annotations on update
	updatedState := CheckRunState{
		Name:       CheckRunName,
		Status:     "completed",
		Conclusion: "success",
		Title:      "IMPROVED",
		Summary:    "IMPROVED",
		Text:       "Updated markdown body with table",
		DetailsURL: "https://eval.example.com/details/updated",
		Images: []CheckRunImage{
			{Alt: "sparkline2", ImageURL: "https://eval.example.com/sparkline2.svg", Caption: "main, last 20 commits"},
		},
		Annotations: []CheckRunAnnotation{
			{Path: "docs/readme.md", StartLine: 1, EndLine: 1, AnnotationLevel: "notice", Message: "sample 2: score 0.50"},
		},
	}
	if err := client.UpdateCheckRun(t.Context(), "gke-labs", "eval-runner", crID, updatedState); err != nil {
		t.Fatalf("UpdateCheckRun failed: %v", err)
	}

	runs = server.CheckRuns()
	if len(runs) != 1 {
		t.Fatalf("expected 1 check run, got %d", len(runs))
	}
	r = runs[0]
	if r.Text != "Updated markdown body with table" {
		t.Errorf("r.Text = %q, want 'Updated markdown body with table'", r.Text)
	}
	if len(r.Images) != 1 || r.Images[0].ImageURL != "https://eval.example.com/sparkline2.svg" {
		t.Errorf("r.Images = %+v", r.Images)
	}
	// GitHub appends annotations on update: 1 initial + 1 updated = 2 annotations
	if len(r.Annotations) != 2 {
		t.Fatalf("expected 2 annotations after update (appended), got %d: %+v", len(r.Annotations), r.Annotations)
	}
	if r.Annotations[0].Path != "README.md" || r.Annotations[1].Path != "docs/readme.md" {
		t.Errorf("unexpected annotations: %+v", r.Annotations)
	}
}

func TestHashContent_ChangesWithImageURLAndAnnotations(t *testing.T) {
	baseState := CheckRunState{
		Name:       CheckRunName,
		Status:     "completed",
		Conclusion: "success",
		Title:      "IMPROVED",
		Summary:    "IMPROVED",
		Text:       "markdown text",
		DetailsURL: "https://eval.example.com/pulls/425",
		Images: []CheckRunImage{
			{Alt: "trend", ImageURL: "https://eval.example.com/sparkline.svg?branch=main&w=600&h=80", Caption: "main, last 20 commits"},
		},
		Annotations: []CheckRunAnnotation{
			{Path: "README.md", StartLine: 1, EndLine: 1, AnnotationLevel: "notice", Message: "sample 1: score 0.00"},
		},
	}
	markdown := "markdown content"

	h1 := HashContent(markdown, baseState)

	// Change image URL -> hash must change
	stateChangedImg := baseState
	stateChangedImg.Images = []CheckRunImage{
		{Alt: "trend", ImageURL: "https://eval.example.com/sparkline.svg?branch=release&w=600&h=80", Caption: "main, last 20 commits"},
	}
	h2 := HashContent(markdown, stateChangedImg)
	if h1 == h2 {
		t.Errorf("HashContent did not change when image URL changed: %q vs %q", h1, h2)
	}

	// Change annotation message -> hash must change
	stateChangedAnn := baseState
	stateChangedAnn.Annotations = []CheckRunAnnotation{
		{Path: "README.md", StartLine: 1, EndLine: 1, AnnotationLevel: "notice", Message: "sample 1: score 0.20"},
	}
	h3 := HashContent(markdown, stateChangedAnn)
	if h1 == h3 {
		t.Errorf("HashContent did not change when annotation message changed: %q vs %q", h1, h3)
	}
}
