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
	"bytes"
	"flag"
	"fmt"
	"html"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/stats"
	"github.com/pmezard/go-difflib/difflib"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var update = flag.Bool("update", false, "update golden files")

func assertGolden(t *testing.T, filename string, actual string) {
	t.Helper()
	goldenPath := filepath.Join("testdata", filename)
	if *update {
		if err := os.WriteFile(goldenPath, []byte(actual), 0644); err != nil {
			t.Fatalf("failed to update golden file %s: %v", goldenPath, err)
		}
		return
	}

	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file %s: %v (run with -update to generate)", goldenPath, err)
	}

	if string(expected) != actual {
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:        difflib.SplitLines(string(expected)),
			B:        difflib.SplitLines(actual),
			FromFile: "Expected",
			ToFile:   "Actual",
			Context:  3,
		})
		t.Errorf("rendered HTML does not match golden file %s:\n%s", goldenPath, diff)
	}
}

func TestCalculateComparisonBarGeometry(t *testing.T) {
	barWidth := 340.0
	// Zero is at 170.0px.
	// scale is 340px / 1.0 unit.

	t.Run("PositiveDelta", func(t *testing.T) {
		delta := 0.23
		ci := &v1alpha1.DeltaConfidenceInterval{
			Lower: 0.11,
			Upper: 0.35,
		}
		leftPx, widthPx, dotPx, hasCI := CalculateComparisonBarGeometry(&delta, ci, barWidth)
		if !hasCI {
			t.Fatal("expected hasCI to be true")
		}

		// left: 170 + 0.11 * 340 = 207.4px
		expectedLeft := 170.0 + 0.11*340.0
		if math.Abs(leftPx-expectedLeft) > 1e-6 {
			t.Errorf("expected leftPx %.2f, got %.2f", expectedLeft, leftPx)
		}

		// right: 170 + 0.35 * 340 = 289.0px
		expectedWidth := (0.35 - 0.11) * 340.0
		if math.Abs(widthPx-expectedWidth) > 1e-6 {
			t.Errorf("expected widthPx %.2f, got %.2f", expectedWidth, widthPx)
		}

		// dot: 170 + 0.23 * 340 - 7 = 241.2px
		expectedDot := 170.0 + 0.23*340.0 - 7.0
		if math.Abs(dotPx-expectedDot) > 1e-6 {
			t.Errorf("expected dotPx %.2f, got %.2f", expectedDot, dotPx)
		}
	})

	t.Run("NegativeDelta", func(t *testing.T) {
		delta := -0.20
		ci := &v1alpha1.DeltaConfidenceInterval{
			Lower: -0.30,
			Upper: -0.10,
		}
		leftPx, widthPx, dotPx, hasCI := CalculateComparisonBarGeometry(&delta, ci, barWidth)
		if !hasCI {
			t.Fatal("expected hasCI to be true")
		}

		// left: 170 - 0.30 * 340 = 68.0px
		expectedLeft := 170.0 - 0.30*340.0
		if math.Abs(leftPx-expectedLeft) > 1e-6 {
			t.Errorf("expected leftPx %.2f, got %.2f", expectedLeft, leftPx)
		}

		expectedWidth := 0.20 * 340.0
		if math.Abs(widthPx-expectedWidth) > 1e-6 {
			t.Errorf("expected widthPx %.2f, got %.2f", expectedWidth, widthPx)
		}

		expectedDot := 170.0 - 0.20*340.0 - 7.0
		if math.Abs(dotPx-expectedDot) > 1e-6 {
			t.Errorf("expected dotPx %.2f, got %.2f", expectedDot, dotPx)
		}
	})

	t.Run("NilCI", func(t *testing.T) {
		delta := 0.10
		_, _, _, hasCI := CalculateComparisonBarGeometry(&delta, nil, barWidth)
		if hasCI {
			t.Errorf("expected hasCI to be false for nil CI")
		}
	})

	t.Run("ClampingExtremeDelta", func(t *testing.T) {
		delta := 0.90
		ci := &v1alpha1.DeltaConfidenceInterval{
			Lower: 0.40,
			Upper: 0.95,
		}
		leftPx, widthPx, dotPx, hasCI := CalculateComparisonBarGeometry(&delta, ci, barWidth)
		if !hasCI {
			t.Fatal("expected hasCI to be true")
		}
		if leftPx < 0 || leftPx > barWidth {
			t.Errorf("leftPx %.2f out of bounds [0, %.2f]", leftPx, barWidth)
		}
		if leftPx+widthPx > barWidth {
			t.Errorf("right bound %.2f exceeds barWidth %.2f", leftPx+widthPx, barWidth)
		}
		if dotPx < 0 || dotPx > barWidth-14.0 {
			t.Errorf("dotPx %.2f out of bounds", dotPx)
		}
	})
}

func TestCalculateRunBarGeometry(t *testing.T) {
	barWidth := 240.0

	t.Run("NormalRate", func(t *testing.T) {
		rate := 0.80
		ci := &v1alpha1.ConfidenceInterval{
			Lower: 0.60,
			Upper: 0.90,
		}
		leftPx, widthPx, dotPx, hasCI := CalculateRunBarGeometry(&rate, nil, ci, barWidth)
		if !hasCI {
			t.Fatal("expected hasCI to be true")
		}

		expectedLeft := 0.60 * 240.0 // 144.0px
		if math.Abs(leftPx-expectedLeft) > 1e-6 {
			t.Errorf("expected leftPx %.2f, got %.2f", expectedLeft, leftPx)
		}

		expectedWidth := (0.90 - 0.60) * 240.0 // 72.0px
		if math.Abs(widthPx-expectedWidth) > 1e-6 {
			t.Errorf("expected widthPx %.2f, got %.2f", expectedWidth, widthPx)
		}

		expectedDot := 0.80*240.0 - 7.0 // 185.0px
		if math.Abs(dotPx-expectedDot) > 1e-6 {
			t.Errorf("expected dotPx %.2f, got %.2f", expectedDot, dotPx)
		}
	})

	t.Run("NilCI", func(t *testing.T) {
		rate := 0.50
		_, _, _, hasCI := CalculateRunBarGeometry(&rate, nil, nil, barWidth)
		if hasCI {
			t.Errorf("expected hasCI to be false")
		}
	})
}

func sampleRunDetailView() RunDetailView {
	return RunDetailView{
		ActiveTab:       NavTabRuns,
		Name:            "docs-eval-cand-05ce4d8",
		Namespace:       "eval-runner",
		Phase:           v1alpha1.RunPhaseCompleted,
		PhaseChip:       "good",
		RepoURL:         "https://github.com/gke-labs/eval-runner.git",
		Commit:          "05ce4d8222222222222222222222222222222222",
		CommitShort:     "05ce4d8",
		Model:           "google/gemini-3.5-flash-lite",
		Age:             "10m",
		ArtifactsPrefix: "gs://eval-bucket/runs/05ce4d8/",
		ArtifactFooter:  "artifacts: gs://eval-bucket",
		TasksSummary:    "1/1",
		TasksCompleted:  1,
		TasksFailed:     0,
		TasksTotal:      1,
		TotalSamples:    48,
		MeanSuccessRate: "0.84",
		Tasks: []RunTaskRow{
			{
				ID:          "triage/runbook_lookup",
				Phase:       v1alpha1.TaskPhaseCompleted,
				PhaseChip:   "good",
				RateStr:     "0.84",
				HasCI:       true,
				CILeftPx:    170.4,
				CIWidthPx:   50.4,
				CIDotPx:     194.6,
				CILabel:     "[0.71, 0.92]",
				Samples:     48,
				Successes:   40,
				Method:      "wilson",
				LogPath:     "gs://eval-bucket/runs/05ce4d8/triage/runbook_lookup/inspect.json",
				SamplesLink: "/runs/eval-runner/docs-eval-cand-05ce4d8/tasks/triage%2Frunbook_lookup/samples",
			},
		},
	}
}

func sampleComparisonDetailView() ComparisonDetailView {
	return ComparisonDetailView{
		ActiveTab:    NavTabComparisons,
		Name:         "docs-eval-same-commit",
		Namespace:    "eval-runner",
		Verdict:      v1alpha1.ComparisonVerdictImproved,
		VerdictChip:  "good",
		Headline:     "05ce4d8 vs a81d0c4 · P(improvement) = 0.98",
		PImprovement: "0.98",
		Tasks: []ComparisonTaskRow{
			{
				ID:          "triage/runbook_lookup",
				BaseRate:    "0.61",
				CandRate:    "0.84",
				Delta:       "+0.23",
				HasCI:       true,
				CILeftPx:    207.4,
				CIWidthPx:   81.6,
				CIDotPx:     241.2,
				CILabel:     "[+0.11, +0.35]",
				StatusClass: "good",
				Label:       "improved",
				Pairs:       48,
				DroppedCand: 0,
				DroppedBase: 0,
			},
		},
		BaseRun:         "docs-eval-base-05ce4d8",
		BaseCommitShort: "a81d0c4",
		CandRun:         "docs-eval-cand-05ce4d8",
		CandCommitShort: "05ce4d8",
		TotalSamples:    96,
		TotalPairs:      48,
	}
}

func sampleRunsListView() RunsListView {
	return RunsListView{
		ActiveTab: NavTabRuns,
		Namespace: "eval-runner",
		Runs: []RunListItem{
			{
				Name:      "docs-eval-cand-05ce4d8",
				Namespace: "eval-runner",
				Phase:     v1alpha1.RunPhaseCompleted,
				PhaseChip: "good",
				Tasks:     "1/1",
				Samples:   48,
				Rates:     "triage/runbook_lookup=0.84[0.71,0.92]",
				Commit:    "05ce4d8",
				Age:       "10m",
				Link:      "/runs/eval-runner/docs-eval-cand-05ce4d8",
			},
			{
				Name:      "docs-eval-base-05ce4d8",
				Namespace: "eval-runner",
				Phase:     v1alpha1.RunPhaseCompleted,
				PhaseChip: "good",
				Tasks:     "1/1",
				Samples:   48,
				Rates:     "triage/runbook_lookup=0.61[0.46,0.74]",
				Commit:    "a81d0c4",
				Age:       "15m",
				Link:      "/runs/eval-runner/docs-eval-base-05ce4d8",
			},
		},
		TotalCount:   2,
		Offset:       0,
		Limit:        100,
		HasMore:      false,
		HasPrev:      false,
		ShowingRange: "Showing 1–2 of 2",
	}
}

func sampleComparisonsListView() ComparisonsListView {
	return ComparisonsListView{
		ActiveTab: NavTabComparisons,
		Namespace: "eval-runner",
		Comparisons: []ComparisonListItem{
			{
				Name:         "docs-eval-same-commit",
				Namespace:    "eval-runner",
				Verdict:      v1alpha1.ComparisonVerdictImproved,
				VerdictChip:  "good",
				PImprovement: "0.98",
				Tasks:        "1/1",
				BaselineRun:  "docs-eval-base-05ce4d8",
				CandidateRun: "docs-eval-cand-05ce4d8",
				Completed:    "2026-10-20 10:35 UTC",
				Age:          "5m",
				Link:         "/comparisons/eval-runner/docs-eval-same-commit",
				Deltas: RunDeltas{
					Quality: MetricDelta{Text: "+23.0 pp", Title: "Mean success rate 61.0% → 84.0% over 1 shared task (Run success rates)", Class: deltaClassGood},
					Tokens:  MetricDelta{Text: "−3.2%", Title: "−1.2K tokens (37.5K → 36.3K) over 1 shared task", Class: deltaClassGood},
					Cost:    MetricDelta{Text: "+$0.07 (+1.8%)", Title: "$3.8900 → $3.9600 over 1 shared task", Class: deltaClassBad},
				},
			},
			{
				Name:         "docs-eval-regressed",
				Namespace:    "eval-runner",
				Verdict:      v1alpha1.ComparisonVerdictRegressed,
				VerdictChip:  "bad",
				PImprovement: "0.05",
				Tasks:        "3/3",
				BaselineRun:  "docs-eval-base-a81d0c4",
				CandidateRun: "docs-eval-cand-9f1e2d3",
				Completed:    "—",
				Age:          "2h",
				Link:         "/comparisons/eval-runner/docs-eval-regressed",
				Reason:       "Regressed on 1 of 3 tasks: docs/readme_comprehension (80% → 40%)",
				ReasonFull:   "Regressed on 1 of 3 tasks: docs/readme_comprehension (80% → 40%)",
				Deltas: RunDeltas{
					Quality: MetricDelta{Text: "0.0 pp", Class: deltaClassZero},
					Tokens:  MetricDelta{Text: "—", Title: "Token counts are missing from one or both Runs", Class: deltaClassMissing},
					Cost:    MetricDelta{Text: "$0.00 (0.0%)", Class: deltaClassZero},
				},
			},
		},
		TotalCount:   2,
		Offset:       0,
		Limit:        100,
		HasMore:      false,
		HasPrev:      false,
		ShowingRange: "Showing 1–2 of 2",
		ShowFlat:     true,
	}
}

// sampleComparisonsListGroupedView is a /comparisons page with labeled Comparisons grouped
// by candidate Run above one unlabeled Comparison.
func sampleComparisonsListGroupedView() ComparisonsListView {
	regressedReason := "Two tasks regressed after the prompt rewrite: readme comprehension answers omit the install section in 3 of 5 samples, and runbook lookups cite a retired page."
	return ComparisonsListView{
		ActiveTab:          NavTabComparisons,
		Namespace:          "eval-runner",
		TotalCount:         4,
		Limit:              100,
		ShowingRange:       "Showing 1–3 of 3",
		ShowKindFilter:     true,
		KindAllLink:        "/comparisons",
		KindBaselineLink:   "/comparisons?kind=baseline",
		KindPreviousLink:   "/comparisons?kind=previous",
		ShowGroups:         true,
		ShowBaselineColumn: true,
		ShowPreviousColumn: true,
		ShowFlat:           true,
		Groups: []ComparisonGroupRow{
			{
				CandidateRun:  "eval-run-v3",
				CandidateLink: "/runs/eval-runner/eval-run-v3",
				Created:       "2026-10-20 02:14 UTC",
				VsBaseline: &ComparisonCell{
					Name:         "eval-v3-vs-v1",
					Link:         "/comparisons/eval-runner/eval-v3-vs-v1",
					Verdict:      v1alpha1.ComparisonVerdictRegressed,
					VerdictChip:  "bad",
					PImprovement: "0.05",
					Completed:    "2026-10-20 10:35 UTC",
					BaselineRun:  "eval-run-v1",
					BaselineLink: "/runs/eval-runner/eval-run-v1",
					Reason:       TruncateAtWord(regressedReason, reasonMaxRunes),
					ReasonFull:   regressedReason,
					Deltas: RunDeltas{
						Quality: MetricDelta{Text: "−4.8 pp", Class: deltaClassBad},
						Tokens:  MetricDelta{Text: "+2.1%", Class: deltaClassBad},
						Cost:    MetricDelta{Text: "−$0.01 (−0.9%)", Class: deltaClassGood},
					},
					Older: []ComparisonRef{{Name: "eval-v3-vs-v1-rerun", Link: "/comparisons/eval-runner/eval-v3-vs-v1-rerun"}},
				},
				VsPrevious: &ComparisonCell{
					Name:         "eval-v3-vs-v2",
					Link:         "/comparisons/eval-runner/eval-v3-vs-v2",
					Verdict:      v1alpha1.ComparisonVerdictUnchanged,
					VerdictChip:  "neutral",
					PImprovement: "0.48",
					Completed:    "2026-10-20 11:02 UTC",
					BaselineRun:  "eval-run-v2",
					BaselineLink: "/runs/eval-runner/eval-run-v2",
					Deltas: RunDeltas{
						Quality: MetricDelta{Text: "0.0 pp", Class: deltaClassZero},
						Tokens:  MetricDelta{Text: "0.0%", Class: deltaClassZero},
						Cost:    MetricDelta{Text: "$0.00 (0.0%)", Class: deltaClassZero},
					},
				},
			},
			{
				CandidateRun:  "eval-run-v2",
				CandidateLink: "/runs/eval-runner/eval-run-v2",
				Created:       "2026-10-20 02:12 UTC",
				VsBaseline: &ComparisonCell{
					Name:         "eval-v2-vs-v1",
					Link:         "/comparisons/eval-runner/eval-v2-vs-v1",
					Verdict:      v1alpha1.ComparisonVerdictImproved,
					VerdictChip:  "good",
					PImprovement: "0.99",
					Completed:    "2026-10-20 06:33 UTC",
					BaselineRun:  "eval-run-v1",
					BaselineLink: "/runs/eval-runner/eval-run-v1",
					Deltas: RunDeltas{
						Quality: MetricDelta{Text: "+6.7 pp", Class: deltaClassGood},
						Tokens:  MetricDelta{Text: "—", Class: deltaClassMissing},
						Cost:    MetricDelta{Text: "—", Class: deltaClassMissing},
					},
				},
			},
		},
		Comparisons: []ComparisonListItem{
			{
				Name:         "pr-42",
				Namespace:    "eval-runner",
				Verdict:      v1alpha1.ComparisonVerdictRunning,
				VerdictChip:  "pending",
				PImprovement: "—",
				Tasks:        "1/3",
				BaselineRun:  "base-a81d0c4",
				CandidateRun: "pr-42-05ce4d8",
				Completed:    "—",
				Age:          "3m",
				Link:         "/comparisons/eval-runner/pr-42",
				Reason:       "Running batch 2 (1 tasks continuing)",
				ReasonFull:   "Running batch 2 (1 tasks continuing)",
			},
		},
	}
}

func samplePullRequestsListView() PullRequestsListView {
	return PullRequestsListView{
		ActiveTab: NavTabPulls,
		Repo:      RepoRef{},
		Namespace: "eval-runner",
		Pulls: []PullRequestListItem{
			{
				Repository:     "gke-labs/eval-runner",
				RepoHash:       RepoHash("gke-labs/eval-runner"),
				Number:         42,
				State:          "Open",
				StateChip:      "open",
				GitHubURL:      "https://github.com/gke-labs/eval-runner/pull/42",
				HeadSHA:        "05ce4d8222222222222222222222222222222222",
				HeadSHAShort:   "05ce4d8",
				BaseSHA:        "a81d0c4111111111111111111111111111111111",
				BaseSHAShort:   "a81d0c4",
				Verdict:        v1alpha1.ComparisonVerdictImproved,
				VerdictChip:    "good",
				PImprovement:   "0.98",
				SamplesRun:     96,
				Batches:        2,
				CompletionTime: "5m",
				Age:            "10m",
				ComparisonName: "pr-42",
				ComparisonLink: "/github.com/gke-labs/eval-runner/comparisons/pr-42",
				HistoryLink:    "/github.com/gke-labs/eval-runner/pulls/42",
				RepoLink:       "/github.com/gke-labs/eval-runner/pulls",
				Namespace:      "eval-runner",
			},
		},
		TotalCount:   1,
		Offset:       0,
		Limit:        100,
		HasMore:      false,
		HasPrev:      false,
		ShowingRange: "Showing 1–1 of 1",
		State:        "all",
		OpenCount:    1,
		ClosedCount:  0,
		OpenLink:     "/pulls?state=open",
		ClosedLink:   "/pulls?state=closed",
		AllLink:      "/pulls?state=all",
	}
}

func sampleRepoPullRequestsListView() PullRequestsListView {
	v := samplePullRequestsListView()
	v.Repo = RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"}
	v.OpenLink = "/github.com/gke-labs/eval-runner/pulls?state=open"
	v.ClosedLink = "/github.com/gke-labs/eval-runner/pulls?state=closed"
	v.AllLink = "/github.com/gke-labs/eval-runner/pulls?state=all"
	return v
}

func samplePullRequestDetailView() PullRequestDetailView {
	return PullRequestDetailView{
		ActiveTab:  NavTabPulls,
		Repo:       RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"},
		Namespace:  "eval-runner",
		Repository: "gke-labs/eval-runner",
		RepoHash:   RepoHash("gke-labs/eval-runner"),
		Number:     42,
		GitHubURL:  "https://github.com/gke-labs/eval-runner/pull/42",
		BackLink:   "/github.com/gke-labs/eval-runner/pulls",
		TotalCount: 2,
		Comparisons: []PullRequestHistoryItem{
			{
				Name:           "pr-42-head2",
				Namespace:      "eval-runner",
				HeadSHA:        "05ce4d8222222222222222222222222222222222",
				HeadSHAShort:   "05ce4d8",
				BaseSHA:        "a81d0c4111111111111111111111111111111111",
				BaseSHAShort:   "a81d0c4",
				CandidateRun:   "docs-eval-cand-05ce4d8",
				BaselineRun:    "docs-eval-base-a81d0c4",
				CandidateLink:  "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-05ce4d8",
				BaselineLink:   "/github.com/gke-labs/eval-runner/runs/docs-eval-base-a81d0c4",
				Verdict:        v1alpha1.ComparisonVerdictImproved,
				VerdictChip:    "good",
				PImprovement:   "0.98",
				SamplesRun:     96,
				Batches:        2,
				CompletionTime: "5m",
				Age:            "10m",
				Link:           "/github.com/gke-labs/eval-runner/comparisons/pr-42-head2",
			},
			{
				Name:           "pr-42-head1",
				Namespace:      "eval-runner",
				HeadSHA:        "1111111222222222222222222222222222222222",
				HeadSHAShort:   "1111111",
				BaseSHA:        "a81d0c4111111111111111111111111111111111",
				BaseSHAShort:   "a81d0c4",
				CandidateRun:   "docs-eval-cand-1111111",
				BaselineRun:    "docs-eval-base-a81d0c4",
				CandidateLink:  "/github.com/gke-labs/eval-runner/runs/docs-eval-cand-1111111",
				BaselineLink:   "/github.com/gke-labs/eval-runner/runs/docs-eval-base-a81d0c4",
				Verdict:        v1alpha1.ComparisonVerdictInconclusive,
				VerdictChip:    "neutral",
				PImprovement:   "0.50",
				SamplesRun:     48,
				Batches:        1,
				CompletionTime: "15m",
				Age:            "20m",
				Link:           "/github.com/gke-labs/eval-runner/comparisons/pr-42-head1",
			},
		},
	}
}

func TestTemplateRenderingRunDetailGolden(t *testing.T) {
	view := sampleRunDetailView()

	var buf bytes.Buffer
	if err := tmplRunDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute run detail template: %v", err)
	}

	assertGolden(t, "run_detail.golden.html", buf.String())
}

func TestTemplateRenderingComparisonDetailGolden(t *testing.T) {
	view := sampleComparisonDetailView()

	var buf bytes.Buffer
	if err := tmplComparisonDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute comparison detail template: %v", err)
	}

	assertGolden(t, "comparison_detail.golden.html", buf.String())
}

func sampleRepoIndexView() RepoIndexView {
	return RepoIndexView{
		Namespace: "eval-runner",
		Repositories: []RepoListItem{
			{
				Host:             "github.com",
				Owner:            "gke-labs",
				Name:             "eval-runner",
				Repository:       "github.com/gke-labs/eval-runner",
				RunsCount:        42,
				ComparisonsCount: 12,
				Link:             "/github.com/gke-labs/eval-runner",
			},
		},
		TotalCount: 1,
	}
}

func TestTemplateRenderingRepoIndexGolden(t *testing.T) {
	view := sampleRepoIndexView()

	var buf bytes.Buffer
	if err := RenderRepoIndex(&buf, view); err != nil {
		t.Fatalf("failed to execute repo index template: %v", err)
	}

	assertGolden(t, "repo_index.golden.html", buf.String())
}

func TestTemplateRenderingRepoRunsGolden(t *testing.T) {
	ref := RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"}
	view := sampleRunsListView()
	view.Repo = ref
	for i := range view.Runs {
		view.Runs[i].Link = "/github.com/gke-labs/eval-runner/runs/" + view.Runs[i].Name
	}

	var buf bytes.Buffer
	if err := tmplRunsList.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute repo runs template: %v", err)
	}

	assertGolden(t, "repo_runs.golden.html", buf.String())
}

func sampleRepoOverviewView() RepoOverviewView {
	rate1 := 0.70
	rate2 := 0.85
	t1 := time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 10, 20, 2, 0, 0, 0, time.UTC)

	sparkItems := []SeriesItem{
		{
			CommitSHA:      "1111111111111111111111111111111111111111",
			CommitSHAShort: "1111111",
			CommitTime:     t1,
			CommitTimeStr:  "2026-10-20T01:00:00Z",
			RunName:        "base-1111111",
			Namespace:      "eval-runner",
			HasPoint:       true,
			SuccessRate:    &rate1,
		},
		{
			CommitSHA:      "2222222222222222222222222222222222222222",
			CommitSHAShort: "2222222",
			CommitTime:     t2,
			CommitTimeStr:  "2026-10-20T02:00:00Z",
			RunName:        "base-2222222",
			Namespace:      "eval-runner",
			HasPoint:       true,
			SuccessRate:    &rate2,
			CI: &v1alpha1.ConfidenceInterval{
				Lower:      0.70,
				Upper:      0.94,
				Confidence: 0.95,
				Method:     "wilson",
			},
		},
	}
	spark := BuildSparklineSVG(sparkItems)

	evalRows := []RepoOverviewEvalRow{
		{
			ID:          "triage/runbook_lookup",
			SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-2222222/tasks/triage%2Frunbook_lookup/samples",
			RunLink:     "/github.com/gke-labs/eval-runner/runs/base-2222222",
			ScoreStr:    "0.85",
			CILabel:     "[0.70, 0.94]",
			DeltaStr:    "Δ +0.15 [+0.02, +0.28]",
			DeltaClass:  "good",
			HasDelta:    true,
			Sparkline:   spark,
		},
	}

	suites := []RepoOverviewSuiteRow{
		{
			Name:       "triage",
			SuiteLink:  "/github.com/gke-labs/eval-runner/evals/triage",
			ScoreStr:   "0.85",
			CILabel:    "[0.70, 0.94]",
			EvalsCount: 1,
			DeltaStr:   "Δ +0.15 [+0.02, +0.28]",
			DeltaClass: "good",
			HasDelta:   true,
			Sparkline:  spark,
			Evals:      evalRows,
		},
	}

	return RepoOverviewView{
		ActiveTab:             NavTabOverview,
		Repo:                  RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"},
		Namespace:             "eval-runner",
		Branch:                "main",
		OverallScore:          "0.85",
		OverallCILabel:        "[0.70, 0.94]",
		OverallCommitSHA:      "2222222222222222222222222222222222222222",
		OverallCommitShort:    "2222222",
		OverallCommitLink:     "/github.com/gke-labs/eval-runner/runs/base-2222222",
		OverallTimeStr:        "10m",
		OverallTimeUTC:        "2026-10-20T02:00:00Z",
		Suites:                suites,
		TotalRunsCount:        2,
		RunsLink:              "/github.com/gke-labs/eval-runner/runs",
		TotalComparisonsCount: 1,
		ComparisonsLink:       "/github.com/gke-labs/eval-runner/comparisons",
		TotalPullsCount:       1,
		PullsLink:             "/github.com/gke-labs/eval-runner/pulls",
		IsEmpty:               false,
	}
}

func sampleRepoOverviewEmptyView() RepoOverviewView {
	return RepoOverviewView{
		ActiveTab:             NavTabOverview,
		Repo:                  RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"},
		Namespace:             "eval-runner",
		Branch:                "main",
		TotalRunsCount:        0,
		RunsLink:              "/github.com/gke-labs/eval-runner/runs",
		TotalComparisonsCount: 0,
		ComparisonsLink:       "/github.com/gke-labs/eval-runner/comparisons",
		TotalPullsCount:       0,
		PullsLink:             "/github.com/gke-labs/eval-runner/pulls",
		IsEmpty:               true,
	}
}

func TestTemplateRenderingRepoOverviewGolden(t *testing.T) {
	view := sampleRepoOverviewView()

	var buf bytes.Buffer
	if err := tmplRepoOverview.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute repo overview template: %v", err)
	}

	assertGolden(t, "repo_overview.golden.html", buf.String())
}

func TestTemplateRenderingRepoOverviewEmptyGolden(t *testing.T) {
	view := sampleRepoOverviewEmptyView()

	var buf bytes.Buffer
	if err := tmplRepoOverview.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute repo overview empty template: %v", err)
	}

	assertGolden(t, "repo_overview_empty.golden.html", buf.String())
}

func TestTemplateRenderingRepoComparisonDetailGolden(t *testing.T) {
	ref := RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"}
	baseRun, candRun, comp := sampleFixtures()
	view := NewComparisonDetailView(comp, baseRun, candRun, ref, "")

	var buf bytes.Buffer
	if err := tmplComparisonDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute repo comparison detail template: %v", err)
	}

	assertGolden(t, "repo_comparison_detail.golden.html", buf.String())
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
					Label:    v1alpha1.ComparisonTaskLabelImproved,
					Decision: v1alpha1.ComparisonTaskLabelImproved,
					Pairs:    16,
					Wins:     8,
					Losses:   2,
					Ties:     6,
					Tokens: &v1alpha1.TaskTokens{
						Input:     600,
						Output:    200,
						Total:     800,
						Reasoning: 100,
					},
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
					Label:         v1alpha1.ComparisonTaskLabelInconclusive,
					Decision:      v1alpha1.ComparisonTaskLabelContinue,
					Pairs:         16,
					Wins:          1,
					Losses:        2,
					Ties:          13,
					SuggestedNext: 16,
				},
				{
					ID:    "docs/readme_summary",
					Label: v1alpha1.ComparisonTaskLabelRunning,
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

func TestTemplateRenderingComparisonDetail_BatchesTokensGolden(t *testing.T) {
	comp, baseRun, candRun := batchesTokensComparisonFixture()
	view := NewComparisonDetailView(comp, baseRun, candRun, RepoRef{}, "")

	var buf bytes.Buffer
	if err := tmplComparisonDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute comparison detail template: %v", err)
	}

	assertGolden(t, "comparison_detail_batches_tokens.golden.html", buf.String())
}

func TestTemplateRenderingComparisonDetail_BudgetTile(t *testing.T) {
	comp, baseRun, candRun := batchesTokensComparisonFixture()
	budget := 0.50
	spent := 0.12
	comp.Spec.BudgetUSD = &budget
	comp.Status.Budget = &v1alpha1.ComparisonBudgetStatus{
		LimitUSD: &budget,
		SpentUSD: &spent,
		Enforced: true,
	}
	view := NewComparisonDetailView(comp, baseRun, candRun, RepoRef{}, "")
	if view.BudgetStr != "$0.12 / $0.50" {
		t.Errorf("expected BudgetStr $0.12 / $0.50, got %q", view.BudgetStr)
	}

	var buf bytes.Buffer
	if err := tmplComparisonDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute comparison detail template: %v", err)
	}
	html := buf.String()
	if !strings.Contains(html, `<div class="label">Budget</div>`) {
		t.Errorf("expected html to contain Budget label")
	}
	if !strings.Contains(html, `<div class="value num">$0.12 / $0.50</div>`) {
		t.Errorf("expected html to contain Budget value $0.12 / $0.50")
	}
}

func TestTemplateRenderingRunsListGolden(t *testing.T) {
	view := sampleRunsListView()

	var buf bytes.Buffer
	if err := tmplRunsList.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute runs list template: %v", err)
	}

	assertGolden(t, "runs_list.golden.html", buf.String())
}

func TestTemplateRenderingComparisonsListGolden(t *testing.T) {
	view := sampleComparisonsListView()

	var buf bytes.Buffer
	if err := tmplComparisonsList.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute comparisons list template: %v", err)
	}

	assertGolden(t, "comparisons_list.golden.html", buf.String())
}

func TestTemplateRenderingComparisonsListGroupedGolden(t *testing.T) {
	view := sampleComparisonsListGroupedView()

	var buf bytes.Buffer
	if err := tmplComparisonsList.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute comparisons list template: %v", err)
	}

	assertGolden(t, "comparisons_list_grouped.golden.html", buf.String())
}

func TestTemplateRenderingComparisonsList_FilteredEmptyState(t *testing.T) {
	view := ComparisonsListView{
		ActiveTab:          NavTabComparisons,
		Namespace:          "eval-runner",
		Kind:               ComparisonKindPrevious,
		KindLabelKey:       LabelVsPrevious,
		ShowKindFilter:     true,
		KindAllLink:        "/comparisons",
		KindBaselineLink:   "/comparisons?kind=baseline",
		KindPreviousLink:   "/comparisons?kind=previous",
		ShowGroups:         true,
		ShowPreviousColumn: true,
	}

	var buf bytes.Buffer
	if err := tmplComparisonsList.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute comparisons list template: %v", err)
	}
	body := buf.String()

	if !strings.Contains(body, "No Comparisons with label evalrunner.gke-labs.dev/vs-previous=true.") {
		t.Errorf("expected filtered empty state naming the label, got:\n%s", body)
	}
	if !strings.Contains(body, `<a href="/comparisons?kind=previous" class="filter-chip active">vs previous</a>`) {
		t.Errorf("expected the vs previous filter chip to be active")
	}
	if strings.Contains(body, "<th>vs baseline</th>") {
		t.Errorf("did not expect a vs baseline column when filtering by previous")
	}
	if strings.Contains(body, "Other comparisons") || strings.Contains(body, "<th>Comparison</th>") {
		t.Errorf("did not expect the unlabeled table in a filtered view")
	}
}

func TestTemplateRenderingComparisonDetail_StructuredReasonGolden(t *testing.T) {
	view := sampleComparisonDetailView()
	view.Message = structuredReasonMessage

	var buf bytes.Buffer
	if err := tmplComparisonDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute comparison detail template: %v", err)
	}
	body := buf.String()

	for _, want := range []string{
		`<div class="reason-headline">Pass rate fell on 2 of 4 tasks and rose on 1; the largest drop is docs/readme_comprehension, 80% -&gt; 40%.</div>`,
		`<p class="reason-para">Every task has 5 paired samples, below the 8 pairs needed to label a task, so all tasks are inconclusive.<br>The headline P(improvement) is the lowest per-task value.</p>`,
		`<div class="reason-block-heading">Largest drops:</div>`,
		`<li>docs/readme_comprehension 80% -&gt; 40% (3 timeouts)</li>`,
		`<li>1 incorrect &lt;b&gt;answer&lt;/b&gt; &amp; &#34;quoted&#34; text</li>`,
		`<p class="reason-para">Code changes: <a href="https://github.com/example/repo/compare/a81d0c4...05ce4d8" target="_blank" rel="noopener noreferrer">https://github.com/example/repo/compare/a81d0c4...05ce4d8</a></p>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expected %q in the structured reason panel", want)
		}
	}
	if strings.Contains(body, `class="reason-text"`) {
		t.Errorf("did not expect the plain reason block for a structured message")
	}

	assertGolden(t, "comparison_detail_reason_structured.golden.html", body)
}

func TestTemplateRenderingComparisonDetail_SingleParagraphReason(t *testing.T) {
	render := func(t *testing.T, message string) string {
		t.Helper()
		view := sampleComparisonDetailView()
		view.Message = message
		var buf bytes.Buffer
		if err := tmplComparisonDetail.Execute(&buf, view); err != nil {
			t.Fatalf("failed to execute comparison detail template: %v", err)
		}
		return buf.String()
	}

	t.Run("markup is unchanged", func(t *testing.T) {
		body := render(t, "Running batch 2 (1 tasks continuing) <i>now</i>")
		want := "        \n        <div class=\"reason-panel\"><div class=\"reason-label\">Reason</div><div class=\"reason-text\">Running batch 2 (1 tasks continuing) &lt;i&gt;now&lt;/i&gt;</div></div>\n        \n"
		if !strings.Contains(body, want) {
			t.Errorf("expected the single-paragraph panel markup to match the unstructured layout, got:\n%s", body)
		}
		if strings.Contains(body, `class="reason-headline"`) {
			t.Errorf("did not expect a headline for a message without line breaks")
		}
	})

	t.Run("URL becomes a link", func(t *testing.T) {
		body := render(t, "Code changes: https://example.com/compare/a...b.")
		want := `<div class="reason-text">Code changes: <a href="https://example.com/compare/a...b" target="_blank" rel="noopener noreferrer">https://example.com/compare/a...b</a>.</div>`
		if !strings.Contains(body, want) {
			t.Errorf("expected a link inside the plain reason block, got:\n%s", body)
		}
	})
}

// regressedComparisonFixture returns a REGRESSED Comparison with a publisher message and
// the two Runs it references, with per-task tokens and cost on both Runs.
func regressedComparisonFixture() (*v1alpha1.Comparison, *v1alpha1.Run, *v1alpha1.Run) {
	f := func(v float64) *float64 { return &v }
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "eval-v3-vs-v1",
			Namespace: "eval-runner",
			Labels:    map[string]string{LabelVsBaseline: "true"},
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "eval-run-v1"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "eval-run-v3"},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:          v1alpha1.ComparisonPhaseCompleted,
			Verdict:        v1alpha1.ComparisonVerdictRegressed,
			PImprovement:   f(0.05),
			CompletionTime: &metav1.Time{Time: time.Date(2026, 10, 20, 10, 35, 14, 0, time.UTC)},
			Message:        "Two tasks regressed after the prompt rewrite.\nreadme comprehension answers omit the install section in 3 of 5 samples.",
			Summary:        &v1alpha1.ComparisonSummary{Tasks: "4/4"},
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{ID: "docs/readme_summary", BaseRate: f(0.90), CandidateRate: f(0.90), Delta: f(0), DeltaCI: &v1alpha1.DeltaConfidenceInterval{Lower: -0.10, Upper: 0.10}, Label: v1alpha1.ComparisonTaskLabelUnchanged, Pairs: 5},
				{ID: "triage/runbook_lookup", BaseRate: f(0.70), CandidateRate: f(0.54), Delta: f(-0.16), DeltaCI: &v1alpha1.DeltaConfidenceInterval{Lower: -0.26, Upper: -0.06}, Label: v1alpha1.ComparisonTaskLabelRegressed, Pairs: 5},
				{ID: "docs/readme_comprehension", BaseRate: f(0.80), CandidateRate: f(0.40), Delta: f(-0.40), DeltaCI: &v1alpha1.DeltaConfidenceInterval{Lower: -0.60, Upper: -0.20}, Label: v1alpha1.ComparisonTaskLabelRegressed, Pairs: 5},
				{ID: "triage/escalation_policy", BaseRate: f(0.50), CandidateRate: f(0.70), Delta: f(0.20), DeltaCI: &v1alpha1.DeltaConfidenceInterval{Lower: 0.10, Upper: 0.30}, Label: v1alpha1.ComparisonTaskLabelImproved, Pairs: 5},
			},
		},
	}
	task := func(id string, rate float64, tokens int, cost float64) v1alpha1.TaskStatus {
		return v1alpha1.TaskStatus{
			ID:          id,
			Phase:       v1alpha1.TaskPhaseCompleted,
			Samples:     5,
			SuccessRate: f(rate),
			Tokens:      &v1alpha1.TaskTokens{Total: tokens},
			CostUSD:     f(cost),
		}
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "eval-run-v1", Namespace: "eval-runner"},
		Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{Commit: "a81d0c4111111111111111111111111111111111"}},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				task("docs/readme_summary", 0.90, 10000, 0.0020),
				task("triage/runbook_lookup", 0.70, 20000, 0.0040),
				task("docs/readme_comprehension", 0.80, 30000, 0.0060),
				task("triage/escalation_policy", 0.50, 40000, 0.0080),
			},
		},
	}
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "eval-run-v3", Namespace: "eval-runner"},
		Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{Commit: "05ce4d8222222222222222222222222222222222"}},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				task("docs/readme_summary", 0.90, 9000, 0.0018),
				task("triage/runbook_lookup", 0.54, 21000, 0.0042),
				task("docs/readme_comprehension", 0.40, 26000, 0.0052),
				task("triage/escalation_policy", 0.70, 41000, 0.0082),
			},
		},
	}
	return comp, baseRun, candRun
}

func TestTemplateRenderingComparisonDetail_RegressedGolden(t *testing.T) {
	comp, baseRun, candRun := regressedComparisonFixture()
	view := NewComparisonDetailView(comp, baseRun, candRun, RepoRef{}, "")

	var buf bytes.Buffer
	if err := tmplComparisonDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute comparison detail template: %v", err)
	}
	body := buf.String()

	if strings.Contains(body, `class="msg-alert"`) {
		t.Errorf("expected the status message in a neutral reason panel, not an error alert")
	}
	if !strings.Contains(body, `<div class="reason-headline">Two tasks regressed after the prompt rewrite.</div>`) {
		t.Errorf("expected the first line of the status message as the reason headline")
	}
	idxComprehension := strings.Index(body, `<td class="mono" style="font-weight:600">docs/readme_comprehension</td>`)
	idxRunbook := strings.Index(body, `<td class="mono" style="font-weight:600">triage/runbook_lookup</td>`)
	if idxComprehension == -1 || idxRunbook == -1 || idxComprehension > idxRunbook {
		t.Errorf("expected regressed tasks ordered by most negative delta first: comprehension=%d runbook=%d", idxComprehension, idxRunbook)
	}

	assertGolden(t, "comparison_detail_regressed.golden.html", body)
}

func TestTemplateRenderingPullsListGolden(t *testing.T) {
	view := samplePullRequestsListView()

	var buf bytes.Buffer
	if err := tmplPullsList.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute pulls list template: %v", err)
	}

	assertGolden(t, "pulls_list.golden.html", buf.String())
}

func TestTemplateRenderingRepoPullsGolden(t *testing.T) {
	view := sampleRepoPullRequestsListView()

	var buf bytes.Buffer
	if err := tmplPullsList.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute repo pulls list template: %v", err)
	}

	assertGolden(t, "repo_pulls.golden.html", buf.String())
}

func TestTemplateRenderingPullDetailGolden(t *testing.T) {
	view := samplePullRequestDetailView()

	var buf bytes.Buffer
	if err := tmplPullDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute pull detail template: %v", err)
	}

	assertGolden(t, "pull_detail.golden.html", buf.String())
}

func TestTemplateRenderingPullsList_RepoScopedVsLegacy(t *testing.T) {
	t.Run("RepoScoped", func(t *testing.T) {
		view := sampleRepoPullRequestsListView()
		var buf bytes.Buffer
		if err := tmplPullsList.Execute(&buf, view); err != nil {
			t.Fatalf("failed to execute pulls list template: %v", err)
		}
		body := buf.String()
		if strings.Contains(body, "<th>Repository</th>") {
			t.Errorf("expected repo-scoped pulls list not to contain <th>Repository</th>")
		}
		if strings.Contains(body, `<td><a href="/github.com/gke-labs/eval-runner/pulls" style="font-weight:600">gke-labs/eval-runner</a></td>`) {
			t.Errorf("expected repo-scoped pulls list row not to contain repository column td")
		}

		// Empty state colspan
		view.Pulls = nil
		buf.Reset()
		if err := tmplPullsList.Execute(&buf, view); err != nil {
			t.Fatalf("failed to execute pulls list template with empty pulls: %v", err)
		}
		if !strings.Contains(buf.String(), `<td colspan="8" class="empty-state">No pull requests found.</td>`) {
			t.Errorf("expected colspan=8 in repo-scoped empty state, got:\n%s", buf.String())
		}
	})

	t.Run("LegacyGlobal", func(t *testing.T) {
		view := samplePullRequestsListView()
		var buf bytes.Buffer
		if err := tmplPullsList.Execute(&buf, view); err != nil {
			t.Fatalf("failed to execute pulls list template: %v", err)
		}
		body := buf.String()
		if !strings.Contains(body, "<th>Repository</th>") {
			t.Errorf("expected legacy global pulls list to contain <th>Repository</th>")
		}
		if !strings.Contains(body, `<td><a href="/github.com/gke-labs/eval-runner/pulls" style="font-weight:600">gke-labs/eval-runner</a></td>`) {
			t.Errorf("expected legacy global pulls list row to contain repository column td")
		}

		// Empty state colspan
		view.Pulls = nil
		buf.Reset()
		if err := tmplPullsList.Execute(&buf, view); err != nil {
			t.Fatalf("failed to execute pulls list template with empty pulls: %v", err)
		}
		if !strings.Contains(buf.String(), `<td colspan="9" class="empty-state">No pull requests found.</td>`) {
			t.Errorf("expected colspan=9 in legacy global empty state, got:\n%s", buf.String())
		}
	})
}

func TestTemplateRenderingPullDetail_RepoScopedVsLegacy(t *testing.T) {
	t.Run("RepoScoped", func(t *testing.T) {
		view := samplePullRequestDetailView()
		var buf bytes.Buffer
		if err := tmplPullDetail.Execute(&buf, view); err != nil {
			t.Fatalf("failed to execute pull detail template: %v", err)
		}
		body := buf.String()
		wantTitle := `<h1 class="card-title"><a href="https://github.com/gke-labs/eval-runner/pull/42" target="_blank" rel="noopener">#42</a></h1>`
		if !strings.Contains(body, wantTitle) {
			t.Errorf("expected repo-scoped card title to be %q, got body:\n%s", wantTitle, body)
		}
		if strings.Contains(body, "<span><b>Repository:</b>") {
			t.Errorf("expected repo-scoped pull detail footer not to contain Repository")
		}
	})

	t.Run("LegacyGlobal", func(t *testing.T) {
		view := samplePullRequestDetailView()
		view.Repo = RepoRef{} // clear repo to simulate legacy /pulls/{repohash}/{number} route
		var buf bytes.Buffer
		if err := tmplPullDetail.Execute(&buf, view); err != nil {
			t.Fatalf("failed to execute pull detail template: %v", err)
		}
		body := buf.String()
		wantTitle := `<h1 class="card-title">gke-labs/eval-runner <a href="https://github.com/gke-labs/eval-runner/pull/42" target="_blank" rel="noopener">#42</a></h1>`
		if !strings.Contains(body, wantTitle) {
			t.Errorf("expected legacy card title to be %q, got body:\n%s", wantTitle, body)
		}
		wantFooter := `<span><b>Repository:</b> gke-labs/eval-runner</span>`
		if !strings.Contains(body, wantFooter) {
			t.Errorf("expected legacy pull detail footer to contain %q, got body:\n%s", wantFooter, body)
		}
	})
}

func TestTemplateRenderingComparisonTableGolden(t *testing.T) {
	tasks := sampleComparisonDetailView().Tasks

	var buf bytes.Buffer
	if err := RenderComparisonTable(&buf, ComparisonTableView{Tasks: tasks}); err != nil {
		t.Fatalf("failed to execute comparison table sub-template: %v", err)
	}

	assertGolden(t, "comparison_table.golden.html", buf.String())
}

func TestTemplateRenderingComparisonTable_WithAffinity(t *testing.T) {
	tasks := []ComparisonTaskRow{
		{
			ID:          "docs/readme_comprehension",
			Affinity:    "matched",
			BaseRate:    "0.60",
			CandRate:    "0.80",
			Delta:       "+0.20",
			HasCI:       true,
			CILeftPx:    160.0,
			CIWidthPx:   40.0,
			CIDotPx:     180.0,
			CILabel:     "[+0.05, +0.35]",
			StatusClass: "good",
			Label:       "improved",
			SamplesRun:  16,
		},
		{
			ID:          "src/other_task",
			Affinity:    "unmatched",
			BaseRate:    "0.50",
			CandRate:    "0.50",
			Delta:       "+0.00",
			StatusClass: "neutral",
			Label:       "unchanged",
			SamplesRun:  16,
		},
	}

	var buf bytes.Buffer
	if err := RenderComparisonTable(&buf, ComparisonTableView{Tasks: tasks, HasAffinity: true}); err != nil {
		t.Fatalf("failed to execute comparison table with affinity: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "<th>affinity</th>") {
		t.Errorf("expected <th>affinity</th> in table header, got:\n%s", output)
	}
	if !strings.Contains(output, "<td class=\"mono\">matched</td>") {
		t.Errorf("expected matched affinity cell, got:\n%s", output)
	}
	if !strings.Contains(output, "<td class=\"mono\">unmatched</td>") {
		t.Errorf("expected unmatched affinity cell, got:\n%s", output)
	}
}

func TestTemplateRenderingRunDetail_RunningProgress(t *testing.T) {
	view := sampleRunDetailView()
	view.Phase = v1alpha1.RunPhaseRunning
	view.Tasks = []RunTaskRow{
		{
			ID:             "triage/runbook_lookup",
			Phase:          v1alpha1.TaskPhaseRunning,
			PhaseChip:      "warn",
			RateStr:        "—",
			SamplesDone:    8,
			SamplesPlanned: 96,
			ProgressPct:    8.333333,
			ProgressText:   "8 of 96 samples",
		},
	}

	var buf bytes.Buffer
	if err := tmplRunDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute run detail template: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, `<meta http-equiv="refresh" content="15">`) {
		t.Errorf("expected meta refresh on running run page, got:\n%s", html)
	}
	if !strings.Contains(html, "8 of 96 samples") {
		t.Errorf("expected '8 of 96 samples' in rendered HTML, got:\n%s", html)
	}
	if !strings.Contains(html, `class="progress-bar"`) {
		t.Errorf("expected progress-bar in rendered HTML")
	}
	if !strings.Contains(html, `class="progress-fill"`) {
		t.Errorf("expected progress-fill in rendered HTML")
	}
}

func TestTemplateRenderingRunDetail_CompletedNoMetaRefresh(t *testing.T) {
	view := sampleRunDetailView()
	view.Phase = v1alpha1.RunPhaseCompleted

	var buf bytes.Buffer
	if err := tmplRunDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute run detail template: %v", err)
	}

	html := buf.String()
	if strings.Contains(html, `http-equiv="refresh"`) {
		t.Errorf("expected NO meta refresh on completed run page")
	}
}

func TestTemplateRenderingRunsList_RunningTasks(t *testing.T) {
	view := sampleRunsListView()
	view.Runs = append(view.Runs, RunListItem{
		Name:      "running-run",
		Namespace: "eval-runner",
		Phase:     "Running",
		PhaseChip: "warn",
		Tasks:     "1/2 tasks running",
		Link:      "/runs/eval-runner/running-run",
	})

	var buf bytes.Buffer
	if err := tmplRunsList.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute runs list template: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "1/2 tasks running") {
		t.Errorf("expected '1/2 tasks running' in rendered HTML, got:\n%s", html)
	}
}

func TestEscapingHostileFixtures(t *testing.T) {
	const (
		hostileScript = `<script>alert('pwned')</script>`
		hostileImg    = `'"><img src=x onerror=alert('img')>`
		hostileIframe = `"><iframe src="javascript:alert('frame')"></iframe>`
	)

	t.Run("RunDetailEscaping", func(t *testing.T) {
		view := RunDetailView{
			ActiveTab:       NavTabRuns,
			Name:            "hostile-run",
			Namespace:       "eval-runner",
			Phase:           v1alpha1.RunPhaseFailed,
			PhaseChip:       "bad",
			RepoURL:         "https://github.com/evil/repo" + hostileImg,
			Commit:          "commit" + hostileScript,
			CommitShort:     "c" + hostileScript,
			Model:           "model" + hostileIframe,
			Age:             "1m",
			ArtifactsPrefix: "gs://bucket/" + hostileScript,
			Message:         "run failed: " + hostileScript + hostileImg,
			Tasks: []RunTaskRow{
				{
					ID:        "task/" + hostileScript,
					Phase:     v1alpha1.TaskPhaseFailed,
					PhaseChip: "bad",
					RateStr:   "0.00",
					Samples:   10,
					Successes: 0,
					Method:    hostileScript,
					LogPath:   "gs://bucket/" + hostileImg,
					Message:   "task message: " + hostileScript,
				},
			},
		}

		var buf bytes.Buffer
		if err := tmplRunDetail.Execute(&buf, view); err != nil {
			t.Fatalf("template render error: %v", err)
		}
		html := buf.String()

		for _, dangerous := range []string{"<script", "<img", "<iframe"} {
			if strings.Contains(html, dangerous) {
				t.Errorf("unescaped dangerous tag found in RunDetail HTML: %q", dangerous)
			}
		}

		// Ensure escaped entities exist
		if !strings.Contains(html, "&lt;script&gt;") {
			t.Errorf("expected escaped &lt;script&gt; in HTML")
		}
		if !strings.Contains(html, "&lt;img") {
			t.Errorf("expected escaped &lt;img in HTML")
		}
		if !strings.Contains(html, "&lt;iframe") {
			t.Errorf("expected escaped &lt;iframe in HTML")
		}
	})

	t.Run("ComparisonDetailEscaping", func(t *testing.T) {
		view := ComparisonDetailView{
			ActiveTab:    NavTabComparisons,
			Name:         "hostile-comp",
			Namespace:    "eval-runner",
			Verdict:      v1alpha1.ComparisonVerdictRegressed,
			VerdictChip:  "bad",
			Headline:     "headline " + hostileScript,
			PImprovement: "0.00",
			SummaryPath:  "gs://bucket/" + hostileImg,
			Message:      "status: " + hostileIframe,
			Tasks: []ComparisonTaskRow{
				{
					ID:          "task/" + hostileScript,
					BaseRate:    "0.50",
					CandRate:    "0.20",
					Delta:       "-0.30" + hostileScript,
					HasCI:       true,
					CILeftPx:    50.0,
					CIWidthPx:   30.0,
					CIDotPx:     60.0,
					CILabel:     "[-0.40, -0.20]" + hostileImg,
					StatusClass: "bad",
					Label:       "regressed",
					Pairs:       20,
				},
			},
			BaseRun:         "base" + hostileScript,
			BaseCommitShort: "b" + hostileScript,
			CandRun:         "cand" + hostileScript,
			CandCommitShort: "c" + hostileScript,
		}

		var buf bytes.Buffer
		if err := tmplComparisonDetail.Execute(&buf, view); err != nil {
			t.Fatalf("template render error: %v", err)
		}
		html := buf.String()

		for _, dangerous := range []string{"<script", "<img", "<iframe"} {
			if strings.Contains(html, dangerous) {
				t.Errorf("unescaped dangerous tag found in ComparisonDetail HTML: %q", dangerous)
			}
		}

		if !strings.Contains(html, "&lt;script&gt;") {
			t.Errorf("expected escaped &lt;script&gt; in HTML")
		}
		if !strings.Contains(html, "&lt;img") {
			t.Errorf("expected escaped &lt;img in HTML")
		}
		if !strings.Contains(html, "&lt;iframe") {
			t.Errorf("expected escaped &lt;iframe in HTML")
		}
	})

	t.Run("ComparisonsListEscaping", func(t *testing.T) {
		hostileDeltas := RunDeltas{
			Quality: MetricDelta{Text: "+1.0 pp" + hostileScript, Title: "title" + hostileImg, Class: deltaClassGood},
			Tokens:  MetricDelta{Text: "—", Title: hostileIframe, Class: deltaClassMissing},
			Cost:    MetricDelta{Text: "$0.00" + hostileImg, Class: deltaClassZero},
		}
		cell := &ComparisonCell{
			Name:         "comp" + hostileScript,
			Link:         "/comparisons/eval-runner/" + hostileImg,
			Verdict:      v1alpha1.ComparisonVerdictRegressed,
			VerdictChip:  "bad",
			PImprovement: "0.05" + hostileIframe,
			Completed:    "2026-10-20 10:35 UTC" + hostileScript,
			BaselineRun:  "base" + hostileScript,
			BaselineLink: "/runs/eval-runner/" + hostileImg,
			Reason:       "reason " + hostileScript,
			ReasonFull:   "full reason " + hostileImg,
			Deltas:       hostileDeltas,
			Older:        []ComparisonRef{{Name: "older" + hostileScript, Link: "/comparisons/eval-runner/" + hostileIframe}},
		}
		view := ComparisonsListView{
			ActiveTab:          NavTabComparisons,
			Namespace:          "eval-runner",
			ShowKindFilter:     true,
			KindAllLink:        "/comparisons?namespace=" + hostileImg,
			KindBaselineLink:   "/comparisons?kind=baseline",
			KindPreviousLink:   "/comparisons?kind=previous",
			ShowGroups:         true,
			ShowBaselineColumn: true,
			ShowPreviousColumn: true,
			ShowFlat:           true,
			Groups: []ComparisonGroupRow{{
				CandidateRun:  "cand" + hostileScript,
				CandidateLink: "/runs/eval-runner/" + hostileImg,
				Created:       "2026-10-20 02:14 UTC" + hostileIframe,
				VsBaseline:    cell,
				VsPrevious:    cell,
			}},
			Comparisons: []ComparisonListItem{{
				Name:         "comp" + hostileScript,
				Namespace:    "eval-runner",
				Verdict:      v1alpha1.ComparisonVerdictRegressed,
				VerdictChip:  "bad",
				PImprovement: "0.05",
				Tasks:        "1/1",
				BaselineRun:  "base" + hostileScript,
				CandidateRun: "cand" + hostileScript,
				Completed:    "—" + hostileImg,
				Age:          "1m",
				Link:         "/comparisons/eval-runner/" + hostileIframe,
				Reason:       "reason " + hostileScript,
				ReasonFull:   "full reason \"" + hostileImg,
				Deltas:       hostileDeltas,
			}},
		}

		var buf bytes.Buffer
		if err := tmplComparisonsList.Execute(&buf, view); err != nil {
			t.Fatalf("template render error: %v", err)
		}
		html := buf.String()

		for _, dangerous := range []string{"<script", "<img", "<iframe"} {
			if strings.Contains(html, dangerous) {
				t.Errorf("unescaped dangerous tag found in ComparisonsList HTML: %q", dangerous)
			}
		}
		if !strings.Contains(html, "&lt;script&gt;") {
			t.Errorf("expected escaped &lt;script&gt; in HTML")
		}
	})

	t.Run("ComparisonDetailStructuredReasonEscaping", func(t *testing.T) {
		view := sampleComparisonDetailView()
		view.Message = "Headline " + hostileScript + "\n\n" +
			"Heading " + hostileImg + ":\n" +
			"- bullet " + hostileIframe + "\n" +
			"- link https://evil.example/x" + hostileImg + "\n\n" +
			"javascript:alert('x') " + hostileScript
		var buf bytes.Buffer
		if err := tmplComparisonDetail.Execute(&buf, view); err != nil {
			t.Fatalf("template render error: %v", err)
		}
		html := buf.String()

		for _, dangerous := range []string{"<script", "<img", "<iframe", `href="javascript`} {
			if strings.Contains(html, dangerous) {
				t.Errorf("unescaped dangerous content in structured reason panel: %q", dangerous)
			}
		}
		if !strings.Contains(html, `<a href="https://evil.example/x" target="_blank" rel="noopener noreferrer">https://evil.example/x</a>&#39;&#34;&gt;&lt;img`) {
			t.Errorf("expected the link to stop at the quote and the rest to stay escaped text")
		}
	})

	t.Run("ComparisonDetailRegressedTasksEscaping", func(t *testing.T) {
		comp, baseRun, candRun := regressedComparisonFixture()
		comp.Status.Message = "reason " + hostileScript + hostileIframe
		comp.Status.Tasks[1].ID = "task/" + hostileImg
		view := NewComparisonDetailView(comp, baseRun, candRun, RepoRef{}, "")

		var buf bytes.Buffer
		if err := tmplComparisonDetail.Execute(&buf, view); err != nil {
			t.Fatalf("template render error: %v", err)
		}
		html := buf.String()

		for _, dangerous := range []string{"<script", "<img", "<iframe"} {
			if strings.Contains(html, dangerous) {
				t.Errorf("unescaped dangerous tag found in ComparisonDetail HTML: %q", dangerous)
			}
		}
		if !strings.Contains(html, "&lt;img") {
			t.Errorf("expected escaped regressed task ID in HTML")
		}
	})

	t.Run("ComparisonTableSubTemplateEscaping", func(t *testing.T) {
		tasks := []ComparisonTaskRow{
			{
				ID:          "hostile/" + hostileScript,
				BaseRate:    "0.60",
				CandRate:    "0.40",
				Delta:       "-0.20" + hostileScript,
				HasCI:       true,
				CILeftPx:    100.0,
				CIWidthPx:   40.0,
				CIDotPx:     120.0,
				CILabel:     "[-0.30, -0.10]" + hostileImg,
				StatusClass: "bad",
				Label:       "regressed",
				Pairs:       15,
			},
		}

		var buf bytes.Buffer
		if err := RenderComparisonTable(&buf, ComparisonTableView{Tasks: tasks}); err != nil {
			t.Fatalf("RenderComparisonTable error: %v", err)
		}
		html := buf.String()

		for _, dangerous := range []string{"<script", "<img", "<iframe"} {
			if strings.Contains(html, dangerous) {
				t.Errorf("unescaped dangerous content in comparison table sub-template: %q", dangerous)
			}
		}
		if !strings.Contains(html, "&lt;script&gt;") {
			t.Errorf("expected escaped &lt;script&gt; in sub-template HTML")
		}
	})

	t.Run("TaskSamplesListEscaping", func(t *testing.T) {
		view := TaskSamplesListView{
			ActiveTab:     NavTabRuns,
			Namespace:     "eval-runner",
			RunName:       "hostile-run" + hostileScript,
			TaskID:        "task/" + hostileImg,
			EscapedTaskID: "task%2Fescaped",
			Commit:        "c" + hostileScript,
			Model:         "model" + hostileIframe,
			LogPath:       "gs://bucket/" + hostileScript,
			Samples: []TaskSampleRow{
				{
					ID:                 "sample-" + hostileScript,
					Score:              "0.00",
					InputPreview:       "input " + hostileScript,
					OutputPreview:      "output " + hostileImg,
					Target:             "rule \"" + hostileIframe,
					TargetPreview:      "rule " + hostileScript,
					Explanation:        "reason \"" + hostileImg,
					ExplanationPreview: "reason " + hostileIframe,
					LatencyStr:         "1.0s",
					TokensStr:          "100",
					Link:               "/runs/eval-runner/hostile/samples/1",
				},
			},
		}

		var buf bytes.Buffer
		if err := RenderTaskSamplesList(&buf, view); err != nil {
			t.Fatalf("RenderTaskSamplesList error: %v", err)
		}
		html := buf.String()

		for _, dangerous := range []string{"<script", "<img", "<iframe"} {
			if strings.Contains(html, dangerous) {
				t.Errorf("unescaped dangerous content in task samples list template: %q", dangerous)
			}
		}
		if !strings.Contains(html, "&lt;script&gt;") {
			t.Errorf("expected escaped &lt;script&gt; in task samples list HTML")
		}
	})

	t.Run("SampleTranscriptEscaping", func(t *testing.T) {
		view := SampleTranscriptView{
			ActiveTab:   NavTabRuns,
			Namespace:   "eval-runner",
			RunName:     "hostile-run" + hostileScript,
			TaskID:      "task/" + hostileImg,
			SampleID:    "sample-" + hostileScript,
			Score:       "0.00",
			Status:      "fail",
			StatusUpper: "FAIL",
			StatusClass: "bad",
			LatencyStr:  "1.0s",
			TokensStr:   "100",
			LogPath:     "gs://bucket/" + hostileScript,
			Target:      "rule " + hostileIframe,
			Explanation: "reason " + hostileImg,
			Rows: []TranscriptRowView{
				{Kind: "user", Content: "user " + hostileScript},
				{Kind: "tool_call", Class: "tool", Content: "call " + hostileImg},
				{Kind: "tool", Class: "tool", Content: "tool " + hostileIframe},
				{Kind: "assistant", Class: "bad", Content: "assistant " + hostileScript},
			},
		}

		var buf bytes.Buffer
		if err := RenderSampleTranscript(&buf, view); err != nil {
			t.Fatalf("RenderSampleTranscript error: %v", err)
		}
		html := buf.String()

		for _, dangerous := range []string{"<script", "<img", "<iframe"} {
			if strings.Contains(html, dangerous) {
				t.Errorf("unescaped dangerous content in sample transcript template: %q", dangerous)
			}
		}
		if !strings.Contains(html, "&lt;script&gt;") {
			t.Errorf("expected escaped &lt;script&gt; in sample transcript HTML")
		}
	})

	t.Run("PullsListEscaping", func(t *testing.T) {
		view := PullRequestsListView{
			ActiveTab: NavTabPulls,
			Namespace: "eval-runner",
			Pulls: []PullRequestListItem{
				{
					Repository:     "org/repo" + hostileScript,
					RepoHash:       "badhash",
					Number:         12,
					GitHubURL:      "https://github.com/evil/repo" + hostileImg,
					HeadSHA:        "head" + hostileScript,
					HeadSHAShort:   "h" + hostileScript,
					BaseSHA:        "base" + hostileScript,
					BaseSHAShort:   "b" + hostileScript,
					Verdict:        v1alpha1.ComparisonVerdictImproved,
					VerdictChip:    "good",
					PImprovement:   "0.99" + hostileIframe,
					SamplesRun:     10,
					Batches:        1,
					CompletionTime: "1m" + hostileScript,
					Age:            "5m" + hostileImg,
					ComparisonName: "comp" + hostileScript,
					ComparisonLink: "/comparisons/eval-runner/" + hostileImg,
					HistoryLink:    "/pulls/badhash/12" + hostileScript,
					RepoLink:       "/pulls" + hostileScript,
					Namespace:      "eval-runner",
				},
			},
		}

		var buf bytes.Buffer
		if err := tmplPullsList.Execute(&buf, view); err != nil {
			t.Fatalf("tmplPullsList error: %v", err)
		}
		html := buf.String()

		for _, dangerous := range []string{"<script", "<img", "<iframe"} {
			if strings.Contains(html, dangerous) {
				t.Errorf("unescaped dangerous content in pulls list template: %q", dangerous)
			}
		}
		if !strings.Contains(html, "&lt;script&gt;") {
			t.Errorf("expected escaped &lt;script&gt; in pulls list HTML")
		}
	})

	t.Run("PullDetailEscaping", func(t *testing.T) {
		view := PullRequestDetailView{
			ActiveTab:  NavTabPulls,
			Namespace:  "eval-runner",
			Repository: "org/repo" + hostileScript,
			RepoHash:   "badhash",
			Number:     12,
			GitHubURL:  "https://github.com/evil/repo" + hostileImg,
			BackLink:   "/pulls" + hostileIframe,
			TotalCount: 1,
			Comparisons: []PullRequestHistoryItem{
				{
					Name:           "comp" + hostileScript,
					Namespace:      "eval-runner",
					HeadSHA:        "head" + hostileScript,
					HeadSHAShort:   "h" + hostileScript,
					BaseSHA:        "base" + hostileScript,
					BaseSHAShort:   "b" + hostileScript,
					CandidateRun:   "cand" + hostileScript,
					BaselineRun:    "base" + hostileScript,
					CandidateLink:  "/runs/cand" + hostileScript,
					BaselineLink:   "/runs/base" + hostileScript,
					Verdict:        v1alpha1.ComparisonVerdictImproved,
					VerdictChip:    "good",
					PImprovement:   "0.99" + hostileIframe,
					SamplesRun:     10,
					Batches:        1,
					CompletionTime: "1m" + hostileScript,
					Age:            "5m" + hostileImg,
					Link:           "/comparisons/eval-runner/comp" + hostileScript,
				},
			},
		}

		var buf bytes.Buffer
		if err := tmplPullDetail.Execute(&buf, view); err != nil {
			t.Fatalf("tmplPullDetail error: %v", err)
		}
		html := buf.String()

		for _, dangerous := range []string{"<script", "<img", "<iframe"} {
			if strings.Contains(html, dangerous) {
				t.Errorf("unescaped dangerous content in pull detail template: %q", dangerous)
			}
		}
		if !strings.Contains(html, "&lt;script&gt;") {
			t.Errorf("expected escaped &lt;script&gt; in pull detail HTML")
		}
	})
}

func TestComparisonRows(t *testing.T) {
	t.Run("NilComparison", func(t *testing.T) {
		rows := ComparisonRows(nil)
		if rows != nil {
			t.Errorf("expected nil rows for nil comparison, got %v", rows)
		}
	})

	t.Run("EmptyTasks", func(t *testing.T) {
		comp := &v1alpha1.Comparison{}
		rows := ComparisonRows(comp)
		if rows == nil || len(rows) != 0 {
			t.Errorf("expected empty non-nil slice for empty tasks, got %v", rows)
		}
	})

	t.Run("FormattingAndGeometry", func(t *testing.T) {
		baseRate := 0.612
		candRate := 0.842
		delta := 0.230
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:            "task/improved",
						BaseRate:      &baseRate,
						CandidateRate: &candRate,
						Delta:         &delta,
						DeltaCI: &v1alpha1.DeltaConfidenceInterval{
							Lower: 0.11,
							Upper: 0.35,
						},
						Dropped: v1alpha1.ComparisonDropped{
							Candidate: 2,
							Baseline:  1,
						},
						Label: v1alpha1.ComparisonTaskLabelImproved,
						Pairs: 20,
					},
					{
						ID:      "task/running",
						Label:   v1alpha1.ComparisonTaskLabelRunning,
						Pairs:   0,
						Dropped: v1alpha1.ComparisonDropped{},
					},
				},
			},
		}

		rows := ComparisonRows(comp)
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}

		r0 := rows[0]
		if r0.ID != "task/improved" {
			t.Errorf("expected ID 'task/improved', got %q", r0.ID)
		}
		if r0.BaseRate != "0.61" {
			t.Errorf("expected BaseRate '0.61', got %q", r0.BaseRate)
		}
		if r0.CandRate != "0.84" {
			t.Errorf("expected CandRate '0.84', got %q", r0.CandRate)
		}
		if r0.Delta != "+0.23" {
			t.Errorf("expected Delta '+0.23', got %q", r0.Delta)
		}
		if !r0.HasCI {
			t.Errorf("expected HasCI to be true")
		}
		if r0.CILabel != "[+0.11, +0.35]" {
			t.Errorf("expected CILabel '[+0.11, +0.35]', got %q", r0.CILabel)
		}
		if r0.StatusClass != "good" {
			t.Errorf("expected StatusClass 'good', got %q", r0.StatusClass)
		}
		if r0.Label != "improved" {
			t.Errorf("expected Label 'improved', got %q", r0.Label)
		}
		if r0.Pairs != 20 {
			t.Errorf("expected Pairs 20, got %d", r0.Pairs)
		}
		if r0.DroppedCand != 2 {
			t.Errorf("expected DroppedCand 2, got %d", r0.DroppedCand)
		}
		if r0.DroppedBase != 1 {
			t.Errorf("expected DroppedBase 1, got %d", r0.DroppedBase)
		}
		if r0.CILeftPx <= 0 || r0.CIWidthPx <= 0 || r0.CIDotPx <= 0 {
			t.Errorf("expected non-zero geometry values, got left=%.1f width=%.1f dot=%.1f", r0.CILeftPx, r0.CIWidthPx, r0.CIDotPx)
		}

		r1 := rows[1]
		if r1.BaseRate != "—" || r1.CandRate != "—" || r1.Delta != "—" || r1.CILabel != "—" {
			t.Errorf("expected dashes for nil rates/delta/CI, got base=%s cand=%s delta=%s ci=%s", r1.BaseRate, r1.CandRate, r1.Delta, r1.CILabel)
		}
		if r1.HasCI {
			t.Errorf("expected HasCI false for nil CI")
		}
		if r1.StatusClass != "pending" {
			t.Errorf("expected StatusClass 'pending' for running, got %q", r1.StatusClass)
		}
	})
}

func TestComparisonRows_SubTemplateConsumes(t *testing.T) {
	baseRate := 0.50
	candRate := 0.75
	delta := 0.25
	comp := &v1alpha1.Comparison{
		Status: v1alpha1.ComparisonStatus{
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:            "eval/test_task",
					BaseRate:      &baseRate,
					CandidateRate: &candRate,
					Delta:         &delta,
					DeltaCI: &v1alpha1.DeltaConfidenceInterval{
						Lower: 0.10,
						Upper: 0.40,
					},
					Label: v1alpha1.ComparisonTaskLabelImproved,
					Pairs: 12,
				},
			},
		},
	}

	rows := ComparisonRows(comp)
	var buf bytes.Buffer
	if err := RenderComparisonTable(&buf, ComparisonTableView{Tasks: rows}); err != nil {
		t.Fatalf("failed to render comparison table sub-template with ComparisonRows: %v", err)
	}

	rawHTML := buf.String()
	unescapedHTML := html.UnescapeString(rawHTML)
	expectedSubstrings := []string{
		"eval/test_task",
		"0.50",
		"0.75",
		"+0.25",
		"[+0.10, +0.40]",
		"chip good",
		"improved",
		"12",
	}
	for _, expected := range expectedSubstrings {
		if !strings.Contains(unescapedHTML, expected) {
			t.Errorf("expected sub-template HTML to contain %q, but got:\n%s", expected, unescapedHTML)
		}
	}
}

func TestComparisonSummary(t *testing.T) {
	t.Run("NilComparison", func(t *testing.T) {
		sum := ComparisonSummary(nil, nil, nil)
		if sum.Headline != "" {
			t.Errorf("expected empty headline for nil comp, got %q", sum.Headline)
		}
		if sum.PImprovement != "—" {
			t.Errorf("expected PImprovement '—' for nil comp, got %q", sum.PImprovement)
		}
	})

	t.Run("ComparisonOnlyWithoutRuns", func(t *testing.T) {
		pImp := 0.26
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-comp",
				Namespace: "default",
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
			},
			Status: v1alpha1.ComparisonStatus{
				PImprovement: &pImp,
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:    "task-1",
						Pairs: 10,
						Dropped: v1alpha1.ComparisonDropped{
							Candidate: 1,
							Baseline:  2,
						},
					},
				},
			},
		}

		sum := ComparisonSummary(comp, nil, nil)
		expectedHeadline := "cand-run vs base-run · P(improvement) = 0.26"
		if sum.Headline != expectedHeadline {
			t.Errorf("expected headline %q, got %q", expectedHeadline, sum.Headline)
		}
		if sum.PImprovement != "0.26" {
			t.Errorf("expected PImprovement '0.26', got %q", sum.PImprovement)
		}
		if sum.BaseRun != "base-run" || sum.CandRun != "cand-run" {
			t.Errorf("expected runs base-run/cand-run, got %s/%s", sum.BaseRun, sum.CandRun)
		}
		if sum.BaseCommitShort != "" || sum.CandCommitShort != "" {
			t.Errorf("expected empty commit shorts, got base=%q cand=%q", sum.BaseCommitShort, sum.CandCommitShort)
		}
		if sum.TotalPairs != 10 {
			t.Errorf("expected TotalPairs 10, got %d", sum.TotalPairs)
		}
		if sum.TotalDroppedCand != 1 || sum.TotalDroppedBase != 2 {
			t.Errorf("expected dropped 1/2, got %d/%d", sum.TotalDroppedCand, sum.TotalDroppedBase)
		}
		// totalSamples = (10+1) + (10+2) = 23
		if sum.TotalSamples != 23 {
			t.Errorf("expected TotalSamples 23, got %d", sum.TotalSamples)
		}
		expectedSamples := "23 run · 10 paired · dropped: 1 cand, 2 base"
		if sum.SamplesSummary != expectedSamples {
			t.Errorf("expected SamplesSummary %q, got %q", expectedSamples, sum.SamplesSummary)
		}
		if sum.BaseSummary != "base-run" {
			t.Errorf("expected BaseSummary 'base-run', got %q", sum.BaseSummary)
		}
		if sum.CandSummary != "cand-run" {
			t.Errorf("expected CandSummary 'cand-run', got %q", sum.CandSummary)
		}
	})

	t.Run("WithRunsAndCommits", func(t *testing.T) {
		pImp := 0.98
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-comp-runs",
				Namespace: "default",
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "run-base"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "run-cand"},
			},
			Status: v1alpha1.ComparisonStatus{
				PImprovement: &pImp,
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:    "task-1",
						Pairs: 48,
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "run-base",
				Namespace: "default",
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					Commit: "a81d0c4111111111111111111111111111111111",
				},
			},
			Status: v1alpha1.RunStatus{
				Summary: &v1alpha1.RunSummary{
					TotalSamples: 48,
				},
			},
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "run-cand",
				Namespace: "default",
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					Commit: "05ce4d8222222222222222222222222222222222",
				},
			},
			Status: v1alpha1.RunStatus{
				Summary: &v1alpha1.RunSummary{
					TotalSamples: 48,
				},
			},
		}

		sum := ComparisonSummary(comp, baseRun, candRun)
		expectedHeadline := "05ce4d8 vs a81d0c4 · P(improvement) = 0.98"
		if sum.Headline != expectedHeadline {
			t.Errorf("expected headline %q, got %q", expectedHeadline, sum.Headline)
		}
		if sum.BaseCommitShort != "a81d0c4" {
			t.Errorf("expected BaseCommitShort 'a81d0c4', got %q", sum.BaseCommitShort)
		}
		if sum.CandCommitShort != "05ce4d8" {
			t.Errorf("expected CandCommitShort '05ce4d8', got %q", sum.CandCommitShort)
		}
		if sum.TotalSamples != 96 {
			t.Errorf("expected TotalSamples 96, got %d", sum.TotalSamples)
		}
		if sum.TotalPairs != 48 {
			t.Errorf("expected TotalPairs 48, got %d", sum.TotalPairs)
		}
		if sum.SamplesSummary != "96 run · 48 paired" {
			t.Errorf("expected SamplesSummary '96 run · 48 paired', got %q", sum.SamplesSummary)
		}
		expectedBaseSummary := "commit a81d0c4 (run-base)"
		if sum.BaseSummary != expectedBaseSummary {
			t.Errorf("expected BaseSummary %q, got %q", expectedBaseSummary, sum.BaseSummary)
		}
		expectedCandSummary := "commit 05ce4d8 (run-cand)"
		if sum.CandSummary != expectedCandSummary {
			t.Errorf("expected CandSummary %q, got %q", expectedCandSummary, sum.CandSummary)
		}

		// Also verify reversed order in args is resolved correctly by name
		sumReversed := ComparisonSummary(comp, candRun, baseRun)
		if sumReversed.Headline != expectedHeadline {
			t.Errorf("expected headline %q with reversed runs, got %q", expectedHeadline, sumReversed.Headline)
		}
	})

	t.Run("NilPImprovement", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "run-b"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "run-c"},
			},
		}
		sum := ComparisonSummary(comp, nil, nil)
		if sum.PImprovement != "—" {
			t.Errorf("expected PImprovement '—', got %q", sum.PImprovement)
		}
		if sum.Headline != "run-c vs run-b" {
			t.Errorf("expected headline 'run-c vs run-b', got %q", sum.Headline)
		}
	})

	t.Run("PositionalFallbackWhenRunsHaveNoName", func(t *testing.T) {
		comp := &v1alpha1.Comparison{}
		r1 := &v1alpha1.Run{
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{Commit: "111111111111"},
			},
		}
		r2 := &v1alpha1.Run{
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{Commit: "222222222222"},
			},
		}
		sum := ComparisonSummary(comp, r1, r2)
		if sum.BaseCommitShort != "1111111" {
			t.Errorf("expected BaseCommitShort '1111111', got %q", sum.BaseCommitShort)
		}
		if sum.CandCommitShort != "2222222" {
			t.Errorf("expected CandCommitShort '2222222', got %q", sum.CandCommitShort)
		}
		if sum.Headline != "2222222 vs 1111111" {
			t.Errorf("expected headline '2222222 vs 1111111', got %q", sum.Headline)
		}
	})

	t.Run("CostAndPossibleSamples", func(t *testing.T) {
		cost := 4.10
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				CostUSD:         &cost,
				PossibleSamples: 3200,
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:         "task-1",
						Pairs:      48,
						SamplesRun: 48,
					},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			Status: v1alpha1.RunStatus{
				Summary: &v1alpha1.RunSummary{TotalSamples: 48},
			},
		}
		candRun := &v1alpha1.Run{
			Status: v1alpha1.RunStatus{
				Summary: &v1alpha1.RunSummary{TotalSamples: 48},
			},
		}
		sum := ComparisonSummary(comp, baseRun, candRun)
		expected := "$4.10 spent, 48 of 3200 possible samples"
		if sum.SamplesSummary != expected {
			t.Errorf("expected SamplesSummary %q, got %q", expected, sum.SamplesSummary)
		}
	})

	t.Run("CostOnlyWithDropped", func(t *testing.T) {
		cost := 0.05
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				CostUSD: &cost,
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:    "task-1",
						Pairs: 10,
						Dropped: v1alpha1.ComparisonDropped{
							Candidate: 1,
							Baseline:  2,
						},
					},
				},
			},
		}
		sum := ComparisonSummary(comp, nil, nil)
		expected := "$0.05 spent, 23 run · 10 paired · dropped: 1 cand, 2 base"
		if sum.SamplesSummary != expected {
			t.Errorf("expected SamplesSummary %q, got %q", expected, sum.SamplesSummary)
		}
	})

	t.Run("CostWithDefaultPricesProvenance", func(t *testing.T) {
		cost := 0.01
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				CostUSD:         &cost,
				PossibleSamples: 100,
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:         "task-1",
						Pairs:      16,
						SamplesRun: 32,
						CostSource: v1alpha1.CostSourceDefault,
					},
				},
			},
		}
		sum := ComparisonSummary(comp, nil, nil)
		expected := "$0.01 spent (default prices), 32 of 100 possible samples"
		if sum.SamplesSummary != expected {
			t.Errorf("expected SamplesSummary %q, got %q", expected, sum.SamplesSummary)
		}
	})

	t.Run("CostWithConfigMapPricesProvenance", func(t *testing.T) {
		cost := 0.01
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				CostUSD:         &cost,
				PossibleSamples: 100,
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:         "task-1",
						Pairs:      16,
						SamplesRun: 32,
						CostSource: v1alpha1.CostSourceConfigMap,
					},
				},
			},
		}
		sum := ComparisonSummary(comp, nil, nil)
		expected := "$0.01 spent, 32 of 100 possible samples"
		if sum.SamplesSummary != expected {
			t.Errorf("expected SamplesSummary %q, got %q", expected, sum.SamplesSummary)
		}
	})

	t.Run("PossibleSamplesOnly", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				PossibleSamples: 100,
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:         "task-1",
						Pairs:      10,
						SamplesRun: 10,
					},
				},
			},
		}
		sum := ComparisonSummary(comp, nil, nil)
		expected := "10 of 100 possible samples"
		if sum.SamplesSummary != expected {
			t.Errorf("expected SamplesSummary %q, got %q", expected, sum.SamplesSummary)
		}
	})

	t.Run("StatusCostNilDoesNotResumTasks", func(t *testing.T) {
		taskCost := 0.50
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				CostUSD: nil, // Controller omitted total on purpose because another task had no price
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:      "task-1",
						Pairs:   10,
						CostUSD: &taskCost,
					},
					{
						ID:      "task-2",
						Pairs:   10,
						CostUSD: nil,
					},
				},
			},
		}
		sum := ComparisonSummary(comp, nil, nil)
		expected := "40 run · 20 paired"
		if sum.SamplesSummary != expected {
			t.Errorf("expected SamplesSummary %q without partial spent, got %q", expected, sum.SamplesSummary)
		}
		if sum.CostUSD != nil {
			t.Errorf("expected nil sum.CostUSD when status.CostUSD is nil, got %v", sum.CostUSD)
		}
	})

	t.Run("CostAndLatencyDeltas", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "delta-comp", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
			},
		}

		fCost := func(v float64) *float64 { return &v }

		// 1. Both cost and latency present: cost per sample -61%, p50 latency -38%
		candRunBoth := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Status: v1alpha1.RunStatus{
				Summary: &v1alpha1.RunSummary{TotalSamples: 96},
				CostUSD: fCost(1.152), // $0.012/sample
				Tasks: []v1alpha1.TaskStatus{
					{ID: "task-1", Latency: &v1alpha1.TaskLatency{P50: 6.2}},
				},
			},
		}
		baseRunBoth := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Status: v1alpha1.RunStatus{
				Summary: &v1alpha1.RunSummary{TotalSamples: 96},
				CostUSD: fCost(2.976), // $0.031/sample
				Tasks: []v1alpha1.TaskStatus{
					{ID: "task-1", Latency: &v1alpha1.TaskLatency{P50: 10.0}},
				},
			},
		}
		sumBoth := ComparisonSummary(comp, baseRunBoth, candRunBoth)
		if sumBoth.CostPerSampleDelta != "cost per sample −61%" {
			t.Errorf("expected CostPerSampleDelta 'cost per sample −61%%', got %q", sumBoth.CostPerSampleDelta)
		}
		if sumBoth.P50LatencyDelta != "p50 latency −38%" {
			t.Errorf("expected P50LatencyDelta 'p50 latency −38%%', got %q", sumBoth.P50LatencyDelta)
		}
		if sumBoth.FooterMetricsDelta != "cost per sample −61% · p50 latency −38%" {
			t.Errorf("expected FooterMetricsDelta 'cost per sample −61%% · p50 latency −38%%', got %q", sumBoth.FooterMetricsDelta)
		}

		// 2. Latency only: p50 latency -38% without separator
		candRunLatOnly := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Status: v1alpha1.RunStatus{
				Tasks: []v1alpha1.TaskStatus{
					{ID: "task-1", Latency: &v1alpha1.TaskLatency{P50: 6.2}},
				},
			},
		}
		baseRunLatOnly := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Status: v1alpha1.RunStatus{
				Tasks: []v1alpha1.TaskStatus{
					{ID: "task-1", Latency: &v1alpha1.TaskLatency{P50: 10.0}},
				},
			},
		}
		sumLatOnly := ComparisonSummary(comp, baseRunLatOnly, candRunLatOnly)
		if sumLatOnly.CostPerSampleDelta != "" {
			t.Errorf("expected empty CostPerSampleDelta for latency-only, got %q", sumLatOnly.CostPerSampleDelta)
		}
		if sumLatOnly.P50LatencyDelta != "p50 latency −38%" {
			t.Errorf("expected P50LatencyDelta 'p50 latency −38%%', got %q", sumLatOnly.P50LatencyDelta)
		}
		if sumLatOnly.FooterMetricsDelta != "p50 latency −38%" {
			t.Errorf("expected FooterMetricsDelta 'p50 latency −38%%' without separator, got %q", sumLatOnly.FooterMetricsDelta)
		}

		// 3. Cost on one side only: no cost segment
		candRunCostOneSide := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Status: v1alpha1.RunStatus{
				Summary: &v1alpha1.RunSummary{TotalSamples: 10},
				CostUSD: fCost(1.0),
			},
		}
		baseRunNoCost := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Status: v1alpha1.RunStatus{
				Summary: &v1alpha1.RunSummary{TotalSamples: 10},
			},
		}
		sumCostOneSide := ComparisonSummary(comp, baseRunNoCost, candRunCostOneSide)
		if sumCostOneSide.CostPerSampleDelta != "" {
			t.Errorf("expected empty CostPerSampleDelta when cost only on candidate, got %q", sumCostOneSide.CostPerSampleDelta)
		}

		// 4. Latency on one side only: no latency segment
		candRunLatOneSide := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Status: v1alpha1.RunStatus{
				Tasks: []v1alpha1.TaskStatus{
					{ID: "task-1", Latency: &v1alpha1.TaskLatency{P50: 5.0}},
				},
			},
		}
		baseRunNoLat := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Status: v1alpha1.RunStatus{
				Tasks: []v1alpha1.TaskStatus{
					{ID: "task-1"},
				},
			},
		}
		sumLatOneSide := ComparisonSummary(comp, baseRunNoLat, candRunLatOneSide)
		if sumLatOneSide.P50LatencyDelta != "" {
			t.Errorf("expected empty P50LatencyDelta when latency only on candidate, got %q", sumLatOneSide.P50LatencyDelta)
		}

		// 5. baseCost == 0: no cost segment
		candRunBaseCostZero := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Status: v1alpha1.RunStatus{
				Summary: &v1alpha1.RunSummary{TotalSamples: 10},
				CostUSD: fCost(1.0),
			},
		}
		baseRunCostZero := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Status: v1alpha1.RunStatus{
				Summary: &v1alpha1.RunSummary{TotalSamples: 10},
				CostUSD: fCost(0.0),
			},
		}
		sumBaseCostZero := ComparisonSummary(comp, baseRunCostZero, candRunBaseCostZero)
		if sumBaseCostZero.CostPerSampleDelta != "" {
			t.Errorf("expected empty CostPerSampleDelta when baseCost is 0, got %q", sumBaseCostZero.CostPerSampleDelta)
		}

		// 6. Zero samples: no cost segment
		candRunZeroSamples := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Status: v1alpha1.RunStatus{
				Summary: &v1alpha1.RunSummary{TotalSamples: 0},
				CostUSD: fCost(1.0),
			},
		}
		baseRunZeroSamples := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Status: v1alpha1.RunStatus{
				Summary: &v1alpha1.RunSummary{TotalSamples: 0},
				CostUSD: fCost(2.0),
			},
		}
		sumZeroSamples := ComparisonSummary(comp, baseRunZeroSamples, candRunZeroSamples)
		if sumZeroSamples.CostPerSampleDelta != "" {
			t.Errorf("expected empty CostPerSampleDelta when samples is 0, got %q", sumZeroSamples.CostPerSampleDelta)
		}
	})
}

func TestTemplateRendering_SampleRange(t *testing.T) {
	t.Run("RunsList_SampleRange", func(t *testing.T) {
		view := RunsListView{
			ActiveTab: NavTabRuns,
			Namespace: "eval-runner",
			Runs: []RunListItem{
				{
					Name:        "test-range-run",
					Namespace:   "eval-runner",
					Phase:       v1alpha1.RunPhaseCompleted,
					PhaseChip:   "good",
					Tasks:       "1/1",
					Samples:     8,
					SampleRange: "samples 9-16",
					Rates:       "task=0.80",
					Commit:      "abc1234",
					Age:         "5m",
					Link:        "/runs/eval-runner/test-range-run",
				},
			},
		}

		var buf bytes.Buffer
		if err := tmplRunsList.Execute(&buf, view); err != nil {
			t.Fatalf("Template execution failed: %v", err)
		}
		output := buf.String()
		if !strings.Contains(output, `<td class="num">samples 9-16</td>`) {
			t.Errorf("Expected output to contain '<td class=\"num\">samples 9-16</td>', got:\n%s", output)
		}
	})

	t.Run("RunDetail_SampleRange", func(t *testing.T) {
		view := RunDetailView{
			ActiveTab:    NavTabRuns,
			Name:         "test-range-run",
			Namespace:    "eval-runner",
			Phase:        v1alpha1.RunPhaseCompleted,
			PhaseChip:    "good",
			TotalSamples: 8,
			SampleRange:  "samples 9-16",
			Tasks: []RunTaskRow{
				{
					ID:          "task-1",
					Phase:       v1alpha1.TaskPhaseCompleted,
					PhaseChip:   "good",
					Samples:     8,
					Successes:   7,
					SampleRange: "samples 9-16",
				},
			},
		}

		var buf bytes.Buffer
		if err := tmplRunDetail.Execute(&buf, view); err != nil {
			t.Fatalf("Template execution failed: %v", err)
		}
		output := buf.String()
		if !strings.Contains(output, "8 <span style=\"font-size:13px;font-weight:normal;color:var(--muted)\">(samples 9-16)</span>") {
			t.Errorf("Expected output to contain '8 (samples 9-16)' in tile, got:\n%s", output)
		}
		if !strings.Contains(output, "7/8 <span style=\"font-size:11px;color:var(--muted)\">(samples 9-16)</span>") {
			t.Errorf("Expected output to contain '7/8 (samples 9-16)' in table, got:\n%s", output)
		}
	})
}

func TestComparisonTable_Tokens(t *testing.T) {
	tasks := []ComparisonTaskRow{
		{
			ID:          "task-with-tokens",
			BaseRate:    "0.60",
			CandRate:    "0.80",
			Delta:       "+0.20",
			StatusClass: "good",
			Label:       "improved",
			Pairs:       16,
			SamplesRun:  16,
			Tokens: &v1alpha1.TaskTokens{
				Input:  500,
				Output: 200,
				Total:  700,
			},
		},
		{
			ID:          "task-without-tokens",
			BaseRate:    "0.50",
			CandRate:    "0.50",
			Delta:       "+0.00",
			StatusClass: "neutral",
			Label:       "unchanged",
			Pairs:       16,
			SamplesRun:  16,
			Tokens:      nil,
		},
		{
			ID:          "task-with-reasoning",
			BaseRate:    "0.70",
			CandRate:    "0.90",
			Delta:       "+0.20",
			StatusClass: "good",
			Label:       "improved",
			Pairs:       16,
			SamplesRun:  16,
			Tokens: &v1alpha1.TaskTokens{
				Input:     500,
				Output:    200,
				Total:     700,
				Reasoning: 80,
			},
		},
	}

	var buf bytes.Buffer
	if err := RenderComparisonTable(&buf, ComparisonTableView{Tasks: tasks}); err != nil {
		t.Fatalf("failed to render comparison table: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "<th>tokens</th>") {
		t.Errorf("expected <th>tokens</th> header, got:\n%s", output)
	}
	if !strings.Contains(output, "<th>samples</th>") {
		t.Errorf("expected <th>samples</th> header, got:\n%s", output)
	}
	if !strings.Contains(output, "<td class=\"num\">700</td>") {
		t.Errorf("expected total tokens 700 for task-with-tokens, got:\n%s", output)
	}
	if !strings.Contains(output, "<td class=\"num\">—</td>") {
		t.Errorf("expected '—' tokens for task-without-tokens, got:\n%s", output)
	}
	if !strings.Contains(output, "<td class=\"num\">700 (+R 80)</td>") {
		t.Errorf("expected total tokens '700 (+R 80)' for task-with-reasoning, got:\n%s", output)
	}
}

func TestComparisonDetail_BatchesAndActions(t *testing.T) {
	view := ComparisonDetailView{
		ActiveTab:   NavTabComparisons,
		Name:        "comp-batches-test",
		Namespace:   "eval-runner",
		Verdict:     v1alpha1.ComparisonVerdictRunning,
		VerdictChip: "pending",
		Headline:    "Running batch 2 (1 tasks continuing)",
		Tasks: []ComparisonTaskRow{
			{
				ID:          "task-decided",
				BaseRate:    "0.20",
				CandRate:    "0.90",
				Delta:       "+0.70",
				StatusClass: "good",
				Label:       "improved",
				Pairs:       8,
				SamplesRun:  8,
			},
			{
				ID:            "task-continuing",
				BaseRate:      "0.50",
				CandRate:      "0.50",
				Delta:         "+0.00",
				StatusClass:   "pending",
				Label:         "running",
				Pairs:         8,
				SamplesRun:    8,
				SuggestedNext: 16,
			},
		},
		Batches: []ComparisonBatchRow{
			{
				Index:        1,
				Phase:        v1alpha1.ComparisonPhaseCompleted,
				PhaseChip:    "good",
				SampleRange:  "1-8",
				CandidateRun: "cand-b1-run",
				BaselineRun:  "base-b1-run",
			},
			{
				Index:        2,
				Phase:        v1alpha1.ComparisonPhaseRunning,
				PhaseChip:    "pending",
				SampleRange:  "9-16",
				CandidateRun: "cand-b2-run",
				BaselineRun:  "base-b2-run",
			},
		},
		Actions: []ComparisonActionRow{
			{TaskID: "task-continuing", SuggestedNext: 16, ShowTaskID: true},
		},
	}

	var buf bytes.Buffer
	if err := tmplComparisonDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute comparison detail template: %v", err)
	}
	output := buf.String()

	// Assert Batches section
	if !strings.Contains(output, "Batches</h2>") {
		t.Errorf("expected Batches heading, got:\n%s", output)
	}
	if !strings.Contains(output, "batch 1") || !strings.Contains(output, "batch 2") {
		t.Errorf("expected batch 1 and batch 2 in table, got:\n%s", output)
	}
	if !strings.Contains(output, "/runs/eval-runner/cand-b1-run") || !strings.Contains(output, "/runs/eval-runner/base-b1-run") {
		t.Errorf("expected batch run links, got:\n%s", output)
	}
	if !strings.Contains(output, "<span><b>Batches:</b> 2</span>") {
		t.Errorf("expected Batches footer count, got:\n%s", output)
	}

	// Assert Actions (extraSamples hint in real <ul>)
	if !strings.Contains(output, "<li>run 16 more samples (task-continuing)</li>") {
		t.Errorf("expected <li> action hint in detail view, got:\n%s", output)
	}
}

func TestComparisonActions(t *testing.T) {
	t.Run("NilComparison", func(t *testing.T) {
		actions := ComparisonActions(nil)
		if actions != nil {
			t.Errorf("expected nil actions for nil comp, got %v", actions)
		}
	})

	t.Run("NoInconclusiveTasks", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{ID: "task-1", Label: v1alpha1.ComparisonTaskLabelImproved, SuggestedNext: 16},
					{ID: "task-2", Label: v1alpha1.ComparisonTaskLabelUnchanged},
				},
			},
		}
		actions := ComparisonActions(comp)
		if len(actions) != 0 {
			t.Errorf("expected 0 actions, got %d", len(actions))
		}
	})

	t.Run("SingleTaskInconclusive", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{ID: "only-task", Label: v1alpha1.ComparisonTaskLabelInconclusive, SuggestedNext: 32},
				},
			},
		}
		actions := ComparisonActions(comp)
		if len(actions) != 1 {
			t.Fatalf("expected 1 action, got %d", len(actions))
		}
		if actions[0].ShowTaskID {
			t.Errorf("expected ShowTaskID to be false for single-task comparison")
		}
		if actions[0].SuggestedNext != 32 || actions[0].TaskID != "only-task" {
			t.Errorf("unexpected action: %+v", actions[0])
		}
	})

	t.Run("MultipleTasksInconclusive", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			Status: v1alpha1.ComparisonStatus{
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{ID: "task-1", Label: v1alpha1.ComparisonTaskLabelImproved},
					{ID: "task-2", Label: v1alpha1.ComparisonTaskLabelInconclusive, SuggestedNext: 16},
					{ID: "task-3", Label: v1alpha1.ComparisonTaskLabelInconclusive, SuggestedNext: 0},
				},
			},
		}
		actions := ComparisonActions(comp)
		if len(actions) != 1 {
			t.Fatalf("expected 1 action, got %d", len(actions))
		}
		if !actions[0].ShowTaskID {
			t.Errorf("expected ShowTaskID to be true for multi-task comparison")
		}
		if actions[0].SuggestedNext != 16 || actions[0].TaskID != "task-2" {
			t.Errorf("unexpected action: %+v", actions[0])
		}
	})
}

func sampleSeriesListView() SeriesListView {
	lastMove := stats.DeltaEstimate{
		Delta:      0.10,
		CI:         stats.Interval{Lower: -0.10, Upper: 0.27},
		Confidence: 0.95,
		Method:     "newcombe",
	}
	return SeriesListView{
		ActiveTab: NavTabSeries,
		Namespace: "eval-runner",
		Series: []SeriesListRow{
			{
				Repository:     "gke-labs/eval-runner",
				RepoHash:       "2cc24c2",
				Branch:         "main",
				RunCount:       42,
				Link:           "/series/2cc24c2/main",
				LastMove:       &lastMove,
				LastMoveStr:    FormatDeltaCI(lastMove.Delta, lastMove.CI.Lower, lastMove.CI.Upper),
				LastMoveChip:   DeltaChipClass(lastMove.CI),
				LastMoveMethod: fmt.Sprintf("%s · %s", "triage/runbook_lookup", FormatMethodTooltip(lastMove.Method)),
			},
		},
		TotalCount: 1,
	}
}

func sampleSeriesDetailView() SeriesDetailView {
	rate1 := 0.75
	rate2 := 0.85
	cost1 := 0.05
	cost2 := 0.06
	t1, _ := time.Parse(time.RFC3339, "2026-10-20T01:00:00Z")
	t2, _ := time.Parse(time.RFC3339, "2026-10-20T02:00:00Z")

	items := []SeriesItem{
		{
			CommitSHA:      "1111111222222233333334444444555555556666",
			CommitSHAShort: "1111111",
			CommitTime:     t1,
			CommitTimeStr:  "2026-10-20T01:00:00Z",
			RunName:        "base-1111111",
			Namespace:      "eval-runner",
			Phase:          v1alpha1.TaskPhaseFailed,
			PhaseChip:      "bad",
			PhaseLabel:     "failed after 10 samples",
			Samples:        "10",
			SamplesCount:   10,
			RateStr:        "0.75",
			CIStr:          "[0.60, 0.88]",
			DeltaPrevStr:   "—",
			TokensStr:      "1,200",
			CostStr:        "$0.05",
			RunLink:        "/runs/eval-runner/base-1111111",
			HasPoint:       true,
			SuccessRate:    &rate1,
			CI: &v1alpha1.ConfidenceInterval{
				Lower:      0.60,
				Upper:      0.88,
				Confidence: 0.95,
				Method:     "wilson",
			},
		},
		{
			CommitSHA:      "2222222333333344444445555555666666667777",
			CommitSHAShort: "2222222",
			CommitTime:     t2,
			CommitTimeStr:  "2026-10-20T02:00:00Z",
			RunName:        "base-2222222",
			Namespace:      "eval-runner",
			Phase:          v1alpha1.TaskPhaseCompleted,
			PhaseChip:      "good",
			Samples:        "10",
			SamplesCount:   10,
			RateStr:        "0.85",
			CIStr:          "[0.70, 0.94]",
			TokensStr:      "1,300",
			CostStr:        "$0.06",
			RunLink:        "/runs/eval-runner/base-2222222",
			HasPoint:       true,
			SuccessRate:    &rate2,
			CI: &v1alpha1.ConfidenceInterval{
				Lower:      0.70,
				Upper:      0.94,
				Confidence: 0.95,
				Method:     "wilson",
			},
		},
	}
	_ = cost1
	_ = cost2

	est0 := items[0].Estimate()
	est1 := items[1].Estimate()
	delta := stats.DifferenceUnpaired(est1, est0, 0.95)
	items[1].DeltaPrev = &delta
	items[1].DeltaPrevMethod = FormatMethodTooltip(delta.Method)
	items[1].DeltaPrevChip = DeltaChipClass(delta.CI)
	items[1].DeltaPrevStr = FormatDeltaCI(delta.Delta, delta.CI.Lower, delta.CI.Upper)

	svg := BuildSeriesSVG(items)

	return SeriesDetailView{
		ActiveTab:    NavTabSeries,
		Namespace:    "eval-runner",
		Repository:   "gke-labs/eval-runner",
		RepoHash:     "2cc24c2",
		Branch:       "main",
		SelectedTask: "triage/runbook_lookup",
		Tasks: []SeriesTaskLink{
			{
				ID:     "triage/runbook_lookup",
				Link:   "/series/2cc24c2/main?task=triage%2Frunbook_lookup",
				Active: true,
			},
		},
		N:         50,
		SVG:       svg,
		Items:     items,
		TotalRuns: 2,
		BackLink:  "/series",
	}
}

func TestTemplateRenderingSeriesListGolden(t *testing.T) {
	view := sampleSeriesListView()

	var buf bytes.Buffer
	if err := tmplSeriesList.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute series list template: %v", err)
	}

	assertGolden(t, "series_list.golden.html", buf.String())
}

func TestTemplateRenderingSeriesDetailGolden(t *testing.T) {
	view := sampleSeriesDetailView()

	var buf bytes.Buffer
	if err := tmplSeriesDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute series detail template: %v", err)
	}

	assertGolden(t, "series_detail.golden.html", buf.String())
}

func TestTemplateRenderingRepoSeriesGolden(t *testing.T) {
	ref := RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"}
	view := sampleSeriesDetailView()
	view.Repo = ref
	view.BackLink = "/github.com/gke-labs/eval-runner/series"
	for i := range view.Tasks {
		view.Tasks[i].Link = "/github.com/gke-labs/eval-runner/series/main?task=" + url.QueryEscape(view.Tasks[i].ID)
	}
	for i := range view.Items {
		view.Items[i].RunLink = "/github.com/gke-labs/eval-runner/runs/" + view.Items[i].RunName
	}

	var buf bytes.Buffer
	if err := tmplSeriesDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute repo series template: %v", err)
	}

	assertGolden(t, "repo_series.golden.html", buf.String())
}

func TestTemplateRenderingSeriesDetail_RepoScopedVsLegacy(t *testing.T) {
	t.Run("RepoScoped", func(t *testing.T) {
		ref := RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"}
		view := sampleSeriesDetailView()
		view.Repo = ref
		var buf bytes.Buffer
		if err := tmplSeriesDetail.Execute(&buf, view); err != nil {
			t.Fatalf("failed to execute series detail template: %v", err)
		}
		body := buf.String()
		if strings.Contains(body, "<span>gke-labs/eval-runner</span>") {
			t.Errorf("expected repo-scoped series detail card title not to contain repository name")
		}
		if strings.Contains(body, "<span><b>Repository:</b>") {
			t.Errorf("expected repo-scoped series detail footer not to contain Repository")
		}
	})

	t.Run("LegacyGlobal", func(t *testing.T) {
		view := sampleSeriesDetailView()
		var buf bytes.Buffer
		if err := tmplSeriesDetail.Execute(&buf, view); err != nil {
			t.Fatalf("failed to execute series detail template: %v", err)
		}
		body := buf.String()
		if !strings.Contains(body, "<span>gke-labs/eval-runner</span>") {
			t.Errorf("expected legacy global series detail card title to contain repository name")
		}
		if !strings.Contains(body, "<span><b>Repository:</b> gke-labs/eval-runner</span>") {
			t.Errorf("expected legacy global series detail footer to contain Repository")
		}
	})
}

func sampleRepoSuiteDetailView() RepoSuiteDetailView {
	ref := RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"}
	t1 := time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 10, 20, 2, 0, 0, 0, time.UTC)

	commits := []CommitEntry{
		{SHA: "1111111111111111111111111111111111111111", CommitTime: t1},
		{SHA: "2222222222222222222222222222222222222222", CommitTime: t2},
	}

	obs := []EvalObservation{
		{
			RunName:     "base-1111111",
			TaskID:      "triage/runbook_lookup",
			CommitSHA:   commits[0].SHA,
			CommitTime:  t1,
			Model:       "gemini-2.5-pro",
			Samples:     20,
			Successes:   14,
			ScoreStr:    "0.70",
			CIStr:       "[0.48, 0.86]",
			Est:         stats.PooledWilsonFromTotals(14, 20, 0.95),
			SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-1111111/tasks/triage%2Frunbook_lookup/samples",
		},
		{
			RunName:     "base-2222222",
			TaskID:      "triage/runbook_lookup",
			CommitSHA:   commits[1].SHA,
			CommitTime:  t2,
			Model:       "gemini-2.5-pro",
			Samples:     20,
			Successes:   17,
			ScoreStr:    "0.85",
			CIStr:       "[0.64, 0.95]",
			Est:         stats.PooledWilsonFromTotals(17, 20, 0.95),
			SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-2222222/tasks/triage%2Frunbook_lookup/samples",
		},
	}

	lines := LinesPerDimension(obs, DimensionModel, commits)
	chartSVG := BuildRepoSuiteChartSVG(lines, obs, commits)

	evals := []RepoSuiteEvalSection{
		{
			ID:                       "triage/runbook_lookup",
			ScoreStr:                 "0.85",
			CIStr:                    "[0.64, 0.95]",
			HasDelta:                 true,
			DeltaStr:                 "Δ +0.15 [-0.11, +0.41]",
			DeltaClass:               "neutral",
			HasModelBreakdown:        false,
			SVG:                      chartSVG,
			FirstCommitShort:         "1111111",
			LastCommitShort:          "2222222",
			NewestChangedCommitShort: "2222222",
			Observations: []RepoSuiteObservationRow{
				{
					CommitSHA:   commits[1].SHA,
					CommitShort: "2222222",
					TimeStr:     "10m",
					TimeUTC:     t2.Format(time.RFC3339),
					Model:       "gemini-2.5-pro",
					Samples:     20,
					ScoreStr:    "0.85",
					CIStr:       "[0.64, 0.95]",
					SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-2222222/tasks/triage%2Frunbook_lookup/samples",
				},
				{
					CommitSHA:   commits[0].SHA,
					CommitShort: "1111111",
					TimeStr:     "1h",
					TimeUTC:     t1.Format(time.RFC3339),
					Model:       "gemini-2.5-pro",
					Samples:     20,
					ScoreStr:    "0.70",
					CIStr:       "[0.48, 0.86]",
					SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-1111111/tasks/triage%2Frunbook_lookup/samples",
				},
			},
		},
	}

	return RepoSuiteDetailView{
		ActiveTab:          NavTabEvals,
		Repo:               ref,
		Namespace:          "eval-runner",
		SuiteName:          "triage",
		SuiteScore:         "0.85",
		SuiteCI:            "[0.64, 0.95]",
		EvalsCount:         1,
		HasDelta:           true,
		DeltaStr:           "Δ +0.15 [-0.11, +0.41]",
		DeltaClass:         "neutral",
		CurrentCommitSHA:   commits[1].SHA,
		CurrentCommitShort: "2222222",
		Evals:              evals,
		OverviewLink:       "/github.com/gke-labs/eval-runner",
	}
}

func sampleRepoSuiteDetailModelsView() RepoSuiteDetailView {
	ref := RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"}
	t1 := time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 10, 20, 2, 0, 0, 0, time.UTC)

	commits := []CommitEntry{
		{SHA: "1111111111111111111111111111111111111111", CommitTime: t1},
		{SHA: "2222222222222222222222222222222222222222", CommitTime: t2},
	}

	obs := []EvalObservation{
		{
			RunName:     "base-flash-1",
			TaskID:      "triage/runbook_lookup",
			CommitSHA:   commits[0].SHA,
			CommitTime:  t1,
			Model:       "gemini-2.5-flash",
			Samples:     20,
			Successes:   10,
			ScoreStr:    "0.50",
			CIStr:       "[0.30, 0.70]",
			Est:         stats.PooledWilsonFromTotals(10, 20, 0.95),
			SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-flash-1/tasks/triage%2Frunbook_lookup/samples",
		},
		{
			RunName:     "base-pro-1",
			TaskID:      "triage/runbook_lookup",
			CommitSHA:   commits[0].SHA,
			CommitTime:  t1,
			Model:       "gemini-2.5-pro",
			Samples:     20,
			Successes:   14,
			ScoreStr:    "0.70",
			CIStr:       "[0.48, 0.86]",
			Est:         stats.PooledWilsonFromTotals(14, 20, 0.95),
			SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-pro-1/tasks/triage%2Frunbook_lookup/samples",
		},
		{
			RunName:     "base-flash-2",
			TaskID:      "triage/runbook_lookup",
			CommitSHA:   commits[1].SHA,
			CommitTime:  t2,
			Model:       "gemini-2.5-flash",
			Samples:     20,
			Successes:   12,
			ScoreStr:    "0.60",
			CIStr:       "[0.39, 0.78]",
			Est:         stats.PooledWilsonFromTotals(12, 20, 0.95),
			SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-flash-2/tasks/triage%2Frunbook_lookup/samples",
		},
		{
			RunName:     "base-pro-2",
			TaskID:      "triage/runbook_lookup",
			CommitSHA:   commits[1].SHA,
			CommitTime:  t2,
			Model:       "gemini-2.5-pro",
			Samples:     20,
			Successes:   17,
			ScoreStr:    "0.85",
			CIStr:       "[0.64, 0.95]",
			Est:         stats.PooledWilsonFromTotals(17, 20, 0.95),
			SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-pro-2/tasks/triage%2Frunbook_lookup/samples",
		},
	}

	lines := LinesPerDimension(obs, DimensionModel, commits)
	chartSVG := BuildRepoSuiteChartSVG(lines, obs, commits)

	var modelLegend []RepoSuiteModelLegendItem
	for _, l := range lines {
		modelLegend = append(modelLegend, RepoSuiteModelLegendItem{
			Name:     l.Name,
			Color:    l.Color,
			ScoreStr: l.ScoreStr,
			CIStr:    l.CIStr,
		})
	}

	evals := []RepoSuiteEvalSection{
		{
			ID:                       "triage/runbook_lookup",
			ScoreStr:                 "0.72",
			CIStr:                    "[0.57, 0.84]",
			HasDelta:                 true,
			DeltaStr:                 "Δ +0.12 [-0.09, +0.33]",
			DeltaClass:               "neutral",
			HasModelBreakdown:        true,
			ModelLegend:              modelLegend,
			SVG:                      chartSVG,
			FirstCommitShort:         "1111111",
			LastCommitShort:          "2222222",
			NewestChangedCommitShort: "2222222",
			Observations: []RepoSuiteObservationRow{
				{
					CommitSHA:   commits[1].SHA,
					CommitShort: "2222222",
					TimeStr:     "10m",
					TimeUTC:     t2.Format(time.RFC3339),
					Model:       "gemini-2.5-pro",
					Samples:     20,
					ScoreStr:    "0.85",
					CIStr:       "[0.64, 0.95]",
					SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-pro-2/tasks/triage%2Frunbook_lookup/samples",
				},
				{
					CommitSHA:   commits[1].SHA,
					CommitShort: "2222222",
					TimeStr:     "10m",
					TimeUTC:     t2.Format(time.RFC3339),
					Model:       "gemini-2.5-flash",
					Samples:     20,
					ScoreStr:    "0.60",
					CIStr:       "[0.39, 0.78]",
					SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-flash-2/tasks/triage%2Frunbook_lookup/samples",
				},
				{
					CommitSHA:   commits[0].SHA,
					CommitShort: "1111111",
					TimeStr:     "1h",
					TimeUTC:     t1.Format(time.RFC3339),
					Model:       "gemini-2.5-pro",
					Samples:     20,
					ScoreStr:    "0.70",
					CIStr:       "[0.48, 0.86]",
					SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-pro-1/tasks/triage%2Frunbook_lookup/samples",
				},
				{
					CommitSHA:   commits[0].SHA,
					CommitShort: "1111111",
					TimeStr:     "1h",
					TimeUTC:     t1.Format(time.RFC3339),
					Model:       "gemini-2.5-flash",
					Samples:     20,
					ScoreStr:    "0.50",
					CIStr:       "[0.30, 0.70]",
					SamplesLink: "/github.com/gke-labs/eval-runner/runs/base-flash-1/tasks/triage%2Frunbook_lookup/samples",
				},
			},
		},
	}

	return RepoSuiteDetailView{
		ActiveTab:          NavTabEvals,
		Repo:               ref,
		Namespace:          "eval-runner",
		SuiteName:          "triage",
		SuiteScore:         "0.72",
		SuiteCI:            "[0.57, 0.84]",
		EvalsCount:         1,
		HasDelta:           true,
		DeltaStr:           "Δ +0.12 [-0.09, +0.33]",
		DeltaClass:         "neutral",
		CurrentCommitSHA:   commits[1].SHA,
		CurrentCommitShort: "2222222",
		Evals:              evals,
		OverviewLink:       "/github.com/gke-labs/eval-runner",
	}
}

func TestTemplateRenderingRepoSuiteDetailGolden(t *testing.T) {
	view := sampleRepoSuiteDetailView()

	var buf bytes.Buffer
	if err := tmplRepoSuiteDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute repo suite detail template: %v", err)
	}

	assertGolden(t, "repo_suite_detail.golden.html", buf.String())
}

func TestTemplateRenderingRepoSuiteDetailModelsGolden(t *testing.T) {
	view := sampleRepoSuiteDetailModelsView()

	var buf bytes.Buffer
	if err := tmplRepoSuiteDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to execute repo suite detail models template: %v", err)
	}

	assertGolden(t, "repo_suite_detail_models.golden.html", buf.String())
}
func TestFormatLogURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "gs file",
			in:   "gs://my-bucket/runs/ee0a14f/task/inspect.json",
			want: "https://console.cloud.google.com/storage/browser/_details/my-bucket/runs/ee0a14f/task/inspect.json",
		},
		{
			name: "gs directory",
			in:   "gs://my-bucket/runs/ee0a14f/",
			want: "https://console.cloud.google.com/storage/browser/my-bucket/runs/ee0a14f/",
		},
		{
			name: "gs with special characters encoded",
			in:   "gs://my-bucket/runs/abc/task#frag?query%20space/inspect.json",
			want: "https://console.cloud.google.com/storage/browser/_details/my-bucket/runs/abc/task%23frag%3Fquery%2520space/inspect.json",
		},
		{
			name: "http url",
			in:   "http://localhost:8080/log.json",
			want: "http://localhost:8080/log.json",
		},
		{
			name: "https url",
			in:   "https://storage.googleapis.com/bucket/log.json",
			want: "https://storage.googleapis.com/bucket/log.json",
		},
		{
			name: "empty",
			in:   "",
			want: "",
		},
		{
			name: "local path",
			in:   "/var/log/eval/inspect.json",
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := FormatLogURL(tc.in)
			if got != tc.want {
				t.Errorf("FormatLogURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRunDetail_CostTooltip(t *testing.T) {
	view := sampleRunDetailView()
	view.Tasks = []RunTaskRow{
		{
			ID:          "task-default-cost",
			Phase:       v1alpha1.TaskPhaseCompleted,
			CostStr:     "$0.0270",
			CostSource:  v1alpha1.CostSourceDefault,
			CostTooltip: "default prices",
		},
		{
			ID:          "task-configmap-cost",
			Phase:       v1alpha1.TaskPhaseCompleted,
			CostStr:     "$0.1400",
			CostSource:  v1alpha1.CostSourceConfigMap,
			CostTooltip: "configmap prices",
		},
	}

	var buf bytes.Buffer
	if err := tmplRunDetail.Execute(&buf, view); err != nil {
		t.Fatalf("failed to render run detail: %v", err)
	}
	html := buf.String()
	if !strings.Contains(html, `<td class="num mono" title="default prices">$0.0270</td>`) {
		t.Errorf("expected default prices tooltip in run detail, got:\n%s", html)
	}
	if !strings.Contains(html, `<td class="num mono" title="configmap prices">$0.1400</td>`) {
		t.Errorf("expected configmap prices tooltip in run detail, got:\n%s", html)
	}
}

func TestComparisonTable_CostTooltip(t *testing.T) {
	tasks := []ComparisonTaskRow{
		{
			ID:              "task-cost-default",
			CostStr:         "$0.0540",
			CostTooltip:     "default prices",
			CandCostStr:     "$0.0270",
			CandCostTooltip: "default prices",
			BaseCostStr:     "$0.0270",
			BaseCostTooltip: "default prices",
		},
	}

	var buf bytes.Buffer
	if err := RenderComparisonTable(&buf, ComparisonTableView{Tasks: tasks, Metric: "cost", HasCost: true}); err != nil {
		t.Fatalf("failed to render comparison table: %v", err)
	}
	html := buf.String()
	if !strings.Contains(html, `title="default prices"`) {
		t.Errorf("expected default prices tooltip in comparison cost table, got:\n%s", html)
	}
}

func TestHeaderWordmarkMarkup(t *testing.T) {
	if !strings.Contains(baseCSS, ".wordmark-dot") {
		t.Errorf("baseCSS missing .wordmark-dot definition")
	}
	if !strings.Contains(baseCSS, "font-size: 0.8em") {
		t.Errorf("expected .wordmark-dot font-size to be 0.8em, got baseCSS:\n%s", baseCSS)
	}
	if !strings.Contains(baseCSS, "opacity: 0.7") {
		t.Errorf("expected .wordmark-dot opacity to be 0.7, got baseCSS:\n%s", baseCSS)
	}

	const expectedWordmark = `<span class="wordmark"><span class="wordmark-dot">.</span>evals</span>`

	repoWithHost := RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"}

	testCases := []struct {
		name   string
		render func(w io.Writer) error
	}{
		{
			name: "repo_index",
			render: func(w io.Writer) error {
				return RenderRepoIndex(w, RepoIndexView{})
			},
		},
		{
			name: "repo_overview",
			render: func(w io.Writer) error {
				return RenderRepoOverview(w, RepoOverviewView{Repo: repoWithHost})
			},
		},
		{
			name: "repo_suite_detail",
			render: func(w io.Writer) error {
				return RenderRepoSuiteDetail(w, RepoSuiteDetailView{Repo: repoWithHost})
			},
		},
		{
			name: "series_list",
			render: func(w io.Writer) error {
				return RenderSeriesList(w, SeriesListView{})
			},
		},
		{
			name: "series_detail_standalone",
			render: func(w io.Writer) error {
				return RenderSeriesDetail(w, SeriesDetailView{})
			},
		},
		{
			name: "series_detail_repo",
			render: func(w io.Writer) error {
				return RenderSeriesDetail(w, SeriesDetailView{Repo: repoWithHost})
			},
		},
		{
			name: "pulls_list_standalone",
			render: func(w io.Writer) error {
				return RenderPullsList(w, PullRequestsListView{})
			},
		},
		{
			name: "pulls_list_repo",
			render: func(w io.Writer) error {
				return RenderPullsList(w, PullRequestsListView{Repo: repoWithHost})
			},
		},
		{
			name: "pull_detail_standalone",
			render: func(w io.Writer) error {
				return RenderPullDetail(w, PullRequestDetailView{})
			},
		},
		{
			name: "pull_detail_repo",
			render: func(w io.Writer) error {
				return RenderPullDetail(w, PullRequestDetailView{Repo: repoWithHost})
			},
		},
		{
			name: "task_samples_list",
			render: func(w io.Writer) error {
				return RenderTaskSamplesList(w, TaskSamplesListView{Repo: repoWithHost})
			},
		},
		{
			name: "sample_transcript",
			render: func(w io.Writer) error {
				return RenderSampleTranscript(w, SampleTranscriptView{Repo: repoWithHost})
			},
		},
		{
			name: "runs_list_standalone",
			render: func(w io.Writer) error {
				return tmplRunsList.Execute(w, RunsListView{})
			},
		},
		{
			name: "runs_list_repo",
			render: func(w io.Writer) error {
				return tmplRunsList.Execute(w, RunsListView{Repo: repoWithHost})
			},
		},
		{
			name: "run_detail_standalone",
			render: func(w io.Writer) error {
				return tmplRunDetail.Execute(w, RunDetailView{})
			},
		},
		{
			name: "run_detail_repo",
			render: func(w io.Writer) error {
				return tmplRunDetail.Execute(w, RunDetailView{Repo: repoWithHost})
			},
		},
		{
			name: "comparisons_list_standalone",
			render: func(w io.Writer) error {
				return tmplComparisonsList.Execute(w, ComparisonsListView{})
			},
		},
		{
			name: "comparisons_list_repo",
			render: func(w io.Writer) error {
				return tmplComparisonsList.Execute(w, ComparisonsListView{Repo: repoWithHost})
			},
		},
		{
			name: "comparison_detail_standalone",
			render: func(w io.Writer) error {
				return tmplComparisonDetail.Execute(w, ComparisonDetailView{})
			},
		},
		{
			name: "comparison_detail_repo",
			render: func(w io.Writer) error {
				return tmplComparisonDetail.Execute(w, ComparisonDetailView{Repo: repoWithHost})
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := tc.render(&buf); err != nil {
				t.Fatalf("render failed: %v", err)
			}
			out := buf.String()
			count := strings.Count(out, expectedWordmark)
			if count != 1 {
				t.Errorf("expected wordmark markup exactly once, found %d times in %s", count, tc.name)
			}
			if !strings.Contains(out, `<header class="header-bar">`) {
				t.Errorf("expected header-bar element in %s", tc.name)
			}
			if !strings.Contains(out, `class="header-logo">`+expectedWordmark+`</a>`) {
				t.Errorf("expected header-logo to enclose wordmark in %s, got:\n%s", tc.name, out)
			}
			if strings.Contains(out, `<title>.evals`) {
				t.Errorf("title should keep eval-runner, not wordmark in %s", tc.name)
			}
		})
	}
}
