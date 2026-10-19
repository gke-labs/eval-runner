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

package stats

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestVerdictTableDriven(t *testing.T) {
	// Acceptance criterion:
	// Stopping rule and verdict tables covered by table-driven tests,
	// including the RUNNING and mixed inconclusive+improved cases.
	tests := []struct {
		name            string
		labels          []TaskLabel
		expectedVerdict VerdictType
	}{
		{
			name:            "empty labels",
			labels:          []TaskLabel{},
			expectedVerdict: VerdictInconclusive,
		},
		{
			name:            "single improved",
			labels:          []TaskLabel{TaskLabelImproved},
			expectedVerdict: VerdictImproved,
		},
		{
			name:            "single regressed",
			labels:          []TaskLabel{TaskLabelRegressed},
			expectedVerdict: VerdictRegressed,
		},
		{
			name:            "single unchanged",
			labels:          []TaskLabel{TaskLabelUnchanged},
			expectedVerdict: VerdictUnchanged,
		},
		{
			name:            "all unchanged",
			labels:          []TaskLabel{TaskLabelUnchanged, TaskLabelUnchanged, TaskLabelUnchanged},
			expectedVerdict: VerdictUnchanged,
		},
		{
			name:            "improved and unchanged",
			labels:          []TaskLabel{TaskLabelImproved, TaskLabelUnchanged, TaskLabelUnchanged},
			expectedVerdict: VerdictImproved,
		},
		{
			name:            "improved and regressed",
			labels:          []TaskLabel{TaskLabelImproved, TaskLabelRegressed},
			expectedVerdict: VerdictRegressed,
		},
		{
			name:            "inconclusive and regressed",
			labels:          []TaskLabel{TaskLabelInconclusive, TaskLabelRegressed},
			expectedVerdict: VerdictRegressed,
		},
		{
			name:            "mixed inconclusive and improved",
			labels:          []TaskLabel{TaskLabelImproved, TaskLabelInconclusive},
			expectedVerdict: VerdictInconclusive,
		},
		{
			name:            "mixed inconclusive and unchanged",
			labels:          []TaskLabel{TaskLabelUnchanged, TaskLabelInconclusive},
			expectedVerdict: VerdictInconclusive,
		},
		{
			name:            "all inconclusive",
			labels:          []TaskLabel{TaskLabelInconclusive, TaskLabelInconclusive},
			expectedVerdict: VerdictInconclusive,
		},
		{
			name:            "running single",
			labels:          []TaskLabel{TaskLabelRunning},
			expectedVerdict: VerdictRunning,
		},
		{
			name:            "running with improved",
			labels:          []TaskLabel{TaskLabelImproved, TaskLabelRunning},
			expectedVerdict: VerdictRunning,
		},
		{
			name:            "running with inconclusive",
			labels:          []TaskLabel{TaskLabelInconclusive, TaskLabelRunning},
			expectedVerdict: VerdictRunning,
		},
		{
			name:            "queued treated as running",
			labels:          []TaskLabel{TaskLabelQueued, TaskLabelUnchanged},
			expectedVerdict: VerdictRunning,
		},
		{
			name:            "continue treated as running",
			labels:          []TaskLabel{TaskLabelContinue, TaskLabelImproved},
			expectedVerdict: VerdictRunning,
		},
		{
			name:            "running takes precedence over regressed while sampling",
			labels:          []TaskLabel{TaskLabelRunning, TaskLabelRegressed},
			expectedVerdict: VerdictRunning,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Verdict(tc.labels)
			if got.Verdict != tc.expectedVerdict {
				t.Errorf("Verdict(%v) = %v, want %v", tc.labels, got.Verdict, tc.expectedVerdict)
			}
			if got.PImprovement != 0.0 {
				t.Errorf("Verdict(%v) returned hardcoded P %v, want 0.0 (AggregateVerdict must be used)", tc.labels, got.PImprovement)
			}
		})
	}
}

func TestAggregateVerdictWithProbabilities(t *testing.T) {
	// Concept worked example (PR comment mockup):
	// triage/runbook_lookup: improved (P = 0.99)
	// triage/escalation_policy: unchanged (P = 0.55)
	// triage/summary_quality: unchanged (P = 0.50)
	tasksImproved := []TaskReport{
		{
			ID:           "triage/runbook_lookup",
			Label:        TaskLabelImproved,
			PImprovement: 0.99,
		},
		{
			ID:           "triage/escalation_policy",
			Label:        TaskLabelUnchanged,
			PImprovement: 0.55,
		},
		{
			ID:           "triage/summary_quality",
			Label:        TaskLabelUnchanged,
			PImprovement: 0.50,
		},
	}

	vImproved := AggregateVerdict(tasksImproved)
	if vImproved.Verdict != VerdictImproved {
		t.Errorf("Verdict = %v, want IMPROVED", vImproved.Verdict)
	}
	// With triage/runbook_lookup P=0.99 (floor 0.999) and unchanged tasks having floor 0.975,
	// min(0.99, 0.975) evaluates to exactly 0.975.
	if math.Abs(vImproved.PImprovement-0.975) > 1e-6 {
		t.Errorf("Headline P(improvement) = %v, want 0.975", vImproved.PImprovement)
	}

	// Concept mixed case: improved + inconclusive
	tasksMixed := []TaskReport{
		{
			ID:           "triage/runbook_lookup",
			Label:        TaskLabelImproved,
			PImprovement: 0.98,
		},
		{
			ID:           "triage/escalation_policy",
			Label:        TaskLabelUnchanged,
			PImprovement: 0.52,
		},
		{
			ID:           "triage/summary_quality",
			Label:        TaskLabelInconclusive,
			PImprovement: 0.60,
		},
	}
	vMixed := AggregateVerdict(tasksMixed)
	if vMixed.Verdict != VerdictInconclusive {
		t.Errorf("Mixed improved+inconclusive verdict = %v, want INCONCLUSIVE", vMixed.Verdict)
	}

	// Case with regression: headline collapses
	tasksRegressed := []TaskReport{
		{
			ID:           "triage/runbook_lookup",
			Label:        TaskLabelImproved,
			PImprovement: 0.99,
		},
		{
			ID:           "triage/escalation_policy",
			Label:        TaskLabelRegressed,
			PImprovement: 0.04,
		},
	}
	vRegressed := AggregateVerdict(tasksRegressed)
	if vRegressed.Verdict != VerdictRegressed {
		t.Errorf("Verdict = %v, want REGRESSED", vRegressed.Verdict)
	}
	if math.Abs(vRegressed.PImprovement-0.04) > 1e-6 {
		t.Errorf("PImprovement with regression = %v, want 0.04", vRegressed.PImprovement)
	}
}

func TestAggregateVerdictUnchangedAndInconclusive(t *testing.T) {
	// Acceptance criterion: run-level P for {unchanged, inconclusive} is capped by min P(not_reg).
	// Example from GKE docs-eval-same-commit:
	// docs/readme_comprehension: 20 ties {0 x20} -> unchanged, P = 0.50 (pNotReg = 0.975)
	// docs/readme_comprehension_blind: 19 ties, 1 loss {-1, 0 x19} -> inconclusive, P ≈ 0.25 (Beta(1, 2), pNotReg = 0.25)
	// Headline P: min(mean, min_k P(not_reg_k)) = min(0.375, 0.25) = 0.25
	diffsComp := make([]float64, 20)
	diffsBlind := make([]float64, 20)
	diffsBlind[0] = -1.0

	estComp := CompareDifferences(diffsComp, 0.95, 42)
	estBlind := CompareDifferences(diffsBlind, 0.95, 43)

	tasksSameCommit := []TaskReport{
		{
			ID:           "docs/readme_comprehension",
			Label:        TaskLabelUnchanged,
			Estimate:     estComp,
			PImprovement: estComp.PImprovement,
		},
		{
			ID:           "docs/readme_comprehension_blind",
			Label:        TaskLabelInconclusive,
			Estimate:     estBlind,
			PImprovement: estBlind.PImprovement,
		},
	}
	vSameCommit := AggregateVerdict(tasksSameCommit)
	if vSameCommit.Verdict != VerdictInconclusive {
		t.Errorf("Verdict = %v, want INCONCLUSIVE", vSameCommit.Verdict)
	}
	// Expected headline P is arithmetic mean capped by min P(not_reg):
	// mean = (0.50 + ~0.25) / 2 = 0.375
	// min P(not_reg) = min(0.975, ~0.25) = ~0.25
	// P_run = min(0.375, ~0.25) = ~0.25
	expectedP := math.Min((estComp.PImprovement+estBlind.PImprovement)/2.0, estBlind.PImprovement)
	if math.Abs(vSameCommit.PImprovement-expectedP) > 1e-6 {
		t.Errorf("PImprovement for {unchanged, inconclusive} = %v, want %v",
			vSameCommit.PImprovement, expectedP)
	}
	if math.Abs(vSameCommit.PImprovement-0.25) > 0.02 {
		t.Errorf("PImprovement for same-commit = %v, want ~0.25", vSameCommit.PImprovement)
	}

	// Dilution guard: one weak inconclusive task cannot be diluted by many unchanged tasks.
	// Acceptance criterion: {unchanged 0.5 x3, inconclusive 0.05} -> 0.05
	tasksDilution := []TaskReport{
		{
			ID:           "task-1",
			Label:        TaskLabelUnchanged,
			PImprovement: 0.50,
		},
		{
			ID:           "task-2",
			Label:        TaskLabelUnchanged,
			PImprovement: 0.50,
		},
		{
			ID:           "task-3",
			Label:        TaskLabelUnchanged,
			PImprovement: 0.50,
		},
		{
			ID:           "task-4",
			Label:        TaskLabelInconclusive,
			PImprovement: 0.05,
		},
	}
	vDilution := AggregateVerdict(tasksDilution)
	if vDilution.Verdict != VerdictInconclusive {
		t.Errorf("Verdict = %v, want INCONCLUSIVE", vDilution.Verdict)
	}
	if math.Abs(vDilution.PImprovement-0.05) > 1e-6 {
		t.Errorf("PImprovement for {unchanged 0.5 x3, inconclusive 0.05} = %v, want 0.05", vDilution.PImprovement)
	}

	// Multiple unchanged and inconclusive tasks where mean <= min P(not_reg):
	tasksMultiple := []TaskReport{
		{
			ID:           "task-1",
			Label:        TaskLabelUnchanged,
			PImprovement: 0.50,
		},
		{
			ID:           "task-2",
			Label:        TaskLabelUnchanged,
			PImprovement: 0.50,
		},
		{
			ID:           "task-3",
			Label:        TaskLabelInconclusive,
			PImprovement: 0.53,
		},
	}
	vMultiple := AggregateVerdict(tasksMultiple)
	if vMultiple.Verdict != VerdictInconclusive {
		t.Errorf("Verdict = %v, want INCONCLUSIVE", vMultiple.Verdict)
	}
	// mean = (0.50 + 0.50 + 0.53) / 3 = 0.51
	// min P(not_reg) = min(0.975, 0.975, 0.53) = 0.53
	// min(mean, minPNotReg) = 0.51
	expectedMultipleP := math.Min((0.50+0.50+0.53)/3.0, 0.53)
	if math.Abs(vMultiple.PImprovement-expectedMultipleP) > 1e-6 {
		t.Errorf("PImprovement = %v, want %v", vMultiple.PImprovement, expectedMultipleP)
	}

	// All inconclusive tasks (no improved, no regressed):
	tasksAllInconclusive := []TaskReport{
		{
			ID:           "task-a",
			Label:        TaskLabelInconclusive,
			PImprovement: 0.46,
		},
		{
			ID:           "task-b",
			Label:        TaskLabelInconclusive,
			PImprovement: 0.54,
		},
	}
	vAllInconclusive := AggregateVerdict(tasksAllInconclusive)
	if vAllInconclusive.Verdict != VerdictInconclusive {
		t.Errorf("Verdict = %v, want INCONCLUSIVE", vAllInconclusive.Verdict)
	}
	// mean = 0.50, min P(not_reg) = 0.46 -> capped at 0.46
	expectedAllInconclusiveP := math.Min((0.46+0.54)/2.0, 0.46)
	if math.Abs(vAllInconclusive.PImprovement-expectedAllInconclusiveP) > 1e-6 {
		t.Errorf("PImprovement = %v, want %v", vAllInconclusive.PImprovement, expectedAllInconclusiveP)
	}

	// All inconclusive tasks at exact parity:
	tasksParityInconclusive := []TaskReport{
		{
			ID:           "task-p1",
			Label:        TaskLabelInconclusive,
			PImprovement: 0.50,
		},
		{
			ID:           "task-p2",
			Label:        TaskLabelInconclusive,
			PImprovement: 0.50,
		},
	}
	vParity := AggregateVerdict(tasksParityInconclusive)
	if vParity.Verdict != VerdictInconclusive {
		t.Errorf("Verdict = %v, want INCONCLUSIVE", vParity.Verdict)
	}
	if math.Abs(vParity.PImprovement-0.50) > 1e-6 {
		t.Errorf("PImprovement = %v, want 0.50", vParity.PImprovement)
	}
}

func TestCapForCandidateFailures(t *testing.T) {
	tasks25 := make([]string, 25)
	for i := 0; i < 25; i++ {
		tasks25[i] = fmt.Sprintf("task-%02d", i+1)
	}

	tests := []struct {
		name        string
		verdict     RunVerdict
		failed      []string
		wantVerdict VerdictType
		wantP       float64
		wantSummary string
		wantCapped  bool
	}{
		{
			name: "empty failed tasks is a no-op with nil",
			verdict: RunVerdict{
				Verdict:      VerdictImproved,
				PImprovement: 0.95,
				Summary:      "IMPROVED: 1 task(s) improved, 0 unchanged",
			},
			failed:      nil,
			wantVerdict: VerdictImproved,
			wantP:       0.95,
			wantSummary: "IMPROVED: 1 task(s) improved, 0 unchanged",
			wantCapped:  false,
		},
		{
			name: "empty failed tasks is a no-op with empty slice",
			verdict: RunVerdict{
				Verdict:      VerdictUnchanged,
				PImprovement: 0.50,
				Summary:      "UNCHANGED: all 1 task(s) unchanged",
			},
			failed:      []string{},
			wantVerdict: VerdictUnchanged,
			wantP:       0.50,
			wantSummary: "UNCHANGED: all 1 task(s) unchanged",
			wantCapped:  false,
		},
		{
			name: "UNCHANGED capped to INCONCLUSIVE with single candidate failure",
			verdict: RunVerdict{
				Verdict:      VerdictUnchanged,
				PImprovement: 0.50,
				Summary:      "UNCHANGED: all 1 task(s) unchanged",
			},
			failed:      []string{"docs/task_two"},
			wantVerdict: VerdictInconclusive,
			wantP:       0.50,
			wantSummary: "INCONCLUSIVE: candidate task failed: docs/task_two; otherwise UNCHANGED: all 1 task(s) unchanged",
			wantCapped:  true,
		},
		{
			name: "UNCHANGED capped to INCONCLUSIVE with multiple candidate failures sorts IDs",
			verdict: RunVerdict{
				Verdict:      VerdictUnchanged,
				PImprovement: 0.50,
				Summary:      "UNCHANGED: all 2 task(s) unchanged",
			},
			failed:      []string{"task-b", "task-a"},
			wantVerdict: VerdictInconclusive,
			wantP:       0.50,
			wantSummary: "INCONCLUSIVE: candidate tasks failed: task-a, task-b; otherwise UNCHANGED: all 2 task(s) unchanged",
			wantCapped:  true,
		},
		{
			name: "IMPROVED capped to INCONCLUSIVE and P capped to 0.50",
			verdict: RunVerdict{
				Verdict:      VerdictImproved,
				PImprovement: 0.975,
				Summary:      "IMPROVED: 2 task(s) improved, 0 unchanged",
			},
			failed:      []string{"docs/task_two"},
			wantVerdict: VerdictInconclusive,
			wantP:       0.50,
			wantSummary: "INCONCLUSIVE: candidate task failed: docs/task_two; otherwise IMPROVED: 2 task(s) improved, 0 unchanged",
			wantCapped:  true,
		},
		{
			name: "IMPROVED capped to INCONCLUSIVE with P already <= 0.50 keeps lower P",
			verdict: RunVerdict{
				Verdict:      VerdictImproved,
				PImprovement: 0.40,
				Summary:      "IMPROVED: 1 task(s) improved, 0 unchanged",
			},
			failed:      []string{"docs/task_two"},
			wantVerdict: VerdictInconclusive,
			wantP:       0.40,
			wantSummary: "INCONCLUSIVE: candidate task failed: docs/task_two; otherwise IMPROVED: 1 task(s) improved, 0 unchanged",
			wantCapped:  true,
		},
		{
			name: "REGRESSED preserved and P untouched",
			verdict: RunVerdict{
				Verdict:      VerdictRegressed,
				PImprovement: 0.04,
				Summary:      "REGRESSED: 1 task(s) regressed",
			},
			failed:      []string{"docs/task_two"},
			wantVerdict: VerdictRegressed,
			wantP:       0.04,
			wantSummary: "REGRESSED: 1 task(s) regressed",
			wantCapped:  false,
		},
		{
			name: "RUNNING preserved and P untouched",
			verdict: RunVerdict{
				Verdict:      VerdictRunning,
				PImprovement: 0.0,
				Summary:      "RUNNING: 2 task(s) in progress",
			},
			failed:      []string{"docs/task_two"},
			wantVerdict: VerdictRunning,
			wantP:       0.0,
			wantSummary: "RUNNING: 2 task(s) in progress",
			wantCapped:  false,
		},
		{
			name: "already-INCONCLUSIVE keeps its P and summary",
			verdict: RunVerdict{
				Verdict:      VerdictInconclusive,
				PImprovement: 0.35,
				Summary:      "INCONCLUSIVE: 1 task(s) inconclusive; more samples recommended",
			},
			failed:      []string{"docs/task_two"},
			wantVerdict: VerdictInconclusive,
			wantP:       0.35,
			wantSummary: "INCONCLUSIVE: 1 task(s) inconclusive; more samples recommended",
			wantCapped:  false,
		},
		{
			name: "UNCHANGED capped with >20 failed tasks truncates with MaxIDList",
			verdict: RunVerdict{
				Verdict:      VerdictUnchanged,
				PImprovement: 0.50,
				Summary:      "UNCHANGED: all 1 task(s) unchanged",
			},
			failed:      tasks25,
			wantVerdict: VerdictInconclusive,
			wantP:       0.50,
			wantSummary: "INCONCLUSIVE: candidate tasks failed: " + strings.Join(tasks25[:20], ", ") + ", ... and 5 more; otherwise UNCHANGED: all 1 task(s) unchanged",
			wantCapped:  true,
		},
		{
			name: "empty summary handled defensively",
			verdict: RunVerdict{
				Verdict:      VerdictUnchanged,
				PImprovement: 0.50,
				Summary:      "",
			},
			failed:      []string{"task-x"},
			wantVerdict: VerdictInconclusive,
			wantP:       0.50,
			wantSummary: "INCONCLUSIVE: candidate task failed: task-x",
			wantCapped:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, capped := CapForCandidateFailures(tc.verdict, tc.failed)
			if got.Verdict != tc.wantVerdict {
				t.Errorf("Verdict = %v, want %v", got.Verdict, tc.wantVerdict)
			}
			if math.Abs(got.PImprovement-tc.wantP) > 1e-6 {
				t.Errorf("PImprovement = %v, want %v", got.PImprovement, tc.wantP)
			}
			if got.Summary != tc.wantSummary {
				t.Errorf("Summary = %q, want %q", got.Summary, tc.wantSummary)
			}
			if capped != tc.wantCapped {
				t.Errorf("capped = %v, want %v", capped, tc.wantCapped)
			}
		})
	}
}

func TestFormatIDList(t *testing.T) {
	if got := FormatIDList(nil, 20); got != "" {
		t.Errorf("expected empty string for nil, got %q", got)
	}
	if got := FormatIDList([]string{}, 20); got != "" {
		t.Errorf("expected empty string for empty slice, got %q", got)
	}
	if got := FormatIDList([]string{"a", "b", "c"}, 20); got != "a, b, c" {
		t.Errorf("expected 'a, b, c', got %q", got)
	}
	items := make([]string, 25)
	for i := range items {
		items[i] = fmt.Sprintf("id-%d", i+1)
	}
	got25 := FormatIDList(items, 20)
	want25 := strings.Join(items[:20], ", ") + ", ... and 5 more"
	if got25 != want25 {
		t.Errorf("FormatIDList(25, 20) = %q, want %q", got25, want25)
	}
}
