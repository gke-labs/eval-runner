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
	"sort"
	"strings"
)

// Verdict determines the overall run-level verdict across a collection of task labels
// implementing the verdict semantics defined in docs/concept/functionality.md §4.4:
//
//   - RUNNING:      Sampling in progress.
//   - REGRESSED:    At least one task regressed.
//   - INCONCLUSIVE: No regressions found but at least one task is inconclusive; more samples recommended.
//   - IMPROVED:     At least one task improved, none regressed (all others unchanged).
//   - UNCHANGED:    All tasks unchanged.
//
// Precedence order:
//  1. RUNNING: Any active sampling (running, queued, continue) means the run is still in progress.
//     Per-task labels (including early regressions) are reported in the per-task table, but the run
//     headline remains RUNNING until sampling terminates.
//  2. REGRESSED: If sampling is finished and any task regressed, the run failed.
//  3. INCONCLUSIVE: If no regressions but any task is inconclusive, the run is inconclusive.
//  4. IMPROVED: At least one task improved and all other tasks are unchanged.
//  5. UNCHANGED: All tasks are unchanged.
//
// Note on P(improvement): Verdict evaluates discrete labels only and returns PImprovement: 0.0.
// Callers requiring posterior probabilities must use AggregateVerdict, which computes
// the headline P(improvement) from per-task estimates.
func Verdict(labels []TaskLabel) RunVerdict {
	if len(labels) == 0 {
		return RunVerdict{
			Verdict: VerdictInconclusive,
			Summary: "INCONCLUSIVE: no tasks evaluated",
		}
	}

	var numImproved, numRegressed, numUnchanged, numInconclusive, numRunning int

	for _, l := range labels {
		switch l {
		case TaskLabelRunning, TaskLabelQueued, TaskLabelContinue:
			numRunning++
		case TaskLabelRegressed:
			numRegressed++
		case TaskLabelInconclusive:
			numInconclusive++
		case TaskLabelImproved:
			numImproved++
		case TaskLabelUnchanged:
			numUnchanged++
		default:
			numInconclusive++
		}
	}

	// 1. Any active task means sampling is in progress (transient state)
	if numRunning > 0 {
		return RunVerdict{
			Verdict: VerdictRunning,
			Summary: fmt.Sprintf("RUNNING: %d task(s) in progress", numRunning),
		}
	}

	// 2. Any regression fails the run once sampling is complete
	if numRegressed > 0 {
		return RunVerdict{
			Verdict: VerdictRegressed,
			Summary: fmt.Sprintf("REGRESSED: %d task(s) regressed", numRegressed),
		}
	}

	// 3. Mixed inconclusive + improved (or inconclusive + unchanged)
	if numInconclusive > 0 {
		var detail string
		if numImproved > 0 {
			detail = fmt.Sprintf("INCONCLUSIVE: %d task(s) improved, but %d task(s) inconclusive; more samples recommended", numImproved, numInconclusive)
		} else {
			detail = fmt.Sprintf("INCONCLUSIVE: %d task(s) inconclusive; more samples recommended", numInconclusive)
		}
		return RunVerdict{
			Verdict: VerdictInconclusive,
			Summary: detail,
		}
	}

	// 4. At least one improved and none regressed / inconclusive
	if numImproved > 0 {
		return RunVerdict{
			Verdict: VerdictImproved,
			Summary: fmt.Sprintf("IMPROVED: %d task(s) improved, %d unchanged", numImproved, numUnchanged),
		}
	}

	// 5. All unchanged
	return RunVerdict{
		Verdict: VerdictUnchanged,
		Summary: fmt.Sprintf("UNCHANGED: all %d task(s) unchanged", numUnchanged),
	}
}

// AggregateVerdict aggregates multiple task reports into a final RunVerdict, combining
// discrete task labels and continuous posterior probabilities into a single headline P(improvement).
//
// Aggregation rule for P(improvement) across tasks:
// To declare high confidence in net improvement, there must be strong evidence of improvement
// on at least one task (max_k P(improvement_k)), while bounding risk by the task most vulnerable
// to regression (min_k P(not_regressed_k)):
//
//	P(run improvement) = min( max_k P(imp_k), min_k P(not_reg_k) )
//
// Per-task P(not_regressed) definition and label floors:
// - For tasks labelled improved, P(not_regressed) has a floor of 0.999.
// - For tasks labelled unchanged, P(not_regressed) has a floor of 0.975 (reflecting equivalence).
// - For all other tasks (regressed, inconclusive, running), P(not_regressed) is taken as P(improvement).
//
// Headline value rules:
//   - If the run verdict is REGRESSED, P(run improvement) collapses to min_k P(not_reg_k).
//   - If UNCHANGED, headline P(run improvement) is 0.50.
//   - If INCONCLUSIVE with only unchanged and inconclusive tasks (no improved and no regressed tasks),
//     headline P(run improvement) is the arithmetic mean of per-task P(improvement_k) capped by
//     min_k P(not_reg_k) (P_run = min(mean_k P_k, min_k P(not_reg_k))), so one risky task cannot be diluted
//     by many unchanged ones.
//   - Otherwise (IMPROVED, or mixed IMPROVED + INCONCLUSIVE), P(run improvement) = min( max_k P(imp_k), min_k P(not_reg_k) ).
func AggregateVerdict(tasks []TaskReport) RunVerdict {
	if len(tasks) == 0 {
		return Verdict(nil)
	}

	labels := make([]TaskLabel, len(tasks))
	var numImproved, numRegressed, numUnchanged, numInconclusive, numRunning int
	for i, t := range tasks {
		labels[i] = t.Label
		switch t.Label {
		case TaskLabelRunning, TaskLabelQueued, TaskLabelContinue:
			numRunning++
		case TaskLabelRegressed:
			numRegressed++
		case TaskLabelInconclusive:
			numInconclusive++
		case TaskLabelImproved:
			numImproved++
		case TaskLabelUnchanged:
			numUnchanged++
		default:
			numInconclusive++
		}
	}

	verdict := Verdict(labels)

	if len(tasks) == 1 {
		verdict.PImprovement = tasks[0].PImprovement
		return verdict
	}

	maxPImp := 0.0
	minPNotReg := 1.0

	for _, t := range tasks {
		pImp := t.PImprovement
		if pImp > maxPImp {
			maxPImp = pImp
		}

		// Probability that task did not regress
		pNotReg := pImp
		if t.Label == TaskLabelUnchanged {
			pNotReg = math.Max(pNotReg, 0.975)
		} else if t.Label == TaskLabelImproved {
			pNotReg = math.Max(pNotReg, 0.999)
		}
		if pNotReg < minPNotReg {
			minPNotReg = pNotReg
		}
	}

	if verdict.Verdict == VerdictRegressed {
		verdict.PImprovement = minPNotReg
	} else if verdict.Verdict == VerdictUnchanged {
		verdict.PImprovement = 0.50
	} else if numImproved == 0 && numRegressed == 0 && numRunning == 0 && numInconclusive > 0 {
		var sumP float64
		for _, t := range tasks {
			sumP += t.PImprovement
		}
		verdict.PImprovement = math.Min(sumP/float64(len(tasks)), minPNotReg)
	} else {
		verdict.PImprovement = math.Min(maxPImp, minPNotReg)
	}

	return verdict
}

// MaxIDList is the default maximum number of task IDs displayed in formatted summaries before truncation.
const MaxIDList = 20

// FormatIDList formats a slice of task IDs, truncating to maxIDs if exceeded,
// appending an explicit "... and N more" marker.
func FormatIDList(ids []string, maxIDs int) string {
	if len(ids) == 0 {
		return ""
	}
	if maxIDs <= 0 || len(ids) <= maxIDs {
		return strings.Join(ids, ", ")
	}
	truncated := ids[:maxIDs]
	remaining := len(ids) - maxIDs
	return fmt.Sprintf("%s, ... and %d more", strings.Join(truncated, ", "), remaining)
}

// CapForCandidateFailures applies the candidate-task-failure verdict cap to an aggregate RunVerdict:
// If one or more candidate tasks failed (while baseline completed), any UNCHANGED or IMPROVED verdict
// is capped to INCONCLUSIVE with P(improvement) capped at 0.50.
//
// Precedence order: RUNNING > REGRESSED > INCONCLUSIVE (cap) > UNCHANGED > IMPROVED.
//
// If the verdict is capped, the summary is prefixed with:
//
//	"INCONCLUSIVE: candidate task[s] failed: <ids>; otherwise <original summary>"
//
// Returns the updated RunVerdict and a boolean indicating whether the verdict was capped.
func CapForCandidateFailures(v RunVerdict, failed []string) (RunVerdict, bool) {
	if len(failed) == 0 {
		return v, false
	}

	// RUNNING, REGRESSED, and already-INCONCLUSIVE take precedence over the cap.
	if v.Verdict != VerdictUnchanged && v.Verdict != VerdictImproved {
		return v, false
	}

	failedSorted := append([]string(nil), failed...)
	sort.Strings(failedSorted)

	taskWord := "task"
	if len(failedSorted) > 1 {
		taskWord = "tasks"
	}

	v.Verdict = VerdictInconclusive
	if v.Summary != "" {
		v.Summary = fmt.Sprintf("INCONCLUSIVE: candidate %s failed: %s; otherwise %s",
			taskWord, FormatIDList(failedSorted, MaxIDList), v.Summary)
	} else {
		v.Summary = fmt.Sprintf("INCONCLUSIVE: candidate %s failed: %s",
			taskWord, FormatIDList(failedSorted, MaxIDList))
	}
	if v.PImprovement > 0.50 {
		v.PImprovement = 0.50
	}

	return v, true
}
