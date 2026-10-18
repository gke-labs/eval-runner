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
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NavTab represents the current active tab in the UI header.
type NavTab string

const (
	NavTabRuns        NavTab = "runs"
	NavTabComparisons NavTab = "comparisons"
)

// RunListItem represents a single run row in the /runs list view.
type RunListItem struct {
	Name        string
	Namespace   string
	Phase       v1alpha1.RunPhase
	PhaseChip   string // good, bad, warn, neutral, pending
	Tasks       string // e.g. "2/2"
	Samples     int
	SampleRange string // compact, e.g. "samples 9-16"
	Rates       string
	Commit      string // short commit
	Age         string // e.g. "14m"
	Link        string // "/runs/<ns>/<name>"
}

// RunsListView is the view model for the /runs list page.
type RunsListView struct {
	ActiveTab    NavTab
	Namespace    string
	Runs         []RunListItem
	TotalCount   int
	Offset       int
	Limit        int
	HasMore      bool
	NextLink     string
	HasPrev      bool
	PrevLink     string
	ShowingRange string
}

// RunTaskRow represents a single eval task in the /runs/<ns>/<name> detail view.
type RunTaskRow struct {
	ID          string
	Phase       v1alpha1.TaskPhase
	PhaseChip   string // good, bad, warn, neutral, pending
	RateStr     string // e.g. "0.84"
	HasCI       bool
	CILeftPx    float64
	CIWidthPx   float64
	CIDotPx     float64
	CILabel     string // e.g. "[0.72, 0.95]"
	Samples     int
	Successes   int
	Method      string // e.g. "wilson", "t"
	LogPath     string // text path or URI
	Message     string
	SampleRange string // e.g. "samples 9-16"
}

// RunDetailView is the view model for the /runs/<ns>/<name> detail page.
type RunDetailView struct {
	ActiveTab       NavTab
	Name            string
	Namespace       string
	Phase           v1alpha1.RunPhase
	PhaseChip       string
	RepoURL         string
	Commit          string
	CommitShort     string
	Model           string
	Age             string
	ArtifactsPrefix string
	Message         string

	// Summary
	TasksSummary     string
	TasksCompleted   int
	TasksFailed      int
	TasksTotal       int
	TotalSamples     int
	SampleRange      string // e.g. "samples 9-16"
	MeanSuccessRate  string
	SuccessRateRange string
	CompactRates     string

	Tasks []RunTaskRow
}

// ComparisonListItem represents a single comparison row in the /comparisons list view.
type ComparisonListItem struct {
	Name         string
	Namespace    string
	Verdict      v1alpha1.ComparisonVerdict
	VerdictChip  string
	PImprovement string
	Tasks        string
	BaselineRun  string
	CandidateRun string
	Age          string
	Link         string
}

// ComparisonsListView is the view model for the /comparisons list page.
type ComparisonsListView struct {
	ActiveTab    NavTab
	Namespace    string
	Comparisons  []ComparisonListItem
	TotalCount   int
	Offset       int
	Limit        int
	HasMore      bool
	NextLink     string
	HasPrev      bool
	PrevLink     string
	ShowingRange string
}

// ComparisonTableView is the view model for rendering the comparison table sub-template.
type ComparisonTableView struct {
	Tasks []ComparisonTaskRow
}

// ComparisonTaskRow represents a single task row in the PR-comment table.
type ComparisonTaskRow struct {
	ID          string
	BaseRate    string // e.g. "0.61"
	CandRate    string // e.g. "0.84"
	Delta       string // e.g. "+0.23"
	HasCI       bool
	CILeftPx    float64
	CIWidthPx   float64
	CIDotPx     float64
	CILabel     string // e.g. "[+0.11, +0.35]"
	StatusClass string // good, bad, warn, neutral, pending
	Label       string // improved, regressed, unchanged, inconclusive, running
	Pairs       int
	DroppedCand int
	DroppedBase int
	SamplesRun  int
	Decision    string
}

// ComparisonRows maps a Comparison resource into a slice of ComparisonTaskRow
// suitable for rendering in both HTML sub-templates and Markdown comments.
func ComparisonRows(comp *v1alpha1.Comparison) []ComparisonTaskRow {
	if comp == nil {
		return nil
	}

	rows := make([]ComparisonTaskRow, 0, len(comp.Status.Tasks))
	for _, task := range comp.Status.Tasks {
		leftPx, widthPx, dotPx, hasCI := CalculateComparisonBarGeometry(task.Delta, task.DeltaCI, 340.0)

		baseRate := "—"
		if task.BaseRate != nil {
			baseRate = fmt.Sprintf("%.2f", *task.BaseRate)
		}

		candRate := "—"
		if task.CandidateRate != nil {
			candRate = fmt.Sprintf("%.2f", *task.CandidateRate)
		}

		deltaStr := "—"
		if task.Delta != nil {
			deltaStr = fmt.Sprintf("%+.2f", *task.Delta)
		}

		ciLabel := "—"
		if task.DeltaCI != nil {
			ciLabel = fmt.Sprintf("[%+.2f, %+.2f]", task.DeltaCI.Lower, task.DeltaCI.Upper)
		}

		rows = append(rows, ComparisonTaskRow{
			ID:          task.ID,
			BaseRate:    baseRate,
			CandRate:    candRate,
			Delta:       deltaStr,
			HasCI:       hasCI,
			CILeftPx:    leftPx,
			CIWidthPx:   widthPx,
			CIDotPx:     dotPx,
			CILabel:     ciLabel,
			StatusClass: labelStatusClass(task.Label),
			Label:       string(task.Label),
			Pairs:       task.Pairs,
			DroppedCand: task.Dropped.Candidate,
			DroppedBase: task.Dropped.Baseline,
			SamplesRun:  task.SamplesRun,
			Decision:    string(task.Decision),
		})
	}
	return rows
}

// ComparisonSummaryData holds aggregate summary, headline, and footer data for a Comparison.
type ComparisonSummaryData struct {
	Headline         string // e.g. "05ce4d8 vs a81d0c4 · P(improvement) = 0.98" or "cand vs base · P(improvement) = 0.26"
	PImprovement     string // e.g. "0.98" or "—"
	BaseRun          string // e.g. "docs-eval-base-05ce4d8"
	BaseCommit       string // full commit SHA or empty
	BaseCommitShort  string // e.g. "a81d0c4" or ""
	CandRun          string // e.g. "docs-eval-cand-05ce4d8"
	CandCommit       string // full commit SHA or empty
	CandCommitShort  string // e.g. "05ce4d8" or ""
	TotalSamples     int
	TotalPairs       int
	TotalDroppedCand int
	TotalDroppedBase int
	SamplesSummary   string // e.g. "96 run · 48 paired" or "96 run · 48 paired · dropped: 2 cand, 1 base"
	BaseSummary      string // e.g. "commit a81d0c4 (docs-eval-base-05ce4d8)"
	CandSummary      string // e.g. "commit 05ce4d8 (docs-eval-cand-05ce4d8)"
}

// ComparisonSummary computes aggregate headline and footer strings for a Comparison.
// Optional Run resources (baseRun, candRun) can be passed to provide commit hashes
// and total sample counts. If omitted or nil, the summary falls back to the Run names
// and task counts in the Comparison spec and status.
func ComparisonSummary(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run) ComparisonSummaryData {
	if comp == nil {
		return ComparisonSummaryData{
			PImprovement: "—",
		}
	}

	// Match by name if possible in case caller passed them in reverse order
	if baseRun != nil && candRun != nil {
		if comp.Spec.Baseline.Run != "" && comp.Spec.Candidate.Run != "" {
			if baseRun.Name == comp.Spec.Candidate.Run && candRun.Name == comp.Spec.Baseline.Run {
				baseRun, candRun = candRun, baseRun
			}
		}
	}

	baseCommit := ""
	candCommit := ""
	baseTotalSamples := 0
	candTotalSamples := 0

	if baseRun != nil {
		baseCommit = baseRun.Spec.Repo.Commit
		if baseRun.Status.Summary != nil {
			baseTotalSamples = baseRun.Status.Summary.TotalSamples
		}
	}

	if candRun != nil {
		candCommit = candRun.Spec.Repo.Commit
		if candRun.Status.Summary != nil {
			candTotalSamples = candRun.Status.Summary.TotalSamples
		}
	}

	totalPairs := 0
	totalDroppedCand := 0
	totalDroppedBase := 0
	for _, task := range comp.Status.Tasks {
		totalPairs += task.Pairs
		totalDroppedCand += task.Dropped.Candidate
		totalDroppedBase += task.Dropped.Baseline
	}

	totalSamples := baseTotalSamples + candTotalSamples
	if totalSamples == 0 {
		totalSamples = (totalPairs + totalDroppedCand) + (totalPairs + totalDroppedBase)
	}

	pImpStr := "—"
	if comp.Status.PImprovement != nil {
		pImpStr = fmt.Sprintf("%.2f", *comp.Status.PImprovement)
	}

	candCommitShort := ShortCommit(candCommit)
	baseCommitShort := ShortCommit(baseCommit)

	candRef := candCommitShort
	if candRef == "" && candRun != nil && candRun.Name != "" {
		candRef = candRun.Name
	}
	if candRef == "" {
		candRef = comp.Spec.Candidate.Run
	}

	baseRef := baseCommitShort
	if baseRef == "" && baseRun != nil && baseRun.Name != "" {
		baseRef = baseRun.Name
	}
	if baseRef == "" {
		baseRef = comp.Spec.Baseline.Run
	}

	headlineParts := make([]string, 0, 2)
	if candRef != "" || baseRef != "" {
		headlineParts = append(headlineParts, fmt.Sprintf("%s vs %s", candRef, baseRef))
	}
	if comp.Status.PImprovement != nil {
		headlineParts = append(headlineParts, fmt.Sprintf("P(improvement) = %.2f", *comp.Status.PImprovement))
	}
	headline := strings.Join(headlineParts, " · ")

	samplesSummary := fmt.Sprintf("%d run · %d paired", totalSamples, totalPairs)
	if totalDroppedCand > 0 || totalDroppedBase > 0 {
		samplesSummary += fmt.Sprintf(" · dropped: %d cand, %d base", totalDroppedCand, totalDroppedBase)
	}

	baseRunName := comp.Spec.Baseline.Run
	if baseRunName == "" && baseRun != nil {
		baseRunName = baseRun.Name
	}
	candRunName := comp.Spec.Candidate.Run
	if candRunName == "" && candRun != nil {
		candRunName = candRun.Name
	}

	baseSummary := baseRunName
	if baseCommitShort != "" {
		if baseSummary != "" {
			baseSummary = fmt.Sprintf("commit %s (%s)", baseCommitShort, baseSummary)
		} else {
			baseSummary = fmt.Sprintf("commit %s", baseCommitShort)
		}
	}

	candSummary := candRunName
	if candCommitShort != "" {
		if candSummary != "" {
			candSummary = fmt.Sprintf("commit %s (%s)", candCommitShort, candSummary)
		} else {
			candSummary = fmt.Sprintf("commit %s", candCommitShort)
		}
	}

	return ComparisonSummaryData{
		Headline:         headline,
		PImprovement:     pImpStr,
		BaseRun:          baseRunName,
		BaseCommit:       baseCommit,
		BaseCommitShort:  baseCommitShort,
		CandRun:          candRunName,
		CandCommit:       candCommit,
		CandCommitShort:  candCommitShort,
		TotalSamples:     totalSamples,
		TotalPairs:       totalPairs,
		TotalDroppedCand: totalDroppedCand,
		TotalDroppedBase: totalDroppedBase,
		SamplesSummary:   samplesSummary,
		BaseSummary:      baseSummary,
		CandSummary:      candSummary,
	}
}

// ComparisonDetailView is the view model for the /comparisons/<ns>/<name> detail page.
type ComparisonDetailView struct {
	ActiveTab    NavTab
	Name         string
	Namespace    string
	Verdict      v1alpha1.ComparisonVerdict
	VerdictChip  string
	Headline     string // e.g. "3f9c2e1 vs a81d0c4 · P(improvement) = 0.98"
	PImprovement string // e.g. "0.98"
	SummaryPath  string
	Message      string

	Tasks []ComparisonTaskRow

	// Summary contains aggregate comparison headline and footer data.
	Summary ComparisonSummaryData

	// Footer data
	BaseRun          string
	BaseCommit       string
	BaseCommitShort  string
	CandRun          string
	CandCommit       string
	CandCommitShort  string
	TotalSamples     int
	TotalPairs       int
	TotalDroppedCand int
	TotalDroppedBase int
	SamplesSummary   string
}

// Helpers for CSS status classes

func phaseChipClass(phase v1alpha1.RunPhase) string {
	switch phase {
	case v1alpha1.RunPhaseCompleted:
		return "good"
	case v1alpha1.RunPhaseRunning:
		return "pending"
	case v1alpha1.RunPhasePending, v1alpha1.RunPhaseDiscovering, v1alpha1.RunPhaseDiscovered:
		return "warn"
	case v1alpha1.RunPhaseFailed:
		return "bad"
	default:
		return "neutral"
	}
}

func taskPhaseChipClass(phase v1alpha1.TaskPhase) string {
	switch phase {
	case v1alpha1.TaskPhaseCompleted:
		return "good"
	case v1alpha1.TaskPhaseRunning:
		return "pending"
	case v1alpha1.TaskPhasePending:
		return "neutral"
	case v1alpha1.TaskPhaseFailed:
		return "bad"
	default:
		return "neutral"
	}
}

func verdictChipClass(verdict v1alpha1.ComparisonVerdict) string {
	switch verdict {
	case v1alpha1.ComparisonVerdictImproved:
		return "good"
	case v1alpha1.ComparisonVerdictRegressed:
		return "bad"
	case v1alpha1.ComparisonVerdictUnchanged:
		return "neutral"
	case v1alpha1.ComparisonVerdictInconclusive:
		return "warn"
	case v1alpha1.ComparisonVerdictRunning:
		return "pending"
	default:
		return "neutral"
	}
}

func labelStatusClass(label v1alpha1.ComparisonTaskLabel) string {
	switch label {
	case v1alpha1.ComparisonTaskLabelImproved:
		return "good"
	case v1alpha1.ComparisonTaskLabelRegressed:
		return "bad"
	case v1alpha1.ComparisonTaskLabelUnchanged:
		return "neutral"
	case v1alpha1.ComparisonTaskLabelInconclusive:
		return "warn"
	case v1alpha1.ComparisonTaskLabelRunning, v1alpha1.ComparisonTaskLabelContinue:
		return "pending"
	default:
		return "neutral"
	}
}

func clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

// CalculateComparisonBarGeometry calculates the geometry for the comparison CI bar.
// Bar width is 340px, with the zero mark at the center (170px).
// Scale is 340px / 1.0 = 340px per unit difference (range [-0.5, +0.5] spans the 340px bar).
func CalculateComparisonBarGeometry(delta *float64, ci *v1alpha1.DeltaConfidenceInterval, barWidth float64) (leftPx, widthPx, dotPx float64, hasCI bool) {
	if ci == nil {
		return 0, 0, 0, false
	}

	zeroPx := barWidth / 2.0
	scale := barWidth / 1.0 // 340 px per unit delta

	lo := ci.Lower
	hi := ci.Upper
	d := 0.0
	if delta != nil {
		d = *delta
	} else {
		d = (lo + hi) / 2.0
	}

	rawLeft := zeroPx + lo*scale
	rawRight := zeroPx + hi*scale

	leftPx = clamp(rawLeft, 0, barWidth)
	rightPx := clamp(rawRight, 0, barWidth)
	if rightPx < leftPx {
		rightPx = leftPx
	}
	widthPx = math.Max(rightPx-leftPx, 2.0) // at least 2px to be visible

	// Point estimate dot is 14px wide, centered on zeroPx + d*scale
	rawDot := zeroPx + d*scale - 7.0
	dotPx = clamp(rawDot, 0, barWidth-14.0)

	return leftPx, widthPx, dotPx, true
}

// CalculateRunBarGeometry calculates the geometry for a Run's single-task CI bar against a 0-1 axis.
// Axis width is barWidth (default 240px). Axis goes from 0.0 (left=0) to 1.0 (right=barWidth).
func CalculateRunBarGeometry(rate *float64, meanScore *float64, ci *v1alpha1.ConfidenceInterval, barWidth float64) (leftPx, widthPx, dotPx float64, hasCI bool) {
	if ci == nil {
		return 0, 0, 0, false
	}

	lo := clamp(ci.Lower, 0.0, 1.0)
	hi := clamp(ci.Upper, 0.0, 1.0)
	if hi < lo {
		hi = lo
	}

	pt := 0.0
	if rate != nil {
		pt = clamp(*rate, 0.0, 1.0)
	} else if meanScore != nil {
		pt = clamp(*meanScore, 0.0, 1.0)
	} else {
		pt = (lo + hi) / 2.0
	}

	leftPx = lo * barWidth
	rightPx := hi * barWidth
	widthPx = math.Max(rightPx-leftPx, 2.0)

	// Point estimate dot is 14px wide, centered on pt * barWidth
	rawDot := pt*barWidth - 7.0
	dotPx = clamp(rawDot, 0, barWidth-14.0)

	return leftPx, widthPx, dotPx, true
}

func FormatAge(t metav1.Time) string {
	if t.IsZero() {
		return "—"
	}
	d := time.Since(t.Time)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func ShortCommit(c string) string {
	if len(c) > 7 {
		return c[:7]
	}
	return c
}
