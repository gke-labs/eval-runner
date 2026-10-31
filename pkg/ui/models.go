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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/stats"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NavTab represents the current active tab in the UI header.
type NavTab string

const (
	NavTabOverview    NavTab = "overview"
	NavTabEvals       NavTab = "evals"
	NavTabPulls       NavTab = "pulls"
	NavTabRuns        NavTab = "runs"
	NavTabComparisons NavTab = "comparisons"
	NavTabSeries      NavTab = "series"
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
	ActiveTab     NavTab
	Repo          RepoRef
	Namespace     string
	LinkNamespace string
	Runs          []RunListItem
	TotalCount    int
	Offset        int
	Limit         int
	HasMore       bool
	NextLink      string
	HasPrev       bool
	PrevLink      string
	ShowingRange  string
}

// RepoListItem represents a single repository entry in the repository index list.
type RepoListItem struct {
	Host             string
	Owner            string
	Name             string
	Repository       string
	RunsCount        int
	ComparisonsCount int
	Link             string
}

// RepoIndexView is the view model for the repository index at GET /.
type RepoIndexView struct {
	Namespace     string
	LinkNamespace string
	Repositories  []RepoListItem
	TotalCount    int
}

// SuiteOf returns the eval suite for a given task ID.
// An eval suite is the first path segment of the task ID (e.g. "docs" for "docs/readme_comprehension").
// A task ID without a "/" is a suite of one.
func SuiteOf(taskID string) string {
	if idx := strings.Index(taskID, "/"); idx != -1 {
		return taskID[:idx]
	}
	return taskID
}

// RepoOverviewEvalRow represents a single eval in the RepoOverview table.
type RepoOverviewEvalRow struct {
	ID          string
	SamplesLink string
	RunLink     string
	ScoreStr    string // e.g. "0.84" or "—"
	CILabel     string // e.g. "[0.70, 0.94]" or "—"
	DeltaStr    string // e.g. "Δ +0.10 [+0.02, +0.18]" or "—"
	DeltaClass  string // "good", "bad", "neutral", or ""
	HasDelta    bool
	Sparkline   SparklineSVG
}

// RepoOverviewSuiteRow represents an eval suite in the RepoOverview table.
type RepoOverviewSuiteRow struct {
	Name       string // e.g. "docs"
	SuiteLink  string // link if route exists, else empty
	ScoreStr   string // mean of its evals' scores, e.g. "0.70"
	CILabel    string // interval from pooled samples, e.g. "[0.58, 0.82]"
	EvalsCount int    // number of evals, e.g. 2
	DeltaStr   string // delta vs previous commit with interval
	DeltaClass string // "good", "bad", "neutral", or ""
	HasDelta   bool
	Sparkline  SparklineSVG
	Evals      []RepoOverviewEvalRow
}

// RepoOverviewView is the view model for the repository overview page (GET /{host}/{owner}/{repo}).
type RepoOverviewView struct {
	ActiveTab     NavTab
	Repo          RepoRef
	Namespace     string
	LinkNamespace string
	Branch        string
	Cohort        string
	IsEmpty       bool

	// Headline
	OverallScore       string
	OverallCILabel     string
	OverallCommitSHA   string
	OverallCommitShort string
	OverallCommitLink  string
	OverallTimeStr     string
	OverallTimeUTC     string

	// Suites and Evals
	Suites []RepoOverviewSuiteRow

	// Tiles below the suite table
	TotalRunsCount        int
	RunsLink              string
	TotalComparisonsCount int
	ComparisonsLink       string
	TotalPullsCount       int
	PullsLink             string
}

// RunTaskRow represents a single eval task in the /runs/<ns>/<name> detail view.
type RunTaskRow struct {
	ID                   string
	Phase                v1alpha1.TaskPhase
	PhaseChip            string // good, bad, warn, neutral, pending
	RateStr              string // e.g. "0.84"
	HasCI                bool
	CILeftPx             float64
	CIWidthPx            float64
	CIDotPx              float64
	CILabel              string // e.g. "[0.72, 0.95]"
	Samples              int
	Successes            int
	Method               string // e.g. "wilson", "t"
	LogPath              string // text path or URI
	SamplesLink          string // e.g. "/runs/<ns>/<name>/tasks/<task>/samples"
	Message              string
	SampleRange          string // e.g. "samples 9-16"
	SamplesDone          int
	SamplesPlanned       int
	ProgressPct          float64
	ProgressText         string // e.g. "8 of 96 samples"
	LatencyStr           string // e.g. "6.01s / 9.78s"
	TokensStr            string // e.g. "4399"
	InputTokens          string
	OutputTokens         string
	ReasoningStr         string // e.g. "133"
	CacheReadStr         string // e.g. "0"
	CostStr              string // e.g. "$0.0013"
	CostSource           v1alpha1.CostSource
	CostTooltip          string // "default prices" or "configmap prices"
	ArtifactsUnavailable bool
}

// LogURL returns a browsable web URL for the task's LogPath if it is a gs:// or http(s):// URI.
func (r RunTaskRow) LogURL() string {
	return FormatLogURL(r.LogPath)
}

// ArtifactsURL returns a browsable web URL for the run's ArtifactsPrefix if it is a gs:// or http(s):// URI.
func (v RunDetailView) ArtifactsURL() string {
	return FormatLogURL(v.ArtifactsPrefix)
}

// FormatLogURL converts a storage URI or URL to a clickable console or web link.
// For gs:// URIs, each path segment is URL-escaped to preserve characters such as #, ?, or %.
func FormatLogURL(uri string) string {
	uri = strings.TrimSpace(uri)
	if strings.HasPrefix(uri, "gs://") {
		trimmed := strings.TrimPrefix(uri, "gs://")
		isDir := strings.HasSuffix(trimmed, "/")
		segs := strings.Split(trimmed, "/")
		for i, s := range segs {
			segs[i] = url.PathEscape(s)
		}
		if isDir {
			return "https://console.cloud.google.com/storage/browser/" + strings.Join(segs, "/")
		}
		return "https://console.cloud.google.com/storage/browser/_details/" + strings.Join(segs, "/")
	}
	if strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://") {
		return uri
	}
	return ""
}

// MetricFilter represents the selected metric dimension to display in the UI.
type MetricFilter string

const (
	MetricFilterAll     MetricFilter = "all"
	MetricFilterRate    MetricFilter = "rate"
	MetricFilterLatency MetricFilter = "latency"
	MetricFilterTokens  MetricFilter = "tokens"
	MetricFilterCost    MetricFilter = "cost"
)

// RunDetailView is the view model for the /runs/<ns>/<name> detail page.
type RunDetailView struct {
	ActiveTab       NavTab
	Repo            RepoRef
	Name            string
	Namespace       string
	LinkNamespace   string
	Phase           v1alpha1.RunPhase
	PhaseChip       string
	RepoURL         string
	Commit          string
	CommitShort     string
	Model           string
	Age             string
	ArtifactsPrefix string
	ArtifactFooter  string
	Version         string
	Message         string
	Metric          MetricFilter
	TotalCost       string
	LatencySummary  string
	TokensSummary   string

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
	Completed    string // status.completionTime in UTC, e.g. "2026-10-20 10:35 UTC", or "—"
	Age          string
	Link         string

	// Reason is status.message, or a regression summary derived from status.tasks when the
	// verdict is REGRESSED and the message is empty, truncated for display.
	Reason string
	// ReasonFull is the untruncated Reason for the title attribute.
	ReasonFull string

	// Deltas are the Quality, Tokens and Cost changes from the baseline Run to the candidate Run.
	Deltas RunDeltas
}

// ComparisonsListView is the view model for the /comparisons list page.
type ComparisonsListView struct {
	ActiveTab     NavTab
	Repo          RepoRef
	Namespace     string
	LinkNamespace string
	Comparisons   []ComparisonListItem
	TotalCount    int
	Offset        int
	Limit         int
	HasMore       bool
	NextLink      string
	HasPrev       bool
	PrevLink      string
	ShowingRange  string

	// Kind is the selected ?kind= filter.
	Kind ComparisonKind
	// KindLabelKey is the label selected by Kind, used in the empty state of filtered views.
	KindLabelKey string
	// ShowKindFilter shows the All | vs baseline | vs previous links. It is set when the
	// namespace has labeled Comparisons or a filter is active.
	ShowKindFilter   bool
	KindAllLink      string
	KindBaselineLink string
	KindPreviousLink string

	// Groups holds one row per candidate Run for Comparisons labeled vs-baseline or vs-previous.
	Groups             []ComparisonGroupRow
	ShowGroups         bool
	ShowBaselineColumn bool
	ShowPreviousColumn bool

	// ShowFlat shows the table of Comparisons that carry neither label.
	ShowFlat bool
}

// PullRequestListItem represents a single aggregated pull request row in the /pulls list view.
type PullRequestListItem struct {
	Repository     string
	RepoHash       string
	Number         int
	State          string // "Open", "Merged", "Closed"
	StateChip      string // "open", "merged", "closed"
	GitHubURL      string
	HeadSHA        string
	HeadSHAShort   string
	BaseSHA        string
	BaseSHAShort   string
	Verdict        v1alpha1.ComparisonVerdict
	VerdictChip    string
	PImprovement   string
	SamplesRun     int
	Batches        int
	CompletionTime string
	Age            string
	ComparisonName string
	ComparisonLink string
	HistoryLink    string
	RepoLink       string
	Namespace      string
}

// PullRequestsListView is the view model for the /pulls list page.
type PullRequestsListView struct {
	ActiveTab     NavTab
	Repo          RepoRef
	Namespace     string
	LinkNamespace string
	Pulls         []PullRequestListItem
	TotalCount    int
	Offset        int
	Limit         int
	HasMore       bool
	NextLink      string
	HasPrev       bool
	PrevLink      string
	ShowingRange  string
	State         string // "all", "open", "closed"
	OpenCount     int
	ClosedCount   int
	OpenLink      string
	ClosedLink    string
	AllLink       string
}

// PullRequestHistoryItem represents one Comparison in a pull request's history.
type PullRequestHistoryItem struct {
	Name           string
	Namespace      string
	HeadSHA        string
	HeadSHAShort   string
	BaseSHA        string
	BaseSHAShort   string
	CandidateRun   string
	BaselineRun    string
	CandidateLink  string
	BaselineLink   string
	Verdict        v1alpha1.ComparisonVerdict
	VerdictChip    string
	PImprovement   string
	SamplesRun     int
	Batches        int
	CompletionTime string
	Age            string
	Link           string
}

// PullRequestDetailView is the view model for the /pulls/{repohash}/{number} history page.
type PullRequestDetailView struct {
	ActiveTab     NavTab
	Repo          RepoRef
	Namespace     string
	LinkNamespace string
	Repository    string
	RepoHash      string
	Number        int
	GitHubURL     string
	BackLink      string
	Comparisons   []PullRequestHistoryItem
	TotalCount    int
	Version       string
}

// RepoHash returns a deterministic 7-character lowercase hexadecimal SHA-256 hash
// of the normalized repository name (e.g. "owner/repo").
func RepoHash(repo string) string {
	norm := strings.ToLower(strings.TrimSpace(repo))
	h := sha256.Sum256([]byte(norm))
	return hex.EncodeToString(h[:])[:7]
}

// BuildGitHubPRURL builds a link to the GitHub pull request.
func BuildGitHubPRURL(repo string, prNum int) string {
	cleanRepo := strings.TrimPrefix(strings.TrimSpace(repo), "https://github.com/")
	cleanRepo = strings.TrimPrefix(cleanRepo, "http://github.com/")
	cleanRepo = strings.TrimSuffix(cleanRepo, ".git")
	cleanRepo = strings.Trim(cleanRepo, "/")
	return fmt.Sprintf("https://github.com/%s/pull/%d", cleanRepo, prNum)
}

// ComparisonSamplesRun returns the total evaluated samples for a Comparison.
func ComparisonSamplesRun(comp *v1alpha1.Comparison) int {
	if comp == nil {
		return 0
	}
	samplesRunTotal := 0
	totalPairs := 0
	for _, task := range comp.Status.Tasks {
		totalPairs += task.Pairs
		if task.SamplesRun > 0 {
			samplesRunTotal += task.SamplesRun
		} else {
			samplesRunTotal += task.Pairs
		}
	}
	if samplesRunTotal == 0 && totalPairs > 0 {
		samplesRunTotal = totalPairs
	}
	return samplesRunTotal
}

// FormatCompletionTime formats a Comparison completion timestamp as an age string, or "—" if not set.
func FormatCompletionTime(t *metav1.Time) string {
	if t == nil || t.IsZero() {
		return "—"
	}
	return FormatAge(*t)
}

// ComparisonTableView is the view model for rendering the comparison table sub-template.
type ComparisonTableView struct {
	Tasks       []ComparisonTaskRow
	HasTokens   bool
	HasLatency  bool
	HasCost     bool
	HasAffinity bool
	Metric      MetricFilter
}

// ComparisonTaskRow represents a single task row in the PR-comment table.
type ComparisonTaskRow struct {
	ID            string
	BaseRate      string // e.g. "0.61"
	CandRate      string // e.g. "0.84"
	Delta         string // e.g. "+0.23"
	HasCI         bool
	CILeftPx      float64
	CIWidthPx     float64
	CIDotPx       float64
	CILabel       string // e.g. "[+0.11, +0.35]"
	StatusClass   string // good, bad, warn, neutral, pending
	Label         string // improved, regressed, unchanged, inconclusive, running
	Pairs         int
	Wins          int
	Losses        int
	Ties          int
	PImprovement  *float64
	DroppedCand   int
	DroppedBase   int
	SamplesRun    int
	Decision      string
	SuggestedNext int
	Tokens        *v1alpha1.TaskTokens
	Affinity      string // always, matched, unmatched
	LatencyStr    string
	CostStr       string
	CostSource    v1alpha1.CostSource
	CostTooltip   string

	// Multi-metric comparison fields
	BaseLatencyStr  string // e.g. "8.50s / 12.00s"
	CandLatencyStr  string // e.g. "6.01s / 9.78s"
	DeltaLatencyStr string // e.g. "-2.49s"

	BaseTokensStr  string // e.g. "3200"
	CandTokensStr  string // e.g. "4399"
	DeltaTokensStr string // e.g. "+1199"

	BaseCostStr     string // e.g. "$0.0010"
	BaseCostTooltip string
	CandCostStr     string // e.g. "$0.0013"
	CandCostTooltip string
	DeltaCostStr    string // e.g. "+$0.0003"
	BaseSamplesLink string // link to baseline task samples list
	CandSamplesLink string // link to candidate task samples list
}

// FormatCostTooltip returns a human-readable tooltip describing the cost provenance.
func FormatCostTooltip(source v1alpha1.CostSource) string {
	switch source {
	case v1alpha1.CostSourceDefault:
		return "default prices"
	case v1alpha1.CostSourceConfigMap:
		return "configmap prices"
	default:
		return ""
	}
}

// ComparisonRows maps a Comparison resource into a slice of ComparisonTaskRow
// suitable for rendering in both HTML sub-templates and Markdown comments.
func ComparisonRows(comp *v1alpha1.Comparison) []ComparisonTaskRow {
	return ComparisonRowsWithRuns(comp, nil, nil, RepoRef{}, "")
}

// ComparisonRowsWithRuns maps a Comparison resource and resolves task latency,
// tokens, and cost from the candidate and baseline Runs when available.
// When ref has a non-empty Host, task sample links use repoLink with linkNS.
func ComparisonRowsWithRuns(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run, ref RepoRef, linkNS string) []ComparisonTaskRow {
	if comp == nil {
		return nil
	}

	candTasks := make(map[string]v1alpha1.TaskStatus)
	if candRun != nil {
		for _, t := range candRun.Status.Tasks {
			candTasks[t.ID] = t
		}
	}
	baseTasks := make(map[string]v1alpha1.TaskStatus)
	if baseRun != nil {
		for _, t := range baseRun.Status.Tasks {
			baseTasks[t.ID] = t
		}
	}

	rows := make([]ComparisonTaskRow, 0, len(comp.Status.Tasks))
	for _, task := range comp.Status.Tasks {
		leftPx, widthPx, dotPx, hasCI := CalculateComparisonBarGeometry(task.Delta, task.DeltaCI, 340.0)

		baseRate := "—"
		var baseLink string
		baseRunName := comp.Spec.Baseline.Run
		if baseRunName == "" && baseRun != nil {
			baseRunName = baseRun.Name
		}
		if baseRunName != "" && (task.BaseRate != nil || task.SamplesRun > 0 || task.Pairs > 0) {
			if task.BaseRate != nil {
				baseRate = fmt.Sprintf("%.2f", *task.BaseRate)
			}
			if ref.Host != "" {
				baseLink = repoLink(ref, fmt.Sprintf("/runs/%s/tasks/%s/samples", baseRunName, url.PathEscape(task.ID)), linkNS)
			} else {
				baseLink = BuildTaskSamplesListLink(comp.Namespace, baseRunName, task.ID)
			}
		} else if task.BaseRate != nil {
			baseRate = fmt.Sprintf("%.2f", *task.BaseRate)
		}

		candRate := "—"
		var candLink string
		candRunName := comp.Spec.Candidate.Run
		if candRunName == "" && candRun != nil {
			candRunName = candRun.Name
		}
		if candRunName != "" && (task.CandidateRate != nil || task.SamplesRun > 0 || task.Pairs > 0) {
			if task.CandidateRate != nil {
				candRate = fmt.Sprintf("%.2f", *task.CandidateRate)
			}
			if ref.Host != "" {
				candLink = repoLink(ref, fmt.Sprintf("/runs/%s/tasks/%s/samples", candRunName, url.PathEscape(task.ID)), linkNS)
			} else {
				candLink = BuildTaskSamplesListLink(comp.Namespace, candRunName, task.ID)
			}
		} else if task.CandidateRate != nil {
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

		affinity := task.Affinity
		if affinity == "" && comp.Status.Affinity != nil {
			affinity = "always"
		}

		// Latency resolution
		candLat := task.Latency
		if candLat == nil {
			if ct, ok := candTasks[task.ID]; ok && ct.Latency != nil {
				candLat = ct.Latency
			}
		}
		var baseLat *v1alpha1.TaskLatency
		if bt, ok := baseTasks[task.ID]; ok && bt.Latency != nil {
			baseLat = bt.Latency
		}

		var latStr, baseLatStr, candLatStr, deltaLatStr string
		if candLat != nil && (candLat.P50 > 0 || candLat.P95 > 0) {
			candLatStr = fmt.Sprintf("%.2fs / %.2fs", candLat.P50, candLat.P95)
			latStr = candLatStr
		}
		if baseLat != nil && (baseLat.P50 > 0 || baseLat.P95 > 0) {
			baseLatStr = fmt.Sprintf("%.2fs / %.2fs", baseLat.P50, baseLat.P95)
		}
		if candLat != nil && baseLat != nil && candLat.P50 > 0 && baseLat.P50 > 0 {
			diff := candLat.P50 - baseLat.P50
			deltaLatStr = fmt.Sprintf("%+.2fs", diff)
		}

		// Tokens resolution
		tokens := task.Tokens
		if tokens == nil {
			if ct, ok := candTasks[task.ID]; ok && ct.Tokens != nil {
				tokens = ct.Tokens
			}
		}
		var baseTokens *v1alpha1.TaskTokens
		if bt, ok := baseTasks[task.ID]; ok && bt.Tokens != nil {
			baseTokens = bt.Tokens
		}

		var baseTokStr, candTokStr, deltaTokStr string
		if tokens != nil {
			candTokStr = fmt.Sprintf("%d", tokens.Total)
		}
		if baseTokens != nil {
			baseTokStr = fmt.Sprintf("%d", baseTokens.Total)
		}
		if tokens != nil && baseTokens != nil {
			diff := tokens.Total - baseTokens.Total
			deltaTokStr = fmt.Sprintf("%+d", diff)
		}

		// Cost resolution
		costUSD := task.CostUSD
		costSource := task.CostSource
		var candCostSource v1alpha1.CostSource
		if ct, ok := candTasks[task.ID]; ok {
			if costUSD == nil && ct.CostUSD != nil {
				costUSD = ct.CostUSD
			}
			candCostSource = ct.CostSource
		}
		if candCostSource == "" {
			candCostSource = costSource
		}
		if costSource == "" {
			costSource = candCostSource
		}

		var baseCostUSD *float64
		var baseCostSource v1alpha1.CostSource
		if bt, ok := baseTasks[task.ID]; ok {
			if bt.CostUSD != nil {
				baseCostUSD = bt.CostUSD
			}
			baseCostSource = bt.CostSource
		}

		var costStr, baseCostStr, candCostStr, deltaCostStr string
		var costTooltip, baseCostTooltip, candCostTooltip string
		if costUSD != nil {
			candCostStr = fmt.Sprintf("$%.4f", *costUSD)
			costStr = candCostStr
			costTooltip = FormatCostTooltip(costSource)
			candCostTooltip = FormatCostTooltip(candCostSource)
		}
		if baseCostUSD != nil {
			baseCostStr = fmt.Sprintf("$%.4f", *baseCostUSD)
			baseCostTooltip = FormatCostTooltip(baseCostSource)
		}
		if costUSD != nil && baseCostUSD != nil {
			diff := *costUSD - *baseCostUSD
			if diff > 0 {
				deltaCostStr = fmt.Sprintf("+$%.4f", diff)
			} else if diff < 0 {
				deltaCostStr = fmt.Sprintf("-$%.4f", -diff)
			} else {
				deltaCostStr = "$0.0000"
			}
		}

		rows = append(rows, ComparisonTaskRow{
			ID:              task.ID,
			BaseRate:        baseRate,
			CandRate:        candRate,
			Delta:           deltaStr,
			HasCI:           hasCI,
			CILeftPx:        leftPx,
			CIWidthPx:       widthPx,
			CIDotPx:         dotPx,
			CILabel:         ciLabel,
			StatusClass:     labelStatusClass(task.Label),
			Label:           string(task.Label),
			Pairs:           task.Pairs,
			Wins:            task.Wins,
			Losses:          task.Losses,
			Ties:            task.Ties,
			PImprovement:    task.PImprovement,
			DroppedCand:     task.Dropped.Candidate,
			DroppedBase:     task.Dropped.Baseline,
			SamplesRun:      task.SamplesRun,
			Decision:        string(task.Decision),
			SuggestedNext:   task.SuggestedNext,
			Tokens:          tokens,
			Affinity:        affinity,
			LatencyStr:      latStr,
			CostStr:         costStr,
			CostSource:      costSource,
			CostTooltip:     costTooltip,
			BaseLatencyStr:  baseLatStr,
			CandLatencyStr:  candLatStr,
			DeltaLatencyStr: deltaLatStr,
			BaseTokensStr:   baseTokStr,
			CandTokensStr:   candTokStr,
			DeltaTokensStr:  deltaTokStr,
			BaseCostStr:     baseCostStr,
			BaseCostTooltip: baseCostTooltip,
			CandCostStr:     candCostStr,
			CandCostTooltip: candCostTooltip,
			DeltaCostStr:    deltaCostStr,
			BaseSamplesLink: baseLink,
			CandSamplesLink: candLink,
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
	CostUSD          *float64
	PossibleSamples  int
	SamplesSummary   string // e.g. "96 run · 48 paired" or "$4.10 spent, 96 of 3,200 possible samples"
	BaseSummary      string // e.g. "commit a81d0c4 (docs-eval-base-05ce4d8)"
	CandSummary      string // e.g. "commit 05ce4d8 (docs-eval-cand-05ce4d8)"
	AffinityMatched  int
	HasAffinity      bool

	// Delta metrics between candidate and baseline
	CostPerSampleDelta string // e.g. "cost per sample −61%"
	P50LatencyDelta    string // e.g. "p50 latency −38%"
	FooterMetricsDelta string // e.g. "cost per sample −61% · p50 latency −38%"
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

	costUSD := comp.Status.CostUSD
	possibleSamples := comp.Status.PossibleSamples

	var spentStr string
	if costUSD != nil {
		if anyDefaultCostSource(comp, candRun, baseRun) {
			spentStr = fmt.Sprintf("$%.2f spent (default prices)", *costUSD)
		} else {
			spentStr = fmt.Sprintf("$%.2f spent", *costUSD)
		}
	}

	samplesRunTotal := 0
	for _, task := range comp.Status.Tasks {
		if task.SamplesRun > 0 {
			samplesRunTotal += task.SamplesRun
		} else {
			samplesRunTotal += task.Pairs
		}
	}
	if samplesRunTotal == 0 && totalPairs > 0 {
		samplesRunTotal = totalPairs
	}

	var samplesStr string
	if possibleSamples > 0 {
		samplesStr = fmt.Sprintf("%d of %d possible samples", samplesRunTotal, possibleSamples)
	} else {
		samplesStr = fmt.Sprintf("%d run · %d paired", totalSamples, totalPairs)
	}
	if totalDroppedCand > 0 || totalDroppedBase > 0 {
		samplesStr += fmt.Sprintf(" · dropped: %d cand, %d base", totalDroppedCand, totalDroppedBase)
	}

	var samplesSummary string
	if spentStr != "" {
		samplesSummary = fmt.Sprintf("%s, %s", spentStr, samplesStr)
	} else {
		samplesSummary = samplesStr
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

	affinityMatched := 0
	hasAffinity := false
	if comp.Status.Summary != nil {
		affinityMatched = comp.Status.Summary.AffinityMatched
	}
	if comp.Status.Affinity != nil || affinityMatched > 0 {
		hasAffinity = true
	}

	// Cost per sample delta
	hasCandCost := false
	candCost := 0.0
	if candRun != nil && candRun.Status.CostUSD != nil {
		candCost = *candRun.Status.CostUSD
		hasCandCost = true
	} else if candRun != nil {
		for _, t := range candRun.Status.Tasks {
			if t.CostUSD != nil {
				candCost += *t.CostUSD
				hasCandCost = true
			}
		}
	}

	hasBaseCost := false
	baseCost := 0.0
	if baseRun != nil && baseRun.Status.CostUSD != nil {
		baseCost = *baseRun.Status.CostUSD
		hasBaseCost = true
	} else if baseRun != nil {
		for _, t := range baseRun.Status.Tasks {
			if t.CostUSD != nil {
				baseCost += *t.CostUSD
				hasBaseCost = true
			}
		}
	}

	candSamples := 0
	if candRun != nil && candRun.Status.Summary != nil && candRun.Status.Summary.TotalSamples > 0 {
		candSamples = candRun.Status.Summary.TotalSamples
	} else if candRun != nil {
		for _, t := range candRun.Status.Tasks {
			if t.Samples > 0 {
				candSamples += t.Samples
			} else {
				candSamples += t.SamplesDone
			}
		}
	}
	if candSamples == 0 {
		candSamples = candTotalSamples
	}
	if candSamples == 0 {
		candSamples = totalPairs + totalDroppedCand
	}

	baseSamples := 0
	if baseRun != nil && baseRun.Status.Summary != nil && baseRun.Status.Summary.TotalSamples > 0 {
		baseSamples = baseRun.Status.Summary.TotalSamples
	} else if baseRun != nil {
		for _, t := range baseRun.Status.Tasks {
			if t.Samples > 0 {
				baseSamples += t.Samples
			} else {
				baseSamples += t.SamplesDone
			}
		}
	}
	if baseSamples == 0 {
		baseSamples = baseTotalSamples
	}
	if baseSamples == 0 {
		baseSamples = totalPairs + totalDroppedBase
	}

	var costDeltaStr string
	if hasCandCost && hasBaseCost && candSamples > 0 && baseSamples > 0 {
		candPerSample := candCost / float64(candSamples)
		basePerSample := baseCost / float64(baseSamples)
		if basePerSample > 0 {
			diffPct := (candPerSample - basePerSample) / basePerSample * 100
			pctStr, _ := signedFixed(diffPct, 0)
			costDeltaStr = fmt.Sprintf("cost per sample %s%%", pctStr)
		}
	}

	// P50 latency delta: mean of per-task p50 latencies over shared tasks reporting latency on both sides
	var latDeltaStr string
	if candRun != nil && baseRun != nil {
		candTaskLat := make(map[string]float64)
		for _, t := range candRun.Status.Tasks {
			if t.Latency != nil && t.Latency.P50 > 0 {
				candTaskLat[t.ID] = t.Latency.P50
			}
		}
		var sumCand, sumBase float64
		count := 0
		for _, t := range baseRun.Status.Tasks {
			if t.Latency != nil && t.Latency.P50 > 0 {
				if cLat, ok := candTaskLat[t.ID]; ok {
					sumCand += cLat
					sumBase += t.Latency.P50
					count++
				}
			}
		}
		if count > 0 {
			candMeanP50 := sumCand / float64(count)
			baseMeanP50 := sumBase / float64(count)
			if baseMeanP50 > 0 {
				diffPct := (candMeanP50 - baseMeanP50) / baseMeanP50 * 100
				pctStr, _ := signedFixed(diffPct, 0)
				latDeltaStr = fmt.Sprintf("p50 latency %s%%", pctStr)
			}
		}
	}

	var footerMetricsDelta string
	if costDeltaStr != "" && latDeltaStr != "" {
		footerMetricsDelta = costDeltaStr + " · " + latDeltaStr
	} else if costDeltaStr != "" {
		footerMetricsDelta = costDeltaStr
	} else if latDeltaStr != "" {
		footerMetricsDelta = latDeltaStr
	}

	return ComparisonSummaryData{
		Headline:           headline,
		PImprovement:       pImpStr,
		BaseRun:            baseRunName,
		BaseCommit:         baseCommit,
		BaseCommitShort:    baseCommitShort,
		CandRun:            candRunName,
		CandCommit:         candCommit,
		CandCommitShort:    candCommitShort,
		TotalSamples:       totalSamples,
		TotalPairs:         totalPairs,
		TotalDroppedCand:   totalDroppedCand,
		TotalDroppedBase:   totalDroppedBase,
		CostUSD:            costUSD,
		PossibleSamples:    possibleSamples,
		SamplesSummary:     samplesSummary,
		BaseSummary:        baseSummary,
		CandSummary:        candSummary,
		AffinityMatched:    affinityMatched,
		HasAffinity:        hasAffinity,
		CostPerSampleDelta: costDeltaStr,
		P50LatencyDelta:    latDeltaStr,
		FooterMetricsDelta: footerMetricsDelta,
	}
}

func anyDefaultCostSource(comp *v1alpha1.Comparison, candRun, baseRun *v1alpha1.Run) bool {
	for _, task := range comp.Status.Tasks {
		if task.CostSource == v1alpha1.CostSourceDefault {
			return true
		}
	}
	if candRun != nil {
		for _, task := range candRun.Status.Tasks {
			if task.CostSource == v1alpha1.CostSourceDefault {
				return true
			}
		}
	}
	if baseRun != nil {
		for _, task := range baseRun.Status.Tasks {
			if task.CostSource == v1alpha1.CostSourceDefault {
				return true
			}
		}
	}
	return false
}

// ComparisonBatchRow represents a single batch execution row in the comparison detail view.
type ComparisonBatchRow struct {
	Index        int
	Phase        v1alpha1.ComparisonPhase
	PhaseChip    string // e.g. "good", "pending", "bad", "neutral"
	SampleRange  string // e.g. "1-8" or "—"
	CandidateRun string
	BaselineRun  string
}

// ComparisonBatches extracts structured batch rows from a Comparison status.
func ComparisonBatches(comp *v1alpha1.Comparison) []ComparisonBatchRow {
	if comp == nil {
		return nil
	}
	var batchRows []ComparisonBatchRow
	for _, b := range comp.Status.Batches {
		sr := "—"
		if b.SampleRange != nil {
			sr = fmt.Sprintf("%d-%d", b.SampleRange.Start, b.SampleRange.End)
		}
		batchRows = append(batchRows, ComparisonBatchRow{
			Index:        b.Index,
			Phase:        b.Phase,
			PhaseChip:    comparisonPhaseChipClass(b.Phase),
			SampleRange:  sr,
			CandidateRun: b.CandidateRun,
			BaselineRun:  b.BaselineRun,
		})
	}
	return batchRows
}

// ComparisonActionRow represents an actionable recommendation for a comparison task.
type ComparisonActionRow struct {
	TaskID        string
	SuggestedNext int
	ShowTaskID    bool
}

// ComparisonActions returns structured action hints for inconclusive tasks in a Comparison.
func ComparisonActions(comp *v1alpha1.Comparison) []ComparisonActionRow {
	if comp == nil {
		return nil
	}
	showTaskID := len(comp.Status.Tasks) > 1
	var actions []ComparisonActionRow
	for _, task := range comp.Status.Tasks {
		if task.Label == v1alpha1.ComparisonTaskLabelInconclusive && task.SuggestedNext > 0 {
			actions = append(actions, ComparisonActionRow{
				TaskID:        task.ID,
				SuggestedNext: task.SuggestedNext,
				ShowTaskID:    showTaskID,
			})
		}
	}
	return actions
}

// ComparisonDetailView is the view model for the /comparisons/<ns>/<name> detail page.
type ComparisonDetailView struct {
	ActiveTab     NavTab
	Repo          RepoRef
	Name          string
	Namespace     string
	LinkNamespace string
	Verdict       v1alpha1.ComparisonVerdict
	VerdictChip   string
	Headline      string // e.g. "3f9c2e1 vs a81d0c4 · P(improvement) = 0.98"
	PImprovement  string // e.g. "0.98"
	SummaryPath   string
	Message       string
	Metric        MetricFilter

	Tasks           []ComparisonTaskRow
	HasTokens       bool
	HasLatency      bool
	HasCost         bool
	HasAffinity     bool
	AffinityMatched int

	Batches []ComparisonBatchRow
	Actions []ComparisonActionRow

	// Deltas are the Quality, Tokens and Cost changes from the baseline Run to the candidate Run.
	Deltas RunDeltas

	// RegressedTasks lists tasks labeled regressed, most negative delta first.
	RegressedTasks []RegressedTaskRow
	// EvaluatedTasks counts the tasks in status.tasks that were not skipped.
	EvaluatedTasks int

	// Summary contains aggregate comparison headline and footer data.
	Summary ComparisonSummaryData

	// Version is the application/controller version.
	Version string

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
	CostUSD          *float64
	PossibleSamples  int
	SamplesSummary   string

	// Histories contains per-task CI history points and inline SVG for tasks with pairs >= 2.
	Histories []ComparisonTaskHistory

	// HistoryNote is set when comparison history cannot be loaded (e.g. "artifact storage unavailable" or "history not available").
	HistoryNote string

	TotalCostStr   string
	BudgetStr      string
	BudgetNote     string
	TotalTokensStr string
	LatencySummary string
}

// TableView converts ComparisonDetailView to a ComparisonTableView for the comparison_table sub-template.
func (v ComparisonDetailView) TableView() ComparisonTableView {
	return ComparisonTableView{
		Tasks:       v.Tasks,
		HasTokens:   v.HasTokens,
		HasLatency:  v.HasLatency,
		HasCost:     v.HasCost,
		HasAffinity: v.HasAffinity,
		Metric:      v.Metric,
	}
}

// ComparisonTaskHistory holds the recomputed CI width history and SVG for a single task.
type ComparisonTaskHistory struct {
	TaskID string
	Points []ComparisonHistoryPoint
	SVG    ComparisonHistorySVG
}

// ComparisonHistoryPoint holds a single point in the CI history table beneath the chart.
type ComparisonHistoryPoint struct {
	N              int
	DeltaFormatted string
	CIFormatted    string
	CIWidth        float64
	Decision       string
	DecisionChip   string
}

// ComparisonHistorySVG holds precomputed numeric coordinates for inline SVG rendering.
type ComparisonHistorySVG struct {
	ViewBox     string
	X0          float64
	Y0          float64
	X1          float64
	Y1          float64
	MarginY     float64
	MarginLabel string
	PointsAttr  string
	Circles     []SVGCircle
	FinalLabel  SVGFinalLabel
	XAxisTicks  []SVGTick
	YAxisTicks  []SVGTick
}

// SVGCircle represents a data point circle on the SVG chart.
type SVGCircle struct {
	CX   float64
	CY   float64
	R    float64
	Fill string
}

// SVGFinalLabel represents the label positioned at the final point.
type SVGFinalLabel struct {
	X    float64
	Y    float64
	Text string
}

// SVGTick represents an axis tick with a numeric coordinate and label.
type SVGTick struct {
	X     float64
	Y     float64
	Y1    float64
	Y2    float64
	TextY float64
	Text  string
}

// DecisionChipClass returns the CSS chip class for a stats decision label string.
func DecisionChipClass(label string) string {
	switch label {
	case "improved":
		return "good"
	case "regressed":
		return "bad"
	case "unchanged":
		return "neutral"
	case "inconclusive":
		return "warn"
	default:
		return "pending"
	}
}

// BuildComparisonHistorySVG generates server-side numeric SVG coordinates for CI width vs n.
func BuildComparisonHistorySVG(points []ComparisonHistoryPoint, margin float64) ComparisonHistorySVG {
	const (
		viewBox = "0 0 640 220"
		x0      = 60.0
		x1      = 560.0
		y0      = 25.0
		y1      = 175.0
	)

	if len(points) == 0 {
		return ComparisonHistorySVG{ViewBox: viewBox, X0: x0, Y0: y0, X1: x1, Y1: y1}
	}

	nMax := points[len(points)-1].N
	if nMax <= 0 {
		nMax = 1
	}

	maxCIWidth := margin
	for _, pt := range points {
		if pt.CIWidth > maxCIWidth {
			maxCIWidth = pt.CIWidth
		}
	}

	yMax := maxCIWidth * 1.25
	if yMax < 0.20 {
		yMax = 0.20
	}

	marginY := y1 - float64((margin/yMax)*(y1-y0))
	marginLabel := fmt.Sprintf("margin ±%.2f", margin)

	var pointsAttrBuilder strings.Builder
	circles := make([]SVGCircle, len(points))

	for i, pt := range points {
		cx := x0 + (float64(pt.N)/float64(nMax))*(x1-x0)
		cy := y1 - float64((pt.CIWidth/yMax)*(y1-y0))
		cx = math.Round(cx*10.0) / 10.0
		cy = math.Round(cy*10.0) / 10.0
		circles[i] = SVGCircle{CX: cx, CY: cy, R: 4.0}

		if i > 0 {
			pointsAttrBuilder.WriteByte(' ')
		}
		pointsAttrBuilder.WriteString(fmt.Sprintf("%.1f,%.1f", cx, cy))
	}

	lastPt := points[len(points)-1]
	lastCircle := circles[len(circles)-1]
	finalLabelY := lastCircle.CY - 10.0
	if finalLabelY < y0+10.0 {
		finalLabelY = lastCircle.CY + 15.0
	}
	finalLabelX := lastCircle.CX
	if finalLabelX > x1-60.0 {
		finalLabelX = x1 - 60.0
	}
	finalLabel := SVGFinalLabel{
		X:    finalLabelX,
		Y:    finalLabelY,
		Text: fmt.Sprintf("n=%d (%.2f)", lastPt.N, lastPt.CIWidth),
	}

	var xTicks []SVGTick
	xTicks = append(xTicks, SVGTick{X: x0, Y1: y1, Y2: y1 + 4.0, TextY: y1 + 18.0, Text: "0"})
	if nMax >= 8 {
		midN := nMax / 2
		midX := x0 + (float64(midN)/float64(nMax))*(x1-x0)
		xTicks = append(xTicks, SVGTick{X: midX, Y1: y1, Y2: y1 + 4.0, TextY: y1 + 18.0, Text: fmt.Sprintf("%d", midN)})
	}
	xTicks = append(xTicks, SVGTick{X: x1, Y1: y1, Y2: y1 + 4.0, TextY: y1 + 18.0, Text: fmt.Sprintf("%d", nMax)})

	yTicks := []SVGTick{
		{X: x0 - 8.0, Y: y1 + 4.0, Text: "0.00"},
		{X: x0 - 8.0, Y: marginY + 4.0, Text: fmt.Sprintf("%.2f", margin)},
		{X: x0 - 8.0, Y: y0 + 4.0, Text: fmt.Sprintf("%.2f", yMax)},
	}

	return ComparisonHistorySVG{
		ViewBox:     viewBox,
		X0:          x0,
		Y0:          y0,
		X1:          x1,
		Y1:          y1,
		MarginY:     marginY,
		MarginLabel: marginLabel,
		PointsAttr:  pointsAttrBuilder.String(),
		Circles:     circles,
		FinalLabel:  finalLabel,
		XAxisTicks:  xTicks,
		YAxisTicks:  yTicks,
	}
}

// CleanRepo normalizes a repository clone URL or path to "owner/repo".
func CleanRepo(repoOrURL string) string {
	s := strings.TrimSpace(repoOrURL)
	s = strings.TrimPrefix(s, "https://github.com/")
	s = strings.TrimPrefix(s, "http://github.com/")
	s = strings.TrimPrefix(s, "git@github.com:")
	s = strings.TrimPrefix(s, "git://github.com/")
	s = strings.TrimSuffix(s, ".git")
	return strings.Trim(s, "/")
}

// SeriesListRow represents a repository and branch entry in the /series list view.
type SeriesListRow struct {
	Repository     string
	RepoHash       string
	Branch         string
	RunCount       int
	Link           string
	LastMove       *stats.DeltaEstimate
	LastMoveStr    string
	LastMoveChip   string
	LastMoveMethod string
}

// SeriesListView is the view model for GET /series.
type SeriesListView struct {
	ActiveTab     NavTab
	Namespace     string
	LinkNamespace string
	Cohort        string
	Series        []SeriesListRow
	TotalCount    int
}

// SeriesTaskLink represents a task link in the task selector.
type SeriesTaskLink struct {
	ID     string
	Link   string
	Active bool
}

// SeriesItem holds the data for one commit run in the series view.
type SeriesItem struct {
	CommitSHA      string
	CommitSHAShort string
	CommitTime     time.Time
	CommitTimeStr  string
	RunName        string
	Namespace      string
	Phase          v1alpha1.TaskPhase
	PhaseChip      string
	PhaseLabel     string
	Samples        string
	RateStr        string
	CIStr          string
	TokensStr      string
	CostStr        string
	CostSource     v1alpha1.CostSource
	CostTooltip    string
	RunLink        string

	HasPoint     bool
	SuccessRate  *float64
	CI           *v1alpha1.ConfidenceInterval
	SD           *float64
	SamplesCount int

	DeltaPrev       *stats.DeltaEstimate
	DeltaPrevStr    string
	DeltaPrevChip   string
	DeltaPrevMethod string

	ObsCount int // Number of observations represented at this commit
}

// Estimate converts the SeriesItem point and CI into a stats.Estimate.
func (it SeriesItem) Estimate() stats.Estimate {
	if it.SuccessRate == nil {
		return stats.Estimate{}
	}
	est := stats.Estimate{
		Point: *it.SuccessRate,
		N:     it.SamplesCount,
		SD:    it.SD,
	}
	if it.CI != nil {
		est.CI = stats.Interval{Lower: it.CI.Lower, Upper: it.CI.Upper}
		est.Confidence = it.CI.Confidence
		est.Method = it.CI.Method
	}
	return est
}

// SeriesDetailView is the view model for GET /series/{repohash}/{branch} and /{host}/{owner}/{repo}/series/{branch}.
type SeriesDetailView struct {
	ActiveTab             NavTab
	Repo                  RepoRef
	Namespace             string
	LinkNamespace         string
	Repository            string
	RepoHash              string
	Branch                string
	SelectedTask          string
	Tasks                 []SeriesTaskLink
	N                     int
	Cohort                string
	OrderedByCreationTime bool
	OrderedNote           string
	SVG                   SeriesSVG
	Items                 []SeriesItem
	TotalRuns             int
	BackLink              string
	IsCohortsView         bool
	CohortsLink           string
	CohortLegend          []CohortLegendItem
	CohortRows            []CohortSummaryRow
	Version               string
}

// CohortLegendItem represents a single entry in the cohorts chart legend.
type CohortLegendItem struct {
	Name  string
	Color string
	Count int
}

// CohortSummaryRow represents a row in the cohort summary table on the series detail page.
type CohortSummaryRow struct {
	Cohort          string
	Color           string
	EvalsCount      int
	FirstSeenCommit string
	LatestMean      string
	CIStr           string
	Method          string
	MethodTooltip   string
}

// SVGAnnotation represents a vertical annotation line with a text label on the SVG chart.
type SVGAnnotation struct {
	X          float64
	Y1         float64
	Y2         float64
	TextX      float64
	TextY      float64
	Text       string
	Stroke     string
	TextAnchor string
}

// SVGPolyline represents a connected line of points with a stroke color.
type SVGPolyline struct {
	Points string
	Stroke string
}

// SeriesSVG holds precomputed numeric coordinates for inline SVG rendering of a task series.
type SeriesSVG struct {
	ViewBox      string
	X0           float64
	Y0           float64
	X1           float64
	Y1           float64
	Polylines    []SVGPolyline   // list of polyline segments with stroke styling
	Polygons     []string        // list of "x0,yu0 ... xk,yuk xk,ylk ... x0,yl0" for shaded CI band
	PolygonFills []string        // parallel fill colors for Polygons (defaults to "var(--indigo)" when empty)
	Circles      []SVGCircle     // data points
	Annotations  []SVGAnnotation // vertical annotation lines with text
	XAxisTicks   []SVGTick       // ticks for commits along X-axis
	YAxisTicks   []SVGTick       // ticks for success rate along Y-axis
}

// SVGObservationTick represents an observation tick mark on a sparkline.
type SVGObservationTick struct {
	X1     float64
	Y1     float64
	X2     float64
	Y2     float64
	Stroke string
}

// SparklineSVG holds viewbox and element coordinates for a compact sparkline.
type SparklineSVG struct {
	ViewBox   string
	Polylines []SVGPolyline
	Polygons  []string
	Circles   []SVGCircle
	Ticks     []SVGObservationTick
	HasPoints bool
}

// seriesXTicks generates SVGTick entries for commit SHAs along the X-axis.
func seriesXTicks(commitSHAs []string) []SVGTick {
	const (
		x0 = 60.0
		x1 = 580.0
		y1 = 175.0
	)
	N := len(commitSHAs)
	if N == 0 {
		return nil
	}
	var xTicks []SVGTick
	if N <= 8 {
		for i, sha := range commitSHAs {
			cx := x0
			if N > 1 {
				cx = x0 + (float64(i)/float64(N-1))*(x1-x0)
			} else {
				cx = (x0 + x1) / 2.0
			}
			cx = math.Round(cx*10.0) / 10.0
			xTicks = append(xTicks, SVGTick{X: cx, Y1: y1, Y2: y1 + 4.0, TextY: y1 + 18.0, Text: ShortCommit(sha)})
		}
	} else {
		cx0 := math.Round(x0*10.0) / 10.0
		cxMid := math.Round((x0+(float64(N/2)/float64(N-1))*(x1-x0))*10.0) / 10.0
		cxLast := math.Round(x1*10.0) / 10.0
		xTicks = append(xTicks, SVGTick{X: cx0, Y1: y1, Y2: y1 + 4.0, TextY: y1 + 18.0, Text: ShortCommit(commitSHAs[0])})
		xTicks = append(xTicks, SVGTick{X: cxMid, Y1: y1, Y2: y1 + 4.0, TextY: y1 + 18.0, Text: ShortCommit(commitSHAs[N/2])})
		xTicks = append(xTicks, SVGTick{X: cxLast, Y1: y1, Y2: y1 + 4.0, TextY: y1 + 18.0, Text: ShortCommit(commitSHAs[N-1])})
	}
	return xTicks
}

// BuildSeriesSVG generates server-side numeric SVG coordinates for success rate vs commit series.
func BuildSeriesSVG(items []SeriesItem) SeriesSVG {
	const (
		viewBox = "0 0 640 220"
		x0      = 60.0
		x1      = 580.0
		y0      = 25.0
		y1      = 175.0
	)

	yTicks := []SVGTick{
		{X: x0 - 8.0, Y: y1 + 4.0, Y1: y1, Text: "0.00"},
		{X: x0 - 8.0, Y: (y0+y1)/2.0 + 4.0, Y1: (y0 + y1) / 2.0, Text: "0.50"},
		{X: x0 - 8.0, Y: y0 + 4.0, Y1: y0, Text: "1.00"},
	}

	if len(items) == 0 {
		return SeriesSVG{
			ViewBox:    viewBox,
			X0:         x0,
			Y0:         y0,
			X1:         x1,
			Y1:         y1,
			YAxisTicks: yTicks,
		}
	}

	commitSHAs := make([]string, len(items))
	for i, it := range items {
		commitSHAs[i] = it.CommitSHA
	}
	xTicks := seriesXTicks(commitSHAs)

	lines := []ChartLine{{Items: items}}
	opts := ChartGeometryOptions{
		X0:      x0,
		X1:      x1,
		Y0:      y0,
		Y1:      y1,
		CircleR: 4.0,
	}
	res := computeChartGeometry(lines, nil, len(items), opts)

	return SeriesSVG{
		ViewBox:    viewBox,
		X0:         x0,
		Y0:         y0,
		X1:         x1,
		Y1:         y1,
		Polylines:  res.Polylines,
		Polygons:   res.Polygons,
		Circles:    res.Circles,
		XAxisTicks: xTicks,
		YAxisTicks: yTicks,
	}
}

// BuildSparklineSVG generates a compact SVG sparkline over the last <= 20 points.
func BuildSparklineSVG(items []SeriesItem) SparklineSVG {
	return BuildSparklineSVGWithOptions(items, 120, 28)
}

// BuildSparklineSVGWithOptions generates a compact SVG sparkline over the last <= 20 points
// with customizable width and height.
func BuildSparklineSVGWithOptions(items []SeriesItem, width, height int) SparklineSVG {
	if width <= 0 {
		width = 120
	}
	if height <= 0 {
		height = 28
	}
	viewBox := fmt.Sprintf("0 0 %d %d", width, height)

	x0 := 4.0
	x1 := float64(width) - 4.0
	if x1 <= x0 {
		x1 = x0 + 1.0
	}
	y0 := 4.0
	y1 := float64(height) - 4.0
	if y1 <= y0 {
		y1 = y0 + 1.0
	}
	circleR := 2.0

	if len(items) > 20 {
		items = items[len(items)-20:]
	}

	if len(items) == 0 {
		return SparklineSVG{
			ViewBox: viewBox,
		}
	}

	lines := []ChartLine{{Items: items}}
	ticksAtCommit := make([][]ChartTick, len(items))
	for i, it := range items {
		if it.HasPoint && it.SuccessRate != nil {
			count := it.ObsCount
			if count <= 0 {
				count = 1
			}
			for k := 0; k < count; k++ {
				ticksAtCommit[i] = append(ticksAtCommit[i], ChartTick{
					Rate:   *it.SuccessRate,
					Stroke: "var(--indigo)",
				})
			}
		}
	}

	tickHalfHeight := math.Min(3.5, (y1-y0)/2.0)
	tickStep := math.Min(2.5, (x1-x0)/float64(math.Max(1, float64(len(items)))))

	opts := ChartGeometryOptions{
		X0:             x0,
		X1:             x1,
		Y0:             y0,
		Y1:             y1,
		CircleR:        circleR,
		TickHalfHeight: tickHalfHeight,
		TickStep:       tickStep,
	}

	res := computeChartGeometry(lines, ticksAtCommit, len(items), opts)
	return SparklineSVG{
		ViewBox:   viewBox,
		Polylines: res.Polylines,
		Polygons:  res.Polygons,
		Circles:   res.Circles,
		Ticks:     res.Ticks,
		HasPoints: res.HasPoints,
	}
}

// FormatMethodTooltip returns a human-readable tooltip describing the estimation method.
func FormatMethodTooltip(method string) string {
	switch method {
	case "newcombe":
		return "Newcombe hybrid Wilson interval (unpaired; different sample windows)"
	case "welch":
		return "Welch t interval from (mean, sd, n) (unpaired)"
	case "pooled-wilson", "pooled_wilson":
		return "Pooled Wilson score interval over summed successes and samples across cohort tasks"
	default:
		return method
	}
}

// CohortPoint represents a data point for a cohort at a specific commit.
type CohortPoint struct {
	HasPoint bool
	Rate     *float64
	CI       *stats.Interval
}

// CohortSeriesLine represents a line in the cohorts series chart.
type CohortSeriesLine struct {
	Name            string
	Color           string
	Points          []CohortPoint // one per commit
	FirstSeenCommit int           // index of commit where this cohort first appears (if added mid-series)
	EvalsCount      int
	IsAll           bool
}

// BuildCohortSeriesSVG generates SVG coordinates for multi-cohort series charts.
func BuildCohortSeriesSVG(commitSHAs []string, lines []CohortSeriesLine) SeriesSVG {
	const (
		viewBox = "0 0 640 220"
		x0      = 60.0
		x1      = 580.0
		y0      = 25.0
		y1      = 175.0
	)

	yTicks := []SVGTick{
		{X: x0 - 8.0, Y: y1 + 4.0, Y1: y1, Text: "0.00"},
		{X: x0 - 8.0, Y: (y0+y1)/2.0 + 4.0, Y1: (y0 + y1) / 2.0, Text: "0.50"},
		{X: x0 - 8.0, Y: y0 + 4.0, Y1: y0, Text: "1.00"},
	}

	N := len(commitSHAs)
	if N == 0 {
		return SeriesSVG{
			ViewBox:    viewBox,
			X0:         x0,
			Y0:         y0,
			X1:         x1,
			Y1:         y1,
			YAxisTicks: yTicks,
		}
	}

	xTicks := seriesXTicks(commitSHAs)

	var polylines []SVGPolyline
	var polygons []string
	var polygonFills []string
	var circles []SVGCircle
	var annotations []SVGAnnotation

	type pt2D struct {
		x float64
		y float64
	}
	type ci2D struct {
		x      float64
		yUpper float64
		yLower float64
	}

	for _, line := range lines {
		var curLine []pt2D
		var curCI []ci2D

		flushLine := func() {
			if len(curLine) >= 2 {
				var sb strings.Builder
				for j, p := range curLine {
					if j > 0 {
						sb.WriteByte(' ')
					}
					sb.WriteString(fmt.Sprintf("%.1f,%.1f", p.x, p.y))
				}
				polylines = append(polylines, SVGPolyline{
					Points: sb.String(),
					Stroke: line.Color,
				})
			}
			curLine = nil

			if len(curCI) >= 2 {
				var sb strings.Builder
				for j, p := range curCI {
					if j > 0 {
						sb.WriteByte(' ')
					}
					sb.WriteString(fmt.Sprintf("%.1f,%.1f", p.x, p.yUpper))
				}
				for j := len(curCI) - 1; j >= 0; j-- {
					sb.WriteByte(' ')
					sb.WriteString(fmt.Sprintf("%.1f,%.1f", curCI[j].x, curCI[j].yLower))
				}
				polygons = append(polygons, sb.String())
				polygonFills = append(polygonFills, line.Color)
			} else if len(curCI) == 1 {
				p := curCI[0]
				poly := fmt.Sprintf("%.1f,%.1f %.1f,%.1f %.1f,%.1f %.1f,%.1f",
					p.x-2.0, p.yUpper,
					p.x+2.0, p.yUpper,
					p.x+2.0, p.yLower,
					p.x-2.0, p.yLower,
				)
				polygons = append(polygons, poly)
				polygonFills = append(polygonFills, line.Color)
			}
			curCI = nil
		}

		for i, pt := range line.Points {
			if i >= N {
				break
			}
			if !pt.HasPoint || pt.Rate == nil {
				flushLine()
				continue
			}

			cx := x0
			if N > 1 {
				cx = x0 + (float64(i)/float64(N-1))*(x1-x0)
			} else {
				cx = (x0 + x1) / 2.0
			}

			rate := *pt.Rate
			if rate < 0.0 {
				rate = 0.0
			} else if rate > 1.0 {
				rate = 1.0
			}
			cy := y1 - rate*(y1-y0)
			cx = math.Round(cx*10.0) / 10.0
			cy = math.Round(cy*10.0) / 10.0

			circles = append(circles, SVGCircle{
				CX:   cx,
				CY:   cy,
				R:    4.0,
				Fill: line.Color,
			})

			curLine = append(curLine, pt2D{x: cx, y: cy})

			if pt.CI != nil {
				lo := pt.CI.Lower
				if lo < 0.0 {
					lo = 0.0
				}
				hi := pt.CI.Upper
				if hi > 1.0 {
					hi = 1.0
				}
				yUpper := y1 - hi*(y1-y0)
				yLower := y1 - lo*(y1-y0)
				yUpper = math.Round(yUpper*10.0) / 10.0
				yLower = math.Round(yLower*10.0) / 10.0
				curCI = append(curCI, ci2D{
					x:      cx,
					yUpper: yUpper,
					yLower: yLower,
				})
			} else {
				curCI = append(curCI, ci2D{
					x:      cx,
					yUpper: cy,
					yLower: cy,
				})
			}
		}
		flushLine()

		// If this is a cohort added mid-series (not "default" and not "all"), place a vertical annotation.
		if !line.IsAll && line.Name != v1alpha1.DefaultCohort && line.FirstSeenCommit > 0 && line.FirstSeenCommit < N {
			i := line.FirstSeenCommit
			cx := x0
			if N > 1 {
				cx = x0 + (float64(i)/float64(N-1))*(x1-x0)
			} else {
				cx = (x0 + x1) / 2.0
			}
			cx = math.Round(cx*10.0) / 10.0
			text := formatEvalsAdded(line.EvalsCount)
			textAnchor := "start"
			textX := cx + 6.0
			if cx > x1-80.0 {
				textAnchor = "end"
				textX = cx - 6.0
			}
			textX = math.Round(textX*10.0) / 10.0
			textY := y0 + 12.0 + 11.0*float64(len(annotations))
			textY = math.Round(textY*10.0) / 10.0
			annotations = append(annotations, SVGAnnotation{
				X:          cx,
				Y1:         y0,
				Y2:         y1,
				TextX:      textX,
				TextY:      textY,
				Text:       text,
				Stroke:     line.Color,
				TextAnchor: textAnchor,
			})
		}
	}

	return SeriesSVG{
		ViewBox:      viewBox,
		X0:           x0,
		Y0:           y0,
		X1:           x1,
		Y1:           y1,
		Polylines:    polylines,
		Polygons:     polygons,
		PolygonFills: polygonFills,
		Circles:      circles,
		Annotations:  annotations,
		XAxisTicks:   xTicks,
		YAxisTicks:   yTicks,
	}
}

func formatEvalsAdded(count int) string {
	if count == 1 {
		return "1 eval added"
	}
	return fmt.Sprintf("%d evals added", count)
}

// FormatDeltaCI formats a delta point estimate and confidence interval,
// e.g. "Δ −0.05 [−0.21, +0.11]".
func FormatDeltaCI(delta, lower, upper float64) string {
	dStr, _ := signedFixed(delta, 2)
	lStr, _ := signedFixed(lower, 2)
	uStr, _ := signedFixed(upper, 2)
	return fmt.Sprintf("Δ %s [%s, %s]", dStr, lStr, uStr)
}

// DeltaChipClass returns "good", "bad", or "neutral" based on whether
// the confidence interval excludes zero.
func DeltaChipClass(ci stats.Interval) string {
	if ci.Lower > 0 {
		return "good"
	}
	if ci.Upper < 0 {
		return "bad"
	}
	return "neutral"
}

// NewComparisonDetailView constructs a ComparisonDetailView from a Comparison and optional Runs.
func NewComparisonDetailView(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run, ref RepoRef, linkNS string) ComparisonDetailView {
	if comp == nil {
		return ComparisonDetailView{}
	}
	summary := ComparisonSummary(comp, baseRun, candRun)
	taskRows := ComparisonRowsWithRuns(comp, baseRun, candRun, ref, linkNS)

	hasTokens := false
	hasLatency := false
	hasCost := false
	hasAffinity := false
	var totalTokens int
	var totalReasoning int
	for _, tr := range taskRows {
		if tr.Tokens != nil {
			hasTokens = true
			totalTokens += tr.Tokens.Total
			totalReasoning += tr.Tokens.Reasoning
		}
		if tr.CandLatencyStr != "" || tr.BaseLatencyStr != "" || tr.LatencyStr != "" {
			hasLatency = true
		}
		if tr.CostStr != "" || tr.CandCostStr != "" || tr.BaseCostStr != "" {
			hasCost = true
		}
		if tr.Affinity != "" {
			hasAffinity = true
		}
	}

	var totalCostUSD float64
	if summary.CostUSD != nil {
		totalCostUSD = *summary.CostUSD
		hasCost = true
	} else if candRun != nil && candRun.Status.CostUSD != nil {
		totalCostUSD = *candRun.Status.CostUSD
		hasCost = true
	}

	var totalCostFormatted string
	if hasCost && totalCostUSD > 0 {
		totalCostFormatted = fmt.Sprintf("$%.4f", totalCostUSD)
	}

	var budgetFormatted string
	var budgetNote string
	if comp.Status.Budget != nil && comp.Status.Budget.LimitUSD != nil && *comp.Status.Budget.LimitUSD > 0 {
		spent := 0.0
		if comp.Status.Budget.SpentUSD != nil {
			spent = *comp.Status.Budget.SpentUSD
		} else if totalCostUSD > 0 {
			spent = totalCostUSD
		}
		budgetFormatted = fmt.Sprintf("$%.2f / $%.2f", spent, *comp.Status.Budget.LimitUSD)
		if !comp.Status.Budget.Enforced {
			budgetFormatted += " (not enforced)"
		}
		budgetNote = comp.Status.Budget.Note
	} else if comp.Spec.BudgetUSD != nil && *comp.Spec.BudgetUSD > 0 {
		spent := 0.0
		if totalCostUSD > 0 {
			spent = totalCostUSD
		}
		budgetFormatted = fmt.Sprintf("$%.2f / $%.2f", spent, *comp.Spec.BudgetUSD)
	}

	var totalTokensFormatted string
	if hasTokens {
		if totalReasoning > 0 {
			totalTokensFormatted = fmt.Sprintf("%d (+%d R)", totalTokens, totalReasoning)
		} else {
			totalTokensFormatted = fmt.Sprintf("%d", totalTokens)
		}
	}

	affinityMatched := 0
	if comp.Status.Summary != nil {
		affinityMatched = comp.Status.Summary.AffinityMatched
	}

	return ComparisonDetailView{
		ActiveTab:        NavTabComparisons,
		Repo:             ref,
		Name:             comp.Name,
		Namespace:        comp.Namespace,
		LinkNamespace:    linkNS,
		Verdict:          comp.Status.Verdict,
		VerdictChip:      verdictChipClass(comp.Status.Verdict),
		Headline:         summary.Headline,
		PImprovement:     summary.PImprovement,
		SummaryPath:      comp.Status.SummaryPath,
		Message:          comp.Status.Message,
		Metric:           MetricFilterAll,
		Tasks:            taskRows,
		HasTokens:        hasTokens,
		HasLatency:       hasLatency,
		HasCost:          hasCost,
		HasAffinity:      hasAffinity,
		AffinityMatched:  affinityMatched,
		TotalCostStr:     totalCostFormatted,
		BudgetStr:        budgetFormatted,
		BudgetNote:       budgetNote,
		TotalTokensStr:   totalTokensFormatted,
		Batches:          ComparisonBatches(comp),
		Actions:          ComparisonActions(comp),
		Deltas:           ComputeRunDeltas(comp, baseRun, candRun),
		RegressedTasks:   RegressedTaskRows(comp),
		EvaluatedTasks:   evaluatedTaskCount(comp),
		Summary:          summary,
		BaseRun:          summary.BaseRun,
		BaseCommit:       summary.BaseCommit,
		BaseCommitShort:  summary.BaseCommitShort,
		CandRun:          summary.CandRun,
		CandCommit:       summary.CandCommit,
		CandCommitShort:  summary.CandCommitShort,
		TotalSamples:     summary.TotalSamples,
		TotalPairs:       summary.TotalPairs,
		TotalDroppedCand: summary.TotalDroppedCand,
		TotalDroppedBase: summary.TotalDroppedBase,
		CostUSD:          summary.CostUSD,
		PossibleSamples:  summary.PossibleSamples,
		SamplesSummary:   summary.SamplesSummary,
	}
}

// Helpers for CSS status classes

func comparisonPhaseChipClass(phase v1alpha1.ComparisonPhase) string {
	switch phase {
	case v1alpha1.ComparisonPhaseCompleted:
		return "good"
	case v1alpha1.ComparisonPhaseRunning:
		return "pending"
	default:
		return "neutral"
	}
}

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
	case v1alpha1.ComparisonTaskLabelSkipped:
		return "neutral"
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

// TaskSampleRow represents an individual sample row in the task samples list.
type TaskSampleRow struct {
	ID            string
	EscapedID     string
	Score         string
	Status        string // "pass", "fail", "unknown"
	StatusClass   string // "good", "bad", "neutral"
	InputPreview  string
	OutputPreview string
	// Target is the sample target ("Pass rule"); TargetPreview is its truncated form.
	Target        string
	TargetPreview string
	// Explanation is the scorer's explanation ("Grader reason"); ExplanationPreview is its truncated form.
	Explanation        string
	ExplanationPreview string
	LatencyStr         string
	TokensStr          string
	Link               string
}

// sampleNotePreviewRunes caps the pass rule and grader reason shown per row on the task
// samples list; tooltips are capped by maxSampleTooltipBytes and the full text (capped
// by maxTranscriptFieldBytes) is on the transcript page.
const sampleNotePreviewRunes = 140

// TaskSamplesListView is the view model for GET /runs/{ns}/{name}/tasks/{task}/samples.
type TaskSamplesListView struct {
	ActiveTab        NavTab
	Repo             RepoRef
	Namespace        string
	LinkNamespace    string
	RunName          string
	TaskID           string
	EscapedTaskID    string
	Commit           string
	Model            string
	LogPath          string
	Samples          []TaskSampleRow
	TotalSamples     int
	Page             int
	PageSize         int
	TotalPages       int
	ShowingStart     int
	ShowingEnd       int
	HasPrev          bool
	PrevLink         string
	HasNext          bool
	NextLink         string
	IsTruncatedLog   bool
	TruncatedMessage string
	WarningMessage   string
	WarningLogPath   string
	WarningLogURL    string
	StorageMessage   string
	ArtifactFooter   string
	Version          string
}

// TranscriptRowView represents a single rendered transcript entry row.
type TranscriptRowView struct {
	Kind    string // "user", "assistant", "tool_call", "tool", "system"
	Class   string // "", "tool", "bad"
	Content string
}

// SampleTranscriptView is the view model for GET /runs/{ns}/{name}/tasks/{task}/samples/{sample}.
type SampleTranscriptView struct {
	ActiveTab              NavTab
	Repo                   RepoRef
	Namespace              string
	LinkNamespace          string
	RunName                string
	TaskID                 string
	EscapedTaskID          string
	SampleID               string
	EscapedSampleID        string
	Score                  string
	Status                 string // "pass", "fail", "unknown"
	StatusUpper            string // "PASS", "FAIL", "UNKNOWN"
	StatusClass            string // "good", "bad", "neutral"
	LatencyStr             string
	TokensStr              string
	LogPath                string
	Target                 string // sample target, shown in full as "Pass rule"
	Explanation            string // scorer explanation, shown in full as "Grader reason"
	Rows                   []TranscriptRowView
	TotalEvents            int
	IsEventsTruncated      bool
	EventsTruncatedMessage string
	IsTruncatedLog         bool
	TruncatedMessage       string
	StorageMessage         string
	ArtifactFooter         string
	Version                string
}

// TruncateRunes truncates string s to maxRunes, appending "..." if truncated.
func TruncateRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "..."
}

// TruncateMessageBytes truncates string s to maxBytes on a valid UTF-8 rune boundary,
// appending a truncation marker if truncated.
func TruncateMessageBytes(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	if maxBytes <= 0 {
		return "... [content truncated]"
	}
	for maxBytes > 0 && !utf8.RuneStart(s[maxBytes]) {
		maxBytes--
	}
	return s[:maxBytes] + "\n... [content truncated]"
}

// TruncateFieldBytes truncates string s to maxBytes on a valid UTF-8 rune boundary,
// avoiding partial HTML entities, and appending a note showing total bytes if truncated.
func TruncateFieldBytes(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	totalBytes := len(s)
	if maxBytes <= 0 {
		return fmt.Sprintf("... [truncated, %d bytes total]", totalBytes)
	}

	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}

	cut = trimPartialHTMLEntity(s, cut)

	prefix := strings.TrimRight(s[:cut], "\r\n")
	if prefix == "" {
		return fmt.Sprintf("... [truncated, %d bytes total]", totalBytes)
	}
	return prefix + fmt.Sprintf("\n... [truncated, %d bytes total]", totalBytes)
}

func trimPartialHTMLEntity(s string, cut int) int {
	if cut <= 0 {
		return cut
	}
	if s[cut-1] == '&' {
		return cut - 1
	}

	start := cut - 35
	if start < 0 {
		start = 0
	}
	window := s[start:cut]
	ampOffset := strings.LastIndexByte(window, '&')
	if ampOffset == -1 {
		return cut
	}
	ampIdx := start + ampOffset
	if strings.IndexByte(s[ampIdx:cut], ';') != -1 {
		return cut
	}

	maxEntityLen := 35
	end := ampIdx + maxEntityLen
	if end > len(s) {
		end = len(s)
	}
	afterAmp := s[ampIdx:end]
	semiIdx := strings.IndexByte(afterAmp, ';')
	if semiIdx == -1 {
		return cut
	}

	entityBody := afterAmp[1:semiIdx]
	if len(entityBody) == 0 {
		return cut
	}
	for i := 0; i < len(entityBody); i++ {
		c := entityBody[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || (c == '#' && i == 0)) {
			return cut
		}
	}

	return ampIdx
}

// FormatTokens formats an integer token count with commas, e.g. 41200 -> "41,200", or "—" if <= 0.
func FormatTokens(tokens int) string {
	if tokens <= 0 {
		return "—"
	}
	in := strconv.Itoa(tokens)
	if len(in) <= 3 {
		return in
	}
	var out []byte
	rem := len(in) % 3
	if rem > 0 {
		out = append(out, in[:rem]...)
		if len(in) > rem {
			out = append(out, ',')
		}
	}
	for i := rem; i < len(in); i += 3 {
		out = append(out, in[i:i+3]...)
		if i+3 < len(in) {
			out = append(out, ',')
		}
	}
	return string(out)
}

// FormatLatency formats a float pointer latency in seconds, e.g. 3.0 -> "3.0s", or "—" if nil.
func FormatLatency(sec *float64) string {
	if sec == nil {
		return "—"
	}
	return fmt.Sprintf("%.1fs", *sec)
}

// BuildTaskSampleLink builds a path-escaped URL to a sample transcript page.
func BuildTaskSampleLink(ns, runName, taskID, sampleID string) string {
	return fmt.Sprintf("/runs/%s/%s/tasks/%s/samples/%s",
		ns,
		runName,
		url.PathEscape(taskID),
		url.PathEscape(sampleID),
	)
}

// BuildTaskSamplesListLink builds a path-escaped URL to a task samples list page.
func BuildTaskSamplesListLink(ns, runName, taskID string) string {
	return fmt.Sprintf("/runs/%s/%s/tasks/%s/samples",
		ns,
		runName,
		url.PathEscape(taskID),
	)
}

// CommitEntry represents a commit SHA and its timestamp.
type CommitEntry struct {
	SHA        string
	CommitTime time.Time
}

// EvalObservation represents a completed task observation from a single Run.
type EvalObservation struct {
	RunName     string
	Run         *v1alpha1.Run
	TaskID      string
	CommitSHA   string
	CommitTime  time.Time
	Model       string
	Harness     string
	Samples     int
	Successes   int
	SuccessRate *float64
	ScoreStr    string
	CIStr       string
	Est         stats.Estimate
	SamplesLink string
}

// TrendDimension specifies the dimension used to group trend lines.
// Currently only DimensionModel is live; DimensionHarness is supported by LinesPerDimension
// and will become active once evalrunner.gke-labs.dev/harness is populated on Runs.
type TrendDimension string

const (
	DimensionModel   TrendDimension = "model"
	DimensionHarness TrendDimension = "harness"
)

// DimensionLine represents a single line in a trend chart.
type DimensionLine struct {
	Name         string
	Color        string
	ScoreStr     string
	CIStr        string
	Estimate     stats.Estimate
	HasEstimate  bool
	Items        []SeriesItem
	Observations []EvalObservation
}

// RepoSuiteDetailView is the view model for GET /{host}/{owner}/{repo}/evals/{suite...}.
type RepoSuiteDetailView struct {
	ActiveTab          NavTab
	Repo               RepoRef
	Namespace          string
	LinkNamespace      string
	SuiteName          string
	SuiteScore         string
	SuiteCI            string
	EvalsCount         int
	HasDelta           bool
	DeltaStr           string
	DeltaClass         string
	CurrentCommitSHA   string
	CurrentCommitShort string
	Evals              []RepoSuiteEvalSection
	OverviewLink       string
}

// RepoSuiteEvalSection represents an eval section within the suite detail page.
type RepoSuiteEvalSection struct {
	ID                       string
	ScoreStr                 string
	CIStr                    string
	HasDelta                 bool
	DeltaStr                 string
	DeltaClass               string
	HasModelBreakdown        bool
	ModelLegend              []RepoSuiteModelLegendItem
	SVG                      RepoSuiteChartSVG
	FirstCommitShort         string
	LastCommitShort          string
	NewestChangedCommitShort string
	Observations             []RepoSuiteObservationRow
}

// RepoSuiteModelLegendItem represents an entry in the model breakdown legend.
type RepoSuiteModelLegendItem struct {
	Name     string
	Color    string
	ScoreStr string
	CIStr    string
}

// RepoSuiteObservationRow represents a row in the eval observations table.
type RepoSuiteObservationRow struct {
	CommitSHA   string
	CommitShort string
	TimeStr     string
	TimeUTC     string
	Model       string
	Samples     int
	ScoreStr    string
	CIStr       string
	SamplesLink string
}

// RepoSuiteChartSVG holds SVG element coordinates for the eval trend chart.
type RepoSuiteChartSVG struct {
	ViewBox      string
	X0           float64
	Y0           float64
	X1           float64
	Y1           float64
	Polylines    []SVGPolyline
	Polygons     []string
	PolygonFills []string
	Circles      []SVGCircle
	Ticks        []SVGObservationTick
	XAxisTicks   []SVGTick
	YAxisTicks   []SVGTick
	HasPoints    bool
}

func evalDimensionValue(o EvalObservation, dim TrendDimension) string {
	if dim == DimensionHarness {
		return o.Harness
	}
	return o.Model
}

// LinesPerDimension groups observations by the given dimension.
// If observations come from more than one distinct dimension value (e.g. spec.model),
// it returns one line per dimension value with colors assigned from the palette.
// Otherwise, it returns a single line.
func LinesPerDimension(obs []EvalObservation, dim TrendDimension, commits []CommitEntry) []DimensionLine {
	palette := []string{"var(--indigo)", "var(--teal)", "#d97706", "#7c3aed", "#dc2626"}

	valueMap := make(map[string][]EvalObservation)
	for _, o := range obs {
		val := evalDimensionValue(o, dim)
		if val == "" {
			val = "unknown"
		}
		valueMap[val] = append(valueMap[val], o)
	}

	var keys []string
	for k := range valueMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	isSingle := len(keys) <= 1

	var lines []DimensionLine
	for idx, key := range keys {
		color := "var(--indigo)"
		if !isSingle {
			color = palette[idx%len(palette)]
		}
		keyObs := valueMap[key]

		var newestKeyObs []EvalObservation
		for i := len(commits) - 1; i >= 0; i-- {
			cSHA := commits[i].SHA
			var atCommit []EvalObservation
			for _, o := range keyObs {
				if o.CommitSHA == cSHA {
					atCommit = append(atCommit, o)
				}
			}
			if len(atCommit) > 0 {
				newestKeyObs = atCommit
				break
			}
		}

		var scoreStr, ciStr string
		var est stats.Estimate
		hasEst := false
		if len(newestKeyObs) > 0 {
			var tasks []stats.TaskCounts
			for _, o := range newestKeyObs {
				tasks = append(tasks, stats.TaskCounts{Successes: o.Successes, Samples: o.Samples})
			}
			est = stats.PooledWilson(tasks, 0.95)
			scoreStr = fmt.Sprintf("%.2f", est.Point)
			ciStr = fmt.Sprintf("[%.2f, %.2f]", est.CI.Lower, est.CI.Upper)
			hasEst = true
		}

		var items []SeriesItem
		for _, sc := range commits {
			var matching []EvalObservation
			for _, o := range keyObs {
				if o.CommitSHA == sc.SHA {
					matching = append(matching, o)
				}
			}
			sItem := SeriesItem{
				CommitSHA:      sc.SHA,
				CommitSHAShort: ShortCommit(sc.SHA),
				CommitTime:     sc.CommitTime,
			}
			if len(matching) > 0 {
				var mTasks []stats.TaskCounts
				for _, o := range matching {
					mTasks = append(mTasks, stats.TaskCounts{Successes: o.Successes, Samples: o.Samples})
				}
				cEst := stats.PooledWilson(mTasks, 0.95)
				p := cEst.Point
				ci := v1alpha1.ConfidenceInterval{
					Lower:      cEst.CI.Lower,
					Upper:      cEst.CI.Upper,
					Confidence: cEst.Confidence,
					Method:     cEst.Method,
				}
				sItem.HasPoint = true
				sItem.SuccessRate = &p
				sItem.CI = &ci
				sItem.ObsCount = len(matching)
			}
			items = append(items, sItem)
		}

		lineName := key
		if isSingle {
			lineName = ""
		}

		lines = append(lines, DimensionLine{
			Name:         lineName,
			Color:        color,
			ScoreStr:     scoreStr,
			CIStr:        ciStr,
			Estimate:     est,
			HasEstimate:  hasEst,
			Items:        items,
			Observations: keyObs,
		})
	}
	return lines
}

// BuildRepoSuiteChartSVG generates SVG coordinates for an eval trend chart,
// plotting trendlines, shaded CI bands, data point circles, and observation tick marks.
func BuildRepoSuiteChartSVG(lines []DimensionLine, allObsForEval []EvalObservation, commits []CommitEntry, dim ...TrendDimension) RepoSuiteChartSVG {
	const (
		viewBox = "0 0 640 220"
		x0      = 60.0
		x1      = 580.0
		y0      = 25.0
		y1      = 175.0
	)

	yTicks := []SVGTick{
		{X: x0 - 8.0, Y: y1 + 4.0, Y1: y1, Text: "0.00"},
		{X: x0 - 8.0, Y: (y0+y1)/2.0 + 4.0, Y1: (y0 + y1) / 2.0, Text: "0.50"},
		{X: x0 - 8.0, Y: y0 + 4.0, Y1: y0, Text: "1.00"},
	}

	if len(commits) == 0 {
		return RepoSuiteChartSVG{
			ViewBox:    viewBox,
			X0:         x0,
			Y0:         y0,
			X1:         x1,
			Y1:         y1,
			YAxisTicks: yTicks,
		}
	}

	commitSHAs := make([]string, len(commits))
	for i, c := range commits {
		commitSHAs[i] = c.SHA
	}
	xTicks := seriesXTicks(commitSHAs)

	dimension := DimensionModel
	if len(dim) > 0 {
		dimension = dim[0]
	}

	// Map line colors by dimension value for tick strokes
	lineColorMap := make(map[string]string)
	chartLines := make([]ChartLine, len(lines))
	for i, l := range lines {
		chartLines[i] = ChartLine{
			Color: l.Color,
			Items: l.Items,
		}
		if l.Name != "" {
			lineColorMap[l.Name] = l.Color
		}
	}

	N := len(commits)
	ticksAtCommit := make([][]ChartTick, N)
	for i, c := range commits {
		for _, o := range allObsForEval {
			if o.CommitSHA == c.SHA {
				rate := o.Est.Point
				if o.SuccessRate != nil {
					rate = *o.SuccessRate
				}
				val := evalDimensionValue(o, dimension)
				stroke := "var(--indigo)"
				if len(lines) > 1 && val != "" && lineColorMap[val] != "" {
					stroke = lineColorMap[val]
				} else if len(lines) == 1 && lines[0].Color != "" {
					stroke = lines[0].Color
				}
				ticksAtCommit[i] = append(ticksAtCommit[i], ChartTick{
					Rate:   rate,
					Stroke: stroke,
				})
			}
		}
	}

	opts := ChartGeometryOptions{
		X0:             x0,
		X1:             x1,
		Y0:             y0,
		Y1:             y1,
		CircleR:        4.0,
		TickHalfHeight: 6.0,
		TickStep:       3.5,
	}

	res := computeChartGeometry(chartLines, ticksAtCommit, N, opts)

	return RepoSuiteChartSVG{
		ViewBox:      viewBox,
		X0:           x0,
		Y0:           y0,
		X1:           x1,
		Y1:           y1,
		Polylines:    res.Polylines,
		Polygons:     res.Polygons,
		PolygonFills: res.PolygonFills,
		Circles:      res.Circles,
		Ticks:        res.Ticks,
		XAxisTicks:   xTicks,
		YAxisTicks:   yTicks,
		HasPoints:    res.HasPoints,
	}
}
