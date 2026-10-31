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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/forge"
	"github.com/gke-labs/eval-runner/pkg/pooling"
	"github.com/gke-labs/eval-runner/pkg/stats"
)

// BranchHeadDetailsURL constructs the details URL for a branch head check run.
func BranchHeadDetailsURL(uiBaseURL string, repoRef forge.RepoRef, branch string) string {
	trimmed := strings.TrimRight(uiBaseURL, "/")
	host := repoRef.Host
	if host == "" {
		host = "github.com"
	}
	owner := repoRef.Owner
	repo := repoRef.Name
	if owner == "" || repo == "" {
		return ""
	}
	if trimmed == "" {
		return fmt.Sprintf("/%s/%s/%s/series/%s", host, owner, repo, branch)
	}
	return fmt.Sprintf("%s/%s/%s/%s/series/%s", trimmed, host, owner, repo, branch)
}

// formatApproxDuration formats a duration for display, e.g. "45m", "1h30m".
func formatApproxDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d >= time.Hour {
		hours := int(d.Hours())
		mins := int(d.Minutes()) % 60
		if mins > 0 {
			return fmt.Sprintf("%dh%dm", hours, mins)
		}
		return fmt.Sprintf("%dh", hours)
	}
	if d >= time.Minute {
		mins := int(math.Round(d.Minutes()))
		if mins < 1 {
			mins = 1
		}
		return fmt.Sprintf("%dm", mins)
	}
	secs := int(d.Seconds())
	return fmt.Sprintf("%ds", secs)
}

// branchHeadTaskSampleLink returns the repo-scoped link to the task samples page.
func branchHeadTaskSampleLink(uiBaseURL string, repoRef forge.RepoRef, runName, taskID string) string {
	trimmedUIBaseURL := strings.TrimRight(strings.TrimSpace(uiBaseURL), "/")
	if trimmedUIBaseURL == "" || runName == "" || repoRef.Owner == "" || repoRef.Name == "" {
		return ""
	}
	host := repoRef.Host
	if host == "" {
		host = "github.com"
	}
	return fmt.Sprintf("%s/%s/%s/%s/runs/%s/tasks/%s/samples",
		trimmedUIBaseURL, host, repoRef.Owner, repoRef.Name, runName, url.PathEscape(taskID))
}

// formatTaskRateWithCI formats a pooled task's rate as percent plus CI, matching FormatTitle.
func formatTaskRateWithCI(t pooling.PooledTask) string {
	if t.SuccessRate == nil {
		if t.Phase == v1alpha1.TaskPhaseFailed {
			return "failed"
		}
		return "—"
	}
	pct := int(math.Round(*t.SuccessRate * 100))
	rateStr := fmt.Sprintf("%d%%", pct)
	if t.Estimate.CI.Upper > t.Estimate.CI.Lower || t.Samples > 0 {
		halfWidth := int(math.Round(((t.Estimate.CI.Upper - t.Estimate.CI.Lower) / 2.0) * 100))
		rateStr = fmt.Sprintf("%d%% ± %d", pct, halfWidth)
	}
	if t.Phase == v1alpha1.TaskPhaseFailed {
		return fmt.Sprintf("%s (failed after %d samples)", rateStr, t.Samples)
	}
	return rateStr
}

type branchHeadCol struct {
	header string
	cell   func(t pooling.PooledTask) string
}

func branchHeadColumns(pool *pooling.CommitPool, uiBaseURL string, repoRef forge.RepoRef) []branchHeadCol {
	hasTokens := false
	hasCost := false
	for _, t := range pool.Tasks {
		if t.Tokens != nil && t.Tokens.Total > 0 {
			hasTokens = true
		}
		if t.CostUSD != nil {
			hasCost = true
		}
	}

	cols := []branchHeadCol{
		{
			header: "eval",
			cell: func(t pooling.PooledTask) string {
				escapedID := EscapeCell(t.ID)
				if link := branchHeadTaskSampleLink(uiBaseURL, repoRef, t.RunName, t.ID); link != "" {
					return fmt.Sprintf("[%s](%s)", escapedID, link)
				}
				return escapedID
			},
		},
		{
			header: "samples",
			cell: func(t pooling.PooledTask) string {
				if t.Samples > 0 {
					return fmt.Sprintf("%d", t.Samples)
				}
				return "—"
			},
		},
		{
			header: "successes",
			cell: func(t pooling.PooledTask) string {
				if t.Samples > 0 || t.Phase == v1alpha1.TaskPhaseCompleted {
					return fmt.Sprintf("%d", t.Successes)
				}
				return "—"
			},
		},
		{
			header: "rate (95% CI)",
			cell:   formatTaskRateWithCI,
		},
	}

	if hasTokens {
		cols = append(cols, branchHeadCol{
			header: "tokens",
			cell: func(t pooling.PooledTask) string {
				if t.Tokens != nil && t.Tokens.Total > 0 {
					if t.Tokens.Reasoning > 0 {
						return fmt.Sprintf("%d (+R %d)", t.Tokens.Total, t.Tokens.Reasoning)
					}
					return fmt.Sprintf("%d", t.Tokens.Total)
				}
				return "—"
			},
		})
	}

	if hasCost {
		cols = append(cols, branchHeadCol{
			header: "cost",
			cell: func(t pooling.PooledTask) string {
				if t.CostUSD != nil {
					return fmt.Sprintf("$%.4f", *t.CostUSD)
				}
				return "—"
			},
		})
	}

	return cols
}

// BuildBranchHeadTable constructs the markdown table header and row strings for a branch-head CommitPool:
// columns: eval, samples, successes, rate (95% CI), tokens (if present), cost (if present).
func BuildBranchHeadTable(pool *pooling.CommitPool, uiBaseURL string, repoRef forge.RepoRef) (string, []string) {
	if pool == nil || len(pool.Tasks) == 0 {
		return "", nil
	}

	cols := branchHeadColumns(pool, uiBaseURL, repoRef)

	headers := make([]string, len(cols))
	seps := make([]string, len(cols))
	for j, col := range cols {
		headers[j] = col.header
		seps[j] = "---"
	}
	tableHeader := fmt.Sprintf("| %s |\n| %s |\n", strings.Join(headers, " | "), strings.Join(seps, " | "))

	rowStrings := make([]string, len(pool.Tasks))
	for i, t := range pool.Tasks {
		cells := make([]string, len(cols))
		for j, col := range cols {
			cells[j] = col.cell(t)
		}
		rowStrings[i] = fmt.Sprintf("| %s |\n", strings.Join(cells, " | "))
	}

	return tableHeader, rowStrings
}

// RenderBranchHeadSummary renders the per-task table and top-up line for a branch-head check run.
func RenderBranchHeadSummary(
	pool *pooling.CommitPool,
	uiBaseURL string,
	repoRef forge.RepoRef,
	branch string,
	topUpsEnabled bool,
	nextTopUpIn time.Duration,
) string {
	if pool == nil || len(pool.Tasks) == 0 {
		return "No task evaluations recorded."
	}

	tableHeader, rowStrings := BuildBranchHeadTable(pool, uiBaseURL, repoRef)

	var sb strings.Builder
	sb.WriteString(tableHeader)
	for _, rStr := range rowStrings {
		sb.WriteString(rStr)
	}

	numRuns := len(pool.Runs)
	if topUpsEnabled {
		runWording := "1 Run"
		if numRuns > 1 {
			runWording = fmt.Sprintf("%d Runs", numRuns)
		}
		if nextTopUpIn > 0 {
			sb.WriteString(fmt.Sprintf("\npooled from %s; next top-up in ~%s", runWording, formatApproxDuration(nextTopUpIn)))
		} else {
			sb.WriteString(fmt.Sprintf("\npooled from %s; next top-up pending", runWording))
		}
	} else if numRuns > 1 {
		sb.WriteString(fmt.Sprintf("\npooled from %d Runs", numRuns))
	}

	return strings.TrimSpace(sb.String())
}

// CheckRunStateForBranchHead generates CheckRunState for a branch head commit.
func CheckRunStateForBranchHead(
	pool *pooling.CommitPool,
	lastMove *stats.DeltaEstimate,
	uiBaseURL string,
	repoRef forge.RepoRef,
	branch string,
	phase v1alpha1.RunPhase,
	topUpsEnabled bool,
	nextTopUpIn time.Duration,
) CheckRunState {
	state := CheckRunState{
		Name:       CheckRunName,
		DetailsURL: BranchHeadDetailsURL(uiBaseURL, repoRef, branch),
	}

	// In-progress / queued lifecycle states
	if phase != v1alpha1.RunPhaseCompleted && phase != v1alpha1.RunPhaseFailed {
		state.Status = "in_progress"
		if phase == v1alpha1.RunPhasePending || phase == "" {
			state.Title = "queued"
			state.Summary = "Evaluation queued."
		} else {
			state.Title = "running"
			state.Summary = "Evaluation in progress."
		}
		return state
	}

	state.Status = "completed"

	// Failure: every task failed to run (no observations scored)
	// Keeps raw internal error messages/paths out of the check run.
	if pool == nil || pool.OverallEstimate == nil {
		state.Conclusion = "failure"
		if pool != nil && pool.TotalTasks > 0 {
			if pool.TotalTasks == 1 {
				state.Title = "evals failed: 1 task failed"
			} else {
				state.Title = fmt.Sprintf("evals failed: all %d tasks failed", pool.TotalTasks)
			}
		} else {
			state.Title = "evals failed"
		}

		if pool != nil && len(pool.Tasks) > 0 {
			state.Summary = RenderBranchHeadSummary(pool, uiBaseURL, repoRef, branch, topUpsEnabled, nextTopUpIn)
			state.Text = RenderBranchHeadText(pool, uiBaseURL, repoRef, branch)
		} else {
			state.Summary = state.Title
		}
		state.Images = BuildBranchHeadImages(uiBaseURL, repoRef, branch)
		return state
	}

	// Neutral if partial
	if pool.CompletedTasks < pool.TotalTasks {
		state.Conclusion = "neutral"
	} else {
		state.Conclusion = "success"
	}

	state.Title = pooling.FormatTitle(pool.OverallEstimate, lastMove)
	state.Summary = RenderBranchHeadSummary(pool, uiBaseURL, repoRef, branch, topUpsEnabled, nextTopUpIn)
	state.Text = RenderBranchHeadText(pool, uiBaseURL, repoRef, branch)
	state.Images = BuildBranchHeadImages(uiBaseURL, repoRef, branch)
	return state
}

// BuildBranchHeadImages builds the images array for a branch-head check run.
func BuildBranchHeadImages(uiBaseURL string, repoRef forge.RepoRef, branch string) []CheckRunImage {
	return sparklineImage(uiBaseURL, repoRef, branch)
}

// RenderBranchHeadText renders the markdown text body for a GitHub Check Run on a branch-head commit:
// the per-task table followed by a footer link to the series page.
func RenderBranchHeadText(
	pool *pooling.CommitPool,
	uiBaseURL string,
	repoRef forge.RepoRef,
	branch string,
) string {
	if pool == nil || len(pool.Tasks) == 0 {
		return ""
	}

	tableHeader, rowStrings := BuildBranchHeadTable(pool, uiBaseURL, repoRef)

	trimmedUIBaseURL := strings.TrimRight(strings.TrimSpace(uiBaseURL), "/")
	var footerStr string
	if trimmedUIBaseURL != "" && repoRef.Owner != "" && repoRef.Name != "" && branch != "" {
		seriesURL := BranchHeadDetailsURL(uiBaseURL, repoRef, branch)
		footerStr = fmt.Sprintf("[series](%s)\n", seriesURL)
	}

	res := renderTableText(tableHeader, rowStrings, footerStr, "see series view")
	return SanitizeArtifactPaths(res)
}

// HashCheckRunState computes a stable content hash for check run state,
// excluding time-varying relative countdown text to prevent resync churn.
func HashCheckRunState(state CheckRunState) string {
	stableSummary := state.Summary
	if idx := strings.LastIndex(stableSummary, "\npooled from "); idx != -1 {
		// Keep the run count in the hash so landing top-up runs trigger updates,
		// but strip any time-varying countdown after the semicolon.
		footer := stableSummary[idx:]
		if semi := strings.Index(footer, ";"); semi != -1 {
			stableSummary = stableSummary[:idx] + footer[:semi]
		}
	}

	h := sha256.New()
	h.Write([]byte(state.Name))
	h.Write([]byte{0})
	h.Write([]byte(state.Status))
	h.Write([]byte{0})
	h.Write([]byte(state.Conclusion))
	h.Write([]byte{0})
	h.Write([]byte(state.Title))
	h.Write([]byte{0})
	h.Write([]byte(stableSummary))
	h.Write([]byte{0})
	h.Write([]byte(state.Text))
	h.Write([]byte{0})
	h.Write([]byte(state.DetailsURL))
	for _, img := range state.Images {
		h.Write([]byte{0})
		h.Write([]byte(img.ImageURL))
		h.Write([]byte{0})
		h.Write([]byte(img.Alt))
		h.Write([]byte{0})
		h.Write([]byte(img.Caption))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ReportBranchHead creates or updates a check run for a branch head commit.
func (r *Reporter) ReportBranchHead(
	ctx context.Context,
	repoRef forge.RepoRef,
	commitSHA string,
	state CheckRunState,
	checkRunID int64,
) (int64, string, error) {
	if r.Client == nil {
		return 0, "", nil
	}
	hash := HashCheckRunState(state)

	if checkRunID != 0 {
		err := r.Client.UpdateCheckRun(ctx, repoRef.Owner, repoRef.Name, checkRunID, state)
		if err == nil {
			return checkRunID, hash, nil
		}
		var pe *PermanentError
		if !errors.As(err, &pe) || pe.StatusCode != http.StatusNotFound {
			return 0, "", fmt.Errorf("updating check run %d: %w", checkRunID, err)
		}
	}

	createdID, err := r.Client.CreateCheckRun(ctx, repoRef.Owner, repoRef.Name, commitSHA, state)
	if err != nil {
		return 0, "", fmt.Errorf("creating check run: %w", err)
	}
	return createdID, hash, nil
}
