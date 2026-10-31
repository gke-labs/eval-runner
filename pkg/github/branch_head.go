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

	trimmedUIBaseURL := strings.TrimRight(uiBaseURL, "/")

	hasTokens := false
	for _, t := range pool.Tasks {
		if t.Tokens != nil && t.Tokens.Total > 0 {
			hasTokens = true
			break
		}
	}

	type summaryCol struct {
		header string
		cell   func(t pooling.PooledTask) string
	}

	var cols []summaryCol
	cols = append(cols, summaryCol{
		header: "eval",
		cell: func(t pooling.PooledTask) string {
			escapedID := EscapeCell(t.ID)
			if trimmedUIBaseURL != "" && t.RunName != "" {
				host := repoRef.Host
				if host == "" {
					host = "github.com"
				}
				samplesLink := fmt.Sprintf("/%s/%s/%s/runs/%s/tasks/%s/samples",
					host, repoRef.Owner, repoRef.Name, t.RunName, url.PathEscape(t.ID))
				return fmt.Sprintf("[%s](%s%s)", escapedID, trimmedUIBaseURL, samplesLink)
			}
			return escapedID
		},
	})

	cols = append(cols,
		summaryCol{
			header: "score",
			cell: func(t pooling.PooledTask) string {
				if t.SuccessRate != nil {
					return fmt.Sprintf("%.2f", *t.SuccessRate)
				}
				if t.Phase == v1alpha1.TaskPhaseFailed {
					return "failed"
				}
				return "—"
			},
		},
		summaryCol{
			header: "samples",
			cell: func(t pooling.PooledTask) string {
				if t.Samples > 0 {
					return fmt.Sprintf("%d", t.Samples)
				}
				return "—"
			},
		},
	)

	if hasTokens {
		cols = append(cols, summaryCol{
			header: "tokens",
			cell: func(t pooling.PooledTask) string {
				if t.Tokens != nil && t.Tokens.Total > 0 {
					return fmt.Sprintf("%d", t.Tokens.Total)
				}
				return "—"
			},
		})
	}

	var sb strings.Builder
	var headerParts []string
	var sepParts []string
	for _, col := range cols {
		headerParts = append(headerParts, col.header)
		sepParts = append(sepParts, "---")
	}
	sb.WriteString("| ")
	sb.WriteString(strings.Join(headerParts, " | "))
	sb.WriteString(" |\n| ")
	sb.WriteString(strings.Join(sepParts, " | "))
	sb.WriteString(" |\n")

	for _, t := range pool.Tasks {
		var rowParts []string
		for _, col := range cols {
			rowParts = append(rowParts, col.cell(t))
		}
		sb.WriteString("| ")
		sb.WriteString(strings.Join(rowParts, " | "))
		sb.WriteString(" |\n")
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

	// Failure: every task failed to run (or 0 completed tasks)
	// Keeps raw internal error messages/paths out of the check run.
	if pool == nil || pool.CompletedTasks == 0 {
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
		} else {
			state.Summary = state.Title
		}
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
	return state
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
	for _, act := range state.Actions {
		h.Write([]byte{0})
		h.Write([]byte(act.Label))
		h.Write([]byte{0})
		h.Write([]byte(act.Description))
		h.Write([]byte{0})
		h.Write([]byte(act.Identifier))
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
