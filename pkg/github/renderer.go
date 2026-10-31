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
	"fmt"
	"strings"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/stats"
	"github.com/gke-labs/eval-runner/pkg/ui"
)

const (
	MaxCommentBytes      = 60000
	MaxCheckRunTextBytes = 65000
	MaxDetailsBlockBytes = 8192
	MaxIDList            = stats.MaxIDList
)

// CapDetailsBlock caps a collapsed <details> error details block to maxBytes,
// preserving the <details> and code block wrapping with an explicit truncation notice.
func CapDetailsBlock(block string, maxBytes int) string {
	if len(block) <= maxBytes {
		return block
	}
	const prefix = "<details>\n<summary>Error details</summary>\n\n```\n"
	const suffix = "\n... (truncated)\n```\n</details>"
	if len(prefix)+len(suffix) >= maxBytes {
		if maxBytes > 0 {
			return block[:maxBytes]
		}
		return ""
	}
	available := maxBytes - len(prefix) - len(suffix)
	inner := block
	if strings.HasPrefix(inner, prefix) {
		inner = strings.TrimPrefix(inner, prefix)
	}
	if strings.HasSuffix(inner, "\n```\n</details>") {
		inner = strings.TrimSuffix(inner, "\n```\n</details>")
	}
	if len(inner) > available {
		inner = inner[:available]
	}
	return prefix + inner + suffix
}

// FormatIDList formats a slice of task IDs, truncating to maxIDs if exceeded,
// appending an explicit "... and N more" marker. Aliased from pkg/stats.
var FormatIDList = stats.FormatIDList

var idListPrefixes = []string{
	"tasks not executed by both runs: ",
	"tasks not executed: ",
	"failed in both runs: ",
	"skipped (not planned): ",
	"skipped (unmatched): ",
	"candidate tasks failed: ",
	"candidate task failed: ",
}

// TruncateNoteIDLists scans a notes string for task ID lists introduced by standard prefixes
// and truncates each ID list to at most MaxIDList IDs with an explicit "... and N more" marker.
// This provides defence-in-depth for older Comparisons or objects created by external writers
// whose status.message was not bounded by the controller.
func TruncateNoteIDLists(notes string) string {
	if notes == "" {
		return ""
	}

	clauses := strings.Split(notes, "; ")
	modified := false
	for i, clause := range clauses {
		for _, prefix := range idListPrefixes {
			idx := strings.Index(clause, prefix)
			if idx == -1 {
				continue
			}

			before := clause[:idx+len(prefix)]
			idListStr := clause[idx+len(prefix):]

			// If already truncated with the marker, skip
			if strings.Contains(idListStr, "... and ") {
				break
			}

			ids := strings.Split(idListStr, ", ")
			if len(ids) > MaxIDList {
				if prefix == "candidate task failed: " {
					before = clause[:idx] + "candidate tasks failed: "
				}
				clauses[i] = before + FormatIDList(ids, MaxIDList)
				modified = true
			}
			break
		}
	}

	if !modified {
		return notes
	}
	return strings.Join(clauses, "; ")
}

// EscapeCell sanitizes an untrusted string for use in Markdown tables and comment text:
// collapses newlines, HTML-escapes raw tags (<, >), and escapes Markdown formatting characters.
func EscapeCell(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "|", `\|`)
	s = strings.ReplaceAll(s, "`", `\`+"`")
	s = strings.ReplaceAll(s, "*", `\*`)
	s = strings.ReplaceAll(s, "_", `\_`)
	s = strings.ReplaceAll(s, "[", `\[`)
	s = strings.ReplaceAll(s, "]", `\]`)
	return s
}

// ComparisonNotes extracts the single-line notes to render under the verdict in PR comments:
// the suffix after the verdict summary in status.message (e.g. skipped or not-executed tasks),
// or status.message as a whole when the verdict is INCONCLUSIVE or REGRESSED.
func ComparisonNotes(comp *v1alpha1.Comparison) string {
	if comp == nil || comp.Status.Message == "" {
		return ""
	}

	var notes string
	switch comp.Status.Verdict {
	case v1alpha1.ComparisonVerdictInconclusive, v1alpha1.ComparisonVerdictRegressed:
		notes = comp.Status.Message
	default:
		msg := comp.Status.Message
		matched := false
		for _, prefix := range []string{"IMPROVED:", "UNCHANGED:", "RUNNING:"} {
			if strings.HasPrefix(msg, prefix) {
				matched = true
				parts := strings.SplitN(msg, "; ", 2)
				if len(parts) > 1 {
					notes = parts[1]
				}
				break
			}
		}
		if !matched {
			notes = msg
		}
	}
	return TruncateNoteIDLists(notes)
}

// RenderScoringFailureDetailsBlock returns the collapsed markdown details block for scoring failures,
// or empty string if comp is not a scoring failure.
func RenderScoringFailureDetailsBlock(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run) string {
	if !IsScoringFailure(comp) {
		return ""
	}
	_, techErr := ScoringFailureDetails(comp, baseRun, candRun)
	return fmt.Sprintf("<details>\n<summary>Error details</summary>\n\n```\n%s\n```\n</details>", techErr)
}

// BuildComparisonTable constructs the markdown table header and row strings for a Comparison.
func BuildComparisonTable(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run, uiBaseURL string) (string, []string) {
	if comp == nil {
		return "", nil
	}

	hasTokens := false
	hasAffinity := false
	hasPImprovement := false
	repoRef, _ := ui.ComparisonRepoRef(comp, baseRun, candRun)
	rows := ui.ComparisonRowsWithRuns(comp, baseRun, candRun, repoRef, "")
	for _, row := range rows {
		if row.Tokens != nil {
			hasTokens = true
		}
		if row.Affinity != "" {
			hasAffinity = true
		}
		if row.PImprovement != nil {
			hasPImprovement = true
		}
	}

	trimmedUIBaseURL := strings.TrimRight(strings.TrimSpace(uiBaseURL), "/")

	type tableColumn struct {
		header string
		cell   func(row ui.ComparisonTaskRow) string
	}

	var cols []tableColumn
	cols = append(cols, tableColumn{
		header: "eval",
		cell: func(row ui.ComparisonTaskRow) string {
			escapedID := EscapeCell(row.ID)
			if trimmedUIBaseURL != "" && row.CandSamplesLink != "" {
				return fmt.Sprintf("[%s](%s%s)", escapedID, trimmedUIBaseURL, row.CandSamplesLink)
			}
			return escapedID
		},
	})

	if hasAffinity {
		cols = append(cols, tableColumn{
			header: "affinity",
			cell: func(row ui.ComparisonTaskRow) string {
				if row.Affinity == "" {
					return "—"
				}
				return row.Affinity
			},
		})
	}

	cols = append(cols,
		tableColumn{
			header: "base",
			cell: func(row ui.ComparisonTaskRow) string {
				return row.BaseRate
			},
		},
		tableColumn{
			header: "this PR",
			cell: func(row ui.ComparisonTaskRow) string {
				return row.CandRate
			},
		},
		tableColumn{
			header: "Δ (95% CI)",
			cell: func(row ui.ComparisonTaskRow) string {
				if row.Delta != "—" && row.CILabel != "—" {
					return fmt.Sprintf("%s %s", row.Delta, row.CILabel)
				} else if row.Delta != "—" {
					return row.Delta
				}
				return "—"
			},
		},
		tableColumn{
			header: "label",
			cell: func(row ui.ComparisonTaskRow) string {
				return row.Label
			},
		},
		tableColumn{
			header: "samples",
			cell: func(row ui.ComparisonTaskRow) string {
				if row.SamplesRun > 0 {
					return fmt.Sprintf("%d", row.SamplesRun)
				} else if row.Pairs > 0 {
					return fmt.Sprintf("%d", row.Pairs)
				}
				return "—"
			},
		},
	)

	if hasTokens {
		cols = append(cols, tableColumn{
			header: "tokens",
			cell: func(row ui.ComparisonTaskRow) string {
				if row.Tokens != nil {
					if row.Tokens.Reasoning > 0 {
						return fmt.Sprintf("%d (+R %d)", row.Tokens.Total, row.Tokens.Reasoning)
					}
					return fmt.Sprintf("%d", row.Tokens.Total)
				}
				return "—"
			},
		})
	}

	cols = append(cols, tableColumn{
		header: "W/L/T",
		cell: func(row ui.ComparisonTaskRow) string {
			if row.Pairs > 0 {
				return fmt.Sprintf("%d/%d/%d", row.Wins, row.Losses, row.Ties)
			}
			return "—"
		},
	})

	if hasPImprovement {
		cols = append(cols, tableColumn{
			header: "P(imp)",
			cell: func(row ui.ComparisonTaskRow) string {
				if row.PImprovement != nil {
					return fmt.Sprintf("%.2f", *row.PImprovement)
				}
				return "—"
			},
		})
	}

	headers := make([]string, len(cols))
	seps := make([]string, len(cols))
	for j, col := range cols {
		headers[j] = col.header
		seps[j] = "---"
	}
	tableHeader := fmt.Sprintf("| %s |\n| %s |\n", strings.Join(headers, " | "), strings.Join(seps, " | "))

	rowStrings := make([]string, len(rows))
	for i, row := range rows {
		cells := make([]string, len(cols))
		for j, col := range cols {
			cells[j] = col.cell(row)
		}
		rowStrings[i] = fmt.Sprintf("| %s |\n", strings.Join(cells, " | "))
	}

	return tableHeader, rowStrings
}

// BuildComparisonFooter constructs the footer lines (samples, paired against, trajectories, web view) and action hints.
func BuildComparisonFooter(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run, uiBaseURL string) string {
	if comp == nil {
		return ""
	}

	repoRef, _ := ui.ComparisonRepoRef(comp, baseRun, candRun)
	trimmedUIBaseURL := strings.TrimRight(strings.TrimSpace(uiBaseURL), "/")

	var footerBuilder strings.Builder
	summary := ui.ComparisonSummary(comp, baseRun, candRun)
	samplesStr := summary.SamplesSummary
	if samplesStr == "" {
		samplesStr = fmt.Sprintf("%d run · %d paired", summary.TotalSamples, summary.TotalPairs)
	}

	pairedAgainstStr := ""
	if summary.BaseCommitShort != "" {
		pairedAgainstStr = fmt.Sprintf("paired against base commit %s", EscapeCell(summary.BaseCommitShort))
	} else if summary.BaseRun != "" {
		pairedAgainstStr = fmt.Sprintf("paired against base commit %s", EscapeCell(summary.BaseRun))
	} else {
		pairedAgainstStr = "paired against base"
	}

	webPath := fmt.Sprintf("/comparisons/%s/%s", comp.Namespace, comp.Name)
	linkStr := fmt.Sprintf("`%s`", webPath)
	if trimmedUIBaseURL != "" {
		webPath = ComparisonWebPath(comp, repoRef)
		linkStr = fmt.Sprintf("[web view](%s%s)", trimmedUIBaseURL, webPath)
	}

	footerParts := []string{samplesStr, pairedAgainstStr}
	hasAffinity := false
	for _, task := range comp.Status.Tasks {
		if task.Affinity != "" {
			hasAffinity = true
			break
		}
	}
	if hasAffinity && comp.Status.Summary != nil {
		footerParts = append(footerParts, fmt.Sprintf("%d matched", comp.Status.Summary.AffinityMatched))
	}
	if comp.Status.Budget != nil && comp.Status.Budget.LimitUSD != nil && *comp.Status.Budget.LimitUSD > 0 {
		spent := 0.0
		if comp.Status.Budget.SpentUSD != nil {
			spent = *comp.Status.Budget.SpentUSD
		} else if comp.Status.CostUSD != nil {
			spent = *comp.Status.CostUSD
		}
		part := fmt.Sprintf("budget $%.2f / $%.2f", spent, *comp.Status.Budget.LimitUSD)
		if !comp.Status.Budget.Enforced {
			part += " (not enforced)"
		}
		footerParts = append(footerParts, part)
	} else if comp.Spec.BudgetUSD != nil && *comp.Spec.BudgetUSD > 0 {
		spent := 0.0
		if comp.Status.CostUSD != nil {
			spent = *comp.Status.CostUSD
		}
		footerParts = append(footerParts, fmt.Sprintf("budget $%.2f / $%.2f", spent, *comp.Spec.BudgetUSD))
	}
	if trimmedUIBaseURL != "" && summary.CandRun != "" {
		trajectoriesPath := fmt.Sprintf("/runs/%s/%s/tasks", comp.Namespace, summary.CandRun)
		if repoRef.Host != "" {
			trajectoriesPath = ui.RepoLink(repoRef, fmt.Sprintf("/runs/%s/tasks", summary.CandRun), "")
		}
		footerParts = append(footerParts, fmt.Sprintf("[trajectories](%s%s)", trimmedUIBaseURL, trajectoriesPath))
	}
	footerParts = append(footerParts, linkStr)
	if comp.Status.SummaryPath != "" {
		footerParts = append(footerParts, EscapeCell(comp.Status.SummaryPath))
	}

	footerBuilder.WriteString(strings.Join(footerParts, " · "))
	footerBuilder.WriteString("\n")
	if summary.FooterMetricsDelta != "" {
		footerBuilder.WriteString(summary.FooterMetricsDelta)
		footerBuilder.WriteString("\n")
	}

	// Actions (e.g. comment `/eval more <n>` to run N more samples for inconclusive tasks)
	lines := ActionHintLines(comp)
	if len(lines) > 0 {
		footerBuilder.WriteString("\n")
		footerBuilder.WriteString(strings.Join(lines, "\n"))
		footerBuilder.WriteString("\n")
	}

	// Audit requested samples (e.g. "+32 samples requested by @user" or "+32 samples requested by @user (x2)")
	type sampleAuditEntry struct {
		user    string
		samples int
		count   int
	}
	var auditList []sampleAuditEntry
	auditIndex := make(map[string]int)

	recordAudit := func(user string, samples int) {
		if user == "" || samples <= 0 {
			return
		}
		key := fmt.Sprintf("%s:%d", user, samples)
		if idx, ok := auditIndex[key]; ok {
			auditList[idx].count++
		} else {
			auditIndex[key] = len(auditList)
			auditList = append(auditList, sampleAuditEntry{
				user:    user,
				samples: samples,
				count:   1,
			})
		}
	}

	if comp != nil {
		for _, cmd := range comp.Status.HandledCommands {
			if cmd.Outcome == "applied" && cmd.Requested > 0 {
				user := cmd.RequestedBy
				if user == "" {
					user = cmd.Login
				}
				recordAudit(user, cmd.Requested)
			}
		}
		for _, cmd := range comp.Status.PendingCommands {
			if cmd.Requested > 0 {
				user := cmd.RequestedBy
				if user == "" {
					user = cmd.Login
				}
				recordAudit(user, cmd.Requested)
			}
		}
	}

	var requestedAudits []string
	for _, entry := range auditList {
		if entry.count > 1 {
			requestedAudits = append(requestedAudits, fmt.Sprintf("+%d samples requested by @%s (x%d)", entry.samples, entry.user, entry.count))
		} else {
			requestedAudits = append(requestedAudits, fmt.Sprintf("+%d samples requested by @%s", entry.samples, entry.user))
		}
	}
	if len(requestedAudits) > 0 {
		footerBuilder.WriteString("\n")
		footerBuilder.WriteString(strings.Join(requestedAudits, "\n"))
		footerBuilder.WriteString("\n")
	}
	return footerBuilder.String()
}

// RenderComparisonMarkdown renders a GitHub pull request comment for a Comparison,
// using ui.ComparisonRows and ui.ComparisonSummary so the comment matches the web view.
// If uiBaseURL is provided, the web-view link is rendered as an absolute Markdown link;
// otherwise it renders as code text.
// The rendered body is guaranteed not to exceed maxCommentBytes (60000 bytes).
func RenderComparisonMarkdown(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run, uiBaseURL string) string {
	if comp == nil {
		return ""
	}

	var header strings.Builder

	// 1. Hidden marker comment
	header.WriteString(ComparisonMarker(comp.Namespace, comp.Name))
	header.WriteString("\n\n")

	// 2. Verdict line
	verdict := string(comp.Status.Verdict)
	if verdict == "" {
		verdict = string(v1alpha1.ComparisonVerdictRunning)
	}
	if comp.Status.PImprovement != nil {
		header.WriteString(fmt.Sprintf("## %s · P(improvement) = %.2f\n\n", verdict, *comp.Status.PImprovement))
	} else {
		header.WriteString(fmt.Sprintf("## %s\n\n", verdict))
	}

	// Notes under the verdict line
	if IsScoringFailure(comp) {
		headline, _ := ScoringFailureDetails(comp, baseRun, candRun)
		header.WriteString(headline)
		header.WriteString("\n\n")
		header.WriteString(RenderScoringFailureDetailsBlock(comp, baseRun, candRun))
		header.WriteString("\n\n")
	} else if notes := ComparisonNotes(comp); notes != "" {
		header.WriteString(EscapeCell(notes))
		header.WriteString("\n\n")
	}

	headerStr := header.String()

	// 3. Table
	tableHeader, rowStrings := BuildComparisonTable(comp, baseRun, candRun, uiBaseURL)

	// 4. Footer & Actions
	footerAndActionsStr := BuildComparisonFooter(comp, baseRun, candRun, uiBaseURL)

	// Assemble full comment
	var fullSB strings.Builder
	fullSB.WriteString(headerStr)
	fullSB.WriteString(tableHeader)
	for _, rStr := range rowStrings {
		fullSB.WriteString(rStr)
	}
	fullSB.WriteString("\n")
	fullSB.WriteString(footerAndActionsStr)
	fullComment := fullSB.String()

	if len(fullComment) <= MaxCommentBytes {
		return fullComment
	}

	// Body exceeds MaxCommentBytes; drop table rows from the bottom while adding
	// an explicit marker separated by blank lines from table and footer.
	prefixLens := make([]int, len(rowStrings)+1)
	for i := 0; i < len(rowStrings); i++ {
		prefixLens[i+1] = prefixLens[i] + len(rowStrings[i])
	}

	baseLen := len(headerStr) + len(tableHeader) + 1 + 1 + len(footerAndActionsStr) // 1 for \n after table, 1 for \n after marker
	k := 0
	for candidates := len(rowStrings) - 1; candidates >= 0; candidates-- {
		dropped := len(rowStrings) - candidates
		taskWord := "tasks"
		if dropped == 1 {
			taskWord = "task"
		}
		markerLine := fmt.Sprintf("... %d more %s (see the Comparison view)\n", dropped, taskWord)
		if baseLen+prefixLens[candidates]+len(markerLine) <= MaxCommentBytes {
			k = candidates
			break
		}
	}

	dropped := len(rowStrings) - k
	taskWord := "tasks"
	if dropped == 1 {
		taskWord = "task"
	}

	var sb strings.Builder
	sb.WriteString(headerStr)
	sb.WriteString(tableHeader)
	for i := 0; i < k; i++ {
		sb.WriteString(rowStrings[i])
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("... %d more %s (see the Comparison view)\n", dropped, taskWord))
	sb.WriteString("\n")
	sb.WriteString(footerAndActionsStr)
	return sb.String()
}

// RenderCheckRunText renders the markdown text body for a GitHub Check Run:
// the per-task table, followed by footer links (web view, trajectories),
// and for failures the collapsed technical details that #417 introduced.
// If the body exceeds MaxCheckRunTextBytes, it safely truncates table rows from the bottom
// with "… N more rows (see web view)".
func RenderCheckRunText(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run, uiBaseURL string) string {
	if comp == nil {
		return ""
	}

	tableHeader, rowStrings := BuildComparisonTable(comp, baseRun, candRun, uiBaseURL)
	footerStr := BuildComparisonFooter(comp, baseRun, candRun, uiBaseURL)

	var detailsBlock string
	if IsScoringFailure(comp) {
		rawBlock := RenderScoringFailureDetailsBlock(comp, baseRun, candRun)
		detailsBlock = CapDetailsBlock(rawBlock, MaxDetailsBlockBytes)
	}

	var footerAndDetails strings.Builder
	footerAndDetails.WriteString(footerStr)
	if detailsBlock != "" {
		if footerStr != "" {
			footerAndDetails.WriteString("\n")
		}
		footerAndDetails.WriteString(detailsBlock)
		footerAndDetails.WriteString("\n")
	}
	footerAndDetailsStr := footerAndDetails.String()

	var fullSB strings.Builder
	if len(rowStrings) > 0 || tableHeader != "" {
		fullSB.WriteString(tableHeader)
		for _, rStr := range rowStrings {
			fullSB.WriteString(rStr)
		}
		fullSB.WriteString("\n")
	}
	fullSB.WriteString(footerAndDetailsStr)
	fullText := fullSB.String()

	if len(fullText) <= MaxCheckRunTextBytes {
		return fullText
	}

	prefixLens := make([]int, len(rowStrings)+1)
	for i := 0; i < len(rowStrings); i++ {
		prefixLens[i+1] = prefixLens[i] + len(rowStrings[i])
	}

	baseLen := len(tableHeader) + 1 + 1 + len(footerAndDetailsStr)
	k := 0
	for candidates := len(rowStrings) - 1; candidates >= 0; candidates-- {
		dropped := len(rowStrings) - candidates
		rowWord := "rows"
		if dropped == 1 {
			rowWord = "row"
		}
		markerLine := fmt.Sprintf("… %d more %s (see web view)\n", dropped, rowWord)
		if baseLen+prefixLens[candidates]+len(markerLine) <= MaxCheckRunTextBytes {
			k = candidates
			break
		}
	}

	dropped := len(rowStrings) - k
	rowWord := "rows"
	if dropped == 1 {
		rowWord = "row"
	}

	var sb strings.Builder
	sb.WriteString(tableHeader)
	for i := 0; i < k; i++ {
		sb.WriteString(rowStrings[i])
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("… %d more %s (see web view)\n", dropped, rowWord))
	sb.WriteString("\n")
	sb.WriteString(footerAndDetailsStr)
	res := sb.String()
	if len(res) > MaxCheckRunTextBytes {
		res = res[:MaxCheckRunTextBytes]
	}
	return res
}

// ActionHintLines returns formatted bulleted action hints for inconclusive tasks in a Comparison,
// bounded to MaxIDList with an overflow notice if the count exceeds MaxIDList.
// Returns nil if comp is nil or if there are no action hints.
func ActionHintLines(comp *v1alpha1.Comparison) []string {
	if comp == nil {
		return nil
	}
	actions := ui.ComparisonActions(comp)
	if len(actions) == 0 {
		return nil
	}
	var lines []string
	actionCount := len(actions)
	limit := actionCount
	if limit > MaxIDList {
		limit = MaxIDList
	}
	budgetReached := false
	if comp.Status.Budget != nil && comp.Status.Budget.Enforced && comp.Status.Budget.LimitUSD != nil && *comp.Status.Budget.LimitUSD > 0 {
		if comp.Status.Budget.SpentUSD != nil && *comp.Status.Budget.SpentUSD >= *comp.Status.Budget.LimitUSD {
			budgetReached = true
		} else if strings.Contains(comp.Status.Message, "budget") && strings.Contains(comp.Status.Message, "reached") {
			budgetReached = true
		}
	}
	for i := 0; i < limit; i++ {
		action := actions[i]
		if budgetReached {
			if action.ShowTaskID && action.TaskID != "" {
				lines = append(lines, fmt.Sprintf("- budget reached; comment `/eval more %d` to exceed it (%s)", action.SuggestedNext, EscapeCell(action.TaskID)))
			} else {
				lines = append(lines, fmt.Sprintf("- budget reached; comment `/eval more %d` to exceed it", action.SuggestedNext))
			}
		} else {
			if action.ShowTaskID && action.TaskID != "" {
				lines = append(lines, fmt.Sprintf("- comment `/eval more %d` to run %d more samples (%s)", action.SuggestedNext, action.SuggestedNext, EscapeCell(action.TaskID)))
			} else {
				lines = append(lines, fmt.Sprintf("- comment `/eval more %d` to run %d more samples", action.SuggestedNext, action.SuggestedNext))
			}
		}
	}
	if actionCount > MaxIDList {
		remaining := actionCount - MaxIDList
		taskWord := "tasks"
		if remaining == 1 {
			taskWord = "task"
		}
		lines = append(lines, fmt.Sprintf("- ... and %d more inconclusive %s (see the Comparison view)", remaining, taskWord))
	}
	return lines
}
