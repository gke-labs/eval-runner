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
	"github.com/gke-labs/eval-runner/pkg/ui"
)

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

// RenderComparisonMarkdown renders a GitHub pull request comment for a Comparison,
// using ui.ComparisonRows and ui.ComparisonSummary so the comment matches the web view.
// If uiBaseURL is provided, the web-view link is rendered as an absolute Markdown link;
// otherwise it renders as code text.
func RenderComparisonMarkdown(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run, uiBaseURL string) string {
	if comp == nil {
		return ""
	}

	var sb strings.Builder

	// 1. Hidden marker comment
	sb.WriteString(ComparisonMarker(comp.Namespace, comp.Name))
	sb.WriteString("\n\n")

	// 2. Verdict line
	verdict := string(comp.Status.Verdict)
	if verdict == "" {
		verdict = string(v1alpha1.ComparisonVerdictRunning)
	}
	if comp.Status.PImprovement != nil {
		sb.WriteString(fmt.Sprintf("## %s · P(improvement) = %.2f\n\n", verdict, *comp.Status.PImprovement))
	} else {
		sb.WriteString(fmt.Sprintf("## %s\n\n", verdict))
	}

	// 3. Table
	sb.WriteString("| eval | base | this PR | Δ (95% CI) | label | runs |\n")
	sb.WriteString("| --- | --- | --- | --- | --- | --- |\n")

	rows := ui.ComparisonRows(comp)
	for _, row := range rows {
		escapedID := EscapeCell(row.ID)
		deltaStr := "—"
		if row.Delta != "—" && row.CILabel != "—" {
			deltaStr = fmt.Sprintf("%s %s", row.Delta, row.CILabel)
		} else if row.Delta != "—" {
			deltaStr = row.Delta
		}

		runsCount := row.Pairs
		if row.SamplesRun > 0 {
			runsCount = row.SamplesRun
		}

		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %d |\n",
			escapedID,
			row.BaseRate,
			row.CandRate,
			deltaStr,
			row.Label,
			runsCount,
		))
	}
	sb.WriteString("\n")

	// 4. Footer
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
	if strings.TrimSpace(uiBaseURL) != "" {
		baseURL := strings.TrimRight(strings.TrimSpace(uiBaseURL), "/")
		linkStr = fmt.Sprintf("[web view](%s%s)", baseURL, webPath)
	}

	footerParts := []string{samplesStr, pairedAgainstStr, linkStr}
	if comp.Status.SummaryPath != "" {
		footerParts = append(footerParts, EscapeCell(comp.Status.SummaryPath))
	}

	sb.WriteString(strings.Join(footerParts, " · "))
	sb.WriteString("\n")

	return sb.String()
}
