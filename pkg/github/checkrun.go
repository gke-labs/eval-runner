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
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"k8s.io/apimachinery/pkg/api/meta"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/affinity"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/configkeys"
	"github.com/gke-labs/eval-runner/pkg/forge"
	"github.com/gke-labs/eval-runner/pkg/ui"
)

// CheckRunName is the standardized check run name for eval verdicts.
const CheckRunName = "eval-runner / evals"

// CheckRunImage represents an image attached to a check run.
type CheckRunImage = forge.CheckRunImage

// CheckRunAnnotation represents a file annotation on a check run.
type CheckRunAnnotation = forge.CheckRunAnnotation

// CheckRunAction represents an interactive action button on a check run.
type CheckRunAction = forge.CheckRunAction

// CheckRun action identifiers.
const (
	ActionIdentifierMore32    = "more-32"
	ActionIdentifierRerunBase = "rerun-base"
	ActionIdentifierRerunCand = "rerun-cand"
)

var (
	// ActionMore32 requests 32 more evaluation samples.
	ActionMore32 = CheckRunAction{
		Label:       "Run 32 more samples",
		Description: "Run 32 more evaluation samples",
		Identifier:  ActionIdentifierMore32,
	}
	// ActionRerunBase re-runs the baseline evaluation run.
	ActionRerunBase = CheckRunAction{
		Label:       "Re-run baseline",
		Description: "Re-run baseline evaluation run",
		Identifier:  ActionIdentifierRerunBase,
	}
	// ActionRerunCand re-runs the candidate evaluation run.
	ActionRerunCand = CheckRunAction{
		Label:       "Re-run candidate",
		Description: "Re-run candidate evaluation run",
		Identifier:  ActionIdentifierRerunCand,
	}
)

// MaxCheckRunActions is the maximum number of actions supported by GitHub on a check run.
const MaxCheckRunActions = 3

// SanitizeCheckRunActions enforces GitHub check-run action limits:
// maximum 3 actions, label <= 20 characters, description <= 40 characters, identifier <= 20 characters.
func SanitizeCheckRunActions(actions []CheckRunAction) []CheckRunAction {
	if len(actions) == 0 {
		return nil
	}
	if len(actions) > MaxCheckRunActions {
		actions = actions[:MaxCheckRunActions]
	}
	sanitized := make([]CheckRunAction, 0, len(actions))
	for _, a := range actions {
		if len(a.Label) > 20 {
			a.Label = a.Label[:20]
		}
		if len(a.Description) > 40 {
			a.Description = a.Description[:40]
		}
		if len(a.Identifier) > 20 {
			a.Identifier = a.Identifier[:20]
		}
		sanitized = append(sanitized, a)
	}
	return sanitized
}

// TaskSampleFailure records a failing sample for an evaluation task.
type TaskSampleFailure struct {
	SampleID string
	Score    float64
}

// CheckRunState represents the payload state of a GitHub Check Run.
type CheckRunState struct {
	Name        string
	Status      string // "in_progress" or "completed"
	Conclusion  string // "success", "neutral", "failure", or "" (when in_progress)
	Title       string // short state summary
	Summary     string // detailed summary text
	Text        string // technical details text (e.g. collapsed error details)
	DetailsURL  string // URL of the details page
	Images      []CheckRunImage
	Annotations []CheckRunAnnotation
	Actions     []CheckRunAction
}

// CheckRunOption configures CheckRunStateForComparison.
type CheckRunOption func(*checkRunOptions)

type checkRunOptions struct {
	annotations []CheckRunAnnotation
}

// WithAnnotations sets the annotations for the check run.
func WithAnnotations(annotations []CheckRunAnnotation) CheckRunOption {
	return func(o *checkRunOptions) {
		o.annotations = annotations
	}
}

// BuildCheckRunImages builds the images array for a Check Run:
//   - PR comparisons: repo-scoped sparkline for the base branch:
//     {uiBaseURL}/{host}/{owner}/{repo}/sparkline.svg?branch=<base>&w=600&h=80, caption "<base>, last 20 commits".
//   - When the comparison table has <= 3 tasks (and > 0), one image per task with ?eval= is also emitted
//     with caption "<taskID> (<base>, last 20 commits)".
//
// Returns nil if uiBaseURL is empty or repo identity cannot be resolved.
func BuildCheckRunImages(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run, uiBaseURL string) []CheckRunImage {
	if comp == nil || strings.TrimSpace(uiBaseURL) == "" {
		return nil
	}
	repoRef, ok := ui.ComparisonRepoRef(comp, baseRun, candRun)
	if !ok || repoRef.Host == "" || repoRef.Owner == "" || repoRef.Name == "" {
		return nil
	}

	trimmedUIBaseURL := strings.TrimRight(strings.TrimSpace(uiBaseURL), "/")

	baseBranch := "main"
	if baseRun != nil && baseRun.Labels != nil && baseRun.Labels[configkeys.LabelBranch] != "" {
		baseBranch = baseRun.Labels[configkeys.LabelBranch]
	} else if comp.Labels != nil && comp.Labels[configkeys.LabelBranch] != "" {
		baseBranch = comp.Labels[configkeys.LabelBranch]
	}

	caption := fmt.Sprintf("%s, last 20 commits", baseBranch)

	overallURL := fmt.Sprintf("%s%s/sparkline.svg?branch=%s&w=600&h=80",
		trimmedUIBaseURL, repoRef.Path(), url.QueryEscape(baseBranch))
	images := []CheckRunImage{
		{
			Alt:      caption,
			ImageURL: overallURL,
			Caption:  caption,
		},
	}

	rows := ui.ComparisonRowsWithRuns(comp, baseRun, candRun, repoRef, "")
	if len(rows) > 0 && len(rows) <= 3 {
		for _, row := range rows {
			imgURL := fmt.Sprintf("%s%s/sparkline.svg?branch=%s&eval=%s&w=600&h=80",
				trimmedUIBaseURL, repoRef.Path(), url.QueryEscape(baseBranch), url.QueryEscape(row.ID))
			taskCaption := fmt.Sprintf("%s (%s, last 20 commits)", row.ID, baseBranch)
			images = append(images, CheckRunImage{
				Alt:      row.ID,
				ImageURL: imgURL,
				Caption:  taskCaption,
			})
		}
	}

	return images
}

// HasGlobMeta reports whether pattern contains any glob metacharacters (*, ?, [, {).
func HasGlobMeta(pattern string) bool {
	return strings.ContainsAny(pattern, "*?[{")
}

// ResolveTaskFilePaths matches declared task path globs against changed files.
// If changedFiles is non-empty, it returns all changed files that match any declared glob pattern.
// If changedFiles is empty, it returns only declared paths that are concrete file paths (contain no glob metacharacters).
func ResolveTaskFilePaths(taskPaths []string, changedFiles []string) []string {
	if len(taskPaths) == 0 {
		return nil
	}
	if len(changedFiles) > 0 {
		var matched []string
		seen := make(map[string]bool)
		for _, f := range changedFiles {
			normFile := affinity.Normalize(f)
			for _, pat := range taskPaths {
				normPat := affinity.Normalize(pat)
				m, err := doublestar.Match(normPat, normFile)
				if err == nil && m {
					if !seen[normFile] {
						seen[normFile] = true
						matched = append(matched, f)
					}
					break
				}
			}
		}
		return matched
	}

	// No changed files provided: keep only literal concrete file paths without glob metacharacters.
	var concrete []string
	for _, p := range taskPaths {
		if !HasGlobMeta(p) {
			concrete = append(concrete, p)
		}
	}
	return concrete
}

// BuildCheckRunAnnotationsFromFailures emits up to 50 notice-level annotations on the task's
// resolved paths with the sample id and score, so the Files tab of the PR shows where the evals point.
// Skip when resolved paths is empty.
func BuildCheckRunAnnotationsFromFailures(tasks []v1alpha1.ManifestTask, changedFiles []string, failures map[string][]TaskSampleFailure) []CheckRunAnnotation {
	if len(tasks) == 0 || len(failures) == 0 {
		return nil
	}

	var annotations []CheckRunAnnotation
	for _, task := range tasks {
		resolvedPaths := ResolveTaskFilePaths(task.Paths, changedFiles)
		if len(resolvedPaths) == 0 {
			continue
		}
		taskFailures, ok := failures[task.ID]
		if !ok || len(taskFailures) == 0 {
			continue
		}
		for _, failure := range taskFailures {
			if failure.Score >= 1.0 {
				continue
			}
			for _, p := range resolvedPaths {
				if len(annotations) >= 50 {
					return annotations
				}
				annotations = append(annotations, CheckRunAnnotation{
					Path:            p,
					StartLine:       1,
					EndLine:         1,
					AnnotationLevel: "notice",
					Title:           task.ID,
					Message:         fmt.Sprintf("sample %s: score %.2f", failure.SampleID, failure.Score),
				})
			}
		}
	}
	return annotations
}

// BuildCheckRunAnnotations extracts sample-level failures from task Inspect logs and emits
// up to 50 notice-level annotations on the task's resolved paths.
func BuildCheckRunAnnotations(tasks []v1alpha1.ManifestTask, changedFiles []string, taskLogs map[string][]byte) []CheckRunAnnotation {
	if len(tasks) == 0 || len(taskLogs) == 0 {
		return nil
	}

	failures := make(map[string][]TaskSampleFailure)
	for _, task := range tasks {
		resolvedPaths := ResolveTaskFilePaths(task.Paths, changedFiles)
		if len(resolvedPaths) == 0 {
			continue
		}
		logBytes, ok := taskLogs[task.ID]
		if !ok || len(logBytes) == 0 {
			continue
		}
		scores, err := artifacts.ParseInspectSampleScores(logBytes)
		if err != nil {
			continue
		}
		for _, s := range scores {
			if s.Score < 1.0 {
				failures[task.ID] = append(failures[task.ID], TaskSampleFailure{
					SampleID: s.ID,
					Score:    s.Score,
				})
			}
		}
	}
	return BuildCheckRunAnnotationsFromFailures(tasks, changedFiles, failures)
}

// ComparisonWebPath returns the web path for a Comparison:
// /{host}/{owner}/{repo}/pulls/{number} when a pull request spec is present,
// /{host}/{owner}/{repo}/comparisons/{name} when repo identity is resolved,
// or /comparisons/{namespace}/{name} as fallback.
func ComparisonWebPath(comp *v1alpha1.Comparison, ref ui.RepoRef) string {
	if comp == nil {
		return ""
	}
	if comp.Spec.PullRequest != nil && comp.Spec.PullRequest.Number > 0 {
		if ref.Host != "" {
			return ui.RepoLink(ref, fmt.Sprintf("/pulls/%d", comp.Spec.PullRequest.Number), "")
		}
	}
	if ref.Host != "" {
		return ui.RepoLink(ref, fmt.Sprintf("/comparisons/%s", comp.Name), "")
	}
	return fmt.Sprintf("/comparisons/%s/%s", comp.Namespace, comp.Name)
}

// CheckRunDetailsURL returns the URL for the check run Details link:
// {uiBaseURL}/{host}/{owner}/{repo}/pulls/{number}; falling back to the comparison page
// only when the Comparison has no pull-request spec.
func CheckRunDetailsURL(comp *v1alpha1.Comparison, uiBaseURL string, runs ...*v1alpha1.Run) string {
	if comp == nil || strings.TrimSpace(uiBaseURL) == "" {
		return ""
	}
	trimmed := strings.TrimRight(strings.TrimSpace(uiBaseURL), "/")
	ref, _ := ui.ComparisonRepoRef(comp, runs...)
	return trimmed + ComparisonWebPath(comp, ref)
}

var (
	uriRegex      = regexp.MustCompile(`(?:gs|s3|file)://[^\s)":',]+`)
	localDirRegex = regexp.MustCompile(`(?:/tmp|/private/tmp|/var/tmp)/[^\s)":',]+`)
)

// SanitizeArtifactPaths replaces known on-disk local directory paths and storage URIs
// with only the artifact name (e.g. "log.json"), preventing internal deployment details
// or /tmp/ and gs:// paths from leaking into user-facing output.
func SanitizeArtifactPaths(s string, runs ...*v1alpha1.Run) string {
	if s == "" {
		return ""
	}
	for _, run := range runs {
		if run == nil {
			continue
		}
		for _, task := range run.Status.Tasks {
			if task.LogPath != "" {
				s = strings.ReplaceAll(s, task.LogPath, path.Base(task.LogPath))
			}
		}
	}
	s = uriRegex.ReplaceAllStringFunc(s, func(m string) string {
		return path.Base(m)
	})
	s = localDirRegex.ReplaceAllStringFunc(s, func(m string) string {
		return filepath.Base(m)
	})
	return s
}

// IsScoringFailure reports whether a Comparison cannot be scored due to missing
// or unreadable artifacts (ConditionTypeArtifactsReadable is False).
func IsScoringFailure(comp *v1alpha1.Comparison) bool {
	if comp == nil {
		return false
	}
	return meta.IsStatusConditionFalse(comp.Status.Conditions, v1alpha1.ConditionTypeArtifactsReadable)
}

// ScoringFailureDetails extracts the user-facing headline and sanitized technical error
// when scoring fails for a Comparison.
func ScoringFailureDetails(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run) (headline string, techError string) {
	if comp == nil {
		return "", ""
	}
	msgLower := strings.ToLower(comp.Status.Message)
	isCand := strings.HasPrefix(msgLower, "failed to load candidate") ||
		strings.HasPrefix(msgLower, "candidate log path") ||
		strings.HasPrefix(msgLower, "failed to parse candidate")

	role := "Baseline"
	target := "baseline"
	if isCand {
		role = "Candidate"
		target = "candidate"
	}

	var shortSHA string
	if !isCand {
		if comp.Spec.PullRequest != nil && comp.Spec.PullRequest.BaseSHA != "" {
			shortSHA = comp.Spec.PullRequest.BaseSHA
		} else if baseRun != nil && baseRun.Spec.Repo.Commit != "" {
			shortSHA = baseRun.Spec.Repo.Commit
		}
	} else {
		if comp.Spec.PullRequest != nil && comp.Spec.PullRequest.HeadSHA != "" {
			shortSHA = comp.Spec.PullRequest.HeadSHA
		} else if candRun != nil && candRun.Spec.Repo.Commit != "" {
			shortSHA = candRun.Spec.Repo.Commit
		}
	}
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}

	if shortSHA != "" {
		headline = fmt.Sprintf("%s results for `%s` are unavailable; eval-runner will need to re-run the %s.", role, shortSHA, target)
	} else {
		headline = fmt.Sprintf("%s results are unavailable; eval-runner will need to re-run the %s.", role, target)
	}

	techError = SanitizeArtifactPaths(comp.Status.Message, baseRun, candRun)
	return headline, techError
}

// PlannedSamples returns the planned total samples for a Run, computed as limit x tasks.
// If limit or task count cannot be determined, it returns 0.
func PlannedSamples(run *v1alpha1.Run) int {
	if run == nil {
		return 0
	}
	limit := 0
	if run.Spec.Limit != nil && *run.Spec.Limit > 0 {
		limit = *run.Spec.Limit
	}
	if limit <= 0 {
		return 0
	}
	tasks := 0
	if len(run.Spec.Tasks) > 0 {
		tasks = len(run.Spec.Tasks)
	} else if run.Status.Discovery != nil && run.Status.Discovery.EligibleTaskCount > 0 {
		tasks = run.Status.Discovery.EligibleTaskCount
	} else if len(run.Status.Tasks) > 0 {
		tasks = len(run.Status.Tasks)
	}
	return limit * tasks
}

// taskLabelCounts builds the comma-separated per-label count string:
// "N improved, M unchanged, K regressed, J inconclusive" from comp.Status.Tasks,
// omitting zero buckets. Returns "" if comp is nil, tasks is empty, or all counts are zero.
func taskLabelCounts(comp *v1alpha1.Comparison) string {
	if comp == nil || len(comp.Status.Tasks) == 0 {
		return ""
	}
	var numImproved, numUnchanged, numRegressed, numInconclusive int
	for _, task := range comp.Status.Tasks {
		switch task.Label {
		case v1alpha1.ComparisonTaskLabelImproved:
			numImproved++
		case v1alpha1.ComparisonTaskLabelUnchanged:
			numUnchanged++
		case v1alpha1.ComparisonTaskLabelRegressed:
			numRegressed++
		case v1alpha1.ComparisonTaskLabelInconclusive:
			numInconclusive++
		}
	}
	var parts []string
	if numImproved > 0 {
		parts = append(parts, fmt.Sprintf("%d improved", numImproved))
	}
	if numUnchanged > 0 {
		parts = append(parts, fmt.Sprintf("%d unchanged", numUnchanged))
	}
	if numRegressed > 0 {
		parts = append(parts, fmt.Sprintf("%d regressed", numRegressed))
	}
	if numInconclusive > 0 {
		parts = append(parts, fmt.Sprintf("%d inconclusive", numInconclusive))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ", ")
}

// CheckRunStateForComparison maps a Comparison and its candidate/baseline Runs
// to the CheckRunState for the "eval-runner / evals" check run across all 5 verdicts.
func CheckRunStateForComparison(comp *v1alpha1.Comparison, baseRun, candRun *v1alpha1.Run, inconclusivePolicy string, uiBaseURL string, extraOpts ...CheckRunOption) CheckRunState {
	state := CheckRunState{
		Name: CheckRunName,
	}
	if comp == nil {
		state.Status = "in_progress"
		state.Title = "running"
		state.Summary = "running"
		return state
	}

	var opts checkRunOptions
	for _, opt := range extraOpts {
		if opt != nil {
			opt(&opts)
		}
	}

	state.Annotations = opts.annotations
	state.Images = BuildCheckRunImages(comp, baseRun, candRun, uiBaseURL)
	state.Text = RenderCheckRunText(comp, baseRun, candRun, uiBaseURL)
	state.DetailsURL = CheckRunDetailsURL(comp, uiBaseURL, baseRun, candRun)

	policy := configkeys.InconclusivePolicyNeutral
	if inconclusivePolicy != "" {
		policy = strings.ToLower(strings.TrimSpace(inconclusivePolicy))
	}

	if IsScoringFailure(comp) {
		headline, _ := ScoringFailureDetails(comp, baseRun, candRun)
		state.Status = "completed"
		if policy == configkeys.InconclusivePolicyFailure {
			state.Conclusion = "failure"
		} else {
			state.Conclusion = "neutral"
		}
		state.Title = headline
		state.Summary = headline
		state.Actions = []CheckRunAction{ActionRerunCand, ActionRerunBase}
		return state
	}

	counts := taskLabelCounts(comp)

	switch comp.Status.Verdict {
	case v1alpha1.ComparisonVerdictImproved:
		state.Status = "completed"
		state.Conclusion = "success"
		if counts != "" {
			state.Title = fmt.Sprintf("%s: %s", comp.Status.Verdict, counts)
		} else if comp.Status.PImprovement != nil {
			state.Title = fmt.Sprintf("IMPROVED · P(improvement) = %.2f", *comp.Status.PImprovement)
		} else {
			state.Title = "IMPROVED"
		}
		state.Summary = state.Title

	case v1alpha1.ComparisonVerdictUnchanged:
		state.Status = "completed"
		state.Conclusion = "success"
		if counts != "" {
			state.Title = fmt.Sprintf("%s: %s", comp.Status.Verdict, counts)
		} else {
			state.Title = "UNCHANGED"
		}
		state.Summary = state.Title

	case v1alpha1.ComparisonVerdictInconclusive:
		state.Status = "completed"
		if policy == configkeys.InconclusivePolicyFailure {
			state.Conclusion = "failure"
		} else {
			state.Conclusion = "neutral"
		}
		if counts != "" {
			state.Title = fmt.Sprintf("%s: %s", comp.Status.Verdict, counts)
		} else {
			state.Title = "INCONCLUSIVE · more samples recommended"
		}
		state.Summary = state.Title

	case v1alpha1.ComparisonVerdictRegressed:
		state.Status = "completed"
		state.Conclusion = "failure"
		if counts != "" {
			state.Title = fmt.Sprintf("%s: %s", comp.Status.Verdict, counts)
		} else {
			state.Title = "REGRESSED"
		}
		state.Summary = state.Title

	case v1alpha1.ComparisonVerdictRunning:
		fallthrough
	default:
		state.Status = "in_progress"
		state.Conclusion = ""

		// Calculate N (completed samples) and M (total planned samples: limit x tasks)
		completed := 0
		total := 0
		if candRun != nil {
			if candRun.Status.Summary != nil {
				completed += candRun.Status.Summary.TotalSamples
			}
			total += PlannedSamples(candRun)
		}
		if baseRun != nil {
			if baseRun.Status.Summary != nil {
				completed += baseRun.Status.Summary.TotalSamples
			}
			total += PlannedSamples(baseRun)
		}

		if total > 0 {
			state.Title = fmt.Sprintf("running · %d of %d samples", completed, total)
		} else if completed > 0 {
			state.Title = fmt.Sprintf("running · %d samples", completed)
		} else {
			state.Title = "running"
		}
		state.Summary = state.Title
	}

	if comp.Status.Message != "" {
		state.Summary = EscapeCell(comp.Status.Message)
	}

	var budgetLine string
	if comp.Status.Budget != nil && comp.Status.Budget.LimitUSD != nil && *comp.Status.Budget.LimitUSD > 0 {
		spent := 0.0
		if comp.Status.Budget.SpentUSD != nil {
			spent = *comp.Status.Budget.SpentUSD
		} else if comp.Status.CostUSD != nil {
			spent = *comp.Status.CostUSD
		}
		budgetLine = fmt.Sprintf("budget $%.2f / $%.2f", spent, *comp.Status.Budget.LimitUSD)
		if !comp.Status.Budget.Enforced {
			budgetLine += " (not enforced)"
		}
	} else if comp.Spec.BudgetUSD != nil && *comp.Spec.BudgetUSD > 0 {
		spent := 0.0
		if comp.Status.CostUSD != nil {
			spent = *comp.Status.CostUSD
		}
		budgetLine = fmt.Sprintf("budget $%.2f / $%.2f", spent, *comp.Spec.BudgetUSD)
	}

	if budgetLine != "" {
		if state.Summary != "" {
			state.Summary = state.Summary + "\n\n" + budgetLine
		} else {
			state.Summary = budgetLine
		}
	}

	lines := ActionHintLines(comp)
	if len(lines) > 0 {
		if state.Summary != "" {
			state.Summary = state.Summary + "\n\n" + strings.Join(lines, "\n")
		} else {
			state.Summary = strings.Join(lines, "\n")
		}
	}

	candFailed := (candRun != nil && candRun.Status.Phase == v1alpha1.RunPhaseFailed) || (comp != nil && strings.Contains(comp.Status.Message, "candidate task failed"))
	baseFailed := (baseRun != nil && baseRun.Status.Phase == v1alpha1.RunPhaseFailed) || (comp != nil && strings.Contains(comp.Status.Message, "baseline task failed"))

	if candFailed || baseFailed {
		var actions []CheckRunAction
		if candFailed {
			actions = append(actions, ActionRerunCand)
		}
		if baseFailed {
			actions = append(actions, ActionRerunBase)
		}
		if len(actions) == 0 {
			actions = []CheckRunAction{ActionRerunCand, ActionRerunBase}
		}
		state.Actions = actions
	} else {
		switch comp.Status.Verdict {
		case v1alpha1.ComparisonVerdictInconclusive:
			state.Actions = []CheckRunAction{ActionMore32, ActionRerunBase}
		case v1alpha1.ComparisonVerdictImproved, v1alpha1.ComparisonVerdictRegressed, v1alpha1.ComparisonVerdictUnchanged:
			state.Actions = []CheckRunAction{ActionMore32}
		}
	}

	state.Actions = SanitizeCheckRunActions(state.Actions)
	state.DetailsURL = CheckRunDetailsURL(comp, uiBaseURL, baseRun, candRun)

	return state
}
