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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ComparisonPhase represents the lifecycle phase of a Comparison.
// +kubebuilder:validation:Enum=Running;Completed
type ComparisonPhase string

const (
	// ComparisonPhaseRunning means one or both runs are still running or pairing is in progress.
	ComparisonPhaseRunning ComparisonPhase = "Running"

	// ComparisonPhaseCompleted means both runs are terminal and the paired comparison is complete.
	ComparisonPhaseCompleted ComparisonPhase = "Completed"
)

// ComparisonTaskLabel represents the statistical decision label for a single task.
// +kubebuilder:validation:Enum=improved;regressed;unchanged;inconclusive;running;continue
type ComparisonTaskLabel string

const (
	ComparisonTaskLabelImproved     ComparisonTaskLabel = "improved"
	ComparisonTaskLabelRegressed    ComparisonTaskLabel = "regressed"
	ComparisonTaskLabelUnchanged    ComparisonTaskLabel = "unchanged"
	ComparisonTaskLabelInconclusive ComparisonTaskLabel = "inconclusive"
	ComparisonTaskLabelRunning      ComparisonTaskLabel = "running"
	ComparisonTaskLabelContinue     ComparisonTaskLabel = "continue"
)

// ComparisonVerdict represents the aggregate run-level verdict.
// +kubebuilder:validation:Enum=IMPROVED;REGRESSED;UNCHANGED;INCONCLUSIVE;RUNNING
type ComparisonVerdict string

const (
	ComparisonVerdictImproved     ComparisonVerdict = "IMPROVED"
	ComparisonVerdictRegressed    ComparisonVerdict = "REGRESSED"
	ComparisonVerdictUnchanged    ComparisonVerdict = "UNCHANGED"
	ComparisonVerdictInconclusive ComparisonVerdict = "INCONCLUSIVE"
	ComparisonVerdictRunning      ComparisonVerdict = "RUNNING"
)

// ComparisonRunRef specifies a reference to a Run resource.
type ComparisonRunRef struct {
	// Run is the name of the Run resource in the same namespace.
	// +kubebuilder:validation:Required
	Run string `json:"run"`
}

// ComparisonSamplingSpec configures adaptive paired batch sampling.
type ComparisonSamplingSpec struct {
	// BatchSize is the number of samples to evaluate in each subsequent batch (default: 8).
	// +kubebuilder:default=8
	// +kubebuilder:validation:Minimum=1
	// +optional
	BatchSize int `json:"batchSize,omitempty"`

	// MaxSamplesPerTask is the maximum total number of samples evaluated per task across all batches (default: 64).
	// +kubebuilder:default=64
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxSamplesPerTask int `json:"maxSamplesPerTask,omitempty"`
}

// EffectiveBatchSize returns the configured batch size, defaulting to 8 if unset or non-positive.
func (s *ComparisonSamplingSpec) EffectiveBatchSize() int {
	if s == nil || s.BatchSize <= 0 {
		return 8
	}
	return s.BatchSize
}

const (
	// DefaultMaxSamplesPerTask is the default maximum total samples per task across batches.
	DefaultMaxSamplesPerTask = 64
)

// EffectiveMaxSamplesPerTask returns the maximum total samples per task, defaulting to DefaultMaxSamplesPerTask if unset or non-positive.
func (s *ComparisonSamplingSpec) EffectiveMaxSamplesPerTask() int {
	if s == nil || s.MaxSamplesPerTask <= 0 {
		return DefaultMaxSamplesPerTask
	}
	return s.MaxSamplesPerTask
}

// PullRequestSpec references a pull request target for reporting.
type PullRequestSpec struct {
	// Repository is the GitHub repository in "owner/name" format.
	// +kubebuilder:validation:Required
	Repository string `json:"repository"`

	// Number is the pull request number.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	Number int `json:"number"`

	// HeadSHA is the SHA of the head commit of the pull request.
	// +optional
	HeadSHA string `json:"headSHA,omitempty"`

	// BaseSHA is the SHA of the base commit of the pull request.
	// +optional
	BaseSHA string `json:"baseSHA,omitempty"`
}

// ComparisonSpec defines the desired state of Comparison.
type ComparisonSpec struct {
	// Baseline specifies the baseline Run to compare against.
	// +kubebuilder:validation:Required
	Baseline ComparisonRunRef `json:"baseline"`

	// Candidate specifies the candidate (e.g. PR) Run.
	// +kubebuilder:validation:Required
	Candidate ComparisonRunRef `json:"candidate"`

	// EquivalenceMargin is the statistical equivalence margin (default: 0.05 for ±0.05).
	// +kubebuilder:default=0.05
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=0.5
	// +optional
	EquivalenceMargin *float64 `json:"equivalenceMargin,omitempty"`

	// Sampling configures paired batch sampling driven by the stopping rule.
	// When omitted, only the single pair of referenced runs is compared.
	// +optional
	Sampling *ComparisonSamplingSpec `json:"sampling,omitempty"`

	// Tasks is an optional allow-list of task IDs to compare.
	// If empty, all tasks present in both candidate and baseline runs are compared.
	// +optional
	Tasks []string `json:"tasks,omitempty"`

	// PullRequest references an optional GitHub pull request to report results to.
	// +optional
	PullRequest *PullRequestSpec `json:"pullRequest,omitempty"`

	// ExtraSamples requests evaluating N additional samples per undecided task (or per all tasks if all are decided) beyond the stopping rule.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ExtraSamples int `json:"extraSamples,omitempty"`
}

// DeltaConfidenceInterval represents a confidence interval on a difference/delta in [-1, 1].
type DeltaConfidenceInterval struct {
	// Lower is the lower confidence bound.
	// +kubebuilder:validation:Minimum=-1
	// +kubebuilder:validation:Maximum=1
	Lower float64 `json:"lower"`

	// Upper is the upper confidence bound.
	// +kubebuilder:validation:Minimum=-1
	// +kubebuilder:validation:Maximum=1
	Upper float64 `json:"upper"`
}

// ComparisonDropped records the count of dropped samples that could not be paired.
type ComparisonDropped struct {
	// Candidate is the number of candidate samples dropped due to lack of a matching baseline.
	// +optional
	Candidate int `json:"candidate"`

	// Baseline is the number of baseline samples dropped due to lack of a matching candidate.
	// +optional
	Baseline int `json:"baseline"`
}

// ComparisonTaskStatus records the paired comparison results for a single task.
type ComparisonTaskStatus struct {
	// ID is the unique identifier of the task (e.g. docs/readme_comprehension).
	// +kubebuilder:validation:Required
	ID string `json:"id"`

	// BaseRate is the mean score or success rate of baseline samples.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	// +optional
	BaseRate *float64 `json:"baseRate,omitempty"`

	// CandidateRate is the mean score or success rate of candidate samples.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	// +optional
	CandidateRate *float64 `json:"candidateRate,omitempty"`

	// Delta is the sample mean difference (candidate - baseline).
	// +kubebuilder:validation:Minimum=-1
	// +kubebuilder:validation:Maximum=1
	// +optional
	Delta *float64 `json:"delta,omitempty"`

	// DeltaCI is the confidence interval on the paired delta.
	// +optional
	DeltaCI *DeltaConfidenceInterval `json:"deltaCI,omitempty"`

	// Pairs is the number of matched (paired) sample pairs.
	// +optional
	Pairs int `json:"pairs"`

	// Wins is the number of paired samples where the candidate scored higher than baseline.
	// +optional
	Wins int `json:"wins"`

	// Losses is the number of paired samples where the candidate scored lower than baseline.
	// +optional
	Losses int `json:"losses"`

	// Ties is the number of paired samples where candidate and baseline scored equally.
	// +optional
	Ties int `json:"ties"`

	// Dropped records the number of unpaired samples dropped from candidate and baseline.
	// +optional
	Dropped ComparisonDropped `json:"dropped"`

	// Label is the decision outcome for this task: improved, regressed, unchanged, inconclusive, or running.
	// +optional
	Label ComparisonTaskLabel `json:"label,omitempty"`

	// SamplesRun is the total number of samples evaluated for this task across all batches.
	// +optional
	SamplesRun int `json:"samplesRun,omitempty"`

	// Decision is the stopping rule decision: improved, regressed, unchanged, inconclusive, or continue.
	// +optional
	Decision ComparisonTaskLabel `json:"decision,omitempty"`

	// SuggestedNext is the suggested number of additional samples to run if undecided.
	// +optional
	SuggestedNext int `json:"suggestedNext,omitempty"`

	// Tokens is the token usage breakdown consumed for this task across evaluated runs, when present in Inspect logs.
	// +optional
	Tokens *TaskTokens `json:"tokens,omitempty"`

	// Latency records sample latency percentiles (p50 and p95 in seconds) aggregated across evaluated samples.
	// +optional
	Latency *TaskLatency `json:"latency,omitempty"`

	// CostUSD is the estimated task evaluation cost in USD from model prices across candidate and baseline.
	// +optional
	CostUSD *float64 `json:"costUSD,omitempty"`

	// PossibleSamples is the total dataset size (possible samples) for this task when known from the Inspect log.
	// +optional
	PossibleSamples int `json:"possibleSamples,omitempty"`
}

// ComparisonBatchStatus records the execution state of one paired batch of Runs.
type ComparisonBatchStatus struct {
	// Index is the 1-based index of this batch.
	// +kubebuilder:validation:Minimum=1
	Index int `json:"index"`

	// SampleRange is the window of samples evaluated in this batch.
	// +optional
	SampleRange *SampleRange `json:"sampleRange,omitempty"`

	// CandidateRun is the name of the candidate Run for this batch.
	CandidateRun string `json:"candidateRun"`

	// BaselineRun is the name of the baseline Run for this batch.
	BaselineRun string `json:"baselineRun"`

	// Phase is the lifecycle phase of this batch (Running, Completed).
	// +optional
	Phase ComparisonPhase `json:"phase,omitempty"`
}

// ComparisonSummary provides aggregate statistics across tasks in a Comparison.
type ComparisonSummary struct {
	// Tasks is a compact string representing completed vs total tasks (e.g. "2/2").
	// +optional
	Tasks string `json:"tasks,omitempty"`

	// Skipped is the count of tasks discovered in the manifest but not planned by either Run.
	// +optional
	Skipped int `json:"skipped,omitempty"`
}

// ComparisonReportingStatus records GitHub comment and check run reporting state.
type ComparisonReportingStatus struct {
	// CommentID is the GitHub issue comment ID created or updated for this comparison.
	// +optional
	CommentID int64 `json:"commentID,omitempty"`

	// CheckRunID is the GitHub check run ID created or updated for this comparison.
	// +optional
	CheckRunID int64 `json:"checkRunID,omitempty"`

	// LastPostedHash is the hash of the last successfully posted content and state.
	// +optional
	LastPostedHash string `json:"lastPostedHash,omitempty"`

	// Message records error or status details (e.g. "disabled" or permanent error message).
	// +optional
	Message string `json:"message,omitempty"`
}

const (
	// MaxPendingCommands is the maximum number of pending commands recorded on a Comparison status.
	MaxPendingCommands = 50

	// MaxHandledCommands is the maximum number of handled commands preserved in a Comparison status.
	MaxHandledCommands = 100

	// MaxCommandAttempts is the maximum number of processing attempts before transient command errors become terminal.
	MaxCommandAttempts = 3
)

// ComparisonPendingCommand represents an unhandled command received from a GitHub review comment.
type ComparisonPendingCommand struct {
	// CommentID is the GitHub issue comment ID where the command was issued.
	CommentID int64 `json:"commentId"`

	// Login is the GitHub username of the user who issued the command.
	Login string `json:"login"`

	// Requested is the number of additional samples requested.
	Requested int `json:"requested"`

	// ReceivedAt is the time when the webhook received and recorded the command.
	ReceivedAt metav1.Time `json:"receivedAt"`

	// Attempts is the number of times the controller has attempted to process this command.
	// +optional
	Attempts int `json:"attempts,omitempty"`

	// NextAttemptAt is the earliest time when the controller should next attempt this command.
	// If nil or in the past, the command is eligible for processing immediately.
	// +optional
	NextAttemptAt *metav1.Time `json:"nextAttemptAt,omitempty"`
}

// ComparisonHandledCommand represents a review comment command that has been processed by the controller.
type ComparisonHandledCommand struct {
	// CommentID is the GitHub issue comment ID where the command was issued.
	CommentID int64 `json:"commentId"`

	// Login is the GitHub username of the user who issued the command.
	Login string `json:"login"`

	// Requested is the number of additional samples requested.
	Requested int `json:"requested"`

	// ReceivedAt is the time when the webhook received and recorded the command.
	ReceivedAt metav1.Time `json:"receivedAt"`

	// HandledAt is the time when the controller processed the command.
	// +optional
	HandledAt *metav1.Time `json:"handledAt,omitempty"`

	// Outcome records the result of processing the command (e.g. "applied", "denied: unauthorized", or error details).
	// +optional
	Outcome string `json:"outcome,omitempty"`
}

// ComparisonStatus defines the observed state of Comparison.
type ComparisonStatus struct {
	// Phase is the lifecycle phase of the comparison (Running, Completed, Failed).
	// +optional
	Phase ComparisonPhase `json:"phase,omitempty"`

	// Verdict is the aggregate run-level verdict (IMPROVED, REGRESSED, UNCHANGED, INCONCLUSIVE, RUNNING).
	// +optional
	Verdict ComparisonVerdict `json:"verdict,omitempty"`

	// PImprovement is the posterior probability that the candidate is a net improvement.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	// +optional
	PImprovement *float64 `json:"pImprovement,omitempty"`

	// Batches records the status of each paired batch of Runs evaluated for this Comparison.
	// +optional
	Batches []ComparisonBatchStatus `json:"batches,omitempty"`

	// ExtraSamplesApplied records the number of extra samples that have been consumed into extension batches.
	// +optional
	ExtraSamplesApplied int `json:"extraSamplesApplied,omitempty"`

	// Tasks records the status and paired comparison results for each evaluated task.
	// +optional
	Tasks []ComparisonTaskStatus `json:"tasks,omitempty"`

	// Summary provides aggregate statistics across tasks in the Comparison.
	// +optional
	Summary *ComparisonSummary `json:"summary,omitempty"`

	// SummaryPath is the storage path or URI where comparisons/<name>.json was written to the artifact sink.
	// +optional
	SummaryPath string `json:"summaryPath,omitempty"`

	// CostUSD is the total estimated evaluation cost in USD across all tasks in this Comparison.
	// +optional
	CostUSD *float64 `json:"costUSD,omitempty"`

	// PossibleSamples is the total possible samples across tasks when known from the Inspect log.
	// +optional
	PossibleSamples int `json:"possibleSamples,omitempty"`

	// Message is human-readable detail on failure, notes, or current status.
	// +optional
	Message string `json:"message,omitempty"`

	// Reporting records the status of GitHub PR comment and check run reporting.
	// +optional
	Reporting *ComparisonReportingStatus `json:"reporting,omitempty"`

	// PendingCommands records unhandled review comment commands waiting for controller processing.
	// +optional
	PendingCommands []ComparisonPendingCommand `json:"pendingCommands,omitempty"`

	// HandledCommands records review comment commands that have been processed by the controller.
	// +optional
	HandledCommands []ComparisonHandledCommand `json:"handledCommands,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=comp;comparison
// +kubebuilder:printcolumn:name="Verdict",type="string",JSONPath=".status.verdict"
// +kubebuilder:printcolumn:name="P(imp)",type="number",JSONPath=".status.pImprovement"
// +kubebuilder:printcolumn:name="Tasks",type="string",JSONPath=".status.summary.tasks"
// +kubebuilder:printcolumn:name="Baseline",type="string",JSONPath=".spec.baseline.run"
// +kubebuilder:printcolumn:name="Candidate",type="string",JSONPath=".spec.candidate.run"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Comparison is the Schema for the comparisons API.
type Comparison struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ComparisonSpec   `json:"spec,omitempty"`
	Status ComparisonStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ComparisonList contains a list of Comparison.
type ComparisonList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Comparison `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Comparison{}, &ComparisonList{})
}
