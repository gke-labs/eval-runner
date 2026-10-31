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

package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/stats"
)

// MaxSummarySamples is the maximum number of sample entries persisted in summary.json before truncation.
const MaxSummarySamples = 10000

// ErrNotFound indicates that an artifact was not found in the sink.
var ErrNotFound = errors.New("artifact not found")

// IsNotFound checks if the error indicates a missing artifact.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// TaskSampleTokens records token usage for an individual evaluated sample in summary.json.
type TaskSampleTokens struct {
	Input  int `json:"input"`
	Output int `json:"output"`
}

// TaskSampleSummary records outcome and metrics for an individual evaluated sample in summary.json.
type TaskSampleSummary struct {
	ID             string            `json:"id"`
	Epoch          int               `json:"epoch,omitempty"`
	Score          *float64          `json:"score,omitempty"`
	Status         string            `json:"status,omitempty"`
	LatencySeconds *float64          `json:"latencySeconds,omitempty"`
	Tokens         *TaskSampleTokens `json:"tokens,omitempty"`
}

// RunMetadata provides metadata about the Run and Task for artifact storage.
type RunMetadata struct {
	RunName           string
	RunNamespace      string
	TaskID            string
	Model             string
	Commit            string
	TaskCount         int
	ControllerVersion string
	Scorer            string
	SuccessRate       *float64
	SuccessRateCI     *v1alpha1.ConfidenceInterval
	SuccessRateSD     *float64

	// Task execution aggregates and timestamps for summary.json
	Samples          []TaskSampleSummary
	SampleCount      int
	SamplesTruncated bool
	Successes        int
	MeanScore        *float64
	StartedAt        *time.Time
	FinishedAt       *time.Time
	SampleRange      *SampleRange
	Tokens           *TaskTokens
	Latency          *TaskLatency
	CostUSD          *float64
	CostSource       v1alpha1.CostSource
	PossibleSamples  int
}

// TaskLatency records latency percentiles aggregated across evaluated samples.
type TaskLatency struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
}

// TaskTokens records token usage breakdown for an evaluation task.
// Invariant: Total == Input + Output across all reporting surfaces.
type TaskTokens struct {
	Input      int `json:"input,omitempty"`
	Output     int `json:"output,omitempty"`
	Total      int `json:"total,omitempty"`
	Reasoning  int `json:"reasoning,omitempty"`
	CacheRead  int `json:"cacheRead,omitempty"`
	CacheWrite int `json:"cacheWrite,omitempty"`
}

// SampleRange specifies a 1-based, inclusive window of samples.
type SampleRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// TaskSummary records per-task aggregates and execution metadata written alongside the Inspect log.
type TaskSummary struct {
	SchemaVersion     int                          `json:"schemaVersion,omitempty"`
	TaskID            string                       `json:"taskId"`
	Model             string                       `json:"model"`
	Commit            string                       `json:"commit"`
	LogFilename       string                       `json:"logFilename,omitempty"`
	RunName           string                       `json:"runName,omitempty"`
	RunNamespace      string                       `json:"runNamespace,omitempty"`
	ControllerVersion string                       `json:"controllerVersion,omitempty"`
	Scorer            string                       `json:"scorer,omitempty"`
	SampleCount       int                          `json:"sampleCount"`
	Samples           []TaskSampleSummary          `json:"samples,omitempty"`
	SamplesTruncated  bool                         `json:"samplesTruncated,omitempty"`
	Successes         int                          `json:"successes"`
	MeanScore         *float64                     `json:"meanScore"`
	SuccessRate       *float64                     `json:"successRate,omitempty"`
	SuccessRateCI     *v1alpha1.ConfidenceInterval `json:"successRateCI,omitempty"`
	SuccessRateSD     *float64                     `json:"successRateSD,omitempty"`
	StartedAt         *time.Time                   `json:"startedAt,omitempty"`
	FinishedAt        *time.Time                   `json:"finishedAt,omitempty"`
	Capped            bool                         `json:"capped"`
	CapBytes          int64                        `json:"capBytes,omitempty"`
	SampleRange       *SampleRange                 `json:"sampleRange,omitempty"`
	Tokens            *TaskTokens                  `json:"tokens,omitempty"`
	Latency           *TaskLatency                 `json:"latency,omitempty"`
	CostUSD           *float64                     `json:"costUSD,omitempty"`
	CostSource        v1alpha1.CostSource          `json:"costSource,omitempty"`
	PossibleSamples   int                          `json:"possibleSamples,omitempty"`
}

type taskSummaryAlias TaskSummary

type rawTaskSummary struct {
	taskSummaryAlias
	RawSamples     json.RawMessage `json:"samples"`
	HasSampleCount *int            `json:"sampleCount"`
}

// UnmarshalJSON unmarshals summary.json, supporting schemaVersion 1 (samples array)
// and backward-compatibility with unversioned legacy summaries where samples was an integer count.
func (s *TaskSummary) UnmarshalJSON(data []byte) error {
	var raw rawTaskSummary
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*s = TaskSummary(raw.taskSummaryAlias)
	if raw.HasSampleCount != nil {
		s.SampleCount = *raw.HasSampleCount
	}
	trimmed := bytes.TrimSpace(raw.RawSamples)
	if len(trimmed) > 0 {
		if trimmed[0] == '[' {
			var sampleList []TaskSampleSummary
			if err := json.Unmarshal(trimmed, &sampleList); err != nil {
				return fmt.Errorf("failed to unmarshal samples array: %w", err)
			}
			s.Samples = sampleList
			if raw.HasSampleCount == nil && !s.SamplesTruncated {
				s.SampleCount = len(sampleList)
			}
		} else {
			var count int
			if err := json.Unmarshal(trimmed, &count); err == nil && raw.HasSampleCount == nil {
				s.SampleCount = count
			}
		}
	}
	return nil
}

// ComparisonTaskArtifact records paired comparison details for a single task in the artifact JSON.
type ComparisonTaskArtifact struct {
	ID               string                           `json:"id"`
	BaseRate         float64                          `json:"baseRate"`
	CandidateRate    float64                          `json:"candidateRate"`
	Delta            float64                          `json:"delta"`
	DeltaCI          v1alpha1.DeltaConfidenceInterval `json:"deltaCI"`
	Pairs            int                              `json:"pairs"`
	Wins             int                              `json:"wins"`
	Losses           int                              `json:"losses"`
	Ties             int                              `json:"ties"`
	Dropped          v1alpha1.ComparisonDropped       `json:"dropped"`
	Label            string                           `json:"label"`
	PImprovement     float64                          `json:"pImprovement"`
	SamplesRun       int                              `json:"samplesRun,omitempty"`
	Decision         string                           `json:"decision,omitempty"`
	SuggestedNext    int                              `json:"suggestedNext,omitempty"`
	Tokens           *v1alpha1.TaskTokens             `json:"tokens,omitempty"`
	CandidateTokens  *v1alpha1.TaskTokens             `json:"candidateTokens,omitempty"`
	BaseTokens       *v1alpha1.TaskTokens             `json:"baseTokens,omitempty"`
	Latency          *v1alpha1.TaskLatency            `json:"latency,omitempty"`
	CandidateLatency *v1alpha1.TaskLatency            `json:"candidateLatency,omitempty"`
	BaseLatency      *v1alpha1.TaskLatency            `json:"baseLatency,omitempty"`
	CostUSD          *float64                         `json:"costUSD,omitempty"`
	CostSource       v1alpha1.CostSource              `json:"costSource,omitempty"`
	CandidateCostUSD *float64                         `json:"candidateCostUSD,omitempty"`
	BaseCostUSD      *float64                         `json:"baseCostUSD,omitempty"`
	PossibleSamples  int                              `json:"possibleSamples,omitempty"`
	Affinity         string                           `json:"affinity,omitempty"`
	// PairsTable records paired scores for each sample in execution order (candidate log order).
	// Callers (such as the UI CI-history chart) rely on this ordering to reconstruct batch prefix series.
	PairsTable []stats.PairedSampleScore `json:"pairsTable,omitempty"`
}

// ComparisonArtifact is the schema stored as comparisons/<name>.json in the artifact sink.
type ComparisonArtifact struct {
	Comparison            string                             `json:"comparison"`
	Namespace             string                             `json:"namespace"`
	Baseline              string                             `json:"baseline"`
	Candidate             string                             `json:"candidate"`
	Verdict               string                             `json:"verdict"`
	PImprovement          float64                            `json:"pImprovement"`
	CostUSD               *float64                           `json:"costUSD,omitempty"`
	PossibleSamples       int                                `json:"possibleSamples,omitempty"`
	Summary               string                             `json:"summary,omitempty"`
	Tasks                 []ComparisonTaskArtifact           `json:"tasks"`
	Batches               []v1alpha1.ComparisonBatchStatus   `json:"batches,omitempty"`
	CreatedAt             time.Time                          `json:"createdAt"`
	SkippedNotPlanned     []string                           `json:"skippedNotPlanned,omitempty"`
	SkippedUnmatched      []string                           `json:"skippedUnmatched,omitempty"`
	NotExecuted           []string                           `json:"notExecuted,omitempty"`
	NotExecutedByBothRuns []string                           `json:"notExecutedByBothRuns,omitempty"`
	FailedInBothRuns      []string                           `json:"failedInBothRuns,omitempty"`
	CandidateFailed       []string                           `json:"candidateFailed,omitempty"`
	Affinity              *v1alpha1.ComparisonAffinityStatus `json:"affinity,omitempty"`
	AffinityMatched       int                                `json:"affinityMatched,omitempty"`
	Budget                *v1alpha1.ComparisonBudgetStatus   `json:"budget,omitempty"`
}

// ArtifactSink defines the interface for storing Inspect evaluation logs and artifacts.
type ArtifactSink interface {
	// StoreArtifact persists the raw log bytes for a task and returns the storage path or URI.
	StoreArtifact(ctx context.Context, meta RunMetadata, filename string, data []byte) (string, error)

	// StoreComparison persists comparison summary data as comparisons/<comparisonName>.json and returns the storage path or URI.
	StoreComparison(ctx context.Context, comparisonName string, data []byte) (string, error)

	// StoreManifest persists the discovered manifest JSON as runs/<commit>/manifest.json and returns the storage path or URI.
	StoreManifest(ctx context.Context, meta RunMetadata, data []byte) (string, error)

	// Load retrieves raw artifact bytes from the given storage path or URI, bounded by the maximum allowed artifact size.
	Load(ctx context.Context, path string) ([]byte, error)

	// ArtifactPrefix returns the run-level storage prefix/URI for a given commit, or empty string if not applicable.
	ArtifactPrefix(commit string) string

	// IsDurable reports whether the sink provides durable persistent storage across restarts.
	IsDurable() bool

	// Close releases any resources held by the sink (e.g. underlying network connections or storage clients).
	Close() error
}

// AccessChecker is implemented by storage sinks that support probing credential and storage accessibility.
type AccessChecker interface {
	CheckAccess(ctx context.Context) error
}
