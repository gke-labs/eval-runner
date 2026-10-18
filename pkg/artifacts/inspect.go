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
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/gke-labs/eval-runner/pkg/stats"
)

// InspectLog represents the top-level structure of an Inspect JSON log file.
type InspectLog struct {
	Version int             `json:"version"`
	Status  string          `json:"status"`
	Eval    InspectEval     `json:"eval"`
	Results *InspectResults `json:"results,omitempty"`
	Stats   *InspectStats   `json:"stats,omitempty"`
	Samples []InspectSample `json:"samples,omitempty"`
}

// InspectStats holds run-level statistics including model usage.
type InspectStats struct {
	ModelUsage map[string]InspectModelUsage `json:"model_usage,omitempty"`
}

// InspectModelUsage reports token counts for a model.
type InspectModelUsage struct {
	InputTokens           int `json:"input_tokens,omitempty"`
	OutputTokens          int `json:"output_tokens,omitempty"`
	TotalTokens           int `json:"total_tokens,omitempty"`
	InputTokensCacheRead  int `json:"input_tokens_cache_read,omitempty"`
	InputTokensCacheWrite int `json:"input_tokens_cache_write,omitempty"`
	ReasoningTokens       int `json:"reasoning_tokens,omitempty"`
}

// InspectEval contains task and scorer configuration metadata.
type InspectEval struct {
	Task    string          `json:"task"`
	Model   string          `json:"model"`
	Scorers []InspectScorer `json:"scorers,omitempty"`
}

// InspectScorer names an evaluator used in the run.
type InspectScorer struct {
	Name string `json:"name"`
}

// InspectResults holds summary metrics and counts.
type InspectResults struct {
	TotalSamples     int                   `json:"total_samples"`
	CompletedSamples int                   `json:"completed_samples"`
	Scores           []InspectScoreSummary `json:"scores,omitempty"`
}

// InspectScoreSummary contains per-scorer aggregate metrics.
type InspectScoreSummary struct {
	Name          string                   `json:"name"`
	Scorer        string                   `json:"scorer"`
	ScoredSamples int                      `json:"scored_samples"`
	Metrics       map[string]InspectMetric `json:"metrics,omitempty"`
}

// InspectMetric holds an individual metric value (e.g. accuracy).
type InspectMetric struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}

// InspectSample represents an individual evaluated sample.
type InspectSample struct {
	ID         any                           `json:"id"`
	Epoch      int                           `json:"epoch,omitempty"`
	Scores     map[string]InspectSampleScore `json:"scores,omitempty"`
	ModelUsage map[string]InspectModelUsage  `json:"model_usage,omitempty"`
}

// InspectSampleScore captures the score and outcome of a scorer for one sample.
type InspectSampleScore struct {
	Value       any    `json:"value"`
	Answer      any    `json:"answer,omitempty"`
	Explanation any    `json:"explanation,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

// TaskAggregates holds per-task scored samples, successes, and mean score.
// Successes counts samples with score >= 1.0 (for binary scorers).
// Numeric scores are taken as-is (unclamped), while 'P'/'PARTIAL' evaluates to 0.5.
type TaskAggregates struct {
	Samples   int
	Successes int
	MeanScore *float64
	Scores    []float64
}

// IsBinary reports whether all parsed sample scores are binary (0.0 or 1.0),
// or if no individual sample scores were recorded (e.g. results fallback).
func (a *TaskAggregates) IsBinary() bool {
	if len(a.Scores) == 0 {
		return true
	}
	for _, s := range a.Scores {
		if s != 0.0 && s != 1.0 {
			return false
		}
	}
	return true
}

// ScoreToFloat converts a scalar score value (number, bool, or sentinel string)
// into a float64. 'C'/'CORRECT'/'YES'/'TRUE' map to 1.0, 'P'/'PARTIAL' map to 0.5,
// 'I'/'INCORRECT'/'N'/'NOANSWER'/'NO'/'FALSE' map to 0.0, bool true/false map to 1.0/0.0,
// and numeric values are passed through unclamped. Returns false if unparseable or NaN/Inf.
func ScoreToFloat(val any) (float64, bool) {
	if val == nil {
		return 0.0, false
	}

	switch v := val.(type) {
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0.0, false
		}
		return v, true
	case float32:
		f := float64(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0.0, false
		}
		return f, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case bool:
		if v {
			return 1.0, true
		}
		return 0.0, true
	case string:
		clean := strings.ToUpper(strings.TrimSpace(v))
		switch clean {
		case "C", "CORRECT", "YES", "TRUE":
			return 1.0, true
		case "P", "PARTIAL":
			return 0.5, true
		case "I", "INCORRECT", "N", "NOANSWER", "NO", "FALSE":
			return 0.0, true
		}
		f, err := strconv.ParseFloat(clean, 64)
		if err != nil {
			return 0.0, false
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0.0, false
		}
		return f, true
	default:
		return 0.0, false
	}
}

// ParseInspectLog parses raw Inspect log JSON bytes and computes TaskAggregates.
func ParseInspectLog(data []byte) (*TaskAggregates, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, fmt.Errorf("empty log data")
	}

	var log InspectLog
	if err := json.Unmarshal(data, &log); err != nil {
		return nil, fmt.Errorf("failed to unmarshal inspect log: %w", err)
	}

	agg := &TaskAggregates{}

	// Determine preferred scorer name if declared
	primaryScorer := ""
	if len(log.Eval.Scorers) > 0 {
		primaryScorer = log.Eval.Scorers[0].Name
	} else if log.Results != nil && len(log.Results.Scores) > 0 {
		primaryScorer = log.Results.Scores[0].Name
	}

	// 1. Compute aggregates from samples if available
	if len(log.Samples) > 0 {
		var totalScore float64
		for _, s := range log.Samples {
			if len(s.Scores) == 0 {
				continue
			}

			var targetScore InspectSampleScore
			var found bool
			if primaryScorer != "" {
				targetScore, found = s.Scores[primaryScorer]
			}
			if !found {
				keys := make([]string, 0, len(s.Scores))
				for k := range s.Scores {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				if len(keys) > 0 {
					targetScore = s.Scores[keys[0]]
					found = true
				}
			}

			scoreVal, valid := ScoreToFloat(targetScore.Value)
			if !valid {
				continue
			}

			agg.Samples++
			totalScore += scoreVal
			agg.Scores = append(agg.Scores, scoreVal)
			if scoreVal >= 1.0 {
				agg.Successes++
			}
		}

		if agg.Samples > 0 {
			mean := totalScore / float64(agg.Samples)
			agg.MeanScore = &mean
		}
		return agg, nil
	}

	// 2. Fallback to Results summary if samples are omitted
	if log.Results != nil {
		if len(log.Results.Scores) > 0 {
			sc := log.Results.Scores[0]
			agg.Samples = sc.ScoredSamples
			if agg.Samples == 0 {
				agg.Samples = log.Results.CompletedSamples
			}
			if metric, ok := sc.Metrics["accuracy"]; ok {
				if f, ok := ScoreToFloat(metric.Value); ok {
					agg.MeanScore = &f
					agg.Successes = int(math.Round(f * float64(agg.Samples)))
				}
			}
		} else {
			agg.Samples = log.Results.CompletedSamples
		}
	}

	return agg, nil
}

// ParseInspectSampleScores extracts per-sample scores from Inspect JSON log bytes,
// matching the primary scorer selection used in ParseInspectLog.
// Each sample score preserves the sample ID as a string.
func ParseInspectSampleScores(data []byte) ([]stats.SampleScore, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, fmt.Errorf("empty log data")
	}

	var log InspectLog
	if err := json.Unmarshal(data, &log); err != nil {
		return nil, fmt.Errorf("failed to unmarshal inspect log: %w", err)
	}

	if len(log.Samples) == 0 {
		return nil, nil
	}

	primaryScorer := ""
	if len(log.Eval.Scorers) > 0 {
		primaryScorer = log.Eval.Scorers[0].Name
	} else if log.Results != nil && len(log.Results.Scores) > 0 {
		primaryScorer = log.Results.Scores[0].Name
	}

	var sampleScores []stats.SampleScore
	for _, s := range log.Samples {
		if len(s.Scores) == 0 {
			continue
		}

		var targetScore InspectSampleScore
		var found bool
		if primaryScorer != "" {
			targetScore, found = s.Scores[primaryScorer]
		}
		if !found {
			keys := make([]string, 0, len(s.Scores))
			for k := range s.Scores {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			if len(keys) > 0 {
				targetScore = s.Scores[keys[0]]
				found = true
			}
		}

		scoreVal, valid := ScoreToFloat(targetScore.Value)
		if !valid {
			continue
		}

		var idStr string
		switch v := s.ID.(type) {
		case float64:
			if v == math.Trunc(v) {
				idStr = strconv.FormatInt(int64(v), 10)
			} else {
				idStr = fmt.Sprint(v)
			}
		case int:
			idStr = strconv.Itoa(v)
		case int64:
			idStr = strconv.FormatInt(v, 10)
		case string:
			idStr = v
		default:
			idStr = fmt.Sprint(s.ID)
		}

		if s.ID == nil || idStr == "" || idStr == "<nil>" {
			continue
		}

		if s.Epoch > 1 {
			idStr = fmt.Sprintf("%s#%d", idStr, s.Epoch)
		}

		sampleScores = append(sampleScores, stats.SampleScore{
			ID:    idStr,
			Score: scoreVal,
		})
	}

	return sampleScores, nil
}

// ExtractInspectTokens parses raw Inspect log JSON bytes and returns token usage breakdown if present.
// It enforces the token accounting invariant Total = Input + Output across all tasks, dropping
// any provider-specific total_tokens from Inspect logs.
// Returns nil if no token counts or model usage are present in the log.
func ExtractInspectTokens(data []byte) (*TaskTokens, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, fmt.Errorf("empty log data")
	}

	var log InspectLog
	if err := json.Unmarshal(data, &log); err != nil {
		return nil, fmt.Errorf("failed to unmarshal inspect log: %w", err)
	}

	inputTokens := 0
	outputTokens := 0
	foundTokens := false

	if log.Stats != nil && len(log.Stats.ModelUsage) > 0 {
		for _, usage := range log.Stats.ModelUsage {
			if usage.TotalTokens > 0 || usage.InputTokens > 0 || usage.OutputTokens > 0 {
				inputTokens += usage.InputTokens
				outputTokens += usage.OutputTokens
				foundTokens = true
			}
		}
		if foundTokens {
			return &TaskTokens{Input: inputTokens, Output: outputTokens, Total: inputTokens + outputTokens}, nil
		}
	}

	for _, s := range log.Samples {
		if len(s.ModelUsage) > 0 {
			for _, usage := range s.ModelUsage {
				if usage.TotalTokens > 0 || usage.InputTokens > 0 || usage.OutputTokens > 0 {
					inputTokens += usage.InputTokens
					outputTokens += usage.OutputTokens
					foundTokens = true
				}
			}
		}
	}

	if foundTokens {
		return &TaskTokens{Input: inputTokens, Output: outputTokens, Total: inputTokens + outputTokens}, nil
	}
	return nil, nil
}
