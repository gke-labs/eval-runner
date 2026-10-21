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
	"time"

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
	Dataset any             `json:"dataset,omitempty"`
}

// DatasetSamples returns the dataset sample count if eval.dataset.samples is present, or 0.
func (e InspectEval) DatasetSamples() int {
	if e.Dataset == nil {
		return 0
	}
	switch d := e.Dataset.(type) {
	case map[string]any:
		if s, ok := d["samples"]; ok {
			switch v := s.(type) {
			case float64:
				return int(v)
			case int:
				return v
			case int64:
				return int(v)
			}
		}
	}
	return 0
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

// InspectToolCall captures an individual tool call initiated by the model.
type InspectToolCall struct {
	ID        string `json:"id,omitempty"`
	Type      string `json:"type,omitempty"`
	Function  string `json:"function,omitempty"`
	Arguments any    `json:"arguments,omitempty"`
}

// InspectMessage captures a conversation message in the Inspect transcript.
type InspectMessage struct {
	ID         string            `json:"id,omitempty"`
	Role       string            `json:"role,omitempty"`
	Content    any               `json:"content,omitempty"`
	Source     string            `json:"source,omitempty"`
	ToolCalls  []InspectToolCall `json:"tool_calls,omitempty"`
	ToolCallID string            `json:"tool_call_id,omitempty"`
	Function   string            `json:"function,omitempty"`
	Error      any               `json:"error,omitempty"`
}

// InspectEvent captures an execution event in the sample's event stream.
type InspectEvent struct {
	Event     string `json:"event,omitempty"`
	Type      string `json:"type,omitempty"`
	ID        string `json:"id,omitempty"`
	Function  string `json:"function,omitempty"`
	Arguments any    `json:"arguments,omitempty"`
	Result    any    `json:"result,omitempty"`
	Error     any    `json:"error,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
	Model     string `json:"model,omitempty"`
	Message   any    `json:"message,omitempty"`
	Input     any    `json:"input,omitempty"`
	Output    any    `json:"output,omitempty"`
}

// InspectTranscriptRow represents a single typed row in the sample transcript.
type InspectTranscriptRow struct {
	Kind    string // "user", "assistant", "tool_call", "tool", "system"
	Content string
}

// InspectSample represents an individual evaluated sample.
type InspectSample struct {
	ID          any                           `json:"id"`
	Epoch       int                           `json:"epoch,omitempty"`
	Input       any                           `json:"input,omitempty"`
	Target      any                           `json:"target,omitempty"`
	Output      any                           `json:"output,omitempty"`
	Messages    []InspectMessage              `json:"messages,omitempty"`
	Events      []InspectEvent                `json:"events,omitempty"`
	StartedAt   string                        `json:"started_at,omitempty"`
	CompletedAt string                        `json:"completed_at,omitempty"`
	TotalTime   float64                       `json:"total_time,omitempty"`
	Scores      map[string]InspectSampleScore `json:"scores,omitempty"`
	ModelUsage  map[string]InspectModelUsage  `json:"model_usage,omitempty"`
}

// ParsedInspectSample contains the full parsed details for a sample.
type ParsedInspectSample struct {
	ID       string
	Epoch    int
	Score    *float64
	HasScore bool
	ScoreStr string
	Status   string // "pass", "fail", "unknown"
	Input    string
	// Target is the sample's target as text: the expected answer or the rule the scorer applies.
	Target string
	// Explanation is the primary scorer's explanation for this sample's score, falling back
	// to its "reason" field. It is empty when the scorer recorded neither.
	Explanation  string
	Output       string
	Latency      *float64
	TotalTokens  int
	InputTokens  int
	OutputTokens int
	Transcript   []InspectTranscriptRow
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

// FormatSampleID formats an Inspect sample ID and optional epoch suffix according to
// eval-runner conventions: numbers become canonical strings, and epochs > 1 append #<epoch>.
func FormatSampleID(rawID any, epoch int) string {
	var idStr string
	switch v := rawID.(type) {
	case float64:
		if v == math.Trunc(v) {
			idStr = strconv.FormatInt(int64(v), 10)
		} else {
			idStr = fmt.Sprint(v)
		}
	case float32:
		f := float64(v)
		if f == math.Trunc(f) {
			idStr = strconv.FormatInt(int64(f), 10)
		} else {
			idStr = fmt.Sprint(f)
		}
	case int:
		idStr = strconv.Itoa(v)
	case int64:
		idStr = strconv.FormatInt(v, 10)
	case string:
		idStr = v
	default:
		idStr = fmt.Sprint(rawID)
	}

	if rawID == nil || idStr == "" || idStr == "<nil>" {
		return ""
	}

	if epoch > 1 {
		idStr = fmt.Sprintf("%s#%d", idStr, epoch)
	}

	return idStr
}

func selectPrimaryScore(scores map[string]InspectSampleScore, primaryScorer string) (InspectSampleScore, bool) {
	if len(scores) == 0 {
		return InspectSampleScore{}, false
	}
	if primaryScorer != "" {
		if targetScore, found := scores[primaryScorer]; found {
			return targetScore, true
		}
	}
	keys := make([]string, 0, len(scores))
	for k := range scores {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		return scores[keys[0]], true
	}
	return InspectSampleScore{}, false
}

func extractStringOrJSON(val any) string {
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		if v == math.Trunc(v) {
			return strconv.FormatInt(int64(v), 10)
		}
		return fmt.Sprint(v)
	case []any:
		var parts []string
		for _, item := range v {
			s := extractStringOrJSON(item)
			if s != "" {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		if text, ok := v["text"].(string); ok && text != "" {
			return text
		}
		if content, ok := v["content"].(string); ok && content != "" {
			return content
		}
		if completion, ok := v["completion"].(string); ok && completion != "" {
			return completion
		}
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(b)
	default:
		return fmt.Sprint(val)
	}
}

func extractOutputString(val any) string {
	if val == nil {
		return ""
	}
	if s, ok := val.(string); ok {
		return s
	}
	if m, ok := val.(map[string]any); ok {
		if comp, ok := m["completion"].(string); ok && comp != "" {
			return comp
		}
		if choices, ok := m["choices"].([]any); ok && len(choices) > 0 {
			if firstChoice, ok := choices[0].(map[string]any); ok {
				if msg, ok := firstChoice["message"].(map[string]any); ok {
					if content, ok := msg["content"].(string); ok && content != "" {
						return content
					}
				}
			}
		}
		if msg, ok := m["message"].(map[string]any); ok {
			if content, ok := msg["content"].(string); ok && content != "" {
				return content
			}
		}
	}
	return extractStringOrJSON(val)
}

func formatToolCall(function string, arguments any) string {
	if arguments == nil {
		if function == "" {
			return "tool_call()"
		}
		return function + "()"
	}
	switch a := arguments.(type) {
	case string:
		if function == "" {
			return a
		}
		return fmt.Sprintf("%s(%s)", function, a)
	case map[string]any:
		keys := make([]string, 0, len(a))
		for k := range a {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			valStr := formatArgValue(a[k])
			parts = append(parts, fmt.Sprintf("%s=%s", k, valStr))
		}
		if function == "" {
			return strings.Join(parts, ", ")
		}
		return fmt.Sprintf("%s(%s)", function, strings.Join(parts, ", "))
	default:
		b, err := json.Marshal(a)
		if err != nil {
			if function == "" {
				return fmt.Sprint(a)
			}
			return fmt.Sprintf("%s(%v)", function, a)
		}
		if function == "" {
			return string(b)
		}
		return fmt.Sprintf("%s(%s)", function, string(b))
	}
}

func formatArgValue(v any) string {
	if s, ok := v.(string); ok {
		return fmt.Sprintf("%q", s)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
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

		targetScore, found := selectPrimaryScore(s.Scores, primaryScorer)
		if !found {
			continue
		}

		scoreVal, valid := ScoreToFloat(targetScore.Value)
		if !valid {
			continue
		}

		idStr := FormatSampleID(s.ID, s.Epoch)
		if idStr == "" {
			continue
		}

		sampleScores = append(sampleScores, stats.SampleScore{
			ID:    idStr,
			Score: scoreVal,
		})
	}

	return sampleScores, nil
}

// ExtractTaskSampleSummaries parses raw Inspect JSON log bytes and returns sample summaries
// along with the primary scorer name used.
func ExtractTaskSampleSummaries(data []byte) ([]TaskSampleSummary, string, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, "", fmt.Errorf("empty log data")
	}

	var log InspectLog
	if err := json.Unmarshal(data, &log); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal inspect log: %w", err)
	}

	primaryScorer := ""
	if len(log.Eval.Scorers) > 0 {
		primaryScorer = log.Eval.Scorers[0].Name
	} else if log.Results != nil && len(log.Results.Scores) > 0 {
		primaryScorer = log.Results.Scores[0].Name
	}
	if primaryScorer == "" && len(log.Samples) > 0 {
		for _, s := range log.Samples {
			if len(s.Scores) > 0 {
				keys := make([]string, 0, len(s.Scores))
				for k := range s.Scores {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				primaryScorer = keys[0]
				break
			}
		}
	}

	if len(log.Samples) == 0 {
		return nil, primaryScorer, nil
	}

	var summaries []TaskSampleSummary
	for _, s := range log.Samples {
		idStr := FormatSampleID(s.ID, s.Epoch)
		if idStr == "" {
			continue
		}

		var score *float64
		status := "unknown"
		if targetScore, found := selectPrimaryScore(s.Scores, primaryScorer); found {
			if scoreVal, valid := ScoreToFloat(targetScore.Value); valid {
				score = &scoreVal
				if scoreVal >= 1.0 {
					status = "pass"
				} else {
					status = "fail"
				}
			}
		}

		var latency *float64
		start, okStart := parseSampleTime(s.StartedAt)
		end, okEnd := parseSampleTime(s.CompletedAt)
		if okStart && okEnd && !end.Before(start) {
			dur := end.Sub(start).Seconds()
			latency = &dur
		} else if s.TotalTime > 0 {
			dur := s.TotalTime
			latency = &dur
		}

		inputTokens := 0
		outputTokens := 0
		for _, usage := range s.ModelUsage {
			inputTokens += usage.InputTokens
			outputTokens += usage.OutputTokens
		}
		var tokens *TaskSampleTokens
		if inputTokens > 0 || outputTokens > 0 {
			tokens = &TaskSampleTokens{
				Input:  inputTokens,
				Output: outputTokens,
			}
		}

		summaries = append(summaries, TaskSampleSummary{
			ID:             idStr,
			Epoch:          s.Epoch,
			Score:          score,
			Status:         status,
			LatencySeconds: latency,
			Tokens:         tokens,
		})
	}

	return summaries, primaryScorer, nil
}

// ParseInspectSamples parses raw Inspect JSON log bytes and returns detailed sample records,
// reusing the primary scorer selection from ParseInspectLog and latency parsing from parseSampleTime.
func ParseInspectSamples(data []byte) ([]ParsedInspectSample, error) {
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

	var parsed []ParsedInspectSample
	for _, s := range log.Samples {
		idStr := FormatSampleID(s.ID, s.Epoch)
		if idStr == "" {
			continue
		}

		var score *float64
		var hasScore bool
		scoreStr := "—"
		status := "unknown"
		explanation := ""

		if targetScore, found := selectPrimaryScore(s.Scores, primaryScorer); found {
			if scoreVal, valid := ScoreToFloat(targetScore.Value); valid {
				score = &scoreVal
				hasScore = true
				scoreStr = fmt.Sprintf("%.2f", scoreVal)
				if scoreVal >= 1.0 {
					status = "pass"
				} else {
					status = "fail"
				}
			}
			explanation = strings.TrimSpace(extractStringOrJSON(targetScore.Explanation))
			if explanation == "" {
				explanation = strings.TrimSpace(targetScore.Reason)
			}
		}

		inputStr := extractStringOrJSON(s.Input)
		targetStr := extractStringOrJSON(s.Target)
		outputStr := extractOutputString(s.Output)

		var latency *float64
		start, okStart := parseSampleTime(s.StartedAt)
		end, okEnd := parseSampleTime(s.CompletedAt)
		if okStart && okEnd && !end.Before(start) {
			dur := end.Sub(start).Seconds()
			latency = &dur
		} else if s.TotalTime > 0 {
			dur := s.TotalTime
			latency = &dur
		}

		inputTokens := 0
		outputTokens := 0
		for _, usage := range s.ModelUsage {
			inputTokens += usage.InputTokens
			outputTokens += usage.OutputTokens
		}
		totalTokens := inputTokens + outputTokens

		// Build transcript rows
		var transcript []InspectTranscriptRow
		if len(s.Messages) > 0 {
			for _, m := range s.Messages {
				role := strings.ToLower(strings.TrimSpace(m.Role))
				switch role {
				case "user":
					content := extractStringOrJSON(m.Content)
					if content != "" {
						transcript = append(transcript, InspectTranscriptRow{Kind: "user", Content: content})
					}
				case "system":
					content := extractStringOrJSON(m.Content)
					if content != "" {
						transcript = append(transcript, InspectTranscriptRow{Kind: "system", Content: content})
					}
				case "assistant":
					for _, tc := range m.ToolCalls {
						transcript = append(transcript, InspectTranscriptRow{
							Kind:    "tool_call",
							Content: formatToolCall(tc.Function, tc.Arguments),
						})
					}
					content := extractStringOrJSON(m.Content)
					if content != "" {
						transcript = append(transcript, InspectTranscriptRow{Kind: "assistant", Content: content})
					}
				case "tool":
					content := extractStringOrJSON(m.Content)
					transcript = append(transcript, InspectTranscriptRow{Kind: "tool", Content: content})
				default:
					content := extractStringOrJSON(m.Content)
					if content != "" {
						transcript = append(transcript, InspectTranscriptRow{Kind: role, Content: content})
					}
				}
			}
		} else if len(s.Events) > 0 {
			for _, e := range s.Events {
				evType := strings.ToLower(strings.TrimSpace(e.Event))
				if evType == "tool" || e.Function != "" {
					transcript = append(transcript, InspectTranscriptRow{
						Kind:    "tool_call",
						Content: formatToolCall(e.Function, e.Arguments),
					})
					if e.Result != nil {
						transcript = append(transcript, InspectTranscriptRow{
							Kind:    "tool",
							Content: extractStringOrJSON(e.Result),
						})
					}
				} else if evType == "model" {
					outStr := extractOutputString(e.Output)
					if outStr != "" {
						transcript = append(transcript, InspectTranscriptRow{
							Kind:    "assistant",
							Content: outStr,
						})
					}
				} else if evType == "input" || evType == "sample_init" {
					inStr := extractStringOrJSON(e.Input)
					if inStr != "" {
						transcript = append(transcript, InspectTranscriptRow{
							Kind:    "user",
							Content: inStr,
						})
					}
				}
			}
		}

		if len(transcript) == 0 {
			if inputStr != "" {
				transcript = append(transcript, InspectTranscriptRow{Kind: "user", Content: inputStr})
			}
			if outputStr != "" {
				transcript = append(transcript, InspectTranscriptRow{Kind: "assistant", Content: outputStr})
			}
		}

		parsed = append(parsed, ParsedInspectSample{
			ID:           idStr,
			Epoch:        s.Epoch,
			Score:        score,
			HasScore:     hasScore,
			ScoreStr:     scoreStr,
			Status:       status,
			Input:        inputStr,
			Target:       targetStr,
			Explanation:  explanation,
			Output:       outputStr,
			Latency:      latency,
			TotalTokens:  totalTokens,
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
			Transcript:   transcript,
		})
	}

	return parsed, nil
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
	reasoningTokens := 0
	cacheReadTokens := 0
	cacheWriteTokens := 0
	foundTokens := false

	if log.Stats != nil && len(log.Stats.ModelUsage) > 0 {
		for _, usage := range log.Stats.ModelUsage {
			if usage.TotalTokens > 0 || usage.InputTokens > 0 || usage.OutputTokens > 0 ||
				usage.ReasoningTokens > 0 || usage.InputTokensCacheRead > 0 || usage.InputTokensCacheWrite > 0 {
				inputTokens += usage.InputTokens
				outputTokens += usage.OutputTokens
				reasoningTokens += usage.ReasoningTokens
				cacheReadTokens += usage.InputTokensCacheRead
				cacheWriteTokens += usage.InputTokensCacheWrite
				foundTokens = true
			}
		}
		if foundTokens {
			return &TaskTokens{
				Input:      inputTokens,
				Output:     outputTokens,
				Total:      inputTokens + outputTokens,
				Reasoning:  reasoningTokens,
				CacheRead:  cacheReadTokens,
				CacheWrite: cacheWriteTokens,
			}, nil
		}
	}

	for _, s := range log.Samples {
		if len(s.ModelUsage) > 0 {
			for _, usage := range s.ModelUsage {
				if usage.TotalTokens > 0 || usage.InputTokens > 0 || usage.OutputTokens > 0 ||
					usage.ReasoningTokens > 0 || usage.InputTokensCacheRead > 0 || usage.InputTokensCacheWrite > 0 {
					inputTokens += usage.InputTokens
					outputTokens += usage.OutputTokens
					reasoningTokens += usage.ReasoningTokens
					cacheReadTokens += usage.InputTokensCacheRead
					cacheWriteTokens += usage.InputTokensCacheWrite
					foundTokens = true
				}
			}
		}
	}

	if foundTokens {
		return &TaskTokens{
			Input:      inputTokens,
			Output:     outputTokens,
			Total:      inputTokens + outputTokens,
			Reasoning:  reasoningTokens,
			CacheRead:  cacheReadTokens,
			CacheWrite: cacheWriteTokens,
		}, nil
	}
	return nil, nil
}

// parseSampleTime attempts to parse an RFC3339 or ISO timestamp.
func parseSampleTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return time.Time{}, false
		}
	}
	return t, true
}

// ExtractInspectSampleLatencies parses raw Inspect log JSON bytes and returns individual
// sample latencies in seconds derived from sample start/end timestamps.
func ExtractInspectSampleLatencies(data []byte) ([]float64, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, fmt.Errorf("empty log data")
	}

	var log InspectLog
	if err := json.Unmarshal(data, &log); err != nil {
		return nil, fmt.Errorf("failed to unmarshal inspect log: %w", err)
	}

	var latencies []float64
	for _, s := range log.Samples {
		start, okStart := parseSampleTime(s.StartedAt)
		end, okEnd := parseSampleTime(s.CompletedAt)
		if okStart && okEnd && !end.Before(start) {
			latencies = append(latencies, end.Sub(start).Seconds())
		} else if s.TotalTime > 0 {
			latencies = append(latencies, s.TotalTime)
		}
	}
	return latencies, nil
}

// CalculateLatencyPercentiles computes p50 and p95 latency in seconds from a slice of latencies.
// Returns nil if latencies is empty.
func CalculateLatencyPercentiles(latencies []float64) *TaskLatency {
	if len(latencies) == 0 {
		return nil
	}
	return &TaskLatency{
		P50: stats.Percentile(latencies, 0.50),
		P95: stats.Percentile(latencies, 0.95),
	}
}

// ExtractInspectLatency parses raw Inspect log JSON bytes and returns the aggregated p50 and p95
// latencies in seconds if sample timestamps are present.
func ExtractInspectLatency(data []byte) (*TaskLatency, error) {
	latencies, err := ExtractInspectSampleLatencies(data)
	if err != nil {
		return nil, err
	}
	return CalculateLatencyPercentiles(latencies), nil
}

// ExtractInspectDatasetSamples parses raw Inspect log JSON bytes and returns the dataset size (M)
// from eval.dataset.samples if present, or 0 if omitted.
func ExtractInspectDatasetSamples(data []byte) (int, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return 0, fmt.Errorf("empty log data")
	}

	var log InspectLog
	if err := json.Unmarshal(data, &log); err != nil {
		return 0, fmt.Errorf("failed to unmarshal inspect log: %w", err)
	}
	return log.Eval.DatasetSamples(), nil
}

// ExtractInspectProgress parses raw Inspect log JSON bytes and returns the count of completed
// samples and dataset sample count from eval.dataset.samples. It decodes only the samples array
// length and eval metadata for context and memory efficiency during live polling.
func ExtractInspectProgress(data []byte) (samplesCount int, datasetSamples int, err error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return 0, 0, fmt.Errorf("empty log data")
	}

	var prog struct {
		Eval    InspectEval `json:"eval"`
		Samples []struct{}  `json:"samples"`
	}
	if err := json.Unmarshal(data, &prog); err != nil {
		return 0, 0, fmt.Errorf("failed to unmarshal inspect progress: %w", err)
	}
	return len(prog.Samples), prog.Eval.DatasetSamples(), nil
}
