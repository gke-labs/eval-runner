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
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestParseInspectLog_Binary20_17(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "binary_20_17.json"))
	if err != nil {
		t.Fatalf("Failed to read testdata: %v", err)
	}

	agg, err := ParseInspectLog(data)
	if err != nil {
		t.Fatalf("ParseInspectLog returned unexpected error: %v", err)
	}

	if agg.Samples != 20 {
		t.Errorf("Expected 20 samples, got %d", agg.Samples)
	}
	if len(agg.Scores) != 20 {
		t.Errorf("Expected 20 per-sample scores, got %d", len(agg.Scores))
	}
	if !agg.IsBinary() {
		t.Errorf("Expected binary_20_17 to be binary")
	}
	if agg.Successes != 17 {
		t.Errorf("Expected 17 successes, got %d", agg.Successes)
	}
	if agg.MeanScore == nil {
		t.Fatalf("Expected non-nil MeanScore")
	}
	if math.Abs(*agg.MeanScore-0.85) > 1e-6 {
		t.Errorf("Expected meanScore 0.85, got %f", *agg.MeanScore)
	}
}

func TestParseInspectLog_MockLLM2(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "mockllm_2.json"))
	if err != nil {
		t.Fatalf("Failed to read testdata: %v", err)
	}

	agg, err := ParseInspectLog(data)
	if err != nil {
		t.Fatalf("ParseInspectLog returned unexpected error: %v", err)
	}

	if agg.Samples != 2 {
		t.Errorf("Expected 2 samples, got %d", agg.Samples)
	}
	if agg.Successes != 0 {
		t.Errorf("Expected 0 successes, got %d", agg.Successes)
	}
	if agg.MeanScore == nil {
		t.Fatalf("Expected non-nil MeanScore")
	}
	if *agg.MeanScore != 0.0 {
		t.Errorf("Expected meanScore 0.0, got %f", *agg.MeanScore)
	}
}

func TestParseInspectLog_MixedScores(t *testing.T) {
	rawJSON := `{
		"version": 2,
		"eval": {"task": "test", "scorers": [{"name": "scorer1"}]},
		"samples": [
			{"id": 1, "scores": {"scorer1": {"value": 1.0}}},
			{"id": 2, "scores": {"scorer1": {"value": 0.5}}},
			{"id": 3, "scores": {"scorer1": {"value": "P"}}},
			{"id": 4, "scores": {"scorer1": {"value": true}}},
			{"id": 5, "scores": {"scorer1": {"value": false}}},
			{"id": 6, "scores": {"scorer1": {"value": "0.75"}}}
		]
	}`

	agg, err := ParseInspectLog([]byte(rawJSON))
	if err != nil {
		t.Fatalf("ParseInspectLog returned unexpected error: %v", err)
	}

	// Scores: 1.0 + 0.5 + 0.5 + 1.0 + 0.0 + 0.75 = 3.75 over 6 samples
	if agg.Samples != 6 {
		t.Errorf("Expected 6 samples, got %d", agg.Samples)
	}
	if len(agg.Scores) != 6 {
		t.Errorf("Expected 6 per-sample scores, got %d", len(agg.Scores))
	}
	if agg.IsBinary() {
		t.Errorf("Expected MixedScores to be non-binary")
	}
	// Successes (score >= 1.0): samples 1 and 4 -> 2
	if agg.Successes != 2 {
		t.Errorf("Expected 2 successes, got %d", agg.Successes)
	}
	if agg.MeanScore == nil {
		t.Fatalf("Expected non-nil MeanScore")
	}
	expectedMean := 3.75 / 6.0
	if math.Abs(*agg.MeanScore-expectedMean) > 1e-6 {
		t.Errorf("Expected meanScore %f, got %f", expectedMean, *agg.MeanScore)
	}
}

func TestParseInspectLog_ResultsFallback(t *testing.T) {
	rawJSON := `{
		"version": 2,
		"eval": {"task": "test"},
		"results": {
			"completed_samples": 10,
			"scores": [
				{
					"name": "choice",
					"scored_samples": 10,
					"metrics": {
						"accuracy": {"value": 0.7}
					}
				}
			]
		}
	}`

	agg, err := ParseInspectLog([]byte(rawJSON))
	if err != nil {
		t.Fatalf("ParseInspectLog returned unexpected error: %v", err)
	}

	if agg.Samples != 10 {
		t.Errorf("Expected 10 samples, got %d", agg.Samples)
	}
	if agg.Successes != 7 {
		t.Errorf("Expected 7 successes, got %d", agg.Successes)
	}
	if agg.MeanScore == nil || math.Abs(*agg.MeanScore-0.7) > 1e-6 {
		t.Errorf("Expected meanScore 0.7, got %v", agg.MeanScore)
	}
}

func TestParseInspectLog_InvalidAndEmpty(t *testing.T) {
	if _, err := ParseInspectLog([]byte("")); err == nil {
		t.Error("Expected error for empty bytes")
	}
	if _, err := ParseInspectLog([]byte("   \n\t ")); err == nil {
		t.Error("Expected error for whitespace-only bytes")
	}
	if _, err := ParseInspectLog([]byte("not-json")); err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestScoreToFloat(t *testing.T) {
	tests := []struct {
		input     any
		wantVal   float64
		wantValid bool
	}{
		{nil, 0.0, false},
		{1.0, 1.0, true},
		{float32(0.5), 0.5, true},
		{1, 1.0, true},
		{int64(0), 0.0, true},
		{true, 1.0, true},
		{false, 0.0, true},
		{"C", 1.0, true},
		{"CORRECT", 1.0, true},
		{"yes", 1.0, true},
		{"true", 1.0, true},
		{"P", 0.5, true},
		{"PARTIAL", 0.5, true},
		{"I", 0.0, true},
		{"INCORRECT", 0.0, true},
		{"N", 0.0, true},
		{"NOANSWER", 0.0, true},
		{"no", 0.0, true},
		{"false", 0.0, true},
		{"0.85", 0.85, true},
		{"invalid", 0.0, false},
		{math.NaN(), 0.0, false},
		{math.Inf(1), 0.0, false},
	}

	for _, tt := range tests {
		val, valid := ScoreToFloat(tt.input)
		if valid != tt.wantValid {
			t.Errorf("ScoreToFloat(%v) valid=%v, want %v", tt.input, valid, tt.wantValid)
		}
		if valid && math.Abs(val-tt.wantVal) > 1e-6 {
			t.Errorf("ScoreToFloat(%v) val=%f, want %f", tt.input, val, tt.wantVal)
		}
	}
}

func TestParseInspectLog_MultipleScorersDeterministic(t *testing.T) {
	// Scorers without explicit primary scorer: "beta" has score 0.0, "alpha" has score 1.0.
	// Since keys are sorted alphabetically, "alpha" should always be chosen deterministically.
	rawJSON := `{
		"version": 2,
		"eval": {"task": "multi-scorer-test"},
		"samples": [
			{
				"id": 1,
				"scores": {
					"beta": {"value": 0.0},
					"alpha": {"value": 1.0}
				}
			}
		]
	}`

	for range 10 {
		agg, err := ParseInspectLog([]byte(rawJSON))
		if err != nil {
			t.Fatalf("ParseInspectLog failed: %v", err)
		}
		if agg.Samples != 1 {
			t.Errorf("Expected 1 sample, got %d", agg.Samples)
		}
		if agg.Successes != 1 {
			t.Errorf("Expected 1 success (alpha chosen), got %d", agg.Successes)
		}
		if agg.MeanScore == nil || *agg.MeanScore != 1.0 {
			t.Errorf("Expected meanScore 1.0, got %v", agg.MeanScore)
		}
	}
}

func TestParseInspectSampleScores(t *testing.T) {
	t.Run("Binary20_17", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join("testdata", "binary_20_17.json"))
		if err != nil {
			t.Fatalf("Failed to read testdata: %v", err)
		}

		scores, err := ParseInspectSampleScores(data)
		if err != nil {
			t.Fatalf("ParseInspectSampleScores failed: %v", err)
		}

		if len(scores) != 20 {
			t.Fatalf("Expected 20 sample scores, got %d", len(scores))
		}

		if scores[0].ID != "1" || scores[0].Score != 1.0 {
			t.Errorf("Expected sample 0 to be ID '1' with score 1.0, got %+v", scores[0])
		}
		if scores[17].ID != "18" || scores[17].Score != 0.0 {
			t.Errorf("Expected sample 17 to be ID '18' with score 0.0, got %+v", scores[17])
		}
	})

	t.Run("MockLLM2", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join("testdata", "mockllm_2.json"))
		if err != nil {
			t.Fatalf("Failed to read testdata: %v", err)
		}

		scores, err := ParseInspectSampleScores(data)
		if err != nil {
			t.Fatalf("ParseInspectSampleScores failed: %v", err)
		}

		if len(scores) != 2 {
			t.Fatalf("Expected 2 sample scores, got %d", len(scores))
		}

		if scores[0].ID != "1" || scores[0].Score != 0.0 {
			t.Errorf("Expected sample 0 ID '1' score 0.0, got %+v", scores[0])
		}
		if scores[1].ID != "2" || scores[1].Score != 0.0 {
			t.Errorf("Expected sample 1 ID '2' score 0.0, got %+v", scores[1])
		}
	})

	t.Run("EmptyAndInvalid", func(t *testing.T) {
		if _, err := ParseInspectSampleScores([]byte("")); err == nil {
			t.Errorf("Expected error for empty input")
		}
		if _, err := ParseInspectSampleScores([]byte("{invalid")); err == nil {
			t.Errorf("Expected error for invalid JSON")
		}
		emptySamples, err := ParseInspectSampleScores([]byte(`{"version": 2, "samples": []}`))
		if err != nil {
			t.Errorf("Unexpected error for empty samples: %v", err)
		}
		if len(emptySamples) != 0 {
			t.Errorf("Expected 0 sample scores, got %d", len(emptySamples))
		}
	})

	t.Run("EpochAndLargeID", func(t *testing.T) {
		raw := `{
			"version": 2,
			"samples": [
				{"id": 120000000, "epoch": 1, "scores": {"accuracy": {"value": 1.0}}},
				{"id": 120000000, "epoch": 2, "scores": {"accuracy": {"value": 0.5}}}
			]
		}`
		scores, err := ParseInspectSampleScores([]byte(raw))
		if err != nil {
			t.Fatalf("ParseInspectSampleScores failed: %v", err)
		}
		if len(scores) != 2 {
			t.Fatalf("Expected 2 scores, got %d", len(scores))
		}
		if scores[0].ID != "120000000" || scores[0].Score != 1.0 {
			t.Errorf("Expected score 0 to be ID '120000000' and score 1.0, got %+v", scores[0])
		}
		if scores[1].ID != "120000000#2" || scores[1].Score != 0.5 {
			t.Errorf("Expected score 1 to be ID '120000000#2' and score 0.5, got %+v", scores[1])
		}
	})
}

func TestExtractInspectTokens(t *testing.T) {
	t.Run("StatsModelUsagePresent", func(t *testing.T) {
		raw := `{
			"version": 2,
			"status": "success",
			"stats": {
				"model_usage": {
					"mockllm/model": {
						"input_tokens": 258,
						"output_tokens": 66,
						"total_tokens": 324
					}
				}
			}
		}`
		tokens, err := ExtractInspectTokens([]byte(raw))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tokens == nil {
			t.Fatalf("expected non-nil tokens")
		}
		if tokens.Input != 258 || tokens.Output != 66 || tokens.Total != 324 {
			t.Fatalf("expected input=258, output=66, total=324, got %+v", tokens)
		}
	})

	t.Run("SamplesModelUsageFallback", func(t *testing.T) {
		raw := `{
			"version": 2,
			"status": "success",
			"samples": [
				{
					"id": 1,
					"model_usage": {
						"mockllm/model": {"input_tokens": 100, "output_tokens": 20, "total_tokens": 120}
					}
				},
				{
					"id": 2,
					"model_usage": {
						"mockllm/model": {"input_tokens": 50, "output_tokens": 10, "total_tokens": 60}
					}
				}
			]
		}`
		tokens, err := ExtractInspectTokens([]byte(raw))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tokens == nil {
			t.Fatalf("expected non-nil tokens")
		}
		if tokens.Input != 150 || tokens.Output != 30 || tokens.Total != 180 {
			t.Fatalf("expected input=150, output=30, total=180, got %+v", tokens)
		}
	})

	t.Run("FixtureWithCacheAndReasoningEnforcesInvariant", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join("testdata", "model_usage_cache_reasoning.json"))
		if err != nil {
			t.Fatalf("failed to read fixture: %v", err)
		}
		tokens, err := ExtractInspectTokens(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tokens == nil {
			t.Fatalf("expected non-nil tokens")
		}
		// Invariant: Total == Input + Output, dropping Inspect's total_tokens (272146).
		if tokens.Input != 140708 {
			t.Errorf("expected input=140708, got %d", tokens.Input)
		}
		if tokens.Output != 788 {
			t.Errorf("expected output=788, got %d", tokens.Output)
		}
		expectedTotal := tokens.Input + tokens.Output
		if tokens.Total != expectedTotal {
			t.Errorf("token accounting invariant violated: total %d != input %d + output %d", tokens.Total, tokens.Input, tokens.Output)
		}
		if tokens.Total != 141496 {
			t.Errorf("expected total=141496, got %d", tokens.Total)
		}
	})

	t.Run("NoTokensReturnsNil", func(t *testing.T) {
		raw := `{
			"version": 2,
			"status": "success",
			"samples": [
				{"id": 1, "scores": {"accuracy": {"value": 1.0}}}
			]
		}`
		tokens, err := ExtractInspectTokens([]byte(raw))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tokens != nil {
			t.Fatalf("expected nil tokens, got %v", tokens)
		}
	})
}
