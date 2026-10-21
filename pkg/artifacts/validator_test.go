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
	"fmt"
	"strings"
	"testing"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
)

func TestValidateSafeComponent(t *testing.T) {
	valid := []string{
		"abc1234",
		"my-comp-1",
		"result.json",
		"eval_runner",
		"v1.0.0",
	}
	for _, v := range valid {
		if err := ValidateSafeComponent("test", v); err != nil {
			t.Errorf("Expected valid for %q, got error: %v", v, err)
		}
	}

	invalid := []struct {
		val string
		msg string
	}{
		{"", "must not be empty"},
		{"   ", "must not be empty"},
		{"a/b", "invalid characters"},
		{"a\\b", "invalid characters"},
		{"..", "invalid characters"},
		{"a..b", "invalid characters"},
		{"foo\x00bar", "invalid characters"},
		{"foo\nbar", "invalid control characters"},
		{"foo\tbar", "invalid control characters"},
		{"foo\rbar", "invalid control characters"},
	}
	for _, tc := range invalid {
		err := ValidateSafeComponent("test", tc.val)
		if err == nil {
			t.Errorf("Expected error for %q, got nil", tc.val)
		} else if !strings.Contains(err.Error(), tc.msg) {
			t.Errorf("Expected error containing %q for %q, got: %v", tc.msg, tc.val, err)
		}
	}
}

func TestValidateTaskID(t *testing.T) {
	valid := []string{
		"task-1",
		"docs/readme_comprehension",
		"a/b/c/d",
		"benchmark_2026",
	}
	for _, v := range valid {
		if err := ValidateTaskID(v); err != nil {
			t.Errorf("Expected valid for %q, got error: %v", v, err)
		}
	}

	invalid := []string{
		"",
		"   ",
		"/leading",
		"trailing/",
		"docs//readme",
		"docs/./readme",
		"docs/../readme",
		"docs\\readme",
		"docs/\x00/readme",
		"docs/\n/readme",
	}
	for _, v := range invalid {
		if err := ValidateTaskID(v); err == nil {
			t.Errorf("Expected error for %q, got nil", v)
		}
	}
}

func TestValidateArtifact_Valid(t *testing.T) {
	meta := RunMetadata{
		Commit: "1234567890ab",
		TaskID: "docs/readme_comprehension",
	}
	if err := ValidateArtifact(meta, "log.json"); err != nil {
		t.Fatalf("Expected valid artifact to pass validation, got: %v", err)
	}
}

func TestValidateTaskSummary(t *testing.T) {
	rate := 0.8
	score := 1.0
	valid := TaskSummary{
		SchemaVersion:     1,
		TaskID:            "docs/readme_comprehension",
		Model:             "gemini-2.5-flash",
		Commit:            "1234567890ab",
		RunName:           "run-1",
		RunNamespace:      "eval-runner",
		ControllerVersion: "v1.0.0",
		Scorer:            "choice",
		SampleCount:       1,
		SuccessRate:       &rate,
		SuccessRateCI: &v1alpha1.ConfidenceInterval{
			Lower:      0.6,
			Upper:      0.9,
			Confidence: 0.95,
			Method:     "wilson",
		},
		Samples: []TaskSampleSummary{
			{
				ID:    "s1",
				Score: &score,
			},
		},
	}

	if err := ValidateTaskSummary(valid); err != nil {
		t.Fatalf("Expected valid TaskSummary to pass, got: %v", err)
	}

	// Unsupported schema version
	invalidSchema := valid
	invalidSchema.SchemaVersion = 2
	if err := ValidateTaskSummary(invalidSchema); err == nil {
		t.Errorf("Expected error for schemaVersion=2, got nil")
	}

	// Invalid TaskID
	invalidTaskID := valid
	invalidTaskID.TaskID = "/invalid"
	if err := ValidateTaskSummary(invalidTaskID); err == nil {
		t.Errorf("Expected error for invalid TaskID, got nil")
	}

	// Invalid commit
	invalidCommit := valid
	invalidCommit.Commit = "../escape"
	if err := ValidateTaskSummary(invalidCommit); err == nil {
		t.Errorf("Expected error for invalid commit, got nil")
	}

	// Invalid runName
	invalidRunName := valid
	invalidRunName.RunName = "foo/bar"
	if err := ValidateTaskSummary(invalidRunName); err == nil {
		t.Errorf("Expected error for invalid runName, got nil")
	}

	// Invalid runNamespace
	invalidRunNamespace := valid
	invalidRunNamespace.RunNamespace = "foo\x00bar"
	if err := ValidateTaskSummary(invalidRunNamespace); err == nil {
		t.Errorf("Expected error for invalid runNamespace, got nil")
	}

	// Invalid successRate (< 0 or > 1)
	invalidRate := valid
	badRate := 1.5
	invalidRate.SuccessRate = &badRate
	if err := ValidateTaskSummary(invalidRate); err == nil {
		t.Errorf("Expected error for successRate=1.5, got nil")
	}

	// Invalid confidence interval (lower > upper)
	invalidCI := valid
	invalidCI.SuccessRateCI = &v1alpha1.ConfidenceInterval{
		Lower:      0.9,
		Upper:      0.5,
		Confidence: 0.95,
		Method:     "wilson",
	}
	if err := ValidateTaskSummary(invalidCI); err == nil {
		t.Errorf("Expected error for lower > upper in CI, got nil")
	}

	// Empty CI method
	emptyMethodCI := valid
	emptyMethodCI.SuccessRateCI = &v1alpha1.ConfidenceInterval{
		Lower:      0.5,
		Upper:      0.9,
		Confidence: 0.95,
		Method:     "",
	}
	if err := ValidateTaskSummary(emptyMethodCI); err == nil {
		t.Errorf("Expected error for empty CI method, got nil")
	}

	// Samples count exceeds MaxSummarySamples
	oversizedSamples := valid
	oversizedList := make([]TaskSampleSummary, MaxSummarySamples+1)
	for i := range oversizedList {
		oversizedList[i] = TaskSampleSummary{ID: fmt.Sprintf("sample-%d", i+1)}
	}
	oversizedSamples.Samples = oversizedList
	if err := ValidateTaskSummary(oversizedSamples); err == nil {
		t.Errorf("Expected error for samples exceeding MaxSummarySamples, got nil")
	}

	// Sample with empty ID
	emptySampleID := valid
	emptySampleID.Samples = []TaskSampleSummary{{ID: "   "}}
	if err := ValidateTaskSummary(emptySampleID); err == nil {
		t.Errorf("Expected error for sample with empty ID, got nil")
	}

	// Sample with invalid score
	badSampleScore := valid
	badScoreVal := 2.5
	badSampleScore.Samples = []TaskSampleSummary{{ID: "s1", Score: &badScoreVal}}
	if err := ValidateTaskSummary(badSampleScore); err == nil {
		t.Errorf("Expected error for sample with score=2.5, got nil")
	}

	// Negative SampleCount
	negativeCount := valid
	negativeCount.SampleCount = -5
	if err := ValidateTaskSummary(negativeCount); err == nil {
		t.Errorf("Expected error for negative sampleCount, got nil")
	}
}
