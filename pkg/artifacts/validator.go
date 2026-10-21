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
)

// Documented Path Validation Rules:
//
// 1. Task IDs:
//    - May contain '/' as a hierarchical namespace separator (e.g. "docs/readme_comprehension").
//    - Must not be empty or whitespace-only.
//    - Must not start or end with a leading or trailing '/'.
//    - Must not contain path traversal elements (".."), backslashes ('\'), null bytes, or control characters.
//    - Each '/'-delimited path segment must be non-empty and must not be "." or "..".
//
// 2. Safe Components (commits, filenames, comparison names, run names, namespaces):
//    - Restricted to a safe charset without path separators.
//    - Must not be empty or whitespace-only.
//    - Must not contain '/' or '\' (and therefore no leading '/').
//    - Must not contain path traversal elements (".."), null bytes, or control characters.

// ValidateSafeComponent validates that a path component (e.g. commit, filename, comparison name)
// is non-empty, contains no slashes, backslashes, directory traversal sequences, or control characters.
func ValidateSafeComponent(name, val string) error {
	if strings.TrimSpace(val) == "" {
		return fmt.Errorf("%s must not be empty", name)
	}
	if strings.Contains(val, "/") || strings.Contains(val, "\\") || strings.Contains(val, "..") || strings.ContainsRune(val, 0) {
		return fmt.Errorf("%s %q contains invalid characters or traversal elements", name, val)
	}
	for _, r := range val {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%s %q contains invalid control characters", name, val)
		}
	}
	return nil
}

// ValidateTaskID validates that taskID is non-empty and adheres to task ID path rules:
// '/' is permitted as a namespace separator, but no leading/trailing slashes, empty segments,
// '.', '..', backslashes, null bytes, or control characters are allowed.
func ValidateTaskID(taskID string) error {
	if strings.TrimSpace(taskID) == "" {
		return fmt.Errorf("taskID must not be empty")
	}
	if strings.Contains(taskID, "\\") || strings.Contains(taskID, "..") || strings.ContainsRune(taskID, 0) {
		return fmt.Errorf("taskID %q contains invalid characters or traversal elements", taskID)
	}
	for _, r := range taskID {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("taskID %q contains invalid control characters", taskID)
		}
	}
	if strings.HasPrefix(taskID, "/") || strings.HasSuffix(taskID, "/") {
		return fmt.Errorf("taskID %q must not start or end with a slash", taskID)
	}
	parts := strings.Split(taskID, "/")
	for _, p := range parts {
		if p == "" || p == "." || p == ".." {
			return fmt.Errorf("taskID %q contains empty or invalid path segment %q", taskID, p)
		}
	}
	return nil
}

// ValidateArtifact validates the components of an artifact path:
// commit (or runNamespace/runName if commit is absent) and filename must be safe components,
// and taskID may contain '/' as a namespace separator.
func ValidateArtifact(meta RunMetadata, filename string) error {
	if meta.Commit != "" {
		if err := ValidateSafeComponent("commit", meta.Commit); err != nil {
			return err
		}
	} else if meta.RunNamespace != "" || meta.RunName != "" {
		if err := ValidateSafeComponent("runNamespace", meta.RunNamespace); err != nil {
			return err
		}
		if err := ValidateSafeComponent("runName", meta.RunName); err != nil {
			return err
		}
	} else {
		if err := ValidateSafeComponent("commit", meta.Commit); err != nil {
			return err
		}
	}
	if err := ValidateTaskID(meta.TaskID); err != nil {
		return err
	}
	if err := ValidateSafeComponent("filename", filename); err != nil {
		return err
	}
	return nil
}

// ValidateTaskSummary validates the structural and semantic constraints of a TaskSummary.
func ValidateTaskSummary(s TaskSummary) error {
	if s.SchemaVersion != 0 && s.SchemaVersion != 1 {
		return fmt.Errorf("unsupported schemaVersion %d: must be 0 or 1", s.SchemaVersion)
	}
	if err := ValidateTaskID(s.TaskID); err != nil {
		return err
	}
	if s.Commit != "" {
		if err := ValidateSafeComponent("commit", s.Commit); err != nil {
			return err
		}
	}
	if s.RunName != "" {
		if err := ValidateSafeComponent("runName", s.RunName); err != nil {
			return err
		}
	}
	if s.RunNamespace != "" {
		if err := ValidateSafeComponent("runNamespace", s.RunNamespace); err != nil {
			return err
		}
	}
	if s.MeanScore != nil {
		if *s.MeanScore < 0.0 || *s.MeanScore > 1.0 {
			return fmt.Errorf("meanScore %f out of range [0, 1]", *s.MeanScore)
		}
	}
	if s.SuccessRate != nil {
		if *s.SuccessRate < 0.0 || *s.SuccessRate > 1.0 {
			return fmt.Errorf("successRate %f out of range [0, 1]", *s.SuccessRate)
		}
	}
	if s.SuccessRateCI != nil {
		if s.SuccessRateCI.Confidence <= 0 || s.SuccessRateCI.Confidence > 1 {
			return fmt.Errorf("confidence %f out of range (0, 1]", s.SuccessRateCI.Confidence)
		}
		if s.SuccessRateCI.Lower < 0 || s.SuccessRateCI.Upper > 1 || s.SuccessRateCI.Lower > s.SuccessRateCI.Upper {
			return fmt.Errorf("invalid confidence interval [%f, %f]", s.SuccessRateCI.Lower, s.SuccessRateCI.Upper)
		}
		if strings.TrimSpace(s.SuccessRateCI.Method) == "" {
			return fmt.Errorf("successRateCI method must not be empty")
		}
	}
	if s.SampleRange != nil {
		if s.SampleRange.Start <= 0 || s.SampleRange.End < s.SampleRange.Start {
			return fmt.Errorf("invalid sample range [%d, %d]", s.SampleRange.Start, s.SampleRange.End)
		}
	}
	if len(s.Samples) > MaxSummarySamples {
		return fmt.Errorf("samples count %d exceeds maximum allowed %d", len(s.Samples), MaxSummarySamples)
	}
	for i, sample := range s.Samples {
		if strings.TrimSpace(sample.ID) == "" {
			return fmt.Errorf("sample at index %d has empty id", i)
		}
		if sample.Score != nil && (*sample.Score < 0.0 || *sample.Score > 1.0) {
			return fmt.Errorf("sample %q score %f out of range [0, 1]", sample.ID, *sample.Score)
		}
	}
	return nil
}
