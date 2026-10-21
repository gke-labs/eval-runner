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
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// TaskSummaryURI resolves the summary.json URI or path for a given artifact or log URI.
// If uri is already a summary.json URI or path, it is returned trimmed.
func TaskSummaryURI(uri string) string {
	cleanURI := strings.TrimSpace(uri)
	if strings.HasSuffix(cleanURI, "/summary.json") || cleanURI == "summary.json" {
		return cleanURI
	}
	idx := strings.LastIndex(cleanURI, "/")
	if idx == -1 {
		return "summary.json"
	}
	return cleanURI[:idx+1] + "summary.json"
}

// ReadTaskSummary loads and parses summary.json for a task from the specified ArtifactSink.
// uri may be the direct summary.json URI or an Inspect log file URI.
func ReadTaskSummary(ctx context.Context, sink ArtifactSink, uri string) (*TaskSummary, error) {
	if sink == nil {
		return nil, fmt.Errorf("artifact sink cannot be nil")
	}
	cleanURI := strings.TrimSpace(uri)
	if cleanURI == "" {
		return nil, fmt.Errorf("uri cannot be empty")
	}

	summaryURI := TaskSummaryURI(cleanURI)

	data, err := sink.Load(ctx, summaryURI)
	if err != nil {
		return nil, err
	}

	var summary TaskSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		return nil, fmt.Errorf("failed to parse summary.json from %s: %w", summaryURI, err)
	}

	if err := ValidateTaskSummary(summary); err != nil {
		return nil, fmt.Errorf("invalid summary.json at %s: %w", summaryURI, err)
	}

	return &summary, nil
}
