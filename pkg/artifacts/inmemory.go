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
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// InMemorySink is an in-memory implementation of ArtifactSink.
// It is non-durable (restarts lose stored artifacts).
type InMemorySink struct {
	mu       sync.RWMutex
	objects  map[string][]byte
	metadata map[string]map[string]string
	maxBytes int64
}

var _ ArtifactSink = (*InMemorySink)(nil)

// NewInMemorySink creates a new InMemorySink.
func NewInMemorySink() *InMemorySink {
	return &InMemorySink{
		objects:  make(map[string][]byte),
		metadata: make(map[string]map[string]string),
		maxBytes: DefaultMaxLoadBytes,
	}
}

// IsDurable reports whether the sink provides durable storage across restarts.
func (s *InMemorySink) IsDurable() bool {
	return false
}

// ArtifactPrefix returns the run-level storage prefix for a given commit.
func (s *InMemorySink) ArtifactPrefix(commit string) string {
	if strings.TrimSpace(commit) == "" {
		return ""
	}
	return fmt.Sprintf("mem://runs/%s/", commit)
}

// Close implements ArtifactSink for InMemorySink.
func (s *InMemorySink) Close() error {
	return nil
}

// StoreManifest persists the discovered manifest JSON as runs/<commit>/manifest.json.
// Writing the same object twice for the same commit is a no-op when the hash matches.
func (s *InMemorySink) StoreManifest(ctx context.Context, meta RunMetadata, data []byte) (string, error) {
	if err := ValidateSafeComponent("commit", meta.Commit); err != nil {
		return "", err
	}

	limit := s.maxBytes
	if limit <= 0 {
		limit = DefaultMaxLoadBytes
	}
	if int64(len(data)) > limit {
		return "", fmt.Errorf("manifest size %d exceeds maximum allowed size of %d bytes", len(data), limit)
	}

	objKey := fmt.Sprintf("runs/%s/manifest.json", meta.Commit)
	uri := fmt.Sprintf("mem://%s", objKey)

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.objects[objKey]; ok {
		if bytes.Equal(existing, data) {
			return uri, nil
		}
	}

	copied := make([]byte, len(data))
	copy(copied, data)
	s.objects[objKey] = copied

	s.metadata[objKey] = map[string]string{
		"run":        meta.RunName,
		"commit":     meta.Commit,
		"task-count": strconv.Itoa(meta.TaskCount),
	}

	return uri, nil
}

// StoreArtifact persists the raw log bytes for a task.
func (s *InMemorySink) StoreArtifact(ctx context.Context, meta RunMetadata, filename string, data []byte) (string, error) {
	if err := ValidateArtifact(meta, filename); err != nil {
		return "", err
	}

	var objKey string
	if meta.Commit != "" {
		objKey = fmt.Sprintf("runs/%s/%s/%s", meta.Commit, meta.TaskID, filename)
	} else {
		objKey = fmt.Sprintf("runs/%s/%s/%s/%s", meta.RunNamespace, meta.RunName, meta.TaskID, filename)
	}
	uri := fmt.Sprintf("mem://%s", objKey)

	s.mu.Lock()
	defer s.mu.Unlock()

	copied := make([]byte, len(data))
	copy(copied, data)
	s.objects[objKey] = copied

	sampleList := meta.Samples
	samplesTruncated := meta.SamplesTruncated
	if len(sampleList) > MaxSummarySamples {
		sampleList = sampleList[:MaxSummarySamples]
		samplesTruncated = true
	}
	sampleCount := meta.SampleCount

	summary := TaskSummary{
		SchemaVersion:     1,
		TaskID:            meta.TaskID,
		Model:             meta.Model,
		Commit:            meta.Commit,
		LogFilename:       filename,
		RunName:           meta.RunName,
		RunNamespace:      meta.RunNamespace,
		ControllerVersion: meta.ControllerVersion,
		Scorer:            meta.Scorer,
		SampleCount:       sampleCount,
		Samples:           sampleList,
		SamplesTruncated:  samplesTruncated,
		Successes:         meta.Successes,
		MeanScore:         meta.MeanScore,
		SuccessRate:       meta.SuccessRate,
		SuccessRateCI:     meta.SuccessRateCI,
		SuccessRateSD:     meta.SuccessRateSD,
		StartedAt:         meta.StartedAt,
		FinishedAt:        meta.FinishedAt,
		SampleRange:       meta.SampleRange,
		Tokens:            meta.Tokens,
		Latency:           meta.Latency,
		CostUSD:           meta.CostUSD,
		CostSource:        meta.CostSource,
		PossibleSamples:   meta.PossibleSamples,
	}
	summaryBytes, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal summary.json: %w", err)
	}
	summaryBytes = append(summaryBytes, '\n')
	var summaryKey string
	if meta.Commit != "" {
		summaryKey = fmt.Sprintf("runs/%s/%s/summary.json", meta.Commit, meta.TaskID)
	} else {
		summaryKey = fmt.Sprintf("runs/%s/%s/%s/summary.json", meta.RunNamespace, meta.RunName, meta.TaskID)
	}
	s.objects[summaryKey] = summaryBytes

	return uri, nil
}

// StoreComparison persists comparison summary data as comparisons/<comparisonName>.json.
func (s *InMemorySink) StoreComparison(ctx context.Context, comparisonName string, data []byte) (string, error) {
	if err := ValidateSafeComponent("comparisonName", comparisonName); err != nil {
		return "", err
	}

	objKey := fmt.Sprintf("comparisons/%s.json", comparisonName)
	uri := fmt.Sprintf("mem://%s", objKey)

	s.mu.Lock()
	defer s.mu.Unlock()

	copied := make([]byte, len(data))
	copy(copied, data)
	s.objects[objKey] = copied

	return uri, nil
}

// Load retrieves raw artifact bytes from memory, bounded by the maximum allowed artifact size.
func (s *InMemorySink) Load(ctx context.Context, path string) ([]byte, error) {
	cleanPath := strings.TrimSpace(path)
	cleanPath = strings.TrimPrefix(cleanPath, "mem://")
	cleanPath = strings.TrimPrefix(cleanPath, "/")

	if cleanPath == "" {
		return nil, fmt.Errorf("artifact path must not be empty")
	}

	if !strings.HasPrefix(cleanPath, "runs/") && !strings.HasPrefix(cleanPath, "comparisons/") {
		return nil, fmt.Errorf("artifact path %q does not have required 'runs/' or 'comparisons/' prefix", path)
	}

	s.mu.RLock()
	data, ok := s.objects[cleanPath]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: object %q not found", ErrNotFound, path)
	}

	limit := s.maxBytes
	if limit <= 0 {
		limit = DefaultMaxLoadBytes
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("artifact %q exceeds maximum load size of %d bytes", path, limit)
	}

	copied := make([]byte, len(data))
	copy(copied, data)
	return copied, nil
}

// GetMetadata retrieves metadata stored with an object.
func (s *InMemorySink) GetMetadata(path string) (map[string]string, bool) {
	cleanPath := strings.TrimSpace(path)
	cleanPath = strings.TrimPrefix(cleanPath, "mem://")
	cleanPath = strings.TrimPrefix(cleanPath, "/")

	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.metadata[cleanPath]
	if !ok {
		return nil, false
	}
	copyMap := make(map[string]string, len(m))
	for k, v := range m {
		copyMap[k] = v
	}
	return copyMap, true
}
