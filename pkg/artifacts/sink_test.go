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

package artifacts_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/internal/fakes"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
)

func TestNoopSink(t *testing.T) {
	sink := &artifacts.NoopSink{}
	meta := artifacts.RunMetadata{
		RunName:      "test-run",
		RunNamespace: "eval-runner",
		TaskID:       "docs/readme_comprehension",
	}

	stored, err := sink.StoreArtifact(t.Context(), meta, "eval.json", []byte("data"))
	if err != nil {
		t.Fatalf("NoopSink returned error: %v", err)
	}
	if stored.Location != "" {
		t.Errorf("Expected empty location from NoopSink, got %q", stored.Location)
	}
	if stored.Hash != "" {
		t.Errorf("Expected empty hash from NoopSink, got %q", stored.Hash)
	}
}

func TestLocalDirSink(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "artifacts-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sink := &artifacts.LocalDirSink{BaseDir: tempDir}
	meta := artifacts.RunMetadata{
		RunName:      "run-123",
		RunNamespace: "test-ns",
		TaskID:       "docs/readme_comprehension",
		Model:        "mockllm/model",
		Commit:       "abc1234",
		SampleCount:  11,
		SampleRange:  &artifacts.SampleRange{Start: 5, End: 15},
	}

	payload := []byte(`{"version": 2}`)
	saved, err := sink.StoreArtifact(t.Context(), meta, "result.json", payload)
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	expectedHash := artifacts.ArtifactHash(payload)
	if saved.Hash != expectedHash {
		t.Errorf("Expected hash %q, got %q", expectedHash, saved.Hash)
	}

	expectedPath := filepath.Join(tempDir, "runs", "abc1234", "docs%2Freadme_comprehension", "result.json")
	if saved.Location != expectedPath {
		t.Errorf("Expected path %q, got %q", expectedPath, saved.Location)
	}

	readBytes, err := os.ReadFile(saved.Location)
	if err != nil {
		t.Fatalf("Failed to read saved file: %v", err)
	}
	if string(readBytes) != string(payload) {
		t.Errorf("Expected content %s, got %s", payload, readBytes)
	}

	// Verify known test vector pinning the sha256:<hex> literal format for empty input
	if emptyHash := artifacts.ArtifactHash([]byte("")); emptyHash != "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Errorf("ArtifactHash of empty input = %q, want sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", emptyHash)
	}

	// Verify summary.json was written with SampleRange and LogHash
	summaryPath := filepath.Join(tempDir, "runs", "abc1234", "docs%2Freadme_comprehension", "summary.json")
	summaryBytes, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("Failed to read summary.json: %v", err)
	}
	var summary artifacts.TaskSummary
	if err := json.Unmarshal(summaryBytes, &summary); err != nil {
		t.Fatalf("Failed to unmarshal summary.json: %v", err)
	}
	if summary.LogFilename != "result.json" {
		t.Errorf("Expected summary.LogFilename 'result.json', got %q", summary.LogFilename)
	}
	if summary.LogHash != expectedHash {
		t.Errorf("Expected summary.LogHash %q, got %q", expectedHash, summary.LogHash)
	}
	if summary.SampleRange == nil || summary.SampleRange.Start != 5 || summary.SampleRange.End != 15 {
		t.Errorf("Expected summary.SampleRange {5, 15}, got %+v", summary.SampleRange)
	}
}

func TestLocalDirSink_DirectoryTraversalRejected(t *testing.T) {
	tempDir := t.TempDir()
	sink := &artifacts.LocalDirSink{BaseDir: tempDir}

	for _, tt := range fakes.ArtifactPathValidationTestCases() {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := sink.StoreArtifact(t.Context(), tt.Meta, tt.Filename, []byte("data"))
			if err == nil {
				t.Errorf("Expected error for %s, got nil", tt.Name)
			}
		})
	}
}

func TestInMemorySink_DirectoryTraversalRejected(t *testing.T) {
	sink := artifacts.NewInMemorySink()

	for _, tt := range fakes.ArtifactPathValidationTestCases() {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := sink.StoreArtifact(t.Context(), tt.Meta, tt.Filename, []byte("data"))
			if err == nil {
				t.Errorf("Expected error for %s, got nil", tt.Name)
			}
		})
	}
}

func TestNoopSink_StoreComparisonAndLoad(t *testing.T) {
	sink := &artifacts.NoopSink{}
	path, err := sink.StoreComparison(t.Context(), "my-comp", []byte("{}"))
	if err != nil {
		t.Fatalf("StoreComparison returned error: %v", err)
	}
	if path != "" {
		t.Errorf("Expected empty path, got %q", path)
	}

	if _, err := sink.Load(t.Context(), "any-path"); err == nil {
		t.Errorf("Expected error from NoopSink.Load, got nil")
	}
}

func TestLocalDirSink_StoreComparisonAndLoad(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "artifacts-comp-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sink := &artifacts.LocalDirSink{BaseDir: tempDir}
	payload := []byte(`{"comparison": "comp-1"}`)

	savedPath, err := sink.StoreComparison(t.Context(), "comp-1", payload)
	if err != nil {
		t.Fatalf("StoreComparison failed: %v", err)
	}

	expectedPath := filepath.Join(tempDir, "comparisons", "comp-1.json")
	if savedPath != expectedPath {
		t.Errorf("Expected path %q, got %q", expectedPath, savedPath)
	}

	// Create and load a valid run artifact under runs/
	runDir := filepath.Join(tempDir, "runs", "commit-1", "task-1")
	if err := os.MkdirAll(runDir, 0755); err != nil {
		t.Fatalf("Failed to create runDir: %v", err)
	}
	runPath := filepath.Join(runDir, "log.json")
	runPayload := []byte(`{"version": 2, "samples": []}`)
	if err := os.WriteFile(runPath, runPayload, 0644); err != nil {
		t.Fatalf("Failed to write runPath: %v", err)
	}

	loaded, err := sink.Load(t.Context(), runPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if string(loaded) != string(runPayload) {
		t.Errorf("Expected content %s, got %s", runPayload, loaded)
	}

	// Load comparison artifact succeeds
	loadedComp, err := sink.Load(t.Context(), savedPath)
	if err != nil {
		t.Fatalf("Load comparison failed: %v", err)
	}
	if string(loadedComp) != string(payload) {
		t.Errorf("Expected content %s, got %s", payload, loadedComp)
	}

	// Reject loading artifacts outside runs/ or comparisons/
	otherDir := filepath.Join(tempDir, "other")
	if err := os.MkdirAll(otherDir, 0755); err != nil {
		t.Fatalf("Failed to create otherDir: %v", err)
	}
	otherPath := filepath.Join(otherDir, "file.json")
	if err := os.WriteFile(otherPath, []byte("{}"), 0644); err != nil {
		t.Fatalf("Failed to write otherPath: %v", err)
	}
	if _, err := sink.Load(t.Context(), otherPath); err == nil {
		t.Errorf("Expected error loading path outside runs/ or comparisons/, got nil")
	}

	// Test empty BaseDir in Load
	emptySink := &artifacts.LocalDirSink{BaseDir: ""}
	if _, err := emptySink.Load(t.Context(), runPath); err == nil {
		t.Errorf("Expected error on empty BaseDir in Load")
	}

	// Test traversal in StoreComparison
	if _, err := sink.StoreComparison(t.Context(), "../escape", payload); err == nil {
		t.Errorf("Expected error on directory traversal in comparison name")
	}

	// Test traversal in Load
	if _, err := sink.Load(t.Context(), filepath.Join(tempDir, "../../../etc/passwd")); err == nil {
		t.Errorf("Expected error on escaping base directory in Load")
	}
}

func TestLocalDirSink_StoreManifest(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "artifacts-manifest-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sink := &artifacts.LocalDirSink{BaseDir: tempDir}
	if !sink.IsDurable() {
		t.Errorf("Expected LocalDirSink to be durable")
	}

	meta := artifacts.RunMetadata{
		RunName:   "run-1",
		Commit:    "abcd123",
		TaskCount: 3,
	}

	payload := []byte(`{"tasks":[{"id":"t1","spec":"s1"}]}`)
	uri, err := sink.StoreManifest(t.Context(), meta, payload)
	if err != nil {
		t.Fatalf("StoreManifest failed: %v", err)
	}

	expectedPath := filepath.Join(tempDir, "runs", "abcd123", "manifest.json")
	if uri != expectedPath {
		t.Errorf("Expected URI %q, got %q", expectedPath, uri)
	}

	// Read and verify
	loaded, err := sink.Load(t.Context(), uri)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if string(loaded) != string(payload) {
		t.Errorf("Expected %s, got %s", payload, loaded)
	}

	// Idempotency: second write with same content returns same path
	uri2, err := sink.StoreManifest(t.Context(), meta, payload)
	if err != nil {
		t.Fatalf("Second StoreManifest failed: %v", err)
	}
	if uri2 != uri {
		t.Errorf("Expected matching URI on idempotent write, got %q vs %q", uri2, uri)
	}

	// Missing file returns ErrNotFound
	_, err = sink.Load(t.Context(), filepath.Join(tempDir, "runs", "abcd123", "missing.json"))
	if !artifacts.IsNotFound(err) {
		t.Errorf("Expected IsNotFound(err) to be true for missing file, got: %v", err)
	}
}

func TestLocalDirSink_StoreTaskFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "artifacts-task-file-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sink := &artifacts.LocalDirSink{BaseDir: tempDir}
	meta := artifacts.RunMetadata{Commit: "c123456", TaskID: "bench/task-1"}
	payload := []byte(`{"steps":[]}`)
	path, err := sink.StoreTaskFile(t.Context(), meta, "agent.json", payload)
	if err != nil {
		t.Fatalf("StoreTaskFile failed: %v", err)
	}
	if loaded, err := os.ReadFile(path); err != nil || string(loaded) != string(payload) {
		t.Fatalf("unexpected content: %s (err: %v)", loaded, err)
	}
	// Verify summary.json was NOT created
	summaryPath := filepath.Join(filepath.Dir(path), "summary.json")
	if _, err := os.Stat(summaryPath); !os.IsNotExist(err) {
		t.Errorf("expected summary.json not to exist, got stat err: %v", err)
	}
}

func TestInMemorySink(t *testing.T) {
	sink := artifacts.NewInMemorySink()
	if sink.IsDurable() {
		t.Errorf("Expected InMemorySink to be non-durable")
	}

	commit := "abcde123"
	prefix := sink.ArtifactPrefix(commit)
	if prefix != "mem://runs/abcde123/" {
		t.Errorf("Expected prefix mem://runs/abcde123/, got %q", prefix)
	}

	meta := artifacts.RunMetadata{
		RunName:      "run-inmem",
		RunNamespace: "eval-runner",
		Commit:       commit,
		TaskCount:    5,
		TaskID:       "task-1",
	}

	manifestData := []byte(`{"tasks":[{"id":"task-1","spec":"spec-1"}]}`)
	uri, err := sink.StoreManifest(t.Context(), meta, manifestData)
	if err != nil {
		t.Fatalf("StoreManifest failed: %v", err)
	}
	if uri != "mem://runs/abcde123/manifest.json" {
		t.Errorf("Expected URI mem://runs/abcde123/manifest.json, got %q", uri)
	}

	// Load manifest
	loaded, err := sink.Load(t.Context(), uri)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if string(loaded) != string(manifestData) {
		t.Errorf("Expected content %s, got %s", manifestData, loaded)
	}

	// Verify metadata
	metaMap, ok := sink.GetMetadata(uri)
	if !ok {
		t.Fatalf("Expected metadata for %s", uri)
	}
	if metaMap["run"] != "run-inmem" || metaMap["commit"] != commit || metaMap["task-count"] != "5" {
		t.Errorf("Unexpected metadata: %+v", metaMap)
	}

	// Idempotency: writing again with same data is no-op
	uri2, err := sink.StoreManifest(t.Context(), meta, manifestData)
	if err != nil {
		t.Fatalf("Second StoreManifest failed: %v", err)
	}
	if uri2 != uri {
		t.Errorf("Expected matching URI on idempotent write: %q vs %q", uri2, uri)
	}

	// Missing object returns ErrNotFound
	_, err = sink.Load(t.Context(), "mem://runs/abcde123/notfound.json")
	if !artifacts.IsNotFound(err) {
		t.Errorf("Expected IsNotFound(err) to be true for missing object, got: %v", err)
	}

	// Store and load task artifact
	artifactData := []byte("log data")
	stored, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", artifactData)
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}
	expectedArtifactHash := artifacts.ArtifactHash(artifactData)
	if stored.Hash != expectedArtifactHash {
		t.Errorf("Expected hash %q, got %q", expectedArtifactHash, stored.Hash)
	}
	loadedArtifact, err := sink.Load(t.Context(), stored.Location)
	if err != nil {
		t.Fatalf("Load artifact failed: %v", err)
	}
	if string(loadedArtifact) != "log data" {
		t.Errorf("Expected 'log data', got %s", loadedArtifact)
	}
	artifactMetadata, ok := sink.GetMetadata(stored.Location)
	if !ok || artifactMetadata["sha256"] != expectedArtifactHash {
		t.Errorf("Expected metadata sha256=%q, got %+v", expectedArtifactHash, artifactMetadata)
	}

	// Store and load comparison
	compURI, err := sink.StoreComparison(t.Context(), "comp-1", []byte("comp data"))
	if err != nil {
		t.Fatalf("StoreComparison failed: %v", err)
	}
	compLoaded, err := sink.Load(t.Context(), compURI)
	if err != nil {
		t.Fatalf("Load comparison failed: %v", err)
	}
	if string(compLoaded) != "comp data" {
		t.Errorf("Expected 'comp data', got %s", compLoaded)
	}
}

func TestInMemorySink_ConcurrentStore(t *testing.T) {
	sink := artifacts.NewInMemorySink()
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			meta := artifacts.RunMetadata{
				Commit: "c1",
				TaskID: fmt.Sprintf("task-%d", id),
			}
			_, _ = sink.StoreArtifact(t.Context(), meta, "results.json", []byte(`{}`))
			_, _ = sink.StoreTaskFile(t.Context(), meta, "agent.json", []byte(`{}`))
		}(i)
	}
	wg.Wait()
}

func TestIsNotFound_ErrorsIsOnly(t *testing.T) {
	if artifacts.IsNotFound(nil) {
		t.Errorf("Expected IsNotFound(nil) to be false")
	}

	notFoundErr := fmt.Errorf("wrapped error: %w", artifacts.ErrNotFound)
	if !artifacts.IsNotFound(notFoundErr) {
		t.Errorf("Expected IsNotFound(wrapped ErrNotFound) to be true")
	}

	// Transient error on a commit whose hex contains 404
	transientErrWith404 := errors.New("rpc error: code = Unavailable desc = connection error for runs/c0404ab/manifest.json: 404")
	if artifacts.IsNotFound(transientErrWith404) {
		t.Errorf("Expected transient error containing 404 in commit hex to NOT be classified as not-found")
	}

	transientErrWithNotFoundString := errors.New("network transient failure: host not found")
	if artifacts.IsNotFound(transientErrWithNotFoundString) {
		t.Errorf("Expected transient error containing 'not found' text to NOT be classified as not-found")
	}

	transientErrNoSuchFile := errors.New("temporary failure: no such file or directory (transient mount blip)")
	if artifacts.IsNotFound(transientErrNoSuchFile) {
		t.Errorf("Expected transient error containing 'no such file or directory' to NOT be classified as not-found")
	}
}

func TestSinkClose(t *testing.T) {
	sinks := []struct {
		name string
		sink artifacts.ArtifactSink
	}{
		{"NoopSink", &artifacts.NoopSink{}},
		{"LocalDirSink", &artifacts.LocalDirSink{BaseDir: t.TempDir()}},
		{"InMemorySink", artifacts.NewInMemorySink()},
	}

	for _, tc := range sinks {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.sink.Close(); err != nil {
				t.Errorf("%s Close returned unexpected error: %v", tc.name, err)
			}
		})
	}
}

func TestLocalDirSink_SummaryJSON_RoundTripAndTruncation(t *testing.T) {
	tempDir := t.TempDir()
	sink := &artifacts.LocalDirSink{BaseDir: tempDir}

	rate := 0.85
	ci := &v1alpha1.ConfidenceInterval{
		Lower:      0.65,
		Upper:      0.95,
		Confidence: 0.95,
		Method:     "wilson",
	}
	sd := 0.12
	latencySec := 1.25
	score1 := 1.0
	score2 := 0.0
	costUSD := 0.05

	samples := []artifacts.TaskSampleSummary{
		{
			ID:             "sample-1",
			Epoch:          1,
			Score:          &score1,
			Status:         "pass",
			LatencySeconds: &latencySec,
			Tokens: &artifacts.TaskSampleTokens{
				Input:  100,
				Output: 20,
			},
		},
		{
			ID:             "sample-2",
			Epoch:          1,
			Score:          &score2,
			Status:         "fail",
			LatencySeconds: &latencySec,
			Tokens: &artifacts.TaskSampleTokens{
				Input:  80,
				Output: 15,
			},
		},
	}

	meta := artifacts.RunMetadata{
		RunName:           "my-run",
		RunNamespace:      "eval-runner",
		TaskID:            "docs/readme_comprehension",
		Model:             "mockllm/model",
		Commit:            "abc1234",
		ControllerVersion: "v1.2.3",
		Scorer:            "choice",
		SampleCount:       len(samples),
		SuccessRate:       &rate,
		SuccessRateCI:     ci,
		SuccessRateSD:     &sd,
		CostUSD:           &costUSD,
		CostSource:        v1alpha1.CostSourceConfigMap,
		Samples:           samples,
		Successes:         1,
	}

	saved, err := sink.StoreArtifact(t.Context(), meta, "result.json", []byte(`{}`))
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	// Verify using ReadTaskSummary
	summary, err := artifacts.ReadTaskSummary(t.Context(), sink, saved.Location)
	if err != nil {
		t.Fatalf("ReadTaskSummary failed: %v", err)
	}

	if summary.SchemaVersion != 1 {
		t.Errorf("Expected SchemaVersion=1, got %d", summary.SchemaVersion)
	}
	if summary.RunName != "my-run" {
		t.Errorf("Expected RunName=my-run, got %q", summary.RunName)
	}
	if summary.RunNamespace != "eval-runner" {
		t.Errorf("Expected RunNamespace=eval-runner, got %q", summary.RunNamespace)
	}
	if summary.ControllerVersion != "v1.2.3" {
		t.Errorf("Expected ControllerVersion=v1.2.3, got %q", summary.ControllerVersion)
	}
	if summary.Scorer != "choice" {
		t.Errorf("Expected Scorer=choice, got %q", summary.Scorer)
	}
	if summary.SuccessRate == nil || *summary.SuccessRate != 0.85 {
		t.Errorf("Expected SuccessRate=0.85, got %v", summary.SuccessRate)
	}
	if summary.SuccessRateCI == nil || summary.SuccessRateCI.Method != "wilson" {
		t.Errorf("Expected SuccessRateCI.Method=wilson, got %+v", summary.SuccessRateCI)
	}
	if summary.SuccessRateSD == nil || *summary.SuccessRateSD != 0.12 {
		t.Errorf("Expected SuccessRateSD=0.12, got %v", summary.SuccessRateSD)
	}
	if summary.CostUSD == nil || *summary.CostUSD != 0.05 {
		t.Errorf("Expected CostUSD=0.05, got %v", summary.CostUSD)
	}
	if summary.CostSource != v1alpha1.CostSourceConfigMap {
		t.Errorf("Expected CostSource=configmap, got %q", summary.CostSource)
	}
	if len(summary.Samples) != 2 {
		t.Fatalf("Expected 2 samples, got %d", len(summary.Samples))
	}
	if summary.Samples[0].ID != "sample-1" || summary.Samples[0].Status != "pass" {
		t.Errorf("Sample[0] mismatch: %+v", summary.Samples[0])
	}
	if summary.Samples[1].ID != "sample-2" || summary.Samples[1].Status != "fail" {
		t.Errorf("Sample[1] mismatch: %+v", summary.Samples[1])
	}
	if summary.SamplesTruncated {
		t.Errorf("Expected SamplesTruncated=false for 2 samples, got true")
	}
	if summary.SampleCount != 2 {
		t.Errorf("Expected SampleCount=2, got %d", summary.SampleCount)
	}

	// Verify truncation at MaxSummarySamples (10,000)
	largeSamples := make([]artifacts.TaskSampleSummary, 10005)
	for i := range largeSamples {
		largeSamples[i] = artifacts.TaskSampleSummary{ID: fmt.Sprintf("sample-%d", i+1)}
	}
	meta.Samples = largeSamples
	meta.SampleCount = len(largeSamples)
	savedLarge, err := sink.StoreArtifact(t.Context(), meta, "result-large.json", []byte(`{}`))
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}
	largeSummary, err := artifacts.ReadTaskSummary(t.Context(), sink, savedLarge.Location)
	if err != nil {
		t.Fatalf("ReadTaskSummary failed: %v", err)
	}
	if !largeSummary.SamplesTruncated {
		t.Errorf("Expected SamplesTruncated=true for 10005 samples, got false")
	}
	if largeSummary.SampleCount != 10005 {
		t.Errorf("Expected SampleCount=10005, got %d", largeSummary.SampleCount)
	}
	if len(largeSummary.Samples) != artifacts.MaxSummarySamples {
		t.Errorf("Expected %d samples, got %d", artifacts.MaxSummarySamples, len(largeSummary.Samples))
	}
}

func TestInMemorySink_SummaryJSON_RoundTripAndTruncation(t *testing.T) {
	sink := artifacts.NewInMemorySink()

	rate := 0.75
	ci := &v1alpha1.ConfidenceInterval{
		Lower:      0.50,
		Upper:      0.90,
		Confidence: 0.95,
		Method:     "t",
	}

	samples := []artifacts.TaskSampleSummary{
		{
			ID:     "s1",
			Status: "pass",
		},
	}

	meta := artifacts.RunMetadata{
		RunName:           "mem-run",
		RunNamespace:      "eval-runner",
		TaskID:            "docs/readme",
		Commit:            "111122223333",
		ControllerVersion: "v1.0.0",
		Scorer:            "exact",
		SampleCount:       len(samples),
		SuccessRate:       &rate,
		SuccessRateCI:     ci,
		Samples:           samples,
	}

	stored, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", []byte(`{}`))
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	summary, err := artifacts.ReadTaskSummary(t.Context(), sink, stored.Location)
	if err != nil {
		t.Fatalf("ReadTaskSummary failed: %v", err)
	}

	if summary.SchemaVersion != 1 {
		t.Errorf("Expected SchemaVersion=1, got %d", summary.SchemaVersion)
	}
	if summary.ControllerVersion != "v1.0.0" {
		t.Errorf("Expected ControllerVersion=v1.0.0, got %s", summary.ControllerVersion)
	}
	if summary.Scorer != "exact" {
		t.Errorf("Expected Scorer=exact, got %s", summary.Scorer)
	}
	if summary.SuccessRateCI == nil || summary.SuccessRateCI.Method != "t" {
		t.Errorf("Expected SuccessRateCI.Method=t, got %+v", summary.SuccessRateCI)
	}
	if summary.SampleCount != 1 {
		t.Errorf("Expected SampleCount=1, got %d", summary.SampleCount)
	}
	if len(summary.Samples) != 1 {
		t.Fatalf("Expected 1 sample, got %d", len(summary.Samples))
	}
}

func TestReadTaskSummary_ValidationAndLegacyShape(t *testing.T) {
	tempDir := t.TempDir()
	sink := &artifacts.LocalDirSink{BaseDir: tempDir}

	// 1. Unsupported schemaVersion (e.g. 2) rejected by ReadTaskSummary
	badSchemaJSON := []byte(`{
		"schemaVersion": 2,
		"taskId": "docs/readme_comprehension"
	}`)
	badPath := filepath.Join(tempDir, "runs", "commit1", "docs%2Freadme_comprehension", "summary.json")
	if err := os.MkdirAll(filepath.Dir(badPath), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.WriteFile(badPath, badSchemaJSON, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := artifacts.ReadTaskSummary(t.Context(), sink, badPath)
	if err == nil {
		t.Fatalf("Expected error reading schemaVersion 2, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported schemaVersion") {
		t.Errorf("Expected unsupported schemaVersion error, got: %v", err)
	}

	// 2. Legacy unversioned shape: "samples" is integer 10, no schemaVersion
	legacyJSON := []byte(`{
		"taskId": "docs/readme_comprehension",
		"model": "google/gemini-3.5-flash-lite",
		"commit": "abcdef123456",
		"samples": 10,
		"successes": 8,
		"meanScore": 0.8,
		"capped": false
	}`)
	legacyPath := filepath.Join(tempDir, "runs", "commit2", "docs%2Freadme_comprehension", "summary.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.WriteFile(legacyPath, legacyJSON, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	legacySummary, err := artifacts.ReadTaskSummary(t.Context(), sink, legacyPath)
	if err != nil {
		t.Fatalf("Expected legacy summary to load successfully, got: %v", err)
	}
	if legacySummary.SchemaVersion != 0 {
		t.Errorf("Expected SchemaVersion=0 for legacy summary, got %d", legacySummary.SchemaVersion)
	}
	if legacySummary.SampleCount != 10 {
		t.Errorf("Expected SampleCount=10 mapped from legacy samples int, got %d", legacySummary.SampleCount)
	}
	if legacySummary.Successes != 8 {
		t.Errorf("Expected Successes=8, got %d", legacySummary.Successes)
	}
	if legacySummary.MeanScore == nil || *legacySummary.MeanScore != 0.8 {
		t.Errorf("Expected MeanScore=0.8, got %v", legacySummary.MeanScore)
	}
	if len(legacySummary.Samples) != 0 {
		t.Errorf("Expected empty Samples slice for legacy summary, got %d items", len(legacySummary.Samples))
	}
}

func TestLocalAndInMemorySink_OpenStream(t *testing.T) {
	baseDirectory := t.TempDir()
	testCases := []struct {
		name string
		sink artifacts.ArtifactSink
	}{
		{name: "LocalDirSink", sink: &artifacts.LocalDirSink{BaseDir: baseDirectory}},
		{name: "InMemorySink", sink: artifacts.NewInMemorySink()},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			content := []byte(`{"stream": "data"}`)
			metadata := artifacts.RunMetadata{Commit: "commit123", TaskID: "task1"}
			stored, err := testCase.sink.StoreArtifact(t.Context(), metadata, "output.json", content)
			if err != nil {
				t.Fatalf("StoreArtifact failed: %v", err)
			}

			stream, err := testCase.sink.OpenStream(t.Context(), stored.Location, stored.Hash)
			if err != nil {
				t.Fatalf("OpenStream failed: %v", err)
			}
			retrievedContent, err := io.ReadAll(stream)
			if err != nil {
				t.Fatalf("ReadAll failed: %v", err)
			}
			if closeError := stream.Close(); closeError != nil {
				t.Fatalf("Close failed: %v", closeError)
			}
			if string(retrievedContent) != string(content) {
				t.Errorf("got %q, want %q", retrievedContent, content)
			}

			// Not found
			_, err = testCase.sink.OpenStream(t.Context(), stored.Location+"-missing", "")
			if !artifacts.IsNotFound(err) {
				t.Errorf("expected IsNotFound, got %v", err)
			}
		})
	}
}

func TestPrebuildMarker_StoreAndLoad(t *testing.T) {
	baseDirectory := t.TempDir()
	marker := artifacts.PrebuildCompletionMarker{
		Commit:       "abcdef123456",
		PrebuildName: "kind-images",
		CompletedAt:  time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC),
	}
	markerBytes, err := json.Marshal(marker)
	if err != nil {
		t.Fatalf("marshal marker: %v", err)
	}

	testCases := []struct {
		name           string
		sink           artifacts.ArtifactSink
		expectedTarget string
	}{
		{
			name:           "LocalDirSink",
			sink:           &artifacts.LocalDirSink{BaseDir: baseDirectory},
			expectedTarget: filepath.Join(baseDirectory, "runs", "abcdef123456", "prebuilds", "kind-images", "done.json"),
		},
		{
			name:           "InMemorySink",
			sink:           artifacts.NewInMemorySink(),
			expectedTarget: "mem://runs/abcdef123456/prebuilds/kind-images/done.json",
		},
		{
			name:           "ReloadableSink",
			sink:           artifacts.NewReloadableSink(artifacts.NewInMemorySink()),
			expectedTarget: "mem://runs/abcdef123456/prebuilds/kind-images/done.json",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			target := artifacts.PrebuildMarkerTarget(testCase.sink, marker.Commit, marker.PrebuildName)
			if target != testCase.expectedTarget {
				t.Fatalf("PrebuildMarkerTarget = %q, want %q", target, testCase.expectedTarget)
			}

			if _, err := artifacts.LoadWithTimeout(t.Context(), testCase.sink, target); !artifacts.IsNotFound(err) {
				t.Fatalf("expected IsNotFound before store, got %v", err)
			}

			storedPath, err := artifacts.StorePrebuildMarkerWithTimeout(t.Context(), testCase.sink, marker.Commit, marker.PrebuildName, markerBytes)
			if err != nil {
				t.Fatalf("StorePrebuildMarkerWithTimeout failed: %v", err)
			}
			if storedPath != testCase.expectedTarget {
				t.Errorf("storedPath = %q, want %q", storedPath, testCase.expectedTarget)
			}

			loadedBytes, err := artifacts.LoadWithTimeout(t.Context(), testCase.sink, target)
			if err != nil {
				t.Fatalf("LoadWithTimeout failed: %v", err)
			}
			var loaded artifacts.PrebuildCompletionMarker
			if err := json.Unmarshal(loadedBytes, &loaded); err != nil {
				t.Fatalf("unmarshal loaded marker: %v", err)
			}
			if loaded.Commit != marker.Commit || loaded.PrebuildName != marker.PrebuildName || !loaded.CompletedAt.Equal(marker.CompletedAt) {
				t.Errorf("loaded marker = %+v, want %+v", loaded, marker)
			}
		})
	}

	t.Run("NoopSink", func(t *testing.T) {
		noop := &artifacts.NoopSink{}
		location, err := artifacts.StorePrebuildMarkerWithTimeout(t.Context(), noop, marker.Commit, marker.PrebuildName, markerBytes)
		if err != nil || location != "" {
			t.Errorf("NoopSink StorePrebuildMarker = (%q, %v), want (\"\", nil)", location, err)
		}
	})

	t.Run("UnavailableSink", func(t *testing.T) {
		unavailable := artifacts.NewUnavailableSink("eval-bucket", errors.New("no credentials"))
		if _, err := artifacts.StorePrebuildMarkerWithTimeout(t.Context(), unavailable, marker.Commit, marker.PrebuildName, markerBytes); err == nil {
			t.Fatal("expected error from UnavailableSink, got nil")
		}
	})
}
