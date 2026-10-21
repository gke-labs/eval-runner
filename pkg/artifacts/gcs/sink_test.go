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

package gcs

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/internal/fakes"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
)

var _ StorageClient = (*fakes.FakeStorageClient)(nil)

func TestGCSSink_ObjectNamingAndSummary(t *testing.T) {
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(t.Context(), "gs://my-eval-bucket", WithStorageClient(fakeClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	if sink.Bucket() != "my-eval-bucket" {
		t.Errorf("Expected bucket %q, got %q", "my-eval-bucket", sink.Bucket())
	}

	score := 0.85
	startTime := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	finishTime := time.Date(2026, 10, 17, 12, 5, 0, 0, time.UTC)

	sampleList := make([]artifacts.TaskSampleSummary, 10)
	for i := range sampleList {
		sampleList[i] = artifacts.TaskSampleSummary{ID: fmt.Sprintf("sample-%d", i+1)}
	}

	meta := artifacts.RunMetadata{
		RunName:      "test-run",
		RunNamespace: "eval-runner",
		TaskID:       "docs/readme_comprehension",
		Model:        "google/gemini-3.5-flash-lite",
		Commit:       "abcdef123456",
		Samples:      sampleList,
		Successes:    8,
		MeanScore:    &score,
		StartedAt:    &startTime,
		FinishedAt:   &finishTime,
		SampleRange:  &artifacts.SampleRange{Start: 1, End: 10},
	}

	logFilename := "2026-10-17_readme_eval.json"
	logContent := []byte(`{"results": {"score": 0.85}}`)

	logURI, err := sink.StoreArtifact(t.Context(), meta, logFilename, logContent)
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	expectedLogURI := "gs://my-eval-bucket/runs/abcdef123456/docs/readme_comprehension/2026-10-17_readme_eval.json"
	if logURI != expectedLogURI {
		t.Errorf("Expected log URI %q, got %q", expectedLogURI, logURI)
	}

	expectedPrefix := "gs://my-eval-bucket/runs/abcdef123456/"
	if prefix := sink.ArtifactPrefix(meta.Commit); prefix != expectedPrefix {
		t.Errorf("Expected artifact prefix %q, got %q", expectedPrefix, prefix)
	}

	// Verify log object in fake client
	logObj, exists := fakeClient.GetObject("my-eval-bucket", "runs/abcdef123456/docs/readme_comprehension/2026-10-17_readme_eval.json")
	if !exists {
		t.Fatalf("Log object was not written to storage")
	}
	if string(logObj.Data) != string(logContent) {
		t.Errorf("Log object data mismatch: expected %s, got %s", logContent, logObj.Data)
	}
	if logObj.ContentType != "application/json" {
		t.Errorf("Expected content type %q, got %q", "application/json", logObj.ContentType)
	}

	// Verify summary.json in fake client
	summaryObj, exists := fakeClient.GetObject("my-eval-bucket", "runs/abcdef123456/docs/readme_comprehension/summary.json")
	if !exists {
		t.Fatalf("summary.json was not written to storage")
	}
	if summaryObj.ContentType != "application/json" {
		t.Errorf("Expected summary content type %q, got %q", "application/json", summaryObj.ContentType)
	}

	var summary artifacts.TaskSummary
	if err := json.Unmarshal(summaryObj.Data, &summary); err != nil {
		t.Fatalf("Failed to parse summary.json: %v", err)
	}

	if summary.TaskID != "docs/readme_comprehension" {
		t.Errorf("Expected TaskID %q, got %q", "docs/readme_comprehension", summary.TaskID)
	}
	if summary.Model != "google/gemini-3.5-flash-lite" {
		t.Errorf("Expected Model %q, got %q", "google/gemini-3.5-flash-lite", summary.Model)
	}
	if summary.Commit != "abcdef123456" {
		t.Errorf("Expected Commit %q, got %q", "abcdef123456", summary.Commit)
	}
	if len(summary.Samples) != 10 {
		t.Errorf("Expected Samples 10, got %d", len(summary.Samples))
	}
	if summary.Successes != 8 {
		t.Errorf("Expected Successes 8, got %d", summary.Successes)
	}
	if summary.MeanScore == nil || *summary.MeanScore != 0.85 {
		t.Errorf("Expected MeanScore 0.85, got %v", summary.MeanScore)
	}
	if summary.SampleRange == nil || summary.SampleRange.Start != 1 || summary.SampleRange.End != 10 {
		t.Errorf("Expected SampleRange {1, 10}, got %+v", summary.SampleRange)
	}
	if summary.StartedAt == nil || !summary.StartedAt.Equal(startTime) {
		t.Errorf("Expected StartedAt %v, got %v", startTime, summary.StartedAt)
	}
	if summary.FinishedAt == nil || !summary.FinishedAt.Equal(finishTime) {
		t.Errorf("Expected FinishedAt %v, got %v", finishTime, summary.FinishedAt)
	}
	if summary.Capped {
		t.Errorf("Expected Capped=false when under cap limit")
	}
	if summary.CapBytes != 0 {
		t.Errorf("Expected CapBytes=0 when under cap limit, got %d", summary.CapBytes)
	}
}

func TestGCSSink_CapBehavior(t *testing.T) {
	fakeClient := fakes.NewFakeStorageClient()
	capLimit := int64(30)
	sink, err := New(t.Context(), "my-eval-bucket",
		WithStorageClient(fakeClient),
		WithMaxUploadBytes(capLimit),
	)
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	meta := artifacts.RunMetadata{
		TaskID:    "docs/readme_comprehension",
		Model:     "test-model",
		Commit:    "1234567890ab",
		Samples:   make([]artifacts.TaskSampleSummary, 5),
		Successes: 5,
	}

	largeLog := []byte(strings.Repeat("A", 100)) // 100 bytes > 30 byte cap

	logURI, err := sink.StoreArtifact(t.Context(), meta, "large.json", largeLog)
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	if logURI != "gs://my-eval-bucket/runs/1234567890ab/docs/readme_comprehension/large.json" {
		t.Errorf("Unexpected log URI: %s", logURI)
	}

	logObj, exists := fakeClient.GetObject("my-eval-bucket", "runs/1234567890ab/docs/readme_comprehension/large.json")
	if !exists {
		t.Fatalf("Log object missing")
	}
	if int64(len(logObj.Data)) != capLimit {
		t.Errorf("Expected uploaded log to be capped at %d bytes, got %d bytes", capLimit, len(logObj.Data))
	}
	if string(logObj.Data) != strings.Repeat("A", int(capLimit)) {
		t.Errorf("Expected uploaded log data to match truncated prefix")
	}

	summaryObj, exists := fakeClient.GetObject("my-eval-bucket", "runs/1234567890ab/docs/readme_comprehension/summary.json")
	if !exists {
		t.Fatalf("summary.json missing")
	}

	var summary artifacts.TaskSummary
	if err := json.Unmarshal(summaryObj.Data, &summary); err != nil {
		t.Fatalf("Failed to parse summary.json: %v", err)
	}
	if !summary.Capped {
		t.Errorf("Expected summary.Capped=true when cap is exceeded")
	}
	if summary.CapBytes != capLimit {
		t.Errorf("Expected summary.CapBytes=%d, got %d", capLimit, summary.CapBytes)
	}
}

func TestGCSSink_PathValidationAndTraversal(t *testing.T) {
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(t.Context(), "eval-bucket", WithStorageClient(fakeClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	for _, tt := range fakes.ArtifactPathValidationTestCases() {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := sink.StoreArtifact(t.Context(), tt.Meta, tt.Filename, []byte("data"))
			if err == nil {
				t.Errorf("Expected error for %s, got nil", tt.Name)
			}
		})
	}
}

func TestGCSSink_BucketNormalizationAndValidation(t *testing.T) {
	fakeClient := fakes.NewFakeStorageClient()

	validBuckets := []struct {
		input    string
		expected string
	}{
		{"gs://my-bucket", "my-bucket"},
		{"gs://my-bucket/", "my-bucket"},
		{"my-bucket", "my-bucket"},
		{"  gs://my-bucket/  ", "my-bucket"},
	}

	for _, tc := range validBuckets {
		sink, err := New(t.Context(), tc.input, WithStorageClient(fakeClient))
		if err != nil {
			t.Errorf("New(%q) failed: %v", tc.input, err)
			continue
		}
		if sink.Bucket() != tc.expected {
			t.Errorf("New(%q) bucket = %q, expected %q", tc.input, sink.Bucket(), tc.expected)
		}
	}

	invalidBuckets := []string{
		"",
		"   ",
		"gs://",
		"gs://bucket/with/path",
		"bucket..name",
		"bucket\\name",
	}

	for _, inv := range invalidBuckets {
		_, err := New(t.Context(), inv, WithStorageClient(fakeClient))
		if err == nil {
			t.Errorf("New(%q) expected error, got nil", inv)
		}
	}
}

func TestGCSSink_ErrorPropagation(t *testing.T) {
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(t.Context(), "eval-bucket", WithStorageClient(fakeClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	meta := artifacts.RunMetadata{
		TaskID: "docs/readme_comprehension",
		Model:  "test-model",
		Commit: "1234567890ab",
	}

	// 1. Injected error on log file upload
	fakeClient.Errors["eval-bucket/runs/1234567890ab/docs/readme_comprehension/log.json"] = errors.New("permission denied")
	_, err = sink.StoreArtifact(t.Context(), meta, "log.json", []byte("data"))
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("Expected permission denied error, got %v", err)
	}

	// 2. Injected error on summary.json upload
	delete(fakeClient.Errors, "eval-bucket/runs/1234567890ab/docs/readme_comprehension/log.json")
	fakeClient.Errors["eval-bucket/runs/1234567890ab/docs/readme_comprehension/summary.json"] = errors.New("quota exceeded")
	_, err = sink.StoreArtifact(t.Context(), meta, "log.json", []byte("data"))
	if err == nil || !strings.Contains(err.Error(), "quota exceeded") {
		t.Errorf("Expected quota exceeded error, got %v", err)
	}
}

func TestValidateBucket(t *testing.T) {
	valid := []string{
		"gs://my-bucket",
		"gs://my-bucket/",
		"my-bucket",
		"  gs://my-bucket/  ",
	}
	for _, b := range valid {
		if err := ValidateBucket(b); err != nil {
			t.Errorf("ValidateBucket(%q) returned unexpected error: %v", b, err)
		}
	}

	invalid := []string{
		"",
		"   ",
		"gs://bucket/with/path",
		"bucket..name",
		"bucket\\name",
		"bucket name",
	}
	for _, b := range invalid {
		if err := ValidateBucket(b); err == nil {
			t.Errorf("ValidateBucket(%q) expected error, got nil", b)
		}
	}
}

func TestGCSSink_StoreComparisonAndLoad(t *testing.T) {
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(t.Context(), "eval-bucket", WithStorageClient(fakeClient), WithMaxUploadBytes(100))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	payload := []byte(`{"comparison": "comp-1", "verdict": "UNCHANGED"}`)
	uri, err := sink.StoreComparison(t.Context(), "comp-1", payload)
	if err != nil {
		t.Fatalf("StoreComparison failed: %v", err)
	}

	expectedURI := "gs://eval-bucket/comparisons/comp-1.json"
	if uri != expectedURI {
		t.Errorf("Expected URI %q, got %q", expectedURI, uri)
	}

	// Verify object exists in fake client
	obj, exists := fakeClient.GetObject("eval-bucket", "comparisons/comp-1.json")
	if !exists {
		t.Fatalf("Object was not written to storage client")
	}
	if string(obj.Data) != string(payload) {
		t.Errorf("Payload mismatch: got %s, want %s", obj.Data, payload)
	}

	// Store and verify run artifact for loading
	runPayload := []byte(`{"version": 2, "samples": []}`)
	fakeClient.Objects["eval-bucket/runs/commit1/task1/log.json"] = fakes.FakeObject{Data: runPayload}

	// Load via gs:// URI
	loaded, err := sink.Load(t.Context(), "gs://eval-bucket/runs/commit1/task1/log.json")
	if err != nil {
		t.Fatalf("Load via URI failed: %v", err)
	}
	if string(loaded) != string(runPayload) {
		t.Errorf("Loaded mismatch: got %s, want %s", loaded, runPayload)
	}

	// Load via object path
	loadedPath, err := sink.Load(t.Context(), "runs/commit1/task1/log.json")
	if err != nil {
		t.Fatalf("Load via object path failed: %v", err)
	}
	if string(loadedPath) != string(runPayload) {
		t.Errorf("Loaded mismatch: got %s, want %s", loadedPath, runPayload)
	}

	// Load comparison artifact via object path
	loadedComp, err := sink.Load(t.Context(), "comparisons/comp-1.json")
	if err != nil {
		t.Fatalf("Load comparison failed: %v", err)
	}
	if string(loadedComp) != string(payload) {
		t.Errorf("Loaded comparison mismatch: got %s, want %s", loadedComp, payload)
	}

	// Missing runs/ or comparisons/ prefix rejected
	if _, err := sink.Load(t.Context(), "other/file.json"); err == nil {
		t.Errorf("Expected error for missing runs/ or comparisons/ prefix, got nil")
	}

	// Bucket mismatch
	if _, err := sink.Load(t.Context(), "gs://other-bucket/runs/commit1/task1/log.json"); err == nil {
		t.Errorf("Expected error for bucket mismatch, got nil")
	}

	// Exceeds max bytes
	oversized := make([]byte, 200)
	fakeClient.Objects["eval-bucket/runs/oversized.json"] = fakes.FakeObject{Data: oversized}
	if _, err := sink.Load(t.Context(), "gs://eval-bucket/runs/oversized.json"); err == nil {
		t.Errorf("Expected error for oversized artifact load, got nil")
	}
}

func TestGCSSink_StoreManifest(t *testing.T) {
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(t.Context(), "gs://eval-bucket",
		WithStorageClient(fakeClient),
		WithMaxUploadBytes(50),
	)
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	if !sink.IsDurable() {
		t.Errorf("Expected GCSSink to be durable")
	}

	meta := artifacts.RunMetadata{
		RunName:      "test-run",
		RunNamespace: "eval-runner",
		Commit:       "abcdef12",
		TaskCount:    4,
	}

	manifestData := []byte(`{"tasks":[{"id":"t1"}]}`)

	// 1. Object absent: first write succeeds and records write
	uri, err := sink.StoreManifest(t.Context(), meta, manifestData)
	if err != nil {
		t.Fatalf("StoreManifest failed: %v", err)
	}

	expectedURI := "gs://eval-bucket/runs/abcdef12/manifest.json"
	if uri != expectedURI {
		t.Errorf("Expected URI %q, got %q", expectedURI, uri)
	}

	if fakeClient.WriteCount() != 1 {
		t.Errorf("Expected WriteCount=1 after first write, got %d", fakeClient.WriteCount())
	}

	// Verify object and metadata
	obj, ok := fakeClient.GetObject("eval-bucket", "runs/abcdef12/manifest.json")
	if !ok {
		t.Fatalf("Expected stored object at eval-bucket/runs/abcdef12/manifest.json")
	}
	if string(obj.Data) != string(manifestData) {
		t.Errorf("Content mismatch: got %s, want %s", obj.Data, manifestData)
	}
	if obj.Metadata["run"] != "test-run" || obj.Metadata["commit"] != "abcdef12" || obj.Metadata["task-count"] != "4" {
		t.Errorf("Metadata mismatch: got %+v", obj.Metadata)
	}

	// 2. Object present with the same hash/content: no-op (no write occurs)
	uri2, err := sink.StoreManifest(t.Context(), meta, manifestData)
	if err != nil {
		t.Fatalf("Second StoreManifest failed: %v", err)
	}
	if uri2 != uri {
		t.Errorf("Expected identical URI on idempotent write: %q vs %q", uri2, uri)
	}
	if fakeClient.WriteCount() != 1 {
		t.Errorf("Expected WriteCount to remain 1 after identical write, got %d", fakeClient.WriteCount())
	}

	// 3. Object present with different hash/content: overwrites existing object
	differentData := []byte(`{"tasks":[{"id":"t2"}]}`)
	uri3, err := sink.StoreManifest(t.Context(), meta, differentData)
	if err != nil {
		t.Fatalf("StoreManifest with different content failed: %v", err)
	}
	if uri3 != uri {
		t.Errorf("Expected URI %q, got %q", uri, uri3)
	}
	if fakeClient.WriteCount() != 2 {
		t.Errorf("Expected WriteCount=2 after overwrite, got %d", fakeClient.WriteCount())
	}
	updatedObj, ok := fakeClient.GetObject("eval-bucket", "runs/abcdef12/manifest.json")
	if !ok || string(updatedObj.Data) != string(differentData) {
		t.Errorf("Expected object data to be overwritten with %s, got %s", differentData, updatedObj.Data)
	}

	// 4. Other error during existence check propagates failure
	fakeClient.Errors["eval-bucket/runs/abcdef12/manifest.json"] = errors.New("storage: 500 internal server error")
	_, err = sink.StoreManifest(t.Context(), meta, manifestData)
	if err == nil || !strings.Contains(err.Error(), "failed to check existing manifest") {
		t.Errorf("Expected error checking manifest when storage fails, got: %v", err)
	}
	delete(fakeClient.Errors, "eval-bucket/runs/abcdef12/manifest.json")

	// 5. Error above cap (must error, NOT truncate)
	oversized := make([]byte, 100)
	_, err = sink.StoreManifest(t.Context(), meta, oversized)
	if err == nil {
		t.Fatalf("Expected error when storing manifest above cap, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds maximum upload size") {
		t.Errorf("Unexpected error message: %v", err)
	}

	// 6. Missing manifest load returns ErrNotFound
	_, err = sink.Load(t.Context(), "gs://eval-bucket/runs/abcdef12/missing.json")
	if !artifacts.IsNotFound(err) {
		t.Errorf("Expected artifacts.IsNotFound(err) to be true, got: %v", err)
	}
}

func TestGCSSink_FirstWriteAgainstEmptyBucket_AllObjectKinds(t *testing.T) {
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(t.Context(), "gs://empty-bucket", WithStorageClient(fakeClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	meta := artifacts.RunMetadata{
		RunName:      "test-run",
		RunNamespace: "eval-runner",
		TaskID:       "task-1",
		Model:        "mockllm/model",
		Commit:       "commit123",
		TaskCount:    1,
	}

	// 1. Manifest first write against empty bucket succeeds
	manifestData := []byte(`{"tasks":[{"id":"task-1"}]}`)
	manifestURI, err := sink.StoreManifest(t.Context(), meta, manifestData)
	if err != nil {
		t.Fatalf("First write of StoreManifest against empty bucket failed: %v", err)
	}
	expectedManifestURI := "gs://empty-bucket/runs/commit123/manifest.json"
	if manifestURI != expectedManifestURI {
		t.Errorf("Expected manifest URI %q, got %q", expectedManifestURI, manifestURI)
	}

	// 2. Artifact first write against empty bucket succeeds
	logData := []byte(`{"results":{"score":1.0}}`)
	logURI, err := sink.StoreArtifact(t.Context(), meta, "eval.json", logData)
	if err != nil {
		t.Fatalf("First write of StoreArtifact against empty bucket failed: %v", err)
	}
	expectedLogURI := "gs://empty-bucket/runs/commit123/task-1/eval.json"
	if logURI != expectedLogURI {
		t.Errorf("Expected log URI %q, got %q", expectedLogURI, logURI)
	}

	// 3. Comparison first write against empty bucket succeeds
	compData := []byte(`{"verdict":"IMPROVED"}`)
	compURI, err := sink.StoreComparison(t.Context(), "comp-123", compData)
	if err != nil {
		t.Fatalf("First write of StoreComparison against empty bucket failed: %v", err)
	}
	expectedCompURI := "gs://empty-bucket/comparisons/comp-123.json"
	if compURI != expectedCompURI {
		t.Errorf("Expected comparison URI %q, got %q", expectedCompURI, compURI)
	}

	// Verify all objects exist in storage client
	if _, ok := fakeClient.GetObject("empty-bucket", "runs/commit123/manifest.json"); !ok {
		t.Errorf("manifest.json not found in storage client")
	}
	if _, ok := fakeClient.GetObject("empty-bucket", "runs/commit123/task-1/eval.json"); !ok {
		t.Errorf("log artifact not found in storage client")
	}
	if _, ok := fakeClient.GetObject("empty-bucket", "runs/commit123/task-1/summary.json"); !ok {
		t.Errorf("summary.json not found in storage client")
	}
	if _, ok := fakeClient.GetObject("empty-bucket", "comparisons/comp-123.json"); !ok {
		t.Errorf("comparison.json not found in storage client")
	}
}

func TestGCSSink_Close(t *testing.T) {
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(t.Context(), "eval-bucket", WithStorageClient(fakeClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Errorf("Expected nil on Close, got: %v", err)
	}
	if !fakeClient.Closed {
		t.Errorf("Expected fakeClient.Closed to be true")
	}

	// Idempotent Close
	if err := sink.Close(); err != nil {
		t.Errorf("Expected nil on second Close, got: %v", err)
	}
}

func TestGCSSink_SummaryJSON_NewFieldsAndTruncation(t *testing.T) {
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(t.Context(), "eval-bucket", WithStorageClient(fakeClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	rate := 0.90
	ci := &v1alpha1.ConfidenceInterval{
		Lower:      0.75,
		Upper:      0.98,
		Confidence: 0.95,
		Method:     "wilson",
	}
	latency := 0.42
	scoreVal := 1.0

	samples := []artifacts.TaskSampleSummary{
		{
			ID:             "sample-001",
			Epoch:          1,
			Score:          &scoreVal,
			Status:         "pass",
			LatencySeconds: &latency,
			Tokens: &artifacts.TaskSampleTokens{
				Input:  50,
				Output: 10,
			},
		},
	}

	meta := artifacts.RunMetadata{
		RunName:           "gcs-run",
		RunNamespace:      "eval-runner",
		TaskID:            "benchmark/speed",
		Model:             "gemini-2.5-flash",
		Commit:            "abcdef012345",
		ControllerVersion: "v2.0.0",
		Scorer:            "exact_match",
		SuccessRate:       &rate,
		SuccessRateCI:     ci,
		Samples:           samples,
		Successes:         1,
	}

	uri, err := sink.StoreArtifact(t.Context(), meta, "inspect.json", []byte(`{}`))
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	summary, err := artifacts.ReadTaskSummary(t.Context(), sink, uri)
	if err != nil {
		t.Fatalf("ReadTaskSummary failed: %v", err)
	}

	if summary.SchemaVersion != 1 {
		t.Errorf("Expected SchemaVersion=1, got %d", summary.SchemaVersion)
	}
	if summary.RunName != "gcs-run" {
		t.Errorf("Expected RunName=gcs-run, got %q", summary.RunName)
	}
	if summary.RunNamespace != "eval-runner" {
		t.Errorf("Expected RunNamespace=eval-runner, got %q", summary.RunNamespace)
	}
	if summary.ControllerVersion != "v2.0.0" {
		t.Errorf("Expected ControllerVersion=v2.0.0, got %q", summary.ControllerVersion)
	}
	if summary.Scorer != "exact_match" {
		t.Errorf("Expected Scorer=exact_match, got %q", summary.Scorer)
	}
	if summary.SuccessRate == nil || *summary.SuccessRate != 0.90 {
		t.Errorf("Expected SuccessRate=0.90, got %v", summary.SuccessRate)
	}
	if summary.SuccessRateCI == nil || summary.SuccessRateCI.Method != "wilson" {
		t.Errorf("Expected SuccessRateCI.Method=wilson, got %+v", summary.SuccessRateCI)
	}
	if len(summary.Samples) != 1 {
		t.Fatalf("Expected 1 sample, got %d", len(summary.Samples))
	}
	if summary.SamplesTruncated {
		t.Errorf("Expected SamplesTruncated=false, got true")
	}

	// Verify truncation on > 10,000 samples
	largeSamples := make([]artifacts.TaskSampleSummary, 10002)
	for i := range largeSamples {
		largeSamples[i] = artifacts.TaskSampleSummary{ID: fmt.Sprintf("sample-%d", i+1)}
	}
	meta.Samples = largeSamples
	uriLarge, err := sink.StoreArtifact(t.Context(), meta, "inspect-large.json", []byte(`{}`))
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}
	summaryLarge, err := artifacts.ReadTaskSummary(t.Context(), sink, uriLarge)
	if err != nil {
		t.Fatalf("ReadTaskSummary failed: %v", err)
	}
	if !summaryLarge.SamplesTruncated {
		t.Errorf("Expected SamplesTruncated=true for 10002 samples, got false")
	}
	if len(summaryLarge.Samples) != artifacts.MaxSummarySamples {
		t.Errorf("Expected %d samples, got %d", artifacts.MaxSummarySamples, len(summaryLarge.Samples))
	}
}
