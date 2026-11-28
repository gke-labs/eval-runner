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
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/internal/fakes"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"google.golang.org/api/option"
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
		SampleCount:  len(sampleList),
		Samples:      sampleList,
		Successes:    8,
		MeanScore:    &score,
		StartedAt:    &startTime,
		FinishedAt:   &finishTime,
		SampleRange:  &artifacts.SampleRange{Start: 1, End: 10},
	}

	logFilename := "2026-10-17_readme_eval.json"
	logContent := []byte(`{"results": {"score": 0.85}}`)

	stored, err := sink.StoreArtifact(t.Context(), meta, logFilename, logContent)
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	expectedHash := artifacts.ArtifactHash(logContent)
	if stored.Hash != expectedHash {
		t.Errorf("Expected logHash %q, got %q", expectedHash, stored.Hash)
	}

	expectedLogURI := "gs://my-eval-bucket/runs/abcdef123456/docs/readme_comprehension/2026-10-17_readme_eval.json"
	if stored.Location != expectedLogURI {
		t.Errorf("Expected log URI %q, got %q", expectedLogURI, stored.Location)
	}

	expectedPrefix := "gs://my-eval-bucket/runs/abcdef123456/"
	if prefix := sink.ArtifactPrefix(meta.Commit); prefix != expectedPrefix {
		t.Errorf("Expected artifact prefix %q, got %q", expectedPrefix, prefix)
	}

	// Verify log object in fake client is compressed with gzip
	logObj, exists := fakeClient.GetObject("my-eval-bucket", "runs/abcdef123456/docs/readme_comprehension/2026-10-17_readme_eval.json")
	if !exists {
		t.Fatalf("Log object was not written to storage")
	}
	if len(logObj.Data) < 2 || logObj.Data[0] != 0x1f || logObj.Data[1] != 0x8b {
		t.Errorf("Expected log object data to be gzip compressed with magic bytes 0x1f, 0x8b")
	}
	decompressedLog, err := decompressBytes(logObj.Data)
	if err != nil {
		t.Fatalf("Failed to decompress log object data: %v", err)
	}
	if string(decompressedLog) != string(logContent) {
		t.Errorf("Log object data mismatch: expected %s, got %s", logContent, decompressedLog)
	}
	if logObj.ContentType != "application/json" {
		t.Errorf("Expected content type %q, got %q", "application/json", logObj.ContentType)
	}
	if logObj.ContentEncoding != "gzip" {
		t.Errorf("Expected content encoding %q, got %q", "gzip", logObj.ContentEncoding)
	}
	if logObj.Metadata == nil || logObj.Metadata["sha256"] != expectedHash {
		t.Errorf("Expected log object metadata sha256=%q, got %+v", expectedHash, logObj.Metadata)
	}

	// Verify loading via sink.Load returns decompressed content
	loadedLog, err := sink.Load(t.Context(), stored.Location)
	if err != nil {
		t.Fatalf("Failed to load stored artifact via sink.Load: %v", err)
	}
	if string(loadedLog) != string(logContent) {
		t.Errorf("sink.Load mismatch: expected %s, got %s", logContent, loadedLog)
	}

	// Verify summary.json in fake client is uncompressed
	summaryObj, exists := fakeClient.GetObject("my-eval-bucket", "runs/abcdef123456/docs/readme_comprehension/summary.json")
	if !exists {
		t.Fatalf("summary.json was not written to storage")
	}
	if summaryObj.ContentType != "application/json" {
		t.Errorf("Expected summary content type %q, got %q", "application/json", summaryObj.ContentType)
	}
	if summaryObj.ContentEncoding != "" {
		t.Errorf("Expected summary content encoding empty, got %q", summaryObj.ContentEncoding)
	}

	var summary artifacts.TaskSummary
	if err := json.Unmarshal(summaryObj.Data, &summary); err != nil {
		t.Fatalf("Failed to parse summary.json: %v", err)
	}

	if summary.LogFilename != logFilename {
		t.Errorf("Expected LogFilename %q, got %q", logFilename, summary.LogFilename)
	}
	if summary.LogHash != expectedHash {
		t.Errorf("Expected LogHash %q, got %q", expectedHash, summary.LogHash)
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
	if summary.SampleCount != 10 {
		t.Errorf("Expected SampleCount 10, got %d", summary.SampleCount)
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
		TaskID:      "docs/readme_comprehension",
		Model:       "test-model",
		Commit:      "1234567890ab",
		SampleCount: 5,
		Successes:   5,
	}

	largeLog := []byte(strings.Repeat("A", 100)) // 100 bytes > 30 byte cap

	// 1. StoreArtifact over limit is refused: error names task, file, size, and limit
	_, err = sink.StoreArtifact(t.Context(), meta, "large.json", largeLog)
	if err == nil {
		t.Fatalf("Expected StoreArtifact to fail when file exceeds cap limit, got nil")
	}

	var tooLargeErr *artifacts.ArtifactTooLargeError
	if !errors.As(err, &tooLargeErr) {
		t.Fatalf("Expected error of type *artifacts.ArtifactTooLargeError, got %T: %v", err, err)
	}
	if tooLargeErr.Task != "docs/readme_comprehension" {
		t.Errorf("Expected task %q, got %q", "docs/readme_comprehension", tooLargeErr.Task)
	}
	if tooLargeErr.File != "large.json" {
		t.Errorf("Expected file %q, got %q", "large.json", tooLargeErr.File)
	}
	if tooLargeErr.Size != 100 {
		t.Errorf("Expected size 100, got %d", tooLargeErr.Size)
	}
	if tooLargeErr.Limit != capLimit {
		t.Errorf("Expected limit %d, got %d", capLimit, tooLargeErr.Limit)
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "docs/readme_comprehension") ||
		!strings.Contains(errMsg, "large.json") ||
		!strings.Contains(errMsg, "100") ||
		!strings.Contains(errMsg, "30") {
		t.Errorf("Error message must name task, file, size, and limit, got: %s", errMsg)
	}

	// Verify nothing was written for it: no partial object, no summary.json
	if _, exists := fakeClient.GetObject("my-eval-bucket", "runs/1234567890ab/docs/readme_comprehension/large.json"); exists {
		t.Fatalf("Expected no log object written when cap is exceeded")
	}
	if _, exists := fakeClient.GetObject("my-eval-bucket", "runs/1234567890ab/docs/readme_comprehension/summary.json"); exists {
		t.Fatalf("Expected no summary.json written when cap is exceeded")
	}

	// 2. StoreTaskFile over limit is refused
	_, err = sink.StoreTaskFile(t.Context(), meta, "agent.json", largeLog)
	if err == nil {
		t.Fatalf("Expected StoreTaskFile to fail when file exceeds cap limit, got nil")
	}
	var taskFileErr *artifacts.ArtifactTooLargeError
	if !errors.As(err, &taskFileErr) {
		t.Fatalf("Expected *artifacts.ArtifactTooLargeError from StoreTaskFile, got %T: %v", err, err)
	}
	if taskFileErr.File != "agent.json" || taskFileErr.Task != "docs/readme_comprehension" {
		t.Errorf("Expected file agent.json and task docs/readme_comprehension, got %+v", taskFileErr)
	}
	if _, exists := fakeClient.GetObject("my-eval-bucket", "runs/1234567890ab/docs/readme_comprehension/agent.json"); exists {
		t.Fatalf("Expected no agent.json written when cap is exceeded")
	}

	// 3. StoreComparison over limit is refused
	_, err = sink.StoreComparison(t.Context(), "comp-1", largeLog)
	if err == nil {
		t.Fatalf("Expected StoreComparison to fail when comparison exceeds cap limit, got nil")
	}
	var compErr *artifacts.ArtifactTooLargeError
	if !errors.As(err, &compErr) {
		t.Fatalf("Expected *artifacts.ArtifactTooLargeError from StoreComparison, got %T: %v", err, err)
	}
	if _, exists := fakeClient.GetObject("my-eval-bucket", "comparisons/comp-1.json"); exists {
		t.Fatalf("Expected no comparison object written when cap is exceeded")
	}

	// 4. Old summary.json carrying capped and capBytes still loads
	oldSummaryJSON := []byte(`{
		"schemaVersion": 1,
		"taskId": "docs/readme_comprehension",
		"model": "test-model",
		"commit": "1234567890ab",
		"sampleCount": 5,
		"successes": 5,
		"capped": true,
		"capBytes": 30
	}`)
	var oldSummary artifacts.TaskSummary
	if err := json.Unmarshal(oldSummaryJSON, &oldSummary); err != nil {
		t.Fatalf("Expected old summary.json with capped field to load successfully: %v", err)
	}
	if oldSummary.TaskID != "docs/readme_comprehension" {
		t.Errorf("Expected TaskID %q, got %q", "docs/readme_comprehension", oldSummary.TaskID)
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
	stored, err := sink.StoreArtifact(t.Context(), meta, "eval.json", logData)
	if err != nil {
		t.Fatalf("First write of StoreArtifact against empty bucket failed: %v", err)
	}
	expectedLogURI := "gs://empty-bucket/runs/commit123/task-1/eval.json"
	if stored.Location != expectedLogURI {
		t.Errorf("Expected log URI %q, got %q", expectedLogURI, stored.Location)
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
	sd := 0.08
	costUSD := 0.04
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
		SampleCount:       len(samples),
		SuccessRate:       &rate,
		SuccessRateCI:     ci,
		SuccessRateSD:     &sd,
		CostUSD:           &costUSD,
		CostSource:        v1alpha1.CostSourceDefault,
		Samples:           samples,
		Successes:         1,
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
	if summary.SuccessRateSD == nil || *summary.SuccessRateSD != 0.08 {
		t.Errorf("Expected SuccessRateSD=0.08, got %v", summary.SuccessRateSD)
	}
	if summary.CostUSD == nil || *summary.CostUSD != 0.04 {
		t.Errorf("Expected CostUSD=0.04, got %v", summary.CostUSD)
	}
	if summary.CostSource != v1alpha1.CostSourceDefault {
		t.Errorf("Expected CostSource=default, got %q", summary.CostSource)
	}
	if len(summary.Samples) != 1 {
		t.Fatalf("Expected 1 sample, got %d", len(summary.Samples))
	}
	if summary.SampleCount != 1 {
		t.Errorf("Expected SampleCount=1, got %d", summary.SampleCount)
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
	meta.SampleCount = len(largeSamples)
	storedLarge, err := sink.StoreArtifact(t.Context(), meta, "inspect-large.json", []byte(`{}`))
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}
	summaryLarge, err := artifacts.ReadTaskSummary(t.Context(), sink, storedLarge.Location)
	if err != nil {
		t.Fatalf("ReadTaskSummary failed: %v", err)
	}
	if !summaryLarge.SamplesTruncated {
		t.Errorf("Expected SamplesTruncated=true for 10002 samples, got false")
	}
	if summaryLarge.SampleCount != 10002 {
		t.Errorf("Expected SampleCount=10002, got %d", summaryLarge.SampleCount)
	}
	if len(summaryLarge.Samples) != artifacts.MaxSummarySamples {
		t.Errorf("Expected %d samples, got %d", artifacts.MaxSummarySamples, len(summaryLarge.Samples))
	}
}

func TestCheckAccess(t *testing.T) {
	ctx := t.Context()
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(ctx, "eval-bucket", WithStorageClient(fakeClient))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := sink.CheckAccess(ctx); err != nil {
		t.Errorf("expected CheckAccess to succeed, got %v", err)
	}

	probeErr := errors.New("dial tcp 169.254.169.254:80: connect: connection refused")
	fakeClient.BucketAccessError = probeErr
	if err := sink.CheckAccess(ctx); !errors.Is(err, probeErr) {
		t.Errorf("expected CheckAccess to return probeErr, got %v", err)
	}
}

func TestRealStorageClient_CheckAccess(t *testing.T) {
	ctx := t.Context()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/b/test-bucket":
			// Bucket metadata refused with 403 (storage.buckets.get missing).
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusForbidden)
			_, _ = writer.Write([]byte(`{"error": {"code": 403, "message": "Caller does not have storage.buckets.get access to the Google Cloud Storage bucket."}}`))
		case "/b/test-bucket/o":
			// Object listing succeeds.
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"kind": "storage#objects"}`))
		case "/b/failing-bucket/o":
			// Real failure on object listing.
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusForbidden)
			_, _ = writer.Write([]byte(`{"error": {"code": 403, "message": "Caller does not have storage.objects.list access to the Google Cloud Storage bucket."}}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	gcsClient, err := storage.NewClient(ctx, option.WithEndpoint(server.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("failed to create storage client: %v", err)
	}
	defer func() {
		if closeErr := gcsClient.Close(); closeErr != nil {
			t.Errorf("error closing gcsClient: %v", closeErr)
		}
	}()

	client := &realStorageClient{client: gcsClient}

	t.Run("passes when object listing works and bucket metadata would be refused", func(t *testing.T) {
		if err := client.CheckAccess(ctx, "test-bucket"); err != nil {
			t.Errorf("expected CheckAccess to succeed when object listing works, got %v", err)
		}
	})

	t.Run("reports real failure when object listing fails", func(t *testing.T) {
		err := client.CheckAccess(ctx, "failing-bucket")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "403") {
			t.Errorf("expected 403 in error, got: %v", err)
		}
	})
}

func TestStoreManifest_ErrorNamesSinkOperation(t *testing.T) {
	ctx := t.Context()
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(ctx, "eval-bucket", WithStorageClient(fakeClient))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	meta := artifacts.RunMetadata{
		RunName: "run-1",
		Commit:  "abcdef12",
	}

	// Make write fail
	fakeClient.Errors["eval-bucket/runs/abcdef12/manifest.json"] = errors.New("connection refused")
	_, err = sink.StoreManifest(ctx, meta, []byte(`{"tasks":[]}`))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.HasPrefix(err.Error(), "store manifest to gs://eval-bucket/runs/abcdef12/manifest.json") {
		t.Errorf("expected error to name sink operation and gs:// URI, got: %v", err)
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("expected error to preserve underlying cause, got: %v", err)
	}
}

func TestWorkloadIdentityConfig(t *testing.T) {
	const provider = "//iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/my-pool/providers/my-provider"

	cfg := workloadIdentityConfig(provider)

	if cfg.Audience != provider {
		t.Errorf("Audience = %q, want %q", cfg.Audience, provider)
	}
	if cfg.SubjectTokenType != "urn:ietf:params:oauth:token-type:jwt" {
		t.Errorf("SubjectTokenType = %q, want urn:ietf:params:oauth:token-type:jwt", cfg.SubjectTokenType)
	}
	if cfg.TokenURL != "https://sts.googleapis.com/v1/token" {
		t.Errorf("TokenURL = %q, want https://sts.googleapis.com/v1/token", cfg.TokenURL)
	}
	if cfg.CredentialSource == nil || cfg.CredentialSource.File != DefaultTokenFilePath {
		t.Errorf("CredentialSource.File = %v, want %q", cfg.CredentialSource, DefaultTokenFilePath)
	}
	expectedScopes := []string{"https://www.googleapis.com/auth/cloud-platform"}
	if !reflect.DeepEqual(cfg.Scopes, expectedScopes) {
		t.Errorf("Scopes = %v, want %v", cfg.Scopes, expectedScopes)
	}
}

func TestBuildClientOptions(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name          string
		provider      string
		expectOptions int
	}{
		{
			name:          "empty provider",
			provider:      "",
			expectOptions: 0,
		},
		{
			name:          "whitespace provider",
			provider:      "   ",
			expectOptions: 0,
		},
		{
			name:          "valid provider",
			provider:      "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/p/providers/pr",
			expectOptions: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := BuildClientOptions(ctx, tt.provider)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(opts) != tt.expectOptions {
				t.Errorf("expected %d options, got %d", tt.expectOptions, len(opts))
			}
			if tt.expectOptions > 0 {
				optStr := fmt.Sprintf("%#v", opts[0])
				if !strings.Contains(optStr, "withTokenSource") {
					t.Errorf("expected option to be WithTokenSource, got %s", optStr)
				}
			}
		})
	}

	// Real storage client construction: when provider is set, storage.NewClient
	// initializes cleanly with the token source without making network calls.
	const audience = "//iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/my-pool/providers/my-provider"
	sinkReal, err := New(ctx, "eval-bucket", WithWorkloadIdentityProvider(audience))
	if err != nil {
		t.Fatalf("New with real storage client and workload identity failed: %v", err)
	}
	defer func() {
		if closeErr := sinkReal.Close(); closeErr != nil {
			t.Errorf("Close failed: %v", closeErr)
		}
	}()
}

func TestStoreTaskFile_SetsMetadataSHA256(t *testing.T) {
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(t.Context(), "eval-bucket", WithStorageClient(fakeClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	meta := artifacts.RunMetadata{
		RunName: "test-run",
		TaskID:  "task-agent",
		Commit:  "c123456",
	}
	agentData := []byte(`{"steps":[{"duration":123}]}`)
	uri, err := sink.StoreTaskFile(t.Context(), meta, "agent.json", agentData)
	if err != nil {
		t.Fatalf("StoreTaskFile failed: %v", err)
	}
	expectedURI := "gs://eval-bucket/runs/c123456/task-agent/agent.json"
	if uri != expectedURI {
		t.Errorf("Expected URI %q, got %q", expectedURI, uri)
	}

	agentObj, ok := fakeClient.GetObject("eval-bucket", "runs/c123456/task-agent/agent.json")
	if !ok {
		t.Fatalf("Expected agent.json object in fake storage")
	}
	expectedHash := artifacts.ArtifactHash(agentData)
	if agentObj.Metadata == nil || agentObj.Metadata["sha256"] != expectedHash {
		t.Errorf("Expected metadata sha256=%q, got %+v", expectedHash, agentObj.Metadata)
	}
}

func TestTaskSummary_LegacyWithoutHashLoads(t *testing.T) {
	legacyJSON := []byte(`{
		"schemaVersion": 1,
		"taskId": "task-legacy",
		"model": "model-1",
		"commit": "c1",
		"logFilename": "eval.json",
		"sampleCount": 5
	}`)
	var summary artifacts.TaskSummary
	if err := json.Unmarshal(legacyJSON, &summary); err != nil {
		t.Fatalf("Failed to unmarshal legacy summary: %v", err)
	}
	if summary.LogFilename != "eval.json" {
		t.Errorf("Expected LogFilename eval.json, got %q", summary.LogFilename)
	}
	if summary.LogHash != "" {
		t.Errorf("Expected LogHash to be empty for legacy summary, got %q", summary.LogHash)
	}
}

func TestGCSSink_LoadLegacyUncompressed(t *testing.T) {
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(t.Context(), "eval-bucket", WithStorageClient(fakeClient))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	legacyContent := []byte(`{"results": {"score": 1.0}}`)
	fakeClient.Objects["eval-bucket/runs/legacy1/task1/log.json"] = fakes.FakeObject{
		Bucket:      "eval-bucket",
		Object:      "runs/legacy1/task1/log.json",
		ContentType: "application/json",
		Data:        legacyContent,
	}

	loaded, err := sink.Load(t.Context(), "gs://eval-bucket/runs/legacy1/task1/log.json")
	if err != nil {
		t.Fatalf("Failed to load legacy uncompressed object: %v", err)
	}
	if string(loaded) != string(legacyContent) {
		t.Errorf("Expected %s, got %s", legacyContent, loaded)
	}
}

func TestStreamCompressionAndDecompression(t *testing.T) {
	raw := []byte(`{"message": "hello world", "repeat": "aaaaaaaaaaaaaaaa"}`)
	compressed, err := compressBytes(raw)
	if err != nil {
		t.Fatalf("compressBytes failed: %v", err)
	}
	if len(compressed) < 2 || compressed[0] != 0x1f || compressed[1] != 0x8b {
		t.Fatalf("expected gzip magic bytes")
	}

	decompressed, err := decompressBytes(compressed)
	if err != nil {
		t.Fatalf("decompressBytes failed: %v", err)
	}
	if string(decompressed) != string(raw) {
		t.Errorf("decompressed mismatch: expected %s, got %s", raw, decompressed)
	}

	decompressedPlain, err := decompressBytes(raw)
	if err != nil {
		t.Fatalf("decompressBytes on plain data failed: %v", err)
	}
	if string(decompressedPlain) != string(raw) {
		t.Errorf("plain data should return unchanged: expected %s, got %s", raw, decompressedPlain)
	}
}

func TestGCSSink_OpenStream(t *testing.T) {
	cacheDirectory := t.TempDir()
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(t.Context(), "gs://my-eval-bucket", WithStorageClient(fakeClient), WithCacheDirectory(cacheDirectory))
	if err != nil {
		t.Fatalf("Failed to create GCSSink: %v", err)
	}

	assertStreamContent := func(t *testing.T, stream io.ReadCloser, expected []byte) {
		t.Helper()
		data, readError := io.ReadAll(stream)
		if readError != nil {
			t.Fatalf("ReadAll failed: %v", readError)
		}
		if closeError := stream.Close(); closeError != nil {
			t.Fatalf("Close stream failed: %v", closeError)
		}
		if string(data) != string(expected) {
			t.Errorf("content mismatch: expected %q, got %q", string(expected), string(data))
		}
	}

	rawContent := []byte(`{"results": {"accuracy": 0.95}, "trace": "sample evaluation data"}`)
	metadata := artifacts.RunMetadata{
		Commit: "1234567890abcdef",
		TaskID: "benchmarks/math",
	}

	stored, err := sink.StoreArtifact(t.Context(), metadata, "eval.json", rawContent)
	if err != nil {
		t.Fatalf("StoreArtifact failed: %v", err)
	}

	expectedKey := strings.TrimPrefix(stored.Hash, "sha256:")
	cachedFilePath := filepath.Join(cacheDirectory, expectedKey)

	// 1. Read-back: stream returns decompressed content backed by local file storing compressed bytes
	stream, err := sink.OpenStream(t.Context(), stored.Location, stored.Hash)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	assertStreamContent(t, stream, rawContent)
	cachedBytes, err := os.ReadFile(cachedFilePath)
	if err != nil {
		t.Errorf("expected cached file at %s: %v", cachedFilePath, err)
	} else if len(cachedBytes) < 2 || cachedBytes[0] != 0x1f || cachedBytes[1] != 0x8b {
		t.Errorf("expected cached file to store compressed gzip bytes, got plain or invalid")
	}

	// 2. Cache hit: caller knowing the hash does not contact store at all
	fakeClient.Errors["my-eval-bucket/runs/1234567890abcdef/benchmarks/math/eval.json"] = errors.New("remote store error: must not be contacted")
	cachedStream, err := sink.OpenStream(t.Context(), stored.Location, stored.Hash)
	if err != nil {
		t.Fatalf("OpenStream from cache failed: %v", err)
	}
	assertStreamContent(t, cachedStream, rawContent)

	// 3. Artifact without known hash (legacy uncompressed): computes hash as bytes arrive, keeps under hash
	legacyContent := []byte(`{"legacy": true, "score": 1.0}`)
	legacyHash := artifacts.ArtifactHash(legacyContent)
	legacyKey := strings.TrimPrefix(legacyHash, "sha256:")
	fakeClient.Objects["my-eval-bucket/runs/legacy/task1/log.json"] = fakes.FakeObject{
		Bucket:      "my-eval-bucket",
		Object:      "runs/legacy/task1/log.json",
		ContentType: "application/json",
		Data:        legacyContent,
	}

	legacyStream, err := sink.OpenStream(t.Context(), "gs://my-eval-bucket/runs/legacy/task1/log.json", "")
	if err != nil {
		t.Fatalf("OpenStream without hash failed: %v", err)
	}
	assertStreamContent(t, legacyStream, legacyContent)
	legacyCachedPath := filepath.Join(cacheDirectory, legacyKey)
	if _, err := os.Stat(legacyCachedPath); err != nil {
		t.Errorf("expected legacy cached file at %s: %v", legacyCachedPath, err)
	}

	// 4. Source that fails mid-stream: returns error and leaves nothing behind in cache
	midStreamKey := "my-eval-bucket/runs/midstream/task1/log.json"
	fakeClient.StreamReaders[midStreamKey] = io.NopCloser(io.MultiReader(
		strings.NewReader(`{"partial": "content before connection fails abruptly"}`),
		iotest.ErrReader(errors.New("connection reset mid-stream")),
	))
	entriesBefore, err := os.ReadDir(cacheDirectory)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	_, midStreamError := sink.OpenStream(t.Context(), "gs://"+midStreamKey, "")
	if midStreamError == nil {
		t.Fatalf("expected error from mid-stream source failure, got nil")
	}
	entriesAfter, err := os.ReadDir(cacheDirectory)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entriesAfter) != len(entriesBefore) {
		t.Errorf("expected no files left behind after mid-stream failure: before %d, after %d", len(entriesBefore), len(entriesAfter))
	}
}

func TestGCSSink_StorePrebuildMarkerAndLoad(t *testing.T) {
	ctx := t.Context()
	fakeClient := fakes.NewFakeStorageClient()
	sink, err := New(ctx, "eval-bucket", WithStorageClient(fakeClient))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	var _ artifacts.PrebuildMarkerStorer = sink

	marker := []byte(`{"commit":"abcdef12","prebuildName":"kind-images","completedAt":"2026-11-28T12:00:00Z"}`)
	expectedURI := "gs://eval-bucket/runs/abcdef12/prebuilds/kind-images/done.json"

	target := artifacts.PrebuildMarkerTarget(sink, "abcdef12", "kind-images")
	if target != expectedURI {
		t.Fatalf("PrebuildMarkerTarget = %q, want %q", target, expectedURI)
	}

	storedURI, err := sink.StorePrebuildMarker(ctx, "abcdef12", "kind-images", marker)
	if err != nil {
		t.Fatalf("StorePrebuildMarker failed: %v", err)
	}
	if storedURI != expectedURI {
		t.Fatalf("storedURI = %q, want %q", storedURI, expectedURI)
	}

	loaded, err := sink.Load(ctx, target)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if string(loaded) != string(marker) {
		t.Errorf("loaded = %q, want %q", string(loaded), string(marker))
	}
}
