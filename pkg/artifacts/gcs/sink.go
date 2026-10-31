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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
)

const (
	// DefaultMaxUploadBytes is the default cap on uploaded artifact size (10 MiB).
	DefaultMaxUploadBytes int64 = 10 * 1024 * 1024

	// DefaultMaxRetryDuration is the maximum duration for retrying GCS operations (60s).
	DefaultMaxRetryDuration = 60 * time.Second
)

// StorageClient defines the interface for interacting with GCS.
type StorageClient interface {
	WriteObject(ctx context.Context, bucket, object, contentType string, data []byte) error
	WriteObjectWithMetadata(ctx context.Context, bucket, object, contentType string, metadata map[string]string, data []byte) error
	ReadObject(ctx context.Context, bucket, object string, maxBytes int64) ([]byte, error)
	CheckAccess(ctx context.Context, bucket string) error
	Close() error
}

type realStorageClient struct {
	client *storage.Client
}

func (c *realStorageClient) CheckAccess(ctx context.Context, bucket string) error {
	_, err := c.client.Bucket(bucket).Attrs(ctx)
	return err
}

func (c *realStorageClient) WriteObject(ctx context.Context, bucket, object, contentType string, data []byte) error {
	return c.WriteObjectWithMetadata(ctx, bucket, object, contentType, nil, data)
}

func (c *realStorageClient) WriteObjectWithMetadata(ctx context.Context, bucket, object, contentType string, metadata map[string]string, data []byte) error {
	w := c.client.Bucket(bucket).Object(object).NewWriter(ctx)
	if contentType != "" {
		w.ContentType = contentType
	}
	if len(metadata) > 0 {
		w.Metadata = metadata
	}
	if _, err := w.Write(data); err != nil {
		return errors.Join(err, w.Close())
	}
	return w.Close()
}

func (c *realStorageClient) ReadObject(ctx context.Context, bucket, object string, maxBytes int64) ([]byte, error) {
	rc, err := c.client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	limit := maxBytes
	if limit <= 0 {
		limit = DefaultMaxUploadBytes
	}
	data, err := io.ReadAll(io.LimitReader(rc, limit+1))
	closeErr := rc.Close()
	if err != nil {
		return nil, errors.Join(err, closeErr)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("object gs://%s/%s exceeds maximum size of %d bytes", bucket, object, limit)
	}
	return data, nil
}

func (c *realStorageClient) Close() error {
	return c.client.Close()
}

// GCSSink implements artifacts.ArtifactSink to write Inspect logs and summary.json to Google Cloud Storage.
type GCSSink struct {
	bucket         string
	maxUploadBytes int64
	client         StorageClient
	mu             sync.Mutex
	closed         bool
}

var _ artifacts.ArtifactSink = (*GCSSink)(nil)
var _ artifacts.AccessChecker = (*GCSSink)(nil)

// Option configures GCSSink.
type Option func(*GCSSink)

// WithStorageClient injects a custom StorageClient (e.g. for testing).
func WithStorageClient(c StorageClient) Option {
	return func(s *GCSSink) {
		s.client = c
	}
}

// WithMaxUploadBytes sets the maximum artifact upload size in bytes.
func WithMaxUploadBytes(bytes int64) Option {
	return func(s *GCSSink) {
		s.maxUploadBytes = bytes
	}
}

// New creates a new GCSSink targeting the specified bucket.
func New(ctx context.Context, bucket string, opts ...Option) (*GCSSink, error) {
	b, err := normalizeAndValidateBucket(bucket)
	if err != nil {
		return nil, err
	}

	sink := &GCSSink{
		bucket:         b,
		maxUploadBytes: DefaultMaxUploadBytes,
	}
	for _, opt := range opts {
		opt(sink)
	}

	if sink.client == nil {
		sc, err := storage.NewClient(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCS client: %w", err)
		}
		sc.SetRetry(storage.WithMaxRetryDuration(DefaultMaxRetryDuration))
		sink.client = &realStorageClient{client: sc}
	}

	return sink, nil
}

// CheckAccess probes credentials and bucket access by querying Bucket.Attrs on the target bucket.
func (s *GCSSink) CheckAccess(ctx context.Context) error {
	if s.bucket == "" {
		return fmt.Errorf("bucket name is empty")
	}
	if s.client == nil {
		return fmt.Errorf("storage client is nil")
	}
	return s.client.CheckAccess(ctx, s.bucket)
}

// Bucket returns the target bucket name.
func (s *GCSSink) Bucket() string {
	return s.bucket
}

// MaxUploadBytes returns the configured maximum upload byte size.
func (s *GCSSink) MaxUploadBytes() int64 {
	return s.maxUploadBytes
}

// Close closes the underlying storage client.
func (s *GCSSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// IsDurable implements artifacts.ArtifactSink for GCSSink.
func (s *GCSSink) IsDurable() bool {
	return true
}

// ArtifactPrefix returns the run-level storage prefix for a given commit.
func (s *GCSSink) ArtifactPrefix(commit string) string {
	if s.bucket == "" || strings.TrimSpace(commit) == "" {
		return ""
	}
	return fmt.Sprintf("gs://%s/runs/%s/", s.bucket, commit)
}

// StoreManifest persists the discovered manifest JSON as runs/<commit>/manifest.json with run/commit/task-count metadata.
// GCS must error, not truncate, above the cap.
// Writing the same object twice for the same commit is a no-op when the hash matches.
func (s *GCSSink) StoreManifest(ctx context.Context, meta artifacts.RunMetadata, data []byte) (string, error) {
	if err := artifacts.ValidateSafeComponent("commit", meta.Commit); err != nil {
		return "", err
	}
	if s.bucket == "" {
		return "", fmt.Errorf("bucket name is empty")
	}

	capLimit := s.maxUploadBytes
	if capLimit <= 0 {
		capLimit = DefaultMaxUploadBytes
	}

	if int64(len(data)) > capLimit {
		return "", fmt.Errorf("manifest size %d exceeds maximum upload size of %d bytes", len(data), capLimit)
	}

	manifestObject := fmt.Sprintf("runs/%s/manifest.json", meta.Commit)

	// Idempotency: writing the same object twice for the same commit is a no-op when the hash matches.
	existing, err := s.client.ReadObject(ctx, s.bucket, manifestObject, capLimit)
	if err == nil {
		if bytes.Equal(existing, data) {
			return fmt.Sprintf("gs://%s/%s", s.bucket, manifestObject), nil
		}
	} else if !isObjectNotFound(err) {
		return "", fmt.Errorf("store manifest to gs://%s/%s: failed to check existing manifest: %w", s.bucket, manifestObject, err)
	}

	metaMap := map[string]string{
		"run":        meta.RunName,
		"commit":     meta.Commit,
		"task-count": strconv.Itoa(meta.TaskCount),
	}

	if err := s.client.WriteObjectWithMetadata(ctx, s.bucket, manifestObject, "application/json", metaMap, data); err != nil {
		return "", fmt.Errorf("store manifest to gs://%s/%s: %w", s.bucket, manifestObject, err)
	}

	return fmt.Sprintf("gs://%s/%s", s.bucket, manifestObject), nil
}

// StoreComparison persists comparison summary data as comparisons/<comparisonName>.json and returns the gs:// URI.
func (s *GCSSink) StoreComparison(ctx context.Context, comparisonName string, data []byte) (string, error) {
	if err := artifacts.ValidateSafeComponent("comparisonName", comparisonName); err != nil {
		return "", err
	}
	if s.bucket == "" {
		return "", fmt.Errorf("bucket name is empty")
	}

	capLimit := s.maxUploadBytes
	if capLimit <= 0 {
		capLimit = DefaultMaxUploadBytes
	}

	uploadData := data
	if int64(len(data)) > capLimit {
		uploadData = data[:capLimit]
	}

	compObject := fmt.Sprintf("comparisons/%s.json", comparisonName)
	if err := s.client.WriteObject(ctx, s.bucket, compObject, "application/json", uploadData); err != nil {
		return "", fmt.Errorf("store comparison to gs://%s/%s: %w", s.bucket, compObject, err)
	}

	return fmt.Sprintf("gs://%s/%s", s.bucket, compObject), nil
}

// Load retrieves raw artifact bytes from the given storage path or URI, bounded by the maximum allowed artifact size.
func (s *GCSSink) Load(ctx context.Context, path string) ([]byte, error) {
	cleanPath := strings.TrimSpace(path)
	if cleanPath == "" {
		return nil, fmt.Errorf("artifact path must not be empty")
	}

	bucket := s.bucket
	object := cleanPath

	if strings.HasPrefix(cleanPath, "gs://") {
		trimmed := strings.TrimPrefix(cleanPath, "gs://")
		parts := strings.SplitN(trimmed, "/", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
			return nil, fmt.Errorf("invalid gs:// URI %q", path)
		}
		if s.bucket != "" && parts[0] != s.bucket {
			return nil, fmt.Errorf("bucket mismatch: requested %q but sink configured for %q", parts[0], s.bucket)
		}
		bucket = parts[0]
		object = parts[1]
	} else {
		object = strings.TrimPrefix(object, "/")
	}

	if bucket == "" {
		return nil, fmt.Errorf("bucket name is empty")
	}

	if !strings.HasPrefix(object, "runs/") && !strings.HasPrefix(object, "comparisons/") {
		return nil, fmt.Errorf("artifact path %q does not have required 'runs/' or 'comparisons/' prefix", path)
	}

	capLimit := s.maxUploadBytes
	if capLimit <= 0 {
		capLimit = DefaultMaxUploadBytes
	}

	data, err := s.client.ReadObject(ctx, bucket, object, capLimit)
	if err != nil {
		if isObjectNotFound(err) {
			return nil, fmt.Errorf("%w: failed to read artifact from gs://%s/%s: %v", artifacts.ErrNotFound, bucket, object, err)
		}
		return nil, fmt.Errorf("load artifact from gs://%s/%s: %w", bucket, object, err)
	}

	return data, nil
}

// StoreArtifact writes the inspect log and summary.json to GCS and returns the gs:// URI.
func (s *GCSSink) StoreArtifact(ctx context.Context, meta artifacts.RunMetadata, filename string, data []byte) (string, error) {
	if err := artifacts.ValidateArtifact(meta, filename); err != nil {
		return "", err
	}
	if meta.Commit == "" {
		return "", fmt.Errorf("commit must not be empty")
	}
	if s.bucket == "" {
		return "", fmt.Errorf("bucket name is empty")
	}

	capLimit := s.maxUploadBytes
	if capLimit <= 0 {
		capLimit = DefaultMaxUploadBytes
	}

	capped := false
	uploadData := data
	if int64(len(data)) > capLimit {
		capped = true
		uploadData = data[:capLimit]
	}

	logObject := fmt.Sprintf("runs/%s/%s/%s", meta.Commit, meta.TaskID, filename)
	if err := s.client.WriteObject(ctx, s.bucket, logObject, "application/json", uploadData); err != nil {
		return "", fmt.Errorf("store artifact to gs://%s/%s: %w", s.bucket, logObject, err)
	}

	sampleList := meta.Samples
	samplesTruncated := meta.SamplesTruncated
	if len(sampleList) > artifacts.MaxSummarySamples {
		sampleList = sampleList[:artifacts.MaxSummarySamples]
		samplesTruncated = true
	}
	sampleCount := meta.SampleCount

	summary := artifacts.TaskSummary{
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
		Capped:            capped,
		SampleRange:       meta.SampleRange,
		Tokens:            meta.Tokens,
		Latency:           meta.Latency,
		CostUSD:           meta.CostUSD,
		CostSource:        meta.CostSource,
		PossibleSamples:   meta.PossibleSamples,
	}
	if capped {
		summary.CapBytes = capLimit
	}

	summaryBytes, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal summary.json: %w", err)
	}
	summaryBytes = append(summaryBytes, '\n')

	summaryObject := fmt.Sprintf("runs/%s/%s/summary.json", meta.Commit, meta.TaskID)
	if err := s.client.WriteObject(ctx, s.bucket, summaryObject, "application/json", summaryBytes); err != nil {
		return "", fmt.Errorf("store summary to gs://%s/%s: %w", s.bucket, summaryObject, err)
	}

	return fmt.Sprintf("gs://%s/%s", s.bucket, logObject), nil
}

// ValidateBucket checks whether raw is a well-formed GCS bucket name or gs:// URI.
func ValidateBucket(raw string) error {
	_, err := normalizeAndValidateBucket(raw)
	return err
}

func normalizeAndValidateBucket(raw string) (string, error) {
	b := strings.TrimSpace(raw)
	b = strings.TrimPrefix(b, "gs://")
	b = strings.TrimSuffix(b, "/")
	b = strings.TrimSpace(b)
	if b == "" {
		return "", fmt.Errorf("bucket name must not be empty")
	}
	if strings.ContainsAny(b, " \t\r\n") || strings.Contains(b, "/") || strings.Contains(b, "\\") || strings.Contains(b, "..") || strings.ContainsRune(b, 0) {
		return "", fmt.Errorf("bucket name %q contains invalid characters", raw)
	}
	return b, nil
}

func isObjectNotFound(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, storage.ErrObjectNotExist) || artifacts.IsNotFound(err) || strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "not found")
}
