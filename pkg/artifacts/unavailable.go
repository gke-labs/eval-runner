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
	"fmt"
	"strings"
)

// UnavailableSink represents an ArtifactSink that is configured but currently unavailable
// (e.g. because credentials cannot be resolved or access probing failed).
type UnavailableSink struct {
	bucket string
	err    error
}

var _ ArtifactSink = (*UnavailableSink)(nil)
var _ AccessChecker = (*UnavailableSink)(nil)

// NewUnavailableSink creates a new UnavailableSink for the given bucket and underlying error.
func NewUnavailableSink(bucket string, err error) *UnavailableSink {
	return &UnavailableSink{
		bucket: bucket,
		err:    err,
	}
}

// Err returns the underlying error that caused the sink to be unavailable.
func (s *UnavailableSink) Err() error {
	return s.err
}

// CheckAccess implements AccessChecker, returning the underlying unavailability error.
func (s *UnavailableSink) CheckAccess(ctx context.Context) error {
	return s.err
}

// StoreArtifact implements ArtifactSink, failing immediately with the unavailability error.
func (s *UnavailableSink) StoreArtifact(ctx context.Context, meta RunMetadata, filename string, data []byte) (string, error) {
	return "", fmt.Errorf("artifactBucket is set but artifact storage is unavailable: %w", s.err)
}

// StoreComparison implements ArtifactSink, failing immediately with the unavailability error.
func (s *UnavailableSink) StoreComparison(ctx context.Context, comparisonName string, data []byte) (string, error) {
	return "", fmt.Errorf("artifactBucket is set but artifact storage is unavailable: %w", s.err)
}

// StoreManifest implements ArtifactSink, failing immediately with the unavailability error.
func (s *UnavailableSink) StoreManifest(ctx context.Context, meta RunMetadata, data []byte) (string, error) {
	return "", fmt.Errorf("artifactBucket is set but artifact storage is unavailable: %w", s.err)
}

// Load implements ArtifactSink, failing immediately with the unavailability error.
func (s *UnavailableSink) Load(ctx context.Context, path string) ([]byte, error) {
	return nil, fmt.Errorf("artifactBucket is set but artifact storage is unavailable: %w", s.err)
}

// ArtifactPrefix implements ArtifactSink.
func (s *UnavailableSink) ArtifactPrefix(commit string) string {
	b := strings.TrimSpace(s.bucket)
	b = strings.TrimPrefix(b, "gs://")
	b = strings.TrimSuffix(b, "/")
	if b == "" || strings.TrimSpace(commit) == "" {
		return ""
	}
	return fmt.Sprintf("gs://%s/runs/%s/", b, commit)
}

// IsDurable implements ArtifactSink.
func (s *UnavailableSink) IsDurable() bool {
	return true
}

// Close implements ArtifactSink.
func (s *UnavailableSink) Close() error {
	return nil
}

// IsUnavailable reports whether the provided sink (unwrapping ReloadableSink if necessary)
// is an UnavailableSink, returning the underlying error if true.
func IsUnavailable(sink ArtifactSink) (error, bool) {
	if sink == nil {
		return nil, false
	}
	if rs, ok := sink.(*ReloadableSink); ok {
		return IsUnavailable(rs.Current())
	}
	if us, ok := sink.(*UnavailableSink); ok {
		return us.Err(), true
	}
	return nil, false
}
