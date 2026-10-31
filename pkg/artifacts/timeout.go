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
	"time"
)

const defaultGraceWindow = 250 * time.Millisecond

func manifestTarget(sink ArtifactSink, commit string) string {
	if sink != nil {
		if prefix := sink.ArtifactPrefix(commit); prefix != "" {
			return prefix + "manifest.json"
		}
	}
	return commit
}

// ArtifactTarget resolves the storage target or URI for a task artifact.
// If the sink implements TaskArtifactPather, it delegates to TaskArtifactPath.
// Otherwise it derives the path using the sink's ArtifactPrefix.
func ArtifactTarget(sink ArtifactSink, commit, taskID, filename string) string {
	if pather, ok := sink.(TaskArtifactPather); ok {
		if path := pather.TaskArtifactPath(commit, taskID, filename); path != "" {
			return path
		}
	}
	if sink != nil {
		if prefix := sink.ArtifactPrefix(commit); prefix != "" {
			return fmt.Sprintf("%s%s/%s", prefix, taskID, filename)
		}
	}
	if commit != "" && taskID != "" {
		return fmt.Sprintf("%s/%s/%s", commit, taskID, filename)
	}
	if taskID != "" {
		return fmt.Sprintf("%s/%s", taskID, filename)
	}
	if commit != "" {
		return fmt.Sprintf("%s/%s", commit, filename)
	}
	return filename
}

func comparisonTarget(sink ArtifactSink, compName string) string {
	if sink != nil {
		if prefix := sink.ArtifactPrefix("_"); prefix != "" && strings.HasSuffix(prefix, "runs/_/") {
			return strings.TrimSuffix(prefix, "runs/_/") + fmt.Sprintf("comparisons/%s.json", compName)
		}
	}
	return fmt.Sprintf("comparisons/%s.json", compName)
}

// StoreManifestWithTimeout executes sink.StoreManifest concurrently, returning immediately if ctx expires.
func StoreManifestWithTimeout(ctx context.Context, sink ArtifactSink, meta RunMetadata, data []byte) (string, error) {
	if sink == nil {
		return "", ErrNoSinkConfigured
	}
	type result struct {
		uri string
		err error
	}
	ch := make(chan result, 1)
	go func() {
		uri, err := sink.StoreManifest(ctx, meta, data)
		ch <- result{uri: uri, err: err}
	}()

	select {
	case r := <-ch:
		return r.uri, r.err
	case <-ctx.Done():
		select {
		case r := <-ch:
			return r.uri, r.err
		case <-time.After(defaultGraceWindow):
			target := manifestTarget(sink, meta.Commit)
			return "", fmt.Errorf("store manifest for %s: sink did not report completion within deadline: %w", target, ctx.Err())
		}
	}
}

// StoreArtifactWithTimeout executes sink.StoreArtifact concurrently, returning immediately if ctx expires.
func StoreArtifactWithTimeout(ctx context.Context, sink ArtifactSink, meta RunMetadata, filename string, data []byte) (string, error) {
	if sink == nil {
		return "", ErrNoSinkConfigured
	}
	type result struct {
		path string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		path, err := sink.StoreArtifact(ctx, meta, filename, data)
		ch <- result{path: path, err: err}
	}()

	select {
	case r := <-ch:
		return r.path, r.err
	case <-ctx.Done():
		select {
		case r := <-ch:
			return r.path, r.err
		case <-time.After(defaultGraceWindow):
			target := ArtifactTarget(sink, meta.Commit, meta.TaskID, filename)
			return "", fmt.Errorf("store artifact for %s: sink did not report completion within deadline: %w", target, ctx.Err())
		}
	}
}

// StoreComparisonWithTimeout executes sink.StoreComparison concurrently, returning immediately if ctx expires.
func StoreComparisonWithTimeout(ctx context.Context, sink ArtifactSink, compName string, data []byte) (string, error) {
	if sink == nil {
		return "", ErrNoSinkConfigured
	}
	type result struct {
		path string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		path, err := sink.StoreComparison(ctx, compName, data)
		ch <- result{path: path, err: err}
	}()

	select {
	case r := <-ch:
		return r.path, r.err
	case <-ctx.Done():
		select {
		case r := <-ch:
			return r.path, r.err
		case <-time.After(defaultGraceWindow):
			target := comparisonTarget(sink, compName)
			return "", fmt.Errorf("store comparison for %s: sink did not report completion within deadline: %w", target, ctx.Err())
		}
	}
}

// LoadWithTimeout executes sink.Load concurrently, returning immediately if ctx expires.
func LoadWithTimeout(ctx context.Context, sink ArtifactSink, path string) ([]byte, error) {
	if sink == nil {
		return nil, ErrNoSinkConfigured
	}
	type result struct {
		data []byte
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		data, err := sink.Load(ctx, path)
		ch <- result{data: data, err: err}
	}()

	select {
	case r := <-ch:
		return r.data, r.err
	case <-ctx.Done():
		select {
		case r := <-ch:
			return r.data, r.err
		case <-time.After(defaultGraceWindow):
			return nil, fmt.Errorf("load artifact from %s: sink did not report completion within deadline: %w", path, ctx.Err())
		}
	}
}
