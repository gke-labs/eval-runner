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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/pkg/artifacts"
)

type blockingSink struct {
	artifacts.ArtifactSink
	blockStoreManifest   chan struct{}
	blockStoreArtifact   chan struct{}
	blockStoreComparison chan struct{}
	blockLoad            chan struct{}
}

func (b *blockingSink) StoreManifest(ctx context.Context, meta artifacts.RunMetadata, data []byte) (string, error) {
	if b.blockStoreManifest != nil {
		select {
		case <-b.blockStoreManifest:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "gs://bucket/manifest.json", nil
}

func (b *blockingSink) StoreArtifact(ctx context.Context, meta artifacts.RunMetadata, filename string, data []byte) (string, error) {
	if b.blockStoreArtifact != nil {
		select {
		case <-b.blockStoreArtifact:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "gs://bucket/artifact.json", nil
}

func (b *blockingSink) StoreComparison(ctx context.Context, comparisonName string, data []byte) (string, error) {
	if b.blockStoreComparison != nil {
		select {
		case <-b.blockStoreComparison:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "gs://bucket/comparison.json", nil
}

func (b *blockingSink) Load(ctx context.Context, path string) ([]byte, error) {
	if b.blockLoad != nil {
		select {
		case <-b.blockLoad:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return []byte("data"), nil
}

func (b *blockingSink) ArtifactPrefix(commit string) string {
	return "gs://bucket/runs/" + commit + "/"
}

type unyieldingSink struct {
	artifacts.ArtifactSink
}

func (u *unyieldingSink) StoreManifest(ctx context.Context, meta artifacts.RunMetadata, data []byte) (string, error) {
	ch := make(chan struct{})
	<-ch // Never returns, completely hung
	return "", nil
}

func (u *unyieldingSink) StoreArtifact(ctx context.Context, meta artifacts.RunMetadata, filename string, data []byte) (string, error) {
	ch := make(chan struct{})
	<-ch
	return "", nil
}

func (u *unyieldingSink) StoreComparison(ctx context.Context, compName string, data []byte) (string, error) {
	ch := make(chan struct{})
	<-ch
	return "", nil
}

func (u *unyieldingSink) Load(ctx context.Context, path string) ([]byte, error) {
	ch := make(chan struct{})
	<-ch
	return nil, nil
}

func (u *unyieldingSink) ArtifactPrefix(commit string) string {
	return "gs://bucket/runs/" + commit + "/"
}

func (u *unyieldingSink) IsDurable() bool {
	return true
}

func (u *unyieldingSink) Close() error {
	return nil
}

type graceWindowSink struct {
	artifacts.ArtifactSink
	errToReturn error
}

func (g *graceWindowSink) StoreManifest(ctx context.Context, meta artifacts.RunMetadata, data []byte) (string, error) {
	<-ctx.Done()
	time.Sleep(10 * time.Millisecond)
	return "", g.errToReturn
}

func (g *graceWindowSink) StoreArtifact(ctx context.Context, meta artifacts.RunMetadata, filename string, data []byte) (string, error) {
	<-ctx.Done()
	time.Sleep(10 * time.Millisecond)
	return "", g.errToReturn
}

func (g *graceWindowSink) StoreComparison(ctx context.Context, compName string, data []byte) (string, error) {
	<-ctx.Done()
	time.Sleep(10 * time.Millisecond)
	return "", g.errToReturn
}

func (g *graceWindowSink) Load(ctx context.Context, path string) ([]byte, error) {
	<-ctx.Done()
	time.Sleep(10 * time.Millisecond)
	return nil, g.errToReturn
}

func (g *graceWindowSink) ArtifactPrefix(commit string) string {
	return "gs://bucket/runs/" + commit + "/"
}

func (g *graceWindowSink) IsDurable() bool {
	return true
}

func (g *graceWindowSink) Close() error {
	return nil
}

func TestTimeoutWrappers_CancelPromptly(t *testing.T) {
	sink := &blockingSink{
		blockStoreManifest:   make(chan struct{}),
		blockStoreArtifact:   make(chan struct{}),
		blockStoreComparison: make(chan struct{}),
		blockLoad:            make(chan struct{}),
	}
	meta := artifacts.RunMetadata{
		RunName: "test-run",
		Commit:  "abcdef",
	}

	t.Run("StoreManifestWithTimeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := artifacts.StoreManifestWithTimeout(ctx, sink, meta, []byte("{}"))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > 1*time.Second {
			t.Fatalf("StoreManifestWithTimeout took too long: %v", elapsed)
		}
	})

	t.Run("StoreArtifactWithTimeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := artifacts.StoreArtifactWithTimeout(ctx, sink, meta, "eval.json", []byte("{}"))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > 1*time.Second {
			t.Fatalf("StoreArtifactWithTimeout took too long: %v", elapsed)
		}
	})

	t.Run("StoreComparisonWithTimeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := artifacts.StoreComparisonWithTimeout(ctx, sink, "comp", []byte("{}"))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > 1*time.Second {
			t.Fatalf("StoreComparisonWithTimeout took too long: %v", elapsed)
		}
	})

	t.Run("LoadWithTimeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := artifacts.LoadWithTimeout(ctx, sink, "gs://bucket/runs/abcdef/manifest.json")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > 1*time.Second {
			t.Fatalf("LoadWithTimeout took too long: %v", elapsed)
		}
	})

	t.Run("NilSink", func(t *testing.T) {
		ctx := t.Context()
		if _, err := artifacts.StoreManifestWithTimeout(ctx, nil, meta, []byte("{}")); !errors.Is(err, artifacts.ErrNoSinkConfigured) {
			t.Errorf("expected ErrNoSinkConfigured, got %v", err)
		}
		if _, err := artifacts.StoreArtifactWithTimeout(ctx, nil, meta, "f", []byte("{}")); !errors.Is(err, artifacts.ErrNoSinkConfigured) {
			t.Errorf("expected ErrNoSinkConfigured, got %v", err)
		}
		if _, err := artifacts.StoreComparisonWithTimeout(ctx, nil, "c", []byte("{}")); !errors.Is(err, artifacts.ErrNoSinkConfigured) {
			t.Errorf("expected ErrNoSinkConfigured, got %v", err)
		}
		if _, err := artifacts.LoadWithTimeout(ctx, nil, "p"); !errors.Is(err, artifacts.ErrNoSinkConfigured) {
			t.Errorf("expected ErrNoSinkConfigured, got %v", err)
		}
	})
}

func TestTimeoutWrappers_TimeoutMessageNamesTargetAndCause(t *testing.T) {
	sink := &unyieldingSink{}
	meta := artifacts.RunMetadata{
		RunName: "test-run",
		Commit:  "abcdef12",
		TaskID:  "task-abc",
	}

	t.Run("StoreManifestWithTimeout names operation and target", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		_, err := artifacts.StoreManifestWithTimeout(ctx, sink, meta, []byte("{}"))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		errMsg := err.Error()
		if !strings.HasPrefix(errMsg, "store manifest for gs://bucket/runs/abcdef12/manifest.json:") {
			t.Errorf("expected error to name operation and manifest target URI, got: %v", errMsg)
		}
		if !strings.Contains(errMsg, "sink did not report completion within deadline") {
			t.Errorf("expected error to indicate sink completion deadline, got: %v", errMsg)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected wrapped context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("StoreArtifactWithTimeout names operation and target", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		_, err := artifacts.StoreArtifactWithTimeout(ctx, sink, meta, "eval.json", []byte("{}"))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		errMsg := err.Error()
		if !strings.HasPrefix(errMsg, "store artifact for gs://bucket/runs/abcdef12/task-abc/eval.json:") {
			t.Errorf("expected error to name operation and artifact target URI, got: %v", errMsg)
		}
		if !strings.Contains(errMsg, "sink did not report completion within deadline") {
			t.Errorf("expected error to indicate sink completion deadline, got: %v", errMsg)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected wrapped context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("StoreComparisonWithTimeout names operation and target", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		_, err := artifacts.StoreComparisonWithTimeout(ctx, sink, "comp-xyz", []byte("{}"))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		errMsg := err.Error()
		if !strings.HasPrefix(errMsg, "store comparison for gs://bucket/comparisons/comp-xyz.json:") {
			t.Errorf("expected error to name operation and comparison target URI, got: %v", errMsg)
		}
		if !strings.Contains(errMsg, "sink did not report completion within deadline") {
			t.Errorf("expected error to indicate sink completion deadline, got: %v", errMsg)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected wrapped context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("LoadWithTimeout names operation and target", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		loadPath := "gs://bucket/runs/abcdef12/manifest.json"
		_, err := artifacts.LoadWithTimeout(ctx, sink, loadPath)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		errMsg := err.Error()
		if !strings.HasPrefix(errMsg, "load artifact from gs://bucket/runs/abcdef12/manifest.json:") {
			t.Errorf("expected error to name operation and target path, got: %v", errMsg)
		}
		if !strings.Contains(errMsg, "sink did not report completion within deadline") {
			t.Errorf("expected error to indicate sink completion deadline, got: %v", errMsg)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected wrapped context.DeadlineExceeded, got: %v", err)
		}
	})
}

func TestTimeoutWrappers_GraceWindowPrefersSinkError(t *testing.T) {
	sinkErr := errors.New("gcs retry exhausted: dial tcp 169.254.169.254:80: connect: connection refused")
	sink := &graceWindowSink{errToReturn: sinkErr}
	meta := artifacts.RunMetadata{
		RunName: "test-run",
		Commit:  "abcdef12",
		TaskID:  "task-abc",
	}

	t.Run("StoreManifestWithTimeout returns sink error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		_, err := artifacts.StoreManifestWithTimeout(ctx, sink, meta, []byte("{}"))
		if !errors.Is(err, sinkErr) {
			t.Fatalf("expected sink error %v within grace window, got: %v", sinkErr, err)
		}
	})

	t.Run("StoreArtifactWithTimeout returns sink error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		_, err := artifacts.StoreArtifactWithTimeout(ctx, sink, meta, "eval.json", []byte("{}"))
		if !errors.Is(err, sinkErr) {
			t.Fatalf("expected sink error %v within grace window, got: %v", sinkErr, err)
		}
	})

	t.Run("StoreComparisonWithTimeout returns sink error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		_, err := artifacts.StoreComparisonWithTimeout(ctx, sink, "comp-xyz", []byte("{}"))
		if !errors.Is(err, sinkErr) {
			t.Fatalf("expected sink error %v within grace window, got: %v", sinkErr, err)
		}
	})

	t.Run("LoadWithTimeout returns sink error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		_, err := artifacts.LoadWithTimeout(ctx, sink, "gs://bucket/runs/abcdef12/manifest.json")
		if !errors.Is(err, sinkErr) {
			t.Fatalf("expected sink error %v within grace window, got: %v", sinkErr, err)
		}
	})
}

func TestArtifactTarget(t *testing.T) {
	memSink := artifacts.NewInMemorySink()
	gotMem := artifacts.ArtifactTarget(memSink, "c0ffee1", "task-1", "summary.json")
	wantMem := "mem://runs/c0ffee1/task-1/summary.json"
	if gotMem != wantMem {
		t.Errorf("ArtifactTarget(memSink) = %q, want %q", gotMem, wantMem)
	}

	localSink := &artifacts.LocalDirSink{BaseDir: "/tmp/eval-artifacts"}
	gotLocal := artifacts.ArtifactTarget(localSink, "c0ffee1", "docs/readme_comprehension", "summary.json")
	wantLocal := "/tmp/eval-artifacts/runs/c0ffee1/docs%2Freadme_comprehension/summary.json"
	if gotLocal != wantLocal {
		t.Errorf("ArtifactTarget(localSink) = %q, want %q", gotLocal, wantLocal)
	}

	reloadable := artifacts.NewReloadableSink(localSink)
	gotReloadable := artifacts.ArtifactTarget(reloadable, "c0ffee1", "docs/readme_comprehension", "summary.json")
	if gotReloadable != wantLocal {
		t.Errorf("ArtifactTarget(reloadable) = %q, want %q", gotReloadable, wantLocal)
	}

	gotNil := artifacts.ArtifactTarget(nil, "c0ffee1", "task-1", "summary.json")
	wantNil := "c0ffee1/task-1/summary.json"
	if gotNil != wantNil {
		t.Errorf("ArtifactTarget(nil) = %q, want %q", gotNil, wantNil)
	}
}
