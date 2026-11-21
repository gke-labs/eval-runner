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
	"errors"
	"strings"
	"testing"

	"github.com/gke-labs/eval-runner/pkg/artifacts"
)

func TestUnavailableSink(t *testing.T) {
	ctx := t.Context()
	probeErr := errors.New("dial tcp 169.254.169.254:80: connect: connection refused")
	sink := artifacts.NewUnavailableSink("my-bucket", probeErr)

	if !sink.IsDurable() {
		t.Errorf("expected IsDurable to be true")
	}

	if prefix := sink.ArtifactPrefix("commit-123"); prefix != "gs://my-bucket/runs/commit-123/" {
		t.Errorf("unexpected ArtifactPrefix: %q", prefix)
	}

	if err := sink.CheckAccess(ctx); !errors.Is(err, probeErr) {
		t.Errorf("expected CheckAccess to return probeErr, got %v", err)
	}

	meta := artifacts.RunMetadata{
		RunName: "test-run",
		Commit:  "commit-123",
		TaskID:  "task-1",
	}

	t.Run("StoreManifest", func(t *testing.T) {
		_, err := sink.StoreManifest(ctx, meta, []byte("{}"))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "artifactBucket is set but artifact storage is unavailable") {
			t.Errorf("unexpected error message: %v", err)
		}
		if !strings.Contains(err.Error(), probeErr.Error()) {
			t.Errorf("expected underlying error to be preserved, got %v", err)
		}
	})

	t.Run("StoreArtifact", func(t *testing.T) {
		_, err := sink.StoreArtifact(ctx, meta, "eval.json", []byte("{}"))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "artifactBucket is set but artifact storage is unavailable") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("StoreComparison", func(t *testing.T) {
		_, err := sink.StoreComparison(ctx, "comp-1", []byte("{}"))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "artifactBucket is set but artifact storage is unavailable") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("Load", func(t *testing.T) {
		_, err := sink.Load(ctx, "gs://my-bucket/runs/commit-123/manifest.json")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "artifactBucket is set but artifact storage is unavailable") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("Close", func(t *testing.T) {
		if err := sink.Close(); err != nil {
			t.Errorf("expected nil error on Close, got %v", err)
		}
	})
}

func TestIsUnavailable(t *testing.T) {
	probeErr := errors.New("metadata server unreachable")
	unavail := artifacts.NewUnavailableSink("bucket", probeErr)

	err, ok := artifacts.IsUnavailable(unavail)
	if !ok || !errors.Is(err, probeErr) {
		t.Errorf("expected IsUnavailable to detect unavail sink, got ok=%v, err=%v", ok, err)
	}

	reloadable := artifacts.NewReloadableSink(unavail)
	err, ok = artifacts.IsUnavailable(reloadable)
	if !ok || !errors.Is(err, probeErr) {
		t.Errorf("expected IsUnavailable to detect wrapped unavail sink, got ok=%v, err=%v", ok, err)
	}

	mem := artifacts.NewInMemorySink()
	if _, ok := artifacts.IsUnavailable(mem); ok {
		t.Errorf("expected in-memory sink to not be unavailable")
	}

	reloadableMem := artifacts.NewReloadableSink(mem)
	if _, ok := artifacts.IsUnavailable(reloadableMem); ok {
		t.Errorf("expected reloadable in-memory sink to not be unavailable")
	}

	if _, ok := artifacts.IsUnavailable(nil); ok {
		t.Errorf("expected nil sink to not be unavailable")
	}
}
