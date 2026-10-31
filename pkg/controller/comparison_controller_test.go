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

package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/internal/fakes"
	"github.com/gke-labs/eval-runner/pkg/affinity"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/artifacts/gcs"
	"github.com/gke-labs/eval-runner/pkg/configkeys"
	"github.com/gke-labs/eval-runner/pkg/forge"
	"github.com/gke-labs/eval-runner/pkg/github"
	"github.com/gke-labs/eval-runner/pkg/stats"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func makeSyntheticInspectLog(taskID string, sampleScores []stats.SampleScore) []byte {
	type sample struct {
		ID     any            `json:"id"`
		Scores map[string]any `json:"scores"`
	}
	type logStruct struct {
		Version int `json:"version"`
		Eval    struct {
			Task    string `json:"task"`
			Scorers []struct {
				Name string `json:"name"`
			} `json:"scorers"`
		} `json:"eval"`
		Samples []sample `json:"samples"`
	}

	var ls logStruct
	ls.Version = 2
	ls.Eval.Task = taskID
	ls.Eval.Scorers = []struct {
		Name string `json:"name"`
	}{{Name: "choice"}}

	for _, s := range sampleScores {
		ls.Samples = append(ls.Samples, sample{
			ID: s.ID,
			Scores: map[string]any{
				"choice": map[string]any{"value": s.Score},
			},
		})
	}

	b, _ := json.Marshal(ls)
	return b
}

func makeSyntheticInspectLogWithDataset(taskID string, sampleScores []stats.SampleScore, datasetSamples int) []byte {
	type sample struct {
		ID     any            `json:"id"`
		Scores map[string]any `json:"scores"`
	}
	type logStruct struct {
		Version int `json:"version"`
		Eval    struct {
			Task    string `json:"task"`
			Scorers []struct {
				Name string `json:"name"`
			} `json:"scorers"`
			Dataset any `json:"dataset,omitempty"`
		} `json:"eval"`
		Samples []sample `json:"samples"`
	}

	var ls logStruct
	ls.Version = 2
	ls.Eval.Task = taskID
	ls.Eval.Scorers = []struct {
		Name string `json:"name"`
	}{{Name: "choice"}}
	if datasetSamples > 0 {
		ls.Eval.Dataset = map[string]any{
			"samples": datasetSamples,
		}
	}

	for _, s := range sampleScores {
		ls.Samples = append(ls.Samples, sample{
			ID: s.ID,
			Scores: map[string]any{
				"choice": map[string]any{"value": s.Score},
			},
		})
	}

	b, _ := json.Marshal(ls)
	return b
}

func TestComparisonReconciler_SyntheticPairedData(t *testing.T) {
	scheme := setupScheme(t)

	taskID := "docs/readme_comprehension"

	tests := []struct {
		name           string
		candScores     []stats.SampleScore
		baseScores     []stats.SampleScore
		candPhase      v1alpha1.RunPhase
		basePhase      v1alpha1.RunPhase
		expectedLabel  v1alpha1.ComparisonTaskLabel
		expectedVerd   v1alpha1.ComparisonVerdict
		expectedPhase  v1alpha1.ComparisonPhase
		expectedPairs  int
		expectedWins   int
		expectedLosses int
		expectedTies   int
		expectedDropC  int
		expectedDropB  int
	}{
		{
			name: "improved - candidate strictly higher than baseline",
			candScores: func() []stats.SampleScore {
				var s []stats.SampleScore
				for i := 1; i <= 20; i++ {
					s = append(s, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
				}
				return s
			}(),
			baseScores: func() []stats.SampleScore {
				var s []stats.SampleScore
				for i := 1; i <= 20; i++ {
					s = append(s, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
				}
				return s
			}(),
			candPhase:      v1alpha1.RunPhaseCompleted,
			basePhase:      v1alpha1.RunPhaseCompleted,
			expectedLabel:  v1alpha1.ComparisonTaskLabelImproved,
			expectedVerd:   v1alpha1.ComparisonVerdictImproved,
			expectedPhase:  v1alpha1.ComparisonPhaseCompleted,
			expectedPairs:  20,
			expectedWins:   20,
			expectedLosses: 0,
			expectedTies:   0,
		},
		{
			name: "regressed - candidate strictly lower than baseline",
			candScores: func() []stats.SampleScore {
				var s []stats.SampleScore
				for i := 1; i <= 20; i++ {
					s = append(s, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
				}
				return s
			}(),
			baseScores: func() []stats.SampleScore {
				var s []stats.SampleScore
				for i := 1; i <= 20; i++ {
					s = append(s, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
				}
				return s
			}(),
			candPhase:      v1alpha1.RunPhaseCompleted,
			basePhase:      v1alpha1.RunPhaseCompleted,
			expectedLabel:  v1alpha1.ComparisonTaskLabelRegressed,
			expectedVerd:   v1alpha1.ComparisonVerdictRegressed,
			expectedPhase:  v1alpha1.ComparisonPhaseCompleted,
			expectedPairs:  20,
			expectedWins:   0,
			expectedLosses: 20,
			expectedTies:   0,
		},
		{
			name: "unchanged - identical scores at n >= 8",
			candScores: func() []stats.SampleScore {
				var s []stats.SampleScore
				for i := 1; i <= 20; i++ {
					s = append(s, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
				}
				return s
			}(),
			baseScores: func() []stats.SampleScore {
				var s []stats.SampleScore
				for i := 1; i <= 20; i++ {
					s = append(s, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
				}
				return s
			}(),
			candPhase:      v1alpha1.RunPhaseCompleted,
			basePhase:      v1alpha1.RunPhaseCompleted,
			expectedLabel:  v1alpha1.ComparisonTaskLabelUnchanged,
			expectedVerd:   v1alpha1.ComparisonVerdictUnchanged,
			expectedPhase:  v1alpha1.ComparisonPhaseCompleted,
			expectedPairs:  20,
			expectedWins:   0,
			expectedLosses: 0,
			expectedTies:   20,
		},
		{
			name: "inconclusive at small n - 4 samples below MinPairsToDecide",
			candScores: []stats.SampleScore{
				{ID: "1", Score: 1.0},
				{ID: "2", Score: 0.0},
				{ID: "3", Score: 1.0},
				{ID: "4", Score: 0.0},
			},
			baseScores: []stats.SampleScore{
				{ID: "1", Score: 0.0},
				{ID: "2", Score: 1.0},
				{ID: "3", Score: 1.0},
				{ID: "4", Score: 0.0},
			},
			candPhase:      v1alpha1.RunPhaseCompleted,
			basePhase:      v1alpha1.RunPhaseCompleted,
			expectedLabel:  v1alpha1.ComparisonTaskLabelInconclusive,
			expectedVerd:   v1alpha1.ComparisonVerdictInconclusive,
			expectedPhase:  v1alpha1.ComparisonPhaseCompleted,
			expectedPairs:  4,
			expectedWins:   1,
			expectedLosses: 1,
			expectedTies:   2,
		},
		{
			name: "dropped unpaired ids",
			candScores: []stats.SampleScore{
				{ID: "1", Score: 1.0},
				{ID: "2", Score: 1.0},
				{ID: "3", Score: 1.0},
				{ID: "4", Score: 1.0},
				{ID: "5", Score: 1.0},
			},
			baseScores: []stats.SampleScore{
				{ID: "2", Score: 1.0},
				{ID: "3", Score: 1.0},
				{ID: "4", Score: 1.0},
				{ID: "5", Score: 1.0},
				{ID: "6", Score: 1.0},
			},
			candPhase:      v1alpha1.RunPhaseCompleted,
			basePhase:      v1alpha1.RunPhaseCompleted,
			expectedLabel:  v1alpha1.ComparisonTaskLabelInconclusive,
			expectedVerd:   v1alpha1.ComparisonVerdictInconclusive,
			expectedPhase:  v1alpha1.ComparisonPhaseCompleted,
			expectedPairs:  4,
			expectedWins:   0,
			expectedLosses: 0,
			expectedTies:   4,
			expectedDropC:  1,
			expectedDropB:  1,
		},
		{
			name: "disjoint sample ids - zero pairs -> inconclusive",
			candScores: []stats.SampleScore{
				{ID: "cand-1", Score: 1.0},
				{ID: "cand-2", Score: 1.0},
			},
			baseScores: []stats.SampleScore{
				{ID: "base-1", Score: 1.0},
				{ID: "base-2", Score: 1.0},
			},
			candPhase:     v1alpha1.RunPhaseCompleted,
			basePhase:     v1alpha1.RunPhaseCompleted,
			expectedLabel: v1alpha1.ComparisonTaskLabelInconclusive,
			expectedVerd:  v1alpha1.ComparisonVerdictInconclusive,
			expectedPhase: v1alpha1.ComparisonPhaseCompleted,
			expectedPairs: 0,
			expectedDropC: 2,
			expectedDropB: 2,
		},
		{
			name: "one Run still running -> RUNNING",
			candScores: func() []stats.SampleScore {
				var s []stats.SampleScore
				for i := 1; i <= 10; i++ {
					s = append(s, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
				}
				return s
			}(),
			baseScores: func() []stats.SampleScore {
				var s []stats.SampleScore
				for i := 1; i <= 10; i++ {
					s = append(s, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
				}
				return s
			}(),
			candPhase:     v1alpha1.RunPhaseRunning,
			basePhase:     v1alpha1.RunPhaseCompleted,
			expectedLabel: v1alpha1.ComparisonTaskLabelRunning,
			expectedVerd:  v1alpha1.ComparisonVerdictRunning,
			expectedPhase: v1alpha1.ComparisonPhaseRunning,
			expectedPairs: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeGCS := fakes.NewFakeStorageClient()
			sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
			if err != nil {
				t.Fatalf("Failed to create GCS sink: %v", err)
			}

			candLogPath := "gs://test-bucket/runs/cand-commit/docs/readme_comprehension/log.json"
			baseLogPath := "gs://test-bucket/runs/base-commit/docs/readme_comprehension/log.json"

			if len(tt.candScores) > 0 {
				fakeGCS.Objects["test-bucket/runs/cand-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
					Data: makeSyntheticInspectLog(taskID, tt.candScores),
				}
			}
			if len(tt.baseScores) > 0 {
				fakeGCS.Objects["test-bucket/runs/base-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
					Data: makeSyntheticInspectLog(taskID, tt.baseScores),
				}
			}

			candRun := &v1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cand-run",
					Namespace: "eval-runner",
				},
				Spec: v1alpha1.RunSpec{
					Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
					Model: "mockllm/model",
				},
				Status: v1alpha1.RunStatus{
					Phase: tt.candPhase,
					Tasks: []v1alpha1.TaskStatus{
						{
							ID:      taskID,
							Phase:   v1alpha1.TaskPhaseCompleted,
							LogPath: candLogPath,
						},
					},
				},
			}

			baseRun := &v1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "base-run",
					Namespace: "eval-runner",
				},
				Spec: v1alpha1.RunSpec{
					Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
					Model: "mockllm/model",
				},
				Status: v1alpha1.RunStatus{
					Phase: tt.basePhase,
					Tasks: []v1alpha1.TaskStatus{
						{
							ID:      taskID,
							Phase:   v1alpha1.TaskPhaseCompleted,
							LogPath: baseLogPath,
						},
					},
				},
			}

			comp := &v1alpha1.Comparison{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-comp",
					Namespace: "eval-runner",
				},
				Spec: v1alpha1.ComparisonSpec{
					Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
					Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(candRun, baseRun, comp).
				WithStatusSubresource(candRun, baseRun, comp).
				Build()

			reconciler := &ComparisonReconciler{
				Client:       cl,
				Scheme:       scheme,
				ArtifactSink: sink,
			}

			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-comp"}}
			_, err = reconciler.Reconcile(t.Context(), req)
			if err != nil {
				t.Fatalf("Reconcile error: %v", err)
			}

			var updatedComp v1alpha1.Comparison
			if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
				t.Fatalf("Failed to get updated Comparison: %v", err)
			}

			if updatedComp.Status.Phase != tt.expectedPhase {
				t.Errorf("Phase: got %s, want %s", updatedComp.Status.Phase, tt.expectedPhase)
			}
			if updatedComp.Status.Verdict != tt.expectedVerd {
				t.Errorf("Verdict: got %s, want %s", updatedComp.Status.Verdict, tt.expectedVerd)
			}
			if updatedComp.Status.Summary == nil || updatedComp.Status.Summary.Tasks == "" {
				t.Errorf("Expected non-empty Status.Summary.Tasks")
			}

			if len(updatedComp.Status.Tasks) != 1 {
				t.Fatalf("Expected 1 task status, got %d", len(updatedComp.Status.Tasks))
			}
			taskRes := updatedComp.Status.Tasks[0]
			if taskRes.Label != tt.expectedLabel {
				t.Errorf("Task label: got %s, want %s", taskRes.Label, tt.expectedLabel)
			}

			if tt.expectedPairs > 0 && taskRes.Pairs != tt.expectedPairs {
				t.Errorf("Pairs: got %d, want %d", taskRes.Pairs, tt.expectedPairs)
			}
			if taskRes.Wins != tt.expectedWins {
				t.Errorf("Wins: got %d, want %d", taskRes.Wins, tt.expectedWins)
			}
			if taskRes.Losses != tt.expectedLosses {
				t.Errorf("Losses: got %d, want %d", taskRes.Losses, tt.expectedLosses)
			}
			if taskRes.Ties != tt.expectedTies {
				t.Errorf("Ties: got %d, want %d", taskRes.Ties, tt.expectedTies)
			}
			if taskRes.Dropped.Candidate != tt.expectedDropC {
				t.Errorf("DroppedCandidate: got %d, want %d", taskRes.Dropped.Candidate, tt.expectedDropC)
			}
			if taskRes.Dropped.Baseline != tt.expectedDropB {
				t.Errorf("DroppedBaseline: got %d, want %d", taskRes.Dropped.Baseline, tt.expectedDropB)
			}

			// Verify artifact storage if completed
			if tt.expectedPhase == v1alpha1.ComparisonPhaseCompleted {
				expectedPath := "gs://test-bucket/comparisons/test-comp.json"
				if updatedComp.Status.SummaryPath != expectedPath {
					t.Errorf("SummaryPath: got %q, want %q", updatedComp.Status.SummaryPath, expectedPath)
				}
				artObj, exists := fakeGCS.GetObject("test-bucket", "comparisons/test-comp.json")
				if !exists {
					t.Fatalf("Expected comparison artifact to be stored in GCS")
				}
				var artifact ComparisonArtifact
				if err := json.Unmarshal(artObj.Data, &artifact); err != nil {
					t.Fatalf("Failed to parse stored comparison artifact: %v", err)
				}
				if artifact.Verdict != string(tt.expectedVerd) {
					t.Errorf("Artifact verdict: got %s, want %s", artifact.Verdict, tt.expectedVerd)
				}
				if len(artifact.Tasks) != 1 {
					t.Fatalf("Artifact tasks: got %d, want 1", len(artifact.Tasks))
				}
				if artifact.Tasks[0].Wins != tt.expectedWins {
					t.Errorf("Artifact task Wins: got %d, want %d", artifact.Tasks[0].Wins, tt.expectedWins)
				}
				if artifact.Tasks[0].Losses != tt.expectedLosses {
					t.Errorf("Artifact task Losses: got %d, want %d", artifact.Tasks[0].Losses, tt.expectedLosses)
				}
				if artifact.Tasks[0].Ties != tt.expectedTies {
					t.Errorf("Artifact task Ties: got %d, want %d", artifact.Tasks[0].Ties, tt.expectedTies)
				}
				if tt.expectedPairs > 0 {
					if taskRes.PImprovement == nil {
						t.Errorf("Task PImprovement: got nil, want non-nil")
					} else if artifact.Tasks[0].PImprovement != *taskRes.PImprovement {
						t.Errorf("Artifact task PImprovement: got %v, want %v", artifact.Tasks[0].PImprovement, *taskRes.PImprovement)
					}
				} else {
					if taskRes.PImprovement != nil {
						t.Errorf("Task PImprovement: got %v, want nil for 0 pairs", *taskRes.PImprovement)
					}
				}
				if tt.expectedPairs > 0 && len(artifact.Tasks[0].PairsTable) != tt.expectedPairs {
					t.Errorf("Artifact pairsTable length: got %d, want %d", len(artifact.Tasks[0].PairsTable), tt.expectedPairs)
				}
			}
		})
	}
}

func TestComparisonReconciler_TaskPImprovementSetAndRoundTrips(t *testing.T) {
	scheme := setupScheme(t)

	taskID := "docs/readme_comprehension"
	var candScores []stats.SampleScore
	var baseScores []stats.SampleScore
	for i := 1; i <= 20; i++ {
		candScores = append(candScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		baseScores = append(baseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
	}

	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	candLogPath := "gs://test-bucket/runs/cand-commit-pimp/docs/readme_comprehension/log.json"
	baseLogPath := "gs://test-bucket/runs/base-commit-pimp/docs/readme_comprehension/log.json"
	fakeGCS.Objects["test-bucket/runs/cand-commit-pimp/docs/readme_comprehension/log.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskID, candScores),
	}
	fakeGCS.Objects["test-bucket/runs/base-commit-pimp/docs/readme_comprehension/log.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskID, baseScores),
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-pimp", Namespace: "eval-runner"},
		Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{Commit: "cand-commit-pimp"}, Model: "mockllm/model"},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: candLogPath},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-pimp", Namespace: "eval-runner"},
		Spec:       v1alpha1.RunSpec{Repo: v1alpha1.RepoSpec{Commit: "base-commit-pimp"}, Model: "mockllm/model"},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: baseLogPath},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "test-comp-pimp-roundtrip", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-pimp"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-pimp"},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       client,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Name: comp.Name, Namespace: comp.Namespace},
	}
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile 1 error: %v", err)
	}

	var updatedComp v1alpha1.Comparison
	if err := client.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
		t.Fatalf("Failed to get updated comparison: %v", err)
	}

	if updatedComp.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Fatalf("Expected phase Completed, got %s", updatedComp.Status.Phase)
	}
	if len(updatedComp.Status.Tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(updatedComp.Status.Tasks))
	}

	task := updatedComp.Status.Tasks[0]
	if task.PImprovement == nil {
		t.Fatalf("Expected tasks[0].pImprovement to be non-nil")
	}
	if *task.PImprovement < 0.99 {
		t.Errorf("Expected tasks[0].pImprovement >= 0.99 for strict improvement, got %v", *task.PImprovement)
	}

	// 1. Verify JSON serialization round-trip
	data, err := json.Marshal(updatedComp)
	if err != nil {
		t.Fatalf("Failed to marshal Comparison with tasks[].pImprovement: %v", err)
	}
	var roundtripped v1alpha1.Comparison
	if err := json.Unmarshal(data, &roundtripped); err != nil {
		t.Fatalf("Failed to unmarshal Comparison with tasks[].pImprovement: %v", err)
	}
	if roundtripped.Status.Tasks[0].PImprovement == nil {
		t.Fatalf("Expected tasks[0].pImprovement to survive JSON round-trip")
	}
	if *roundtripped.Status.Tasks[0].PImprovement != *task.PImprovement {
		t.Errorf("PImprovement mismatch after JSON round-trip: got %v, want %v",
			*roundtripped.Status.Tasks[0].PImprovement, *task.PImprovement)
	}

	// 2. Verify stored comparison artifact matches
	artObj, exists := fakeGCS.GetObject("test-bucket", "comparisons/test-comp-pimp-roundtrip.json")
	if !exists {
		t.Fatalf("Expected comparison artifact to be stored in GCS")
	}
	var artifact ComparisonArtifact
	if err := json.Unmarshal(artObj.Data, &artifact); err != nil {
		t.Fatalf("Failed to parse stored comparison artifact: %v", err)
	}
	if len(artifact.Tasks) != 1 {
		t.Fatalf("Expected 1 task in artifact, got %d", len(artifact.Tasks))
	}
	if artifact.Tasks[0].PImprovement != *task.PImprovement {
		t.Errorf("Artifact task PImprovement %v does not match task status %v",
			artifact.Tasks[0].PImprovement, *task.PImprovement)
	}
}

func TestComparisonReconciler_PairsTableExecutionOrder(t *testing.T) {
	scheme := setupScheme(t)

	taskID := "docs/readme_comprehension"
	// Execution order: sample-10, sample-2, sample-1, sample-20
	// If sorted lexicographically: sample-1, sample-10, sample-2, sample-20
	executionOrder := []string{"sample-10", "sample-2", "sample-1", "sample-20"}

	var candScores []stats.SampleScore
	var baseScores []stats.SampleScore
	for _, id := range executionOrder {
		candScores = append(candScores, stats.SampleScore{ID: id, Score: 1.0})
		baseScores = append(baseScores, stats.SampleScore{ID: id, Score: 0.5})
	}
	// Add an unpaired baseline and candidate sample to ensure skipping works
	candScores = append(candScores, stats.SampleScore{ID: "cand-only", Score: 1.0})
	baseScores = append([]stats.SampleScore{{ID: "base-only", Score: 0.5}}, baseScores...)

	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	candLogPath := "gs://test-bucket/runs/cand-commit/docs/readme_comprehension/log.json"
	baseLogPath := "gs://test-bucket/runs/base-commit/docs/readme_comprehension/log.json"

	fakeGCS.Objects["test-bucket/runs/cand-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskID, candScores),
	}
	fakeGCS.Objects["test-bucket/runs/base-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskID, baseScores),
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cand-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: candLogPath,
				},
			},
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-run",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: baseLogPath,
				},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-comp-order",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      comp.Name,
			Namespace: comp.Namespace,
		},
	}

	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	artObj, exists := fakeGCS.Objects["test-bucket/comparisons/test-comp-order.json"]
	if !exists {
		t.Fatalf("Expected comparison artifact in GCS")
	}

	var artifact ComparisonArtifact
	if err := json.Unmarshal(artObj.Data, &artifact); err != nil {
		t.Fatalf("Failed to parse stored comparison artifact: %v", err)
	}

	if len(artifact.Tasks) != 1 {
		t.Fatalf("Expected 1 task in artifact, got %d", len(artifact.Tasks))
	}

	pairs := artifact.Tasks[0].PairsTable
	if len(pairs) != len(executionOrder) {
		t.Fatalf("Expected %d pairs, got %d", len(executionOrder), len(pairs))
	}

	for i, wantID := range executionOrder {
		if pairs[i].ID != wantID {
			t.Errorf("pairsTable[%d].ID: got %q, want execution order %q", i, pairs[i].ID, wantID)
		}
	}
}

func TestComparisonReconciler_MissingRuns(t *testing.T) {
	scheme := setupScheme(t)

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-comp-missing",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "missing-base"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "missing-cand"},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(comp).
		WithStatusSubresource(comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-comp-missing"}}
	_, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get error: %v", err)
	}

	if updated.Status.Verdict != v1alpha1.ComparisonVerdictRunning {
		t.Errorf("Verdict: got %s, want %s", updated.Status.Verdict, v1alpha1.ComparisonVerdictRunning)
	}
	if updated.Status.Phase != v1alpha1.ComparisonPhaseRunning {
		t.Errorf("Phase: got %s, want %s", updated.Status.Phase, v1alpha1.ComparisonPhaseRunning)
	}
}

func TestComparisonReconciler_FindComparisonsForRun(t *testing.T) {
	scheme := setupScheme(t)

	comp1 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-1", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "run-a"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "run-b"},
		},
	}
	comp2 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-2", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "run-c"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "run-a"},
		},
	}
	compUnrelated := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-unrelated", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "run-x"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "run-y"},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(comp1, comp2, compUnrelated).
		Build()

	reconciler := &ComparisonReconciler{
		Client: cl,
		Scheme: scheme,
	}

	reqs := reconciler.findComparisonsForRun(t.Context(), &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run-a", Namespace: "eval-runner"},
	})

	if len(reqs) != 2 {
		t.Fatalf("Expected 2 requests for run-a, got %d", len(reqs))
	}

	names := map[string]bool{reqs[0].Name: true, reqs[1].Name: true}
	if !names["comp-1"] || !names["comp-2"] {
		t.Errorf("Expected comp-1 and comp-2, got %v", names)
	}
}

func TestComparisonReconciler_CommitPrefixMismatch(t *testing.T) {
	scheme := setupScheme(t)

	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	taskID := "docs/readme_comprehension"
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:    taskID,
					Phase: v1alpha1.TaskPhaseCompleted,
					// Wrong commit prefix!
					LogPath: "gs://test-bucket/runs/other-commit/docs/readme_comprehension/log.json",
				},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://test-bucket/runs/base-commit/docs/readme_comprehension/log.json",
				},
			},
		},
	}
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "test-comp-prefix", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-comp-prefix"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Unexpected reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected RequeueAfter 0 when TTL=0, got %v", res.RequeueAfter)
	}

	var updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("Expected phase Completed, got %s", updated.Status.Phase)
	}
	if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
		t.Errorf("Expected verdict INCONCLUSIVE, got %s", updated.Status.Verdict)
	}
	if !strings.Contains(updated.Status.Message, "does not match commit prefix") {
		t.Errorf("Expected status message to record prefix mismatch, got %q", updated.Status.Message)
	}
	cond := meta.FindStatusCondition(updated.Status.Conditions, v1alpha1.ConditionTypeArtifactsReadable)
	if cond == nil {
		t.Fatalf("Expected ArtifactsReadable condition to be set")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Expected condition status False, got %s", cond.Status)
	}
	if cond.Reason != v1alpha1.ConditionReasonPrefixMismatch {
		t.Errorf("Expected condition reason PrefixMismatch, got %s", cond.Reason)
	}
	if !strings.Contains(cond.Message, "does not match commit prefix") {
		t.Errorf("Expected condition message to contain prefix mismatch, got %q", cond.Message)
	}
}

func TestComparisonReconciler_UnreadableArtifacts_PrefixMismatch_CompletesInconclusive(t *testing.T) {
	scheme := setupScheme(t)

	server := github.NewFakeGitHubServer()
	defer server.Close()

	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	ttl := 1 * time.Hour
	taskID := "docs/readme_comprehension"

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:    taskID,
					Phase: v1alpha1.TaskPhaseCompleted,
					// Wrong commit prefix (e.g. from local /tmp/artifacts or another commit/sink)
					LogPath: "gs://test-bucket/runs/other-commit/docs/readme_comprehension/log.json",
				},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://test-bucket/runs/base-commit/docs/readme_comprehension/log.json",
				},
			},
		},
	}
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-prefix-mismatch", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         4,
				MaxSamplesPerTask: 16,
			},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     123,
			},
		},
	}
	tokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
		Data:       map[string][]byte{"token": []byte("test-gh-token")},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp, tokenSecret).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:              cl,
		Scheme:              scheme,
		ArtifactSink:        sink,
		ComparisonTTL:       ttl,
		Now:                 func() time.Time { return now },
		ControllerNamespace: "eval-runner",
		GitHubTokenSecret:   "github-token",
		GitHubClientFactory: func(creds GitHubCredentials) github.Client {
			return server.Client(creds.Token)
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-prefix-mismatch"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Unexpected reconcile error: %v", err)
	}
	if res.RequeueAfter != ttl {
		t.Errorf("Expected RequeueAfter %v (TTL requeue), got %v", ttl, res.RequeueAfter)
	}

	var updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("Expected phase Completed, got %s", updated.Status.Phase)
	}
	if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
		t.Errorf("Expected verdict INCONCLUSIVE, got %s", updated.Status.Verdict)
	}
	if !strings.Contains(updated.Status.Message, "does not match commit prefix") {
		t.Errorf("Expected status message to record prefix mismatch, got %q", updated.Status.Message)
	}
	cond := meta.FindStatusCondition(updated.Status.Conditions, v1alpha1.ConditionTypeArtifactsReadable)
	if cond == nil {
		t.Fatalf("Expected ArtifactsReadable condition to be set")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Expected condition status False, got %s", cond.Status)
	}
	if cond.Reason != v1alpha1.ConditionReasonPrefixMismatch {
		t.Errorf("Expected condition reason PrefixMismatch, got %s", cond.Reason)
	}
	if cond.Message != updated.Status.Message {
		t.Errorf("Expected condition message %q, got %q", updated.Status.Message, cond.Message)
	}

	// Verify no orphan batch runs were spawned when seed is unreadable
	var runList v1alpha1.RunList
	if err := cl.List(t.Context(), &runList); err != nil {
		t.Fatalf("failed to list runs: %v", err)
	}
	if len(runList.Items) != 2 {
		t.Errorf("expected only seed runs [cand-run, base-run], found %d runs", len(runList.Items))
	}

	// GitHub comment check
	comments := server.Comments()
	if len(comments) != 1 {
		t.Fatalf("Expected 1 PR comment, got %d", len(comments))
	}
	if !strings.Contains(comments[0].Body, "## INCONCLUSIVE") {
		t.Errorf("Expected PR comment to contain '## INCONCLUSIVE', got:\n%s", comments[0].Body)
	}
	if !strings.Contains(comments[0].Body, "does not match commit prefix") {
		t.Errorf("Expected PR comment to contain prefix mismatch message, got:\n%s", comments[0].Body)
	}

	// GitHub check run check
	checkRuns := server.CheckRuns()
	if len(checkRuns) != 1 {
		t.Fatalf("Expected 1 check run, got %d", len(checkRuns))
	}
	if checkRuns[0].Status != "completed" {
		t.Errorf("Expected check run status 'completed', got %q", checkRuns[0].Status)
	}
	if checkRuns[0].Conclusion != "neutral" {
		t.Errorf("Expected check run conclusion 'neutral', got %q", checkRuns[0].Conclusion)
	}

	// Second reconcile: steady state makes zero GitHub calls and zero status writes
	reqCountBefore := len(server.RequestLog())
	rvBefore := updated.ResourceVersion
	res2, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("second reconcile error: %v", err)
	}
	if res2.RequeueAfter != ttl {
		t.Errorf("expected RequeueAfter %v, got %v", ttl, res2.RequeueAfter)
	}
	if len(server.RequestLog()) != reqCountBefore {
		t.Errorf("expected 0 new GitHub requests on second reconcile, got %d", len(server.RequestLog())-reqCountBefore)
	}
	var updated2 v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated2); err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if updated2.ResourceVersion != rvBefore {
		t.Errorf("expected 0 status writes on second reconcile, resourceVersion changed from %s to %s", rvBefore, updated2.ResourceVersion)
	}
}

func TestComparisonReconciler_UnreadableArtifacts_NotFound_CompletesInconclusive(t *testing.T) {
	scheme := setupScheme(t)

	server := github.NewFakeGitHubServer()
	defer server.Close()

	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	ttl := 1 * time.Hour
	taskID := "docs/readme_comprehension"

	// LogPath matches prefix, but the object is NOT in fakeGCS, returning ErrNotFound
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://test-bucket/runs/cand-commit/docs/readme_comprehension/log.json",
				},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://test-bucket/runs/base-commit/docs/readme_comprehension/log.json",
				},
			},
		},
	}
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-not-found", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     124,
			},
		},
	}
	tokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
		Data:       map[string][]byte{"token": []byte("test-gh-token")},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp, tokenSecret).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:              cl,
		Scheme:              scheme,
		ArtifactSink:        sink,
		ComparisonTTL:       ttl,
		Now:                 func() time.Time { return now },
		ControllerNamespace: "eval-runner",
		GitHubTokenSecret:   "github-token",
		GitHubClientFactory: func(creds GitHubCredentials) github.Client {
			return server.Client(creds.Token)
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-not-found"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Unexpected reconcile error: %v", err)
	}
	if res.RequeueAfter != ttl {
		t.Errorf("Expected RequeueAfter %v (TTL requeue), got %v", ttl, res.RequeueAfter)
	}

	var updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("Expected phase Completed, got %s", updated.Status.Phase)
	}
	if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
		t.Errorf("Expected verdict INCONCLUSIVE, got %s", updated.Status.Verdict)
	}
	if !strings.Contains(updated.Status.Message, "failed to load candidate log") || !strings.Contains(updated.Status.Message, "artifact not found") {
		t.Errorf("Expected status message to record not found error, got %q", updated.Status.Message)
	}
	cond := meta.FindStatusCondition(updated.Status.Conditions, v1alpha1.ConditionTypeArtifactsReadable)
	if cond == nil {
		t.Fatalf("Expected ArtifactsReadable condition to be set")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Expected condition status False, got %s", cond.Status)
	}
	if cond.Reason != v1alpha1.ConditionReasonNotFound {
		t.Errorf("Expected condition reason NotFound, got %s", cond.Reason)
	}
	if cond.Message != updated.Status.Message {
		t.Errorf("Expected condition message %q, got %q", updated.Status.Message, cond.Message)
	}

	// GitHub comment check
	comments := server.Comments()
	if len(comments) != 1 {
		t.Fatalf("Expected 1 PR comment, got %d", len(comments))
	}
	if !strings.Contains(comments[0].Body, "## INCONCLUSIVE") {
		t.Errorf("Expected PR comment to contain '## INCONCLUSIVE', got:\n%s", comments[0].Body)
	}
	if !strings.Contains(comments[0].Body, "artifact not found") {
		t.Errorf("Expected PR comment to contain not found message, got:\n%s", comments[0].Body)
	}

	// GitHub check run check
	checkRuns := server.CheckRuns()
	if len(checkRuns) != 1 {
		t.Fatalf("Expected 1 check run, got %d", len(checkRuns))
	}
	if checkRuns[0].Status != "completed" {
		t.Errorf("Expected check run status 'completed', got %q", checkRuns[0].Status)
	}
	if checkRuns[0].Conclusion != "neutral" {
		t.Errorf("Expected check run conclusion 'neutral', got %q", checkRuns[0].Conclusion)
	}
}

func TestComparisonReconciler_UnreadableArtifacts_TransientSinkError_Retries(t *testing.T) {
	scheme := setupScheme(t)

	server := github.NewFakeGitHubServer()
	defer server.Close()

	fakeGCS := fakes.NewFakeStorageClient()
	taskID := "docs/readme_comprehension"
	// Inject a transient 503 error
	fakeGCS.Errors["test-bucket/runs/cand-commit/docs/readme_comprehension/log.json"] = errors.New("503 Service Unavailable")

	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	ttl := 1 * time.Hour

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://test-bucket/runs/cand-commit/docs/readme_comprehension/log.json",
				},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://test-bucket/runs/base-commit/docs/readme_comprehension/log.json",
				},
			},
		},
	}
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-transient-err", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     125,
			},
		},
	}
	tokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
		Data:       map[string][]byte{"token": []byte("test-gh-token")},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp, tokenSecret).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:              cl,
		Scheme:              scheme,
		ArtifactSink:        sink,
		ComparisonTTL:       ttl,
		Now:                 func() time.Time { return now },
		ControllerNamespace: "eval-runner",
		GitHubTokenSecret:   "github-token",
		GitHubClientFactory: func(creds GitHubCredentials) github.Client {
			return server.Client(creds.Token)
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-transient-err"}}
	_, err = reconciler.Reconcile(t.Context(), req)
	if err == nil {
		t.Fatalf("Expected error on transient 503 sink error, got nil")
	}
	if !strings.Contains(err.Error(), "503 Service Unavailable") {
		t.Errorf("Expected error to mention 503 Service Unavailable, got %v", err)
	}

	var updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if updated.Status.Phase == v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("Expected phase to NOT be Completed on transient error, got %s", updated.Status.Phase)
	}
	cond := meta.FindStatusCondition(updated.Status.Conditions, v1alpha1.ConditionTypeArtifactsReadable)
	if cond != nil {
		t.Errorf("Expected ArtifactsReadable condition to NOT be set on transient error, got %+v", cond)
	}
	if !strings.Contains(updated.Status.Message, "503 Service Unavailable") {
		t.Errorf("Expected status message to record transient error, got %q", updated.Status.Message)
	}
}

func TestComparisonReconciler_UnreadableArtifacts_BaselineVariants(t *testing.T) {
	scheme := setupScheme(t)

	taskID := "docs/readme_comprehension"
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	ttl := 1 * time.Hour

	t.Run("BaselinePrefixMismatch", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-b", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      taskID,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/docs/readme_comprehension/log.json",
					},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-b", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      taskID,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/foreign-prefix/docs/readme_comprehension/log.json",
					},
				},
			},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-base-prefix-mismatch", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-b"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-b"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:        cl,
			Scheme:        scheme,
			ArtifactSink:  sink,
			ComparisonTTL: ttl,
			Now:           func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-base-prefix-mismatch"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Unexpected reconcile error: %v", err)
		}
		if res.RequeueAfter != ttl {
			t.Errorf("Expected RequeueAfter %v, got %v", ttl, res.RequeueAfter)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("Expected phase Completed, got %s", updated.Status.Phase)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("Expected verdict INCONCLUSIVE, got %s", updated.Status.Verdict)
		}
		if !strings.Contains(updated.Status.Message, "baseline log path") || !strings.Contains(updated.Status.Message, "does not match commit prefix") {
			t.Errorf("Expected baseline prefix mismatch message, got %q", updated.Status.Message)
		}
		cond := meta.FindStatusCondition(updated.Status.Conditions, v1alpha1.ConditionTypeArtifactsReadable)
		if cond == nil {
			t.Fatalf("Expected ArtifactsReadable condition to be set")
		}
		if cond.Status != metav1.ConditionFalse || cond.Reason != v1alpha1.ConditionReasonPrefixMismatch {
			t.Errorf("Expected False/PrefixMismatch, got %s/%s", cond.Status, cond.Reason)
		}
	})

	t.Run("BaselineNotFound", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		// Cand object exists, baseline object does not exist
		fakeGCS.Objects["test-bucket/runs/cand-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, []stats.SampleScore{{ID: "1", Score: 1.0}}),
		}

		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-nf", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      taskID,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/docs/readme_comprehension/log.json",
					},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-nf", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      taskID,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/docs/readme_comprehension/log.json",
					},
				},
			},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-base-not-found", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-nf"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-nf"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:        cl,
			Scheme:        scheme,
			ArtifactSink:  sink,
			ComparisonTTL: ttl,
			Now:           func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-base-not-found"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Unexpected reconcile error: %v", err)
		}
		if res.RequeueAfter != ttl {
			t.Errorf("Expected RequeueAfter %v, got %v", ttl, res.RequeueAfter)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("Expected phase Completed, got %s", updated.Status.Phase)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("Expected verdict INCONCLUSIVE, got %s", updated.Status.Verdict)
		}
		if !strings.Contains(updated.Status.Message, "failed to load baseline log") || !strings.Contains(updated.Status.Message, "artifact not found") {
			t.Errorf("Expected baseline not found message, got %q", updated.Status.Message)
		}
		cond := meta.FindStatusCondition(updated.Status.Conditions, v1alpha1.ConditionTypeArtifactsReadable)
		if cond == nil {
			t.Fatalf("Expected ArtifactsReadable condition to be set")
		}
		if cond.Status != metav1.ConditionFalse || cond.Reason != v1alpha1.ConditionReasonNotFound {
			t.Errorf("Expected False/NotFound, got %s/%s", cond.Status, cond.Reason)
		}
	})
}

func TestComparisonReconciler_UnreadableArtifacts_InconclusivePolicyFailure(t *testing.T) {
	scheme := setupScheme(t)

	server := github.NewFakeGitHubServer()
	defer server.Close()

	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	ttl := 1 * time.Hour
	taskID := "docs/readme_comprehension"

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-policy", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://test-bucket/runs/other-commit/docs/readme_comprehension/log.json",
				},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-policy", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://test-bucket/runs/base-commit/docs/readme_comprehension/log.json",
				},
			},
		},
	}
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-policy-failure", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-policy"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-policy"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     126,
			},
		},
	}
	tokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
		Data:       map[string][]byte{"token": []byte("test-gh-token")},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp, tokenSecret).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:              cl,
		Scheme:              scheme,
		ArtifactSink:        sink,
		ComparisonTTL:       ttl,
		InconclusivePolicy:  configkeys.InconclusivePolicyFailure,
		Now:                 func() time.Time { return now },
		ControllerNamespace: "eval-runner",
		GitHubTokenSecret:   "github-token",
		GitHubClientFactory: func(creds GitHubCredentials) github.Client {
			return server.Client(creds.Token)
		},
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-policy-failure"}}
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Unexpected reconcile error: %v", err)
	}

	checkRuns := server.CheckRuns()
	if len(checkRuns) != 1 {
		t.Fatalf("Expected 1 check run, got %d", len(checkRuns))
	}
	if checkRuns[0].Conclusion != "failure" {
		t.Errorf("Expected check run conclusion 'failure' with InconclusivePolicy=failure, got %q", checkRuns[0].Conclusion)
	}
}

func TestComparisonReconciler_TerminalComparison_NotFound_PreservesVerdict(t *testing.T) {
	scheme := setupScheme(t)

	server := github.NewFakeGitHubServer()
	defer server.Close()
	server.SetUserPermission("gke-labs", "eval-runner", "reviewer", "write")

	tokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
		Data:       map[string][]byte{"token": []byte("ghp_test")},
	}

	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	ttl := 1 * time.Hour

	// Both log paths match prefix, but candidate log does not exist in fakeGCS -> NotFound
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-term", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-sha"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      "task1",
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://test-bucket/runs/cand-sha/task1/inspect.json",
				},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-term", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-sha"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      "task1",
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://test-bucket/runs/base-sha/task1/inspect.json",
				},
			},
		},
	}

	compTime := metav1.NewTime(now.Add(-30 * time.Minute))
	verdictMsg := "Evaluated 1 task(s): 1 improved"
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "comp-term-notfound",
			Namespace:         "eval-runner",
			CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-term"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-term"},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     99,
				HeadSHA:    "cand-sha",
				BaseSHA:    "base-sha",
			},
			ExtraSamples: 0,
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:               v1alpha1.ComparisonPhaseCompleted,
			Verdict:             v1alpha1.ComparisonVerdictImproved,
			Message:             verdictMsg,
			CompletionTime:      &compTime,
			ExtraSamplesApplied: 0,
			Reporting: &v1alpha1.ComparisonReportingStatus{
				CommentID: 7777,
			},
			PendingCommands: []v1alpha1.ComparisonPendingCommand{
				{
					CommentID:  8888,
					Login:      "reviewer",
					Requested:  8,
					ReceivedAt: metav1.NewTime(now),
				},
			},
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:         "task1",
					Label:      v1alpha1.ComparisonTaskLabelImproved,
					SamplesRun: 8,
				},
			},
			Batches: []v1alpha1.ComparisonBatchStatus{
				{
					Index:        0,
					Phase:        v1alpha1.ComparisonPhaseCompleted,
					CandidateRun: "cand-run-term",
					BaselineRun:  "base-run-term",
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(baseRun, candRun, comp, tokenSecret).
		WithStatusSubresource(baseRun, candRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:              cl,
		Scheme:              scheme,
		ArtifactSink:        sink,
		ComparisonTTL:       ttl,
		ControllerNamespace: "eval-runner",
		GitHubTokenSecret:   "github-token",
		GitHubClientFactory: func(creds GitHubCredentials) github.Client {
			return server.Client(creds.Token)
		},
		Now: func() time.Time { return now },
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-term-notfound"}}

	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	expectedRequeue := ttl - 30*time.Minute
	if res.RequeueAfter != expectedRequeue {
		t.Errorf("expected RequeueAfter %v, got %v", expectedRequeue, res.RequeueAfter)
	}

	var updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get error: %v", err)
	}

	// VERDICT MUST BE PRESERVED!
	if updated.Status.Verdict != v1alpha1.ComparisonVerdictImproved {
		t.Errorf("expected verdict %s to be preserved, got %s", v1alpha1.ComparisonVerdictImproved, updated.Status.Verdict)
	}
	if updated.Status.Message != verdictMsg {
		t.Errorf("expected message %q to be preserved, got %q", verdictMsg, updated.Status.Message)
	}
	if updated.Status.ExtraSamplesApplied != 8 {
		t.Errorf("expected ExtraSamplesApplied 8, got %d", updated.Status.ExtraSamplesApplied)
	}
	if len(updated.Status.HandledCommands) != 1 {
		t.Fatalf("expected 1 handled command, got %d", len(updated.Status.HandledCommands))
	}
	if updated.Status.HandledCommands[0].Outcome != "unserviceable: artifact not found" {
		t.Errorf("expected outcome 'unserviceable: artifact not found', got %q", updated.Status.HandledCommands[0].Outcome)
	}
	cond := meta.FindStatusCondition(updated.Status.Conditions, v1alpha1.ConditionTypeExtraSamples)
	if cond == nil {
		t.Fatalf("expected ExtraSamples condition to be set")
	}
	if cond.Status != metav1.ConditionFalse || cond.Reason != "ArtifactNotFound" {
		t.Errorf("expected False/ArtifactNotFound, got %s/%s", cond.Status, cond.Reason)
	}

	// Verify note was posted to PR
	var noteComments []string
	for _, c := range server.Comments() {
		if strings.Contains(c.Body, "Extra samples cannot be run: artifact not found.") {
			noteComments = append(noteComments, c.Body)
		}
	}
	if len(noteComments) != 1 {
		t.Fatalf("expected 1 unserviceable note comment on PR, got %d", len(noteComments))
	}
}

func TestComparisonReconciler_BatchRun_NotFound_TreatsBatchAsFailed(t *testing.T) {
	scheme := setupScheme(t)

	fakeGCS := fakes.NewFakeStorageClient()
	taskID := "docs/readme_comprehension"
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

	// Seed logs exist in fakeGCS
	candSeedLog := "gs://test-bucket/runs/cand-commit/task/seed.json"
	baseSeedLog := "gs://test-bucket/runs/base-commit/task/seed.json"
	fakeGCS.Objects["test-bucket/runs/cand-commit/task/seed.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskID, []stats.SampleScore{{ID: "1", Score: 1.0}}),
	}
	fakeGCS.Objects["test-bucket/runs/base-commit/task/seed.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskID, []stats.SampleScore{{ID: "1", Score: 0.0}}),
	}

	// Batch 2 candidate log is MISSING from fakeGCS -> NotFound
	candB2Log := "gs://test-bucket/runs/cand-commit/task/b2.json"
	baseB2Log := "gs://test-bucket/runs/base-commit/task/b2.json"
	fakeGCS.Objects["test-bucket/runs/base-commit/task/b2.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskID, []stats.SampleScore{{ID: "2", Score: 0.0}}),
	}

	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	// Seed runs (batchIdx 0)
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-seed", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: candSeedLog,
				},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-seed", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: baseSeedLog,
				},
			},
		},
	}

	// Extension batch runs (batchIdx 1)
	candB2Run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-b2", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: candB2Log, // missing from fakeGCS
				},
			},
		},
	}
	baseB2Run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-b2", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: baseB2Log,
				},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-batch-notfound", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:     v1alpha1.ComparisonRunRef{Run: "base-run-seed"},
			Candidate:    v1alpha1.ComparisonRunRef{Run: "cand-run-seed"},
			ExtraSamples: 1,
		},
		Status: v1alpha1.ComparisonStatus{
			Batches: []v1alpha1.ComparisonBatchStatus{
				{
					Index:        1,
					CandidateRun: "cand-run-seed",
					BaselineRun:  "base-run-seed",
					Phase:        v1alpha1.ComparisonPhaseCompleted,
				},
				{
					Index:        2,
					CandidateRun: "cand-run-b2",
					BaselineRun:  "base-run-b2",
					Phase:        v1alpha1.ComparisonPhaseRunning,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, candB2Run, baseB2Run, comp).
		WithStatusSubresource(candRun, baseRun, candB2Run, baseB2Run, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
		Now:          func() time.Time { return now },
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-batch-notfound"}}
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get error: %v", err)
	}

	// Comparison completes based on seed batch, noting failed batch run
	if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("expected phase %s, got %s", v1alpha1.ComparisonPhaseCompleted, updated.Status.Phase)
	}
	if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
		t.Errorf("expected verdict %s, got %s", v1alpha1.ComparisonVerdictInconclusive, updated.Status.Verdict)
	}
	if len(updated.Status.Tasks) != 1 || updated.Status.Tasks[0].SamplesRun != 1 {
		t.Errorf("expected seed batch sample to be preserved (SamplesRun == 1), got %+v", updated.Status.Tasks)
	}
	if !strings.Contains(updated.Status.Message, "failed batch run") || !strings.Contains(updated.Status.Message, "cand-run-b2") {
		t.Errorf("expected status message to note failed batch run cand-run-b2, got %q", updated.Status.Message)
	}
	// ArtifactsReadable condition should NOT be False/NotFound (seed was readable!)
	cond := meta.FindStatusCondition(updated.Status.Conditions, v1alpha1.ConditionTypeArtifactsReadable)
	if cond != nil {
		t.Errorf("expected ArtifactsReadable condition to not be set, got %+v", cond)
	}
}

type sampleWithEpoch struct {
	ID     any            `json:"id"`
	Epoch  int            `json:"epoch,omitempty"`
	Scores map[string]any `json:"scores"`
}

func makeSyntheticInspectLogWithEpochs(taskID string, samples []sampleWithEpoch) []byte {
	type logStruct struct {
		Version int `json:"version"`
		Eval    struct {
			Task    string `json:"task"`
			Scorers []struct {
				Name string `json:"name"`
			} `json:"scorers"`
		} `json:"eval"`
		Samples []sampleWithEpoch `json:"samples"`
	}

	var ls logStruct
	ls.Version = 2
	ls.Eval.Task = taskID
	ls.Eval.Scorers = []struct {
		Name string `json:"name"`
	}{{Name: "choice"}}
	ls.Samples = samples

	b, _ := json.Marshal(ls)
	return b
}

func TestComparisonReconciler_EpochAwarePairing(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"

	tests := []struct {
		name          string
		candSamples   []sampleWithEpoch
		baseSamples   []sampleWithEpoch
		expectedPairs int
		expectedDropC int
		expectedDropB int
	}{
		{
			name: "2 epochs pair correctly across runs",
			candSamples: func() []sampleWithEpoch {
				var s []sampleWithEpoch
				for epoch := 1; epoch <= 2; epoch++ {
					for i := 1; i <= 10; i++ {
						s = append(s, sampleWithEpoch{
							ID:    i,
							Epoch: epoch,
							Scores: map[string]any{
								"choice": map[string]any{"value": 1.0},
							},
						})
					}
				}
				return s
			}(),
			baseSamples: func() []sampleWithEpoch {
				var s []sampleWithEpoch
				for epoch := 1; epoch <= 2; epoch++ {
					for i := 1; i <= 10; i++ {
						s = append(s, sampleWithEpoch{
							ID:    i,
							Epoch: epoch,
							Scores: map[string]any{
								"choice": map[string]any{"value": 0.0},
							},
						})
					}
				}
				return s
			}(),
			expectedPairs: 20,
			expectedDropC: 0,
			expectedDropB: 0,
		},
		{
			name: "candidate has 2 epochs but baseline has 1 epoch - epoch 2 dropped",
			candSamples: func() []sampleWithEpoch {
				var s []sampleWithEpoch
				for epoch := 1; epoch <= 2; epoch++ {
					for i := 1; i <= 10; i++ {
						s = append(s, sampleWithEpoch{
							ID:    i,
							Epoch: epoch,
							Scores: map[string]any{
								"choice": map[string]any{"value": 1.0},
							},
						})
					}
				}
				return s
			}(),
			baseSamples: func() []sampleWithEpoch {
				var s []sampleWithEpoch
				for i := 1; i <= 10; i++ {
					s = append(s, sampleWithEpoch{
						ID:    i,
						Epoch: 1,
						Scores: map[string]any{
							"choice": map[string]any{"value": 0.0},
						},
					})
				}
				return s
			}(),
			expectedPairs: 10,
			expectedDropC: 10,
			expectedDropB: 0,
		},
		{
			name: "baseline has 2 epochs but candidate has 1 epoch - epoch 2 dropped",
			candSamples: func() []sampleWithEpoch {
				var s []sampleWithEpoch
				for i := 1; i <= 10; i++ {
					s = append(s, sampleWithEpoch{
						ID:    i,
						Epoch: 1,
						Scores: map[string]any{
							"choice": map[string]any{"value": 1.0},
						},
					})
				}
				return s
			}(),
			baseSamples: func() []sampleWithEpoch {
				var s []sampleWithEpoch
				for epoch := 1; epoch <= 2; epoch++ {
					for i := 1; i <= 10; i++ {
						s = append(s, sampleWithEpoch{
							ID:    i,
							Epoch: epoch,
							Scores: map[string]any{
								"choice": map[string]any{"value": 0.0},
							},
						})
					}
				}
				return s
			}(),
			expectedPairs: 10,
			expectedDropC: 0,
			expectedDropB: 10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeGCS := fakes.NewFakeStorageClient()
			sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
			if err != nil {
				t.Fatalf("Failed to create GCS sink: %v", err)
			}
			candLogPath := "gs://test-bucket/runs/cand-commit/docs/readme_comprehension/log.json"
			baseLogPath := "gs://test-bucket/runs/base-commit/docs/readme_comprehension/log.json"
			fakeGCS.Objects["test-bucket/runs/cand-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
				Data: makeSyntheticInspectLogWithEpochs(taskID, tc.candSamples),
			}
			fakeGCS.Objects["test-bucket/runs/base-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
				Data: makeSyntheticInspectLogWithEpochs(taskID, tc.baseSamples),
			}

			candRun := &v1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
				Spec: v1alpha1.RunSpec{
					Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
					Model: "mockllm/model",
				},
				Status: v1alpha1.RunStatus{
					Phase: v1alpha1.RunPhaseCompleted,
					Tasks: []v1alpha1.TaskStatus{
						{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: candLogPath},
					},
				},
			}
			baseRun := &v1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
				Spec: v1alpha1.RunSpec{
					Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
					Model: "mockllm/model",
				},
				Status: v1alpha1.RunStatus{
					Phase: v1alpha1.RunPhaseCompleted,
					Tasks: []v1alpha1.TaskStatus{
						{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: baseLogPath},
					},
				},
			}

			comp := &v1alpha1.Comparison{
				ObjectMeta: metav1.ObjectMeta{Name: "test-comp-epoch", Namespace: "eval-runner"},
				Spec: v1alpha1.ComparisonSpec{
					Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
					Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(candRun, baseRun, comp).
				WithStatusSubresource(candRun, baseRun, comp).
				Build()

			reconciler := &ComparisonReconciler{
				Client:       cl,
				Scheme:       scheme,
				ArtifactSink: sink,
			}

			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-comp-epoch"}}
			_, err = reconciler.Reconcile(t.Context(), req)
			if err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}

			var updated v1alpha1.Comparison
			if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
				t.Fatalf("Get error: %v", err)
			}

			if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
				t.Fatalf("Expected phase Completed, got %v", updated.Status.Phase)
			}
			if len(updated.Status.Tasks) != 1 {
				t.Fatalf("Expected 1 task status, got %d", len(updated.Status.Tasks))
			}
			ts := updated.Status.Tasks[0]
			if ts.Pairs != tc.expectedPairs {
				t.Errorf("Pairs = %d, want %d", ts.Pairs, tc.expectedPairs)
			}
			if ts.Dropped.Candidate != tc.expectedDropC {
				t.Errorf("Dropped.Candidate = %d, want %d", ts.Dropped.Candidate, tc.expectedDropC)
			}
			if ts.Dropped.Baseline != tc.expectedDropB {
				t.Errorf("Dropped.Baseline = %d, want %d", ts.Dropped.Baseline, tc.expectedDropB)
			}
		})
	}
}

func TestComparisonReconciler_ExplicitMarginZero(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"

	// 20 samples with small symmetric differences:
	// 10 samples: cand 0.82, base 0.80 (delta +0.02)
	// 10 samples: cand 0.78, base 0.80 (delta -0.02)
	// Mean delta = 0.0, 95% CI is approx [-0.01, +0.01].
	// Within default margin ±0.05 -> unchanged.
	// Outside explicit margin ±0.00 -> inconclusive.
	var candScores []stats.SampleScore
	var baseScores []stats.SampleScore
	for i := 1; i <= 10; i++ {
		candScores = append(candScores, stats.SampleScore{ID: fmt.Sprintf("%d", i), Score: 0.82})
		baseScores = append(baseScores, stats.SampleScore{ID: fmt.Sprintf("%d", i), Score: 0.80})
	}
	for i := 11; i <= 20; i++ {
		candScores = append(candScores, stats.SampleScore{ID: fmt.Sprintf("%d", i), Score: 0.78})
		baseScores = append(baseScores, stats.SampleScore{ID: fmt.Sprintf("%d", i), Score: 0.80})
	}

	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}
	candLogPath := "gs://test-bucket/runs/cand-commit/docs/readme_comprehension/log.json"
	baseLogPath := "gs://test-bucket/runs/base-commit/docs/readme_comprehension/log.json"
	fakeGCS.Objects["test-bucket/runs/cand-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskID, candScores),
	}
	fakeGCS.Objects["test-bucket/runs/base-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskID, baseScores),
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: candLogPath},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: baseLogPath},
			},
		},
	}

	// Case 1: EquivalenceMargin is nil (default 0.05 applied) -> UNCHANGED
	t.Run("nil margin defaults to 0.05", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-default-margin", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-default-margin"}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictUnchanged {
			t.Errorf("Verdict = %v, want UNCHANGED with default margin", updated.Status.Verdict)
		}
	})

	// Case 2: EquivalenceMargin is explicitly set to 0.0 -> must NOT be overridden by 0.05 -> INCONCLUSIVE
	t.Run("explicit margin 0 is preserved", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-zero-margin", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:          v1alpha1.ComparisonRunRef{Run: "base-run"},
				Candidate:         v1alpha1.ComparisonRunRef{Run: "cand-run"},
				EquivalenceMargin: ptr.To(0.0),
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-zero-margin"}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("Verdict = %v, want INCONCLUSIVE with explicit margin 0.0", updated.Status.Verdict)
		}
	})
}

func TestComparisonReconciler_GitHubReporting(t *testing.T) {
	scheme := setupScheme(t)

	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://eval-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("failed to create GCS sink: %v", err)
	}

	candLogPath := "gs://eval-bucket/runs/cand-commit/" + taskID + "/inspect.json"
	baseLogPath := "gs://eval-bucket/runs/base-commit/" + taskID + "/inspect.json"

	candInspect := makeSyntheticInspectLog(taskID, []stats.SampleScore{
		{ID: "1", Score: 1.0},
		{ID: "2", Score: 1.0},
	})
	baseInspect := makeSyntheticInspectLog(taskID, []stats.SampleScore{
		{ID: "1", Score: 0.0},
		{ID: "2", Score: 0.0},
	})
	fakeGCS.Objects["eval-bucket/runs/cand-commit/"+taskID+"/inspect.json"] = fakes.FakeObject{Data: candInspect}
	fakeGCS.Objects["eval-bucket/runs/base-commit/"+taskID+"/inspect.json"] = fakes.FakeObject{Data: baseInspect}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: candLogPath, Samples: 2},
			},
			Summary: &v1alpha1.RunSummary{TotalSamples: 2},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: baseLogPath, Samples: 2},
			},
			Summary: &v1alpha1.RunSummary{TotalSamples: 2},
		},
	}

	makeComp := func(name string) *v1alpha1.Comparison {
		return &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     98,
					HeadSHA:    "cand-commit",
					BaseSHA:    "base-commit",
				},
			},
		}
	}

	t.Run("token absent -> disabled", func(t *testing.T) {
		comp := makeComp("comp-no-token")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token", // Secret does not exist
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-no-token"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile returned error: %v", err)
		}
		if res.Requeue {
			t.Errorf("unexpected requeue")
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated.Status.Reporting == nil || updated.Status.Reporting.Message != "disabled" {
			t.Errorf("expected reporting.message = 'disabled', got %+v", updated.Status.Reporting)
		}
		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed, got %v", updated.Status.Phase)
		}
	})

	t.Run("first post creates and second updates", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("test-gh-token")},
		}

		candRunLocal := candRun.DeepCopy()
		candRunLocal.Status.Phase = v1alpha1.RunPhaseRunning
		candRunLocal.Status.Tasks[0].Phase = v1alpha1.TaskPhaseRunning

		comp := makeComp("comp-post")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRunLocal, baseRun, comp, tokenSecret).
			WithStatusSubresource(candRunLocal, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-post"}}

		// 1. First post creates comment and check run
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("first Reconcile error: %v", err)
		}

		var updated1 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated1); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated1.Status.Reporting == nil {
			t.Fatalf("expected non-nil Status.Reporting")
		}
		if updated1.Status.Reporting.CommentID == 0 {
			t.Errorf("expected non-zero CommentID")
		}
		if updated1.Status.Reporting.CheckRunID == 0 {
			t.Errorf("expected non-zero CheckRunID")
		}
		if updated1.Status.Reporting.LastPostedHash == "" {
			t.Errorf("expected non-empty LastPostedHash")
		}
		if len(server.Comments()) != 1 {
			t.Fatalf("expected 1 comment created, got %d", len(server.Comments()))
		}
		if len(server.CheckRuns()) != 1 {
			t.Fatalf("expected 1 check run created, got %d", len(server.CheckRuns()))
		}
		if server.CheckRuns()[0].Status != "in_progress" {
			t.Errorf("expected check run status 'in_progress', got %q", server.CheckRuns()[0].Status)
		}
		if server.CheckRuns()[0].Conclusion != "" {
			t.Errorf("expected empty check run conclusion for in_progress, got %q", server.CheckRuns()[0].Conclusion)
		}

		// 2. Idempotency: second reconcile with same status makes no new HTTP requests
		reqCountBefore := len(server.RequestLog())
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("idempotent Reconcile error: %v", err)
		}
		if len(server.RequestLog()) != reqCountBefore {
			t.Errorf("expected 0 new requests on idempotent reconcile, got %d", len(server.RequestLog())-reqCountBefore)
		}

		// 3. Second reconcile when candidate completes updates comment and check run in place
		candRunLocal.Status.Phase = v1alpha1.RunPhaseCompleted
		candRunLocal.Status.Tasks[0].Phase = v1alpha1.TaskPhaseCompleted
		if err := cl.Status().Update(t.Context(), candRunLocal); err != nil {
			t.Fatalf("failed updating candidate run status: %v", err)
		}

		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("second Reconcile error: %v", err)
		}

		var updated2 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated2); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated2.Status.Reporting.CommentID != updated1.Status.Reporting.CommentID {
			t.Errorf("expected same CommentID %d, got %d", updated1.Status.Reporting.CommentID, updated2.Status.Reporting.CommentID)
		}
		if updated2.Status.Reporting.CheckRunID != updated1.Status.Reporting.CheckRunID {
			t.Errorf("expected same CheckRunID %d, got %d", updated1.Status.Reporting.CheckRunID, updated2.Status.Reporting.CheckRunID)
		}
		if len(server.Comments()) != 1 {
			t.Errorf("expected still 1 comment, got %d", len(server.Comments()))
		}
		if len(server.CheckRuns()) != 1 {
			t.Errorf("expected still 1 check run, got %d", len(server.CheckRuns()))
		}
		if server.CheckRuns()[0].Status != "completed" {
			t.Errorf("expected check run status 'completed', got %q", server.CheckRuns()[0].Status)
		}
		if server.CheckRuns()[0].Conclusion != "neutral" {
			t.Errorf("expected check run conclusion 'neutral', got %q", server.CheckRuns()[0].Conclusion)
		}
		if updated2.Status.Reporting.LastPostedHash == updated1.Status.Reporting.LastPostedHash {
			t.Errorf("expected LastPostedHash to change from in_progress to completed")
		}
		if !updated2.Status.Reporting.AnnotationsPosted {
			t.Errorf("expected AnnotationsPosted to be true at completion")
		}

		// 4. Third reconcile after terminal completion makes 0 new requests and duplicates nothing
		reqCountAfterTerminal := len(server.RequestLog())
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("third Reconcile error: %v", err)
		}
		if len(server.RequestLog()) != reqCountAfterTerminal {
			t.Errorf("expected 0 new requests after terminal reporting, got %d", len(server.RequestLog())-reqCountAfterTerminal)
		}
	})

	t.Run("terminal annotations resolved on changed files", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.SetPullRequestFiles("gke-labs", "eval-runner", 101, []string{"docs/readme.md"})

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("test-gh-token")},
		}

		manifestJSON := `{"tasks":[{"id":"` + taskID + `","paths":["docs/**"]}]}`
		fakeGCS.Objects["eval-bucket/runs/cand-commit-annot/manifest.json"] = fakes.FakeObject{Data: []byte(manifestJSON)}
		fakeGCS.Objects["eval-bucket/runs/base-commit-annot/manifest.json"] = fakes.FakeObject{Data: []byte(manifestJSON)}

		candLog := makeSyntheticInspectLog(taskID, []stats.SampleScore{
			{ID: "sample-1", Score: 0.0},
			{ID: "sample-2", Score: 1.0},
		})
		baseLog := makeSyntheticInspectLog(taskID, []stats.SampleScore{
			{ID: "sample-1", Score: 1.0},
			{ID: "sample-2", Score: 1.0},
		})
		candLogP := "gs://eval-bucket/runs/cand-commit-annot/" + taskID + "/inspect.json"
		baseLogP := "gs://eval-bucket/runs/base-commit-annot/" + taskID + "/inspect.json"
		fakeGCS.Objects["eval-bucket/runs/cand-commit-annot/"+taskID+"/inspect.json"] = fakes.FakeObject{Data: candLog}
		fakeGCS.Objects["eval-bucket/runs/base-commit-annot/"+taskID+"/inspect.json"] = fakes.FakeObject{Data: baseLog}

		candRunAnnot := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-annot", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit-annot"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					ManifestPath: "gs://eval-bucket/runs/cand-commit-annot/manifest.json",
				},
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: candLogP, Samples: 2, Successes: 1},
				},
				Summary: &v1alpha1.RunSummary{TotalSamples: 2},
			},
		}
		baseRunAnnot := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-annot", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit-annot"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: baseLogP, Samples: 2, Successes: 2},
				},
				Summary: &v1alpha1.RunSummary{TotalSamples: 2},
			},
		}

		compAnnot := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-annot", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-annot"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-annot"},
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     101,
					HeadSHA:    "cand-commit-annot",
					BaseSHA:    "base-commit-annot",
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRunAnnot, baseRunAnnot, compAnnot, tokenSecret).
			WithStatusSubresource(candRunAnnot, baseRunAnnot, compAnnot).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		reqAnnot := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-annot"}}
		if _, err := reconciler.Reconcile(t.Context(), reqAnnot); err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updatedAnnot v1alpha1.Comparison
		if err := cl.Get(t.Context(), reqAnnot.NamespacedName, &updatedAnnot); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if !updatedAnnot.Status.Reporting.AnnotationsPosted {
			t.Errorf("expected AnnotationsPosted to be true")
		}

		runs := server.CheckRuns()
		if len(runs) != 1 {
			t.Fatalf("expected 1 check run, got %d", len(runs))
		}
		if len(runs[0].Annotations) != 1 {
			t.Fatalf("expected 1 annotation, got %d: %+v", len(runs[0].Annotations), runs[0].Annotations)
		}
		// Path must be resolved to concrete file docs/readme.md, not glob docs/**
		if runs[0].Annotations[0].Path != "docs/readme.md" {
			t.Errorf("annotation path = %q, want 'docs/readme.md'", runs[0].Annotations[0].Path)
		}

		// Reconcile again: no extra requests or duplicate annotations
		reqCountBefore := len(server.RequestLog())
		if _, err := reconciler.Reconcile(t.Context(), reqAnnot); err != nil {
			t.Fatalf("second Reconcile error: %v", err)
		}
		if len(server.RequestLog()) != reqCountBefore {
			t.Errorf("expected 0 new requests on subsequent reconcile, got %d", len(server.RequestLog())-reqCountBefore)
		}
		if len(runs[0].Annotations) != 1 {
			t.Errorf("expected annotations to remain 1 (no duplicate), got %d", len(runs[0].Annotations))
		}
	})

	t.Run("transient failure at completion retries on next reconcile", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("test-gh-token")},
		}

		candRunLocal := candRun.DeepCopy()
		candRunLocal.Status.Phase = v1alpha1.RunPhaseRunning
		candRunLocal.Status.Tasks[0].Phase = v1alpha1.TaskPhaseRunning

		comp := makeComp("comp-retry-completion")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRunLocal, baseRun, comp, tokenSecret).
			WithStatusSubresource(candRunLocal, baseRun, comp).
			Build()

		testNow := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			Now:                 func() time.Time { return testNow },
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-retry-completion"}}

		// 1. Initial reconcile while candidate Run is Running: posts in_progress report
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("first Reconcile error: %v", err)
		}
		var initial v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &initial); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if initial.Status.Phase != v1alpha1.ComparisonPhaseRunning {
			t.Fatalf("expected phase Running, got %s", initial.Status.Phase)
		}
		h1 := initial.Status.Reporting.LastPostedHash
		if h1 == "" {
			t.Fatalf("expected non-empty lastPostedHash after first report")
		}

		// 2. Candidate Run completes; GitHub fails transiently (500) during completion reporting
		candRunLocal.Status.Phase = v1alpha1.RunPhaseCompleted
		candRunLocal.Status.Tasks[0].Phase = v1alpha1.TaskPhaseCompleted
		if err := cl.Status().Update(t.Context(), candRunLocal); err != nil {
			t.Fatalf("failed updating candidate run status: %v", err)
		}

		server.InjectError(500, `{"message":"GitHub internal server error"}`)

		_, err := reconciler.Reconcile(t.Context(), req)
		if err == nil {
			t.Fatalf("expected error from failed GitHub report at completion, got nil")
		}

		var mid v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &mid); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if mid.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Fatalf("expected phase Completed after completion attempt, got %s", mid.Status.Phase)
		}
		if mid.Status.Reporting.LastPostedHash != h1 {
			t.Fatalf("expected lastPostedHash to remain %s when report failed, got %s", h1, mid.Status.Reporting.LastPostedHash)
		}
		if mid.Status.Reporting.Message != "" {
			t.Fatalf("expected reporting message to remain empty on transient failure, got %q", mid.Status.Reporting.Message)
		}
		if mid.Status.Reporting.Attempts != 1 {
			t.Fatalf("expected reporting attempts 1, got %d", mid.Status.Reporting.Attempts)
		}

		// Immediate reconcile before due time: does not consume attempt, does not make requests
		reqCountBeforeSame := len(server.RequestLog())
		resSame, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected error on immediate reconcile: %v", err)
		}
		if resSame.RequeueAfter != 10*time.Second {
			t.Errorf("expected RequeueAfter 10s on immediate reconcile, got %v", resSame.RequeueAfter)
		}
		if len(server.RequestLog()) != reqCountBeforeSame {
			t.Errorf("expected no GitHub requests before retry is due, got %d", len(server.RequestLog())-reqCountBeforeSame)
		}

		// 3. Advance time past backoff delay: healthy server retries and posts final report
		testNow = testNow.Add(11 * time.Second)
		reqCountBefore := len(server.RequestLog())
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("retry Reconcile error: %v", err)
		}

		var finalComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &finalComp); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if finalComp.Status.Reporting.LastPostedHash == h1 {
			t.Errorf("expected lastPostedHash to update after successful retry, still %s", h1)
		}
		if len(server.RequestLog()) <= reqCountBefore {
			t.Errorf("expected new GitHub requests on retry, got 0")
		}

		// Check run should now be completed
		checkRuns := server.CheckRuns()
		if len(checkRuns) != 1 {
			t.Fatalf("expected 1 check run, got %d", len(checkRuns))
		}
		if checkRuns[0].Status != "completed" {
			t.Errorf("expected check run status 'completed', got %q", checkRuns[0].Status)
		}

		// 4. Steady-state terminal reconcile: makes zero new GitHub requests and zero status writes
		reqCountSteady := len(server.RequestLog())
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("steady-state Reconcile error: %v", err)
		}
		if res.RequeueAfter != 0 {
			t.Errorf("expected RequeueAfter 0, got %v", res.RequeueAfter)
		}
		if len(server.RequestLog()) != reqCountSteady {
			t.Errorf("expected 0 new requests on steady-state terminal reconcile, got %d", len(server.RequestLog())-reqCountSteady)
		}
		var steadyComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &steadyComp); err != nil {
			t.Fatalf("Get steadyComp error: %v", err)
		}
		if steadyComp.ResourceVersion != finalComp.ResourceVersion {
			t.Errorf("expected zero status writes on steady-state terminal reconcile (resourceVersion changed from %s to %s)", finalComp.ResourceVersion, steadyComp.ResourceVersion)
		}
	})

	t.Run("marker search avoids duplicate comment", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("test-gh-token")},
		}

		comp := makeComp("comp-marker")
		// Simulate existing comment with marker already on PR
		marker := github.ComparisonMarker("eval-runner", "comp-marker")
		_, err := server.Client("test-gh-token").CreateIssueComment(t.Context(), "gke-labs", "eval-runner", 98, marker+"\nold comment")
		if err != nil {
			t.Fatalf("failed to create existing marker comment: %v", err)
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, tokenSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-marker"}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		if len(server.Comments()) != 1 {
			t.Errorf("expected marker search to update existing comment without creating duplicate, got %d comments", len(server.Comments()))
		}
	})

	t.Run("403/404 -> recorded not retried", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.InjectError(404, `{"message":"Not Found"}`)

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("test-gh-token")},
		}

		comp := makeComp("comp-404")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, tokenSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-404"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("expected nil error for permanent 404, got %v", err)
		}
		if res.Requeue {
			t.Errorf("expected no requeue for permanent 404")
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated.Status.Reporting == nil || !strings.Contains(updated.Status.Reporting.Message, "404") {
			t.Errorf("expected reporting.message to contain '404', got %+v", updated.Status.Reporting)
		}
		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed, got %v", updated.Status.Phase)
		}
	})

	t.Run("5xx -> requeue", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.InjectError(500, `{"message":"Internal Server Error"}`)

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("test-gh-token")},
		}

		comp := makeComp("comp-500")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, tokenSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-500"}}
		_, err := reconciler.Reconcile(t.Context(), req)
		if err == nil {
			t.Fatalf("expected error on 500 for controller-runtime requeue, got nil")
		}
		if !strings.Contains(err.Error(), "500") {
			t.Errorf("expected error to mention 500, got %v", err)
		}
	})

	t.Run("check run 403 on PAT -> comment posted, partial status recorded", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.InjectPathError("POST", "/repos/gke-labs/eval-runner/check-runs", 403, `{"message":"Resource not accessible by personal access token"}`)

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("test-pat-token")},
		}

		comp := makeComp("comp-pat-403")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, tokenSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-pat-403"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("expected nil error on check run 403, got: %v", err)
		}
		if res.Requeue {
			t.Errorf("expected no requeue on check run 403")
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated.Status.Reporting == nil {
			t.Fatalf("expected non-nil status.reporting")
		}
		if updated.Status.Reporting.CommentID == 0 {
			t.Errorf("expected commentID to be saved in status")
		}
		if updated.Status.Reporting.LastPostedHash == "" {
			t.Errorf("expected lastPostedHash to be saved in status")
		}
		if updated.Status.Reporting.CheckRunID != 0 {
			t.Errorf("expected checkRunID 0, got %d", updated.Status.Reporting.CheckRunID)
		}
		if updated.Status.Reporting.Message != "check runs unavailable with token auth; comment posted" {
			t.Errorf("expected message 'check runs unavailable with token auth; comment posted', got %q", updated.Status.Reporting.Message)
		}
		if len(server.Comments()) != 1 {
			t.Errorf("expected 1 comment created, got %d", len(server.Comments()))
		}

		// Verify idempotency on second reconcile (gated on hash, no new requests)
		reqCountBefore := len(server.RequestLog())
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("second Reconcile error: %v", err)
		}
		if len(server.RequestLog()) != reqCountBefore {
			t.Errorf("expected 0 new requests on second reconcile, got %d", len(server.RequestLog())-reqCountBefore)
		}
	})

	t.Run("explicitly disabled githubTokenSecret in ConfigMap", func(t *testing.T) {
		comp := makeComp("comp-disabled")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "", // Explicitly disabled
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-disabled"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}
		if res.Requeue {
			t.Errorf("unexpected requeue")
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated.Status.Reporting == nil || updated.Status.Reporting.Message != "disabled" {
			t.Errorf("expected reporting.message = 'disabled', got %+v", updated.Status.Reporting)
		}
	})

	t.Run("app auth in secret preferred over token and creates check run", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()

		privKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("failed to generate test RSA key: %v", err)
		}
		pemBytes := pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privKey),
		})

		appSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data: map[string][]byte{
				"app-id":          []byte("12345"),
				"installation-id": []byte("67890"),
				"private-key":     pemBytes,
				"token":           []byte("fallback-pat-token"),
			},
		}

		comp := makeComp("comp-app-auth")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, appSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		factoryCalled := false
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				factoryCalled = true
				if creds.AppID != "12345" || creds.InstallationID != "67890" || creds.AppAuth == nil {
					t.Errorf("expected App credentials with AppAuth, got: %+v", creds)
				}
				return server.Client("app-inst-token")
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-app-auth"}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		if !factoryCalled {
			t.Errorf("expected GitHubClientFactory to be called")
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated.Status.Reporting == nil {
			t.Fatalf("expected non-nil Status.Reporting")
		}
		if updated.Status.Reporting.CommentID == 0 {
			t.Errorf("expected non-zero CommentID")
		}
		if updated.Status.Reporting.CheckRunID == 0 {
			t.Errorf("expected non-zero CheckRunID with App auth")
		}
	})

	t.Run("uiBaseURL produces absolute link in PR comment", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("test-gh-token")},
		}

		comp := makeComp("comp-ui-url")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, tokenSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			UIBaseURL:           "https://eval-ui.example.com",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-ui-url"}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		comments := server.Comments()
		if len(comments) != 1 {
			t.Fatalf("expected 1 comment, got %d", len(comments))
		}
		expectedURL := "[web view](https://eval-ui.example.com/github.com/gke-labs/eval-runner/pulls/98)"
		if !strings.Contains(comments[0].Body, expectedURL) {
			t.Errorf("expected comment body to contain absolute link %q, got:\n%s", expectedURL, comments[0].Body)
		}
		checkRuns := server.CheckRuns()
		if len(checkRuns) != 1 {
			t.Fatalf("expected 1 check run, got %d", len(checkRuns))
		}
		if checkRuns[0].DetailsURL != "https://eval-ui.example.com/github.com/gke-labs/eval-runner/pulls/98" {
			t.Errorf("expected check run DetailsURL %q, got %q", "https://eval-ui.example.com/github.com/gke-labs/eval-runner/pulls/98", checkRuns[0].DetailsURL)
		}
	})

	t.Run("inconclusivePolicy failure produces failure check run", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("test-gh-token")},
		}

		comp := makeComp("comp-inconclusive-failure")
		comp.Status.Verdict = v1alpha1.ComparisonVerdictInconclusive
		comp.Status.Phase = v1alpha1.ComparisonPhaseCompleted
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, tokenSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			InconclusivePolicy:  "failure",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-inconclusive-failure"}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		checkRuns := server.CheckRuns()
		if len(checkRuns) != 1 {
			t.Fatalf("expected 1 check run, got %d", len(checkRuns))
		}
		if checkRuns[0].Conclusion != "failure" {
			t.Errorf("expected conclusion 'failure', got %q", checkRuns[0].Conclusion)
		}
	})

	t.Run("retryable 502 on check run persists comment ID with empty hash and retries on second reconcile", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()

		// Inject 502 Bad Gateway on check runs
		server.InjectPathError("POST", "/repos/gke-labs/eval-runner/check-runs", 502, `{"message":"Bad Gateway"}`)

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("test-gh-token")},
		}

		comp := makeComp("comp-retry-checkrun")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, tokenSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		testNow := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			Now:                 func() time.Time { return testNow },
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-retry-checkrun"}}

		// 1. First reconcile fails on check run with 502 (retryable error returned)
		_, err := reconciler.Reconcile(t.Context(), req)
		if err == nil {
			t.Fatalf("expected error on 502 check-run creation")
		}

		var updated1 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated1); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated1.Status.Reporting == nil {
			t.Fatalf("expected non-nil Status.Reporting")
		}
		if updated1.Status.Reporting.CommentID == 0 {
			t.Errorf("expected CommentID to be preserved despite check run failure")
		}
		if updated1.Status.Reporting.LastPostedHash != "" {
			t.Errorf("expected LastPostedHash to be empty so requeue is not swallowed by hash gate, got %q", updated1.Status.Reporting.LastPostedHash)
		}
		if updated1.Status.Reporting.CheckRunID != 0 {
			t.Errorf("expected CheckRunID 0, got %d", updated1.Status.Reporting.CheckRunID)
		}

		// 2. Second reconcile (check run error is cleared) after retry backoff
		testNow = testNow.Add(11 * time.Second)
		// Hash gate does not swallow because LastPostedHash is empty; check run is created!
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("second Reconcile error: %v", err)
		}

		var updated2 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated2); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated2.Status.Reporting.CheckRunID == 0 {
			t.Errorf("expected non-zero CheckRunID after second reconcile")
		}
		if updated2.Status.Reporting.LastPostedHash == "" {
			t.Errorf("expected LastPostedHash to be set after check run succeeds")
		}
		if len(server.CheckRuns()) != 1 {
			t.Errorf("expected 1 check run on server, got %d", len(server.CheckRuns()))
		}
	})

	t.Run("app auth installation token cache survives across reconciles", func(t *testing.T) {
		ctx := t.Context()
		server := github.NewFakeGitHubServer()
		defer server.Close()

		privKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("failed to generate test RSA key: %v", err)
		}
		pemBytes := pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privKey),
		})

		appSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "github-token",
				Namespace:       "eval-runner",
				ResourceVersion: "1",
			},
			Data: map[string][]byte{
				"app-id":          []byte("12345"),
				"installation-id": []byte("67890"),
				"private-key":     pemBytes,
			},
		}

		comp := makeComp("comp-cached-auth")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, appSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubBaseURL:       server.URL(),
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-cached-auth"}}

		// First reconcile: makes 1 access_tokens request
		if _, err := reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("first Reconcile error: %v", err)
		}

		countAccessTokens := func() int {
			count := 0
			for _, r := range server.RequestLog() {
				if strings.Contains(r, "/access_tokens") {
					count++
				}
			}
			return count
		}

		if got := countAccessTokens(); got != 1 {
			t.Fatalf("expected 1 access_tokens request on first reconcile, got %d", got)
		}

		// Update candidate run inspected log to trigger second report (non-idempotent)
		candInspectUpdated := makeSyntheticInspectLog(taskID, []stats.SampleScore{
			{ID: "1", Score: 0.0},
			{ID: "2", Score: 0.0},
		})
		fakeGCS.Objects["eval-bucket/runs/cand-commit/"+taskID+"/inspect.json"] = fakes.FakeObject{Data: candInspectUpdated}

		// Second reconcile: should reuse cached AppTokenProvider, making ZERO new access_tokens requests
		if _, err := reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("second Reconcile error: %v", err)
		}

		if got := countAccessTokens(); got != 1 {
			t.Errorf("expected still 1 access_tokens request after second reconcile (cache survived), got %d", got)
		}

		// Now simulate updating the secret (new ResourceVersion or new key)
		var latestSecret corev1.Secret
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: "github-token"}, &latestSecret); err != nil {
			t.Fatalf("failed to get secret: %v", err)
		}
		latestSecret.Data["app-id"] = []byte("99999")
		if err := cl.Update(ctx, &latestSecret); err != nil {
			t.Fatalf("failed to update secret: %v", err)
		}

		// Third reconcile: use a new comparison because completed comparisons return early without re-evaluating or re-reporting.
		// Since the secret changed, the token cache is invalidated -> exactly 2 access_tokens requests.
		comp3 := makeComp("comp-cached-auth-3")
		if err := cl.Create(ctx, comp3); err != nil {
			t.Fatalf("failed to create comp3: %v", err)
		}
		req3 := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-cached-auth-3"}}
		if _, err := reconciler.Reconcile(ctx, req3); err != nil {
			t.Fatalf("third Reconcile error: %v", err)
		}

		if got := countAccessTokens(); got != 2 {
			t.Errorf("expected 2 access_tokens requests after secret changed, got %d", got)
		}
	})

	t.Run("app auth identity failure surfaces in status.reporting.message", func(t *testing.T) {
		ctx := t.Context()
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.SetAppSlug("") // /app returns no slug

		privKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("failed to generate test RSA key: %v", err)
		}
		pemBytes := pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privKey),
		})

		appSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "github-token",
				Namespace: "eval-runner",
			},
			Data: map[string][]byte{
				"app-id":          []byte("12345"),
				"installation-id": []byte("67890"),
				"private-key":     pemBytes,
			},
		}

		comp := makeComp("comp-no-slug")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, appSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubBaseURL:       server.URL(),
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-no-slug"}}
		res, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("expected nil error for permanent identity failure, got: %v", err)
		}
		if res.Requeue {
			t.Errorf("expected no requeue for permanent identity failure")
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(ctx, req.NamespacedName, &updated); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated.Status.Reporting == nil {
			t.Fatalf("expected non-nil Status.Reporting")
		}
		if !strings.Contains(updated.Status.Reporting.Message, "did not contain an app slug") {
			t.Errorf("expected reporting.message to contain 'did not contain an app slug', got: %q", updated.Status.Reporting.Message)
		}
	})

	t.Run("token endpoint transient failure at completion retries on next reconcile", func(t *testing.T) {
		ctx := t.Context()
		server := github.NewFakeGitHubServer()
		defer server.Close()

		privKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("failed to generate test RSA key: %v", err)
		}
		pemBytes := pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privKey),
		})

		appSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "github-token",
				Namespace: "eval-runner",
			},
			Data: map[string][]byte{
				"app-id":          []byte("12345"),
				"installation-id": []byte("67890"),
				"private-key":     pemBytes,
			},
		}

		comp := makeComp("comp-token-retry")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, appSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		testNow := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubBaseURL:       server.URL(),
			Now:                 func() time.Time { return testNow },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-token-retry"}}

		// 1. Token endpoint fails once with 500 at completion reporting
		server.InjectPathError("POST", "/app/installations/67890/access_tokens", 500, `{"message":"token server error"}`)

		_, err = reconciler.Reconcile(ctx, req)
		if err == nil {
			t.Fatalf("expected error from failed token endpoint on completion reporting, got nil")
		}

		var updated1 v1alpha1.Comparison
		if err := cl.Get(ctx, req.NamespacedName, &updated1); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated1.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Fatalf("expected phase Completed, got %s", updated1.Status.Phase)
		}
		if updated1.Status.Reporting == nil {
			t.Fatalf("expected non-nil Status.Reporting")
		}
		if updated1.Status.Reporting.Message != "" {
			t.Errorf("expected reporting.message to be empty on transient failure, got %q", updated1.Status.Reporting.Message)
		}
		if updated1.Status.Reporting.Attempts != 1 {
			t.Errorf("expected reporting.attempts 1, got %d", updated1.Status.Reporting.Attempts)
		}
		if updated1.Status.Reporting.NextAttemptAt == nil {
			t.Fatalf("expected non-nil nextAttemptAt")
		}
		expectedNext := testNow.Add(10 * time.Second)
		if !updated1.Status.Reporting.NextAttemptAt.Equal(&metav1.Time{Time: expectedNext}) {
			t.Errorf("expected nextAttemptAt %v, got %v", expectedNext, updated1.Status.Reporting.NextAttemptAt.Time)
		}

		// Immediate reconcile before due time does not consume attempt and makes 0 new requests
		reqCountBeforeSame := len(server.RequestLog())
		resSame, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("unexpected error on immediate reconcile: %v", err)
		}
		if resSame.RequeueAfter != 10*time.Second {
			t.Errorf("expected RequeueAfter 10s, got %v", resSame.RequeueAfter)
		}
		if len(server.RequestLog()) != reqCountBeforeSame {
			t.Errorf("expected 0 new requests before retry is due, got %d", len(server.RequestLog())-reqCountBeforeSame)
		}

		// 2. Next reconcile after backoff delay: healthy token endpoint posts final comment and check run
		testNow = testNow.Add(11 * time.Second)
		reqCountBefore := len(server.RequestLog())
		if _, err := reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("retry Reconcile error: %v", err)
		}

		var updated2 v1alpha1.Comparison
		if err := cl.Get(ctx, req.NamespacedName, &updated2); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if updated2.Status.Reporting.Message != "" {
			t.Errorf("expected empty message after successful reporting, got %q", updated2.Status.Reporting.Message)
		}
		if updated2.Status.Reporting.CommentID == 0 {
			t.Errorf("expected non-zero CommentID")
		}
		if updated2.Status.Reporting.CheckRunID == 0 {
			t.Errorf("expected non-zero CheckRunID")
		}
		if updated2.Status.Reporting.LastPostedHash == "" {
			t.Errorf("expected non-empty LastPostedHash")
		}
		if updated2.Status.Reporting.Attempts != 0 {
			t.Errorf("expected attempts reset to 0, got %d", updated2.Status.Reporting.Attempts)
		}
		if updated2.Status.Reporting.NextAttemptAt != nil {
			t.Errorf("expected nextAttemptAt cleared, got %v", updated2.Status.Reporting.NextAttemptAt)
		}
		if len(server.Comments()) != 1 {
			t.Errorf("expected 1 comment on server, got %d", len(server.Comments()))
		}
		checkRuns := server.CheckRuns()
		if len(checkRuns) != 1 {
			t.Fatalf("expected 1 check run on server, got %d", len(checkRuns))
		}
		if checkRuns[0].Status != "completed" {
			t.Errorf("expected check run status 'completed', got %q", checkRuns[0].Status)
		}
		if len(server.RequestLog()) <= reqCountBefore {
			t.Errorf("expected new requests on retry")
		}

		// 3. Steady-state terminal reconcile: 0 new requests, 0 status writes
		reqCountSteady := len(server.RequestLog())
		resSteady, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("steady-state Reconcile error: %v", err)
		}
		if resSteady.RequeueAfter != 0 {
			t.Errorf("expected RequeueAfter 0, got %v", resSteady.RequeueAfter)
		}
		if len(server.RequestLog()) != reqCountSteady {
			t.Errorf("expected 0 new requests on steady-state terminal reconcile, got %d", len(server.RequestLog())-reqCountSteady)
		}
		var steadyComp v1alpha1.Comparison
		if err := cl.Get(ctx, req.NamespacedName, &steadyComp); err != nil {
			t.Fatalf("Get steadyComp error: %v", err)
		}
		if steadyComp.ResourceVersion != updated2.ResourceVersion {
			t.Errorf("expected 0 status writes on steady-state terminal reconcile, resourceVersion changed")
		}
	})

	t.Run("token endpoint transient failures capped at limit with permanent message", func(t *testing.T) {
		ctx := t.Context()
		server := github.NewFakeGitHubServer()
		defer server.Close()

		privKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("failed to generate test RSA key: %v", err)
		}
		pemBytes := pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privKey),
		})

		appSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "github-token",
				Namespace: "eval-runner",
			},
			Data: map[string][]byte{
				"app-id":          []byte("12345"),
				"installation-id": []byte("67890"),
				"private-key":     pemBytes,
			},
		}

		comp := makeComp("comp-token-cap")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, appSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		testNow := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubBaseURL:       server.URL(),
			Now:                 func() time.Time { return testNow },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-token-cap"}}

		// Attempt 1: Token endpoint fails
		server.InjectPathError("POST", "/app/installations/67890/access_tokens", 500, `{"message":"token server error 1"}`)
		_, err = reconciler.Reconcile(ctx, req)
		if err == nil {
			t.Fatalf("expected error on attempt 1")
		}

		var compAttempt1 v1alpha1.Comparison
		if err := cl.Get(ctx, req.NamespacedName, &compAttempt1); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if compAttempt1.Status.Reporting.Message != "" {
			t.Errorf("expected empty message on attempt 1, got %q", compAttempt1.Status.Reporting.Message)
		}
		if compAttempt1.Status.Reporting.Attempts != 1 {
			t.Errorf("expected attempts 1, got %d", compAttempt1.Status.Reporting.Attempts)
		}

		// Attempt 2: Advance time past 10s backoff, token endpoint fails again
		testNow = testNow.Add(11 * time.Second)
		server.InjectPathError("POST", "/app/installations/67890/access_tokens", 500, `{"message":"token server error 2"}`)
		_, err = reconciler.Reconcile(ctx, req)
		if err == nil {
			t.Fatalf("expected error on attempt 2")
		}

		var compAttempt2 v1alpha1.Comparison
		if err := cl.Get(ctx, req.NamespacedName, &compAttempt2); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if compAttempt2.Status.Reporting.Message != "" {
			t.Errorf("expected empty message on attempt 2, got %q", compAttempt2.Status.Reporting.Message)
		}
		if compAttempt2.Status.Reporting.Attempts != 2 {
			t.Errorf("expected attempts 2, got %d", compAttempt2.Status.Reporting.Attempts)
		}

		// Attempt 3: Advance time past 1m backoff, token endpoint fails third time
		testNow = testNow.Add(61 * time.Second)
		server.InjectPathError("POST", "/app/installations/67890/access_tokens", 500, `{"message":"token server error 3"}`)
		_, err = reconciler.Reconcile(ctx, req)
		if err == nil {
			t.Fatalf("expected error on attempt 3")
		}

		var compAttempt3 v1alpha1.Comparison
		if err := cl.Get(ctx, req.NamespacedName, &compAttempt3); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if compAttempt3.Status.Reporting.Message != "" {
			t.Errorf("expected empty message on attempt 3, got %q", compAttempt3.Status.Reporting.Message)
		}
		if compAttempt3.Status.Reporting.Attempts != 3 {
			t.Errorf("expected attempts 3, got %d", compAttempt3.Status.Reporting.Attempts)
		}

		// Attempt 4: Advance time past 5m backoff, token endpoint fails fourth time reaching cap (MaxReportingAttempts = 4)
		testNow = testNow.Add(301 * time.Second)
		server.InjectPathError("POST", "/app/installations/67890/access_tokens", 500, `{"message":"token server error 4"}`)
		res4, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("expected nil error on cap exhaustion (stops retrying), got: %v", err)
		}
		if res4.RequeueAfter != 0 {
			t.Errorf("expected RequeueAfter 0, got %v", res4.RequeueAfter)
		}

		var compCapped v1alpha1.Comparison
		if err := cl.Get(ctx, req.NamespacedName, &compCapped); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if compCapped.Status.Reporting.Attempts != 4 {
			t.Errorf("expected attempts 4, got %d", compCapped.Status.Reporting.Attempts)
		}
		if compCapped.Status.Reporting.NextAttemptAt != nil {
			t.Errorf("expected nextAttemptAt nil, got %v", compCapped.Status.Reporting.NextAttemptAt)
		}
		if !strings.Contains(compCapped.Status.Reporting.Message, "token server error 4") {
			t.Errorf("expected reporting.message to contain 'token server error 4', got %q", compCapped.Status.Reporting.Message)
		}

		// Subsequent reconcile: steady state, zero new requests, zero status writes
		reqCountBefore := len(server.RequestLog())
		resSteady, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("steady-state reconcile error: %v", err)
		}
		if resSteady.RequeueAfter != 0 {
			t.Errorf("expected RequeueAfter 0, got %v", resSteady.RequeueAfter)
		}
		if len(server.RequestLog()) != reqCountBefore {
			t.Errorf("expected 0 new requests on subsequent reconcile after cap, got %d", len(server.RequestLog())-reqCountBefore)
		}
		var steadyComp v1alpha1.Comparison
		if err := cl.Get(ctx, req.NamespacedName, &steadyComp); err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if steadyComp.ResourceVersion != compCapped.ResourceVersion {
			t.Errorf("expected zero status writes on steady-state reconcile after cap")
		}
	})

	t.Run("in-progress transient errors do not burn attempt cap", func(t *testing.T) {
		ctx := t.Context()
		server := github.NewFakeGitHubServer()
		defer server.Close()

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("test-gh-token")},
		}

		candRunLocal := candRun.DeepCopy()
		candRunLocal.Status.Phase = v1alpha1.RunPhaseRunning
		candRunLocal.Status.Tasks[0].Phase = v1alpha1.TaskPhaseRunning

		comp := makeComp("comp-running-blips")
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRunLocal, baseRun, comp, tokenSecret).
			WithStatusSubresource(candRunLocal, baseRun, comp).
			Build()

		testNow := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			Now:                 func() time.Time { return testNow },
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-running-blips"}}

		// 3 Running blips with no clock advance: none should consume attempts or set permanent message
		for i := 1; i <= 3; i++ {
			server.InjectError(500, fmt.Sprintf(`{"message":"in-progress error %d"}`, i))
			_, err := reconciler.Reconcile(ctx, req)
			if err == nil {
				t.Fatalf("expected error on in-progress reconcile %d, got nil", i)
			}
			var cur v1alpha1.Comparison
			if err := cl.Get(ctx, req.NamespacedName, &cur); err != nil {
				t.Fatalf("Get error: %v", err)
			}
			if cur.Status.Reporting != nil {
				if cur.Status.Reporting.Attempts != 0 {
					t.Errorf("expected attempts 0 during running phase, got %d", cur.Status.Reporting.Attempts)
				}
				if cur.Status.Reporting.Message != "" {
					t.Errorf("expected empty message during running phase, got %q", cur.Status.Reporting.Message)
				}
			}
		}

		// Candidate Run completes; 1 blip at completion: attempts bumps to 1, message remains empty
		candRunLocal.Status.Phase = v1alpha1.RunPhaseCompleted
		candRunLocal.Status.Tasks[0].Phase = v1alpha1.TaskPhaseCompleted
		if err := cl.Status().Update(ctx, candRunLocal); err != nil {
			t.Fatalf("failed updating candidate run status: %v", err)
		}

		server.InjectError(500, `{"message":"completion reporting error"}`)
		_, err := reconciler.Reconcile(ctx, req)
		if err == nil {
			t.Fatalf("expected error on completion report failure, got nil")
		}

		var mid v1alpha1.Comparison
		if err := cl.Get(ctx, req.NamespacedName, &mid); err != nil {
			t.Fatalf("Get mid error: %v", err)
		}
		if mid.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Fatalf("expected phase Completed, got %s", mid.Status.Phase)
		}
		if mid.Status.Reporting == nil {
			t.Fatalf("expected non-nil Status.Reporting")
		}
		if mid.Status.Reporting.Attempts != 1 {
			t.Errorf("expected attempts 1 at completion failure, got %d", mid.Status.Reporting.Attempts)
		}
		if mid.Status.Reporting.Message != "" {
			t.Errorf("expected empty message on transient completion failure, got %q", mid.Status.Reporting.Message)
		}

		// Healthy reconcile after backoff delay: posts final report and completes check run
		testNow = testNow.Add(11 * time.Second)
		if _, err := reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("retry Reconcile error: %v", err)
		}

		var finalComp v1alpha1.Comparison
		if err := cl.Get(ctx, req.NamespacedName, &finalComp); err != nil {
			t.Fatalf("Get finalComp error: %v", err)
		}
		if finalComp.Status.Reporting.Message != "" {
			t.Errorf("expected empty message on success, got %q", finalComp.Status.Reporting.Message)
		}
		if finalComp.Status.Reporting.LastPostedHash == "" {
			t.Errorf("expected non-empty LastPostedHash")
		}
		if len(server.Comments()) != 1 {
			t.Errorf("expected 1 comment, got %d", len(server.Comments()))
		}
		checkRuns := server.CheckRuns()
		if len(checkRuns) != 1 || checkRuns[0].Status != "completed" {
			t.Errorf("expected 1 completed check run, got %v", checkRuns)
		}
	})
}

func TestComparisonReconcile_SampleRangePairing(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	taskID := "docs/readme_comprehension"

	t.Run("same range yields full pairs", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		// Both Runs evaluated range 1-10 -> sample IDs "1" to "10"
		var candScores, baseScores []stats.SampleScore
		for i := 1; i <= 10; i++ {
			candScores = append(candScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
			baseScores = append(baseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		}

		fakeGCS.Objects["test-bucket/runs/cand-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, candScores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, baseScores),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cand-run",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 10},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/docs/readme_comprehension/log.json",
						Samples:     10,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 10},
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "base-run",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 10},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/docs/readme_comprehension/log.json",
						Samples:     10,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 10},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-comp",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-comp"}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if len(updated.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task in comparison, got %d", len(updated.Status.Tasks))
		}
		task := updated.Status.Tasks[0]
		if task.Pairs != 10 {
			t.Errorf("Expected 10 pairs for identical ranges, got %d", task.Pairs)
		}
		if task.Dropped.Candidate != 0 || task.Dropped.Baseline != 0 {
			t.Errorf("Expected 0 dropped samples, got cand=%d, base=%d", task.Dropped.Candidate, task.Dropped.Baseline)
		}
		if task.Label != v1alpha1.ComparisonTaskLabelUnchanged {
			t.Errorf("Expected label unchanged, got %s", task.Label)
		}
	})

	t.Run("disjoint ranges yield zero pairs", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		// Candidate evaluated range 1-5 -> sample IDs "1" to "5"
		var candScores []stats.SampleScore
		for i := 1; i <= 5; i++ {
			candScores = append(candScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		}
		// Baseline evaluated disjoint range 6-10 -> sample IDs "6" to "10"
		var baseScores []stats.SampleScore
		for i := 6; i <= 10; i++ {
			baseScores = append(baseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		}

		fakeGCS.Objects["test-bucket/runs/cand-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, candScores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/docs/readme_comprehension/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, baseScores),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cand-run",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 5},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/docs/readme_comprehension/log.json",
						Samples:     5,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 5},
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "base-run",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 6, End: 10},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/docs/readme_comprehension/log.json",
						Samples:     5,
						SampleRange: &v1alpha1.SampleRange{Start: 6, End: 10},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-comp-disjoint",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "test-comp-disjoint"}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if len(updated.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task in comparison, got %d", len(updated.Status.Tasks))
		}
		task := updated.Status.Tasks[0]
		if task.Pairs != 0 {
			t.Errorf("Expected 0 pairs for disjoint ranges, got %d", task.Pairs)
		}
		if task.Dropped.Candidate != 5 || task.Dropped.Baseline != 5 {
			t.Errorf("Expected 5 dropped cand and 5 dropped base, got cand=%d, base=%d", task.Dropped.Candidate, task.Dropped.Baseline)
		}
		if task.Label != v1alpha1.ComparisonTaskLabelInconclusive {
			t.Errorf("Expected label inconclusive for disjoint ranges, got %s", task.Label)
		}
	})
}

func TestComparisonReconciler_ManifestOnlyTasksExcluded(t *testing.T) {
	scheme := setupScheme(t)
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	execTaskID := "docs/readme_comprehension"
	blindTaskID := "docs/readme_comprehension_blind"
	candLogPath := "gs://test-bucket/runs/cand-commit/" + execTaskID + "/log.json"
	baseLogPath := "gs://test-bucket/runs/base-commit/" + execTaskID + "/log.json"

	// 8 identical sample scores => unchanged within margin
	scores := make([]stats.SampleScore, 8)
	for i := 0; i < 8; i++ {
		scores[i] = stats.SampleScore{ID: fmt.Sprintf("sample-%d", i+1), Score: 0.8}
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/"+execTaskID+"/log.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(execTaskID, scores),
	}
	fakeGCS.Objects["test-bucket/runs/base-commit/"+execTaskID+"/log.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(execTaskID, scores),
	}

	manifestData := []byte(`{"tasks": [{"id": "docs/readme_comprehension"}, {"id": "docs/readme_comprehension_blind"}]}`)
	fakeGCS.Objects["test-bucket/runs/cand-commit/manifest.json"] = fakes.FakeObject{
		Data: manifestData,
	}
	fakeGCS.Objects["test-bucket/runs/base-commit/manifest.json"] = fakes.FakeObject{
		Data: manifestData,
	}

	t.Run("two runs restricted to one of two manifest tasks produce a single status.tasks entry and UNCHANGED verdict", func(t *testing.T) {
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-overlap", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model: "mockllm/model",
				Tasks: []string{execTaskID},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					TaskCount:         2,
					EligibleTaskCount: 2,
					TaskIDs:           []string{execTaskID, blindTaskID},
					ManifestPath:      "gs://test-bucket/runs/cand-commit/manifest.json",
				},
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      execTaskID,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: candLogPath,
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-overlap", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model: "mockllm/model",
				Tasks: []string{execTaskID},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					TaskCount:         2,
					EligibleTaskCount: 2,
					TaskIDs:           []string{execTaskID, blindTaskID},
					ManifestPath:      "gs://test-bucket/runs/base-commit/manifest.json",
				},
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      execTaskID,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: baseLogPath,
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "cmp-overlap", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-overlap"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-overlap"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "cmp-overlap"}}
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if len(updated.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task in comparison status.tasks, got %d: %+v", len(updated.Status.Tasks), updated.Status.Tasks)
		}
		if updated.Status.Tasks[0].ID != execTaskID {
			t.Errorf("Expected task %s, got %s", execTaskID, updated.Status.Tasks[0].ID)
		}
		if updated.Status.Tasks[0].Label != v1alpha1.ComparisonTaskLabelUnchanged {
			t.Errorf("Expected task label %s, got %s", v1alpha1.ComparisonTaskLabelUnchanged, updated.Status.Tasks[0].Label)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictUnchanged {
			t.Errorf("Expected aggregate verdict %s, got %s", v1alpha1.ComparisonVerdictUnchanged, updated.Status.Verdict)
		}
		if updated.Status.Summary == nil || updated.Status.Summary.Tasks != "1/1" {
			t.Errorf("Expected summary tasks 1/1, got %+v", updated.Status.Summary)
		}
		if updated.Status.Summary.Skipped != 1 {
			t.Errorf("Expected summary.skipped 1, got %d", updated.Status.Summary.Skipped)
		}
		if !strings.Contains(updated.Status.Message, "skipped (not planned): docs/readme_comprehension_blind") {
			t.Errorf("Expected status.message to contain 'skipped (not planned): docs/readme_comprehension_blind', got %q", updated.Status.Message)
		}
	})

	t.Run("spec.tasks naming unexecuted task surfaces in status.message and does not add inconclusive row", func(t *testing.T) {
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-spec", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model: "mockllm/model",
				Tasks: []string{execTaskID},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      execTaskID,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: candLogPath,
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-spec", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model: "mockllm/model",
				Tasks: []string{execTaskID},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      execTaskID,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: baseLogPath,
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "cmp-spec-unexecuted", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-spec"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-spec"},
				Tasks:     []string{execTaskID, "docs/never_executed_task"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "cmp-spec-unexecuted"}}
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if len(updated.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task in comparison status.tasks, got %d: %+v", len(updated.Status.Tasks), updated.Status.Tasks)
		}
		if updated.Status.Tasks[0].ID != execTaskID {
			t.Errorf("Expected task %s, got %s", execTaskID, updated.Status.Tasks[0].ID)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictUnchanged {
			t.Errorf("Expected aggregate verdict %s, got %s", v1alpha1.ComparisonVerdictUnchanged, updated.Status.Verdict)
		}
		if !strings.Contains(updated.Status.Message, "docs/never_executed_task") {
			t.Errorf("Expected status.message to mention docs/never_executed_task, got %q", updated.Status.Message)
		}
	})

	t.Run("no spec.tasks with candidate completed a and b and baseline failed b excludes b and surfaces in message and artifact", func(t *testing.T) {
		taskA := "docs/task_a"
		taskB := "docs/task_b"
		compName := "cmp-mixed-failure"

		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskA+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskA, scores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskA+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskA, scores),
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskB+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskB, scores),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-mixed", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      taskA,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/" + taskA + "/log.json",
					},
					{
						ID:      taskB,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/" + taskB + "/log.json",
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-mixed", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      taskA,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/" + taskA + "/log.json",
					},
					{
						ID:    taskB,
						Phase: v1alpha1.TaskPhaseFailed,
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: compName, Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-mixed"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-mixed"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: compName}}
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if len(updated.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task in comparison status.tasks, got %d: %+v", len(updated.Status.Tasks), updated.Status.Tasks)
		}
		if updated.Status.Tasks[0].ID != taskA {
			t.Errorf("Expected task %s, got %s", taskA, updated.Status.Tasks[0].ID)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictUnchanged {
			t.Errorf("Expected aggregate verdict %s, got %s", v1alpha1.ComparisonVerdictUnchanged, updated.Status.Verdict)
		}
		if !strings.Contains(updated.Status.Message, taskB) {
			t.Errorf("Expected status.message to mention %s, got %q", taskB, updated.Status.Message)
		}

		// Assert stored artifact carries the note in summary
		artObj, ok := fakeGCS.Objects["test-bucket/comparisons/"+compName+".json"]
		if !ok {
			t.Fatalf("Expected comparison artifact at test-bucket/comparisons/%s.json", compName)
		}
		var artifact ComparisonArtifact
		if err := json.Unmarshal(artObj.Data, &artifact); err != nil {
			t.Fatalf("Failed to unmarshal stored comparison artifact: %v", err)
		}
		if !strings.Contains(artifact.Summary, taskB) {
			t.Errorf("Expected artifact.Summary to mention %s, got %q", taskB, artifact.Summary)
		}
		if artifact.Summary != updated.Status.Message {
			t.Errorf("Expected artifact.Summary (%q) to match status.Message (%q)", artifact.Summary, updated.Status.Message)
		}
		if len(artifact.NotExecutedByBothRuns) != 1 || artifact.NotExecutedByBothRuns[0] != taskB {
			t.Errorf("Expected artifact.NotExecutedByBothRuns [%s], got %v", taskB, artifact.NotExecutedByBothRuns)
		}
	})

	t.Run("no common completed tasks yields INCONCLUSIVE and no tasks evaluated message", func(t *testing.T) {
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-disjoint", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task-cand-only",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/task-cand-only/log.json",
					},
					{
						ID:    "task-neither",
						Phase: v1alpha1.TaskPhasePending,
					},
					{
						ID:    "task-failed-both",
						Phase: v1alpha1.TaskPhaseFailed,
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-disjoint", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task-base-only",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/task-base-only/log.json",
					},
					{
						ID:    "task-neither",
						Phase: v1alpha1.TaskPhasePending,
					},
					{
						ID:    "task-failed-both",
						Phase: v1alpha1.TaskPhaseFailed,
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "cmp-no-common-tasks", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-disjoint"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-disjoint"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "cmp-no-common-tasks"}}
		_, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if len(updated.Status.Tasks) != 0 {
			t.Fatalf("Expected 0 tasks in comparison status.tasks, got %d: %+v", len(updated.Status.Tasks), updated.Status.Tasks)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("Expected aggregate verdict %s, got %s", v1alpha1.ComparisonVerdictInconclusive, updated.Status.Verdict)
		}
		if updated.Status.Summary == nil || updated.Status.Summary.Tasks != "0/0" {
			t.Errorf("Expected summary tasks 0/0, got %+v", updated.Status.Summary)
		}
		if !strings.Contains(updated.Status.Message, "no tasks evaluated") {
			t.Errorf("Expected status.message to mention 'no tasks evaluated', got %q", updated.Status.Message)
		}

		artObj, ok := fakeGCS.Objects["test-bucket/comparisons/cmp-no-common-tasks.json"]
		if !ok {
			t.Fatalf("Expected comparison artifact at test-bucket/comparisons/cmp-no-common-tasks.json")
		}
		var artifact ComparisonArtifact
		if err := json.Unmarshal(artObj.Data, &artifact); err != nil {
			t.Fatalf("Failed to unmarshal stored comparison artifact: %v", err)
		}
		if len(artifact.NotExecuted) != 1 || artifact.NotExecuted[0] != "task-neither" {
			t.Errorf("Expected artifact.NotExecuted [task-neither], got %v", artifact.NotExecuted)
		}
		if len(artifact.FailedInBothRuns) != 1 || artifact.FailedInBothRuns[0] != "task-failed-both" {
			t.Errorf("Expected artifact.FailedInBothRuns [task-failed-both], got %v", artifact.FailedInBothRuns)
		}
		expectedNotBoth := []string{"task-base-only", "task-cand-only"}
		if len(artifact.NotExecutedByBothRuns) != 2 ||
			artifact.NotExecutedByBothRuns[0] != expectedNotBoth[0] ||
			artifact.NotExecutedByBothRuns[1] != expectedNotBoth[1] {
			t.Errorf("Expected artifact.NotExecutedByBothRuns %v, got %v", expectedNotBoth, artifact.NotExecutedByBothRuns)
		}
	})
}

func TestComparisonReconciler_TaskFailedPolicy(t *testing.T) {
	scheme := setupScheme(t)

	task1 := "docs/task_one"
	task2 := "docs/task_two"

	scoresEqual := make([]stats.SampleScore, 8)
	for i := 0; i < 8; i++ {
		scoresEqual[i] = stats.SampleScore{ID: fmt.Sprintf("sample-%d", i+1), Score: 0.8}
	}
	scoresLow := make([]stats.SampleScore, 8)
	for i := 0; i < 8; i++ {
		scoresLow[i] = stats.SampleScore{ID: fmt.Sprintf("sample-%d", i+1), Score: 0.0}
	}
	scoresHigh := make([]stats.SampleScore, 8)
	for i := 0; i < 8; i++ {
		scoresHigh[i] = stats.SampleScore{ID: fmt.Sprintf("sample-%d", i+1), Score: 1.0}
	}

	t.Run("candidate task failed while baseline completed caps UNCHANGED verdict to INCONCLUSIVE and names failed task", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		fakeGCS.Objects["test-bucket/runs/cand-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task1, scoresEqual),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task1, scoresEqual),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+task2+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task2, scoresEqual),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-failed", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      task1,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/" + task1 + "/log.json",
					},
					{
						ID:      task2,
						Phase:   v1alpha1.TaskPhaseFailed,
						Message: "exit status 1",
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-failed", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      task1,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/" + task1 + "/log.json",
					},
					{
						ID:      task2,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/" + task2 + "/log.json",
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "cmp-task-failed", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-failed"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-failed"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "cmp-task-failed"}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		// Only task1 was completed by both runs
		if len(updated.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task in comparison status.tasks, got %d", len(updated.Status.Tasks))
		}
		if updated.Status.Tasks[0].ID != task1 {
			t.Errorf("Expected task %s, got %s", task1, updated.Status.Tasks[0].ID)
		}
		if updated.Status.Tasks[0].Label != v1alpha1.ComparisonTaskLabelUnchanged {
			t.Errorf("Expected task label %s, got %s", v1alpha1.ComparisonTaskLabelUnchanged, updated.Status.Tasks[0].Label)
		}

		// Verdict is capped to INCONCLUSIVE because candidate task2 failed while baseline completed
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("Expected aggregate verdict %s, got %s", v1alpha1.ComparisonVerdictInconclusive, updated.Status.Verdict)
		}
		if !strings.Contains(updated.Status.Message, "candidate task failed: docs/task_two") {
			t.Errorf("Expected status.message to mention 'candidate task failed: docs/task_two', got %q", updated.Status.Message)
		}
		if updated.Status.Summary == nil || updated.Status.Summary.Tasks != "1/1" {
			t.Errorf("Expected summary tasks 1/1, got %+v", updated.Status.Summary)
		}
	})

	t.Run("candidate task failed while baseline completed caps IMPROVED verdict to INCONCLUSIVE and caps P(improvement) to 0.50", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		fakeGCS.Objects["test-bucket/runs/cand-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task1, scoresHigh),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task1, scoresLow),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+task2+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task2, scoresEqual),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-imp-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      task1,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/" + task1 + "/log.json",
					},
					{
						ID:      task2,
						Phase:   v1alpha1.TaskPhaseFailed,
						Message: "exit status 1",
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-imp-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      task1,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/" + task1 + "/log.json",
					},
					{
						ID:      task2,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/" + task2 + "/log.json",
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "cmp-task-imp-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-imp-fail"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-imp-fail"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "cmp-task-imp-fail"}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		// task1 is improved
		if len(updated.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task in comparison status.tasks, got %d", len(updated.Status.Tasks))
		}
		if updated.Status.Tasks[0].Label != v1alpha1.ComparisonTaskLabelImproved {
			t.Errorf("Expected task label %s, got %s", v1alpha1.ComparisonTaskLabelImproved, updated.Status.Tasks[0].Label)
		}

		// Verdict is capped to INCONCLUSIVE and PImprovement capped to 0.50
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("Expected aggregate verdict %s, got %s", v1alpha1.ComparisonVerdictInconclusive, updated.Status.Verdict)
		}
		if updated.Status.PImprovement == nil || *updated.Status.PImprovement > 0.50 {
			t.Errorf("Expected P(improvement) <= 0.50, got %v", updated.Status.PImprovement)
		}
		if !strings.Contains(updated.Status.Message, "candidate task failed: docs/task_two") {
			t.Errorf("Expected status.message to mention 'candidate task failed: docs/task_two', got %q", updated.Status.Message)
		}
	})

	t.Run("candidate task failed while another task regressed preserves REGRESSED verdict and names failed task in message", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		fakeGCS.Objects["test-bucket/runs/cand-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task1, scoresLow),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task1, scoresHigh),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+task2+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task2, scoresEqual),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-reg-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      task1,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/" + task1 + "/log.json",
					},
					{
						ID:      task2,
						Phase:   v1alpha1.TaskPhaseFailed,
						Message: "exit status 1",
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-reg-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      task1,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/" + task1 + "/log.json",
					},
					{
						ID:      task2,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/" + task2 + "/log.json",
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "cmp-task-reg-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-reg-fail"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-reg-fail"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "cmp-task-reg-fail"}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if updated.Status.Verdict != v1alpha1.ComparisonVerdictRegressed {
			t.Errorf("Expected aggregate verdict %s, got %s", v1alpha1.ComparisonVerdictRegressed, updated.Status.Verdict)
		}
		if !strings.Contains(updated.Status.Message, "candidate task failed: docs/task_two") {
			t.Errorf("Expected status.message to mention 'candidate task failed: docs/task_two', got %q", updated.Status.Message)
		}
	})

	t.Run("baseline task failed while candidate completed does not cap UNCHANGED verdict and notes task not executed by both runs", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		fakeGCS.Objects["test-bucket/runs/cand-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task1, scoresEqual),
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+task2+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task2, scoresEqual),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task1, scoresEqual),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-base-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      task1,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/" + task1 + "/log.json",
					},
					{
						ID:      task2,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/" + task2 + "/log.json",
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-base-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      task1,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/" + task1 + "/log.json",
					},
					{
						ID:      task2,
						Phase:   v1alpha1.TaskPhaseFailed,
						Message: "exit status 1",
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "cmp-task-base-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-base-fail"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-base-fail"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "cmp-task-base-fail"}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		// Verdict remains UNCHANGED
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictUnchanged {
			t.Errorf("Expected aggregate verdict %s, got %s", v1alpha1.ComparisonVerdictUnchanged, updated.Status.Verdict)
		}
		if !strings.Contains(updated.Status.Message, "tasks not executed by both runs: docs/task_two") {
			t.Errorf("Expected status.message to mention 'tasks not executed by both runs: docs/task_two', got %q", updated.Status.Message)
		}
	})

	t.Run("task failed in both runs does not cap UNCHANGED verdict and notes failed in both runs", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		fakeGCS.Objects["test-bucket/runs/cand-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task1, scoresEqual),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task1, scoresEqual),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-both-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      task1,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/" + task1 + "/log.json",
					},
					{
						ID:      task2,
						Phase:   v1alpha1.TaskPhaseFailed,
						Message: "exit status 1",
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-both-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      task1,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/" + task1 + "/log.json",
					},
					{
						ID:      task2,
						Phase:   v1alpha1.TaskPhaseFailed,
						Message: "exit status 1",
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "cmp-task-both-fail", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-both-fail"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-both-fail"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "cmp-task-both-fail"}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		// Verdict remains UNCHANGED
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictUnchanged {
			t.Errorf("Expected aggregate verdict %s, got %s", v1alpha1.ComparisonVerdictUnchanged, updated.Status.Verdict)
		}
		if !strings.Contains(updated.Status.Message, "failed in both runs: docs/task_two") {
			t.Errorf("Expected status.message to mention 'failed in both runs: docs/task_two', got %q", updated.Status.Message)
		}
		if strings.Contains(updated.Status.Message, "tasks not executed: docs/task_two") {
			t.Errorf("Expected status.message NOT to mention 'tasks not executed: docs/task_two', got %q", updated.Status.Message)
		}

		artObj, ok := fakeGCS.Objects["test-bucket/comparisons/cmp-task-both-fail.json"]
		if !ok {
			t.Fatalf("Expected comparison artifact at test-bucket/comparisons/cmp-task-both-fail.json")
		}
		var artifact ComparisonArtifact
		if err := json.Unmarshal(artObj.Data, &artifact); err != nil {
			t.Fatalf("Failed to unmarshal stored comparison artifact: %v", err)
		}
		if len(artifact.FailedInBothRuns) != 1 || artifact.FailedInBothRuns[0] != task2 {
			t.Errorf("Expected artifact.FailedInBothRuns [%s], got %v", task2, artifact.FailedInBothRuns)
		}
		if len(artifact.NotExecuted) != 0 {
			t.Errorf("Expected artifact.NotExecuted to be empty, got %v", artifact.NotExecuted)
		}
	})

	t.Run("task unattempted in both runs does not cap UNCHANGED verdict and notes tasks not executed", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		fakeGCS.Objects["test-bucket/runs/cand-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task1, scoresEqual),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task1, scoresEqual),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-both-unattempted", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      task1,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/" + task1 + "/log.json",
					},
					{
						ID:    task2,
						Phase: v1alpha1.TaskPhasePending,
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-both-unattempted", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      task1,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/" + task1 + "/log.json",
					},
					{
						ID:    task2,
						Phase: v1alpha1.TaskPhasePending,
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "cmp-task-both-unattempted", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-both-unattempted"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-both-unattempted"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "cmp-task-both-unattempted"}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile error: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		// Verdict remains UNCHANGED
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictUnchanged {
			t.Errorf("Expected aggregate verdict %s, got %s", v1alpha1.ComparisonVerdictUnchanged, updated.Status.Verdict)
		}
		if !strings.Contains(updated.Status.Message, "tasks not executed: docs/task_two") {
			t.Errorf("Expected status.message to mention 'tasks not executed: docs/task_two', got %q", updated.Status.Message)
		}
		if strings.Contains(updated.Status.Message, "failed in both runs:") {
			t.Errorf("Expected status.message NOT to mention 'failed in both runs:', got %q", updated.Status.Message)
		}

		artObj, ok := fakeGCS.Objects["test-bucket/comparisons/cmp-task-both-unattempted.json"]
		if !ok {
			t.Fatalf("Expected comparison artifact at test-bucket/comparisons/cmp-task-both-unattempted.json")
		}
		var artifact ComparisonArtifact
		if err := json.Unmarshal(artObj.Data, &artifact); err != nil {
			t.Fatalf("Failed to unmarshal stored comparison artifact: %v", err)
		}
		if len(artifact.NotExecuted) != 1 || artifact.NotExecuted[0] != task2 {
			t.Errorf("Expected artifact.NotExecuted [%s], got %v", task2, artifact.NotExecuted)
		}
		if len(artifact.FailedInBothRuns) != 0 {
			t.Errorf("Expected artifact.FailedInBothRuns to be empty, got %v", artifact.FailedInBothRuns)
		}
	})
}

func TestComparisonReconciler_PairedBatchLoop(t *testing.T) {
	scheme := setupScheme(t)

	t.Run("ClearImprovement_DecidesAfterBatch1_NoFurtherBatch", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskID := "docs/readme_comprehension"
		var candScores, baseScores []stats.SampleScore
		for i := 1; i <= 8; i++ {
			candScores = append(candScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
			baseScores = append(baseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, candScores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, baseScores),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "cand-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskID + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "base-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-batch-improved", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed, got %s", updated.Status.Phase)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictImproved {
			t.Errorf("expected verdict IMPROVED, got %s", updated.Status.Verdict)
		}
		if len(updated.Status.Batches) != 1 {
			t.Fatalf("expected 1 batch, got %d", len(updated.Status.Batches))
		}
		if updated.Status.Tasks[0].SamplesRun != 8 {
			t.Errorf("expected samplesRun 8, got %d", updated.Status.Tasks[0].SamplesRun)
		}
		if updated.Status.Tasks[0].Decision != v1alpha1.ComparisonTaskLabelImproved {
			t.Errorf("expected decision improved, got %s", updated.Status.Tasks[0].Decision)
		}

		// Ensure no batch 2 Run was created
		var b2Cand v1alpha1.Run
		err = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-batch-improved-b2-cand"}, &b2Cand)
		if err == nil {
			t.Errorf("expected no batch 2 Run to be created, but found one")
		}
	})

	t.Run("IdenticalScores_ReachesUnchangedWithinMargin", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskID := "docs/readme_comprehension"
		var b1Scores []stats.SampleScore
		for i := 1; i <= 4; i++ {
			b1Scores = append(b1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, b1Scores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, b1Scores),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-unchanged", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "cand-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskID + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-unchanged", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "base-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-batch-unchanged", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-unchanged"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-unchanged"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// Reconcile 1: should notice continue and create batch 2
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if updated.Status.Phase != v1alpha1.ComparisonPhaseRunning {
			t.Errorf("expected phase Running after batch 1, got %s", updated.Status.Phase)
		}
		if len(updated.Status.Batches) != 2 {
			t.Fatalf("expected 2 batches recorded, got %d", len(updated.Status.Batches))
		}
		if updated.Status.Batches[1].SampleRange.Start != 5 || updated.Status.Batches[1].SampleRange.End != 8 {
			t.Errorf("expected batch 2 range [5, 8], got %v", updated.Status.Batches[1].SampleRange)
		}

		// Verify batch 2 Runs exist
		var b2Cand, b2Base v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-batch-unchanged-b2-cand"}, &b2Cand); err != nil {
			t.Fatalf("expected batch 2 candidate Run to exist: %v", err)
		}
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-batch-unchanged-b2-base"}, &b2Base); err != nil {
			t.Fatalf("expected batch 2 baseline Run to exist: %v", err)
		}

		// Provide logs for batch 2 (samples 5..8, also identical 0.8) and mark completed
		var b2Scores []stats.SampleScore
		for i := 5; i <= 8; i++ {
			b2Scores = append(b2Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log2.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, b2Scores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log2.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, b2Scores),
		}

		b2Cand.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Cand.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskID,
				Phase:       v1alpha1.TaskPhaseCompleted,
				LogPath:     "gs://test-bucket/runs/cand-commit/" + taskID + "/log2.json",
				Samples:     4,
				SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8},
			},
		}
		if err := cl.Status().Update(t.Context(), &b2Cand); err != nil {
			t.Fatalf("Failed to update b2Cand status: %v", err)
		}

		b2Base.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Base.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskID,
				Phase:       v1alpha1.TaskPhaseCompleted,
				LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/log2.json",
				Samples:     4,
				SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8},
			},
		}
		if err := cl.Status().Update(t.Context(), &b2Base); err != nil {
			t.Fatalf("Failed to update b2Base status: %v", err)
		}

		// Reconcile 2: now n=8 identical scores => reaches unchanged
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}

		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed after batch 2, got %s", updated.Status.Phase)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictUnchanged {
			t.Errorf("expected verdict UNCHANGED, got %s", updated.Status.Verdict)
		}
		if updated.Status.Tasks[0].SamplesRun != 8 {
			t.Errorf("expected samplesRun 8, got %d", updated.Status.Tasks[0].SamplesRun)
		}
		if updated.Status.Tasks[0].Decision != v1alpha1.ComparisonTaskLabelUnchanged {
			t.Errorf("expected decision unchanged, got %s", updated.Status.Tasks[0].Decision)
		}
		if len(updated.Status.Batches) != 2 {
			t.Errorf("expected 2 batches, got %d", len(updated.Status.Batches))
		}
	})

	t.Run("NoisyScores_HitsMaxSamples_Inconclusive", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskID := "docs/readme_comprehension"
		// Batch 1: 4 noisy samples
		candB1 := []stats.SampleScore{
			{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0},
		}
		baseB1 := []stats.SampleScore{
			{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 0.0}, {ID: "4", Score: 1.0},
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, candB1),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, baseB1),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-noisy", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "cand-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskID + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-noisy", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "base-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-noisy", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-noisy"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-noisy"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 8, // Cap at 8
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// Reconcile 1: creates batch 2
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		// Provide noisy scores for batch 2
		candB2 := []stats.SampleScore{
			{ID: "5", Score: 0.0}, {ID: "6", Score: 1.0}, {ID: "7", Score: 0.0}, {ID: "8", Score: 1.0},
		}
		baseB2 := []stats.SampleScore{
			{ID: "5", Score: 1.0}, {ID: "6", Score: 0.0}, {ID: "7", Score: 1.0}, {ID: "8", Score: 0.0},
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log2.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, candB2),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log2.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, baseB2),
		}

		var b2Cand, b2Base v1alpha1.Run
		_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-noisy-b2-cand"}, &b2Cand)
		_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-noisy-b2-base"}, &b2Base)
		b2Cand.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Cand.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskID,
				Phase:       v1alpha1.TaskPhaseCompleted,
				LogPath:     "gs://test-bucket/runs/cand-commit/" + taskID + "/log2.json",
				Samples:     4,
				SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8},
			},
		}
		_ = cl.Status().Update(t.Context(), &b2Cand)
		b2Base.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Base.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskID,
				Phase:       v1alpha1.TaskPhaseCompleted,
				LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/log2.json",
				Samples:     4,
				SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8},
			},
		}
		_ = cl.Status().Update(t.Context(), &b2Base)

		// Reconcile 2: cap of 8 is hit, scores noisy => inconclusive
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed after hitting maxSamplesPerTask, got %s", updated.Status.Phase)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("expected verdict INCONCLUSIVE, got %s", updated.Status.Verdict)
		}
		if updated.Status.Tasks[0].SamplesRun != 8 {
			t.Errorf("expected samplesRun 8, got %d", updated.Status.Tasks[0].SamplesRun)
		}
		if updated.Status.Tasks[0].Decision != v1alpha1.ComparisonTaskLabelInconclusive {
			t.Errorf("expected decision inconclusive, got %s", updated.Status.Tasks[0].Decision)
		}
	})

	t.Run("ExhaustedDataset_Stops", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskID := "docs/readme_comprehension"
		// Window requests 8 samples [1, 8], but dataset only has 3 samples
		candScores := []stats.SampleScore{
			{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0},
		}
		baseScores := []stats.SampleScore{
			{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 1.0},
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, candScores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, baseScores),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-exhausted", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "cand-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskID + "/log.json",
						Samples:     3, // fewer than window (8)
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-exhausted", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "base-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/log.json",
						Samples:     3, // fewer than window (8)
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-exhausted", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-exhausted"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-exhausted"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		// Dataset exhausted: decided with what exists => inconclusive, and stopped (no batch 2)
		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed due to dataset exhaustion, got %s", updated.Status.Phase)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("expected verdict INCONCLUSIVE, got %s", updated.Status.Verdict)
		}
		if len(updated.Status.Batches) != 1 {
			t.Errorf("expected exactly 1 batch, got %d", len(updated.Status.Batches))
		}
	})

	t.Run("Reconcile_IsIdempotent", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskID := "docs/readme_comprehension"
		var b1Scores []stats.SampleScore
		for i := 1; i <= 4; i++ {
			b1Scores = append(b1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, b1Scores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, b1Scores),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-idem", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "cand-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskID + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-idem", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "base-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-idem", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-idem"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-idem"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 16,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// Reconcile 1
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		// Reconcile 2 without changing state (batch 2 still running)
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}

		// Reconcile 3
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 3 failed: %v", err)
		}

		var runList v1alpha1.RunList
		if err := cl.List(t.Context(), &runList); err != nil {
			t.Fatalf("Failed to list runs: %v", err)
		}

		// Initial 2 runs + batch 2 pair (2 runs) = 4 runs total. No duplicate batch Runs!
		if len(runList.Items) != 4 {
			t.Errorf("expected 4 Runs total across repeated reconciles, found %d", len(runList.Items))
		}
	})

	t.Run("MixedRun_OneStopsEarlyOneContinues", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskA := "docs/task_a"
		taskB := "docs/task_b"

		// Task A: 8 samples with clear improvement (cand 1.0, base 0.0) -> stops as improved
		var taskAcand, taskAbase []stats.SampleScore
		for i := 1; i <= 8; i++ {
			taskAcand = append(taskAcand, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
			taskAbase = append(taskAbase, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskA+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskA, taskAcand),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskA+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskA, taskAbase),
		}

		// Task B: 8 samples with noisy differences -> continues
		taskBcand := []stats.SampleScore{
			{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0},
			{ID: "5", Score: 1.0}, {ID: "6", Score: 0.0}, {ID: "7", Score: 1.0}, {ID: "8", Score: 0.0},
		}
		taskBbase := []stats.SampleScore{
			{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 0.0}, {ID: "4", Score: 1.0},
			{ID: "5", Score: 0.0}, {ID: "6", Score: 1.0}, {ID: "7", Score: 0.0}, {ID: "8", Score: 1.0},
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskB+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskB, taskBcand),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskB+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskB, taskBbase),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-mixed", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "cand-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
				Tasks:       []string{taskA, taskB},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskA,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskA + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
					{
						ID:          taskB,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskB + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-mixed", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "base-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
				Tasks:       []string{taskA, taskB},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskA,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskA + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
					{
						ID:          taskB,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskB + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-mixed", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-mixed"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-mixed"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 16,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// Reconcile 1: Task A decides improved; Task B continues
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		// Verify batch 2 Runs exist and contain ONLY taskB in Spec.Tasks!
		var b2Cand, b2Base v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-mixed-b2-cand"}, &b2Cand); err != nil {
			t.Fatalf("expected batch 2 candidate Run: %v", err)
		}
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-mixed-b2-base"}, &b2Base); err != nil {
			t.Fatalf("expected batch 2 baseline Run: %v", err)
		}

		if len(b2Cand.Spec.Tasks) != 1 || b2Cand.Spec.Tasks[0] != taskB {
			t.Errorf("expected batch 2 candidate to only evaluate [%s], got %v", taskB, b2Cand.Spec.Tasks)
		}
		if len(b2Base.Spec.Tasks) != 1 || b2Base.Spec.Tasks[0] != taskB {
			t.Errorf("expected batch 2 baseline to only evaluate [%s], got %v", taskB, b2Base.Spec.Tasks)
		}

		// Provide batch 2 logs for Task B (samples 9..16 with candidate=1.0, baseline=0.0) -> Task B reaches cap
		var b2CandScores, b2BaseScores []stats.SampleScore
		for i := 9; i <= 16; i++ {
			b2CandScores = append(b2CandScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
			b2BaseScores = append(b2BaseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskB+"/log2.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskB, b2CandScores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskB+"/log2.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskB, b2BaseScores),
		}

		b2Cand.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Cand.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskB,
				Phase:       v1alpha1.TaskPhaseCompleted,
				LogPath:     "gs://test-bucket/runs/cand-commit/" + taskB + "/log2.json",
				Samples:     8,
				SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
			},
		}
		_ = cl.Status().Update(t.Context(), &b2Cand)

		b2Base.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Base.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskB,
				Phase:       v1alpha1.TaskPhaseCompleted,
				LogPath:     "gs://test-bucket/runs/base-commit/" + taskB + "/log2.json",
				Samples:     8,
				SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
			},
		}
		_ = cl.Status().Update(t.Context(), &b2Base)

		// Reconcile 2: Task B is now evaluated across batch 1 & 2; both tasks are decided!
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed after both tasks decided, got %s", updated.Status.Phase)
		}
		if len(updated.Status.Tasks) != 2 {
			t.Fatalf("expected 2 tasks in status, got %d", len(updated.Status.Tasks))
		}

		taskAStatus := findComparisonTaskStatus(updated.Status.Tasks, taskA)
		taskBStatus := findComparisonTaskStatus(updated.Status.Tasks, taskB)
		if taskAStatus == nil || taskBStatus == nil {
			t.Fatalf("missing task in comparison status")
		}

		if taskAStatus.Decision != v1alpha1.ComparisonTaskLabelImproved {
			t.Errorf("expected taskA decision improved, got %s", taskAStatus.Decision)
		}
		if taskAStatus.SamplesRun != 8 {
			t.Errorf("expected taskA samplesRun 8, got %d", taskAStatus.SamplesRun)
		}
		if taskBStatus.SamplesRun != 16 {
			t.Errorf("expected taskB samplesRun 16, got %d", taskBStatus.SamplesRun)
		}
	})

	t.Run("DecidedLabelsSurviveNextBatchSchedulingAndRunning", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskA := "docs/task_a"
		taskB := "docs/task_b"

		// Task A: 8 samples with candidate=1.0, baseline=0.0 -> decided improved in batch 1
		var taskAcand, taskAbase []stats.SampleScore
		for i := 1; i <= 8; i++ {
			taskAcand = append(taskAcand, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
			taskAbase = append(taskAbase, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskA+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskA, taskAcand),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskA+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskA, taskAbase),
		}

		// Task B: 8 samples with alternating scores -> continues in batch 2
		taskBcand := []stats.SampleScore{
			{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0},
			{ID: "5", Score: 1.0}, {ID: "6", Score: 0.0}, {ID: "7", Score: 1.0}, {ID: "8", Score: 0.0},
		}
		taskBbase := []stats.SampleScore{
			{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 0.0}, {ID: "4", Score: 1.0},
			{ID: "5", Score: 0.0}, {ID: "6", Score: 1.0}, {ID: "7", Score: 0.0}, {ID: "8", Score: 1.0},
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskB+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskB, taskBcand),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskB+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskB, taskBbase),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-survive", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "cand-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
				Tasks:       []string{taskA, taskB},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskA,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskA + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
					{
						ID:          taskB,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskB + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-survive", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "base-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
				Tasks:       []string{taskA, taskB},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskA,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskA + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
					{
						ID:          taskB,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskB + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-survive", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-survive"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-survive"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 16,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// Reconcile 1: Batch 1 completes. Task A decides improved; Task B continues; batch 2 is created.
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		var compPostB1 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &compPostB1); err != nil {
			t.Fatalf("failed to get comparison after reconcile 1: %v", err)
		}
		if compPostB1.Status.Summary == nil || compPostB1.Status.Summary.Tasks != "1/2" {
			t.Errorf("expected summary.tasks 1/2 after batch 1, got %+v", compPostB1.Status.Summary)
		}
		taskAPost1 := findComparisonTaskStatus(compPostB1.Status.Tasks, taskA)
		if taskAPost1 == nil || taskAPost1.Label != v1alpha1.ComparisonTaskLabelImproved {
			t.Fatalf("expected taskA label improved after batch 1, got %+v", taskAPost1)
		}

		// Now simulate batch 2 Run actively running (not terminal)
		var b2Cand v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-survive-b2-cand"}, &b2Cand); err != nil {
			t.Fatalf("expected batch 2 candidate Run: %v", err)
		}
		b2Cand.Status.Phase = v1alpha1.RunPhaseRunning
		if err := cl.Status().Update(t.Context(), &b2Cand); err != nil {
			t.Fatalf("failed to update b2Cand status: %v", err)
		}

		// Reconcile 2: triggered while batch 2 is executing!
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}

		var compDuringB2 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &compDuringB2); err != nil {
			t.Fatalf("failed to get comparison during batch 2: %v", err)
		}

		// Comparison is Running overall
		if compDuringB2.Status.Phase != v1alpha1.ComparisonPhaseRunning {
			t.Errorf("expected phase Running during batch 2, got %s", compDuringB2.Status.Phase)
		}
		if compDuringB2.Status.Verdict != v1alpha1.ComparisonVerdictRunning {
			t.Errorf("expected verdict Running during batch 2, got %s", compDuringB2.Status.Verdict)
		}

		// Task A's decided label MUST SURVIVE as 'improved', not reset to 'running'!
		taskADuring := findComparisonTaskStatus(compDuringB2.Status.Tasks, taskA)
		if taskADuring == nil {
			t.Fatalf("taskA missing in status during batch 2")
		}
		if taskADuring.Label != v1alpha1.ComparisonTaskLabelImproved {
			t.Errorf("expected taskA label to remain 'improved' while batch 2 runs, got %q", taskADuring.Label)
		}
		if taskADuring.Decision != v1alpha1.ComparisonTaskLabelImproved {
			t.Errorf("expected taskA decision to remain 'improved', got %q", taskADuring.Decision)
		}

		// Task B is continuing in batch 2, so its label is 'running'
		taskBDuring := findComparisonTaskStatus(compDuringB2.Status.Tasks, taskB)
		if taskBDuring == nil {
			t.Fatalf("taskB missing in status during batch 2")
		}
		if taskBDuring.Label != v1alpha1.ComparisonTaskLabelRunning {
			t.Errorf("expected taskB label to be 'running', got %q", taskBDuring.Label)
		}

		// summary.tasks must count the decided task (1/2), not reset to 0/2!
		if compDuringB2.Status.Summary == nil || compDuringB2.Status.Summary.Tasks != "1/2" {
			t.Errorf("expected summary.tasks to be '1/2' during batch 2, got %+v", compDuringB2.Status.Summary)
		}

		// message should show batch 2 running info, not generic "Runs are still executing"
		expectedMsg := "Running batch 2 (1 tasks continuing)"
		if compDuringB2.Status.Message != expectedMsg {
			t.Errorf("expected status.message %q, got %q", expectedMsg, compDuringB2.Status.Message)
		}
	})

	// Blocking (2) test: Seeds 1-8, cap 8, one unmatched id (7 pairs). Window must clamp by remaining samples, not position, and stop as inconclusive if no window can be issued.
	t.Run("WindowClampingByRemainingSamples_DoesNotHangInRunning", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskID := "docs/readme_comprehension"
		// Candidate has 8 samples (1..7 and cand-8), baseline has 8 samples (1..7 and base-8) -> 7 matching pairs
		var candScores, baseScores []stats.SampleScore
		for i := 1; i <= 7; i++ {
			candScores = append(candScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
			baseScores = append(baseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
		}
		candScores = append(candScores, stats.SampleScore{ID: "cand-8", Score: 0.8})
		baseScores = append(baseScores, stats.SampleScore{ID: "base-8", Score: 0.8})

		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, candScores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, baseScores),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-clamp", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "cand-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskID + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-clamp", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "base-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		// Cap = 8, batchSize = 4. With 7 pairs, remaining is 1. Next batch should be window [9, 9] (size 1)
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-clamp", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-clamp"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-clamp"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 8,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if len(updated.Status.Batches) != 2 {
			t.Fatalf("expected 2 batches, got %d", len(updated.Status.Batches))
		}
		// Window was clamped to remaining (1): [9, 9]
		b2Range := updated.Status.Batches[1].SampleRange
		if b2Range.Start != 9 || b2Range.End != 9 {
			t.Errorf("expected batch 2 range [9, 9], got [%d, %d]", b2Range.Start, b2Range.End)
		}

		// Provide sample 9 and complete batch 2
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log2.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, []stats.SampleScore{{ID: "9", Score: 0.8}}),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log2.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, []stats.SampleScore{{ID: "9", Score: 0.8}}),
		}

		var b2Cand, b2Base v1alpha1.Run
		_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-clamp-b2-cand"}, &b2Cand)
		_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-clamp-b2-base"}, &b2Base)
		b2Cand.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Cand.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskID,
				Phase:       v1alpha1.TaskPhaseCompleted,
				LogPath:     "gs://test-bucket/runs/cand-commit/" + taskID + "/log2.json",
				Samples:     1,
				SampleRange: &v1alpha1.SampleRange{Start: 9, End: 9},
			},
		}
		_ = cl.Status().Update(t.Context(), &b2Cand)
		b2Base.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Base.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskID,
				Phase:       v1alpha1.TaskPhaseCompleted,
				LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/log2.json",
				Samples:     1,
				SampleRange: &v1alpha1.SampleRange{Start: 9, End: 9},
			},
		}
		_ = cl.Status().Update(t.Context(), &b2Base)

		// Reconcile 2: now at cap 8
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}

		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed, got %s", updated.Status.Phase)
		}
	})

	// Blocking (3) test: A deleted/NotFound batch Run should be treated as Failed terminal, not hang in Running.
	t.Run("DisappearedBatchRun_TreatedAsFailedTerminal_DoesNotHang", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskID := "docs/readme_comprehension"
		var b1Scores []stats.SampleScore
		for i := 1; i <= 4; i++ {
			b1Scores = append(b1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, b1Scores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, b1Scores),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-disappear", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "cand-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskID + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-disappear", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "base-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-disappear", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-disappear"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-disappear"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 16,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// Reconcile 1 creates batch 2 Runs
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		// Delete batch 2 baseline Run so it returns NotFound
		var b2Base v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-disappear-b2-base"}, &b2Base); err != nil {
			t.Fatalf("failed to find b2Base: %v", err)
		}
		if err := cl.Delete(t.Context(), &b2Base); err != nil {
			t.Fatalf("failed to delete b2Base: %v", err)
		}

		// Reconcile 2 should treat NotFound as Failed/terminal batch and complete
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed despite disappeared batch Run, got %s", updated.Status.Phase)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("expected verdict INCONCLUSIVE, got %s", updated.Status.Verdict)
		}
	})

	// Blocking (4) test: Both batch Runs fail with no status.tasks (e.g. clone/discovery failure). Must not loop.
	t.Run("BatchRunFailedBeforeTaskStatus_StopsWithoutLoopingToCap", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskID := "docs/readme_comprehension"
		var b1Scores []stats.SampleScore
		for i := 1; i <= 4; i++ {
			b1Scores = append(b1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, b1Scores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, b1Scores),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-failfast", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "cand-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskID + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-failfast", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "base-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-failfast", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-failfast"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-failfast"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 64, // Large cap
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// Reconcile 1 creates batch 2 Runs
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		// Both batch 2 runs fail without status.tasks (e.g. image pull failure)
		var b2Cand, b2Base v1alpha1.Run
		_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-failfast-b2-cand"}, &b2Cand)
		_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-failfast-b2-base"}, &b2Base)
		b2Cand.Status.Phase = v1alpha1.RunPhaseFailed
		b2Cand.Status.Tasks = nil
		_ = cl.Status().Update(t.Context(), &b2Cand)
		b2Base.Status.Phase = v1alpha1.RunPhaseFailed
		b2Base.Status.Tasks = nil
		_ = cl.Status().Update(t.Context(), &b2Base)

		// Reconcile 2 should stop and NOT create batch 3
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed after batch failure, got %s", updated.Status.Phase)
		}

		// Check that batch 3 was NOT created
		var b3Cand v1alpha1.Run
		err = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-failfast-b3-cand"}, &b3Cand)
		if err == nil {
			t.Errorf("expected no batch 3 Run, but found one")
		}
	})

	// Blocking (5) test: Unwindowed seeds run the whole dataset, must not issue batch 2 past the end.
	t.Run("UnwindowedSeeds_TreatedAsExhausted_NoBatch2Issued", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskID := "docs/readme_comprehension"
		var scores []stats.SampleScore
		for i := 1; i <= 20; i++ {
			scores = append(scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, scores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskID, scores),
		}

		// Neither run has SampleRange nor Limit
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-unwindowed", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{Commit: "cand-commit"},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      taskID,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/cand-commit/" + taskID + "/log.json",
						Samples: 20,
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-unwindowed", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{Commit: "base-commit"},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      taskID,
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "gs://test-bucket/runs/base-commit/" + taskID + "/log.json",
						Samples: 20,
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-unwindowed", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-unwindowed"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-unwindowed"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 64,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed, got %s", updated.Status.Phase)
		}
		if len(updated.Status.Batches) != 1 {
			t.Errorf("expected only 1 batch, got %d", len(updated.Status.Batches))
		}

		// Ensure batch 2 was NOT created
		var b2Cand v1alpha1.Run
		err = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-unwindowed-b2-cand"}, &b2Cand)
		if err == nil {
			t.Errorf("expected no batch 2 Run to be created past end of dataset, but found one")
		}
	})
	// #141 interaction: a candidate-side task failure in the seed Runs caps the terminal verdict
	// to INCONCLUSIVE, and the failed task is never scheduled into later batches.
	t.Run("CandidateSeedTaskFailed_CapsVerdictInconclusive_NeverSchedulesFailedTask", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskOK := "docs/task_one"
		taskFailed := "docs/task_two"

		// taskOK: 4 identical samples -> continue after batch 1, UNCHANGED after batch 2
		var b1Scores []stats.SampleScore
		for i := 1; i <= 4; i++ {
			b1Scores = append(b1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskOK+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskOK, b1Scores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskOK+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskOK, b1Scores),
		}
		// taskFailed: baseline completed, candidate failed (no log)
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskFailed+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskFailed, b1Scores),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-seed-failed", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "cand-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
				Tasks:       []string{taskOK, taskFailed},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskOK,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskOK + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
					{
						ID:      taskFailed,
						Phase:   v1alpha1.TaskPhaseFailed,
						Message: "exit status 1",
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-seed-failed", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "base-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
				Tasks:       []string{taskOK, taskFailed},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskOK,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskOK + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
					{
						ID:          taskFailed,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskFailed + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-seed-failed", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-seed-failed"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-seed-failed"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// Reconcile 1: taskOK continues into batch 2; taskFailed must not be scheduled
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}
		if updated.Status.Phase != v1alpha1.ComparisonPhaseRunning {
			t.Errorf("expected phase Running after batch 1, got %s", updated.Status.Phase)
		}
		if len(updated.Status.Batches) != 2 {
			t.Fatalf("expected 2 batches recorded, got %d", len(updated.Status.Batches))
		}

		var b2Cand, b2Base v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-seed-failed-b2-cand"}, &b2Cand); err != nil {
			t.Fatalf("expected batch 2 candidate Run to exist: %v", err)
		}
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-seed-failed-b2-base"}, &b2Base); err != nil {
			t.Fatalf("expected batch 2 baseline Run to exist: %v", err)
		}
		if len(b2Cand.Spec.Tasks) != 1 || b2Cand.Spec.Tasks[0] != taskOK {
			t.Errorf("expected batch 2 candidate to only evaluate [%s], got %v", taskOK, b2Cand.Spec.Tasks)
		}
		if len(b2Base.Spec.Tasks) != 1 || b2Base.Spec.Tasks[0] != taskOK {
			t.Errorf("expected batch 2 baseline to only evaluate [%s], got %v", taskOK, b2Base.Spec.Tasks)
		}

		// Complete batch 2 for taskOK with identical scores (samples 5..8)
		var b2Scores []stats.SampleScore
		for i := 5; i <= 8; i++ {
			b2Scores = append(b2Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskOK+"/log2.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskOK, b2Scores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskOK+"/log2.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskOK, b2Scores),
		}

		b2Cand.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Cand.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskOK,
				Phase:       v1alpha1.TaskPhaseCompleted,
				LogPath:     "gs://test-bucket/runs/cand-commit/" + taskOK + "/log2.json",
				Samples:     4,
				SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8},
			},
		}
		if err := cl.Status().Update(t.Context(), &b2Cand); err != nil {
			t.Fatalf("Failed to update b2Cand status: %v", err)
		}
		b2Base.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Base.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskOK,
				Phase:       v1alpha1.TaskPhaseCompleted,
				LogPath:     "gs://test-bucket/runs/base-commit/" + taskOK + "/log2.json",
				Samples:     4,
				SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8},
			},
		}
		if err := cl.Status().Update(t.Context(), &b2Base); err != nil {
			t.Fatalf("Failed to update b2Base status: %v", err)
		}

		// Reconcile 2: taskOK is UNCHANGED, but the seed candidate failure caps the verdict
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed after batch 2, got %s", updated.Status.Phase)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("expected capped verdict %s, got %s", v1alpha1.ComparisonVerdictInconclusive, updated.Status.Verdict)
		}
		if !strings.Contains(updated.Status.Message, "INCONCLUSIVE: candidate task failed: "+taskFailed+"; otherwise UNCHANGED") {
			t.Errorf("expected capped message naming %s, got %q", taskFailed, updated.Status.Message)
		}
		if updated.Status.PImprovement == nil || *updated.Status.PImprovement > 0.50 {
			t.Errorf("expected P(improvement) capped at 0.50, got %v", updated.Status.PImprovement)
		}
		if len(updated.Status.Tasks) != 1 || updated.Status.Tasks[0].ID != taskOK {
			t.Fatalf("expected only %s in status.tasks, got %+v", taskOK, updated.Status.Tasks)
		}
		if updated.Status.Tasks[0].Decision != v1alpha1.ComparisonTaskLabelUnchanged {
			t.Errorf("expected %s decision unchanged, got %s", taskOK, updated.Status.Tasks[0].Decision)
		}
		if updated.Status.Tasks[0].SamplesRun != 8 {
			t.Errorf("expected samplesRun 8, got %d", updated.Status.Tasks[0].SamplesRun)
		}
		if updated.Status.Summary == nil || updated.Status.Summary.Tasks != "1/1" {
			t.Errorf("expected summary tasks 1/1, got %+v", updated.Status.Summary)
		}
		if len(updated.Status.Batches) != 2 {
			t.Errorf("expected 2 batches, got %d", len(updated.Status.Batches))
		}
		var b3Cand v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-seed-failed-b3-cand"}, &b3Cand); err == nil {
			t.Errorf("expected no batch 3 Run to be created, but found one")
		}
	})

	// #141 interaction: a candidate-side task failure in a later batch pair is reported exactly as a
	// seed failure would be. The failed task is necessarily inconclusive (no new scores), so the
	// aggregate is already INCONCLUSIVE and main's policy appends the "candidate task failed" note.
	t.Run("CandidateBatch2TaskFailed_NamesFailedTaskInMessage", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskA := "docs/task_a"
		taskB := "docs/task_b"

		// Task A: clear improvement -> IMPROVED after batch 1
		var taskAcand, taskAbase []stats.SampleScore
		for i := 1; i <= 8; i++ {
			taskAcand = append(taskAcand, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
			taskAbase = append(taskAbase, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskA+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskA, taskAcand),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskA+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskA, taskAbase),
		}

		// Task B: noisy -> continues into batch 2
		taskBcand := []stats.SampleScore{
			{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0},
			{ID: "5", Score: 1.0}, {ID: "6", Score: 0.0}, {ID: "7", Score: 1.0}, {ID: "8", Score: 0.0},
		}
		taskBbase := []stats.SampleScore{
			{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 0.0}, {ID: "4", Score: 1.0},
			{ID: "5", Score: 0.0}, {ID: "6", Score: 1.0}, {ID: "7", Score: 0.0}, {ID: "8", Score: 1.0},
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskB+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskB, taskBcand),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskB+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskB, taskBbase),
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-b2-failed", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "cand-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
				Tasks:       []string{taskA, taskB},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskA,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskA + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
					{
						ID:          taskB,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskB + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-b2-failed", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{Commit: "base-commit"},
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
				Tasks:       []string{taskA, taskB},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskA,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskA + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
					{
						ID:          taskB,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskB + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-b2-failed", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-b2-failed"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-b2-failed"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		var b2Cand, b2Base v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-b2-failed-b2-cand"}, &b2Cand); err != nil {
			t.Fatalf("expected batch 2 candidate Run: %v", err)
		}
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-b2-failed-b2-base"}, &b2Base); err != nil {
			t.Fatalf("expected batch 2 baseline Run: %v", err)
		}
		if len(b2Cand.Spec.Tasks) != 1 || b2Cand.Spec.Tasks[0] != taskB {
			t.Fatalf("expected batch 2 candidate to only evaluate [%s], got %v", taskB, b2Cand.Spec.Tasks)
		}

		// Batch 2: baseline completes taskB, candidate fails taskB
		var b2BaseScores []stats.SampleScore
		for i := 9; i <= 16; i++ {
			b2BaseScores = append(b2BaseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskB+"/log2.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(taskB, b2BaseScores),
		}

		b2Cand.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Cand.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:      taskB,
				Phase:   v1alpha1.TaskPhaseFailed,
				Message: "exit status 1",
			},
		}
		if err := cl.Status().Update(t.Context(), &b2Cand); err != nil {
			t.Fatalf("Failed to update b2Cand status: %v", err)
		}
		b2Base.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Base.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskB,
				Phase:       v1alpha1.TaskPhaseCompleted,
				LogPath:     "gs://test-bucket/runs/base-commit/" + taskB + "/log2.json",
				Samples:     8,
				SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
			},
		}
		if err := cl.Status().Update(t.Context(), &b2Base); err != nil {
			t.Fatalf("Failed to update b2Base status: %v", err)
		}

		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}

		var updated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
			t.Fatalf("Failed to get updated comparison: %v", err)
		}

		if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed, got %s", updated.Status.Phase)
		}
		if updated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("expected verdict %s, got %s", v1alpha1.ComparisonVerdictInconclusive, updated.Status.Verdict)
		}
		if !strings.Contains(updated.Status.Message, "candidate task failed: "+taskB) {
			t.Errorf("expected message to name failed candidate task %s, got %q", taskB, updated.Status.Message)
		}
		taskAStatus := findComparisonTaskStatus(updated.Status.Tasks, taskA)
		taskBStatus := findComparisonTaskStatus(updated.Status.Tasks, taskB)
		if taskAStatus == nil || taskBStatus == nil {
			t.Fatalf("missing task in comparison status: %+v", updated.Status.Tasks)
		}
		if taskAStatus.Decision != v1alpha1.ComparisonTaskLabelImproved {
			t.Errorf("expected taskA decision improved, got %s", taskAStatus.Decision)
		}
		if taskBStatus.Decision != v1alpha1.ComparisonTaskLabelInconclusive {
			t.Errorf("expected taskB decision inconclusive, got %s", taskBStatus.Decision)
		}
		if len(updated.Status.Batches) != 2 {
			t.Errorf("expected 2 batches, got %d", len(updated.Status.Batches))
		}
	})

}

func makeInspectLogWithTokens(taskID string, sampleScores []stats.SampleScore, inputTokens, outputTokens int) []byte {
	type sample struct {
		ID     any            `json:"id"`
		Scores map[string]any `json:"scores"`
	}
	type modelUsage struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
		TotalTokens  int `json:"total_tokens"`
	}
	type inspectStats struct {
		ModelUsage map[string]modelUsage `json:"model_usage"`
	}
	type inspectLog struct {
		Version int          `json:"version"`
		Status  string       `json:"status"`
		Eval    any          `json:"eval"`
		Stats   inspectStats `json:"stats"`
		Samples []sample     `json:"samples"`
	}

	var samples []sample
	for _, s := range sampleScores {
		samples = append(samples, sample{
			ID: s.ID,
			Scores: map[string]any{
				"choice": map[string]any{
					"value": s.Score,
				},
			},
		})
	}

	totalTokens := inputTokens + outputTokens
	logObj := inspectLog{
		Version: 2,
		Status:  "success",
		Eval: map[string]any{
			"task": taskID,
		},
		Stats: inspectStats{
			ModelUsage: map[string]modelUsage{
				"mockllm/model": {
					InputTokens:  inputTokens,
					OutputTokens: outputTokens,
					TotalTokens:  totalTokens,
				},
			},
		},
		Samples: samples,
	}

	data, _ := json.Marshal(logObj)
	return data
}

func TestAdaptivePairedSampling_ExtraSamplesOnDecidedComparison(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	// Batch 1: 8 identical samples (decides UNCHANGED)
	var b1Scores []stats.SampleScore
	for i := 1; i <= 8; i++ {
		b1Scores = append(b1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Scores)}
	fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Scores)}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-extra", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-b1"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-b1"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         8,
				MaxSamplesPerTask: 64,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-extra"}}

	// Reconcile 1: should finalize at 8 pairs with UNCHANGED
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}

	var compB1 v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compB1)
	if compB1.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Fatalf("Expected completed after batch 1, got %s", compB1.Status.Phase)
	}
	if compB1.Status.Verdict != v1alpha1.ComparisonVerdictUnchanged {
		t.Fatalf("Expected UNCHANGED, got %s", compB1.Status.Verdict)
	}

	// First bump: User requests extraSamples: 8
	compB1.Spec.ExtraSamples = 8
	if err := cl.Update(t.Context(), &compB1); err != nil {
		t.Fatalf("Failed to update extraSamples: %v", err)
	}

	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile with extraSamples failed: %v", err)
	}

	var compWithExtra v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compWithExtra)

	if len(compWithExtra.Status.Batches) != 2 {
		t.Fatalf("Expected 2 batches, got %d", len(compWithExtra.Status.Batches))
	}
	if compWithExtra.Status.Batches[1].SampleRange.Start != 9 || compWithExtra.Status.Batches[1].SampleRange.End != 16 {
		t.Errorf("Expected batch 2 range 9-16, got %+v", compWithExtra.Status.Batches[1].SampleRange)
	}
	if compWithExtra.Status.ExtraSamplesApplied != 8 {
		t.Errorf("Expected extraSamplesApplied 8, got %d", compWithExtra.Status.ExtraSamplesApplied)
	}

	// Complete Batch 2
	var b2Scores []stats.SampleScore
	for i := 9; i <= 16; i++ {
		b2Scores = append(b2Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b2Scores)}
	fakeGCS.Objects["test-bucket/runs/base-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b2Scores)}

	var candB2, baseB2 v1alpha1.Run
	_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-extra-b2-cand"}, &candB2)
	_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-extra-b2-base"}, &baseB2)

	candB2.Status.Phase = v1alpha1.RunPhaseCompleted
	candB2.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b2.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16}},
	}
	_ = cl.Status().Update(t.Context(), &candB2)

	baseB2.Status.Phase = v1alpha1.RunPhaseCompleted
	baseB2.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b2.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16}},
	}
	_ = cl.Status().Update(t.Context(), &baseB2)

	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile after batch 2 failed: %v", err)
	}

	var compAfterB2 v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compAfterB2)

	if compAfterB2.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("Expected Completed after batch 2, got %s", compAfterB2.Status.Phase)
	}
	if compAfterB2.Status.Tasks[0].SamplesRun != 16 {
		t.Errorf("Expected samplesRun 16, got %d", compAfterB2.Status.Tasks[0].SamplesRun)
	}
	if len(compAfterB2.Status.Batches) != 2 {
		t.Errorf("Expected exactly 2 batches, got %d", len(compAfterB2.Status.Batches))
	}

	// Second bump: User bumps extraSamples to 16
	compAfterB2.Spec.ExtraSamples = 16
	if err := cl.Update(t.Context(), &compAfterB2); err != nil {
		t.Fatalf("Failed to bump extraSamples: %v", err)
	}

	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile on second bump failed: %v", err)
	}

	var compB3 v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compB3)

	if len(compB3.Status.Batches) != 3 {
		t.Fatalf("Expected 3 batches after second bump, got %d", len(compB3.Status.Batches))
	}
	if compB3.Status.Batches[2].SampleRange.Start != 17 || compB3.Status.Batches[2].SampleRange.End != 24 {
		t.Errorf("Expected batch 3 range 17-24, got %+v", compB3.Status.Batches[2].SampleRange)
	}
	if compB3.Status.ExtraSamplesApplied != 16 {
		t.Errorf("Expected extraSamplesApplied 16, got %d", compB3.Status.ExtraSamplesApplied)
	}
}

func TestAdaptivePairedSampling_ExtraSamples_LargerThanBatchSize(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	var b1Scores []stats.SampleScore
	for i := 1; i <= 8; i++ {
		b1Scores = append(b1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Scores)}
	fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Scores)}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}

	// ExtraSamples: 8, BatchSize: 4 -> ceil(8/4) = 2 extra batches
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-ceil", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate:    v1alpha1.ComparisonRunRef{Run: "cand-run-b1"},
			Baseline:     v1alpha1.ComparisonRunRef{Run: "base-run-b1"},
			ExtraSamples: 8,
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         4,
				MaxSamplesPerTask: 64,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-ceil"}}

	// Reconcile 1: batch 1 decided, extraSamples=8 > 0 -> schedules batch 2 (window 9-12, 4 samples)
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}

	var compB2 v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compB2)
	if len(compB2.Status.Batches) != 2 {
		t.Fatalf("Expected 2 batches, got %d", len(compB2.Status.Batches))
	}
	if compB2.Status.Batches[1].SampleRange.Start != 9 || compB2.Status.Batches[1].SampleRange.End != 12 {
		t.Errorf("Expected batch 2 range 9-12, got %+v", compB2.Status.Batches[1].SampleRange)
	}
	if compB2.Status.ExtraSamplesApplied != 4 {
		t.Errorf("Expected extraSamplesApplied 4 after batch 2 scheduled, got %d", compB2.Status.ExtraSamplesApplied)
	}

	// Complete batch 2 with 4 samples
	var b2Scores []stats.SampleScore
	for i := 9; i <= 12; i++ {
		b2Scores = append(b2Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b2Scores)}
	fakeGCS.Objects["test-bucket/runs/base-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b2Scores)}

	var candB2, baseB2 v1alpha1.Run
	_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-ceil-b2-cand"}, &candB2)
	_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-ceil-b2-base"}, &baseB2)
	candB2.Status.Phase = v1alpha1.RunPhaseCompleted
	candB2.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b2.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 12}},
	}
	_ = cl.Status().Update(t.Context(), &candB2)
	baseB2.Status.Phase = v1alpha1.RunPhaseCompleted
	baseB2.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b2.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 12}},
	}
	_ = cl.Status().Update(t.Context(), &baseB2)

	// Reconcile 2: batch 2 completed, extraSamplesApplied (4) < extraSamples (8) -> schedules batch 3 (window 13-16, 4 samples)
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile 2 failed: %v", err)
	}

	var compB3 v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compB3)
	if len(compB3.Status.Batches) != 3 {
		t.Fatalf("Expected 3 batches, got %d", len(compB3.Status.Batches))
	}
	if compB3.Status.Batches[2].SampleRange.Start != 13 || compB3.Status.Batches[2].SampleRange.End != 16 {
		t.Errorf("Expected batch 3 range 13-16, got %+v", compB3.Status.Batches[2].SampleRange)
	}
	if compB3.Status.ExtraSamplesApplied != 8 {
		t.Errorf("Expected extraSamplesApplied 8 after batch 3 scheduled, got %d", compB3.Status.ExtraSamplesApplied)
	}

	// Complete batch 3 with 4 samples
	var b3Scores []stats.SampleScore
	for i := 13; i <= 16; i++ {
		b3Scores = append(b3Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/b3.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b3Scores)}
	fakeGCS.Objects["test-bucket/runs/base-commit/b3.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b3Scores)}

	var candB3, baseB3 v1alpha1.Run
	_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-ceil-b3-cand"}, &candB3)
	_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-ceil-b3-base"}, &baseB3)
	candB3.Status.Phase = v1alpha1.RunPhaseCompleted
	candB3.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b3.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 13, End: 16}},
	}
	_ = cl.Status().Update(t.Context(), &candB3)
	baseB3.Status.Phase = v1alpha1.RunPhaseCompleted
	baseB3.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b3.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 13, End: 16}},
	}
	_ = cl.Status().Update(t.Context(), &baseB3)

	// Reconcile 3: batch 3 completed, extraSamplesApplied (8) == extraSamples (8) -> completes, no batch 4
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile 3 failed: %v", err)
	}

	var compFinal v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compFinal)
	if compFinal.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("Expected Completed phase, got %s", compFinal.Status.Phase)
	}
	if len(compFinal.Status.Batches) != 3 {
		t.Errorf("Expected exactly 3 batches, got %d", len(compFinal.Status.Batches))
	}
	if compFinal.Status.ExtraSamplesApplied != 8 {
		t.Errorf("Expected extraSamplesApplied 8, got %d", compFinal.Status.ExtraSamplesApplied)
	}
}

func TestAdaptivePairedSampling_BaselineReuse(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	repoURL := "git://repo"
	baseCommit := "base-commit-hash"
	model := "mockllm/model"

	// Existing Completed Run for window 9-16 with batch-key label
	batchKey := ComputeBatchKey(repoURL, baseCommit, model, []string{taskID}, v1alpha1.SampleRange{Start: 9, End: 16})
	existingBaseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pre-existing-base-run-b2",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelBatchKey: batchKey,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v1alpha1.GroupVersion.String(),
					Kind:       "Comparison",
					Name:       "earlier-comparison",
					UID:        "earlier-uid-1234",
				},
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskID,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     "gs://test-bucket/runs/base-commit-hash/pre_existing.json",
					Samples:     8,
					SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
				},
			},
		},
	}

	// Batch 1 runs (1-8): 8 noisy samples so task continues to batch 2
	candScoresB1 := []stats.SampleScore{
		{ID: "1", Score: 1.0},
		{ID: "2", Score: 0.0},
		{ID: "3", Score: 1.0},
		{ID: "4", Score: 0.0},
		{ID: "5", Score: 1.0},
		{ID: "6", Score: 0.0},
		{ID: "7", Score: 1.0},
		{ID: "8", Score: 0.0},
	}
	baseScoresB1 := []stats.SampleScore{
		{ID: "1", Score: 0.0},
		{ID: "2", Score: 1.0},
		{ID: "3", Score: 0.0},
		{ID: "4", Score: 1.0},
		{ID: "5", Score: 0.0},
		{ID: "6", Score: 1.0},
		{ID: "7", Score: 0.0},
		{ID: "8", Score: 1.0},
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, candScoresB1)}
	fakeGCS.Objects["test-bucket/runs/base-commit-hash/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, baseScoresB1)}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: "cand-commit"},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-hash/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-reuse", Namespace: "eval-runner", UID: "comp-reuse-uid"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-b1"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-b1"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         8,
				MaxSamplesPerTask: 96,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingBaseRun, candRun, baseRun, comp).
		WithStatusSubresource(existingBaseRun, candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-reuse"}}

	// Reconcile: batch 1 needs extension -> schedules batch 2 (window 9-16)
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	var compUpdated v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compUpdated)

	if len(compUpdated.Status.Batches) != 2 {
		t.Fatalf("Expected 2 batches, got %d", len(compUpdated.Status.Batches))
	}

	// Verify BaselineRun in batch 2 references the existing Run!
	b2 := compUpdated.Status.Batches[1]
	if b2.BaselineRun != "pre-existing-base-run-b2" {
		t.Errorf("Expected batch 2 baselineRun to be %q, got %q", "pre-existing-base-run-b2", b2.BaselineRun)
	}

	// Verify that a new baseline run was NOT created
	var nonExistentBase v1alpha1.Run
	err = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-reuse-b2-base"}, &nonExistentBase)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected comp-reuse-b2-base not to exist, got err: %v", err)
	}

	// Verify existingBaseRun now has MULTIPLE owner references (both earlier-comparison and comp-reuse)
	var reloadedBase v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "pre-existing-base-run-b2"}, &reloadedBase); err != nil {
		t.Fatalf("Failed to reload existingBaseRun: %v", err)
	}
	if len(reloadedBase.OwnerReferences) != 2 {
		t.Fatalf("Expected 2 owner references on reused baseline run, got %d (%+v)", len(reloadedBase.OwnerReferences), reloadedBase.OwnerReferences)
	}
	foundEarlier := false
	foundReuser := false
	for _, ref := range reloadedBase.OwnerReferences {
		if ref.Name == "earlier-comparison" {
			foundEarlier = true
		}
		if ref.Name == "comp-reuse" && ref.UID == "comp-reuse-uid" {
			foundReuser = true
		}
	}
	if !foundEarlier || !foundReuser {
		t.Errorf("Expected both owner references, foundEarlier=%v, foundReuser=%v", foundEarlier, foundReuser)
	}
}

func TestAdaptivePairedSampling_BaselineReuse_AcrossCompletedComparison(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	repoURL := "git://repo"
	baseCommit := "base-commit-hash"
	model := "mockllm/model"

	// Batch 1 (samples 1-8): scores configured so stopping rule decides Continue
	candScoresB1 := []stats.SampleScore{
		{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0},
		{ID: "3", Score: 1.0}, {ID: "4", Score: 0.0},
		{ID: "5", Score: 1.0}, {ID: "6", Score: 0.0},
		{ID: "7", Score: 1.0}, {ID: "8", Score: 0.0},
	}
	baseScoresB1 := []stats.SampleScore{
		{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0},
		{ID: "3", Score: 0.0}, {ID: "4", Score: 1.0},
		{ID: "5", Score: 0.0}, {ID: "6", Score: 1.0},
		{ID: "7", Score: 0.0}, {ID: "8", Score: 1.0},
	}
	fakeGCS.Objects["test-bucket/runs/cand1-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, candScoresB1)}
	fakeGCS.Objects["test-bucket/runs/base-commit-hash/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, baseScoresB1)}

	candRun1 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: "cand1-commit"},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand1-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-hash/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}

	comp1 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-1", Namespace: "eval-runner", UID: "comp-1-uid"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-1"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         8,
				MaxSamplesPerTask: 16,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun1, baseRun, comp1).
		WithStatusSubresource(candRun1, baseRun, comp1).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req1 := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-1"}}

	// 1. Reconcile comp-1: batch 1 needs extension -> schedules batch 2 (window 9-16)
	if _, err := reconciler.Reconcile(t.Context(), req1); err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}

	var comp1Updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req1.NamespacedName, &comp1Updated); err != nil {
		t.Fatalf("Get comp1 failed: %v", err)
	}
	if len(comp1Updated.Status.Batches) != 2 {
		t.Fatalf("Expected 2 batches on comp1, got %d", len(comp1Updated.Status.Batches))
	}
	b2BaseName := comp1Updated.Status.Batches[1].BaselineRun
	if b2BaseName != "comp-1-b2-base" {
		t.Fatalf("Expected comp1 batch 2 baseline run to be 'comp-1-b2-base', got %q", b2BaseName)
	}

	// Verify comp-1-b2-base was created with the batch-key label
	var b2BaseRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b2BaseName}, &b2BaseRun); err != nil {
		t.Fatalf("Get comp-1-b2-base failed: %v", err)
	}
	expectedBatchKey := ComputeBatchKey(repoURL, baseCommit, model, []string{taskID}, v1alpha1.SampleRange{Start: 9, End: 16})
	if b2BaseRun.Labels[LabelBatchKey] != expectedBatchKey {
		t.Fatalf("Expected batch-key %q on comp-1-b2-base, got %q", expectedBatchKey, b2BaseRun.Labels[LabelBatchKey])
	}

	// 2. Complete batch 2 for comp-1
	var candScoresB2, baseScoresB2 []stats.SampleScore
	for i := 9; i <= 16; i++ {
		candScoresB2 = append(candScoresB2, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		baseScoresB2 = append(baseScoresB2, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
	}
	fakeGCS.Objects["test-bucket/runs/cand1-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, candScoresB2)}
	fakeGCS.Objects["test-bucket/runs/base-commit-hash/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, baseScoresB2)}

	var candB2Run v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-1-b2-cand"}, &candB2Run); err != nil {
		t.Fatalf("Get comp-1-b2-cand failed: %v", err)
	}
	candB2Run.Status.Phase = v1alpha1.RunPhaseCompleted
	candB2Run.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand1-commit/b2.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16}},
	}
	if err := cl.Status().Update(t.Context(), &candB2Run); err != nil {
		t.Fatalf("Status update candB2Run failed: %v", err)
	}

	b2BaseRun.Status.Phase = v1alpha1.RunPhaseCompleted
	b2BaseRun.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-hash/b2.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16}},
	}
	if err := cl.Status().Update(t.Context(), &b2BaseRun); err != nil {
		t.Fatalf("Status update b2BaseRun failed: %v", err)
	}

	// 3. Reconcile comp-1: batch 2 completes, comp-1 reaches phase Completed
	if _, err := reconciler.Reconcile(t.Context(), req1); err != nil {
		t.Fatalf("Reconcile 2 failed: %v", err)
	}

	if err := cl.Get(t.Context(), req1.NamespacedName, &comp1Updated); err != nil {
		t.Fatalf("Get comp1 failed: %v", err)
	}
	if comp1Updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Fatalf("Expected comp1 to be Completed, got %s", comp1Updated.Status.Phase)
	}

	// 4. Reconcile comp-1 again in terminal state: should early return clean
	if _, err := reconciler.Reconcile(t.Context(), req1); err != nil {
		t.Fatalf("Reconcile comp1 terminal failed: %v", err)
	}

	// 5. Create comp-2 sharing the same baseline Run
	fakeGCS.Objects["test-bucket/runs/cand2-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, candScoresB1)}
	candRun2 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-2", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: "cand2-commit"},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand2-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}
	if err := cl.Create(t.Context(), candRun2); err != nil {
		t.Fatalf("Create candRun2 failed: %v", err)
	}

	comp2 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-2", Namespace: "eval-runner", UID: "comp-2-uid"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-2"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         8,
				MaxSamplesPerTask: 16,
			},
		},
	}
	if err := cl.Create(t.Context(), comp2); err != nil {
		t.Fatalf("Create comp2 failed: %v", err)
	}

	req2 := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-2"}}

	// 6. Reconcile comp-2: batch 1 needs extension -> schedules batch 2 (window 9-16)
	if _, err := reconciler.Reconcile(t.Context(), req2); err != nil {
		t.Fatalf("Reconcile comp2 failed: %v", err)
	}

	var comp2Updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req2.NamespacedName, &comp2Updated); err != nil {
		t.Fatalf("Get comp2 failed: %v", err)
	}
	if len(comp2Updated.Status.Batches) != 2 {
		t.Fatalf("Expected 2 batches on comp2, got %d", len(comp2Updated.Status.Batches))
	}

	// 7. ASSERTION: comp2 reused comp-1-b2-base!
	if comp2Updated.Status.Batches[1].BaselineRun != "comp-1-b2-base" {
		t.Errorf("Expected comp-2 batch 2 baseline run to reuse %q, got %q", "comp-1-b2-base", comp2Updated.Status.Batches[1].BaselineRun)
	}

	// Verify no new baseline run was created
	var nonExistentBase v1alpha1.Run
	err = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-2-b2-base"}, &nonExistentBase)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("Expected comp-2-b2-base not to exist, got err: %v", err)
	}

	// Verify comp-1-b2-base now has owner references to both comp-1 and comp-2
	var reusedBaseRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-1-b2-base"}, &reusedBaseRun); err != nil {
		t.Fatalf("Get comp-1-b2-base failed: %v", err)
	}
	foundComp1 := false
	foundComp2 := false
	for _, ref := range reusedBaseRun.OwnerReferences {
		if ref.Name == "comp-1" {
			foundComp1 = true
		}
		if ref.Name == "comp-2" {
			foundComp2 = true
		}
	}
	if !foundComp1 || !foundComp2 {
		t.Errorf("Expected owner references to both comparisons, foundComp1=%v, foundComp2=%v", foundComp1, foundComp2)
	}
}

func TestAdaptivePairedSampling_BaselineReuse_StaleReadStateRecovery(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	repoURL := "git://repo"
	baseCommit := "base-commit-hash"
	model := "mockllm/model"

	// Batch 1 (samples 1-8): scores configured so stopping rule decides Continue
	candScoresB1 := []stats.SampleScore{
		{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0},
		{ID: "3", Score: 1.0}, {ID: "4", Score: 0.0},
		{ID: "5", Score: 1.0}, {ID: "6", Score: 0.0},
		{ID: "7", Score: 1.0}, {ID: "8", Score: 0.0},
	}
	baseScoresB1 := []stats.SampleScore{
		{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0},
		{ID: "3", Score: 0.0}, {ID: "4", Score: 1.0},
		{ID: "5", Score: 0.0}, {ID: "6", Score: 1.0},
		{ID: "7", Score: 0.0}, {ID: "8", Score: 1.0},
	}
	fakeGCS.Objects["test-bucket/runs/cand1-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, candScoresB1)}
	fakeGCS.Objects["test-bucket/runs/base-commit-hash/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, baseScoresB1)}

	candRun1 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: "cand1-commit"},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand1-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-hash/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}

	comp1 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-1", Namespace: "eval-runner", UID: "comp-1-uid"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-1"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         8,
				MaxSamplesPerTask: 16,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun1, baseRun, comp1).
		WithStatusSubresource(candRun1, baseRun, comp1).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req1 := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-1"}}

	// 1. Reconcile comp-1 to schedule batch 2
	if _, err := reconciler.Reconcile(t.Context(), req1); err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}

	// 2. Complete batch 2 for comp-1
	var candScoresB2, baseScoresB2 []stats.SampleScore
	for i := 9; i <= 16; i++ {
		candScoresB2 = append(candScoresB2, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		baseScoresB2 = append(baseScoresB2, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
	}
	fakeGCS.Objects["test-bucket/runs/cand1-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, candScoresB2)}
	fakeGCS.Objects["test-bucket/runs/base-commit-hash/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, baseScoresB2)}

	var candB2Run, b2BaseRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-1-b2-cand"}, &candB2Run); err != nil {
		t.Fatalf("Get comp-1-b2-cand failed: %v", err)
	}
	candB2Run.Status.Phase = v1alpha1.RunPhaseCompleted
	candB2Run.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand1-commit/b2.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16}},
	}
	if err := cl.Status().Update(t.Context(), &candB2Run); err != nil {
		t.Fatalf("Status update candB2Run failed: %v", err)
	}

	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-1-b2-base"}, &b2BaseRun); err != nil {
		t.Fatalf("Get comp-1-b2-base failed: %v", err)
	}
	b2BaseRun.Status.Phase = v1alpha1.RunPhaseCompleted
	b2BaseRun.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-hash/b2.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16}},
	}
	if err := cl.Status().Update(t.Context(), &b2BaseRun); err != nil {
		t.Fatalf("Status update b2BaseRun failed: %v", err)
	}

	// 3. Reconcile comp-1: completes
	if _, err := reconciler.Reconcile(t.Context(), req1); err != nil {
		t.Fatalf("Reconcile 2 failed: %v", err)
	}

	// 4. Create comp-2 sharing the same baseline Run
	fakeGCS.Objects["test-bucket/runs/cand2-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, candScoresB1)}
	candRun2 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-2", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: "cand2-commit"},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand2-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}
	if err := cl.Create(t.Context(), candRun2); err != nil {
		t.Fatalf("Create candRun2 failed: %v", err)
	}

	comp2 := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-2", Namespace: "eval-runner", UID: "comp-2-uid"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-2"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         8,
				MaxSamplesPerTask: 16,
			},
		},
	}
	if err := cl.Create(t.Context(), comp2); err != nil {
		t.Fatalf("Create comp2 failed: %v", err)
	}

	req2 := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-2"}}

	// 5. Reconcile comp-2: schedules batch 2 and reuses comp-1-b2-base
	if _, err := reconciler.Reconcile(t.Context(), req2); err != nil {
		t.Fatalf("Reconcile comp2 failed: %v", err)
	}

	var comp2Loaded v1alpha1.Comparison
	if err := cl.Get(t.Context(), req2.NamespacedName, &comp2Loaded); err != nil {
		t.Fatalf("Get comp2 failed: %v", err)
	}
	if len(comp2Loaded.Status.Batches) != 2 {
		t.Fatalf("Expected 2 batches on comp2 after scheduling batch 2, got %d", len(comp2Loaded.Status.Batches))
	}
	if comp2Loaded.Status.Batches[1].BaselineRun != "comp-1-b2-base" {
		t.Fatalf("Expected reused baseline run 'comp-1-b2-base', got %q", comp2Loaded.Status.Batches[1].BaselineRun)
	}

	// 6. Simulate stale cache read: roll comp2 status.batches back to [batch 1]
	// while comp-2-b2-cand exists on the cluster.
	comp2Loaded.Status.Batches = comp2Loaded.Status.Batches[:1]
	if err := cl.Status().Update(t.Context(), &comp2Loaded); err != nil {
		t.Fatalf("Failed to roll back comp2 status.batches: %v", err)
	}

	// 7. Reconcile comp-2 again (stale-read reconcile):
	// The recovery loop should discover comp-2-b2-cand, notice comp-2-b2-base is NotFound,
	// resolve the reused baseline via findCompletedBaselineRun, add owner ref, and recover comp-1-b2-base.
	if _, err := reconciler.Reconcile(t.Context(), req2); err != nil {
		t.Fatalf("Reconcile comp2 during stale-read recovery failed: %v", err)
	}

	var comp2Recovered v1alpha1.Comparison
	if err := cl.Get(t.Context(), req2.NamespacedName, &comp2Recovered); err != nil {
		t.Fatalf("Get comp2Recovered failed: %v", err)
	}
	if len(comp2Recovered.Status.Batches) != 2 {
		t.Fatalf("Expected 2 batches recovered on comp2, got %d", len(comp2Recovered.Status.Batches))
	}
	if comp2Recovered.Status.Batches[1].BaselineRun != "comp-1-b2-base" {
		t.Errorf("Expected recovered baseline run to be 'comp-1-b2-base', got %q", comp2Recovered.Status.Batches[1].BaselineRun)
	}
	if strings.Contains(comp2Recovered.Status.Message, "missing batch run") {
		t.Errorf("Expected no 'missing batch run' error in message, got: %q", comp2Recovered.Status.Message)
	}

	// Verify owner reference to comp-2 is present on the reused baseline run
	var reusedBase v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-1-b2-base"}, &reusedBase); err != nil {
		t.Fatalf("Get reusedBase failed: %v", err)
	}
	hasComp2Ref := false
	for _, ref := range reusedBase.OwnerReferences {
		if ref.Name == "comp-2" && ref.UID == "comp-2-uid" {
			hasComp2Ref = true
		}
	}
	if !hasComp2Ref {
		t.Errorf("Expected comp-2 owner reference on comp-1-b2-base, got %+v", reusedBase.OwnerReferences)
	}
}

func TestAdaptivePairedSampling_CostAccountingAndTokens(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	var scores []stats.SampleScore
	for i := 1; i <= 8; i++ {
		scores = append(scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
	}

	// 500 tokens (400 in, 100 out) in candidate log, 300 tokens (200 in, 100 out) in baseline log
	fakeGCS.Objects["test-bucket/runs/cand-commit/log.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, scores, 400, 100)}
	fakeGCS.Objects["test-bucket/runs/base-commit/log.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, scores, 200, 100)}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/log.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/log.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-cost", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize: 8,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-cost"}}
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	var compFinal v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compFinal)

	if len(compFinal.Status.Tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(compFinal.Status.Tasks))
	}
	task := compFinal.Status.Tasks[0]
	if task.Tokens == nil || task.Tokens.Input != 600 || task.Tokens.Output != 200 || task.Tokens.Total != 800 {
		t.Errorf("Expected task.Tokens to be {600, 200, 800}, got %+v", task.Tokens)
	}
	if task.SamplesRun != 8 {
		t.Errorf("Expected samplesRun 8, got %d", task.SamplesRun)
	}

	// Verify artifact in sink
	artData, ok := fakeGCS.Objects["test-bucket/comparisons/comp-cost.json"]
	if !ok {
		t.Fatalf("Expected comparison artifact to exist in sink")
	}
	var art ComparisonArtifact
	if err := json.Unmarshal(artData.Data, &art); err != nil {
		t.Fatalf("Failed to unmarshal artifact: %v", err)
	}
	if len(art.Tasks) != 1 {
		t.Fatalf("Expected 1 task in artifact, got %d", len(art.Tasks))
	}
	artTask := art.Tasks[0]
	if artTask.Tokens == nil || artTask.Tokens.Input != 600 || artTask.Tokens.Output != 200 || artTask.Tokens.Total != 800 {
		t.Errorf("Expected artifact task.Tokens {600, 200, 800}, got %+v", artTask.Tokens)
	}
	if artTask.CandidateTokens == nil || artTask.CandidateTokens.Input != 400 || artTask.CandidateTokens.Output != 100 || artTask.CandidateTokens.Total != 500 {
		t.Errorf("Expected artifact candidateTokens {400, 100, 500}, got %+v", artTask.CandidateTokens)
	}
	if artTask.BaseTokens == nil || artTask.BaseTokens.Input != 200 || artTask.BaseTokens.Output != 100 || artTask.BaseTokens.Total != 300 {
		t.Errorf("Expected artifact baseTokens {200, 100, 300}, got %+v", artTask.BaseTokens)
	}
	if artTask.SamplesRun != 8 {
		t.Errorf("Expected artifact samplesRun=8, got samplesRun=%d", artTask.SamplesRun)
	}
}

func TestComparison_Tokens_AccountingInvariantWithFixture(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	fixturePath := filepath.Join("..", "artifacts", "testdata", "model_usage_cache_reasoning.json")
	candFixtureData, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("Failed to read fixture: %v", err)
	}

	// Candidate log has input=140708, output=788, total_tokens=272146 (cache_read=130000, cache_write=650, reasoning=150)
	fakeGCS.Objects["test-bucket/runs/cand-commit/log.json"] = fakes.FakeObject{Data: candFixtureData}

	// Baseline log with cache/reasoning fields: input=1000, output=200, total_tokens=3000 (cache_read=1800)
	baseLogJSON := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "docs/readme_comprehension", "model": "mockllm/model"},
		"stats": {
			"model_usage": {
				"mockllm/model": {
					"input_tokens": 1000,
					"output_tokens": 200,
					"total_tokens": 3000,
					"input_tokens_cache_read": 1800,
					"reasoning_tokens": 50
				}
			}
		},
		"samples": [
			{"id": 1, "scores": {"choice": {"value": "C"}}},
			{"id": 2, "scores": {"choice": {"value": "C"}}}
		]
	}`
	fakeGCS.Objects["test-bucket/runs/base-commit/log.json"] = fakes.FakeObject{Data: []byte(baseLogJSON)}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-tokens", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/log.json", Samples: 2, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-tokens", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/log.json", Samples: 2, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2}},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-token-invariant", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-tokens"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-tokens"},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-token-invariant"}}
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	var compFinal v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compFinal)

	if len(compFinal.Status.Tasks) != 1 {
		t.Fatalf("Expected 1 task in comparison status, got %d", len(compFinal.Status.Tasks))
	}
	task := compFinal.Status.Tasks[0]
	if task.Tokens == nil {
		t.Fatalf("Expected non-nil task.Tokens in comparison status")
	}

	// Verify invariant Total == Input + Output on comparison status
	if task.Tokens.Total != task.Tokens.Input+task.Tokens.Output {
		t.Errorf("Comparison status token invariant violated: total %d != input %d + output %d",
			task.Tokens.Total, task.Tokens.Input, task.Tokens.Output)
	}
	expectedCandTotal := 140708 + 788 // 141496 (Inspect total 272146 dropped)
	expectedBaseTotal := 1000 + 200   // 1200 (Inspect total 3000 dropped)
	expectedCombinedInput := 140708 + 1000
	expectedCombinedOutput := 788 + 200
	expectedCombinedTotal := expectedCandTotal + expectedBaseTotal

	if task.Tokens.Input != expectedCombinedInput || task.Tokens.Output != expectedCombinedOutput || task.Tokens.Total != expectedCombinedTotal {
		t.Errorf("Expected comparison task tokens {%d, %d, %d}, got %+v",
			expectedCombinedInput, expectedCombinedOutput, expectedCombinedTotal, task.Tokens)
	}
	if task.Tokens.Reasoning != 200 || task.Tokens.CacheRead != 131800 || task.Tokens.CacheWrite != 650 {
		t.Errorf("Expected comparison task tokens {Reasoning: 200, CacheRead: 131800, CacheWrite: 650}, got %+v", task.Tokens)
	}

	// Verify invariant in ComparisonArtifact
	artData, ok := fakeGCS.Objects["test-bucket/comparisons/comp-token-invariant.json"]
	if !ok {
		t.Fatalf("Expected comparison artifact comp-token-invariant.json in sink")
	}
	var art ComparisonArtifact
	if err := json.Unmarshal(artData.Data, &art); err != nil {
		t.Fatalf("Failed to unmarshal artifact: %v", err)
	}
	if len(art.Tasks) != 1 {
		t.Fatalf("Expected 1 task in artifact, got %d", len(art.Tasks))
	}
	artTask := art.Tasks[0]

	// Check candidate tokens invariant in artifact
	if artTask.CandidateTokens == nil {
		t.Fatalf("Expected non-nil candidateTokens in artifact")
	}
	if artTask.CandidateTokens.Total != artTask.CandidateTokens.Input+artTask.CandidateTokens.Output {
		t.Errorf("Artifact candidateTokens invariant violated: total %d != input %d + output %d",
			artTask.CandidateTokens.Total, artTask.CandidateTokens.Input, artTask.CandidateTokens.Output)
	}
	if artTask.CandidateTokens.Input != 140708 || artTask.CandidateTokens.Output != 788 || artTask.CandidateTokens.Total != expectedCandTotal {
		t.Errorf("Expected artifact candidateTokens {140708, 788, %d}, got %+v",
			expectedCandTotal, artTask.CandidateTokens)
	}
	if artTask.CandidateTokens.Reasoning != 150 || artTask.CandidateTokens.CacheRead != 130000 || artTask.CandidateTokens.CacheWrite != 650 {
		t.Errorf("Expected artifact candidateTokens {Reasoning: 150, CacheRead: 130000, CacheWrite: 650}, got %+v", artTask.CandidateTokens)
	}

	// Check base tokens invariant in artifact
	if artTask.BaseTokens == nil {
		t.Fatalf("Expected non-nil baseTokens in artifact")
	}
	if artTask.BaseTokens.Total != artTask.BaseTokens.Input+artTask.BaseTokens.Output {
		t.Errorf("Artifact baseTokens invariant violated: total %d != input %d + output %d",
			artTask.BaseTokens.Total, artTask.BaseTokens.Input, artTask.BaseTokens.Output)
	}
	if artTask.BaseTokens.Input != 1000 || artTask.BaseTokens.Output != 200 || artTask.BaseTokens.Total != expectedBaseTotal {
		t.Errorf("Expected artifact baseTokens {1000, 200, %d}, got %+v",
			expectedBaseTotal, artTask.BaseTokens)
	}
	if artTask.BaseTokens.Reasoning != 50 || artTask.BaseTokens.CacheRead != 1800 || artTask.BaseTokens.CacheWrite != 0 {
		t.Errorf("Expected artifact baseTokens {Reasoning: 50, CacheRead: 1800, CacheWrite: 0}, got %+v", artTask.BaseTokens)
	}

	// Check total tokens invariant in artifact
	if artTask.Tokens == nil {
		t.Fatalf("Expected non-nil tokens in artifact")
	}
	if artTask.Tokens.Total != artTask.Tokens.Input+artTask.Tokens.Output {
		t.Errorf("Artifact tokens invariant violated: total %d != input %d + output %d",
			artTask.Tokens.Total, artTask.Tokens.Input, artTask.Tokens.Output)
	}
	if artTask.Tokens.Input != expectedCombinedInput || artTask.Tokens.Output != expectedCombinedOutput || artTask.Tokens.Total != expectedCombinedTotal {
		t.Errorf("Expected artifact tokens {%d, %d, %d}, got %+v",
			expectedCombinedInput, expectedCombinedOutput, expectedCombinedTotal, artTask.Tokens)
	}
	if artTask.Tokens.Reasoning != 200 || artTask.Tokens.CacheRead != 131800 || artTask.Tokens.CacheWrite != 650 {
		t.Errorf("Expected artifact tokens {Reasoning: 200, CacheRead: 131800, CacheWrite: 650}, got %+v", artTask.Tokens)
	}
}

func TestComparison_Tokens_AccountingFallbackPerSample(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	candLogJSON := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "docs/readme_comprehension", "model": "mockllm/model"},
		"samples": [
			{
				"id": 1,
				"model_usage": {
					"mockllm/model": {
						"input_tokens": 100,
						"output_tokens": 20,
						"total_tokens": 200,
						"reasoning_tokens": 15,
						"input_tokens_cache_read": 80,
						"input_tokens_cache_write": 10
					}
				},
				"scores": {"choice": {"value": "C"}}
			},
			{
				"id": 2,
				"model_usage": {
					"mockllm/model": {
						"input_tokens": 50,
						"output_tokens": 10,
						"total_tokens": 100,
						"reasoning_tokens": 5,
						"input_tokens_cache_read": 40,
						"input_tokens_cache_write": 5
					}
				},
				"scores": {"choice": {"value": "C"}}
			}
		]
	}`
	fakeGCS.Objects["test-bucket/runs/cand-commit/log.json"] = fakes.FakeObject{Data: []byte(candLogJSON)}

	baseLogJSON := `{
		"version": 2,
		"status": "success",
		"eval": {"task": "docs/readme_comprehension", "model": "mockllm/model"},
		"samples": [
			{
				"id": 1,
				"model_usage": {
					"mockllm/model": {
						"input_tokens": 80,
						"output_tokens": 15,
						"total_tokens": 150,
						"reasoning_tokens": 10,
						"input_tokens_cache_read": 60,
						"input_tokens_cache_write": 5
					}
				},
				"scores": {"choice": {"value": "C"}}
			},
			{
				"id": 2,
				"model_usage": {
					"mockllm/model": {
						"input_tokens": 40,
						"output_tokens": 5,
						"total_tokens": 80,
						"reasoning_tokens": 5,
						"input_tokens_cache_read": 20,
						"input_tokens_cache_write": 2
					}
				},
				"scores": {"choice": {"value": "C"}}
			}
		]
	}`
	fakeGCS.Objects["test-bucket/runs/base-commit/log.json"] = fakes.FakeObject{Data: []byte(baseLogJSON)}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-fallback", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/log.json", Samples: 2, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-fallback", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/log.json", Samples: 2, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2}},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-token-fallback", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-fallback"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-fallback"},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-token-fallback"}}
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	var compFinal v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compFinal)

	if len(compFinal.Status.Tasks) != 1 {
		t.Fatalf("Expected 1 task in comparison status, got %d", len(compFinal.Status.Tasks))
	}
	task := compFinal.Status.Tasks[0]
	if task.Tokens == nil {
		t.Fatalf("Expected non-nil task.Tokens in comparison status")
	}

	// Cand: input 150, output 30, total 180, reasoning 20, cacheRead 120, cacheWrite 15
	// Base: input 120, output 20, total 140, reasoning 15, cacheRead 80, cacheWrite 7
	// Combined: input 270, output 50, total 320, reasoning 35, cacheRead 200, cacheWrite 22
	if task.Tokens.Input != 270 || task.Tokens.Output != 50 || task.Tokens.Total != 320 {
		t.Errorf("Expected combined tokens {270, 50, 320}, got %+v", task.Tokens)
	}
	if task.Tokens.Reasoning != 35 || task.Tokens.CacheRead != 200 || task.Tokens.CacheWrite != 22 {
		t.Errorf("Expected combined tokens {Reasoning: 35, CacheRead: 200, CacheWrite: 22}, got %+v", task.Tokens)
	}

	artData, ok := fakeGCS.Objects["test-bucket/comparisons/comp-token-fallback.json"]
	if !ok {
		t.Fatalf("Expected comparison artifact comp-token-fallback.json in sink")
	}
	var art ComparisonArtifact
	if err := json.Unmarshal(artData.Data, &art); err != nil {
		t.Fatalf("Failed to unmarshal artifact: %v", err)
	}
	artTask := art.Tasks[0]

	if artTask.CandidateTokens.Input != 150 || artTask.CandidateTokens.Output != 30 || artTask.CandidateTokens.Total != 180 {
		t.Errorf("Expected candidateTokens {150, 30, 180}, got %+v", artTask.CandidateTokens)
	}
	if artTask.CandidateTokens.Reasoning != 20 || artTask.CandidateTokens.CacheRead != 120 || artTask.CandidateTokens.CacheWrite != 15 {
		t.Errorf("Expected candidateTokens {Reasoning: 20, CacheRead: 120, CacheWrite: 15}, got %+v", artTask.CandidateTokens)
	}

	if artTask.BaseTokens.Input != 120 || artTask.BaseTokens.Output != 20 || artTask.BaseTokens.Total != 140 {
		t.Errorf("Expected baseTokens {120, 20, 140}, got %+v", artTask.BaseTokens)
	}
	if artTask.BaseTokens.Reasoning != 15 || artTask.BaseTokens.CacheRead != 80 || artTask.BaseTokens.CacheWrite != 7 {
		t.Errorf("Expected baseTokens {Reasoning: 15, CacheRead: 80, CacheWrite: 7}, got %+v", artTask.BaseTokens)
	}

	if artTask.Tokens.Input != 270 || artTask.Tokens.Output != 50 || artTask.Tokens.Total != 320 {
		t.Errorf("Expected artifact tokens {270, 50, 320}, got %+v", artTask.Tokens)
	}
	if artTask.Tokens.Reasoning != 35 || artTask.Tokens.CacheRead != 200 || artTask.Tokens.CacheWrite != 22 {
		t.Errorf("Expected artifact tokens {Reasoning: 35, CacheRead: 200, CacheWrite: 22}, got %+v", artTask.Tokens)
	}
}

func TestComparison_CostAndLatencyAccountingWithFixture(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"

	fixtureBytes, err := os.ReadFile("../artifacts/testdata/cost_latency_fixture.json")
	if err != nil {
		t.Fatalf("Failed to read fixture: %v", err)
	}

	t.Run("PriceKnown_CostAndLatencyComputed", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		fakeGCS.Objects["test-bucket/runs/cand-commit/log.json"] = fakes.FakeObject{Data: fixtureBytes}
		fakeGCS.Objects["test-bucket/runs/base-commit/log.json"] = fakes.FakeObject{Data: fixtureBytes}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-cost", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/log.json", Samples: 2, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2}},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-cost", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/log.json", Samples: 2, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2}},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-cost-latency", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-cost"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-cost"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
			ModelPrices: map[string]ModelPrice{
				"mockllm/model": {InputPer1M: 1.0, OutputPer1M: 2.0},
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-cost-latency"}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var compFinal v1alpha1.Comparison
		_ = cl.Get(t.Context(), req.NamespacedName, &compFinal)

		if len(compFinal.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task in comparison status, got %d", len(compFinal.Status.Tasks))
		}
		task := compFinal.Status.Tasks[0]

		// Cost: 0.14 cand + 0.14 base = 0.28
		expectedCost := 0.28
		if task.CostUSD == nil || math.Abs(*task.CostUSD-expectedCost) > 1e-9 {
			t.Errorf("Expected task.CostUSD=%f, got %v", expectedCost, task.CostUSD)
		}
		if task.CostSource != v1alpha1.CostSourceConfigMap {
			t.Errorf("Expected task.CostSource=%q, got %q", v1alpha1.CostSourceConfigMap, task.CostSource)
		}
		if compFinal.Status.CostUSD == nil || math.Abs(*compFinal.Status.CostUSD-expectedCost) > 1e-9 {
			t.Errorf("Expected compFinal.Status.CostUSD=%f, got %v", expectedCost, compFinal.Status.CostUSD)
		}

		// Latency: P50=2.0
		if task.Latency == nil || math.Abs(task.Latency.P50-2.0) > 1e-9 {
			t.Errorf("Expected task.Latency.P50=2.0, got %+v", task.Latency)
		}

		// Possible samples: 21
		if task.PossibleSamples != 21 {
			t.Errorf("Expected task.PossibleSamples=21, got %d", task.PossibleSamples)
		}
		if compFinal.Status.PossibleSamples != 21 {
			t.Errorf("Expected compFinal.Status.PossibleSamples=21, got %d", compFinal.Status.PossibleSamples)
		}

		// Check artifact
		artData, ok := fakeGCS.Objects["test-bucket/comparisons/comp-cost-latency.json"]
		if !ok {
			t.Fatalf("Expected comparison artifact in sink")
		}
		var art ComparisonArtifact
		if err := json.Unmarshal(artData.Data, &art); err != nil {
			t.Fatalf("Failed to unmarshal artifact: %v", err)
		}
		artTask := art.Tasks[0]

		if artTask.CostUSD == nil || math.Abs(*artTask.CostUSD-expectedCost) > 1e-9 {
			t.Errorf("Expected artTask.CostUSD=%f, got %v", expectedCost, artTask.CostUSD)
		}
		if artTask.CostSource != v1alpha1.CostSourceConfigMap {
			t.Errorf("Expected artTask.CostSource=%q, got %q", v1alpha1.CostSourceConfigMap, artTask.CostSource)
		}
		if artTask.CandidateCostUSD == nil || math.Abs(*artTask.CandidateCostUSD-0.14) > 1e-9 {
			t.Errorf("Expected artTask.CandidateCostUSD=0.14, got %v", artTask.CandidateCostUSD)
		}
		if artTask.BaseCostUSD == nil || math.Abs(*artTask.BaseCostUSD-0.14) > 1e-9 {
			t.Errorf("Expected artTask.BaseCostUSD=0.14, got %v", artTask.BaseCostUSD)
		}
		if artTask.Latency == nil || math.Abs(artTask.Latency.P50-2.0) > 1e-9 {
			t.Errorf("Expected artTask.Latency.P50=2.0, got %+v", artTask.Latency)
		}
		if artTask.CandidateLatency == nil || math.Abs(artTask.CandidateLatency.P50-2.0) > 1e-9 {
			t.Errorf("Expected artTask.CandidateLatency.P50=2.0, got %+v", artTask.CandidateLatency)
		}
		if artTask.BaseLatency == nil || math.Abs(artTask.BaseLatency.P50-2.0) > 1e-9 {
			t.Errorf("Expected artTask.BaseLatency.P50=2.0, got %+v", artTask.BaseLatency)
		}
		if artTask.PossibleSamples != 21 {
			t.Errorf("Expected artTask.PossibleSamples=21, got %d", artTask.PossibleSamples)
		}
		if art.CostUSD == nil || math.Abs(*art.CostUSD-expectedCost) > 1e-9 {
			t.Errorf("Expected art.CostUSD=%f, got %v", expectedCost, art.CostUSD)
		}
		if art.PossibleSamples != 21 {
			t.Errorf("Expected art.PossibleSamples=21, got %d", art.PossibleSamples)
		}
	})

	t.Run("DefaultModel_NoConfigMap_CostAndProvenanceDefault", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		fakeGCS.Objects["test-bucket/runs/cand-commit/log.json"] = fakes.FakeObject{Data: fixtureBytes}
		fakeGCS.Objects["test-bucket/runs/base-commit/log.json"] = fakes.FakeObject{Data: fixtureBytes}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-default-cost", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "google/gemini-3.5-flash-lite",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/log.json", Samples: 2, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2}},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-default-cost", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "google/gemini-3.5-flash-lite",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/log.json", Samples: 2, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2}},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-default-cost", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-default-cost"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-default-cost"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
			// ModelPrices: nil simulating no ConfigMap configured
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-default-cost"}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var compFinal v1alpha1.Comparison
		_ = cl.Get(t.Context(), req.NamespacedName, &compFinal)

		if len(compFinal.Status.Tasks) != 1 {
			t.Fatalf("Expected 1 task in comparison status, got %d", len(compFinal.Status.Tasks))
		}
		task := compFinal.Status.Tasks[0]

		// 100k input ($0.30/1M), 20k output ($2.50/1M) -> 0.08 cand + 0.08 base = 0.16
		expectedCost := 0.16
		if task.CostUSD == nil || math.Abs(*task.CostUSD-expectedCost) > 1e-9 {
			t.Errorf("Expected task.CostUSD=%f, got %v", expectedCost, task.CostUSD)
		}
		if task.CostSource != v1alpha1.CostSourceDefault {
			t.Errorf("Expected task.CostSource=%q, got %q", v1alpha1.CostSourceDefault, task.CostSource)
		}
		if compFinal.Status.CostUSD == nil || math.Abs(*compFinal.Status.CostUSD-expectedCost) > 1e-9 {
			t.Errorf("Expected compFinal.Status.CostUSD=%f, got %v", expectedCost, compFinal.Status.CostUSD)
		}

		// Check artifact
		artData, ok := fakeGCS.Objects["test-bucket/comparisons/comp-default-cost.json"]
		if !ok {
			t.Fatalf("Expected comparison artifact in sink")
		}
		var art ComparisonArtifact
		if err := json.Unmarshal(artData.Data, &art); err != nil {
			t.Fatalf("Failed to unmarshal artifact: %v", err)
		}
		artTask := art.Tasks[0]

		if artTask.CostUSD == nil || math.Abs(*artTask.CostUSD-expectedCost) > 1e-9 {
			t.Errorf("Expected artTask.CostUSD=%f, got %v", expectedCost, artTask.CostUSD)
		}
		if artTask.CostSource != v1alpha1.CostSourceDefault {
			t.Errorf("Expected artTask.CostSource=%q, got %q", v1alpha1.CostSourceDefault, artTask.CostSource)
		}
	})

	t.Run("PriceUnknown_CostOmitted", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		fakeGCS.Objects["test-bucket/runs/cand-commit/log.json"] = fakes.FakeObject{Data: fixtureBytes}
		fakeGCS.Objects["test-bucket/runs/base-commit/log.json"] = fakes.FakeObject{Data: fixtureBytes}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-noprice", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "mockllm/no-price",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/log.json", Samples: 2, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2}},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-noprice", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "mockllm/no-price",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/log.json", Samples: 2, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 2}},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-cost-omitted", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-noprice"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-noprice"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
			ModelPrices: map[string]ModelPrice{
				"other/model": {InputPer1M: 1.0, OutputPer1M: 2.0},
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-cost-omitted"}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var compFinal v1alpha1.Comparison
		_ = cl.Get(t.Context(), req.NamespacedName, &compFinal)

		task := compFinal.Status.Tasks[0]
		if task.CostUSD != nil {
			t.Errorf("Expected nil task.CostUSD, got %v", task.CostUSD)
		}
		if compFinal.Status.CostUSD != nil {
			t.Errorf("Expected nil compFinal.Status.CostUSD, got %v", compFinal.Status.CostUSD)
		}

		artData, ok := fakeGCS.Objects["test-bucket/comparisons/comp-cost-omitted.json"]
		if !ok {
			t.Fatalf("Expected comparison artifact in sink")
		}
		var art ComparisonArtifact
		if err := json.Unmarshal(artData.Data, &art); err != nil {
			t.Fatalf("Failed to unmarshal artifact: %v", err)
		}
		if art.Tasks[0].CostUSD != nil {
			t.Errorf("Expected nil artTask.CostUSD, got %v", art.Tasks[0].CostUSD)
		}
		if art.CostUSD != nil {
			t.Errorf("Expected nil art.CostUSD, got %v", art.CostUSD)
		}
	})
}

func TestAdaptivePairedSampling_DatasetExhausted_ZeroSuggestedNext(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	// Batch 1: 4 samples
	var b1Scores []stats.SampleScore
	for i := 1; i <= 4; i++ {
		b1Scores = append(b1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Scores)}
	fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Scores)}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-ex", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-ex", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-ex", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-ex"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-ex"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         4,
				MaxSamplesPerTask: 64,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-ex"}}
	// Reconcile 1: schedules batch 2 (window 5-8)
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}

	// Batch 2 returns only 1 sample (ID "5"), so dataset is exhausted (5 < 4 returned)
	b2Scores := []stats.SampleScore{{ID: "5", Score: 1.0}}
	fakeGCS.Objects["test-bucket/runs/cand-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b2Scores)}
	fakeGCS.Objects["test-bucket/runs/base-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b2Scores)}

	var candB2, baseB2 v1alpha1.Run
	_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-ex-b2-cand"}, &candB2)
	_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-ex-b2-base"}, &baseB2)
	candB2.Status.Phase = v1alpha1.RunPhaseCompleted
	candB2.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b2.json", Samples: 1, SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8}},
	}
	_ = cl.Status().Update(t.Context(), &candB2)
	baseB2.Status.Phase = v1alpha1.RunPhaseCompleted
	baseB2.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b2.json", Samples: 1, SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8}},
	}
	_ = cl.Status().Update(t.Context(), &baseB2)

	// Reconcile 2: evaluates batch 1 + batch 2; task is exhausted and inconclusive
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile 2 failed: %v", err)
	}

	var compFinal v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compFinal)
	if compFinal.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("Expected Completed phase, got %s", compFinal.Status.Phase)
	}
	task := compFinal.Status.Tasks[0]
	if task.SuggestedNext != 0 {
		t.Errorf("Expected suggestedNext to be 0 for dataset-exhausted task, got %d", task.SuggestedNext)
	}
}

func TestAdaptivePairedSampling_MixedPerTaskStopping(t *testing.T) {
	scheme := setupScheme(t)
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	taskA := "docs/task_a"
	taskB := "docs/task_b"

	// Task A: 8 samples with candidate=1.0, baseline=0.0 -> decides improved after batch 1
	var taskAcand, taskAbase []stats.SampleScore
	for i := 1; i <= 8; i++ {
		taskAcand = append(taskAcand, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		taskAbase = append(taskAbase, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskA+"/b1.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskA, taskAcand),
	}
	fakeGCS.Objects["test-bucket/runs/base-commit/"+taskA+"/b1.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskA, taskAbase),
	}

	// Task B: 8 samples with alternating scores -> stays continue (undecided) after batch 1
	taskBcand := []stats.SampleScore{
		{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0},
		{ID: "5", Score: 1.0}, {ID: "6", Score: 0.0}, {ID: "7", Score: 1.0}, {ID: "8", Score: 0.0},
	}
	taskBbase := []stats.SampleScore{
		{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 0.0}, {ID: "4", Score: 1.0},
		{ID: "5", Score: 0.0}, {ID: "6", Score: 1.0}, {ID: "7", Score: 0.0}, {ID: "8", Score: 1.0},
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskB+"/b1.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskB, taskBcand),
	}
	fakeGCS.Objects["test-bucket/runs/base-commit/"+taskB+"/b1.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskB, taskBbase),
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-mixed", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			Tasks:       []string{taskA, taskB},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskA,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     "gs://test-bucket/runs/cand-commit/" + taskA + "/b1.json",
					Samples:     8,
					SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
				},
				{
					ID:          taskB,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     "gs://test-bucket/runs/cand-commit/" + taskB + "/b1.json",
					Samples:     8,
					SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
				},
			},
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-mixed", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			Tasks:       []string{taskA, taskB},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskA,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     "gs://test-bucket/runs/base-commit/" + taskA + "/b1.json",
					Samples:     8,
					SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
				},
				{
					ID:          taskB,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     "gs://test-bucket/runs/base-commit/" + taskB + "/b1.json",
					Samples:     8,
					SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
				},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-mixed", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-mixed"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-mixed"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         8,
				MaxSamplesPerTask: 16,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
	// Reconcile 1: Task A decides improved; Task B stays continue. Batch 2 is scheduled.
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}

	// Assert batch 2 Runs have spec.tasks: [B] only
	var b2Cand, b2Base v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-mixed-b2-cand"}, &b2Cand); err != nil {
		t.Fatalf("expected batch 2 candidate Run: %v", err)
	}
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-mixed-b2-base"}, &b2Base); err != nil {
		t.Fatalf("expected batch 2 baseline Run: %v", err)
	}

	if len(b2Cand.Spec.Tasks) != 1 || b2Cand.Spec.Tasks[0] != taskB {
		t.Errorf("expected batch 2 candidate to have spec.tasks: [%s], got %v", taskB, b2Cand.Spec.Tasks)
	}
	if len(b2Base.Spec.Tasks) != 1 || b2Base.Spec.Tasks[0] != taskB {
		t.Errorf("expected batch 2 baseline to have spec.tasks: [%s], got %v", taskB, b2Base.Spec.Tasks)
	}

	// Verify status after batch 1: task A keeps its label and samplesRun
	var compAfterB1 v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &compAfterB1); err != nil {
		t.Fatalf("failed to get comparison after reconcile 1: %v", err)
	}
	taskAPost1 := findComparisonTaskStatus(compAfterB1.Status.Tasks, taskA)
	if taskAPost1 == nil {
		t.Fatalf("missing taskA in status after batch 1")
	}
	if taskAPost1.Decision != v1alpha1.ComparisonTaskLabelImproved {
		t.Errorf("expected taskA decision improved after batch 1, got %s", taskAPost1.Decision)
	}
	if taskAPost1.Label != v1alpha1.ComparisonTaskLabelImproved {
		t.Errorf("expected taskA label improved after batch 1, got %s", taskAPost1.Label)
	}
	if taskAPost1.SamplesRun != 8 {
		t.Errorf("expected taskA samplesRun 8 after batch 1, got %d", taskAPost1.SamplesRun)
	}

	// Provide batch 2 logs for Task B (samples 9..16 with cand=1.0, base=0.0)
	var b2CandScores, b2BaseScores []stats.SampleScore
	for i := 9; i <= 16; i++ {
		b2CandScores = append(b2CandScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		b2BaseScores = append(b2BaseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskB+"/b2.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskB, b2CandScores),
	}
	fakeGCS.Objects["test-bucket/runs/base-commit/"+taskB+"/b2.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(taskB, b2BaseScores),
	}

	b2Cand.Status.Phase = v1alpha1.RunPhaseCompleted
	b2Cand.Status.Tasks = []v1alpha1.TaskStatus{
		{
			ID:          taskB,
			Phase:       v1alpha1.TaskPhaseCompleted,
			LogPath:     "gs://test-bucket/runs/cand-commit/" + taskB + "/b2.json",
			Samples:     8,
			SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
		},
	}
	if err := cl.Status().Update(t.Context(), &b2Cand); err != nil {
		t.Fatalf("failed to update b2Cand status: %v", err)
	}

	b2Base.Status.Phase = v1alpha1.RunPhaseCompleted
	b2Base.Status.Tasks = []v1alpha1.TaskStatus{
		{
			ID:          taskB,
			Phase:       v1alpha1.TaskPhaseCompleted,
			LogPath:     "gs://test-bucket/runs/base-commit/" + taskB + "/b2.json",
			Samples:     8,
			SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
		},
	}
	if err := cl.Status().Update(t.Context(), &b2Base); err != nil {
		t.Fatalf("failed to update b2Base status: %v", err)
	}

	// Reconcile 2: evaluates batch 1 + batch 2; Task B finishes
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile 2 failed: %v", err)
	}

	var updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Failed to get updated comparison: %v", err)
	}

	if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("expected phase Completed after both tasks resolved, got %s", updated.Status.Phase)
	}
	if len(updated.Status.Tasks) != 2 {
		t.Fatalf("expected 2 tasks in status, got %d", len(updated.Status.Tasks))
	}

	taskAFinal := findComparisonTaskStatus(updated.Status.Tasks, taskA)
	taskBFinal := findComparisonTaskStatus(updated.Status.Tasks, taskB)
	if taskAFinal == nil || taskBFinal == nil {
		t.Fatalf("missing task in comparison status")
	}

	// Task A keeps its label and samplesRun
	if taskAFinal.Decision != v1alpha1.ComparisonTaskLabelImproved {
		t.Errorf("expected taskA decision improved, got %s", taskAFinal.Decision)
	}
	if taskAFinal.Label != v1alpha1.ComparisonTaskLabelImproved {
		t.Errorf("expected taskA label improved, got %s", taskAFinal.Label)
	}
	if taskAFinal.SamplesRun != 8 {
		t.Errorf("expected taskA samplesRun 8, got %d", taskAFinal.SamplesRun)
	}

	// Task B ran through batch 2
	if taskBFinal.SamplesRun != 16 {
		t.Errorf("expected taskB samplesRun 16, got %d", taskBFinal.SamplesRun)
	}

	// Final samplesRun differs between A and B
	if taskAFinal.SamplesRun == taskBFinal.SamplesRun {
		t.Errorf("expected final samplesRun to differ between A and B, but both are %d", taskAFinal.SamplesRun)
	}
}

func TestAdaptivePairedSampling_DatasetExhaustion(t *testing.T) {
	scheme := setupScheme(t)

	t.Run("PartialWindow_Dataset13", func(t *testing.T) {
		taskID := "docs/readme_comprehension"
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		// Batch 1: 8 samples with alternating scores so the task stays continue (undecided)
		b1Cand := []stats.SampleScore{
			{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0},
			{ID: "5", Score: 1.0}, {ID: "6", Score: 0.0}, {ID: "7", Score: 1.0}, {ID: "8", Score: 0.0},
		}
		b1Base := []stats.SampleScore{
			{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 0.0}, {ID: "4", Score: 1.0},
			{ID: "5", Score: 0.0}, {ID: "6", Score: 1.0}, {ID: "7", Score: 0.0}, {ID: "8", Score: 1.0},
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Cand)}
		fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Base)}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-ex13", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-ex13", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-ex13", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-ex13"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-ex13"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// Reconcile 1: schedules batch 2 (window 9-16)
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		// Batch 2 window 9-16 returns 5 scored samples on both sides (dataset of 13)
		b2Cand := []stats.SampleScore{
			{ID: "9", Score: 1.0}, {ID: "10", Score: 0.0}, {ID: "11", Score: 1.0}, {ID: "12", Score: 0.0}, {ID: "13", Score: 1.0},
		}
		b2Base := []stats.SampleScore{
			{ID: "9", Score: 0.0}, {ID: "10", Score: 1.0}, {ID: "11", Score: 0.0}, {ID: "12", Score: 1.0}, {ID: "13", Score: 0.0},
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b2Cand)}
		fakeGCS.Objects["test-bucket/runs/base-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b2Base)}

		var candB2, baseB2 v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-ex13-b2-cand"}, &candB2); err != nil {
			t.Fatalf("expected batch 2 candidate Run: %v", err)
		}
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-ex13-b2-base"}, &baseB2); err != nil {
			t.Fatalf("expected batch 2 baseline Run: %v", err)
		}

		candB2.Status.Phase = v1alpha1.RunPhaseCompleted
		candB2.Status.Tasks = []v1alpha1.TaskStatus{
			{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b2.json", Samples: 5, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16}},
		}
		if err := cl.Status().Update(t.Context(), &candB2); err != nil {
			t.Fatalf("failed to update candB2 status: %v", err)
		}

		baseB2.Status.Phase = v1alpha1.RunPhaseCompleted
		baseB2.Status.Tasks = []v1alpha1.TaskStatus{
			{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b2.json", Samples: 5, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16}},
		}
		if err := cl.Status().Update(t.Context(), &baseB2); err != nil {
			t.Fatalf("failed to update baseB2 status: %v", err)
		}

		// Reconcile 2: evaluates batch 1 + batch 2; dataset is exhausted
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}

		var compFinal v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &compFinal); err != nil {
			t.Fatalf("failed to get updated comparison: %v", err)
		}

		// Assert no batch 3 is created
		if len(compFinal.Status.Batches) != 2 {
			t.Errorf("expected exactly 2 batches (no batch 3), got %d", len(compFinal.Status.Batches))
		}
		var b3Cand v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-ex13-b3-cand"}, &b3Cand); !apierrors.IsNotFound(err) {
			t.Errorf("expected no batch 3 candidate Run to exist, but got err: %v", err)
		}

		if compFinal.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed due to dataset exhaustion, got %s", compFinal.Status.Phase)
		}
		if compFinal.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("expected verdict INCONCLUSIVE, got %s", compFinal.Status.Verdict)
		}

		if len(compFinal.Status.Tasks) != 1 {
			t.Fatalf("expected 1 task in comparison status, got %d", len(compFinal.Status.Tasks))
		}
		task := compFinal.Status.Tasks[0]
		// When still undecided, stopping rule labels task as inconclusive due to dataset exhaustion
		if task.Decision != v1alpha1.ComparisonTaskLabelInconclusive {
			t.Errorf("expected task decision inconclusive, got %s", task.Decision)
		}
		if task.Label != v1alpha1.ComparisonTaskLabelInconclusive {
			t.Errorf("expected task label inconclusive, got %s", task.Label)
		}
		if task.SamplesRun != 13 {
			t.Errorf("expected samplesRun 13, got %d", task.SamplesRun)
		}
		if task.SuggestedNext != 0 {
			t.Errorf("expected suggestedNext 0 on dataset exhaustion, got %d", task.SuggestedNext)
		}
	})

	t.Run("WindowPastEnd_TaskFailedNoScoredSamples", func(t *testing.T) {
		taskID := "docs/readme_comprehension"
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		// Batch 1: 8 samples with alternating scores so the task stays continue (undecided)
		b1Cand := []stats.SampleScore{
			{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0},
			{ID: "5", Score: 1.0}, {ID: "6", Score: 0.0}, {ID: "7", Score: 1.0}, {ID: "8", Score: 0.0},
		}
		b1Base := []stats.SampleScore{
			{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 0.0}, {ID: "4", Score: 1.0},
			{ID: "5", Score: 0.0}, {ID: "6", Score: 1.0}, {ID: "7", Score: 0.0}, {ID: "8", Score: 1.0},
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Cand)}
		fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Base)}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-pastend", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-pastend", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-pastend", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-pastend"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-pastend"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// Reconcile 1: schedules batch 2 (window 9-16)
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		// Batch 2 window is fully past the end: task Failed with "no scored samples"
		var candB2, baseB2 v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-pastend-b2-cand"}, &candB2); err != nil {
			t.Fatalf("expected batch 2 candidate Run: %v", err)
		}
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-pastend-b2-base"}, &baseB2); err != nil {
			t.Fatalf("expected batch 2 baseline Run: %v", err)
		}

		candB2.Status.Phase = v1alpha1.RunPhaseFailed
		candB2.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskID,
				Phase:       v1alpha1.TaskPhaseFailed,
				Message:     "no scored samples",
				Samples:     0,
				SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
			},
		}
		if err := cl.Status().Update(t.Context(), &candB2); err != nil {
			t.Fatalf("failed to update candB2 status: %v", err)
		}

		baseB2.Status.Phase = v1alpha1.RunPhaseFailed
		baseB2.Status.Tasks = []v1alpha1.TaskStatus{
			{
				ID:          taskID,
				Phase:       v1alpha1.TaskPhaseFailed,
				Message:     "no scored samples",
				Samples:     0,
				SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
			},
		}
		if err := cl.Status().Update(t.Context(), &baseB2); err != nil {
			t.Fatalf("failed to update baseB2 status: %v", err)
		}

		// Reconcile 2: evaluates batch 1 + batch 2; window past end treated as exhausted
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}

		var compFinal v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &compFinal); err != nil {
			t.Fatalf("failed to get updated comparison: %v", err)
		}

		// Assert the same termination: no batch 3, Completed phase, INCONCLUSIVE verdict, task inconclusive
		if len(compFinal.Status.Batches) != 2 {
			t.Errorf("expected exactly 2 batches (no batch 3), got %d", len(compFinal.Status.Batches))
		}
		var b3Cand v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-pastend-b3-cand"}, &b3Cand); !apierrors.IsNotFound(err) {
			t.Errorf("expected no batch 3 candidate Run to exist, but got err: %v", err)
		}

		if compFinal.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected phase Completed due to dataset exhaustion, got %s", compFinal.Status.Phase)
		}
		if compFinal.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("expected verdict INCONCLUSIVE, got %s", compFinal.Status.Verdict)
		}

		if len(compFinal.Status.Tasks) != 1 {
			t.Fatalf("expected 1 task in comparison status, got %d", len(compFinal.Status.Tasks))
		}
		task := compFinal.Status.Tasks[0]
		if task.Decision != v1alpha1.ComparisonTaskLabelInconclusive {
			t.Errorf("expected task decision inconclusive, got %s", task.Decision)
		}
		if task.Label != v1alpha1.ComparisonTaskLabelInconclusive {
			t.Errorf("expected task label inconclusive, got %s", task.Label)
		}
		if task.SamplesRun != 8 {
			t.Errorf("expected samplesRun 8, got %d", task.SamplesRun)
		}
		if task.SuggestedNext != 0 {
			t.Errorf("expected suggestedNext 0 on dataset exhaustion, got %d", task.SuggestedNext)
		}
	})
}

func TestAdaptivePairedSampling_CapBySampleCountNotPosition(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	// Seed window starts at 101, End: 108 (8 samples).
	// MaxSamplesPerTask: 16, BatchSize: 8.
	// If clamped by absolute position (nextEnd > 16), it would fail to issue batch 2.
	// Capping by count (maxRemaining = 16 - 8 = 8) allows batch 2 at window [109, 116].
	b1Cand := []stats.SampleScore{
		{ID: "101", Score: 1.0}, {ID: "102", Score: 0.0}, {ID: "103", Score: 1.0}, {ID: "104", Score: 0.0},
		{ID: "105", Score: 1.0}, {ID: "106", Score: 0.0}, {ID: "107", Score: 1.0}, {ID: "108", Score: 0.0},
	}
	b1Base := []stats.SampleScore{
		{ID: "101", Score: 0.0}, {ID: "102", Score: 1.0}, {ID: "103", Score: 0.0}, {ID: "104", Score: 1.0},
		{ID: "105", Score: 0.0}, {ID: "106", Score: 1.0}, {ID: "107", Score: 0.0}, {ID: "108", Score: 1.0},
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Cand)}
	fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Base)}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-offset", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 101, End: 108},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 101, End: 108}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-offset", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 101, End: 108},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 101, End: 108}},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-count-cap", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-offset"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-offset"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         8,
				MaxSamplesPerTask: 16,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
	// Reconcile 1: should schedule batch 2 with window [109, 116]
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}

	var candB2, baseB2 v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-count-cap-b2-cand"}, &candB2); err != nil {
		t.Fatalf("expected batch 2 candidate Run: %v", err)
	}
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-count-cap-b2-base"}, &baseB2); err != nil {
		t.Fatalf("expected batch 2 baseline Run: %v", err)
	}
	if candB2.Spec.SampleRange == nil || candB2.Spec.SampleRange.Start != 109 || candB2.Spec.SampleRange.End != 116 {
		t.Fatalf("expected batch 2 range [109, 116], got %+v", candB2.Spec.SampleRange)
	}

	// Complete batch 2 with another 8 alternating samples so total samplesRun reaches 16 == maxSamples
	b2Cand := []stats.SampleScore{
		{ID: "109", Score: 1.0}, {ID: "110", Score: 0.0}, {ID: "111", Score: 1.0}, {ID: "112", Score: 0.0},
		{ID: "113", Score: 1.0}, {ID: "114", Score: 0.0}, {ID: "115", Score: 1.0}, {ID: "116", Score: 0.0},
	}
	b2Base := []stats.SampleScore{
		{ID: "109", Score: 0.0}, {ID: "110", Score: 1.0}, {ID: "111", Score: 0.0}, {ID: "112", Score: 1.0},
		{ID: "113", Score: 0.0}, {ID: "114", Score: 1.0}, {ID: "115", Score: 0.0}, {ID: "116", Score: 1.0},
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b2Cand)}
	fakeGCS.Objects["test-bucket/runs/base-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b2Base)}

	candB2.Status.Phase = v1alpha1.RunPhaseCompleted
	candB2.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b2.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 109, End: 116}},
	}
	if err := cl.Status().Update(t.Context(), &candB2); err != nil {
		t.Fatalf("failed to update candB2: %v", err)
	}
	baseB2.Status.Phase = v1alpha1.RunPhaseCompleted
	baseB2.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b2.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 109, End: 116}},
	}
	if err := cl.Status().Update(t.Context(), &baseB2); err != nil {
		t.Fatalf("failed to update baseB2: %v", err)
	}

	// Reconcile 2: cap of 16 reached by sample count, marks task as inconclusive and stops
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile 2 failed: %v", err)
	}

	var compFinal v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &compFinal); err != nil {
		t.Fatalf("failed to get comparison: %v", err)
	}
	if compFinal.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("expected comparison phase Completed, got %s", compFinal.Status.Phase)
	}
	if len(compFinal.Status.Tasks) != 1 {
		t.Fatalf("expected 1 task status, got %d", len(compFinal.Status.Tasks))
	}
	task := compFinal.Status.Tasks[0]
	if task.SamplesRun != 16 {
		t.Errorf("expected samplesRun 16, got %d", task.SamplesRun)
	}
	if task.Decision != v1alpha1.ComparisonTaskLabelInconclusive {
		t.Errorf("expected task decision inconclusive, got %s", task.Decision)
	}
}

func TestAdaptivePairedSampling_TransientErrorFetchingBatchRun(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 8},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 8},
			},
		},
	}

	candB2 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-err-b2-cand", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
		},
		Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseRunning},
	}
	baseB2 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-err-b2-base", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
		},
		Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseRunning},
	}

	t.Run("transient candidate batch run get error returned for retry", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-err", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 32,
				},
			},
			Status: v1alpha1.ComparisonStatus{
				Batches: []v1alpha1.ComparisonBatchStatus{
					{Index: 1, CandidateRun: "cand-run", BaselineRun: "base-run", Phase: v1alpha1.ComparisonPhaseCompleted},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, candB2, baseB2, comp).
			WithStatusSubresource(candRun, baseRun, candB2, baseB2, comp).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if key.Name == "comp-err-b2-cand" {
						return apierrors.NewInternalError(errors.New("transient apiserver connection error"))
					}
					return c.Get(ctx, key, obj, opts...)
				},
			}).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		_, err := reconciler.Reconcile(t.Context(), req)
		if err == nil {
			t.Fatal("expected non-nil error on transient candidate Get failure, got nil")
		}
		if !strings.Contains(err.Error(), "transient apiserver connection error") {
			t.Errorf("expected error message to contain transient error, got: %v", err)
		}

		// Verify comparison was not transitioned to Completed
		var updatedComp v1alpha1.Comparison
		if getErr := cl.Get(t.Context(), req.NamespacedName, &updatedComp); getErr != nil {
			t.Fatalf("failed to get comparison: %v", getErr)
		}
		if updatedComp.Status.Phase == v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("expected comparison not to be marked Completed on transient error, got %s", updatedComp.Status.Phase)
		}
	})

	t.Run("transient baseline batch run get error returned for retry", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-err", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 32,
				},
			},
			Status: v1alpha1.ComparisonStatus{
				Batches: []v1alpha1.ComparisonBatchStatus{
					{Index: 1, CandidateRun: "cand-run", BaselineRun: "base-run", Phase: v1alpha1.ComparisonPhaseCompleted},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, candB2, baseB2, comp).
			WithStatusSubresource(candRun, baseRun, candB2, baseB2, comp).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if key.Name == "comp-err-b2-base" {
						return apierrors.NewInternalError(errors.New("transient apiserver connection error"))
					}
					return c.Get(ctx, key, obj, opts...)
				},
			}).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		_, err := reconciler.Reconcile(t.Context(), req)
		if err == nil {
			t.Fatal("expected non-nil error on transient baseline Get failure, got nil")
		}
		if !strings.Contains(err.Error(), "transient apiserver connection error") {
			t.Errorf("expected error message to contain transient error, got: %v", err)
		}
	})
}

func TestAdaptivePairedSampling_ZeroSampleCrashNotDatasetExhausted(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	b1Cand := []stats.SampleScore{
		{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0},
		{ID: "5", Score: 1.0}, {ID: "6", Score: 0.0}, {ID: "7", Score: 1.0}, {ID: "8", Score: 0.0},
	}
	b1Base := []stats.SampleScore{
		{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 0.0}, {ID: "4", Score: 1.0},
		{ID: "5", Score: 0.0}, {ID: "6", Score: 1.0}, {ID: "7", Score: 0.0}, {ID: "8", Score: 1.0},
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Cand)}
	fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Base)}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-crash", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-crash", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-crash", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-crash"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-crash"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         8,
				MaxSamplesPerTask: 32,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
	// Reconcile 1: schedules batch 2
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}

	var candB2, baseB2 v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-crash-b2-cand"}, &candB2); err != nil {
		t.Fatalf("expected batch 2 candidate Run: %v", err)
	}
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-crash-b2-base"}, &baseB2); err != nil {
		t.Fatalf("expected batch 2 baseline Run: %v", err)
	}

	// In batch 2: candidate crashes with exit status 137 (OOM), Samples: 0. Baseline succeeds.
	candB2.Status.Phase = v1alpha1.RunPhaseCompleted
	candB2.Status.Tasks = []v1alpha1.TaskStatus{
		{
			ID:          taskID,
			Phase:       v1alpha1.TaskPhaseFailed,
			Message:     "container killed (exit status 137)",
			Samples:     0,
			SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
		},
	}
	if err := cl.Status().Update(t.Context(), &candB2); err != nil {
		t.Fatalf("failed to update candB2: %v", err)
	}

	b2Base := []stats.SampleScore{
		{ID: "9", Score: 1.0}, {ID: "10", Score: 0.0}, {ID: "11", Score: 1.0}, {ID: "12", Score: 0.0},
		{ID: "13", Score: 1.0}, {ID: "14", Score: 0.0}, {ID: "15", Score: 1.0}, {ID: "16", Score: 0.0},
	}
	fakeGCS.Objects["test-bucket/runs/base-commit/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b2Base)}
	baseB2.Status.Phase = v1alpha1.RunPhaseCompleted
	baseB2.Status.Tasks = []v1alpha1.TaskStatus{
		{
			ID:          taskID,
			Phase:       v1alpha1.TaskPhaseCompleted,
			LogPath:     "gs://test-bucket/runs/base-commit/b2.json",
			Samples:     8,
			SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
		},
	}
	if err := cl.Status().Update(t.Context(), &baseB2); err != nil {
		t.Fatalf("failed to update baseB2: %v", err)
	}

	// Reconcile 2: guards the #141 candidate task failed path and suggestedNext == 0 on a crash
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile 2 failed: %v", err)
	}

	var compFinal v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &compFinal); err != nil {
		t.Fatalf("failed to get comparison: %v", err)
	}
	if compFinal.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("expected comparison phase Completed, got %s", compFinal.Status.Phase)
	}
	if len(compFinal.Status.Tasks) != 1 {
		t.Fatalf("expected 1 task status, got %d", len(compFinal.Status.Tasks))
	}
	task := compFinal.Status.Tasks[0]
	if !strings.Contains(compFinal.Status.Message, "candidate task failed: "+taskID) {
		t.Errorf("comparison status message should report candidate task failed per #136 policy, got: %s", compFinal.Status.Message)
	}
	if task.SuggestedNext != 0 {
		t.Errorf("expected suggestedNext 0 on crash, got %d", task.SuggestedNext)
	}
}

func findComparisonTaskStatus(tasks []v1alpha1.ComparisonTaskStatus, id string) *v1alpha1.ComparisonTaskStatus {
	for i := range tasks {
		if tasks[i].ID == id {
			return &tasks[i]
		}
	}
	return nil
}

func TestComparisonReconciler_StatusMessageBoundedWithMarker(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	execTaskID := "evals/task_executed"
	scores := make([]stats.SampleScore, 8)
	for i := 0; i < 8; i++ {
		scores[i] = stats.SampleScore{ID: fmt.Sprintf("sample-%d", i+1), Score: 1.0}
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/"+execTaskID+"/log.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(execTaskID, scores),
	}
	fakeGCS.Objects["test-bucket/runs/base-commit/"+execTaskID+"/log.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(execTaskID, scores),
	}

	const skippedCount = 25
	allTaskIDs := []string{execTaskID}
	manifestTasks := []map[string]string{{"id": execTaskID}}
	for i := 0; i < skippedCount; i++ {
		id := fmt.Sprintf("evals/task_skipped_%02d", i)
		allTaskIDs = append(allTaskIDs, id)
		manifestTasks = append(manifestTasks, map[string]string{"id": id})
	}
	manifestBytes, _ := json.Marshal(map[string]any{"tasks": manifestTasks})
	fakeGCS.Objects["test-bucket/runs/cand-commit/manifest.json"] = fakes.FakeObject{Data: manifestBytes}
	fakeGCS.Objects["test-bucket/runs/base-commit/manifest.json"] = fakes.FakeObject{Data: manifestBytes}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-bound", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
			Tasks: []string{execTaskID},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         len(allTaskIDs),
				EligibleTaskCount: len(allTaskIDs),
				TaskIDs:           allTaskIDs,
				ManifestPath:      "gs://test-bucket/runs/cand-commit/manifest.json",
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: execTaskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/" + execTaskID + "/log.json"},
			},
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-bound", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
			Tasks: []string{execTaskID},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskCount:         len(allTaskIDs),
				EligibleTaskCount: len(allTaskIDs),
				TaskIDs:           allTaskIDs,
				ManifestPath:      "gs://test-bucket/runs/base-commit/manifest.json",
			},
			Tasks: []v1alpha1.TaskStatus{
				{ID: execTaskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/" + execTaskID + "/log.json"},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "cmp-bound", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-bound"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-bound"},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "cmp-bound"}}
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	var updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Failed to get updated comparison: %v", err)
	}

	// Verify that status.message has the truncated skipped list with the marker
	expectedMarker := fmt.Sprintf("... and %d more", skippedCount-20)
	if !strings.Contains(updated.Status.Message, expectedMarker) {
		t.Errorf("Expected status.message to contain marker %q, got: %s", expectedMarker, updated.Status.Message)
	}
	if !strings.HasSuffix(updated.Status.Message, expectedMarker) {
		t.Errorf("Expected status.message to end with marker %q, got: %s", expectedMarker, updated.Status.Message)
	}

	// Verify that exactly 20 task IDs are in the comma-separated skipped list + marker
	skippedPrefix := "skipped (not planned): "
	idx := strings.Index(updated.Status.Message, skippedPrefix)
	if idx == -1 {
		t.Fatalf("Expected status.message to contain %q", skippedPrefix)
	}
	skippedPart := updated.Status.Message[idx+len(skippedPrefix):]
	tokens := strings.Split(skippedPart, ", ")
	if len(tokens) != 21 { // 20 tasks + "... and 5 more"
		t.Errorf("Expected 21 tokens in skipped list (20 items + marker), got %d: %q", len(tokens), skippedPart)
	}

	// Verify that the comparison artifact in GCS contains all 25 skipped task IDs
	artObj, ok := fakeGCS.Objects["test-bucket/comparisons/cmp-bound.json"]
	if !ok {
		t.Fatalf("Expected comparison artifact at test-bucket/comparisons/cmp-bound.json")
	}
	var artifact ComparisonArtifact
	if err := json.Unmarshal(artObj.Data, &artifact); err != nil {
		t.Fatalf("Failed to unmarshal comparison artifact: %v", err)
	}
	if len(artifact.SkippedNotPlanned) != skippedCount {
		t.Fatalf("Expected artifact.SkippedNotPlanned to have %d items, got %d: %v", skippedCount, len(artifact.SkippedNotPlanned), artifact.SkippedNotPlanned)
	}
	for i := 0; i < skippedCount; i++ {
		expectedID := fmt.Sprintf("evals/task_skipped_%02d", i)
		if artifact.SkippedNotPlanned[i] != expectedID {
			t.Errorf("Expected artifact.SkippedNotPlanned[%d] to be %q, got %q", i, expectedID, artifact.SkippedNotPlanned[i])
		}
	}

	// Verify omitempty: empty lists should not appear in artifact JSON
	artJSONStr := string(artObj.Data)
	if strings.Contains(artJSONStr, `"notExecuted":`) {
		t.Errorf("Expected artifact JSON to omit 'notExecuted', got: %s", artJSONStr)
	}
	if strings.Contains(artJSONStr, `"notExecutedByBothRuns":`) {
		t.Errorf("Expected artifact JSON to omit 'notExecutedByBothRuns', got: %s", artJSONStr)
	}
	if strings.Contains(artJSONStr, `"failedInBothRuns":`) {
		t.Errorf("Expected artifact JSON to omit 'failedInBothRuns', got: %s", artJSONStr)
	}
	if strings.Contains(artJSONStr, `"candidateFailed":`) {
		t.Errorf("Expected artifact JSON to omit 'candidateFailed', got: %s", artJSONStr)
	}
}

func TestComparisonReconciler_ArtifactRecordsAllUnexecutedAndFailedLists(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	execTaskID := "evals/task_executed"
	scores := make([]stats.SampleScore, 8)
	for i := 0; i < 8; i++ {
		scores[i] = stats.SampleScore{ID: fmt.Sprintf("sample-%d", i+1), Score: 1.0}
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/"+execTaskID+"/log.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(execTaskID, scores),
	}
	fakeGCS.Objects["test-bucket/runs/base-commit/"+execTaskID+"/log.json"] = fakes.FakeObject{
		Data: makeSyntheticInspectLog(execTaskID, scores),
	}

	candFailedTask := "evals/task_cand_failed"
	notBothTask := "evals/task_not_both"
	neitherTask := "evals/task_neither"
	failedBothTask := "evals/task_failed_both"

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-lists", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
			Tasks: []string{execTaskID, candFailedTask, notBothTask, neitherTask, failedBothTask},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: execTaskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/" + execTaskID + "/log.json"},
				{ID: candFailedTask, Phase: v1alpha1.TaskPhaseFailed},
				{ID: notBothTask, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/" + notBothTask + "/log.json"},
				{ID: neitherTask, Phase: v1alpha1.TaskPhasePending},
				{ID: failedBothTask, Phase: v1alpha1.TaskPhaseFailed},
			},
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-lists", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
			Tasks: []string{execTaskID, candFailedTask, notBothTask, neitherTask, failedBothTask},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: execTaskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/" + execTaskID + "/log.json"},
				{ID: candFailedTask, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/" + candFailedTask + "/log.json"},
				{ID: notBothTask, Phase: v1alpha1.TaskPhasePending},
				{ID: neitherTask, Phase: v1alpha1.TaskPhasePending},
				{ID: failedBothTask, Phase: v1alpha1.TaskPhaseFailed},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "cmp-lists", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-lists"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-lists"},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "cmp-lists"}}
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	artObj, ok := fakeGCS.Objects["test-bucket/comparisons/cmp-lists.json"]
	if !ok {
		t.Fatalf("Expected comparison artifact at test-bucket/comparisons/cmp-lists.json")
	}
	var artifact ComparisonArtifact
	if err := json.Unmarshal(artObj.Data, &artifact); err != nil {
		t.Fatalf("Failed to unmarshal comparison artifact: %v", err)
	}

	if len(artifact.CandidateFailed) != 1 || artifact.CandidateFailed[0] != candFailedTask {
		t.Errorf("Expected CandidateFailed [%s], got %v", candFailedTask, artifact.CandidateFailed)
	}
	if len(artifact.NotExecuted) != 1 || artifact.NotExecuted[0] != neitherTask {
		t.Errorf("Expected NotExecuted [%s], got %v", neitherTask, artifact.NotExecuted)
	}
	if len(artifact.NotExecutedByBothRuns) != 1 || artifact.NotExecutedByBothRuns[0] != notBothTask {
		t.Errorf("Expected NotExecutedByBothRuns [%s], got %v", notBothTask, artifact.NotExecutedByBothRuns)
	}
	if len(artifact.FailedInBothRuns) != 1 || artifact.FailedInBothRuns[0] != failedBothTask {
		t.Errorf("Expected FailedInBothRuns [%s], got %v", failedBothTask, artifact.FailedInBothRuns)
	}
	if len(artifact.SkippedNotPlanned) != 0 {
		t.Errorf("Expected SkippedNotPlanned to be empty, got %v", artifact.SkippedNotPlanned)
	}

	// Verify omitempty for skippedNotPlanned
	if strings.Contains(string(artObj.Data), `"skippedNotPlanned":`) {
		t.Errorf("Expected artifact JSON to omit 'skippedNotPlanned', got: %s", string(artObj.Data))
	}
}

func TestComparisonReconciler_TTLReconcile(t *testing.T) {
	scheme := setupScheme(t)

	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	ttl := 1 * time.Hour

	t.Run("ExpiredCompletedComparisonIsDeleted", func(t *testing.T) {
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-expired",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * ttl)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
			},
			Status: v1alpha1.ComparisonStatus{
				Phase: v1alpha1.ComparisonPhaseCompleted,
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:        cl,
			Scheme:        scheme,
			ComparisonTTL: ttl,
			Now:           func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-expired"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
		if res.RequeueAfter != 0 {
			t.Errorf("expected RequeueAfter 0, got %v", res.RequeueAfter)
		}

		var checkComp v1alpha1.Comparison
		err = cl.Get(t.Context(), req.NamespacedName, &checkComp)
		if !apierrors.IsNotFound(err) {
			t.Fatalf("expected comparison to be deleted (NotFound), got err: %v", err)
		}

		// Baseline run must be preserved
		var checkBase v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "base-run"}, &checkBase); err != nil {
			t.Fatalf("expected baseline run to be preserved, got err: %v", err)
		}
	})

	t.Run("ActiveCompletedComparisonWithinTTLRequeues", func(t *testing.T) {
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-active", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-active", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-active",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-20 * time.Minute)), // 40m remaining
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-active"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-active"},
			},
			Status: v1alpha1.ComparisonStatus{
				Phase: v1alpha1.ComparisonPhaseCompleted,
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:        cl,
			Scheme:        scheme,
			ComparisonTTL: ttl,
			Now:           func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-active"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
		if res.RequeueAfter != 40*time.Minute {
			t.Errorf("expected RequeueAfter 40m, got %v", res.RequeueAfter)
		}

		var checkComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &checkComp); err != nil {
			t.Fatalf("expected comparison to still exist, got err: %v", err)
		}
	})

	t.Run("CompletedComparisonWithExtraSamplesRequestedNotDeleted", func(t *testing.T) {
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-extra", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-extra", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-extra",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * ttl)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:     v1alpha1.ComparisonRunRef{Run: "base-run-extra"},
				Candidate:    v1alpha1.ComparisonRunRef{Run: "cand-run-extra"},
				ExtraSamples: 10,
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:               v1alpha1.ComparisonPhaseCompleted,
				ExtraSamplesApplied: 5,
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:        cl,
			Scheme:        scheme,
			ComparisonTTL: ttl,
			Now:           func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-extra"}}
		_, _ = reconciler.Reconcile(t.Context(), req)

		var checkComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &checkComp); err != nil {
			t.Fatalf("expected comparison with extraSamples pending to NOT be deleted, got err: %v", err)
		}
	})

	t.Run("DisabledTTLDoesNotDelete", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-no-ttl",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-48 * time.Hour)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
			},
			Status: v1alpha1.ComparisonStatus{
				Phase: v1alpha1.ComparisonPhaseCompleted,
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(comp).
			WithStatusSubresource(comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:        cl,
			Scheme:        scheme,
			ComparisonTTL: 0, // disabled
			Now:           func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-no-ttl"}}
		_, _ = reconciler.Reconcile(t.Context(), req)

		var checkComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &checkComp); err != nil {
			t.Fatalf("expected comparison to NOT be deleted when TTL=0, got err: %v", err)
		}
	})

	t.Run("CompletedComparisonRetriesReportingWhenTTLConfigured", func(t *testing.T) {
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-report", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-report", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-report-retry",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Minute)), // 50m remaining
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-report"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-report"},
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "owner/repo",
					Number:     42,
					HeadSHA:    "cand-sha",
					BaseSHA:    "base-sha",
				},
			},
			Status: v1alpha1.ComparisonStatus{
				Phase: v1alpha1.ComparisonPhaseCompleted,
			},
		}
		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("ghp_test")},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp, tokenSecret).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.InjectError(500, `{"message":"Internal Server Error"}`)

		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://eval-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("failed to create GCS sink: %v", err)
		}

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ComparisonTTL:       ttl,
			Now:                 func() time.Time { return now },
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-report-retry"}}

		// 1. First reconcile should return error from GitHub report, NOT swallow it with RequeueAfter
		_, err = reconciler.Reconcile(t.Context(), req)
		if err == nil {
			t.Fatalf("expected error from failed GitHub report on Completed comparison with TTL, got nil")
		}

		// 2. Second reconcile after reporting retry backoff with GitHub recovered (InjectError was one-shot) should succeed and schedule TTL RequeueAfter
		now = now.Add(10 * time.Second)
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected error on second reconcile: %v", err)
		}
		expectedWait := 50*time.Minute - 10*time.Second
		if res.RequeueAfter != expectedWait {
			t.Errorf("expected RequeueAfter %v, got %v", expectedWait, res.RequeueAfter)
		}
		if len(server.Comments()) != 1 {
			t.Errorf("expected 1 comment posted to GitHub, got %d", len(server.Comments()))
		}
	})

	t.Run("CompletedComparisonCreated2xTTLAgoCompletedRecentlyNotDeleted", func(t *testing.T) {
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-2x", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-2x", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}
		compTime := metav1.NewTime(now.Add(-1 * time.Minute))
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-2x-ttl",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * ttl)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-2x"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-2x"},
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:          v1alpha1.ComparisonPhaseCompleted,
				CompletionTime: &compTime,
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:        cl,
			Scheme:        scheme,
			ComparisonTTL: ttl,
			Now:           func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-2x-ttl"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
		expectedRequeue := ttl - 1*time.Minute
		if res.RequeueAfter != expectedRequeue {
			t.Errorf("expected RequeueAfter %v (TTL - 1m), got %v", expectedRequeue, res.RequeueAfter)
		}

		var checkComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &checkComp); err != nil {
			t.Fatalf("expected comparison to NOT be deleted, got err: %v", err)
		}
	})

	t.Run("ComparisonCompletedPastTTLDeleted", func(t *testing.T) {
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-past", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-past", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}
		compTime := metav1.NewTime(now.Add(-ttl - 5*time.Minute))
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-past-ttl",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-5 * ttl)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-past"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-past"},
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:          v1alpha1.ComparisonPhaseCompleted,
				CompletionTime: &compTime,
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:        cl,
			Scheme:        scheme,
			ComparisonTTL: ttl,
			Now:           func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-past-ttl"}}
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
		if res.RequeueAfter != 0 {
			t.Errorf("expected RequeueAfter 0, got %v", res.RequeueAfter)
		}

		var checkComp v1alpha1.Comparison
		err = cl.Get(t.Context(), req.NamespacedName, &checkComp)
		if !apierrors.IsNotFound(err) {
			t.Fatalf("expected comparison to be deleted (NotFound), got err: %v", err)
		}
		var checkBase v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "base-run-past"}, &checkBase); err != nil {
			t.Fatalf("expected baseline run to be preserved, got err: %v", err)
		}
	})

	t.Run("ExtraSamplesBumpClearsCompletionTimestamp", func(t *testing.T) {
		taskID := "docs/readme_comprehension"
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		var b1Scores []stats.SampleScore
		for i := 1; i <= 8; i++ {
			b1Scores = append(b1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Scores)}
		fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Scores)}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-clear", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-clear", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-clear", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-clear"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-clear"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 64,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
			Now:          func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-clear"}}
		// Reconcile 1: Reaches Completed, sets CompletionTime
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("first reconcile failed: %v", err)
		}

		var completedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &completedComp); err != nil {
			t.Fatalf("failed to get completed comparison: %v", err)
		}
		if completedComp.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Fatalf("expected phase Completed, got %s", completedComp.Status.Phase)
		}
		if completedComp.Status.CompletionTime == nil {
			t.Fatalf("expected CompletionTime to be set on Completed comparison, got nil")
		}

		// ExtraSamples bump
		completedComp.Spec.ExtraSamples = 8
		if err := cl.Update(t.Context(), &completedComp); err != nil {
			t.Fatalf("failed to update extraSamples: %v", err)
		}

		// Reconcile 2: Schedules extra batch, moves back to Running, clears CompletionTime
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("reconcile after extraSamples bump failed: %v", err)
		}

		var runningComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &runningComp); err != nil {
			t.Fatalf("failed to get running comparison: %v", err)
		}
		if runningComp.Status.Phase != v1alpha1.ComparisonPhaseRunning {
			t.Fatalf("expected phase Running after extraSamples bump, got %s", runningComp.Status.Phase)
		}
		if runningComp.Status.CompletionTime != nil {
			t.Errorf("expected CompletionTime to be nil when moved back to Running, got %v", runningComp.Status.CompletionTime)
		}
	})

	t.Run("TerminalComparisonForeignPrefixPendingExtraSamplesDeletedByTTL", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.SetUserPermission("gke-labs", "eval-runner", "reviewer", "write")

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("ghp_test")},
		}

		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://new-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		foreignCandLog := "gs://old-bucket/runs/cand-sha/task1/inspect.json"
		foreignBaseLog := "gs://old-bucket/runs/base-sha/task1/inspect.json"

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-foreign", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task1",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: foreignBaseLog,
					},
				},
			},
		}
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-foreign", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task1",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: foreignCandLog,
					},
				},
			},
		}

		compTime := metav1.NewTime(now.Add(-30 * time.Minute)) // 30m ago, within ttl (1h)
		verdictMsg := "Evaluated 1 task(s): 1 improved"
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-foreign-ttl",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-foreign"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-foreign"},
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     99,
					HeadSHA:    "cand-sha",
					BaseSHA:    "base-sha",
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:               v1alpha1.ComparisonPhaseCompleted,
				Verdict:             v1alpha1.ComparisonVerdictImproved,
				Message:             verdictMsg,
				CompletionTime:      &compTime,
				ExtraSamplesApplied: 0,
				Reporting: &v1alpha1.ComparisonReportingStatus{
					CommentID: 7777,
				},
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  8888,
						Login:      "reviewer",
						Requested:  8,
						ReceivedAt: metav1.NewTime(now),
					},
				},
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:         "task1",
						Label:      v1alpha1.ComparisonTaskLabelImproved,
						SamplesRun: 8,
					},
				},
				Batches: []v1alpha1.ComparisonBatchStatus{
					{
						Index:        0,
						Phase:        v1alpha1.ComparisonPhaseCompleted,
						CandidateRun: "cand-run-foreign",
						BaselineRun:  "base-run-foreign",
					},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp, tokenSecret).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		currentNow := now
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ComparisonTTL:       ttl,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
			Now: func() time.Time { return currentNow },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-foreign-ttl"}}

		// Reconcile 1: processes /eval more 8, detects prefix mismatch, records outcome, schedules TTL requeue
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
		if res.Requeue {
			t.Errorf("expected Requeue false, got true")
		}
		expectedRequeue := ttl - 30*time.Minute
		if res.RequeueAfter != expectedRequeue {
			t.Errorf("expected RequeueAfter %v, got %v", expectedRequeue, res.RequeueAfter)
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if updatedComp.Spec.ExtraSamples != 8 {
			t.Errorf("expected spec.extraSamples 8, got %d", updatedComp.Spec.ExtraSamples)
		}
		if updatedComp.Status.ExtraSamplesApplied != 8 {
			t.Errorf("expected status.extraSamplesApplied 8, got %d", updatedComp.Status.ExtraSamplesApplied)
		}
		if updatedComp.Status.Message != verdictMsg {
			t.Errorf("expected status.message %q (preserved), got %q", verdictMsg, updatedComp.Status.Message)
		}
		if len(updatedComp.Status.HandledCommands) != 1 {
			t.Fatalf("expected 1 handled command, got %d", len(updatedComp.Status.HandledCommands))
		}
		if updatedComp.Status.HandledCommands[0].Outcome != "unserviceable: artifact prefix mismatch" {
			t.Errorf("expected handled command outcome 'unserviceable: artifact prefix mismatch', got %q", updatedComp.Status.HandledCommands[0].Outcome)
		}
		cond := meta.FindStatusCondition(updatedComp.Status.Conditions, v1alpha1.ConditionTypeExtraSamples)
		if cond == nil {
			t.Fatalf("expected ConditionTypeExtraSamples on status.conditions")
		}
		if cond.Status != metav1.ConditionFalse {
			t.Errorf("expected condition status False, got %s", cond.Status)
		}
		if cond.Reason != "ArtifactPrefixMismatch" {
			t.Errorf("expected condition reason ArtifactPrefixMismatch, got %s", cond.Reason)
		}
		if !strings.Contains(cond.Message, "extra samples cannot be run: artifact prefix mismatch") {
			t.Errorf("expected condition message to contain 'extra samples cannot be run: artifact prefix mismatch', got %q", cond.Message)
		}
		if !strings.Contains(cond.Message, "does not match commit prefix") {
			t.Errorf("expected condition message to contain mismatch detail 'does not match commit prefix', got %q", cond.Message)
		}

		// Verify exactly one unserviceable note was posted to PR
		var noteComments []string
		for _, c := range server.Comments() {
			if strings.Contains(c.Body, "Extra samples cannot be run: artifact prefix mismatch.") {
				noteComments = append(noteComments, c.Body)
			}
		}
		if len(noteComments) != 1 {
			t.Fatalf("expected exactly 1 unserviceable note comment on PR, got %d: %+v", len(noteComments), server.Comments())
		}
		expectedNote := "Extra samples cannot be run: artifact prefix mismatch."
		if noteComments[0] != expectedNote {
			t.Errorf("expected comment body %q, got %q", expectedNote, noteComments[0])
		}

		commentsCountAfterR1 := len(server.Comments())

		// Middle reconcile (age < TTL): advance 10m (age = 40m < 60m ttl).
		// Ensures no duplicate comments are posted and remaining TTL requeue is returned.
		currentNow = currentNow.Add(10 * time.Minute)
		resMid, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected middle reconcile error: %v", err)
		}
		expectedMidRequeue := ttl - 40*time.Minute
		if resMid.RequeueAfter != expectedMidRequeue {
			t.Errorf("expected middle RequeueAfter %v, got %v", expectedMidRequeue, resMid.RequeueAfter)
		}
		if len(server.Comments()) != commentsCountAfterR1 {
			t.Errorf("expected comments count unchanged (%d) on middle reconcile, got %d", commentsCountAfterR1, len(server.Comments()))
		}

		// Reconcile after TTL expires: advance clock past TTL (now was +40m since completion, advance by 25m so age = 65m > 60m ttl)
		currentNow = currentNow.Add(25 * time.Minute)
		res2, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error on expiry: %v", err)
		}
		if res2.RequeueAfter != 0 {
			t.Errorf("expected RequeueAfter 0 on deletion, got %v", res2.RequeueAfter)
		}

		var expiredComp v1alpha1.Comparison
		err = cl.Get(t.Context(), req.NamespacedName, &expiredComp)
		if !apierrors.IsNotFound(err) {
			t.Fatalf("expected comparison to be deleted by TTL, got err: %v", err)
		}
	})

	t.Run("TerminalComparisonForeignPrefixEarlierServedCommandsPreserved", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.SetUserPermission("gke-labs", "eval-runner", "reviewer", "write")

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("ghp_test")},
		}

		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://new-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		foreignCandLog := "gs://old-bucket/runs/cand-sha/task1/inspect.json"
		foreignBaseLog := "gs://old-bucket/runs/base-sha/task1/inspect.json"

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-served", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task1",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: foreignBaseLog,
					},
				},
			},
		}
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-served", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task1",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: foreignCandLog,
					},
				},
			},
		}

		compTime := metav1.NewTime(now.Add(-30 * time.Minute))
		verdictMsg := "Evaluated 1 task(s): 1 improved"
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-foreign-served",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-served"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-served"},
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     99,
					HeadSHA:    "cand-sha",
					BaseSHA:    "base-sha",
				},
				ExtraSamples: 4,
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:               v1alpha1.ComparisonPhaseCompleted,
				Verdict:             v1alpha1.ComparisonVerdictImproved,
				Message:             verdictMsg,
				CompletionTime:      &compTime,
				ExtraSamplesApplied: 4,
				Reporting: &v1alpha1.ComparisonReportingStatus{
					CommentID: 7777,
				},
				HandledCommands: []v1alpha1.ComparisonHandledCommand{
					{
						CommentID:  1111,
						Login:      "reviewer",
						Requested:  4,
						ReceivedAt: metav1.NewTime(now.Add(-40 * time.Minute)),
						HandledAt:  &metav1.Time{Time: now.Add(-35 * time.Minute)},
						Outcome:    "applied",
					},
				},
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  8888,
						Login:      "reviewer",
						Requested:  8,
						ReceivedAt: metav1.NewTime(now),
					},
				},
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:         "task1",
						Label:      v1alpha1.ComparisonTaskLabelImproved,
						SamplesRun: 8,
					},
				},
				Batches: []v1alpha1.ComparisonBatchStatus{
					{
						Index:        0,
						Phase:        v1alpha1.ComparisonPhaseCompleted,
						CandidateRun: "cand-run-served",
						BaselineRun:  "base-run-served",
					},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp, tokenSecret).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ComparisonTTL:       ttl,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
			Now: func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-foreign-served"}}

		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
		if res.Requeue {
			t.Errorf("expected Requeue false, got true")
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if updatedComp.Spec.ExtraSamples != 12 {
			t.Errorf("expected spec.extraSamples 12, got %d", updatedComp.Spec.ExtraSamples)
		}
		if updatedComp.Status.ExtraSamplesApplied != 12 {
			t.Errorf("expected status.extraSamplesApplied 12, got %d", updatedComp.Status.ExtraSamplesApplied)
		}
		if len(updatedComp.Status.HandledCommands) != 2 {
			t.Fatalf("expected 2 handled commands, got %d", len(updatedComp.Status.HandledCommands))
		}
		// Earlier command 1111 MUST remain "applied"
		if updatedComp.Status.HandledCommands[0].CommentID != 1111 || updatedComp.Status.HandledCommands[0].Outcome != "applied" {
			t.Errorf("expected command 1111 to remain 'applied', got %+v", updatedComp.Status.HandledCommands[0])
		}
		// New unserviceable command 8888 MUST be flipped
		if updatedComp.Status.HandledCommands[1].CommentID != 8888 || updatedComp.Status.HandledCommands[1].Outcome != "unserviceable: artifact prefix mismatch" {
			t.Errorf("expected command 8888 to be 'unserviceable: artifact prefix mismatch', got %+v", updatedComp.Status.HandledCommands[1])
		}

		// Verify exactly one unserviceable note was posted (for 8888, none for 1111)
		var noteComments []string
		for _, c := range server.Comments() {
			if strings.Contains(c.Body, "Extra samples cannot be run: artifact prefix mismatch.") {
				noteComments = append(noteComments, c.Body)
			}
		}
		if len(noteComments) != 1 {
			t.Fatalf("expected exactly 1 unserviceable note comment on PR, got %d: %+v", len(noteComments), server.Comments())
		}
	})

	t.Run("TerminalComparisonForeignPrefixSpecEditPreservesServedCommand", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.SetUserPermission("gke-labs", "eval-runner", "reviewer", "write")

		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("ghp_test")},
		}

		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://new-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		foreignCandLog := "gs://old-bucket/runs/cand-sha/task1/inspect.json"
		foreignBaseLog := "gs://old-bucket/runs/base-sha/task1/inspect.json"

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-specedit", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task1",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: foreignBaseLog,
					},
				},
			},
		}
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-specedit", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task1",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: foreignCandLog,
					},
				},
			},
		}

		compTime := metav1.NewTime(now.Add(-30 * time.Minute))
		verdictMsg := "Evaluated 1 task(s): 1 improved"
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-foreign-specedit",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-specedit"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-specedit"},
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     99,
					HeadSHA:    "cand-sha",
					BaseSHA:    "base-sha",
				},
				ExtraSamples: 12, // Spec edited to 12 while ExtraSamplesApplied was 4
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:               v1alpha1.ComparisonPhaseCompleted,
				Verdict:             v1alpha1.ComparisonVerdictImproved,
				Message:             verdictMsg,
				CompletionTime:      &compTime,
				ExtraSamplesApplied: 4,
				Reporting: &v1alpha1.ComparisonReportingStatus{
					CommentID: 7777,
				},
				HandledCommands: []v1alpha1.ComparisonHandledCommand{
					{
						CommentID:  1111,
						Login:      "reviewer",
						Requested:  4,
						ReceivedAt: metav1.NewTime(now.Add(-40 * time.Minute)),
						HandledAt:  &metav1.Time{Time: now.Add(-35 * time.Minute)}, // before CompletionTime
						Outcome:    "applied",
					},
				},
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:         "task1",
						Label:      v1alpha1.ComparisonTaskLabelImproved,
						SamplesRun: 8,
					},
				},
				Batches: []v1alpha1.ComparisonBatchStatus{
					{
						Index:        0,
						Phase:        v1alpha1.ComparisonPhaseCompleted,
						CandidateRun: "cand-run-specedit",
						BaselineRun:  "base-run-specedit",
					},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp, tokenSecret).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ComparisonTTL:       ttl,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
			Now: func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-foreign-specedit"}}

		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
		if res.Requeue {
			t.Errorf("expected Requeue false, got true")
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if updatedComp.Spec.ExtraSamples != 12 {
			t.Errorf("expected spec.extraSamples 12, got %d", updatedComp.Spec.ExtraSamples)
		}
		if updatedComp.Status.ExtraSamplesApplied != 12 {
			t.Errorf("expected status.extraSamplesApplied 12, got %d", updatedComp.Status.ExtraSamplesApplied)
		}
		if updatedComp.Status.Message != verdictMsg {
			t.Errorf("expected verdict message preserved %q, got %q", verdictMsg, updatedComp.Status.Message)
		}
		if len(updatedComp.Status.HandledCommands) != 1 {
			t.Fatalf("expected 1 handled command, got %d", len(updatedComp.Status.HandledCommands))
		}
		// Stale served command 1111 MUST remain untouched as "applied"
		if updatedComp.Status.HandledCommands[0].CommentID != 1111 || updatedComp.Status.HandledCommands[0].Outcome != "applied" {
			t.Errorf("expected command 1111 to remain untouched 'applied', got %+v", updatedComp.Status.HandledCommands[0])
		}

		cond := meta.FindStatusCondition(updatedComp.Status.Conditions, v1alpha1.ConditionTypeExtraSamples)
		if cond == nil {
			t.Fatalf("expected ConditionTypeExtraSamples to be present")
		}
		if cond.Status != metav1.ConditionFalse || cond.Reason != "ArtifactPrefixMismatch" {
			t.Errorf("expected condition False/ArtifactPrefixMismatch, got %s/%s", cond.Status, cond.Reason)
		}

		// Verify zero unserviceable notes were posted to the PR because the extra samples came from a spec edit, not a new command
		var noteComments []string
		for _, c := range server.Comments() {
			if strings.Contains(c.Body, "Extra samples cannot be run: artifact prefix mismatch.") {
				noteComments = append(noteComments, c.Body)
			}
		}
		if len(noteComments) != 0 {
			t.Fatalf("expected 0 unserviceable note comments on PR, got %d: %+v", len(noteComments), noteComments)
		}
	})

	t.Run("TerminalComparisonForeignPrefixPastTTLRequeuesForDelete", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://new-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		foreignCandLog := "gs://old-bucket/runs/cand-sha/task1/inspect.json"
		foreignBaseLog := "gs://old-bucket/runs/base-sha/task1/inspect.json"

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-pastttl", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task1",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: foreignBaseLog,
					},
				},
			},
		}
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-pastttl", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task1",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: foreignCandLog,
					},
				},
			},
		}

		compTime := metav1.NewTime(now.Add(-2 * time.Hour))
		verdictMsg := "Evaluated 1 task(s): 1 improved"
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-foreign-pastttl",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-3 * time.Hour)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:     v1alpha1.ComparisonRunRef{Run: "base-run-pastttl"},
				Candidate:    v1alpha1.ComparisonRunRef{Run: "cand-run-pastttl"},
				ExtraSamples: 8,
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:               v1alpha1.ComparisonPhaseCompleted,
				Verdict:             v1alpha1.ComparisonVerdictImproved,
				Message:             verdictMsg,
				CompletionTime:      &compTime,
				ExtraSamplesApplied: 4,
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:         "task1",
						Label:      v1alpha1.ComparisonTaskLabelImproved,
						SamplesRun: 8,
					},
				},
				Batches: []v1alpha1.ComparisonBatchStatus{
					{
						Index:        0,
						Phase:        v1alpha1.ComparisonPhaseCompleted,
						CandidateRun: "cand-run-pastttl",
						BaselineRun:  "base-run-pastttl",
					},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ComparisonTTL:       1 * time.Hour, // age (2h) > TTL (1h)
			ControllerNamespace: "eval-runner",
			Now:                 func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-foreign-pastttl"}}

		// Reconcile 1: should record unserviceable outcome and schedule a small RequeueAfter for deletion
		res1, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error on reconcile 1: %v", err)
		}
		if res1.RequeueAfter != time.Second {
			t.Errorf("expected small RequeueAfter (%v) when past TTL, got %v", time.Second, res1.RequeueAfter)
		}

		// Reconcile 2: early TTL check should now delete the object
		res2, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error on reconcile 2: %v", err)
		}
		if res2.RequeueAfter != 0 {
			t.Errorf("expected RequeueAfter 0 on deletion reconcile, got %v", res2.RequeueAfter)
		}

		var expiredComp v1alpha1.Comparison
		err = cl.Get(t.Context(), req.NamespacedName, &expiredComp)
		if !apierrors.IsNotFound(err) {
			t.Fatalf("expected comparison to be deleted by early TTL check, got err: %v", err)
		}
	})

	t.Run("TerminalComparisonForeignPrefixDirectExtraSamplesDeletedByTTL", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://new-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		foreignCandLog := "gs://old-bucket/runs/cand-sha/task1/inspect.json"
		foreignBaseLog := "gs://old-bucket/runs/base-sha/task1/inspect.json"

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-direct", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task1",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: foreignBaseLog,
					},
				},
			},
		}
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-direct", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-sha"},
				Model: "mockllm/model",
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task1",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: foreignCandLog,
					},
				},
			},
		}

		compTime := metav1.NewTime(now.Add(-30 * time.Minute))
		verdictMsg := "Evaluated 1 task(s): 1 improved"
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-foreign-direct",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:     v1alpha1.ComparisonRunRef{Run: "base-run-direct"},
				Candidate:    v1alpha1.ComparisonRunRef{Run: "cand-run-direct"},
				ExtraSamples: 8,
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:               v1alpha1.ComparisonPhaseCompleted,
				Verdict:             v1alpha1.ComparisonVerdictImproved,
				Message:             verdictMsg,
				CompletionTime:      &compTime,
				ExtraSamplesApplied: 0,
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:         "task1",
						Label:      v1alpha1.ComparisonTaskLabelImproved,
						SamplesRun: 8,
					},
				},
				Batches: []v1alpha1.ComparisonBatchStatus{
					{
						Index:        0,
						Phase:        v1alpha1.ComparisonPhaseCompleted,
						CandidateRun: "cand-run-direct",
						BaselineRun:  "base-run-direct",
					},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		currentNow := now
		reconciler := &ComparisonReconciler{
			Client:        cl,
			Scheme:        scheme,
			ArtifactSink:  sink,
			ComparisonTTL: ttl,
			Now:           func() time.Time { return currentNow },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-foreign-direct"}}

		// Reconcile 1: detects prefix mismatch, sets extraSamplesApplied = 8, preserves verdict message, does not requeue
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
		if res.Requeue {
			t.Errorf("expected Requeue false, got true")
		}
		expectedRequeue := ttl - 30*time.Minute
		if res.RequeueAfter != expectedRequeue {
			t.Errorf("expected RequeueAfter %v, got %v", expectedRequeue, res.RequeueAfter)
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if updatedComp.Status.ExtraSamplesApplied != 8 {
			t.Errorf("expected status.extraSamplesApplied 8, got %d", updatedComp.Status.ExtraSamplesApplied)
		}
		if updatedComp.Status.Message != verdictMsg {
			t.Errorf("expected status.message %q (preserved), got %q", verdictMsg, updatedComp.Status.Message)
		}
		cond := meta.FindStatusCondition(updatedComp.Status.Conditions, v1alpha1.ConditionTypeExtraSamples)
		if cond == nil {
			t.Fatalf("expected ConditionTypeExtraSamples condition on status")
		}
		if cond.Status != metav1.ConditionFalse {
			t.Errorf("expected condition status False, got %s", cond.Status)
		}

		// Reconcile 2 (after TTL expires): advance clock past TTL
		currentNow = currentNow.Add(45 * time.Minute)
		res2, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error on expiry: %v", err)
		}
		if res2.RequeueAfter != 0 {
			t.Errorf("expected RequeueAfter 0 on deletion, got %v", res2.RequeueAfter)
		}

		var expiredComp v1alpha1.Comparison
		err = cl.Get(t.Context(), req.NamespacedName, &expiredComp)
		if !apierrors.IsNotFound(err) {
			t.Fatalf("expected comparison to be deleted by TTL, got err: %v", err)
		}
	})
}

func TestComparisonReconciler_TerminalComparisonNoEligibleTasks(t *testing.T) {
	scheme := setupScheme(t)

	now := time.Now().UTC().Truncate(time.Second)
	ttl := 1 * time.Hour

	tokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
		Data:       map[string][]byte{"token": []byte("ghp_test")},
	}

	t.Run("DatasetExhausted_SettlesExtraSamplesAndDeletedByTTL", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.SetUserPermission("gke-labs", "eval-runner", "reviewer", "write")

		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskID := "task1"
		candLog := "gs://test-bucket/runs/cand-commit/" + taskID + "/log.json"
		baseLog := "gs://test-bucket/runs/base-commit/" + taskID + "/log.json"

		var candScores []stats.SampleScore
		var baseScores []stats.SampleScore
		for i := 1; i <= 8; i++ {
			candScores = append(candScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
			baseScores = append(baseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
		}
		// Dataset has 8 possible samples total; all 8 ran.
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLogWithDataset(taskID, candScores, 8),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLogWithDataset(taskID, baseScores, 8),
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     baseLog,
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     candLog,
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		compTime := metav1.NewTime(now.Add(-30 * time.Minute)) // 30m ago, within ttl (1h)
		verdictMsg := "Evaluated 1 task(s): 1 improved"
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-dataset-exhausted-8",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     99,
					HeadSHA:    "cand-commit",
					BaseSHA:    "base-commit",
				},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 8,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:               v1alpha1.ComparisonPhaseCompleted,
				Verdict:             v1alpha1.ComparisonVerdictImproved,
				Message:             verdictMsg,
				CompletionTime:      &compTime,
				ExtraSamplesApplied: 0,
				Reporting: &v1alpha1.ComparisonReportingStatus{
					CommentID: 7777,
				},
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  8888,
						Login:      "reviewer",
						Requested:  8,
						ReceivedAt: metav1.NewTime(now),
					},
				},
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:              taskID,
						Label:           v1alpha1.ComparisonTaskLabelImproved,
						Decision:        v1alpha1.ComparisonTaskLabelImproved,
						SamplesRun:      8,
						PossibleSamples: 8,
					},
				},
				Batches: []v1alpha1.ComparisonBatchStatus{
					{
						Index:        1,
						Phase:        v1alpha1.ComparisonPhaseCompleted,
						CandidateRun: "cand-run",
						BaselineRun:  "base-run",
						SampleRange:  &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp, tokenSecret).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		currentNow := now
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ComparisonTTL:       ttl,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
			Now: func() time.Time { return currentNow },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}

		// Reconcile 1: processes /eval more 8, detects dataset exhausted, settles extraSamplesApplied, sets condition, posts 1 PR note, TTL requeue
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
		if res.Requeue {
			t.Errorf("expected Requeue false, got true")
		}
		expectedRequeue := ttl - 30*time.Minute
		if res.RequeueAfter != expectedRequeue {
			t.Errorf("expected RequeueAfter %v, got %v", expectedRequeue, res.RequeueAfter)
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if updatedComp.Spec.ExtraSamples != 8 {
			t.Errorf("expected spec.extraSamples 8, got %d", updatedComp.Spec.ExtraSamples)
		}
		if updatedComp.Status.ExtraSamplesApplied != 8 {
			t.Errorf("expected status.extraSamplesApplied 8, got %d", updatedComp.Status.ExtraSamplesApplied)
		}
		if updatedComp.Status.Verdict != v1alpha1.ComparisonVerdictImproved {
			t.Errorf("expected verdict Improved preserved, got %s", updatedComp.Status.Verdict)
		}
		if !strings.Contains(updatedComp.Status.Message, "improved") {
			t.Errorf("expected status.message to preserve verdict, got %q", updatedComp.Status.Message)
		}
		if len(updatedComp.Status.HandledCommands) != 1 {
			t.Fatalf("expected 1 handled command, got %d", len(updatedComp.Status.HandledCommands))
		}
		if updatedComp.Status.HandledCommands[0].Outcome != "unserviceable: no eligible tasks" {
			t.Errorf("expected handled command outcome 'unserviceable: no eligible tasks', got %q", updatedComp.Status.HandledCommands[0].Outcome)
		}

		cond := meta.FindStatusCondition(updatedComp.Status.Conditions, v1alpha1.ConditionTypeExtraSamples)
		if cond == nil {
			t.Fatalf("expected ConditionTypeExtraSamples on status.conditions")
		}
		if cond.Status != metav1.ConditionFalse {
			t.Errorf("expected condition status False, got %s", cond.Status)
		}
		if cond.Reason != "NoEligibleTasks" {
			t.Errorf("expected condition reason NoEligibleTasks, got %s", cond.Reason)
		}
		if !strings.Contains(cond.Message, "extra samples cannot be run: no eligible tasks") {
			t.Errorf("expected condition message to contain prefix, got %q", cond.Message)
		}
		if !strings.Contains(cond.Message, "dataset exhausted: task1") {
			t.Errorf("expected condition message to contain 'dataset exhausted: task1', got %q", cond.Message)
		}

		// Verify exactly one unserviceable note was posted to PR
		var noteComments []string
		for _, c := range server.Comments() {
			if strings.Contains(c.Body, "Extra samples cannot be run: no eligible tasks.") {
				noteComments = append(noteComments, c.Body)
			}
		}
		if len(noteComments) != 1 {
			t.Fatalf("expected exactly 1 unserviceable note comment on PR, got %d: %+v", len(noteComments), server.Comments())
		}
		expectedNote := "Extra samples cannot be run: no eligible tasks."
		if noteComments[0] != expectedNote {
			t.Errorf("expected comment body %q, got %q", expectedNote, noteComments[0])
		}

		// Reconcile 2 (after TTL expires): advance clock past TTL
		currentNow = currentNow.Add(35 * time.Minute)
		reqCountBefore := len(server.RequestLog())
		res2, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error on expiry: %v", err)
		}
		if res2.RequeueAfter != 0 {
			t.Errorf("expected RequeueAfter 0 on deletion, got %v", res2.RequeueAfter)
		}
		if len(server.RequestLog()) != reqCountBefore {
			t.Errorf("expected zero new GitHub requests on reconcile 2, got %d new requests", len(server.RequestLog())-reqCountBefore)
		}

		var expiredComp v1alpha1.Comparison
		err = cl.Get(t.Context(), req.NamespacedName, &expiredComp)
		if !apierrors.IsNotFound(err) {
			t.Fatalf("expected comparison to be deleted by TTL, got err: %v", err)
		}
	})

	t.Run("PossibleSamplesExhausted_SettlesExtraSamplesAndDeletedByTTL", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.SetUserPermission("gke-labs", "eval-runner", "reviewer", "write")

		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskID := "task1"
		candLog := "gs://test-bucket/runs/cand-commit/" + taskID + "/log.json"
		baseLog := "gs://test-bucket/runs/base-commit/" + taskID + "/log.json"

		var candScores []stats.SampleScore
		var baseScores []stats.SampleScore
		for i := 1; i <= 4; i++ {
			candScores = append(candScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
			baseScores = append(baseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
		}
		// Dataset has 4 possible samples total; all 4 ran.
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLogWithDataset(taskID, candScores, 4),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskID+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLogWithDataset(taskID, baseScores, 4),
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     baseLog,
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskID,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     candLog,
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		compTime := metav1.NewTime(now.Add(-30 * time.Minute))
		verdictMsg := "INCONCLUSIVE: 1 task(s) inconclusive; more samples recommended"
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-dataset-exhausted",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     99,
					HeadSHA:    "cand-commit",
					BaseSHA:    "base-commit",
				},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 16,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:               v1alpha1.ComparisonPhaseCompleted,
				Verdict:             v1alpha1.ComparisonVerdictInconclusive,
				Message:             verdictMsg,
				CompletionTime:      &compTime,
				ExtraSamplesApplied: 0,
				Reporting: &v1alpha1.ComparisonReportingStatus{
					CommentID: 7777,
				},
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  8888,
						Login:      "reviewer",
						Requested:  8,
						ReceivedAt: metav1.NewTime(now),
					},
				},
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:              taskID,
						Label:           v1alpha1.ComparisonTaskLabelInconclusive,
						Decision:        v1alpha1.ComparisonTaskLabelInconclusive,
						SamplesRun:      4,
						PossibleSamples: 4,
					},
				},
				Batches: []v1alpha1.ComparisonBatchStatus{
					{
						Index:        1,
						Phase:        v1alpha1.ComparisonPhaseCompleted,
						CandidateRun: "cand-run",
						BaselineRun:  "base-run",
						SampleRange:  &v1alpha1.SampleRange{Start: 1, End: 4},
					},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp, tokenSecret).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		currentNow := now
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ComparisonTTL:       ttl,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
			Now: func() time.Time { return currentNow },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}

		// Reconcile 1: processes /eval more 8, detects dataset exhausted, settles extraSamplesApplied, sets condition, posts 1 PR note, TTL requeue
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}
		if res.Requeue {
			t.Errorf("expected Requeue false, got true")
		}
		expectedRequeue := ttl - 30*time.Minute
		if res.RequeueAfter != expectedRequeue {
			t.Errorf("expected RequeueAfter %v, got %v", expectedRequeue, res.RequeueAfter)
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if updatedComp.Spec.ExtraSamples != 8 {
			t.Errorf("expected spec.extraSamples 8, got %d", updatedComp.Spec.ExtraSamples)
		}
		if updatedComp.Status.ExtraSamplesApplied != 8 {
			t.Errorf("expected status.extraSamplesApplied 8, got %d", updatedComp.Status.ExtraSamplesApplied)
		}
		if updatedComp.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("expected verdict Inconclusive preserved, got %s", updatedComp.Status.Verdict)
		}
		if !strings.Contains(updatedComp.Status.Message, "INCONCLUSIVE") {
			t.Errorf("expected status.message to preserve verdict, got %q", updatedComp.Status.Message)
		}
		if len(updatedComp.Status.HandledCommands) != 1 {
			t.Fatalf("expected 1 handled command, got %d", len(updatedComp.Status.HandledCommands))
		}
		if updatedComp.Status.HandledCommands[0].Outcome != "unserviceable: no eligible tasks" {
			t.Errorf("expected handled command outcome 'unserviceable: no eligible tasks', got %q", updatedComp.Status.HandledCommands[0].Outcome)
		}

		cond := meta.FindStatusCondition(updatedComp.Status.Conditions, v1alpha1.ConditionTypeExtraSamples)
		if cond == nil {
			t.Fatalf("expected ConditionTypeExtraSamples on status.conditions")
		}
		if cond.Status != metav1.ConditionFalse {
			t.Errorf("expected condition status False, got %s", cond.Status)
		}
		if cond.Reason != "NoEligibleTasks" {
			t.Errorf("expected condition reason NoEligibleTasks, got %s", cond.Reason)
		}
		if !strings.Contains(cond.Message, "extra samples cannot be run: no eligible tasks") {
			t.Errorf("expected condition message to contain prefix, got %q", cond.Message)
		}
		if !strings.Contains(cond.Message, "dataset exhausted: task1") {
			t.Errorf("expected condition message to contain 'dataset exhausted: task1', got %q", cond.Message)
		}

		// Verify exactly one unserviceable note was posted to PR
		var noteComments []string
		for _, c := range server.Comments() {
			if strings.Contains(c.Body, "Extra samples cannot be run: no eligible tasks.") {
				noteComments = append(noteComments, c.Body)
			}
		}
		if len(noteComments) != 1 {
			t.Fatalf("expected exactly 1 unserviceable note comment on PR, got %d: %+v", len(noteComments), server.Comments())
		}
		expectedNote := "Extra samples cannot be run: no eligible tasks."
		if noteComments[0] != expectedNote {
			t.Errorf("expected comment body %q, got %q", expectedNote, noteComments[0])
		}

		// Reconcile 2 (after TTL expires): advance clock past TTL
		currentNow = currentNow.Add(35 * time.Minute)
		reqCountBefore := len(server.RequestLog())
		res2, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error on expiry: %v", err)
		}
		if res2.RequeueAfter != 0 {
			t.Errorf("expected RequeueAfter 0 on deletion, got %v", res2.RequeueAfter)
		}
		if len(server.RequestLog()) != reqCountBefore {
			t.Errorf("expected zero new GitHub requests on reconcile 2, got %d new requests", len(server.RequestLog())-reqCountBefore)
		}

		var expiredComp v1alpha1.Comparison
		err = cl.Get(t.Context(), req.NamespacedName, &expiredComp)
		if !apierrors.IsNotFound(err) {
			t.Fatalf("expected comparison to be deleted by TTL, got err: %v", err)
		}
	})

	t.Run("MixedTasks_ListsAllReasons", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.SetUserPermission("gke-labs", "eval-runner", "reviewer", "write")

		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		task1 := "task1"
		task2 := "task2"

		var t1CandScores, t1BaseScores []stats.SampleScore
		for i := 1; i <= 4; i++ {
			t1CandScores = append(t1CandScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
			t1BaseScores = append(t1BaseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
		}
		// task1 is dataset exhausted (4 of 4 possible samples)
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLogWithDataset(task1, t1CandScores, 4),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+task1+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLogWithDataset(task1, t1BaseScores, 4),
		}

		var t2CandScores, t2BaseScores []stats.SampleScore
		for i := 1; i <= 8; i++ {
			t2CandScores = append(t2CandScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.5})
			t2BaseScores = append(t2BaseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.5})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+task2+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task2, t2CandScores),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+task2+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLog(task2, t2BaseScores),
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          task1,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + task1 + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
					{
						ID:          task2,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + task2 + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          task1,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + task1 + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
					{
						ID:          task2,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + task2 + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		// Batch 2: task2 evaluated and failed
		baseRunB2 := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run-b2", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "mockllm/model",
				Tasks:       []string{task2},
				SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          task2,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + task2 + "/log-b2.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
					},
				},
			},
		}
		candRunB2 := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run-b2", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "mockllm/model",
				Tasks:       []string{task2},
				SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseFailed,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          task2,
						Phase:       v1alpha1.TaskPhaseFailed,
						Message:     "task failed",
						Samples:     0,
						SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
					},
				},
			},
		}

		compTime := metav1.NewTime(now.Add(-30 * time.Minute))
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-mixed",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     99,
					HeadSHA:    "cand-commit",
					BaseSHA:    "base-commit",
				},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 16,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:               v1alpha1.ComparisonPhaseCompleted,
				Verdict:             v1alpha1.ComparisonVerdictInconclusive,
				Message:             "Evaluated 2 task(s)",
				CompletionTime:      &compTime,
				ExtraSamplesApplied: 0,
				Reporting: &v1alpha1.ComparisonReportingStatus{
					CommentID: 7777,
				},
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  8888,
						Login:      "reviewer",
						Requested:  8,
						ReceivedAt: metav1.NewTime(now),
					},
				},
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:              task1,
						Label:           v1alpha1.ComparisonTaskLabelInconclusive,
						Decision:        v1alpha1.ComparisonTaskLabelInconclusive,
						SamplesRun:      4,
						PossibleSamples: 4,
					},
					{
						ID:         task2,
						Label:      v1alpha1.ComparisonTaskLabelInconclusive,
						Decision:   v1alpha1.ComparisonTaskLabelInconclusive,
						SamplesRun: 8,
					},
				},
				Batches: []v1alpha1.ComparisonBatchStatus{
					{
						Index:        1,
						Phase:        v1alpha1.ComparisonPhaseCompleted,
						CandidateRun: "cand-run",
						BaselineRun:  "base-run",
						SampleRange:  &v1alpha1.SampleRange{Start: 1, End: 8},
					},
					{
						Index:        2,
						Phase:        v1alpha1.ComparisonPhaseCompleted,
						CandidateRun: "cand-run-b2",
						BaselineRun:  "base-run-b2",
						SampleRange:  &v1alpha1.SampleRange{Start: 9, End: 16},
					},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, baseRunB2, candRunB2, comp, tokenSecret).
			WithStatusSubresource(baseRun, candRun, baseRunB2, candRunB2, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ComparisonTTL:       ttl,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
			Now: func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}

		cond := meta.FindStatusCondition(updatedComp.Status.Conditions, v1alpha1.ConditionTypeExtraSamples)
		if cond == nil {
			t.Fatalf("expected ConditionTypeExtraSamples condition on status")
		}
		if cond.Reason != "NoEligibleTasks" {
			t.Errorf("expected reason NoEligibleTasks, got %s", cond.Reason)
		}
		if !strings.Contains(cond.Message, "dataset exhausted: task1") {
			t.Errorf("expected message to contain 'dataset exhausted: task1', got %q", cond.Message)
		}
		if !strings.Contains(cond.Message, "task failed: task2") {
			t.Errorf("expected message to contain 'task failed: task2', got %q", cond.Message)
		}
	})

	t.Run("PartiallyEligible_ExtraBatchRunsForEligibleTaskOnly", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.SetUserPermission("gke-labs", "eval-runner", "reviewer", "write")

		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		taskExhausted := "task-exhausted"
		taskEligible := "task-eligible"

		var t1Scores, t2Scores []stats.SampleScore
		for i := 1; i <= 4; i++ {
			t1Scores = append(t1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		}
		for i := 1; i <= 8; i++ {
			t2Scores = append(t2Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		}

		// taskExhausted has 4 possible samples (all 4 ran)
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskExhausted+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLogWithDataset(taskExhausted, t1Scores, 4),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskExhausted+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLogWithDataset(taskExhausted, t1Scores, 4),
		}

		// taskEligible has 16 possible samples (only 8 ran)
		fakeGCS.Objects["test-bucket/runs/cand-commit/"+taskEligible+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLogWithDataset(taskEligible, t2Scores, 16),
		}
		fakeGCS.Objects["test-bucket/runs/base-commit/"+taskEligible+"/log.json"] = fakes.FakeObject{
			Data: makeSyntheticInspectLogWithDataset(taskEligible, t2Scores, 16),
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskExhausted,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskExhausted + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
					{
						ID:          taskEligible,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/base-commit/" + taskEligible + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       "mockllm/model",
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:          taskExhausted,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskExhausted + "/log.json",
						Samples:     4,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
					},
					{
						ID:          taskEligible,
						Phase:       v1alpha1.TaskPhaseCompleted,
						LogPath:     "gs://test-bucket/runs/cand-commit/" + taskEligible + "/log.json",
						Samples:     8,
						SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		compTime := metav1.NewTime(now.Add(-30 * time.Minute))
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "comp-partially-eligible",
				Namespace:         "eval-runner",
				CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
			},
			Spec: v1alpha1.ComparisonSpec{
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     99,
					HeadSHA:    "cand-commit",
					BaseSHA:    "base-commit",
				},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8,
					MaxSamplesPerTask: 16,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:               v1alpha1.ComparisonPhaseCompleted,
				Verdict:             v1alpha1.ComparisonVerdictInconclusive,
				Message:             "Evaluated 2 task(s)",
				CompletionTime:      &compTime,
				ExtraSamplesApplied: 0,
				Reporting: &v1alpha1.ComparisonReportingStatus{
					CommentID: 7777,
				},
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  8888,
						Login:      "reviewer",
						Requested:  8,
						ReceivedAt: metav1.NewTime(now),
					},
				},
				Tasks: []v1alpha1.ComparisonTaskStatus{
					{
						ID:              taskExhausted,
						Label:           v1alpha1.ComparisonTaskLabelInconclusive,
						Decision:        v1alpha1.ComparisonTaskLabelInconclusive,
						SamplesRun:      4,
						PossibleSamples: 4,
					},
					{
						ID:              taskEligible,
						Label:           v1alpha1.ComparisonTaskLabelInconclusive,
						Decision:        v1alpha1.ComparisonTaskLabelInconclusive,
						SamplesRun:      8,
						PossibleSamples: 16,
					},
				},
				Batches: []v1alpha1.ComparisonBatchStatus{
					{
						Index:        1,
						Phase:        v1alpha1.ComparisonPhaseCompleted,
						CandidateRun: "cand-run",
						BaselineRun:  "base-run",
						SampleRange:  &v1alpha1.SampleRange{Start: 1, End: 8},
					},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseRun, candRun, comp, tokenSecret).
			WithStatusSubresource(baseRun, candRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ComparisonTTL:       ttl,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
			Now: func() time.Time { return now },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error: %v", err)
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}

		// Comparison transitions to Running because extra batch was scheduled
		if updatedComp.Status.Phase != v1alpha1.ComparisonPhaseRunning {
			t.Errorf("expected phase Running, got %s", updatedComp.Status.Phase)
		}
		if len(updatedComp.Status.Batches) != 2 {
			t.Fatalf("expected 2 batches scheduled, got %d", len(updatedComp.Status.Batches))
		}
		if !strings.Contains(updatedComp.Status.Message, "Running extra batch 2 (1 tasks)") {
			t.Errorf("expected message to indicate extra batch with 1 task, got %q", updatedComp.Status.Message)
		}

		// ExtraSamples condition should be set to True/Scheduled
		cond := meta.FindStatusCondition(updatedComp.Status.Conditions, v1alpha1.ConditionTypeExtraSamples)
		if cond == nil {
			t.Fatalf("expected ExtraSamples condition on scheduled comparison")
		}
		if cond.Status != metav1.ConditionTrue || cond.Reason != v1alpha1.ConditionReasonExtraSamplesScheduled {
			t.Errorf("expected ExtraSamples=True/Scheduled, got %s/%s", cond.Status, cond.Reason)
		}

		// Verify zero unserviceable note comments on PR
		for _, c := range server.Comments() {
			if strings.Contains(c.Body, "Extra samples cannot be run") {
				t.Errorf("unexpected unserviceable note comment on PR: %s", c.Body)
			}
		}
	})
}

func TestComparisonReconciler_ExtraSamples_ConditionResetOnServiceableRequest(t *testing.T) {
	testCases := []struct {
		name           string
		initialReason  string
		initialMessage string
	}{
		{
			name:           "NoEligibleTasks",
			initialReason:  v1alpha1.ConditionReasonExtraSamplesNoEligibleTasks,
			initialMessage: "extra samples cannot be run: no eligible tasks (dataset exhausted: task1)",
		},
		{
			name:           "ArtifactPrefixMismatch",
			initialReason:  v1alpha1.ConditionReasonExtraSamplesArtifactMismatch,
			initialMessage: "extra samples cannot be run: artifact prefix mismatch (candidate log path \"gs://other-bucket/log.json\" does not match commit prefix \"runs/cand-commit/\")",
		},
		{
			name:           "ArtifactNotFound",
			initialReason:  v1alpha1.ConditionReasonExtraSamplesArtifactNotFound,
			initialMessage: "extra samples cannot be run: artifact not found",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			taskID := "task1"
			now := time.Now().Truncate(time.Second)
			ttl := 1 * time.Hour

			server := github.NewFakeGitHubServer()
			defer server.Close()
			server.SetUserPermission("gke-labs", "eval-runner", "reviewer", "write")

			fakeGCS := fakes.NewFakeStorageClient()
			sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
			if err != nil {
				t.Fatalf("failed to create GCS sink: %v", err)
			}
			scheme := setupScheme(t)

			candCommit := "cand-commit"
			baseCommit := "base-commit"

			// Seed batch 1 inspect logs (8 samples, dataset 16)
			var b1CandScores, b1BaseScores []stats.SampleScore
			for i := 1; i <= 8; i++ {
				b1CandScores = append(b1CandScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
				b1BaseScores = append(b1BaseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
			}
			fakeGCS.Objects["test-bucket/runs/cand-commit/task1/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLogWithDataset(taskID, b1CandScores, 16)}
			fakeGCS.Objects["test-bucket/runs/base-commit/task1/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLogWithDataset(taskID, b1BaseScores, 16)}

			// Seed batch 2 inspect logs (8 samples 9-16, dataset 16)
			var b2CandScores, b2BaseScores []stats.SampleScore
			for i := 9; i <= 16; i++ {
				b2CandScores = append(b2CandScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
				b2BaseScores = append(b2BaseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
			}
			fakeGCS.Objects["test-bucket/runs/cand-commit/task1/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLogWithDataset(taskID, b2CandScores, 16)}
			fakeGCS.Objects["test-bucket/runs/base-commit/task1/b2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLogWithDataset(taskID, b2BaseScores, 16)}

			tokenSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
				Data:       map[string][]byte{"token": []byte("test-token")},
			}

			baseRun := &v1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
				Spec: v1alpha1.RunSpec{
					Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: baseCommit},
					Model:       "mockllm/model",
					SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
				},
				Status: v1alpha1.RunStatus{
					Phase: v1alpha1.RunPhaseCompleted,
					Tasks: []v1alpha1.TaskStatus{
						{
							ID:          taskID,
							Phase:       v1alpha1.TaskPhaseCompleted,
							LogPath:     "gs://test-bucket/runs/base-commit/task1/b1.json",
							Samples:     8,
							SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
						},
					},
				},
			}

			candRun := &v1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
				Spec: v1alpha1.RunSpec{
					Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: candCommit},
					Model:       "mockllm/model",
					SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
				},
				Status: v1alpha1.RunStatus{
					Phase: v1alpha1.RunPhaseCompleted,
					Tasks: []v1alpha1.TaskStatus{
						{
							ID:          taskID,
							Phase:       v1alpha1.TaskPhaseCompleted,
							LogPath:     "gs://test-bucket/runs/cand-commit/task1/b1.json",
							Samples:     8,
							SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
						},
					},
				},
			}

			compTime := metav1.NewTime(now.Add(-30 * time.Minute))
			compName := fmt.Sprintf("comp-reset-%s", strings.ToLower(tc.name))
			comp := &v1alpha1.Comparison{
				ObjectMeta: metav1.ObjectMeta{
					Name:              compName,
					Namespace:         "eval-runner",
					CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)),
				},
				Spec: v1alpha1.ComparisonSpec{
					Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
					Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
					PullRequest: &v1alpha1.PullRequestSpec{
						Repository: "gke-labs/eval-runner",
						Number:     99,
						HeadSHA:    candCommit,
						BaseSHA:    baseCommit,
					},
					Sampling: &v1alpha1.ComparisonSamplingSpec{
						BatchSize:         8,
						MaxSamplesPerTask: 16,
					},
					ExtraSamples: 8,
				},
				Status: v1alpha1.ComparisonStatus{
					Phase:               v1alpha1.ComparisonPhaseCompleted,
					Verdict:             v1alpha1.ComparisonVerdictInconclusive,
					Message:             "Evaluated 1 task(s)",
					CompletionTime:      &compTime,
					ExtraSamplesApplied: 8,
					Reporting: &v1alpha1.ComparisonReportingStatus{
						CommentID: 7777,
					},
					Conditions: []metav1.Condition{
						{
							Type:               v1alpha1.ConditionTypeExtraSamples,
							Status:             metav1.ConditionFalse,
							Reason:             tc.initialReason,
							Message:            tc.initialMessage,
							LastTransitionTime: metav1.NewTime(now.Add(-30 * time.Minute)),
						},
					},
					PendingCommands: []v1alpha1.ComparisonPendingCommand{
						{
							CommentID:  8888,
							Login:      "reviewer",
							Requested:  8,
							ReceivedAt: metav1.NewTime(now),
						},
					},
					Tasks: []v1alpha1.ComparisonTaskStatus{
						{
							ID:              taskID,
							Label:           v1alpha1.ComparisonTaskLabelInconclusive,
							Decision:        v1alpha1.ComparisonTaskLabelInconclusive,
							SamplesRun:      8,
							PossibleSamples: 16,
						},
					},
					Batches: []v1alpha1.ComparisonBatchStatus{
						{
							Index:        1,
							Phase:        v1alpha1.ComparisonPhaseCompleted,
							CandidateRun: "cand-run",
							BaselineRun:  "base-run",
							SampleRange:  &v1alpha1.SampleRange{Start: 1, End: 8},
						},
					},
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(baseRun, candRun, comp, tokenSecret).
				WithStatusSubresource(baseRun, candRun, comp).
				Build()

			reconciler := &ComparisonReconciler{
				Client:              cl,
				Scheme:              scheme,
				ArtifactSink:        sink,
				ComparisonTTL:       ttl,
				ControllerNamespace: "eval-runner",
				GitHubTokenSecret:   "github-token",
				GitHubClientFactory: func(creds GitHubCredentials) github.Client {
					return server.Client(creds.Token)
				},
				Now: func() time.Time { return now },
			}

			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}

			// Reconcile 1: /eval more is authorized, extra batch is scheduled -> condition flips to True/Scheduled
			_, err = reconciler.Reconcile(t.Context(), req)
			if err != nil {
				t.Fatalf("unexpected reconcile error on scheduling extra batch: %v", err)
			}

			var runningComp v1alpha1.Comparison
			if err := cl.Get(t.Context(), req.NamespacedName, &runningComp); err != nil {
				t.Fatalf("failed to get running comparison: %v", err)
			}

			if runningComp.Status.Phase != v1alpha1.ComparisonPhaseRunning {
				t.Errorf("expected phase Running, got %s", runningComp.Status.Phase)
			}
			if len(runningComp.Status.Batches) != 2 {
				t.Fatalf("expected 2 batches scheduled, got %d", len(runningComp.Status.Batches))
			}
			if runningComp.Status.ExtraSamplesApplied != 16 {
				t.Errorf("expected ExtraSamplesApplied 16, got %d", runningComp.Status.ExtraSamplesApplied)
			}

			// Condition must flip to True / Scheduled while the batch runs
			condRunning := meta.FindStatusCondition(runningComp.Status.Conditions, v1alpha1.ConditionTypeExtraSamples)
			if condRunning == nil {
				t.Fatalf("expected ExtraSamples condition on running comparison")
			}
			if condRunning.Status != metav1.ConditionTrue {
				t.Errorf("expected ExtraSamples condition Status True, got %s", condRunning.Status)
			}
			if condRunning.Reason != v1alpha1.ConditionReasonExtraSamplesScheduled {
				t.Errorf("expected ExtraSamples condition Reason %s, got %s", v1alpha1.ConditionReasonExtraSamplesScheduled, condRunning.Reason)
			}

			// Complete batch 2 runs
			b2CandRunName := runningComp.Status.Batches[1].CandidateRun
			b2BaseRunName := runningComp.Status.Batches[1].BaselineRun

			var b2Cand, b2Base v1alpha1.Run
			if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b2CandRunName}, &b2Cand); err != nil {
				t.Fatalf("failed to get b2 candidate run %s: %v", b2CandRunName, err)
			}
			if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b2BaseRunName}, &b2Base); err != nil {
				t.Fatalf("failed to get b2 baseline run %s: %v", b2BaseRunName, err)
			}

			b2Cand.Status.Phase = v1alpha1.RunPhaseCompleted
			b2Cand.Status.Tasks = []v1alpha1.TaskStatus{
				{
					ID:          taskID,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     "gs://test-bucket/runs/cand-commit/task1/b2.json",
					Samples:     8,
					SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
				},
			}
			if err := cl.Status().Update(t.Context(), &b2Cand); err != nil {
				t.Fatalf("failed to update b2 candidate run: %v", err)
			}

			b2Base.Status.Phase = v1alpha1.RunPhaseCompleted
			b2Base.Status.Tasks = []v1alpha1.TaskStatus{
				{
					ID:          taskID,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     "gs://test-bucket/runs/base-commit/task1/b2.json",
					Samples:     8,
					SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
				},
			}
			if err := cl.Status().Update(t.Context(), &b2Base); err != nil {
				t.Fatalf("failed to update b2 baseline run: %v", err)
			}

			// Reconcile 2: batch 2 completes, comparison re-completes -> condition stays True/Scheduled
			_, err = reconciler.Reconcile(t.Context(), req)
			if err != nil {
				t.Fatalf("unexpected reconcile error on completing extra batch: %v", err)
			}

			var completedComp v1alpha1.Comparison
			if err := cl.Get(t.Context(), req.NamespacedName, &completedComp); err != nil {
				t.Fatalf("failed to get completed comparison: %v", err)
			}

			if completedComp.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
				t.Errorf("expected phase Completed after batch 2 completes, got %s", completedComp.Status.Phase)
			}
			if completedComp.Status.ExtraSamplesApplied != 16 {
				t.Errorf("expected ExtraSamplesApplied 16, got %d", completedComp.Status.ExtraSamplesApplied)
			}

			// Condition must stay True / Scheduled on completion
			condCompleted := meta.FindStatusCondition(completedComp.Status.Conditions, v1alpha1.ConditionTypeExtraSamples)
			if condCompleted == nil {
				t.Fatalf("expected ExtraSamples condition on completed comparison")
			}
			if condCompleted.Status != metav1.ConditionTrue {
				t.Errorf("expected ExtraSamples condition Status True on completion, got %s", condCompleted.Status)
			}
			if condCompleted.Reason != v1alpha1.ConditionReasonExtraSamplesScheduled {
				t.Errorf("expected ExtraSamples condition Reason %s on completion, got %s", v1alpha1.ConditionReasonExtraSamplesScheduled, condCompleted.Reason)
			}
		})
	}
}

func TestComparisonReconciler_CompletedComparisonForeignSinkPrefixNoop(t *testing.T) {
	scheme := setupScheme(t)
	sink := artifacts.NewInMemorySink() // prefix: mem://runs/<commit>/

	taskID := "docs/readme_comprehension"
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "/tmp/artifacts/runs/cand-commit/docs%2Freadme_comprehension/log.json",
				},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      taskID,
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "/tmp/artifacts/runs/base-commit/docs%2Freadme_comprehension/log.json",
				},
			},
		},
	}
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-completed-foreign", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:   v1alpha1.ComparisonPhaseCompleted,
			Verdict: v1alpha1.ComparisonVerdictUnchanged,
			Message: "Evaluated 1 task(s): 1 unchanged",
			Tasks: []v1alpha1.ComparisonTaskStatus{
				{
					ID:    taskID,
					Label: v1alpha1.ComparisonTaskLabelUnchanged,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-completed-foreign"}}
	res, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("expected no reconcile error, got: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("expected RequeueAfter 0, got: %v", res.RequeueAfter)
	}

	var updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if updated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("expected phase %s, got %s", v1alpha1.ComparisonPhaseCompleted, updated.Status.Phase)
	}
	if updated.Status.Verdict != v1alpha1.ComparisonVerdictUnchanged {
		t.Errorf("expected verdict %s, got %s", v1alpha1.ComparisonVerdictUnchanged, updated.Status.Verdict)
	}
	if updated.Status.Message != "Evaluated 1 task(s): 1 unchanged" {
		t.Errorf("expected message unchanged, got %q", updated.Status.Message)
	}
}

func TestComparisonReconciler_CompletedComparisonWithPendingCommands(t *testing.T) {
	scheme := setupScheme(t)
	sink := artifacts.NewInMemorySink()

	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
		},
	}
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
		},
	}
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-completed-cmd", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:     v1alpha1.ComparisonRunRef{Run: "base-run"},
			Candidate:    v1alpha1.ComparisonRunRef{Run: "cand-run"},
			ExtraSamples: 0,
		},
		Status: v1alpha1.ComparisonStatus{
			Phase:               v1alpha1.ComparisonPhaseCompleted,
			Verdict:             v1alpha1.ComparisonVerdictUnchanged,
			ExtraSamplesApplied: 0,
			PendingCommands: []v1alpha1.ComparisonPendingCommand{
				{
					CommentID:  42,
					Login:      "alice",
					Requested:  4,
					ReceivedAt: metav1.NewTime(now),
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(baseRun, candRun, comp).
		WithStatusSubresource(baseRun, candRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
		CommandUsers: []string{"alice"},
		Now:          func() time.Time { return now },
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-completed-cmd"}}
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("unexpected Reconcile error: %v", err)
	}

	var updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if updated.Spec.ExtraSamples != 4 {
		t.Errorf("expected Spec.ExtraSamples bumped to 4, got %d", updated.Spec.ExtraSamples)
	}
	if len(updated.Status.PendingCommands) != 0 {
		t.Errorf("expected pendingCommands cleared, got %d", len(updated.Status.PendingCommands))
	}
	// Completed comparison has no sample window or planned tasks, so dataset is exhausted and
	// no tasks are eligible to receive extra samples; the command settles as unserviceable.
	if len(updated.Status.HandledCommands) != 1 || updated.Status.HandledCommands[0].Outcome != "unserviceable: no eligible tasks" {
		t.Errorf("expected handledCommands to record 'unserviceable: no eligible tasks', got %+v", updated.Status.HandledCommands)
	}
	if updated.Status.ExtraSamplesApplied != 4 {
		t.Errorf("expected status.extraSamplesApplied settled to 4, got %d", updated.Status.ExtraSamplesApplied)
	}
}

func TestComparisonController_PendingCommands(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	t.Run("outsider denied", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("ghp_test")},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-outsider", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  5001,
						Login:      "outsider",
						Requested:  8,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, tokenSecret).WithStatusSubresource(comp).Build()
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-outsider"}}
		_, _ = reconciler.Reconcile(t.Context(), req)

		var latest v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &latest); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if latest.Spec.ExtraSamples != 0 {
			t.Errorf("expected ExtraSamples 0, got %d", latest.Spec.ExtraSamples)
		}
		if len(latest.Status.PendingCommands) != 0 {
			t.Errorf("expected 0 pendingCommands, got %d", len(latest.Status.PendingCommands))
		}
		if len(latest.Status.HandledCommands) != 1 {
			t.Fatalf("expected 1 handledCommand, got %d", len(latest.Status.HandledCommands))
		}
		h := latest.Status.HandledCommands[0]
		if h.CommentID != 5001 || h.Login != "outsider" || h.Outcome != "denied: unauthorized" {
			t.Errorf("unexpected handled command: %+v", h)
		}
		reactions := server.CommentReactions(5001)
		if len(reactions) != 1 || reactions[0] != "confused" {
			t.Errorf("expected confused reaction on denied comment, got %v", reactions)
		}
		for _, c := range server.Comments() {
			if strings.Contains(c.Body, "Evaluating") || strings.Contains(c.Body, "extraSamples") {
				t.Errorf("expected no command acknowledgement comment, got: %s", c.Body)
			}
		}
	})

	t.Run("bot login denied even in commandUsers", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-bot", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  5099,
						Login:      "renovate[bot]",
						Requested:  8,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp).WithStatusSubresource(comp).Build()
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
			CommandUsers:        []string{"renovate[bot]"},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-bot"}}
		_, _ = reconciler.Reconcile(t.Context(), req)

		var latest v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &latest); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if latest.Spec.ExtraSamples != 0 {
			t.Errorf("expected ExtraSamples 0 for bot user, got %d", latest.Spec.ExtraSamples)
		}
		if len(latest.Status.HandledCommands) != 1 || latest.Status.HandledCommands[0].Outcome != "denied: bot" {
			t.Errorf("expected outcome 'denied: bot', got %+v", latest.Status.HandledCommands)
		}
	})

	t.Run("reader denied", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.SetUserPermission("gke-labs", "eval-runner", "reader", "read")
		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("ghp_test")},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-reader", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  5002,
						Login:      "reader",
						Requested:  8,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, tokenSecret).WithStatusSubresource(comp).Build()
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-reader"}}
		_, _ = reconciler.Reconcile(t.Context(), req)

		var latest v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &latest); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if latest.Spec.ExtraSamples != 0 {
			t.Errorf("expected ExtraSamples 0, got %d", latest.Spec.ExtraSamples)
		}
		if len(latest.Status.HandledCommands) != 1 || latest.Status.HandledCommands[0].Outcome != "denied: unauthorized" {
			t.Errorf("expected outcome 'denied: unauthorized', got %+v", latest.Status.HandledCommands)
		}
		reactions := server.CommentReactions(5002)
		if len(reactions) != 1 || reactions[0] != "confused" {
			t.Errorf("expected confused reaction on denied comment, got %v", reactions)
		}
		for _, c := range server.Comments() {
			if strings.Contains(c.Body, "Evaluating") || strings.Contains(c.Body, "extraSamples") {
				t.Errorf("expected no command acknowledgement comment, got: %s", c.Body)
			}
		}
	})

	t.Run("allowlisted user authorized", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("ghp_test")},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-allowlisted", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
				ExtraSamples: 2,
			},
			Status: v1alpha1.ComparisonStatus{
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  5003,
						Login:      "alice",
						Requested:  8,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, tokenSecret).WithStatusSubresource(comp).Build()
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
			CommandUsers:        []string{"ALICE"},
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-allowlisted"}}
		_, _ = reconciler.Reconcile(t.Context(), req)

		var latest v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &latest); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if latest.Spec.ExtraSamples != 10 { // 2 + 8
			t.Errorf("expected ExtraSamples 10, got %d", latest.Spec.ExtraSamples)
		}
		if len(latest.Status.HandledCommands) != 1 || latest.Status.HandledCommands[0].Outcome != "applied" {
			t.Errorf("expected outcome 'applied', got %+v", latest.Status.HandledCommands)
		}
	})

	t.Run("writer authorized and reacts", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.SetUserPermission("gke-labs", "eval-runner", "writer", "write")
		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("ghp_test")},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-writer", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  5004,
						Login:      "writer",
						Requested:  16,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, tokenSecret).WithStatusSubresource(comp).Build()
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-writer"}}
		_, _ = reconciler.Reconcile(t.Context(), req)

		var latest v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &latest); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if latest.Spec.ExtraSamples != 16 {
			t.Errorf("expected ExtraSamples 16, got %d", latest.Spec.ExtraSamples)
		}
		reactions := server.CommentReactions(5004)
		if len(reactions) != 1 || reactions[0] != "eyes" {
			t.Errorf("expected reaction ['eyes'], got %v", reactions)
		}
		if len(latest.Status.HandledCommands) != 1 || latest.Status.HandledCommands[0].Outcome != "applied" {
			t.Errorf("expected outcome 'applied', got %+v", latest.Status.HandledCommands)
		}
	})

	t.Run("transient api error bounded retry then terminal with confused reaction", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.InjectPathError("GET", "/repos/gke-labs/eval-runner/collaborators/writer/permission", 500, `{"message":"Internal Server Error"}`)
		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("ghp_test")},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-api-err-retry", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  5005,
						Login:      "writer",
						Requested:  8,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, tokenSecret).WithStatusSubresource(comp).Build()
		testNow := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			Now:                 func() time.Time { return testNow },
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-api-err-retry"}}

		// Attempt 1: 500 error -> retry scheduled at +10s, attempts=1, HandledCommands=0
		res1, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error on attempt 1: %v", err)
		}
		if res1.RequeueAfter != 10*time.Second {
			t.Errorf("expected RequeueAfter 10s, got %v", res1.RequeueAfter)
		}

		var comp1 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &comp1); err != nil {
			t.Fatalf("failed to get comparison after attempt 1: %v", err)
		}
		if comp1.Spec.ExtraSamples != 0 {
			t.Errorf("expected ExtraSamples to remain 0, got %d", comp1.Spec.ExtraSamples)
		}
		if len(comp1.Status.PendingCommands) != 1 || comp1.Status.PendingCommands[0].Attempts != 1 {
			t.Errorf("expected 1 pending command with attempts=1, got %+v", comp1.Status.PendingCommands)
		}
		exp1 := metav1.NewTime(testNow.Add(10 * time.Second))
		if comp1.Status.PendingCommands[0].NextAttemptAt == nil || !comp1.Status.PendingCommands[0].NextAttemptAt.Equal(&exp1) {
			t.Errorf("expected nextAttemptAt at testNow+10s, got %v", comp1.Status.PendingCommands[0].NextAttemptAt)
		}
		if len(comp1.Status.HandledCommands) != 0 {
			t.Errorf("expected 0 handled commands on attempt 1, got %d", len(comp1.Status.HandledCommands))
		}
		if len(server.CommentReactions(5005)) != 0 {
			t.Errorf("expected no reaction on attempt 1, got %v", server.CommentReactions(5005))
		}

		// Immediate re-reconcile at the exact same Now(): must NOT burn an attempt because retry is not yet due
		resSame, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error on immediate re-reconcile: %v", err)
		}
		if resSame.RequeueAfter != 10*time.Second {
			t.Errorf("expected RequeueAfter 10s on immediate re-reconcile, got %v", resSame.RequeueAfter)
		}
		var compSame v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &compSame); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if len(compSame.Status.PendingCommands) != 1 || compSame.Status.PendingCommands[0].Attempts != 1 {
			t.Errorf("expected attempts to still be 1 without time advancing, got %+v", compSame.Status.PendingCommands)
		}

		// Advance time past 10s: attempt 2 should run
		testNow = testNow.Add(11 * time.Second)
		server.InjectPathError("GET", "/repos/gke-labs/eval-runner/collaborators/writer/permission", 500, `{"message":"Internal Server Error"}`)
		res2, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error on attempt 2: %v", err)
		}
		if res2.RequeueAfter != 1*time.Minute {
			t.Errorf("expected RequeueAfter 1m on attempt 2, got %v", res2.RequeueAfter)
		}

		var comp2 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &comp2); err != nil {
			t.Fatalf("failed to get comparison after attempt 2: %v", err)
		}
		if len(comp2.Status.PendingCommands) != 1 || comp2.Status.PendingCommands[0].Attempts != 2 {
			t.Errorf("expected 1 pending command with attempts=2, got %+v", comp2.Status.PendingCommands)
		}
		exp2 := metav1.NewTime(testNow.Add(1 * time.Minute))
		if comp2.Status.PendingCommands[0].NextAttemptAt == nil || !comp2.Status.PendingCommands[0].NextAttemptAt.Equal(&exp2) {
			t.Errorf("expected nextAttemptAt at testNow+1m, got %v", comp2.Status.PendingCommands[0].NextAttemptAt)
		}
		if len(comp2.Status.HandledCommands) != 0 {
			t.Errorf("expected 0 handled commands on attempt 2, got %d", len(comp2.Status.HandledCommands))
		}

		// Advance time past 1m: attempt 3 should run -> max attempts reached (3) -> terminal!
		testNow = testNow.Add(61 * time.Second)
		server.InjectPathError("GET", "/repos/gke-labs/eval-runner/collaborators/writer/permission", 500, `{"message":"Internal Server Error"}`)
		_, err = reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected reconcile error on attempt 3: %v", err)
		}

		var comp3 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &comp3); err != nil {
			t.Fatalf("failed to get comparison after attempt 3: %v", err)
		}
		if len(comp3.Status.PendingCommands) != 0 {
			t.Errorf("expected 0 pending commands after attempt 3, got %d", len(comp3.Status.PendingCommands))
		}
		if len(comp3.Status.HandledCommands) != 1 || !strings.HasPrefix(comp3.Status.HandledCommands[0].Outcome, "error: failed to verify permissions:") {
			t.Errorf("expected terminal error outcome in handledCommands, got %+v", comp3.Status.HandledCommands)
		}
		reactions := server.CommentReactions(5005)
		if len(reactions) != 1 || reactions[0] != "confused" {
			t.Errorf("expected confused reaction on terminal errored command, got %v", reactions)
		}
		for _, c := range server.Comments() {
			if strings.Contains(c.Body, "Evaluating") || strings.Contains(c.Body, "extraSamples") {
				t.Errorf("expected no command acknowledgement comment, got: %s", c.Body)
			}
		}
	})

	t.Run("transient api error recovers on subsequent reconcile", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.InjectPathError("GET", "/repos/gke-labs/eval-runner/collaborators/writer/permission", 500, `{"message":"Internal Server Error"}`)
		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("ghp_test")},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-api-recover", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  5015,
						Login:      "writer",
						Requested:  8,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, tokenSecret).WithStatusSubresource(comp).Build()
		testNow := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			Now:                 func() time.Time { return testNow },
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-api-recover"}}

		// Attempt 1: 500 error
		_, _ = reconciler.Reconcile(t.Context(), req)

		var comp1 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &comp1); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if len(comp1.Status.PendingCommands) != 1 || comp1.Status.PendingCommands[0].Attempts != 1 {
			t.Fatalf("expected pending command with attempts=1, got %+v", comp1.Status.PendingCommands)
		}

		// Advance time past 10s and GitHub recovered (write permission)
		testNow = testNow.Add(11 * time.Second)
		server.SetUserPermission("gke-labs", "eval-runner", "writer", "write")
		_, _ = reconciler.Reconcile(t.Context(), req)

		var comp2 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &comp2); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if comp2.Spec.ExtraSamples != 8 {
			t.Errorf("expected ExtraSamples 8, got %d", comp2.Spec.ExtraSamples)
		}
		if len(comp2.Status.PendingCommands) != 0 {
			t.Errorf("expected 0 pending commands after recovery, got %d", len(comp2.Status.PendingCommands))
		}
		if len(comp2.Status.HandledCommands) != 1 || comp2.Status.HandledCommands[0].Outcome != "applied" {
			t.Errorf("expected handled command outcome applied, got %+v", comp2.Status.HandledCommands)
		}
		reactions := server.CommentReactions(5015)
		if len(reactions) != 1 || reactions[0] != "eyes" {
			t.Errorf("expected eyes reaction on successful command, got %v", reactions)
		}
	})

	t.Run("transient rate limit retried", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.InjectPathError("GET", "/repos/gke-labs/eval-runner/collaborators/writer/permission", 429, `{"message":"Too Many Requests"}`, map[string]string{"Retry-After": "45"})
		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("ghp_test")},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-ratelimit-retry", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  5016,
						Login:      "writer",
						Requested:  8,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, tokenSecret).WithStatusSubresource(comp).Build()
		testNow := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			Now:                 func() time.Time { return testNow },
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-ratelimit-retry"}}

		// Attempt 1: 429 error with Retry-After: 45 -> retried with 45s wait
		res, err := reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.RequeueAfter != 45*time.Second {
			t.Errorf("expected RequeueAfter 45s from Retry-After, got %v", res.RequeueAfter)
		}

		var latest v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &latest); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if len(latest.Status.PendingCommands) != 1 || latest.Status.PendingCommands[0].Attempts != 1 {
			t.Errorf("expected 1 pending command with attempts=1, got %+v", latest.Status.PendingCommands)
		}
		exp45 := metav1.NewTime(testNow.Add(45 * time.Second))
		if latest.Status.PendingCommands[0].NextAttemptAt == nil || !latest.Status.PendingCommands[0].NextAttemptAt.Equal(&exp45) {
			t.Errorf("expected nextAttemptAt at testNow+45s, got %v", latest.Status.PendingCommands[0].NextAttemptAt)
		}
		if len(latest.Status.HandledCommands) != 0 {
			t.Errorf("expected 0 handled commands, got %d", len(latest.Status.HandledCommands))
		}
	})

	t.Run("transient credentials error bounded retry then terminal", func(t *testing.T) {
		appSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-app", Namespace: "eval-runner"},
			Data: map[string][]byte{
				"app-id":          []byte("1234"),
				"installation-id": []byte("5678"),
				"private-key":     []byte("invalid-pem-bytes"),
			},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-cred-err", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  5017,
						Login:      "writer",
						Requested:  8,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, appSecret).WithStatusSubresource(comp).Build()
		testNow := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
			GitHubAppSecret:     "github-app",
			Now:                 func() time.Time { return testNow },
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-cred-err"}}

		// Attempt 1: credentials error -> retry at +10s
		res1, _ := reconciler.Reconcile(t.Context(), req)
		if res1.RequeueAfter != 10*time.Second {
			t.Errorf("expected RequeueAfter 10s, got %v", res1.RequeueAfter)
		}
		var c1 v1alpha1.Comparison
		_ = cl.Get(t.Context(), req.NamespacedName, &c1)
		if len(c1.Status.PendingCommands) != 1 || c1.Status.PendingCommands[0].Attempts != 1 {
			t.Errorf("expected attempts=1, got %+v", c1.Status.PendingCommands)
		}
		if len(c1.Status.HandledCommands) != 0 {
			t.Errorf("expected 0 handled, got %d", len(c1.Status.HandledCommands))
		}

		// Re-reconcile at same Now() does not increment attempts
		_, _ = reconciler.Reconcile(t.Context(), req)
		var cSame v1alpha1.Comparison
		_ = cl.Get(t.Context(), req.NamespacedName, &cSame)
		if len(cSame.Status.PendingCommands) != 1 || cSame.Status.PendingCommands[0].Attempts != 1 {
			t.Errorf("expected attempts to remain 1, got %+v", cSame.Status.PendingCommands)
		}

		// Advance time past 10s: Attempt 2
		testNow = testNow.Add(11 * time.Second)
		res2, _ := reconciler.Reconcile(t.Context(), req)
		if res2.RequeueAfter != 1*time.Minute {
			t.Errorf("expected RequeueAfter 1m, got %v", res2.RequeueAfter)
		}
		var c2 v1alpha1.Comparison
		_ = cl.Get(t.Context(), req.NamespacedName, &c2)
		if len(c2.Status.PendingCommands) != 1 || c2.Status.PendingCommands[0].Attempts != 2 {
			t.Errorf("expected attempts=2, got %+v", c2.Status.PendingCommands)
		}

		// Advance time past 1m: Attempt 3 -> terminal
		testNow = testNow.Add(61 * time.Second)
		_, _ = reconciler.Reconcile(t.Context(), req)
		var c3 v1alpha1.Comparison
		_ = cl.Get(t.Context(), req.NamespacedName, &c3)
		if len(c3.Status.PendingCommands) != 0 {
			t.Errorf("expected 0 pending, got %d", len(c3.Status.PendingCommands))
		}
		if len(c3.Status.HandledCommands) != 1 || !strings.HasPrefix(c3.Status.HandledCommands[0].Outcome, "error: github credentials unavailable:") {
			t.Errorf("expected terminal outcome 'error: github credentials unavailable: ...', got %+v", c3.Status.HandledCommands)
		}
	})

	t.Run("no credentials configured stays terminal immediately", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-no-creds", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  5018,
						Login:      "writer",
						Requested:  8,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp).WithStatusSubresource(comp).Build()
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-no-creds"}}

		_, _ = reconciler.Reconcile(t.Context(), req)
		var c1 v1alpha1.Comparison
		_ = cl.Get(t.Context(), req.NamespacedName, &c1)
		if len(c1.Status.PendingCommands) != 0 {
			t.Errorf("expected 0 pending commands, got %d", len(c1.Status.PendingCommands))
		}
		if len(c1.Status.HandledCommands) != 1 || c1.Status.HandledCommands[0].Outcome != "error: github credentials unavailable" {
			t.Errorf("expected immediate terminal outcome 'error: github credentials unavailable', got %+v", c1.Status.HandledCommands)
		}
	})

	t.Run("reaction failure fallback to reply", func(t *testing.T) {
		server := github.NewFakeGitHubServer()
		defer server.Close()
		server.SetUserPermission("gke-labs", "eval-runner", "writer", "write")
		server.InjectPathError("POST", "/repos/gke-labs/eval-runner/issues/comments/5006/reactions", 500, `{"message":"reactions disabled"}`)
		tokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data:       map[string][]byte{"token": []byte("ghp_test")},
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-react-fallback", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  5006,
						Login:      "writer",
						Requested:  8,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, tokenSecret).WithStatusSubresource(comp).Build()
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-react-fallback"}}
		_, _ = reconciler.Reconcile(t.Context(), req)

		var latest v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &latest); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if latest.Spec.ExtraSamples != 8 {
			t.Errorf("expected ExtraSamples 8, got %d", latest.Spec.ExtraSamples)
		}
		foundAck := false
		for _, c := range server.Comments() {
			if strings.Contains(c.Body, "Evaluating 8 more sample(s) for PR #10 (extraSamples=8)") {
				foundAck = true
				break
			}
		}
		if !foundAck {
			t.Errorf("expected acknowledgement comment on PR, got: %v", server.Comments())
		}
	})

	t.Run("clamping by custom sampling spec", func(t *testing.T) {
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-clamp", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 16,
				},
				ExtraSamples: 0,
			},
			Status: v1alpha1.ComparisonStatus{
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  5007,
						Login:      "alice",
						Requested:  64,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp).WithStatusSubresource(comp).Build()
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
			CommandUsers:        []string{"alice"},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-clamp"}}
		_, _ = reconciler.Reconcile(t.Context(), req)

		var latest v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &latest); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if latest.Spec.ExtraSamples != 16 {
			t.Errorf("expected ExtraSamples 16 (clamped from 64 to 16), got %d", latest.Spec.ExtraSamples)
		}
		if len(latest.Status.HandledCommands) != 1 || latest.Status.HandledCommands[0].Outcome != "applied" {
			t.Errorf("expected outcome applied, got %+v", latest.Status.HandledCommands)
		}
	})

	t.Run("handledCommands bounded", func(t *testing.T) {
		var existingHandled []v1alpha1.ComparisonHandledCommand
		for i := 0; i < 110; i++ {
			existingHandled = append(existingHandled, v1alpha1.ComparisonHandledCommand{
				CommentID: int64(1000 + i),
				Login:     "alice",
				Requested: 2,
				Outcome:   "applied",
			})
		}
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-bounded", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     10,
				},
			},
			Status: v1alpha1.ComparisonStatus{
				HandledCommands: existingHandled,
				PendingCommands: []v1alpha1.ComparisonPendingCommand{
					{
						CommentID:  9999,
						Login:      "alice",
						Requested:  2,
						ReceivedAt: metav1.Now(),
					},
				},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp).WithStatusSubresource(comp).Build()
		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ControllerNamespace: "eval-runner",
			CommandUsers:        []string{"alice"},
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-bounded"}}
		_, _ = reconciler.Reconcile(t.Context(), req)

		var latest v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &latest); err != nil {
			t.Fatalf("failed to get comparison: %v", err)
		}
		if len(latest.Status.HandledCommands) > v1alpha1.MaxHandledCommands {
			t.Errorf("expected HandledCommands to be bounded by %d, got %d", v1alpha1.MaxHandledCommands, len(latest.Status.HandledCommands))
		}
	})
}

func TestAdaptivePairedSampling_BaselineReuse_AddOwnerReferenceErrorRequeues(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	repoURL := "git://repo"
	baseCommit := "base-commit-hash"
	model := "mockllm/model"

	batchKey := ComputeBatchKey(repoURL, baseCommit, model, []string{taskID}, v1alpha1.SampleRange{Start: 9, End: 16})
	existingBaseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pre-existing-base-run-b2",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelBatchKey: batchKey,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v1alpha1.GroupVersion.String(),
					Kind:       "Comparison",
					Name:       "earlier-comparison",
					UID:        "earlier-uid-1234",
				},
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskID,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     "gs://test-bucket/runs/base-commit-hash/pre_existing.json",
					Samples:     8,
					SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
				},
			},
		},
	}

	candScoresB1 := []stats.SampleScore{
		{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0},
		{ID: "5", Score: 1.0}, {ID: "6", Score: 0.0}, {ID: "7", Score: 1.0}, {ID: "8", Score: 0.0},
	}
	baseScoresB1 := []stats.SampleScore{
		{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 0.0}, {ID: "4", Score: 1.0},
		{ID: "5", Score: 0.0}, {ID: "6", Score: 1.0}, {ID: "7", Score: 0.0}, {ID: "8", Score: 1.0},
	}
	fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, candScoresB1)}
	fakeGCS.Objects["test-bucket/runs/base-commit-hash/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, baseScoresB1)}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: "cand-commit"},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-hash/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-reuse-err", Namespace: "eval-runner", UID: "comp-reuse-uid"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-b1"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-b1"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         8,
				MaxSamplesPerTask: 96,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingBaseRun, candRun, baseRun, comp).
		WithStatusSubresource(existingBaseRun, candRun, baseRun, comp).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if run, ok := obj.(*v1alpha1.Run); ok && run.Name == "pre-existing-base-run-b2" {
					return errors.New("simulated update error on reused baseline run")
				}
				return cl.Update(ctx, obj, opts...)
			},
		}).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-reuse-err"}}
	_, err = reconciler.Reconcile(t.Context(), req)
	if err == nil {
		t.Fatalf("Expected Reconcile to return error when addOwnerReference fails, got nil")
	}
	if !strings.Contains(err.Error(), "failed to add owner reference to reused baseline run \"pre-existing-base-run-b2\"") {
		t.Errorf("Expected error mentioning reused baseline run, got: %v", err)
	}
	if !strings.Contains(err.Error(), "simulated update error on reused baseline run") {
		t.Errorf("Expected root cause in error, got: %v", err)
	}

	// Verify comparison was not updated with batch 2
	var checkComp v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &checkComp)
	if len(checkComp.Status.Batches) > 1 {
		t.Errorf("Comparison should not have scheduled batch 2 after owner reference failure, got batches: %+v", checkComp.Status.Batches)
	}
}

func TestAdaptivePairedSampling_ExtraSamples_ConsumedRequestMarker_ShortOrFailedBatch(t *testing.T) {
	scheme := setupScheme(t)
	task1 := "task-short"
	task2 := "task-normal"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	// Batch 1: 8 samples that decide "improved" for both tasks
	var b1Cand, b1Base []stats.SampleScore
	for i := 1; i <= 8; i++ {
		b1Cand = append(b1Cand, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		b1Base = append(b1Base, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
	}
	fakeGCS.Objects["test-bucket/runs/cand/b1-t1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(task1, b1Cand)}
	fakeGCS.Objects["test-bucket/runs/base/b1-t1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(task1, b1Base)}
	fakeGCS.Objects["test-bucket/runs/cand/b1-t2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(task2, b1Cand)}
	fakeGCS.Objects["test-bucket/runs/base/b1-t2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(task2, b1Base)}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-run-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand"},
			Model:       "mockllm/model",
			Tasks:       []string{task1, task2},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: task1, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand/b1-t1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
				{ID: task2, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand/b1-t2.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base"},
			Model:       "mockllm/model",
			Tasks:       []string{task1, task2},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: task1, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base/b1-t1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
				{ID: task2, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base/b1-t2.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-extra-short", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate:    v1alpha1.ComparisonRunRef{Run: "cand-run-b1"},
			Baseline:     v1alpha1.ComparisonRunRef{Run: "base-run-b1"},
			Sampling:     &v1alpha1.ComparisonSamplingSpec{BatchSize: 8, MaxSamplesPerTask: 8},
			ExtraSamples: 8,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(candRun, baseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-extra-short"}}

	// Reconcile 1: All tasks decided (improved), extraSamples=8 > applied=0,
	// so extra batch 2 is scheduled across all decided tasks (window 9-16),
	// and extraSamplesApplied is set to 8 (consumed-request marker).
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}

	var compB2 v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compB2)
	if compB2.Status.ExtraSamplesApplied != 8 {
		t.Fatalf("Expected ExtraSamplesApplied to be 8 after scheduling, got %d", compB2.Status.ExtraSamplesApplied)
	}
	if len(compB2.Status.Batches) != 2 {
		t.Fatalf("Expected 2 batches scheduled, got %d", len(compB2.Status.Batches))
	}

	// In batch 2:
	// task1 returns short (only 3 samples, 9-11 -> exhausted)
	// task2 returns full window (8 samples, 9-16)
	var b2ShortCand, b2ShortBase []stats.SampleScore
	for i := 9; i <= 11; i++ {
		b2ShortCand = append(b2ShortCand, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		b2ShortBase = append(b2ShortBase, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
	}
	var b2FullCand, b2FullBase []stats.SampleScore
	for i := 9; i <= 16; i++ {
		b2FullCand = append(b2FullCand, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		b2FullBase = append(b2FullBase, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
	}
	fakeGCS.Objects["test-bucket/runs/cand/b2-t1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(task1, b2ShortCand)}
	fakeGCS.Objects["test-bucket/runs/base/b2-t1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(task1, b2ShortBase)}
	fakeGCS.Objects["test-bucket/runs/cand/b2-t2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(task2, b2FullCand)}
	fakeGCS.Objects["test-bucket/runs/base/b2-t2.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(task2, b2FullBase)}

	var candB2, baseB2 v1alpha1.Run
	_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-extra-short-b2-cand"}, &candB2)
	_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-extra-short-b2-base"}, &baseB2)
	candB2.Status.Phase = v1alpha1.RunPhaseCompleted
	candB2.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: task1, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand/b2-t1.json", Samples: 3, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16}},
		{ID: task2, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand/b2-t2.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16}},
	}
	baseB2.Status.Phase = v1alpha1.RunPhaseCompleted
	baseB2.Status.Tasks = []v1alpha1.TaskStatus{
		{ID: task1, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base/b2-t1.json", Samples: 3, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16}},
		{ID: task2, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base/b2-t2.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16}},
	}
	_ = cl.Status().Update(t.Context(), &candB2)
	_ = cl.Status().Update(t.Context(), &baseB2)

	// Reconcile 2: Completes batch 2. ExtraSamplesApplied remains 8 as a consumed-request marker,
	// even though task1 only returned 3 samples.
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile 2 failed: %v", err)
	}

	var compCompleted v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compCompleted)
	if compCompleted.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Fatalf("Expected comparison Completed, got %s", compCompleted.Status.Phase)
	}
	if compCompleted.Status.ExtraSamplesApplied != 8 {
		t.Errorf("Expected ExtraSamplesApplied to remain 8 (consumed-request marker), got %d", compCompleted.Status.ExtraSamplesApplied)
	}
	if len(compCompleted.Status.Batches) != 2 {
		t.Errorf("Expected exactly 2 batches, got %d", len(compCompleted.Status.Batches))
	}

	// Bump extraSamples to 16 -> triggers another batch for non-exhausted tasks (task2)
	compCompleted.Spec.ExtraSamples = 16
	_ = cl.Update(t.Context(), &compCompleted)

	// Reconcile 3: Schedules batch 3 (window 17-24) for task2
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile 3 failed: %v", err)
	}

	var compB3 v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &compB3)
	if compB3.Status.Phase != v1alpha1.ComparisonPhaseRunning {
		t.Fatalf("Expected comparison Running for batch 3, got %s", compB3.Status.Phase)
	}
	if compB3.Status.ExtraSamplesApplied != 16 {
		t.Errorf("Expected ExtraSamplesApplied bumped to 16, got %d", compB3.Status.ExtraSamplesApplied)
	}
	if len(compB3.Status.Batches) != 3 {
		t.Errorf("Expected 3 batches, got %d", len(compB3.Status.Batches))
	}
}

func TestComparisonReconciler_MissingBatchRunMessage(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("Failed to create GCS sink: %v", err)
	}

	candScores := []stats.SampleScore{{ID: "1", Score: 1.0}}
	baseScores := []stats.SampleScore{{ID: "1", Score: 1.0}}
	fakeGCS.Objects["test-bucket/runs/cand/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, candScores)}
	fakeGCS.Objects["test-bucket/runs/base/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, baseScores)}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand"},
			Model:       "mockllm/model",
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand/b1.json", Samples: 1, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base"},
			Model:       "mockllm/model",
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base/b1.json", Samples: 1, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
			},
		},
	}

	// Batch 2 candidate exists, but batch 2 baseline run ("shared-base-b2") was deleted / is missing!
	candB2 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-b2", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand"},
			Model:       "mockllm/model",
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand/b1.json", Samples: 1, SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8}},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-missing-shared", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-b1"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-b1"},
			Sampling:  &v1alpha1.ComparisonSamplingSpec{BatchSize: 4, MaxSamplesPerTask: 16},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase: v1alpha1.ComparisonPhaseRunning,
			Batches: []v1alpha1.ComparisonBatchStatus{
				{Index: 1, CandidateRun: "cand-b1", BaselineRun: "base-b1", Phase: v1alpha1.ComparisonPhaseCompleted},
				{Index: 2, CandidateRun: "cand-b2", BaselineRun: "shared-base-b2", Phase: v1alpha1.ComparisonPhaseRunning},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, candB2, comp).
		WithStatusSubresource(candRun, baseRun, candB2, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-missing-shared"}}
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	var compUpdated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &compUpdated); err != nil {
		t.Fatalf("Get comparison failed: %v", err)
	}

	if compUpdated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("Expected comparison to terminate Completed, got %s", compUpdated.Status.Phase)
	}
	if compUpdated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
		t.Errorf("Expected verdict Inconclusive, got %s", compUpdated.Status.Verdict)
	}
	if !strings.Contains(compUpdated.Status.Message, "missing batch run: shared-base-b2") {
		t.Errorf("Expected status.Message to explicitly name missing shared run, got %q", compUpdated.Status.Message)
	}
}

func TestComparisonReconciler_BaselineReuse_SkipsMismatchedSinkPrefix(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	tempDir := t.TempDir()
	sink := &artifacts.LocalDirSink{BaseDir: tempDir}

	candCommit := "cand-commit"
	baseCommit := "base-commit"
	repoURL := "git://repo"
	model := "mockllm/model"
	b2Window := v1alpha1.SampleRange{Start: 5, End: 8}
	batchKey := ComputeBatchKey(repoURL, baseCommit, model, []string{taskID}, b2Window)

	// Write batch 1 Inspect logs into the LocalDir sink
	var b1Scores []stats.SampleScore
	for i := 1; i <= 4; i++ {
		b1Scores = append(b1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
	}
	candB1Path, err := sink.StoreArtifact(t.Context(), artifacts.RunMetadata{
		Commit:       candCommit,
		RunName:      "cand-b1",
		RunNamespace: "eval-runner",
		TaskID:       taskID,
	}, "b1.json", makeSyntheticInspectLog(taskID, b1Scores))
	if err != nil {
		t.Fatalf("Failed to store candidate b1 log: %v", err)
	}
	baseB1Path, err := sink.StoreArtifact(t.Context(), artifacts.RunMetadata{
		Commit:       baseCommit,
		RunName:      "base-b1",
		RunNamespace: "eval-runner",
		TaskID:       taskID,
	}, "b1.json", makeSyntheticInspectLog(taskID, b1Scores))
	if err != nil {
		t.Fatalf("Failed to store baseline b1 log: %v", err)
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: candCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskID,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     candB1Path,
					Samples:     4,
					SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
				},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskID,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     baseB1Path,
					Samples:     4,
					SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
				},
			},
		},
	}

	// Labelled Completed Run matching the batch key, but with gs:// log paths under LocalDir sink
	existingGCSBaseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gcs-reused-base-run",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelBatchKey: batchKey,
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &b2Window,
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskID,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/pre_existing.json",
					Samples:     4,
					SampleRange: &b2Window,
				},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-sink-skip", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-b1"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-b1"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         4,
				MaxSamplesPerTask: 16,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, existingGCSBaseRun, comp).
		WithStatusSubresource(candRun, baseRun, existingGCSBaseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
	// Reconcile 1: evaluates batch 1 and schedules batch 2
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Existing GCS run should have been skipped, and a fresh baseline created
	var createdBase v1alpha1.Run
	freshBaseName := fmt.Sprintf("%s-b2-base", comp.Name)
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: freshBaseName}, &createdBase); err != nil {
		t.Fatalf("Expected fresh baseline run %q to be created, got error: %v", freshBaseName, err)
	}

	// The old GCS run must not have been adopted/reused with owner references
	var checkOldBase v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "gcs-reused-base-run"}, &checkOldBase); err != nil {
		t.Fatalf("Failed to get gcs-reused-base-run: %v", err)
	}
	if len(checkOldBase.OwnerReferences) > 0 {
		t.Errorf("Expected gcs-reused-base-run not to be reused (no owner references), got: %+v", checkOldBase.OwnerReferences)
	}
}

func TestComparisonReconciler_ReferencedBatchRun_PrefixMismatchTerminatesInconclusive(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	tempDir := t.TempDir()
	sink := &artifacts.LocalDirSink{BaseDir: tempDir}

	candCommit := "cand-commit"
	baseCommit := "base-commit"
	repoURL := "git://repo"
	model := "mockllm/model"

	// Write batch 1 and batch 2 candidate Inspect logs into the LocalDir sink
	var b1Scores []stats.SampleScore
	for i := 1; i <= 4; i++ {
		b1Scores = append(b1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
	}
	candB1Path, err := sink.StoreArtifact(t.Context(), artifacts.RunMetadata{
		Commit:       candCommit,
		RunName:      "cand-b1",
		RunNamespace: "eval-runner",
		TaskID:       taskID,
	}, "b1.json", makeSyntheticInspectLog(taskID, b1Scores))
	if err != nil {
		t.Fatalf("Failed to store candidate b1 log: %v", err)
	}
	baseB1Path, err := sink.StoreArtifact(t.Context(), artifacts.RunMetadata{
		Commit:       baseCommit,
		RunName:      "base-b1",
		RunNamespace: "eval-runner",
		TaskID:       taskID,
	}, "b1.json", makeSyntheticInspectLog(taskID, b1Scores))
	if err != nil {
		t.Fatalf("Failed to store baseline b1 log: %v", err)
	}

	candB2Path, err := sink.StoreArtifact(t.Context(), artifacts.RunMetadata{
		Commit:       candCommit,
		RunName:      "cand-b2",
		RunNamespace: "eval-runner",
		TaskID:       taskID,
	}, "b2.json", makeSyntheticInspectLog(taskID, b1Scores))
	if err != nil {
		t.Fatalf("Failed to store candidate b2 log: %v", err)
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: candCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskID,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     candB1Path,
					Samples:     4,
					SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
				},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskID,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     baseB1Path,
					Samples:     4,
					SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
				},
			},
		},
	}

	candB2 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-b2", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: candCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskID,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     candB2Path,
					Samples:     4,
					SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8},
				},
			},
		},
	}

	// Referenced batch Run with mismatched paths (gs:// under LocalDir sink)
	mismatchedBaseB2 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "reused-base-b2", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
			Model:       model,
			Tasks:       []string{taskID},
			SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskID,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     "gs://test-bucket/runs/base-commit/" + taskID + "/b2.json",
					Samples:     4,
					SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8},
				},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-batch-mismatch", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-b1"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-b1"},
			Sampling:  &v1alpha1.ComparisonSamplingSpec{BatchSize: 4, MaxSamplesPerTask: 16},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase: v1alpha1.ComparisonPhaseRunning,
			Batches: []v1alpha1.ComparisonBatchStatus{
				{Index: 1, CandidateRun: "cand-b1", BaselineRun: "base-b1", Phase: v1alpha1.ComparisonPhaseCompleted},
				{Index: 2, CandidateRun: "cand-b2", BaselineRun: "reused-base-b2", Phase: v1alpha1.ComparisonPhaseRunning},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, candB2, mismatchedBaseB2, comp).
		WithStatusSubresource(candRun, baseRun, candB2, mismatchedBaseB2, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
	_, err = reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("Reconcile should terminate without error, got: %v", err)
	}

	var compUpdated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &compUpdated); err != nil {
		t.Fatalf("Get comparison failed: %v", err)
	}

	if compUpdated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("Expected comparison to terminate Completed, got %s", compUpdated.Status.Phase)
	}
	if compUpdated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
		t.Errorf("Expected verdict Inconclusive, got %s", compUpdated.Status.Verdict)
	}
	if !strings.Contains(compUpdated.Status.Message, "reused-base-b2") {
		t.Errorf("Expected status.Message to explicitly name mismatched run %q, got %q", "reused-base-b2", compUpdated.Status.Message)
	}
	if !strings.Contains(compUpdated.Status.Message, "failed batch run: reused-base-b2") {
		t.Errorf("Expected status.Message to contain 'failed batch run: reused-base-b2', got %q", compUpdated.Status.Message)
	}
}

func TestComparisonReconciler_BaselineReuse_MatchedTasksValidation(t *testing.T) {
	scheme := setupScheme(t)
	tempDir := t.TempDir()
	sink := &artifacts.LocalDirSink{BaseDir: tempDir}

	repoURL := "git://repo"
	commit := "base-commit-hash"
	model := "mockllm/model"
	window := v1alpha1.SampleRange{Start: 1, End: 8}
	goodLogPath := filepath.Join(tempDir, "runs", commit, "task-A", "inspect.json")

	// (a) Run with tasks A Completed (good path) and B Failed (empty path), Comparison needs only A: reused
	t.Run("NeedsOnlyTaskA_TaskBFailedInOtherSlot_Reused", func(t *testing.T) {
		tasks := []string{"task-A"}
		batchKey := ComputeBatchKey(repoURL, commit, model, tasks, window)

		candidateRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "candidate-run-reuse-a",
				Namespace: "eval-runner",
				Labels: map[string]string{
					LabelBatchKey: batchKey,
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task-A",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: goodLogPath,
					},
					{
						ID:      "task-B",
						Phase:   v1alpha1.TaskPhaseFailed,
						LogPath: "",
					},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candidateRun).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		found, err := reconciler.findCompletedBaselineRun(t.Context(), "eval-runner", repoURL, commit, model, tasks, window)
		if err != nil {
			t.Fatalf("findCompletedBaselineRun failed: %v", err)
		}
		if found == nil {
			t.Fatalf("Expected candidate run with completed task-A to be reused even though task-B in another slot failed")
		}
		if found.Name != candidateRun.Name {
			t.Errorf("Expected reused run name %q, got %q", candidateRun.Name, found.Name)
		}
	})

	// (b) Comparison needs A and B: not reused because matched task B is Failed
	t.Run("NeedsTaskAAndTaskB_TaskBFailed_NotReused", func(t *testing.T) {
		tasks := []string{"task-A", "task-B"}
		batchKey := ComputeBatchKey(repoURL, commit, model, tasks, window)

		candidateRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "candidate-run-reuse-ab",
				Namespace: "eval-runner",
				Labels: map[string]string{
					LabelBatchKey: batchKey,
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task-A",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: goodLogPath,
					},
					{
						ID:      "task-B",
						Phase:   v1alpha1.TaskPhaseFailed,
						LogPath: "",
					},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candidateRun).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		found, err := reconciler.findCompletedBaselineRun(t.Context(), "eval-runner", repoURL, commit, model, tasks, window)
		if err != nil {
			t.Fatalf("findCompletedBaselineRun failed: %v", err)
		}
		if found != nil {
			t.Fatalf("Expected run to not be reused when matched task-B is Failed, got: %s", found.Name)
		}
	})

	t.Run("NeedsTaskA_TaskAEmptyLogPath_NotReused", func(t *testing.T) {
		tasks := []string{"task-A"}
		batchKey := ComputeBatchKey(repoURL, commit, model, tasks, window)

		candidateRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "candidate-run-empty-log",
				Namespace: "eval-runner",
				Labels: map[string]string{
					LabelBatchKey: batchKey,
				},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{
						ID:      "task-A",
						Phase:   v1alpha1.TaskPhaseCompleted,
						LogPath: "",
					},
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candidateRun).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
		}

		found, err := reconciler.findCompletedBaselineRun(t.Context(), "eval-runner", repoURL, commit, model, tasks, window)
		if err != nil {
			t.Fatalf("findCompletedBaselineRun failed: %v", err)
		}
		if found != nil {
			t.Fatalf("Expected run to not be reused when matched task-A has empty logPath, got: %s", found.Name)
		}
	})
}

func TestComparisonReconciler_BaselineReuse_FailedTaskInOtherSlot_Reconcile(t *testing.T) {
	scheme := setupScheme(t)
	taskA := "docs/readme_comprehension"
	tempDir := t.TempDir()
	sink := &artifacts.LocalDirSink{BaseDir: tempDir}

	candCommit := "cand-commit"
	baseCommit := "base-commit"
	repoURL := "git://repo"
	model := "mockllm/model"
	b2Window := v1alpha1.SampleRange{Start: 5, End: 8}
	batchKey := ComputeBatchKey(repoURL, baseCommit, model, []string{taskA}, b2Window)

	// Write batch 1 Inspect logs into the LocalDir sink
	var b1Scores []stats.SampleScore
	for i := 1; i <= 4; i++ {
		b1Scores = append(b1Scores, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.8})
	}
	candB1Path, err := sink.StoreArtifact(t.Context(), artifacts.RunMetadata{
		Commit:       candCommit,
		RunName:      "cand-b1",
		RunNamespace: "eval-runner",
		TaskID:       taskA,
	}, "b1.json", makeSyntheticInspectLog(taskA, b1Scores))
	if err != nil {
		t.Fatalf("Failed to store candidate b1 log: %v", err)
	}
	baseB1Path, err := sink.StoreArtifact(t.Context(), artifacts.RunMetadata{
		Commit:       baseCommit,
		RunName:      "base-b1",
		RunNamespace: "eval-runner",
		TaskID:       taskA,
	}, "b1.json", makeSyntheticInspectLog(taskA, b1Scores))
	if err != nil {
		t.Fatalf("Failed to store baseline b1 log: %v", err)
	}

	baseB2TaskAPath, err := sink.StoreArtifact(t.Context(), artifacts.RunMetadata{
		Commit:       baseCommit,
		RunName:      "reused-base-run",
		RunNamespace: "eval-runner",
		TaskID:       taskA,
	}, "b2.json", makeSyntheticInspectLog(taskA, b1Scores))
	if err != nil {
		t.Fatalf("Failed to store baseline b2 taskA log: %v", err)
	}

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: candCommit},
			Model:       model,
			Tasks:       []string{taskA},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskA,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     candB1Path,
					Samples:     4,
					SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
				},
			},
		},
	}
	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-b1", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
			Model:       model,
			Tasks:       []string{taskA},
			SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskA,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     baseB1Path,
					Samples:     4,
					SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
				},
			},
		},
	}

	// Labelled Completed Run matching batchKey for taskA, but also containing a Failed taskB in another slot
	existingBaseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "reused-base-run",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelBatchKey: batchKey,
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
			Model:       model,
			Tasks:       []string{taskA},
			SampleRange: &b2Window,
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          taskA,
					Phase:       v1alpha1.TaskPhaseCompleted,
					LogPath:     baseB2TaskAPath,
					Samples:     4,
					SampleRange: &b2Window,
				},
				{
					ID:      "other-task-B",
					Phase:   v1alpha1.TaskPhaseFailed,
					LogPath: "",
				},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "comp-reuse-ok", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-b1"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-b1"},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         4,
				MaxSamplesPerTask: 16,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, existingBaseRun, comp).
		WithStatusSubresource(candRun, baseRun, existingBaseRun, comp).
		Build()

	reconciler := &ComparisonReconciler{
		Client:       cl,
		Scheme:       scheme,
		ArtifactSink: sink,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
	// Reconcile: evaluates batch 1 and schedules batch 2
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// existingBaseRun should have been reused (no fresh baseline created)
	var freshBase v1alpha1.Run
	freshBaseName := fmt.Sprintf("%s-b2-base", comp.Name)
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: freshBaseName}, &freshBase); err == nil {
		t.Errorf("Fresh baseline run %q should not have been created; existingBaseRun should have been reused", freshBaseName)
	}

	// Verify existingBaseRun received an owner reference
	var updatedBase v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "reused-base-run"}, &updatedBase); err != nil {
		t.Fatalf("Failed to get reused-base-run: %v", err)
	}
	foundOwner := false
	for _, owner := range updatedBase.OwnerReferences {
		if owner.Name == comp.Name {
			foundOwner = true
			break
		}
	}
	if !foundOwner {
		t.Errorf("Expected reused-base-run to have owner reference for comparison %s, got: %+v", comp.Name, updatedBase.OwnerReferences)
	}
}

func TestComparisonReconciler_TaskAffinity(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	repoURL := "https://github.com/gke-labs/eval-runner.git"
	candCommit := "cand-commit-sha"
	baseCommit := "base-commit-sha"
	model := "google/gemini-pro"

	taskDocs := "docs/readme_comprehension"
	taskSrc := "src/core_logic"

	manifest := &v1alpha1.Manifest{
		Tasks: []v1alpha1.ManifestTask{
			{ID: taskDocs, Paths: []string{"docs/**"}, Spec: "docs.py@eval"},
			{ID: taskSrc, Paths: []string{"src/**"}, Spec: "src.py@eval"},
		},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}

	tokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
		Data:       map[string][]byte{"token": []byte("test-token")},
	}

	setupFixture := func(t *testing.T, affinityMode string) (*github.FakeGitHubServer, *ComparisonReconciler, client.Client, *v1alpha1.Comparison, *fakes.FakeStorageClient) {
		server := github.NewFakeGitHubServer()
		server.SetPullRequestFiles("gke-labs", "eval-runner", 123, []string{"docs/readme.md"})

		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("failed to create GCS sink: %v", err)
		}

		fakeGCS.Objects["test-bucket/runs/cand-commit-sha/manifest.json"] = fakes.FakeObject{Data: manifestBytes}

		scores := []stats.SampleScore{
			{ID: "1", Score: 1.0},
			{ID: "2", Score: 0.0},
			{ID: "3", Score: 1.0},
			{ID: "4", Score: 0.0},
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit-sha/docs.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskDocs, scores)}
		fakeGCS.Objects["test-bucket/runs/base-commit-sha/docs.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskDocs, scores)}
		fakeGCS.Objects["test-bucket/runs/cand-commit-sha/src.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskSrc, scores)}
		fakeGCS.Objects["test-bucket/runs/base-commit-sha/src.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskSrc, scores)}

		b1Range := v1alpha1.SampleRange{Start: 1, End: 4}
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-b1", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: candCommit},
				Model:       model,
				Tasks:       []string{taskSrc, taskDocs}, // Seed order has taskSrc first
				SampleRange: &b1Range,
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Discovery: &v1alpha1.RunDiscoveryStatus{
					ManifestPath: "gs://test-bucket/runs/cand-commit-sha/manifest.json",
				},
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskSrc, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit-sha/src.json", Samples: 4, SampleRange: &b1Range},
					{ID: taskDocs, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit-sha/docs.json", Samples: 4, SampleRange: &b1Range},
				},
			},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-b1", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: repoURL, Commit: baseCommit},
				Model:       model,
				Tasks:       []string{taskSrc, taskDocs},
				SampleRange: &b1Range,
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskSrc, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-sha/src.json", Samples: 4, SampleRange: &b1Range},
					{ID: taskDocs, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-sha/docs.json", Samples: 4, SampleRange: &b1Range},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-affinity", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-b1"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-b1"},
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 16,
				},
				PullRequest: &v1alpha1.PullRequestSpec{
					Repository: "gke-labs/eval-runner",
					Number:     123,
					HeadSHA:    candCommit,
					BaseSHA:    baseCommit,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp, tokenSecret).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              scheme,
			ArtifactSink:        sink,
			ControllerNamespace: "eval-runner",
			GitHubTokenSecret:   "github-token",
			AffinityMode:        affinityMode,
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}

		return server, reconciler, cl, comp, fakeGCS
	}

	t.Run("order mode (default): sorts batch spec.tasks matched first then unmatched, records status", func(t *testing.T) {
		server, reconciler, cl, comp, _ := setupFixture(t, configkeys.AffinityModeOrder)
		defer server.Close()

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		// Verify status fields
		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("Get comparison failed: %v", err)
		}

		if updatedComp.Status.Affinity == nil {
			t.Fatalf("expected non-nil Status.Affinity")
		}
		if updatedComp.Status.Affinity.FilesCount != 1 {
			t.Errorf("expected FilesCount 1, got %d", updatedComp.Status.Affinity.FilesCount)
		}
		if updatedComp.Status.Affinity.Truncated {
			t.Errorf("expected Truncated false, got true")
		}
		expectedHash := affinity.ComputeFilesHash([]string{"docs/readme.md"})
		if updatedComp.Status.Affinity.FilesHash != expectedHash {
			t.Errorf("expected FilesHash %q, got %q", expectedHash, updatedComp.Status.Affinity.FilesHash)
		}

		if updatedComp.Status.Summary == nil || updatedComp.Status.Summary.AffinityMatched != 1 {
			t.Errorf("expected AffinityMatched 1, got %+v", updatedComp.Status.Summary)
		}

		// Verify per-task affinity
		for _, task := range updatedComp.Status.Tasks {
			switch task.ID {
			case taskDocs:
				if task.Affinity != v1alpha1.ComparisonTaskAffinityMatched {
					t.Errorf("expected taskDocs affinity 'matched', got %q", task.Affinity)
				}
			case taskSrc:
				if task.Affinity != v1alpha1.ComparisonTaskAffinityUnmatched {
					t.Errorf("expected taskSrc affinity 'unmatched', got %q", task.Affinity)
				}
			}
		}

		// Verify batch 2 Runs have spec.tasks with matched first then unmatched
		var b2Cand v1alpha1.Run
		b2CandName := fmt.Sprintf("%s-b2-cand", comp.Name)
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b2CandName}, &b2Cand); err != nil {
			t.Fatalf("failed to get batch 2 candidate run: %v", err)
		}
		if len(b2Cand.Spec.Tasks) != 2 {
			t.Fatalf("expected 2 tasks in batch 2, got %v", b2Cand.Spec.Tasks)
		}
		if b2Cand.Spec.Tasks[0] != taskDocs {
			t.Errorf("expected matched task %q first in batch 2 spec.tasks, got %q", taskDocs, b2Cand.Spec.Tasks[0])
		}
		if b2Cand.Spec.Tasks[1] != taskSrc {
			t.Errorf("expected unmatched task %q second in batch 2 spec.tasks, got %q", taskSrc, b2Cand.Spec.Tasks[1])
		}
	})

	t.Run("prune mode: drops unmatched tasks from batch spec.tasks and reports as skipped", func(t *testing.T) {
		server, reconciler, cl, comp, _ := setupFixture(t, configkeys.AffinityModePrune)
		defer server.Close()

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		// Verify batch 2 has ONLY matched task
		var b2Cand v1alpha1.Run
		b2CandName := fmt.Sprintf("%s-b2-cand", comp.Name)
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b2CandName}, &b2Cand); err != nil {
			t.Fatalf("failed to get batch 2 candidate run: %v", err)
		}
		if len(b2Cand.Spec.Tasks) != 1 || b2Cand.Spec.Tasks[0] != taskDocs {
			t.Errorf("expected batch 2 spec.tasks to contain only matched task [%s], got %v", taskDocs, b2Cand.Spec.Tasks)
		}

		// Verify comparison status reports taskSrc as skipped with reason unmatched
		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("Get comparison failed: %v", err)
		}
		if updatedComp.Status.Summary == nil || updatedComp.Status.Summary.Skipped != 1 {
			t.Errorf("expected Skipped count 1, got %+v", updatedComp.Status.Summary)
		}
		if !strings.Contains(updatedComp.Status.Message, "skipped (unmatched): "+taskSrc) {
			t.Errorf("expected message to contain 'skipped (unmatched): %s', got %q", taskSrc, updatedComp.Status.Message)
		}
	})

	t.Run("API 500 error: falls back to order mode with all tasks always", func(t *testing.T) {
		server, reconciler, cl, comp, _ := setupFixture(t, configkeys.AffinityModePrune) // configured as prune, but should fall back to order
		defer server.Close()

		// Inject 500 error on PR files endpoint
		server.InjectPathError("GET", "/repos/gke-labs/eval-runner/pulls/123/files", 500, `{"message": "internal error"}`)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile should not fail on files API 500 error, got %v", err)
		}

		// Verify batch 2 includes both tasks (not pruned because fell back to order)
		var b2Cand v1alpha1.Run
		b2CandName := fmt.Sprintf("%s-b2-cand", comp.Name)
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b2CandName}, &b2Cand); err != nil {
			t.Fatalf("failed to get batch 2 candidate run: %v", err)
		}
		if len(b2Cand.Spec.Tasks) != 2 {
			t.Errorf("expected both tasks in batch 2 after fallback, got %v", b2Cand.Spec.Tasks)
		}

		// Verify status tasks have affinity "always"
		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("Get comparison failed: %v", err)
		}
		for _, task := range updatedComp.Status.Tasks {
			if task.Affinity != v1alpha1.ComparisonTaskAffinityAlways {
				t.Errorf("expected task %s affinity 'always' after API error fallback, got %q", task.ID, task.Affinity)
			}
		}
		if updatedComp.Status.Summary != nil && updatedComp.Status.Summary.AffinityMatched != 0 {
			t.Errorf("expected AffinityMatched 0 after API fallback, got %d", updatedComp.Status.Summary.AffinityMatched)
		}
	})

	t.Run("Comparison without pullRequest or without task paths is unchanged", func(t *testing.T) {
		server, reconciler, cl, comp, _ := setupFixture(t, configkeys.AffinityModeOrder)
		defer server.Close()

		comp.Spec.PullRequest = nil
		if err := cl.Update(t.Context(), comp); err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if updatedComp.Status.Affinity != nil {
			t.Errorf("expected nil Status.Affinity without pullRequest, got %+v", updatedComp.Status.Affinity)
		}
	})

	t.Run("one files API call per head/base pair across multiple reconciles", func(t *testing.T) {
		server, reconciler, _, comp, _ := setupFixture(t, configkeys.AffinityModeOrder)
		defer server.Close()

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// First reconcile
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("first Reconcile failed: %v", err)
		}
		// Second reconcile
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("second Reconcile failed: %v", err)
		}

		// Check request log on fake server: exactly 1 GET to /files
		fileCalls := 0
		for _, logEntry := range server.RequestLog() {
			if strings.Contains(logEntry, "/files") {
				fileCalls++
			}
		}
		if fileCalls != 1 {
			t.Errorf("expected exactly 1 files API call, got %d", fileCalls)
		}
	})

	t.Run("two-batch prune survives multiple batches without rescheduling unmatched task", func(t *testing.T) {
		server, reconciler, cl, comp, fakeGCS := setupFixture(t, configkeys.AffinityModePrune)
		defer server.Close()

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// Reconcile 1: batch 1 complete -> schedules batch 2 with [taskDocs] only
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("first Reconcile failed: %v", err)
		}

		var b2Cand v1alpha1.Run
		b2CandName := fmt.Sprintf("%s-b2-cand", comp.Name)
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b2CandName}, &b2Cand); err != nil {
			t.Fatalf("failed to get batch 2 candidate run: %v", err)
		}
		if len(b2Cand.Spec.Tasks) != 1 || b2Cand.Spec.Tasks[0] != taskDocs {
			t.Fatalf("expected batch 2 spec.tasks to be [%s], got %v", taskDocs, b2Cand.Spec.Tasks)
		}

		// Simulate batch 2 completing with inconclusive samples for taskDocs
		b2Range := v1alpha1.SampleRange{Start: 5, End: 8}
		b2CandScores := []stats.SampleScore{
			{ID: "5", Score: 1.0},
			{ID: "6", Score: 0.0},
			{ID: "7", Score: 1.0},
			{ID: "8", Score: 0.0},
		}
		b2BaseScores := []stats.SampleScore{
			{ID: "5", Score: 0.0},
			{ID: "6", Score: 1.0},
			{ID: "7", Score: 0.0},
			{ID: "8", Score: 1.0},
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit-sha/b2-docs.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskDocs, b2CandScores)}
		fakeGCS.Objects["test-bucket/runs/base-commit-sha/b2-docs.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskDocs, b2BaseScores)}

		b2Cand.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Cand.Status.Tasks = []v1alpha1.TaskStatus{
			{ID: taskDocs, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit-sha/b2-docs.json", Samples: 4, SampleRange: &b2Range},
		}
		if err := cl.Status().Update(t.Context(), &b2Cand); err != nil {
			t.Fatalf("failed to update b2Cand status: %v", err)
		}

		var b2Base v1alpha1.Run
		b2BaseName := fmt.Sprintf("%s-b2-base", comp.Name)
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b2BaseName}, &b2Base); err != nil {
			t.Fatalf("failed to get batch 2 baseline run: %v", err)
		}
		b2Base.Status.Phase = v1alpha1.RunPhaseCompleted
		b2Base.Status.Tasks = []v1alpha1.TaskStatus{
			{ID: taskDocs, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-sha/b2-docs.json", Samples: 4, SampleRange: &b2Range},
		}
		if err := cl.Status().Update(t.Context(), &b2Base); err != nil {
			t.Fatalf("failed to update b2Base status: %v", err)
		}

		// Reconcile 2: evaluates batch 2 -> schedules batch 3
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("second Reconcile failed: %v", err)
		}

		// Verify batch 3 still excludes taskSrc and contains only taskDocs
		var b3Cand v1alpha1.Run
		b3CandName := fmt.Sprintf("%s-b3-cand", comp.Name)
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b3CandName}, &b3Cand); err != nil {
			t.Fatalf("failed to get batch 3 candidate run: %v", err)
		}
		if len(b3Cand.Spec.Tasks) != 1 || b3Cand.Spec.Tasks[0] != taskDocs {
			t.Errorf("expected batch 3 spec.tasks to still contain ONLY [%s], got %v", taskDocs, b3Cand.Spec.Tasks)
		}

		// Verify comparison status still reports taskSrc as skipped
		var updatedComp v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &updatedComp); err != nil {
			t.Fatalf("Get comparison failed: %v", err)
		}
		if updatedComp.Status.Summary == nil || updatedComp.Status.Summary.Skipped != 1 {
			t.Errorf("expected Skipped count to stay 1, got %+v", updatedComp.Status.Summary)
		}
		if !strings.Contains(updatedComp.Status.Message, "skipped (unmatched): "+taskSrc) {
			t.Errorf("expected message to still contain 'skipped (unmatched): %s', got %q", taskSrc, updatedComp.Status.Message)
		}
	})

	t.Run("persistent 403 API failure is cached and does not retry across reconciles or controller restarts", func(t *testing.T) {
		server, reconciler, cl, comp, _ := setupFixture(t, configkeys.AffinityModeOrder)
		defer server.Close()

		// Inject persistent 403 Forbidden error
		server.InjectPathError("GET", "/repos/gke-labs/eval-runner/pulls/123/files", http.StatusForbidden, `{"message": "Resource not accessible by integration"}`)

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}
		// Reconcile 1
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("reconcile 1 failed: %v", err)
		}

		// Verify status records FallbackReason
		var compAfter1 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &compAfter1); err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if compAfter1.Status.Affinity == nil || compAfter1.Status.Affinity.FallbackReason == "" {
			t.Fatalf("expected FallbackReason in Status.Affinity, got %+v", compAfter1.Status.Affinity)
		}

		// Reconcile 2 with same reconciler
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("reconcile 2 failed: %v", err)
		}

		// Reconcile 3 with brand new reconciler (simulating controller restart, in-memory cache empty)
		freshReconciler := &ComparisonReconciler{
			Client:              cl,
			Scheme:              reconciler.Scheme,
			ArtifactSink:        reconciler.ArtifactSink,
			ControllerNamespace: reconciler.ControllerNamespace,
			GitHubTokenSecret:   reconciler.GitHubTokenSecret,
			AffinityMode:        reconciler.AffinityMode,
			GitHubClientFactory: func(creds GitHubCredentials) github.Client {
				return server.Client(creds.Token)
			},
		}
		if _, err := freshReconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("reconcile 3 (restart) failed: %v", err)
		}

		// Assert exactly 1 call was made to files API despite 3 reconciles and a controller restart
		fileCalls := 0
		for _, logEntry := range server.RequestLog() {
			if strings.Contains(logEntry, "/files") {
				fileCalls++
			}
		}
		if fileCalls != 1 {
			t.Errorf("expected exactly 1 files API call across reconciles and restart, got %d", fileCalls)
		}
	})

	t.Run("status.summary.tasks denominator is consistent between Running and Completed", func(t *testing.T) {
		t.Run("prune mode: planned denominator is identical while Running and Completed", func(t *testing.T) {
			server, reconciler, cl, comp, fakeGCS := setupFixture(t, configkeys.AffinityModePrune)
			defer server.Close()

			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}

			// Reconcile 1: batch 1 complete but inconclusive -> schedules batch 2 for taskDocs (prunes taskSrc)
			if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("first Reconcile failed: %v", err)
			}

			var compRunning v1alpha1.Comparison
			if err := cl.Get(t.Context(), req.NamespacedName, &compRunning); err != nil {
				t.Fatalf("Get comparison failed: %v", err)
			}
			if compRunning.Status.Phase != v1alpha1.ComparisonPhaseRunning {
				t.Fatalf("expected Phase %q, got %q", v1alpha1.ComparisonPhaseRunning, compRunning.Status.Phase)
			}
			if compRunning.Status.Summary == nil {
				t.Fatalf("expected non-nil Status.Summary")
			}
			if compRunning.Status.Summary.Tasks != "0/1" {
				t.Errorf("expected Running summary.tasks '0/1', got %q", compRunning.Status.Summary.Tasks)
			}
			if compRunning.Status.Summary.Skipped != 1 {
				t.Errorf("expected Running summary.skipped 1, got %d", compRunning.Status.Summary.Skipped)
			}

			runningParts := strings.Split(compRunning.Status.Summary.Tasks, "/")
			if len(runningParts) != 2 {
				t.Fatalf("malformed running summary.tasks: %q", compRunning.Status.Summary.Tasks)
			}
			runningDenominator := runningParts[1]

			// Also verify while batch 2 runs are in Running phase
			var b2Cand v1alpha1.Run
			b2CandName := fmt.Sprintf("%s-b2-cand", comp.Name)
			if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b2CandName}, &b2Cand); err != nil {
				t.Fatalf("failed to get batch 2 candidate run: %v", err)
			}
			b2Cand.Status.Phase = v1alpha1.RunPhaseRunning
			if err := cl.Status().Update(t.Context(), &b2Cand); err != nil {
				t.Fatalf("failed to set b2Cand to Running: %v", err)
			}

			if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("Reconcile while batch 2 running failed: %v", err)
			}

			if err := cl.Get(t.Context(), req.NamespacedName, &compRunning); err != nil {
				t.Fatalf("Get comparison failed: %v", err)
			}
			if compRunning.Status.Summary.Tasks != "0/1" {
				t.Errorf("expected summary.tasks '0/1' while batch 2 executes, got %q", compRunning.Status.Summary.Tasks)
			}

			// Complete batch 2 with decisive scores for taskDocs
			b2Range := v1alpha1.SampleRange{Start: 5, End: 8}
			decisiveCandScores := []stats.SampleScore{
				{ID: "5", Score: 1.0},
				{ID: "6", Score: 1.0},
				{ID: "7", Score: 1.0},
				{ID: "8", Score: 1.0},
			}
			decisiveBaseScores := []stats.SampleScore{
				{ID: "5", Score: 0.0},
				{ID: "6", Score: 0.0},
				{ID: "7", Score: 0.0},
				{ID: "8", Score: 0.0},
			}
			fakeGCS.Objects["test-bucket/runs/cand-commit-sha/b2-docs.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskDocs, decisiveCandScores)}
			fakeGCS.Objects["test-bucket/runs/base-commit-sha/b2-docs.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskDocs, decisiveBaseScores)}

			b2Cand.Status.Phase = v1alpha1.RunPhaseCompleted
			b2Cand.Status.Tasks = []v1alpha1.TaskStatus{
				{ID: taskDocs, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit-sha/b2-docs.json", Samples: 4, SampleRange: &b2Range},
			}
			if err := cl.Status().Update(t.Context(), &b2Cand); err != nil {
				t.Fatalf("failed to update b2Cand status: %v", err)
			}

			var b2Base v1alpha1.Run
			b2BaseName := fmt.Sprintf("%s-b2-base", comp.Name)
			if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b2BaseName}, &b2Base); err != nil {
				t.Fatalf("failed to get batch 2 baseline run: %v", err)
			}
			b2Base.Status.Phase = v1alpha1.RunPhaseCompleted
			b2Base.Status.Tasks = []v1alpha1.TaskStatus{
				{ID: taskDocs, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-sha/b2-docs.json", Samples: 4, SampleRange: &b2Range},
			}
			if err := cl.Status().Update(t.Context(), &b2Base); err != nil {
				t.Fatalf("failed to update b2Base status: %v", err)
			}

			// Terminal reconcile
			if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("terminal Reconcile failed: %v", err)
			}

			var compTerminal v1alpha1.Comparison
			if err := cl.Get(t.Context(), req.NamespacedName, &compTerminal); err != nil {
				t.Fatalf("Get terminal comparison failed: %v", err)
			}
			if compTerminal.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
				t.Fatalf("expected terminal Phase %q, got %q", v1alpha1.ComparisonPhaseCompleted, compTerminal.Status.Phase)
			}
			if compTerminal.Status.Summary == nil {
				t.Fatalf("expected non-nil terminal Status.Summary")
			}
			if compTerminal.Status.Summary.Tasks != "1/1" {
				t.Errorf("expected Completed summary.tasks '1/1', got %q", compTerminal.Status.Summary.Tasks)
			}
			if compTerminal.Status.Summary.Skipped != 1 {
				t.Errorf("expected Completed summary.skipped 1, got %d", compTerminal.Status.Summary.Skipped)
			}

			terminalParts := strings.Split(compTerminal.Status.Summary.Tasks, "/")
			if len(terminalParts) != 2 {
				t.Fatalf("malformed terminal summary.tasks: %q", compTerminal.Status.Summary.Tasks)
			}
			terminalDenominator := terminalParts[1]

			if runningDenominator != terminalDenominator {
				t.Errorf("expected running denominator %q to match terminal denominator %q in prune mode", runningDenominator, terminalDenominator)
			}
		})

		t.Run("non-prune mode: total denominator is identical while Running and Completed", func(t *testing.T) {
			server, reconciler, cl, comp, fakeGCS := setupFixture(t, configkeys.AffinityModeOrder)
			defer server.Close()

			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}

			// Reconcile 1: batch 1 complete but inconclusive -> schedules batch 2 for both tasks
			if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("first Reconcile failed: %v", err)
			}

			var compRunning v1alpha1.Comparison
			if err := cl.Get(t.Context(), req.NamespacedName, &compRunning); err != nil {
				t.Fatalf("Get comparison failed: %v", err)
			}
			if compRunning.Status.Phase != v1alpha1.ComparisonPhaseRunning {
				t.Fatalf("expected Phase %q, got %q", v1alpha1.ComparisonPhaseRunning, compRunning.Status.Phase)
			}
			if compRunning.Status.Summary == nil {
				t.Fatalf("expected non-nil Status.Summary")
			}
			if compRunning.Status.Summary.Tasks != "0/2" {
				t.Errorf("expected Running summary.tasks '0/2', got %q", compRunning.Status.Summary.Tasks)
			}
			if compRunning.Status.Summary.Skipped != 0 {
				t.Errorf("expected Running summary.skipped 0, got %d", compRunning.Status.Summary.Skipped)
			}

			runningParts := strings.Split(compRunning.Status.Summary.Tasks, "/")
			if len(runningParts) != 2 {
				t.Fatalf("malformed running summary.tasks: %q", compRunning.Status.Summary.Tasks)
			}
			runningDenominator := runningParts[1]

			// Complete batch 2 with decisive scores for both tasks
			b2Range := v1alpha1.SampleRange{Start: 5, End: 8}
			decisiveCandScores := []stats.SampleScore{
				{ID: "5", Score: 1.0},
				{ID: "6", Score: 1.0},
				{ID: "7", Score: 1.0},
				{ID: "8", Score: 1.0},
			}
			decisiveBaseScores := []stats.SampleScore{
				{ID: "5", Score: 0.0},
				{ID: "6", Score: 0.0},
				{ID: "7", Score: 0.0},
				{ID: "8", Score: 0.0},
			}
			fakeGCS.Objects["test-bucket/runs/cand-commit-sha/b2-docs.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskDocs, decisiveCandScores)}
			fakeGCS.Objects["test-bucket/runs/base-commit-sha/b2-docs.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskDocs, decisiveBaseScores)}
			fakeGCS.Objects["test-bucket/runs/cand-commit-sha/b2-src.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskSrc, decisiveCandScores)}
			fakeGCS.Objects["test-bucket/runs/base-commit-sha/b2-src.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskSrc, decisiveBaseScores)}

			var b2Cand v1alpha1.Run
			b2CandName := fmt.Sprintf("%s-b2-cand", comp.Name)
			if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b2CandName}, &b2Cand); err != nil {
				t.Fatalf("failed to get batch 2 candidate run: %v", err)
			}
			b2Cand.Status.Phase = v1alpha1.RunPhaseCompleted
			b2Cand.Status.Tasks = []v1alpha1.TaskStatus{
				{ID: taskDocs, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit-sha/b2-docs.json", Samples: 4, SampleRange: &b2Range},
				{ID: taskSrc, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit-sha/b2-src.json", Samples: 4, SampleRange: &b2Range},
			}
			if err := cl.Status().Update(t.Context(), &b2Cand); err != nil {
				t.Fatalf("failed to update b2Cand status: %v", err)
			}

			var b2Base v1alpha1.Run
			b2BaseName := fmt.Sprintf("%s-b2-base", comp.Name)
			if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: b2BaseName}, &b2Base); err != nil {
				t.Fatalf("failed to get batch 2 baseline run: %v", err)
			}
			b2Base.Status.Phase = v1alpha1.RunPhaseCompleted
			b2Base.Status.Tasks = []v1alpha1.TaskStatus{
				{ID: taskDocs, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-sha/b2-docs.json", Samples: 4, SampleRange: &b2Range},
				{ID: taskSrc, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-sha/b2-src.json", Samples: 4, SampleRange: &b2Range},
			}
			if err := cl.Status().Update(t.Context(), &b2Base); err != nil {
				t.Fatalf("failed to update b2Base status: %v", err)
			}

			// Terminal reconcile
			if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("terminal Reconcile failed: %v", err)
			}

			var compTerminal v1alpha1.Comparison
			if err := cl.Get(t.Context(), req.NamespacedName, &compTerminal); err != nil {
				t.Fatalf("Get terminal comparison failed: %v", err)
			}
			if compTerminal.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
				t.Fatalf("expected terminal Phase %q, got %q", v1alpha1.ComparisonPhaseCompleted, compTerminal.Status.Phase)
			}
			if compTerminal.Status.Summary == nil {
				t.Fatalf("expected non-nil terminal Status.Summary")
			}
			if compTerminal.Status.Summary.Tasks != "2/2" {
				t.Errorf("expected Completed summary.tasks '2/2', got %q", compTerminal.Status.Summary.Tasks)
			}
			if compTerminal.Status.Summary.Skipped != 0 {
				t.Errorf("expected Completed summary.skipped 0, got %d", compTerminal.Status.Summary.Skipped)
			}

			terminalParts := strings.Split(compTerminal.Status.Summary.Tasks, "/")
			if len(terminalParts) != 2 {
				t.Fatalf("malformed terminal summary.tasks: %q", compTerminal.Status.Summary.Tasks)
			}
			terminalDenominator := terminalParts[1]

			if runningDenominator != terminalDenominator {
				t.Errorf("expected running denominator %q to match terminal denominator %q in non-prune mode", runningDenominator, terminalDenominator)
			}
		})

		t.Run("prune mode: initial running reconcile before runs complete preserves denominator to completion", func(t *testing.T) {
			server, reconciler, cl, comp, fakeGCS := setupFixture(t, configkeys.AffinityModePrune)
			defer server.Close()

			// Set initial candidate and baseline runs to Running phase (not yet completed)
			var candRun v1alpha1.Run
			if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "cand-b1"}, &candRun); err != nil {
				t.Fatalf("Get cand-b1 failed: %v", err)
			}
			candRun.Status.Phase = v1alpha1.RunPhaseRunning
			candRun.Status.Tasks = nil
			if err := cl.Status().Update(t.Context(), &candRun); err != nil {
				t.Fatalf("Update cand-b1 failed: %v", err)
			}

			var baseRun v1alpha1.Run
			if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "base-b1"}, &baseRun); err != nil {
				t.Fatalf("Get base-b1 failed: %v", err)
			}
			baseRun.Status.Phase = v1alpha1.RunPhaseRunning
			baseRun.Status.Tasks = nil
			if err := cl.Status().Update(t.Context(), &baseRun); err != nil {
				t.Fatalf("Update base-b1 failed: %v", err)
			}

			// Clear sampling so single batch run decides completion
			comp.Spec.Sampling = nil
			if err := cl.Update(t.Context(), comp); err != nil {
				t.Fatalf("Update comp spec failed: %v", err)
			}

			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}}

			// Reconcile 1: runs are still running
			if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("running Reconcile failed: %v", err)
			}

			var compRunning v1alpha1.Comparison
			if err := cl.Get(t.Context(), req.NamespacedName, &compRunning); err != nil {
				t.Fatalf("Get comparison failed: %v", err)
			}
			if compRunning.Status.Phase != v1alpha1.ComparisonPhaseRunning {
				t.Fatalf("expected Phase %q, got %q", v1alpha1.ComparisonPhaseRunning, compRunning.Status.Phase)
			}
			if compRunning.Status.Summary == nil || compRunning.Status.Summary.Tasks != "0/1" {
				t.Errorf("expected initial running summary.tasks '0/1', got %+v", compRunning.Status.Summary)
			}
			if compRunning.Status.Summary.Skipped != 1 {
				t.Errorf("expected initial running summary.skipped 1, got %d", compRunning.Status.Summary.Skipped)
			}

			// Complete batch 1 runs decisively for taskDocs
			decisiveCandScores := []stats.SampleScore{
				{ID: "1", Score: 1.0},
				{ID: "2", Score: 1.0},
				{ID: "3", Score: 1.0},
				{ID: "4", Score: 1.0},
			}
			decisiveBaseScores := []stats.SampleScore{
				{ID: "1", Score: 0.0},
				{ID: "2", Score: 0.0},
				{ID: "3", Score: 0.0},
				{ID: "4", Score: 0.0},
			}
			fakeGCS.Objects["test-bucket/runs/cand-commit-sha/docs.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskDocs, decisiveCandScores)}
			fakeGCS.Objects["test-bucket/runs/base-commit-sha/docs.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskDocs, decisiveBaseScores)}

			b1Range := v1alpha1.SampleRange{Start: 1, End: 4}
			candRun.Status.Phase = v1alpha1.RunPhaseCompleted
			candRun.Status.Tasks = []v1alpha1.TaskStatus{
				{ID: taskDocs, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit-sha/docs.json", Samples: 4, SampleRange: &b1Range},
				{ID: taskSrc, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit-sha/src.json", Samples: 4, SampleRange: &b1Range},
			}
			if err := cl.Status().Update(t.Context(), &candRun); err != nil {
				t.Fatalf("Update candRun to Completed failed: %v", err)
			}

			baseRun.Status.Phase = v1alpha1.RunPhaseCompleted
			baseRun.Status.Tasks = []v1alpha1.TaskStatus{
				{ID: taskDocs, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-sha/docs.json", Samples: 4, SampleRange: &b1Range},
				{ID: taskSrc, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit-sha/src.json", Samples: 4, SampleRange: &b1Range},
			}
			if err := cl.Status().Update(t.Context(), &baseRun); err != nil {
				t.Fatalf("Update baseRun to Completed failed: %v", err)
			}

			// Terminal reconcile
			if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("terminal Reconcile failed: %v", err)
			}

			var compTerminal v1alpha1.Comparison
			if err := cl.Get(t.Context(), req.NamespacedName, &compTerminal); err != nil {
				t.Fatalf("Get terminal comparison failed: %v", err)
			}
			if compTerminal.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
				t.Fatalf("expected terminal Phase %q, got %q", v1alpha1.ComparisonPhaseCompleted, compTerminal.Status.Phase)
			}
			if compTerminal.Status.Summary == nil || compTerminal.Status.Summary.Tasks != "1/1" {
				t.Errorf("expected Completed summary.tasks '1/1', got %+v", compTerminal.Status.Summary)
			}
			if compTerminal.Status.Summary.Skipped != 1 {
				t.Errorf("expected Completed summary.skipped 1, got %d", compTerminal.Status.Summary.Skipped)
			}
		})
	})
}

func TestAdaptivePairedSampling_Budget(t *testing.T) {
	scheme := setupScheme(t)
	taskID := "docs/readme_comprehension"
	modelName := "test/budget-model"

	prices := map[string]ModelPrice{
		modelName: {
			InputPer1M:  0.50, // $0.50 / 1M tokens
			OutputPer1M: 0.50,
		},
	}

	t.Run("BudgetHitAfterBatch2_Inconclusive_NoBatch3", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		// Batch 1: 4 samples with noisy scores (cand 1,0,1,0; base 0,1,0,1).
		// 200,000 input tokens each -> 200,000 * 0.50 / 1M = $0.10 for cand, $0.10 for base = $0.20 per batch.
		b1CandScores := []stats.SampleScore{{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0}}
		b1BaseScores := []stats.SampleScore{{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 0.0}, {ID: "4", Score: 1.0}}
		fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, b1CandScores, 200_000, 0)}
		fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, b1BaseScores, 200_000, 0)}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       modelName,
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       modelName,
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
				},
			},
		}

		budgetUSD := 0.35
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-budget", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
				BudgetUSD: &budgetUSD,
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
			ModelPrices:  prices,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-budget"}}

		// Reconcile 1: Batch 1 evaluated ($0.20 spent < $0.35 budget). Schedules batch 2.
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		var compB2 v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &compB2); err != nil {
			t.Fatalf("Get comp failed: %v", err)
		}
		if len(compB2.Status.Batches) != 2 {
			t.Fatalf("Expected 2 batches after reconcile 1, got %d", len(compB2.Status.Batches))
		}
		if compB2.Status.Budget == nil || !compB2.Status.Budget.Enforced {
			t.Fatalf("Expected budget enforced in batch 2 status, got %+v", compB2.Status.Budget)
		}

		// Complete Batch 2: another $0.20 spent -> cumulative $0.40 >= $0.35 budget.
		b2CandScores := []stats.SampleScore{{ID: "5", Score: 1.0}, {ID: "6", Score: 0.0}, {ID: "7", Score: 1.0}, {ID: "8", Score: 0.0}}
		b2BaseScores := []stats.SampleScore{{ID: "5", Score: 0.0}, {ID: "6", Score: 1.0}, {ID: "7", Score: 0.0}, {ID: "8", Score: 1.0}}
		fakeGCS.Objects["test-bucket/runs/cand-commit/b2.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, b2CandScores, 200_000, 0)}
		fakeGCS.Objects["test-bucket/runs/base-commit/b2.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, b2BaseScores, 200_000, 0)}

		var candB2, baseB2 v1alpha1.Run
		_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-budget-b2-cand"}, &candB2)
		_ = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-budget-b2-base"}, &baseB2)
		candB2.Status.Phase = v1alpha1.RunPhaseCompleted
		candB2.Status.Tasks = []v1alpha1.TaskStatus{
			{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b2.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8}},
		}
		_ = cl.Status().Update(t.Context(), &candB2)
		baseB2.Status.Phase = v1alpha1.RunPhaseCompleted
		baseB2.Status.Tasks = []v1alpha1.TaskStatus{
			{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b2.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 5, End: 8}},
		}
		_ = cl.Status().Update(t.Context(), &baseB2)

		// Reconcile 2: Batch 2 evaluated. Total spent $0.40 >= $0.35 budget -> stops as inconclusive with budget reason.
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 2 failed: %v", err)
		}

		var compFinal v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &compFinal); err != nil {
			t.Fatalf("Get final comp failed: %v", err)
		}

		if compFinal.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("Expected Phase Completed, got %s", compFinal.Status.Phase)
		}
		if compFinal.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("Expected Verdict INCONCLUSIVE, got %s", compFinal.Status.Verdict)
		}
		if !strings.Contains(compFinal.Status.Message, "budget ($0.35) reached (spent $0.40)") {
			t.Errorf("Expected status.Message to contain budget reason, got %q", compFinal.Status.Message)
		}
		if compFinal.Status.Budget == nil {
			t.Fatalf("Expected status.Budget to be non-nil")
		}
		if !compFinal.Status.Budget.Enforced {
			t.Errorf("Expected status.Budget.Enforced true")
		}
		if compFinal.Status.Budget.LimitUSD == nil || *compFinal.Status.Budget.LimitUSD != 0.35 {
			t.Errorf("Expected LimitUSD 0.35, got %v", compFinal.Status.Budget.LimitUSD)
		}
		if compFinal.Status.Budget.SpentUSD == nil || math.Abs(*compFinal.Status.Budget.SpentUSD-0.40) > 1e-4 {
			t.Errorf("Expected SpentUSD 0.40, got %v", compFinal.Status.Budget.SpentUSD)
		}

		// Ensure NO batch 3 run was created
		var b3Cand v1alpha1.Run
		err = cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-budget-b3-cand"}, &b3Cand)
		if err == nil {
			t.Errorf("Expected no batch 3 candidate run to be created, but found one")
		}

		// Verify artifact in sink
		artData, ok := fakeGCS.Objects["test-bucket/comparisons/comp-budget.json"]
		if !ok {
			t.Fatalf("Expected comparison artifact to exist in sink")
		}
		var art ComparisonArtifact
		if err := json.Unmarshal(artData.Data, &art); err != nil {
			t.Fatalf("Failed to unmarshal artifact: %v", err)
		}
		if art.Budget == nil || !art.Budget.Enforced || art.Budget.LimitUSD == nil || *art.Budget.LimitUSD != 0.35 {
			t.Errorf("Expected artifact.Budget with limit 0.35, got %+v", art.Budget)
		}
	})

	t.Run("UnknownPrice_NotEnforced_NoteSet", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		unpricedModel := "unpriced/custom-model"
		b1Scores := []stats.SampleScore{{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0}}
		fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Scores)}
		fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(taskID, b1Scores)}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-unpriced", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       unpricedModel,
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-unpriced", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       modelName,
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
				},
			},
		}

		budgetUSD := 0.50
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-unpriced", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-unpriced"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-unpriced"},
				BudgetUSD: &budgetUSD,
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
			ModelPrices:  prices,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-unpriced"}}

		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var compUpdated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &compUpdated); err != nil {
			t.Fatalf("Get comp failed: %v", err)
		}

		if compUpdated.Status.Budget == nil {
			t.Fatalf("Expected status.Budget to be set")
		}
		if compUpdated.Status.Budget.Enforced {
			t.Errorf("Expected Enforced to be false for unknown price")
		}
		expectedNote := "cost unknown for unpriced/custom-model; budget not enforced"
		if compUpdated.Status.Budget.Note != expectedNote {
			t.Errorf("Expected note %q, got %q", expectedNote, compUpdated.Status.Budget.Note)
		}

		// Ensure batch 2 WAS created (not blocked by budget)
		if len(compUpdated.Status.Batches) != 2 {
			t.Errorf("Expected 2 batches scheduled, got %d", len(compUpdated.Status.Batches))
		}
	})

	t.Run("ShrinkToFit", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		// Batch 1: 8 samples.
		// 400,000 input tokens each -> 400,000 * 0.50 / 1M = $0.20 cand, $0.20 base = $0.40 total spent.
		// Avg cost per sample = $0.40 / 16 = $0.025. Cost per pair = $0.05.
		// Budget is $0.50. Remaining = $0.10. Available pairs that fit = 2.
		var b1CandScores, b1BaseScores []stats.SampleScore
		for i := 1; i <= 8; i++ {
			b1CandScores = append(b1CandScores, stats.SampleScore{ID: fmt.Sprint(i), Score: float64(i % 2)})
			b1BaseScores = append(b1BaseScores, stats.SampleScore{ID: fmt.Sprint(i), Score: float64((i + 1) % 2)})
		}
		fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, b1CandScores, 400_000, 0)}
		fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, b1BaseScores, 400_000, 0)}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-shrink", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       modelName,
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-shrink", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       modelName,
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 8, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 8}},
				},
			},
		}

		budgetUSD := 0.50
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-shrink", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-shrink"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-shrink"},
				BudgetUSD: &budgetUSD,
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         8, // Wants 8, but only 2 fit
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
			ModelPrices:  prices,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-shrink"}}

		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var compUpdated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &compUpdated); err != nil {
			t.Fatalf("Get comp failed: %v", err)
		}

		if len(compUpdated.Status.Batches) != 2 {
			t.Fatalf("Expected 2 batches, got %d", len(compUpdated.Status.Batches))
		}
		b2Range := compUpdated.Status.Batches[1].SampleRange
		if b2Range == nil || b2Range.Start != 9 || b2Range.End != 10 {
			t.Errorf("Expected batch 2 range shrunk to [9, 10], got %+v", b2Range)
		}
	})

	t.Run("ShrinkToFit_StopWhenOnePairExceeds", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		// Batch 1: 4 samples with noisy scores.
		// 300,000 input tokens each -> 300,000 * 0.50 / 1M = $0.15 cand, $0.15 base = $0.30 total spent.
		// Total samples = 8 (4 pairs). Avg cost per sample = $0.30 / 8 = $0.0375.
		// Cost per pair = $0.075.
		// Budget is $0.35. Remaining = $0.05 < $0.075 per pair -> fit = 0.
		b1CandScores := []stats.SampleScore{{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0}}
		b1BaseScores := []stats.SampleScore{{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 0.0}, {ID: "4", Score: 1.0}}
		fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, b1CandScores, 300_000, 0)}
		fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, b1BaseScores, 300_000, 0)}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-stop", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       modelName,
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-stop", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       modelName,
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
				},
			},
		}

		budgetUSD := 0.35
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-stop", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-stop"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-stop"},
				BudgetUSD: &budgetUSD,
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
			ModelPrices:  prices,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-stop"}}

		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var compUpdated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &compUpdated); err != nil {
			t.Fatalf("Get comp failed: %v", err)
		}

		if compUpdated.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
			t.Errorf("Expected Phase Completed, got %s", compUpdated.Status.Phase)
		}
		if compUpdated.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Errorf("Expected Verdict INCONCLUSIVE, got %s", compUpdated.Status.Verdict)
		}
		if len(compUpdated.Status.Batches) != 1 {
			t.Errorf("Expected exactly 1 batch, got %d", len(compUpdated.Status.Batches))
		}
		if !strings.Contains(compUpdated.Status.Message, "budget ($0.35) reached (spent $0.30)") {
			t.Errorf("Expected status.Message to contain budget reason, got %q", compUpdated.Status.Message)
		}

		// Ensure no batch 2 runs were created
		var b2Cand v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-stop-b2-cand"}, &b2Cand); err == nil {
			t.Errorf("Expected batch 2 candidate Run to not exist, but found one")
		}
		var b2Base v1alpha1.Run
		if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: "comp-stop-b2-base"}, &b2Base); err == nil {
			t.Errorf("Expected batch 2 baseline Run to not exist, but found one")
		}
	})

	t.Run("BudgetUSD_ZeroIsUnset", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		b1Scores := []stats.SampleScore{{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0}}
		fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, b1Scores, 200_000, 0)}
		fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, b1Scores, 200_000, 0)}

		zeroBudget := 0.0
		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-zero", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       modelName,
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-zero", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       modelName,
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
				},
			},
		}

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-zero", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-zero"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-zero"},
				BudgetUSD: &zeroBudget,
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
			ModelPrices:  prices,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-zero"}}

		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		var compUpdated v1alpha1.Comparison
		if err := cl.Get(t.Context(), req.NamespacedName, &compUpdated); err != nil {
			t.Fatalf("Get comp failed: %v", err)
		}

		// budgetUSD: 0 must not stop the comparison after batch 1; batch 2 should be scheduled
		if len(compUpdated.Status.Batches) != 2 {
			t.Errorf("Expected 2 batches scheduled for budgetUSD: 0, got %d", len(compUpdated.Status.Batches))
		}
		if compUpdated.Status.Budget != nil {
			t.Errorf("Expected status.Budget to be nil when budgetUSD is 0, got %+v", compUpdated.Status.Budget)
		}
	})

	t.Run("ExtraSamples_Bypass", func(t *testing.T) {
		fakeGCS := fakes.NewFakeStorageClient()
		sink, err := gcs.New(t.Context(), "gs://test-bucket", gcs.WithStorageClient(fakeGCS))
		if err != nil {
			t.Fatalf("Failed to create GCS sink: %v", err)
		}

		// Initial batch with 4 samples costing $0.40 >= budget $0.35 -> stops as inconclusive
		b1CandScores := []stats.SampleScore{{ID: "1", Score: 1.0}, {ID: "2", Score: 0.0}, {ID: "3", Score: 1.0}, {ID: "4", Score: 0.0}}
		b1BaseScores := []stats.SampleScore{{ID: "1", Score: 0.0}, {ID: "2", Score: 1.0}, {ID: "3", Score: 0.0}, {ID: "4", Score: 1.0}}
		fakeGCS.Objects["test-bucket/runs/cand-commit/b1.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, b1CandScores, 400_000, 0)}
		fakeGCS.Objects["test-bucket/runs/base-commit/b1.json"] = fakes.FakeObject{Data: makeInspectLogWithTokens(taskID, b1BaseScores, 400_000, 0)}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-extra", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "cand-commit"},
				Model:       modelName,
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/cand-commit/b1.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
				},
			},
		}
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-extra", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "base-commit"},
				Model:       modelName,
				SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4},
			},
			Status: v1alpha1.RunStatus{
				Phase: v1alpha1.RunPhaseCompleted,
				Tasks: []v1alpha1.TaskStatus{
					{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://test-bucket/runs/base-commit/b1.json", Samples: 4, SampleRange: &v1alpha1.SampleRange{Start: 1, End: 4}},
				},
			},
		}

		budgetUSD := 0.35
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-extra", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-extra"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-extra"},
				BudgetUSD: &budgetUSD,
				Sampling: &v1alpha1.ComparisonSamplingSpec{
					BatchSize:         4,
					MaxSamplesPerTask: 32,
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(candRun, baseRun, comp).
			WithStatusSubresource(candRun, baseRun, comp).
			Build()

		reconciler := &ComparisonReconciler{
			Client:       cl,
			Scheme:       scheme,
			ArtifactSink: sink,
			ModelPrices:  prices,
		}

		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-extra"}}

		// Reconcile 1: Reaches budget limit, stops with Completed & INCONCLUSIVE
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile 1 failed: %v", err)
		}

		var compB1 v1alpha1.Comparison
		_ = cl.Get(t.Context(), req.NamespacedName, &compB1)
		if compB1.Status.Phase != v1alpha1.ComparisonPhaseCompleted || compB1.Status.Verdict != v1alpha1.ComparisonVerdictInconclusive {
			t.Fatalf("Expected Completed & INCONCLUSIVE, got phase=%s, verdict=%s", compB1.Status.Phase, compB1.Status.Verdict)
		}

		// User requests extraSamples = 4
		compB1.Spec.ExtraSamples = 4
		if err := cl.Update(t.Context(), &compB1); err != nil {
			t.Fatalf("Update extraSamples failed: %v", err)
		}

		// Reconcile 2: extraSamples bypasses budget limit and schedules batch 2
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile with extraSamples failed: %v", err)
		}

		var compWithExtra v1alpha1.Comparison
		_ = cl.Get(t.Context(), req.NamespacedName, &compWithExtra)
		if len(compWithExtra.Status.Batches) != 2 {
			t.Fatalf("Expected 2 batches after extraSamples, got %d", len(compWithExtra.Status.Batches))
		}
		if compWithExtra.Status.Batches[1].SampleRange.Start != 5 || compWithExtra.Status.Batches[1].SampleRange.End != 8 {
			t.Errorf("Expected batch 2 range [5, 8], got %+v", compWithExtra.Status.Batches[1].SampleRange)
		}
		if compWithExtra.Status.ExtraSamplesApplied != 4 {
			t.Errorf("Expected extraSamplesApplied 4, got %d", compWithExtra.Status.ExtraSamplesApplied)
		}
	})
}

func TestComparisonReconciler_FakeForgeReporting(t *testing.T) {
	ctx := t.Context()
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	repoRef := forge.RepoRef{Host: "github.com", Owner: "gke-labs", Name: "eval-runner"}

	t.Run("successful comment upsert and status setting on fake forge", func(t *testing.T) {
		ff := forge.NewFakeForge("github.com")
		reg := forge.NewRegistry(ff)

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "comp-forge-test",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Number:     42,
					Repository: "gke-labs/eval-runner",
					HeadSHA:    "cand-sha-1234",
					BaseSHA:    "base-sha-5678",
				},
				Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run"},
				Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			},
			Status: v1alpha1.ComparisonStatus{
				Phase:   v1alpha1.ComparisonPhaseCompleted,
				Verdict: v1alpha1.ComparisonVerdictInconclusive,
			},
		}

		candRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "cand-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "https://github.com/gke-labs/eval-runner.git", Commit: "cand-sha-1234"},
				Model: "gemini-pro",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}

		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
			Spec: v1alpha1.RunSpec{
				Repo:  v1alpha1.RepoSpec{URL: "https://github.com/gke-labs/eval-runner.git", Commit: "base-sha-5678"},
				Model: "gemini-pro",
			},
			Status: v1alpha1.RunStatus{Phase: v1alpha1.RunPhaseCompleted},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(comp, candRun, baseRun).Build()

		r := &ComparisonReconciler{
			Client:        cl,
			Scheme:        scheme,
			ForgeRegistry: reg,
			Now:           time.Now,
		}

		err := r.reportToGitHub(ctx, comp, baseRun, candRun)
		if err != nil {
			t.Fatalf("reportToGitHub failed: %v", err)
		}

		if comp.Status.Reporting == nil || comp.Status.Reporting.CommentID == 0 {
			t.Errorf("expected non-zero CommentID, got %+v", comp.Status.Reporting)
		}
		if comp.Status.Reporting.CheckRunID == 0 {
			t.Errorf("expected non-zero CheckRunID, got %+v", comp.Status.Reporting)
		}

		calls := ff.GetUpsertCalls()
		if len(calls) != 1 {
			t.Fatalf("expected 1 UpsertComment call, got %d", len(calls))
		}
		if calls[0].ID != "42" || !calls[0].Ref.Equal(repoRef) {
			t.Errorf("call = %+v", calls[0])
		}

		statusCalls := ff.GetSetStatusCalls()
		if len(statusCalls) != 1 {
			t.Fatalf("expected 1 SetStatus call, got %d", len(statusCalls))
		}
		if statusCalls[0].SHA != "cand-sha-1234" {
			t.Errorf("statusCall = %+v", statusCalls[0])
		}
		if statusCalls[0].Status.Status != "completed" {
			t.Errorf("statusCall status = %q, want 'completed'", statusCalls[0].Status.Status)
		}
		if statusCalls[0].Status.Conclusion != "neutral" {
			t.Errorf("statusCall conclusion = %q, want 'neutral'", statusCalls[0].Status.Conclusion)
		}
	})

	t.Run("postUnserviceableCommandNote uses forge with empty key", func(t *testing.T) {
		ff := forge.NewFakeForge("github.com")
		reg := forge.NewRegistry(ff)

		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "comp-unserviceable",
				Namespace: "eval-runner",
			},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{
					Number:     101,
					Repository: "gke-labs/eval-runner",
				},
			},
		}

		r := &ComparisonReconciler{
			ForgeRegistry: reg,
		}

		if err := r.postUnserviceableCommandNote(ctx, comp, "budget exceeded"); err != nil {
			t.Fatalf("postUnserviceableCommandNote failed: %v", err)
		}

		calls := ff.GetUpsertCalls()
		if len(calls) != 1 {
			t.Fatalf("expected 1 UpsertComment call, got %d", len(calls))
		}
		if calls[0].ID != "101" || !strings.Contains(calls[0].Body, "budget exceeded") {
			t.Errorf("call = %+v", calls[0])
		}
		if calls[0].Key != "" {
			t.Errorf("expected empty key for plain unserviceable note, got %q", calls[0].Key)
		}
	})

	t.Run("forge without Reporter disables reporting cleanly", func(t *testing.T) {
		reg := forge.NewDefaultRegistry(nil, nil)
		comp := &v1alpha1.Comparison{
			ObjectMeta: metav1.ObjectMeta{Name: "comp-no-reporter", Namespace: "eval-runner"},
			Spec: v1alpha1.ComparisonSpec{
				PullRequest: &v1alpha1.PullRequestSpec{Number: 10, Repository: "gke-labs/eval-runner"},
			},
		}
		r := &ComparisonReconciler{ForgeRegistry: reg, Now: time.Now}
		if err := r.reportToGitHub(ctx, comp, nil, nil); err != nil {
			t.Fatalf("expected nil error when reporter is missing, got: %v", err)
		}
		if comp.Status.Reporting == nil || comp.Status.Reporting.Message != "disabled" {
			t.Errorf("expected reporting message 'disabled', got %+v", comp.Status.Reporting)
		}
	})
}

func TestEnsureBatchRun_InheritsLabels(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-391",
			Namespace: "eval-runner",
			UID:       types.UID("comp-uid-391"),
		},
		Spec: v1alpha1.ComparisonSpec{
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: "gke-labs/eval-runner",
				Number:     391,
			},
		},
	}

	candTemplate := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr-391-184dfbc",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelTargetRepo: "gke-labs_eval-runner",
				LabelRepo:       "codebot-robot_eval-runner",
				LabelPR:         "391",
				LabelHeadSHA:    "184dfbc",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/codebot-robot/eval-runner.git",
				Commit: "184dfbc",
			},
			Model: "mockllm/model",
		},
	}

	baseTemplate := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-a81d0c4",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelTargetRepo: "gke-labs_eval-runner",
				LabelRepo:       "gke-labs_eval-runner",
				LabelHeadSHA:    "a81d0c4",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "a81d0c4",
			},
			Model: "mockllm/model",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(comp, candTemplate, baseTemplate).
		Build()

	r := &ComparisonReconciler{
		Client: cl,
		Scheme: scheme,
	}

	tasks := []string{"docs/test_task"}
	window := v1alpha1.SampleRange{Start: 9, End: 16}

	// 1. Candidate batch Run
	candBatchRunName, err := r.ensureBatchRun(t.Context(), comp, candTemplate, RoleCandidate, tasks, window, 2)
	if err != nil {
		t.Fatalf("ensureBatchRun for candidate failed: %v", err)
	}
	if candBatchRunName != "pr-391-b2-cand" {
		t.Errorf("expected candidate batch run name 'pr-391-b2-cand', got %q", candBatchRunName)
	}

	var candBatchRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: candBatchRunName}, &candBatchRun); err != nil {
		t.Fatalf("failed to get candidate batch run: %v", err)
	}

	// Verify all inherited labels
	expectedCandLabels := map[string]string{
		LabelComparison: "pr-391",
		LabelBatch:      "2",
		LabelRole:       RoleCandidate,
		LabelTargetRepo: "gke-labs_eval-runner",
		LabelRepo:       "codebot-robot_eval-runner",
		LabelPR:         "391",
		LabelHeadSHA:    "184dfbc",
	}
	for k, want := range expectedCandLabels {
		if got := candBatchRun.Labels[k]; got != want {
			t.Errorf("candidate batch run label %s = %q, want %q", k, got, want)
		}
	}

	// 2. Baseline batch Run
	baseBatchRunName, err := r.ensureBatchRun(t.Context(), comp, baseTemplate, RoleBaseline, tasks, window, 2)
	if err != nil {
		t.Fatalf("ensureBatchRun for baseline failed: %v", err)
	}
	if baseBatchRunName != "pr-391-b2-base" {
		t.Errorf("expected baseline batch run name 'pr-391-b2-base', got %q", baseBatchRunName)
	}

	var baseBatchRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: baseBatchRunName}, &baseBatchRun); err != nil {
		t.Fatalf("failed to get baseline batch run: %v", err)
	}

	expectedBaseLabels := map[string]string{
		LabelComparison: "pr-391",
		LabelBatch:      "2",
		LabelRole:       RoleBaseline,
		LabelTargetRepo: "gke-labs_eval-runner",
		LabelRepo:       "gke-labs_eval-runner",
		LabelHeadSHA:    "a81d0c4",
	}
	for k, want := range expectedBaseLabels {
		if got := baseBatchRun.Labels[k]; got != want {
			t.Errorf("baseline batch run label %s = %q, want %q", k, got, want)
		}
	}

	// 3. Template Run without target-repo label falls back to comp.Spec.PullRequest.Repository
	legacyTemplate := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-cand",
			Namespace: "eval-runner",
			Labels: map[string]string{
				LabelRepo:    "codebot-robot_eval-runner",
				LabelPR:      "391",
				LabelHeadSHA: "184dfbc",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/codebot-robot/eval-runner.git",
				Commit: "184dfbc",
			},
			Model: "mockllm/model",
		},
	}
	legacyBatchRunName, err := r.ensureBatchRun(t.Context(), comp, legacyTemplate, RoleCandidate, tasks, window, 3)
	if err != nil {
		t.Fatalf("ensureBatchRun for legacy template failed: %v", err)
	}
	var legacyBatchRun v1alpha1.Run
	if err := cl.Get(t.Context(), types.NamespacedName{Namespace: "eval-runner", Name: legacyBatchRunName}, &legacyBatchRun); err != nil {
		t.Fatalf("failed to get legacy batch run: %v", err)
	}
	if got := legacyBatchRun.Labels[LabelTargetRepo]; got != "gke-labs_eval-runner" {
		t.Errorf("legacy batch run target-repo label = %q, want %q", got, "gke-labs_eval-runner")
	}
}

func TestComparisonReconciler_HungSinkCallsBounded(t *testing.T) {
	scheme := setupScheme(t)

	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cand-run-hung",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/org/repo.git",
				Commit: "c123456",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskIDs: []string{"task-1"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      "task-1",
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://bucket/runs/c123456/task-1/eval.json",
				},
			},
		},
	}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-run-hung",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/org/repo.git",
				Commit: "b123456",
			},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Discovery: &v1alpha1.RunDiscoveryStatus{
				TaskIDs: []string{"task-1"},
			},
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:      "task-1",
					Phase:   v1alpha1.TaskPhaseCompleted,
					LogPath: "gs://bucket/runs/b123456/task-1/eval.json",
				},
			},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "comp-hung",
			Namespace: "eval-runner",
		},
		Spec: v1alpha1.ComparisonSpec{
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand-run-hung"},
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run-hung"},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase: v1alpha1.ComparisonPhaseRunning,
		},
	}

	hungCh := make(chan struct{})
	defer close(hungCh)
	sink := &fakeBlockingOrFailingSink{
		storeManifestFn: nil,
	}
	// override Load on fakeBlockingOrFailingSink
	blockingSink := &blockingSinkComparison{
		loadFn: func(ctx context.Context, path string) ([]byte, error) {
			<-hungCh
			return []byte("{}"), nil
		},
	}
	_ = sink

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(candRun, baseRun, comp).
		WithStatusSubresource(comp).
		Build()

	r := &ComparisonReconciler{
		Client:              cl,
		Scheme:              scheme,
		ArtifactSink:        blockingSink,
		ArtifactSinkTimeout: 50 * time.Millisecond,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: "comp-hung"}}
	start := time.Now()
	_, _ = r.Reconcile(t.Context(), req)
	elapsed := time.Since(start)

	if elapsed > 3*time.Second {
		t.Errorf("Reconcile took too long (%v), expected ~50ms", elapsed)
	}

	var updated v1alpha1.Comparison
	_ = cl.Get(t.Context(), req.NamespacedName, &updated)
	if !strings.Contains(updated.Status.Message, "failed to load candidate log") {
		t.Errorf("expected status message to contain 'failed to load candidate log', got: %s", updated.Status.Message)
	}
	if !strings.Contains(updated.Status.Message, "task-1") {
		t.Errorf("expected status message to contain task target 'task-1', got: %s", updated.Status.Message)
	}
	if !strings.Contains(updated.Status.Message, "sink did not report completion within deadline") {
		t.Errorf("expected status message to contain 'sink did not report completion within deadline', got: %s", updated.Status.Message)
	}
}

type blockingSinkComparison struct {
	loadFn func(ctx context.Context, path string) ([]byte, error)
}

func (b *blockingSinkComparison) StoreArtifact(ctx context.Context, meta artifacts.RunMetadata, filename string, data []byte) (string, error) {
	return "", nil
}

func (b *blockingSinkComparison) StoreComparison(ctx context.Context, comparisonName string, data []byte) (string, error) {
	return "", nil
}

func (b *blockingSinkComparison) StoreManifest(ctx context.Context, meta artifacts.RunMetadata, data []byte) (string, error) {
	return "", nil
}

func (b *blockingSinkComparison) Load(ctx context.Context, path string) ([]byte, error) {
	if b.loadFn != nil {
		return b.loadFn(ctx, path)
	}
	return []byte("{}"), nil
}

func (b *blockingSinkComparison) ArtifactPrefix(commit string) string {
	return "gs://bucket/runs/" + commit + "/"
}

func (b *blockingSinkComparison) IsDurable() bool {
	return true
}

func (b *blockingSinkComparison) Close() error {
	return nil
}

type headMoveFixture struct {
	taskID     string
	baseSHA    string
	cand1SHA   string
	cand2SHA   string
	baseRun    *v1alpha1.Run
	cand1Run   *v1alpha1.Run
	cand2Run   *v1alpha1.Run
	comp       *v1alpha1.Comparison
	client     client.Client
	reconciler *ComparisonReconciler
	server     *github.FakeGitHubServer
	req        ctrl.Request
}

func newHeadMoveFixture(t *testing.T, withGitHub bool) *headMoveFixture {
	t.Helper()
	ctx := t.Context()
	scheme := setupScheme(t)

	taskID := "docs/readme_comprehension"
	cand1SHA := "1111111111111111111111111111111111111111"
	cand2SHA := "2222222222222222222222222222222222222222"
	baseSHA := "0000000000000000000000000000000000000000"

	fakeGCS := fakes.NewFakeStorageClient()
	sink, err := gcs.New(ctx, "gs://eval-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("failed to create GCS sink: %v", err)
	}

	baseInspect := makeSyntheticInspectLog(taskID, []stats.SampleScore{{ID: "1", Score: 0.0}})
	cand1Inspect := makeSyntheticInspectLog(taskID, []stats.SampleScore{{ID: "1", Score: 0.0}})
	cand2Inspect := makeSyntheticInspectLog(taskID, []stats.SampleScore{{ID: "1", Score: 1.0}})

	baseLogPath := "gs://eval-bucket/runs/" + baseSHA + "/" + taskID + "/inspect.json"
	cand1LogPath := "gs://eval-bucket/runs/" + cand1SHA + "/" + taskID + "/inspect.json"
	cand2LogPath := "gs://eval-bucket/runs/" + cand2SHA + "/" + taskID + "/inspect.json"

	fakeGCS.Objects["eval-bucket/runs/"+baseSHA+"/"+taskID+"/inspect.json"] = fakes.FakeObject{Data: baseInspect}
	fakeGCS.Objects["eval-bucket/runs/"+cand1SHA+"/"+taskID+"/inspect.json"] = fakes.FakeObject{Data: cand1Inspect}
	fakeGCS.Objects["eval-bucket/runs/"+cand2SHA+"/"+taskID+"/inspect.json"] = fakes.FakeObject{Data: cand2Inspect}

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: baseSHA},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: baseLogPath, Samples: 1},
			},
			Summary: &v1alpha1.RunSummary{TotalSamples: 1},
		},
	}
	cand1Run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand1-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: cand1SHA},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: cand1LogPath, Samples: 1},
			},
			Summary: &v1alpha1.RunSummary{TotalSamples: 1},
		},
	}
	cand2Run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand2-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: cand2SHA},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: cand2LogPath, Samples: 1},
			},
			Summary: &v1alpha1.RunSummary{TotalSamples: 1},
		},
	}

	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "pr-test", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: "base-run"},
			Candidate: v1alpha1.ComparisonRunRef{Run: "cand1-run"},
		},
	}

	builder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cand1Run, cand2Run, baseRun, comp).
		WithStatusSubresource(cand1Run, cand2Run, baseRun, comp)

	var server *github.FakeGitHubServer
	reconciler := &ComparisonReconciler{
		Scheme:              scheme,
		ArtifactSink:        sink,
		ControllerNamespace: "eval-runner",
	}

	if withGitHub {
		comp.Spec.PullRequest = &v1alpha1.PullRequestSpec{
			Repository: "gke-labs/eval-runner",
			Number:     440,
			HeadSHA:    cand1SHA,
			BaseSHA:    baseSHA,
		}
		server = github.NewFakeGitHubServer()
		t.Cleanup(server.Close)

		privKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("failed to generate test RSA key: %v", err)
		}
		pemBytes := pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privKey),
		})
		appSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-token", Namespace: "eval-runner"},
			Data: map[string][]byte{
				"app-id":          []byte("12345"),
				"installation-id": []byte("67890"),
				"private-key":     pemBytes,
			},
		}
		builder.WithObjects(appSecret)
		reconciler.GitHubTokenSecret = "github-token"
		reconciler.GitHubClientFactory = func(creds GitHubCredentials) github.Client {
			return server.Client("app-inst-token")
		}
	}

	cl := builder.Build()
	reconciler.Client = cl

	return &headMoveFixture{
		taskID:     taskID,
		baseSHA:    baseSHA,
		cand1SHA:   cand1SHA,
		cand2SHA:   cand2SHA,
		baseRun:    baseRun,
		cand1Run:   cand1Run,
		cand2Run:   cand2Run,
		comp:       comp,
		client:     cl,
		reconciler: reconciler,
		server:     server,
		req:        ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "eval-runner", Name: comp.Name}},
	}
}

func TestComparisonReconciler_HeadMove_RescoresAgainstNewCandidate(t *testing.T) {
	ctx := t.Context()
	f := newHeadMoveFixture(t, true)

	// 1. Initial reconcile: cand1 is evaluated and completes
	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}

	var curComp v1alpha1.Comparison
	if err := f.client.Get(ctx, f.req.NamespacedName, &curComp); err != nil {
		t.Fatalf("get comp failed: %v", err)
	}
	if curComp.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Fatalf("expected phase Completed on first run, got %v", curComp.Status.Phase)
	}
	if curComp.Status.ObservedCandidateRun != "cand1-run" {
		t.Errorf("expected ObservedCandidateRun 'cand1-run', got %q", curComp.Status.ObservedCandidateRun)
	}
	if curComp.Status.Reporting == nil || curComp.Status.Reporting.CommentID == 0 {
		t.Fatalf("expected non-zero commentID in status.reporting")
	}
	origCommentID := curComp.Status.Reporting.CommentID

	comments := f.server.Comments()
	if len(comments) != 1 {
		t.Fatalf("expected 1 comment after first reconcile, got %d", len(comments))
	}

	// 2. Head moves to cand2: forge consumer updates spec.candidate.run and spec.pullRequest.headSHA
	curComp.Spec.Candidate.Run = "cand2-run"
	curComp.Spec.PullRequest.HeadSHA = f.cand2SHA
	if err := f.client.Update(ctx, &curComp); err != nil {
		t.Fatalf("update comp failed: %v", err)
	}

	// Reconcile: should detect candidate change, reset status, create new batch against cand2,
	// and update the existing PR comment in place.
	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}

	if err := f.client.Get(ctx, f.req.NamespacedName, &curComp); err != nil {
		t.Fatalf("get comp after head-move failed: %v", err)
	}
	if curComp.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("expected completed phase for cand2, got %v", curComp.Status.Phase)
	}
	if curComp.Status.ObservedCandidateRun != "cand2-run" {
		t.Errorf("expected ObservedCandidateRun 'cand2-run', got %q", curComp.Status.ObservedCandidateRun)
	}
	if len(curComp.Status.Batches) == 0 || curComp.Status.Batches[0].CandidateRun != "cand2-run" {
		t.Errorf("expected batch 0 CandidateRun 'cand2-run', got %+v", curComp.Status.Batches)
	}
	if curComp.Status.Reporting == nil || curComp.Status.Reporting.CommentID != origCommentID {
		t.Errorf("expected commentID preserved (%d), got %+v", origCommentID, curComp.Status.Reporting)
	}

	// Comments: single comment updated in place
	comments = f.server.Comments()
	if len(comments) != 1 {
		t.Fatalf("expected 1 comment updated in place, got %d", len(comments))
	}
	if comments[0].ID != origCommentID {
		t.Errorf("expected comment ID %d, got %d", origCommentID, comments[0].ID)
	}

	// Baseline run was NOT rerun: exactly 1 baseline run exists
	var runList v1alpha1.RunList
	if err := f.client.List(ctx, &runList); err != nil {
		t.Fatalf("list runs failed: %v", err)
	}
	baselineRuns := 0
	for _, r := range runList.Items {
		if r.Spec.Repo.Commit == f.baseSHA {
			baselineRuns++
		}
	}
	if baselineRuns != 1 {
		t.Errorf("expected exactly 1 baseline run, got %d", baselineRuns)
	}
}

func TestComparisonReconciler_BatchDiscovery_StaleExtensionRunWithDifferentCommitNotAdopted(t *testing.T) {
	ctx := t.Context()
	f := newHeadMoveFixture(t, false)

	// Update comp to point to cand2-run initially
	var curComp v1alpha1.Comparison
	if err := f.client.Get(ctx, f.req.NamespacedName, &curComp); err != nil {
		t.Fatalf("get comp failed: %v", err)
	}
	curComp.Spec.Candidate.Run = "cand2-run"
	if err := f.client.Update(ctx, &curComp); err != nil {
		t.Fatalf("update comp failed: %v", err)
	}

	// Pre-existing extension Run from previous head with stale commit "old-cand-commit"
	staleBatch2Run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-b2-cand", f.comp.Name), Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: "old-cand-commit"},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 2, End: 2},
		},
	}
	if err := f.client.Create(ctx, staleBatch2Run); err != nil {
		t.Fatalf("create stale batch run failed: %v", err)
	}

	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if err := f.client.Get(ctx, f.req.NamespacedName, &curComp); err != nil {
		t.Fatalf("get comp failed: %v", err)
	}

	// 1. Verify batch discovery loop ignores stale batch 2 run
	if len(curComp.Status.Batches) != 1 {
		t.Errorf("expected exactly 1 batch (stale b2-cand ignored), got %d: %+v", len(curComp.Status.Batches), curComp.Status.Batches)
	}

	// 2. Verify ensureBatchRun refuses to adopt stale existingRun and returns an explicit error
	_, err := f.reconciler.ensureBatchRun(ctx, &curComp, f.cand2Run, RoleCandidate, []string{f.taskID}, v1alpha1.SampleRange{Start: 2, End: 2}, 2)
	if err == nil || !strings.Contains(err.Error(), "stale batch run") || !strings.Contains(err.Error(), "cleanup pending") {
		t.Fatalf("expected stale batch run error, got %v", err)
	}

	// 3. Verify ensureBatchRun also refuses terminating run with DeletionTimestamp
	staleBatch2Run.Finalizers = []string{"test-finalizer"}
	staleBatch2Run.Spec.Repo.Commit = f.cand2SHA
	if err := f.client.Update(ctx, staleBatch2Run); err != nil {
		t.Fatalf("update run with finalizer failed: %v", err)
	}
	if err := f.client.Delete(ctx, staleBatch2Run); err != nil {
		t.Fatalf("delete run with finalizer failed: %v", err)
	}
	_, err = f.reconciler.ensureBatchRun(ctx, &curComp, f.cand2Run, RoleCandidate, []string{f.taskID}, v1alpha1.SampleRange{Start: 2, End: 2}, 2)
	if err == nil || !strings.Contains(err.Error(), "waiting for cleanup") {
		t.Fatalf("expected terminating batch run error, got %v", err)
	}
}

func TestComparisonReconciler_HeadMove_ExtraSamplesWorksAfterReset(t *testing.T) {
	ctx := t.Context()
	f := newHeadMoveFixture(t, false)

	// Provide 8 decided samples for base, cand1, cand2
	var b1Cand, b1Base []stats.SampleScore
	for i := 1; i <= 8; i++ {
		b1Cand = append(b1Cand, stats.SampleScore{ID: fmt.Sprint(i), Score: 1.0})
		b1Base = append(b1Base, stats.SampleScore{ID: fmt.Sprint(i), Score: 0.0})
	}
	fakeGCS := fakes.NewFakeStorageClient()
	fakeGCS.Objects["eval-bucket/runs/"+f.baseSHA+"/"+f.taskID+"/inspect.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(f.taskID, b1Base)}
	fakeGCS.Objects["eval-bucket/runs/"+f.cand1SHA+"/"+f.taskID+"/inspect.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(f.taskID, b1Cand)}
	fakeGCS.Objects["eval-bucket/runs/"+f.cand2SHA+"/"+f.taskID+"/inspect.json"] = fakes.FakeObject{Data: makeSyntheticInspectLog(f.taskID, b1Cand)}
	sink, err := gcs.New(ctx, "gs://eval-bucket", gcs.WithStorageClient(fakeGCS))
	if err != nil {
		t.Fatalf("failed to create GCS sink: %v", err)
	}
	f.reconciler.ArtifactSink = sink

	// Update baseRun, cand1Run, cand2Run with 8 samples
	for _, r := range []*v1alpha1.Run{f.baseRun, f.cand1Run, f.cand2Run} {
		r.Spec.SampleRange = &v1alpha1.SampleRange{Start: 1, End: 8}
		r.Status.Tasks[0].Samples = 8
		r.Status.Tasks[0].SampleRange = &v1alpha1.SampleRange{Start: 1, End: 8}
		r.Status.Summary.TotalSamples = 8
		if err := f.client.Update(ctx, r); err != nil {
			t.Fatalf("update run failed: %v", err)
		}
	}

	var curComp v1alpha1.Comparison
	if err := f.client.Get(ctx, f.req.NamespacedName, &curComp); err != nil {
		t.Fatalf("get comp failed: %v", err)
	}
	curComp.Spec.Sampling = &v1alpha1.ComparisonSamplingSpec{
		BatchSize:         8,
		MaxSamplesPerTask: 32,
	}
	if err := f.client.Update(ctx, &curComp); err != nil {
		t.Fatalf("update comp failed: %v", err)
	}

	// 1. Initial reconcile: cand1 batch 1 completes
	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}
	if err := f.client.Get(ctx, f.req.NamespacedName, &curComp); err != nil {
		t.Fatalf("get comp failed: %v", err)
	}
	if curComp.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Fatalf("expected phase Completed on first run, got %v", curComp.Status.Phase)
	}

	// Seed stale pr-test-b2-cand with old commit cand1SHA, LabelComparison and LabelRole labels, and discovery sandbox finalizer
	staleB2Cand := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       fmt.Sprintf("%s-b2-cand", curComp.Name),
			Namespace:  "eval-runner",
			Labels:     map[string]string{LabelComparison: curComp.Name, LabelRole: RoleCandidate},
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: f.cand1SHA},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 9, End: 16},
		},
	}
	if err := f.client.Create(ctx, staleB2Cand); err != nil {
		t.Fatalf("create stale batch 2 run failed: %v", err)
	}

	// 2. Head moves to cand2: forge consumer updates spec.candidate.run
	curComp.Spec.Candidate.Run = "cand2-run"
	if err := f.client.Update(ctx, &curComp); err != nil {
		t.Fatalf("update comp failed: %v", err)
	}

	// Reconcile: status resets, cand2 batch 1 evaluates and completes.
	// The head-move reset path deletes stale Runs matching LabelComparison whose commit != new candidate/baseline.
	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}

	// Stale b2-cand had a finalizer, so deletion marks it with DeletionTimestamp
	var terminatingB2Cand v1alpha1.Run
	if err := f.client.Get(ctx, types.NamespacedName{Namespace: staleB2Cand.Namespace, Name: staleB2Cand.Name}, &terminatingB2Cand); err != nil {
		t.Fatalf("failed getting stale batch 2 run: %v", err)
	}
	if terminatingB2Cand.DeletionTimestamp == nil {
		t.Fatalf("expected DeletionTimestamp on stale b2-cand after head move")
	}

	if err := f.client.Get(ctx, f.req.NamespacedName, &curComp); err != nil {
		t.Fatalf("get comp after reset failed: %v", err)
	}
	if curComp.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("expected phase Completed for cand2, got %v", curComp.Status.Phase)
	}
	if curComp.Status.ObservedCandidateRun != "cand2-run" {
		t.Errorf("expected ObservedCandidateRun 'cand2-run', got %q", curComp.Status.ObservedCandidateRun)
	}
	if curComp.Status.ExtraSamplesApplied != 0 {
		t.Errorf("expected ExtraSamplesApplied 0 after reset, got %d", curComp.Status.ExtraSamplesApplied)
	}
	if len(curComp.Status.Batches) != 1 {
		t.Errorf("expected 1 batch for cand2 before extra samples, got %d", len(curComp.Status.Batches))
	}

	// 3. User requests /eval more after reset: spec.extraSamples = 8
	curComp.Spec.ExtraSamples = 8
	if err := f.client.Update(ctx, &curComp); err != nil {
		t.Fatalf("update extra samples failed: %v", err)
	}

	// Reconcile: ensureBatchRun detects terminating b2-cand and returns RequeueAfter 15s instead of an error!
	result, err := f.reconciler.Reconcile(ctx, f.req)
	if err != nil {
		t.Fatalf("expected no error while waiting for terminating batch run, got %v", err)
	}
	if result.RequeueAfter != 15*time.Second {
		t.Fatalf("expected RequeueAfter: 15s, got %v", result.RequeueAfter)
	}

	// Now simulate the Run controller finishing cleanup and removing the finalizer:
	terminatingB2Cand.Finalizers = nil
	if err := f.client.Update(ctx, &terminatingB2Cand); err != nil {
		t.Fatalf("failed removing finalizer: %v", err)
	}
	if err := f.client.Delete(ctx, &terminatingB2Cand); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("failed deleting stale run after finalizer removed: %v", err)
	}

	// Next reconcile: now that stale run is gone, fresh batch 2 for cand2 is scheduled with no cleanup pending error!
	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("reconcile after stale run cleanup failed: %v", err)
	}

	if err := f.client.Get(ctx, f.req.NamespacedName, &curComp); err != nil {
		t.Fatalf("get comp after extra samples failed: %v", err)
	}
	if curComp.Status.ExtraSamplesApplied != 8 {
		t.Errorf("expected ExtraSamplesApplied 8 for cand2, got %d", curComp.Status.ExtraSamplesApplied)
	}
	if len(curComp.Status.Batches) != 2 {
		t.Errorf("expected 2 batches for cand2, got %d", len(curComp.Status.Batches))
	}

	var cand2B2 v1alpha1.Run
	if err := f.client.Get(ctx, types.NamespacedName{Namespace: "eval-runner", Name: fmt.Sprintf("%s-b2-cand", curComp.Name)}, &cand2B2); err != nil {
		t.Fatalf("expected new batch 2 run for cand2: %v", err)
	}
	if cand2B2.Spec.Repo.Commit != f.cand2SHA {
		t.Errorf("expected new batch 2 run to have commit %q, got %q", f.cand2SHA, cand2B2.Spec.Repo.Commit)
	}
}

func TestComparisonReconciler_HeadMove_CurrentCandidateAndBaselineRunsNotDeleted(t *testing.T) {
	ctx := t.Context()
	f := newHeadMoveFixture(t, false)

	// Complete initial reconcile with cand1
	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}

	// Create an existing extension Run for the current baseline (commit f.baseSHA)
	baseBatchRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       fmt.Sprintf("%s-b2-base", f.comp.Name),
			Namespace:  "eval-runner",
			Labels:     map[string]string{LabelComparison: f.comp.Name, LabelRole: RoleBaseline},
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: f.baseSHA},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 2, End: 2},
		},
	}
	if err := f.client.Create(ctx, baseBatchRun); err != nil {
		t.Fatalf("create base batch run failed: %v", err)
	}

	// Create an existing extension Run for the new candidate (commit f.cand2SHA)
	cand2BatchRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       fmt.Sprintf("%s-b3-cand", f.comp.Name),
			Namespace:  "eval-runner",
			Labels:     map[string]string{LabelComparison: f.comp.Name, LabelRole: RoleCandidate},
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: f.cand2SHA},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 3, End: 3},
		},
	}
	if err := f.client.Create(ctx, cand2BatchRun); err != nil {
		t.Fatalf("create cand2 batch run failed: %v", err)
	}

	// Create a stale extension Run for old candidate (commit f.cand1SHA)
	staleCand1BatchRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       fmt.Sprintf("%s-b2-cand", f.comp.Name),
			Namespace:  "eval-runner",
			Labels:     map[string]string{LabelComparison: f.comp.Name, LabelRole: RoleCandidate},
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: f.cand1SHA},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 2, End: 2},
		},
	}
	if err := f.client.Create(ctx, staleCand1BatchRun); err != nil {
		t.Fatalf("create stale cand1 batch run failed: %v", err)
	}

	// Head moves to cand2: update spec.candidate.run
	var curComp v1alpha1.Comparison
	if err := f.client.Get(ctx, f.req.NamespacedName, &curComp); err != nil {
		t.Fatalf("get comp failed: %v", err)
	}
	curComp.Spec.Candidate.Run = "cand2-run"
	if err := f.client.Update(ctx, &curComp); err != nil {
		t.Fatalf("update comp failed: %v", err)
	}

	// Reconcile head-move
	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("head-move reconcile failed: %v", err)
	}

	// Verify current baseline seed run and current baseline batch run are NOT deleted
	var checkBaseRun v1alpha1.Run
	if err := f.client.Get(ctx, types.NamespacedName{Namespace: f.baseRun.Namespace, Name: f.baseRun.Name}, &checkBaseRun); err != nil {
		t.Fatalf("expected baseline run to exist: %v", err)
	}
	if checkBaseRun.DeletionTimestamp != nil {
		t.Errorf("baseline run should not have DeletionTimestamp")
	}

	var checkBaseBatchRun v1alpha1.Run
	if err := f.client.Get(ctx, types.NamespacedName{Namespace: baseBatchRun.Namespace, Name: baseBatchRun.Name}, &checkBaseBatchRun); err != nil {
		t.Fatalf("expected baseline batch run to exist: %v", err)
	}
	if checkBaseBatchRun.DeletionTimestamp != nil {
		t.Errorf("baseline batch run should not have DeletionTimestamp")
	}

	// Verify current candidate seed run and current candidate batch run are NOT deleted
	var checkCand2Run v1alpha1.Run
	if err := f.client.Get(ctx, types.NamespacedName{Namespace: f.cand2Run.Namespace, Name: f.cand2Run.Name}, &checkCand2Run); err != nil {
		t.Fatalf("expected cand2 run to exist: %v", err)
	}
	if checkCand2Run.DeletionTimestamp != nil {
		t.Errorf("cand2 run should not have DeletionTimestamp")
	}

	var checkCand2BatchRun v1alpha1.Run
	if err := f.client.Get(ctx, types.NamespacedName{Namespace: cand2BatchRun.Namespace, Name: cand2BatchRun.Name}, &checkCand2BatchRun); err != nil {
		t.Fatalf("expected cand2 batch run to exist: %v", err)
	}
	if checkCand2BatchRun.DeletionTimestamp != nil {
		t.Errorf("cand2 batch run should not have DeletionTimestamp")
	}

	// Verify stale cand1 batch run WAS deleted (has DeletionTimestamp)
	var checkStaleRun v1alpha1.Run
	if err := f.client.Get(ctx, types.NamespacedName{Namespace: staleCand1BatchRun.Namespace, Name: staleCand1BatchRun.Name}, &checkStaleRun); err != nil {
		t.Fatalf("expected stale run to still exist with finalizer: %v", err)
	}
	if checkStaleRun.DeletionTimestamp == nil {
		t.Errorf("stale cand1 batch run should have DeletionTimestamp")
	}
}

func TestComparisonReconciler_BaseMove_SharedBaselineBatchRunSurvives(t *testing.T) {
	ctx := t.Context()
	f := newHeadMoveFixture(t, false)

	// Complete initial reconcile for Comparison A (f.comp, "pr-test")
	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("first reconcile of comp A failed: %v", err)
	}

	// Comparison A has a baseline batch Run (pr-test-b2-base) with commit f.baseSHA
	compAB2Base := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       fmt.Sprintf("%s-b2-base", f.comp.Name),
			Namespace:  "eval-runner",
			Labels:     map[string]string{LabelComparison: f.comp.Name, LabelRole: RoleBaseline, LabelBatchKey: "shared-batch-key-1"},
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: f.baseSHA},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 2, End: 2},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
		},
	}
	if err := f.client.Create(ctx, compAB2Base); err != nil {
		t.Fatalf("create comp A b2 base run failed: %v", err)
	}

	// Comparison A also has an old candidate batch Run (pr-test-b2-cand) with old commit f.cand1SHA
	compAB2Cand := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:       fmt.Sprintf("%s-b2-cand", f.comp.Name),
			Namespace:  "eval-runner",
			Labels:     map[string]string{LabelComparison: f.comp.Name, LabelRole: RoleCandidate},
			Finalizers: []string{v1alpha1.FinalizerDiscoverySandbox},
		},
		Spec: v1alpha1.RunSpec{
			Repo:        v1alpha1.RepoSpec{URL: "git://repo", Commit: f.cand1SHA},
			Model:       "mockllm/model",
			SampleRange: &v1alpha1.SampleRange{Start: 2, End: 2},
		},
	}
	if err := f.client.Create(ctx, compAB2Cand); err != nil {
		t.Fatalf("create comp A b2 cand run failed: %v", err)
	}

	// Comparison B adopts Comparison A's baseline batch Run by name
	compB := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{Name: "pr-other", Namespace: "eval-runner"},
		Spec: v1alpha1.ComparisonSpec{
			Baseline:  v1alpha1.ComparisonRunRef{Run: f.baseRun.Name},
			Candidate: v1alpha1.ComparisonRunRef{Run: "other-cand-run"},
		},
		Status: v1alpha1.ComparisonStatus{
			Phase: v1alpha1.ComparisonPhaseRunning,
			Batches: []v1alpha1.ComparisonBatchStatus{
				{Index: 1, CandidateRun: "other-cand-run", BaselineRun: f.baseRun.Name, Phase: v1alpha1.ComparisonPhaseCompleted},
				{Index: 2, CandidateRun: "other-cand-b2", BaselineRun: compAB2Base.Name, Phase: v1alpha1.ComparisonPhaseCompleted},
			},
		},
	}
	if err := f.client.Create(ctx, compB); err != nil {
		t.Fatalf("create comp B failed: %v", err)
	}

	// Now Comparison A undergoes a base move (e.g. consumer rewrites spec.baseline.run)
	newBaseSHA := "3333333333333333333333333333333333333333"
	newBaseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "base-run-2", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: newBaseSHA},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: f.taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://eval-bucket/runs/" + newBaseSHA + "/" + f.taskID + "/inspect.json", Samples: 1},
			},
			Summary: &v1alpha1.RunSummary{TotalSamples: 1},
		},
	}
	if err := f.client.Create(ctx, newBaseRun); err != nil {
		t.Fatalf("create new baseline run failed: %v", err)
	}

	var curCompA v1alpha1.Comparison
	if err := f.client.Get(ctx, f.req.NamespacedName, &curCompA); err != nil {
		t.Fatalf("get comp A failed: %v", err)
	}
	curCompA.Spec.Baseline.Run = newBaseRun.Name
	// Candidate also moves to cand2
	curCompA.Spec.Candidate.Run = f.cand2Run.Name
	if err := f.client.Update(ctx, &curCompA); err != nil {
		t.Fatalf("update comp A failed: %v", err)
	}

	// Reconcile Comparison A on base/cand move
	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("reconcile comp A on base move failed: %v", err)
	}

	// Verify Comparison A's -b2-base adopted by Comparison B is NOT deleted (survives A's base move)
	var checkAdoptedBase v1alpha1.Run
	if err := f.client.Get(ctx, types.NamespacedName{Namespace: compAB2Base.Namespace, Name: compAB2Base.Name}, &checkAdoptedBase); err != nil {
		t.Fatalf("expected adopted baseline batch run to survive: %v", err)
	}
	if checkAdoptedBase.DeletionTimestamp != nil {
		t.Errorf("adopted baseline batch run %s should not have DeletionTimestamp", compAB2Base.Name)
	}

	// Verify Comparison A's stale candidate batch run WAS deleted
	var checkStaleCand v1alpha1.Run
	if err := f.client.Get(ctx, types.NamespacedName{Namespace: compAB2Cand.Namespace, Name: compAB2Cand.Name}, &checkStaleCand); err != nil {
		t.Fatalf("expected stale cand run to exist with finalizer: %v", err)
	}
	if checkStaleCand.DeletionTimestamp == nil {
		t.Errorf("stale cand run %s should have DeletionTimestamp", compAB2Cand.Name)
	}
}

func TestComparisonReconciler_HeadMove_CandidateRunNotFound(t *testing.T) {
	ctx := t.Context()
	f := newHeadMoveFixture(t, true)

	// 1. Initial reconcile completes
	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}

	// Delete cand2Run so it does not exist yet when candidate changes
	if err := f.client.Delete(ctx, f.cand2Run); err != nil {
		t.Fatalf("delete cand2 failed: %v", err)
	}

	// 2. Candidate changes to cand2-run, but cand2-run does NOT exist yet in the cluster
	var curComp v1alpha1.Comparison
	if err := f.client.Get(ctx, f.req.NamespacedName, &curComp); err != nil {
		t.Fatalf("get comp failed: %v", err)
	}
	curComp.Spec.Candidate.Run = "cand2-run"
	curComp.Spec.PullRequest.HeadSHA = f.cand2SHA
	if err := f.client.Update(ctx, &curComp); err != nil {
		t.Fatalf("update comp failed: %v", err)
	}

	// Reconcile: must reset status, keep Running with message "waiting for candidate run",
	// pass nil to renderer (no panic), and edit comment in place.
	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}

	if err := f.client.Get(ctx, f.req.NamespacedName, &curComp); err != nil {
		t.Fatalf("get comp failed: %v", err)
	}
	if curComp.Status.Phase != v1alpha1.ComparisonPhaseRunning {
		t.Errorf("expected phase Running, got %v", curComp.Status.Phase)
	}
	if curComp.Status.Verdict != v1alpha1.ComparisonVerdictRunning {
		t.Errorf("expected verdict RUNNING, got %v", curComp.Status.Verdict)
	}
	if curComp.Status.Message != "waiting for candidate run" {
		t.Errorf("expected message 'waiting for candidate run', got %q", curComp.Status.Message)
	}

	comments := f.server.Comments()
	if len(comments) != 1 {
		t.Fatalf("expected 1 comment updated in place, got %d", len(comments))
	}
	if !strings.Contains(comments[0].Body, "waiting for candidate run") {
		t.Errorf("expected comment body to contain 'waiting for candidate run', got:\n%s", comments[0].Body)
	}

	// 3. Now cand2-run is recreated and completed
	cand2Run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "cand2-run", Namespace: "eval-runner"},
		Spec: v1alpha1.RunSpec{
			Repo:  v1alpha1.RepoSpec{URL: "git://repo", Commit: f.cand2SHA},
			Model: "mockllm/model",
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: f.taskID, Phase: v1alpha1.TaskPhaseCompleted, LogPath: "gs://eval-bucket/runs/" + f.cand2SHA + "/" + f.taskID + "/inspect.json", Samples: 1},
			},
			Summary: &v1alpha1.RunSummary{TotalSamples: 1},
		},
	}
	if err := f.client.Create(ctx, cand2Run); err != nil {
		t.Fatalf("recreate cand2 failed: %v", err)
	}

	// Reconcile again: should score cand2 and complete
	if _, err := f.reconciler.Reconcile(ctx, f.req); err != nil {
		t.Fatalf("third reconcile failed: %v", err)
	}

	if err := f.client.Get(ctx, f.req.NamespacedName, &curComp); err != nil {
		t.Fatalf("get comp final failed: %v", err)
	}
	if curComp.Status.Phase != v1alpha1.ComparisonPhaseCompleted {
		t.Errorf("expected phase Completed, got %v", curComp.Status.Phase)
	}
}
