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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"strings"
	"testing"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/internal/fakes"
	"github.com/gke-labs/eval-runner/pkg/artifacts/gcs"
	"github.com/gke-labs/eval-runner/pkg/github"
	"github.com/gke-labs/eval-runner/pkg/stats"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
				if tt.expectedPairs > 0 && len(artifact.Tasks[0].PairsTable) != tt.expectedPairs {
					t.Errorf("Artifact pairsTable length: got %d, want %d", len(artifact.Tasks[0].PairsTable), tt.expectedPairs)
				}
			}
		})
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
	_, err = reconciler.Reconcile(t.Context(), req)
	if err == nil || !strings.Contains(err.Error(), "does not match commit prefix") {
		t.Errorf("Expected error containing 'does not match commit prefix', got %v", err)
	}

	var updated v1alpha1.Comparison
	if err := cl.Get(t.Context(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if !strings.Contains(updated.Status.Message, "does not match commit prefix") {
		t.Errorf("Expected status message to record prefix mismatch, got %q", updated.Status.Message)
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

		comp := makeComp("comp-post")
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

		// 2. Idempotency: second reconcile with same status makes no new HTTP requests
		reqCountBefore := len(server.RequestLog())
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("idempotent Reconcile error: %v", err)
		}
		if len(server.RequestLog()) != reqCountBefore {
			t.Errorf("expected 0 new requests on idempotent reconcile, got %d", len(server.RequestLog())-reqCountBefore)
		}

		// 3. Second reconcile with changed status updates comment and check run in place
		candInspectUpdated := makeSyntheticInspectLog(taskID, []stats.SampleScore{
			{ID: "1", Score: 0.0},
			{ID: "2", Score: 0.0},
		})
		fakeGCS.Objects["eval-bucket/runs/cand-commit/"+taskID+"/inspect.json"] = fakes.FakeObject{Data: candInspectUpdated}

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
		expectedURL := "[web view](https://eval-ui.example.com/comparisons/eval-runner/comp-ui-url)"
		if !strings.Contains(comments[0].Body, expectedURL) {
			t.Errorf("expected comment body to contain absolute link %q, got:\n%s", expectedURL, comments[0].Body)
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

		// 2. Second reconcile (check run error is cleared)
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

		// Update candidate log again
		candInspectUpdated3 := makeSyntheticInspectLog(taskID, []stats.SampleScore{
			{ID: "1", Score: 1.0},
			{ID: "2", Score: 1.0},
		})
		fakeGCS.Objects["eval-bucket/runs/cand-commit/"+taskID+"/inspect.json"] = fakes.FakeObject{Data: candInspectUpdated3}

		// Third reconcile: secret changed, cache invalidated -> exactly 2 access_tokens requests
		if _, err := reconciler.Reconcile(ctx, req); err != nil {
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
