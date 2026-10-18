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

	t.Run("task failed in both runs does not cap UNCHANGED verdict and notes task not executed", func(t *testing.T) {
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
		if !strings.Contains(updated.Status.Message, "tasks not executed: docs/task_two") {
			t.Errorf("Expected status.message to mention 'tasks not executed: docs/task_two', got %q", updated.Status.Message)
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

func findComparisonTaskStatus(tasks []v1alpha1.ComparisonTaskStatus, id string) *v1alpha1.ComparisonTaskStatus {
	for i := range tasks {
		if tasks[i].ID == id {
			return &tasks[i]
		}
	}
	return nil
}
