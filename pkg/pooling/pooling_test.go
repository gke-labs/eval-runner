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

package pooling

import (
	"math"
	"testing"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestExtractTaskCounts(t *testing.T) {
	rate := 0.75
	task1 := v1alpha1.TaskStatus{
		ID:          "task1",
		Phase:       v1alpha1.TaskPhaseCompleted,
		Samples:     8,
		Successes:   6,
		SuccessRate: &rate,
	}
	tc, ok := ExtractTaskCounts(task1)
	if !ok {
		t.Fatalf("expected ok=true for completed task")
	}
	if tc.Samples != 8 || tc.Successes != 6 {
		t.Errorf("got samples=%d, succ=%d; want 8, 6", tc.Samples, tc.Successes)
	}

	// Fallback when Successes is 0 but SuccessRate is set
	task2 := v1alpha1.TaskStatus{
		ID:          "task2",
		Phase:       v1alpha1.TaskPhaseCompleted,
		Samples:     8,
		Successes:   0,
		SuccessRate: &rate,
	}
	tc2, ok2 := ExtractTaskCounts(task2)
	if !ok2 {
		t.Fatalf("expected ok=true for completed task with rate fallback")
	}
	if tc2.Samples != 8 || tc2.Successes != 6 {
		t.Errorf("got samples=%d, succ=%d; want 8, 6 (from rate fallback)", tc2.Samples, tc2.Successes)
	}

	// Not completed or 0 samples
	taskRunning := v1alpha1.TaskStatus{
		ID:      "task3",
		Phase:   v1alpha1.TaskPhaseRunning,
		Samples: 4,
	}
	if _, ok := ExtractTaskCounts(taskRunning); ok {
		t.Errorf("expected ok=false for running task")
	}

	taskZeroSamples := v1alpha1.TaskStatus{
		ID:      "task4",
		Phase:   v1alpha1.TaskPhaseCompleted,
		Samples: 0,
	}
	if _, ok := ExtractTaskCounts(taskZeroSamples); ok {
		t.Errorf("expected ok=false for 0 samples")
	}

	// Failed task with scored samples and successes
	taskFailedScored := v1alpha1.TaskStatus{
		ID:        "task5",
		Phase:     v1alpha1.TaskPhaseFailed,
		Samples:   8,
		Successes: 8,
	}
	tcFailed, okFailed := ExtractTaskCounts(taskFailedScored)
	if !okFailed {
		t.Fatalf("expected ok=true for failed task with scored samples")
	}
	if tcFailed.Samples != 8 || tcFailed.Successes != 8 {
		t.Errorf("got samples=%d, succ=%d; want 8, 8", tcFailed.Samples, tcFailed.Successes)
	}

	// Failed task with 0 samples
	taskFailedZero := v1alpha1.TaskStatus{
		ID:      "task6",
		Phase:   v1alpha1.TaskPhaseFailed,
		Samples: 0,
	}
	if _, ok := ExtractTaskCounts(taskFailedZero); ok {
		t.Errorf("expected ok=false for failed task with 0 samples")
	}

	// Failed task with no derivable success count (Successes=0 and SuccessRate=nil)
	taskFailedNoSuccesses := v1alpha1.TaskStatus{
		ID:      "task7",
		Phase:   v1alpha1.TaskPhaseFailed,
		Samples: 8,
	}
	if _, ok := ExtractTaskCounts(taskFailedNoSuccesses); ok {
		t.Errorf("expected ok=false for failed task with no derivable success count")
	}

	// Completed task with 0/N successes (Successes=0 and SuccessRate=nil)
	taskCompletedZeroSuccesses := v1alpha1.TaskStatus{
		ID:        "task8",
		Phase:     v1alpha1.TaskPhaseCompleted,
		Samples:   8,
		Successes: 0,
	}
	tcZero, okZero := ExtractTaskCounts(taskCompletedZeroSuccesses)
	if !okZero {
		t.Fatalf("expected ok=true for completed 0/N task")
	}
	if tcZero.Samples != 8 || tcZero.Successes != 0 {
		t.Errorf("got samples=%d, succ=%d; want 8, 0", tcZero.Samples, tcZero.Successes)
	}
}

func TestPoolCommitRuns(t *testing.T) {
	cost1 := 0.12
	cost2 := 0.15

	run1 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name: "base-aaaaaaa",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/head-sha": "aaaaaaa",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{Commit: "aaaaaaa"},
		},
		Status: v1alpha1.RunStatus{
			Phase:   v1alpha1.RunPhaseCompleted,
			CostUSD: &cost1,
			Tasks: []v1alpha1.TaskStatus{
				{ID: "taskA", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 7},
				{ID: "taskB", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 4},
			},
		},
	}

	run2 := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name: "base-aaaaaaa-2",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/head-sha": "aaaaaaa",
				"evalrunner.gke-labs.dev/topup":    "2",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{Commit: "aaaaaaa"},
		},
		Status: v1alpha1.RunStatus{
			Phase:   v1alpha1.RunPhaseCompleted,
			CostUSD: &cost2,
			Tasks: []v1alpha1.TaskStatus{
				{ID: "taskA", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 8},
				{ID: "taskB", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 5},
			},
		},
	}

	runOtherCommit := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name: "base-bbbbbbb",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/head-sha": "bbbbbbb",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{Commit: "bbbbbbb"},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{ID: "taskA", Phase: v1alpha1.TaskPhaseCompleted, Samples: 8, Successes: 1},
			},
		},
	}

	pool := PoolCommitRuns([]*v1alpha1.Run{run1, run2, runOtherCommit}, "aaaaaaa")

	if len(pool.Runs) != 2 {
		t.Fatalf("expected 2 runs in pool for aaaaaaa, got %d", len(pool.Runs))
	}
	if pool.TotalSamples != 32 {
		t.Errorf("expected 32 total samples, got %d", pool.TotalSamples)
	}
	if pool.TotalSuccesses != 24 { // 7 + 4 + 8 + 5 = 24
		t.Errorf("expected 24 total successes, got %d", pool.TotalSuccesses)
	}
	if pool.SamplesPerTask["taskA"] != 16 {
		t.Errorf("expected 16 samples for taskA, got %d", pool.SamplesPerTask["taskA"])
	}
	if pool.SamplesPerTask["taskB"] != 16 {
		t.Errorf("expected 16 samples for taskB, got %d", pool.SamplesPerTask["taskB"])
	}
	if !pool.HasCost || math.Abs(pool.TotalCostUSD-0.27) > 1e-6 {
		t.Errorf("expected cost $0.27, got %v (hasCost=%v)", pool.TotalCostUSD, pool.HasCost)
	}

	est := pool.OverallEstimate()
	if est.N != 32 {
		t.Errorf("expected pooled estimate N=32, got %d", est.N)
	}
	expectedPoint := 24.0 / 32.0 // 0.75
	if math.Abs(est.Point-expectedPoint) > 1e-6 {
		t.Errorf("expected point %.4f, got %.4f", expectedPoint, est.Point)
	}

	hw := pool.OverallCIHalfWidth()
	expectedHW := (est.CI.Upper - est.CI.Lower) / 2.0
	if math.Abs(hw-expectedHW) > 1e-6 {
		t.Errorf("expected half-width %.4f, got %.4f", expectedHW, hw)
	}
}

func TestPoolCommitRuns_FailedTaskWithScoredSamples(t *testing.T) {
	rateFailed := 1.0
	rateRunning := 1.0
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name: "base-failed-run",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/head-sha": "c0ffee1",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    "https://github.com/gke-labs/eval-runner.git",
				Commit: "c0ffee1",
			},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:          "taskFailed",
					Phase:       v1alpha1.TaskPhaseFailed,
					Samples:     8,
					Successes:   8,
					SuccessRate: &rateFailed,
				},
				{
					ID:          "taskRunning",
					Phase:       v1alpha1.TaskPhaseRunning,
					Samples:     4,
					Successes:   4,
					SuccessRate: &rateRunning,
				},
			},
		},
	}

	// 1. CommitObservationPool via PoolCommitRuns
	obsPool := PoolCommitRuns([]*v1alpha1.Run{run}, "c0ffee1")
	if obsPool.ObsCount != 1 {
		t.Errorf("expected ObsCount=1 in CommitObservationPool, got %d", obsPool.ObsCount)
	}
	if obsPool.TotalSamples != 8 {
		t.Errorf("expected TotalSamples=8, got %d", obsPool.TotalSamples)
	}
	if obsPool.TotalSuccesses != 8 {
		t.Errorf("expected TotalSuccesses=8, got %d", obsPool.TotalSuccesses)
	}
	if len(obsPool.TaskCounts["taskFailed"]) != 1 {
		t.Fatalf("expected 1 task count for taskFailed, got %d", len(obsPool.TaskCounts["taskFailed"]))
	}
	if obsPool.TaskCounts["taskFailed"][0].Samples != 8 || obsPool.TaskCounts["taskFailed"][0].Successes != 8 {
		t.Errorf("expected taskFailed to contribute 8/8, got %d/%d",
			obsPool.TaskCounts["taskFailed"][0].Successes, obsPool.TaskCounts["taskFailed"][0].Samples)
	}
	if _, exists := obsPool.TaskCounts["taskRunning"]; exists {
		t.Errorf("expected taskRunning not to contribute to TaskCounts")
	}

	est := obsPool.OverallEstimate()
	if est.N != 8 {
		t.Errorf("expected overall estimate N=8, got %d", est.N)
	}
	if math.Abs(est.Point-1.0) > 1e-6 {
		t.Errorf("expected overall estimate point 1.0, got %.4f", est.Point)
	}

	// 2. CommitPool via PoolRunsForCommit
	cPool := PoolRunsForCommit([]*v1alpha1.Run{run})
	if cPool.ObsCount != 1 {
		t.Errorf("expected ObsCount=1 in CommitPool, got %d", cPool.ObsCount)
	}
	if cPool.OverallEstimate == nil {
		t.Fatalf("expected OverallEstimate to be populated")
	}
	if cPool.OverallEstimate.N != 8 || math.Abs(cPool.OverallEstimate.Point-1.0) > 1e-6 {
		t.Errorf("expected overall estimate 8/8 (1.0), got %d samples, point %.4f",
			cPool.OverallEstimate.N, cPool.OverallEstimate.Point)
	}
	if cPool.FailedTasks != 1 {
		t.Errorf("expected FailedTasks=1, got %d", cPool.FailedTasks)
	}

	var ptFailed *PooledTask
	for i := range cPool.Tasks {
		if cPool.Tasks[i].ID == "taskFailed" {
			ptFailed = &cPool.Tasks[i]
		}
	}
	if ptFailed == nil {
		t.Fatalf("taskFailed not found in cPool.Tasks")
	}
	if ptFailed.Phase != v1alpha1.TaskPhaseFailed {
		t.Errorf("expected ptFailed.Phase=Failed, got %s", ptFailed.Phase)
	}
	if ptFailed.Samples != 8 || ptFailed.Successes != 8 {
		t.Errorf("expected ptFailed samples=8, succ=8, got %d, %d", ptFailed.Samples, ptFailed.Successes)
	}
	if ptFailed.ObsCount != 1 {
		t.Errorf("expected ptFailed.ObsCount=1, got %d", ptFailed.ObsCount)
	}
	if math.Abs(ptFailed.Estimate.Point-1.0) > 1e-6 {
		t.Errorf("expected ptFailed.Estimate point 1.0, got %.4f", ptFailed.Estimate.Point)
	}
}

func TestPoolCommitRuns_CompletedZeroSuccesses(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name: "base-zero-run",
			Labels: map[string]string{
				"evalrunner.gke-labs.dev/head-sha": "d00d1e",
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{Commit: "d00d1e"},
		},
		Status: v1alpha1.RunStatus{
			Phase: v1alpha1.RunPhaseCompleted,
			Tasks: []v1alpha1.TaskStatus{
				{
					ID:        "taskZero",
					Phase:     v1alpha1.TaskPhaseCompleted,
					Samples:   10,
					Successes: 0,
				},
			},
		},
	}

	obsPool := PoolCommitRuns([]*v1alpha1.Run{run}, "d00d1e")
	if obsPool.TotalSamples != 10 {
		t.Errorf("expected TotalSamples=10, got %d", obsPool.TotalSamples)
	}
	if obsPool.TotalSuccesses != 0 {
		t.Errorf("expected TotalSuccesses=0, got %d", obsPool.TotalSuccesses)
	}
	est := obsPool.OverallEstimate()
	if est.N != 10 {
		t.Errorf("expected N=10, got %d", est.N)
	}
	if est.Point != 0.0 {
		t.Errorf("expected Point=0.0, got %.4f", est.Point)
	}

	cPool := PoolRunsForCommit([]*v1alpha1.Run{run})
	if cPool.CompletedTasks != 1 {
		t.Errorf("expected CompletedTasks=1, got %d", cPool.CompletedTasks)
	}
	if cPool.OverallEstimate == nil || cPool.OverallEstimate.Point != 0.0 {
		t.Errorf("expected overall estimate point 0.0, got %+v", cPool.OverallEstimate)
	}
}
