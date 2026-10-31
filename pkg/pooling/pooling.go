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

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/forge/consumer"
	"github.com/gke-labs/eval-runner/pkg/stats"
)

// RunCommitSHA returns the git commit SHA for a Run, checking Spec.Repo.Commit first
// and falling back to the LabelHeadSHA label.
func RunCommitSHA(run *v1alpha1.Run) string {
	if run == nil {
		return ""
	}
	if run.Spec.Repo.Commit != "" {
		return run.Spec.Repo.Commit
	}
	if run.Labels != nil {
		return run.Labels[consumer.LabelHeadSHA]
	}
	return ""
}

// TaskSuccesses extracts the count of successful samples from a TaskStatus,
// deriving from SuccessRate when Successes is 0 but SuccessRate is positive.
func TaskSuccesses(t v1alpha1.TaskStatus) int {
	succ := t.Successes
	if succ == 0 && t.SuccessRate != nil && *t.SuccessRate > 0 {
		succ = int(math.Round(*t.SuccessRate * float64(t.Samples)))
	}
	return succ
}

// ExtractTaskCounts extracts sample and success counts from a completed or failed TaskStatus.
// Returns false if the task is not in a terminal phase (Completed or Failed), has no samples,
// or a success count is not derivable (for Failed tasks, neither Successes > 0 nor SuccessRate is set).
func ExtractTaskCounts(t v1alpha1.TaskStatus) (stats.TaskCounts, bool) {
	if (t.Phase != v1alpha1.TaskPhaseCompleted && t.Phase != v1alpha1.TaskPhaseFailed) || t.Samples <= 0 {
		return stats.TaskCounts{}, false
	}
	if t.Phase == v1alpha1.TaskPhaseFailed && t.Successes == 0 && t.SuccessRate == nil {
		return stats.TaskCounts{}, false
	}
	return stats.TaskCounts{
		Successes: TaskSuccesses(t),
		Samples:   t.Samples,
	}, true
}

// CommitObservationPool aggregates completed task counts and spend across Runs for a specific commit.
type CommitObservationPool struct {
	CommitSHA      string
	Runs           []*v1alpha1.Run
	TaskCounts     map[string][]stats.TaskCounts // taskID -> list of task counts
	SamplesPerTask map[string]int                // taskID -> total samples
	TotalSamples   int
	TotalSuccesses int
	AllTasks       []stats.TaskCounts // flattened task counts for overall pooling
	TotalCostUSD   float64
	HasCost        bool
	ObsCount       int
}

// PoolCommitRuns pools completed task observations from runs matching the given commit.
// If commitSHA is non-empty, only runs targeting commitSHA are included.
func PoolCommitRuns(runs []*v1alpha1.Run, commitSHA string) *CommitObservationPool {
	pool := &CommitObservationPool{
		CommitSHA:      commitSHA,
		TaskCounts:     make(map[string][]stats.TaskCounts),
		SamplesPerTask: make(map[string]int),
	}

	for _, run := range runs {
		if run == nil {
			continue
		}
		if commitSHA != "" {
			runSHA := RunCommitSHA(run)
			if runSHA != commitSHA {
				continue
			}
		}

		pool.Runs = append(pool.Runs, run)

		if run.Status.CostUSD != nil {
			pool.TotalCostUSD += *run.Status.CostUSD
			pool.HasCost = true
		}

		for idx := range run.Status.Tasks {
			t := run.Status.Tasks[idx]
			if t.ID == "" {
				continue
			}

			tc, ok := ExtractTaskCounts(t)
			if !ok {
				continue
			}

			pool.TaskCounts[t.ID] = append(pool.TaskCounts[t.ID], tc)
			pool.SamplesPerTask[t.ID] += tc.Samples
			pool.TotalSamples += tc.Samples
			pool.TotalSuccesses += tc.Successes
			pool.AllTasks = append(pool.AllTasks, tc)
			pool.ObsCount++
		}
	}

	return pool
}

// OverallEstimate returns the pooled Wilson score estimate across all completed tasks at 95% confidence.
func (p *CommitObservationPool) OverallEstimate() stats.Estimate {
	return stats.PooledWilson(p.AllTasks, 0.95)
}

// OverallCIHalfWidth returns the half-width of the pooled 95% Wilson confidence interval ((upper - lower) / 2.0).
func (p *CommitObservationPool) OverallCIHalfWidth() float64 {
	est := p.OverallEstimate()
	return (est.CI.Upper - est.CI.Lower) / 2.0
}
