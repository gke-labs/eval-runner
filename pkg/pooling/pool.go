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
	"sort"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/stats"
)

// PooledTask contains pooled results for a single task at a commit.
type PooledTask struct {
	ID          string
	Samples     int
	Successes   int
	SuccessRate *float64
	Estimate    stats.Estimate
	Tokens      *v1alpha1.TaskTokens
	CostUSD     *float64
	Phase       v1alpha1.TaskPhase
	Message     string
	RunName     string
	ObsCount    int
}

// CommitPool contains pooled evaluation observations across all Runs for a single commit,
// layered on top of CommitObservationPool.
type CommitPool struct {
	CommitSHA       string
	CommitTime      time.Time
	Runs            []*v1alpha1.Run
	Tasks           []PooledTask
	OverallEstimate *stats.Estimate
	CompletedTasks  int
	TotalTasks      int
	FailedTasks     int
	ObsPool         *CommitObservationPool
	ObsCount        int
}

// PoolRunsForCommit combines task results across all Runs (seed and top-up) for a single commit,
// building on top of PoolCommitRuns and ExtractTaskCounts.
func PoolRunsForCommit(runs []*v1alpha1.Run) *CommitPool {
	pool := &CommitPool{
		Runs: runs,
	}
	if len(runs) == 0 {
		return pool
	}

	commitSHA := RunCommitSHA(runs[0])
	pool.CommitSHA = commitSHA
	pool.CommitTime, _ = RunCommitTime(runs[0])

	// Delegate statistical pooling of completed tasks to PoolCommitRuns
	obsPool := PoolCommitRuns(runs, commitSHA)
	pool.ObsPool = obsPool

	// Gather all unique task IDs declared or present in status
	taskSet := make(map[string]bool)
	for _, r := range runs {
		if r == nil {
			continue
		}
		for _, t := range r.Status.Tasks {
			if t.ID != "" {
				taskSet[t.ID] = true
			}
		}
		for _, tid := range r.Spec.Tasks {
			if tid != "" {
				taskSet[tid] = true
			}
		}
	}
	var sortedTaskIDs []string
	for tid := range taskSet {
		sortedTaskIDs = append(sortedTaskIDs, tid)
	}
	sort.Strings(sortedTaskIDs)
	pool.TotalTasks = len(sortedTaskIDs)

	for _, tid := range sortedTaskIDs {
		counts := obsPool.TaskCounts[tid]
		totalSamples := obsPool.SamplesPerTask[tid]

		var totalTokens *v1alpha1.TaskTokens
		var totalCostUSD *float64
		var lastPhase v1alpha1.TaskPhase
		var lastMsg string
		var lastRunName string
		allFailed := true
		var singleRunCI *v1alpha1.ConfidenceInterval
		var singleRunRate *float64
		var singleRunSD *float64

		for _, r := range runs {
			if r == nil {
				continue
			}
			for idx := range r.Status.Tasks {
				t := &r.Status.Tasks[idx]
				if t.ID != tid {
					continue
				}
				lastRunName = r.Name
				if t.Message != "" {
					lastMsg = t.Message
				}

				if t.Phase == v1alpha1.TaskPhaseCompleted || t.Phase == v1alpha1.TaskPhaseFailed {
					if t.Tokens != nil {
						if totalTokens == nil {
							totalTokens = &v1alpha1.TaskTokens{}
						}
						totalTokens.Input += t.Tokens.Input
						totalTokens.Output += t.Tokens.Output
						totalTokens.Total += t.Tokens.Total
						totalTokens.Reasoning += t.Tokens.Reasoning
					}
					if t.CostUSD != nil {
						if totalCostUSD == nil {
							totalCostUSD = new(float64)
						}
						*totalCostUSD += *t.CostUSD
					}
					if len(counts) == 1 && t.SuccessRateCI != nil {
						singleRunCI = t.SuccessRateCI
						singleRunRate = t.SuccessRate
						singleRunSD = t.SuccessRateSD
					}
				}
				if t.Phase == v1alpha1.TaskPhaseCompleted {
					allFailed = false
				} else if t.Phase != v1alpha1.TaskPhaseFailed {
					allFailed = false
					lastPhase = t.Phase
				}
			}
		}

		pt := PooledTask{
			ID:      tid,
			RunName: lastRunName,
			Message: lastMsg,
		}

		if len(counts) > 0 && totalSamples > 0 {
			totalSuccesses := 0
			for _, tc := range counts {
				totalSuccesses += tc.Successes
			}
			rate := float64(totalSuccesses) / float64(totalSamples)
			if allFailed {
				pt.Phase = v1alpha1.TaskPhaseFailed
				pool.FailedTasks++
			} else {
				pt.Phase = v1alpha1.TaskPhaseCompleted
				pool.CompletedTasks++
			}
			pt.Samples = totalSamples
			pt.Successes = totalSuccesses
			pt.SuccessRate = &rate
			pt.ObsCount = len(counts)

			if singleRunCI != nil && singleRunRate != nil {
				pt.Estimate = stats.Estimate{
					Point:      *singleRunRate,
					N:          totalSamples,
					SD:         singleRunSD,
					CI:         stats.Interval{Lower: singleRunCI.Lower, Upper: singleRunCI.Upper},
					Confidence: singleRunCI.Confidence,
					Method:     singleRunCI.Method,
				}
			} else {
				pt.Estimate = stats.PooledWilson(counts, 0.95)
			}
			pt.Tokens = totalTokens
			pt.CostUSD = totalCostUSD
		} else if allFailed {
			pt.Phase = v1alpha1.TaskPhaseFailed
			pool.FailedTasks++
		} else {
			pt.Phase = lastPhase
		}

		pool.Tasks = append(pool.Tasks, pt)
	}

	if len(obsPool.AllTasks) > 0 {
		overall := obsPool.OverallEstimate()
		pool.OverallEstimate = &overall
	}
	pool.ObsCount = obsPool.ObsCount

	return pool
}

// ComputeCommitDelta computes the difference between a head commit and parent commit.
func ComputeCommitDelta(parentPool *CommitPool, headPool *CommitPool) *stats.DeltaEstimate {
	if parentPool == nil || headPool == nil {
		return nil
	}
	if parentPool.OverallEstimate == nil || headPool.OverallEstimate == nil {
		return nil
	}
	delta := stats.DifferenceUnpaired(*headPool.OverallEstimate, *parentPool.OverallEstimate, 0.95)
	return &delta
}
