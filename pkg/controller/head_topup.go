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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/forge/consumer"
	"github.com/gke-labs/eval-runner/pkg/forge/syncer"
	"github.com/gke-labs/eval-runner/pkg/pooling"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// HeadTopUpRunnerOptions configures a HeadTopUpRunner.
type HeadTopUpRunnerOptions struct {
	Client       client.Client
	Namespace    string
	Logger       logr.Logger
	Config       ClusterConfig
	InitialDelay time.Duration
}

// HeadTopUpRunner periodically creates bounded top-up runs at the current head
// of each tracked branch so head scores converge toward the true score.
type HeadTopUpRunner struct {
	client       client.Client
	namespace    string
	logger       logr.Logger
	mu           sync.RWMutex
	cfg          ClusterConfig
	rearmCh      chan struct{}
	initialDelay time.Duration
}

// NewHeadTopUpRunner constructs a HeadTopUpRunner.
func NewHeadTopUpRunner(opts HeadTopUpRunnerOptions) *HeadTopUpRunner {
	return &HeadTopUpRunner{
		client:       opts.Client,
		namespace:    opts.Namespace,
		logger:       opts.Logger,
		cfg:          opts.Config,
		rearmCh:      make(chan struct{}, 1),
		initialDelay: opts.InitialDelay,
	}
}

// UpdateConfig updates the active configuration for the runner.
// Rearms the ticker only if HeadTopUpInterval actually changed.
func (r *HeadTopUpRunner) UpdateConfig(cfg ClusterConfig) {
	r.mu.Lock()
	intervalChanged := r.cfg.HeadTopUpInterval != cfg.HeadTopUpInterval
	r.cfg = cfg
	r.mu.Unlock()

	if intervalChanged {
		select {
		case r.rearmCh <- struct{}{}:
		default:
		}
	}
}

// Start implements manager.Runnable.
func (r *HeadTopUpRunner) Start(ctx context.Context) error {
	var ticker *time.Ticker
	var tickerCh <-chan time.Time

	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()

	r.mu.RLock()
	interval := r.cfg.HeadTopUpInterval
	r.mu.RUnlock()

	var initialTimer *time.Timer
	var initialCh <-chan time.Time
	if interval > 0 {
		delay := r.initialDelay
		if delay <= 0 {
			delay = 1 * time.Minute
		}
		initialTimer = time.NewTimer(delay)
		initialCh = initialTimer.C
		defer func() {
			if initialTimer != nil {
				initialTimer.Stop()
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.rearmCh:
			r.mu.RLock()
			newInterval := r.cfg.HeadTopUpInterval
			r.mu.RUnlock()

			if ticker != nil {
				ticker.Stop()
				ticker = nil
				tickerCh = nil
			}
			if initialTimer != nil {
				initialTimer.Stop()
				initialTimer = nil
				initialCh = nil
			}
			if newInterval > 0 {
				ticker = time.NewTicker(newInterval)
				tickerCh = ticker.C
			}
		case <-initialCh:
			initialCh = nil
			if _, err := r.RunTopUpPass(ctx); err != nil {
				r.logger.V(1).Info("head top-up check encountered error", "error", err)
			}
			r.mu.RLock()
			currInterval := r.cfg.HeadTopUpInterval
			r.mu.RUnlock()
			if currInterval > 0 && ticker == nil {
				ticker = time.NewTicker(currInterval)
				tickerCh = ticker.C
			}
		case <-tickerCh:
			if _, err := r.RunTopUpPass(ctx); err != nil {
				r.logger.V(1).Info("head top-up check encountered error", "error", err)
			}
		}
	}
}

type repoBranchTarget struct {
	repoConfig *syncer.RepoConfig
	targetRepo string
	branch     string
}

// RunTopUpPass evaluates all tracked branch heads and spawns top-up runs where needed.
// Returns the list of created top-up Runs.
func (r *HeadTopUpRunner) RunTopUpPass(ctx context.Context) ([]*v1alpha1.Run, error) {
	r.mu.RLock()
	cfg := r.cfg
	r.mu.RUnlock()

	if cfg.HeadTopUpInterval <= 0 {
		return nil, nil
	}

	var runList v1alpha1.RunList
	listOpts := []client.ListOption{client.InNamespace(r.namespace)}
	if err := r.client.List(ctx, &runList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list runs: %w", err)
	}

	allRuns := make([]*v1alpha1.Run, len(runList.Items))
	existingRunNames := make(map[string]bool)
	for i := range runList.Items {
		allRuns[i] = &runList.Items[i]
		existingRunNames[runList.Items[i].Name] = true
	}

	targets := r.determineTargets(cfg, allRuns)

	var created []*v1alpha1.Run

	// Note: A race with the forge consumer (e.g. a push advancing branch head between List and Create)
	// may top up a just-superseded head once; this is benign and self-correcting on the next pass.
	for _, target := range targets {
		matchedRuns := r.filterRunsForTarget(allRuns, target)
		if len(matchedRuns) == 0 {
			continue
		}

		runsBySHA := make(map[string][]*v1alpha1.Run)
		for _, run := range matchedRuns {
			sha := pooling.RunCommitSHA(run)
			if sha == "" {
				continue
			}
			runsBySHA[sha] = append(runsBySHA[sha], run)
		}
		if len(runsBySHA) == 0 {
			continue
		}

		headCommit := r.findNewestHeadCommit(runsBySHA)
		if headCommit == "" {
			continue
		}

		commitRuns := runsBySHA[headCommit]

		// Condition 5: Do not run while a Run at that commit is still Pending/Discovering/Running.
		if hasInFlightRun(commitRuns) {
			r.logger.V(1).Info("skipping head top-up: run in-flight at commit", "commit", headCommit, "branch", target.branch)
			continue
		}

		// Stop only if the last two runs at this commit failed and contributed zero scored samples.
		sortedCommitRuns := sortRunsDescending(commitRuns)
		if len(sortedCommitRuns) >= 2 {
			r0, r1 := sortedCommitRuns[0], sortedCommitRuns[1]
			if r0.Status.Phase == v1alpha1.RunPhaseFailed && r1.Status.Phase == v1alpha1.RunPhaseFailed &&
				runScoredSamples(r0) == 0 && runScoredSamples(r1) == 0 {
				r.logger.Info("skipping head top-up: latest run failed at commit", "commit", headCommit, "branch", target.branch, "run", r0.Name)
				continue
			}
		}

		var candidateRuns []*v1alpha1.Run
		for _, run := range commitRuns {
			if run.Status.Phase == v1alpha1.RunPhaseCompleted || run.Status.Phase == v1alpha1.RunPhaseFailed {
				candidateRuns = append(candidateRuns, run)
			}
		}
		if len(candidateRuns) == 0 {
			// No completed or failed runs at head yet
			continue
		}

		pool := pooling.PoolCommitRuns(candidateRuns, headCommit)
		if pool.TotalSamples == 0 {
			r.logger.Info("skipping head top-up: zero completed samples across completed runs", "commit", headCommit, "branch", target.branch)
			continue
		}

		// Stop Condition A: pooled overall CI half-width at 95% is already <= headTargetCIHalfWidth.
		targetCIHalfWidth := cfg.HeadTargetCIHalfWidth
		if targetCIHalfWidth <= 0 {
			targetCIHalfWidth = DefaultHeadTargetCIHalfWidth
		}
		hw := pool.OverallCIHalfWidth()
		if hw <= targetCIHalfWidth {
			r.logger.V(1).Info("skipping head top-up: target CI half-width reached",
				"commit", headCommit, "branch", target.branch, "ciHalfWidth", hw, "target", targetCIHalfWidth)
			continue
		}

		// Stop Condition B: samples per task at that commit >= headMaxSamplesPerTask.
		maxSamplesPerTask := cfg.HeadMaxSamplesPerTask
		maxReached := false
		for taskID, count := range pool.SamplesPerTask {
			if count >= maxSamplesPerTask {
				maxReached = true
				r.logger.V(1).Info("skipping head top-up: task sample cap reached",
					"commit", headCommit, "branch", target.branch, "task", taskID, "samples", count, "cap", maxSamplesPerTask)
				break
			}
		}
		if maxReached {
			continue
		}

		// Stop Condition C: USD budget would be exceeded.
		if cfg.DefaultBudgetUSD != nil && *cfg.DefaultBudgetUSD > 0 {
			budgetLimit := *cfg.DefaultBudgetUSD
			if pool.TotalCostUSD >= budgetLimit {
				r.logger.V(1).Info("skipping head top-up: budget reached",
					"commit", headCommit, "branch", target.branch, "spent", pool.TotalCostUSD, "budget", budgetLimit)
				continue
			}
			if pool.TotalCostUSD > 0 {
				avgCostPerSample := pool.TotalCostUSD / float64(pool.TotalSamples)
				batchSize := cfg.DefaultBatchSize
				if batchSize <= 0 {
					batchSize = 8
				}
				numTasks := len(pool.SamplesPerTask)
				if numTasks == 0 {
					numTasks = 1
				}
				projected := pool.TotalCostUSD + float64(batchSize*numTasks)*avgCostPerSample
				if projected > budgetLimit {
					r.logger.V(1).Info("skipping head top-up: projected cost exceeds budget",
						"commit", headCommit, "branch", target.branch, "projected", projected, "budget", budgetLimit)
					continue
				}
			}
		}

		// Create one more Run at headCommit: name base-<sha7>-<n>.
		seedRun := findSeedRun(commitRuns)
		if seedRun == nil {
			continue
		}

		nextIndex, nextRunName := findNextRunName(seedRun, existingRunNames)

		batchSize := cfg.DefaultBatchSize
		if batchSize <= 0 {
			batchSize = 8
		}

		var progressRuns []*v1alpha1.Run
		for _, run := range candidateRuns {
			if run.Status.Phase == v1alpha1.RunPhaseCompleted || runScoredSamples(run) > 0 {
				progressRuns = append(progressRuns, run)
			}
		}

		prevRun := findLatestCompletedRun(progressRuns)
		nextSampleRange := determineNextSampleRange(prevRun, batchSize)

		labels := make(map[string]string)
		for k, v := range seedRun.Labels {
			labels[k] = v
		}
		labels[consumer.LabelTopUp] = strconv.Itoa(nextIndex)

		annotations := make(map[string]string)
		for k, v := range seedRun.Annotations {
			annotations[k] = v
		}

		model := seedRun.Spec.Model
		if model == "" {
			model = cfg.DefaultModel
		}
		cohort := seedRun.Spec.Cohort
		if cohort == "" {
			cohort = cfg.DefaultCohort
		}
		if cohort == "" {
			cohort = "default"
		}

		newRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:        nextRunName,
				Namespace:   r.namespace,
				Labels:      labels,
				Annotations: annotations,
			},
			Spec: v1alpha1.RunSpec{
				Repo:        seedRun.Spec.Repo,
				Model:       model,
				Cohort:      cohort,
				Tasks:       seedRun.Spec.Tasks,
				SampleRange: nextSampleRange,
			},
		}

		if err := r.client.Create(ctx, newRun); err != nil {
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			return created, fmt.Errorf("failed to create top-up run %s: %w", nextRunName, err)
		}

		existingRunNames[nextRunName] = true
		created = append(created, newRun)
		r.logger.Info("created head top-up run",
			"name", nextRunName,
			"commit", headCommit,
			"branch", target.branch,
			"topUp", nextIndex,
			"sampleRange", fmt.Sprintf("%d..%d", nextSampleRange.Start, nextSampleRange.End),
		)
	}

	return created, nil
}

func (r *HeadTopUpRunner) determineTargets(cfg ClusterConfig, runs []*v1alpha1.Run) []repoBranchTarget {
	var targets []repoBranchTarget

	if len(cfg.Repositories) > 0 {
		for i := range cfg.Repositories {
			repo := &cfg.Repositories[i]
			branches := repo.Branches
			if len(branches) == 0 {
				branches = []string{"main"}
			}
			for _, b := range branches {
				targets = append(targets, repoBranchTarget{
					repoConfig: repo,
					branch:     b,
				})
			}
		}
		return targets
	}

	// If no repositories are explicitly configured, discover targets from cluster base-* runs
	seen := make(map[string]bool)
	for _, run := range runs {
		if !strings.HasPrefix(run.Name, "base-") || run.Labels == nil {
			continue
		}
		tRepo := run.Labels[consumer.LabelTargetRepo]
		if tRepo == "" {
			tRepo = run.Labels[consumer.LabelRepo]
		}
		branch := run.Labels[consumer.LabelBranch]
		if tRepo == "" || branch == "" {
			continue
		}
		key := tRepo + "/" + branch
		if !seen[key] {
			seen[key] = true
			targets = append(targets, repoBranchTarget{
				targetRepo: tRepo,
				branch:     branch,
			})
		}
	}

	return targets
}

func (r *HeadTopUpRunner) filterRunsForTarget(runs []*v1alpha1.Run, target repoBranchTarget) []*v1alpha1.Run {
	var matched []*v1alpha1.Run
	for _, run := range runs {
		if !strings.HasPrefix(run.Name, "base-") || run.Labels == nil {
			continue
		}

		if !runMatchesBranch(run, target.branch) {
			continue
		}

		if target.repoConfig != nil {
			if !runMatchesRepoConfig(run, *target.repoConfig) {
				continue
			}
		} else if target.targetRepo != "" {
			tRepo := run.Labels[consumer.LabelTargetRepo]
			if tRepo == "" {
				tRepo = run.Labels[consumer.LabelRepo]
			}
			if tRepo != target.targetRepo {
				continue
			}
		}

		matched = append(matched, run)
	}
	return matched
}

func runMatchesBranch(run *v1alpha1.Run, branch string) bool {
	if run.Labels == nil {
		return false
	}
	lBranch := run.Labels[consumer.LabelBranch]
	return lBranch == branch || lBranch == consumer.SanitizeLabelValue(branch)
}

func runMatchesRepoConfig(run *v1alpha1.Run, r syncer.RepoConfig) bool {
	if run.Labels == nil {
		return false
	}
	lTarget := run.Labels[consumer.LabelTargetRepo]
	lRepo := run.Labels[consumer.LabelRepo]

	check := func(labelVal string) bool {
		if labelVal == "" {
			return false
		}
		if labelVal == consumer.SanitizeLabelValue(r.Name) ||
			labelVal == consumer.SanitizeLabelValue(r.Ref.String()) ||
			labelVal == consumer.SanitizeLabelValue(r.Ref.Owner+"/"+r.Ref.Name) ||
			(r.Ref.Owner == "-" && labelVal == consumer.SanitizeLabelValue(r.Ref.Name)) {
			return true
		}
		return false
	}

	if check(lTarget) || check(lRepo) {
		return true
	}
	if r.CloneURL != "" && run.Spec.Repo.URL == r.CloneURL {
		return true
	}
	return false
}

func runCommitTime(run *v1alpha1.Run, logger logr.Logger) time.Time {
	if run == nil {
		return time.Time{}
	}
	if run.Annotations != nil {
		if raw := run.Annotations[consumer.AnnotationCommitTime]; raw != "" {
			t, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				if logger.GetSink() != nil {
					logger.V(1).Info("failed to parse commit time annotation, falling back to creation timestamp", "run", run.Name, "annotation", raw, "error", err)
				}
			} else {
				return t
			}
		}
	}
	return run.CreationTimestamp.Time
}

func (r *HeadTopUpRunner) findNewestHeadCommit(runsBySHA map[string][]*v1alpha1.Run) string {
	type commitInfo struct {
		sha        string
		commitTime time.Time
		created    time.Time
	}

	var commits []commitInfo
	for sha, runs := range runsBySHA {
		var latestCTime time.Time
		var latestCreated time.Time
		for _, run := range runs {
			ct := runCommitTime(run, r.logger)
			if ct.After(latestCTime) {
				latestCTime = ct
			}
			if run.CreationTimestamp.Time.After(latestCreated) {
				latestCreated = run.CreationTimestamp.Time
			}
		}
		commits = append(commits, commitInfo{
			sha:        sha,
			commitTime: latestCTime,
			created:    latestCreated,
		})
	}

	if len(commits) == 0 {
		return ""
	}

	sort.Slice(commits, func(i, j int) bool {
		if !commits[i].commitTime.Equal(commits[j].commitTime) {
			return commits[i].commitTime.After(commits[j].commitTime)
		}
		if !commits[i].created.Equal(commits[j].created) {
			return commits[i].created.After(commits[j].created)
		}
		return commits[i].sha > commits[j].sha
	})

	return commits[0].sha
}

func hasInFlightRun(runs []*v1alpha1.Run) bool {
	for _, run := range runs {
		switch run.Status.Phase {
		case "", v1alpha1.RunPhasePending, v1alpha1.RunPhaseDiscovering, v1alpha1.RunPhaseRunning:
			return true
		}
	}
	return false
}

func findSeedRun(runs []*v1alpha1.Run) *v1alpha1.Run {
	if len(runs) == 0 {
		return nil
	}

	// Look for a run without topup label or suffix
	for _, r := range runs {
		if r.Labels != nil && r.Labels[consumer.LabelTopUp] != "" {
			continue
		}
		return r
	}

	// Fallback to earliest run
	sorted := make([]*v1alpha1.Run, len(runs))
	copy(sorted, runs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreationTimestamp.Before(&sorted[j].CreationTimestamp)
	})
	return sorted[0]
}

func runScoredSamples(run *v1alpha1.Run) int {
	if run == nil {
		return 0
	}
	total := 0
	for _, t := range run.Status.Tasks {
		if t.ID == "" {
			continue
		}
		if tc, ok := pooling.ExtractTaskCounts(t); ok {
			total += tc.Samples
		}
	}
	return total
}

func sortRunsDescending(runs []*v1alpha1.Run) []*v1alpha1.Run {
	if len(runs) == 0 {
		return nil
	}
	sorted := make([]*v1alpha1.Run, len(runs))
	copy(sorted, runs)
	sort.Slice(sorted, func(i, j int) bool {
		idxI := runTopUpIndex(sorted[i])
		idxJ := runTopUpIndex(sorted[j])
		if idxI != idxJ {
			return idxI > idxJ
		}
		return sorted[i].CreationTimestamp.After(sorted[j].CreationTimestamp.Time)
	})
	return sorted
}

func findLatestCompletedRun(runs []*v1alpha1.Run) *v1alpha1.Run {
	sorted := sortRunsDescending(runs)
	if len(sorted) == 0 {
		return nil
	}
	return sorted[0]
}

func runTopUpIndex(run *v1alpha1.Run) int {
	if run == nil || run.Labels == nil {
		return 1
	}
	val := run.Labels[consumer.LabelTopUp]
	if val == "" {
		return 1
	}
	n, err := strconv.Atoi(val)
	if err != nil || n < 1 {
		return 1
	}
	return n
}

func findNextRunName(seedRun *v1alpha1.Run, existing map[string]bool) (int, string) {
	baseName := seedRun.Name
	for n := 2; ; n++ {
		candidate := fmt.Sprintf("%s-%d", baseName, n)
		if !existing[candidate] {
			return n, candidate
		}
	}
}

func determineNextSampleRange(prevRun *v1alpha1.Run, batchSize int) *v1alpha1.SampleRange {
	if prevRun == nil {
		return &v1alpha1.SampleRange{Start: 1, End: batchSize}
	}

	prevEnd := batchSize
	if prevRun.Spec.SampleRange != nil {
		prevEnd = prevRun.Spec.SampleRange.End
	}

	requestedSamples := batchSize
	if prevRun.Spec.SampleRange != nil {
		requestedSamples = prevRun.Spec.SampleRange.End - prevRun.Spec.SampleRange.Start + 1
	}

	datasetExhausted := false
	if len(prevRun.Status.Tasks) > 0 {
		for _, t := range prevRun.Status.Tasks {
			if t.PossibleSamples > 0 {
				if prevEnd >= t.PossibleSamples {
					datasetExhausted = true
					break
				}
			} else if t.Phase == v1alpha1.TaskPhaseCompleted && t.Samples < requestedSamples {
				datasetExhausted = true
				break
			}
		}
	}

	if datasetExhausted {
		return &v1alpha1.SampleRange{Start: 1, End: batchSize}
	}

	return &v1alpha1.SampleRange{
		Start: prevEnd + 1,
		End:   prevEnd + batchSize,
	}
}
