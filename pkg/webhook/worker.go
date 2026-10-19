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

package webhook

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkItem represents a pre-validated event enqueued for asynchronous background processing.
type WorkItem struct {
	Event       string               // EventPullRequest or EventPush
	PullRequest *PullRequestWorkItem // populated if Event == EventPullRequest
	Push        *PushWorkItem        // populated if Event == EventPush
}

// WorkerPool manages bounded background workers that execute Kubernetes API operations.
type WorkerPool struct {
	cfg     Config
	queue   chan WorkItem
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	running bool
	mu      sync.Mutex
}

// NewWorkerPool constructs a WorkerPool with the configured queue size and workers.
func NewWorkerPool(cfg Config) *WorkerPool {
	qSize := cfg.QueueSize
	if qSize <= 0 {
		qSize = DefaultQueueSize
	}
	return &WorkerPool{
		cfg:   cfg,
		queue: make(chan WorkItem, qSize),
	}
}

// Start launches worker goroutines with their own background context.
func (wp *WorkerPool) Start() {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	if wp.running {
		return
	}
	wp.ctx, wp.cancel = context.WithCancel(context.Background())
	wp.running = true

	workerCount := wp.cfg.Workers
	if workerCount <= 0 {
		workerCount = DefaultWorkers
	}

	for i := 0; i < workerCount; i++ {
		wp.wg.Add(1)
		go wp.workerLoop(i)
	}
}

// Stop gracefully shuts down workers, draining the queue before cancelling context (F5).
func (wp *WorkerPool) Stop() {
	wp.mu.Lock()
	if !wp.running {
		wp.mu.Unlock()
		return
	}
	close(wp.queue)
	wp.running = false
	wp.mu.Unlock()

	wp.wg.Wait()
	if wp.cancel != nil {
		wp.cancel()
	}
}

// Enqueue attempts to add a work item to the bounded worker queue without blocking.
// Returns false if the queue is full.
func (wp *WorkerPool) Enqueue(item WorkItem) bool {
	select {
	case wp.queue <- item:
		return true
	default:
		return false
	}
}

func (wp *WorkerPool) workerLoop(id int) {
	defer wp.wg.Done()
	for item := range wp.queue {
		if err := wp.ProcessItem(wp.ctx, item); err != nil {
			log.Printf("[worker %d] error processing item: %v", id, err)
		}
	}
}

// ProcessItem executes the corresponding Kubernetes mutations for a WorkItem.
func (wp *WorkerPool) ProcessItem(ctx context.Context, item WorkItem) error {
	switch item.Event {
	case EventPullRequest:
		if item.PullRequest != nil {
			if item.PullRequest.Action == ActionClosed {
				return wp.ProcessPullRequestClosed(ctx, item.PullRequest)
			}
			return wp.ProcessPullRequest(ctx, item.PullRequest)
		}
	case EventPush:
		if item.Push != nil {
			return wp.ProcessPush(ctx, item.Push)
		}
	default:
		log.Printf("worker: unsupported work item event %q", item.Event)
	}
	return nil
}

// ProcessPullRequestClosed cleans up Comparisons and seed Runs when a pull request closes.
// Baseline Runs are preserved for cache reuse. Owned batch Runs are cleaned up via
// Kubernetes owner references cascading from the Comparison deletion.
func (wp *WorkerPool) ProcessPullRequestClosed(ctx context.Context, item *PullRequestWorkItem) error {
	if item == nil || wp.cfg.Client == nil {
		return nil
	}

	ns := wp.cfg.Namespace
	if ns == "" {
		ns = DefaultNamespace
	}
	prNum := item.PRNumber
	prNumStr := strconv.Itoa(prNum)
	targetRepo := item.TargetRepo

	// 1. Label-select Comparisons for that PR and delete them.
	// Check spec.pullRequest.repository as well as number (see #163).
	var compList v1alpha1.ComparisonList
	opts := []client.ListOption{client.MatchingLabels{LabelPR: prNumStr}}
	if ns != "" {
		opts = append(opts, client.InNamespace(ns))
	}
	if err := wp.cfg.Client.List(ctx, &compList, opts...); err != nil {
		return fmt.Errorf("failed to list comparisons for PR #%d: %w", prNum, err)
	}

	candRunNames := make(map[string]bool)
	deletedComparisonNames := make(map[string]bool)
	var deletedComparisons int
	for i := range compList.Items {
		comp := &compList.Items[i]
		if comp.Spec.PullRequest == nil {
			continue
		}
		if comp.Spec.PullRequest.Number != prNum {
			continue
		}
		if targetRepo != "" && !strings.EqualFold(comp.Spec.PullRequest.Repository, targetRepo) {
			continue
		}

		if comp.Spec.Candidate.Run != "" {
			candRunNames[comp.Spec.Candidate.Run] = true
		}

		if err := wp.cfg.Client.Delete(ctx, comp, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete comparison %s/%s for PR #%d: %w", comp.Namespace, comp.Name, prNum, err)
		}
		deletedComparisonNames[comp.Name] = true
		deletedComparisons++
	}

	// Find any baseline Runs referenced by surviving Comparisons in the namespace so they are not deleted out from under them.
	survivingBaselineRuns := make(map[string]bool)
	var allComps v1alpha1.ComparisonList
	var compListOpts []client.ListOption
	if ns != "" {
		compListOpts = append(compListOpts, client.InNamespace(ns))
	}
	if err := wp.cfg.Client.List(ctx, &allComps, compListOpts...); err != nil {
		return fmt.Errorf("failed to list surviving comparisons: %w", err)
	}
	for i := range allComps.Items {
		c := &allComps.Items[i]
		if !deletedComparisonNames[c.Name] && c.Spec.Baseline.Run != "" {
			survivingBaselineRuns[c.Spec.Baseline.Run] = true
		}
	}

	// 2. Label-select seed Runs created by the webhook for the PR head and delete them.
	// Baseline Runs do not carry LabelPR and are preserved.
	var runList v1alpha1.RunList
	if err := wp.cfg.Client.List(ctx, &runList, opts...); err != nil {
		return fmt.Errorf("failed to list runs for PR #%d: %w", prNum, err)
	}

	sanitizedTargetRepo := SanitizeLabelValue(targetRepo)
	var sanitizedCandRepo string
	if item.CandidateRepo != "" {
		sanitizedCandRepo = SanitizeLabelValue(item.CandidateRepo)
	}

	var deletedRuns int
	for i := range runList.Items {
		run := &runList.Items[i]
		isMatch := candRunNames[run.Name] ||
			(sanitizedTargetRepo != "" && run.Labels[LabelRepo] == sanitizedTargetRepo) ||
			(sanitizedCandRepo != "" && run.Labels[LabelRepo] == sanitizedCandRepo)

		if !isMatch {
			continue
		}

		// Guard: skip Runs referenced as baseline by a surviving Comparison,
		// or carrying an owner reference to a Comparison not deleted in this call.
		if survivingBaselineRuns[run.Name] {
			log.Printf("preserving seed run %s/%s: referenced as baseline by surviving comparison", run.Namespace, run.Name)
			continue
		}
		var ownedBySurvivingComp bool
		for _, ref := range run.OwnerReferences {
			if (ref.Kind == "Comparison" || ref.Kind == "") && !deletedComparisonNames[ref.Name] {
				ownedBySurvivingComp = true
				break
			}
		}
		if ownedBySurvivingComp {
			log.Printf("preserving seed run %s/%s: carries owner reference to surviving comparison", run.Namespace, run.Name)
			continue
		}

		if err := wp.cfg.Client.Delete(ctx, run); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete seed run %s/%s for PR #%d: %w", run.Namespace, run.Name, prNum, err)
		}
		deletedRuns++
	}

	log.Printf("closed PR #%d for %s: deleted %d comparison(s) and %d seed run(s)", prNum, targetRepo, deletedComparisons, deletedRuns)
	return nil
}

// ProcessPullRequest reconciles candidate Run, baseline Run (with cache reuse), and Comparison for a PR.
func (wp *WorkerPool) ProcessPullRequest(ctx context.Context, item *PullRequestWorkItem) error {
	if item == nil || wp.cfg.Client == nil {
		return nil
	}

	prNum := item.PRNumber
	headSHA := item.HeadSHA
	baseSHA := item.BaseSHA

	candRunName := fmt.Sprintf("pr-%d-%s", prNum, ShortSHA(headSHA))
	defaultBaseRunName := fmt.Sprintf("base-%s", ShortSHA(baseSHA))
	compName := fmt.Sprintf("pr-%d", prNum)

	ns := wp.cfg.Namespace
	sanitizedCandRepo := SanitizeLabelValue(item.CandidateRepo)
	sanitizedBaseRepo := SanitizeLabelValue(item.BaselineRepo)
	sanitizedTargetRepo := SanitizeLabelValue(item.TargetRepo)

	// 1. Create or update Candidate Run: pr-<number>-<headSHA7>
	// Use CandidateCloneURL (head repo for fork PRs) so checkout succeeds (Blocking 1)
	candRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      candRunName,
			Namespace: ns,
			Labels: map[string]string{
				LabelPR:      strconv.Itoa(prNum),
				LabelRepo:    sanitizedCandRepo,
				LabelHeadSHA: headSHA,
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    item.CandidateCloneURL,
				Commit: headSHA,
			},
			Model: wp.cfg.DefaultModel,
			SampleRange: &v1alpha1.SampleRange{
				Start: 1,
				End:   wp.cfg.DefaultBatchSize,
			},
		},
	}
	if err := wp.createOrUpdateRun(ctx, candRun); err != nil {
		return fmt.Errorf("failed to create/update candidate run %s/%s: %w", ns, candRunName, err)
	}

	// 2. Reconcile Baseline Run with Baseline Cache (§4.1):
	// Reuse if a Completed Run for that exact commit, repo URL, and model already exists (F1).
	baselineRunName, err := wp.findCompletedBaselineRun(ctx, ns, baseSHA, item.BaselineCloneURL, wp.cfg.DefaultModel)
	if err != nil {
		log.Printf("warning: error checking baseline cache for commit %s: %v", baseSHA, err)
	}

	if baselineRunName != "" {
		log.Printf("reusing completed baseline run %s/%s for base commit %s (model %s)", ns, baselineRunName, baseSHA, wp.cfg.DefaultModel)
	} else {
		// Baseline not in cache: ensure base-<baseSHA7> exists or create it
		baselineRunName = defaultBaseRunName
		baseRun := &v1alpha1.Run{
			ObjectMeta: metav1.ObjectMeta{
				Name:      baselineRunName,
				Namespace: ns,
				Labels: map[string]string{
					LabelRepo:    sanitizedBaseRepo,
					LabelHeadSHA: baseSHA,
				},
			},
			Spec: v1alpha1.RunSpec{
				Repo: v1alpha1.RepoSpec{
					URL:    item.BaselineCloneURL,
					Commit: baseSHA,
				},
				Model: wp.cfg.DefaultModel,
				SampleRange: &v1alpha1.SampleRange{
					Start: 1,
					End:   wp.cfg.DefaultBatchSize,
				},
			},
		}
		if err := wp.createOrUpdateRun(ctx, baseRun); err != nil {
			return fmt.Errorf("failed to create/update baseline run %s/%s: %w", ns, baselineRunName, err)
		}
	}

	// 3. Create or update Comparison pr-<number>
	comp := &v1alpha1.Comparison{
		ObjectMeta: metav1.ObjectMeta{
			Name:      compName,
			Namespace: ns,
			Labels: map[string]string{
				LabelPR:      strconv.Itoa(prNum),
				LabelRepo:    sanitizedTargetRepo,
				LabelHeadSHA: headSHA,
			},
		},
		Spec: v1alpha1.ComparisonSpec{
			Baseline: v1alpha1.ComparisonRunRef{
				Run: baselineRunName,
			},
			Candidate: v1alpha1.ComparisonRunRef{
				Run: candRunName,
			},
			Sampling: &v1alpha1.ComparisonSamplingSpec{
				BatchSize:         wp.cfg.DefaultBatchSize,
				MaxSamplesPerTask: wp.cfg.DefaultMaxSamplesPerTask,
			},
			PullRequest: &v1alpha1.PullRequestSpec{
				Repository: item.TargetRepo,
				Number:     prNum,
				HeadSHA:    headSHA,
				BaseSHA:    baseSHA,
			},
		},
	}
	if err := wp.createOrUpdateComparison(ctx, comp); err != nil {
		return fmt.Errorf("failed to create/update comparison %s/%s: %w", ns, compName, err)
	}

	log.Printf("successfully reconciled PR #%d: candidate=%s, baseline=%s, comparison=%s", prNum, candRunName, baselineRunName, compName)
	return nil
}

// ProcessPush reconciles base-<sha7> on pushes to the default branch to warm the baseline cache.
func (wp *WorkerPool) ProcessPush(ctx context.Context, item *PushWorkItem) error {
	if item == nil || wp.cfg.Client == nil {
		return nil
	}

	commitSHA := item.CommitSHA
	ns := wp.cfg.Namespace
	baseRunName := fmt.Sprintf("base-%s", ShortSHA(commitSHA))
	sanitizedRepo := SanitizeLabelValue(item.Repo)

	baseRun := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name:      baseRunName,
			Namespace: ns,
			Labels: map[string]string{
				LabelRepo:    sanitizedRepo,
				LabelHeadSHA: commitSHA,
			},
		},
		Spec: v1alpha1.RunSpec{
			Repo: v1alpha1.RepoSpec{
				URL:    item.CloneURL,
				Commit: commitSHA,
			},
			Model: wp.cfg.DefaultModel,
			SampleRange: &v1alpha1.SampleRange{
				Start: 1,
				End:   wp.cfg.DefaultBatchSize,
			},
		},
	}

	if err := wp.createOrUpdateRun(ctx, baseRun); err != nil {
		return fmt.Errorf("failed to create/update warm baseline run %s/%s: %w", ns, baseRunName, err)
	}

	log.Printf("successfully reconciled push event: warm baseline run %s/%s created/updated", ns, baseRunName)
	return nil
}

// findCompletedBaselineRun checks if any Run in the namespace targeting commit, URL, and model has completed (F1).
func (wp *WorkerPool) findCompletedBaselineRun(ctx context.Context, namespace, commit, expectedURL, model string) (string, error) {
	// 1. Direct candidate base-<sha7>
	directName := fmt.Sprintf("base-%s", ShortSHA(commit))
	var directRun v1alpha1.Run
	if err := wp.cfg.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: directName}, &directRun); err != nil {
		if !apierrors.IsNotFound(err) {
			return "", err
		}
	} else if directRun.Status.Phase == v1alpha1.RunPhaseCompleted &&
		directRun.Spec.Model == model &&
		strings.EqualFold(directRun.Spec.Repo.Commit, commit) &&
		directRun.Spec.Repo.URL == expectedURL {
		return directRun.Name, nil
	}

	// 2. Query runs matching label head-sha in the namespace (F1: exact commit and repo URL match)
	var runList v1alpha1.RunList
	if err := wp.cfg.Client.List(ctx, &runList, client.InNamespace(namespace), client.MatchingLabels{LabelHeadSHA: commit}); err != nil {
		return "", err
	}

	for _, r := range runList.Items {
		if r.Status.Phase == v1alpha1.RunPhaseCompleted &&
			r.Spec.Model == model &&
			strings.EqualFold(r.Spec.Repo.Commit, commit) &&
			r.Spec.Repo.URL == expectedURL {
			return r.Name, nil
		}
	}
	return "", nil
}

func (wp *WorkerPool) createOrUpdateRun(ctx context.Context, run *v1alpha1.Run) error {
	var existing v1alpha1.Run
	key := types.NamespacedName{
		Namespace: run.Namespace,
		Name:      run.Name,
	}

	if err := wp.cfg.Client.Get(ctx, key, &existing); err != nil {
		if apierrors.IsNotFound(err) {
			if err := wp.cfg.Client.Create(ctx, run); err != nil {
				// Handle concurrency race where another worker created it (F4)
				if apierrors.IsAlreadyExists(err) {
					if getErr := wp.cfg.Client.Get(ctx, key, &existing); getErr != nil {
						return getErr
					}
				} else {
					return err
				}
			} else {
				return nil
			}
		} else {
			return err
		}
	}

	// Run already exists. Only reconcile labels (F2). Do not rewrite spec.
	needsUpdate := false
	if existing.Labels == nil {
		existing.Labels = make(map[string]string)
	}
	for k, v := range run.Labels {
		if existing.Labels[k] != v {
			existing.Labels[k] = v
			needsUpdate = true
		}
	}

	if needsUpdate {
		return wp.cfg.Client.Update(ctx, &existing)
	}
	return nil
}

func (wp *WorkerPool) createOrUpdateComparison(ctx context.Context, comp *v1alpha1.Comparison) error {
	var existing v1alpha1.Comparison
	key := types.NamespacedName{
		Namespace: comp.Namespace,
		Name:      comp.Name,
	}

	if err := wp.cfg.Client.Get(ctx, key, &existing); err != nil {
		if apierrors.IsNotFound(err) {
			if err := wp.cfg.Client.Create(ctx, comp); err != nil {
				// Handle concurrency race where another worker created it (F4)
				if apierrors.IsAlreadyExists(err) {
					if getErr := wp.cfg.Client.Get(ctx, key, &existing); getErr != nil {
						return getErr
					}
				} else {
					return err
				}
			} else {
				return nil
			}
		} else {
			return err
		}
	}

	// Comparison exists. Update baseline/candidate refs, pullRequest spec, and labels (for synchronize re-pointing).
	needsUpdate := false

	if existing.Spec.Candidate.Run != comp.Spec.Candidate.Run {
		existing.Spec.Candidate.Run = comp.Spec.Candidate.Run
		needsUpdate = true
	}
	if existing.Spec.Baseline.Run != comp.Spec.Baseline.Run {
		existing.Spec.Baseline.Run = comp.Spec.Baseline.Run
		needsUpdate = true
	}

	if comp.Spec.PullRequest != nil {
		if existing.Spec.PullRequest == nil ||
			existing.Spec.PullRequest.Repository != comp.Spec.PullRequest.Repository ||
			existing.Spec.PullRequest.Number != comp.Spec.PullRequest.Number ||
			existing.Spec.PullRequest.HeadSHA != comp.Spec.PullRequest.HeadSHA ||
			existing.Spec.PullRequest.BaseSHA != comp.Spec.PullRequest.BaseSHA {
			existing.Spec.PullRequest = comp.Spec.PullRequest.DeepCopy()
			needsUpdate = true
		}
	}

	if comp.Spec.Sampling != nil {
		if existing.Spec.Sampling == nil ||
			existing.Spec.Sampling.BatchSize != comp.Spec.Sampling.BatchSize ||
			existing.Spec.Sampling.MaxSamplesPerTask != comp.Spec.Sampling.MaxSamplesPerTask {
			existing.Spec.Sampling = comp.Spec.Sampling.DeepCopy()
			needsUpdate = true
		}
	}

	if existing.Labels == nil {
		existing.Labels = make(map[string]string)
	}
	for k, v := range comp.Labels {
		if existing.Labels[k] != v {
			existing.Labels[k] = v
			needsUpdate = true
		}
	}

	if needsUpdate {
		return wp.cfg.Client.Update(ctx, &existing)
	}
	return nil
}
