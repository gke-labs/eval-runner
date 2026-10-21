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

package ui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gke-labs/eval-runner/api/v1alpha1"
	"github.com/gke-labs/eval-runner/pkg/artifacts"
	"github.com/gke-labs/eval-runner/pkg/artifacts/sinkconfig"
	"github.com/gke-labs/eval-runner/pkg/configkeys"
	"github.com/gke-labs/eval-runner/pkg/stats"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DefaultPageLimit = 100
	MaxPageLimit     = 500
)

func isValidNamespace(ns string) bool {
	if ns == "" {
		return false
	}
	return len(validation.IsDNS1123Label(ns)) == 0
}

func isValidResourceName(name string) bool {
	if name == "" {
		return false
	}
	return len(validation.IsDNS1123Subdomain(name)) == 0
}

func isValidRepoHash(h string) bool {
	if len(h) < 1 || len(h) > 128 {
		return false
	}
	for _, ch := range h {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.') {
			return false
		}
	}
	return true
}

func parsePagination(r *http.Request) (offset, limit int, err error) {
	q := r.URL.Query()
	offset = 0
	limit = DefaultPageLimit

	if oStr := q.Get("offset"); oStr != "" {
		o, err := strconv.Atoi(oStr)
		if err != nil || o < 0 {
			return 0, 0, fmt.Errorf("invalid offset %q: must be non-negative integer", oStr)
		}
		offset = o
	}

	if lStr := q.Get("limit"); lStr != "" {
		l, err := strconv.Atoi(lStr)
		if err != nil || l <= 0 {
			return 0, 0, fmt.Errorf("invalid limit %q: must be positive integer", lStr)
		}
		if l > MaxPageLimit {
			l = MaxPageLimit
		}
		limit = l
	}

	return offset, limit, nil
}

func buildPageURL(baseURL string, query url.Values, newOffset int, limit int) string {
	q := make(url.Values)
	for k, v := range query {
		q[k] = v
	}
	if newOffset > 0 {
		q.Set("offset", strconv.Itoa(newOffset))
	} else {
		q.Del("offset")
	}
	if limit != DefaultPageLimit {
		q.Set("limit", strconv.Itoa(limit))
	} else {
		q.Del("limit")
	}
	encoded := q.Encode()
	if encoded == "" {
		return baseURL
	}
	return baseURL + "?" + encoded
}

// Config configures the web UI server.
type Config struct {
	Client           client.Reader
	Cache            client.Reader
	CacheSyncChecker func() bool
	DefaultNamespace string
	ArtifactSink     artifacts.ArtifactSink
	InitialBucket    string
	InitialDir       string
}

// Server serves read-only web views of Runs and Comparisons.
type Server struct {
	cfg              Config
	mux              *http.ServeMux
	cache            *logLRUCache
	compCache        *comparisonLRUCache
	reloadableSink   *artifacts.ReloadableSink
	configGeneration atomic.Int64
	sinkMu           sync.RWMutex
	lastBucket       string
	lastDir          string
	configuredOnce   bool
}

// readerFor returns the cache reader for the default namespace when available,
// falling back to the direct client for cross-namespace queries or when cache is not configured.
func (s *Server) readerFor(ns string) client.Reader {
	if s.cfg.Cache != nil && ns == s.cfg.DefaultNamespace {
		return s.cfg.Cache
	}
	if s.cfg.Client != nil {
		return s.cfg.Client
	}
	// Fallback to cache when Client is unset, allowing unit tests to inject a cache-only reader.
	return s.cfg.Cache
}

// NewServer initializes a new Server with route registration.
func NewServer(cfg Config) *Server {
	if cfg.DefaultNamespace == "" {
		cfg.DefaultNamespace = "eval-runner"
	}

	var relSink *artifacts.ReloadableSink
	if rs, ok := cfg.ArtifactSink.(*artifacts.ReloadableSink); ok {
		relSink = rs
	} else if cfg.ArtifactSink != nil {
		relSink = artifacts.NewReloadableSink(cfg.ArtifactSink)
	} else {
		relSink = artifacts.NewReloadableSink(nil)
	}

	s := &Server{
		cfg:            cfg,
		mux:            http.NewServeMux(),
		cache:          newLogLRUCache(64*1024*1024, 1*time.Hour),
		compCache:      newComparisonLRUCache(32*1024*1024, 1*time.Hour),
		reloadableSink: relSink,
		lastBucket:     cfg.InitialBucket,
		lastDir:        cfg.InitialDir,
		configuredOnce: cfg.InitialBucket != "" || cfg.InitialDir != "" || cfg.ArtifactSink != nil,
	}
	s.configGeneration.Store(1)

	s.routes()
	return s
}

// ArtifactSink returns the ReloadableSink wrapping the active artifact storage.
func (s *Server) ArtifactSink() artifacts.ArtifactSink {
	return s.reloadableSink
}

// hasArtifactStorage reports whether a persistent or local artifact sink is currently configured.
func (s *Server) hasArtifactStorage() bool {
	return s.reloadableSink.Current() != nil
}

// SetArtifactSink updates the underlying artifact sink on Server.
// It swaps the sink so in-flight reads complete safely.
func (s *Server) SetArtifactSink(sink artifacts.ArtifactSink) {
	s.reloadableSink.Swap(sink)
}

// ConfigGeneration returns the current configuration generation counter.
func (s *Server) ConfigGeneration() int64 {
	return s.configGeneration.Load()
}

// BumpConfigGeneration increments and returns the configuration generation counter.
func (s *Server) BumpConfigGeneration() int64 {
	return s.configGeneration.Add(1)
}

// ReloadConfigMap updates the server's artifact sink and bumps config_generation
// based on changes to the eval-runner-config ConfigMap.
func (s *Server) ReloadConfigMap(ctx context.Context, cm *corev1.ConfigMap, flagBucket, flagDir string, sinkFactory sinkconfig.SinkFactory) error {
	if sinkFactory == nil {
		sinkFactory = sinkconfig.NewArtifactSink
	}
	effBucket := flagBucket
	effDir := flagDir
	if cm != nil && cm.Data != nil {
		if effBucket == "" {
			effBucket = cm.Data[configkeys.ConfigMapKeyArtifactBucket]
		}
		if effDir == "" {
			effDir = cm.Data[configkeys.ConfigMapKeyArtifactDir]
		}
	}
	effBucket = strings.TrimSpace(effBucket)
	if strings.EqualFold(effBucket, configkeys.ConfigSentinelDisabled) {
		effBucket = ""
	}
	effDir = strings.TrimSpace(effDir)
	if strings.EqualFold(effDir, configkeys.ConfigSentinelDisabled) {
		effDir = ""
	}

	s.sinkMu.Lock()
	if effBucket == s.lastBucket && effDir == s.lastDir && s.configuredOnce {
		s.sinkMu.Unlock()
		return nil
	}
	s.sinkMu.Unlock()

	var newSink artifacts.ArtifactSink
	if effBucket != "" || effDir != "" {
		s, err := sinkFactory(ctx, effBucket, effDir, 0)
		if err != nil {
			return fmt.Errorf("unable to configure artifact sink (bucket=%q, dir=%q): %w", effBucket, effDir, err)
		}
		newSink = s
		log.Printf("configured artifact sink for UI (bucket=%q, dir=%q)", effBucket, effDir)
	} else {
		log.Printf("no artifact storage configured; sample trajectories will show configuration guidance")
	}

	s.SetArtifactSink(newSink)
	s.BumpConfigGeneration()

	s.sinkMu.Lock()
	s.lastBucket = effBucket
	s.lastDir = effDir
	s.configuredOnce = true
	s.sinkMu.Unlock()

	return nil
}

// ServeHTTP delegates to the internal mux.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /livez", s.handleLivez)
	s.mux.HandleFunc("GET /{$}", s.handleRoot)
	s.mux.HandleFunc("GET /runs", s.handleRunsList)
	s.mux.HandleFunc("GET /runs/{$}", s.handleRunsList)
	s.mux.HandleFunc("GET /runs/{namespace}/{name}", s.handleRunDetail)
	s.mux.HandleFunc("GET /runs/{namespace}/{name}/tasks", s.handleRunTasks)
	s.mux.HandleFunc("GET /runs/{namespace}/{name}/tasks/{rest...}", s.handleRunTasks)
	s.mux.HandleFunc("GET /comparisons", s.handleComparisonsList)
	s.mux.HandleFunc("GET /comparisons/{$}", s.handleComparisonsList)
	s.mux.HandleFunc("GET /comparisons/{namespace}/{name}", s.handleComparisonDetail)
	s.mux.HandleFunc("GET /pulls", s.handlePullsList)
	s.mux.HandleFunc("GET /pulls/{$}", s.handlePullsList)
	s.mux.HandleFunc("GET /pulls/{repohash}/{number}", s.handlePullDetail)
	s.mux.HandleFunc("GET /series", s.handleSeriesList)
	s.mux.HandleFunc("GET /series/{repohash}/{branch...}", s.handleSeriesDetail)
}

func parseMetricFilter(r *http.Request) MetricFilter {
	switch m := MetricFilter(strings.ToLower(strings.TrimSpace(r.URL.Query().Get("metric")))); m {
	case MetricFilterRate, MetricFilterLatency, MetricFilterTokens, MetricFilterCost:
		return m
	}
	return MetricFilterAll
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if s.cfg.CacheSyncChecker != nil && !s.cfg.CacheSyncChecker() {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, err := w.Write([]byte("cache not synced\n")); err != nil {
			return
		}
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("ok\n")); err != nil {
		return
	}
}

func (s *Server) handleLivez(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("ok\n")); err != nil {
		return
	}
}

// httpError writes a fixed public message and logs the detail, which must never reach the body.
func httpError(w http.ResponseWriter, status int, public string, detail string, err error) {
	if err != nil {
		if detail != "" {
			log.Printf("%s: %v", detail, err)
		} else {
			log.Printf("%v", err)
		}
	}
	http.Error(w, public, status)
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/runs", http.StatusFound)
}

func (s *Server) namespaceFor(r *http.Request) (string, error) {
	ns := r.URL.Query().Get("namespace")
	if ns != "" {
		if !isValidNamespace(ns) {
			return "", fmt.Errorf("invalid namespace %q: must be a valid DNS-1123 label", ns)
		}
		return ns, nil
	}
	return s.cfg.DefaultNamespace, nil
}

func (s *Server) handleRunsList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ns, err := s.namespaceFor(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid namespace", "invalid namespace query param", err)
		return
	}

	offset, limit, err := parsePagination(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid pagination params", "invalid pagination query params", err)
		return
	}

	var runList v1alpha1.RunList
	if err := s.readerFor(ns).List(ctx, &runList, client.InNamespace(ns)); err != nil {
		if apierrors.IsForbidden(err) {
			httpError(w, http.StatusForbidden, "forbidden: access to namespace denied", fmt.Sprintf("access to namespace %q denied", ns), err)
			return
		}
		httpError(w, http.StatusInternalServerError, "failed to list runs", fmt.Sprintf("failed to list runs in namespace %s", ns), err)
		return
	}

	// Sort runs by CreationTimestamp descending (newest first), with name tiebreaker
	sort.Slice(runList.Items, func(i, j int) bool {
		if !runList.Items[i].CreationTimestamp.Equal(&runList.Items[j].CreationTimestamp) {
			return runList.Items[i].CreationTimestamp.After(runList.Items[j].CreationTimestamp.Time)
		}
		return runList.Items[i].Name < runList.Items[j].Name
	})

	totalCount := len(runList.Items)
	start := offset
	if start > totalCount {
		start = totalCount
	}
	end := start + limit
	if end > totalCount {
		end = totalCount
	}

	pageRuns := runList.Items[start:end]
	items := make([]RunListItem, 0, len(pageRuns))
	for _, run := range pageRuns {
		tasks := "—"
		samples := 0
		rates := ""
		if run.Status.Phase == v1alpha1.RunPhaseRunning {
			runningTasks := 0
			for _, t := range run.Status.Tasks {
				if t.Phase == v1alpha1.TaskPhaseRunning {
					runningTasks++
				}
			}
			totalTasks := len(run.Status.Tasks)
			if totalTasks == 0 && run.Status.Discovery != nil {
				totalTasks = run.Status.Discovery.TaskCount
			}
			tasks = fmt.Sprintf("%d/%d tasks running", runningTasks, totalTasks)
			if run.Status.Summary != nil {
				samples = run.Status.Summary.TotalSamples
				rates = run.Status.Summary.Rates
			}
		} else if run.Status.Summary != nil {
			if run.Status.Summary.Tasks != "" {
				tasks = run.Status.Summary.Tasks
			}
			samples = run.Status.Summary.TotalSamples
			rates = run.Status.Summary.Rates
		} else if run.Status.Discovery != nil && run.Status.Discovery.TaskCount > 0 {
			tasks = fmt.Sprintf("0/%d", run.Status.Discovery.TaskCount)
		}

		var sampleRangeStr string
		if run.Spec.SampleRange != nil {
			sampleRangeStr = fmt.Sprintf("samples %d-%d", run.Spec.SampleRange.Start, run.Spec.SampleRange.End)
		} else {
			for _, t := range run.Status.Tasks {
				if t.SampleRange != nil && run.Spec.Limit == nil {
					sampleRangeStr = fmt.Sprintf("samples %d-%d", t.SampleRange.Start, t.SampleRange.End)
					break
				}
			}
		}

		items = append(items, RunListItem{
			Name:        run.Name,
			Namespace:   run.Namespace,
			Phase:       run.Status.Phase,
			PhaseChip:   phaseChipClass(run.Status.Phase),
			Tasks:       tasks,
			Samples:     samples,
			SampleRange: sampleRangeStr,
			Rates:       rates,
			Commit:      ShortCommit(run.Spec.Repo.Commit),
			Age:         FormatAge(run.CreationTimestamp),
			Link:        fmt.Sprintf("/runs/%s/%s", run.Namespace, run.Name),
		})
	}

	hasMore := end < totalCount
	hasPrev := start > 0

	var nextLink, prevLink string
	if hasMore {
		nextLink = buildPageURL("/runs", r.URL.Query(), end, limit)
	}
	if hasPrev {
		prevOffset := start - limit
		if prevOffset < 0 {
			prevOffset = 0
		}
		prevLink = buildPageURL("/runs", r.URL.Query(), prevOffset, limit)
	}

	showingRange := ""
	if totalCount > 0 {
		showingRange = fmt.Sprintf("Showing %d–%d of %d", start+1, end, totalCount)
	}

	view := RunsListView{
		ActiveTab:    NavTabRuns,
		Namespace:    ns,
		Runs:         items,
		TotalCount:   totalCount,
		Offset:       start,
		Limit:        limit,
		HasMore:      hasMore,
		NextLink:     nextLink,
		HasPrev:      hasPrev,
		PrevLink:     prevLink,
		ShowingRange: showingRange,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmplRunsList.Execute(w, view); err != nil {
		httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
	}
}

func (s *Server) handleRunDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	if !isValidNamespace(ns) || !isValidResourceName(name) {
		httpError(w, http.StatusBadRequest, "invalid route params", fmt.Sprintf("invalid route params: namespace=%q name=%q", ns, name), nil)
		return
	}

	var run v1alpha1.Run
	if err := s.readerFor(ns).Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &run); err != nil {
		if apierrors.IsNotFound(err) {
			httpError(w, http.StatusNotFound, "run not found", "", nil)
			return
		}
		if apierrors.IsForbidden(err) {
			httpError(w, http.StatusForbidden, "forbidden: access to run denied", fmt.Sprintf("access to run %s/%s denied", ns, name), err)
			return
		}
		httpError(w, http.StatusInternalServerError, "failed to get run", fmt.Sprintf("failed to get run %s/%s", ns, name), err)
		return
	}

	// Parse metric filter query param
	metricFilter := parseMetricFilter(r)

	// Prepare task rows
	taskRows := make([]RunTaskRow, 0, len(run.Status.Tasks))
	var totalCostUSD float64
	var hasCost bool
	var totalTokens int
	var totalReasoningTokens int
	var hasTokens bool
	var sumP50, sumP95 float64
	var countLatency int
	for _, task := range run.Status.Tasks {
		leftPx, widthPx, dotPx, hasCI := CalculateRunBarGeometry(task.SuccessRate, task.MeanScore, task.SuccessRateCI, 240.0)

		rateStr := "—"
		if task.SuccessRate != nil {
			rateStr = fmt.Sprintf("%.2f", *task.SuccessRate)
		} else if task.MeanScore != nil {
			rateStr = fmt.Sprintf("%.2f", *task.MeanScore)
		}

		ciLabel := "—"
		method := ""
		if task.SuccessRateCI != nil {
			ciLabel = fmt.Sprintf("[%.2f, %.2f]", task.SuccessRateCI.Lower, task.SuccessRateCI.Upper)
			method = task.SuccessRateCI.Method
		}

		var taskSampleRange string
		if run.Spec.SampleRange != nil {
			taskSampleRange = fmt.Sprintf("samples %d-%d", run.Spec.SampleRange.Start, run.Spec.SampleRange.End)
		} else if task.SampleRange != nil && run.Spec.Limit == nil {
			taskSampleRange = fmt.Sprintf("samples %d-%d", task.SampleRange.Start, task.SampleRange.End)
		}

		var samplesLink string
		hasStorage := s.hasArtifactStorage()
		artifactsUnavailable := !hasStorage && task.LogPath != ""
		if task.LogPath != "" && hasStorage {
			samplesLink = BuildTaskSamplesListLink(ns, name, task.ID)
		}

		samplesDone := task.SamplesDone
		samplesPlanned := task.SamplesPlanned
		if samplesPlanned == 0 {
			if task.SampleRange != nil {
				samplesPlanned = task.SampleRange.End - task.SampleRange.Start + 1
			} else if run.Spec.SampleRange != nil {
				samplesPlanned = run.Spec.SampleRange.End - run.Spec.SampleRange.Start + 1
			} else if run.Spec.Limit != nil {
				samplesPlanned = *run.Spec.Limit
			} else if task.PossibleSamples > 0 {
				samplesPlanned = task.PossibleSamples
			}
		}
		if task.PossibleSamples > 0 && task.PossibleSamples < samplesPlanned {
			samplesPlanned = task.PossibleSamples
		}

		var progressText string
		var progressPct float64
		if task.Phase == v1alpha1.TaskPhaseRunning {
			if samplesPlanned > 0 {
				progressText = fmt.Sprintf("%d of %d samples", samplesDone, samplesPlanned)
				progressPct = float64(samplesDone) / float64(samplesPlanned) * 100.0
				if progressPct > 100.0 {
					progressPct = 100.0
				}
			} else {
				progressText = fmt.Sprintf("%d samples", samplesDone)
			}
		}

		var latStr string = "—"
		if task.Latency != nil && (task.Latency.P50 > 0 || task.Latency.P95 > 0) {
			latStr = fmt.Sprintf("%.2fs / %.2fs", task.Latency.P50, task.Latency.P95)
			sumP50 += task.Latency.P50
			sumP95 += task.Latency.P95
			countLatency++
		}

		var tokensStr string = "—"
		var inTokensStr string = "—"
		var outTokensStr string = "—"
		var reasoningStr string = "—"
		var cacheReadStr string = "—"
		if task.Tokens != nil {
			hasTokens = true
			totalTokens += task.Tokens.Total
			tokensStr = fmt.Sprintf("%d", task.Tokens.Total)
			inTokensStr = fmt.Sprintf("%d", task.Tokens.Input)
			outTokensStr = fmt.Sprintf("%d", task.Tokens.Output)
			if task.Tokens.Reasoning > 0 {
				totalReasoningTokens += task.Tokens.Reasoning
				reasoningStr = fmt.Sprintf("%d", task.Tokens.Reasoning)
			}
			if task.Tokens.CacheRead > 0 {
				cacheReadStr = fmt.Sprintf("%d", task.Tokens.CacheRead)
			}
		}

		var costStr string = "—"
		if task.CostUSD != nil {
			hasCost = true
			totalCostUSD += *task.CostUSD
			costStr = fmt.Sprintf("$%.4f", *task.CostUSD)
		}

		taskRows = append(taskRows, RunTaskRow{
			ID:                   task.ID,
			Phase:                task.Phase,
			PhaseChip:            taskPhaseChipClass(task.Phase),
			RateStr:              rateStr,
			HasCI:                hasCI,
			CILeftPx:             leftPx,
			CIWidthPx:            widthPx,
			CIDotPx:              dotPx,
			CILabel:              ciLabel,
			Samples:              task.Samples,
			Successes:            task.Successes,
			Method:               method,
			LogPath:              task.LogPath,
			SamplesLink:          samplesLink,
			Message:              task.Message,
			SampleRange:          taskSampleRange,
			SamplesDone:          samplesDone,
			SamplesPlanned:       samplesPlanned,
			ProgressPct:          progressPct,
			ProgressText:         progressText,
			LatencyStr:           latStr,
			TokensStr:            tokensStr,
			InputTokens:          inTokensStr,
			OutputTokens:         outTokensStr,
			ReasoningStr:         reasoningStr,
			CacheReadStr:         cacheReadStr,
			CostStr:              costStr,
			ArtifactsUnavailable: artifactsUnavailable,
		})
	}

	if len(run.Status.Tasks) == 0 && run.Status.Discovery != nil {
		taskIDs := run.Status.Discovery.TaskIDs
		if s.hasArtifactStorage() && run.Status.Discovery.TaskCount > len(run.Status.Discovery.TaskIDs) && run.Status.Discovery.ManifestPath != "" {
			manifestBytes, loadErr := s.reloadableSink.Load(r.Context(), run.Status.Discovery.ManifestPath)
			if loadErr != nil {
				// Fall back to capped taskIDs
			} else {
				var m v1alpha1.Manifest
				if parseErr := json.Unmarshal(manifestBytes, &m); parseErr != nil {
					// Fall back
				} else {
					var allIDs []string
					for _, t := range m.Tasks {
						allIDs = append(allIDs, t.ID)
					}
					taskIDs = allIDs
				}
			}
		}
		for _, id := range taskIDs {
			taskRows = append(taskRows, RunTaskRow{
				ID:        id,
				Phase:     "Discovered",
				PhaseChip: "neutral",
				RateStr:   "—",
			})
		}
	}

	tasksSummary := ""
	tasksCompleted := 0
	tasksFailed := 0
	tasksTotal := 0
	totalSamples := 0
	meanSuccessRate := ""
	successRateRange := ""
	compactRates := ""

	if run.Status.Summary != nil {
		tasksSummary = run.Status.Summary.Tasks
		tasksCompleted = run.Status.Summary.TasksCompleted
		tasksFailed = run.Status.Summary.TasksFailed
		tasksTotal = run.Status.Summary.TasksTotal
		totalSamples = run.Status.Summary.TotalSamples
		if run.Status.Summary.MeanSuccessRate != nil {
			meanSuccessRate = fmt.Sprintf("%.2f", *run.Status.Summary.MeanSuccessRate)
		}
		if run.Status.Summary.SuccessRateRange != nil {
			successRateRange = fmt.Sprintf("[%.2f, %.2f]", run.Status.Summary.SuccessRateRange.Min, run.Status.Summary.SuccessRateRange.Max)
		}
		compactRates = run.Status.Summary.Rates
	} else if run.Status.Discovery != nil && run.Status.Discovery.TaskCount > 0 {
		tasksSummary = fmt.Sprintf("0/%d", run.Status.Discovery.TaskCount)
		tasksTotal = run.Status.Discovery.TaskCount
	}

	var detailSampleRange string
	if run.Spec.SampleRange != nil {
		detailSampleRange = fmt.Sprintf("samples %d-%d", run.Spec.SampleRange.Start, run.Spec.SampleRange.End)
	} else {
		for _, t := range run.Status.Tasks {
			if t.SampleRange != nil && run.Spec.Limit == nil {
				detailSampleRange = fmt.Sprintf("samples %d-%d", t.SampleRange.Start, t.SampleRange.End)
				break
			}
		}
	}

	var totalCostFormatted string
	if hasCost {
		totalCostFormatted = fmt.Sprintf("$%.4f", totalCostUSD)
	}

	var totalTokensFormatted string
	if hasTokens {
		if totalReasoningTokens > 0 {
			totalTokensFormatted = fmt.Sprintf("%d (+%d R)", totalTokens, totalReasoningTokens)
		} else {
			totalTokensFormatted = fmt.Sprintf("%d", totalTokens)
		}
	}

	var latencySummaryFormatted string
	if countLatency > 0 {
		avgP50 := sumP50 / float64(countLatency)
		avgP95 := sumP95 / float64(countLatency)
		latencySummaryFormatted = fmt.Sprintf("%.2fs / %.2fs", avgP50, avgP95)
	}

	view := RunDetailView{
		ActiveTab:        NavTabRuns,
		Name:             run.Name,
		Namespace:        run.Namespace,
		Phase:            run.Status.Phase,
		PhaseChip:        phaseChipClass(run.Status.Phase),
		RepoURL:          run.Spec.Repo.URL,
		Commit:           run.Spec.Repo.Commit,
		CommitShort:      ShortCommit(run.Spec.Repo.Commit),
		Model:            run.Spec.Model,
		Age:              FormatAge(run.CreationTimestamp),
		ArtifactsPrefix:  run.Status.Artifacts,
		ArtifactFooter:   FormatArtifactFooter(s.ArtifactSink()),
		Message:          run.Status.Message,
		Metric:           metricFilter,
		TotalCost:        totalCostFormatted,
		TokensSummary:    totalTokensFormatted,
		LatencySummary:   latencySummaryFormatted,
		TasksSummary:     tasksSummary,
		TasksCompleted:   tasksCompleted,
		TasksFailed:      tasksFailed,
		TasksTotal:       tasksTotal,
		TotalSamples:     totalSamples,
		SampleRange:      detailSampleRange,
		MeanSuccessRate:  meanSuccessRate,
		SuccessRateRange: successRateRange,
		CompactRates:     compactRates,
		Tasks:            taskRows,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmplRunDetail.Execute(w, view); err != nil {
		httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
	}
}

func (s *Server) handleComparisonsList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ns, err := s.namespaceFor(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid namespace", "invalid namespace query param", err)
		return
	}

	offset, limit, err := parsePagination(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid pagination params", "invalid pagination query params", err)
		return
	}
	kind := ParseComparisonKind(r.URL.Query().Get("kind"))

	var compList v1alpha1.ComparisonList
	if err := s.readerFor(ns).List(ctx, &compList, client.InNamespace(ns)); err != nil {
		if apierrors.IsForbidden(err) {
			httpError(w, http.StatusForbidden, "forbidden: access to namespace denied", fmt.Sprintf("access to namespace %q denied", ns), err)
			return
		}
		httpError(w, http.StatusInternalServerError, "failed to list comparisons", fmt.Sprintf("failed to list comparisons in namespace %s", ns), err)
		return
	}

	var runs map[string]*v1alpha1.Run
	if len(compList.Items) > 0 {
		runs = s.runsByName(ctx, ns)
	}

	// Rows are labeled candidate groups and unlabeled Comparisons, newest first.
	rows, totalCount := comparisonListRows(compList.Items, kind, runs)

	totalRows := len(rows)
	start := offset
	if start > totalRows {
		start = totalRows
	}
	end := start + limit
	if end > totalRows {
		end = totalRows
	}

	var items []ComparisonListItem
	var groups []ComparisonGroupRow
	for _, row := range rows[start:end] {
		if row.isGroup() {
			groups = append(groups, row.groupRow(runs))
		} else {
			items = append(items, row.listItem(runs))
		}
	}

	hasMore := end < totalRows
	hasPrev := start > 0

	var nextLink, prevLink string
	if hasMore {
		nextLink = buildPageURL("/comparisons", r.URL.Query(), end, limit)
	}
	if hasPrev {
		prevOffset := start - limit
		if prevOffset < 0 {
			prevOffset = 0
		}
		prevLink = buildPageURL("/comparisons", r.URL.Query(), prevOffset, limit)
	}

	showingRange := ""
	if totalRows > 0 {
		showingRange = fmt.Sprintf("Showing %d–%d of %d", start+1, end, totalRows)
	}

	query := r.URL.Query()
	view := ComparisonsListView{
		ActiveTab:          NavTabComparisons,
		Namespace:          ns,
		Comparisons:        items,
		TotalCount:         totalCount,
		Offset:             start,
		Limit:              limit,
		HasMore:            hasMore,
		NextLink:           nextLink,
		HasPrev:            hasPrev,
		PrevLink:           prevLink,
		ShowingRange:       showingRange,
		Kind:               kind,
		KindLabelKey:       kindLabelKey(kind),
		ShowKindFilter:     kind != ComparisonKindAll || hasLabeledComparison(compList.Items),
		KindAllLink:        comparisonsKindLink(query, ComparisonKindAll),
		KindBaselineLink:   comparisonsKindLink(query, ComparisonKindBaseline),
		KindPreviousLink:   comparisonsKindLink(query, ComparisonKindPrevious),
		Groups:             groups,
		ShowGroups:         len(groups) > 0 || kind != ComparisonKindAll,
		ShowBaselineColumn: kind != ComparisonKindPrevious,
		ShowPreviousColumn: kind != ComparisonKindBaseline,
		// The flat table also carries the empty state when a page has no rows at all.
		ShowFlat: kind == ComparisonKindAll && (len(items) > 0 || len(groups) == 0),
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmplComparisonsList.Execute(w, view); err != nil {
		httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
	}
}

// runsByName lists the Runs in ns once per request so that each Comparison row can resolve
// its baseline and candidate Runs. A failed list is logged and returns nil: rows then show
// missing run deltas, and candidate groups are ordered by Comparison creation time.
func (s *Server) runsByName(ctx context.Context, ns string) map[string]*v1alpha1.Run {
	var runList v1alpha1.RunList
	if err := s.readerFor(ns).List(ctx, &runList, client.InNamespace(ns)); err != nil {
		log.Printf("failed to list runs in namespace %s for the comparisons page: %v", ns, err)
		return nil
	}
	runs := make(map[string]*v1alpha1.Run, len(runList.Items))
	for i := range runList.Items {
		runs[runList.Items[i].Name] = &runList.Items[i]
	}
	return runs
}

func (s *Server) handleComparisonDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	if !isValidNamespace(ns) || !isValidResourceName(name) {
		httpError(w, http.StatusBadRequest, "invalid route params", fmt.Sprintf("invalid route params: namespace=%q name=%q", ns, name), nil)
		return
	}

	var comp v1alpha1.Comparison
	if err := s.readerFor(ns).Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &comp); err != nil {
		if apierrors.IsNotFound(err) {
			httpError(w, http.StatusNotFound, "comparison not found", "", nil)
			return
		}
		if apierrors.IsForbidden(err) {
			httpError(w, http.StatusForbidden, "forbidden: access to comparison denied", fmt.Sprintf("access to comparison %s/%s denied", ns, name), err)
			return
		}
		httpError(w, http.StatusInternalServerError, "failed to get comparison", fmt.Sprintf("failed to get comparison %s/%s", ns, name), err)
		return
	}

	// Fetch baseline and candidate Run resources to discover commits and sample counts
	var baseRunPtr, candRunPtr *v1alpha1.Run
	if comp.Spec.Baseline.Run != "" {
		var baseRun v1alpha1.Run
		if err := s.readerFor(ns).Get(ctx, types.NamespacedName{Namespace: ns, Name: comp.Spec.Baseline.Run}, &baseRun); err != nil {
			if !apierrors.IsNotFound(err) {
				// Non-fatal lookup error, continue with run name reference
				_ = err
			}
		} else {
			baseRunPtr = &baseRun
		}
	}

	if comp.Spec.Candidate.Run != "" {
		var candRun v1alpha1.Run
		if err := s.readerFor(ns).Get(ctx, types.NamespacedName{Namespace: ns, Name: comp.Spec.Candidate.Run}, &candRun); err != nil {
			if !apierrors.IsNotFound(err) {
				// Non-fatal lookup error, continue with run name reference
				_ = err
			}
		} else {
			candRunPtr = &candRun
		}
	}

	view := NewComparisonDetailView(&comp, baseRunPtr, candRunPtr)
	view.Metric = parseMetricFilter(r)
	s.populateComparisonHistories(ctx, &comp, &view)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmplComparisonDetail.Execute(w, view); err != nil {
		httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
	}
}

func (s *Server) handlePullsList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ns, err := s.namespaceFor(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid namespace", "invalid namespace query param", err)
		return
	}

	repoQuery := r.URL.Query().Get("repo")
	if repoQuery == "" {
		repoQuery = r.URL.Query().Get("repohash")
	}
	numberQuery := r.URL.Query().Get("number")
	if repoQuery != "" && numberQuery != "" {
		num, err := strconv.Atoi(numberQuery)
		if err != nil || num <= 0 {
			httpError(w, http.StatusBadRequest, "invalid route params", fmt.Sprintf("invalid pr number in query: %q", numberQuery), nil)
			return
		}
		s.renderPullHistory(w, r, ns, repoQuery, num)
		return
	}

	offset, limit, err := parsePagination(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid pagination params", "invalid pagination query params", err)
		return
	}

	var compList v1alpha1.ComparisonList
	if err := s.readerFor(ns).List(ctx, &compList, client.InNamespace(ns)); err != nil {
		if apierrors.IsForbidden(err) {
			httpError(w, http.StatusForbidden, "forbidden: access to namespace denied", fmt.Sprintf("access to namespace %q denied", ns), err)
			return
		}
		httpError(w, http.StatusInternalServerError, "failed to list comparisons", fmt.Sprintf("failed to list comparisons in namespace %s", ns), err)
		return
	}

	type prGroup struct {
		repository  string
		repoHash    string
		number      int
		comparisons []v1alpha1.Comparison
	}

	groupsMap := make(map[string]*prGroup)
	var groupKeys []string

	for _, comp := range compList.Items {
		if comp.Spec.PullRequest == nil || comp.Spec.PullRequest.Repository == "" || comp.Spec.PullRequest.Number <= 0 {
			continue
		}
		repo := comp.Spec.PullRequest.Repository
		prNum := comp.Spec.PullRequest.Number
		key := fmt.Sprintf("%s#%d", strings.ToLower(strings.TrimSpace(repo)), prNum)

		grp, exists := groupsMap[key]
		if !exists {
			grp = &prGroup{
				repository: repo,
				repoHash:   RepoHash(repo),
				number:     prNum,
			}
			groupsMap[key] = grp
			groupKeys = append(groupKeys, key)
		}
		grp.comparisons = append(grp.comparisons, comp)
	}

	// For each group, sort comparisons newest first (by CreationTimestamp descending, tiebreaker Name ascending)
	groups := make([]*prGroup, 0, len(groupKeys))
	for _, key := range groupKeys {
		grp := groupsMap[key]
		sort.Slice(grp.comparisons, func(i, j int) bool {
			if !grp.comparisons[i].CreationTimestamp.Equal(&grp.comparisons[j].CreationTimestamp) {
				return grp.comparisons[i].CreationTimestamp.After(grp.comparisons[j].CreationTimestamp.Time)
			}
			return grp.comparisons[i].Name < grp.comparisons[j].Name
		})
		groups = append(groups, grp)
	}

	// Sort groups newest head first (by latest comparison CreationTimestamp descending, tiebreaker repo, then number)
	sort.Slice(groups, func(i, j int) bool {
		latestI := groups[i].comparisons[0]
		latestJ := groups[j].comparisons[0]
		if !latestI.CreationTimestamp.Equal(&latestJ.CreationTimestamp) {
			return latestI.CreationTimestamp.After(latestJ.CreationTimestamp.Time)
		}
		if groups[i].repository != groups[j].repository {
			return groups[i].repository < groups[j].repository
		}
		return groups[i].number < groups[j].number
	})

	totalCount := len(groups)
	start := offset
	if start > totalCount {
		start = totalCount
	}
	end := start + limit
	if end > totalCount {
		end = totalCount
	}

	pageGroups := groups[start:end]
	items := make([]PullRequestListItem, 0, len(pageGroups))
	for _, grp := range pageGroups {
		latest := grp.comparisons[0]
		pImp := "—"
		if latest.Status.PImprovement != nil {
			pImp = fmt.Sprintf("%.2f", *latest.Status.PImprovement)
		}

		historyLink := fmt.Sprintf("/pulls/%s/%d", grp.repoHash, grp.number)
		if ns != s.cfg.DefaultNamespace {
			historyLink += "?namespace=" + url.QueryEscape(ns)
		}

		items = append(items, PullRequestListItem{
			Repository:     grp.repository,
			RepoHash:       grp.repoHash,
			Number:         grp.number,
			GitHubURL:      BuildGitHubPRURL(grp.repository, grp.number),
			HeadSHA:        latest.Spec.PullRequest.HeadSHA,
			HeadSHAShort:   ShortCommit(latest.Spec.PullRequest.HeadSHA),
			Verdict:        latest.Status.Verdict,
			VerdictChip:    verdictChipClass(latest.Status.Verdict),
			PImprovement:   pImp,
			SamplesRun:     ComparisonSamplesRun(&latest),
			Batches:        len(latest.Status.Batches),
			CompletionTime: FormatCompletionTime(latest.Status.CompletionTime),
			ComparisonName: latest.Name,
			ComparisonLink: fmt.Sprintf("/comparisons/%s/%s", latest.Namespace, latest.Name),
			HistoryLink:    historyLink,
			Namespace:      latest.Namespace,
		})
	}

	hasMore := end < totalCount
	hasPrev := start > 0

	var nextLink, prevLink string
	if hasMore {
		nextLink = buildPageURL("/pulls", r.URL.Query(), end, limit)
	}
	if hasPrev {
		prevOffset := start - limit
		if prevOffset < 0 {
			prevOffset = 0
		}
		prevLink = buildPageURL("/pulls", r.URL.Query(), prevOffset, limit)
	}

	showingRange := ""
	if totalCount > 0 {
		showingRange = fmt.Sprintf("Showing %d–%d of %d", start+1, end, totalCount)
	}

	view := PullRequestsListView{
		ActiveTab:    NavTabPulls,
		Namespace:    ns,
		Pulls:        items,
		TotalCount:   totalCount,
		Offset:       start,
		Limit:        limit,
		HasMore:      hasMore,
		NextLink:     nextLink,
		HasPrev:      hasPrev,
		PrevLink:     prevLink,
		ShowingRange: showingRange,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmplPullsList.Execute(w, view); err != nil {
		httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
	}
}

func (s *Server) handlePullDetail(w http.ResponseWriter, r *http.Request) {
	repohash := r.PathValue("repohash")
	numberStr := r.PathValue("number")
	if !isValidRepoHash(repohash) {
		httpError(w, http.StatusBadRequest, "invalid route params", fmt.Sprintf("invalid repo hash: %q", repohash), nil)
		return
	}
	num, err := strconv.Atoi(numberStr)
	if err != nil || num <= 0 {
		httpError(w, http.StatusBadRequest, "invalid route params", fmt.Sprintf("invalid pr number: %q", numberStr), nil)
		return
	}

	ns, err := s.namespaceFor(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid namespace", "invalid namespace query param", err)
		return
	}

	s.renderPullHistory(w, r, ns, repohash, num)
}

func (s *Server) renderPullHistory(w http.ResponseWriter, r *http.Request, ns string, repoOrHash string, num int) {
	ctx := r.Context()
	var compList v1alpha1.ComparisonList
	if err := s.readerFor(ns).List(ctx, &compList, client.InNamespace(ns)); err != nil {
		if apierrors.IsForbidden(err) {
			httpError(w, http.StatusForbidden, "forbidden: access to namespace denied", fmt.Sprintf("access to namespace %q denied", ns), err)
			return
		}
		httpError(w, http.StatusInternalServerError, "failed to list comparisons", fmt.Sprintf("failed to list comparisons in namespace %s", ns), err)
		return
	}

	var matched []v1alpha1.Comparison
	repoDisplay := ""
	for _, comp := range compList.Items {
		if comp.Spec.PullRequest == nil || comp.Spec.PullRequest.Number != num {
			continue
		}
		repo := comp.Spec.PullRequest.Repository
		hash := RepoHash(repo)
		if strings.EqualFold(hash, repoOrHash) || strings.EqualFold(repo, repoOrHash) {
			matched = append(matched, comp)
			if repoDisplay == "" {
				repoDisplay = repo
			}
		}
	}

	if len(matched) == 0 {
		httpError(w, http.StatusNotFound, "pull request not found", fmt.Sprintf("no comparisons for %s #%d in %s", repoOrHash, num, ns), nil)
		return
	}

	// Sort matched comparisons newest first (by CreationTimestamp descending, tiebreaker Name ascending)
	sort.Slice(matched, func(i, j int) bool {
		if !matched[i].CreationTimestamp.Equal(&matched[j].CreationTimestamp) {
			return matched[i].CreationTimestamp.After(matched[j].CreationTimestamp.Time)
		}
		return matched[i].Name < matched[j].Name
	})

	historyItems := make([]PullRequestHistoryItem, 0, len(matched))
	for _, comp := range matched {
		pImp := "—"
		if comp.Status.PImprovement != nil {
			pImp = fmt.Sprintf("%.2f", *comp.Status.PImprovement)
		}

		headSHA := ""
		baseSHA := ""
		if comp.Spec.PullRequest != nil {
			headSHA = comp.Spec.PullRequest.HeadSHA
			baseSHA = comp.Spec.PullRequest.BaseSHA
		}

		historyItems = append(historyItems, PullRequestHistoryItem{
			Name:           comp.Name,
			Namespace:      comp.Namespace,
			HeadSHA:        headSHA,
			HeadSHAShort:   ShortCommit(headSHA),
			BaseSHA:        baseSHA,
			BaseSHAShort:   ShortCommit(baseSHA),
			Verdict:        comp.Status.Verdict,
			VerdictChip:    verdictChipClass(comp.Status.Verdict),
			PImprovement:   pImp,
			SamplesRun:     ComparisonSamplesRun(&comp),
			Batches:        len(comp.Status.Batches),
			CompletionTime: FormatCompletionTime(comp.Status.CompletionTime),
			Age:            FormatAge(comp.CreationTimestamp),
			Link:           fmt.Sprintf("/comparisons/%s/%s", comp.Namespace, comp.Name),
		})
	}

	backLink := "/pulls"
	if ns != s.cfg.DefaultNamespace {
		backLink += "?namespace=" + url.QueryEscape(ns)
	}

	view := PullRequestDetailView{
		ActiveTab:   NavTabPulls,
		Namespace:   ns,
		Repository:  repoDisplay,
		RepoHash:    RepoHash(repoDisplay),
		Number:      num,
		GitHubURL:   BuildGitHubPRURL(repoDisplay, num),
		BackLink:    backLink,
		Comparisons: historyItems,
		TotalCount:  len(historyItems),
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmplPullDetail.Execute(w, view); err != nil {
		httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
	}
}

func sanitizeBranchLabel(val string) string {
	val = strings.TrimSpace(val)
	if val == "" {
		return ""
	}
	var sb strings.Builder
	for _, r := range val {
		switch {
		case r == '/':
			sb.WriteRune('_')
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.':
			sb.WriteRune(r)
		default:
			sb.WriteRune('_')
		}
	}
	return sb.String()
}

func runRepository(run *v1alpha1.Run) string {
	if run == nil {
		return ""
	}
	if run.Spec.Repo.URL != "" {
		cleaned := CleanRepo(run.Spec.Repo.URL)
		if cleaned != "" {
			return cleaned
		}
	}
	if labelVal, ok := run.Labels["evalrunner.gke-labs.dev/repo"]; ok && labelVal != "" {
		return strings.ReplaceAll(labelVal, "_", "/")
	}
	return ""
}

func runCommitTime(run *v1alpha1.Run) (time.Time, bool) {
	if run.Annotations != nil {
		if raw, ok := run.Annotations["evalrunner.gke-labs.dev/commit-time"]; ok && raw != "" {
			t, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				return run.CreationTimestamp.Time, true
			}
			return t, false
		}
	}
	return run.CreationTimestamp.Time, true
}

func (s *Server) handleSeriesList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ns, err := s.namespaceFor(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid namespace", "invalid namespace query param", err)
		return
	}

	cohortFilter := strings.TrimSpace(r.URL.Query().Get("cohort"))

	var runList v1alpha1.RunList
	if err := s.readerFor(ns).List(ctx, &runList, client.InNamespace(ns)); err != nil {
		httpError(w, http.StatusInternalServerError, "failed to list runs", "k8s list runs failed", err)
		return
	}

	type groupKey struct {
		repo   string
		branch string
	}
	counts := make(map[groupKey]int)
	repoHashes := make(map[string]string)

	for i := range runList.Items {
		run := &runList.Items[i]
		if !strings.HasPrefix(run.Name, "base-") {
			continue
		}
		branch := run.Labels["evalrunner.gke-labs.dev/branch"]
		if branch == "" {
			continue
		}
		if cohortFilter != "" && run.Spec.Cohort != cohortFilter {
			continue
		}

		repo := runRepository(run)
		if repo == "" {
			continue
		}

		gk := groupKey{repo: repo, branch: branch}
		counts[gk]++
		repoHashes[repo] = RepoHash(repo)
	}

	var seriesRows []SeriesListRow
	for gk, count := range counts {
		rHash := repoHashes[gk.repo]
		link := fmt.Sprintf("/series/%s/%s", url.PathEscape(rHash), url.PathEscape(gk.branch))
		q := url.Values{}
		if ns != s.cfg.DefaultNamespace {
			q.Set("namespace", ns)
		}
		if cohortFilter != "" {
			q.Set("cohort", cohortFilter)
		}
		if len(q) > 0 {
			link += "?" + q.Encode()
		}

		seriesRows = append(seriesRows, SeriesListRow{
			Repository: gk.repo,
			RepoHash:   rHash,
			Branch:     gk.branch,
			RunCount:   count,
			Link:       link,
		})
	}

	sort.Slice(seriesRows, func(i, j int) bool {
		if seriesRows[i].Repository != seriesRows[j].Repository {
			return seriesRows[i].Repository < seriesRows[j].Repository
		}
		return seriesRows[i].Branch < seriesRows[j].Branch
	})

	view := SeriesListView{
		ActiveTab:  NavTabSeries,
		Namespace:  ns,
		Cohort:     cohortFilter,
		Series:     seriesRows,
		TotalCount: len(seriesRows),
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := RenderSeriesList(w, view); err != nil {
		httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
	}
}

func (s *Server) handleSeriesDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ns, err := s.namespaceFor(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid namespace", "invalid namespace query param", err)
		return
	}

	repoOrHash := r.PathValue("repohash")
	if !isValidRepoHash(repoOrHash) {
		httpError(w, http.StatusBadRequest, "invalid repo hash", fmt.Sprintf("invalid repo hash: %q", repoOrHash), nil)
		return
	}

	branch := r.PathValue("branch")
	if branch == "" {
		httpError(w, http.StatusBadRequest, "invalid branch", "empty branch in path", nil)
		return
	}

	cohortFilter := strings.TrimSpace(r.URL.Query().Get("cohort"))

	n := 50
	if nStr := r.URL.Query().Get("n"); nStr != "" {
		if parsedN, err := strconv.Atoi(nStr); err == nil && parsedN > 0 {
			n = parsedN
		}
	}
	if n > 50 {
		n = 50
	}

	taskFilter := strings.TrimSpace(r.URL.Query().Get("task"))

	var runList v1alpha1.RunList
	if err := s.readerFor(ns).List(ctx, &runList, client.InNamespace(ns)); err != nil {
		httpError(w, http.StatusInternalServerError, "failed to list runs", "k8s list runs failed", err)
		return
	}

	type runWithTime struct {
		run        *v1alpha1.Run
		commitTime time.Time
		isFallback bool
	}

	var matched []runWithTime
	var repoDisplay string
	matchedRepoHash := repoOrHash

	for i := range runList.Items {
		run := &runList.Items[i]
		if !strings.HasPrefix(run.Name, "base-") {
			continue
		}
		runBranch := run.Labels["evalrunner.gke-labs.dev/branch"]
		if runBranch == "" {
			continue
		}
		if runBranch != branch && runBranch != sanitizeBranchLabel(branch) {
			continue
		}
		if cohortFilter != "" && run.Spec.Cohort != cohortFilter {
			continue
		}

		repo := runRepository(run)
		h := RepoHash(repo)
		if !strings.EqualFold(h, repoOrHash) && !strings.EqualFold(repo, repoOrHash) {
			continue
		}
		if repoDisplay == "" {
			repoDisplay = repo
			matchedRepoHash = h
		}

		cTime, fallback := runCommitTime(run)
		matched = append(matched, runWithTime{
			run:        run,
			commitTime: cTime,
			isFallback: fallback,
		})
	}

	if len(matched) == 0 {
		httpError(w, http.StatusNotFound, "series not found", fmt.Sprintf("no runs for repo %s branch %s in namespace %s", repoOrHash, branch, ns), nil)
		return
	}

	sort.Slice(matched, func(i, j int) bool {
		if !matched[i].commitTime.Equal(matched[j].commitTime) {
			return matched[i].commitTime.Before(matched[j].commitTime)
		}
		return matched[i].run.Name < matched[j].run.Name
	})

	totalRuns := len(matched)

	if len(matched) > n {
		matched = matched[len(matched)-n:]
	}

	orderedByCreationTime := false
	for _, m := range matched {
		if m.isFallback {
			orderedByCreationTime = true
			break
		}
	}
	var orderedNote string
	if orderedByCreationTime {
		orderedNote = "ordered by creation time"
	}

	taskSet := make(map[string]bool)
	for _, m := range matched {
		for _, t := range m.run.Status.Tasks {
			if t.ID != "" {
				taskSet[t.ID] = true
			}
		}
		for _, tid := range m.run.Spec.Tasks {
			if tid != "" {
				taskSet[tid] = true
			}
		}
	}

	var allTaskIDs []string
	for tid := range taskSet {
		allTaskIDs = append(allTaskIDs, tid)
	}
	sort.Strings(allTaskIDs)

	selectedTask := taskFilter
	if selectedTask == "" && len(allTaskIDs) > 0 {
		selectedTask = allTaskIDs[0]
	}

	var taskLinks []SeriesTaskLink
	for _, tid := range allTaskIDs {
		tParams := url.Values{}
		tParams.Set("task", tid)
		if n != 50 {
			tParams.Set("n", strconv.Itoa(n))
		}
		if ns != s.cfg.DefaultNamespace {
			tParams.Set("namespace", ns)
		}
		if cohortFilter != "" {
			tParams.Set("cohort", cohortFilter)
		}
		link := fmt.Sprintf("/series/%s/%s?%s", url.PathEscape(matchedRepoHash), url.PathEscape(branch), tParams.Encode())
		taskLinks = append(taskLinks, SeriesTaskLink{
			ID:     tid,
			Link:   link,
			Active: tid == selectedTask,
		})
	}

	var items []SeriesItem
	for _, m := range matched {
		run := m.run
		commit := run.Spec.Repo.Commit
		if commit == "" {
			commit = run.Labels["evalrunner.gke-labs.dev/head-sha"]
		}

		commitTimeStr := "—"
		if !m.commitTime.IsZero() {
			commitTimeStr = m.commitTime.UTC().Format(time.RFC3339)
		}

		var matchedTask *v1alpha1.TaskStatus
		for idx := range run.Status.Tasks {
			if run.Status.Tasks[idx].ID == selectedTask {
				matchedTask = &run.Status.Tasks[idx]
				break
			}
		}

		item := SeriesItem{
			CommitSHA:      commit,
			CommitSHAShort: ShortCommit(commit),
			CommitTime:     m.commitTime,
			CommitTimeStr:  commitTimeStr,
			RunName:        run.Name,
			Namespace:      run.Namespace,
			Samples:        "—",
			RateStr:        "—",
			CIStr:          "—",
			TokensStr:      "—",
			CostStr:        "—",
			RunLink:        fmt.Sprintf("/runs/%s/%s", run.Namespace, run.Name),
		}

		if matchedTask != nil && matchedTask.Phase == v1alpha1.TaskPhaseCompleted && matchedTask.SuccessRate != nil {
			item.HasPoint = true
			item.SuccessRate = matchedTask.SuccessRate
			item.CI = matchedTask.SuccessRateCI
			item.Phase = matchedTask.Phase
			item.PhaseChip = "good"
			item.Samples = strconv.Itoa(matchedTask.Samples)
			item.RateStr = fmt.Sprintf("%.2f", *matchedTask.SuccessRate)
			if matchedTask.SuccessRateCI != nil {
				item.CIStr = fmt.Sprintf("[%.2f, %.2f]", matchedTask.SuccessRateCI.Lower, matchedTask.SuccessRateCI.Upper)
			}
			if matchedTask.Tokens != nil {
				item.TokensStr = FormatTokens(matchedTask.Tokens.Total)
			}
			if matchedTask.CostUSD != nil {
				item.CostStr = fmt.Sprintf("$%.2f", *matchedTask.CostUSD)
			}
		} else {
			item.HasPoint = false
			if matchedTask != nil && matchedTask.Phase != "" {
				item.Phase = matchedTask.Phase
				item.PhaseChip = taskPhaseChipClass(matchedTask.Phase)
				if matchedTask.Samples > 0 {
					item.Samples = strconv.Itoa(matchedTask.Samples)
				}
				if matchedTask.Tokens != nil {
					item.TokensStr = FormatTokens(matchedTask.Tokens.Total)
				}
				if matchedTask.CostUSD != nil {
					item.CostStr = fmt.Sprintf("$%.2f", *matchedTask.CostUSD)
				}
			} else {
				item.Phase = v1alpha1.TaskPhase(run.Status.Phase)
				item.PhaseChip = phaseChipClass(run.Status.Phase)
			}
		}

		items = append(items, item)
	}

	svg := BuildSeriesSVG(items)

	backLink := "/series"
	bParams := url.Values{}
	if ns != s.cfg.DefaultNamespace {
		bParams.Set("namespace", ns)
	}
	if cohortFilter != "" {
		bParams.Set("cohort", cohortFilter)
	}
	if len(bParams) > 0 {
		backLink += "?" + bParams.Encode()
	}

	view := SeriesDetailView{
		ActiveTab:             NavTabSeries,
		Namespace:             ns,
		Repository:            repoDisplay,
		RepoHash:              matchedRepoHash,
		Branch:                branch,
		SelectedTask:          selectedTask,
		Tasks:                 taskLinks,
		N:                     n,
		Cohort:                cohortFilter,
		OrderedByCreationTime: orderedByCreationTime,
		OrderedNote:           orderedNote,
		SVG:                   svg,
		Items:                 items,
		TotalRuns:             totalRuns,
		BackLink:              backLink,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := RenderSeriesDetail(w, view); err != nil {
		httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
	}
}

func (s *Server) loadInspectSamplesFromStorage(ctx context.Context, logPath string) ([]artifacts.ParsedInspectSample, bool, error) {
	data, err := s.reloadableSink.Load(ctx, logPath)
	if err != nil {
		if strings.Contains(err.Error(), "exceeds maximum") {
			return nil, true, err
		}
		return nil, false, err
	}

	samples, parseErr := artifacts.ParseInspectSamples(data)
	if parseErr != nil {
		if s.cache != nil {
			s.cache.set(logPath, int64(len(data)), nil, parseErr)
		}
		return nil, true, parseErr
	}

	if s.cache != nil {
		s.cache.set(logPath, int64(len(data)), samples, nil)
	}
	return samples, false, nil
}

func (s *Server) loadInspectSamples(ctx context.Context, logPath string) ([]artifacts.ParsedInspectSample, bool, error) {
	if s.cache != nil {
		if samples, parseErr, ok := s.cache.get(logPath); ok {
			if parseErr != nil {
				return nil, true, parseErr
			}
			return samples, false, nil
		}
	}
	return s.loadInspectSamplesFromStorage(ctx, logPath)
}

func (s *Server) loadTaskSamplesList(ctx context.Context, logPath string) ([]artifacts.ParsedInspectSample, bool, error) {
	if s.cache != nil {
		if samples, parseErr, ok := s.cache.get(logPath); ok {
			if parseErr != nil {
				return nil, true, parseErr
			}
			return samples, false, nil
		}
	}

	// Fast path: try ReadTaskSummary before loading the full Inspect log.
	sink := s.ArtifactSink()
	if sink != nil {
		if summary, err := artifacts.ReadTaskSummary(ctx, sink, logPath); err == nil && summary != nil && len(summary.Samples) > 0 {
			samples := make([]artifacts.ParsedInspectSample, 0, len(summary.Samples))
			for _, item := range summary.Samples {
				scoreStr := "—"
				hasScore := false
				status := item.Status
				if item.Score != nil {
					hasScore = true
					scoreStr = fmt.Sprintf("%.2f", *item.Score)
					if status == "" {
						if *item.Score >= 1.0 {
							status = "pass"
						} else {
							status = "fail"
						}
					}
				}
				if status == "" {
					status = "unknown"
				}
				var inTokens, outTokens int
				if item.Tokens != nil {
					inTokens = item.Tokens.Input
					outTokens = item.Tokens.Output
				}
				samples = append(samples, artifacts.ParsedInspectSample{
					ID:           item.ID,
					Epoch:        item.Epoch,
					Score:        item.Score,
					HasScore:     hasScore,
					ScoreStr:     scoreStr,
					Status:       status,
					Latency:      item.LatencySeconds,
					InputTokens:  inTokens,
					OutputTokens: outTokens,
					TotalTokens:  inTokens + outTokens,
				})
			}
			if s.cache != nil {
				s.cache.set(logPath, int64(len(samples)*128), samples, nil)
			}
			return samples, false, nil
		}
	}

	return s.loadInspectSamplesFromStorage(ctx, logPath)
}

func hasComparisonPrefix(path string) bool {
	clean := strings.TrimSpace(path)
	if strings.HasPrefix(clean, "gs://") {
		trimmed := strings.TrimPrefix(clean, "gs://")
		parts := strings.SplitN(trimmed, "/", 2)
		return len(parts) == 2 && strings.HasPrefix(parts[1], "comparisons/")
	}
	if strings.HasPrefix(clean, "mem://") {
		trimmed := strings.TrimPrefix(clean, "mem://")
		return strings.HasPrefix(strings.TrimPrefix(trimmed, "/"), "comparisons/")
	}
	clean = filepath.ToSlash(filepath.Clean(clean))
	return strings.HasPrefix(clean, "comparisons/") || strings.Contains(clean, "/comparisons/")
}

func (s *Server) loadComparisonArtifact(ctx context.Context, summaryPath string, isCompleted bool) (*artifacts.ComparisonArtifact, bool, error) {
	if isCompleted && s.compCache != nil {
		if art, loadErr, ok := s.compCache.get(summaryPath); ok {
			if loadErr != nil {
				return nil, true, loadErr
			}
			return art, false, nil
		}
	}

	data, err := s.reloadableSink.Load(ctx, summaryPath)
	if err != nil {
		if strings.Contains(err.Error(), "exceeds maximum") {
			if isCompleted && s.compCache != nil {
				s.compCache.set(summaryPath, 0, nil, err)
			}
			return nil, true, err
		}
		return nil, false, err
	}

	var art artifacts.ComparisonArtifact
	if parseErr := json.Unmarshal(data, &art); parseErr != nil {
		if isCompleted && s.compCache != nil {
			s.compCache.set(summaryPath, int64(len(data)), nil, parseErr)
		}
		return nil, true, parseErr
	}

	if isCompleted && s.compCache != nil {
		s.compCache.set(summaryPath, int64(len(data)), &art, nil)
	}
	return &art, false, nil
}

func computeBatchBoundaries(batches []v1alpha1.ComparisonBatchStatus, totalPairs int) []int {
	if totalPairs < 2 {
		return nil
	}

	var rawBoundaries []int
	hasBatchWidths := false

	cumulative := 0
	for _, b := range batches {
		if b.SampleRange != nil && b.SampleRange.End >= b.SampleRange.Start {
			hasBatchWidths = true
			width := b.SampleRange.End - b.SampleRange.Start + 1
			cumulative += width
			if cumulative <= totalPairs {
				rawBoundaries = append(rawBoundaries, cumulative)
			} else {
				rawBoundaries = append(rawBoundaries, totalPairs)
				break
			}
		}
	}

	if hasBatchWidths {
		if len(rawBoundaries) > 0 && rawBoundaries[len(rawBoundaries)-1] < totalPairs {
			rawBoundaries = append(rawBoundaries, totalPairs)
		}
	} else {
		// Fall back to every 4 pairs when batches are absent
		for n := 4; n < totalPairs; n += 4 {
			rawBoundaries = append(rawBoundaries, n)
		}
		rawBoundaries = append(rawBoundaries, totalPairs)
	}

	// Filter for n >= 2 and strictly monotone
	var boundaries []int
	last := 0
	for _, n := range rawBoundaries {
		if n >= 2 && n <= totalPairs && n > last {
			boundaries = append(boundaries, n)
			last = n
		}
	}

	if len(boundaries) > 50 {
		boundaries = subsampleBoundaries(boundaries, 50)
	}

	return boundaries
}

func subsampleBoundaries(in []int, maxPoints int) []int {
	if len(in) <= maxPoints || maxPoints < 2 {
		return in
	}
	out := make([]int, maxPoints)
	out[0] = in[0]
	out[maxPoints-1] = in[len(in)-1]
	step := float64(len(in)-1) / float64(maxPoints-1)
	for i := 1; i < maxPoints-1; i++ {
		idx := int(math.Round(float64(i) * step))
		if idx <= 0 {
			idx = 1
		}
		if idx >= len(in)-1 {
			idx = len(in) - 2
		}
		out[i] = in[idx]
	}
	for i := 1; i < maxPoints; i++ {
		if out[i] <= out[i-1] {
			return in[:maxPoints]
		}
	}
	return out
}

func (s *Server) populateComparisonHistories(ctx context.Context, comp *v1alpha1.Comparison, view *ComparisonDetailView) {
	if !s.hasArtifactStorage() {
		view.HistoryNote = "artifact storage unavailable"
		return
	}
	if comp.Status.SummaryPath == "" {
		return
	}
	if !hasComparisonPrefix(comp.Status.SummaryPath) {
		view.HistoryNote = "history not available"
		return
	}

	isCompleted := comp.Status.Phase == v1alpha1.ComparisonPhaseCompleted
	art, parseOrSizeErr, err := s.loadComparisonArtifact(ctx, comp.Status.SummaryPath, isCompleted)
	if err != nil {
		if errors.Is(err, artifacts.ErrNotFound) || parseOrSizeErr {
			view.HistoryNote = "history not available"
		} else {
			view.HistoryNote = "artifact storage unavailable"
		}
		return
	}
	if art == nil {
		view.HistoryNote = "history not available"
		return
	}

	margin := stats.DefaultEquivalenceMargin
	if comp.Spec.EquivalenceMargin != nil && *comp.Spec.EquivalenceMargin > 0 {
		margin = *comp.Spec.EquivalenceMargin
	}

	sampleCap := 0
	if comp.Spec.Sampling != nil {
		sampleCap = comp.Spec.Sampling.EffectiveMaxSamplesPerTask()
	}

	taskIDs := make([]string, 0, len(comp.Status.Tasks))
	for _, t := range comp.Status.Tasks {
		taskIDs = append(taskIDs, t.ID)
	}
	if len(taskIDs) == 0 {
		for _, t := range art.Tasks {
			taskIDs = append(taskIDs, t.ID)
		}
	}

	var histories []ComparisonTaskHistory
	for _, tid := range taskIDs {
		var matchedArt *artifacts.ComparisonTaskArtifact
		for i := range art.Tasks {
			if art.Tasks[i].ID == tid {
				matchedArt = &art.Tasks[i]
				break
			}
		}
		if matchedArt == nil || len(matchedArt.PairsTable) < 2 {
			continue
		}

		boundaries := computeBatchBoundaries(comp.Status.Batches, len(matchedArt.PairsTable))
		if len(boundaries) == 0 {
			continue
		}

		points := stats.ComputePairedCISeries(matchedArt.PairsTable, boundaries, 0.95, stats.DefaultSeed, margin, sampleCap)
		if len(points) == 0 {
			continue
		}

		historyPoints := make([]ComparisonHistoryPoint, len(points))
		for i, pt := range points {
			historyPoints[i] = ComparisonHistoryPoint{
				N:              pt.N,
				DeltaFormatted: fmt.Sprintf("%+.2f", pt.Estimate.MeanDelta),
				CIFormatted:    fmt.Sprintf("[%+.2f, %+.2f]", pt.Estimate.CI.Lower, pt.Estimate.CI.Upper),
				CIWidth:        pt.CIWidth,
				Decision:       string(pt.Decision.Label),
				DecisionChip:   DecisionChipClass(string(pt.Decision.Label)),
			}
		}

		svg := BuildComparisonHistorySVG(historyPoints, margin)
		histories = append(histories, ComparisonTaskHistory{
			TaskID: matchedArt.ID,
			Points: historyPoints,
			SVG:    svg,
		})
	}

	if len(histories) > 0 {
		view.Histories = histories
	}
}

// FormatArtifactFooter formats the artifact sink status for the page footer.
// Returns "artifacts: gs://<bucket>", "artifacts: dir <path>", or "artifacts: none".
func FormatArtifactFooter(sink artifacts.ArtifactSink) string {
	if sink == nil {
		return "artifacts: none"
	}
	type currentGetter interface {
		Current() artifacts.ArtifactSink
	}
	if cg, ok := sink.(currentGetter); ok {
		sink = cg.Current()
		if sink == nil {
			return "artifacts: none"
		}
	}
	type bucketGetter interface {
		Bucket() string
	}
	if bg, ok := sink.(bucketGetter); ok {
		return fmt.Sprintf("artifacts: gs://%s", bg.Bucket())
	}
	if ld, ok := sink.(*artifacts.LocalDirSink); ok {
		return fmt.Sprintf("artifacts: dir %s", ld.BaseDir)
	}
	if _, ok := sink.(*artifacts.InMemorySink); ok {
		return "artifacts: memory"
	}
	prefix := sink.ArtifactPrefix("dummy")
	if strings.HasPrefix(prefix, "gs://") {
		trimmed := strings.TrimPrefix(prefix, "gs://")
		parts := strings.Split(trimmed, "/")
		if len(parts) > 0 && parts[0] != "" {
			return fmt.Sprintf("artifacts: gs://%s", parts[0])
		}
	}
	return "artifacts: none"
}

// parseRunTaskRest parses the {rest...} path segment and query parameters into taskID and sampleID.
// It splits rest at the last "/samples" segment into (task, sample?).
// Query parameters ?task=&sample= act as an explicit escape hatch.
func parseRunTaskRest(rest string, q url.Values, fragment, rawFragment string) (taskID string, sampleID string, ok bool) {
	qTask := q.Get("task")
	qSample := q.Get("sample")

	var parsedTask, parsedSample string
	var foundSamples bool

	search := "/samples"
	for i := len(rest) - len(search); i >= 0; i-- {
		if rest[i:i+len(search)] == search {
			after := i + len(search)
			if after == len(rest) || rest[after] == '/' {
				parsedTask = rest[:i]
				parsedSample = strings.TrimPrefix(rest[after:], "/")
				foundSamples = true
				break
			}
		}
	}

	if !foundSamples {
		if rest == "samples" || strings.HasPrefix(rest, "samples/") {
			foundSamples = true
			parsedTask = ""
			if len(rest) > len("samples") {
				parsedSample = strings.TrimPrefix(rest[len("samples"):], "/")
			}
		}
	}

	if qTask != "" {
		taskID = qTask
	} else if foundSamples {
		parsedTask = strings.TrimPrefix(parsedTask, "/")
		if u, err := url.PathUnescape(parsedTask); err == nil {
			taskID = u
		} else {
			taskID = parsedTask
		}
	}

	if qSample != "" {
		sampleID = qSample
	} else if foundSamples && parsedSample != "" {
		parsedSample = strings.TrimSuffix(parsedSample, "/")
		if u, err := url.PathUnescape(parsedSample); err == nil {
			sampleID = u
		} else {
			sampleID = parsedSample
		}
		if fragment != "" && !strings.Contains(sampleID, "#") {
			sampleID = sampleID + "#" + fragment
		} else if rawFragment != "" && !strings.Contains(sampleID, "#") {
			sampleID = sampleID + "#" + rawFragment
		}
	}

	if taskID == "" {
		return "", "", false
	}
	return taskID, sampleID, true
}

func (s *Server) handleRunTasks(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	rest := r.PathValue("rest")

	if !isValidNamespace(ns) || !isValidResourceName(name) {
		httpError(w, http.StatusBadRequest, "invalid route params", "", nil)
		return
	}

	taskID, sampleID, ok := parseRunTaskRest(rest, r.URL.Query(), r.URL.Fragment, r.URL.RawFragment)
	if !ok || taskID == "" {
		httpError(w, http.StatusNotFound, "not found", "", nil)
		return
	}

	ctx := r.Context()
	var run v1alpha1.Run
	if err := s.readerFor(ns).Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &run); err != nil {
		if apierrors.IsNotFound(err) {
			httpError(w, http.StatusNotFound, "run not found", "", nil)
			return
		}
		httpError(w, http.StatusInternalServerError, "error loading run", fmt.Sprintf("error loading run %s/%s", ns, name), err)
		return
	}

	var matchedTask *v1alpha1.TaskStatus
	for i := range run.Status.Tasks {
		if run.Status.Tasks[i].ID == taskID {
			matchedTask = &run.Status.Tasks[i]
			break
		}
	}

	if matchedTask == nil || matchedTask.LogPath == "" {
		httpError(w, http.StatusNotFound, "log path not found for task", "", nil)
		return
	}

	commit := run.Spec.Repo.Commit
	expectedPrefix := s.reloadableSink.ArtifactPrefix(commit)
	if expectedPrefix != "" && !strings.HasPrefix(matchedTask.LogPath, expectedPrefix) {
		httpError(w, http.StatusForbidden, "forbidden: log path does not match run commit prefix", "", nil)
		return
	}

	if !s.hasArtifactStorage() {
		log.Printf("artifact storage not configured for UI (namespace=%s, run=%s, task=%s)", ns, name, taskID)
		if sampleID == "" {
			view := TaskSamplesListView{
				ActiveTab:      NavTabRuns,
				Namespace:      ns,
				RunName:        name,
				TaskID:         taskID,
				EscapedTaskID:  url.PathEscape(taskID),
				Commit:         ShortCommit(commit),
				Model:          run.Spec.Model,
				LogPath:        matchedTask.LogPath,
				StorageMessage: "artifact storage is not configured for the UI; set artifactBucket or artifactDir in eval-runner-config",
				ArtifactFooter: FormatArtifactFooter(s.ArtifactSink()),
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			if err := tmplTaskSamplesList.Execute(w, view); err != nil {
				httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
			}
			return
		}
		view := SampleTranscriptView{
			ActiveTab:       NavTabRuns,
			Namespace:       ns,
			RunName:         name,
			TaskID:          taskID,
			EscapedTaskID:   url.PathEscape(taskID),
			SampleID:        sampleID,
			EscapedSampleID: url.PathEscape(sampleID),
			LogPath:         matchedTask.LogPath,
			StorageMessage:  "artifact storage is not configured for the UI; set artifactBucket or artifactDir in eval-runner-config",
			ArtifactFooter:  FormatArtifactFooter(s.ArtifactSink()),
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := tmplSampleTranscript.Execute(w, view); err != nil {
			httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
		}
		return
	}

	if sampleID == "" {
		s.renderTaskSamplesList(w, r, ns, name, taskID, commit, run.Spec.Model, matchedTask.LogPath)
		return
	}
	s.renderSampleTranscript(w, r, ns, name, taskID, sampleID, matchedTask.LogPath)
}

// Task samples list size guards.
const (
	pageSize              = 200
	maxSampleTooltipBytes = 512
)

func (s *Server) renderTaskSamplesList(w http.ResponseWriter, r *http.Request, ns, name, taskID, commit, model, logPath string) {
	ctx := r.Context()
	view := TaskSamplesListView{
		ActiveTab:      NavTabRuns,
		Namespace:      ns,
		RunName:        name,
		TaskID:         taskID,
		EscapedTaskID:  url.PathEscape(taskID),
		Commit:         ShortCommit(commit),
		Model:          model,
		LogPath:        logPath,
		ArtifactFooter: FormatArtifactFooter(s.ArtifactSink()),
	}

	samples, isTruncated, err := s.loadTaskSamplesList(ctx, logPath)
	if isTruncated {
		view.IsTruncatedLog = true
		view.TruncatedMessage = "The evaluation log exceeded the size limit or was truncated during storage. Sample details cannot be displayed."
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := tmplTaskSamplesList.Execute(w, view); err != nil {
			httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
		}
		return
	}
	if err != nil {
		if errors.Is(err, artifacts.ErrNotFound) {
			httpError(w, http.StatusNotFound, "task log not found on storage", "", nil)
			return
		}
		httpError(w, http.StatusBadGateway, "artifact storage unavailable; see UI logs", fmt.Sprintf("storage error loading artifact %s", logPath), err)
		return
	}

	totalSamples := len(samples)
	view.TotalSamples = totalSamples

	page := 1
	if pStr := r.URL.Query().Get("page"); pStr != "" {
		if p, err := strconv.Atoi(pStr); err == nil && p > 1 {
			page = p
		}
	}

	totalPages := 1
	if totalSamples > 0 {
		totalPages = int(math.Ceil(float64(totalSamples) / float64(pageSize)))
	}
	if page > totalPages {
		page = totalPages
	}

	view.Page = page
	view.PageSize = pageSize
	view.TotalPages = totalPages

	startIdx := (page - 1) * pageSize
	endIdx := startIdx + pageSize
	if endIdx > totalSamples {
		endIdx = totalSamples
	}
	if startIdx > endIdx {
		startIdx = endIdx
	}

	view.ShowingStart = startIdx + 1
	if totalSamples == 0 {
		view.ShowingStart = 0
	}
	view.ShowingEnd = endIdx

	baseLink := BuildTaskSamplesListLink(ns, name, taskID)
	if page > 1 {
		view.HasPrev = true
		view.PrevLink = fmt.Sprintf("%s?page=%d", baseLink, page-1)
	}
	if page < totalPages {
		view.HasNext = true
		view.NextLink = fmt.Sprintf("%s?page=%d", baseLink, page+1)
	}

	var sampleRows []TaskSampleRow
	for _, sRecord := range samples[startIdx:endIdx] {
		statusClass := "neutral"
		if sRecord.Status == "pass" {
			statusClass = "good"
		} else if sRecord.Status == "fail" {
			statusClass = "bad"
		}

		sampleRows = append(sampleRows, TaskSampleRow{
			ID:                 sRecord.ID,
			EscapedID:          url.PathEscape(sRecord.ID),
			Score:              sRecord.ScoreStr,
			Status:             sRecord.Status,
			StatusClass:        statusClass,
			InputPreview:       TruncateRunes(sRecord.Input, 200),
			OutputPreview:      TruncateRunes(sRecord.Output, 200),
			Target:             TruncateFieldBytes(sRecord.Target, maxSampleTooltipBytes),
			TargetPreview:      TruncateRunes(sRecord.Target, sampleNotePreviewRunes),
			Explanation:        TruncateFieldBytes(sRecord.Explanation, maxSampleTooltipBytes),
			ExplanationPreview: TruncateRunes(sRecord.Explanation, sampleNotePreviewRunes),
			LatencyStr:         FormatLatency(sRecord.Latency),
			TokensStr:          FormatTokens(sRecord.TotalTokens),
			Link:               BuildTaskSampleLink(ns, name, taskID, sRecord.ID),
		})
	}
	view.Samples = sampleRows

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmplTaskSamplesList.Execute(w, view); err != nil {
		httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
	}
}

// Transcript size guards.
const (
	maxEvents               = 500
	maxMessageBytes         = 16 * 1024
	maxTranscriptFieldBytes = 16 * 1024
)

func (s *Server) renderSampleTranscript(w http.ResponseWriter, r *http.Request, ns, name, taskID, sampleID, logPath string) {
	ctx := r.Context()
	view := SampleTranscriptView{
		ActiveTab:       NavTabRuns,
		Namespace:       ns,
		RunName:         name,
		TaskID:          taskID,
		EscapedTaskID:   url.PathEscape(taskID),
		SampleID:        sampleID,
		EscapedSampleID: url.PathEscape(sampleID),
		LogPath:         logPath,
		ArtifactFooter:  FormatArtifactFooter(s.ArtifactSink()),
	}

	samples, isTruncated, err := s.loadInspectSamples(ctx, logPath)
	if isTruncated {
		view.IsTruncatedLog = true
		view.TruncatedMessage = "The evaluation log exceeded the size limit or was truncated during storage. Transcript details cannot be displayed."
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := tmplSampleTranscript.Execute(w, view); err != nil {
			httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
		}
		return
	}
	if err != nil {
		if errors.Is(err, artifacts.ErrNotFound) {
			httpError(w, http.StatusNotFound, "task log not found on storage", "", nil)
			return
		}
		httpError(w, http.StatusBadGateway, "artifact storage unavailable; see UI logs", fmt.Sprintf("storage error loading artifact %s", logPath), err)
		return
	}

	var matchedSample *artifacts.ParsedInspectSample
	for i := range samples {
		if samples[i].ID == sampleID {
			matchedSample = &samples[i]
			break
		}
	}

	if matchedSample != nil && len(matchedSample.Transcript) == 0 {
		// If samples were loaded via the summary fast-path, load the full Inspect log for transcripts.
		fullSamples, fullTruncated, fullErr := s.loadInspectSamplesFromStorage(ctx, logPath)
		if fullErr == nil && !fullTruncated {
			samples = fullSamples
			matchedSample = nil
			for i := range samples {
				if samples[i].ID == sampleID {
					matchedSample = &samples[i]
					break
				}
			}
		}
	}

	if matchedSample == nil {
		httpError(w, http.StatusNotFound, "sample not found", "", nil)
		return
	}

	statusClass := "neutral"
	statusUpper := "UNKNOWN"
	if matchedSample.Status == "pass" {
		statusClass = "good"
		statusUpper = "PASS"
	} else if matchedSample.Status == "fail" {
		statusClass = "bad"
		statusUpper = "FAIL"
	}

	view.Score = matchedSample.ScoreStr
	view.Status = matchedSample.Status
	view.StatusUpper = statusUpper
	view.StatusClass = statusClass
	view.LatencyStr = FormatLatency(matchedSample.Latency)
	view.TokensStr = FormatTokens(matchedSample.TotalTokens)
	view.Target = TruncateFieldBytes(matchedSample.Target, maxTranscriptFieldBytes)
	view.Explanation = TruncateFieldBytes(matchedSample.Explanation, maxTranscriptFieldBytes)

	totalEvents := len(matchedSample.Transcript)
	view.TotalEvents = totalEvents

	rows := matchedSample.Transcript
	if len(rows) > maxEvents {
		rows = rows[:maxEvents]
		view.IsEventsTruncated = true
		view.EventsTruncatedMessage = fmt.Sprintf("[Transcript truncated: showing first %d of %d events]", maxEvents, totalEvents)
	}

	lastAssistantIdx := -1
	for i, r := range rows {
		if r.Kind == "assistant" {
			lastAssistantIdx = i
		}
	}

	var rowViews []TranscriptRowView
	for i, r := range rows {
		class := ""
		switch r.Kind {
		case "tool", "tool_call":
			class = "tool"
		case "assistant":
			if matchedSample.Status == "fail" && i == lastAssistantIdx {
				class = "bad"
			}
		}
		rowViews = append(rowViews, TranscriptRowView{
			Kind:    r.Kind,
			Class:   class,
			Content: TruncateMessageBytes(r.Content, maxMessageBytes),
		})
	}
	view.Rows = rowViews

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmplSampleTranscript.Execute(w, view); err != nil {
		httpError(w, http.StatusInternalServerError, "template render error", "template render error", err)
	}
}
