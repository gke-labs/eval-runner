<!--
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# API Reference: Run (v1alpha1)

**Group**: `evalrunner.gke-labs.dev`  
**Version**: `v1alpha1`  
**Kind**: `Run`  
**Scope**: Namespaced (runs reside in the `eval-runner` namespace)

> **Note**: `v1alpha1` is provisional and subject to change in future releases.

## Overview

A `Run` represents an evaluation run against a specific commit of a git repository. The eval-runner controller reconciles `Run` objects in two stages:
1. **Discovery**: Executes `.evals/list` discovery inside an untrusted, zero-privilege sandbox in the `eval-runner-sandbox` namespace (`mkdir -p /workspace/out && ./.evals/list > /workspace/out/manifest.json`). The controller pulls the file via sandboxd REST `ReadFile` (`/out/manifest.json`), stores the manifest in the artifact store under `runs/<commit>/manifest.json`, and records progress and pointers in `status.discovery` (`manifestPath`, `manifestHash`, `taskCount`, `eligibleTaskCount`, and up to 50 `taskIDs`). If `spec.model` is empty, the Run stops at `Discovered`.
2. **Sample Execution**: If `spec.model` is set, the controller loads the manifest by URI (with an in-memory cache, commit prefix confinement, and hash verification), executes eligible tasks from the manifest in isolated sandboxes in `eval-runner-sandbox` using Inspect, pulls out results, computes aggregate scores, and records per-task status in `status.tasks`.

## Example

```yaml
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: docs-comprehension-run
  namespace: eval-runner
spec:
  repo:
    url: https://github.com/gke-labs/eval-runner.git
    commit: abc123def456...
  model: google/gemini-3.5-flash-lite
  tasks:
    - docs/readme_comprehension
    - docs/readme_comprehension_blind
  limit: 20
status:
  phase: Completed
  message: ""
  conditions:
    - type: Discovered
      status: "True"
      reason: Discovered
      message: "Discovered 2 tasks"
  discovery:
    manifestPath: gs://my-eval-bucket/runs/abc123def456/manifest.json
    manifestHash: sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    taskCount: 2
    eligibleTaskCount: 2
    taskIDs:
      - docs/readme_comprehension
      - docs/readme_comprehension_blind
  tasks:
    - id: docs/readme_comprehension
      phase: Completed
      samples: 20
      successes: 17
      meanScore: 0.85
      successRate: 0.85
      successRateCI:
        lower: 0.64
        upper: 0.95
        confidence: 0.95
        method: wilson
      logPath: ""
    - id: docs/readme_comprehension_blind
      phase: Completed
      samples: 20
      successes: 5
      meanScore: 0.25
      successRate: 0.25
      successRateCI:
        lower: 0.11
        upper: 0.47
        confidence: 0.95
        method: wilson
      logPath: ""
  summary:
    tasks: "2/2"
    tasksCompleted: 2
    tasksFailed: 0
    tasksTotal: 2
    totalSamples: 40
    meanSuccessRate: 0.55
    successRateRange:
      min: 0.25
      max: 0.85
    rates: "docs/readme_comprehension=0.85 [0.64, 0.95], docs/readme_comprehension_blind=0.25 [0.11, 0.47]"
```

## Specification (`spec`)

| Field | Type | Description |
|---|---|---|
| `repo` | `RepoSpec` | Git repository and commit to evaluate. **Required.** |
| `repo.url` | `string` | The clone URL for the repository under test. **Required.** |
| `repo.commit` | `string` | The exact git commit SHA (7 to 64 hex characters) to check out. **Required.** |
| `model` | `string` | Inspect model identifier (e.g. `mockllm/model`, `google/gemini-3.5-flash-lite`). If empty, the Run stops after discovery. |
| `tasks` | `[]string` | Optional allow-list of task IDs to run. Default: all tasks declaring `resources: []`. |
| `limit` | `*int` | Optional per-task cap on samples (passed to Inspect as `--limit`) for cost and time control. Mutually exclusive with `sampleRange`. |
| `sampleRange` | `*SampleRange` | Optional 1-based inclusive window of samples to evaluate (`{start, end}`). Passed to Inspect as `--limit START-END`. Mutually exclusive with `limit`. |
| `sampleRange.start` | `int` | 1-based inclusive start index in the dataset order ($\ge 1$). |
| `sampleRange.end` | `int` | 1-based inclusive end index in the dataset order ($\ge \text{start}$). |
| `cohort` | `string` | Optional run-level cohort identifier grouping runs across suite expansions or configurations (e.g. set by webhook `defaultCohort`). Operates as a run-level filter (`?cohort=`) on `/series` and series detail pages. Distinct from task-level cohorts (`tasks[].cohort` in manifest and `status.tasks[].cohort` in Run status) which group tasks within a Run. |

## Status (`status`)

| Field | Type | Description |
|---|---|---|
| `phase` | `string` | The current lifecycle phase of the run. See [Phases](#phases). |
| `message` | `string` | Human-readable explanation or error details (including stderr tail on failure). |
| `conditions` | `[]metav1.Condition` | Standard Kubernetes condition list. Includes condition `Discovered`. |
| `manifest` | `*Manifest` | The parsed output of `.evals/list` upon successful discovery. |
| `discoveryDeadline` | `*metav1.Time` | Overall deadline by which discovery must complete. |
| `executionDeadline` | `*metav1.Time` | Overall deadline by which sample execution must complete. |
| `tasks` | `[]TaskStatus` | Per-task execution results and score aggregates. See [Tasks](#taskstatus). |
| `artifacts` | `string` | Storage base URI or prefix where artifacts for this Run are persisted (e.g. `gs://<bucket>/runs/<commit>/`). |
| `summary` | `*RunSummary` | Aggregate execution counts and success rate range across tasks. See [Summary](#runsummary). |
| `costUSD` | `*float64` | Total estimated evaluation cost in USD for this Run from model prices when known (omitted otherwise). |

### Phases

| Phase | Description |
|---|---|
| `Pending` | The Run has been created and is queued for discovery. |
| `Discovering` | A sandbox is running in `eval-runner-sandbox` performing discovery via git clone and `.evals/list`. |
| `Discovered` | Discovery succeeded. `status.manifest` contains the parsed tasks and the discovery sandbox has been cleaned up. (Terminal if `spec.model` is empty). |
| `Running` | Sample tasks are actively executing in parallel sandboxes in `eval-runner-sandbox`. |
| `Completed` | All sample tasks have finished execution and per-task aggregates are recorded in `status.tasks`. |
| `Failed` | Discovery or execution encountered a terminal error (e.g. invalid commit, non-zero command exit, invalid manifest, deadline exceeded). Details are in `status.message`. |

### TaskStatus (`status.tasks[]`)

| Field | Type | Description |
|---|---|---|
| `id` | `string` | Unique task identifier matching the manifest. |
| `phase` | `string` | Execution phase of the task (`Pending`, `Running`, `Completed`, `Failed`). |
| `startedAt` | `*metav1.Time` | Timestamp when the task transitioned to `Running`. |
| `samples` | `int` | Number of samples scored. |
| `successes` | `int` | Number of samples with score >= 1.0 (for binary scorers). |
| `meanScore` | `*float64` | Mean of per-sample scores. Numeric values are taken as-is (unclamped), 'C'/'CORRECT'/'YES'/'TRUE' map to 1.0, 'P'/'PARTIAL' map to 0.5, and 'I'/'INCORRECT'/'N'/'NOANSWER' map to 0.0. |
| `successRate` | `*float64` | Point estimate of the task success rate in [0, 1]. |
| `successRateCI` | `*ConfidenceInterval` | 95% confidence interval for success rate. See [ConfidenceInterval](#confidenceinterval) and [Statistics](statistics.md). |
| `successRateSD` | `*float64` | Sample standard deviation of scores (for non-binary scorers via `stats.Mean`). |
| `message` | `string` | Failure detail or bounded error information if the task failed. |
| `logPath` | `string` | Storage path or URI where the raw Inspect log was stored by the artifact sink. |
| `sampleRange` | `*SampleRange` | Sample window evaluated for this task (`{start, end}`). |
| `tokens` | `*TaskTokens` | Token usage breakdown consumed for this task when present in Inspect logs. |
| `latency` | `*TaskLatency` | Sample latency percentiles (`p50`, `p95` in seconds) aggregated across evaluated samples. |
| `costUSD` | `*float64` | Estimated task evaluation cost in USD from model prices when known (omitted otherwise). |
| `costSource` | `string` | Provenance of model prices used to compute `costUSD` (`configmap` or `default`). |
| `possibleSamples` | `int` | Total dataset size (possible samples) when known from the Inspect log (`eval.dataset.samples`). |
| `samplesDone` | `int` | Number of samples completed so far while the task is running. |
| `samplesPlanned` | `int` | Expected total number of samples to be evaluated for this task. |
| `lastProgressAt` | `*metav1.Time` | Timestamp when `samplesDone` was last updated. |
| `cohort` | `string` | Cohort identifier of the task (defaults to `"default"`). Written with the task execution result so that the UI can group tasks by cohort directly from Run status without reloading the stored manifest. |

### TaskLatency

| Field | Type | Description |
|---|---|---|
| `p50` | `float64` | 50th percentile sample latency in seconds. |
| `p95` | `float64` | 95th percentile sample latency in seconds. |

### ConfidenceInterval

| Field | Type | Description |
|---|---|---|
| `lower` | `float64` | Lower confidence bound (bounded in [0, 1]). |
| `upper` | `float64` | Upper confidence bound (bounded in [0, 1]). |
| `confidence` | `float64` | Confidence level (0.95 for 95% confidence). |
| `method` | `string` | Method used: `"wilson"` for binary scorers via `stats.Proportion`, or `"t"` for partial credit continuous scores via `stats.Mean`. |

### RunSummary (`status.summary`)

| Field | Type | Description |
|---|---|---|
| `tasks` | `string` | Compact completed vs total tasks string (e.g. `"2/2"`). Displayed in `kubectl get runs` under `TASKS`. |
| `tasksCompleted` | `int` | Number of tasks that completed successfully. |
| `tasksFailed` | `int` | Number of tasks that failed. |
| `tasksTotal` | `int` | Total number of tasks. |
| `totalSamples` | `int` | Sum of scored samples across all tasks. Displayed in `kubectl get runs` under `SAMPLES`. |
| `meanSuccessRate` | `*float64` | Simple arithmetic mean of task success rates across completed tasks. |
| `successRateRange` | `*ScoreRange` | Min and max task success rates across completed tasks (`{min, max}`). |
| `rates` | `string` | Compact per-task rates and confidence intervals `id=rate [lo,hi]` capped at 120 chars. Displayed in `kubectl get runs -o wide` under `RATES`. |

### Printer Columns

The `Run` CRD declares additional printer columns for `kubectl`:

- **Standard view (`kubectl get runs`)**:
  - `PHASE` (`.status.phase`): Current lifecycle phase.
  - `TASKS` (`.status.summary.tasks`): Tasks completed / total (e.g. `2/2`).
  - `SAMPLES` (`.status.summary.totalSamples`): Total samples evaluated across all tasks.
  - `AGE` (`.metadata.creationTimestamp`): Age of the custom resource.
- **Wide view (`kubectl get runs -o wide`)**:
  - `RATES` (`.status.summary.rates`): Compact string of per-task rates and 95% confidence intervals (e.g. `docs/readme_comprehension=1.00 [0.84, 1.00]`), capped in length with ellipsis.

### Discovery (`status.discovery`)

> **Breaking change in v1alpha1**: `status.manifest` has been removed from the `Run` status to prevent etcd bloat and unbounded custom resource sizes. Discovered manifests are stored durably in the artifact store under `runs/<commit>/manifest.json`, and progress, pointers, and counts are tracked in `status.discovery`.

| Field | Type | Description |
|---|---|---|
| `manifestPath` | `string` | Storage path or URI in the artifact store (e.g., `gs://bucket/runs/<commit>/manifest.json`, local path, or `mem://...`). |
| `manifestHash` | `string` | Checksum of the raw manifest JSON stored in the artifact store (`sha256:<hex>`). Verified on load to detect hash drift. |
| `taskCount` | `int` | Total number of tasks declared in the discovered manifest. |
| `eligibleTaskCount` | `int` | Number of tasks in the manifest eligible for execution (tasks with empty resources). |
| `taskIDs` | `[]string` | Initial slice of discovered task IDs, capped at 50 to bound serialized CRD size. Consumers requiring more than 50 tasks load the manifest by URI from `manifestPath`. |

### Stored Manifest Format (`runs/<commit>/manifest.json`)

The raw manifest generated by the repository's `.evals/list` discovery step is written to `/workspace/out/manifest.json` in the discovery sandbox, pulled by the controller, and persisted to the artifact store as `runs/<commit>/manifest.json`:

| Field | Type | Description |
|---|---|---|
| `images` | `[]ManifestImage` | Container images declared by the repository under test. |
| `images[].name` | `string` | Name and tag of the image. |
| `images[].context` | `string` | Build context directory within the repository. |
| `tasks` | `[]ManifestTask` | Discovered evaluation tasks. |
| `tasks[].id` | `string` | Stable unique task identifier (e.g., `triage/runbook_lookup`). |
| `tasks[].workdir` | `string` | Optional subdirectory from which to execute the task. |
| `tasks[].spec` | `string` | Framework-specific entrypoint specification (e.g. `file.py@task_fn`). |
| `tasks[].resources` | `[]ManifestResource` | Declarative resource requirements for the task. (Tasks with non-empty resources are marked `Failed` as unsupported in this release). |
| `tasks[].images` | `[]string` | Names of images required by this task. |
| `tasks[].paths` | `[]string` | Optional repository-relative glob patterns (e.g. `["docs/**", "evals/docs_*"]`) this task is sensitive to. Used for changed-file affinity in PR Comparisons. |
| `tasks[].cohort` | `string` | Optional task cohort identifier (e.g. `customer-reported-q4`). Validated to $\le 64$ characters, label-safe (`^[a-zA-Z0-9]([-_a-zA-Z0-9.]*[a-zA-Z0-9])?$`). Defaults to `"default"`. Groups tasks into cohorts for suite expansion tracking on `/series?view=cohorts`. |

## Stored Artifact Formats

Artifact sinks store evaluation logs and metadata according to the [functionality concept](concept/functionality.md §6). Each task run writes its evaluation log (an Inspect JSON log) and a companion `summary.json` metadata document under `runs/<commit>/<task>/summary.json` (or `runs/<namespace>/<runName>/<task>/summary.json` when commit is empty).

### Task Summary (`summary.json`)

`summary.json` provides a self-describing record of per-task statistics, provenance, and sample outcomes that can be recovered without access to the Kubernetes API or re-parsing the full Inspect log.

| Field | Type | Description |
|---|---|---|
| `schemaVersion` | `integer` | Schema version indicator (`1` for current self-describing summaries; legacy unversioned summaries omit this field). |
| `taskId` | `string` | Unique task identifier matching the manifest and Run status. |
| `model` | `string` | Model identifier evaluated (from `Run.spec.model`). |
| `commit` | `string` | Git commit SHA evaluated (from `Run.spec.repo.commit`). |
| `runName` | `string` | Name of the Kubernetes `Run` resource (omitted if empty). |
| `runNamespace` | `string` | Namespace of the Kubernetes `Run` resource (omitted if empty). |
| `controllerVersion` | `string` | Version or build identifier of the eval-runner controller (omitted if empty). |
| `scorer` | `string` | Primary scorer used to score samples, e.g. `accuracy`, `choice`, `includes` (omitted if empty). |
| `sampleCount` | `integer` | Total number of scored samples evaluated for this task (matching `Run.status.tasks[].samples`, denominator N for statistical estimates). Note that `sampleCount` counts scored samples only; `samples[]` may contain unscored samples as well. |
| `samples` | `[]object` | Per-sample outcome summaries, capped at 10,000 entries (omitted if empty). |
| `samples[].id` | `string` | Unique sample identifier formatted per eval-runner conventions (e.g. `"1"`, `"sample-1"`). |
| `samples[].epoch` | `integer` | Evaluation epoch index (omitted if 0). |
| `samples[].score` | `number` | Numeric sample score in [0, 1] (null/omitted if unscored). |
| `samples[].status` | `string` | Sample outcome: `"pass"` (score >= 1.0), `"fail"` (score < 1.0), or `"unknown"`. |
| `samples[].latencySeconds` | `number` | Sample execution duration in seconds (omitted if unknown). |
| `samples[].tokens` | `object` | Sample token usage (omitted if unknown). |
| `samples[].tokens.input` | `integer` | Number of input/prompt tokens consumed by this sample. |
| `samples[].tokens.output` | `integer` | Number of completion/output tokens generated by this sample. |
| `samplesTruncated` | `boolean` | True if the task evaluated more than 10,000 samples and `samples` was truncated to 10,000 entries (omitted if false). |
| `successes` | `integer` | Total number of passing samples (score >= 1.0). |
| `meanScore` | `number` | Arithmetic mean score across scored samples (null if no scored samples). |
| `successRate` | `number` | Point estimate of task success rate in [0, 1] (point proportion or mean score; omitted if uncomputed). |
| `successRateCI` | `object` | 95% confidence interval for success rate (omitted if uncomputed). |
| `successRateCI.lower` | `number` | Lower confidence bound in [0, 1]. |
| `successRateCI.upper` | `number` | Upper confidence bound in [0, 1]. |
| `successRateCI.confidence` | `number` | Confidence level (0.95). |
| `successRateCI.method` | `string` | Statistical method used: `"wilson"` for binary scores, `"t"` for continuous scores. |
| `successRateSD` | `number` | Sample standard deviation for continuous scorers (omitted for binary scorers). |
| `startedAt` | `string` | RFC 3339 timestamp when task execution started (omitted if empty). |
| `finishedAt` | `string` | RFC 3339 timestamp when task execution finished (omitted if empty). |
| `capped` | `boolean` | True if the raw Inspect log exceeded storage size limits and was capped. |
| `capBytes` | `integer` | Size limit in bytes when `capped` is true (omitted if uncapped). |
| `sampleRange` | `object` | 1-based inclusive sample range executed if configured on the Run (omitted if whole dataset). |
| `sampleRange.start` | `integer` | First sample index in range. |
| `sampleRange.end` | `integer` | Last sample index in range. |
| `tokens` | `object` | Aggregated task token usage breakdown (omitted if empty). |
| `tokens.input` | `integer` | Total input tokens consumed by the task. |
| `tokens.output` | `integer` | Total output tokens generated by the task. |
| `tokens.total` | `integer` | Total tokens (strictly equal to input + output). |
| `tokens.reasoning` | `integer` | Reasoning/thinking tokens included in output. |
| `tokens.cacheRead` | `integer` | Prompt tokens read from cache. |
| `tokens.cacheWrite` | `integer` | Prompt tokens written to cache. |
| `latency` | `object` | Aggregated sample latency percentiles (omitted if empty). |
| `latency.p50` | `number` | 50th percentile (median) sample execution duration in seconds. |
| `latency.p95` | `number` | 95th percentile sample execution duration in seconds. |
| `costUSD` | `number` | Estimated task evaluation cost in USD from model prices (omitted if empty). |
| `costSource` | `string` | Provenance of model pricing used to compute costUSD (`"configmap"` or `"default"`; omitted if empty). |
| `possibleSamples` | `integer` | Total dataset size (possible samples) when known from the Inspect log (omitted if empty). |

## Resource and Output Limits

Because `.evals/list` and `inspect eval` execute within untrusted sandboxes, the controller enforces strict size boundaries on exec streams, manifests, and status updates. This protects the controller against memory exhaustion and prevents rejected status writes to the Kubernetes API server (etcd enforces a ~1.5 MiB object limit).

Limits on exec streams and sandbox file reads are owned exclusively by `pkg/sandboxclient`. The reconciler interprets typed limit errors (`discovery.LimitError`) without duplicate checks. Oversized gRPC responses (including gRPC `ResourceExhausted` errors when a command exceeds message bounds) map directly to typed stream limit errors.

| Limit | Constant / Flag | Default | Behavior when Exceeded |
|---|---|---|---|
| Discovery stdout | `--max-stdout-bytes` | `1048576` (1 MiB) | Enforced by sandbox client; remaining output discarded; Run marked `Failed` with `stdout limit exceeded`. |
| Discovery stderr | `--max-stderr-bytes` | `65536` (64 KiB) | Enforced by sandbox client; remaining output discarded; Run marked `Failed` with `stderr limit exceeded`. |
| Sandbox file read | `DefaultReadFileLimit` | `10485760` (10 MiB) | Enforced by sandbox client REST `ReadFile` (`io.LimitReader`); fails with typed `file limit exceeded`. |
| Manifest file size | `MaxManifestBytes` | `1048576` (1 MiB) | Controller-side check on manifest bytes read from `/out/manifest.json`; fails Run if exceeded. |
| gRPC message receive headroom | `DefaultGRPCHeadroom` | `4194304` (4 MiB) | Headroom added to stream limits to configure gRPC `MaxCallRecvMsgSize`; oversized messages trigger `ResourceExhausted` which maps to typed `stdout limit exceeded`. |
| Manifest tasks | `MaxTasks` | `1000` | Validation fails, Run marked `Failed` with task count error. |
| Failure message length | `MaxMessageBytes` | `4096` | Truncated safely with `\n... [truncated]`. |
| Stderr tail lines | `MaxStderrLines` | `10` | Only the most recent lines of stderr are retained. |
| Stderr tail bytes | `MaxStderrTailBytes` | `3072` | Stderr tail is capped before inclusion in failure message. |
| Discovery deadline | `--discovery-deadline` / ConfigMap `discoveryDeadline` | `10m` | Overall discovery deadline exceeded; Run marked `Failed` and sandbox deleted. |
| Execution deadline | `--execution-deadline` / ConfigMap `executionDeadline` | `30m` | Overall sample execution deadline exceeded; Run marked `Failed` and sandboxes deleted. |
| Max parallel tasks | `--max-parallel-tasks` / ConfigMap `maxParallelTasks` | `4` | Caps concurrent task sandboxes executing at one time. |
| Poll progress interval | `--poll-progress-interval` / ConfigMap `pollProgressInterval` | `10s` | Interval between task progress polling attempts and per-task write floor (minimum `1s`). |
| Model credentials | `--model-secret` / ConfigMap `modelSecret` | `"model-credentials"` | Names a Secret in `eval-runner-sandbox` whose keys are injected into sample sandboxes (optional by presence; pass `""` or `"none"` to disable). |
| Model pricing | ConfigMap `modelPrices` | Built-in defaults | JSON map model -> `{inputPer1M, outputPer1M}` in USD for computing task and comparison evaluation costs. Built-in defaults ship for `google/gemini-3.5-flash-lite`, `google/gemini-2.5-flash`, `google/gemini-2.5-pro`, and `mockllm/model` (0). ConfigMap entries override or extend defaults (merge, not replace); sentinel `"none"` disables defaults entirely. An existing `modelPrices: "{}"` (the previously documented default) now yields the built-in defaults rather than omitting cost; pass `"none"` to disable model pricing entirely. Supports exact string match on `Run.spec.model` or `"prefix:<model-prefix>"` for family-wide pricing. ConfigMap entries (exact first, then longest `prefix:`) take precedence over built-in defaults; within a source, exact beats prefix. Unknown JSON fields and negative prices rejected. |
| Default batch size | `--default-batch-size` / ConfigMap `defaultBatchSize` | `8` | Default batch size for paired adaptive sampling in webhook-created Comparisons and seed Run sampleRange. |
| Default max samples | `--default-max-samples-per-task` / ConfigMap `defaultMaxSamplesPerTask` | `64` | Maximum total samples per task across batches in webhook-created Comparisons; bounds `/eval more <n>`. |
| Command users | `--command-users` / ConfigMap `commandUsers` | `""` | Comma-separated list of GitHub logins allowed to execute `/eval more <n>` PR comment commands without checking GitHub permissions. |
| Strict configuration | `--strict-config` | `false` | When true, fails controller startup immediately if `eval-runner-config` contains unrecognised keys. |
| Sample runner image | `--sample-runner-image` | `""` (derived from controller pod image) | Container image used for sample task sandboxes (derived from controller pod image when unset). |
| Artifact bucket | `--artifact-bucket` / ConfigMap `artifactBucket` | `""` (in-memory sink) | GCS bucket URI (`gs://<bucket>`) or name where manifests, Inspect logs, and `summary.json` are stored. |
| Artifact directory | `--artifact-dir` | `""` (in-memory sink) | Local filesystem directory for storing manifests, evaluation logs, and comparison artifacts when GCS is not configured. |
| Artifact upload size | `--artifact-max-upload-bytes` / ConfigMap `artifactMaxUploadBytes` | `10485760` (10 MiB) | Maximum size of an uploaded artifact (default 10 MiB, equal to sandbox ReadFile limit); manifests error above cap; task logs are capped and recorded with `capped` in `summary.json`. |
| Artifact load size | `DefaultMaxLoadBytes` | `10485760` (10 MiB) | Maximum size of an artifact loaded from the sink. |
| Task path patterns | `MaxTaskPaths` | `100` | Maximum number of path glob patterns allowed per task in a discovered manifest. |
| Task path pattern length | `MaxPathLength` | `1024` | Maximum character length of a single path glob pattern. |
| Task cohort length | `MaxCohortLength` | `64` | Maximum character length of a cohort identifier; must be label-safe. |
| Changed files cap | `MaxPullRequestFiles` | `300` | Maximum changed files retrieved from GitHub; exceeding cap treats all tasks declaring paths as matched. |
| Affinity mode | `--affinity-mode` / ConfigMap `affinityMode` | `"order"` | Changed-file task affinity scheduling mode: `"order"` (matched/always tasks scheduled first, unmatched second) or `"prune"` (unmatched tasks dropped from batches and reported as skipped). |
| Comparison TTL | `--comparison-ttl` / ConfigMap `comparisonTTL` | `0` (keep indefinitely) | Duration after which `Completed` Comparisons are deleted by the controller as a safety net; defaults to `0` (keep indefinitely). Governs retention of Comparisons on PR close. Owned batch Runs are deleted via Kubernetes owner references. Baseline Runs are preserved. |
| ls-remote interval | ConfigMap `lsRemoteInterval` | `60s` | Interval between `git ls-remote` reference advertisement checks per repository; randomized with ±20% jitter. |
| Full reconcile interval | ConfigMap `fullReconcileInterval` | `1h` | Cadence for full reconciliation of branches and change requests using HTTP conditional headers. |
| Rate limit floor percent | ConfigMap `rateLimitFloorPercent` | `10` | Minimum percentage of GitHub rate limit quota remaining before REST API calls are suppressed until reset. |

---

# Comparison (`evalrunner.gke-labs.dev/v1alpha1`)

The `Comparison` custom resource specifies and reports a paired statistical evaluation between two `Run` resources (baseline and candidate) evaluated on identical sample inputs. The controller loads per-sample Inspect logs from the artifact sink, matches samples by sample ID, computes paired differences and confidence intervals, assigns task-level decision labels, and produces a run-level verdict with $P(\text{improvement})$.

## Example

```yaml
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Comparison
metadata:
  name: pr-418-eval
  namespace: eval-runner
spec:
  baseline:
    run: run-main-a81d0c4
  candidate:
    run: run-pr-418-3f9c2e1
  equivalenceMargin: 0.05
  tasks:
  - triage/runbook_lookup
  - triage/escalation_policy
```

## Spec (`spec`)

| Field | Type | Description |
|---|---|---|
| `baseline` | `ComparisonRunRef` | **Required.** Reference to the baseline `Run` resource in the same namespace (`spec.baseline.run`). |
| `candidate` | `ComparisonRunRef` | **Required.** Reference to the candidate (e.g. PR) `Run` resource in the same namespace (`spec.candidate.run`). |
| `equivalenceMargin` | `*float64` | Statistical equivalence margin $\delta$ (default: `0.05` for $\pm 0.05$). |
| `sampling` | `*ComparisonSamplingSpec` | Optional. Configures adaptive paired batch sampling driven by the stopping rule. When omitted, only the single pair of referenced runs is compared. |
| `sampling.batchSize` | `int` | Number of samples to evaluate in each subsequent batch (default: `8`). |
| `sampling.maxSamplesPerTask` | `int` | Maximum total number of samples evaluated per task across all batches (default: `64`). Clamps the stopping rule's sample range window; once reached without resolving, undecided tasks are marked `inconclusive`. Note that explicit `extraSamples` requests are not bounded by this cap. |
| `budgetUSD` | `*float64` | Optional maximum total evaluation cost in USD for this Comparison across all tasks and batches (minimum 0; 0 = unset / unlimited). Webhook default via ConfigMap key `defaultBudgetUSD`. When spent reaches budget, undecided tasks stop sampling with decision `inconclusive` and the budget reason. Prior to spawning each batch, window shrinks to fit remaining budget (min 1) or stops early. Explicit `extraSamples` requests are exempt from this limit. |
| `extraSamples` | `int` | Optional. Additional samples requested beyond the stopping rule per undecided task (or all tasks if decided) (default: `0`). Evaluates beyond `sampling.maxSamplesPerTask` (explicit user requests are not bounded by the sample cap); bumping triggers more. In PR workflows, review comments of the form `/eval more <n>` increment this value. |
| `tasks` | `[]string` | Optional allow-list of task IDs to evaluate. When omitted, all tasks present in both runs are compared. |

## Status (`status`)

| Field | Type | Description |
|---|---|---|
| `phase` | `ComparisonPhase` | High-level comparison lifecycle phase: `Running`, `Completed`. |
| `completionTime` | `*metav1.Time` | Timestamp when the comparison reached the `Completed` phase; cleared if a subsequent `extraSamples` bump moves the comparison back to `Running`. Used to measure `comparisonTTL` retention. |
| `verdict` | `ComparisonVerdict` | Aggregate run-level verdict: `IMPROVED`, `REGRESSED`, `UNCHANGED`, `INCONCLUSIVE`, `RUNNING`. |
| `pImprovement` | `*float64` | Posterior probability that the candidate is a net improvement across evaluated tasks (range `[0, 1]`). |
| `batches` | `[]ComparisonBatchStatus` | Records execution state of each paired batch of Runs (`index`, `sampleRange`, `candidateRun`, `baselineRun`, `phase`). |
| `extraSamplesApplied` | `int` | Count of extra samples scheduled into extension batches (consumed request count). |
| `tasks` | `[]ComparisonTaskStatus` | Per-task paired comparison statistics and labels. |
| `tasks[].id` | `string` | Unique task identifier (e.g. `triage/runbook_lookup`). |
| `tasks[].baseRate` | `*float64` | Mean score or success rate on baseline samples. |
| `tasks[].candidateRate` | `*float64` | Mean score or success rate on candidate samples. |
| `tasks[].delta` | `*float64` | Mean paired difference ($\text{candidate} - \text{baseline}$). |
| `tasks[].deltaCI` | `*DeltaConfidenceInterval` | 95% confidence interval on the delta (`lower`, `upper`). |
| `tasks[].pImprovement` | `*float64` | Posterior probability that candidate is better than baseline for this task (range `[0, 1]`). |
| `tasks[].pairs` | `int` | Number of matched sample pairs evaluated. |
| `tasks[].wins` | `int` | Number of pairs where candidate scored higher than baseline. |
| `tasks[].losses` | `int` | Number of pairs where candidate scored lower than baseline. |
| `tasks[].ties` | `int` | Number of pairs where candidate and baseline scored equally. |
| `tasks[].dropped` | `ComparisonDropped` | Samples dropped without matches (`candidate`, `baseline`). |
| `tasks[].label` | `ComparisonTaskLabel` | Decision outcome: `improved`, `regressed`, `unchanged`, `inconclusive`, `running`. |
| `tasks[].samplesRun` | `int` | Total number of samples evaluated for this task across all batches. |
| `tasks[].decision` | `ComparisonTaskLabel` | Stopping rule decision: `improved`, `regressed`, `unchanged`, `inconclusive`, `continue`. |
| `tasks[].suggestedNext` | `int` | Suggested number of additional samples to evaluate if undecided (0 when dataset is exhausted). |
| `tasks[].tokens` | `*TaskTokens` | Token counts (`input`, `output`, `total`) consumed across evaluated task runs when present in Inspect logs. |
| `tasks[].latency` | `*TaskLatency` | Sample latency percentiles (`p50`, `p95` in seconds) aggregated across evaluated samples. |
| `tasks[].costUSD` | `*float64` | Estimated task evaluation cost in USD from model prices across candidate and baseline runs when known. |
| `tasks[].costSource` | `string` | Provenance of model prices used to compute `costUSD` (`configmap` or `default`). |
| `tasks[].possibleSamples` | `int` | Total dataset size (possible samples) for this task when known from the Inspect log. |
| `tasks[].affinity` | `string` | Changed-file task affinity: `always` (no paths declared), `matched` (path globs matched changed files), or `unmatched` (globs declared but none matched). |
| `costUSD` | `*float64` | Total estimated evaluation cost in USD across all tasks in this Comparison when prices are known. |
| `possibleSamples` | `int` | Total possible samples across tasks when known from Inspect logs. |
| `summary` | `*ComparisonSummary` | Aggregate statistics across tasks in the Comparison. |
| `summary.tasks` | `string` | Compact string representing decided vs planned tasks (e.g. `2/2`, where planned excludes pruned tasks). |
| `summary.skipped` | `int` | Count of tasks discovered in the manifest but not planned by either Run, or pruned by affinityMode=prune. |
| `summary.affinityMatched` | `int` | Count of tasks with affinity `matched`. |
| `affinity` | `*ComparisonAffinityStatus` | Metadata of changed-file PR affinity resolution (`filesHash`, `filesCount`, `truncated`). |
| `budget` | `*ComparisonBudgetStatus` | Budget spend and enforcement status against `spec.budgetUSD` (`limitUSD`, `spentUSD`, `enforced`, `note`). |
| `summaryPath` | `string` | Storage path or URI where the complete paired table was stored (`comparisons/<name>.json`). |
| `message` | `string` | Human-readable explanation of the aggregate verdict or failure. In addition to the aggregate verdict summary, the controller emits policy notes: `missing batch run: <ids>` (or `runs`, batch Runs not found), `failed batch run: <ids>` (or `runs`, batch Runs that failed or whose task logs did not match the artifact sink's commit prefix), `candidate task failed: <ids>` (caps UNCHANGED or IMPROVED verdicts to INCONCLUSIVE with $P(\text{improvement}) \le 0.50$), `failed in both runs: <ids>` (tasks that failed in both runs), `tasks not executed: <ids>` (tasks in scope executed by neither run), `tasks not executed by both runs: <ids>` (tasks executed by only one run), `skipped (not planned): <ids>` (tasks discovered in the manifest but not planned by either run), and `skipped (unmatched): <ids>` (tasks whose declared paths did not match PR changed files when affinityMode=prune). |
| `conditions` | `[]metav1.Condition` | Standard Kubernetes condition list. Includes conditions `ExtraSamples` (reasons: `ArtifactPrefixMismatch`, `ArtifactNotFound`, `NoEligibleTasks`, `Scheduled`) and `ArtifactsReadable` (reasons: `PrefixMismatch`, `NotFound`). |
| `reporting` | `*ComparisonReportingStatus` | GitHub PR comment and Check Run reporting state (`commentID`, `checkRunIDs`, `checkRunID`, `lastPostedHash`, `message`). |

### TaskTokens (`status.tasks[].tokens`)

`TaskTokens` records token usage breakdown for an evaluation task. Across all resources (`Run.status.tasks[].tokens`, `Comparison.status.tasks[].tokens`), summary files (`summary.json`), and comparison artifacts (`comparisons/<name>.json`), eval-runner enforces the token accounting invariant:

$$\text{total} = \text{input} + \text{output}$$

Inspect log provider-specific totals are dropped so that `total` is strictly and deterministically reproducible from the exposed components. Reasoning tokens (`reasoning`) and prompt cache token counts (`cacheRead`, `cacheWrite`) are recorded as separate optional fields and are not folded into `total` because token billing differs per provider (e.g. Gemini and Anthropic bill reasoning as output tokens; cost mapping is configured via `modelPrices`).

| Field | Type | Description |
|---|---|---|
| `input` | `int` | Input/prompt tokens consumed by models for this task. |
| `output` | `int` | Output/completion tokens consumed by models for this task. |
| `total` | `int` | Total tokens consumed for this task (`input + output`). |
| `reasoning` | `int` | Reasoning/thinking tokens consumed by models for this task (reported separately). |
| `cacheRead` | `int` | Cached input tokens read by models for this task. |
| `cacheWrite` | `int` | Tokens written to cache by models for this task. |

### ComparisonAffinityStatus (`status.affinity`)

| Field | Type | Description |
|---|---|---|
| `filesHash` | `string` | SHA-256 checksum of the sorted changed files list (`sha256:<hex>`). |
| `filesCount` | `int` | Count of changed files retrieved from GitHub (capped at 300). |
| `truncated` | `bool` | True if changed files exceeded the cap (300 files), causing all tasks with paths to be treated as matched. |
| `fallbackReason` | `string` | Reason why changed-file affinity fell back to order mode with all tasks always (e.g. GitHub API error or permissions). |

### ComparisonBudgetStatus (`status.budget`)

| Field | Type | Description |
|---|---|---|
| `limitUSD` | `*float64` | Configured budget limit in USD from `spec.budgetUSD`. |
| `spentUSD` | `*float64` | Total spend in USD across all batches and tasks so far. |
| `enforced` | `bool` | Whether the budget was enforced. Set to `false` if any evaluated model lacks a known price. |
| `note` | `string` | Explanation if budget could not be enforced (e.g. `"cost unknown for <model>; budget not enforced"`). |

### ComparisonSummary (`status.summary`)

| Field | Type | Description |
|---|---|---|
| `tasks` | `string` | Compact string representing decided vs planned tasks (e.g. `"2/2"`, where planned excludes pruned tasks). |
| `skipped` | `int` | Count of tasks discovered in the manifest but not planned by either Run, or pruned by affinityMode=prune. |
| `affinityMatched` | `int` | Count of tasks with affinity `"matched"`. |

### ComparisonReportingStatus (`status.reporting`)

| Field | Type | Description |
|---|---|---|
| `commentID` | `int64` | GitHub PR comment ID updated in-place for this comparison. |
| `checkRunIDs` | `map[string]int64` | GitHub check run IDs keyed by commit SHA created and updated for this comparison. |
| `checkRunID` | `int64` | Legacy GitHub check run ID created and updated for this comparison (deprecated). |
| `lastPostedHash` | `string` | Idempotency SHA-256 hash across markdown comment body and check run state. |
| `message` | `string` | Informational status message (e.g. `"disabled"`, permanent error message). |
| `attempts` | `int` | Number of times the controller has attempted to report to GitHub for the current update. |
| `nextAttemptAt` | `metav1.Time` | Earliest time for the next reporting retry attempt. |

Check run titles display per-label counts: `<VERDICT>: N improved, M unchanged, K regressed, J inconclusive` built from `status.tasks[].label` (omitting zero buckets). The check run conclusion for `INCONCLUSIVE` comparisons is governed by the `inconclusivePolicy` cluster ConfigMap key: `"neutral"` (default, non-blocking) or `"failure"` (blocks merging).

## Series Labels

Tools that compare a series of candidate Runs can label each Comparison with the kind of baseline it uses. The controller ignores these labels; the web UI reads them to group Comparisons on `/comparisons`.

| Label | Value | Meaning |
|---|---|---|
| `evalrunner.gke-labs.dev/vs-baseline` | `"true"` | The baseline Run is the fixed reference Run for the series. |
| `evalrunner.gke-labs.dev/vs-previous` | `"true"` | The baseline Run is the Run that preceded the candidate in the series. |

A Comparison may carry both labels, for example when the previous Run is also the reference Run. The UI shows one row per `spec.candidate.run` with the newest Comparison of each kind; Comparisons with neither label are listed individually.

## Webhook Resource Naming

Comparisons, candidate Runs, and baseline Runs created automatically by `eval-runner-webhook` follow deterministic naming conventions based on the configured `webhookRepositories`:

- **Single repository**: `pr-<number>` for Comparisons, `pr-<number>-<headSHA7>` for candidate Runs, and `base-<sha7>` for baseline Runs.
- **Multiple repositories or wildcard (`*`)**: `pr-<repoHash>-<number>` for Comparisons, `pr-<repoHash>-<number>-<headSHA7>` for candidate Runs, and `base-<repoHash>-<sha7>` for baseline Runs, where `<repoHash>` is a 7-character hexadecimal SHA-256 hash of the normalized repository name (`owner/repo`). This guarantees RFC 1123 DNS subdomain compliance and prevents collisions when multiple allow-listed repositories share the same pull request number or commit SHA.
- **Branch and Commit-Time Tracking**: Baseline Runs created on pushes to the default branch carry the label `evalrunner.gke-labs.dev/branch=<defaultBranch>` and the annotation `evalrunner.gke-labs.dev/commit-time=<RFC3339>`. The web UI uses these to power the main-branch series page (`/series/{repohash}/{branch}`), ordering commits chronologically by commit-time (falling back to `creationTimestamp` with a visible note if absent). Pre-existing Runs lacking the branch label are excluded.
- **Label-based resolution**: Webhook lookups (synchronization, `/eval more` comment commands, and PR closure cleanup) resolve objects by labels (`evalrunner.gke-labs.dev/pr` and `evalrunner.gke-labs.dev/repo`), preserving compatibility with existing single-repo or pre-upgrade objects. Baseline reuse keys on repository URL, commit, model, tasks, and window via labels, never on name alone.

## Retention and Cleanup

Eval-runner provides two complementary mechanisms to manage the lifecycle of Comparisons and Runs:

### 1. Webhook PR Close Handling
When a pull request closes (merged or closed without merge), GitHub sends a `pull_request` event with `action: closed`. The forge consumer / webhook handler retains the pull request's Comparisons and referenced candidate/baseline Runs to preserve evaluation history:
- **Comparisons**: Retained and labeled `evalrunner.gke-labs.dev/pr-state: merged` (when merged) or `evalrunner.gke-labs.dev/pr-state: closed` (when closed without merge).
- **Candidate and Baseline Runs**: Any Run referenced by a retained Comparison as candidate or baseline is preserved.
- **Orphaned Seed Runs**: Orphaned PR-labelled seed Runs (not referenced by any retained Comparison or carrying an owner reference to a surviving Comparison) may still be deleted on close.
- **Baseline Runs**: Never touched. Baseline Runs are keyed to default branch commits (`base-<sha7>` or `base-<repoHash>-<sha7>`), are not tagged with PR labels, and are reused across pull requests to eliminate redundant evaluation work.
- **Reopen**: If a closed PR is reopened, existing Comparisons have their `evalrunner.gke-labs.dev/pr-state` label updated back to `open`.
- **Artifacts**: Artifacts in the artifact sink (GCS or local directory) are not purged by PR closure or TTL; artifact retention is governed independently by storage lifecycle policies.

### 2. Comparison TTL (`comparisonTTL`)
Comparison retention is governed by the existing `comparisonTTL` setting (in the ConfigMap or via `--comparison-ttl` CLI flag, duration, default `0` = keep indefinitely). When configured (> 0):
- The controller automatically deletes `Completed` Comparisons whose age exceeds the TTL. Age is measured from completion (`status.completionTime`), falling back to creation (`metadata.creationTimestamp`) only for objects that predate the field.
- Owned batch Runs are garbage-collected via owner references.
- Baseline Runs are preserved.
- Active Comparisons (`Running` phase, or `Completed` comparisons with pending `extraSamples` requested) are not deleted until complete.
- Artifacts in the artifact sink (GCS or local directory) are not purged.

## Printer Columns

`kubectl get comparisons` displays:

```text
NAME          VERDICT     P(IMP)   TASKS   BASELINE             CANDIDATE             COMPLETED   AGE
pr-418-eval   IMPROVED    0.98     2/2     run-main-a81d0c4     run-pr-418-3f9c2e1    1m          2m
```


