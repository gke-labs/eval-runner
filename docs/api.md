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
| `message` | `string` | Failure detail or bounded error information if the task failed. |
| `logPath` | `string` | Storage path or URI where the raw Inspect log was stored by the artifact sink. |
| `sampleRange` | `*SampleRange` | Sample window evaluated for this task (`{start, end}`). |

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
| Model credentials | `--model-secret` / ConfigMap `modelSecret` | `"model-credentials"` | Names a Secret in `eval-runner-sandbox` whose keys are injected into sample sandboxes (optional by presence; pass `""` or `"none"` to disable). |
| Strict configuration | `--strict-config` | `false` | When true, fails controller startup immediately if `eval-runner-config` contains unrecognised keys. |
| Sample runner image | `--sample-runner-image` | `""` (derived from controller pod image) | Container image used for sample task sandboxes (derived from controller pod image when unset). |
| Artifact bucket | `--artifact-bucket` / ConfigMap `artifactBucket` | `""` (in-memory sink) | GCS bucket URI (`gs://<bucket>`) or name where manifests, Inspect logs, and `summary.json` are stored. |
| Artifact directory | `--artifact-dir` | `""` (in-memory sink) | Local filesystem directory for storing manifests, evaluation logs, and comparison artifacts when GCS is not configured. |
| Artifact upload size | `--artifact-max-upload-bytes` / ConfigMap `artifactMaxUploadBytes` | `10485760` (10 MiB) | Maximum size of an uploaded artifact (default 10 MiB, equal to sandbox ReadFile limit); manifests error above cap; task logs are capped and recorded with `capped` in `summary.json`. |
| Artifact load size | `DefaultMaxLoadBytes` | `10485760` (10 MiB) | Maximum size of an artifact loaded from the sink. |

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
| `sampling.maxSamplesPerTask` | `int` | Maximum total number of samples evaluated per task across all batches (default: `64`). |
| `tasks` | `[]string` | Optional allow-list of task IDs to evaluate. When omitted, all tasks present in both runs are compared. |

## Status (`status`)

| Field | Type | Description |
|---|---|---|
| `phase` | `ComparisonPhase` | High-level comparison lifecycle phase: `Running`, `Completed`. |
| `verdict` | `ComparisonVerdict` | Aggregate run-level verdict: `IMPROVED`, `REGRESSED`, `UNCHANGED`, `INCONCLUSIVE`, `RUNNING`. |
| `pImprovement` | `*float64` | Posterior probability that the candidate is a net improvement across evaluated tasks (range `[0, 1]`). |
| `batches` | `[]ComparisonBatchStatus` | Records execution state of each paired batch of Runs (`index`, `sampleRange`, `candidateRun`, `baselineRun`, `phase`). |
| `tasks` | `[]ComparisonTaskStatus` | Per-task paired comparison statistics and labels. |
| `tasks[].id` | `string` | Unique task identifier (e.g. `triage/runbook_lookup`). |
| `tasks[].baseRate` | `*float64` | Mean score or success rate on baseline samples. |
| `tasks[].candidateRate` | `*float64` | Mean score or success rate on candidate samples. |
| `tasks[].delta` | `*float64` | Mean paired difference ($\text{candidate} - \text{baseline}$). |
| `tasks[].deltaCI` | `*DeltaConfidenceInterval` | 95% confidence interval on the delta (`lower`, `upper`). |
| `tasks[].pairs` | `int` | Number of matched sample pairs evaluated. |
| `tasks[].wins` | `int` | Number of pairs where candidate scored higher than baseline. |
| `tasks[].losses` | `int` | Number of pairs where candidate scored lower than baseline. |
| `tasks[].ties` | `int` | Number of pairs where candidate and baseline scored equally. |
| `tasks[].dropped` | `ComparisonDropped` | Samples dropped without matches (`candidate`, `baseline`). |
| `tasks[].label` | `ComparisonTaskLabel` | Decision outcome: `improved`, `regressed`, `unchanged`, `inconclusive`, `running`. |
| `tasks[].samplesRun` | `int` | Total number of samples evaluated for this task across all batches. |
| `tasks[].decision` | `ComparisonTaskLabel` | Stopping rule decision: `improved`, `regressed`, `unchanged`, `inconclusive`, `continue`. |
| `summaryPath` | `string` | Storage path or URI where the complete paired table was stored (`comparisons/<name>.json`). |
| `message` | `string` | Human-readable explanation of the aggregate verdict or failure. |

## Printer Columns

`kubectl get comparisons` displays:

```text
NAME          VERDICT     P(IMP)   BASELINE             CANDIDATE             AGE
pr-418-eval   IMPROVED    0.98     run-main-a81d0c4     run-pr-418-3f9c2e1    2m
```


