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

# Architecture Decision Records (ADRs)

This document records the architectural and design decisions made for `eval-runner`, along with the context and rationale for each decision.

Future pull requests that introduce an architectural decision should append an entry here.

---

### ADR-001: Discovery is code, not config (`.evals/list`)
- **Date**: 2026-10-16
- **Status**: Accepted
- **Decision**: Eval tasks are discovered by executing an executable entrypoint at the repository root (`.evals/list`) inside an untrusted sandbox at the target commit, which outputs a JSON manifest to stdout.
- **Rationale**: Keeps discovery reproducible, isolated, and format-agnostic. The runner does not need language- or framework-specific AST parsers in the controller, and repositories can dynamically generate task lists or integrate any evaluation framework.

### ADR-002: API group is `evalrunner.gke-labs.dev` under `v1alpha1`
- **Date**: 2026-10-16
- **Status**: Accepted
- **Decision**: Define the Custom Resource Definition group as `evalrunner.gke-labs.dev` with version `v1alpha1`, introducing the `Run` custom resource.
- **Rationale**: Domain-namespaced under the project's GitHub organization (`gke-labs`), reserving clean namespace room for future API evolution while marking current types explicitly as alpha.

### ADR-003: Repository commit must be an exact hex SHA
- **Date**: 2026-10-16
- **Status**: Accepted
- **Decision**: `spec.repo.commit` is required and strictly validated by regex `^[0-9a-fA-F]{7,64}$` in CRD OpenAPI schema validation.
- **Rationale**: Branch and tag names are mutable and lead to non-deterministic eval runs. Requiring an exact immutable commit SHA ensures that runs are completely reproducible and that PR head and base comparisons evaluate exact, frozen states.

### ADR-004: Separate namespaces for controller and sandboxes
- **Date**: 2026-10-16
- **Status**: Accepted
- **Decision**: Run the controller and user `Run` resources in `eval-runner`, while spawning all untrusted `Sandbox` resources and pods in `eval-runner-sandbox`.
- **Rationale**: Strict isolation between the control plane and untrusted user workloads. Prevents untrusted sandbox code from discovering or interfering with controller pods or secrets, scopes controller informers and caches, and allows independent Pod Security Admission enforcement.

### ADR-005: Pin `agent-sandbox` release (`v1.0.7`) as external prerequisite
- **Date**: 2026-10-16
- **Status**: Accepted
- **Decision**: Require `sigs.k8s.io/agent-sandbox` release `v1.0.7` to be installed on the cluster as a prerequisite rather than bundling or sub-managing the sandbox controller.
- **Rationale**: Leverages standard Kubernetes SIGs infrastructure for agent sandboxing while avoiding the complexity and maintenance burden of managing custom sandbox lifecycle controllers. The controller validates that the `Sandbox` CRD is present at startup and fails fast if missing.

### ADR-006: Finalizer-based sandbox cleanup instead of `ownerReferences`
- **Date**: 2026-10-16
- **Status**: Accepted
- **Decision**: Attach a finalizer (`evalrunner.gke-labs.dev/discovery-sandbox`) to the `Run` to ensure discovery sandboxes in `eval-runner-sandbox` are deleted when a `Run` is deleted.
- **Rationale**: Kubernetes `ownerReferences` cannot cross namespace boundaries (`Run` in `eval-runner` cannot own `Sandbox` in `eval-runner-sandbox`). A finalizer prevents leaking orphaned sandbox pods if a user or CI deletes a Run mid-discovery.

### ADR-007: Strict status size discipline and exec stream limits
- **Date**: 2026-10-16
- **Status**: Accepted
- **Decision**: Impose strict size limits on sandbox exec streams (1 MiB stdout, 64 KiB stderr), manifest tasks (max 1,000), error message length (4,096 B), and stderr tail retention (10 lines / 3 KiB).
- **Rationale**: Untrusted repository entrypoints could emit megabytes of data, causing controller memory exhaustion or status write rejections against etcd's ~1.5 MiB request limit. Today `status.manifest` holds the full discovered manifest (bounded by `MaxTasks = 1000`). Enforcing limits at the stream level ensures cluster stability, and per-sample execution data will stay out of status once sample runs are implemented (#26).

### ADR-008: Project automation, manifests, and CI via `ap` (autoproject)
- **Date**: 2026-10-16
- **Status**: Accepted
- **Decision**: Standardize repository workflows (formatting, copyright header verification, CRD code generation, image builds, deployment, and presubmit CI generation) on `ap`.
- **Rationale**: Eliminates ad-hoc Makefiles, ensures identical presubmit execution between local developers and GitHub Actions, and prevents CI configuration drift.

### ADR-009: Google Gemini (`google/gemini-3.5-flash-lite`) as default evaluation model
- **Date**: 2026-10-16
- **Status**: Accepted
- **Decision**: Standardize on `google/gemini-3.5-flash-lite` as the recommended default model when running Inspect evaluation tasks with live model providers.
- **Rationale**: Provides fast latency and cost efficiency suited for frequent CI and evaluation workflows while demonstrating strong reasoning accuracy on comprehension and benchmarking tasks.

### ADR-010: Paired blind control task for documentation comprehension eval
- **Date**: 2026-10-16
- **Status**: Accepted
- **Decision**: Run a paired blind evaluation task (`docs/readme_comprehension_blind`) alongside the full comprehension task (`docs/readme_comprehension`).
- **Rationale**: Evaluates the identical question set without providing repository documentation, establishing an empirical baseline for pretraining data leakage versus genuine comprehension of the provided text.

### ADR-011: Namespaced-only RBAC and least-privilege informers
- **Date**: 2026-10-16
- **Status**: Accepted
- **Decision**: Avoid cluster-scoped RBAC (`ClusterRole`); grant controller permissions strictly through namespaced `Role` and `RoleBinding` in `eval-runner` and `eval-runner-sandbox`, and scope manager cache informers per object to their respective namespaces.
- **Rationale**: Enforces least privilege. The controller cannot read or manipulate pods, secrets, or workloads in any other cluster namespace, and cache informers do not watch cluster-wide events.

### ADR-012: Images are deployed by git SHA tag, never by `latest`
- **Date**: 2026-10-17
- **Status**: Accepted
- **Decision**: Deploy container images tagged with an explicit git commit SHA (with a `-dirty` suffix when the working tree has uncommitted modifications), never with the mutable `latest` tag. The `dev/tasks/deploy` script automates setting `IMAGE_TAG` from git, always executes `ap` at the repository-pinned version via `go run` (never from `PATH`), and verifies the pushed multi-arch index contains `linux/amd64` before applying manifests.
- **Rationale**: Kubernetes `Deployment` controllers only initiate a rolling update when the pod template spec changes. Deploying with `latest` results in a byte-identical manifest, so `kubectl apply` reports `unchanged` and the cluster silently continues running previous image digests until manually restarted. Using git SHA tags ensures every deployment manifest change triggers a new ReplicaSet rollout. Multi-platform images pinned in `.ap/images.yaml` and verified for `linux/amd64` presence guard against deploying single-architecture (e.g. arm64-only) images that cause node ImagePullBackOff.

### ADR-013: Adopt agent-sandbox's sandboxd for in-sandbox execution, step-based discovery, and ingress NetworkPolicy
- **Date**: 2026-10-17
- **Status**: Accepted
- **Decision**: Replace `kubectl exec`-style SPDY execution with `kubernetes-sigs/agent-sandbox`'s `sandboxd` runtime daemon running directly inside the discovery sandbox pods. The daemon is injected via deployment topology B (initContainer copying `sandboxd` into a shared volume), exposing gRPC `ProcessService` on `:9090` and REST `FilesystemService` on `:8080`, with readiness determined by `/v1/health`. The controller connects directly to the sandbox Pod IP via `pkg/sandboxclient`. Discovery is executed as discrete steps:
  1. `git clone -- <url> repo`: retries up to N times with backoff on non-zero exit;
  2. `git checkout <commit>`: permanent failure on non-zero exit (no retry);
  3. `./.evals/list`: executed in the repo directory, categorizing missing, non-executable, non-zero exit, and invalid JSON as distinct, typed failures without retries.
  Controller RBAC drops `pods/exec`. An ingress `NetworkPolicy` in `eval-runner-sandbox` restricts ports `:9090` and `:8080` to controller pods in `eval-runner`.
- **Rationale**: Driving execution via structured RPCs supersedes heuristic regex parsing of mixed stderr streams. Step-based discovery guarantees transient clone failures are retried without any risk of retrying permanent errors like missing `.evals/list` or non-existent commit SHAs. Direct in-cluster RPC avoids routing through the Kubernetes API server via `pods/exec`, adhering to least-privilege RBAC. Because sandboxd originally had no authentication (superseded by per-sandbox bearer authentication in ADR-014), ingress NetworkPolicy ensures sandbox pods are only accessible by the authorized controller.

### ADR-014: Enable sandboxd bearer auth (--auth-token-sha256) and generalise NetworkPolicy podSelector
- **Date**: 2026-10-17
- **Status**: Accepted
- **Decision**: Configure `sandboxd` with `--auth-token-sha256=<hex>` in sandbox pod specs. The controller generates a random 256-bit bearer token per sandbox, persists it in a label-free Secret in `eval-runner` (and caches in controller memory), and injects only the hex SHA-256 digest into the sandbox pod command line. The controller presents the bearer token on all gRPC and REST RPCs. `/v1/health` remains unauthenticated for kubelet readiness probes. Generalise the ingress NetworkPolicy podSelector from `evalrunner.gke-labs.dev/role: discovery` to `evalrunner.gke-labs.dev/sandbox: "true"`, covering both discovery and future sample sandboxes by default.
- **Rationale**: Defense in depth against cluster environments where NetworkPolicy enforcement is disabled or incomplete. Minting tokens in the controller and only exposing SHA-256 digests in pod specs ensures sandbox processes cannot read the credentials required to reach sandboxd from outside. Label-free Secrets in `eval-runner` prevent leakage into the sandbox namespace while guaranteeing controller restart resilience. Generalising the podSelector to `evalrunner.gke-labs.dev/sandbox: "true"` prevents new sandbox roles from being accidentally exposed without ingress policies.

### ADR-015: Sibling images are derived from the controller image until ap supports sibling placeholders (gke-labs-infra#464)
- **Date**: 2026-10-17
- **Status**: Accepted
- **Decision**: Sibling images are derived from the controller image until ap supports sibling placeholders (gke-labs-infra#464). Remove the deploy-time manifest rewrite in `dev/tasks/deploy` and derive the `sample-runner` image reference at runtime from the controller's own Pod spec via the Downward API (`POD_NAME` and `POD_NAMESPACE`). The controller queries its own Pod in `eval-runner` (granted via scoped RBAC `get` on `pods`), finds the `eval-runner` container image, and replaces the final path segment `eval-runner` with `sample-runner`, preserving repository prefix and tag. Image references with digest references (`@sha256:`) or mismatched naming fail fast at startup asking for the flag. `--sample-runner-image` is retained as an explicit override.
- **Rationale**: Eliminates deploy-time mutations of tracked deployment manifests that caused dirty git working trees (`-dirty` image tags) and relied on platform-specific sed semantics. ap guarantees one `IMAGE_PREFIX` and one `IMAGE_TAG` per deploy, making derivation consistent. Once `gke-labs-infra#464` ships declarative placeholder substitution for sibling images, this runtime derivation can be replaced cleanly.

### ADR-016: Per-task success rate with 95% Wilson/Student-t CI on Run status and printer columns
- **Date**: 2026-10-17
- **Status**: Accepted
- **Decision**: Record `status.tasks[].successRate` and `status.tasks[].successRateCI: {lower, upper, confidence: 0.95, method: "wilson"|"t"}` on each completed task on a `Run`. Binary scorers compute the Wilson score interval via `stats.Proportion`; continuous non-binary scorers compute Student's t-intervals via `stats.Mean` on in-memory per-sample scores. Add `status.summary` recording completed/failed counts, total samples, and simple mean success rate with its range. Declare CRD printer columns for `kubectl get runs` (`PHASE`, `TASKS` [completed/total], `SAMPLES`, `AGE`) and `kubectl get runs -o wide` (`RATES` compact `id=rate [lo,hi]` string capped at 120 chars). Provide CLI inspection via `evalstats -from-run <name>`.
- **Rationale**: Concrete first step of "CI stands for confidence interval". In accordance with the status discipline in ADR-007 and #53's principle, durable per-sample trajectories belong in the artifact sink (#44), while the `Run` resource carries bounded summary progress and intervals. This provides immediate statistical visibility in `kubectl` and the terminal before baseline pairing and web UI land.

### ADR-017: Manifests are project-agnostic; per-cluster config via ConfigMap
- **Date**: 2026-10-17
- **Status**: Accepted
- **Decision**: Read deployment-specific settings from a per-cluster ConfigMap `eval-runner/eval-runner-config` that is not part of tracked manifests in `k8s/`, created once by the cluster operator. Supported keys include `artifactBucket` and operational limits (`discoveryDeadline`, `executionDeadline`, `maxParallelTasks`, default `modelSecret`, `taskGracePeriod`). Precedence is: explicit CLI flag > ConfigMap key > built-in default. The Deployment manifest in `k8s/` passes no deployment-specific flags. The controller reads this ConfigMap via the uncached API reader at startup; missing ConfigMap is treated gracefully as using built-in defaults. RBAC grants scoped `get` on `configmaps` named `eval-runner-config` in the `eval-runner` namespace.
- **Rationale**: Deployment manifests in `k8s/` must remain project-agnostic so that `ap deploy` can be run against any cluster without mutating tracked files or requiring post-deploy `kubectl patch` steps. Because `kubectl apply` resets container arguments to what is declared in `k8s/runner-deployment.yaml`, passing deployment-specific settings as container flags previously required re-patching the deployment after every deploy. A per-cluster ConfigMap decouples environment-specific configuration (such as artifact storage buckets) from deployment manifests, adhering to 12-factor application design and GitOps cleanliness.

### ADR-018: Comparison CRD, Paired Baseline Evaluation, and Sink-Based Sample Pairing
- **Date**: 2026-10-17
- **Status**: Accepted
- **Decision**: Introduce namespaced `Comparison` CRD (`evalrunner.gke-labs.dev/v1alpha1`) pairing two `Run` resources (`spec.baseline.run` and `spec.candidate.run`) with `spec.equivalenceMargin` (default 0.05) and optional `spec.tasks` filter. The Comparison controller watches `Run` resources and maintains status `verdict: RUNNING` until both runs are terminal (`Completed` or `Failed`). Upon completion, the controller loads per-sample Inspect evaluation logs from the artifact sink via a bounded `Load` method on `ArtifactSink`, pairs samples by stable sample ID, computes paired differences and 95% confidence intervals via `pkg/stats.ComparePaired`, assigns stopping/decision labels (`improved`, `regressed`, `unchanged`, `inconclusive`) via `stats.Decide`, and aggregates a run-level verdict with posterior $P(\text{improvement})$ via `stats.AggregateVerdict`. The full per-sample paired table is persisted to `comparisons/<name>.json` in the artifact sink, while `Comparison.status` carries bounded aggregate metrics and `summaryPath`. Provide CLI inspection matching the PR-comment table mockup via `evalstats -from-comparison <name>`.
- **Rationale**: Implements the statistical evaluation core ("CI stands for confidence interval") where PR verdicts are derived from paired sample-level evaluations against a baseline. Preserves zero-trust and Kubernetes status boundedness: sandboxes never possess cloud storage credentials or access to baseline data; the controller reconciles pairing after runs finish; and large per-sample trajectories live in the artifact store rather than etcd. Bounded `Load` protects controller memory, while watching Runs guarantees automated, event-driven verdict updates.

### ADR-019: Tie-Splitting for P(improvement) and Neutral Run-Level Aggregation for Inconclusive Comparisons
- **Date**: 2026-10-17
- **Status**: Accepted
- **Decision**:
  1. Define per-task $P(\text{improvement})$ as $P(\Delta > 0) + 0.5 \cdot P(\Delta = 0)$ (ties split evenly) for both Rubin's Bayesian Bootstrap and the analytical normal-approximation fallback. An all-tied task reports $0.50$, matching intuition and the `unchanged` label. Regularise the Bayesian bootstrap posterior with a uniform prior ($+1.0, -1.0$ pseudo-observations) to avoid degenerate zero-mass when no wins or no losses are observed among ties.
  2. In `stats.AggregateVerdict`, when a comparison contains only `unchanged` and `inconclusive` tasks (without any `improved` or `regressed` tasks), aggregate headline $P(\text{improvement})$ as the arithmetic mean across tasks capped by $\min_k P(\text{not\_reg}_k)$ ($P_{\text{run}} = \min(\bar{P}, \min_k P(\text{not\_reg}_k))$), so one risky task cannot be diluted by many unchanged ones (yielding $\min(0.375, 0.25) = 0.25$ for the same-commit run with 0W/0L/20T and 0W/1L/19T) instead of collapsing under unregularised regression bounds or taking a biased maximum.
  3. Surface small integer counts of `wins`, `losses`, and `ties` in `ComparisonTaskStatus`, `PairedEstimate`, and `ComparisonTaskArtifact`. Display `wins/losses/ties` next to `runs` in `evalstats -from-comparison` table output.
  4. Keep stopping rules and labels unchanged.
- **Rationale**: In real-world cluster evaluation (`docs-eval-same-commit` on commit `05ce4d8`), comparing two runs of the exact same commit reported `Verdict: INCONCLUSIVE · P(improvement) = 0.00`. An all-tied task concentrated all bootstrap mass at $\Delta = 0$, producing $P(\Delta > 0) = 0$; subsequent run-level aggregation collapsed under regression risk bounds. Splitting ties evenly reflects standard statistical decision theory (Mann-Whitney / sign-test tie convention) and user intuition that identical configurations have 50% probability of superiority. Regularising the posterior with a uniform prior prevents 0.0000 on 0W/1L/19T (yielding 0.25), and capping the average probability by $\min P(\text{not\_reg})$ yields $\min(0.375, 0.25) = 0.25$ so regression risk is not diluted by unchanged tasks. At small $n$, the categorical label and verdict are authoritative and $P$ is a summary. Surfacing win/loss/tie breakdowns immediately explains why confidence intervals are wide on high-tie evals.
### ADR-020: Mutual TLS (mTLS) between controller and sandboxd with controller CA and pushed server certs
- **Date**: 2026-10-17
- **Status**: Accepted
- **Decision**: Replace cleartext bearer token communication with mutual TLS (mTLS) for all gRPC (`ProcessService` on `:9090`) and REST (`FilesystemService` on `:8080`) traffic between the controller and sandboxd:
  1. **Controller CA**: The controller owns a self-generated CA (ECDSA P-256) persisted in Secret `sandbox-ca` in `eval-runner`, publishing the public CA certificate in ConfigMap `sandbox-ca` in `eval-runner-sandbox`.
  2. **Controller Client Identity**: The controller holds a client certificate and private key signed by the CA, presenting it when dialing sandboxes. sandboxd enforces `RequireAndVerifyClientCert` against the public CA certificate.
  3. **Per-Sandbox Server Identity (Pushed at creation)**: When creating a Sandbox, the controller mints a short-lived server keypair signed by the CA with SAN `<name>.<namespace>.sandbox.evalrunner.local` and delivers it via a per-sandbox Secret in `eval-runner-sandbox` (`sb-tls-<name>`), deleted explicitly by the controller's finalizer path (ADR-006) when the sandbox is cleaned up.
  4. **SAN Pinning**: The controller dials the sandbox pod IP with `ServerName` pinned to `<name>.<namespace>.sandbox.evalrunner.local`, cryptographically verifying that a compromised sandbox cannot impersonate another.
  5. **Plaintext Health Port**: sandboxd serves unauthenticated HTTP `GET /v1/health` on a separate plaintext port `:8081` (`--health-port=8081`), keeping kubelet readiness probes functional without exposing application endpoints.
  6. **Defense in Depth**: Bearer token authentication (`--auth-token-sha256`) and ingress `NetworkPolicy` remain enabled alongside mTLS as secondary layers.
  7. **Fork Prototyping**: Prototyped on a fork of `kubernetes-sigs/agent-sandbox` (`codebot-robot/agent-sandbox`, branch `sandboxd-mtls`), consumed via `go.work` locally and a pinned `replace` directive in `go.mod`.
- **Rationale**:
  In cluster environments where NetworkPolicy enforcement is disabled or missing (such as certain GKE configurations), plaintext bearer tokens over the pod network offer limited security against compromised neighbor pods. mTLS guarantees confidentiality, mutual authentication, and cryptographic isolation independent of the CNI. Pushing server certificates into the sandbox at creation time via a Secret deleted by the finalizer path (ADR-006) is atomic, avoids initial plaintext bootstrap channels, and ensures reliable garbage collection without requiring ownerReference patching across async object creation. The alternative—having sandboxd generate its private key locally and exchange a Certificate Signing Request (CSR) with the controller—would require an initial bootstrap channel, additional polling or coordination protocols, and complex state management, without providing significant additional security since sandboxd runs inside the sandbox pod managed by the controller. Serving `/v1/health` on dedicated plaintext port `:8081` preserves kubelet readiness checks without requiring kubelet to participate in controller mTLS. Future upstream proposals to agent-sandbox can also explore dynamic CA reloading (to hot-reload CA changes without container restarts) and optional pod-IP SANs (minted or updated once the pod IP is assigned by the CNI).

### ADR-021: GitHub PR Reporting via In-Place Comment Upsert and Check Run State Mapping
- **Date**: 2026-10-17
- **Status**: Accepted
- **Decision**:
  1. **Linking Comparison to PR**: Add optional `spec.pullRequest: {repository, number, headSHA, baseSHA}` to the `Comparison` CRD. The Comparison already references candidate and baseline Runs; the PR reference specifies where to report.
  2. **Shared Markdown Renderer**: Create `pkg/github.RenderComparisonMarkdown` built directly on `ui.ComparisonRows` and `ui.ComparisonSummary` so PR comments and the web view cannot drift. Format a hidden HTML marker (`<!-- eval-runner:comparison:<ns>/<name> -->`), verdict headline (`## <VERDICT> · P(improvement) = <p>`), table (`eval | base | this PR | Δ (95% CI) | label | runs`), and footer (samples run, paired against base commit, link to web view path, and storage URI as text).
  3. **Check Run State Mapping**: Create and update a GitHub check run named `eval-runner / evals` on `headSHA` mapping the five verdicts:
     - `RUNNING`: status `in_progress`, conclusion `""`, description `running · N of M samples`
     - `IMPROVED`: status `completed`, conclusion `success`, description `IMPROVED · P(improvement) = 0.98`
     - `UNCHANGED`: status `completed`, conclusion `success`, description `UNCHANGED`
     - `INCONCLUSIVE`: status `completed`, conclusion `neutral`, description `INCONCLUSIVE · more samples recommended`
     - `REGRESSED`: status `completed`, conclusion `failure`, description `REGRESSED`
  4. **Marker-Based In-Place Upsert & Idempotency**: Update existing PR comments in-place by searching for the hidden marker comment if `status.reporting.commentID` is unknown. Record `status.reporting{commentID, checkRunID, lastPostedHash}` to skip GitHub API calls when content and state have not changed.
  5. **Credentials & Zero-Trust Containment**: Token is read from Secret `eval-runner/github-token` (key `token`), configurable via `githubTokenSecret` in `eval-runner-config`. The token exists exclusively in the controller namespace and is never mounted in sandboxes. If the secret is absent, reporting is cleanly disabled (`status.reporting.message: "disabled"`) without failing the comparison.
  6. **Error Classification**: Rate-limit (429 or 403 rate limit) and 5xx errors return errors to requeue with backoff. Permanent errors (403, 404, 422) are recorded in `status.reporting.message` without failing the comparison.
- **Rationale**:
  Delivers the product moment of eval-runner (functionality.md §5: verdict landing on the PR). Using a single in-place updating comment prevents PR clutter and avoids notification spam across incremental status updates. Building the Markdown renderer on top of `pkg/ui` models ensures that formatting, confidence intervals, and summary calculations remain consistent with the web view. Deriving check run conclusions from the five verdicts allows GitHub PR status checks to reflect statistical significance directly (with `neutral` for inconclusive results avoiding merge blockage by default). Storing credentials in the controller namespace preserves the project's zero-trust model.

### ADR-022: CRD = pointers and progress; object store = records
- **Date**: 2026-10-18
- **Status**: Accepted
- **Decision**:
  1. Remove `status.manifest` from the `Run` CRD. Replace with `status.discovery` containing lightweight progress indicators and pointers: `manifestPath` (URI), `manifestHash` (`sha256:<hex>`), `taskCount`, `eligibleTaskCount`, and `taskIDs` (capped at 50 IDs).
  2. In the discovery sandbox, `.evals/list` writes its output to `/workspace/out/manifest.json`. The controller pulls the file via sandboxd REST `ReadFile` (`/out/manifest.json` relative to `--root-dir=/workspace`), checks size against the 1 MiB limit, parses and validates the manifest.
  3. Persist the raw manifest JSON into the artifact store under `runs/<commit>/manifest.json` via `ArtifactSink.StoreManifest(ctx, meta, bytes) (uri, error)` across all sinks (GCS with run/commit/task-count metadata; LocalDir in kind; InMemory as non-durable default). Above the cap, GCS errors rather than truncating. Writing the same object twice for the same commit is a no-op when the hash matches.
  4. Consumers (RunReconciler during task execution, ComparisonReconciler, evalstats, UI) load the manifest by URI when more than the 50 capped IDs are needed. Loading employs a per-Run in-memory cache keyed by namespace/name with eviction on terminal phase or deletion. Manifest loading enforces commit prefix confinement (`ArtifactPrefix(commit)`) and hash verification against `status.discovery.manifestHash`.
  5. Load errors are classified: transient errors (timeouts, 5xx) requeue with a message and preserve healthy task sandboxes; not-found errors or non-durable sink restarts reset the Run to `Pending`, clear `status.discoveryDeadline` (so a restart > 10 minutes later re-arms a fresh deadline rather than failing), and re-discover; hash drift keeps the phase and reports.
- **Rationale**:
  Kubernetes custom resources (CRDs) in etcd are designed for declarative state, progress reporting, and synchronization pointers, not durable, unbounded storage of data records. A repository with hundreds of evaluation tasks serializes tens or hundreds of kilobytes into `Run.status.manifest`, inflating etcd storage, increasing apiserver watch bandwidth, and risking etcd's 1.5 MiB request limit. Moving the full manifest to the object store (`runs/<commit>/manifest.json`) bounds CRD status size while treating the artifact store as the authoritative source of records. Writing discovery output to `/workspace/out/manifest.json` decouples discovery execution from stdout buffer limits. In-memory caching avoids repetitive object-store reads, while hash verification prevents cache poisoning or silent drift. Robust error classification and deadline clearing ensure restarts—even after the original discovery deadline—seamlessly re-discover without failing Runs or deleting task sandboxes on temporary network blips.

### ADR-023: GitHub Webhook Integration for Automated PR Evaluation and Baseline Reuse
- **Date**: 2026-10-18
- **Status**: Accepted
- **Decision**:
  1. **Dedicated Webhook Component**: Implement a standalone binary `cmd/eval-runner-webhook` packaged as a minimal distroless image (`eval-runner-webhook`) and deployed in namespace `eval-runner` with a ClusterIP Service on port 80 (targetPort 8080).
  2. **Signature Verification & Zero-Trust Boundary**: Validate the `X-Hub-Signature-256` HMAC-SHA256 signature on every incoming request against a secret stored in Secret `eval-runner/github-webhook` (key `secret`, configurable via `githubWebhookSecret`). Reject missing, malformed, or invalid signatures immediately with HTTP 401. The webhook holds exclusively the webhook HMAC secret, preserving strict zero-trust separation: it has no access to GitHub personal access tokens, model credentials, or cloud storage buckets.
  3. **Asynchronous Bounded Execution**: Fast acknowledgment to GitHub is mandatory (<10s). The HTTP handler verifies signatures, filters by repository allow-list (`webhookRepositories` in ConfigMap) and action, enqueues actionable events into a bounded in-memory worker queue, and immediately returns HTTP 200 OK. Background worker goroutines perform Kubernetes API operations.
  4. **Deterministic Naming & Idempotency**:
     - Candidate Run: `pr-<number>-<headSHA7>` with PR head commit and `defaultModel`/`defaultLimit` from cluster configuration.
     - Baseline Run: `base-<baseSHA7>` with merge target commit.
     - Comparison: `pr-<number>` linking candidate and baseline runs with `spec.pullRequest` configured.
     - Redelivered webhook events are idempotent: resources are queried before creation and updated cleanly without generating duplicate runs.
  5. **Baseline Cache Reuse (Concept §4.1)**: When handling a `pull_request` event, before creating a new baseline run, the worker checks if any Run in the namespace targeting the base commit and model has already reached `Completed` phase. If found, the existing Run is reused as the comparison baseline, avoiding redundant evaluations on shared base commits.
  6. **Handling `synchronize` Events**: When new commits are pushed to a PR, the candidate Run `pr-<number>-<newHead7>` is created and the existing Comparison `pr-<number>` is updated to point its `spec.candidate.run` and `spec.pullRequest.headSHA` to the new candidate run, triggering re-evaluation and reporting.
  7. **Warming Baseline on Default-Branch Push**: On `push` events to the default branch, `base-<sha7>` is created to pre-warm the baseline cache before PRs are opened.
  8. **Labels**: Created resources are labeled with `evalrunner.gke-labs.dev/pr`, `evalrunner.gke-labs.dev/repo` (sanitized for label value syntax), and `evalrunner.gke-labs.dev/head-sha`.
- **Rationale**:
  Closes the automated evaluation loop described in concept §5.1, transitioning eval-runner from manual CR creation to fully automated PR-driven execution. Running the webhook in its own container with least-privilege RBAC isolates public HTTP exposure from the controller core and enforces defense-in-depth: compromising the webhook reveals only the webhook HMAC secret, not repository tokens or model keys. Bounded asynchronous workers prevent request timeouts and shield the Kubernetes apiserver from webhook bursts. Baseline cache reuse significantly optimizes compute budget and turnaround time by running merge target evaluations only once per base commit.

### Amendment to ADR-023: Retention of Comparisons and Seed Runs on PR Close (#384)
- **Date**: 2026-10-23
- **Status**: Accepted
- **Decision**:
  1. **Retain and Label on Close**: Closing or merging a pull request no longer deletes its Comparisons or referenced seed Runs. Comparisons are retained and labeled `evalrunner.gke-labs.dev/pr-state: merged` (when merged) or `evalrunner.gke-labs.dev/pr-state: closed` (when closed without merge).
  2. **Reopening**: On `ChangeOpenedEvent` (or webhook reopened actions), existing Comparisons for the PR are updated back to `evalrunner.gke-labs.dev/pr-state: open`.
  3. **Run Retention**: Any Run referenced as candidate or baseline by a retained Comparison is preserved. Orphaned PR-labelled seed Runs (not referenced by any retained Comparison) may still be deleted on close; baseline Runs are never touched.
  4. **Retention Policy**: Comparison retention is governed by the existing `comparisonTTL` (default `0` = keep indefinitely).
- **Rationale**:
  Preserves historical evaluation data so developers can inspect how recent (and merged) PRs performed without losing comparison results when PRs close.

### ADR-024: GitHub App Authentication for Check Runs, Comment Pagination, and Author Verification
- **Date**: 2026-10-18
- **Status**: Accepted
- **Decision**:
  1. **Dual GitHub Authentication (App Preferred, Token Fallback)**: Support GitHub App authentication via `app-id`, `installation-id`, and `private-key` stored in the controller Secret (`eval-runner/github-token` or configured via `githubAppSecret`/`githubTokenSecret`). The controller mints RS256 JWTs using standard Go crypto, exchanges them for 1-hour installation access tokens via `POST /app/installations/{id}/access_tokens`, and caches tokens until near expiration (5 minutes buffer). PAT authentication (`token` key) remains fully supported for comment-only installations. When both App credentials and a token are provided, App auth is preferred.
  2. **Check Run Permissions**: GitHub Apps provide official read/write permissions for both `checks` and `pull_requests`. Check run creation and updates (`eval-runner / evals`) succeed under App auth, resolving the 403 limitation of fine-grained personal access tokens.
  3. **Comment Pagination & Link Header Traversal**: `ListIssueComments` follows RFC 5988 `Link` headers (`rel="next"`) to traverse all pages of issue comments (with an upper safety bound of 100 pages), eliminating duplicate comments on busy PRs with >100 comments.
  4. **Marker Matching Author Verification**: When searching for existing comparison marker comments (`<!-- eval-runner:comparison:<ns>/<name> -->`), the reporter requires `comment.user.login` to match the authenticated identity (the user login for PAT auth, or `<slug>[bot]` for GitHub App auth). This prevents malicious commenters from hijacking the bot's reporting by posting the marker.
  5. **Absolute Web UI Links**: Support `uiBaseURL` in cluster configuration. When set, web-view links in comments are rendered as absolute Markdown links `[web view](<uiBaseURL>/comparisons/<ns>/<name>)` rather than code-text paths.
  6. **Planned Sample Counting**: In check run status during execution (`running · N of M samples`), calculate M using planned samples (`limit x tasks` across candidate and baseline runs) rather than scored counts, providing an accurate progress denominator.
  7. **Hash-Gate Retry On Check Run Failures**: Clear `LastPostedHash` when check run creation or update fails with a retryable error (while retaining `CommentID`), ensuring the subsequent reconcile requeue is not swallowed by the idempotency hash gate.
- **Rationale**:
  Check runs provide high-visibility PR check status directly integrated into GitHub's merge-blocking policies. Enabling native GitHub App authentication unlocks check runs while preserving zero-trust containment within the controller namespace. Author verification and Link-header pagination harden comment upserting against hijacking and duplicate comment creation on high-activity repositories.

### ADR-025: Runs Evaluate Sample Windows; Batches are New Runs, Not Mutated Runs
- **Date**: 2026-10-18
- **Status**: Accepted
- **Decision**:
  1. **Windowed Sampling via `spec.sampleRange {start, end}`**: Add `sampleRange` (`{start, end}`) to `RunSpec`, representing 1-based inclusive positions in dataset order. It is mutually exclusive with `spec.limit` (enforced via OpenAPI CEL validation on the CRD and controller-side validation). `spec.limit: N` maintains the semantic of evaluating `sampleRange {1, N}`. If neither is set, all samples in the dataset are evaluated.
  2. **Inspect CLI Flag**: Pass `spec.sampleRange` directly to Inspect CLI as `--limit START-END` (e.g. `--limit 1-5`, `--limit 6-10`), which is natively supported in Inspect 0.3.281.
  3. **Batches As New, Immutable Runs**: For adaptive paired sampling, extension batches are scheduled by creating new, immutable `Run` resources with disjoint sample windows (e.g. Run 1 evaluates `1-8`, Run 2 evaluates `9-16`), rather than reopening or mutating completed Runs.
  4. **Status & Artifact Recording**: Record the sample window in `status.tasks[].sampleRange`, in the per-run `summary.json`, and in the UI Runs view (e.g. `samples 9-16`).
- **Rationale**:
  Kubernetes controllers and cloud-native workflows heavily favor declarative immutability. Terminal Runs (`Completed` or `Failed`) clean up sandboxes, mTLS certificates, tokens, and ephemeral resources. Reopening terminal Runs would require complex re-attachment to destroyed sandboxes, mutating history, invalidating cached artifacts, and re-entrant state machines. Creating new Runs for each batch guarantees:
  - **Run Immutability**: Once a Run reaches `Completed`, its status, logs, and artifacts are permanently immutable.
  - **Baseline Reuse at Batch Granularity**: Baseline runs evaluated on window `1-8` or `9-16` can be reused by multiple candidate runs or PRs without re-evaluation.
  - **No Sandbox Re-attachment**: Sandboxes are cleanly provisioned and deleted per Run lifecycle, eliminating persistent pod leakage and complicated re-attachment protocols.

### ADR-026: Paired Batch Loop Driven by Stopping Rule (Adaptive Paired Batches)
- **Date**: 2026-10-18
- **Status**: Accepted
- **Decision**: Comparisons implement an adaptive paired batch loop bounded by confidence intervals and the stopping rule rather than fixed static budgets:
  1. `Comparison.spec.sampling {batchSize, maxSamplesPerTask}` configures the loop. When omitted, the controller performs a single-pass comparison over the referenced Runs.
  2. The initial referenced Runs define batch 1. When both Runs are terminal, the controller concatenates sample scores across all batches, pairs by sample ID, and evaluates `stats.Decide`.
  3. If a task returns `continue` and has evaluated fewer samples than `maxSamplesPerTask` without dataset exhaustion, the controller spawns a new Run pair windowed to the next contiguous sample range (`[highestPosition + 1, nextEnd]`) evaluating only undecided tasks.
  4. Runs remain strictly immutable and owned by the Comparison with deterministic names `<comparison>-b<k>-cand` and `-base` looked up idempotently before creation.
  5. If a batch returns fewer samples than its window, the dataset is marked exhausted and decided with what exists (`inconclusive` if still `continue`), preventing any window from extending past the dataset boundary.
  6. **Extra Samples (`spec.extraSamples`)**: Setting `spec.extraSamples` requests evaluating $N$ additional samples per undecided task (or all tasks if decided) beyond the stopping rule (evaluating beyond `maxSamplesPerTask`). If `extraSamples > batchSize`, the controller schedules $\lceil \text{extraSamples} / \text{batchSize} \rceil$ batches. The controller records `status.extraSamplesApplied` with the count of extra samples scheduled into extension batches (consumed request count); bumping the field again triggers further batches. Inconclusive tasks with remaining dataset samples display `- run N more samples` in PR comments using `suggestedNext` from `stats.Decide` (zeroed when the dataset is exhausted).
  7. **Baseline Batch Reuse**: Before creating a baseline batch Run, the controller computes a deterministic hash (`evalrunner.gke-labs.dev/batch-key`) covering repo URL, commit, model, task set, and sample window. If a matching `Completed` Run exists in the namespace, it is reused and updated with an additional owner reference (`Controller: false`), preventing premature garbage collection while eliminating duplicate compute across Comparisons.
  8. **Cost and Token Tracking**: Inspect log model usage (`input_tokens`, `output_tokens`, `total_tokens`) is parsed via `ExtractInspectTokens` as structured `{input, output, total}` tokens, accumulated per task across evaluated runs, and recorded in `Run.status.tasks[].tokens`, `Comparison.status.tasks[].tokens`, and `summary.json`.
- **Rationale**:
  Fulfills Concept §4.2 and §4.5 ("bound the confidence interval, not the budget"). Identical runs (or large regressions/improvements) reach statistical certainty quickly within a few batches and terminate early, saving significant evaluation tokens and compute, while noisy or close comparisons gather additional evidence up to the task sample cap. Baseline reuse prevents redundant execution across PRs sharing merge-base commits. Tracking token usage directly couples statistical stopping decisions to monetary and compute costs.

### Amendment to ADR-026: Webhook Adaptive Sampling and /eval more <n> PR Comments (#151)
- **Date**: 2026-10-18
- **Status**: Accepted
- **Decision**:
  1. **Webhook Default Adaptive Sampling**: Webhook-created Comparisons configure `spec.sampling {batchSize: defaultBatchSize, maxSamplesPerTask: defaultMaxSamplesPerTask}` from ConfigMap keys `defaultBatchSize` (default 8) and `defaultMaxSamplesPerTask` (default 64). Seed candidate, baseline, and warm push Runs receive `sampleRange {1, defaultBatchSize}` instead of `limit`.
  2. **Reviewer Trigger via PR Comments (`/eval more <n>`)**: Reviewers trigger additional samples directly from GitHub PRs by posting a comment of the form `/eval more <n>`.
  3. **Event Handling & Zero-Trust Authorization**: The webhook handles `issue_comment` events behind HMAC signature verification and repository allow-lists. The author must have write or admin access per GitHub's collaborator permission API (`GET /repos/{owner}/{repo}/collaborators/{user}/permission`) or be listed in the ConfigMap allow-list `commandUsers`. The webhook Role is granted scoped `get` access on `github-token` and `github-app` secrets by `resourceNames` strictly to query collaborator permissions and post comment acknowledgements (`eyes` / `+1` reactions or fallback comments).
  4. **Bounding & Idempotency**: The requested count $n$ is bounded by `defaultMaxSamplesPerTask`. The webhook atomically increments `spec.extraSamples` by $n$ on the PR Comparison and records the comment ID in the annotation `evalrunner.gke-labs.dev/handled-comments` (bounded to the last 100 entries). Redeliveries are strictly idempotent.
  5. **PR Comment Action Hint**: When tasks are inconclusive, the PR comment renders `- comment `/eval more <n>` to run <n> more samples` where $n$ is suggested by `suggestedNext`. The webhook acknowledges comments via GitHub reaction (`eyes` or `+1`) or acknowledgement comment.
- **Rationale**:
  Connects Concept §4.2 and §4.5 to the PR review lifecycle. Allows reviewers to interactively purchase additional statistical certainty directly in GitHub PR review discussions without requiring direct kubectl or Kubernetes API access.

### Amendment to ADR-026: Credential-Free Webhook and Controller Command Processing (#164)
- **Date**: 2026-10-19
- **Status**: Accepted
- **Decision**:
  1. **Credential-Free Webhook**: The `eval-runner-webhook` component remains strictly credential-free. It holds exclusively the webhook HMAC secret (`eval-runner/github-webhook`) and has no access to GitHub tokens or App credentials (`github-token` / `github-app`). It makes no outbound GitHub API calls.
  2. **Bounded Command Enqueueing**: Upon verifying the webhook HMAC signature, repository allow-list, and `/eval more <n>` command syntax, the webhook appends `{commentId, login, requested, receivedAt}` to a bounded `status.pendingCommands` list on the repository-matched Comparison. The operation is idempotent per `commentId` against both pending and handled commands. The webhook Role drops the two secret grants and gains `comparisons/status` update.
  3. **Controller Authorization and Acknowledgement**: The controller (which already holds credentials for PR reporting) asynchronously processes `status.pendingCommands`: it verifies write or admin access via GitHub's collaborator permissions API or checks exact matches in `commandUsers`, failing closed on API error. Transient permission API errors (5xx, rate limits) and transient Secret read errors are retried up to `MaxCommandAttempts` (3) across reconciles with a time-gated backoff schedule (10s, 1m, 5m, or `Retry-After`) and `attempts` and `nextAttemptAt` tracked on the pending entry before becoming terminal. For authorized commands, it clamps and bumps `spec.extraSamples`, posts a GitHub acknowledgement (`eyes` reaction or fallback reply comment), and moves the entry to a bounded `status.handledCommands` list with the outcome. Denied commands and terminal errors react with `confused` without posting reply comments.
- **Rationale**:
  Preserves the zero-trust credential isolation model by eliminating GitHub credentials and outbound calls from the internet-facing webhook pod. Authorization and GitHub interactions are centralized in the controller alongside reporting.

### ADR-027: Changed-File Task Affinity via Per-Task Path Globs (#224)
- **Date**: 2026-10-20
- **Status**: Accepted
- **Decision**:
  1. **Manifest Path Globs (`ManifestTask.paths`)**: Discovered evaluation tasks optionally declare a list of repository-relative glob patterns (`tasks[].paths: []string`, e.g. `["docs/**", "evals/docs_*"]`) indicating the repository paths the task is sensitive to. Globs support `**` and `{a,b}` via `github.com/bmatcuk/doublestar/v4`, normalized with stripped leading `./`, forward slashes, and case-sensitivity. Globs are bounded by manifest validation limits (`MaxTaskPaths = 100`, `MaxPathLength = 1024`). Empty or omitted `paths` implies sensitivity to all files (`always`); repository workdirs do not imply path boundaries.
  2. **Controller Changed-File Fetching**: For Comparisons specifying `spec.pullRequest`, the controller calls GitHub's `GET /repos/{owner}/{repo}/pulls/{number}/files` using existing controller credentials (no webhook credentials). Results are paginated and capped at 300 files (`MaxPullRequestFiles`). Beyond 300 files, all tasks declaring paths are treated as `matched`. Changed files are fetched at most once per `(headSHA, baseSHA)` pair and recorded in `status.affinity {filesHash, filesCount, truncated}`; the raw file list is never stored in etcd.
  3. **Per-Task Affinity Labels**: Each evaluated task is classified as `always` (no paths declared), `matched` (one or more path globs matched a changed file, or cap exceeded), or `unmatched` (globs declared but none matched changed files). Recorded in `status.tasks[].affinity` and summarized in `status.summary.affinityMatched`.
  4. **Scheduling and Pruning Modes (`affinityMode`)**: Configured cluster-wide via ConfigMap key `affinityMode` (default: `"order"`):
     - `order` (default): Stable sorts each batch's `spec.tasks` so that `matched` and `always` tasks are evaluated first, followed by `unmatched` tasks.
     - `prune`: Drops `unmatched` tasks from batch execution and comparison evaluation; pruned tasks are reported as skipped with reason `skipped (unmatched): <tasks>` in `status.message` and counted in `status.summary.skipped`.
     The seed batch created by the webhook still runs every discovered task (since the manifest is not yet discovered when seed Runs are enqueued); prune and order apply to subsequent batches (batch 2 onward) scheduled by the controller. Comparisons without `spec.pullRequest`, or where no tasks declare `paths`, remain completely unchanged.
  5. **Fault Tolerance and Fallback**: If GitHub's pull request files API returns an error (e.g. 403, 404, 500), the controller logs the failure once, records `status.affinity.fallbackReason` and caches the fallback to avoid calling GitHub on every reconcile, and gracefully falls back to `order` mode with all tasks assigned `always` affinity, avoiding pipeline breakage.
  6. **Surfaces**: An `affinity` column is displayed in the PR comment table (`RenderComparisonMarkdown`) and the UI Comparison view when task affinity is present. PR Comparisons also surface the count of matched tasks (`N matched`).
- **Rationale**:
  Implements the first step of ROI-driven task selection (Concept §4.3). Prioritizes evals relevant to code changes under test without complex learning or historical tracking, while preserving full backward compatibility for repositories without declared paths.

### ADR-028: Default-Deny Egress NetworkPolicy for eval-runner-sandbox (#33)
- **Date**: 2026-10-21
- **Status**: Accepted (egress ownership migrated to controller in #298)
- **Decision**:
  1. **Dual NetworkPolicy Architecture**: Enforce ingress and egress controls for sandboxes labeled `evalrunner.gke-labs.dev/sandbox: "true"`. The ingress policy (`eval-runner-sandbox-ingress`) remains a static manifest in `k8s/sandbox-networkpolicy.yaml`, while ownership of the egress policy (`eval-runner-sandbox-egress`) is transferred to the controller to enable dynamic hostname narrowing without allow-union bypasses.
  2. **Default-Deny Egress PolicyType**: Configure `policyTypes: [Egress]`, blocking all outbound traffic from sandboxes unless explicitly allowed by egress rules.
  3. **Cluster DNS Allowance**: Allow UDP and TCP port 53 to CoreDNS/kube-dns/node-local-dns pods in `kube-system` (`k8s-app In [kube-dns, node-local-dns]`) and link-local DNS endpoints (`169.254.20.10/32` NodeLocal DNSCache on Autopilot/Cloud DNS, and `169.254.169.254/32` Cloud DNS data plane on the metadata server).
  4. **Git Protocol and HTTP/HTTPS Internet Allowance**: Allow TCP ports 80, 443, and 9418 to external IPs via `ipBlock: 0.0.0.0/0` with RFC1918, CGNAT, and link-local CIDR exceptions `[10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10, 169.254.0.0/16]` when FQDN narrowing is absent or disabled. When FQDN narrowing is active, this rule is dropped by the controller.
  5. **Internal Isolation and Metadata Server Block**: In-cluster pod traffic is restricted to `app: git-server` on port 9418 only; egress to all other namespaces (including controller endpoints in `eval-runner`) is blocked by default deny. Node IPs, private control-plane endpoints, and the GCE metadata server (`169.254.169.254:80`, preventing ADC token acquisition) are blocked by the CIDR exceptions.
  6. **Intra-Namespace Fixture Allowance**: Allow intra-namespace egress on port 9418 via a dedicated rule selecting `podSelector: matchLabels: {app: git-server}` so sandboxes can clone from the `git-server` fixture in `eval-runner-sandbox`.
  7. **Enforcement Requirements**: Document that NetworkPolicy enforcement requires a cluster CNI with policy enforcement enabled, such as GKE Dataplane V2 (e.g. `gke-labs-corp-dev`) or Calico. On clusters without policy enforcement (like default kindnet in kind), manifests apply cleanly and rules are validated by unit tests in `pkg/discovery`.
- **Rationale**:
  Hardens the untrusted sandbox environment at the network perimeter. In addition to running without Kubernetes ServiceAccount tokens (`automountServiceAccountToken: false`) and dropping all Linux capabilities, sandboxes are prevented from accessing controller endpoints (:8081 health probe, UI, webhook), internal cluster services, GCE metadata credentials, and arbitrary outbound ports.

### ADR-029: GKE Dataplane V2 FQDNNetworkPolicy for Narrowed Sandbox HTTPS Egress (#283, #298)
- **Date**: 2026-10-21
- **Status**: Accepted
- **Decision**:
  1. **Dynamic GKE FQDNNetworkPolicy**: On clusters running GKE Dataplane V2 (`networking.gke.io/v1alpha1`), the controller dynamically renders and manages an `FQDNNetworkPolicy` (`eval-runner-sandbox-fqdn-egress`) selecting `evalrunner.gke-labs.dev/sandbox: "true"` in `eval-runner-sandbox`, specifying the allowed FQDN hostnames for HTTPS traffic on port 443.
  2. **Configurable Host List with Sane Defaults**: Allowed egress hosts are driven by ConfigMap key `sandboxEgressHosts` in `eval-runner-config` (or CLI flag `--sandbox-egress-hosts`). When unset, it defaults to:
     - Model APIs: `*.googleapis.com`
     - Git hosts: `github.com`, `*.github.com`, `*.githubusercontent.com`
     - Package indexes: `pypi.org`, `*.pypi.org`, `files.pythonhosted.org`, `*.pythonhosted.org`
     Operators can supply a comma-separated list to extend allowed hostnames without code changes, or use the sentinel `none` to disable the FQDN policy.
  3. **Runtime ConfigMap Hot-Reloading**: When `sandboxEgressHosts` is updated in `eval-runner-config`, `ConfigReloader` reconciles the policy without requiring a controller restart.
  4. **Controller Ownership Eliminates Allow-Union Loophole**: Because GKE Dataplane V2 evaluates multiple policies selecting the same Pod as an allow-union, the controller owns both `eval-runner-sandbox-egress` and `eval-runner-sandbox-fqdn-egress`. Whenever the FQDN policy is active, the controller automatically drops the `0.0.0.0/0` rule (allowing TCP 80/443/9418) from `eval-runner-sandbox-egress`, leaving only DNS and git-server. HTTPS traffic on port 443 is permitted solely by the `FQDNNetworkPolicy` for the designated hostnames. Requests to unlisted hostnames (like `https://example.com`) time out and fail.
  5. **Graceful Degradation on Non-GKE Clusters**: If the `FQDNNetworkPolicy` CRD is not present on the cluster (detected via discovery and NoMatch handling), the controller renders the full 3-rule baseline egress policy and skips FQDN policy management without failing startup or reconciliation.
- **Rationale**:
  Eliminates arbitrary HTTPS exfiltration and blocks reachability to unlisted endpoints by removing the allow-union loophole while preserving full portability on clusters without Dataplane V2.

---

### ADR-030: Manifest-Declared Per-Task Cohorts and Pooled Wilson Tracking (#293)
- **Status**: Decided
- **Context**:
  Concept §9.2 and §2.2 left the cohort declaration mechanism open ("Declared in the manifest or by path convention remains explicitly open"). As agent benchmark suites expand over time with new, harder evals (such as customer-reported failures), the overall suite average drops, mimicking a regression even when agent behavior on the original benchmark suite remains unchanged or improves.
- **Decision**:
  1. **Manifest-Declared Cohorts (`tasks[].cohort`)**:
     Tasks declare their cohort explicitly in the `.evals/list` output (`tasks[].cohort: string`, e.g. `"customer-reported"`). When omitted or empty, the cohort defaults to `"default"`.
  2. **Path Convention Rejected**:
     Inferring cohorts from file or directory paths was rejected: repository layout reflects code domain (e.g. `evals/triage/`) rather than evaluation provenance or expansion era (e.g. `customer-reported-q4`). Manifest declaration allows tasks anywhere in the repository to belong to any cohort without brittle filesystem pattern matching.
  3. **Validation**:
     Cohort identifiers are optional strings bounded to $\le 64$ characters (`MaxCohortLength = 64` per issue #293; note that Kubernetes label values cap at 63 characters, so 63 is the practical bound if ever written as a Kubernetes label value) and validated to be label-safe (`^[a-zA-Z0-9]([-_a-zA-Z0-9.]*[a-zA-Z0-9])?$`).
  4. **Run Status Persistence**:
     Discovery persists the manifest in artifact storage. The run controller writes `Run.status.tasks[].cohort` when initializing tasks and recording execution results. This decouples the web UI series view from artifact storage, enabling server-side rendering directly from Kubernetes Run status without loading manifests for every commit.
  5. **Series Cohorts View (`?view=cohorts`) and Pooled Wilson**:
     `/series/{repohash}/{branch}?view=cohorts` tracks mean score per cohort over commits. Within each commit and cohort, successes and samples are pooled across tasks and scored via `stats.PooledWilson` (Wilson score interval over summed counts; method `"pooled-wilson"`). When multiple cohorts exist, lines are rendered for each cohort plus `all` (combined suite), with vertical dashed annotations at the commit where each cohort first appears ("N evals added"). When only `default` exists, a single `default` line is rendered.
  6. **Dual Cohort Semantics**:
     `Run.spec.cohort` continues to function as a run-level identifier and filter (`/series?cohort=`), while `tasks[].cohort` / `status.tasks[].cohort` operates as a task-level grouping within Runs.
- **Rationale**:
  Declarative manifests provide unambiguous provenance, avoid brittle directory structure assumptions, and allow teams to clearly separate suite expansion from regressions.

---

### ADR-031: Inspect Task Discovery Fallback via Hybrid Archive Scan (#313)
- **Status**: Superseded (reverted; `.evals/list` is required; a Python list run via uv is the planned path)
- **Context**:
  Concept §2.3 and architecture §2.3 specified fallback task discovery when a repository lacks a `.evals/list` executable, allowing repositories using the Inspect AI framework to be evaluated with zero manifest configuration.
- **Decision**:
  1. **Hybrid Execution Model**:
     In accordance with ADR-004 (the controller never clones repository code directly), the discovery sandbox archives candidate Python files using `git ls-files -z -- '*.py'` (falling back to `find` excluding `.git`, `.venv`, `node_modules`, and `vendor` bounded to 5,000 files if not a git checkout), and pipes them into `/workspace/out/scan.tar` via BusyBox `tar -cf - -T -` (writing an empty file if no Python files match). The controller fetches `scan.tar` via bounded `sandboxd.ReadFile` (capped at 1 MiB), safely unpacks into a temporary directory verifying path-traversal safety, skipping symlinks, and forcing 0600/0700 file modes, and executes the scanner in Go.
  2. **Trigger Semantics**:
     Absence of `.evals/list` must be confirmed explicitly by running `test -f .evals/list` in the sandbox. Exit 0 indicates presence, proceeding to the normal `.evals/list` execution path (where unreadable or non-executable files fail the Run immediately without fallback). Exit 1 triggers the Inspect scan fallback. Any other exit code or transport/gRPC failure fails the Run immediately. An empty `.evals/list` does not trigger scan fallback.
  3. **Go Line-Based Scanner vs. AST Trade-off**:
     Running a Python AST parser inside the discovery sandbox was rejected to avoid running untrusted Python code or installing heavy Python runtimes in the minimal discovery image. Building a full Python AST parser in Go was rejected as disproportionate complexity. Instead, a line-based Go scanner is used:
     - Candidate files must contain a code-level import of `inspect_ai` (`import inspect_ai` or `from inspect_ai import ...`). Aliases (e.g. `import inspect_ai as ia`) are unsupported.
     - Triple-quoted strings (`"""` and `'''`) track code vs. string context across lines. The code state is determined at the start of each line, and updated across the whole line by scanning quote tokens in order with the first opener fixing the matching closer. This correctly handles multi-line docstrings, mid-line openers (e.g. `return f"""`, `X = """`), and prevents false positive decorator matches inside docstrings.
     - Decorators `@task`, `@task(`, and `@inspect_ai.task` are detected at column 0 only (ignoring class methods and docstring mentions), tolerating trailing `# comments` and tracking multi-line argument balancing until parentheses close, followed by function definition extraction (`def <name>` / `async def <name>`).
  4. **Task Synthesis and Collision Disambiguation**:
     Tasks are synthesized with `id = <relative-dir>/<function>` (or `<function>` if at root), `workdir = <dir of file>`, `spec = <file>@<function>`, `paths = [<dir>/**]` (or `**` if root), no resources, and default cohort. When multiple tasks produce identical IDs, collisions are disambiguated deterministically to `<dir>/<file-stem>/<function>` (or `<file-stem>/<function>` if at root), and recorded in `status.message`.
  5. **Caps & Provenance**:
     The scanner enforces caps of 5,000 files and 200 tasks; reached caps are recorded in `status.message` and the Discovered condition message. Discovered tasks are sorted by ID and validated via `ValidateManifest`. Provenance is tracked with `Run.status.manifestSource` enum `list | inspect-scan`, the stored manifest JSON records `source: "inspect-scan"`, and the web UI displays a one-line note on the Run detail page only when `manifestSource == "inspect-scan"`.
- **Rationale**:
  The hybrid model strictly respects isolation and bounded communication boundaries, avoids executing arbitrary repository scripts during discovery when no manifest is provided, and provides deterministic zero-config evaluation for Inspect projects.

---

### ADR-032: Observations as the Product Unit of Measurement (#322)
- **Status**: Proposed
- **Context**:
  On 2026-10-21, Justin observed: *"I am uneasy about 'runs' being a central unit of observation, because I think the whole point is that we don't do runs, we just run as few individual evals as we can to try to understand how we are doing."* Today the `Run` CRD serves double-duty as both the execution batch primitive and the primary conceptual and navigation unit. This creates friction with the statistical core of eval-runner (sequential sampling, stop when confident, independent per-task convergence, baseline reuse, and changed-file affinity).
- **Decision (Proposed)**:
  1. **The Observation Model**: Define an Observation as an immutable measurement record for 7 coordinates: `(repository, commit, eval, model, harness, sampleRange, time)` with its score, confidence interval (Wilson/Student-t), cost/token accounting, and artifact pointers. Formally map `Run.status.tasks[]` entries as scheduled batches of observations, and `Comparison` as a statistical decision function over two sets of observations.
  2. **Product Navigation Hierarchy**: Re-orient web UI and CLI navigation to follow the evaluation hierarchy: `/{host}/{owner}/{repo}` → `.../series` (or cohorts) → `.../evals/{task}` → `Observations (ticks on the trendline)` → `Samples / Trajectory`. Demote the repository-level "Runs" tab to a secondary "Activity" view, while preserving permanent compatibility for `/runs/{ns}/{name}` and `/{host}/{owner}/{repo}/runs/...` deep links, PR comment links, and `kubectl` / `evalstats` workflows.
  3. **Scheduling and Dimension Proposals**: Specific API architectures (two-tier intent vs. per-eval CRDs), harness declaration, and indexing strategies are proposed with open questions and recommendations in [docs/concept/observations.md](concept/observations.md) §3, §4, and §5.
- **Rationale**:
  Aligns the product information architecture with statistical reality. Decoupling the measurement unit (Observation) from the scheduling container (Run) eliminates lockstep execution waste, clarifies that baselines are reusable across PRs, enables multi-dimensional slicing across models and harnesses, and presents users with questions they actually care about: agent capability trends and PR merge decisions.

---

### ADR-033: Forge Abstraction and Git Remote Implementation (#325)
- **Status**: Decided
- **Context**:
  eval-runner was previously tied directly to GitHub REST APIs and webhook payloads for repository information, branch heads, change requests (PRs), and result reporting. To support arbitrary repository hosting platforms (including github.com, GitLab, Gerrit, and bare git repositories) and decouple evaluation pipelines, Runs, Comparisons, and UI from forge-specific APIs or webhook delivery mechanisms, a general forge abstraction is needed.
- **Decision**:
  1. **The Forge Abstraction (`pkg/forge`)**:
     Define a `Forge` interface representing the source of truth for repository and change request metadata:
     - `Kind() string`: Stable identifier such as `"github.com"` or `"gitremote"`.
     - `Repository(ctx, RepoRef) (*Repository, error)`: Fetches repository metadata (`Ref`, `DefaultBranch`, `CloneURL`).
     - `Branches(ctx, RepoRef) ([]Branch, error)`: Lists branch heads (`Name`, `SHA`).
     - `ChangeRequests(ctx, RepoRef) ([]ChangeRequest, error)`: Lists change requests (`ID`, `Number`, `Title`, `Author`, `Head`, `Base`, `State`, `UpdatedAt`, and optional `MergeSHA`). Forges that discover change requests from refs without access to state APIs (such as `gitremote`) report `State: "unknown"`.
     - `Commit(ctx, RepoRef, sha string) (*Commit, error)`: Fetches commit metadata (`SHA`, `Parents`, `Time`, `Title`).
     - Optional `Reporter` interface: Implemented by forges capable of posting evaluation results back, exposing `UpsertComment(ctx, RepoRef, ChangeRequestID, key string, body string) (int64, error)` and `SetStatus(ctx, RepoRef, sha string, Status) (int64, error)`.
  2. **Canonical RepoRef Types**:
     Move `RepoRef{Host, Owner, Name}`, URL parsing (`ParseRepoURL`, `RepoRefFromOwnerName`), and validation functions into `pkg/forge`. `pkg/ui` aliases `RepoRef` for backwards compatibility.
  3. **Implementations**:
     - `pkg/forge/github`: Wraps `pkg/github.HTTPClient`. Read methods operate without authentication on public repositories; `Reporter` requires token or GitHub App credentials and maps status reporting to check runs (preserving legacy check run conclusion and token-auth fallback behavior).
     - `pkg/forge/gitremote`: Built using pure Go remote ref advertisement via go-git (`git.NewRemote(memory.NewStorage(), ...).ListContext(...)`). It requires no `git` binary, executes no subcommands, and operates in distroless/static images. It discovers the default branch from symbolic `HEAD`, lists branch heads (`refs/heads/*`), ignores tags, and extracts change requests from `refs/pull/*/head` (reporting `State: "unknown"`) and `refs/pull/*/merge` (recording `MergeSHA`) for GitHub remotes. Does not implement `Reporter`.
     - `Registry`: Selects the appropriate forge for a `RepoRef` by host name (`github.com` maps to the GitHub forge; all other hosts fall back to `gitremote`).
  4. **Pure Go Ref Advertisement vs. ADR-004**:
     ADR-004 mandates strict isolation by ensuring the controller never clones repository code directly. go-git's in-memory remote ref listing performs only the initial ref advertisement protocol handshake over network transport (with a default 30-second timeout); it downloads no commit objects or packfiles, unpacks no blobs, and creates no working tree files. This strictly adheres to ADR-004 while removing the need for a `git` binary in minimal distroless controller images.
  5. **Non-Goals**:
     Persistence/storage, background polling loops, GitHub App registration, and UI changes are deferred to subsequent development work.
- **Rationale**:
  The forge abstraction cleanly decouples eval-runner from GitHub-specific transports while preserving sandbox isolation, keeping controller images minimal and static, and enabling generic Git server evaluations.

### ADR-034: Local bbolt Store, Syncer, and Consumer for Forge State
- **Status**: Decided
- **Context**:
  eval-runner requires a durable, local cache of forge metadata (repositories, branches, commits, change requests) and a synchronization engine that transforms forge state into evaluation Runs and Comparisons. Previously, the controller had no local database of forge state, and object reconciliation was tightly coupled with webhook handlers or ad-hoc queries.
- **Decision**:
  1. **Authoritative Forge vs. Local Cache**:
     The remote forge is authoritative; the local bbolt database is strictly a cache. The store can be dropped, rebuilt, or wiped at any time without losing system correctness.
  2. **Storage and In-Memory Indexes (`pkg/forge/store`)**:
     Data is persisted in a local bbolt file with separate buckets (`repos`, `branches/<repo>`, `commits/<repo>`, `changes/<repo>`, `sync/<repo>`, `meta`). In-memory indexes (repos, commits by SHA, branch heads newest-first, open change requests) are rebuilt on open in milliseconds (~14 ms for 10,000 commits) and maintained across batch updates.
  3. **File Lifecycle and Robustness**:
     The bbolt database is opened with a page-size-aware sanity check that reads meta page 0, derives the actual database page size, reads two meta pages (2*pageSize bytes), and verifies page allocation against file size. If the file is truncated, contains garbage, has a schema version mismatch, or fails opening, it is safely moved aside to `<path>.corrupted.<timestamp>`, logged once, and a clean fresh store is opened.
  4. **File Storage on emptyDir and Restart Re-sync Cost**:
     The database file is configured via the `--forge-db` flag (default `/tmp/forge.db`). In Kubernetes deployments, `/tmp` is backed by an `emptyDir` volume, meaning the database cache is ephemeral and recreated on pod restarts. The re-sync cost on restart for N tracked repositories is bounded to N × (1 repository call + 1 branches call + 1 change requests call + 1 commit call per unknown SHA). Quantifying this cost: for N=10 repositories with ~100 open PRs, a cold restart initiates ~30 API calls taking ~500 ms wall-clock time over REST, consuming <1% of the 5,000 req/h GitHub App rate limit budget. For N=100 repositories with ~1,000 open PRs, it initiates ~300 API calls taking ~5-10 s wall clock and ~6% of the rate limit budget. In the in-cluster e2e test against gitremote, cold sync completed in 210 ms. Because the consumer is strictly idempotent, re-syncing an empty cache across a pod restart safely reconciles existing Runs and Comparisons without creating duplicate objects.
  5. **Delivery Model (Consume-Before-Commit)**:
     The syncer operates under a synchronous consume-before-commit delivery model: for each generated event (`BranchAdvanced`, `ChangeOpened`, `ChangeUpdated`, `ChangeClosed`), the event consumer is called synchronously. The store batch is committed to disk only after all events in that sync batch have succeeded. If a consumer error occurs, the commit is skipped, counted as an error, and the error is returned to the caller, ensuring the subsequent sync attempt regenerates the event for at-least-once processing across crashes.
  6. **Single Consumer and Unified Multi-Repo Rule**:
     Object creation is unified in `pkg/forge/consumer`. The controller wires the syncer to the consumer, while the webhook binary creates objects directly through the shared consumer, ensuring byte-identical Runs and Comparisons between webhook and syncer paths. Exactly one exported `IsMultiRepo` rule in `pkg/forge/consumer` is shared across controller and webhook binaries.
  7. **Observability**:
     Metrics are exposed via the controller-runtime Prometheus registry: `evalrunner_forge_syncs_total` (labels: repo, status ["success", "error"]), `evalrunner_forge_events_total` (labels: repo, kind), `evalrunner_forge_errors_total` (labels: repo, phase), and `evalrunner_forge_store_bytes` (gauge). Error tracking is observable via `evalrunner_forge_syncs_total{status="error"}` and `evalrunner_forge_errors_total`.
- **Rationale**:
  Treating the local store as a cache ensures resilience against corruption and crashes. Consume-before-commit guarantees no dropped events without requiring complex distributed queues. A unified consumer guarantees identical Kubernetes object creation across all ingestion paths.

### ADR-035: GitHub Polling Strategy Without Webhooks, Closed-Head Seeding, and Rate Budget Floor (#354)
- **Status**: Decided
- **Context**:
  eval-runner must support monitoring repositories on github.com without requiring inbound webhook connectivity, operating reliably within GitHub REST API rate limits. On October 22, 2026, measurements of the GitHub API and git remote transport revealed crucial properties that govern effective polling:
  1. **Unauthenticated Rate Limits and Conditional Requests**:
     Unauthenticated conditional requests to `api.github.com` still count against the 60 requests/hour IP budget (empirically measured: rate limit remaining decremented 59 → 58 → 57 across responses returning 200, 304, and 304). Conditional requests save rate limit budget only when authenticated with a token or GitHub App (which has a 5,000 req/h budget). Without an authenticated token, `git ls-remote` (pure-Go ref advertisement) is the only free, unmetered lever available.
  2. **Surviving Pull Request Head Advertisements**:
     `github.com` advertises and preserves `refs/pull/N/head` indefinitely over git transport, including for closed and merged pull requests. In production repositories, hundreds of historical pull heads remain advertised (e.g. 163 advertised pull heads in an evaluation repository with only 1 open PR). Naive ref diffing would perceive all historical closed pull heads as newly opened PRs on every tick, triggering catastrophic rate limit exhaustion.
- **Decision**:
  1. **Three-Tier Polling Engine**:
     - **Free Ref Discovery (`git ls-remote`)**: Every 60s per repository (randomized with ±20% symmetric jitter), the syncer queries advertised remote refs using go-git's pure-Go ref advertisement transport over git/https. This protocol download requires no blobs or commit objects (ADR-004, ADR-033) and consumes zero REST API rate limit quota.
     - **Fill-In API Queries**: REST API calls are executed only when advertised refs differ from locally stored state. On fill-in ticks, the syncer fetches only changed refs (new commits or specific PRs via `GET /repos/:owner/:repo/pulls/:number`). `Repository()` metadata and the full `/pulls` list are never fetched on fill-in ticks.
     - **Hourly Full Reconcile**: Periodic full reconciliation runs once per hour (configurable via `fullReconcileInterval`), querying repository metadata, branches, and open pull requests using conditional HTTP headers (`If-None-Match` and `If-Modified-Since`).
  2. **Closed-Head Seeding and Ref Diffing**:
     - Remote advertised refs are diffed against all stored state in the local bbolt store, keyed by PR number and head SHA. Tags (`refs/tags/*`), `HEAD`, and merge refs (`refs/pull/*/merge`) are ignored.
     - On the initial full reconcile, the syncer seeds placeholder closed change requests for all advertised `refs/pull/N/head` references that were not returned in the open pull request list. Because all historical closed PRs are recorded in the store with their current head SHAs, subsequent ticks compare against the full set and trigger zero API calls when refs are unchanged.
  3. **Batch-Coupled Validator Persistence**:
     HTTP cache validators (ETag and Last-Modified) travel with the sync batch. The forge stages pending validators during API requests, returning them to the syncer only when the batch is assembled. The syncer writes validators to the bbolt sync bucket (`url:<endpoint>`) atomically within `SaveSyncBatch` only after the event consumer has successfully processed all batch events. If the consumer fails, pending validators are cleared, the store is not updated, and the subsequent tick refetches fully without conditional 304 skipping.
  4. **Rate Limit Budget Floor Protection**:
     `X-RateLimit-Remaining`, `X-RateLimit-Limit`, and `X-RateLimit-Reset` headers are parsed on every HTTP response, including 304 Not Modified. If the remaining quota drops below a configurable floor percentage (default 10%, configured via `rateLimitFloorPercent`), API calls are suppressed until the reset window passes, while `git ls-remote` polling continues uninterrupted. State transitions between normal and suppressed states are logged exactly once per transition, and gauges are published to Prometheus (`evalrunner_forge_ratelimit_remaining`, `evalrunner_forge_ratelimit_limit`, `evalrunner_forge_ratelimit_reset_seconds`).
  5. **Startup Credential Resolution and Reporter Client Accessor**:
     Credentials in `cmd/eval-runner` are resolved once at startup with strict precedence: GitHub App installation token > plain personal access token > unauthenticated. Polling uses these credentials from startup and never fails for lack of a token. The shared forge instance is immutable; the comparison reporter accesses the underlying HTTP client via the forge's `Client()` accessor rather than mutating the forge with `SetClient`. Hot-reloaded configuration snapshots interval and floor fields under a read lock (`s.mu.RLock()`), ensuring clean thread safety under `go test -race`.
  6. **Empirically Asserted API Call Counts and Sizing Formula**:
     The unit test suite (`TestSyncer_ExactAPICallSequencePerScenario`) empirically asserts the exact sequence of REST API calls per scenario:
     - **Seeding tick (first sync)**: $3 + B + P$ calls ($3$ for `Repository`, `Branches`, `ChangeRequests`, plus $1$ `Commit` per branch $B$, plus $1$ `Commit` per open PR $P$). For a 1-branch repo with 0 open PRs, this is 4 calls (`[Repository, Branches, ChangeRequests, Commit(mainSHA)]`); for a repo with 3 branches and 1 open PR, it is 7 calls.
     - **Unchanged tick**: 0 calls (`[]`)
     - **Branch push**: 1 call (`Commit(newSHA)`)
     - **New branch**: 1 call (`Commit(branchSHA)`)
     - **Branch deleted**: 0 calls (`[]`)
     - **PR opened**: 2 calls (`PullRequest(number)`, `Commit(headSHA)`)
     - **PR synchronised**: 2 calls (`PullRequest(number)`, `Commit(headSHA)`)
     - **Closed PR head moved**: 2 calls (`PullRequest(number)`, `Commit(headSHA)`) / 0 events (`[PullRequest, Commit]`, head SHA updated in store without events)
     - **Hourly reconcile**: 3 calls (`Repository`, `Branches`, `ChangeRequests`, returning 304s when unchanged)
     - **Restart cost**: Because `hasEverSynced` is maintained in-memory, the first tick after a controller restart performs a full conditional reconcile costing 3 calls per repository (`Repository`, `Branches`, `ChangeRequests`), which count against unauthenticated rate limits.
- **Rationale**:
  Diffing against all stored change requests with closed-head seeding eliminates false-positive API churn from GitHub's permanent pull-head advertisements. Free `git ls-remote` polling permits responsive change detection (60s) without risking rate limit exhaustion. Sizing rate limit floors and coupling validators atomically with event consumption ensures resilience across crashes and transient network failures.

### ADR-036: Periodic Head Top-Up Runs for Baseline Score Convergence (#419)
- **Status**: Decided
- **Context**:
  When a tracked branch advances, `pkg/forge/consumer` creates a single baseline Run `base-<sha7>` evaluating an initial seed batch (`1..defaultBatchSize`, typically 8 samples). When main is quiet, the head commit remains at this 8-sample seed batch indefinitely. Confidence interval (CI) width scales inversely with sample size; leaving baseline commits at 8 samples results in wide confidence intervals for baseline scores. Because PR comparisons pair candidate PR samples against baseline samples at the base commit, noisy baseline estimates degrade PR verdict certainty ("CI stands for confidence interval: per-PR verdicts only get better if the baseline they are paired with is well estimated").
  The web UI already aggregates and pools every completed Run at the same commit into a single estimate via Wilson score interval pooling across all task observations (`pkg/pooling`, `pkg/ui/repo_observations.go`), with series pages selecting Runs by the `base-` name prefix and branch label. Additional head Runs require no UI schema changes.
- **Decision**:
  1. **Controller-Side Top-Up Loop**:
     Introduce a periodic `HeadTopUpRunner` in `pkg/controller/head_topup.go` running every `headTopUpInterval` (ConfigMap key, default `0` = off/disabled; reloadable).
  2. **Newest Head Selection**:
     For each tracked repository x baseline branch, query cluster `base-*` Runs (labels `target-repo`, `branch`, `head-sha`, and annotation `evalrunner.gke-labs.dev/commit-time`) to determine the newest branch-head commit. Cluster state is sufficient; no forge API calls are made. Only the newest head per branch receives top-ups; older commits keep whatever observations they already have.
  3. **Stop Conditions**:
     Pool existing completed observations at the newest head commit using shared pooling logic (`pkg/pooling.PoolCommitRuns`). Skip the commit if any of the following hold:
     - Pooled overall 95% Wilson CI half-width is `<= headTargetCIHalfWidth` (ConfigMap key, default `0.05`).
     - Evaluated samples for any task at that commit reaches `>= headMaxSamplesPerTask` (ConfigMap key, defaults to `defaultMaxSamplesPerTask`, typically 64).
     - Accumulated evaluation spend reaches or projected next batch spend exceeds `defaultBudgetUSD`.
  4. **In-Flight Safety**:
     Skip top-up if any Run at that commit is still in-flight (`Pending`, `Discovering`, `Running`), ensuring at most one in-flight top-up per commit.
  5. **Run Creation and Window Progression**:
     Create an incremental Run named `base-<sha7>-<n>` where n >= 2 is the next free index, copying labels, annotations, model, and cohort from the seed Run, recording the top-up epoch in label `evalrunner.gke-labs.dev/topup: "<n>"`.
     Evaluate the next window of `defaultBatchSize` samples (`Start: prevEnd + 1, End: prevEnd + batchSize`). If the previous run evaluated fewer samples than requested (dataset exhausted), wrap back to window `1..batchSize`; re-sampling the same inputs is a valid new observation because LLM evaluation is stochastic.
- **Rationale**:
  Automating bounded top-ups when branches are quiet operationalizes "bound the confidence interval, not the budget" (concept functionality §4). Reusing the `base-` naming prefix and labels seamlessly feeds the UI's existing commit pooling without schema migrations. Defaulting to disabled preserves zero change in behavior on existing clusters until explicitly configured.

### ADR-037: Re-Score Comparisons and Post Fresh Check Runs on Head Moves (#436)
- **Status**: Decided
- **Context**:
  When a pull request gets a new head commit, the forge consumer creates a new candidate Run and updates `spec.candidate.run` and `spec.pullRequest.headSHA` on the existing PR Comparison. Previously, the Comparison reconciler treated terminal phases (`Completed`, `Failed`) as inert (unless `spec.extraSamples` grew), causing the Comparison to stay `Completed` with the prior commit's results, summary, and `completionTime`. Furthermore, no new GitHub check run was created on the second SHA, leaving the new commit without eval status on GitHub, while the PR comment continued displaying stale results from the first head commit.
- **Decision**:
  1. **Head-Move Detection**: At the top of `Reconcile` (before TTL check and terminal phase short-circuiting), compare `spec.candidate.run` and `spec.baseline.run` against `status.observedCandidateRun` and `status.observedBaselineRun` (falling back to batch 1 Runs if unpopulated). Record observed runs upon status updates.
  2. **Scoring State Reset**: Reset scoring state through `updateComparisonStatus` using `RetryOnConflict`: clear `phase`, `verdict`, `summary`, `batches`, `completionTime`, `extraSamplesApplied`, and per-task results. Retain `status.reporting.commentID` so the single PR comment is updated in place, while clearing `lastPostedHash` and annotation flags.
  3. **Per-Commit Check Runs**: Record check run IDs in `status.reporting.checkRunIDs[sha]`. On candidate head moves, create a fresh check run on the new head commit immediately in `in_progress` status, avoiding overwriting new head state with late updates from prior commits. For baseline-only moves (base branch advances, same candidate head), preserve the existing check run ID on the head SHA and PATCH it back to `in_progress`.
  4. **Extension Run Cleanup and Deletion Handling**: List all extension Runs with label `evalrunner.gke-labs.dev/comparison: <name>` and delete them. In `ensureBatchRun` and the batch-discovery loop, treat Runs with non-nil `deletionTimestamp` as not-found and requeue (`RequeueAfter`) so terminating extension Runs holding finalizers are not re-adopted against the new head.
  5. **Baseline Run Preservation and Pairing**: Do not re-run or duplicate the baseline Run when only the candidate changes; re-pair candidate samples against the existing complete baseline Run.
  6. **Missing Run Handling**: When a newly referenced candidate or baseline Run is not yet present on the cluster, pass `nil` (rather than a zero-value struct) to renderers and report waiting for the Run to be created.
  7. **Preserve Extra Samples Semantics**: Ensure review-comment commands (`/eval more`) and `spec.extraSamples` work identically after a reset.
- **Rationale**:
  Retaining the single PR Comparison resource per PR number matches forge consumer idempotency and keeps PR discussion clean with a single updated comment. Keying check runs by head SHA aligns eval status with GitHub's commit-centric check run model, guaranteeing that every pushed commit reflects accurate, fresh evaluation progress and results.
