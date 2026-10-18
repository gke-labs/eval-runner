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
- **Rationale**:
  Fulfills Concept §4.2 and §4.5 ("bound the confidence interval, not the budget"). Identical runs (or large regressions/improvements) reach statistical certainty quickly within a few batches and terminate early, saving significant evaluation tokens and compute, while noisy or close comparisons gather additional evidence up to the task sample cap.







