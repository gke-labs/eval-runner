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





