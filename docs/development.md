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

# Development Guide

This document describes how to set up your local development environment, deploy eval-runner, and run tests.

## Prerequisites

- **Go**: 1.27 or newer
- **Docker**: Docker daemon running (locally or via `DOCKER_HOST`). (The `buildx` plugin is only required if building multi-arch images with `ap`.)
- **Git**
- **Kubernetes cluster with agent-sandbox**: `agent-sandbox` is a prerequisite for running `eval-runner`. On any cluster where `eval-runner` is deployed, install the pinned `v1.0.7` release:

  ```bash
  kubectl apply -f https://github.com/kubernetes-sigs/agent-sandbox/releases/download/v1.0.7/sandbox.yaml
  ```

  The controller will verify that the `Sandbox` CRD (`agents.x-k8s.io/v1beta1`) is present upon startup and fail fast with an error message if it is missing.

### Upgrading the `sandboxd` Image

`eval-runner` injects `sandboxd` into discovery sandboxes via an initContainer using the image reference pinned by digest in `pkg/discovery/sandbox.go` (`SandboxdImage` constant).

To upgrade or repin the `sandboxd` image:
1. Locate the target release or staging image from [agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox).
2. Inspect the multi-architecture manifest list using `docker buildx imagetools inspect <image-ref>` to verify available platforms and obtain its exact `@sha256:` digest.
3. Update `SandboxdImage` in `pkg/discovery/sandbox.go` with the pinned digest reference.
4. Run `ap test` and `ap e2e` to verify.

### Working Against the `agent-sandbox` Fork

For mTLS support between the controller and `sandboxd`, `eval-runner` relies on a prototype fork of `kubernetes-sigs/agent-sandbox` (`github.com/codebot-robot/agent-sandbox` on branch `sandboxd-mtls`).

#### Local Development with `go.work`
For local multi-module development, a `go.work` file at the repository root points to a sibling checkout:
```bash
# Sibling checkout
cd /workspaces
git clone https://github.com/codebot-robot/agent-sandbox.git
cd agent-sandbox
git checkout sandboxd-mtls

# In eval-runner:
go work init . ../agent-sandbox
```
`go.work` and `go.work.sum` are git-ignored so local workspace configurations do not affect CI.

#### CI Builds via `replace`
In `go.mod`, a pinned `replace` directive resolves `sigs.k8s.io/agent-sandbox` to the fork commit on GitHub so CI builds succeed without requiring a multi-module workspace:
```
replace sigs.k8s.io/agent-sandbox => github.com/codebot-robot/agent-sandbox <commit>
```

#### Building the `sandboxd` Image
To build the `sandboxd` image from the fork:
```bash
./dev/tasks/build-sandboxd
```
Pass `--push` to push the image to a container registry, or pass `IMAGE_PREFIX` and `IMAGE_TAG` to customize the tag.
Pass `--clone-only` to prepare the checkout without building the image.

`dev/tasks/build-sandboxd` uses a sibling checkout (`../agent-sandbox`) if present, or clones the pinned commit outside the repository into `${XDG_CACHE_HOME:-~/.cache}/eval-runner/agent-sandbox` (configurable via `AGENT_SANDBOX_DIR`). The fork is intentionally never cloned under the repository tree.

For production and multi-platform releases (`linux/amd64,linux/arm64`), maintainers build and push the image using:
```bash
git clone https://github.com/codebot-robot/agent-sandbox.git && cd agent-sandbox && git checkout 0d9e0d2e62d1
docker buildx build --platform linux/amd64,linux/arm64 -f packages/sandboxd/Dockerfile \
  --build-arg GIT_VERSION=v1.0.7-mtls --build-arg GIT_SHA=0d9e0d2e62d1 \
  -t gcr.io/justinsb-knotai-dev/sandboxd:0d9e0d2 --push .
```
Note: Pushing the multi-arch image to the registry is a maintainer step whenever the fork commit changes. After pushing, update the pinned image digest in `pkg/discovery/sandbox.go`.

### CA Bootstrap and Rotation Procedure

The controller maintains a self-signed CA that issues the controller's client certificate and per-sandbox server certificates:
1. **Initial Bootstrap**: At startup, the controller checks for Secret `sandbox-ca` in `eval-runner`. If absent, it generates an ECDSA P-256 keypair and self-signed CA certificate, stores both CA and client keypairs in `eval-runner/sandbox-ca`, and synchronizes the public CA certificate to ConfigMap `sandbox-ca` in `eval-runner-sandbox`.
2. **Restart Idempotency**: Controller restarts read the existing `sandbox-ca` Secret and verify the ConfigMap without regenerating certificates.
3. **Manual CA Rotation Procedure**:
   - **Important**: Because sandboxd instances load the CA certificate pool once at startup and the controller verifies sandbox server certificates against the active CA, rotating `sandbox-ca` breaks mTLS handshakes with in-flight sandboxes (the controller's new client certificate will not be trusted by existing sandboxd daemons, and existing sandboxes' server certificates will not chain to the controller's new CA). Therefore, in-flight Runs will fail upon CA rotation. Operators should allow active Runs to complete (or drain/delete active Runs) before rotating the CA.
   - Delete or replace Secret `eval-runner/sandbox-ca` with a newly minted CA keypair and client keypair.
   - Restart the controller deployment:
     ```bash
     kubectl rollout restart deployment/eval-runner-controller -n eval-runner
     ```
   - On startup, the controller updates ConfigMap `eval-runner-sandbox/sandbox-ca` with the new public CA cert.
   - All subsequent sandboxes created by the controller mount the updated ConfigMap and receive server certificates signed by the new CA.

Tools like `kind` and `kubectl` do not need to be installed beforehand for testing; test scripts can download and manage them automatically under `.build/bin/`.

## Project Automation (`ap`)

This repository uses [`ap` (autoproject)](https://github.com/gke-labs/gke-labs-infra/blob/main/ap/llms.txt) for project automation, manifests, and CI generation.

Common commands:

- `ap generate`: Formats code, stamps copyright headers, runs CRD generation tasks, updates CI presubmit scripts (`dev/ci/presubmits/`), and regenerates `.github/workflows/ci-presubmits.yaml`.
- `ap lint`: Runs Go linters (`govet`, `gofmt`, etc.).
- `ap test`: Runs unit tests (`go test`).
- `ap build`: Builds container images defined under `images/`.
- `ap deploy`: Builds and deploys images and Kubernetes manifests from `k8s/` into the active cluster.
- `ap e2e`: Discovers and executes all end-to-end tests.

## Generating CRDs and DeepCopy

CRD manifests under `k8s/` and Go `runtime.Object` deepcopy methods are generated using `controller-gen`.

Run:

```bash
./dev/tasks/generate-crds
```

This task is also invoked automatically whenever `ap generate` is executed.

## Deploying to a Cluster (`ap deploy`)

To deploy `eval-runner` into a target Kubernetes cluster (with `agent-sandbox` installed), deploys must use a unique image tag rather than `latest`. Deploying with `latest` leaves the Deployment manifest byte-identical when updating an existing installation, causing `kubectl apply` to report `unchanged` and leaving the cluster silently running the previous digest.

The recommended way to deploy is using the `./dev/tasks/deploy` wrapper, which automatically sets `IMAGE_TAG` from git (short SHA, plus a `-dirty` suffix when the working tree is dirty), invokes `ap` at the repository-pinned version via `go run` (never from `PATH`), verifies that the pushed multi-platform index contains `linux/amd64`, and applies the manifests:

```bash
IMAGE_PREFIX=<registry> ./dev/tasks/deploy
```

For example, on Google Kubernetes Engine (GKE):

```bash
IMAGE_PREFIX=gcr.io/my-gke-project ./dev/tasks/deploy
```

Alternatively, invoke `ap deploy` directly by setting a unique tag:

```bash
IMAGE_TAG=$(git rev-parse --short HEAD) IMAGE_PREFIX=<registry> ap deploy
```

(`ap` honours `IMAGE_TAG`.)

`./dev/tasks/deploy` will:
1. Build the `eval-runner` container image (for `linux/amd64` and `linux/arm64`, pinned in `.ap/images.yaml`).
2. Tag and push the multi-platform image index to `<IMAGE_PREFIX>/eval-runner:<IMAGE_TAG>`.
3. Inspect the pushed index with `docker buildx imagetools inspect` and verify it contains `linux/amd64`, failing immediately before applying manifests if absent.
4. Apply the manifests in `k8s/` in order (`00-namespaces.yaml`, `evalrunner.gke-labs.dev_runs.yaml`, `rbac.yaml`, `sandbox-networkpolicy.yaml`, `runner-deployment.yaml`).
5. Substitute the `eval-runner` image reference in the deployment with the full pushed image reference (`<IMAGE_PREFIX>/eval-runner:<IMAGE_TAG>`).
6. Wait for the CRD to be established and the deployment to become available.

### Verifying the Running Digest

To verify which image digest is actively running in the cluster:

```bash
kubectl -n eval-runner get pods -o jsonpath='{.items[*].status.containerStatuses[*].imageID}'
```

### Recovery: Manual Rollout Restart

If a cluster was previously deployed with `latest` (or an unchanged tag) and did not roll automatically, restart the deployment to force Kubernetes to pull and start updated pods:

```bash
kubectl -n eval-runner rollout restart deployment/eval-runner-controller
```

### Repository Isolation and Manifest Guards

`ap` automatically discovers all `k8s/` and `images/` directories located anywhere under the repository tree to determine manifests to apply and container images to build.

**Rule**: Nothing under the repository tree may contain a `k8s/` or `images/` directory that is not ours.

**Why**: If an external project or fork (such as `agent-sandbox`) is cloned under the repo tree (e.g. under `.build/`), `ap deploy` will discover and attempt to apply all of its manifests (such as `controller.yaml` and CRD definitions) to the target cluster. In the case of `agent-sandbox`, this can roll the cluster's `agent-sandbox-controller` Deployment to an unresolvable image reference and fail CRD reconciliation, breaking sandbox management across the cluster.

Both `./dev/tasks/deploy` and `./dev/tasks/test-deploy` enforce this by asserting that `ap deploy --dry-run` lists only the manifests from this repository's `k8s/` directory and rejecting any foreign manifest paths or directories.

#### Operator Recovery Note

If a deployment was accidentally performed with foreign `agent-sandbox` manifests in the tree, rolling `agent-sandbox-controller` to an unresolvable image, operators can recover the deployment on the cluster with:

```bash
kubectl -n agent-sandbox-system rollout undo deployment/agent-sandbox-controller
```

## Creating a Run by Hand

Once the controller is deployed, you can create a `Run` resource targeting any git repository:

```bash
kubectl apply -f - <<EOF
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Run
metadata:
  name: test-run
  namespace: eval-runner
spec:
  repo:
    url: https://github.com/my-org/my-repo.git
    commit: abc1234def5678...
EOF
```

### Inspecting Runs

View the status and phase of runs:

```bash
kubectl get runs -n eval-runner
```

View detailed status, conditions, and the discovered manifest:

```bash
kubectl get run test-run -n eval-runner -o yaml
```

### Web UI (`eval-runner-ui`)

`eval-runner` includes a read-only web view (`eval-runner-ui`) that displays Runs and Comparisons with per-task success rates, 95% confidence interval bars, and PR-comment comparison tables.

#### Port-Forwarding to the UI

To access the UI running in your cluster:

```bash
kubectl -n eval-runner port-forward svc/eval-runner-ui 8080:80
```

Then open your browser:
- `http://localhost:8080/runs`: view all Runs in the namespace with their phase, task completion, sample counts, and rates.
- `http://localhost:8080/runs/<namespace>/<name>`: view detailed Run statistics with per-task rates, 95% CI bars against a 0–1 axis, sample counts, estimation method, and artifact pointers.
- `http://localhost:8080/comparisons/<namespace>/<name>`: view paired comparison results in the PR-comment table shape with Δ CI bars drawn against a zero line, verdict chip, P(improvement), and base/candidate commits.
- `http://localhost:8080/comparisons`: view all Comparisons in the namespace.

#### Running the UI Locally

You can also run the web UI binary locally against your active Kubernetes context:

```bash
go run ./cmd/eval-runner-ui --namespace=eval-runner --listen-addr=:8080
```

#### Security Architecture

- **No Custom Auth**: The UI has no authentication mechanism of its own. Access is restricted to `kubectl port-forward` (authenticated via Kubernetes RBAC) or cluster-internal Services.
- **Zero Secrets**: Nothing rendered in the UI is a secret. Model credentials and tokens never appear in custom resources or templates; the UI only displays public metadata and pointers.
- **Least-Privilege RBAC**: The UI ServiceAccount (`eval-runner-ui`) has read-only (`get`, `list`, `watch`) access exclusively to `runs` and `comparisons` custom resources in the `eval-runner` namespace.
- **PSA Restricted**: The UI Deployment runs as a non-root distroless container with a read-only root filesystem and all capabilities dropped, fully compliant with Pod Security Standards `restricted`.

### Inspecting Sandboxes in `eval-runner-sandbox`

During discovery, the controller creates an isolated sandbox in the `eval-runner-sandbox` namespace:

```bash
# View active sandboxes
kubectl get sandboxes -n eval-runner-sandbox

# View backing discovery pods
kubectl get pods -n eval-runner-sandbox

# View logs from a discovery container
kubectl logs -n eval-runner-sandbox discovery-test-run -c agent
```

Once discovery completes (or fails), the sandbox is automatically deleted.

### Running Sample Evaluations with a Real Model (e.g. Gemini on GKE)

To execute real evals against a repository, provide `spec.model` on the `Run`. For cloud models (such as Gemini), provider credentials are provided via a Kubernetes Secret in the sandbox namespace:

1. **Ensure the Model Secret exists in `eval-runner-sandbox`:**

   The controller defaults `--model-secret` to `model-credentials` and treats it as optional-by-presence: at startup and on each task launch, the controller checks whether the Secret exists in `eval-runner-sandbox`. If present, sample sandboxes receive it automatically via `envFrom`. If absent, sample sandboxes run without credentials, and tasks requesting a non-mock model record a clear message explaining how to configure credentials.

   Our GKE dev cluster uses `eval-runner-sandbox/model-credentials` containing key `GEMINI_API_KEY`. If creating it from scratch:

   ```bash
   kubectl -n eval-runner-sandbox create secret generic model-credentials \
     --from-literal=GEMINI_API_KEY="${GEMINI_API_KEY}"
   ```

   *(Note: This is an interim credential injection mechanism that the model gateway will replace. To use a different secret name, pass `--model-secret=<name>`; to explicitly disable credential injection, pass `--model-secret=""`.)*

2. **Create the Run:**

   Because the controller defaults `--model-secret` to `model-credentials` and checks for its presence automatically, no manual deployment arg patch is required after deploying. Create the Run directly:

   ```bash
   kubectl apply -f - <<EOF
   apiVersion: evalrunner.gke-labs.dev/v1alpha1
   kind: Run
   metadata:
     name: docs-comprehension-gemini
     namespace: eval-runner
   spec:
     repo:
       url: https://github.com/gke-labs/eval-runner.git
       commit: $(git rev-parse HEAD)
     model: google/gemini-3.5-flash-lite
     tasks:
       - docs/readme_comprehension
       - docs/readme_comprehension_blind
     limit: 10
   EOF
   ```

3. **Watch and View Results:**

   Watch the run progress from `Pending` -> `Discovering` -> `Running` -> `Completed`:

   ```bash
   kubectl get run docs-comprehension-gemini -n eval-runner -w
   ```

   View per-task scores and aggregates once completed:

   ```bash
   kubectl get run docs-comprehension-gemini -n eval-runner -o yaml
   ```

#### Provider Dependencies and Custom Images

The default sample runner sandbox image (`images/sample-runner`) includes Inspect AI alongside pinned provider dependencies for common model providers out of the box:
- `google-genai` (for Gemini models, our default)
- `openai`
- `anthropic`

For evaluations requiring other model providers or custom environments, a repository can declare custom images in `.evals/list` using the manifest's top-level `images[]` declarations and `tasks[].images[]` references (see concept §2.2 in `docs/concept/functionality.md`).

## Results in GCS

By default, the controller runs with an in-memory no-op artifact sink. When `artifactBucket` is configured (via the `eval-runner-config` ConfigMap or the `--artifact-bucket` flag), the controller persists raw Inspect evaluation JSON logs alongside a per-task aggregate `summary.json` directly to Google Cloud Storage under `runs/<commit>/<task-id>/`.

Writes happen exclusively from the controller pod using Application Default Credentials (ADC). Sandboxes never hold bucket credentials or cloud service account tokens (zero-trust architecture).

### One-Time Cloud Setup

To grant the controller permissions to write artifacts to a GCS bucket using GKE Workload Identity:

1. **Create the Google Service Account (GSA):**

   ```bash
   gcloud iam service-accounts create eval-runner-controller \
     --project=<project> \
     --description="Service account for eval-runner controller artifact uploads"
   ```

2. **Grant `roles/storage.objectAdmin` on the target bucket:**

   ```bash
   gcloud storage buckets add-iam-policy-binding gs://<bucket> \
     --member="serviceAccount:eval-runner-controller@<project>.iam.gserviceaccount.com" \
     --role="roles/storage.objectAdmin"
   ```

3. **Bind the Kubernetes ServiceAccount (KSA) via Workload Identity:**

   Allow the `eval-runner/eval-runner-controller` Kubernetes ServiceAccount to impersonate the Google Service Account:

   ```bash
   gcloud iam service-accounts add-iam-policy-binding eval-runner-controller@<project>.iam.gserviceaccount.com \
     --project=<project> \
     --role="roles/iam.workloadIdentityUser" \
     --member="serviceAccount:<project>.svc.id.goog[eval-runner/eval-runner-controller]"
   ```

4. **Annotate the Kubernetes ServiceAccount:**

   Annotate `eval-runner-controller` in the `eval-runner` namespace with the GSA email:

   ```bash
   kubectl -n eval-runner annotate serviceaccount eval-runner-controller \
     iam.gke.io/gcp-service-account=eval-runner-controller@<project>.iam.gserviceaccount.com \
     --overwrite
   ```

   *(Note: Manifests remain project-agnostic and do not include the GCP project annotation so that repeated `ap deploy` runs do not overwrite cluster annotations. `kubectl apply` leaves existing annotations in place.)*

### Configuring the Cluster ConfigMap

Per-cluster settings (such as the artifact bucket) live in a ConfigMap named `eval-runner-config` in the `eval-runner` namespace, created once by the operator. Because deployment manifests in `k8s/` are project-agnostic, deploys require no argument patching with `kubectl patch`.

Create the ConfigMap once:

```bash
kubectl -n eval-runner create configmap eval-runner-config \
  --from-literal=artifactBucket=gs://<bucket>
```

For our GKE dev cluster (bucket `gs://justinbs-knotai-dev-eval-runner`):

```bash
kubectl -n eval-runner create configmap eval-runner-config \
  --from-literal=artifactBucket=gs://justinbs-knotai-dev-eval-runner
```

Or apply as a manifest:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: eval-runner-config
  namespace: eval-runner
data:
  artifactBucket: gs://justinbs-knotai-dev-eval-runner
```

Supported ConfigMap keys include:
- `artifactBucket`: GCS bucket URI (`gs://<bucket>`) or name where Inspect logs and `summary.json` are stored.
- `artifactDir`: Local filesystem directory for storing evaluation artifacts.
- `artifactMaxUploadBytes`: Maximum size in bytes of an uploaded Inspect log before truncation (default `10485760`, 10 MiB).
- `commandUsers`: Comma-separated allow-list of specific GitHub usernames permitted to execute comment commands (e.g. `/eval more <n>`) without checking the GitHub collaborator permission API (exact case-insensitive match; wildcards are not supported).
- `defaultBatchSize`: Default batch size for paired adaptive sampling in webhook-created Comparisons and initial sample window on seed Runs (default `8`).
- `defaultLimit`: Deprecated: previously set sample limit for webhook-created Runs; seed Runs now evaluate paired sample windows (`defaultBatchSize`).
- `defaultMaxSamplesPerTask`: Maximum total samples evaluated per task across all batches in webhook-created Comparisons (default `64`).
- `defaultModel`: Default model identifier for webhook-created Runs (e.g. `mockllm/model`).
- `discoveryDeadline`: Overall deadline for discovery to complete (default `10m`).
- `executionDeadline`: Overall deadline for sample execution to complete (default `30m`).
- `githubAppSecret`: Name of Secret in controller namespace containing GitHub App credentials.
- `githubTokenSecret`: Name of Secret in controller namespace containing GitHub credentials (default `github-token`).
- `githubWebhookSecret`: Name of Secret in controller namespace containing GitHub webhook secret (default `github-webhook`).
- `inconclusivePolicy`: Check-run conclusion policy for INCONCLUSIVE comparisons (`neutral` or `failure`, default `neutral`). When set to `failure`, an INCONCLUSIVE comparison marks the `eval-runner / evals` check run conclusion as `failure` to block PR merging.
- `maxParallelTasks`: Maximum concurrent task sandboxes (default `4`).
- `modelSecret`: Name of Secret in sandbox namespace containing model credentials (default `model-credentials`).
- `sandboxdImage`: Container image used for sandboxd init container (defaults to pinned SandboxdImage if unset).
- `taskGracePeriod`: Grace period before failing a Running task whose sandbox is missing (default `30s`).
- `uiBaseURL`: Base URL for the eval-runner UI (e.g. `https://eval-runner.example.com`) to generate absolute links in PR comments.
- `webhookRepositories`: Comma-separated list of allowed repositories (`owner/name`). If unset or empty, all incoming repositories are rejected (fail-closed). Use `*` to explicitly allow all repositories.

Precedence is: explicit CLI flag > ConfigMap key > built-in default. If the ConfigMap is absent, all built-in defaults are used.

### Empty-Value Semantics and Feature Disabling

Empty values in the ConfigMap (empty string or whitespace-only) are unified across all keys to mean **"unset, use default"**; they do not disable features or clear defaults.

To explicitly disable a feature or credential that has a non-empty built-in default (such as `modelSecret` or `githubTokenSecret`), specify the sentinel value `none`:

```yaml
data:
  # Disables injecting model credentials into sample sandboxes
  modelSecret: "none"
  # Disables posting GitHub comments or check runs
  githubTokenSecret: "none"
```

Note that `sandboxdImage` is required for sandbox initialization and cannot be disabled with `none`. Attempts to set `sandboxdImage: none` will fail configuration validation.

### Unrecognised Keys and Strict Configuration

At startup, the controller inspects `eval-runner-config` and logs an informational warning listing any unrecognised keys (such as typos like `artifactBuckets`).

To protect against misconfigurations by failing startup immediately when unrecognised keys are detected, pass `--strict-config` to the controller (default is off).

## GitHub PR Reporting

When a `Comparison` resource is linked to a pull request via `spec.pullRequest`, the controller automatically posts and updates a single in-place comment on the PR containing the statistical verdict and per-eval results table, alongside a GitHub check run named `eval-runner / evals`.

Both GitHub App authentication (preferred, supporting full check runs) and Personal Access Token (PAT) authentication (comment-only) are supported. When credentials for both are provided, GitHub App authentication is preferred.

### Option A: GitHub App Authentication (Preferred)

Creating and updating GitHub Check Runs via the REST API requires a GitHub App (fine-grained personal access tokens are restricted by GitHub's security model from creating check runs).

1. **Register a GitHub App:**
   In GitHub Organization / User Settings -> Developer Settings -> GitHub Apps -> New GitHub App:
   - **GitHub App name**: e.g. `eval-runner` (or your choice).
   - **Homepage URL**: Your project or UI URL.
   - **Webhook**: Active can be disabled (or configure webhook URL if integrating with `eval-runner-webhook`).
   - **Repository permissions:**
     - **Pull requests**: Read and write (to list, create, and update PR comments)
     - **Checks**: Read and write (to create and update check runs)
   - Install the GitHub App on your target repositories (e.g. `gke-labs/eval-runner`).

2. **Download Private Key & Note IDs:**
   - Under App settings, generate and download a private key (`eval-runner.private-key.pem`).
   - Note the **App ID** on the General settings page.
   - Under Install App, select your account/organization and note the **Installation ID** from the URL (`https://github.com/settings/installations/<installation-id>`).

3. **Create the Secret in the controller namespace:**
   ```bash
   kubectl create secret generic github-token \
     --namespace=eval-runner \
     --from-literal=app-id=123456 \
     --from-literal=installation-id=987654 \
     --from-file=private-key=eval-runner.private-key.pem
   ```

   *(Note: The controller mints RS256 JWTs and exchanges them for GitHub App installation access tokens with automatic 1-hour caching and refresh. The private key and tokens live exclusively in the `eval-runner` controller namespace and are never mounted in sandbox pods.)*

### Option B: Personal Access Token (PAT) Authentication

If you do not have permission to install a GitHub App, a personal access token (PAT) can be used for PR commenting. Check runs will be skipped gracefully with a recorded status message.

1. **Generate a fine-grained GitHub token:**
   In GitHub Settings -> Developer Settings -> Personal Access Tokens -> Fine-grained tokens:
   - Target repository: Select the target repository (e.g. `gke-labs/eval-runner`).
   - Repository permissions:
     - **Pull requests**: Read and write (to list, create, and update PR comments)

2. **Create the Secret in the controller namespace:**
   ```bash
   kubectl create secret generic github-token \
     --namespace=eval-runner \
     --from-literal=token=github_pat_...
   ```

3. **Custom Secret Name (Optional):**
   To use a secret other than `github-token`, configure `githubTokenSecret` (or `githubAppSecret`) in the `eval-runner-config` ConfigMap or pass `--github-token-secret=<secret-name>` (or `--github-app-secret=<secret-name>`).

### Web UI Links in Comments

By default, the web view link in the footer of PR comments renders as code text (e.g. `` `/comparisons/eval-runner/comp-1` ``). To format it as an absolute Markdown link (e.g. `[web view](https://eval-runner.example.com/comparisons/eval-runner/comp-1)`), configure `uiBaseURL` in the `eval-runner-config` ConfigMap or pass `--ui-base-url=<url>`.

### Linking a Comparison to a PR

Specify `spec.pullRequest` on a `Comparison` custom resource:

```yaml
apiVersion: evalrunner.gke-labs.dev/v1alpha1
kind: Comparison
metadata:
  name: docs-eval-same-commit
  namespace: eval-runner
spec:
  baseline:
    run: docs-eval-base-05ce4d8
  candidate:
    run: docs-eval-cand-05ce4d8
  pullRequest:
    repository: gke-labs/eval-runner
    number: 98
    headSHA: 05ce4d8
    baseSHA: a81d0c4
```

### What the PR Comment Looks Like

The comment is updated in-place on each reconcile using a hidden HTML marker (`<!-- eval-runner:comparison:<namespace>/<name> -->`), ensuring zero duplicate comments:

```markdown
<!-- eval-runner:comparison:eval-runner/docs-eval-same-commit -->

## INCONCLUSIVE · P(improvement) = 0.26

| eval | base | this PR | Δ (95% CI) | label | samples |
| --- | --- | --- | --- | --- | --- |
| docs/readme\_comprehension | 1.00 | 1.00 | +0.00 [+0.00, +0.00] | unchanged | 20 |
| docs/readme\_comprehension\_blind | 0.20 | 0.15 | -0.05 [-0.15, +0.00] | inconclusive | 20 |

80 run · 40 paired · paired against base commit 05ce4d8 · `/comparisons/eval-runner/docs-eval-same-commit` · gs://eval-bucket/comparisons/docs-eval-same-commit.json
```

### Check Run State Mapping

The controller creates and updates a check run named `eval-runner / evals` on `headSHA`:
- **Title**: `<VERDICT>: N improved, M unchanged, K regressed, J inconclusive` built from `status.tasks[].label` (omitting zero buckets). For running comparisons, `running · N of M samples` or `running`.
- **Summary**: The existing body plus, for inconclusive tasks, the suggested extra sample hint text (e.g. `- comment `/eval more <n>` to run N more samples`).
- **State and conclusion mapping**:
  - `RUNNING`: status `in_progress`, conclusion `""`
  - `IMPROVED`: status `completed`, conclusion `success`
  - `UNCHANGED`: status `completed`, conclusion `success`
  - `INCONCLUSIVE`: status `completed`, conclusion `neutral` (default) or `failure` if `inconclusivePolicy` is set to `failure`
  - `REGRESSED`: status `completed`, conclusion `failure`

### Browsing and Inspecting Results

Artifacts follow a deterministic layout:
- Raw Inspect log: `gs://<bucket>/runs/<commit>/<task-id>/<log-filename>.json`
- Task summary: `gs://<bucket>/runs/<commit>/<task-id>/summary.json`

List all artifacts for a specific commit:

```bash
gcloud storage ls --recursive gs://<bucket>/runs/<commit>/
```

View the JSON summary for a specific task:

```bash
gcloud storage cat gs://<bucket>/runs/<commit>/<task-id>/summary.json
```

The `summary.json` contains:
```json
{
  "taskId": "docs/readme_comprehension",
  "model": "google/gemini-3.5-flash-lite",
  "commit": "abcdef123456",
  "samples": 10,
  "successes": 8,
  "meanScore": 0.8,
  "startedAt": "2026-10-17T14:52:45Z",
  "finishedAt": "2026-10-17T14:52:47Z",
  "capped": false
}
```

Upload sizes are bounded to 10 MiB; if a log exceeds this limit, the log is uploaded in full up to the cap, and `"capped": true` along with `"capBytes"` are recorded in `summary.json`.

On the `Run` custom resource, `status.artifacts` records the run-level prefix (e.g. `gs://<bucket>/runs/<commit>/`), and `status.tasks[].logPath` points directly to the `gs://` URI of each task's log.

## GitHub Webhook Automation

The `eval-runner-webhook` service receives GitHub webhook events (`pull_request` and `push`), creates candidate and baseline `Run` custom resources, and automatically creates a `Comparison` linked to the PR.

### Creating the Webhook Secret

The webhook verifies incoming payloads using HMAC-SHA256 (`X-Hub-Signature-256`). Create a Kubernetes Secret containing your shared webhook secret in the `eval-runner` namespace:

```bash
kubectl create secret generic github-webhook \
  --namespace=eval-runner \
  --from-literal=secret=YOUR_WEBHOOK_SECRET
```

*(Note: To use a secret name other than `github-webhook`, set `githubWebhookSecret` in the `eval-runner-config` ConfigMap or pass `--github-webhook-secret=<name>`.)*

### Configuring the GitHub Webhook

In your GitHub repository settings under **Settings -> Webhooks -> Add webhook**:
1. **Payload URL**: `https://<your-domain-or-tunnel>/webhook` (or `/`)
2. **Content type**: `application/json`
3. **Secret**: `YOUR_WEBHOOK_SECRET` (matching the value stored in the Secret)
4. **SSL verification**: Enable SSL verification
5. **Which events would you like to trigger this webhook?**:
   - Select **Let me select individual events**:
     - Check **Pull requests**
     - Check **Pushes**
6. Click **Add webhook**.

### Exposing the Service

`eval-runner-webhook` runs in namespace `eval-runner` and exposes a ClusterIP Service on port 80 (targetPort 8080):
- **Production Ingress**: Configure your cluster's ingress controller or Gateway API (e.g. GKE Gateway or Ingress with a managed TLS certificate) routing traffic to `eval-runner-webhook.eval-runner.svc.cluster.local:80`.
- **Development / Local Testing**: Use a secure tunnel (e.g. Cloudflare Tunnel `cloudflared` or `ngrok`), or port-forward for local testing:
  ```bash
  kubectl port-forward -n eval-runner svc/eval-runner-webhook 8080:80
  ```

### What Gets Created

When an event arrives:
1. **On `pull_request` (`opened`, `synchronize`, `reopened`)**:
   - **Candidate Run**: `pr-<number>-<headSHA7>` (evaluates PR head commit with `defaultModel` and initial `sampleRange {1, defaultBatchSize}`).
   - **Baseline Run**: `base-<baseSHA7>` (evaluates merge target commit with initial `sampleRange {1, defaultBatchSize}`). If an already `Completed` Run with that commit, repo URL, and model exists, it is reused immediately (baseline cache from concept §4.1).
   - **Comparison**: `pr-<number>` linking candidate and baseline runs with `spec.pullRequest` and `spec.sampling {batchSize: defaultBatchSize, maxSamplesPerTask: defaultMaxSamplesPerTask}` configured.
   - **Redelivery & Synchronize**: Redelivery of the same event is idempotent and makes no duplicate objects. A `synchronize` event creates the new candidate Run `pr-<number>-<newHead7>`, keeps previous candidate Runs in place (they are not deleted, preserving execution history), and updates `spec.candidate.run` and `spec.pullRequest.headSHA` on the existing Comparison.
2. **On `pull_request` (`closed`)**:
   - **Cleanup**: Label-selects the Comparison(s) matching `evalrunner.gke-labs.dev/pr` and `spec.pullRequest.repository` and deletes them; their owned batch Runs cascade via Kubernetes owner references.
   - **Seed Runs**: Deletes seed candidate Runs for the PR head. If a seed Run carries an owner reference to a surviving live Comparison or is referenced as `spec.baseline.run` by another surviving Comparison (e.g. stacked branches or fast-forward merges), it is preserved.
   - **Baseline Runs**: Preserved for reuse across PRs.
   - **Reopen**: If a closed PR is subsequently reopened, resources are recreated on the next `pull_request` event.
   - **Artifacts**: Artifacts stored in the artifact sink (GCS or local directory) are not purged by PR closure.
3. **On `push` to the default branch**:
   - **Warm Baseline Run**: `base-<sha7>` is created with initial `sampleRange {1, defaultBatchSize}` so the baseline run is pre-warmed before PRs arrive.
4. **On `issue_comment` (`created`)**:
   - **More Samples Command**: Comments of the form `/eval more <n>` on a pull request trigger evaluation of additional samples.
   - **Authorization**: The author must have write or admin access per the GitHub collaborator permission API, or be listed in the `commandUsers` ConfigMap allow-list. Unauthorized authors receive HTTP 403 Forbidden.
   - **Bounding**: The requested sample count $n$ is bounded by `defaultMaxSamplesPerTask`.
   - **Action**: Increments `spec.extraSamples` on the existing PR Comparison by $n$, scheduling additional adaptive paired batches.
   - **Acknowledgment & Idempotency**: Acknowledges the comment with a GitHub reaction (`eyes` or `+1`) or short comment. Comment IDs are stored in the Comparison annotation `evalrunner.gke-labs.dev/handled-comments`, ensuring redeliveries are strictly idempotent.
5. **Labels**:
   All created resources are labeled with:
   - `evalrunner.gke-labs.dev/pr`: PR number (for PR runs and comparisons)
   - `evalrunner.gke-labs.dev/repo`: Sanitized repository identifier (e.g. `owner_name`)
   - `evalrunner.gke-labs.dev/head-sha`: Commit SHA

### Operational Notes

- **Queue Saturation & 503 Responses**: If event volume fills the bounded in-memory worker queue, the webhook responds with HTTP 503 (`worker queue full`). Note that GitHub does not automatically retry failed deliveries on 5xx responses; operators can trigger manual redeliveries from the repository's Webhooks settings under **Recent Deliveries**.
- **ConfigMap Startup Loading**: The `eval-runner-config` ConfigMap is read at process startup. After modifying settings such as `defaultModel`, `defaultLimit`, or `webhookRepositories`, restart the webhook deployment to pick up changes:
  ```bash
  kubectl rollout restart deployment/eval-runner-webhook -n eval-runner
  ```

## Running Unit Tests

Run all unit tests with:

```bash
go test ./...
```

E2E tests will automatically be skipped unless `RUN_E2E=1` is provided.

## Running End-to-End (E2E) Tests

E2E tests live in `tests/e2e/`. They test eval-runner against a real Kubernetes cluster using `kind`, `kubernetes-sigs/agent-sandbox`, and in-cluster git server fixtures.

### Option 1: Automated via `ap e2e` (Recommended)

Run:

```bash
ap e2e
```

(or `go run github.com/gke-labs/gke-labs-infra/ap@latest e2e .` or `./dev/ci/presubmits/ap-e2e`)

This provides a one-command run with no pre-installed `kind` or `kubectl`:
1. Executes `dev/tasks/test-e2e` to download and install pinned versions of `kind` and `kubectl` into `.build/bin/`.
2. Automatically discovers and executes all Go E2E tests under `tests/e2e/` with `RUN_E2E=1`, running `TestRunDiscovery`.

### Option 2: Running via `go test` directly

If `kind` and `kubectl` are already installed on your system `PATH` (or after running `./dev/tasks/test-e2e` once to populate `.build/bin`):

```bash
RUN_E2E=1 go test -v -count=1 ./tests/e2e/...
```

To run a specific test:

```bash
RUN_E2E=1 go test -v -count=1 ./tests/e2e/... -run TestRunDiscovery
```

> **Note on Kubeconfig Context:** When running tests against a cluster, the harness switches the active kubectl context using `kubectl config use-context kind-<clusterName>`, which updates the current context in your kubeconfig (`~/.kube/config`).

## Environment Variables

The E2E test harness recognizes several environment variables:

| Variable | Default | Description |
|---|---|---|
| `RUN_E2E` | _(unset)_ | When set to `1` (or any non-empty value), enables running the E2E test suite. When unset, E2E tests are skipped. |
| `KIND_CLUSTER_NAME` | `kind` | Name of the kind cluster to create or use. If a cluster with this name already exists, the test reuses it without re-creating it or deleting it afterwards. |
| `SKIP_CLEANUP` | _(unset)_ | When set to `1` (or any non-empty value), preserves the kind cluster and test namespaces created by the test instead of deleting them when the test completes. This is useful for manual debugging, e.g. inspecting pods and resources with `kubectl`. |

## Evals

We dogfood our own evals to measure whether our documentation is understandable to a reader. An Inspect task feeds the documentation (`README.md` and `docs/development.md`) to a model and asks multiple-choice comprehension questions whose answers can only be found in the docs.

The eval is managed with [`uv`](https://docs.astral.sh/uv/) under the `evals/` directory.

### Setup

Install `uv` if not already installed, then install Python dependencies in `evals/`:

```bash
cd evals
uv sync
```

### Running Locally

To run the comprehension eval with a real model, set your provider API key in the environment (e.g. `GEMINI_API_KEY` for Google Gemini models). The default model across code and configuration is `google/gemini-3.5-flash-lite`:

```bash
cd evals
uv run inspect eval docs_comprehension.py@readme_comprehension --model google/gemini-3.5-flash-lite
```

(Note: `--model` can also be omitted when `GEMINI_API_KEY` is set; the task falls back to `DEFAULT_MODEL = "google/gemini-3.5-flash-lite"`.)

To run the blind control task (which omits the documents to check for pretraining leakage):

```bash
cd evals
uv run inspect eval docs_comprehension.py@readme_comprehension_blind --model google/gemini-3.5-flash-lite
```

To run offline without requiring an API key (using Inspect's built-in `mockllm/model` provider):

```bash
cd evals
uv run inspect eval docs_comprehension.py --model mockllm/model
```

### Viewing Results (`inspect view`)

Inspect includes an interactive web UI for inspecting sample logs, evaluation scores, prompts, and model responses:

```bash
cd evals
uv run inspect view
```

This launches a local web server (usually at `http://localhost:7575`) displaying evaluation runs stored in `evals/logs/`.

### How to Add a Question

Questions live in `evals/datasets/readme_questions.jsonl`. Each line is a JSON record with the following schema:

```json
{
  "id": "dev-unit-tests",
  "input": "Which command runs unit tests in the repository?",
  "choices": [
    "go test ./...",
    "npm test",
    "cargo test",
    "The documents do not state this."
  ],
  "target": "A",
  "source": "docs/development.md: Running Unit Tests",
  "evidence": "go test ./..."
}
```

Guidelines for writing questions:
- **`id`**: A unique, stable sample identifier string (e.g. `readme-tagline`, `dev-kind-cluster`). Tasks must assign explicit sample IDs so that cross-commit comparisons pair identical questions by ID rather than relying on positional fallback. Positional IDs are a fallback, not a design. **Invariant: IDs are permanent.** Never rename or renumber an existing ID, and a question whose text, choices, or target changes materially gets a *new* ID (the old one retires). Otherwise the comparison will pair two different questions under the same key.
- **`input`**: A clear question testing comprehension of facts stated in `README.md` or `docs/development.md`.
- **`choices`**: Exactly 4 options (`A`, `B`, `C`, `D`).
- **`target`**: The letter of the correct answer (`A`, `B`, `C`, or `D`).
- **`source`**: The document and section where the answer is found (e.g., `README.md: Core Concepts`, `docs/development.md: Environment Variables`).
- **`evidence`**: A short, non-empty substring from the injected documentation that must be present (verified by `test-evals` in CI as a drift guard).
- **`absent`**: An optional list of non-empty substrings that must NOT appear in the injected documentation (verified by `test-evals` in CI to ensure negative controls do not break when previously omitted facts are added to the documentation).
- **`dataset order`**: Keep dataset order stable by appending new questions to the end of the file. Sampling windows are positional, but explicit IDs ensure cross-commit pairing survives dataset edits.
- **Guard against leakage**: Ask about repo-specific details (names of tools, commands, environment variables, verdicts) rather than general Kubernetes or software engineering facts.
- **Negative controls ("Not stated")**: Include questions about plausible facts that are intentionally omitted from our documentation, with the correct target being "The documents do not state this." (or similar). This punishes models that guess confidently.

